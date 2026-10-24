from datetime import timedelta, datetime
import json

import requests
from airflow.sdk import DAG
from airflow.sdk import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
import logging

from s3_bucket_service.s3_class import S3BucketService
from .extensions_for_orchestration.validators import validate_cities


def get_s3_client():
    """
    Creating client s3 storage
    """
    return S3BucketService(
        bucket_name = 'weather-data-pirate',
        endpoint='https://s3.storage.selcloud.ru',
        access_key=Variable.get("SELECTEL_ACCESS_KEY"),
        secret_key=Variable.get("SELECTEL_SECRET_KEY")
    )

task_logger = logging.getLogger("airflow.task")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(year=2025, month=11, day=10),
    'email_on_failure': True,
    'email': ['georgijmironenko36@gmail.com'],
    'retries': 4,
    'retry_delay': timedelta(minutes=2),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=10)
}


with DAG(
    dag_id='weather_data_get',
    default_args=default_args,
    schedule='@hourly',
    catchup=False,
) as dag:

    @task
    def get_list_cities():
        """
        Эта задача получает список городов из базы данных
        :return: Список городов
        """
        sql_select = 'SELECT name, lat, lon FROM "cities"'
        try:
            hook = PostgresHook(postgres_conn_id="my_postgres")
            task_logger.debug("Успешное подключение к postgres")

            records = hook.get_records(sql_select)
            task_logger.debug("Получили данные из бд")

            return records

        except Exception as error:
            task_logger.error(f"Error fetching cities: {error}")
            raise


    @task
    def get_cities_list(records) -> list[dict] | None:
        """Валидирует города из БД."""
        return validate_cities(records)

    @task
    def get_city_weather(city_data: str):
        """
        Функция для получения данных о погоде из API
        :return: Словарь с данными о погоде.
        :raises requests.exceptions.HTTPError: При HTTP ошибках.
        :raises requests.exceptions.RequestException: При ошибках запроса.
        """
        city = city_data[0]
        lat = city_data[1]
        lon = city_data[2]

        # Получаем api ключ из airflow
        API_KEY = Variable.get("API_PIRATEWEATHER")

        if not API_KEY or not API_KEY.strip():
            raise ValueError("API_PIRATEWEATHER не задан в Variable")

        url = f"https://api.pirateweather.net/forecast/{API_KEY}/{lat:.4f},{lon:.4f}"

        # Строим api ссылку
        params = {
            "units": "si",
            "lang": "ru",
        }

        try:
            response = requests.get(url=url, params=params, timeout=60)
            task_logger.debug("Сделали запрос к Api")

            response.raise_for_status()
            task_logger.debug("Проверили на ошибку")

            data = response.json()
            task_logger.debug("Успешно преобразовали в json")

            s3 = get_s3_client()
            task_logger.debug("Клиент s3 создан")

            json_bytes = json.dumps(data, ensure_ascii=False).encode('utf-8')

            file_name = (f'data_{city}-{datetime.now().year}-{datetime.now().month:02d}-{datetime.now().day}'
                         f'-{datetime.now().time()}.json')
            task_logger.debug("Название файла создано")

            # Отправка данных
            prefix = (f"raw/pirateweather/forecast/{datetime.now().year}/"
                      f"{datetime.now().month:02d}/{datetime.now().day}/")
            s3.upload_file_object(prefix, file_name, json_bytes)

            task_logger.info("Успешно отправили файл в s3")

        except requests.exceptions.HTTPError as http_err:
            task_logger.error(f"HTTP ошибка: {http_err}")
            raise
        except requests.exceptions.RequestException as req_err:
            task_logger.error(f"Ошибка запроса: {req_err}")
            raise
        except Exception as e:
            task_logger.error(f"Произошла ошибка: {e}")
            raise

    # Получаем список городов
    records = get_list_cities()

    # Преобразовываем в нужный формат
    cities = get_cities_list(records)

    # Получаем данные о погоде для каждого города
    get_city_weather.expand(city=cities).max_active_tis_per_dag(5)

