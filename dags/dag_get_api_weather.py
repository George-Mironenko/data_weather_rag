from datetime import timedelta, datetime
import json

import requests
from airflow.sdk import DAG
from airflow.sdk import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

import logging

from s3_bucket_service.s3_class import S3BucketService


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
    def get_cities_list(
            records: tuple[tuple[str, float, float], ...]
    ) -> list[dict] | None:
        """
        Преобразует список кортежей (name, lat, lon) в список словарей.

        Проверки для каждой записи:
          1. Запись — кортеж из 3 элементов.
          2. name — непустая строка.
          3. lat/lon — числа (int, float, Decimal).
          4. lat/lon — не NaN и не inf.
          5. lat в диапазоне [-90, 90], lon — [-180, 180].

        :param records: Список кортежей (name, lat, lon) из БД
        :return: Список словарей [{"city": ..., "lat": ..., "lon": ...}]
                 или None, если ни одна запись не прошла валидацию
        """
        try:
            if not records:
                task_logger.error("Ошибка: в функцию передан пустой список")
                return None

            def write_log(warning: str):
                task_logger.warning(f"Запись #{idx} пропущена: {warning}")

            cities_list = []

            for idx, record in enumerate(records, 1):
                # 1. Проверка, что запись — кортеж
                if not isinstance(record, tuple):
                    write_log("ожидался tuple")
                    continue

                # 2. Проверка длины кортежа (name, lat, lon)
                if len(record) != 3:
                    write_log(f"ожидалось 3 элемента, получено {len(record)}")
                    continue

                name, lat, lon = record

                # 3. Проверка названия города
                if not isinstance(name, str):
                    write_log("название города не строка")
                    continue

                if not name.strip():
                    write_log("пустая строка или пробелы в названии")
                    continue

                # 4. Проверка координат на числовой тип
                # Postgres может вернуть Decimal, поэтому проверяем через (int, float, Decimal)
                if not isinstance(lat, (int, float)):
                    write_log(f"широта не число: {type(lat).__name__}")
                    continue

                if not isinstance(lon, (int, float)):
                    write_log(f"долгота не число: {type(lon).__name__}")
                    continue

                # 5. Приведение к float (Decimal -> float)
                lat_f = float(lat)
                lon_f = float(lon)

                # 6. Проверка, что координаты не NaN / inf
                import math
                if math.isnan(lat_f) or math.isinf(lat_f):
                    write_log(f"широта NaN или inf: {lat}")
                    continue

                if math.isnan(lon_f) or math.isinf(lon_f):
                    write_log(f"долгота NaN или inf: {lon}")
                    continue

                # 7. Проверка диапазонов: широта -90..90, долгота -180..180
                if not (-90.0 <= lat_f <= 90.0):
                    write_log(f"широта вне диапазона [-90, 90]: {lat_f}")
                    continue

                if not (-180.0 <= lon_f <= 180.0):
                    write_log(f"долгота вне диапазона [-180, 180]: {lon_f}")
                    continue

                # 8. Всё ок — добавляем
                cities_list.append({
                    "city": name.strip(),
                    "lat": lat_f,
                    "lon": lon_f,
                })

            task_logger.debug("Успешно список преобразован")

            if not cities_list:
                task_logger.error("Ни одна запись не прошла валидацию.")
                return None

            task_logger.info(
                f"Прошли валидацию {len(cities_list)} из {len(records)} записей"
            )
            return cities_list

        except Exception as error:
            task_logger.error(
                f"Ошибка при преобразовании списка кортежей в список словарей: {error}"
            )
            return None

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
            response = requests.get(url=url, params=params, timeout=30)
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

