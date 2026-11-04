from datetime import timedelta, datetime, date
from io import BytesIO

import requests
from airflow.sdk import DAG
from airflow.sdk import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
import logging
import pyarrow as pa
import pyarrow.parquet as pq

from s3_bucket_service.s3_class import S3BucketService
from extensions_for_orchestration.validators import validate_cities
from extensions_for_orchestration.fetch_city_data import fetch_city_data


def get_s3_client():
    return S3BucketService(
        bucket_name='weather-data',
        endpoint='https://s3.storage.selcloud.ru',
        access_key=Variable.get("SELECTEL_ACCESS_KEY"),
        secret_key=Variable.get("SELECTEL_SECRET_KEY"),
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
    'max_retry_delay': timedelta(minutes=10),
}


with DAG(
    dag_id='dag_daily_weather',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
) as dag:

    @task
    def get_list_cities() -> dict:
        """Города + список daily-переменных из БД."""
        sql_select_cities = 'SELECT name, lat, lon FROM cities ORDER BY name'
        sql_select_variables = 'SELECT variables FROM daily_variables ORDER BY variables'

        try:
            hook = PostgresHook(postgres_conn_id="weather_dwh")
            cities_records = hook.get_records(sql_select_cities)
            variables_records = hook.get_records(sql_select_variables)
            task_logger.debug(
                f"Городов: {len(cities_records)}, переменных: {len(variables_records)}"
            )
            return {
                "cities": cities_records,
                "variables": [row[0] for row in variables_records],
            }
        except Exception as error:
            task_logger.error(f"Error fetching cities/variables: {error}")
            raise

    @task
    def build_city_payloads(payload: dict) -> list[dict]:
        """
        Возвращает список словарей: по одному на город.
        Каждый элемент содержит city/lat/lon и общий список variables.
        """
        cities = validate_cities(payload["cities"])
        variables = payload["variables"]
        return [
            {"city": c["city"], "lat": c["lat"], "lon": c["lon"], "variables": variables}
            for c in cities
        ]

    @task(max_active_tis_per_dag=5)
    def get_city_weather(city_data: dict) -> str:
        """Запрашивает один день по одному городу и кладёт Parquet в S3."""
        city = city_data["city"]
        lat = city_data["lat"]
        lon = city_data["lon"]
        variables = city_data["variables"]

        # Архив отстаёт, поэтому берём вчерашний день (можно расширить до 7 дней назад)
        target_date: date = date.today() - timedelta(days=1)

        try:
            df = fetch_city_data(
                city=city,
                lat=lat,
                lon=lon,
                start_date=target_date.isoformat(),
                end_date=target_date.isoformat(),
                daily_variables=variables,
            )

            if df is None or df.empty:
                task_logger.warning(f"[{city}] {target_date}: данных нет в архиве, пропуск")
                return f"skipped:{city}:{target_date}"

            table = pa.Table.from_pandas(df)
            buffer = BytesIO()
            pq.write_table(table, buffer, compression="snappy")

            s3 = get_s3_client()
            key_prefix = (
                f"weather-data/weather-data/raw/open-meteo/"
                f"city={city}/date={target_date.isoformat()}"
            )
            s3.upload_file_object(
                prefix=key_prefix,
                source_file_name="data.parquet",
                content=buffer.getvalue(),
            )
            task_logger.info(f"[{city}] {target_date} -> s3://{key_prefix}/data.parquet")
            return f"s3://{key_prefix}/data.parquet"

        except requests.exceptions.HTTPError as http_err:
            task_logger.error(f"HTTP ошибка [{city}]: {http_err}")
            raise
        except requests.exceptions.RequestException as req_err:
            task_logger.error(f"Ошибка запроса [{city}]: {req_err}")
            raise
        except Exception as e:
            task_logger.error(f"Ошибка [{city}]: {e}")
            raise

    # Пайплайн
    records = get_list_cities()
    payloads = build_city_payloads(records)
    get_city_weather.expand(city_data=payloads)