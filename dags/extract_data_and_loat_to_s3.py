import logging
import duckdb
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import requests
from datetime import datetime, timedelta
import pandas as pd


OWNER = "nikenor"
DAG_ID = "get_data_and_load_to_s3"

ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")

LAYER = "raw"
SOURCE = "news"

MINIO_BUCKET = "prod"
MINIO_ENDPOINT = "minio:9000"

API_KEY = Variable.get("api_key")  

args = {
    "owner": OWNER,
    "start_date": datetime(2025, 6, 10),
    "catchup": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=20)
}

def get_dates(**kwargs) -> tuple[str, str]:
    """
    Получаем start_date и end_date в формате строки
    """
    start_date = kwargs["data_interval_start"] - timedelta(days=2)
    end_date = kwargs["data_interval_end"] - timedelta(days=2)
    
    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")

def from_api_to_s3(**kwargs):
    """
    Получение данных из API и загрузка их в S3.
    """
    start_date, end_date = get_dates(**kwargs)

    logging.info(f"💻 Загрузка за: {start_date}/{end_date}")

    url = f"https://newsapi.org/v2/everything?q=*&from={start_date}&to={end_date}&language=en&sortBy=popularity&apiKey={API_KEY}"

    logging.info(f"Отправка запроса к API по адресу: {url}")
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        logging.info(f"Получены данные от API.")

        if "articles" in data and len(data["articles"]) > 0:
            df = pd.DataFrame(data["articles"])

            logging.info(f"Получено {len(df)} новостей.")

            try:
                
                con = duckdb.connect()

                con.sql(f"""
                    SET TIMEZONE='UTC';
                    INSTALL httpfs;
                    LOAD httpfs;
                    SET s3_url_style = 'path';
                    SET s3_endpoint = '{MINIO_ENDPOINT}';
                    SET s3_access_key_id = '{ACCESS_KEY}';
                    SET s3_secret_access_key = '{SECRET_KEY}';
                    SET s3_use_ssl = FALSE;
                """)

                save_path = f"s3://{MINIO_BUCKET}/{LAYER}/{SOURCE}/{start_date}/{end_date}_news_data.parquet"
                con.from_df(df).to_parquet(save_path)

                logging.info(f"Данные успешно загружены в S3: {save_path}")

            except Exception as e:
                logging.error(f"Ошибка при работе с DuckDB: {e}")
                raise
        else:
            logging.error("API не вернуло поле 'articles' или данные пусты.")
            raise
    else:
        logging.error(f"Ошибка при запросе API: {response.status_code} - {response.text}")
        raise

with DAG(
    dag_id=DAG_ID,
    schedule="0 9 * * *",  
    default_args=args,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:

    start = EmptyOperator(
        task_id="start",
    )

    from_api_to_s3_task = PythonOperator(
        task_id="from_api_to_s3_task",
        python_callable=from_api_to_s3,
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> from_api_to_s3_task >> end
