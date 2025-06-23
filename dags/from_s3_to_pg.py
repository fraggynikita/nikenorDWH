import logging
import duckdb
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable
from datetime import datetime, timedelta

OWNER = "nikenor"
DAG_ID = "from_s3_to_pg"

ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")

LAYER = "raw"
SOURCE = "news"
SCHEMA = "ods"
TARGET_TABLE = "news"

MINIO_BUCKET = "prod"
MINIO_ENDPOINT = "minio:9000"  

PASSWORD = Variable.get("pg_password")

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

def from_s3_to_pg(**args):
    """
    Загрузка данных из S3 в PostgreSQL
    """
    start_date, end_date = get_dates(**args)

    logging.info(f"Connecting to DuckDB and PostgreSQL for {start_date} to {end_date}")

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

        
        con.sql(f"""
            CREATE SECRET dwh_postgres (
                TYPE postgres,
                HOST 'postgres_dwh',
                PORT 5432,
                DATABASE postgres,
                USER 'postgres',
                PASSWORD 'postgres'
            );

            ATTACH '' AS dwh_postgres_db (TYPE postgres, SECRET dwh_postgres);
        """)

        con.sql(f"""
            INSERT INTO dwh_postgres_db.{SCHEMA}.{TARGET_TABLE}
            (
                source,
                author,
                title,
                description,
                url,
                published_at,
                content
            )
            SELECT 
                source->>'name' AS source,
                author,
                title,
                description,
                url,
                publishedAt as published_at,
                content
            FROM 's3://{MINIO_BUCKET}/{LAYER}/{SOURCE}/{start_date}/{end_date}_news_data.parquet';
        """)

        logging.info("Данные успешно загружены в PostgreSQL")
    except Exception as e:
        logging.error(f"Ошибка с DuckDB или PostgreSQL: {e}")
        raise
    finally:
        con.close()

with DAG(
    dag_id=DAG_ID,
    schedule="0 9 * * *",
    default_args=args,
    max_active_tasks=1,
    max_active_runs=1,
    concurrency=1,
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    sensor_ods = ExternalTaskSensor(
        task_id="sensor_ods",
        external_dag_id="get_data_and_load_to_s3", 
        external_task_id="from_api_to_s3_task",  
        allowed_states=["success"],
        mode="poke", 
        timeout=3600, 
        poke_interval=60,  
    )

    from_s3_to_pg = PythonOperator(
        task_id="from_s3_to_pg_task",
        python_callable=from_s3_to_pg,
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> sensor_ods >> from_s3_to_pg >> end
