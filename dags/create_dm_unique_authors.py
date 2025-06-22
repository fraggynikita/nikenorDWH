from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow import DAG



OWNER = "nikenor"
DAG_ID = "create_dm_count_unique_authors"

LAYER = "raw"
SOURCE = "news"
SCHEMA = "dm"
TARGET_TABLE = "count_day_unique_authors"

POSTGRES_CONN = "postgres_dwh"

args = {
    "owner": OWNER,
    "start_date": datetime(2025, 6, 10),
    "catchup": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=20)
}

with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 5 * * *",
    default_args=args,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:

    start = EmptyOperator(
        task_id="start",
    )

    sensor_on_smth = ExternalTaskSensor(
        task_id="sensor_on_smth",
        external_dag_id="from_s3_to_pg",
        external_task_id="from_s3_to_pg",
        allowed_states=["success"],
        mode="poke", 
        timeout=3600, 
        poke_interval=60, 
    )

    drop_stg_table_before = SQLExecuteQueryOperator(
        task_id="drop_stg_table_before",
        conn_id=POSTGRES_CONN,
        autocommit=True,
        sql=f"""
        DROP TABLE IF EXISTS stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}"
        """
    )

    create_stg_table = SQLExecuteQueryOperator(
        task_id="create_stg_table",
        conn_id=POSTGRES_CONN,
        autocommit=True,
        sql=f"""
        CREATE TABLE IF NOT EXISTS stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}" AS
        SELECT 
            COUNT(DISTINCT(author)) as unique_author, 
            date(published_at) as published_at
        FROM
            ods.news
        WHERE 
            DATE(published_at) = '{{{{ data_interval_start.format('YYYY-MM-DD') }}}}'
        GROUP BY DATE(published_at)
        """
    )

    drop_from_target_table = SQLExecuteQueryOperator(
        task_id="drop_from_target_table",
        conn_id=POSTGRES_CONN,
        autocommit=True,
        sql=f"""
        DELETE FROM {SCHEMA}.{TARGET_TABLE}
        WHERE DATE(published_at) IN
        (
            SELECT published_at FROM stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}"
        )
        """
    )

    insert_into_target_table = SQLExecuteQueryOperator(
        task_id="insert_into_target_table",
        conn_id=POSTGRES_CONN,
        autocommit=True,
        sql=f"""
        INSERT INTO {SCHEMA}.{TARGET_TABLE}
        SELECT * FROM stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}"
        """
    )
    

    drop_stg_table_after = SQLExecuteQueryOperator(
        task_id='drop_stg_table_after',
        conn_id=POSTGRES_CONN,
        autocommit=True,
        sql=f"""
        DROP TABLE IF EXISTS stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}"
        """
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> sensor_on_smth >> drop_stg_table_before >> create_stg_table >> drop_from_target_table >> insert_into_target_table >> drop_stg_table_after >> end

    