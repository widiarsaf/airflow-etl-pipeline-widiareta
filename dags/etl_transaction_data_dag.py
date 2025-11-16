from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from pendulum import timezone
import sys
sys.path.append("/opt/airflow/scripts")

from extract_data import load_csv_to_postgres, extract_all_data_sources
from transform_data import transform_data
from load_data import load_data

local_tz = timezone("Asia/Jakarta")

with DAG(
    dag_id="etl_transaction_data_dag",
    start_date=datetime(2025, 11, 1, tzinfo=local_tz),
    schedule_interval="0 7 * * *",
    catchup=False,
    tags=["ETL", "extract"]
) as dag:

    start = EmptyOperator(task_id="start")

    load_csv = PythonOperator(
        task_id="load_csv_to_postgres",
        python_callable=load_csv_to_postgres
    )

    extract_all = PythonOperator(
        task_id="extract_all_data_sources",
        python_callable=extract_all_data_sources
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data
    )

    load = PythonOperator(
        task_id="load_data",
        python_callable=load_data
    )

    end = EmptyOperator(task_id="end")

    start >> load_csv >> extract_all >> transform >> load >> end
