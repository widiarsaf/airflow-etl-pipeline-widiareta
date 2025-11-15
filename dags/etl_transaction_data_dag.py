import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

DATA_PATH = "/opt/airflow/data/input/"

def extract_csv_files():
    print("Reading all csv files")

    transaksi_bus = pd.read_csv(DATA_PATH + "dummy_transaksi_bus.csv")
    transaksi_halte = pd.read_csv(DATA_PATH + "dummy_transaksi_halte.csv")
    routes = pd.read_csv(DATA_PATH + "dummy_routes.csv")
    realisasi_bus = pd.read_csv(DATA_PATH + "dummy_realisasi_bus.csv")
    shelter_corridor = pd.read_csv(DATA_PATH + "dummy_shelter_corridor.csv")

    return {
        "transaksi_bus" : transaksi_bus.to_json(),
        "transaksi_halte" : transaksi_halte.to_json(),
        "routes" : routes.to_json(),
        "realisasi_bus" : realisasi_bus.to_json(),
        "shelter_corridor" : shelter_corridor.to_json()
    }

# def insert_transaksiBus_to_postgres():
#     df = pd.read_csv(DATA_PATH + "dummy_transaksi_bus.csv")
#     pg = PostgresHook(postgres_conn_id="postgres_default")

#     engine = pg.get_sqlalchemy_engine()

#     df.to_sql("dummy_transaksi_bus", engine, if_exists="replace", index=False)
#     print("dummy_transaksi_bus inserted into PostgreSQL")

# def insert_transaksiHalte_to_postgres():
#     df = pd.read_csv(DATA_PATH + "dummy_transaksi_halte.csv")
#     pg = PostgresHook(postgres_conn_id="postgres_default")

#     engine = pg.get_sqlalchemy_engine()

#     df.to_sql("dummy_transaksi_halte", engine, if_exists="replace", index=False)
#     print("dummy_transaksi_halte inserted into PostgreSQL")


def transform_function():
    print("Transforming data...")

def load_function():
    print("Loading data...")







with DAG(
    dag_id="etl_transaction_data_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["ETL", "extract"]
) as dag:

    # Extract Data: read csv, insert to postgreSQL
    extract_data = PythonOperator(
        task_id="extract_csv_files",
        python_callable=extract_csv_files
    )

    # insert_transaksiBus = PythonOperator(
    #     task_id="insert_transaksiBus_to_postgres",
    #     python_callable=insert_transaksiBus_to_postgres
    # )

    # insert_transaksiHalte = PythonOperator(
    #     task_id="insert_transaksiHalte_to_postgres",
    #     python_callable=insert_transaksiHalte_to_postgres
    # )


    # Transform Data
    transform_data = PythonOperator(
        task_id="transform_data",
        python_callable=transform_function
    )


    # Load Data
    load_data = PythonOperator(
        task_id="load_data",
        python_callable=load_function
    )

    extract_data >> transform_data >> load_data
