import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

DATA_PATH = "/opt/airflow/data/input/"

def load_csv_to_postgres():
    df_transaksi_bus = pd.read_csv(DATA_PATH + "dummy_transaksi_bus.csv")
    df_transaksi_halte = pd.read_csv(DATA_PATH + "dummy_transaksi_halte.csv")

    pg = PostgresHook(postgres_conn_id="postgres_default")
    engine = pg.get_sqlalchemy_engine()

    df_transaksi_bus.to_sql("dummy_transaksi_bus", engine, if_exists="replace", index=False)
    df_transaksi_halte.to_sql("dummy_transaksi_halte", engine, if_exists="replace", index=False)
    print("Insert data into PostgreSQL completed")


    
def extract_all_data_sources(ti):
    df_routes = pd.read_csv(DATA_PATH + "dummy_routes.csv")
    df_realisasi_bus = pd.read_csv(DATA_PATH + "dummy_realisasi_bus.csv")
    df_shelter_corridor = pd.read_csv(DATA_PATH + "dummy_shelter_corridor.csv")

    pg = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg.get_conn()

    df_transaksi_bus = pd.read_sql("SELECT * FROM dummy_transaksi_bus", conn)
    df_transaksi_halte = pd.read_sql("SELECT * FROM dummy_transaksi_halte", conn)

    # Send all to Transform via XCom
    ti.xcom_push(key="routes", value=df_routes.to_json())
    ti.xcom_push(key="realisasi_bus", value=df_realisasi_bus.to_json())
    ti.xcom_push(key="shelter_corridor", value=df_shelter_corridor.to_json())
    ti.xcom_push(key="transaksi_bus", value=df_transaksi_bus.to_json())
    ti.xcom_push(key="transaksi_halte", value=df_transaksi_halte.to_json())

    print("Extracting data sources completed")