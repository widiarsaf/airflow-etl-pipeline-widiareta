import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

DATA_PATH = "/opt/airflow/data/input/"
OUTPUT_PATH = "/opt/airflow/data/output/"


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
    


def transform_data(ti):
    df_transaksi_bus = pd.read_json(ti.xcom_pull(key="transaksi_bus"))
    df_transaksi_halte = pd.read_json(ti.xcom_pull(key="transaksi_halte"))
    df_transaksi_halte["no_body_var"] = None

    df = pd.concat([df_transaksi_bus, df_transaksi_halte], ignore_index=True)
    df = df.drop_duplicates()

    def standardize_no_body(val):
        if pd.isna(val) or str(val).strip() == "":
            return None  # Halte transactions → No bus body → Leave null

        val = str(val).replace("_", "-").replace(" ", "-")
        parts = val.split("-")

        # If value is only "BRT" or "TJ" without number
        if len(parts) < 2:
            return f"{parts[0]}-000"

        prefix = parts[0]
        digits = ''.join([x for x in parts[1] if x.isdigit()])
        digits = digits.zfill(3)

        return f"{prefix}-{digits}"
    
    df['no_body_var'] = df["no_body_var"].astype(str).apply(standardize_no_body)
    df["tanggal"] = pd.to_datetime(df["waktu_transaksi"]).dt.date
    df["is_pelanggan"] = df["status_var"].apply(lambda x: 1 if x == "S" else 0)
    df["amount"] = df["fare_int"]

    # Aggregation by cards
    agg_by_card = df.groupby(
        ["tanggal", "card_type_var", "gate_in_boo"]
    ).agg(
        pelanggan=("is_pelanggan", "sum"),     
        amount=("amount", "sum")               
    ).reset_index()

    # Aggregation by tarif
    agg_by_tarif = df.groupby(
        ["tanggal", "fare_int", "gate_in_boo"]
    ).agg(
        pelanggan=("is_pelanggan", "sum"),
        amount=("amount", "sum")
    ).reset_index()
    agg_by_tarif = agg_by_tarif.rename(columns={"fare_int": "tarif"})

    # Aggregation by routes
    df_routes = pd.read_json(ti.xcom_pull(key="routes"))
    df_realisasi = pd.read_json(ti.xcom_pull(key="realisasi_bus"))

    df_realisasi["bus_body_no"] = df_realisasi["bus_body_no"].astype(str).apply(standardize_no_body)

    df_route_join = df.merge(
        df_realisasi,
        how="left",
        left_on="no_body_var",
        right_on="bus_body_no"
    )

    possible_route_cols = ["route_code_var", "route_code", "rute_realisasi"]

    route_col_found = None
    for col in possible_route_cols:
        if col in df_route_join.columns:
            route_col_found = col
            break

    if not route_col_found:
        raise Exception(f"❌ No route code column found in merged data. Columns: {df_route_join.columns.tolist()}")

    df_route_join = df_route_join.merge(
        df_routes,
        left_on=route_col_found,
        right_on="route_code",
        how="left"
    )


    agg_by_route = df_route_join.groupby(
        ["tanggal", "route_code", "route_name", "gate_in_boo"]
    ).agg(
        pelanggan=("is_pelanggan", "sum"),
        amount=("amount", "sum")
    ).reset_index()


    ti.xcom_push("agg_by_card", agg_by_card.to_json())
    ti.xcom_push("agg_by_tarif", agg_by_tarif.to_json())
    ti.xcom_push("agg_by_route", agg_by_route.to_json())
    
    print("Tranforming and aggregating data completed")


def load_function():
    print("Loading data...")


with DAG(
    dag_id="etl_transaction_data_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["ETL", "extract"]
) as dag:
    start = EmptyOperator(
        task_id="start"
    )

    # Extract Data: read csv, insert to postgreSQL

    load_csv_to_postgres = PythonOperator(
        task_id="load_csv_to_postgres",
        python_callable=load_csv_to_postgres
    )

    extract_all_data_sources = PythonOperator(
        task_id="extract_all_data_sources",
        python_callable=extract_all_data_sources,
        provide_context=True
    )

    
    # Transform Data
    transform_data = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
        provide_context=True
    )


    # Load Data
    load_data = PythonOperator(
        task_id="load_data",
        python_callable=load_function
    )


    end = EmptyOperator(
            task_id="end"
        )


    start >> load_csv_to_postgres >> extract_all_data_sources >> transform_data >> load_data >> end
