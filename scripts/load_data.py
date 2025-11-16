import pandas as pd
import os
from airflow.providers.postgres.hooks.postgres import PostgresHook

OUTPUT_PATH = "/opt/airflow/data/output/"


def load_data(ti):

    fact_transaction = pd.read_json(ti.xcom_pull(key="fact_transaction"))
    agg_by_card = pd.read_json(ti.xcom_pull(key="agg_by_card"))
    agg_by_tarif = pd.read_json(ti.xcom_pull(key="agg_by_tarif"))
    agg_by_route = pd.read_json(ti.xcom_pull(key="agg_by_route"))

    os.makedirs(OUTPUT_PATH, exist_ok=True)

    fact_transaction.to_csv(f"{OUTPUT_PATH}/fact_transaction.csv", index=False)
    agg_by_card.to_csv(f"{OUTPUT_PATH}/report_by_card.csv", index=False)
    agg_by_tarif.to_csv(f"{OUTPUT_PATH}/report_by_tarif.csv", index=False)
    agg_by_route.to_csv(f"{OUTPUT_PATH}/report_by_route.csv", index=False)

    print("CSV output saved.")

    pg = PostgresHook(postgres_conn_id="postgres_default")
    engine = pg.get_sqlalchemy_engine()

    fact_transaction.to_sql("fact_transaction", engine, if_exists="replace", index=False)
    agg_by_card.to_sql("report_by_card", engine, if_exists="replace", index=False)
    agg_by_tarif.to_sql("report_by_tarif", engine, if_exists="replace", index=False)
    agg_by_route.to_sql("report_by_route", engine, if_exists="replace", index=False)

    print("All data successfully loaded to PostgreSQL.")
