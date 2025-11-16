import pandas as pd


def standardize_no_body(val):
    if pd.isna(val) or str(val).strip() == "":
        return None

    val = str(val).replace("_", "-").replace(" ", "-")
    parts = val.split("-")

    if len(parts) < 2:
        return f"{parts[0]}-000"

    prefix = parts[0]
    digits = ''.join([x for x in parts[1] if x.isdigit()]).zfill(3)
    return f"{prefix}-{digits}"


def transform_data(ti):

    df_transaksi_bus = pd.read_json(ti.xcom_pull(key="transaksi_bus"))
    df_transaksi_halte = pd.read_json(ti.xcom_pull(key="transaksi_halte"))

    df_transaksi_halte["no_body_var"] = None

    df = pd.concat([df_transaksi_bus, df_transaksi_halte], ignore_index=True)
    df = df.drop_duplicates()

    df['no_body_var'] = df['no_body_var'].astype(str).apply(standardize_no_body)
    df["tanggal"] = pd.to_datetime(df["waktu_transaksi"]).dt.date
    df["is_pelanggan"] = df["status_var"].apply(lambda x: 1 if x == "S" else 0)
    df["amount"] = df["fare_int"]

    df_routes = pd.read_json(ti.xcom_pull(key="routes"))
    df_realisasi = pd.read_json(ti.xcom_pull(key="realisasi_bus"))
    df_realisasi["bus_body_no"] = df_realisasi["bus_body_no"].astype(str).apply(standardize_no_body)

    df = df.merge(df_realisasi, left_on="no_body_var", right_on="bus_body_no", how="left")
    possible_route_cols = ["route_code_var", "route_code", "rute_realisasi"]
    route_col_found = next((col for col in possible_route_cols if col in df.columns), None)

    if route_col_found is None:
        raise Exception(f"No route code column found. Available columns: {df.columns.tolist()}")

    df = df.merge(df_routes, left_on=route_col_found, right_on="route_code", how="left")

    ti.xcom_push("fact_transaction", df.to_json())


    # Aggregation
    agg_by_card = df.groupby(["tanggal", "card_type_var", "gate_in_boo"]) \
        .agg(pelanggan=("is_pelanggan", "sum"), amount=("amount", "sum")).reset_index()

    agg_by_tarif = df.groupby(["tanggal", "fare_int", "gate_in_boo"]) \
        .agg(pelanggan=("is_pelanggan", "sum"), amount=("amount", "sum")) \
        .reset_index().rename(columns={"fare_int": "tarif"})

    agg_by_route = df.groupby(["tanggal", "route_code", "route_name", "gate_in_boo"]) \
        .agg(pelanggan=("is_pelanggan", "sum"), amount=("amount", "sum")).reset_index()

    ti.xcom_push("agg_by_card", agg_by_card.to_json())
    ti.xcom_push("agg_by_tarif", agg_by_tarif.to_json())
    ti.xcom_push("agg_by_route", agg_by_route.to_json())

    print("Transform and aggregate completed.")
