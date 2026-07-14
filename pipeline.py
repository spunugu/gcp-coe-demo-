"""
Pipeline logic for the GCP Data & AI CoE demo, kept independent of Streamlit
so it can be unit tested and reused (e.g. from a batch job or notebook)
without needing a Streamlit runtime.
"""

import json
import logging
import numpy as np
import pandas as pd

logger = logging.getLogger("gcp_coe_pipeline")

REGIONS = ["North America", "EMEA", "APAC", "LATAM"]
PRODUCTS = ["Data Platform", "Analytics Suite", "ML Toolkit", "Streaming Connector", "BI Dashboard"]

REQUIRED_COLUMNS = {"order_id", "region", "product", "quantity", "unit_price", "order_date"}


def _lineage_entry(stage, rows_before, rows_after, notes):
    return {
        "stage": stage,
        "rows_before": rows_before,
        "rows_after": rows_after,
        "notes": notes,
        "timestamp": pd.Timestamp.now().strftime("%H:%M:%S"),
    }


def generate_sample_data(n=800, seed=7):
    rng = np.random.default_rng(seed)
    order_ids = np.arange(1, n + 1)
    dates = pd.date_range("2026-01-01", "2026-06-30", periods=n)
    date_choices = pd.to_datetime(rng.choice(dates.to_numpy(), size=n))
    df = pd.DataFrame({
        "order_id": order_ids,
        "order_date": date_choices,
        "region": rng.choice(REGIONS, size=n, p=[0.4, 0.3, 0.2, 0.1]).astype(str),
        "product": rng.choice(PRODUCTS, size=n).astype(str),
        "quantity": rng.integers(1, 20, size=n).astype("int64"),
        "unit_price": rng.choice([99.0, 149.0, 249.0, 499.0, 999.0], size=n).astype("float64"),
        "customer_email": [f"customer{i}@example.com" for i in order_ids],
    })
    dupe_idx = rng.choice(df.index, size=int(n * 0.03), replace=False)
    df = pd.concat([df, df.loc[dupe_idx]], ignore_index=True)
    null_idx = rng.choice(df.index, size=int(n * 0.02), replace=False)
    df.loc[null_idx, "unit_price"] = np.nan
    bad_qty_idx = rng.choice(df.index, size=int(n * 0.01), replace=False)
    df.loc[bad_qty_idx, "quantity"] = -1
    spike_idx = rng.choice(df.index, size=6, replace=False)
    df.loc[spike_idx, "quantity"] = rng.integers(150, 300, size=6)
    df = df.sample(frac=1, random_state=seed).reset_index(drop=True)
    # Explicit dtypes as a final pass - defends against Arrow-based table
    # rendering crashing on ambiguous/object dtype columns.
    df["order_date"] = pd.to_datetime(df["order_date"])
    df["order_id"] = df["order_id"].astype("int64")
    df["quantity"] = df["quantity"].astype("int64")
    df["unit_price"] = df["unit_price"].astype("float64")
    df["region"] = df["region"].astype(str)
    df["product"] = df["product"].astype(str)
    df["customer_email"] = df["customer_email"].astype(str)
    return df


def validate_schema(df):
    missing = REQUIRED_COLUMNS - set(df.columns)
    return sorted(missing)


# ---------------------------------------------------------------------------
# Real GCP infrastructure calls. Imports are lazy so this module (and its
# tests) work fine without the google-cloud-* packages installed unless
# these specific functions are actually called.
# ---------------------------------------------------------------------------

def fetch_bigquery_data(project_id, query):
    from google.cloud import bigquery
    client = bigquery.Client(project=project_id)
    return client.query(query).to_dataframe()


def read_csv_path(path_or_uri):
    """Reads a CSV from a local file path or a gs:// URI. GCS URIs need
    the optional 'gcsfs' package installed (pip install gcsfs) - pandas
    uses it automatically via the gs:// scheme."""
    return pd.read_csv(path_or_uri)


def read_from_kafka(bootstrap_servers, topic, group_id="gcp-coe-demo", max_messages=50, timeout_ms=5000,
                     security_protocol="PLAINTEXT", sasl_username=None, sasl_password=None):
    """Consumes up to max_messages JSON messages from a Kafka topic and
    returns them as a DataFrame. Uses kafka-python (pure Python, no C
    extension) rather than confluent-kafka to avoid the same class of
    native-library instability that caused issues with grpc on Streamlit
    Cloud. For Confluent Cloud or any SASL-secured broker, pass
    security_protocol='SASL_SSL' plus sasl_username/sasl_password (API
    key/secret)."""
    import json
    from kafka import KafkaConsumer

    consumer_kwargs = {
        "bootstrap_servers": bootstrap_servers,
        "group_id": group_id,
        "auto_offset_reset": "earliest",
        "enable_auto_commit": False,
        "consumer_timeout_ms": timeout_ms,
        "value_deserializer": lambda v: json.loads(v.decode("utf-8")),
        "security_protocol": security_protocol,
    }
    if security_protocol.startswith("SASL"):
        consumer_kwargs.update({
            "sasl_mechanism": "PLAIN",
            "sasl_plain_username": sasl_username,
            "sasl_plain_password": sasl_password,
        })

    consumer = KafkaConsumer(topic, **consumer_kwargs)
    rows = []
    try:
        for message in consumer:
            rows.append(message.value)
            if len(rows) >= max_messages:
                break
    finally:
        consumer.close()

    if not rows:
        raise ValueError(
            f"No messages read from topic '{topic}' within {timeout_ms}ms. "
            "Check the topic has messages and the broker is reachable from "
            "wherever this app is running."
        )
    return pd.DataFrame(rows)


def read_from_sql(connection_string, query):
    """Reads from any SQL database SQLAlchemy supports via a dialect-prefixed
    connection string - the same function covers PostgreSQL, MySQL, Cloud
    SQL, AlloyDB, SQL Server, and more. Examples:
      postgresql+psycopg2://user:password@host:5432/dbname
      mysql+pymysql://user:password@host:3306/dbname
    For Cloud SQL without a public IP, run the Cloud SQL Auth Proxy locally
    and point the connection string at 127.0.0.1 with the proxy's port."""
    import sqlalchemy
    engine = sqlalchemy.create_engine(connection_string)
    with engine.connect() as conn:
        return pd.read_sql(query, conn)


def read_from_rest_api(url, method="GET", headers_json="", body_json="", json_path=""):
    """Calls any JSON REST API and returns the result as a DataFrame.
    headers_json/body_json are raw JSON strings (e.g. for an Authorization
    bearer token). json_path drills into a nested response, dot-separated
    (e.g. 'data.records') if the array of rows isn't at the top level."""
    import json as json_lib
    import requests
    headers = json_lib.loads(headers_json) if headers_json else {}
    body = json_lib.loads(body_json) if body_json else None
    response = requests.request(method, url, headers=headers, json=body, timeout=30)
    response.raise_for_status()
    data = response.json()
    if json_path:
        for key in json_path.split("."):
            data = data[key]
    return pd.json_normalize(data)


def read_from_google_sheets(sheet_url_or_id, worksheet_name="Sheet1", service_account_json=""):
    """Reads a worksheet from Google Sheets. Pass a service account's JSON
    key as a string (paste the file contents), or leave blank to use
    gspread's default credential discovery (e.g. GOOGLE_APPLICATION_CREDENTIALS)."""
    import json as json_lib
    import gspread
    if service_account_json:
        creds_dict = json_lib.loads(service_account_json)
        gc = gspread.service_account_from_dict(creds_dict)
    else:
        gc = gspread.service_account()
    sheet = gc.open_by_url(sheet_url_or_id) if sheet_url_or_id.startswith("http") else gc.open_by_key(sheet_url_or_id)
    worksheet = sheet.worksheet(worksheet_name)
    return pd.DataFrame(worksheet.get_all_records())


# ---------------------------------------------------------------------------
# Connectivity tests - "can I reach this" checks, cheap and fast, separate
# from actually fetching data. Each returns (True, message) on success and
# raises on failure, so the caller's try/except handles both missing
# packages (ImportError) and real connection failures the same way as the
# fetch functions.
# ---------------------------------------------------------------------------

def test_bigquery(project_id):
    from google.cloud import bigquery
    client = bigquery.Client(project=project_id)
    list(client.query("SELECT 1").result())
    return True, f"Connected to BigQuery project '{project_id}'."


def test_sql(connection_string):
    import sqlalchemy
    engine = sqlalchemy.create_engine(connection_string)
    with engine.connect() as conn:
        conn.execute(sqlalchemy.text("SELECT 1"))
    dialect = connection_string.split("://")[0] if "://" in connection_string else "database"
    return True, f"Connected via {dialect}."


def test_kafka_broker(bootstrap_servers, timeout=5):
    """Checks TCP reachability of the first broker in the list - fast,
    doesn't require the topic to have messages (unlike a real consume)."""
    import socket
    first = bootstrap_servers.split(",")[0].strip()
    host, port = first.split(":")
    with socket.create_connection((host, int(port)), timeout=timeout):
        pass
    return True, f"Reached broker {first}."


def test_rest_api(url, headers_json=""):
    import json as json_lib
    import requests
    headers = json_lib.loads(headers_json) if headers_json else {}
    response = requests.get(url, headers=headers, timeout=10)
    response.raise_for_status()
    return True, f"Got HTTP {response.status_code} from {url}."


def test_google_sheets(sheet_url_or_id, service_account_json=""):
    import json as json_lib
    import gspread
    if service_account_json:
        gc = gspread.service_account_from_dict(json_lib.loads(service_account_json))
    else:
        gc = gspread.service_account()
    sheet = gc.open_by_url(sheet_url_or_id) if sheet_url_or_id.startswith("http") else gc.open_by_key(sheet_url_or_id)
    return True, f"Opened sheet '{sheet.title}'."


def test_csv_path(path_or_uri):
    import os
    if path_or_uri.startswith("gs://"):
        from google.cloud import storage
        bucket_name, blob_path = path_or_uri[5:].split("/", 1)
        client = storage.Client()
        blob = client.bucket(bucket_name).blob(blob_path)
        if not blob.exists():
            raise FileNotFoundError(f"{path_or_uri} does not exist or isn't accessible.")
        return True, f"Found {path_or_uri} in Cloud Storage."
    if not os.path.exists(path_or_uri):
        raise FileNotFoundError(f"{path_or_uri} does not exist on this server.")
    return True, f"Found {path_or_uri} on disk."


def real_publish_to_pubsub(project_id, topic_id, df, sample_n=20):
    from google.cloud import pubsub_v1
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    message_ids = []
    for _, row in df.head(sample_n).iterrows():
        payload = json.dumps(row.astype(str).to_dict()).encode("utf-8")
        future = publisher.publish(topic_path, payload)
        message_ids.append(future.result(timeout=30))
    logger.info("Published %d messages to Pub/Sub topic %s", len(message_ids), topic_id)
    return message_ids


def real_pull_from_pubsub(project_id, subscription_id, max_messages=20, timeout=10):
    from google.cloud import pubsub_v1
    subscriber = pubsub_v1.SubscriberClient()
    sub_path = subscriber.subscription_path(project_id, subscription_id)
    response = subscriber.pull(request={"subscription": sub_path, "max_messages": max_messages}, timeout=timeout)
    rows, ack_ids = [], []
    for msg in response.received_messages:
        rows.append(json.loads(msg.message.data.decode("utf-8")))
        ack_ids.append(msg.ack_id)
    if ack_ids:
        subscriber.acknowledge(request={"subscription": sub_path, "ack_ids": ack_ids})
    logger.info("Pulled and acknowledged %d messages from subscription %s", len(rows), subscription_id)
    return pd.DataFrame(rows)


def real_write_to_gcs(project_id, bucket_name, blob_path, df):
    from google.cloud import storage
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_string(df.to_csv(index=False), content_type="text/csv")
    return f"gs://{bucket_name}/{blob_path}"


def real_load_to_bigquery(project_id, dataset_id, table_id, df):
    from google.cloud import bigquery
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    return table_ref, job.output_rows


def real_read_from_bigquery(project_id, dataset_id, table_id):
    from google.cloud import bigquery
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    return client.query(f"SELECT * FROM `{table_ref}`").to_dataframe()


# ---------------------------------------------------------------------------
# Pipeline stages (pure pandas logic + optional real GCP calls)
# ---------------------------------------------------------------------------

def run_bronze(raw_df, gcp_cfg=None):
    df = raw_df.copy()
    df["_ingested_at"] = pd.Timestamp.now()
    note = "Raw data landed as-is, ingestion timestamp added."
    gcs_uri = None
    if gcp_cfg and gcp_cfg.get("enabled"):
        ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        gcs_uri = real_write_to_gcs(gcp_cfg["project_id"], gcp_cfg["bucket"], f"bronze/raw_{ts}.csv", df)
        note += f" Written to real GCS: {gcs_uri}"
    entry = _lineage_entry("Bronze (raw landing)", len(raw_df), len(df), note)
    return df, gcs_uri, entry


def run_silver(bronze_df, gcp_cfg=None):
    df = bronze_df.copy()
    before = len(df)
    df = df.drop_duplicates(subset=["order_id"])
    dupes_removed = before - len(df)

    nulls_before = int(df["unit_price"].isna().sum())
    df["unit_price"] = df["unit_price"].fillna(df["unit_price"].median())

    bad_qty = int((df["quantity"] <= 0).sum())
    df = df[df["quantity"] > 0]

    df["order_date"] = pd.to_datetime(df["order_date"])
    df["revenue"] = df["quantity"] * df["unit_price"]

    stats = {
        "duplicates_removed": int(dupes_removed),
        "nulls_filled": nulls_before,
        "invalid_rows_dropped": bad_qty,
        "rows_after_cleaning": len(df),
    }
    note = f"Removed {dupes_removed} duplicates, filled {nulls_before} nulls, dropped {bad_qty} invalid rows."
    gcs_uri = None
    if gcp_cfg and gcp_cfg.get("enabled"):
        ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        gcs_uri = real_write_to_gcs(gcp_cfg["project_id"], gcp_cfg["bucket"], f"silver/cleaned_{ts}.csv", df)
        note += f" Written to real GCS: {gcs_uri}"
    entry = _lineage_entry("Silver (cleaned)", before, len(df), note)
    return df, stats, gcs_uri, entry


def run_gold(silver_df, gcp_cfg=None):
    df = silver_df.copy()
    by_region = df.groupby("region", as_index=False).agg(
        total_revenue=("revenue", "sum"),
        orders=("order_id", "count"),
        avg_order_value=("revenue", "mean"),
    ).sort_values("total_revenue", ascending=False)

    by_product = df.groupby("product", as_index=False).agg(
        total_revenue=("revenue", "sum"),
        units_sold=("quantity", "sum"),
    ).sort_values("total_revenue", ascending=False)

    df["month"] = df["order_date"].dt.to_period("M").astype(str)
    monthly = df.groupby("month", as_index=False).agg(total_revenue=("revenue", "sum")).sort_values("month")

    kpis = {
        "total_revenue": df["revenue"].sum(),
        "total_orders": df["order_id"].nunique(),
        "avg_order_value": df["revenue"].mean(),
        "top_region": by_region.iloc[0]["region"] if len(by_region) else "-",
    }
    note = f"Built {len(by_region)} region and {len(by_product)} product rollups."
    bq_ref = None
    if gcp_cfg and gcp_cfg.get("enabled"):
        bq_ref, rows_loaded = real_load_to_bigquery(gcp_cfg["project_id"], gcp_cfg["dataset"], "gold_region_rollup", by_region)
        note += f" Loaded {rows_loaded} rows into real BigQuery table {bq_ref}."
        by_region = real_read_from_bigquery(gcp_cfg["project_id"], gcp_cfg["dataset"], "gold_region_rollup")
        by_region = by_region.sort_values("total_revenue", ascending=False)
    entry = _lineage_entry("Gold (aggregated)", len(df), len(by_region) + len(by_product), note)
    return by_region, by_product, monthly, kpis, bq_ref, entry


def run_ml(silver_df, monthly_df):
    df = silver_df.copy()
    mean, std = df["revenue"].mean(), df["revenue"].std()
    df["revenue_zscore"] = (df["revenue"] - mean) / std if std > 0 else 0
    anomalies = df[df["revenue_zscore"].abs() > 3][
        ["order_id", "region", "product", "quantity", "unit_price", "revenue", "revenue_zscore"]
    ].sort_values("revenue_zscore", ascending=False)

    forecast = pd.DataFrame()
    if len(monthly_df) >= 3:
        m = monthly_df.copy().reset_index(drop=True)
        m["t"] = np.arange(len(m))
        # Simple linear fit via numpy (avoids a scikit-learn dependency)
        slope, intercept = np.polyfit(m["t"], m["total_revenue"], deg=1)
        future_t = np.arange(len(m), len(m) + 2)
        future_preds = slope * future_t + intercept
        future_months = pd.period_range(
            pd.Period(m["month"].iloc[-1]) + 1, periods=2, freq="M"
        ).astype(str)
        forecast = pd.DataFrame({"month": future_months, "predicted_revenue": future_preds})

    entry = _lineage_entry(
        "ML platform (anomaly detection + forecast)", len(df), len(anomalies),
        f"Flagged {len(anomalies)} revenue outliers (|z| > 3); forecasted {len(forecast)} future months.",
    )
    return anomalies, forecast, entry
