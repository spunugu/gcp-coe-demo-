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
    df = pd.DataFrame({
        "order_id": order_ids,
        "order_date": rng.choice(dates, size=n),
        "region": rng.choice(REGIONS, size=n, p=[0.4, 0.3, 0.2, 0.1]),
        "product": rng.choice(PRODUCTS, size=n),
        "quantity": rng.integers(1, 20, size=n),
        "unit_price": rng.choice([99.0, 149.0, 249.0, 499.0, 999.0], size=n),
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
    return df.sample(frac=1, random_state=seed).reset_index(drop=True)


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
        from sklearn.linear_model import LinearRegression
        m = monthly_df.copy().reset_index(drop=True)
        m["t"] = np.arange(len(m))
        model = LinearRegression().fit(m[["t"]], m["total_revenue"])
        future_t = np.arange(len(m), len(m) + 2)
        future_preds = model.predict(future_t.reshape(-1, 1))
        future_months = pd.period_range(
            pd.Period(m["month"].iloc[-1]) + 1, periods=2, freq="M"
        ).astype(str)
        forecast = pd.DataFrame({"month": future_months, "predicted_revenue": future_preds})

    entry = _lineage_entry(
        "ML platform (anomaly detection + forecast)", len(df), len(anomalies),
        f"Flagged {len(anomalies)} revenue outliers (|z| > 3); forecasted {len(forecast)} future months.",
    )
    return anomalies, forecast, entry
