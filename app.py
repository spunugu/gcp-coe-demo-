"""
GCP Data & AI CoE - Live Pipeline Demo
----------------------------------------
A usable, demoable prototype for the Incedo Data Technology CoE (GCP track).
Runs sample or uploaded data (or optionally real BigQuery data) through a
full ingestion -> bronze -> silver -> gold -> ML -> analytics pipeline,
with a data quality / lineage audit trail at every stage.

Run locally:
    pip install -r requirements.txt
    streamlit run app.py
"""

import io
import time
import numpy as np
import pandas as pd
import streamlit as st
import plotly.express as px

st.set_page_config(
    page_title="GCP Data & AI CoE - Live Demo",
    layout="wide",
    initial_sidebar_state="expanded",
)

REGIONS = ["North America", "EMEA", "APAC", "LATAM"]
PRODUCTS = ["Data Platform", "Analytics Suite", "ML Toolkit", "Streaming Connector", "BI Dashboard"]

# ---------------------------------------------------------------------------
# Lineage / audit trail helper
# ---------------------------------------------------------------------------

def log_stage(name, rows_before, rows_after, notes):
    st.session_state.setdefault("lineage", [])
    st.session_state["lineage"].append({
        "stage": name,
        "rows_before": rows_before,
        "rows_after": rows_after,
        "notes": notes,
        "timestamp": pd.Timestamp.now().strftime("%H:%M:%S"),
    })


# ---------------------------------------------------------------------------
# Sample data generator (stands in for a real source system)
# ---------------------------------------------------------------------------

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
    # Inject a handful of genuine revenue outliers for the anomaly stage to catch
    spike_idx = rng.choice(df.index, size=6, replace=False)
    df.loc[spike_idx, "quantity"] = rng.integers(150, 300, size=6)
    return df.sample(frac=1, random_state=seed).reset_index(drop=True)


def fetch_bigquery_data(project_id, query):
    from google.cloud import bigquery
    client = bigquery.Client(project=project_id)
    return client.query(query).to_dataframe()


# ---------------------------------------------------------------------------
# Real GCP infrastructure calls (Pub/Sub, Cloud Storage, BigQuery)
# Used only when "Real GCP mode" is enabled and credentials are configured.
# ---------------------------------------------------------------------------

def real_publish_to_pubsub(project_id, topic_id, df, sample_n=20):
    import json
    from google.cloud import pubsub_v1
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    message_ids = []
    sample = df.head(sample_n)
    for _, row in sample.iterrows():
        payload = json.dumps(row.astype(str).to_dict()).encode("utf-8")
        future = publisher.publish(topic_path, payload)
        message_ids.append(future.result(timeout=30))
    return message_ids


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
# Pipeline stages
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
    log_stage("Bronze (raw landing)", len(raw_df), len(df), note)
    return df, gcs_uri


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
    log_stage("Silver (cleaned)", before, len(df), note)
    return df, stats, gcs_uri


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
    log_stage("Gold (aggregated)", len(df), len(by_region) + len(by_product), note)
    return by_region, by_product, monthly, kpis, bq_ref


def run_ml(silver_df, monthly_df):
    df = silver_df.copy()

    # Anomaly detection on order revenue using z-score (simple, dependency-free)
    mean, std = df["revenue"].mean(), df["revenue"].std()
    df["revenue_zscore"] = (df["revenue"] - mean) / std if std > 0 else 0
    anomalies = df[df["revenue_zscore"].abs() > 3][
        ["order_id", "region", "product", "quantity", "unit_price", "revenue", "revenue_zscore"]
    ].sort_values("revenue_zscore", ascending=False)

    # Simple forecast: linear regression on monthly revenue trend
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

    log_stage(
        "ML platform (anomaly detection + forecast)", len(df), len(anomalies),
        f"Flagged {len(anomalies)} revenue outliers (|z| > 3); forecasted {len(forecast)} future months.",
    )
    return anomalies, forecast


# ---------------------------------------------------------------------------
# Sidebar navigation
# ---------------------------------------------------------------------------

st.sidebar.title("GCP Data & AI CoE")
page = st.sidebar.radio(
    "Navigate",
    ["Live pipeline demo", "Architecture overview", "Reusable asset catalog"],
)
st.sidebar.markdown("---")
st.sidebar.caption("Incedo Data Technology CoE — GCP track")

# ---------------------------------------------------------------------------
# Live pipeline demo page
# ---------------------------------------------------------------------------

if page == "Live pipeline demo":
    st.title("Live pipeline demo")
    st.markdown(
        "Runs data through a real ingestion → bronze → silver → gold → ML → "
        "analytics pipeline, mirroring the GCP reference architecture."
    )

    with st.expander("Data source", expanded=True):
        source_mode = st.radio(
            "Choose data source",
            ["Sample data (instant, no setup)", "Upload CSV", "Live BigQuery query (requires GCP credentials)"],
            horizontal=True,
        )

        raw_df = None
        if source_mode == "Upload CSV":
            uploaded = st.file_uploader("Upload CSV", type=["csv"])
            if uploaded is not None:
                raw_df = pd.read_csv(uploaded)
                st.caption(f"Loaded {len(raw_df)} rows from {uploaded.name}")

        elif source_mode == "Live BigQuery query (requires GCP credentials)":
            project_id = st.text_input("GCP project ID")
            query = st.text_area(
                "SQL query",
                value=(
                    "SELECT station_id, COUNT(*) AS trip_count\n"
                    "FROM `bigquery-public-data.austin_bikeshare.bikeshare_trips`\n"
                    "WHERE start_time BETWEEN '2019-01-01' AND '2019-01-31'\n"
                    "GROUP BY station_id"
                ),
                height=120,
            )
            st.caption(
                "Note: this sample query's schema differs from the sales pipeline below. "
                "Point it at a table with order_id/region/product/quantity/unit_price/order_date "
                "columns for the full pipeline to run against it."
            )
            if st.button("Fetch from BigQuery"):
                try:
                    raw_df = fetch_bigquery_data(project_id, query)
                    st.success(f"Fetched {len(raw_df)} rows from BigQuery.")
                except Exception as e:
                    st.error(f"Could not reach BigQuery ({e}). Falling back to sample data.")
                    raw_df = generate_sample_data()

        if raw_df is None:
            raw_df = generate_sample_data()
            if source_mode == "Sample data (instant, no setup)":
                st.caption(f"Using generated sample sales data ({len(raw_df)} rows, includes duplicates/nulls/outliers on purpose)")

    with st.expander("Real GCP infrastructure (optional)"):
        st.caption(
            "Off by default so the demo runs instantly. Turn this on to have the "
            "pipeline actually write to real Cloud Storage and BigQuery (and "
            "publish sample messages to Pub/Sub) instead of simulating those steps."
        )
        gcp_enabled = st.checkbox("Use real GCP infrastructure for this run", value=False)
        gcp_cfg = {"enabled": False}
        if gcp_enabled:
            gcol1, gcol2 = st.columns(2)
            with gcol1:
                gcp_project = st.text_input("GCP project ID", key="gcp_project")
                gcp_bucket = st.text_input("Cloud Storage bucket name", key="gcp_bucket")
            with gcol2:
                gcp_dataset = st.text_input("BigQuery dataset ID", key="gcp_dataset")
                gcp_topic = st.text_input("Pub/Sub topic ID (optional)", key="gcp_topic")
            gcp_cfg = {
                "enabled": True,
                "project_id": gcp_project,
                "bucket": gcp_bucket,
                "dataset": gcp_dataset,
                "topic": gcp_topic,
            }
            if not (gcp_project and gcp_bucket and gcp_dataset):
                st.warning("Enter project ID, bucket, and dataset to enable real GCP calls.")
                gcp_cfg["enabled"] = False

    instant = st.checkbox("Instant mode (skip animation)", value=False)

    with st.expander("Preview raw data"):
        st.dataframe(raw_df.head(20), use_container_width=True)

    run = st.button("Run pipeline", type="primary")

    if run:
        st.session_state["lineage"] = []
        required_cols = {"order_id", "region", "product", "quantity", "unit_price", "order_date"}
        if not required_cols.issubset(raw_df.columns):
            st.error(
                f"This dataset is missing columns the pipeline needs: "
                f"{sorted(required_cols - set(raw_df.columns))}. Falling back to sample data."
            )
            raw_df = generate_sample_data()

        delay = 0 if instant else 0.7
        progress = st.progress(0, text="Starting pipeline...")
        using_real = gcp_cfg.get("enabled", False)

        with st.status("Stage 1/5: Ingestion", expanded=True) as status:
            try:
                if using_real:
                    if gcp_cfg.get("topic"):
                        st.write(f"Publishing sample messages to real Pub/Sub topic `{gcp_cfg['topic']}`...")
                        msg_ids = real_publish_to_pubsub(gcp_cfg["project_id"], gcp_cfg["topic"], raw_df)
                        st.write(f"Published **{len(msg_ids)}** real Pub/Sub messages. Sample message ID: `{msg_ids[0]}`")
                    else:
                        st.write("No Pub/Sub topic given — writing straight to real Cloud Storage.")
                    time.sleep(delay)
                    bronze_df, gcs_uri = run_bronze(raw_df, gcp_cfg)
                    st.success(f"Real write confirmed: {gcs_uri}")
                else:
                    st.write("Simulating Pub/Sub → Dataflow ingestion into Cloud Storage.")
                    time.sleep(delay)
                    bronze_df, _ = run_bronze(raw_df, None)
                st.write(f"Ingested **{len(bronze_df)}** rows into the bronze zone.")
                status.update(label="Stage 1/5: Ingestion — done", state="complete")
            except Exception as e:
                st.error(f"Real GCP call failed ({e}). Falling back to simulated ingestion for this run.")
                using_real = False
                bronze_df, _ = run_bronze(raw_df, None)
                status.update(label="Stage 1/5: Ingestion — done (simulated fallback)", state="complete")
        progress.progress(20, text="Bronze zone landed")

        with st.status("Stage 2/5: Bronze → Silver (cleaning)", expanded=True) as status:
            st.write("Deduplicating, filling nulls, dropping invalid rows, casting types.")
            time.sleep(delay)
            try:
                silver_df, silver_stats, silver_gcs_uri = run_silver(bronze_df, gcp_cfg if using_real else None)
                if using_real and silver_gcs_uri:
                    st.success(f"Real write confirmed: {silver_gcs_uri}")
            except Exception as e:
                st.error(f"Real GCP call failed ({e}). Continuing with local computation only.")
                silver_df, silver_stats, _ = run_silver(bronze_df, None)
                using_real = False
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Duplicates removed", silver_stats["duplicates_removed"])
            c2.metric("Nulls filled", silver_stats["nulls_filled"])
            c3.metric("Invalid rows dropped", silver_stats["invalid_rows_dropped"])
            c4.metric("Clean rows", silver_stats["rows_after_cleaning"])
            status.update(label="Stage 2/5: Silver zone — done", state="complete")
        progress.progress(40, text="Silver zone cleaned")

        with st.status("Stage 3/5: Silver → Gold (aggregation)", expanded=True) as status:
            st.write("Aggregating into business-ready tables (BigQuery-style).")
            time.sleep(delay)
            try:
                by_region, by_product, monthly, kpis, bq_ref = run_gold(silver_df, gcp_cfg if using_real else None)
                if using_real and bq_ref:
                    st.success(f"Real round trip confirmed: wrote to and read back from `{bq_ref}`")
            except Exception as e:
                st.error(f"Real GCP call failed ({e}). Continuing with local computation only.")
                by_region, by_product, monthly, kpis, _ = run_gold(silver_df, None)
            st.write(f"Built **{len(by_region)}** region rollups and **{len(by_product)}** product rollups.")
            status.update(label="Stage 3/5: Gold zone — done", state="complete")
        progress.progress(60, text="Gold tables built")

        with st.status("Stage 4/5: ML platform (anomaly detection + forecast)", expanded=True) as status:
            st.write("Flagging revenue outliers and forecasting the next 2 months (Vertex AI-style).")
            time.sleep(delay)
            anomalies, forecast = run_ml(silver_df, monthly)
            st.write(f"Flagged **{len(anomalies)}** anomalous orders; forecasted **{len(forecast)}** future months.")
            status.update(label="Stage 4/5: ML platform — done", state="complete")
        progress.progress(80, text="ML stage complete")

        with st.status("Stage 5/5: Analytics & BI", expanded=True) as status:
            time.sleep(delay)
            status.update(label="Stage 5/5: Dashboard ready", state="complete")
        progress.progress(100, text="Pipeline complete")

        st.success("Pipeline complete — dashboard below is built from the gold and ML outputs.")
        st.divider()

        st.subheader("Business dashboard")
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Total revenue", f"${kpis['total_revenue']:,.0f}")
        k2.metric("Total orders", f"{kpis['total_orders']:,}")
        k3.metric("Avg order value", f"${kpis['avg_order_value']:,.0f}")
        k4.metric("Top region", kpis["top_region"])

        chart_col1, chart_col2 = st.columns(2)
        with chart_col1:
            fig1 = px.bar(by_region, x="region", y="total_revenue", title="Revenue by region")
            st.plotly_chart(fig1, use_container_width=True)
        with chart_col2:
            fig2 = px.pie(by_product, names="product", values="total_revenue", title="Revenue by product")
            st.plotly_chart(fig2, use_container_width=True)

        if len(forecast):
            trend_df = pd.concat([
                monthly.assign(kind="Actual").rename(columns={"total_revenue": "revenue"}),
                forecast.assign(kind="Forecast").rename(columns={"predicted_revenue": "revenue"}),
            ], ignore_index=True)
            fig3 = px.line(trend_df, x="month", y="revenue", color="kind", markers=True, title="Monthly revenue: actual + forecast")
        else:
            fig3 = px.line(monthly, x="month", y="total_revenue", markers=True, title="Monthly revenue trend")
        st.plotly_chart(fig3, use_container_width=True)

        st.subheader("ML platform outputs")
        m1, m2 = st.tabs(["Anomalies detected", "Revenue forecast"])
        with m1:
            if len(anomalies):
                st.dataframe(anomalies, use_container_width=True, hide_index=True)
            else:
                st.caption("No anomalies above the |z| > 3 threshold in this run.")
        with m2:
            if len(forecast):
                st.dataframe(forecast, use_container_width=True, hide_index=True)
            else:
                st.caption("Not enough monthly history to forecast (need 3+ months).")

        st.subheader("Gold tables")
        t1, t2 = st.tabs(["By region", "By product"])
        with t1:
            st.dataframe(by_region, use_container_width=True, hide_index=True)
        with t2:
            st.dataframe(by_product, use_container_width=True, hide_index=True)

        csv_buf = io.StringIO()
        by_region.to_csv(csv_buf, index=False)
        st.download_button("Download region rollup (CSV)", csv_buf.getvalue(), file_name="gold_region_rollup.csv", mime="text/csv")

        st.subheader("Data quality & lineage audit")
        mode_label = "Real GCP infrastructure (Cloud Storage + BigQuery)" if using_real else "Simulated (in-memory, no GCP infrastructure provisioned)"
        st.caption(f"Mode for this run: **{mode_label}**. Every stage logs its row counts and what it did.")
        st.dataframe(pd.DataFrame(st.session_state.get("lineage", [])), use_container_width=True, hide_index=True)

# ---------------------------------------------------------------------------
# Architecture overview page
# ---------------------------------------------------------------------------

elif page == "Architecture overview":
    st.title("GCP end-to-end architecture")
    st.markdown("The pipeline demo mirrors this reference architecture, stage for stage.")

    LAYERS = [
        {"name": "1. Data sources", "services": ["Cloud SQL / AlloyDB (OLTP)", "SaaS & on-prem apps", "IoT / Pub/Sub streams", "Cloud Storage (files)", "Third-party / partner APIs"], "purpose": "Capture all enterprise signals, structured and unstructured, batch and streaming."},
        {"name": "2. Ingestion", "services": ["Pub/Sub (streaming ingestion)", "Datastream (CDC)", "Cloud Data Fusion (batch ETL)", "Dataflow (unified batch + stream)"], "purpose": "Collect and land data reliably, whether it arrives continuously or on a schedule."},
        {"name": "3. Lakehouse storage", "services": ["Cloud Storage (bronze/silver/gold)", "BigLake (open table format)", "Iceberg / Delta / Hudi", "BigQuery native storage"], "purpose": "Medallion architecture: raw, cleaned, and curated zones on open, queryable storage."},
        {"name": "4. Processing", "services": ["Dataflow (batch/stream compute)", "Dataproc (Spark/Hadoop)", "BigQuery SQL / BigQuery ML", "Cloud Composer (orchestration)"], "purpose": "Transform, join, and aggregate data into analytics- and ML-ready tables."},
        {"name": "5. ML platform", "services": ["Vertex AI Training / AutoML", "Vertex AI Feature Store", "Vertex AI Model Registry & Endpoints", "Vertex AI Pipelines & Model Monitoring"], "purpose": "Build, train, deploy, and monitor ML/AI models at scale, including LLM/RAG workloads."},
        {"name": "6. Analytics & BI", "services": ["BigQuery (warehouse)", "Looker / Looker Studio", "Connected Sheets", "BigQuery BI Engine"], "purpose": "Explore, visualize, and operationalize metrics and KPIs for business consumption."},
        {"name": "7. Application layer", "services": ["Cloud Run (containerized apps)", "API Gateway", "Streamlit / web apps", "Cloud Functions"], "purpose": "Deliver insights, APIs, and AI capabilities to end users and downstream systems."},
    ]
    for layer in LAYERS:
        with st.expander(layer["name"]):
            st.write(layer["purpose"])
            for svc in layer["services"]:
                st.markdown(f"- {svc}")

    st.subheader("July deliverables")
    JULY_DELIVERABLES = pd.DataFrame([
        {"Deliverable": "Finalize charter and governance", "Outcome": "Operating model established", "Status": "Done"},
        {"Deliverable": "Publish initial technology archetypes", "Outcome": "Standard solution patterns", "Status": "Done"},
        {"Deliverable": "Catalogue reusable IP", "Outcome": "Shared technology assets", "Status": "In progress"},
        {"Deliverable": "Launch certification roadmap", "Outcome": "Capability development begins", "Status": "Done"},
        {"Deliverable": "Establish Architecture Review Board", "Outcome": "Technical governance in place", "Status": "Done"},
        {"Deliverable": "Support strategic pursuits", "Outcome": "Immediate business impact", "Status": "In progress"},
    ])
    st.dataframe(JULY_DELIVERABLES, use_container_width=True, hide_index=True)

# ---------------------------------------------------------------------------
# Reusable asset catalog page
# ---------------------------------------------------------------------------

elif page == "Reusable asset catalog":
    st.title("Reusable asset catalog")
    ASSET_CATALOG = pd.DataFrame([
        {"Asset": "GCP end-to-end reference architecture", "Type": "Architecture diagram", "Layer": "All", "Status": "Published"},
        {"Asset": "Live pipeline demo (this app)", "Type": "Demo asset", "Layer": "All", "Status": "Published"},
        {"Asset": "BigQuery lakehouse starter kit", "Type": "Terraform template", "Layer": "Storage", "Status": "Planned (Aug)"},
        {"Asset": "Dataflow streaming pipeline template", "Type": "Code accelerator", "Layer": "Ingestion", "Status": "Planned (Aug)"},
        {"Asset": "Vertex AI RAG starter", "Type": "Code accelerator", "Layer": "ML platform", "Status": "Planned (Sep)"},
    ])
    st.dataframe(ASSET_CATALOG, use_container_width=True, hide_index=True)

    st.subheader("Add a new asset")
    with st.form("add_asset"):
        name = st.text_input("Asset name")
        atype = st.selectbox("Type", ["Architecture diagram", "Terraform template", "Code accelerator", "Demo asset", "Documentation"])
        status = st.selectbox("Status", ["Planned", "In progress", "Published"])
        submitted = st.form_submit_button("Add to catalog (session only)")
        if submitted and name:
            st.session_state.setdefault("extra_assets", []).append({"Asset": name, "Type": atype, "Status": status})
            st.success(f"Added '{name}' for this session.")
    if st.session_state.get("extra_assets"):
        st.dataframe(pd.DataFrame(st.session_state["extra_assets"]), use_container_width=True, hide_index=True)
