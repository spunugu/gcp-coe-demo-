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
# Pipeline stages
# ---------------------------------------------------------------------------

def run_bronze(raw_df):
    df = raw_df.copy()
    df["_ingested_at"] = pd.Timestamp.now()
    log_stage("Bronze (raw landing)", len(raw_df), len(df), "Raw data landed as-is, ingestion timestamp added.")
    return df


def run_silver(bronze_df):
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
    log_stage(
        "Silver (cleaned)", before, len(df),
        f"Removed {dupes_removed} duplicates, filled {nulls_before} nulls, dropped {bad_qty} invalid rows.",
    )
    return df, stats


def run_gold(silver_df):
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
    log_stage("Gold (aggregated)", len(df), len(by_region) + len(by_product), f"Built {len(by_region)} region and {len(by_product)} product rollups.")
    return by_region, by_product, monthly, kpis


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

        with st.status("Stage 1/5: Ingestion", expanded=True) as status:
            st.write("Simulating Pub/Sub → Dataflow ingestion into Cloud Storage.")
            time.sleep(delay)
            bronze_df = run_bronze(raw_df)
            st.write(f"Ingested **{len(bronze_df)}** rows into the bronze zone.")
            status.update(label="Stage 1/5: Ingestion — done", state="complete")
        progress.progress(20, text="Bronze zone landed")

        with st.status("Stage 2/5: Bronze → Silver (cleaning)", expanded=True) as status:
            st.write("Deduplicating, filling nulls, dropping invalid rows, casting types.")
            time.sleep(delay)
            silver_df, silver_stats = run_silver(bronze_df)
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
            by_region, by_product, monthly, kpis = run_gold(silver_df)
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
        st.caption("Every stage logs its row counts and what it did, for governance and traceability.")
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
