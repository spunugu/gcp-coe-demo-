"""
GCP Data & AI CoE - Live Pipeline Demo
----------------------------------------
A usable, demoable prototype for the Incedo Data Technology CoE (GCP track).
Runs a real (simulated) ingestion -> bronze -> silver -> gold -> analytics
pipeline against sample or uploaded data, so it can be demoed to a manager
with zero GCP setup required.

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

# ---------------------------------------------------------------------------
# Sample data generator (stands in for a real source system)
# ---------------------------------------------------------------------------

REGIONS = ["North America", "EMEA", "APAC", "LATAM"]
PRODUCTS = ["Data Platform", "Analytics Suite", "ML Toolkit", "Streaming Connector", "BI Dashboard"]


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
    return df.sample(frac=1, random_state=seed).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Pipeline stages (real pandas transformations, mirrors the medallion pattern)
# ---------------------------------------------------------------------------

def run_bronze(raw_df):
    df = raw_df.copy()
    df["_ingested_at"] = pd.Timestamp.now()
    return df, {"rows_ingested": len(df)}


def run_silver(bronze_df):
    df = bronze_df.copy()
    before = len(df)
    df = df.drop_duplicates(subset=["order_id"])
    dupes_removed = before - len(df)

    nulls_before = df["unit_price"].isna().sum()
    df["unit_price"] = df["unit_price"].fillna(df["unit_price"].median())

    bad_qty = (df["quantity"] <= 0).sum()
    df = df[df["quantity"] > 0]

    df["order_date"] = pd.to_datetime(df["order_date"])
    df["revenue"] = df["quantity"] * df["unit_price"]

    stats = {
        "duplicates_removed": int(dupes_removed),
        "nulls_filled": int(nulls_before),
        "invalid_rows_dropped": int(bad_qty),
        "rows_after_cleaning": len(df),
    }
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
    monthly = df.groupby("month", as_index=False).agg(total_revenue=("revenue", "sum"))

    kpis = {
        "total_revenue": df["revenue"].sum(),
        "total_orders": df["order_id"].nunique(),
        "avg_order_value": df["revenue"].mean(),
        "top_region": by_region.iloc[0]["region"] if len(by_region) else "-",
    }
    return by_region, by_product, monthly, kpis


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
# Live pipeline demo page (flagship page)
# ---------------------------------------------------------------------------

if page == "Live pipeline demo":
    st.title("Live pipeline demo")
    st.markdown(
        "Runs sample data through a real ingestion → bronze → silver → gold → "
        "analytics pipeline, mirroring the GCP reference architecture "
        "(Pub/Sub/Dataflow → Cloud Storage/BigLake → Dataflow/BigQuery → "
        "BigQuery/Looker)."
    )

    col_a, col_b = st.columns([2, 1])
    with col_a:
        uploaded = st.file_uploader("Upload your own CSV (optional)", type=["csv"])
    with col_b:
        instant = st.checkbox("Instant mode (skip animation)", value=False)

    if uploaded is not None:
        raw_df = pd.read_csv(uploaded)
        st.caption(f"Loaded {len(raw_df)} rows from {uploaded.name}")
    else:
        raw_df = generate_sample_data()
        st.caption(f"Using generated sample sales data ({len(raw_df)} rows, includes duplicates/nulls/bad values on purpose)")

    with st.expander("Preview raw data"):
        st.dataframe(raw_df.head(20), use_container_width=True)

    run = st.button("Run pipeline", type="primary")

    if run:
        delay = 0 if instant else 0.7
        progress = st.progress(0, text="Starting pipeline...")

        with st.status("Stage 1/4: Ingestion", expanded=True) as status:
            st.write("Simulating Pub/Sub → Dataflow ingestion into Cloud Storage.")
            time.sleep(delay)
            bronze_df, bronze_stats = run_bronze(raw_df)
            st.write(f"Ingested **{bronze_stats['rows_ingested']}** rows into the bronze zone.")
            status.update(label="Stage 1/4: Ingestion — done", state="complete")
        progress.progress(25, text="Bronze zone landed")

        with st.status("Stage 2/4: Bronze → Silver (cleaning)", expanded=True) as status:
            st.write("Deduplicating, filling nulls, dropping invalid rows, casting types.")
            time.sleep(delay)
            silver_df, silver_stats = run_silver(bronze_df)
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Duplicates removed", silver_stats["duplicates_removed"])
            c2.metric("Nulls filled", silver_stats["nulls_filled"])
            c3.metric("Invalid rows dropped", silver_stats["invalid_rows_dropped"])
            c4.metric("Clean rows", silver_stats["rows_after_cleaning"])
            status.update(label="Stage 2/4: Silver zone — done", state="complete")
        progress.progress(55, text="Silver zone cleaned")

        with st.status("Stage 3/4: Silver → Gold (aggregation)", expanded=True) as status:
            st.write("Aggregating into business-ready tables (BigQuery-style).")
            time.sleep(delay)
            by_region, by_product, monthly, kpis = run_gold(silver_df)
            st.write(f"Built **{len(by_region)}** region rollups and **{len(by_product)}** product rollups.")
            status.update(label="Stage 3/4: Gold zone — done", state="complete")
        progress.progress(80, text="Gold tables built")

        with st.status("Stage 4/4: Analytics & BI", expanded=True) as status:
            time.sleep(delay)
            status.update(label="Stage 4/4: Dashboard ready", state="complete")
        progress.progress(100, text="Pipeline complete")

        st.success("Pipeline complete — dashboard below is built from the gold tables.")
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

        fig3 = px.line(monthly.sort_values("month"), x="month", y="total_revenue", markers=True, title="Monthly revenue trend")
        st.plotly_chart(fig3, use_container_width=True)

        st.subheader("Gold tables")
        t1, t2 = st.tabs(["By region", "By product"])
        with t1:
            st.dataframe(by_region, use_container_width=True, hide_index=True)
        with t2:
            st.dataframe(by_product, use_container_width=True, hide_index=True)

        csv_buf = io.StringIO()
        by_region.to_csv(csv_buf, index=False)
        st.download_button("Download region rollup (CSV)", csv_buf.getvalue(), file_name="gold_region_rollup.csv", mime="text/csv")

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
