"""
GCP Data & AI CoE - Architecture Explorer & Live Demo
------------------------------------------------------
A starter-kit / demo asset for the Incedo Data Technology CoE (GCP track).

Run locally:
    pip install -r requirements.txt
    streamlit run app.py

Optional live BigQuery demo:
    Set up Application Default Credentials pointing at a GCP project with
    BigQuery access, e.g.:
        gcloud auth application-default login
        export GOOGLE_CLOUD_PROJECT=your-project-id
    Without credentials, the app automatically falls back to sample data so
    it still runs end to end for a demo.
"""

import os
import numpy as np
import pandas as pd
import streamlit as st
import plotly.express as px

st.set_page_config(
    page_title="GCP Data & AI CoE",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Static content mirroring the CoE charter / architecture deck
# ---------------------------------------------------------------------------

LAYERS = [
    {
        "name": "1. Data sources",
        "services": ["Cloud SQL / AlloyDB (OLTP)", "SaaS & on-prem apps", "IoT / Pub/Sub streams", "Cloud Storage (files)", "Third-party / partner APIs"],
        "purpose": "Capture all enterprise signals, structured and unstructured, batch and streaming.",
    },
    {
        "name": "2. Ingestion",
        "services": ["Pub/Sub (streaming ingestion)", "Datastream (CDC)", "Cloud Data Fusion (batch ETL)", "Dataflow (unified batch + stream)"],
        "purpose": "Collect and land data reliably, whether it arrives continuously or on a schedule.",
    },
    {
        "name": "3. Lakehouse storage",
        "services": ["Cloud Storage (bronze/silver/gold)", "BigLake (open table format)", "Iceberg / Delta / Hudi", "BigQuery native storage"],
        "purpose": "Medallion architecture: raw, cleaned, and curated zones on open, queryable storage.",
    },
    {
        "name": "4. Processing",
        "services": ["Dataflow (batch/stream compute)", "Dataproc (Spark/Hadoop)", "BigQuery SQL / BigQuery ML", "Cloud Composer (orchestration)"],
        "purpose": "Transform, join, and aggregate data into analytics- and ML-ready tables.",
    },
    {
        "name": "5. ML platform",
        "services": ["Vertex AI Training / AutoML", "Vertex AI Feature Store", "Vertex AI Model Registry & Endpoints", "Vertex AI Pipelines & Model Monitoring"],
        "purpose": "Build, train, deploy, and monitor ML/AI models at scale, including LLM/RAG workloads.",
    },
    {
        "name": "6. Analytics & BI",
        "services": ["BigQuery (warehouse)", "Looker / Looker Studio", "Connected Sheets", "BigQuery BI Engine"],
        "purpose": "Explore, visualize, and operationalize metrics and KPIs for business consumption.",
    },
    {
        "name": "7. Application layer",
        "services": ["Cloud Run (containerized apps)", "API Gateway", "Streamlit / web apps", "Cloud Functions"],
        "purpose": "Deliver insights, APIs, and AI capabilities to end users and downstream systems.",
    },
]

CROSS_CUTTING = [
    ("Security", "Cloud KMS, Secret Manager, encryption at rest & in transit"),
    ("Identity & access", "Cloud IAM, org policies, fine-grained access control"),
    ("Compliance & governance", "Dataplex, Data Catalog, audit logs, data lineage"),
    ("FinOps", "Billing budgets, cost anomaly detection, BigQuery slot reservations"),
    ("Infra as code", "Terraform, Config Connector, policy as code"),
    ("Observability", "Cloud Monitoring, Cloud Logging, SLOs/SLIs"),
]

JULY_DELIVERABLES = pd.DataFrame([
    {"Deliverable": "Finalize charter and governance", "Outcome": "Operating model established", "Status": "Done"},
    {"Deliverable": "Publish initial technology archetypes", "Outcome": "Standard solution patterns", "Status": "Done"},
    {"Deliverable": "Catalogue reusable IP", "Outcome": "Shared technology assets", "Status": "In progress"},
    {"Deliverable": "Launch certification roadmap", "Outcome": "Capability development begins", "Status": "Done"},
    {"Deliverable": "Establish Architecture Review Board", "Outcome": "Technical governance in place", "Status": "Done"},
    {"Deliverable": "Support strategic pursuits", "Outcome": "Immediate business impact", "Status": "In progress"},
])

ASSET_CATALOG = pd.DataFrame([
    {"Asset": "GCP end-to-end reference architecture", "Type": "Architecture diagram", "Layer": "All", "Status": "Published"},
    {"Asset": "BigQuery lakehouse starter kit", "Type": "Terraform template", "Layer": "Storage", "Status": "Planned (Aug)"},
    {"Asset": "Dataflow streaming pipeline template", "Type": "Code accelerator", "Layer": "Ingestion", "Status": "Planned (Aug)"},
    {"Asset": "Vertex AI RAG starter", "Type": "Code accelerator", "Layer": "ML platform", "Status": "Planned (Sep)"},
    {"Asset": "Streamlit CoE demo app", "Type": "Demo asset", "Layer": "Application", "Status": "This app"},
])

# ---------------------------------------------------------------------------
# Sidebar navigation
# ---------------------------------------------------------------------------

st.sidebar.title("GCP Data & AI CoE")
page = st.sidebar.radio(
    "Navigate",
    ["Overview", "Architecture layers", "Live demo", "Reusable asset catalog"],
)
st.sidebar.markdown("---")
st.sidebar.caption("Incedo Data Technology CoE \u2014 GCP track")

# ---------------------------------------------------------------------------
# Overview page
# ---------------------------------------------------------------------------

if page == "Overview":
    st.title("GCP Data & AI Center of Excellence")
    st.markdown(
        "Establishing GCP as one of Incedo's Data Technology CoEs: reference "
        "architectures, reusable IP, and hands-on demos for modern data and "
        "AI platforms."
    )

    col1, col2, col3 = st.columns(3)
    col1.metric("Architecture layers", len(LAYERS))
    col2.metric("Cross-cutting capabilities", len(CROSS_CUTTING))
    col3.metric("July deliverables done", f"{(JULY_DELIVERABLES['Status'] == 'Done').sum()}/{len(JULY_DELIVERABLES)}")

    st.subheader("July deliverables")
    st.dataframe(JULY_DELIVERABLES, use_container_width=True, hide_index=True)

    st.subheader("Cross-cutting capabilities")
    cc_cols = st.columns(3)
    for i, (name, desc) in enumerate(CROSS_CUTTING):
        with cc_cols[i % 3]:
            st.markdown(f"**{name}**")
            st.caption(desc)

# ---------------------------------------------------------------------------
# Architecture layers page
# ---------------------------------------------------------------------------

elif page == "Architecture layers":
    st.title("GCP end-to-end architecture")
    st.markdown("Click into each layer to see the GCP services mapped to it.")

    for layer in LAYERS:
        with st.expander(layer["name"], expanded=False):
            st.write(layer["purpose"])
            st.markdown("**GCP services:**")
            for svc in layer["services"]:
                st.markdown(f"- {svc}")

# ---------------------------------------------------------------------------
# Live demo page
# ---------------------------------------------------------------------------

elif page == "Live demo":
    st.title("Live demo: ingestion \u2192 storage \u2192 analytics")
    st.markdown(
        "This panel queries a public BigQuery dataset to show the "
        "storage-to-analytics slice of the architecture working end to end. "
        "If no GCP credentials are configured in this environment, it falls "
        "back to representative sample data so the demo still runs."
    )

    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT", "")
    project_input = st.text_input(
        "GCP project ID (for BigQuery billing)", value=project_id,
        help="Required to run the live BigQuery query. Leave blank to use sample data.",
    )

    query = """
        SELECT
          station_id,
          COUNT(*) AS trip_count
        FROM `bigquery-public-data.austin_bikeshare.bikeshare_trips`
        WHERE start_time BETWEEN '2019-01-01' AND '2019-01-31'
        GROUP BY station_id
        ORDER BY trip_count DESC
        LIMIT 10
    """
    st.code(query.strip(), language="sql")

    use_live = st.button("Run live BigQuery query")

    df = None
    if use_live:
        if not project_input:
            st.warning("Enter a GCP project ID to run a live query, or view the sample data below.")
        else:
            try:
                from google.cloud import bigquery
                client = bigquery.Client(project=project_input)
                df = client.query(query).to_dataframe()
                st.success("Live results from BigQuery.")
            except Exception as e:
                st.error(f"Could not reach BigQuery ({e}). Showing sample data instead.")

    if df is None:
        rng = np.random.default_rng(42)
        df = pd.DataFrame({
            "station_id": [f"STN-{i:03d}" for i in range(1, 11)],
            "trip_count": sorted(rng.integers(200, 2000, size=10), reverse=True),
        })
        st.caption("Sample data shown (no live BigQuery connection).")

    fig = px.bar(df, x="station_id", y="trip_count", title="Top stations by trip count")
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(df, use_container_width=True, hide_index=True)

# ---------------------------------------------------------------------------
# Reusable asset catalog page
# ---------------------------------------------------------------------------

elif page == "Reusable asset catalog":
    st.title("Reusable asset catalog")
    st.markdown("Tracks accelerators, templates, and demo assets produced by the CoE.")
    st.dataframe(ASSET_CATALOG, use_container_width=True, hide_index=True)

    st.subheader("Add a new asset")
    with st.form("add_asset"):
        name = st.text_input("Asset name")
        atype = st.selectbox("Type", ["Architecture diagram", "Terraform template", "Code accelerator", "Demo asset", "Documentation"])
        layer = st.selectbox("Layer", [l["name"] for l in LAYERS] + ["All"])
        status = st.selectbox("Status", ["Planned", "In progress", "Published"])
        submitted = st.form_submit_button("Add to catalog (session only)")
        if submitted and name:
            st.session_state.setdefault("extra_assets", []).append(
                {"Asset": name, "Type": atype, "Layer": layer, "Status": status}
            )
            st.success(f"Added '{name}' for this session.")

    if st.session_state.get("extra_assets"):
        st.dataframe(pd.DataFrame(st.session_state["extra_assets"]), use_container_width=True, hide_index=True)
