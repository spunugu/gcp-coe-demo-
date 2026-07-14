"""
GCP Data & AI CoE - Live Pipeline Demo
----------------------------------------
A usable, demoable prototype for the Incedo Data Technology CoE (GCP track).
Runs sample or uploaded data through a full ingestion -> bronze -> silver ->
gold -> ML -> analytics pipeline, with an optional "Real GCP infrastructure"
mode (Pub/Sub, Cloud Storage, BigQuery) and a data quality / lineage audit
trail at every stage. Pipeline logic lives in pipeline.py so it can be
unit tested independently (see tests/test_pipeline.py).

Each page is a function, and every page call is wrapped in a top-level
try/except so an unexpected error on one page shows a friendly recoverable
message instead of crashing the whole app or taking down other pages.

Run locally:
    pip install -r requirements.txt
    streamlit run app.py
"""

import io
import json
import logging
import os
import time

import pandas as pd
import streamlit as st
import plotly.express as px

import pipeline
import ai_helper
import help_bot
import animated_architecture
import connectors
import streamlit.components.v1 as components

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("gcp_coe_demo")
# Cloud Run automatically ships stdout/stderr to Cloud Logging, so these
# log lines become real, queryable logs once deployed there.

st.set_page_config(
    page_title="GCP Data & AI CoE - Live Demo",
    layout="wide",
    initial_sidebar_state="expanded",
)

CATALOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "asset_catalog.json")

# ---------------------------------------------------------------------------
# Optional access gate. Configure APP_PASSWORD in .streamlit/secrets.toml
# (local) or the platform's secrets manager (Streamlit Cloud / Cloud Run env
# var) to require a password. If unset, the app stays open.
# ---------------------------------------------------------------------------

def check_access():
    required_password = st.secrets.get("APP_PASSWORD") if hasattr(st, "secrets") else None
    if not required_password:
        return True
    if st.session_state.get("authed"):
        return True
    st.title("GCP Data & AI CoE")
    pw = st.text_input("Enter access password", type="password")
    if st.button("Enter"):
        if pw == required_password:
            st.session_state["authed"] = True
            logger.info("Access granted")
            st.rerun()
        else:
            st.error("Incorrect password.")
            logger.warning("Failed access attempt")
    return False


# ---------------------------------------------------------------------------
# Asset catalog persistence (local JSON file - survives page refresh and
# other users hitting the same running instance; not durable across Cloud
# Run cold starts/scaling. For true multi-instance durability, swap this
# for Firestore or a BigQuery table - see README "Extending" section.)
# ---------------------------------------------------------------------------

DEFAULT_CATALOG = [
    {"Asset": "GCP end-to-end reference architecture", "Type": "Architecture diagram", "Layer": "All", "Status": "Published"},
    {"Asset": "Live pipeline demo (this app)", "Type": "Demo asset", "Layer": "All", "Status": "Published"},
    {"Asset": "Terraform module (terraform/)", "Type": "Terraform template", "Layer": "Storage/Ingestion", "Status": "Published"},
    {"Asset": "Pipeline unit tests (tests/)", "Type": "Code accelerator", "Layer": "Processing", "Status": "Published"},
    {"Asset": "Vertex AI RAG starter", "Type": "Code accelerator", "Layer": "ML platform", "Status": "Planned (Sep)"},
]


def load_catalog():
    if os.path.exists(CATALOG_FILE):
        try:
            with open(CATALOG_FILE) as f:
                return json.load(f)
        except Exception:
            logger.exception("Failed to read catalog file, using defaults")
    return list(DEFAULT_CATALOG)


def save_catalog(catalog):
    try:
        with open(CATALOG_FILE, "w") as f:
            json.dump(catalog, f, indent=2)
    except Exception:
        logger.exception("Failed to persist catalog file")


# ---------------------------------------------------------------------------
# Live pipeline demo page
# ---------------------------------------------------------------------------

def render_live_pipeline_demo():
    st.title("Live pipeline demo")
    st.markdown(
        "Runs data through a real ingestion → bronze → silver → gold → ML → "
        "analytics pipeline, mirroring the GCP reference architecture."
    )

    with st.expander("Data source", expanded=True):
        connector_keys = list(connectors.CONNECTORS.keys())
        labels = [connectors.CONNECTORS[k]["label"] for k in connector_keys]
        selected_label = st.selectbox("Choose data source", labels)
        selected_key = connector_keys[labels.index(selected_label)]
        spec = connectors.CONNECTORS[selected_key]

        raw_df = None

        if selected_key == "sample":
            raw_df = pipeline.generate_sample_data()
            st.caption(f"Using generated sample sales data ({len(raw_df)} rows, includes duplicates/nulls/outliers on purpose)")

        elif selected_key == "csv_upload":
            uploaded = st.file_uploader("Upload CSV", type=["csv"])
            if uploaded is not None:
                try:
                    raw_df = pd.read_csv(uploaded)
                    st.caption(f"Loaded {len(raw_df)} rows from {uploaded.name}")
                except Exception as e:
                    st.error(f"Could not read that CSV ({e}). Falling back to sample data.")
                    raw_df = None

        else:
            # Generic path: any connector registered in connectors.py renders
            # itself from its field spec - adding a new source means adding
            # an entry there, not new code here.
            if spec.get("help"):
                st.caption(spec["help"])
            if spec.get("requires"):
                st.caption(f"Requires: `pip install {spec['requires']}`")

            params = {}
            fields = spec.get("fields", [])
            cols = st.columns(2) if len(fields) > 1 else [st.container()]
            for i, f in enumerate(fields):
                target = cols[i % len(cols)]
                key = f"conn_{selected_key}_{f['name']}"
                with target:
                    if f["type"] == "password":
                        use_secret = st.checkbox(
                            f"{f['label']}: load from Secret Manager",
                            key=f"{key}_use_secret",
                            help="Enterprise pattern: store this credential once in Secret Manager instead of retyping it each session.",
                        )
                        if use_secret:
                            secret_ref = st.text_input(
                                f"{f['label']} — Secret Manager resource name", key=f"{key}_secret_ref",
                                placeholder="projects/PROJECT_ID/secrets/SECRET_NAME/versions/latest",
                            )
                            try:
                                params[f["name"]] = pipeline.get_secret(secret_ref) if secret_ref else ""
                                if secret_ref:
                                    st.caption("Loaded from Secret Manager.")
                            except ImportError:
                                st.error("Run `pip install google-cloud-secret-manager` to use this.")
                                params[f["name"]] = ""
                            except Exception as e:
                                st.error(f"Could not load secret: {e}")
                                params[f["name"]] = ""
                        else:
                            params[f["name"]] = st.text_input(f["label"], type="password", key=key)
                    elif f["type"] == "number":
                        params[f["name"]] = st.number_input(f["label"], value=f.get("default", 0), key=key)
                    elif f["type"] == "textarea":
                        params[f["name"]] = st.text_area(f["label"], value=f.get("default", ""), key=key)
                    elif f["type"] == "select":
                        options = f["options"]
                        default_idx = options.index(f["default"]) if f.get("default") in options else 0
                        params[f["name"]] = st.selectbox(f["label"], options, index=default_idx, key=key)
                    else:
                        params[f["name"]] = st.text_input(
                            f["label"], value=f.get("default", ""), placeholder=f.get("placeholder", ""), key=key
                        )

            btn_col1, btn_col2 = st.columns([1, 1.4])
            with btn_col1:
                if spec.get("test") and st.button("Test connection", key=f"test_{selected_key}"):
                    try:
                        ok, msg = spec["test"](params)
                        st.success(f"Connected. {msg}")
                    except ImportError as e:
                        st.error(f"Missing package ({e}). Run `pip install {spec.get('requires', 'the required package')}`.")
                    except Exception as e:
                        st.error(f"Not reachable: {e}")
            with btn_col2:
                fetch_clicked = st.button(f"Connect and fetch — {spec['label']}", key=f"fetch_{selected_key}")

            if fetch_clicked:
                try:
                    raw_df = spec["fetch"](params)
                    st.success(f"Fetched {len(raw_df)} rows via {spec['label']}.")
                except ImportError as e:
                    st.error(f"Missing package ({e}). Run `pip install {spec.get('requires', 'the required package')}` and restart the app.")
                    raw_df = pipeline.generate_sample_data()
                except Exception as e:
                    st.error(f"Connection failed ({e}). Falling back to sample data.")
                    raw_df = pipeline.generate_sample_data()

        if raw_df is None:
            raw_df = pipeline.generate_sample_data()

    with st.expander("Real GCP infrastructure (optional)"):
        st.caption(
            "Off by default so the demo runs instantly. Turn this on to have the "
            "pipeline actually publish to Pub/Sub, write to Cloud Storage, and "
            "round-trip through BigQuery instead of simulating those steps. "
            "Provision resources first with terraform/ (see GCP_SETUP.md)."
        )
        gcp_enabled = st.checkbox("Use real GCP infrastructure for this run", value=False)
        gcp_cfg = {"enabled": False}
        if gcp_enabled:
            gcol1, gcol2 = st.columns(2)
            with gcol1:
                gcp_project = st.text_input("GCP project ID", key="gcp_project")
                gcp_bucket = st.text_input("Cloud Storage bucket name", key="gcp_bucket")
                gcp_topic = st.text_input("Pub/Sub topic ID (optional)", key="gcp_topic")
            with gcol2:
                gcp_dataset = st.text_input("BigQuery dataset ID", key="gcp_dataset")
                gcp_subscription = st.text_input("Pub/Sub subscription ID (optional, pulls messages back)", key="gcp_subscription")
            gcp_cfg = {
                "enabled": True,
                "project_id": gcp_project,
                "bucket": gcp_bucket,
                "dataset": gcp_dataset,
                "topic": gcp_topic,
                "subscription": gcp_subscription,
            }
            if not (gcp_project and gcp_bucket and gcp_dataset):
                st.warning("Enter project ID, bucket, and dataset to enable real GCP calls.")
                gcp_cfg["enabled"] = False

    instant = st.checkbox("Instant mode (skip animation)", value=False)

    with st.expander("Preview raw data"):
        st.dataframe(raw_df.head(20), width='stretch')

    run = st.button("Run pipeline", type="primary")

    if run:
        lineage = []
        missing = pipeline.validate_schema(raw_df)
        if missing:
            st.error(f"This dataset is missing columns the pipeline needs: {missing}. Falling back to sample data.")
            raw_df = pipeline.generate_sample_data()

        delay = 0 if instant else 0.7
        progress = st.progress(0, text="Starting pipeline...")
        using_real = gcp_cfg.get("enabled", False)

        with st.status("Stage 1/5: Ingestion", expanded=True) as status:
            try:
                if using_real:
                    if gcp_cfg.get("topic"):
                        st.write(f"Publishing sample messages to real Pub/Sub topic `{gcp_cfg['topic']}`...")
                        msg_ids = pipeline.real_publish_to_pubsub(gcp_cfg["project_id"], gcp_cfg["topic"], raw_df)
                        st.write(f"Published **{len(msg_ids)}** real Pub/Sub messages. Sample message ID: `{msg_ids[0]}`")
                        if gcp_cfg.get("subscription"):
                            st.write(f"Pulling messages back from subscription `{gcp_cfg['subscription']}` to close the ingest loop...")
                            time.sleep(1.5)
                            pulled_df = pipeline.real_pull_from_pubsub(gcp_cfg["project_id"], gcp_cfg["subscription"])
                            if len(pulled_df):
                                st.success(f"Pulled and acknowledged **{len(pulled_df)}** real messages back from Pub/Sub.")
                                raw_df = pulled_df
                            else:
                                st.warning("No messages available to pull yet (Pub/Sub delivery lag) — continuing with original data.")
                    else:
                        st.write("No Pub/Sub topic given — writing straight to real Cloud Storage.")
                    time.sleep(delay)
                    bronze_df, gcs_uri, entry = pipeline.run_bronze(raw_df, gcp_cfg)
                    st.success(f"Real write confirmed: {gcs_uri}")
                else:
                    st.write("Simulating Pub/Sub → Dataflow ingestion into Cloud Storage.")
                    time.sleep(delay)
                    bronze_df, _, entry = pipeline.run_bronze(raw_df, None)
                lineage.append(entry)
                st.write(f"Ingested **{len(bronze_df)}** rows into the bronze zone.")
                logger.info("Ingestion stage complete: %d rows, real_mode=%s", len(bronze_df), using_real)
                status.update(label="Stage 1/5: Ingestion — done", state="complete")
            except Exception as e:
                logger.exception("Ingestion stage failed, falling back to simulated")
                st.error(f"Real GCP call failed ({e}). Falling back to simulated ingestion for this run.")
                using_real = False
                bronze_df, _, entry = pipeline.run_bronze(raw_df, None)
                lineage.append(entry)
                status.update(label="Stage 1/5: Ingestion — done (simulated fallback)", state="complete")
        progress.progress(20, text="Bronze zone landed")

        with st.status("Stage 2/5: Bronze → Silver (cleaning)", expanded=True) as status:
            st.write("Deduplicating, filling nulls, dropping invalid rows, casting types.")
            time.sleep(delay)
            try:
                silver_df, silver_stats, silver_gcs_uri, entry = pipeline.run_silver(bronze_df, gcp_cfg if using_real else None)
                lineage.append(entry)
                if using_real and silver_gcs_uri:
                    st.success(f"Real write confirmed: {silver_gcs_uri}")
            except Exception as e:
                st.error(f"Real GCP call failed ({e}). Continuing with local computation only.")
                silver_df, silver_stats, _, entry = pipeline.run_silver(bronze_df, None)
                lineage.append(entry)
                using_real = False
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Duplicates removed", silver_stats["duplicates_removed"])
            c2.metric("Nulls filled", silver_stats["nulls_filled"])
            c3.metric("Invalid rows dropped", silver_stats["invalid_rows_dropped"])
            c4.metric("Clean rows", silver_stats["rows_after_cleaning"])
            logger.info("Silver stage complete: %s", silver_stats)
            status.update(label="Stage 2/5: Silver zone — done", state="complete")
        progress.progress(40, text="Silver zone cleaned")

        with st.status("Stage 3/5: Silver → Gold (aggregation)", expanded=True) as status:
            st.write("Aggregating into business-ready tables (BigQuery-style).")
            time.sleep(delay)
            try:
                by_region, by_product, monthly, kpis, bq_ref, entry = pipeline.run_gold(silver_df, gcp_cfg if using_real else None)
                lineage.append(entry)
                if using_real and bq_ref:
                    st.success(f"Real round trip confirmed: wrote to and read back from `{bq_ref}`")
            except Exception as e:
                st.error(f"Real GCP call failed ({e}). Continuing with local computation only.")
                by_region, by_product, monthly, kpis, _, entry = pipeline.run_gold(silver_df, None)
                lineage.append(entry)
            st.write(f"Built **{len(by_region)}** region rollups and **{len(by_product)}** product rollups.")
            status.update(label="Stage 3/5: Gold zone — done", state="complete")
        progress.progress(60, text="Gold tables built")

        with st.status("Stage 4/5: ML platform (anomaly detection + forecast)", expanded=True) as status:
            st.write("Flagging revenue outliers and forecasting the next 2 months (Vertex AI-style).")
            time.sleep(delay)
            anomalies, forecast, entry = pipeline.run_ml(silver_df, monthly)
            lineage.append(entry)
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
            st.plotly_chart(fig1, width='stretch')
        with chart_col2:
            fig2 = px.pie(by_product, names="product", values="total_revenue", title="Revenue by product")
            st.plotly_chart(fig2, width='stretch')

        if len(forecast):
            trend_df = pd.concat([
                monthly.assign(kind="Actual").rename(columns={"total_revenue": "revenue"}),
                forecast.assign(kind="Forecast").rename(columns={"predicted_revenue": "revenue"}),
            ], ignore_index=True)
            fig3 = px.line(trend_df, x="month", y="revenue", color="kind", markers=True, title="Monthly revenue: actual + forecast")
        else:
            fig3 = px.line(monthly, x="month", y="total_revenue", markers=True, title="Monthly revenue trend")
        st.plotly_chart(fig3, width='stretch')

        st.subheader("ML platform outputs")
        m1, m2 = st.tabs(["Anomalies detected", "Revenue forecast"])
        with m1:
            if len(anomalies):
                st.dataframe(anomalies, width='stretch', hide_index=True)
            else:
                st.caption("No anomalies above the |z| > 3 threshold in this run.")
        with m2:
            if len(forecast):
                st.dataframe(forecast, width='stretch', hide_index=True)
            else:
                st.caption("Not enough monthly history to forecast (need 3+ months).")

        st.subheader("Gold tables")
        t1, t2 = st.tabs(["By region", "By product"])
        with t1:
            st.dataframe(by_region, width='stretch', hide_index=True)
        with t2:
            st.dataframe(by_product, width='stretch', hide_index=True)

        csv_buf = io.StringIO()
        by_region.to_csv(csv_buf, index=False)
        st.download_button("Download region rollup (CSV)", csv_buf.getvalue(), file_name="gold_region_rollup.csv", mime="text/csv")

        st.subheader("Data quality & lineage audit")
        mode_label = "Real GCP infrastructure (Pub/Sub + Cloud Storage + BigQuery)" if using_real else "Simulated (in-memory, no GCP infrastructure provisioned)"
        st.caption(f"Mode for this run: **{mode_label}**. Every stage logs its row counts and what it did.")
        st.dataframe(pd.DataFrame(lineage), width='stretch', hide_index=True)

        # Save a compact summary for the AI Assistant page to use as context
        st.session_state["last_pipeline_summary"] = (
            f"Pipeline run mode: {mode_label}.\n"
            f"KPIs: total revenue ${kpis['total_revenue']:,.0f}, "
            f"{kpis['total_orders']} orders, avg order value ${kpis['avg_order_value']:,.0f}, "
            f"top region {kpis['top_region']}.\n"
            f"Revenue by region: {by_region.to_dict(orient='records')}\n"
            f"Revenue by product: {by_product.to_dict(orient='records')}\n"
            f"Anomalies detected: {len(anomalies)} orders flagged (|z-score| > 3).\n"
            f"Forecast (next 2 months): {forecast.to_dict(orient='records') if len(forecast) else 'not enough history'}\n"
            f"Data quality: {silver_stats}"
        )

        # Structured stats for the animated architecture page - lets that
        # diagram show real numbers from this run instead of generic text.
        st.session_state["last_pipeline_stats"] = {
            "bronze_rows": f"This run: {silver_stats['rows_after_cleaning'] + silver_stats['duplicates_removed'] + silver_stats['invalid_rows_dropped']} rows landed.",
            "silver_stats": (
                f"This run: removed {silver_stats['duplicates_removed']} duplicates, "
                f"filled {silver_stats['nulls_filled']} nulls, dropped {silver_stats['invalid_rows_dropped']} invalid rows."
            ),
            "gold_rows": f"This run: {len(by_region)} region and {len(by_product)} product rollups produced.",
            "anomalies": f"This run: flagged {len(anomalies)} orders as revenue outliers.",
            "forecast": f"This run: forecasted {len(forecast)} future months." if len(forecast) else "This run: not enough history to forecast.",
            "kpi_summary": f"This run: ${kpis['total_revenue']:,.0f} total revenue across {kpis['total_orders']} orders.",
        }


# ---------------------------------------------------------------------------
# Architecture overview page
# ---------------------------------------------------------------------------

def render_architecture_overview():
    st.title("GCP end-to-end architecture")
    st.markdown("The pipeline demo mirrors this reference architecture, stage for stage.")

    LAYERS = [
        {"name": "1. Data sources", "services": ["Cloud SQL / AlloyDB (OLTP)", "SaaS & on-prem apps", "IoT / Pub/Sub streams", "Cloud Storage (files)", "Third-party / partner APIs"], "purpose": "Capture all enterprise signals, structured and unstructured, batch and streaming."},
        {"name": "2. Ingestion", "services": ["Pub/Sub (streaming ingestion) — real in this app", "Datastream (CDC)", "Cloud Data Fusion (batch ETL)", "Dataflow (unified batch + stream)"], "purpose": "Collect and land data reliably, whether it arrives continuously or on a schedule."},
        {"name": "3. Lakehouse storage", "services": ["Cloud Storage (bronze/silver zones) — real in this app", "BigLake (open table format)", "Iceberg / Delta / Hudi", "BigQuery native storage (gold zone) — real in this app"], "purpose": "Medallion architecture: raw, cleaned, and curated zones on open, queryable storage."},
        {"name": "4. Processing", "services": ["Dataflow (batch/stream compute)", "Dataproc (Spark/Hadoop)", "BigQuery SQL / BigQuery ML", "Cloud Composer (orchestration)"], "purpose": "Transform, join, and aggregate data into analytics- and ML-ready tables."},
        {"name": "5. ML platform", "services": ["Vertex AI Training / AutoML", "Vertex AI Feature Store", "Vertex AI Model Registry & Endpoints", "Vertex AI Pipelines & Model Monitoring"], "purpose": "Build, train, deploy, and monitor ML/AI models at scale, including LLM/RAG workloads."},
        {"name": "6. Analytics & BI", "services": ["BigQuery (warehouse)", "Looker / Looker Studio", "Connected Sheets", "BigQuery BI Engine"], "purpose": "Explore, visualize, and operationalize metrics and KPIs for business consumption."},
        {"name": "7. Application layer", "services": ["Cloud Run (containerized apps) — recommended host", "API Gateway", "Streamlit / web apps — this app", "Cloud Functions"], "purpose": "Deliver insights, APIs, and AI capabilities to end users and downstream systems."},
    ]
    for layer in LAYERS:
        with st.expander(layer["name"]):
            st.write(layer["purpose"])
            for svc in layer["services"]:
                st.markdown(f"- {svc}")


# ---------------------------------------------------------------------------
# Animated architecture page - MLOps platform flow with icons, mirrors the
# target architecture reference image, mapped to GCP services
# ---------------------------------------------------------------------------

def render_animated_architecture():
    st.title("Animated architecture: GCP MLOps platform")
    st.markdown(
        "Click **Run pipeline flow** to watch data and models move through "
        "the 8-stage platform, one service at a time — the GCP equivalent "
        "of a full production data and ML platform (data lake, feature "
        "store, model registry, training, monitoring, serving)."
    )

    has_real_stats = bool(st.session_state.get("last_pipeline_stats"))
    use_real = st.checkbox(
        "Show numbers from my last real pipeline run",
        value=has_real_stats,
        disabled=not has_real_stats,
        help="Run the pipeline on 'Live pipeline demo' first to enable this — the walkthrough below will then show actual row counts and KPIs instead of generic descriptions.",
    )
    if not has_real_stats:
        st.caption("No pipeline run yet this session — showing generic descriptions. Run the pipeline demo first to see real numbers here.")
    elif use_real:
        st.caption("Showing real numbers from your last pipeline run.")

    stats = st.session_state.get("last_pipeline_stats") if (has_real_stats and use_real) else None
    html = animated_architecture.build_html(stats=stats)
    components.html(html, height=1200, scrolling=True)

    st.caption(
        "Each service activates individually with its own action line, and the log below "
        "records everything once the run finishes. The 'Live pipeline demo' page runs the "
        "actual working slice of this (ingestion through analytics) against real or sample data."
    )


# ---------------------------------------------------------------------------
# Reusable asset catalog page (persisted to a local JSON file)
# ---------------------------------------------------------------------------

def render_asset_catalog():
    st.title("Reusable asset catalog")
    st.caption(
        "Persisted to a local file on this server so it survives a page refresh. "
        "For durability across multiple Cloud Run instances, swap this for "
        "Firestore or a BigQuery table (same pattern as the pipeline's real-GCP mode)."
    )

    catalog = load_catalog()
    catalog_df = pd.DataFrame(catalog).astype(str).reset_index(drop=True)
    st.table(catalog_df.drop(columns=["Status"]))

    st.subheader("Add a new asset")
    with st.form("add_asset"):
        name = st.text_input("Asset name")
        atype = st.selectbox("Type", ["Architecture diagram", "Terraform template", "Code accelerator", "Demo asset", "Documentation"])
        layer = st.selectbox("Layer", ["All", "Data sources", "Ingestion", "Storage", "Processing", "ML platform", "Analytics & BI", "Application"])
        status = st.selectbox("Status", ["Planned", "In progress", "Published"])
        submitted = st.form_submit_button("Add to catalog")
        if submitted and name:
            catalog.append({"Asset": name, "Type": atype, "Layer": layer, "Status": status})
            save_catalog(catalog)
            logger.info("Catalog asset added: %s", name)
            st.success(f"Added '{name}' — saved to the catalog file.")
            st.rerun()


# ---------------------------------------------------------------------------
# AI Assistant page - free built-in help by default, optional bring-your-own-key
# ---------------------------------------------------------------------------

def render_ai_assistant():
    st.title("AI Assistant")

    mode = st.radio(
        "Mode",
        ["Free built-in help (no API key, no cost)", "Bring your own API key (Claude / ChatGPT / Gemini / Groq)"],
        horizontal=False,
    )

    if "chat_history" not in st.session_state:
        st.session_state["chat_history"] = []

    if mode == "Free built-in help (no API key, no cost)":
        st.markdown(
            "Answers common questions about this CoE, the architecture, and "
            "the app using a built-in knowledge base — no API key, no "
            "external call, no cost. Ask about: architecture layers, "
            "pipeline stages, what's real vs simulated, deployment, real "
            "GCP mode, the catalog, certifications, or errors like segfaults."
        )

        for msg in st.session_state["chat_history"]:
            with st.chat_message(msg["role"]):
                st.markdown(msg["content"])

        user_input = st.chat_input("Ask a question about the CoE or this app...")
        if user_input:
            st.session_state["chat_history"].append({"role": "user", "content": user_input})
            with st.chat_message("user"):
                st.markdown(user_input)
            reply = help_bot.answer(user_input) or help_bot.FALLBACK
            with st.chat_message("assistant"):
                st.markdown(reply)
            st.session_state["chat_history"].append({"role": "assistant", "content": reply})

    else:
        st.markdown(
            "For open-ended questions the built-in FAQ can't answer, or to "
            "discuss your last pipeline run in detail, bring your own API key."
        )
        st.caption(
            "Your API key is used only for this session — it is never written "
            "to disk, saved to the asset catalog, or logged. Requires "
            "`pip install -r requirements-ai.txt` for the provider you choose. "
            "Gemini and Groq both have genuinely free tiers (no credit card) — "
            "see the app's setup notes for how to get a key."
        )

        col1, col2 = st.columns(2)
        with col1:
            provider = st.selectbox("Provider", list(ai_helper.DEFAULT_MODELS.keys()))
        with col2:
            model = st.text_input(
                "Model name (editable — provider lineups change)",
                value=ai_helper.DEFAULT_MODELS[provider],
            )

        api_key = st.text_input(f"{provider} API key", type="password", key=f"api_key_{provider}")

        use_context = st.checkbox(
            "Include my last pipeline run as context",
            value=bool(st.session_state.get("last_pipeline_summary")),
            disabled="last_pipeline_summary" not in st.session_state,
            help="Run the pipeline on the 'Live pipeline demo' page first to enable this.",
        )

        for msg in st.session_state["chat_history"]:
            with st.chat_message(msg["role"]):
                st.markdown(msg["content"])

        user_input = st.chat_input("Ask about the architecture, the CoE, or your pipeline results...")

        if user_input:
            if not api_key:
                st.error(f"Enter your {provider} API key above first.")
            else:
                st.session_state["chat_history"].append({"role": "user", "content": user_input})
                with st.chat_message("user"):
                    st.markdown(user_input)

                system_prompt = (
                    "You are a helpful assistant embedded in a GCP Data & AI Center "
                    "of Excellence demo app for Incedo. Answer questions about the "
                    "GCP reference architecture (data sources, ingestion, lakehouse "
                    "storage, processing, ML platform, analytics/BI, application "
                    "layer) and, when provided, the user's own pipeline run results. "
                    "Be concise and specific."
                )
                if use_context and st.session_state.get("last_pipeline_summary"):
                    system_prompt += "\n\nLatest pipeline run summary:\n" + st.session_state["last_pipeline_summary"]

                with st.chat_message("assistant"):
                    with st.spinner(f"Asking {provider}..."):
                        try:
                            reply = ai_helper.ask(
                                provider, api_key, model,
                                st.session_state["chat_history"],
                                system=system_prompt,
                            )
                            st.markdown(reply)
                            st.session_state["chat_history"].append({"role": "assistant", "content": reply})
                        except ImportError:
                            st.error(
                                f"The {provider} SDK isn't installed. Run "
                                f"`pip install -r requirements-ai.txt` and restart the app."
                            )
                        except Exception as e:
                            logger.exception("AI assistant call failed")
                            st.error(f"{provider} call failed: {e}")

    if st.session_state["chat_history"] and st.button("Clear chat"):
        st.session_state["chat_history"] = []
        st.rerun()


# ---------------------------------------------------------------------------
# Main entry point: access gate, sidebar navigation, and a top-level error
# boundary around whichever page is selected. An unexpected exception on
# one page shows a friendly recoverable message instead of Streamlit's raw
# crash screen, and is logged for debugging rather than silently swallowed.
# ---------------------------------------------------------------------------

def main():
    if not check_access():
        st.stop()

    st.sidebar.title("GCP Data & AI CoE")
    page = st.sidebar.radio(
        "Navigate",
        ["Live pipeline demo", "Architecture overview", "Animated architecture", "Reusable asset catalog", "AI Assistant"],
    )
    st.sidebar.markdown("---")
    st.sidebar.caption("Incedo Data Technology CoE — GCP track")

    pages = {
        "Live pipeline demo": render_live_pipeline_demo,
        "Architecture overview": render_architecture_overview,
        "Animated architecture": render_animated_architecture,
        "Reusable asset catalog": render_asset_catalog,
        "AI Assistant": render_ai_assistant,
    }

    try:
        pages[page]()
    except Exception as e:
        logger.exception("Unhandled error rendering page: %s", page)
        st.error(
            "Something went wrong rendering this page. It's been logged. "
            "Try refreshing, or switch to a different page from the sidebar."
        )
        with st.expander("Technical details (for debugging)"):
            st.code(f"{type(e).__name__}: {e}")


if __name__ == "__main__":
    main()
