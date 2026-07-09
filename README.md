# GCP Data & AI CoE — Live Pipeline Demo

A working prototype for the Incedo Data Technology CoE (GCP track). Runs
sample or uploaded data through a real ingestion → bronze → silver → gold →
ML → analytics pipeline, with an optional mode that swaps the simulated
steps for genuine Pub/Sub, Cloud Storage, and BigQuery calls.

## Project structure

```
app.py              Streamlit UI (pages, forms, charts)
pipeline.py          Pure pipeline logic (no Streamlit dependency) - transform,
                      aggregate, ML, and the real-GCP call wrappers
tests/test_pipeline.py  Unit tests for pipeline.py (pytest)
terraform/            Infrastructure as code: bucket, dataset, topic,
                      subscription, service account and IAM bindings
requirements.txt
Dockerfile            For Cloud Run deployment
GCP_SETUP.md          One-time GCP setup + how to enable real GCP mode
```

## Run locally

```bash
pip install -r requirements.txt
streamlit run app.py
```

Works immediately with generated sample data — no GCP project required.

## Run the tests

```bash
pip install -r requirements.txt
pytest tests/
```

Tests cover the pandas transformation logic, the ML anomaly/forecast stage,
and schema validation — all independent of Streamlit and GCP credentials.

## Enable real GCP infrastructure (optional)

By default, ingestion and storage are simulated in-memory so the demo runs
instantly. To have the pipeline genuinely write to Cloud Storage, publish
and pull from Pub/Sub, and round-trip through BigQuery:

1. Provision resources with Terraform — see `terraform/` and `GCP_SETUP.md`
2. In the app, expand **"Real GCP infrastructure (optional)"**, check the
   box, and fill in your project ID, bucket, dataset, and (optionally)
   topic/subscription
3. Run the pipeline — each stage shows a real `gs://` URI or BigQuery table
   reference confirming the call actually happened

Full setup commands (`gcloud`, `terraform apply`, IAM roles) are in
`GCP_SETUP.md`.

## Access control (optional)

Set an `APP_PASSWORD` in `.streamlit/secrets.toml` (local) or your hosting
platform's secrets manager to gate access. Unset by default, which is fine
for an internal demo but not recommended once real GCP credentials are wired
in and the app is reachable publicly.

## Deploy to Cloud Run (recommended for real-GCP mode)

```bash
cd terraform
terraform init
terraform apply -var="project_id=YOUR_PROJECT" -var="bucket_name=YOUR_UNIQUE_BUCKET"
# note the service_account_email output

cd ..
gcloud run deploy gcp-coe-demo \
  --source . \
  --region us-central1 \
  --service-account YOUR_SERVICE_ACCOUNT_EMAIL \
  --allow-unauthenticated \
  --set-env-vars GOOGLE_CLOUD_PROJECT=YOUR_PROJECT
```

Using the Terraform-provisioned service account means no key file and no
personal credentials are needed at runtime — Cloud Run authenticates
automatically.

## Deploy to Streamlit Community Cloud (fastest, simulated mode only)

Push to GitHub, then create an app at share.streamlit.io pointing at
`app.py`. Fine for demos in simulated mode; for real-GCP mode, Cloud Run is
the better host since it can use a service account instead of personal
credentials in a third-party platform.

## Extending this as a CoE asset

- **Data sources**: currently sample-generated or CSV upload only. Point it
  at a real Cloud SQL instance or client dataset once one is identified.
- **Reusable asset catalog**: currently persisted to a local JSON file,
  which survives refreshes on a single running instance but isn't durable
  across multiple Cloud Run instances. Swap for Firestore or a BigQuery
  table for multi-instance durability.
- **ML platform**: currently anomaly detection + linear forecast. A RAG/LLM
  use case (Vertex AI Search) would extend this layer further.
