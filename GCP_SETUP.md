# Connecting the app to real GCP infrastructure

By default the pipeline demo runs entirely simulated — no GCP project needed.
This guide covers turning on the **"Use real GCP infrastructure"** toggle so
Bronze/Silver actually write to Cloud Storage and Gold actually round-trips
through BigQuery, using dummy sample data.

## 1. One-time GCP project setup

```bash
gcloud auth login
gcloud config set project YOUR_PROJECT_ID

# Enable the APIs the app needs
gcloud services enable storage.googleapis.com \
  bigquery.googleapis.com \
  pubsub.googleapis.com
```

## 2. Create the resources the app writes to

```bash
# Cloud Storage bucket (bronze/silver zones)
gsutil mb -l us-central1 gs://YOUR_BUCKET_NAME

# BigQuery dataset (gold zone)
bq mk --dataset --location=us-central1 YOUR_PROJECT_ID:YOUR_DATASET_ID

# Optional: Pub/Sub topic (ingestion proof)
gcloud pubsub topics create YOUR_TOPIC_ID
```

## 3. Authenticate locally

```bash
gcloud auth application-default login
```

This lets the app run using your own user credentials — no service account
key file needed for local testing.

## 4. Run the app and enable real mode

```bash
streamlit run app.py
```

In the app: expand **"Real GCP infrastructure (optional)"**, check
**"Use real GCP infrastructure for this run"**, and fill in:
- **GCP project ID** — the project you set up above
- **Cloud Storage bucket name** — the bucket you created (no `gs://` prefix)
- **BigQuery dataset ID** — the dataset you created
- **Pub/Sub topic ID** — optional, only if you created one

Click **Run pipeline**. You'll see real confirmations in the stage log:
- A real `gs://...` URI after Bronze and Silver
- A real BigQuery table reference after Gold, with the dashboard data read
  straight back from that table (proving the write-then-read round trip)

## 5. Deploying with real GCP mode on Cloud Run (recommended over Streamlit Cloud)

Because this now touches real GCP resources, Cloud Run is the better host —
it can use a service account instead of handing out personal credentials.

```bash
# Grant the Cloud Run service account the roles it needs
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:YOUR_PROJECT_NUMBER-compute@developer.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"

gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:YOUR_PROJECT_NUMBER-compute@developer.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:YOUR_PROJECT_NUMBER-compute@developer.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"

gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:YOUR_PROJECT_NUMBER-compute@developer.gserviceaccount.com" \
  --role="roles/pubsub.publisher"

# Deploy
gcloud run deploy gcp-coe-demo \
  --source . \
  --region us-central1 \
  --allow-unauthenticated \
  --set-env-vars GOOGLE_CLOUD_PROJECT=YOUR_PROJECT_ID
```

No key file, no `GOOGLE_APPLICATION_CREDENTIALS` env var needed — Cloud Run
automatically authenticates as its service account.

## Cost note

This uses genuinely tiny amounts of GCP usage — a few KB written to Cloud
Storage, a handful of rows loaded into BigQuery, at most 20 Pub/Sub
messages per run. Comfortably within any free tier for demo purposes.

## If something fails

The app catches GCP errors per stage and automatically falls back to
simulated mode for that run rather than crashing — check the red error
message in the relevant stage box for the underlying cause (usually a
missing IAM role or a typo'd resource name).
