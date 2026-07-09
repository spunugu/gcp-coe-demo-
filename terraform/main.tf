terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ---------------------------------------------------------------------------
# Cloud Storage - bronze/silver landing zones
# ---------------------------------------------------------------------------

resource "google_storage_bucket" "lakehouse" {
  name                        = var.bucket_name
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true

  lifecycle_rule {
    condition { age = 30 }
    action { type = "Delete" }
  }
}

# ---------------------------------------------------------------------------
# BigQuery - gold zone
# ---------------------------------------------------------------------------

resource "google_bigquery_dataset" "gold" {
  dataset_id                 = var.dataset_id
  location                   = var.region
  delete_contents_on_destroy = true
}

# ---------------------------------------------------------------------------
# Pub/Sub - ingestion topic and subscription
# ---------------------------------------------------------------------------

resource "google_pubsub_topic" "ingestion" {
  name = var.topic_id
}

resource "google_pubsub_subscription" "ingestion_pull" {
  name  = var.subscription_id
  topic = google_pubsub_topic.ingestion.name

  ack_deadline_seconds      = 20
  message_retention_duration = "600s"
}

# ---------------------------------------------------------------------------
# Service account for the Cloud Run deployment, with least-privilege roles
# ---------------------------------------------------------------------------

resource "google_service_account" "app_runner" {
  account_id   = var.service_account_id
  display_name = "GCP CoE demo app runner"
}

resource "google_storage_bucket_iam_member" "app_can_write_bucket" {
  bucket = google_storage_bucket.lakehouse.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.app_runner.email}"
}

resource "google_bigquery_dataset_iam_member" "app_can_edit_dataset" {
  dataset_id = google_bigquery_dataset.gold.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.app_runner.email}"
}

resource "google_project_iam_member" "app_can_run_bq_jobs" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.app_runner.email}"
}

resource "google_pubsub_topic_iam_member" "app_can_publish" {
  topic  = google_pubsub_topic.ingestion.name
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:${google_service_account.app_runner.email}"
}

resource "google_pubsub_subscription_iam_member" "app_can_subscribe" {
  subscription = google_pubsub_subscription.ingestion_pull.name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:${google_service_account.app_runner.email}"
}
