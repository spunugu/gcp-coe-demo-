variable "project_id" {
  description = "GCP project ID to deploy into"
  type        = string
}

variable "region" {
  description = "GCP region for all resources"
  type        = string
  default     = "us-central1"
}

variable "bucket_name" {
  description = "Cloud Storage bucket name for the bronze/silver lakehouse zones (must be globally unique)"
  type        = string
}

variable "dataset_id" {
  description = "BigQuery dataset ID for the gold zone"
  type        = string
  default     = "gcp_coe_demo"
}

variable "topic_id" {
  description = "Pub/Sub topic ID for ingestion"
  type        = string
  default     = "gcp-coe-demo-ingestion"
}

variable "subscription_id" {
  description = "Pub/Sub subscription ID that the app pulls from"
  type        = string
  default     = "gcp-coe-demo-ingestion-sub"
}

variable "service_account_id" {
  description = "Service account ID (short name) for the Cloud Run app"
  type        = string
  default     = "gcp-coe-demo-runner"
}
