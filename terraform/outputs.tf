output "bucket_name" {
  value = google_storage_bucket.lakehouse.name
}

output "dataset_id" {
  value = google_bigquery_dataset.gold.dataset_id
}

output "topic_id" {
  value = google_pubsub_topic.ingestion.name
}

output "subscription_id" {
  value = google_pubsub_subscription.ingestion_pull.name
}

output "service_account_email" {
  value = google_service_account.app_runner.email
}
