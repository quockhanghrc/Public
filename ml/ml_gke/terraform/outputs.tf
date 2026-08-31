output "internal_dataset_id" {
  value = google_bigquery_dataset.internal_app.dataset_id
}

output "external_dataset_id" {
  value = google_bigquery_dataset.external_bureau.dataset_id
}

output "gke_cluster_name" {
  value = google_container_cluster.ml_cluster.name
}

output "artifact_registry_repo" {
  value = google_artifact_registry_repository.ml_repository.name
}

output "internal_table" {
  value = "${var.project_id}.${google_bigquery_dataset.internal_app.dataset_id}.${google_bigquery_table.application_data.table_id}"
}

output "external_table" {
  value = "${var.project_id}.${google_bigquery_dataset.external_bureau.dataset_id}.${google_bigquery_table.bureau_features.table_id}"
}
