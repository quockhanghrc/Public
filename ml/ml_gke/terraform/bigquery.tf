# 1. External Database (Credit Bureau)
resource "google_bigquery_dataset" "external_bureau" {
  dataset_id = "external_bureau"
  location   = var.region
}

resource "google_bigquery_table" "bureau_features" {
  dataset_id = google_bigquery_dataset.external_bureau.dataset_id
  table_id   = "credit_history"
  deletion_protection = false

  schema = <<EOF
[
  {"name": "bureau_user_id", "type": "STRING", "mode": "REQUIRED"},
  {"name": "fico_score", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "active_credit_lines", "type": "INTEGER", "mode": "NULLABLE"},
  {"name": "past_due_incidents", "type": "INTEGER", "mode": "NULLABLE"}
]
EOF
}

# 2. Internal Database (Application Data)
resource "google_bigquery_dataset" "internal_app" {
  dataset_id = "internal_app"
  location   = var.region
}

resource "google_bigquery_table" "application_data" {
  dataset_id = google_bigquery_dataset.internal_app.dataset_id
  table_id   = "applicant_profiles"
  deletion_protection = false

  schema = <<EOF
[
  {"name": "app_user_id", "type": "STRING", "mode": "REQUIRED"},
  {"name": "annual_income", "type": "FLOAT", "mode": "NULLABLE"},
  {"name": "current_debt", "type": "FLOAT", "mode": "NULLABLE"},
  {"name": "approved_limit", "type": "FLOAT", "mode": "NULLABLE"},
  {"name": "application_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
  {"name": "data_split", "type": "STRING", "mode": "NULLABLE"}
]
EOF
}
