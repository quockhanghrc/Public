# 1. Create the GCP Service Account
resource "google_service_account" "training_sa" {
  account_id   = "training-sa"
  display_name = "ML Pipeline Training Service Account"
}

# 2. Grant BigQuery Editor permissions to the SA
resource "google_project_iam_member" "bq_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.training_sa.email}"
}

resource "google_project_iam_member" "bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.training_sa.email}"
}

resource "google_project_iam_member" "bq_read_session" {
  project = var.project_id
  role    = "roles/bigquery.readSessionUser"
  member  = "serviceAccount:${google_service_account.training_sa.email}"
}

resource "google_project_iam_member" "storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.training_sa.email}"
}

# 3. Allow Kubernetes SA to use the GCP SA via Workload Identity
resource "google_service_account_iam_member" "workload_identity_user" {
  service_account_id = google_service_account.training_sa.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[card-approval-training/training-sa]"
}
