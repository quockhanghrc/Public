resource "google_artifact_registry_repository" "ml_repository" {
  location      = var.region
  repository_id = "mlflow"
  description   = "Docker repository for ML Pipeline images"
  format        = "DOCKER"

  docker_config {
    immutable_tags = false
  }
}
