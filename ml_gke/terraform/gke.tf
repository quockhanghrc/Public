resource "google_container_cluster" "ml_cluster" {
  name     = "ml-pipeline-cluster"
  location = var.region

  enable_autopilot = true
  
  network    = "default"
  subnetwork = "default"

  deletion_protection = false

  monitoring_config {
    managed_prometheus {
      enabled = true
    }
    enable_components = ["SYSTEM_COMPONENTS", "DEPLOYMENT", "POD"]
  }
}
