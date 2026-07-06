resource "google_compute_network" "ml_vpc" {
  name                    = "ml-pipeline-vpc"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "ml_subnet" {
  name          = "ml-pipeline-subnet"
  ip_cidr_range = "10.0.0.0/20"
  region        = var.region
  network       = google_compute_network.ml_vpc.id

  secondary_ip_range {
    range_name    = "pods"
    ip_cidr_range = "10.1.0.0/16"
  }

  secondary_ip_range {
    range_name    = "services"
    ip_cidr_range = "10.2.0.0/20"
  }
}
