# Infrastructure Overview

## Architecture

The ML Pipeline deploys infrastructure on GCP via Terraform:

| Resource | Description |
| :--- | :--- |
| **GKE Autopilot** | Managed Kubernetes cluster for training & inference |
| **BigQuery** | Dual-database feature store (internal_app + external_bureau) |
| **Artifact Registry** | Docker image repository |
| **Workload Identity** | IAM-based K8s-to-GCP authentication |
| **VPC** | Custom network with secondary IP ranges for pods/services |

## Data Flow

```
BigQuery (Feature Store) → GKE Training Job (LightGBM) → MLflow (Tracking + Registry)
                                                              ↓
Confluent Kafka → Kafka Bridge → GKE Inference API → BigQuery (Analytics)
```

## Monitoring

- Prometheus metrics exposed on port `8000` (training) and port `8080` (inference)
- Google Cloud Managed Service for Prometheus scrapes metrics automatically
- Key metrics: R², RMSE, CPU/memory usage, inference latency