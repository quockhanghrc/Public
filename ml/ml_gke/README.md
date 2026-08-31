# 🧠 ML Pipeline — High-Volume Credit Limit Prediction

A **cloud-native MLOps pipeline** for training, evaluating, and serving a **LightGBM regression model** that predicts credit limits at scale (~2.5M records). Built on **Google Cloud Platform (GCP)** with **GKE**, **BigQuery**, **MLflow**, and **Confluent Kafka**.

---

## 📁 Folder Structure

```
publish/
├── train_model.py              # LightGBM training with grid search + Prometheus metrics
├── Dockerfile                  # Python 3.10-slim container for GKE
├── requirements.txt            # Python dependencies
│
├── terraform/                  # GCP Infrastructure as Code
│   ├── provider.tf             # GCP provider config
│   ├── bigquery.tf             # BigQuery datasets & tables
│   ├── gke.tf                  # GKE Autopilot cluster
│   ├── iam.tf                  # Service account + Workload Identity
│   ├── network.tf              # VPC & subnet
│   ├── registry.tf             # Artifact Registry
│   ├── variables.tf            # Input variables
│   ├── outputs.tf              # Output values
│   └── terraform.tfvars.example  # Example config (copy to terraform.tfvars)
│
├── manifests/                  # Kubernetes YAML manifests
│   ├── train-job.yaml          # GKE training Job
│   ├── inference.yaml          # Inference Deployment + Service
│   ├── kafka-bridge.yaml       # Kafka → Inference → BigQuery bridge
│   ├── mlflow-setup.yaml       # MLflow tracking server
│   └── prometheus-podmonitoring.yaml  # Managed Prometheus scraping
│
├── scripts/                    # Utility scripts
│   ├── seed_bigquery.py        # Generate & seed synthetic records into BigQuery
│   ├── bootstrap_kafka.py      # Create Kafka topic via AdminClient
│   ├── deploy_datagen_api.py   # Deploy Confluent Datagen Connector
│   ├── kafka_inference_bridge.py  # Kafka consumer → inference → BigQuery sink
│   ├── evaluate_regression.py  # Champion-Challenger model comparison
│   ├── test_inference.py       # Smoke test for the inference API
│   └── setup_analytics_bq.py   # Create inference_analytics BigQuery dataset
│
├── kafka_secrets/              # Kafka configuration (placeholders)
│   ├── confluent.conf          # Confluent Cloud config template
│   └── inference_schema.json   # Avro schema for credit application events
│
└── docs/                       # Documentation
    ├── deployment_guide.md     # Step-by-step deployment
    ├── inference_serving_guide.md  # Real-time inference setup
    └── infrastructure_overview.md  # Architecture overview
```

---

## 🏗️ Architecture

```
┌──────────────┐     ┌──────────────────┐     ┌──────────────┐
│  BigQuery    │────▶│  GKE Training    │────▶│   MLflow     │
│  (Feature    │     │  Job (LightGBM)  │     │  (Tracking)  │
│   Store)     │     └──────────────────┘     └──────────────┘
└──────────────┘              │
                              ▼
┌──────────────┐     ┌──────────────────┐     ┌──────────────┐
│  Confluent   │────▶│  Kafka Bridge    │────▶│  BigQuery    │
│  Kafka       │     │  (Inference)     │     │  (Analytics) │
│  (Events)    │     └──────────────────┘     └──────────────┘
└──────────────┘              │
                              ▼
                      ┌──────────────────┐
                      │  GKE Inference   │
                      │  Service (2 pods)│
                      └──────────────────┘
```

### Data Flow
1. **Training**: BigQuery → LightGBM grid search → MLflow registry + GCS model storage
2. **Inference**: Confluent Kafka → Kafka Bridge → GKE Inference API → BigQuery analytics sink
3. **Evaluation**: Champion-Challenger — new model promoted only if RMSE beats production

---

## 🚀 Quick Start

### 1. Configure & Deploy Infrastructure
```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars  # Set your GCP project ID
terraform init
terraform apply -refresh=false
```

### 2. Seed Data
```bash
pip install -r requirements.txt
python scripts/seed_bigquery.py --project_id YOUR_PROJECT_ID
```

### 3. Deploy MLflow
```bash
kubectl create namespace mlflow
kubectl apply -f manifests/mlflow-setup.yaml
```

### 4. Build & Push Image
```bash
gcloud builds submit --tag REGION-docker.pkg.dev/PROJECT_ID/mlflow/credit-limit-training:latest .
```

### 5. Run Training
```bash
kubectl apply -f manifests/train-job.yaml
kubectl logs -f job/credit-limit-training -n card-approval-training
```

---

## 🛠️ Tech Stack

| Component | Technology |
| :--- | :--- |
| **Model** | LightGBM (Regression with grid search) |
| **Orchestration** | GKE Autopilot |
| **Feature Store** | BigQuery (dual-database: internal_app + external_bureau) |
| **Experiment Tracking** | MLflow |
| **Streaming** | Confluent Kafka |
| **Monitoring** | Prometheus + Google Managed Prometheus |
| **Infrastructure** | Terraform |
| **Container** | Docker + Artifact Registry |

---

## 📊 Metrics

The training job exposes Prometheus metrics on port `8000`:

| Metric | Description |
| :--- | :--- |
| `ml_training_trial_current` | Current grid search trial |
| `ml_training_best_r2` | Best R² score achieved |
| `ml_training_current_rmse` | RMSE of latest trial |
| `ml_training_cpu_usage_percent` | Container CPU usage |
| `ml_training_memory_usage_bytes` | Container memory usage |
| `ml_training_disk_usage_percent` | Container disk usage |