# Deployment Guide: High-Volume ML Pipeline (From Scratch)

This guide takes you from an empty environment to a fully running ML pipeline in Google Cloud Platform (GCP).

---

## Prerequisites

| Tool | Purpose |
| :--- | :--- |
| **Google Cloud SDK (`gcloud`)** | CLI to talk to GCP |
| **BigQuery CLI (`bq`)** | BigQuery specific CLI |
| **Terraform** | Cloud infrastructure as code |
| **kubectl** | Kubernetes CLI |

---

## Step 1: Provision Infrastructure (Terraform)

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with your GCP project ID
tf init
tf apply -refresh=false
```

## Step 2: Seed the Data

```powershell
pip install -r requirements.txt
python scripts/seed_bigquery.py --project_id YOUR_PROJECT_ID
```

## Step 3: Setup MLflow Tracking

```powershell
kubectl create namespace mlflow
kubectl apply -f manifests/mlflow-setup.yaml
kubectl port-forward service/mlflow -n mlflow 5000:5000
```

## Step 4: Build & Push Training Image

```bash
gcloud builds submit --tag REGION-docker.pkg.dev/PROJECT_ID/mlflow/credit-limit-training:latest .
```

## Step 5: Run the Training Job (GKE)

```powershell
gcloud container clusters get-credentials ml-pipeline-cluster --region YOUR_REGION
kubectl create namespace card-approval-training
kubectl apply -f manifests/prometheus-podmonitoring.yaml
kubectl apply -f manifests/train-job.yaml
kubectl logs -f job/credit-limit-training -n card-approval-training
```

## Step 6: Extract Trained Model

```powershell
# Download the best model from GCS
gsutil cp gs://ml-models-YOUR_PROJECT_ID/limit_predictor_best.txt .
```

---

## Cleanup

| Script | Impact |
| :--- | :--- |
| `clean_up/clean_all.ps1` | Delete K8s pods & deployments, keep cluster |
| `clean_up/destroy_everything.ps1` | Destroy cluster, registry, and buckets |