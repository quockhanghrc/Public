# Inference Serving Guide

Deploy real-time inference for the ML Pipeline on GKE.

---

## Setup

```powershell
$PROJECT_ID = "YOUR_PROJECT_ID"
$REGION = "YOUR_REGION"
$CLUSTER = "ml-pipeline-cluster"
$NAMESPACE = "card-approval-training"

gcloud auth login
gcloud config set project $PROJECT_ID
```

## 1. Enable APIs

```powershell
gcloud services enable container.googleapis.com artifactregistry.googleapis.com cloudbuild.googleapis.com monitoring.googleapis.com logging.googleapis.com iamcredentials.googleapis.com
```

## 2. Create GKE Autopilot Cluster

```powershell
gcloud container clusters create-auto $CLUSTER --region $REGION --project $PROJECT_ID
gcloud container clusters get-credentials $CLUSTER --region $REGION --project $PROJECT_ID
```

## 3. Create Namespace & SA

```powershell
kubectl create namespace $NAMESPACE
kubectl create serviceaccount training-sa --namespace $NAMESPACE
kubectl annotate serviceaccount training-sa --namespace $NAMESPACE iam.gke.io/gcp-service-account=training-sa@$PROJECT_ID.iam.gserviceaccount.com --overwrite
```

## 4. Build & Push Image

```powershell
gcloud builds submit --tag $REGION-docker.pkg.dev/$PROJECT_ID/mlflow/credit-limit-training:latest .
```

## 5. Deploy Inference Service

```powershell
kubectl apply -f manifests/inference.yaml
```

## 6. Sideload Model

```powershell
gsutil cp gs://ml-models-$PROJECT_ID/limit_predictor_best.txt ./model.txt
$POD = kubectl get pods -n $NAMESPACE -l app=inference -o jsonpath='{.items[0].metadata.name}'
kubectl cp ./model.txt ${POD}:/app/models/active_model.txt -n $NAMESPACE
```

## 7. Test Inference

```powershell
kubectl port-forward service/credit-limit-inference -n $NAMESPACE 8080:80
python scripts/test_inference.py
```