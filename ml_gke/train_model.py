import os
import pandas as pd
import lightgbm as lgb
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
import mlflow
import mlflow.lightgbm
from google.cloud import bigquery
import numpy as np
import psutil
import uuid
import time
from prometheus_client import start_http_server, Gauge, Counter

# Define Prometheus metrics
TRIAL_GAUGE = Gauge('ml_training_trial_current', 'Current trial number being processed')
BEST_R2_GAUGE = Gauge('ml_training_best_r2', 'Highest R2 score achieved so far')
CURRENT_RMSE_GAUGE = Gauge('ml_training_current_rmse', 'RMSE of the most recent completed trial')

# System Metrics
CPU_USAGE_GAUGE = Gauge('ml_training_cpu_usage_percent', 'CPU usage of the training process')
RAM_USAGE_GAUGE = Gauge('ml_training_memory_usage_bytes', 'Memory usage of the training process in bytes')
DISK_USAGE_GAUGE = Gauge('ml_training_disk_usage_percent', 'Disk usage percent of the container storage')

def update_system_metrics():
    """Helper to capture current container resource usage"""
    CPU_USAGE_GAUGE.set(psutil.cpu_percent())
    RAM_USAGE_GAUGE.set(psutil.Process().memory_info().rss)
    DISK_USAGE_GAUGE.set(psutil.disk_usage('/').percent)

def train_lightgbm():
    # Start Prometheus metrics server on port 8000
    try:
        start_http_server(8000)
        print("Prometheus metrics server started on port 8000")
    except Exception as e:
        print(f"Warning: Could not start Prometheus server: {e}")

    # Configuration — override via environment variables
    PROJECT_ID = os.getenv("PROJECT_ID", "YOUR_PROJECT_ID")
    DATASET_ID = os.getenv("DATASET_ID", "credit_bureau")
    TABLE_ID = os.getenv("TABLE_ID", "user_features")
    MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow.mlflow:5000")
    
    # MLflow Setup (Optional)
    try:
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        mlflow.set_experiment("credit-limit-regression")
        use_mlflow = True
        print(f"MLflow tracking enabled at: {MLFLOW_TRACKING_URI}")
    except Exception as e:
        print(f"MLflow tracking disabled (could not connect): {e}")
        use_mlflow = False

    client = bigquery.Client(project=PROJECT_ID)
    
    # DEBUG: Print Identity
    try:
        from google.auth import default
        credentials, project = default()
        print(f"DEBUG: Authenticated Service Account: {credentials.service_account_email if hasattr(credentials, 'service_account_email') else 'Unknown'}")
    except Exception as e:
        print(f"DEBUG: Could not determine identity: {e}")
    
    # Query Data: JOIN Internal and External with DISTINCT columns
    # joining on app_user_id = bureau_user_id
    query = f"""
    SELECT 
        i.annual_income, i.current_debt, i.approved_limit, i.data_split,
        e.fico_score, e.active_credit_lines, e.past_due_incidents
    FROM `{PROJECT_ID}.internal_app.applicant_profiles` AS i
    LEFT JOIN `{PROJECT_ID}.external_bureau.credit_history` AS e
    ON i.app_user_id = e.bureau_user_id
    """
    print(f"Loading data from joined BigQuery sources...")
    
    # Load into dataframe
    df = client.query(query).to_dataframe()
    
    # Split into train/validation based on the 'data_split' column
    train_df = df[df['data_split'] == 'train']
    val_df = df[df['data_split'] == 'val']
    
    # Map new column names to feature list
    features = ["annual_income", "fico_score", "current_debt", "active_credit_lines", "past_due_incidents"]
    target = "approved_limit"
    
    X_train, y_train = train_df[features], train_df[target]
    X_val, y_val = val_df[features], val_df[target]

    print(f"Training on {len(X_train)} samples, validating on {len(X_val)} samples...")

    # Hyperparameter Tuning: Grid Search of All Combinations
    import itertools
    
    # Define the search space
    param_space = {
        "learning_rate": [0.01, 0.05, 0.1],
        "num_leaves": [31],
        "bagging_fraction": [0.8, 0.9],
        "bagging_freq": [1, 5]
    }
    
    # Generate all combinations
    keys, values = zip(*param_space.items())
    param_grid = [dict(zip(keys, v)) for v in itertools.product(*values)]
    
    print(f"Generated {len(param_grid)} combinations for Grid Search.")

    # Unique run identifier for this specific training session
    run_uuid = str(uuid.uuid4())[:8]
    start_time = int(time.time())
    run_folder = f"run_{run_uuid}_{start_time}"
    print(f"Starting unique training session: {run_folder}")

    best_r2 = -float('inf')
    best_model = None
    all_results = []

    for i, grid_params in enumerate(param_grid):
        print(f"\n--- Trial {i+1}/{len(param_grid)}: Params={grid_params} ---")
        TRIAL_GAUGE.set(i + 1)
        update_system_metrics()
        
        # Base Parameters
        params = {
            "objective": "regression",
            "metric": "rmse",
            "verbosity": -1,
            "boosting_type": "gbdt",
            "feature_fraction": 0.9,
            **grid_params
        }

        run_name = f"trial-{i+1}"
        
        if use_mlflow:
            with mlflow.start_run(run_name=run_name):
                model, r2, rmse, mae = run_training(params, X_train, y_train, X_val, y_val, use_mlflow)
        else:
            model, r2, rmse, mae = run_training(params, X_train, y_train, X_val, y_val, use_mlflow)
            
        # Update Prometheus metrics
        CURRENT_RMSE_GAUGE.set(rmse)
        if r2 > best_r2:
            BEST_R2_GAUGE.set(r2)
            
        # Save and Upload THIS trial's model
        trial_filename = f"model_trial_{i+1}.txt"
        local_path = f"models/{trial_filename}"
        os.makedirs("models", exist_ok=True)
        model.save_model(local_path)
        
        # Upload to GCS immediately
        BUCKET_NAME = os.getenv("MODEL_BUCKET", f"ml-models-{PROJECT_ID}")
        try:
            from google.cloud import storage
            storage_client = storage.Client(project=PROJECT_ID)
            bucket = storage_client.bucket(BUCKET_NAME)
            
            # Use unique folder for this run session
            gcs_path = f"training_runs/{run_folder}/trials/{trial_filename}"
            blob = bucket.blob(gcs_path)
            
            blob.upload_from_filename(local_path)
            print(f"   fw saved to gs://{BUCKET_NAME}/{gcs_path}")
        except Exception as e:
            print(f"   ⚠️ Failed to upload trial model: {e}")

        all_results.append({
            "trial": i + 1,
            "r2": r2,
            "rmse": rmse,
            "mae": mae,
            "params": grid_params
        })

        if r2 > best_r2:
            best_r2 = r2
            best_model = model
            best_trial = i + 1

    # Print Final Comparative Report
    print("\n" + "="*50)
    print("🏆 FINAL HYPERPARAMETER TUNING REPORT 🏆")
    print("="*50)
    print(f"{'Trial':<6} | {'R2':<8} | {'RMSE':<10} | {'Learning Rate':<15} | {'Leaves':<6}")
    print("-" * 50)
    for res in all_results:
        p = res['params']
        print(f"{res['trial']:<6} | {res['r2']:<8.4f} | {res['rmse']:<10.2f} | {p['learning_rate']:<15} | {p['num_leaves']:<6}")
    
    # Save best model locally
    if best_model:
        model_path = "models/limit_predictor_best.txt"
        best_model.save_model(model_path)
        print(f"\n✨ WINNER: Trial {best_trial} with R2: {best_r2:.4f}")
        print(f"Best model saved locally to {model_path}")

        # Upload Best Model to GCS Root and Training Run Folder
        try:
            from google.cloud import storage
            storage_client = storage.Client(project=PROJECT_ID)
            bucket = storage_client.bucket(BUCKET_NAME)
            
            # 1. Update the universal 'best' model at root
            blob_best = bucket.blob("limit_predictor_best.txt")
            print(f"Uploading BEST model to gs://{BUCKET_NAME}/limit_predictor_best.txt ...")
            blob_best.upload_from_filename(model_path)
            
            # 2. ALSO save it in the unique run folder for history
            blob_run_best = bucket.blob(f"training_runs/{run_folder}/limit_predictor_best.txt")
            blob_run_best.upload_from_filename(model_path)
            
            print("✅ Best Model successfully exported to Cloud Storage!")
        except Exception as e:
            print(f"⚠️ Could not upload model to GCS: {e}")

def run_training(params, X_train, y_train, X_val, y_val, use_mlflow):
    if use_mlflow:
        mlflow.log_params(params)

    # Create LightGBM datasets
    train_data = lgb.Dataset(X_train, label=y_train)
    val_data = lgb.Dataset(X_val, label=y_val, reference=train_data)

    # Train with early stopping
    model = lgb.train(
        params,
        train_data,
        valid_sets=[val_data],
        num_boost_round=1000,
        callbacks=[lgb.early_stopping(stopping_rounds=50)]
    )

    # Predictions and Metrics
    y_pred = model.predict(X_val)
    rmse = np.sqrt(mean_squared_error(y_val, y_pred))
    mae = mean_absolute_error(y_val, y_pred)
    r2 = r2_score(y_val, y_pred)

    print(f"Metrics: RMSE={rmse:.2f}, MAE={mae:.2f}, R2={r2:.2f}")

    if use_mlflow:
        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("mae", mae)
        mlflow.log_metric("r2", r2)
        mlflow.lightgbm.log_model(model, artifact_path="model")
    
    return model, r2, rmse, mae

if __name__ == "__main__":
    train_lightgbm()
