import mlflow
import mlflow.lightgbm
import pandas as pd
import lightgbm as lgb
from sklearn.metrics import mean_squared_error
import numpy as np
import os
from google.cloud import bigquery

def evaluate_and_promote():
    # Configuration
    PROJECT_ID = os.getenv("PROJECT_ID", "YOUR_PROJECT_ID")
    DATASET_ID = os.getenv("DATASET_ID", "credit_bureau")
    TABLE_ID = os.getenv("TABLE_ID", "user_features")
    MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow.mlflow:5000")
    MODEL_NAME = "credit-limit-predictor"
    
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    client = bigquery.Client(project=PROJECT_ID)
    
    # 1. Load Validation Data from BigQuery
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` WHERE split = 'val'"
    print(f"Fetching validation data: {query}")
    val_df = client.query(query).to_dataframe()
    
    features = ["income", "credit_score", "existing_debt", "total_accounts", "late_payments_last_year"]
    target = "target_limit"
    X_val, y_val = val_df[features], val_df[target]

    # 2. Load the "New" model (Challenger)
    new_model = lgb.Booster(model_file="models/limit_predictor.txt")
    y_pred_new = new_model.predict(X_val)
    rmse_new = np.sqrt(mean_squared_error(y_val, y_pred_new))
    print(f"Challenger RMSE: {rmse_new:.2f}")

    # 3. Load the "Champion" model (Production)
    try:
        champion_uri = f"models:/{MODEL_NAME}/Production"
        champion_model = mlflow.lightgbm.load_model(champion_uri)
        y_pred_prod = champion_model.predict(X_val)
        rmse_prod = np.sqrt(mean_squared_error(y_val, y_pred_prod))
        print(f"Current Champion RMSE: {rmse_prod:.2f}")
    except Exception as e:
        print("No Production model found. Promoting first model.")
        rmse_prod = float('inf')

    # 4. Comparison Logic
    if rmse_new < rmse_prod:
        print(f"✅ Challenger WINS! Promoting to Production (RMSE: {rmse_new:.2f} vs {rmse_prod:.2f})")
        # Register the new model in MLflow Model Registry
        with mlflow.start_run():
            mlflow.lightgbm.log_model(new_model, artifact_path="model", registered_model_name=MODEL_NAME)
            client = mlflow.tracking.MlflowClient()
            client.transition_model_version_stage(
                name=MODEL_NAME,
                version=1,
                stage="Production"
            )
    else:
        print(f"❌ Champion retains title (RMSE: {rmse_prod:.2f} vs {rmse_new:.2f})")

if __name__ == "__main__":
    evaluate_and_promote()
