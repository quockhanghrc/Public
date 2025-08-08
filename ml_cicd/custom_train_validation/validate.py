# validator/validate.py (Integrated Comparison Version)

import os
import json
import joblib
import argparse
import pandas as pd
import numpy as np
from sklearn.metrics import r2_score, mean_squared_error
from google.cloud import storage
from google.cloud import bigquery

def download_artifacts(gcs_dir_path):
    """Downloads model.joblib and scaler.joblib from a GCS directory."""
    print(f"--- Downloading artifacts from: {gcs_dir_path} ---")
    storage_client = storage.Client()
    bucket_name = gcs_dir_path.split('/')[2]
    prefix = '/'.join(gcs_dir_path.split('/')[3:])
    
    bucket = storage_client.bucket(bucket_name)
    model_blob = bucket.blob(os.path.join(prefix, 'model.joblib'))
    scaler_blob = bucket.blob(os.path.join(prefix, 'scaler.joblib'))

    # Use unique local names to avoid conflicts
    local_model_path = f"/tmp/{os.path.basename(prefix)}_model.joblib"
    local_scaler_path = f"/tmp/{os.path.basename(prefix)}_scaler.joblib"
    
    model_blob.download_to_filename(local_model_path)
    scaler_blob.download_to_filename(local_scaler_path)
    print(f"Downloaded model and scaler for {prefix}")
    
    return joblib.load(local_model_path), joblib.load(local_scaler_path)

def upload_json_to_gcs(data_dict, gcs_uri):
    """
    Serializes a Python dictionary to a JSON string and uploads it to a
    specified GCS location.

    Args:
        data_dict (dict): The Python dictionary to upload.
        gcs_uri (str): The full GCS path, including the bucket and object name.
                       (e.g., "gs://your-bucket/outputs/metrics.json")
    """
    # 1. Initialize the GCS client.
    #    It will automatically use the service account credentials of the
    #    Vertex AI job environment.
    storage_client = storage.Client()

    # 2. Parse the GCS URI to get the bucket name and the object (file) name.
    #    Example: "gs://my-bucket/my/folder/metrics.json"
    #    - bucket_name becomes "my-bucket"
    #    - destination_blob_name becomes "my/folder/metrics.json"
    if not gcs_uri.startswith("gs://"):
        raise ValueError("GCS URI must start with gs://")
    
    bucket_name = gcs_uri.split('/')[2]
    destination_blob_name = '/'.join(gcs_uri.split('/')[3:])

    # 3. Get the target bucket and create a blob (file) reference.
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # 4. Convert the Python dictionary to a JSON formatted string.
    #    Using indent=2 makes the resulting JSON file human-readable.
    json_data = json.dumps(data_dict, indent=2)

    # 5. Upload the JSON string to the blob.
    #    Specifying the content_type is a best practice.
    blob.upload_from_string(
        data=json_data,
        content_type='application/json'
    )
    
    print(f"✅ Successfully uploaded JSON results to {gcs_uri}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    
    # --- Input Arguments ---
    parser.add_argument('--new-model-dir', required=True, type=str, help="GCS path to the new 'challenger' model.")
    parser.add_argument('--old-model-dir', required=True, type=str, help="GCS path to the old 'champion' model.")
    parser.add_argument('--project-id', required=True, type=str)
    parser.add_argument('--bq-eval-table', required=True, type=str)
    parser.add_argument('--metrics-output-gcs-path', required=True, type=str)
    parser.add_argument('--target-column', type=str, default='target')
    parser.add_argument('--r2-improvement-threshold', type=float, default=0.01)

    args = parser.parse_args()

    # --- 1. Load both models ---
    new_model, new_scaler = download_artifacts(args.new_model_dir)
    old_model, old_scaler = download_artifacts(args.old_model_dir)

    # --- 2. Load Evaluation Data from BigQuery ---
    print(f"--- Loading evaluation data from BigQuery table: {args.bq_eval_table} ---")
    client = bigquery.Client(project=args.project_id)
    query = f"SELECT * FROM `{args.bq_eval_table}`"
    eval_df = client.query(query).to_dataframe()

    feature_cols = ['fts_1', 'fts_2', 'fts_3', 'fts_4', 'fts_5', 'fts_6']
    X_eval = eval_df[feature_cols].values
    y_eval_true = eval_df[args.target_column].values

    # --- 3. Make predictions with BOTH models ---
    print("--- Generating predictions... ---")
    X_eval_scaled_new = new_scaler.transform(X_eval)
    y_pred_new = new_model.predict(X_eval_scaled_new)

    X_eval_scaled_old = old_scaler.transform(X_eval)
    y_pred_old = old_model.predict(X_eval_scaled_old)

    # --- 4. Calculate metrics for BOTH models ---
    r2_new = r2_score(y_eval_true, y_pred_new)
    rmse_new = np.sqrt(mean_squared_error(y_eval_true, y_pred_new))
    
    r2_old = r2_score(y_eval_true, y_pred_old)
    rmse_old = np.sqrt(mean_squared_error(y_eval_true, y_pred_old))

    # --- 5. The Decision Logic ---
    is_new_model_better = r2_new > (r2_old + args.r2_improvement_threshold)
    promotion_flag = 1 if is_new_model_better else 0
    print("\n--- Comparison Results ---")
    print(f"New Model R2: {r2_new:.4f}, RMSE: {rmse_new:.4f}")
    print(f"Old Model R2: {r2_old:.4f}, RMSE: {rmse_old:.4f}")
    print(f"Is new model better (with threshold {args.r2_improvement_threshold})? -> {is_new_model_better}")

    # --- 6. Construct and Upload the Final Output JSON ---
 

    #upload_json_to_gcs(final_output, args.metrics_output_gcs_path)
    final_output = {
            "promotion_flag": promotion_flag,
            "decision_details": {
               # "decision_made": is_new_model_better,
                "r2_improvement_threshold": args.r2_improvement_threshold
            },
            "new_model_metrics": {"r2_score": r2_new, "rmse": rmse_new},
            "old_model_metrics": {"r2_score": r2_old, "rmse": rmse_old},
            "metadata": {
                "new_model_path": args.new_model_dir,
                "old_model_path": args.old_model_dir,
                "evaluation_data_source": f"bq://{args.bq_eval_table}"
            }
        }

        # === Call the helper function to perform the upload ===
        # args.metrics_output_gcs_path is the GCS URI passed in from the workflow
    upload_json_to_gcs(final_output, args.metrics_output_gcs_path)
    print("\n--- Validation and comparison job complete. ---")