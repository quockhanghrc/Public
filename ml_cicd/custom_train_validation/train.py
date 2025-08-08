import os
import argparse
import joblib
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.neural_network import MLPRegressor
from sklearn.metrics import r2_score
from google.cloud import storage
from google.cloud import bigquery
# =============================================================================
# Helper Functions
# =============================================================================


def upload_to_gcs(local_path, gcs_uri):
    """Uploads a local file to a GCS bucket."""
    # Example gcs_uri: "gs://your-bucket/models/job123/model.joblib"
    # The client will automatically use the container's service account.
    storage_client = storage.Client()
    bucket_name = gcs_uri.split('/')[2]
    destination_blob_name = '/'.join(gcs_uri.split('/')[3:])
    
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(local_path)
    print(f"✅ Uploaded {local_path} to {gcs_uri}")

# =============================================================================
# Main Training Script
# =============================================================================

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    # --- Input Data and Hyperparameters ---
    parser.add_argument('--hidden-layer-sizes', type=str, default='64,32', help='Comma-separated integers for layer sizes')
    parser.add_argument('--max-iter', type=int, default=500)
    
    # --- Output Location ---
    parser.add_argument(
        '--model-dir',
        type=str,
        required=True,
        help='GCS directory to save the trained model and scaler, e.g., gs://bucket/output/job123'
    )
    
    args = parser.parse_args()
    hidden_layers = tuple(int(x.strip()) for x in args.hidden_layer_sizes.split(';'))

    query ='''
    select fts_1,fts_2,fts_3,fts_4,fts_5,fts_6,target 
    from ml_hourly.training_data

    '''
    client= bigquery.Client()
    df=client.query(query).to_dataframe()

    X_train_full = df[['fts_1', 'fts_2', 'fts_3', 'fts_4', 'fts_5', 'fts_6']].values
    y_train_full = df['target'].values

    # Split and scale
    X_train, X_test, y_train, y_test = train_test_split(X_train_full, y_train_full, test_size=0.2, random_state=42)
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    # Train the model using arguments
    print("Training MLPRegressor model...")
    model = MLPRegressor(
        hidden_layer_sizes=hidden_layers,
        activation='relu',
        solver='adam',
        max_iter=args.max_iter,
        early_stopping=True,
        n_iter_no_change=15,
        verbose=False,
        random_state=42
    )
    model.fit(X_train_scaled, y_train)

    # Evaluate on its own test set to confirm it learned well
    y_pred_initial = model.predict(X_test_scaled)
    r2_initial = r2_score(y_test, y_pred_initial)
    print(f"✅ Initial model performance on non-drifted test data (R2): {r2_initial:.4f}\n")

    # --- Save and Upload Artifacts ---
    # Save artifacts locally in the container first
    local_model_path = "model.joblib"
    local_scaler_path = "scaler.joblib"
    joblib.dump(model, local_model_path)
    joblib.dump(scaler, local_scaler_path)
    print(f"Model and scaler saved locally.")

    # Construct full GCS paths
    gcs_model_path = os.path.join(args.model_dir, 'model.joblib')
    gcs_scaler_path = os.path.join(args.model_dir, 'scaler.joblib')

    # Upload to the GCS directory specified by the workflow
    upload_to_gcs(local_model_path, gcs_model_path)
    upload_to_gcs(local_scaler_path, gcs_scaler_path)

    print("\n--- Training job complete. ---")