import base64
import functions_framework
import requests
import json
import numpy as np
import pandas as pd
import time  # Import time for adding delays
import joblib
import gcsfs
from sklearn.metrics import mean_squared_error, r2_score

# Purpose: This module generates synthetic data points for regression tasks, simulating a dynamic target function with drifted coefficients.
# It use unix timestamps to simulate a change in production over time, and it generates a specified number of data points with noise.


def dynamic_true_function(features, coeffs, noise_level=0.0):
    """Calculates the target value based on dynamic coefficients."""
    # np.dot is a clean way to do: c1*v1 + c2*v2 + ... + intercept
    base_value = np.dot(features, coeffs[:-1]) + coeffs[-1]
    if noise_level > 0:
        noise = np.random.normal(0, noise_level, size=base_value.shape)
        return base_value + noise
    return base_value

def generate_one_datapoint():
    """Generates features for one new data point."""
    x = np.random.uniform(-3, 3)
    y = np.random.uniform(-4, 4)
    a = np.random.normal(0, 2)
    b = np.random.normal(5, 1.5)
    c = np.random.uniform(0, 10)
    d = np.random.normal(-2, 1)
    e = np.random.uniform(-1, 1)

    # Return a dictionary that matches the expected input format
    return {
        "v1_x_pow_4": x**4,
        "v2_y_pow_3": y**3,
        "v3_a_sq_b": a**2 * b,
        "v4_c_sq": c**2,
        "v5_d": d,
        "v6_e": e
    }
# --- Lazy Loading Setup ---
# Declare global variables, initialized to None. They will be loaded only once.
# model = None
# scaler = None

def load_artifacts_from_gcs():
    """Loads and returns model and scaler from GCS on every call."""
    model = None
    scaler = None
    try:
        print("Attempting to load artifacts from GCS for this invocation...")
        fs = gcsfs.GCSFileSystem()
        model_path = "model.joblib"
        scaler_path = "scaler.joblib"

        with fs.open(model_path, 'rb') as f_model:
            model = joblib.load(f_model)
        with fs.open(scaler_path, 'rb') as f_scaler:
            scaler = joblib.load(f_scaler)
        print("✅ Artifacts loaded successfully.")
    except Exception as e:
        print(f"!!! Error loading artifacts: {e}")

    # --- THE CRITICAL FIX ---
    # Return the loaded objects (or None if loading failed)
    return model, scaler

def calculate_regression_metrics(y_true, y_pred):
    """Calculates RMSE and R-squared."""
    mse = mean_squared_error(y_true, y_pred)
    rmse = np.sqrt(mse)
    r2 = r2_score(y_true, y_pred)
    return rmse, r2


# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def hello_pubsub(cloud_event):

    #Load artifacts 
    noise_level=25
    base_unix=1754302543
    cur_unix=pd.Timestamp.now().timestamp()
    model, scaler = load_artifacts_from_gcs()
    base_coeffs = np.array([1.5, -2.0, 1.0, 3.0, -4.0, 2.0, 5.0])
    # 2. Drifted coefficients for simulating a change in production
    drift_scale = 0.1 # The magnitude of the drift
    drift = np.random.normal(0, drift_scale, size=base_coeffs.shape)
    drifted_coeffs = base_coeffs + drift +(cur_unix-base_unix)/75000
    all_data = []  # List to hold all the generated data points
    random_number = str(np.random.randint(0, 9999999))

    for _ in range(np.random.randint(5000, 20000)):  # Generate 100 data points
        input_data_dict = generate_one_datapoint()
        features_for_truth_calc = np.array(list(input_data_dict.values()))
        ground_truth = dynamic_true_function(features_for_truth_calc, drifted_coeffs,noise_level)

        # Add the target and batch information
        input_data_dict['target'] = ground_truth
        input_data_dict['batch'] = 'synthetic_v1'
        input_data_dict['runtime'] = pd.Timestamp.now().isoformat()

        # Append the data point to the list
        all_data.append(input_data_dict)


    # Convert the list of dictionaries to a DataFrame
    fts_df = pd.DataFrame(all_data)
    fts_df.columns=['fts_1','fts_2','fts_3','fts_4','fts_5','fts_6','target','batch','runtime']

    # Save all data to a parquet file
    fts_df.to_parquet(f'dataset/baseline_data_{random_number}.parquet', 
                      index=False)
    

    feature_cols = ['fts_1','fts_2','fts_3','fts_4','fts_5','fts_6']
    X_new = fts_df[feature_cols].values
    y_true_new = fts_df['target'].values
    X_new_scaled = scaler.transform(X_new)
    y_pred_new = model.predict(X_new_scaled)
    fts_df['prediction'] = y_pred_new
    
    rmse, r2 = calculate_regression_metrics(y_true_new, y_pred_new)

    metrics_df=pd.DataFrame(data={'version':'synthetic_v1','runtime':pd.Timestamp.now().isoformat(),'r2':r2,'rmse':rmse,'observations':len(fts_df)},index=[0])

    fts_df.to_parquet(f'dataset_prediction/baseline_data_{random_number}.parquet', 
                      index=False)
    metrics_df.to_parquet(f'dataset_metrics/metrics_{random_number}.parquet', 
                      index=False)
