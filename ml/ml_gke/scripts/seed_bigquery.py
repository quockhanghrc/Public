import pandas as pd
import numpy as np
from google.cloud import bigquery
import argparse
import time
from datetime import datetime, timedelta

def generate_synthetic_data(num_records, split_name):
    print(f"Generating {num_records} records for {split_name} split...")
    
    np.random.seed(42 if split_name == "train" else 123)
    
    # Generate realistic features
    data = {
        "user_id": [f"USR-{split_name}-{i:07d}" for i in range(num_records)],
        "income": np.random.normal(60000, 25000, num_records).clip(20000, 500000),
        "credit_score": np.random.randint(300, 850, num_records),
        "total_accounts": np.random.randint(1, 20, num_records),
        "late_payments_last_year": np.random.poisson(0.5, num_records).clip(0, 10),
    }
    
    # Derivative feature: existing debt
    data["existing_debt"] = (data["income"] * np.random.uniform(0.1, 0.6, num_records)).clip(0)
    
    # Target variable: target_limit (what we want to predict)
    # Simple linear relationship with some noise for regression task
    data["target_limit"] = (
        (data["income"] * 0.2) + 
        (data["credit_score"] * 10) - 
        (data["existing_debt"] * 0.1) - 
        (data["late_payments_last_year"] * 500) + 
        np.random.normal(0, 2000, num_records)
    ).clip(1000, 100000)
    
    data["created_at"] = [datetime.now() - timedelta(days=np.random.randint(0, 30)) for _ in range(num_records)]
    data["split"] = split_name
    
    return pd.DataFrame(data)

def seed_bigquery(project_id):
    client = bigquery.Client(project=project_id)
    
    # 4M records total: 3.2M train, 0.8M val
    splits = [("train", 3200000), ("val", 800000)]
    
    # Dataset/Table Definitions
    INTERNAL_TABLE = f"{project_id}.internal_app.applicant_profiles"
    EXTERNAL_TABLE = f"{project_id}.external_bureau.credit_history"

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

    for split_name, num_records in splits:
        df = generate_synthetic_data(num_records, split_name)
        
        # Internal table: all columns
        internal_df = df[["user_id", "income", "existing_debt", "target_limit", "created_at", "split"]]
        internal_df.columns = ["app_user_id", "annual_income", "current_debt", "approved_limit", "application_date", "data_split"]
        
        print(f"Loading {len(internal_df)} rows into {INTERNAL_TABLE}...")
        job = client.load_table_from_dataframe(internal_df, INTERNAL_TABLE, job_config=job_config)
        job.result()
        print(f"  ✅ Loaded {job.output_rows} rows into internal_app.applicant_profiles")
        
        # External table: subset of columns
        external_df = df[["user_id", "credit_score", "total_accounts", "late_payments_last_year"]]
        external_df.columns = ["bureau_user_id", "fico_score", "active_credit_lines", "past_due_incidents"]
        
        print(f"Loading {len(external_df)} rows into {EXTERNAL_TABLE}...")
        job = client.load_table_from_dataframe(external_df, EXTERNAL_TABLE, job_config=job_config)
        job.result()
        print(f"  ✅ Loaded {job.output_rows} rows into external_bureau.credit_history")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed BigQuery with synthetic credit data")
    parser.add_argument("--project_id", required=True, help="GCP Project ID")
    args = parser.parse_args()
    
    seed_bigquery(args.project_id)
