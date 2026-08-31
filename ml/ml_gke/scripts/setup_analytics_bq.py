from google.cloud import bigquery
from google.api_core.exceptions import Conflict
import os

PROJECT_ID = os.getenv("PROJECT_ID", "YOUR_PROJECT_ID")
DATASET_ID = "inference_analytics"
TABLE_ID = "prediction_logs"

client = bigquery.Client(project=PROJECT_ID)

def setup_analytics_bq():
    # 1. Create Dataset
    dataset_ref = client.dataset(DATASET_ID)
    try:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "asia-southeast2"
        dataset = client.create_dataset(dataset)
        print(f"Dataset {DATASET_ID} created.")
    except Conflict:
        print(f"Dataset {DATASET_ID} already exists.")

    # 2. Define Schema
    schema = [
        bigquery.SchemaField("prediction_id", "STRING", mode="REQUIRED", description="Unique ID for each prediction"),
        bigquery.SchemaField("trace_id", "STRING", mode="NULLABLE", description="Distributed Trace ID (hex)"),
        bigquery.SchemaField("span_id", "STRING", mode="NULLABLE", description="Unique Span ID (hex)"),
        bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED", description="Time of prediction"),
        bigquery.SchemaField("input_payload", "JSON", mode="NULLABLE", description="Full input features from Kafka"),
        bigquery.SchemaField("prediction_result", "JSON", mode="NULLABLE", description="Full output from Model"),
        bigquery.SchemaField("credit_limit", "FLOAT", mode="NULLABLE", description="Extracted predicted limit"),
        bigquery.SchemaField("model_version", "STRING", mode="NULLABLE", description="Model version used"),
        bigquery.SchemaField("kafka_topic", "STRING", mode="NULLABLE", description="Source Kafka topic"),
        bigquery.SchemaField("latency_ms", "FLOAT", mode="NULLABLE", description="Inference latency in milliseconds")
    ]

    # 3. Create Table
    table_ref = dataset_ref.table(TABLE_ID)
    table = bigquery.Table(table_ref, schema=schema)
    
    # Enable Time Partitioning (best for analytics logs)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="timestamp"
    )

    try:
        table = client.create_table(table)
        print(f"Table {TABLE_ID} created with partitioning.")
    except Conflict:
        print(f"Table {TABLE_ID} already exists.")

if __name__ == "__main__":
    setup_analytics_bq()
