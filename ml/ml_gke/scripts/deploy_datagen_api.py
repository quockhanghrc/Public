import json
import os
import requests
import sys

def read_confluent_config(config_file):
    """Parses the confluent.conf into a dict."""
    conf = {}
    if not os.path.exists(config_file):
        return None
    with open(config_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                conf[key.strip()] = value.strip()
    return conf

def deploy_datagen_api():
    # 1. Configuration source: confluent.conf
    conf_file = os.path.join("kafka_secrets", "confluent.conf")
    local_conf = read_confluent_config(conf_file)
    
    # Priority: Env Var > confluent.conf > Default
    api_key = os.getenv("CONFLUENT_API_KEY", local_conf.get("sasl.username") if local_conf else "YOUR_CONFLUENT_API_KEY")
    api_secret = os.getenv("CONFLUENT_API_SECRET", local_conf.get("sasl.password") if local_conf else "YOUR_CONFLUENT_API_SECRET")
    
    env_id = os.getenv("CONFLUENT_ENV_ID", "YOUR_ENV_ID")
    cluster_id = os.getenv("CONFLUENT_CLUSTER_ID", "YOUR_CLUSTER_ID")
    
    connector_name = "datagen-inference-v1"
    topic_name = "credit-applications"
    schema_file = os.path.join("kafka_secrets", "inference_schema.json")

    if not os.path.exists(schema_file):
        print(f"Error: {schema_file} not found.")
        sys.exit(1)

    # 2. Prepare Minified Schema
    with open(schema_file, 'r') as f:
        minified_schema = json.dumps(json.load(f))

    # 3. Connector Config Payload
    payload = {
        "name": connector_name,
        "config": {
            "connector.class": "DatagenSource",
            "kafka.auth.mode": "KAFKA_API_KEY",
            "kafka.api.key": api_key,
            "kafka.api.secret": api_secret,
            "kafka.topic": topic_name,
            "output.data.format": "JSON",
            "quickstart": "",
            "tasks.max": "1",
            "schema.string": minified_schema,
            "max.interval": "1000",
            "iterations": "-1"
        }
    }

    # 4. REST API Endpoint
    url = f"https://api.confluent.cloud/connect/v1/environments/{env_id}/clusters/{cluster_id}/connectors"

    print(f"--- Deploying Connector via REST API: {connector_name} ---")
    print(f"Using Cloud API Key: {api_key[:4]}...{api_key[-4:]}")
    
    response = requests.post(
        url,
        auth=(api_key, api_secret),
        json=payload,
        timeout=10
    )

    if response.status_code in [200, 201]:
        print(f"✅ Connector {connector_name} deployed successfully via API.")
    else:
        print(f"❌ Failed to deploy connector: {response.status_code} {response.text}")
        sys.exit(1)

if __name__ == "__main__":
    deploy_datagen_api()
