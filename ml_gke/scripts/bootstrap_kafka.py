#!/usr/bin/env python3
"""
Bootstrap Kafka Resources for Credit Limit Inference.
This script creates the required topic directly using the AdminClient.
"""
import sys
import os
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaError

def bootstrap_kafka():
    # 1. Configuration source: confluent.conf and env variables
    conf_file = "kafka_secrets/confluent.conf"
    local_conf = {}
    if os.path.exists(conf_file):
        with open(conf_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    local_conf[key.strip()] = value.strip()

    # Priority: Env Var > confluent.conf
    api_key = os.getenv("CONFLUENT_API_KEY", local_conf.get("sasl.username"))
    api_secret = os.getenv("CONFLUENT_API_SECRET", local_conf.get("sasl.password"))
    bootstrap_servers = local_conf.get("bootstrap.servers")

    if not api_key or not bootstrap_servers:
        print(f"Error: Missing credentials. Ensure {conf_file} exists or set CONFLUENT_API_KEY environment variable.")
        sys.exit(1)

    # Initialize AdminClient using Kafka protocol (Port 9092)
    kafka_conf = {
        'bootstrap.servers': bootstrap_servers,
        'sasl.username': api_key,
        'sasl.password': api_secret,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN'
    }
    
    admin_client = AdminClient(kafka_conf)

    topic_name = "credit-applications"
    num_partitions = 3
    replication_factor = 3

    new_topics = [NewTopic(topic_name, num_partitions=num_partitions, replication_factor=replication_factor)]

    print(f"--- Bootstrapping Kafka Topic: {topic_name} ---")
    fs = admin_client.create_topics(new_topics)

    # Wait for each operation to finish
    for topic, f in fs.items():
        try:
            f.result()
            print(f"Topic '{topic}' created successfully.")
        except Exception as e:
            if "exists" in str(e).lower():
                print(f"Topic '{topic}' already exists. Skipping.")
            else:
                print(f"Failed to create topic '{topic}': {e}")
                sys.exit(1)

    print("\n--- Next Steps ---")
    print("1. The topic is ready.")
    print("2. IMPORTANT: You still need to MANUALLY enable the Datagen Connector in Confluent Portal.")
    print("   Why? Confluent Cloud Managed Connectors are usually managed via REST API or UI, not Kafka protocol.")
    print(f"   Use the schema at: kafka_secrets/inference_schema.json")

if __name__ == "__main__":
    bootstrap_kafka()
