import os
import json
import requests
import structlog
import uuid
from datetime import datetime
from confluent_kafka import Consumer
from google.cloud import bigquery

# -------------------------------------------------------------------------
# 1. Setup Logging & BigQuery Client
# -------------------------------------------------------------------------
logger = structlog.get_logger()
bq_client = bigquery.Client()

# -------------------------------------------------------------------------
# 2. Configuration & Secrets
# -------------------------------------------------------------------------
# These can be overridden by Environment Variables in GKE
PROJECT_ID = os.getenv("PROJECT_ID", "YOUR_PROJECT_ID")
BQ_DATASET = os.getenv("BQ_DATASET", "inference_analytics")
BQ_TABLE = os.getenv("BQ_TABLE", "prediction_logs")
TABLE_ID = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"

INFERENCE_URL = os.getenv("INFERENCE_URL", "http://credit-limit-inference.card-approval-training.svc.cluster.local/predict")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "credit-applications")
CONF_FILE = os.path.join(os.path.dirname(__file__), "..", "kafka_secrets", "confluent.conf")

def read_config(config_file):
    """Parses the confluent.conf into a dict."""
    conf = {}
    with open(config_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                key, value = line.split("=", 1)
                conf[key] = value
    return conf

# -------------------------------------------------------------------------
# 3. BigQuery Sink
# -------------------------------------------------------------------------
def log_to_bigquery(input_data, prediction_result, metadata):
    """Sinks inputs, results and metadata to BigQuery."""
    try:
        row = {
            "prediction_id": metadata.get("prediction_id"),
            "trace_id": metadata.get("trace_id"),
            "span_id": metadata.get("span_id"),
            "timestamp": datetime.utcnow().isoformat(),
            "input_payload": json.dumps(input_data),
            "prediction_result": json.dumps(prediction_result),
            "credit_limit": prediction_result.get("credit_limit"),
            "model_version": metadata.get("model_version", "unknown"),
            "kafka_topic": KAFKA_TOPIC,
            "latency_ms": metadata.get("latency_ms", 0)
        }
        
        errors = bq_client.insert_rows_json(TABLE_ID, [row])
        if errors:
            logger.error("bq_insert_errors", errors=errors)
        else:
            logger.debug("bq_insert_success", prediction_id=row["prediction_id"], trace_id=row["trace_id"])
            
    except Exception as e:
        logger.error("bq_sink_failed", error=str(e))

# -------------------------------------------------------------------------
# 4. Process Message
# -------------------------------------------------------------------------
def process_message(msg_value):
    """Forwards Kafka JSON to Inference API."""
    try:
        data = json.loads(msg_value)
        
        # 1. Generate Correlation IDs (Tracing)
        prediction_id = str(uuid.uuid4())
        trace_id = uuid.uuid4().hex
        span_id = uuid.uuid4().hex[:16]

        # 2. Call Inference API with Tracing Headers
        headers = {
            "X-Prediction-ID": prediction_id,
            "X-Trace-ID": trace_id,
            "X-Span-ID": span_id
        }
        
        start_time = datetime.utcnow()
        response = requests.post(INFERENCE_URL, json=data, headers=headers, timeout=5)
        latency = (datetime.utcnow() - start_time).total_seconds() * 1000
        
        if response.status_code == 200:
            result = response.json()
            logger.info("inference_success", 
                        prediction_id=prediction_id,
                        trace_id=trace_id,
                        credit_limit=result.get("credit_limit"))
            
            # 3. Send to BigQuery with Tracing Metadata
            metadata = {
                "prediction_id": prediction_id,
                "trace_id": trace_id,
                "span_id": span_id,
                "latency_ms": latency,
                "model_version": response.headers.get("X-Model-Version", "v1")
            }
            log_to_bigquery(data, result, metadata)
            
        elif response.status_code == 422:
            logger.warning("validation_error", detail=response.json(), data=data)
        else:
            logger.error("api_error", status_code=response.status_code, text=response.text)

    except json.JSONDecodeError:
        logger.error("parse_error", raw_msg=msg_value)
    except Exception as e:
        logger.error("unexpected_error", error=str(e))

# -------------------------------------------------------------------------
# 5. Main Consumer Loop
# -------------------------------------------------------------------------
def main():
    if not os.path.exists(CONF_FILE):
        logger.error("missing_config", path=CONF_FILE)
        return

    kafka_conf = read_config(CONF_FILE)
    kafka_conf.update({
        'group.id': 'inference-bridge-group',
        'auto.offset.reset': 'earliest'
    })

    consumer = Consumer(kafka_conf)
    consumer.subscribe([KAFKA_TOPIC])

    logger.info("bridge_started", topic=KAFKA_TOPIC, target=INFERENCE_URL)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("kafka_error", error=msg.error())
                continue

            process_message(msg.value().decode('utf-8'))

    except KeyboardInterrupt:
        logger.info("bridge_stopped")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
