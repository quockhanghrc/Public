import functions_framework
from google.cloud import aiplatform
from datetime import datetime
import gcsfs
import json

# Purpose: This Cloud Function logs performance metrics from a JSON file stored in Google Cloud Storage to an Vertex AI Experiment.
# Its to monitor performance of model versions over time.


@functions_framework.http
def log_records(request):
    # Initialize your experiment settings.
    experiment_name = 'hourly-evaluate-experiment'
    aiplatform.init(experiment=experiment_name, location='asia-southeast2')

    # Create a unique run name based on the current timestamp.
    run_name = f"run-promote-{datetime.now().strftime('%Y%m%d-%H%M%S')}"

    # Parse the JSON request body to get the input JSON path.
    request_json = request.get_json(silent=True)
    if request_json and 'json_path' in request_json:
        json_path = request_json['json_path']
    else:
        return 'Invalid request: json_path is missing', 400

    # Create a Google Cloud Storage file system object.
    fs = gcsfs.GCSFileSystem()

    # Read the JSON file from the specified GCS path.
    with fs.open(json_path, 'r') as f_json:
        data = json.load(f_json)

    # Extract relevant metrics from the loaded JSON data.
    if 'new_model_metrics' in data: 
        new_model_metrics = data['new_model_metrics']
        
        # Extracting r2_score and rmse. Default to 0 if not found.
        metrics_to_log = {
            'r2': new_model_metrics.get('r2_score', 0),
            'rmse': new_model_metrics.get('rmse', 0)
        }
    else:
        return 'No new model metrics found in JSON.', 400

    # Start logging metrics in a new run.
    with aiplatform.start_run(run=run_name) as run:
        print(f"--- Started Experiment Run: {run_name} in Experiment: {experiment_name} ---")
        # Log extracted metrics to the run object.
        run.log_metrics(metrics_to_log)
        print(f"Logged performance parameters: {metrics_to_log}")
        run.end_run()

    return 'Done', 200