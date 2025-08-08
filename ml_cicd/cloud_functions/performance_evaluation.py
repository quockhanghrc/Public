import base64
import functions_framework
import pandas
from google.cloud import bigquery
from datetime import datetime
import json
from datetime import datetime
from google.cloud import pubsub_v1

# Purpose: This module evaluates the performance of a regression model using synthetic data.
# It measures rmse, r2 and trigger retraining if the model performance degrades beyond a threshold.

def publish_message(topic_name,retrain=False):
    publisher = pubsub_v1.PublisherClient()
    bigquery_client = bigquery.Client()

    # Get data from the request
    current_datetime = datetime.utcnow()
    bigquery_datetime = current_datetime.strftime('%Y-%m-%d %H:%M:%S')

    message = {
        'event_datetime':bigquery_datetime,
        'message':'No need retrain' if not retrain else 'Need retrain',
        'retrain':0 if not retrain else 1,
    }

    # Publish the message

    if retrain:
        future = publisher.publish(topic_name, json.dumps(message).encode('utf-8'))
        future.result()  # Wait for the publish call to complete
    else:
        pass

        
    row_to_insert = {
        "event_datetime": message.get('event_datetime'),
        "message": message.get('message'),
        "retrain": message.get('retrain'),
    }

    BQ_TABLE_ID = 'log_trigger' 
    # Insert the row into BigQuery
    errors = bigquery_client.insert_rows_json(BQ_TABLE_ID, [row_to_insert])  # Make an API request.

def training_data_preparation(version):

    client = bigquery.Client()
    query=f'''
    DROP TABLE IF EXISTS training_data;


    CREATE TABLE ml_hourly.training_data as (
    SELECT * FROM prediction_label
    where batch="{version}"
    and timestamp_sub(current_timestamp(),interval 2 hour)<=timestamp(runtime) ## get recent data only
    );
    '''
    client.query(query).result()

@functions_framework.cloud_event
def hourly_evaluate(cloud_event):
    topic_name='_____'

    client = bigquery.Client()
    query='''
    select  r2 as r2,max_r2 as ref_r2, rmse as rmse, min_rmse as ref_rmse

    from metrics_monitoring
    where version="synthetic_v1"
    QUALIFY ROW_NUMBER() OVER(PARTITION BY VERSION ORDER BY RUNTIME DESC)=1

    '''
    df = client.query(query).to_dataframe()  # Fetch results directly into a DataFrame

    if df['ref_r2'].iloc[0]>df['r2'].iloc[0]*1.05 or df['rmse'].iloc[0]>df['ref_rmse'].iloc[0]*1.2:
        publish_message(topic_name,retrain=True)
        training_data_preparation('synthetic_v1')
    else:
        publish_message(topic_name,retrain=False)