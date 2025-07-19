# app.py

import pickle
import numpy as np
import os
import json
import time
from flask import Flask, request, jsonify
import mysql.connector
from mysql.connector import Error

# 1. Initialize the Flask application
app = Flask(__name__)

# --- Database Connection Details from Environment Variables ---
DB_HOST = os.environ.get('DB_HOST')
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')

# --- Load the Model ---
# This part remains the same
try:
    with open('decision_tree_clf.pkl', 'rb') as f:
        model = pickle.load(f)
except FileNotFoundError:
    model = None
    app.logger.error("Error: decision_tree_clf.pkl not found.")

def get_db_connection():
    """Establishes a connection to the database."""
    conn = None
    # Retry connection to give the DB container time to start
    for i in range(5): # Retry 5 times
        try:
            conn = mysql.connector.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            if conn.is_connected():
                return conn
        except Error as e:
            app.logger.error(f"DB connection attempt {i+1} failed: {e}")
            time.sleep(5) # Wait 5 seconds before retrying
    return None

# --- Prediction Endpoint ---
@app.route('/predict', methods=['POST'])
def predict():
    if model is None:
        return jsonify({'error': 'Model not loaded'}), 500

    json_data = request.get_json()
    if 'features' not in json_data:
        return jsonify({'error': 'Missing "features" key in JSON payload'}), 400

    features = np.array(json_data['features']).reshape(1, -1)
    prediction = model.predict(features)
    prediction_result = prediction.tolist()

    # --- Store the result in the database ---
    conn = None
    try:
        conn = get_db_connection()
        if conn is None or not conn.is_connected():
            raise Error("Could not connect to the database after several retries.")

        cursor = conn.cursor()
        # Use placeholders (%s) to prevent SQL injection!
        query = "INSERT INTO predictions (features, prediction) VALUES (%s, %s)"
        # Convert lists to JSON strings for storage
        data_to_store = (json.dumps(json_data['features']), json.dumps(prediction_result))
        
        cursor.execute(query, data_to_store)
        conn.commit()
        app.logger.info("Prediction stored successfully in the database.")

    except Error as e:
        app.logger.error(f"Error storing prediction in database: {e}")
        # Optionally, you could still return the prediction even if DB fails
        # return jsonify({'prediction': prediction_result, 'db_status': 'failed'})
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()
    
    return jsonify({'prediction': prediction_result})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)