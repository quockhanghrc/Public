# app.py

import pickle
import numpy as np
from flask import Flask, request, jsonify
from sklearn.datasets import load_iris

# 1. Initialize the Flask application
app = Flask(__name__)

# 2. Load the trained model from the pickle file
# The model is loaded only once when the app starts
try:
    with open('decision_tree_clf.pkl', 'rb') as f:
        model = pickle.load(f)
except FileNotFoundError:
    print("Error: model.pkl not found. Make sure the model file is in the same directory.")
    model = None # Set to None to handle the error gracefully

# 3. Define the prediction endpoint
## Enrich the endpoint with classification name 
@app.route('/predict', methods=['POST'])
def predict():
    if model is None:
        return jsonify({'error': 'Model not loaded'}), 500

    json_data = request.get_json()
    if not json_data or 'features' not in json_data:
        return jsonify({'error': 'Missing or invalid JSON payload'}), 400
    target_name = load_iris().target_names
    try:
        features = np.array(json_data['features']).reshape(1, -1)
        prediction = model.predict(features)
        return jsonify({
    'prediction': prediction.tolist(),
    'features': json_data['features'],
    'class_name': target_name[prediction][0],
    'model_version': 'Decision Tree Classifier v1.0'
        })


    # CATCH THE POTENTIAL ERRORS!
    except ValueError as e:
        # Catches errors from reshape and model.predict feature count mismatch
        return jsonify({'error': f'Invalid input data: {e}'}), 400
    except Exception as e:
        # Catch any other unexpected errors
        # You can log the full error for debugging
        app.logger.error(f"Prediction failed with error: {e}")
        return jsonify({'error': 'An internal error occurred during prediction.'}), 500

    # 4. Get the prediction from the model
    prediction = model.predict(features)

    # 5. Return the prediction as a JSON response
    # Convert numpy array to a list for JSON serialization
    return jsonify({
    'prediction': prediction.tolist(),
    'features': json_data['features']
        })

# 6. Run the app
if __name__ == '__main__':
    # The host '0.0.0.0' makes the app accessible from outside the container
    app.run(host='0.0.0.0', port=5000)