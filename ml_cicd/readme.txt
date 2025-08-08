The pipeline simulates an ML CI/CD workflow that:

- Periodically generates synthetic data exhibiting time-based drift to mimic real-world changes in data distribution.
- Continuously monitors model performance over time and initiates retraining when performance drops below a set threshold.
- Conducts retraining with validation by comparing the new model's performance against the previous version, promoting the new model only if it demonstrates improvement.
- Maintains detailed records of model performance metrics for ongoing analytics and monitoring.

Tools:
- Google Cloud Functions: to generate data, evaluate performance, record keeping
- Google Cloud Workflows: to maintain training / validation flow
- Vertex AI Custom Training: to maintain training / validation scripts
- Vertex AI Experiment: to store model performance
- Google CLoud Storage / Google BigQuery / Looker Studio: data storage, analytics, reporting
