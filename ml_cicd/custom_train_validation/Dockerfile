# Dockerfile

# Step 1: Use an official Python runtime as a parent image.
# Using a specific version like 3.9 is better for reproducibility than 'latest'.
# The '-slim' variant is smaller, which is great for MLOps.
FROM python:3.9-slim

# Step 2: Set the working directory inside the container.
# This is where your code will live and run.
WORKDIR /app

# Step 3: Copy the dependencies file FIRST.
# This is a key optimization. Docker layers are cached. If requirements.txt doesn't
# change, Docker won't re-run the time-consuming pip install step on future builds.
COPY requirements.txt .

# Step 4: Install the Python dependencies.
# --no-cache-dir keeps the image size smaller.
RUN pip install --no-cache-dir -r requirements.txt

# Step 5: Copy the rest of your application's source code into the container.
# The '.' means copy from the current directory on your machine to the WORKDIR in the container.
COPY train.py .
COPY validate.py .

# Step 6: Define the command to run when the container starts.
# Vertex AI will execute this command and append any 'args' from the workflow YAML.
# e.g., it becomes `python train.py --data-gcs-path ... --model-dir ...`
ENTRYPOINT ["python", "train.py"]