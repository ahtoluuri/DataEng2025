# Start from the official Airflow image
FROM apache/airflow:2.8.1

# Switch to root to avoid permission issues during build, then switch back
USER root
RUN apt-get update && apt-get install -y --no-install-recommends git 

USER airflow

# Copy and install Python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt