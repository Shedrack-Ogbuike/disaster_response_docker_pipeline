# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory inside the container
WORKDIR /usr/src/app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the ETL application code
COPY etl_app/etl_pipeline.py .

# Copy any other app files if needed (optional)
# COPY etl_app/ .   ← use this if you have other helper modules

# Default command to run the ETL job
CMD ["python", "etl_pipeline.py"]
