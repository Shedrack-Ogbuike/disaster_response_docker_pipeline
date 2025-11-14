# Base image
FROM python:3.11-slim

# Set working directory inside the container
WORKDIR /usr/src/etl_app

# Install dependencies
RUN apt-get update && apt-get install -y gcc libpq-dev && rm -rf /var/lib/apt/lists/*

# Copy Python dependencies and install them
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the ETL script into the working directory
COPY etl_app/etl_pipeline.py .

# Default command (can be overridden in docker-compose)
CMD ["python", "etl_pipeline.py"]
