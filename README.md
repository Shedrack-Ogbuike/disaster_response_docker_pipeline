# disaster_response_docker_pipeline
Automated, Dockerized ETL pipeline for ingesting, transforming, and loading FEMA Disaster Data into a PostgreSQL database. Ensures data readiness for downstream analytics and real-time visualization ( Power BI) through Continuous Integration (GitHub Actions).
# 🚨 Disaster Response Analytics

![CI](https://github.com/yourusername/disaster-response-analytics/actions/workflows/ci.yml/badge.svg)
![Docker](https://img.shields.io/badge/Docker-Ready-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-green)
![Python](https://img.shields.io/badge/Python-3.11-yellow)

A real-time ETL pipeline for analyzing FEMA disaster response data with PostgreSQL and Docker.

## 📊 Features

- **Real-time Data Ingestion**: ETL pipeline from FEMA's Open API
- **PostgreSQL Database**: Efficient data storage and querying
- **Dockerized Setup**: Easy deployment with Docker Compose
- **pgAdmin Interface**: Web-based database management
- **Scalable Architecture**: Modular and extensible design
- **CI/CD Pipeline**: Automated testing with GitHub Actions

## 🛠️ Quick Start

```bash
# Clone and setup
git clone https://github.com/yourusername/disaster-response-analytics.git
cd disaster-response-analytics



# Start services
docker-compose up -d --build

# Access the application:
# - pgAdmin: http://localhost:5050 
# - PostgreSQL: localhost:
