# 📊 Delta Lake Data Pipeline with Airflow, PySpark and MinIO

End-to-end data engineering pipeline that ingests, processes, and models U.S. economic indicators and stock market data using a lakehouse architecture.

Built with Apache Airflow, PySpark, Delta Lake, and MinIO (S3-compatible storage), the project follows a medallion architecture (bronze, silver, gold) to deliver analytics-ready datasets.

## 📌 Overview

This project implements a complete data pipeline capable of:

- Ingesting economic and financial data from an external API
- Processing and cleaning data with PySpark
- Storing data in a Delta Lake format
- Orchestrating workflows using Airflow
- Serving curated datasets for analytical consumption

## ⚙ Architecture

The pipeline follows the Medallion Architecture:

`Bronze (Raw) → Silver (Cleaned) → Gold (Curated)`

- **Bronze Layer**
  - Raw ingestion from APIs
  - Minimal transformation
  - Data stored as-is for traceability
- **Silver Layer**
  - Data cleaning and normalization
  - Schema enforcement
  - Incremental processing
- **Gold Layer**
  - Dimensional modeling (Facts & Dimensions)
  - Analytics-ready datasets
  - Optimized for BI tools (e.g., Dremio, Metabase)

## 🔧 Tech Stack
- **Orchestration**: Apache Airflow
- **Processing**: PySpark
- **Storage**: MinIO (S3-compatible object storage)
- **Table Format**: Delta Lake
- **Containerization**: Docker & Docker Compose
- **Query Layer** (optional): Dremio
- **Visualization** (optional): Metabase

## 💿 Data Sources

The pipeline processes:

- **U.S. economic indicators**:
  - Inflation
  - Federal Funds Rate
- **Stock market data**:
  - Daily time series
  - Company overview
  - Simple Moving Average

## 🧪 Setup
1. Clone the repository
```
git clone https://github.com/Rafael-Rech/stocks-deltalake.git
cd stocks-deltalake
```
2. Configure environment variables
```
cp .env.example .env
```

Edit the .env file:

```
AIRFLOW_UID=50000

ACCESS_KEY=your_access_key
SECRET_KEY=your_secret_key

API_KEY=your_api_key

PROJECT_PATH=/your/project/path
```
3. Start the environment
```
docker compose up --build -d
```
4. Access services
   - **Airflow UI** → http://localhost:8081
   - **MinIO AIStor UI** → http://localhost:9001

## 📜 License

This project is licensed under the MIT License.

## ❕ Notes

This project is intended for learning and portfolio purposes, focusing on real-world data engineering practices and architecture patterns.