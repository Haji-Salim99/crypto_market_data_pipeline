# Crypto Market Data Pipeline (ETL Project)

## Overview

This project is an end-to-end Data Engineering pipeline that extracts cryptocurrency market data from a public API, transforms it into a clean and structured format, and loads it into a PostgreSQL database for analysis.

The pipeline is designed to simulate a real-world data workflow, including raw data ingestion, transformation, and incremental loading into a database.

## Objective

The goal of this project is to build a production-style ETL pipeline that:

- Extracts real-time cryptocurrency data from an external API
- Cleans and transforms the data using Python (pandas)
- Stores the data in PostgreSQL
- Supports incremental loading (append-only design)
- Tracks historical data using timestamps

## Pipeline Architecture

The pipeline follows a layered architecture:

API → Raw JSON → Transformation (pandas) → PostgreSQL

1. Extraction Layer:
   - Fetches crypto market data from CoinGecko API
   - Saves raw JSON files with timestamps

2. Transformation Layer:
   - Converts JSON data into pandas DataFrame
   - Selects relevant columns
   - Cleans data types
   - Adds derived features

3. Load Layer:
   - Loads processed data into PostgreSQL
   - Uses incremental loading (append mode)
   - Preserves historical snapshots

   ## Technologies Used So Far

- Python
- pandas
- requests (API calls)
- PostgreSQL
- SQLAlchemy
- python-dotenv
- logging
- pathlib

## Technologies Used

- Python
- pandas
- requests (API calls)
- PostgreSQL
- SQLAlchemy
- python-dotenv
- logging
- pathlib

## Project Structure

project2_crypto_pipeline/
│
├── data/
│   ├── raw/              # Raw JSON data from API
│   └── processed/        # Cleaned CSV data
│
├── scripts/
│   ├── extract_api_data.py
│   ├── transform_crypto_data.py
│   ├── load_to_postgres.py
│
├── logs/                 # Log files for each stage
│
├── .env                  # Environment variables
├── requirements.txt
├── README.md

## Features Implemented So Far

- API data extraction (CoinGecko)
- Raw data storage (JSON files)
- Data transformation with pandas
- Feature engineering:
  - price_range_24h
  - market_cap_category
  - snapshot_timestamp
- Logging (file + console)
- Path handling using pathlib
- Environment configuration using .env
- Incremental data loading (append mode)
- Automatic table creation in PostgreSQL

## Incremental Loading

This pipeline uses an append-only strategy when loading data into PostgreSQL.

Instead of overwriting existing data, each run adds new records with a timestamp:

- Preserves historical data
- Enables time-series analysis
- Reflects real-world data engineering practices

Each record includes a snapshot_timestamp to track when the data was collected.

## How to Run

1. Install dependencies:

pip install -r requirements.txt

2. Set up environment variables in `.env`

3. Run extraction:

python scripts/extract_api_data.py

4. Run transformation:

python scripts/transform_crypto_data.py

5. Load into PostgreSQL:

python scripts/load_to_postgres.py

## Example SQL Queries

Check total records:

SELECT COUNT(*) FROM crypto_market_data;

View recent data:

SELECT * FROM crypto_market_data
ORDER BY snapshot_timestamp DESC
LIMIT 10;

Check incremental loading:

SELECT snapshot_timestamp, COUNT(*)
FROM crypto_market_data
GROUP BY snapshot_timestamp
ORDER BY snapshot_timestamp;

## Future Improvements

- Add Airflow for orchestration
- Dockerize the pipeline
- Add data quality checks
- Build dashboards (Power BI / Streamlit)
- Implement partitioning for large datasets
