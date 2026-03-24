# Market Data Pipeline

ETL pipeline for fetching, processing and storing daily stock market data.

## Overview

Pipeline fetches daily stock market data from Alpha Vantage API, processes it using Polars and stores it in a DuckDB database. Orchestrated with Prefect.

## Tech Stack

- Python 3.x
- httpx
- Polars
- DuckDB
- Prefect

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/justgodtears/market-data-pipeline.git
   
   cd market-data-pipeline
   ```
2. Install dependencies:
   ```
   uv pip install -r requirements.txt
   ```
3. Create `.env` file with your Alpha Vantage API key:
   ```
   AV_API_KEY=your_api_key_here
   ```

## Usage

Run the pipeline:
```
python main.py
```

## Pipeline Steps

1. Fetch daily market data from Alpha Vantage API
2. Clean and parse raw JSON response
3. Transform data using Polars DataFrame
4. Store results in DuckDB

## Notes

- Free Alpha Vantage API plan is limited to 25 requests per day
- Pipeline is designed to run daily on business days