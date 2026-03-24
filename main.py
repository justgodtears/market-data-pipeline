import httpx
import os
from dotenv import load_dotenv
import polars as pl
import json
import duckdb
from prefect import task, flow

load_dotenv()

API_KEY = os.getenv("AV_API_KEY")
ENDPOINT = "https://www.alphavantage.co/query"

@task
def fetch_market_data(endpoint, symbol, api_key):
    """
    Function to fetch market data from Alpha Vantage API
    :param endpoint: Alpha Vantage API endpoint
    :param symbol: Market symbol of company
    :param api_key: User API Key
    :return: Raw response from Alpha Vantage API
    """
    parameters = {"function":"TIME_SERIES_DAILY", "symbol":symbol, "apikey":api_key}
    response = httpx.get(endpoint, params=parameters)
    return response.json()

@task
def json_cleaning(data):
    """
    Cleaning raw data from raw JSON file
    :param data: Raw response from Alpha Vantage API
    :return: List of dictionaries in correct format
    """
    day_data = []
    time_series = data.get("Time Series (Daily)")
    for date,values in time_series.items():
        row = {"date": date}
        for key, val in values.items():
            clean_key = key.split(". ")[1]
            row[clean_key] = val
        day_data.append(row)
    return day_data

@task
def data_processing(data,symbol):
    """
    Processing data using Polars dataframe
    :param data:  Cleaned json data
    :param symbol: Company symbol
    :return: Dataframe with processed data
    """
    df = pl.DataFrame(data)
    df = df.with_columns(pl.lit(symbol).alias("symbol"))
    result = df.select(
        pl.col("date").cast(pl.Date),
        pl.col("symbol").cast(pl.String),
        pl.col("open").cast(pl.Float64),
        pl.col("high").cast(pl.Float64),
        pl.col("low").cast(pl.Float64),
        pl.col("close").cast(pl.Float64),
        pl.col("volume").cast(pl.Int64)
    )
    return result

@task
def save_to_duckdb(df, db_path):
   with duckdb.connect(db_path) as con:
       con.sql("""CREATE TABLE IF NOT EXISTS market_data(
           date DATE ,
           symbol VARCHAR,
           open FLOAT,
           high FLOAT,
           low FLOAT,
           close FLOAT,
           volume INT,
           PRIMARY KEY (date, symbol)
                  )
       """)
       con.sql("INSERT OR IGNORE INTO market_data SELECT * FROM df")

@flow
def main():
    load_dotenv()
    api_key = os.getenv("AV_API_KEY")
    endpoint = "https://www.alphavantage.co/query"
    api_data = fetch_market_data(endpoint, "PLTR", api_key)
    cleaned_data = json_cleaning(api_data)
    proc_data = data_processing(cleaned_data, "PLTR")
    save_to_duckdb(proc_data, "mde_data.db")


if __name__ == "__main__":
    main()