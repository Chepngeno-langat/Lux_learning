from airflow.decorators import dag, task
from datetime import datetime
import requests
import pandas as pd
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine

load_dotenv()

API_KEY = os.getenv("API_KEY")

POSTGRES_CONFIG = {
        "host": os.getenv("AIVEN_HOST"),
        "port": os.getenv("AIVEN_PORT"),
        "database": os.getenv("AIVEN_DB"),
        "user": os.getenv("AIVEN_USER"),
        "password": os.getenv("AIVEN_PASSWORD"),
        "sslmode": "require"
}

TICKERS = ["AAPL", "GOOGL", "TSLA", "NFLX", "AMZN"]

def get_engine():
	engine = create_engine(
        	f"postgresql+psycopg2://{POSTGRES_CONFIG['user']}:{POSTGRES_CONFIG['password']}"
        	f"@{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}",
        	connect_args={"sslmode": "require"}
	)
	return engine


@dag(
	dag_id="Stocks_etl",
	start_date=datetime(2025, 1, 1),
	schedule="@hourly",
	catchup=False
)

def stock_pipeline():

	@task
	def extract():
		all_data = []
		for ticker in TICKERS:
			url = f"https://api.massive.com/v2/aggs/ticker/{ticker}/range/1/hour/2026-01-01/2026-04-04"
			params = params = {"adjusted": "true", "sort": "asc", "limit": 5000, "apiKey": API_KEY}
			response = requests.get(url, params)
			data = response.json()
			print(f"{ticker} RESPONSE:", data)

			results = data.get("results", [])
			print(f"{ticker} RESULTS COUNT:", len(results))
	
			for r in results:
				r["ticker"] = ticker
				all_data.append(r)
		print("TOTAL RECORDS:", len(all_data))
		df = pd.DataFrame(all_data)
		print(df.columns)
		return all_data

	@task
	def transform(data):
		
		df = pd.DataFrame(data)

		df["timestamp"] = pd.to_datetime(df["t"], unit="ms")

		df = df.rename(columns={
			"o": "open",
			"h": "high",
			"l": "low",
			"c": "close",
			"v": "volume"
		})

		df["avg_price"] = (df["high"] + df["low"]) / 2
		df["price_change"] = df["close"] - df["open"]

		file_path = "/tmp/stock_data.csv"
		df.to_csv(file_path, index=False)
		
		return file_path

	@task
	def load(file_path):
		df = pd.read_csv(file_path)
		engine = get_engine()

		df.to_sql("stock_prices", engine, if_exists="append", index=False)

	
	load(transform(extract()))

dag = stock_pipeline()





















































