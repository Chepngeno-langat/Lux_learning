from airflow.decorators import dag, task
from datetime import datetime
import requests
import pandas as pd

COINS = ["btc-bitcoin", "eth-ethereum", "sol-solana"]

@dag(
	dag_id="coinpaprika_taskflow_api",
	start_date=datetime(2025, 1, 1),
	schedule="@daily",
	catchup=False
)

def crypto_pipeline():
	@task
	def extract():
		results = []
		
		for coin in COINS:
			url = f"https://api.coinpaprika.com/v1/tickers/{coin}"
			response = requests.get(url)
			data = response.json()
			results.append(data)
		return results

	@task
	def transform(data):
		rows = []
		
		for item in data:
			usd = item.get("quotes", {}).get("USD", {})

			rows.append({
				"id": item.get("id"),
				"name": item.get("name"),
				"symbol": item.get("symbol"),
				"rank": item.get("rank"),
				"price": usd.get("price"),
				"volume_24h": usd.get("volume_24h"),
				"market_cap": usd.get("market_cap"),
				"percent_change_24h": usd.get("percent_change_24h"),
				"last_updated": item.get("last_updated")
			})
		df = pd.DataFrame(rows)
		
		file_path = "/tmp/crypto_data.csv"
		df.to_csv(file_path, index=False)

		return file_path

	@task
	def load(file_path):
		df = pd.read_csv(file_path)
		print("Loaded rows:", len(df))
		print(df.head())

		return "Loaded Successfully"
	
	
	load(transform(extract()))

dag = crypto_pipeline()













































