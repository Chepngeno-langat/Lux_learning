from airflow.decorators import dag, task
from datetime import datetime
import requests
import pandas as pd
from dotenv import load_dotenv
import os
import time

load_dotenv("/home/karen/airflow/.env")

API_KEY = os.getenv("COLLECT_API_KEY")

STATES = ["CA", "TX", "FL"]

@dag(
    dag_id="gas_prices_etl",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False
)
def gas_pipeline():

    @task
    def extract():

        all_data = []

        headers = {
            "authorization": f"apikey {API_KEY}",
            "content-type": "application/json"
        }

        STATES = ["CA", "TX", "FL"]

        for state in STATES:

            url = (
                f"https://api.collectapi.com/gasPrice/stateUsaPrice"
                f"?state={state}"
            )

            response = requests.get(url, headers=headers)
            data = response.json()
            
            result = data.get("result", {})
            state_info = result.get("state", {})

            if isinstance(state_info, dict):
                state_info["state_code"] = state
                state_info["level"] = "state"
                all_data.append(state_info)

            cities = result.get("cities", [])

            if isinstance(cities, list):
                for item in cities:
                    if isinstance(item, dict):
                        item["state_code"] = state
                        item["level"] = "city"
                        all_data.append(item)

            time.sleep(1.5)

        print("Total records:", len(all_data))

        return all_data

    @task
    def transform(data):
        df = pd.DataFrame(data)
        print(df.columns)

        df.columns = df.columns.str.lower()
        df["ingestion_time"] = datetime.now()

        file_path = "/tmp/gas_prices.csv"
        df.to_csv(file_path, index=False)

        return file_path

    @task
    def load(file_path):
        df = pd.read_csv(file_path)

        print("Loaded rows:", len(df))
        print(df.head())
        return "Loaded successfully"

    load(transform(extract()))


dag = gas_pipeline()
