import pandas as pd
from extract import extract_weather_data
from transform import transform_weather_data
from load import load_to_postgres, load_to_cassandra

def run_pipeline():

    print("Extracting data...")
    raw_data = extract_weather_data()

    print("Transforming data...")
    df = transform_weather_data(raw_data)

    print("Loading to PostgreSQL...")
    load_to_postgres(df)

    print("Loading to Cassandra...")
    load_to_cassandra(df)

    print("Pipeline completed successfully!")


run_pipeline()

