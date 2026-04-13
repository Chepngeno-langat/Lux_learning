import pandas as pd
import psycopg2
from config import POSTGRES_CONFIG
from cassandra.cluster import Cluster
from config import CASSANDRA_HOST, KEYSPACE

def load_to_postgres(df):
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()

        cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS weather_data (
                id SERIAL PRIMARY KEY,
                city TEXT,
                region TEXT,
                country TEXT,
                temperature_c FLOAT,
                humidity INT,
                wind_kph FLOAT,
                condition TEXT,
                last_updated TIMESTAMP,
                extracted_at TIMESTAMP
        );
        """)

        for _, row in df.iterrows():
                cursor.execute(
                """
                INSERT INTO weather_data
                (city, region, country, temperature_c, humidity, wind_kph, condition, last_updated, extracted_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, tuple(row)
                )

        conn.commit()
        cursor.close()
        conn.close()

def load_to_cassandra(df):
        cluster = Cluster(CASSANDRA_HOST)
        session = cluster.connect()

        session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{'class':'SimpleStrategy','replication_factor':1}};
        """)

        session.set_keyspace(KEYSPACE)

        session.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
                city TEXT,
                extracted_at TIMESTAMP,
                temperature_c FLOAT,
                humidity INT,
                wind_kph FLOAT,
                condition TEXT,
                PRIMARY KEY (city, extracted_at)
        );
        """)

        for _, row in df.iterrows():
                session.execute("""
                INSERT INTO weather_data
                (city, extracted_at, temperature_c, humidity, wind_kph, condition)
                VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                row["city"],
                row["extracted_at"].to_pydatetime(),
                row["temperature_c"],
                row["humidity"],
                row["wind_kph"],
                row["condition"]
                ))

        cluster.shutdown()
