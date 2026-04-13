import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("WEATHER_API_KEY")
CITY = "NAIROBI"

POSTGRES_CONFIG = {
	"host": os.getenv("AIVEN_HOST"),
	"port": os.getenv("AIVEN_PORT"),
    	"database": os.getenv("AIVEN_DB"),
    	"user": os.getenv("AIVEN_USER"),
    	"password": os.getenv("AIVEN_PASSWORD"),
    	"sslmode": "require"
}

CASSANDRA_HOST = ["127.0.0.1"]
KEYSPACE = "weather_keyspace"
