import os
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from dotenv import load_dotenv
import logging

load_dotenv()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "crypto_analytics")
CASSANDRA_USER = os.getenv("CASSANDRA_USER", "cassandra")
CASSANDRA_PASSWORD = os.getenv("CASSANDRA_PASSWORD", "cassandra")

def create_connection():
    auth_provider = PlainTextAuthProvider(username=CASSANDRA_USER, password=CASSANDRA_PASSWORD)
    cluster = Cluster(contact_points=[CASSANDRA_HOST], port=CASSANDRA_PORT, auth_provider=auth_provider)
    session = cluster.connect()
    logger.info(f"Connected to Cassandra at {CASSANDRA_HOST}:{CASSANDRA_PORT}")
    return cluster, session

def create_keyspace(session):
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
    """)
    session.set_keyspace(CASSANDRA_KEYSPACE)
    logger.info(f"Keyspace '{CASSANDRA_KEYSPACE}' ready")

def create_tables(session):
    tables = {
        "minute_price_avg": "symbol TEXT, window_start TIMESTAMP, window_end TIMESTAMP, avg_price DOUBLE, min_price DOUBLE, max_price DOUBLE, tick_count BIGINT, PRIMARY KEY ((symbol), window_start)",
        "hourly_price_avg": "symbol TEXT, window_start TIMESTAMP, window_end TIMESTAMP, avg_price DOUBLE, min_price DOUBLE, max_price DOUBLE, price_stddev DOUBLE, tick_count BIGINT, PRIMARY KEY ((symbol), window_start)",
        "volatility_metrics": "symbol TEXT, timestamp TIMESTAMP, price DOUBLE, rolling_avg DOUBLE, rolling_stddev DOUBLE, volatility_pct DOUBLE, PRIMARY KEY ((symbol), timestamp)",
        "trade_volume": "symbol TEXT, window_start TIMESTAMP, window_end TIMESTAMP, total_volume_usd DOUBLE, total_quantity DOUBLE, trade_count BIGINT, avg_trade_price DOUBLE, min_trade_price DOUBLE, max_trade_price DOUBLE, PRIMARY KEY ((symbol), window_start)",
        "anomalies": "symbol TEXT, timestamp TIMESTAMP, price DOUBLE, avg_price DOUBLE, stddev_price DOUBLE, z_score DOUBLE, PRIMARY KEY ((symbol), timestamp)",
        "latest_prices": "symbol TEXT PRIMARY KEY, price DOUBLE, timestamp TIMESTAMP, avg_price DOUBLE, volatility_pct DOUBLE"
    }
    
    for table, schema in tables.items():
        session.execute(f"CREATE TABLE IF NOT EXISTS {table} ({schema}) WITH CLUSTERING ORDER BY (window_start DESC)" if "window_start" in schema else f"CREATE TABLE IF NOT EXISTS {table} ({schema})")
    logger.info("All tables created")

def setup_cassandra():
    cluster, session = create_connection()
    create_keyspace(session)
    create_tables(session)
    logger.info("✅ Cassandra setup complete")
    return cluster, session

def get_latest_prices(session, symbols=None):
    if symbols:
        placeholders = ','.join(['%s'] * len(symbols))
        rows = session.execute(f"SELECT * FROM latest_prices WHERE symbol IN ({placeholders})", symbols)
    else:
        rows = session.execute("SELECT * FROM latest_prices")
    return [dict(row._asdict()) for row in rows]

def get_minute_averages(session, symbol, limit=60):
    rows = session.execute("SELECT * FROM minute_price_avg WHERE symbol = %s LIMIT %s", (symbol, limit))
    return [dict(row._asdict()) for row in rows]

def get_hourly_averages(session, symbol, limit=24):
    rows = session.execute("SELECT * FROM hourly_price_avg WHERE symbol = %s LIMIT %s", (symbol, limit))
    return [dict(row._asdict()) for row in rows]

def get_volatility_metrics(session, symbol, limit=100):
    rows = session.execute("SELECT * FROM volatility_metrics WHERE symbol = %s LIMIT %s", (symbol, limit))
    return [dict(row._asdict()) for row in rows]

def get_trade_volume(session, symbol, limit=12):
    rows = session.execute("SELECT * FROM trade_volume WHERE symbol = %s LIMIT %s", (symbol, limit))
    return [dict(row._asdict()) for row in rows]

def get_anomalies(session, symbol=None, limit=50):
    if symbol:
        rows = session.execute("SELECT * FROM anomalies WHERE symbol = %s LIMIT %s", (symbol, limit))
    else:
        rows = session.execute("SELECT * FROM anomalies LIMIT %s ALLOW FILTERING", (limit,))
    return [dict(row._asdict()) for row in rows]

def close_connection(cluster, session):
    session.shutdown()
    cluster.shutdown()

if __name__ == "__main__":
    cluster, session = setup_cassandra()
    close_connection(cluster, session)
