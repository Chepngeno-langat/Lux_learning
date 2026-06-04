import os
import logging
import requests
from datetime import datetime, timezone
import pandas as pd
from typing import List, Dict
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

DB_URL = (
    f"postgresql://{os.getenv('AIVEN_USER')}:"
    f"{os.getenv('AIVEN_PASSWORD')}@"
    f"{os.getenv('AIVEN_HOST')}:"
    f"{os.getenv('AIVEN_PORT')}/"
    f"{os.getenv('AIVEN_DB')}?sslmode=require"
)

BINANCE_BASE_URL = "https://api.binance.com/api/v3"

SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
    "SOLUSDT",
    "XRPUSDT",
]

def fetch_binance_data(endpoint, params):
    url = f"{BINANCE_BASE_URL}/{endpoint}"
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    return response.json()

def fetch_live_price_ticks(symbols):
    data = []
    for symbol in symbols:
        try:
            params = {"symbol": symbol}
            result = fetch_binance_data("ticker/price", params)
            result["timestamp"] = datetime.now(timezone.utc).isoformat()
            data.append(result)
        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {e}")
    return data

def store_price_ticks_to_db(data):
    df = pd.DataFrame(data)
    engine = create_engine(DB_URL)
    with engine.connect() as connection:
        df.to_sql("binance_price_ticks", con=connection, if_exists="append", index=False)
    logger.info(f"Stored {len(data)} price ticks to database")
    
def fetch_ohlcv_data(symbols, interval="1m", limit=100):
    data = []
    for symbol in symbols:
        try:
            params = {"symbol": symbol, "interval": interval, "limit": limit}
            result = fetch_binance_data("klines", params)
            for item in result:
                data.append({
                    "symbol": symbol,
                    "interval": interval,
                    "open_time": datetime.fromtimestamp(item[0] / 1000, tz=timezone.utc).isoformat(),
                    "open": item[1],
                    "high": item[2],
                    "low": item[3],
                    "close": item[4],
                    "volume": item[5],
                    "close_time": datetime.fromtimestamp(item[6] / 1000, tz=timezone.utc).isoformat(),
                })
        except Exception as e:
            logger.error(f"Error fetching OHLCV data for {symbol}: {e}")
    return data

def store_ohlcv_data_to_db(data):
    df = pd.DataFrame(data)
    engine = create_engine(DB_URL)
    with engine.connect() as connection:
        df.to_sql("binance_ohlcv_data", con=connection, if_exists="append", index=False)
    logger.info(f"Stored {len(data)} OHLCV records to database")


def fetch_recent_trades(symbols, limit=100):
    data = []
    for symbol in symbols:
        try:
            params = {"symbol": symbol, "limit": limit}
            result = fetch_binance_data("trades", params)
            for item in result:
                data.append({
                    "symbol": symbol,
                    "trade_id": item["id"],
                    "price": item["price"],
                    "quantity": item["qty"],
                    "trade_time": datetime.fromtimestamp(item["time"] / 1000, tz=timezone.utc).isoformat(),
                })
        except Exception as e:
            logger.error(f"Error fetching recent trades for {symbol}: {e}")
    return data

def store_recent_trades_to_db(data):
    df = pd.DataFrame(data)
    engine = create_engine(DB_URL)
    with engine.connect() as connection:
        df.to_sql("binance_recent_trades", con=connection, if_exists="append", index=False)
    logger.info(f"Stored {len(data)} recent trades to database")


# Ingestions function calls
def run_binance_ingestion():
    logger.info("Starting Binance data ingestion")
    print("Running manual ingestion for Binance data...")
    
    # Fetch and store live price ticks
    price_ticks = fetch_live_price_ticks(SYMBOLS)
    store_price_ticks_to_db(price_ticks)
    
    # Fetch and store OHLCV data
    ohlcv_data = fetch_ohlcv_data(SYMBOLS)
    store_ohlcv_data_to_db(ohlcv_data)
    
    # Fetch and store recent trades
    recent_trades = fetch_recent_trades(SYMBOLS)
    store_recent_trades_to_db(recent_trades)
    
    logger.info("Completed Binance data ingestion")
    print("Binance data ingestion completed successfully.")

