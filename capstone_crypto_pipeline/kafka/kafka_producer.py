import os
import json
import logging
from datetime import datetime
from decimal import Decimal
from kafka import KafkaProducer
from dotenv import load_dotenv
import psycopg2
import time

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_PRICES      = os.getenv("KAFKA_TOPIC_PRICES",  "crypto.prices")
TOPIC_TRADES      = os.getenv("KAFKA_TOPIC_TRADES",  "crypto.trades")

# JSON serializer to handle Decimal and datetime objects
def json_serialiser(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serialisable")

def make_producer():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, default=json_serialiser).encode("utf-8"),
        retries=5,
        linger_ms=10,
        batch_size=32 * 1024,
    )
    return producer


def publish_price_tick(producer, symbol, price, timestamp):
    message = {
        "symbol": symbol,
        "price": price,
        "timestamp": timestamp
    }
    producer.send(TOPIC_PRICES, value=message)
    logger.info(f"Published price tick: {message}")

def publish_trades(producer, symbol, price, quantity, timestamp):
    message = {
        "symbol": symbol,
        "price": price,
        "quantity": quantity,
        "timestamp": timestamp
    }
    producer.send(TOPIC_TRADES, value=message)
    logger.info(f"Published trade: {message}")


def stream_from_postgres():
    conn = None
    cursor = None
    producer = None
    
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST"),
            database=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD")
        )
        producer = make_producer()
        cursor = conn.cursor()
        
        last_processed_price_id = 0
        last_processed_trade_id = 0
        
        logger.info("Starting PostgreSQL to Kafka streaming...")
        
        while True:
            # Stream prices
            cursor.execute("""
                SELECT id, symbol, price, timestamp 
                FROM prices 
                WHERE id > %s 
                ORDER BY id LIMIT 100
            """, (last_processed_price_id,))
            
            rows = cursor.fetchall()
            for row in rows:
                id, symbol, price, timestamp = row
                publish_price_tick(producer, symbol, price, timestamp)
                last_processed_price_id = id
            
            # Stream trades
            cursor.execute("""
                SELECT id, symbol, price, quantity, timestamp 
                FROM trades 
                WHERE id > %s 
                ORDER BY id LIMIT 100
            """, (last_processed_trade_id,))
            
            rows = cursor.fetchall()
            for row in rows:
                id, symbol, price, quantity, timestamp = row
                publish_trades(producer, symbol, price, quantity, timestamp)
                last_processed_trade_id = id
            
            producer.flush()  # Ensure messages are sent
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Shutting down gracefully...")
    except Exception as e:
        logger.error(f"Error in streaming: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        if producer:
            producer.close()
        logger.info("Streaming complete")