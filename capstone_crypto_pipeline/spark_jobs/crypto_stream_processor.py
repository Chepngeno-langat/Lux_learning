import os
from dotenv import load_dotenv
 
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType, TimestampType
)
 
load_dotenv()
 
KAFKA_SERVERS   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CASSANDRA_HOST  = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT  = os.getenv("CASSANDRA_PORT", "9042")
CASSANDRA_KS    = os.getenv("CASSANDRA_KEYSPACE", "crypto_analytics")

def create_spark_session():
    spark = SparkSession.builder \
        .appName("CryptoStreamProcessor") \
        .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
        .config("spark.cassandra.connection.port", CASSANDRA_PORT) \
        .config("spark.cassandra.auth.username", os.getenv("CASSANDRA_USER")) \
        .config("spark.cassandra.auth.password", os.getenv("CASSANDRA_PASSWORD")) \
        .getOrCreate()
    return spark

def read_kafka_stream(spark, topic):
    # Read streaming data from Kafka topic
    return spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load()

def parse_price_stream(df):
    # Parse price data from Kafka
    schema = StructType([
        StructField("symbol", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("timestamp", StringType(), True)
    ])
    
    return df \
        .select(F.from_json(F.col("value").cast("string"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", F.to_timestamp(F.col("timestamp"))) \
        .withWatermark("timestamp", "10 minutes")

def parse_trade_stream(df):
    # Parse trade data from Kafka
    schema = StructType([
        StructField("symbol", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", DoubleType(), True),
        StructField("timestamp", StringType(), True)
    ])
    
    return df \
        .select(F.from_json(F.col("value").cast("string"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", F.to_timestamp(F.col("timestamp"))) \
        .withWatermark("timestamp", "10 minutes")

def calculate_minute_averages(prices_df):
    # Calculate minute-level price aggregations
    return prices_df \
        .groupBy(
            F.col("symbol"),
            F.window(F.col("timestamp"), "1 minute")
        ) \
        .agg(
            F.avg("price").alias("avg_price"),
            F.min("price").alias("min_price"),
            F.max("price").alias("max_price"),
            F.count("*").alias("tick_count")
        ) \
        .select(
            F.col("symbol"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("avg_price"),
            F.col("min_price"),
            F.col("max_price"),
            F.col("tick_count")
        )

def calculate_hourly_averages(prices_df):
    # Calculate hourly price aggregations
    return prices_df \
        .groupBy(
            F.col("symbol"),
            F.window(F.col("timestamp"), "1 hour")
        ) \
        .agg(
            F.avg("price").alias("avg_price"),
            F.min("price").alias("min_price"),
            F.max("price").alias("max_price"),
            F.stddev("price").alias("price_stddev"),
            F.count("*").alias("tick_count")
        ) \
        .select(
            F.col("symbol"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("avg_price"),
            F.col("min_price"),
            F.col("max_price"),
            F.col("price_stddev"),
            F.col("tick_count")
        )

def calculate_volatility(prices_df):
    # Calculate rolling volatility metrics
    # Define 1-hour rolling window
    window_spec = Window \
        .partitionBy("symbol") \
        .orderBy(F.col("timestamp").cast("long")) \
        .rangeBetween(-3600, 0)  # 1 hour in seconds
    
    return prices_df \
        .withColumn("rolling_avg", F.avg("price").over(window_spec)) \
        .withColumn("rolling_stddev", F.stddev("price").over(window_spec)) \
        .withColumn("volatility_pct", 
                    (F.col("rolling_stddev") / F.col("rolling_avg") * 100)) \
        .select(
            "symbol", 
            "timestamp", 
            "price", 
            "rolling_avg", 
            "rolling_stddev", 
            "volatility_pct"
        )

def analyze_trade_volume(trades_df):
    # Analyze trade volume in 5-minute windows
    return trades_df \
        .groupBy(
            F.col("symbol"),
            F.window(F.col("timestamp"), "5 minutes")
        ) \
        .agg(
            F.sum(F.col("price") * F.col("quantity")).alias("total_volume_usd"),
            F.sum("quantity").alias("total_quantity"),
            F.count("*").alias("trade_count"),
            F.avg("price").alias("avg_trade_price"),
            F.min("price").alias("min_trade_price"),
            F.max("price").alias("max_trade_price")
        ) \
        .select(
            F.col("symbol"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("total_volume_usd"),
            F.col("total_quantity"),
            F.col("trade_count"),
            F.col("avg_trade_price"),
            F.col("min_trade_price"),
            F.col("max_trade_price")
        )


def write_to_cassandra(df, table_name, checkpoint_dir):
   # Write streaming DataFrame to Cassandra
    return df \
        .writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", CASSANDRA_KS) \
        .option("table", table_name) \
        .option("checkpointLocation", f"/tmp/spark-checkpoint/{checkpoint_dir}") \
        .outputMode("append") \
        .start()

def write_to_kafka(df, topic, checkpoint_dir):
    # Write streaming DataFrame back to Kafka
    return df \
        .selectExpr("to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
        .option("topic", topic) \
        .option("checkpointLocation", f"/tmp/spark-checkpoint/{checkpoint_dir}") \
        .start()

def write_to_console(df, query_name):
    # Write streaming DataFrame to console for debugging
    return df \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .queryName(query_name) \
        .start()

def main():
    # Create Spark session
    spark = create_spark_session()
    
    # Read streams from Kafka
    price_raw = read_kafka_stream(spark, "crypto.prices")
    trade_raw = read_kafka_stream(spark, "crypto.trades")
    
    # Parse incoming data
    prices_df = parse_price_stream(price_raw)
    trades_df = parse_trade_stream(trade_raw)
    
    # 1. Minute averages
    minute_avg = calculate_minute_averages(prices_df)
    
    # 2. Hourly averages
    hourly_avg = calculate_hourly_averages(prices_df)
    
    # 3. Volatility calculations
    volatility = calculate_volatility(prices_df)
    
    # 4. Trade volume analysis
    trade_volume = analyze_trade_volume(trades_df)
    
    
    # Write minute averages to Cassandra
    query1 = write_to_cassandra(minute_avg, "minute_price_avg", "minute_avg")
    
    # Write hourly averages to Cassandra
    query2 = write_to_cassandra(hourly_avg, "hourly_price_avg", "hourly_avg")
    
    # Write volatility metrics to Cassandra
    query3 = write_to_cassandra(volatility, "volatility_metrics", "volatility")
    
    # Write trade volume to Cassandra
    query4 = write_to_cassandra(trade_volume, "trade_volume", "trade_volume")
    
    print("Spark Streaming job started successfully!")
    print(f"Processing streams from Kafka: crypto.prices, crypto.trades")
    print(f"Writing results to Cassandra keyspace: {CASSANDRA_KS}")
    print(f"Anomalies will be published to: crypto.anomalies")
    
    # Await termination
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()