import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "ingestion"))
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "kafka"))
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "cassandra"))

default_args = {
    "owner": "Karen",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=10),
}

with DAG(
    dag_id="crypto_ingestion_dag",
    default_args=default_args,
    schedule="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    
    from binance_ingest import run_binance_ingestion
    from cassandra_setup import setup_cassandra
    
    def initialize_cassandra():
        cluster, session = setup_cassandra()
        session.shutdown()
        cluster.shutdown()
        
    def start_kafka_streaming():
        from kafka_producer import stream_from_postgres
        stream_from_postgres()
        
    # Task 1: Initialize Cassandra
    cassandra_task = PythonOperator(
        task_id="initialize_cassandra",
        python_callable=initialize_cassandra,
    )

    # Task 2: Ingest data from Binance API  
    ingest_task = PythonOperator(
        task_id="run_binance_ingestion",
        python_callable=run_binance_ingestion,
    )
    
    # Task 3: Stream data from PostgreSQL to Kafka
    kafka_stream_task = PythonOperator(
        task_id="start_kafka_streaming",
        python_callable=start_kafka_streaming,
    )
    
    # Task 4: Run Spark job to process data from Kafka and write to Cassandra
    spark_task = PythonOperator(
        task_id="run_spark_job",
        python_callable=lambda: os.system("spark-submit spark_processor.py"),
    )
    
    cassandra_task >> ingest_task >> kafka_stream_task

