import os
from kafka.admin import KafkaAdminClient, NewTopic
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
 
TOPICS = [
    NewTopic(name="crypto.prices",   num_partitions=3, replication_factor=1),
    NewTopic(name="crypto.trades",   num_partitions=3, replication_factor=1),
]

def create_topics():
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    existing = admin.list_topics()
 
    to_create = [t for t in TOPICS if t.name not in existing]
    if not to_create:
        print("All topics already exist.")
        return
 
    admin.create_topics(new_topics=to_create, validate_only=False)
    for t in to_create:
        print(f"✅  Created topic: {t.name}")
 
    admin.close()
 
 
if __name__ == "__main__":
    create_topics()
 