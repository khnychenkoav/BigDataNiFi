import csv
import glob
import json
import os
import time

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import NoBrokersAvailable, TopicAlreadyExistsError


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "pet_sales_raw")
DATA_DIR = os.getenv("DATA_DIR", "/data")


def wait_for_kafka():
    last_error = None
    for _ in range(60):
        try:
            admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS, client_id="csv-json-producer-admin")
            admin.close()
            return
        except NoBrokersAvailable as exc:
            last_error = exc
            time.sleep(2)
    raise last_error


def ensure_topic():
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS, client_id="csv-json-producer-admin")
    try:
        topic = NewTopic(name=TOPIC, num_partitions=3, replication_factor=1)
        try:
            admin.create_topics([topic])
        except TopicAlreadyExistsError:
            pass
    finally:
        admin.close()


def iter_rows():
    for path in sorted(glob.glob(os.path.join(DATA_DIR, "*.csv"))):
        source_file = os.path.basename(path)
        with open(path, encoding="utf-8", newline="") as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                row["source_file"] = source_file
                yield row


def main():
    wait_for_kafka()
    ensure_topic()

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
        linger_ms=20,
        acks="all",
    )

    sent = 0
    for row in iter_rows():
        key = f"{row['source_file']}:{row['id']}"
        producer.send(TOPIC, key=key, value=row)
        sent += 1

    producer.flush()
    producer.close()
    print(f"sent {sent} messages to {TOPIC}")


if __name__ == "__main__":
    main()
