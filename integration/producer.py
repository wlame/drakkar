"""Integration test producer: pushes search requests to Kafka."""

import json
import os
import random
import time
import uuid

from confluent_kafka import Producer

BROKERS = os.environ.get("KAFKA_BROKERS", "kafka:9092")
TOPIC = os.environ.get("SOURCE_TOPIC", "search-requests")
RATE = float(os.environ.get("MESSAGES_PER_SECOND", "10"))
TOTAL_MESSAGES = int(os.environ.get("TOTAL_MESSAGES", "500"))

PATTERNS = [
    r"error",
    r"warning",
    r"INFO",
    r"DEBUG",
    r"Exception",
    r"failed",
    r"success",
    r"timeout",
    r"connection",
    r"request",
    r"response",
    r"import",
    r"class\s+\w+",
    r"def\s+\w+",
    r"return",
    r"async",
    r"await",
    r"try",
    r"except",
    r"raise",
]

SAMPLE_FILES = [f"/data/sample_{i}.txt" for i in range(10)]


def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}", flush=True)


def main():
    print(f"Producer starting: {TOTAL_MESSAGES} messages at {RATE}/sec to {TOPIC}", flush=True)

    # wait for Kafka to be ready
    time.sleep(10)

    producer = Producer({"bootstrap.servers": BROKERS})
    interval = 1.0 / RATE if RATE > 0 else 0

    for i in range(TOTAL_MESSAGES):
        message = {
            "request_id": str(uuid.uuid4()),
            "pattern": random.choice(PATTERNS),
            "file_path": random.choice(SAMPLE_FILES),
        }

        producer.produce(
            topic=TOPIC,
            key=message["request_id"].encode(),
            value=json.dumps(message).encode(),
            callback=delivery_report,
        )
        producer.poll(0)

        if (i + 1) % 50 == 0:
            print(f"Produced {i + 1}/{TOTAL_MESSAGES} messages", flush=True)

        if interval > 0:
            time.sleep(interval)

    producer.flush(timeout=30)
    print(f"Producer finished: {TOTAL_MESSAGES} messages sent", flush=True)

    # keep running so docker doesn't restart
    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
