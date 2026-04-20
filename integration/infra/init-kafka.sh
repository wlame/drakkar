#!/bin/bash
set -e

echo "Waiting for Kafka to be ready..."
sleep 3

for topic in \
        search-requests \
        search-results \
        search-results-priority \
        search-requests_dlq \
        symbol-counts \
        symbol-count-windows; do
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
        --create --if-not-exists \
        --topic "$topic" \
        --partitions 50 \
        --replication-factor 1
done

echo "Topics created successfully"

/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
