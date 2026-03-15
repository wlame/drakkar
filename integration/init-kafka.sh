#!/bin/bash
set -e

echo "Waiting for Kafka to be ready..."
sleep 3

/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
    --create --if-not-exists \
    --topic search-requests \
    --partitions 50 \
    --replication-factor 1

/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
    --create --if-not-exists \
    --topic search-results \
    --partitions 50 \
    --replication-factor 1

echo "Topics created successfully"

/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
