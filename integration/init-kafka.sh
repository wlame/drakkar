#!/bin/bash
set -e

echo "Waiting for Kafka to be ready..."
sleep 5

kafka-topics --bootstrap-server kafka:9092 \
    --create --if-not-exists \
    --topic search-requests \
    --partitions 50 \
    --replication-factor 1

kafka-topics --bootstrap-server kafka:9092 \
    --create --if-not-exists \
    --topic search-results \
    --partitions 50 \
    --replication-factor 1

echo "Topics created successfully"

kafka-topics --bootstrap-server kafka:9092 --list
