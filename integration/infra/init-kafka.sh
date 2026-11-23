#!/bin/bash
set -e

echo "Waiting for Kafka to be ready..."
sleep 3

for topic in \
        search-requests \
        search-results \
        search-results-priority \
        search-results-http \
        search-requests_dlq \
        symbol-counts \
        symbol-count-windows; do
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
        --create --if-not-exists \
        --topic "$topic" \
        --partitions 50 \
        --replication-factor 1
done

# The stream worker's topics have one partition each: its messages stream
# one after the other, and a reader sees its records in delivery order.
for topic in stream-requests stream-records; do
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
        --create --if-not-exists \
        --topic "$topic" \
        --partitions 1 \
        --replication-factor 1
done

# The split worker's topics. The source topic has 6 partitions; the results
# topic must have exactly as many (the worker checks it at start); the parts
# topic has 6, at least one for each of the three workers.
for topic in \
        split-requests \
        split-requests_drakkar-split_split_parts \
        split-requests_drakkar-split_split_results \
        split-items \
        split-summaries \
        split-requests_dlq; do
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
        --create --if-not-exists \
        --topic "$topic" \
        --partitions 6 \
        --replication-factor 1
done

# The status topic of the split worker's HTTP jobs: one record per change of
# a job, read from the start by every split worker. The delete cleanup
# policy is required (a compacted topic could keep a late `accepted` record
# and drop the end state), and the retention covers the result retention.
/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
    --create --if-not-exists \
    --topic split-requests_status \
    --partitions 3 \
    --replication-factor 1 \
    --config cleanup.policy=delete \
    --config retention.ms=86400000

echo "Topics created successfully"

/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
