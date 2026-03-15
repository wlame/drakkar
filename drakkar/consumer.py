"""Kafka consumer wrapper for Drakkar framework."""

import asyncio
from collections.abc import Callable
from typing import Any

import structlog
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

from drakkar.config import KafkaConfig
from drakkar.metrics import consumer_errors, offsets_committed, rebalance_events
from drakkar.models import SourceMessage

logger = structlog.get_logger()

OnAssignCallback = Callable[[list[int]], Any]
OnRevokeCallback = Callable[[list[int]], Any]


class KafkaConsumer:
    """Wraps confluent_kafka.Consumer with cooperative-sticky rebalancing
    and manual offset commits, integrated with asyncio.

    Rebalance callbacks from librdkafka's internal thread are dispatched
    to the asyncio event loop via call_soon_threadsafe.
    """

    def __init__(
        self,
        config: KafkaConfig,
        on_assign: OnAssignCallback | None = None,
        on_revoke: OnRevokeCallback | None = None,
        loop: asyncio.AbstractEventLoop | None = None,
    ):
        self._config = config
        self._on_assign_cb = on_assign
        self._on_revoke_cb = on_revoke
        self._loop = loop

        self._consumer = Consumer({
            "bootstrap.servers": config.brokers,
            "group.id": config.consumer_group,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "partition.assignment.strategy": "cooperative-sticky",
            "max.poll.interval.ms": config.max_poll_interval_ms,
            "session.timeout.ms": config.session_timeout_ms,
            "heartbeat.interval.ms": config.heartbeat_interval_ms,
        })

    def subscribe(self) -> None:
        """Subscribe to the source topic with rebalance callbacks."""
        self._consumer.subscribe(
            [self._config.source_topic],
            on_assign=self._handle_assign,
            on_revoke=self._handle_revoke,
        )

    def _handle_assign(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        """Called from librdkafka thread — dispatches to event loop."""
        partition_ids = [p.partition for p in partitions]
        rebalance_events.labels(type="assign").inc()
        logger.info("partitions_assigned", category="kafka", partitions=partition_ids, count=len(partition_ids))
        if self._on_assign_cb:
            if self._loop:
                self._loop.call_soon_threadsafe(self._on_assign_cb, partition_ids)
            else:
                self._on_assign_cb(partition_ids)

    def _handle_revoke(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        """Called from librdkafka thread — dispatches to event loop."""
        partition_ids = [p.partition for p in partitions]
        rebalance_events.labels(type="revoke").inc()
        logger.info("partitions_revoked", category="kafka", partitions=partition_ids, count=len(partition_ids))
        if self._on_revoke_cb:
            if self._loop:
                self._loop.call_soon_threadsafe(self._on_revoke_cb, partition_ids)
            else:
                self._on_revoke_cb(partition_ids)

    async def poll_batch(self, max_messages: int | None = None, timeout: float = 1.0) -> list[SourceMessage]:
        """Poll up to max_messages from Kafka, non-blocking via executor."""
        loop = asyncio.get_running_loop()
        count = max_messages or self._config.max_poll_records

        raw_messages = await loop.run_in_executor(
            None, self._consume_batch, count, timeout
        )

        messages = []
        for msg in raw_messages:
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                consumer_errors.inc()
                logger.warning("consumer_error", category="kafka", error=str(msg.error()))
                continue
            messages.append(SourceMessage(
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
                key=msg.key(),
                value=msg.value(),
                timestamp=msg.timestamp()[1],
            ))
        return messages

    def _consume_batch(self, count: int, timeout: float) -> list:
        """Synchronous consume call, run in executor."""
        return self._consumer.consume(num_messages=count, timeout=timeout)

    async def commit(self, offsets: dict[int, int]) -> None:
        """Commit offsets for specific partitions."""
        topic_partitions = [
            TopicPartition(self._config.source_topic, partition, offset)
            for partition, offset in offsets.items()
        ]
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None, self._consumer.commit, topic_partitions
        )
        for partition_id in offsets:
            offsets_committed.labels(partition=str(partition_id)).inc()
        logger.debug("offsets_committed", category="kafka", offsets=offsets)

    def close(self) -> None:
        """Close the consumer."""
        self._consumer.close()
