"""Kafka consumer wrapper for Drakkar framework."""

import asyncio
from collections.abc import Callable
from typing import Any

import structlog
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

from drakkar.config import KafkaConfig
from drakkar.models import SourceMessage

logger = structlog.get_logger()

OnAssignCallback = Callable[[list[int]], Any]
OnRevokeCallback = Callable[[list[int]], Any]


class KafkaConsumer:
    """Wraps confluent_kafka.Consumer with cooperative-sticky rebalancing
    and manual offset commits, integrated with asyncio.
    """

    def __init__(
        self,
        config: KafkaConfig,
        on_assign: OnAssignCallback | None = None,
        on_revoke: OnRevokeCallback | None = None,
    ):
        self._config = config
        self._on_assign_cb = on_assign
        self._on_revoke_cb = on_revoke
        self._loop: asyncio.AbstractEventLoop | None = None

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
        partition_ids = [p.partition for p in partitions]
        logger.info("partitions_assigned", partitions=partition_ids)
        if self._on_assign_cb:
            self._on_assign_cb(partition_ids)

    def _handle_revoke(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        partition_ids = [p.partition for p in partitions]
        logger.info("partitions_revoked", partitions=partition_ids)
        if self._on_revoke_cb:
            self._on_revoke_cb(partition_ids)

    async def poll_batch(self, max_messages: int | None = None, timeout: float = 1.0) -> list[SourceMessage]:
        """Poll up to max_messages from Kafka, non-blocking via executor.

        Returns empty list if no messages available.
        """
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
                logger.warning("consumer_error", error=str(msg.error()))
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
        """Commit offsets for specific partitions.

        Args:
            offsets: mapping of partition_id -> offset_to_commit
        """
        topic_partitions = [
            TopicPartition(self._config.source_topic, partition, offset)
            for partition, offset in offsets.items()
        ]
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None, self._consumer.commit, topic_partitions
        )
        logger.debug("offsets_committed", offsets=offsets)

    def close(self) -> None:
        """Close the consumer."""
        self._consumer.close()
