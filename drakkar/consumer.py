"""Kafka consumer wrapper for Drakkar framework.

Uses confluent_kafka's AIOConsumer for native asyncio integration.
Rebalance callbacks run on the event loop — no call_soon_threadsafe needed.
"""

from collections.abc import Callable
from typing import Any

import structlog
from confluent_kafka import KafkaError, TopicPartition
from confluent_kafka.aio import AIOConsumer

from drakkar.config import KafkaConfig
from drakkar.metrics import consumer_errors, offsets_committed, rebalance_events
from drakkar.models import SourceMessage

logger = structlog.get_logger()

OnAssignCallback = Callable[[list[int]], Any]
OnRevokeCallback = Callable[[list[int]], Any]


class KafkaConsumer:
    """Wraps confluent_kafka.AIOConsumer with cooperative-sticky rebalancing
    and manual offset commits.
    """

    def __init__(
        self,
        config: KafkaConfig,
        on_assign: OnAssignCallback | None = None,
        on_revoke: OnRevokeCallback | None = None,
    ) -> None:
        self._config = config
        self._on_assign_cb = on_assign
        self._on_revoke_cb = on_revoke

        self._consumer = AIOConsumer(
            {
                'bootstrap.servers': config.brokers,
                'group.id': config.consumer_group,
                'enable.auto.commit': False,
                'auto.offset.reset': 'earliest',
                'partition.assignment.strategy': 'cooperative-sticky',
                'max.poll.interval.ms': config.max_poll_interval_ms,
                'session.timeout.ms': config.session_timeout_ms,
                'heartbeat.interval.ms': config.heartbeat_interval_ms,
            }
        )

    async def subscribe(self) -> None:
        """Subscribe to the source topic with rebalance callbacks."""
        await self._consumer.subscribe(
            [self._config.source_topic],
            on_assign=self._handle_assign,
            on_revoke=self._handle_revoke,
        )

    async def _handle_assign(self, consumer: object, partitions: list[TopicPartition]) -> None:
        """Async rebalance callback — runs on the event loop."""
        partition_ids = [p.partition for p in partitions]
        rebalance_events.labels(type='assign').inc()
        logger.info(
            'partitions_assigned',
            category='kafka',
            partitions=partition_ids,
            count=len(partition_ids),
        )
        if self._on_assign_cb:
            self._on_assign_cb(partition_ids)

    async def _handle_revoke(self, consumer: object, partitions: list[TopicPartition]) -> None:
        """Async rebalance callback — runs on the event loop."""
        partition_ids = [p.partition for p in partitions]
        rebalance_events.labels(type='revoke').inc()
        logger.info(
            'partitions_revoked',
            category='kafka',
            partitions=partition_ids,
            count=len(partition_ids),
        )
        if self._on_revoke_cb:
            self._on_revoke_cb(partition_ids)

    async def poll_batch(self, max_messages: int | None = None, timeout: float = 1.0) -> list[SourceMessage]:
        """Poll up to max_messages from Kafka."""
        count = max_messages or self._config.max_poll_records
        raw_messages = await self._consumer.consume(
            num_messages=count,
            timeout=timeout,
        )

        messages = []
        for msg in raw_messages:
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                consumer_errors.inc()
                logger.warning('consumer_error', category='kafka', error=str(msg.error()))
                continue
            messages.append(
                SourceMessage(
                    topic=msg.topic(),
                    partition=msg.partition(),
                    offset=msg.offset(),
                    key=msg.key(),
                    value=msg.value(),
                    timestamp=msg.timestamp()[1],
                )
            )
        return messages

    async def commit(self, offsets: dict[int, int]) -> None:
        """Commit offsets for specific partitions."""
        topic_partitions = [
            TopicPartition(self._config.source_topic, partition, offset) for partition, offset in offsets.items()
        ]
        await self._consumer.commit(offsets=topic_partitions, asynchronous=False)
        for partition_id in offsets:
            offsets_committed.labels(partition=str(partition_id)).inc()
        logger.debug('offsets_committed', category='kafka', offsets=offsets)

    async def pause(self, partition_ids: list[int]) -> None:
        """Pause consuming from specific partitions (backpressure)."""
        tps = [TopicPartition(self._config.source_topic, pid) for pid in partition_ids]
        await self._consumer.pause(tps)
        logger.debug('partitions_paused', category='kafka', partitions=partition_ids)

    async def resume(self, partition_ids: list[int]) -> None:
        """Resume consuming from previously paused partitions."""
        tps = [TopicPartition(self._config.source_topic, pid) for pid in partition_ids]
        await self._consumer.resume(tps)
        logger.debug('partitions_resumed', category='kafka', partitions=partition_ids)

    async def get_total_lag(self, partition_ids: list[int]) -> int:
        """Get total consumer lag across all partitions."""
        if not partition_ids:
            return 0
        import asyncio

        tps = [TopicPartition(self._config.source_topic, pid) for pid in partition_ids]
        try:
            committed_list = await self._consumer.committed(tps)
        except Exception:
            return 0

        committed_map = {}
        for tp_result in committed_list or []:
            if tp_result and tp_result.offset >= 0:
                committed_map[tp_result.partition] = tp_result.offset

        async def _watermark(pid: int) -> int:
            try:
                tp = TopicPartition(self._config.source_topic, pid)
                _low, high = await self._consumer.get_watermark_offsets(tp)
                return max(0, high - committed_map.get(pid, 0))
            except Exception:
                return 0

        lags = await asyncio.gather(*[_watermark(pid) for pid in partition_ids])
        return sum(lags)

    async def get_partition_lag(self, partition_ids: list[int]) -> dict[int, dict]:
        """Get committed offset, high watermark, and lag for each partition."""
        result = {}
        for pid in partition_ids:
            try:
                tp = TopicPartition(self._config.source_topic, pid)
                _low, high = await self._consumer.get_watermark_offsets(tp)
                committed_list = await self._consumer.committed([tp])
                committed_offset = committed_list[0].offset if committed_list and committed_list[0].offset >= 0 else 0
                result[pid] = {
                    'committed': committed_offset,
                    'high_watermark': high,
                    'lag': high - committed_offset,
                }
            except Exception:
                result[pid] = {'committed': 0, 'high_watermark': 0, 'lag': 0}
        return result

    async def close(self) -> None:
        """Close the consumer."""
        await self._consumer.close()
