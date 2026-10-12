"""Kafka consumer wrapper for Drakkar framework.

Uses confluent_kafka's AIOConsumer for native asyncio integration.
Rebalance callbacks run on the event loop — no call_soon_threadsafe needed.
"""

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

import structlog
from confluent_kafka import KafkaError, TopicPartition
from confluent_kafka.aio import AIOConsumer

from drakkar.config import KafkaConfig, KafkaSourceConfig
from drakkar.kafka_security import merge_client_config
from drakkar.metrics import consumer_errors, offsets_committed, rebalance_events
from drakkar.models import SourceMessage

logger = structlog.get_logger()

OnAssignCallback = Callable[[list[int]], Any]
# The revoke callback is awaited: the rebalance must not complete until the
# worker has drained and committed the revoked partitions (see
# KafkaSource.on_revoke in drakkar/sources/kafka.py). AIOConsumer runs this via
# run_coroutine_threadsafe(...).result(), so librdkafka waits on it.
OnRevokeCallback = Callable[[list[int]], Awaitable[None]]

# Wall-clock cap on the metadata RPCs behind the UI's lag panels
# (``committed`` / ``get_watermark_offsets``). librdkafka blocks
# INDEFINITELY when the timeout is omitted, and every AIOConsumer call —
# including ``consume`` and ``commit`` — shares one small thread pool, so an
# untimed lag query against an unreachable partition leader parks a pool
# thread forever and the poll loop eventually queues behind it. The worker
# then stops consuming with no error, while readiness still reports healthy.
# 2s is long enough for a healthy broker round trip and short enough that
# a wedged one cannot hold the poll loop.
LAG_QUERY_TIMEOUT_SECONDS = 2.0

# AIOConsumer defaults to 2 executor threads for EVERY consumer operation.
# The UI's per-partition watermark fan-out can submit one work item per
# assigned partition at once, so a 2-thread pool leaves the pipeline's
# ``consume``/``commit`` calls waiting behind UI traffic. The timeout above
# bounds the damage; this gives the pipeline headroom so it is not merely
# bounded but unimpeded.
CONSUMER_MAX_WORKERS = 8

# ``auto.offset.reset`` values (librdkafka spellings) under which a partition
# with no committed offset starts reading at the high watermark. Every other
# value starts at the low watermark — including ``error``, which never starts
# reading on its own, so every retained message is backlog.
AUTO_OFFSET_RESET_TO_END = frozenset({'largest', 'latest', 'end'})


def _report_lag_query_failure(*, op: str, error: object, **context: Any) -> None:
    """Log and count one failed lag query, so an outage never reads as "no backlog"."""
    consumer_errors.inc()
    logger.warning(
        'lag_query_failed',
        category='kafka',
        op=op,
        error=str(error),
        error_type=type(error).__name__,
        **context,
    )


class KafkaConsumer:
    """Wraps confluent_kafka.AIOConsumer with cooperative-sticky rebalancing
    and manual offset commits.
    """

    def __init__(
        self,
        connection: KafkaConfig,
        source: KafkaSourceConfig,
        on_assign: OnAssignCallback | None = None,
        on_revoke: OnRevokeCallback | None = None,
    ) -> None:
        self._connection = connection
        self._source = source
        self._on_assign_cb = on_assign
        self._on_revoke_cb = on_revoke

        # Framework-owned keys first, then the typed security block, then the
        # raw client_config escape hatch. ``merge_client_config`` defines that
        # precedence; the keys this dict sets which must never be overridden
        # (manual commit, cooperative-sticky) are refused at config load by
        # RESERVED_CLIENT_KEYS, so the merge cannot break invariants #6/#7.
        client_config = merge_client_config(
            {
                'bootstrap.servers': connection.brokers,
                'group.id': source.consumer_group,
                'enable.auto.commit': False,
                'auto.offset.reset': 'earliest',
                'partition.assignment.strategy': 'cooperative-sticky',
                'max.poll.interval.ms': source.max_poll_interval_ms,
                'session.timeout.ms': source.session_timeout_ms,
                'heartbeat.interval.ms': source.heartbeat_interval_ms,
            },
            connection.security,
            connection.client_config,
        )
        # Lag for a partition with no committed offset counts from where the
        # consumer will start reading it. ``client_config`` may override the
        # reset policy, and librdkafka accepts its value in any case.
        self._starts_at_high_watermark = str(client_config['auto.offset.reset']).lower() in AUTO_OFFSET_RESET_TO_END
        # The last offset this worker committed per partition: the lag
        # fallback when the broker's committed() answer lacks a partition.
        self._offsets_committed_here: dict[int, int] = {}
        self._consumer = AIOConsumer(client_config, max_workers=CONSUMER_MAX_WORKERS)

    @property
    def source_topic(self) -> str:
        """The Kafka topic this consumer is subscribed to."""
        return self._source.topic

    @property
    def consumer_group(self) -> str:
        """The consumer group this consumer joins."""
        return self._source.consumer_group

    async def subscribe(self) -> None:
        """Subscribe to the source topic with rebalance callbacks."""
        await self._consumer.subscribe(
            [self._source.topic],
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
        # A commit remembered from an earlier assignment is stale once another
        # member has committed past it. Forget it when ownership starts rather
        # than on revoke: a commit from the old assignment can still land
        # after the revoke callback returns.
        for partition_id in partition_ids:
            self._offsets_committed_here.pop(partition_id, None)
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
            await self._on_revoke_cb(partition_ids)

    async def poll_batch(self, max_messages: int | None = None, timeout: float = 1.0) -> list[SourceMessage]:
        """Poll up to max_messages from Kafka."""
        count = max_messages or self._source.max_poll_records
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
            TopicPartition(self._source.topic, partition, offset) for partition, offset in offsets.items()
        ]
        await self._consumer.commit(offsets=topic_partitions, asynchronous=False)
        self._offsets_committed_here.update(offsets)
        for partition_id in offsets:
            offsets_committed.labels(partition=str(partition_id)).inc()
        logger.debug('offsets_committed', category='kafka', offsets=offsets)

    async def pause(self, partition_ids: list[int]) -> None:
        """Pause consuming from specific partitions (backpressure)."""
        tps = [TopicPartition(self._source.topic, pid) for pid in partition_ids]
        await self._consumer.pause(tps)
        logger.debug('partitions_paused', category='kafka', partitions=partition_ids)

    async def resume(self, partition_ids: list[int]) -> None:
        """Resume consuming from previously paused partitions."""
        tps = [TopicPartition(self._source.topic, pid) for pid in partition_ids]
        await self._consumer.resume(tps)
        logger.debug('partitions_resumed', category='kafka', partitions=partition_ids)

    async def get_total_lag(self, partition_ids: list[int]) -> int:
        """Get total consumer lag across all partitions.

        The sum of ``get_partition_lag``, so the dashboard total and the
        partitions table agree, also when a query fails.
        """
        partition_lag = await self.get_partition_lag(partition_ids)
        return sum(row['lag'] for row in partition_lag.values())

    async def get_partition_lag(self, partition_ids: list[int]) -> dict[int, dict]:
        """Get committed offset, high watermark, and lag for each partition.

        Batches the committed() call and parallelizes watermark lookups
        to avoid O(N) sequential Kafka RPCs.

        ``committed`` is ``None`` when neither the broker nor this worker
        knows a committed offset. Lag then counts from where the consumer
        starts reading, never from offset 0: an unknown committed offset
        must not read as a backlog of the partition's whole history.
        """
        if not partition_ids:
            return {}

        broker_committed = await self._fetch_committed(partition_ids)
        watermarks = await asyncio.gather(*[self._fetch_watermarks(pid) for pid in partition_ids])

        result: dict[int, dict] = {}
        for pid, (low, high) in zip(partition_ids, watermarks, strict=True):
            committed = self._committed_offset(pid, broker_committed)
            start = committed if committed is not None else self._start_offset(low, high)
            result[pid] = {
                'committed': committed,
                'high_watermark': high,
                'lag': max(0, high - start),
            }
        return result

    async def _fetch_committed(self, partition_ids: list[int]) -> dict[int, int]:
        """Committed offsets the broker reports, by partition, from one batched call.

        A partition is absent when the broker has no committed offset for it
        or answers with a per-partition error; a failed call returns an empty
        map. Every failure is logged and counted.
        """
        tps = [TopicPartition(self._source.topic, pid) for pid in partition_ids]
        try:
            rows = await self._consumer.committed(tps, timeout=LAG_QUERY_TIMEOUT_SECONDS)
        except Exception as e:
            _report_lag_query_failure(op='committed', error=e)
            return {}

        committed: dict[int, int] = {}
        for row in rows:
            if row.error is not None:
                _report_lag_query_failure(op='committed', error=row.error, partition=row.partition)
            elif row.offset >= 0:
                committed[row.partition] = row.offset
        return committed

    async def _fetch_watermarks(self, partition_id: int) -> tuple[int, int]:
        """Low and high watermark of one partition; ``(0, 0)`` when the query fails."""
        try:
            tp = TopicPartition(self._source.topic, partition_id)
            low, high = await self._consumer.get_watermark_offsets(tp, timeout=LAG_QUERY_TIMEOUT_SECONDS)
        except Exception as e:
            _report_lag_query_failure(op='watermark', error=e, partition=partition_id)
            return 0, 0
        return low, high

    def _committed_offset(self, partition_id: int, broker_committed: dict[int, int]) -> int | None:
        """Best known committed offset of a partition, or ``None`` when nothing is known.

        The larger of the broker's answer and this worker's own last commit.
        Commits of one partition are serialized and only move forward, so the
        larger value is the current one; the local value also covers a
        partition that the broker's answer lacked.
        """
        candidates = (broker_committed.get(partition_id), self._offsets_committed_here.get(partition_id))
        return max((offset for offset in candidates if offset is not None), default=None)

    def _start_offset(self, low: int, high: int) -> int:
        """Where the consumer starts reading a partition that has no committed offset."""
        return high if self._starts_at_high_watermark else low

    async def close(self) -> None:
        """Close the consumer."""
        await self._consumer.close()
