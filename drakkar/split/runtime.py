"""What one worker holds for the split feature: the producer and the resolved names of its topics and groups."""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

import structlog
from confluent_kafka import Producer

from drakkar.config import DrakkarConfig
from drakkar.split.producer import SplitProducer
from drakkar.split.topics import check_split_topics, topic_partition_counts, topic_retention_ms

logger = structlog.get_logger()

# The longest time the stop gives the producer to deliver what it still holds.
PRODUCER_CLOSE_SECONDS = 5.0
# The shortest: a stop whose time is spent still sends what is ready.
MIN_PRODUCER_CLOSE_SECONDS = 0.5

TopicCounts = Callable[[], Awaitable[Mapping[str, int]]]
TopicRetention = Callable[[str], Awaitable[int | None]]


class SplitRuntime:
    """The split feature of one worker.

    Built once, after ``on_startup``, when ``split.enabled`` is true and the
    handler splits. The parts source reads its topic, its group and the
    producer of the part results from here.
    """

    def __init__(
        self,
        config: DrakkarConfig,
        *,
        worker: str,
        instance: str,
        producer_factory: Callable[[dict], Any] = Producer,
        metadata: TopicCounts | None = None,
        retention: TopicRetention | None = None,
    ) -> None:
        self._config = config
        self.worker = worker
        self.instance = instance
        self.connection = config.declared_connection('split.connection', 'kafka', config.split.connection)
        self.parts_topic = config.resolved_split_parts_topic
        self.results_topic = config.resolved_split_results_topic
        self.parts_group = config.resolved_split_parts_group
        self.results_group = config.resolved_split_results_group
        # The consumer group of the source: every split record carries it, and a record of another group is skipped.
        self.source_group = config.resolved_consumer_group
        self.producer = SplitProducer(self.connection, producer_factory=producer_factory)
        self._metadata = metadata if metadata is not None else topic_partition_counts(self.connection)
        self._retention = retention if retention is not None else topic_retention_ms(self.connection)

    async def start(self) -> None:
        """Start the producer and check the two topics.

        Raises:
            ConfigurationError: a topic does not exist, or the results topic has another
                partition count than the source topic.
        """
        await self.producer.start()
        warnings = await check_split_topics(self._config, metadata=self._metadata, retention=self._retention)
        for detail in warnings:
            await logger.awarning('split_topic_retention_short', category='split', detail=detail)
        await logger.ainfo(
            'split_enabled',
            category='split',
            parts_topic=self.parts_topic,
            results_topic=self.results_topic,
            parts_group=self.parts_group,
            results_group=self.results_group,
        )

    async def stop(self, deadline: float) -> None:
        """Close the producer; what it still holds gets the time left to ``deadline`` (monotonic), within bounds."""
        time_left = deadline - time.monotonic()
        await self.producer.close(min(max(time_left, MIN_PRODUCER_CLOSE_SECONDS), PRODUCER_CLOSE_SECONDS))
