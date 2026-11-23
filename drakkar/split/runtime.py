"""What one worker holds for the split feature: the producer, the job tracker, the owners, the resolved names,
and, with ``split.http.enabled``, the status follower of HTTP jobs."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING, Any

import structlog
from confluent_kafka import Producer
from confluent_kafka.aio import AIOConsumer

from drakkar.config import DrakkarConfig
from drakkar.split.http_follower import StatusFollower
from drakkar.split.http_status import StatusMap
from drakkar.split.owner import OwnerHost, OwnerSettings, SplitOwner
from drakkar.split.producer import SplitProducer
from drakkar.split.reader import results_consumer_factory
from drakkar.split.table import JobState, JobTable
from drakkar.split.topics import (
    check_split_topics,
    check_status_topic,
    topic_cleanup_policy,
    topic_partition_counts,
    topic_retention_ms,
)
from drakkar.split.tracker import JobTracker, TimeoutKind

if TYPE_CHECKING:
    from drakkar.sinks.manager import CollectCallback, SinkManager

logger = structlog.get_logger()

# The longest time the stop gives the producer to deliver what it still holds.
PRODUCER_CLOSE_SECONDS = 5.0
# The shortest: a stop whose time is spent still sends what is ready.
MIN_PRODUCER_CLOSE_SECONDS = 0.5

TopicCounts = Callable[[], Awaitable[Mapping[str, int]]]
TopicRetention = Callable[[str], Awaitable[int | None]]
TopicCleanupPolicy = Callable[[str], Awaitable[str | None]]


def _close_seconds(deadline: float) -> float:
    """The time a producer gets to deliver what it still holds: what is left to ``deadline`` (monotonic), bounded."""
    return min(max(deadline - time.monotonic(), MIN_PRODUCER_CLOSE_SECONDS), PRODUCER_CLOSE_SECONDS)


class HttpJobsRuntime:
    """What a worker holds for split jobs that arrive over HTTP.

    The status map and its follower read the status topic on the split
    connection. ``source_producer`` writes the request into the source topic:
    the split producer when the source and the split topics share their
    connection, else a producer of its own on the source connection.
    """

    def __init__(
        self,
        config: DrakkarConfig,
        *,
        split_connection_producer: SplitProducer,
        group: str,
        producer_factory: Callable[[dict], Any],
        consumer_factory: Callable[[dict], Any],
        clock: Callable[[], float],
        sleep: Callable[[float], Awaitable[None]],
    ) -> None:
        http = config.split.http
        self.status_topic = http.status_topic
        self.source_topic = config.sources.kafka.topic
        self.result_retention = http.result_retention
        self.max_response = http.max_response
        self.deliver_sinks_default = config.sources.http.deliver_to_sinks
        self.status_map = StatusMap(http.result_retention)
        split_connection = config.declared_connection('split.connection', 'kafka', config.split.connection)
        self.follower = StatusFollower(
            connection=split_connection,
            topic=self.status_topic,
            group=group,
            status_map=self.status_map,
            consumer_factory=consumer_factory,
            clock=clock,
            sleep=sleep,
        )
        # The request must reach the cluster of the source topic, which can be another one than that of the split topics.
        self.owns_source_producer = config.sources.kafka.connection != config.split.connection
        if self.owns_source_producer:
            source_connection = config.declared_connection(
                'sources.kafka.connection', 'kafka', config.sources.kafka.connection
            )
            self.source_producer = SplitProducer(source_connection, producer_factory=producer_factory)
        else:
            self.source_producer = split_connection_producer

    async def start(self) -> None:
        """Start the source producer when it is one of its own, and the follower."""
        if self.owns_source_producer:
            await self.source_producer.start()
        self.follower.start()

    async def stop(self) -> None:
        """Stop the follower."""
        await self.follower.stop()

    async def close_source_producer(self, deadline: float) -> None:
        """Close the source producer when it is one of its own, with the time left to ``deadline`` (monotonic)."""
        if self.owns_source_producer:
            await self.source_producer.close(_close_seconds(deadline))


class _OwnerEvents:
    """Hands each event of the job tracker to the owner of its partition.

    A partition has no owner from the start of its restore until its
    processor is ready, and an event of a partition with no owner is
    dropped. Nothing is lost by that: an owner acts on what the job table
    says when it takes hold of a message, so it needs no event that came
    before.
    """

    def __init__(self) -> None:
        self._owners: dict[int, SplitOwner] = {}

    def register(self, partition: int, owner: SplitOwner) -> None:
        """Give ``owner`` every later event of its partition."""
        self._owners[partition] = owner

    def forget(self, partition: int) -> None:
        """The partition is no longer followed: drop its owner."""
        self._owners.pop(partition, None)

    def clear(self) -> None:
        """Drop every owner."""
        self._owners.clear()

    def owners(self) -> dict[int, SplitOwner]:
        """The registered owners, by partition."""
        return dict(self._owners)

    def _send(self, partition: int, deliver: Callable[[SplitOwner], None]) -> None:
        owner = self._owners.get(partition)
        if owner is not None:
            deliver(owner)

    def job_complete(self, partition: int, job: JobState) -> None:
        """Every part of ``job`` has a result."""
        self._send(partition, lambda owner: owner.job_complete(job))

    def job_timed_out(self, partition: int, job: JobState, kind: TimeoutKind) -> None:
        """``job`` passed its job timeout or its idle timeout."""
        self._send(partition, lambda owner: owner.job_timed_out(job, kind))

    def attempt_opened(self, partition: int, job: JobState) -> None:
        """A new attempt became the current one of ``job``."""
        self._send(partition, lambda owner: owner.attempt_opened(job))

    def attempt_cancelled(self, partition: int, job: JobState) -> None:
        """The current attempt of ``job`` was cancelled."""
        self._send(partition, lambda owner: owner.attempt_cancelled(job))

    def attempt_stalled(self, partition: int, job: JobState) -> None:
        """Another instance's split of ``job`` shows no end after the grace time."""
        self._send(partition, lambda owner: owner.attempt_stalled(job))

    def job_closed(self, partition: int, job: JobState) -> None:
        """``job`` got its ``closed`` record."""
        self._send(partition, lambda owner: owner.job_closed(job))

    def log_read(self, partition: int) -> None:
        """The tracker read on in the results log of ``partition``."""
        self._send(partition, lambda owner: owner.log_read())

    def record_failed(self, partition: int, offset: int) -> None:
        """The tracker could not apply the record at ``offset`` of the results log of ``partition``."""
        self._send(partition, lambda owner: owner.record_failed(offset))


class SplitRuntime:
    """The split feature of one worker.

    Built once, after ``on_startup``, when ``split.enabled`` is true and the
    handler splits. The parts source reads its topic, its group and the
    producer of the part results from here. The source of the source topic
    asks it to follow the results partition of each partition it owns, and
    takes from it the owner of the split jobs of that partition.
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
        cleanup_policy: TopicCleanupPolicy | None = None,
        consumer_factory: Callable[[dict], Any] = AIOConsumer,
        results_reader: Callable[[], Any] | None = None,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        http_enabled: bool = False,
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
        # The partition count of the parts topic, read once at the start: the partition of each part needs it.
        self.parts_partition_count = 0
        self._metadata = metadata if metadata is not None else topic_partition_counts(self.connection)
        self._retention = retention if retention is not None else topic_retention_ms(self.connection)
        self._cleanup_policy = cleanup_policy if cleanup_policy is not None else topic_cleanup_policy(self.connection)
        self._results_reader = (
            results_reader
            if results_reader is not None
            else results_consumer_factory(self.connection, self.results_group)
        )
        self._clock = clock
        self._sleep = sleep
        self._events = _OwnerEvents()
        self.tracker = JobTracker(
            connection=self.connection,
            results_topic=self.results_topic,
            results_group=self.results_group,
            source_group=self.source_group,
            instance=instance,
            job_timeout=config.split.job_timeout,
            idle_timeout=config.split.idle_timeout,
            events=self._events,
            consumer_factory=consumer_factory,
            clock=clock,
            sleep=sleep,
        )
        # The start of the job tracker, made by the first ``follow``; None until then.
        self._tracker_start: asyncio.Task[None] | None = None
        # HTTP split jobs: only when the app turned them on before ``on_startup`` and the config still has them.
        self.http: HttpJobsRuntime | None = None
        if http_enabled and config.split.http.enabled:
            self.http = HttpJobsRuntime(
                config,
                split_connection_producer=self.producer,
                group=self.results_group,
                producer_factory=producer_factory,
                consumer_factory=consumer_factory,
                clock=clock,
                sleep=sleep,
            )

    async def start(self) -> None:
        """Start the producer and check the topics; with HTTP jobs, also start following the status topic.

        The job tracker starts with the first source partition it follows: a
        worker that owns no source partition reads no results. The restore
        of the status map runs in the background.

        Raises:
            ConfigurationError: a topic does not exist, or the results topic has another
                partition count than the source topic.
        """
        await self.producer.start()
        counts = await self._metadata()

        async def read_counts() -> Mapping[str, int]:
            return counts

        warnings = await check_split_topics(self._config, metadata=read_counts, retention=self._retention)
        self.parts_partition_count = counts[self.parts_topic]
        if self.http is not None:
            warnings += await check_status_topic(
                self.http.status_topic,
                result_retention=self.http.result_retention,
                metadata=read_counts,
                retention=self._retention,
                cleanup_policy=self._cleanup_policy,
            )
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
        if self.http is not None:
            await self.http.start()
            await logger.ainfo(
                'split_http_enabled',
                category='split',
                status_topic=self.http.status_topic,
                result_retention_seconds=self.http.result_retention,
            )

    def owners(self) -> dict[int, SplitOwner]:
        """The owners of the split jobs, by source partition; a copy, read-only."""
        return self._events.owners()

    async def follow(self, partition: int, *, committed_source_offset: int | None) -> JobTable:
        """Restore the split jobs of a source partition this worker now owns; the table when it is ready.

        ``committed_source_offset`` is the committed offset of the source
        partition, or None when it is not known. A failed restore is tried
        again by the tracker for as long as it takes.

        Raises:
            asyncio.CancelledError: the partition was released, or the worker stops.
        """
        if self._tracker_start is None:
            # One start for every caller: partitions assigned together follow at the same time.
            self._tracker_start = asyncio.ensure_future(self.tracker.start())
        await asyncio.shield(self._tracker_start)
        # The owner of an earlier assignment of the partition gets no event of this one.
        self._events.forget(partition)
        return await self.tracker.follow(partition, committed_source_offset=committed_source_offset)

    def owner_for(
        self,
        partition: int,
        *,
        table: JobTable,
        host: OwnerHost,
        handler: Any,
        sink_manager: SinkManager,
        on_collect: CollectCallback,
        on_dlq_failure: str,
        partition_label: int | None = None,
    ) -> SplitOwner:
        """Build the owner of the split jobs of a source partition on its restored table.

        The events of the job tracker for the partition go to this owner
        from here on, until :meth:`release`. ``host`` is the processor of
        the partition; ``on_collect`` delivers the answer of ``aggregate``.
        """
        settings = OwnerSettings(
            producer=self.producer,
            source_group=self.source_group,
            parts_topic=self.parts_topic,
            results_topic=self.results_topic,
            parts_partition_count=self.parts_partition_count,
            worker=self.worker,
            instance=self.instance,
            max_attempts=self._config.split.max_attempts,
            results_reader=self._results_reader,
            read_position=self.tracker.read_position,
            end_offset=self.tracker.end_offset,
            job_timeout=self._config.split.job_timeout,
            idle_timeout=self._config.split.idle_timeout,
            clock=self._clock,
            sleep=self._sleep,
        )
        owner = SplitOwner(
            settings,
            partition=partition,
            table=table,
            host=host,
            handler=handler,
            sink_manager=sink_manager,
            on_collect=on_collect,
            on_dlq_failure=on_dlq_failure,
            partition_label=partition_label,
        )
        self._events.register(partition, owner)
        return owner

    async def release(self, partition: int) -> None:
        """Forget the owner of a source partition this worker lost, and stop following its results partition."""
        self._events.forget(partition)
        if self._tracker_start is not None:
            await self.tracker.release(partition)

    def note_source_commit(self, partition: int, offset: int) -> None:
        """The source consumer committed ``offset`` (the next offset to read) for ``partition``: jobs below it retire."""
        if self._tracker_start is not None:
            self.tracker.note_source_commit(partition, offset)

    async def stop(self, deadline: float) -> None:
        """Stop the status follower and the job tracker, then close the producers.

        What a producer still holds gets the time left to ``deadline``
        (monotonic), within bounds.
        """
        self._events.clear()
        try:
            if self.http is not None:
                await self.http.stop()
            if self._tracker_start is not None:
                await self.tracker.stop()
        finally:
            try:
                await self.producer.close(_close_seconds(deadline))
            finally:
                if self.http is not None:
                    await self.http.close_source_producer(deadline)
