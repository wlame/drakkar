"""What one worker holds for the split feature: the producer, the job tracker, the owners and the resolved names."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING, Any

import structlog
from confluent_kafka import Producer
from confluent_kafka.aio import AIOConsumer

from drakkar.config import DrakkarConfig
from drakkar.split.owner import OwnerHost, OwnerSettings, SplitOwner
from drakkar.split.producer import SplitProducer
from drakkar.split.reader import results_consumer_factory
from drakkar.split.table import JobState, JobTable
from drakkar.split.topics import check_split_topics, topic_partition_counts, topic_retention_ms
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


class _OwnerEvents:
    """Hands each event of the job tracker to the owner of its partition.

    A partition has no owner from the start of its restore until its
    processor is ready. The tracker goes live at the end of the restore, a
    few loop turns before the owner exists, and it tells a timeout only
    once. So the events of a partition that is being followed are kept and
    given to its owner when it registers. An event of a partition that is
    neither followed nor owned is dropped.
    """

    def __init__(self) -> None:
        self._owners: dict[int, SplitOwner] = {}
        self._kept: dict[int, list[Callable[[SplitOwner], None]]] = {}

    def expect(self, partition: int) -> None:
        """A restore of ``partition`` starts: keep its events until its owner registers."""
        self._owners.pop(partition, None)
        self._kept[partition] = []

    def register(self, partition: int, owner: SplitOwner) -> None:
        """Give ``owner`` the events kept for its partition, and every later one."""
        self._owners[partition] = owner
        for deliver in self._kept.pop(partition, []):
            deliver(owner)

    def forget(self, partition: int) -> None:
        """The partition is no longer followed: drop its owner and what was kept for it."""
        self._owners.pop(partition, None)
        self._kept.pop(partition, None)

    def clear(self) -> None:
        """Drop every owner and every kept event."""
        self._owners.clear()
        self._kept.clear()

    def owners(self) -> dict[int, SplitOwner]:
        """The registered owners, by partition."""
        return dict(self._owners)

    def _send(self, partition: int, deliver: Callable[[SplitOwner], None]) -> None:
        owner = self._owners.get(partition)
        if owner is not None:
            deliver(owner)
        elif partition in self._kept:
            self._kept[partition].append(deliver)

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
        consumer_factory: Callable[[dict], Any] = AIOConsumer,
        results_reader: Callable[[], Any] | None = None,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
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

    async def start(self) -> None:
        """Start the producer and check the two topics.

        The job tracker starts with the first source partition it follows: a
        worker that owns no source partition reads no results.

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
        # From here on no event of the partition is lost: the owner gets them when it registers.
        self._events.expect(partition)
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

        The events of the job tracker for the partition go to this owner,
        until :meth:`release`: first the ones that came since the restore
        ended, then every later one. ``host`` is the processor of the
        partition; ``on_collect`` delivers the answer of ``aggregate``.
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
        """Stop the job tracker, then close the producer.

        What the producer still holds gets the time left to ``deadline``
        (monotonic), within bounds.
        """
        self._events.clear()
        try:
            if self._tracker_start is not None:
                await self.tracker.stop()
        finally:
            time_left = deadline - time.monotonic()
            await self.producer.close(min(max(time_left, MIN_PRODUCER_CLOSE_SECONDS), PRODUCER_CLOSE_SECONDS))
