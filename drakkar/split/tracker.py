"""The job tracker: follows the results log for the source partitions this worker owns.

One consumer without group membership reads, for each followed source partition,
the results partition with the same number. After an assignment it restores the
job table of the partition from the committed position up to the end it saw at
that moment, and sends no event while it does; then it reads live and tells its
listener when a job is complete, timed out, or opened again by another worker.
The positions are committed under a group id that never has members. The tracker
calls no handler hook itself.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol, cast

import structlog
from confluent_kafka import TopicPartition
from confluent_kafka.aio import AIOConsumer

from drakkar.config.connections import KafkaConnectionConfig
from drakkar.kafka_security import merge_client_config
from drakkar.metrics import (
    split_jobs_open,
    split_oldest_job_age,
    split_restore_duration,
    split_restoring_partitions,
    split_results_ignored,
    split_results_received,
)
from drakkar.providers.resilience import RateLimitedLog, describe_error
from drakkar.split.records import LogEntry, parse_log_record
from drakkar.split.table import MS_PER_SECOND, Applied, IgnoreReason, JobState, JobTable

logger = structlog.get_logger()

POSITION_COMMIT_SECONDS = 30
TIMEOUT_CHECK_SECONDS = 5
BATCH_SIZE = 500
POLL_TIMEOUT_SECONDS = 1.0
QUERY_TIMEOUT_SECONDS = 5.0
CONSUMER_ERROR_PAUSE_SECONDS = 1.0
RESTORE_BACKOFF_FIRST_SECONDS = 1.0
RESTORE_BACKOFF_MAX_SECONDS = 30.0
# Kafka gives a record without a known time the timestamp -1.
NO_TIMESTAMP = -1

TimeoutKind = Literal['job', 'idle']
SleepFn = Callable[[float], Awaitable[None]]


class JobEvents(Protocol):
    """What the tracker tells its listener. Each call runs on the event loop and must return at once."""

    def job_complete(self, partition: int, job: JobState) -> None:
        """Every part of ``job`` has a result."""

    def job_timed_out(self, partition: int, job: JobState, kind: TimeoutKind) -> None:
        """``job`` passed its job timeout (``job``) or its idle timeout (``idle``)."""

    def job_reopened_elsewhere(self, partition: int, job: JobState) -> None:
        """Another worker instance started a new attempt of a job this worker had open."""


@dataclass
class _Followed:
    """What the tracker knows about one followed partition."""

    partition: int
    table: JobTable
    committed_source: int | None
    started_at: float
    restoring: bool = True
    assigned: bool = False
    # Batches of the read loop that started before the assignment may carry records of an older one.
    first_batch: int = 0
    next_offset: int = 0
    restore_end: int = 0
    records: int = 0
    restore_done: asyncio.Future[None] | None = None
    restore_task: asyncio.Task[JobTable] | None = None
    last_committed_position: int | None = None
    timed_out: set[tuple[int, str, TimeoutKind]] = field(default_factory=set)


def _timestamp_ms(message: Any, fallback_ms: int) -> int:
    """The Kafka timestamp of a message in milliseconds, or ``fallback_ms`` when it has none."""
    _kind, timestamp = message.timestamp()
    return fallback_ms if timestamp == NO_TIMESTAMP else timestamp


def _restore_backoff(attempt: int) -> float:
    """The wait after failed restore attempt number ``attempt``: 1, 2, 4, ... seconds, at most 30."""
    return min(RESTORE_BACKOFF_FIRST_SECONDS * 2 ** (attempt - 1), RESTORE_BACKOFF_MAX_SECONDS)


class JobTracker:
    """Keeps the job table of every followed source partition current from the results log."""

    def __init__(
        self,
        *,
        connection: KafkaConnectionConfig,
        results_topic: str,
        results_group: str,
        source_group: str,
        instance: str,
        job_timeout: int,
        idle_timeout: int,
        events: JobEvents,
        consumer_factory: Callable[[dict], Any] = AIOConsumer,
        clock: Callable[[], float] = time.time,
        sleep: SleepFn = asyncio.sleep,
    ) -> None:
        self._config = merge_client_config(
            {
                'bootstrap.servers': connection.brokers,
                'group.id': results_group,
                'enable.auto.commit': False,
                'auto.offset.reset': 'earliest',
            },
            connection.security,
            connection.client_config,
        )
        self._topic = results_topic
        self._source_group = source_group
        self._instance = instance
        self._job_timeout = job_timeout
        self._idle_timeout = idle_timeout
        self._events = events
        self._consumer_factory = consumer_factory
        self._clock = clock
        self._sleep = sleep
        self._consumer: Any = None
        self._followed: dict[int, _Followed] = {}
        self._tasks: set[asyncio.Task[Any]] = set()
        self._batch = 0
        self._commit_pending: set[int] = set()
        self._started = False
        self._stopped = False
        self._rate_limited = RateLimitedLog(time.monotonic)

    # --- lifecycle ---

    async def start(self) -> None:
        """Create the consumer and start the read loop, the position commits and the timeout check."""
        if self._started or self._stopped:
            return
        self._consumer = self._consumer_factory(self._config)
        self._started = True
        self._spawn(self._read_loop())
        self._spawn(self._periodic(POSITION_COMMIT_SECONDS, self._commit_changed_positions))
        self._spawn(self._periodic(TIMEOUT_CHECK_SECONDS, self._check_timeouts_async))

    async def stop(self) -> None:
        """Stop every loop, end pending ``follow`` calls with a cancellation, and close the consumer."""
        if self._stopped:
            return
        self._stopped = True
        self._followed.clear()
        tasks = list(self._tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if self._consumer is not None:
            try:
                await self._consumer.close()
            except Exception as exc:
                logger.warning('split_tracker_close_failed', category='split', error=describe_error(exc))
        split_restoring_partitions.set(0)
        split_jobs_open.set(0)
        split_oldest_job_age.set(0)

    # --- public queries ---

    def table(self, partition: int) -> JobTable | None:
        """The job table of a followed partition, or None while it restores or when it is not followed."""
        state = self._followed.get(partition)
        return None if state is None or state.restoring else state.table

    def is_restoring(self, partition: int) -> bool:
        """True while the job table of ``partition`` is being restored."""
        state = self._followed.get(partition)
        return state is not None and state.restoring

    # --- follow and release ---

    async def follow(self, partition: int, *, committed_source_offset: int | None) -> JobTable:
        """Assign results partition ``partition``, restore its job table, and return the table once it is ready.

        ``committed_source_offset`` is the committed offset of the source partition; jobs below it retire
        at the end of the restore. A failed restore is tried again with a backoff of 1 to 30 seconds.

        Raises:
            asyncio.CancelledError: ``release(partition)`` or ``stop()`` was called first.
            RuntimeError: the tracker was not started.
        """
        if self._stopped:
            raise asyncio.CancelledError
        if not self._started:
            raise RuntimeError('the job tracker is not started')
        if partition in self._followed:
            await self.release(partition)
        state = _Followed(
            partition=partition,
            table=self._new_table(partition),
            committed_source=committed_source_offset,
            started_at=time.monotonic(),
        )
        self._followed[partition] = state
        self._update_restoring_gauge()
        state.restore_task = self._spawn(self._restore(state))
        try:
            return await state.restore_task
        except asyncio.CancelledError:
            # The caller gave up on the restore: the partition is dropped, as by a release.
            if self._followed.get(partition) is state:
                self._forget(state)
                self._spawn(self._unassign(state))
            raise

    async def release(self, partition: int) -> None:
        """Stop following ``partition``: end its restore, unassign it and drop its table."""
        state = self._followed.get(partition)
        if state is None or self._stopped:
            return
        self._forget(state)
        task = state.restore_task
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await self._unassign(state)

    def note_source_commit(self, partition: int, offset: int) -> None:
        """Take note that the source consumer committed ``offset`` for ``partition``; retire the jobs below it."""
        state = self._followed.get(partition)
        if state is None or self._stopped:
            return
        state.committed_source = offset if state.committed_source is None else max(state.committed_source, offset)
        if state.restoring:
            return
        if state.table.retire_through(offset):
            self._forget_retired_timeouts(state)
            self._request_commit(state)

    # --- restore ---

    async def _restore(self, state: _Followed) -> JobTable:
        """Run restore attempts until one works; the table is live when this returns."""
        attempt = 0
        while True:
            attempt += 1
            try:
                await self._restore_attempt(state, attempt)
                return state.table
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._log_restore_failure(state.partition, exc, attempt)
                await self._reset_for_retry(state)
                await self._sleep(_restore_backoff(attempt))

    async def _restore_attempt(self, state: _Followed, attempt: int) -> None:
        """Assign the partition at its committed position and wait until the read loop reaches the end."""
        tp = TopicPartition(self._topic, state.partition)
        rows = await self._consumer.committed([tp], timeout=QUERY_TIMEOUT_SECONDS)
        low, high = await self._consumer.get_watermark_offsets(tp, timeout=QUERY_TIMEOUT_SECONDS)
        committed = rows[0].offset if rows and rows[0].offset >= 0 else None
        # A committed position below the low watermark points at records that expired.
        position = low if committed is None else max(committed, low)
        state.last_committed_position = committed
        state.next_offset = position
        state.restore_end = high
        state.records = 0
        state.restore_done = asyncio.get_running_loop().create_future()
        logger.info(
            'split_restore_started',
            category='split',
            partition=state.partition,
            position=position,
            end=high,
            attempt=attempt,
        )
        state.first_batch = self._batch
        # Marked first, so a release or a retry unassigns also an assignment that a cancellation cut short.
        state.assigned = True
        await self._consumer.incremental_assign([TopicPartition(self._topic, state.partition, position)])
        if position >= high:
            self._finish_restore(state)
        await state.restore_done

    async def _reset_for_retry(self, state: _Followed) -> None:
        """Unassign a partition whose restore failed and start its table again from nothing."""
        await self._unassign(state)
        state.table = self._new_table(state.partition)
        state.restore_done = None

    def _finish_restore(self, state: _Followed) -> None:
        """End the restore: retire committed jobs, go live, and report the jobs that are complete."""
        if state.committed_source is not None:
            state.table.retire_through(state.committed_source)
        state.restoring = False
        seconds = time.monotonic() - state.started_at
        split_restore_duration.observe(seconds)
        self._update_restoring_gauge()
        logger.info(
            'split_restore_finished',
            category='split',
            partition=state.partition,
            records=state.records,
            jobs=len(state.table),
            seconds=round(seconds, 3),
        )
        if state.restore_done is not None and not state.restore_done.done():
            state.restore_done.set_result(None)
        if state.last_committed_position != state.table.position(state.next_offset):
            self._request_commit(state)
        for job in state.table.open_jobs():
            if job.is_complete:
                self._emit('job_complete', state.partition, job)

    def _fail_restores(self, exc: BaseException) -> None:
        """Make every restore that waits for the read loop fail with ``exc``, so it starts again."""
        for state in self._followed.values():
            done = state.restore_done
            if state.restoring and done is not None and not done.done():
                done.set_exception(exc)

    def _log_restore_failure(self, partition: int, exc: BaseException, attempt: int) -> None:
        suppressed = self._rate_limited.admit(f'restore:{partition}')
        if suppressed is not None:
            logger.warning(
                'split_restore_failed',
                category='split',
                partition=partition,
                error=describe_error(exc),
                attempt=attempt,
                suppressed=suppressed,
            )

    # --- the read loop ---

    async def _read_loop(self) -> None:
        """Read the followed partitions and apply every record to its table; never ends on an error."""
        while True:
            batch = self._batch
            self._batch += 1
            try:
                messages = await self._consumer.consume(num_messages=BATCH_SIZE, timeout=POLL_TIMEOUT_SECONDS)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._fail_restores(exc)
                self._log_consumer_error(describe_error(exc))
                await self._sleep(CONSUMER_ERROR_PAUSE_SECONDS)
                continue
            had_error = False
            for message in messages:
                if message.error():
                    had_error = True
                    self._log_consumer_error(str(message.error()))
                    continue
                self._handle_message(message, batch)
            if had_error:
                await self._sleep(CONSUMER_ERROR_PAUSE_SECONDS)
            else:
                await asyncio.sleep(0)

    def _log_consumer_error(self, error: str) -> None:
        suppressed = self._rate_limited.admit('consume')
        if suppressed is not None:
            logger.warning('split_tracker_consume_failed', category='split', error=error, suppressed=suppressed)

    def _handle_message(self, message: Any, batch: int) -> None:
        """Apply one consumed record to the table of its partition, if the partition is still followed."""
        state = self._followed.get(message.partition())
        if state is None or not state.assigned or batch < state.first_batch or message.topic() != self._topic:
            return
        offset = message.offset()
        if offset < state.next_offset:
            return
        entry = parse_log_record(message.headers(), message.value())
        state.next_offset = offset + 1
        if state.restoring:
            state.records += 1
            if entry is not None:
                self._apply(state, entry, offset, message)
            if state.next_offset >= state.restore_end:
                self._finish_restore(state)
            return
        if entry is None:
            split_results_ignored.labels(reason=IgnoreReason.MALFORMED.value).inc()
            return
        self._apply_live(state, entry, offset, message)

    def _apply(self, state: _Followed, entry: LogEntry, offset: int, message: Any) -> Applied:
        now_ms = int(self._clock() * MS_PER_SECOND)
        return state.table.apply(entry, offset=offset, timestamp_ms=_timestamp_ms(message, now_ms))

    def _apply_live(self, state: _Followed, entry: LogEntry, offset: int, message: Any) -> None:
        """Apply a live record and send the events it causes."""
        held_here = self._held_by_this_instance(state, entry)
        applied = self._apply(state, entry, offset, message)
        if applied.ignored is not None:
            split_results_ignored.labels(reason=applied.ignored.value).inc()
            return
        # A record the table did not ignore always names its job.
        job = cast(JobState, applied.job)
        if entry.kind == 'result':
            split_results_received.inc()
        if applied.completed:
            self._emit('job_complete', state.partition, job)
        if applied.opened and held_here and job.instance != self._instance:
            self._emit('job_reopened_elsewhere', state.partition, job)
        if applied.closed:
            self._forget_job_timeouts(state, job.offset)
            self._retire_stray_closed(state, job)

    def _held_by_this_instance(self, state: _Followed, entry: LogEntry) -> bool:
        """True when ``entry`` is an ``open`` of a job that is open under this tracker's instance."""
        if entry.kind != 'open':
            return False
        job = state.table.job(entry.ref.offset)
        return job is not None and job.closed is None and job.instance == self._instance

    def _retire_stray_closed(self, state: _Followed, job: JobState) -> None:
        """A ``closed`` of a job below the committed source offset must not hold the position back."""
        committed = state.committed_source
        if committed is not None and job.offset < committed and state.table.retire_through(committed):
            self._request_commit(state)

    # --- events ---

    def _emit(self, name: str, *args: Any) -> None:
        """Call one listener method; an exception from it is logged and does not stop the tracker."""
        try:
            getattr(self._events, name)(*args)
        except Exception as exc:
            suppressed = self._rate_limited.admit(f'listener:{name}')
            if suppressed is not None:
                logger.error(
                    'split_tracker_listener_failed',
                    category='split',
                    listener=name,
                    error=describe_error(exc),
                    suppressed=suppressed,
                )

    # --- positions ---

    def _request_commit(self, state: _Followed) -> None:
        """Schedule a position commit for ``state`` soon; at most one commit task waits at a time."""
        if self._stopped:
            return
        first = not self._commit_pending
        self._commit_pending.add(state.partition)
        if first:
            self._spawn(self._commit_requested())

    async def _commit_requested(self) -> None:
        await asyncio.sleep(0)
        partitions, self._commit_pending = self._commit_pending, set()
        await self._commit_positions([p for p in partitions if p in self._followed])

    async def _commit_changed_positions(self) -> None:
        await self._commit_positions(list(self._followed))

    async def _commit_positions(self, partitions: list[int]) -> None:
        """Commit the position of each live partition whose position differs from its last commit."""
        planned: list[tuple[_Followed, int]] = []
        for partition in sorted(partitions):
            state = self._followed.get(partition)
            if state is None or state.restoring:
                continue
            position = state.table.position(state.next_offset)
            if position != state.last_committed_position:
                planned.append((state, position))
        if not planned:
            return
        offsets = [TopicPartition(self._topic, state.partition, position) for state, position in planned]
        try:
            await self._consumer.commit(offsets=offsets, asynchronous=False)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            suppressed = self._rate_limited.admit('commit')
            if suppressed is not None:
                logger.warning(
                    'split_position_commit_failed',
                    category='split',
                    partitions=[state.partition for state, _ in planned],
                    error=describe_error(exc),
                    suppressed=suppressed,
                )
            return
        for state, position in planned:
            if self._followed.get(state.partition) is state:
                state.last_committed_position = position

    # --- timeouts ---

    async def _check_timeouts_async(self) -> None:
        self._check_timeouts()

    def _check_timeouts(self) -> None:
        """Send ``job_timed_out`` once per job, attempt and kind; set the job gauges from the live tables."""
        now_ms = int(self._clock() * MS_PER_SECOND)
        open_count = 0
        oldest_ms: int | None = None
        for state in list(self._followed.values()):
            if state.restoring:
                continue
            open_jobs = state.table.open_jobs()
            open_count += len(open_jobs)
            for job in open_jobs:
                oldest_ms = job.opened_at_ms if oldest_ms is None else min(oldest_ms, job.opened_at_ms)
            current = {(job.offset, job.attempt_id) for job in open_jobs}
            state.timed_out = {key for key in state.timed_out if key[:2] in current}
            due = state.table.due(now_ms=now_ms, job_timeout=self._job_timeout, idle_timeout=self._idle_timeout)
            for job, kind in due:
                key = (job.offset, job.attempt_id, kind)
                if key in state.timed_out:
                    continue
                state.timed_out.add(key)
                self._emit('job_timed_out', state.partition, job, kind)
        split_jobs_open.set(open_count)
        split_oldest_job_age.set(0 if oldest_ms is None else max(now_ms - oldest_ms, 0) / MS_PER_SECOND)

    def _forget_job_timeouts(self, state: _Followed, offset: int) -> None:
        state.timed_out = {key for key in state.timed_out if key[0] != offset}

    def _forget_retired_timeouts(self, state: _Followed) -> None:
        state.timed_out = {key for key in state.timed_out if state.table.job(key[0]) is not None}

    # --- helpers ---

    async def _periodic(self, seconds: float, work: Callable[[], Awaitable[None]]) -> None:
        """Run ``work`` every ``seconds``; an error is logged and the loop goes on."""
        while True:
            await self._sleep(seconds)
            try:
                await work()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error('split_tracker_periodic_failed', category='split', error=describe_error(exc))

    def _new_table(self, partition: int) -> JobTable:
        return JobTable(partition=partition, group=self._source_group)

    def _forget(self, state: _Followed) -> None:
        """Drop all memory of a followed partition at once, so no later record of it sends an event."""
        if self._followed.get(state.partition) is state:
            del self._followed[state.partition]
        self._commit_pending.discard(state.partition)
        self._update_restoring_gauge()

    async def _unassign(self, state: _Followed) -> None:
        """Take the partition off the consumer; a failure is logged."""
        if not state.assigned:
            return
        state.assigned = False
        try:
            await self._consumer.incremental_unassign([TopicPartition(self._topic, state.partition)])
        except Exception as exc:
            logger.warning(
                'split_tracker_unassign_failed',
                category='split',
                partition=state.partition,
                error=describe_error(exc),
            )

    def _update_restoring_gauge(self) -> None:
        split_restoring_partitions.set(sum(1 for state in self._followed.values() if state.restoring))

    def _spawn(self, coro: Coroutine[Any, Any, Any]) -> asyncio.Task[Any]:
        """Start a task the tracker owns; ``stop`` cancels it."""
        task = asyncio.get_running_loop().create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task
