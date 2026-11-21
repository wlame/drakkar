"""The job tracker: follows the results log for the source partitions this worker owns.

One consumer without group membership reads, for each followed source partition,
the results partition with the same number. After an assignment it restores the
job table of the partition from the committed position up to the end it saw at
that moment, and sends no event while it does; then it reads live and tells its
listener when a job is complete, timed out or closed, and when an attempt of a
job is opened, cancelled, or shows no end of its split for too long.
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
from confluent_kafka import KafkaException, TopicPartition
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
from drakkar.split.table import (
    MS_PER_SECOND,
    Applied,
    IgnoreReason,
    JobState,
    JobTable,
)

logger = structlog.get_logger()

POSITION_COMMIT_SECONDS = 30
TIMEOUT_CHECK_SECONDS = 5
BATCH_SIZE = 500
POLL_TIMEOUT_SECONDS = 1.0
QUERY_TIMEOUT_SECONDS = 5.0
CONSUMER_ERROR_PAUSE_SECONDS = 0.5
LOOP_RESTART_SECONDS = 3.0
RESTORE_BACKOFF_FIRST_SECONDS = 1.0
RESTORE_BACKOFF_MAX_SECONDS = 30.0
# How often a restore that gets no record asks the broker for the end of its partition again.
RESTORE_END_CHECK_SECONDS = 1.0
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

    def attempt_opened(self, partition: int, job: JobState) -> None:
        """An ``open`` record made a new attempt the current one of ``job``; ``job.instance`` wrote it."""

    def attempt_cancelled(self, partition: int, job: JobState) -> None:
        """The current attempt of ``job`` got ``split-cancelled``."""

    def attempt_stalled(self, partition: int, job: JobState) -> None:
        """The current attempt of ``job``, of another instance, shows no end of its split after the grace time."""

    def job_closed(self, partition: int, job: JobState) -> None:
        """``job`` got its ``closed`` record."""

    def log_read(self, partition: int) -> None:
        """The tracker read on in the live log of ``partition``: ``read_position`` may have moved."""


@dataclass
class _Followed:
    """What the tracker knows about one followed partition."""

    partition: int
    table: JobTable
    committed_source: int | None
    started_at: float
    restoring: bool = True
    assigned: bool = False
    assign_complete: bool = False
    # A batch of the read loop that started before the assignment may carry records of an older
    # assignment; its records are dropped, and the partition is moved back to ``next_offset``.
    first_batch: int = 0
    reposition: bool = False
    # ``next_offset`` moves only past a record that was applied, or skipped because it lies below it, or to
    # the consumer's position at the end of a restore (offsets that hold no record a consumer returns). It
    # never jumps over offsets that may still hold records: while a seek back is pending, every record of
    # the partition is dropped and leaves it where it is.
    next_offset: int = 0
    restore_end: int = 0
    records: int = 0
    last_end_check: float = 0.0
    position_check_due: bool = False
    end_check_task: asyncio.Task[None] | None = None
    restore_done: asyncio.Future[None] | None = None
    restore_task: asyncio.Task[JobTable] | None = None
    last_committed_position: int | None = None
    timed_out: set[tuple[int, str, TimeoutKind]] = field(default_factory=set)
    # The attempts already reported as stalled: source offset and attempt id.
    stalled: set[tuple[int, str]] = field(default_factory=set)


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
        self._unassigning: dict[int, asyncio.Task[None]] = {}
        self._tasks: set[asyncio.Task[Any]] = set()
        self._batch = 0
        self._commit_pending: set[int] = set()
        self._commit_lock = asyncio.Lock()
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
        self._spawn(self._supervised('read', self._read_loop))
        self._spawn(self._supervised('commit', lambda: self._periodic(POSITION_COMMIT_SECONDS, self._commit_changed)))
        self._spawn(
            self._supervised('timeout', lambda: self._periodic(TIMEOUT_CHECK_SECONDS, lambda: self._check_timeouts()))
        )

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

    def read_position(self, partition: int) -> int | None:
        """The next offset the tracker reads in the live results log of ``partition``.

        Every record below it is in the job table. None for a partition that
        is not followed or is still restoring. A writer that knows the offset
        of its own record can tell from this whether the table has it.
        """
        state = self._followed.get(partition)
        if state is None or state.restoring:
            return None
        return state.next_offset

    async def end_offset(self, partition: int) -> int | None:
        """The end of the results log of ``partition`` as the broker has it now: the offset its next record gets.

        Every record that was acknowledged before the call lies below it. So
        a writer whose delivery report named no offset can still tell when
        the table has its record: once the tracker has read up to this end.
        None for a partition that is not followed.

        Raises:
            KafkaException: the broker did not answer.
        """
        if partition not in self._followed:
            return None
        _low, high = await self._consumer.get_watermark_offsets(
            TopicPartition(self._topic, partition), timeout=QUERY_TIMEOUT_SECONDS
        )
        return high

    def is_restoring(self, partition: int) -> bool:
        """True while the job table of ``partition`` is being restored."""
        state = self._followed.get(partition)
        return state is not None and state.restoring

    # --- follow and release ---

    async def follow(self, partition: int, *, committed_source_offset: int | None) -> JobTable:
        """Assign results partition ``partition``, restore its job table, and return the table once it is ready.

        ``committed_source_offset`` is the committed offset of the source partition; jobs below it retire
        at the end of the restore. A failed restore is tried again with a backoff of 1 to 30 seconds. A
        ``follow`` of a partition that is followed already replaces the earlier one, whose call then
        raises ``asyncio.CancelledError``.

        Raises:
            asyncio.CancelledError: ``release(partition)`` or ``stop()`` was called first.
            RuntimeError: the tracker was not started.
        """
        if not self._started and not self._stopped:
            raise RuntimeError('the job tracker is not started')
        # A release awaits, so another follow of the partition can start meanwhile; replace it too.
        while partition in self._followed and not self._stopped:
            await self.release(partition)
        if self._stopped:
            raise asyncio.CancelledError
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
                self._start_unassign(state)
            raise

    async def release(self, partition: int) -> None:
        """Stop following ``partition``: end its restore, unassign it and drop its table."""
        state = self._followed.get(partition)
        if state is None or self._stopped:
            return
        self._forget(state)
        task = state.restore_task
        if task is not None and not task.done():
            await asyncio.wait({task})
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
        committed = self._committed_offset(rows)
        position = self._start_position(state.partition, committed, low, high)
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
        await self._wait_for_unassign(state.partition)
        state.first_batch = self._batch
        # Marked first, so a release or a retry unassigns also an assignment that a cancellation cut short.
        state.assigned = True
        await self._consumer.incremental_assign([TopicPartition(self._topic, state.partition, position)])
        state.assign_complete = True
        if state.reposition:
            await self._seek_back(state)
        if position >= high:
            self._finish_restore(state)
        await state.restore_done

    @staticmethod
    def _committed_offset(rows: list[TopicPartition]) -> int | None:
        """The committed offset of the one row ``committed`` gave; None when the group has none.

        Raises:
            KafkaException: the row carries an error, so the committed offset is not known.
        """
        row = rows[0] if rows else None
        if row is None:
            return None
        if row.error is not None:
            raise KafkaException(row.error)
        return row.offset if row.offset >= 0 else None

    def _start_position(self, partition: int, committed: int | None, low: int, high: int) -> int:
        """Where a restore reads from: the committed position, kept inside the records the log holds."""
        if committed is None:
            return low
        if committed > high:
            # The topic was created again or cut: offsets above the end would skip every new record.
            logger.warning(
                'split_restore_position_reset',
                category='split',
                partition=partition,
                committed=committed,
                low=low,
                high=high,
            )
            return low
        # A committed position below the low watermark points at records that expired.
        return max(committed, low)

    async def _reset_for_retry(self, state: _Followed) -> None:
        """Unassign a partition whose restore failed and start its table again from nothing."""
        await self._unassign(state)
        state.table = self._new_table(state.partition)
        state.restore_done = None
        state.reposition = False

    def _finish_restore(self, state: _Followed) -> None:
        """End the restore: retire committed jobs, go live, and report the jobs that are complete."""
        if not state.restoring:
            return
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

    def _start_end_checks(self, seen: set[int]) -> None:
        """Ask the broker for the end again for each restore that got no record in the last batch."""
        now = time.monotonic()
        for state in self._followed.values():
            waiting = state.restoring and state.assign_complete and state.restore_done is not None
            idle = state.partition not in seen and now - state.last_end_check >= RESTORE_END_CHECK_SECONDS
            running = state.end_check_task is not None and not state.end_check_task.done()
            if waiting and idle and not running:
                state.last_end_check = now
                state.end_check_task = self._spawn(self._check_restore_end(state))

    async def _check_restore_end(self, state: _Followed) -> None:
        """End a restore whose next offset reached the end of the partition as it is now.

        The end read at the assignment can fall (the log was cut); then no record will ever reach it.
        """
        try:
            _low, high = await self._consumer.get_watermark_offsets(
                TopicPartition(self._topic, state.partition), timeout=QUERY_TIMEOUT_SECONDS
            )
        except Exception as exc:
            self._log_limited(
                'warning',
                f'end:{state.partition}',
                'split_restore_end_check_failed',
                partition=state.partition,
                error=exc,
            )
            return
        if self._followed.get(state.partition) is not state or not state.restoring:
            return
        state.restore_end = min(state.restore_end, high)
        if state.next_offset >= state.restore_end:
            self._finish_restore(state)
        else:
            # Offsets below the end may hold no record a consumer returns; the read loop asks for the position.
            state.position_check_due = True

    async def _check_position(self, state: _Followed) -> None:
        """Finish a restore whose consumer position is at or past the end of the restore.

        Called by the read loop between batches, so every record the consumer returned has been handled,
        and never while a seek back is pending.
        """
        state.position_check_due = False
        try:
            rows = await self._consumer.position([TopicPartition(self._topic, state.partition)])
            row = rows[0]
            if getattr(row, 'error', None) is not None:
                raise KafkaException(row.error)
        except Exception as exc:
            self._log_limited(
                'warning',
                f'position:{state.partition}',
                'split_restore_position_check_failed',
                partition=state.partition,
                error=exc,
            )
            return
        if row.offset < 0:
            # The client knows no position from an assignment or a seek until it has returned a record of
            # the partition. So a restore whose whole remaining range holds no record a consumer returns
            # still waits; a producer that is not transactional writes no such offsets.
            return
        current = self._followed.get(state.partition) is state and state.restoring and not state.reposition
        if current and row.offset >= state.restore_end:
            state.next_offset = max(state.next_offset, row.offset)
            self._finish_restore(state)

    def _log_restore_failure(self, partition: int, exc: BaseException, attempt: int) -> None:
        self._log_limited(
            'warning', f'restore:{partition}', 'split_restore_failed', partition=partition, error=exc, attempt=attempt
        )

    # --- the read loop ---

    async def _read_loop(self) -> None:
        """Read the followed partitions and apply every record to its table, batch after batch."""
        while True:
            await self._read_once()

    async def _read_once(self) -> None:
        """One batch: consume, apply, move back what a batch across an assignment dropped, check idle restores."""
        batch = self._batch
        self._batch += 1
        try:
            messages = await self._consumer.consume(num_messages=BATCH_SIZE, timeout=POLL_TIMEOUT_SECONDS)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._fail_restores(exc)
            self._log_limited('warning', 'consume', 'split_tracker_consume_failed', error=exc)
            await self._sleep(CONSUMER_ERROR_PAUSE_SECONDS)
            return
        had_error = False
        seen: set[int] = set()
        for message in messages:
            if message.error():
                had_error = True
                self._log_limited('warning', 'consume', 'split_tracker_consume_failed', error=message.error())
                continue
            seen.add(message.partition())
            self._handle_message_safely(message, batch)
        for partition in sorted(seen):
            state = self._followed.get(partition)
            if state is not None and not state.restoring:
                # Once for a batch: whoever waits for the table to hold a record it wrote looks again.
                self._emit('log_read', partition)
        for state in list(self._followed.values()):
            if state.reposition and state.assign_complete:
                await self._seek_back(state)
        for state in list(self._followed.values()):
            ready = state.restoring and state.assign_complete and not state.reposition
            if state.position_check_due and ready and state.partition not in seen:
                await self._check_position(state)
        self._start_end_checks(seen)
        if had_error:
            await self._sleep(CONSUMER_ERROR_PAUSE_SECONDS)
        else:
            await asyncio.sleep(0)

    def _handle_message_safely(self, message: Any, batch: int) -> None:
        """Handle one record; an unexpected error is logged and the next record is handled."""
        try:
            self._handle_message(message, batch)
        except Exception as exc:
            self._log_limited(
                'error',
                'record',
                'split_tracker_record_failed',
                partition=message.partition(),
                offset=message.offset(),
                error=exc,
            )

    def _handle_message(self, message: Any, batch: int) -> None:
        """Apply one consumed record to the table of its partition, if the partition is still followed."""
        state = self._followed.get(message.partition())
        if state is None or not state.assigned or message.topic() != self._topic:
            return
        offset = message.offset()
        if offset < state.next_offset:
            return
        if batch < state.first_batch or state.reposition:
            # The record may belong to the new assignment, and the consumer has moved past what came
            # before it; the partition is moved back to ``next_offset`` and read again from there.
            state.reposition = True
            return
        state.next_offset = offset + 1
        try:
            self._apply_record(state, offset, message)
        finally:
            if state.restoring and state.next_offset >= state.restore_end:
                self._finish_restore(state)

    def _apply_record(self, state: _Followed, offset: int, message: Any) -> None:
        entry = parse_log_record(message.headers(), message.value())
        if state.restoring:
            state.records += 1
            if entry is not None:
                self._apply(state, entry, offset, message)
            return
        if entry is None:
            split_results_ignored.labels(reason=IgnoreReason.MALFORMED.value).inc()
            return
        self._apply_live(state, entry, offset, message)

    async def _seek_back(self, state: _Followed) -> None:
        """Move the partition back to ``next_offset``; records read again below it are skipped.

        ``reposition`` stays set until the seek worked, so no record moves ``next_offset`` meanwhile.
        """
        if self._followed.get(state.partition) is not state or not state.assigned:
            return
        # A batch that started before the seek can still carry records from the old position.
        seek_batch = self._batch
        try:
            await self._consumer.seek(TopicPartition(self._topic, state.partition, state.next_offset))
        except Exception as exc:
            self._log_limited(
                'warning', f'seek:{state.partition}', 'split_tracker_seek_failed', partition=state.partition, error=exc
            )
            return
        state.first_batch = max(state.first_batch, seek_batch)
        state.reposition = False

    def _apply(self, state: _Followed, entry: LogEntry, offset: int, message: Any) -> Applied:
        now_ms = int(self._clock() * MS_PER_SECOND)
        return state.table.apply(
            entry, offset=offset, timestamp_ms=_timestamp_ms(message, now_ms), applied_at_ms=now_ms
        )

    def _apply_live(self, state: _Followed, entry: LogEntry, offset: int, message: Any) -> None:
        """Apply a live record and send the events it causes."""
        applied = self._apply(state, entry, offset, message)
        if applied.ignored is not None:
            split_results_ignored.labels(reason=applied.ignored.value).inc()
            return
        # A record the table did not ignore always names its job.
        job = cast(JobState, applied.job)
        if entry.kind == 'result':
            split_results_received.inc()
        if applied.opened:
            self._emit('attempt_opened', state.partition, job)
        if entry.kind == 'split-cancelled':
            self._emit('attempt_cancelled', state.partition, job)
        if applied.completed:
            self._emit('job_complete', state.partition, job)
        if applied.closed:
            self._forget_job_timeouts(state, job.offset)
            self._emit('job_closed', state.partition, job)
            self._retire_stray_closed(state, job)

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
            self._log_limited('error', f'listener:{name}', 'split_tracker_listener_failed', listener=name, error=exc)

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

    async def _commit_changed(self) -> None:
        await self._commit_positions(list(self._followed))

    async def _commit_positions(self, partitions: list[int]) -> None:
        """Commit the position of each live partition whose position differs from its last commit."""
        async with self._commit_lock:
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
                partitions_failed = [state.partition for state, _ in planned]
                self._log_limited(
                    'warning', 'commit', 'split_position_commit_failed', partitions=partitions_failed, error=exc
                )
                return
            for state, position in planned:
                if self._followed.get(state.partition) is state:
                    state.last_committed_position = position

    # --- timeouts ---

    async def _check_timeouts(self) -> None:
        """Send ``job_timed_out`` once per job, attempt and kind while that timeout lasts; set the job gauges.

        The same pass sends ``attempt_stalled`` once for each attempt of
        another instance that shows no end of its split after the grace time.
        """
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
            due = state.table.due(now_ms=now_ms, job_timeout=self._job_timeout, idle_timeout=self._idle_timeout)
            # A timeout is told once for as long as it lasts. A job that got a new result is not
            # idle any more; when it is idle again, that is told again.
            state.timed_out &= {(job.offset, job.attempt_id, kind) for job, kind in due}
            for job, kind in due:
                key = (job.offset, job.attempt_id, kind)
                if key in state.timed_out:
                    continue
                state.timed_out.add(key)
                self._emit('job_timed_out', state.partition, job, kind)
            state.stalled &= current
            for job in open_jobs:
                attempt = (job.offset, job.attempt_id)
                if attempt not in state.stalled and self._is_stalled(job, now_ms):
                    state.stalled.add(attempt)
                    self._emit('attempt_stalled', state.partition, job)
        split_jobs_open.set(open_count)
        split_oldest_job_age.set(0 if oldest_ms is None else max(now_ms - oldest_ms, 0) / MS_PER_SECOND)

    def _is_stalled(self, job: JobState, now_ms: int) -> bool:
        """Whether the current attempt of ``job`` is a split of another instance that is taken for dead."""
        return job.instance != self._instance and job.is_split_left_behind(now_ms)

    def _forget_job_timeouts(self, state: _Followed, offset: int) -> None:
        state.timed_out = {key for key in state.timed_out if key[0] != offset}

    def _forget_retired_timeouts(self, state: _Followed) -> None:
        state.timed_out = {key for key in state.timed_out if state.table.job(key[0]) is not None}
        state.stalled = {key for key in state.stalled if state.table.job(key[0]) is not None}

    # --- helpers ---

    async def _supervised(self, name: str, run: Callable[[], Awaitable[None]]) -> None:
        """Run a loop of the tracker; when it ends on an error, log it and start it again after a pause."""
        while True:
            try:
                await run()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error('split_tracker_loop_failed', category='split', loop=name, error=describe_error(exc))
            await self._sleep(LOOP_RESTART_SECONDS)

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

    def _log_limited(self, level: str, key: str, event: str, *, error: object, **fields: Any) -> None:
        """Log a line that can repeat, at most once per key and interval; ``error`` is an exception or a text."""
        suppressed = self._rate_limited.admit(key)
        if suppressed is None:
            return
        text = describe_error(error) if isinstance(error, BaseException) else str(error)
        getattr(logger, level)(event, category='split', error=text, suppressed=suppressed, **fields)

    def _new_table(self, partition: int) -> JobTable:
        return JobTable(partition=partition, group=self._source_group)

    def _forget(self, state: _Followed) -> None:
        """Drop all memory of a followed partition at once, so no later record of it sends an event.

        Its restore is cancelled, so no restore task stays waiting for a read loop that ignores it.
        """
        if self._followed.get(state.partition) is state:
            del self._followed[state.partition]
        self._commit_pending.discard(state.partition)
        for task in (state.restore_task, state.end_check_task):
            if task is not None and not task.done() and task is not asyncio.current_task():
                task.cancel()
        self._update_restoring_gauge()

    def _start_unassign(self, state: _Followed) -> None:
        """Unassign in a task of its own; the next ``follow`` of the partition waits for it."""
        if not state.assigned:
            return
        task = self._spawn(self._unassign(state))
        self._unassigning[state.partition] = task
        task.add_done_callback(lambda done: self._drop_unassign(state.partition, done))

    def _drop_unassign(self, partition: int, task: asyncio.Task[Any]) -> None:
        if self._unassigning.get(partition) is task:
            del self._unassigning[partition]

    async def _wait_for_unassign(self, partition: int) -> None:
        """Wait for an unassign of ``partition`` that still runs, so it cannot undo the next assignment."""
        pending = self._unassigning.get(partition)
        if pending is not None and not pending.done():
            await asyncio.wait({pending})

    async def _unassign(self, state: _Followed) -> None:
        """Take the partition off the consumer; a failure is logged.

        An assignment that a cancellation or an error cut short may never
        have reached the client (the call waited for a thread of the
        consumer, and its cancel took it out of the queue). The unassign is
        still sent, because the assignment may have happened; its failure
        then only says that there was nothing to take off, and is logged at
        debug level. Nothing tries an unassign again: the partition is
        assigned anew by the next ``follow``.
        """
        if not state.assigned:
            return
        was_complete = state.assign_complete
        state.assigned = False
        state.assign_complete = False
        try:
            await self._consumer.incremental_unassign([TopicPartition(self._topic, state.partition)])
        except Exception as exc:
            if not was_complete:
                logger.debug(
                    'split_tracker_unassign_not_needed',
                    category='split',
                    partition=state.partition,
                    error=describe_error(exc),
                )
                return
            logger.warning(
                'split_tracker_unassign_failed',
                category='split',
                partition=state.partition,
                error=describe_error(exc),
            )

    def _update_restoring_gauge(self) -> None:
        split_restoring_partitions.set(sum(1 for state in self._followed.values() if state.restoring))

    def _spawn(self, coro: Coroutine[Any, Any, Any]) -> asyncio.Task[Any]:
        """Start a task the tracker owns; ``stop`` cancels it, and an error that ends it is logged."""
        task = asyncio.get_running_loop().create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._task_done)
        return task

    def _task_done(self, task: asyncio.Task[Any]) -> None:
        self._tasks.discard(task)
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            logger.error('split_tracker_task_failed', category='split', task=task.get_name(), error=describe_error(exc))
