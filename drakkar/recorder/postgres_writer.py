"""Stage P: the writer that takes chunks from the P queue and COPYs them into the recorder events table.

Stage P runs on a thread of its own with its own event loop
(:class:`PostgresLoopThread`), which owns the one-connection write pool. A
COPY encodes every record of a chunk on the loop that runs it, and on a slow
CPU that work alone can hold a loop long enough to delay the worker's own
pipeline. The main loop therefore only posts runs to the thread and waits for
their results; it never builds a record or runs a COPY.

The writer logs with the plain (synchronous) structlog calls, not the
``a``-prefixed ones. Those exist to keep a log call off the main event loop;
on this thread they would only hand each line to a pool thread that nobody
joins, so a line could still be in flight after the stop returned.

One run (:meth:`PostgresWriter.run_once`):

1. start when not started yet: open the pool, then make the table available;
   a failure ends the run and the next run tries again;
2. the partition upkeep when it is due; its outcome never ends the run. It
   creates the partitions ahead, drops the hours past the stored retention,
   and with ``recorder.postgres.max_size`` above 0 drops the oldest hours
   before the previous one while the table is larger than that;
3. until the slot and the queue are empty: form a chunk into the slot when it
   is empty, and COPY it. A written chunk empties the slot and the loop goes
   on, so a backlog drains as fast as Postgres takes it; any other outcome
   ends the run;
4. set ``drakkar_recorder_postgres_queue_rows`` to the rows of the queue plus
   the slot and ``drakkar_recorder_postgres_queue_bytes`` to the text of the
   queue, and publish a new :class:`WriterStatus`.

A chunk is dropped only after ``POSTGRES_CHUNK_FINAL_FAILURES`` final COPY
errors: an error that its rows cause (a data or integrity error of the server,
or a value the client cannot encode). Any other failure keeps it: a transient
failure, any other server error (counted as a failed operation, so the breaker
opens as in an outage), an open breaker or a writer that has not started. A
missing table (42P01) on a COPY or in the upkeep makes the next run start the
writer again, which creates the table when the schema is managed. At stop,
:meth:`PostgresWriter.final` makes one attempt per chunk within the time left
and drops what is left with ``shutdown``.

The writer publishes what it did as one immutable :class:`WriterStatus`,
replaced after each chunk, each upkeep and each run; another thread (the API
server) reads the reference and never touches the writer itself.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import threading
import time
from collections.abc import Awaitable, Callable, Coroutine, Sequence
from dataclasses import dataclass, field, replace
from enum import StrEnum
from typing import Any

import asyncpg
import structlog

from drakkar.config.connections import PostgresConnectionConfig
from drakkar.metrics import (
    recorder_postgres_dropped_rows,
    recorder_postgres_partitions_dropped,
    recorder_postgres_queue_bytes,
    recorder_postgres_queue_rows,
    recorder_postgres_rows_written,
)
from drakkar.providers import postgres as postgres_providers
from drakkar.providers.postgres import (
    COPY_COLUMNS,
    COPY_STATEMENT_TIMEOUT,
    PARTITION_DDL_TEMPLATES,
    PARTITION_MAINTENANCE_SECONDS,
    PARTITIONS_AHEAD_HOURS,
    POSTGRES_CHUNK_FINAL_FAILURES,
    POSTGRES_CHUNK_MAX_BYTES,
    POSTGRES_PROVIDER,
    RECORDER_FEATURE,
    RECORDER_STATEMENT_TEMPLATES,
    SECONDS_PER_HOUR,
    SET_LOCAL_VALUES,
    SETTING_LOCK_TIMEOUT,
    SETTING_SYNCHRONOUS_COMMIT,
    SQLSTATE_CHECK_VIOLATION,
    SQLSTATE_UNDEFINED_TABLE,
    SQLSTATE_UNIQUE_VIOLATION,
    STATEMENT_TIMEOUT_SETTING,
    borrow_transaction,
    ensure_recorder_table,
    is_recorder_final_sqlstate,
    parse_partition_hour,
    parse_retention_comment,
    partition_bounds,
    partition_name,
    render_recorder,
)
from drakkar.providers.resilience import OUTCOME_NOT_STARTED, RateLimitedLog, ResilientCaller, describe_error
from drakkar.recorder.postgres import (
    PendingChunk,
    PostgresQueue,
    RecorderPostgresSettings,
    build_record,
    row_text_bytes,
)

logger = structlog.get_logger()

LOG_CATEGORY = 'recorder'
DROP_REASON_REJECTED = 'rejected'
DROP_REASON_NO_PARTITION = 'no_partition'
DROP_REASON_SHUTDOWN = 'shutdown'
PARTITION_DROP_REASON_RETENTION = 'retention'
PARTITION_DROP_REASON_SIZE = 'size'
COPY_REJECTED_LOG_KEY = 'copy_rejected'
# How long the thread's loop gets to finish the coroutines it cancels when it
# ends. Shorter than the least wait of a stop for the thread, so a thread that
# only drains is never reported as one that did not end.
LOOP_DRAIN_SECONDS = 0.05
# What the client raises for a record value it cannot encode for its column:
# a wrong type, a number out of range, or its data error (a ``ValueError``).
# A broken connection is not one of them; it stays a transient error.
CLIENT_ENCODE_ERRORS: tuple[type[Exception], ...] = (ValueError, TypeError, OverflowError)

OpenPool = Callable[[PostgresConnectionConfig], Awaitable[Any]]
CopyAnswer = tuple['CopyOutcome', str | None, str]


class CopyOutcome(StrEnum):
    """What one COPY of a chunk came to."""

    WRITTEN = 'written'
    ALREADY_WRITTEN = 'already_written'
    KEPT = 'kept'
    REJECTED = 'rejected'


WRITTEN_OUTCOMES = frozenset((CopyOutcome.WRITTEN, CopyOutcome.ALREADY_WRITTEN))
# The upkeep keeps the partitions of the hour before the current one up to
# PARTITIONS_AHEAD_HOURS after it, so a row of a slightly late clock still has one.
PARTITIONS_BEHIND_HOURS = 1


@dataclass(frozen=True)
class UpkeepActions:
    """What one upkeep changes, each list ascending.

    ``create`` holds (name, from, to) of the partitions to create, ``drop``
    the partitions past the stored retention, and ``drop_for_size`` the
    partitions dropped to bring the table under ``max_size``.
    """

    create: list[tuple[str, int, int]]
    drop: list[str]
    drop_for_size: list[str] = field(default_factory=list)

    def __bool__(self) -> bool:
        return bool(self.create or self.drop or self.drop_for_size)


def upkeep_actions(table: str, now: float, stored_retention: int | None, listed: Sequence[str]) -> UpkeepActions:
    """Return the partitions an upkeep at server time ``now`` creates and drops.

    It creates every missing partition of the hours ``H - 1`` to
    ``H + PARTITIONS_AHEAD_HOURS``, where ``H`` is the hour of ``now``. With a
    stored retention ``R`` it drops every listed partition whose hour ended
    ``R`` hours before ``now`` or earlier; without one it drops nothing. A
    listed name that is not a partition name of ``table`` is left alone.
    """
    listed_hours = {hour for name in listed if (hour := parse_partition_hour(table, name)) is not None}
    current_hour = int(now // SECONDS_PER_HOUR)
    create = [
        (partition_name(table, hour), *partition_bounds(hour))
        for hour in range(current_hour - PARTITIONS_BEHIND_HOURS, current_hour + PARTITIONS_AHEAD_HOURS + 1)
        if hour not in listed_hours
    ]
    if stored_retention is None:
        return UpkeepActions(create=create, drop=[])
    expired_before = now - stored_retention * SECONDS_PER_HOUR
    drop = [
        partition_name(table, hour) for hour in sorted(listed_hours) if (hour + 1) * SECONDS_PER_HOUR <= expired_before
    ]
    return UpkeepActions(create=create, drop=drop)


def measured_partitions(table: str, listed: Sequence[str], dropped: Sequence[str]) -> list[str]:
    """Return the listed partitions of ``table`` that the retention keeps, ascending: the names the size cap measures.

    A listed name that is not a partition name of ``table`` is not measured,
    so a table of another shape never makes this one drop its history.
    """
    gone = set(dropped)
    return sorted(name for name in listed if parse_partition_hour(table, name) is not None and name not in gone)


def size_drops(table: str, now: float, measured: Sequence[str], sizes: dict[str, int], max_size: int) -> list[str]:
    """Return the oldest partitions to drop, ascending, so the table gets to at most ``max_size`` bytes.

    ``sizes`` is the answer of ``partition_sizes`` for ``measured``; a name
    it does not hold counts as 0. While the total of ``measured`` is above
    ``max_size``, the oldest hour goes. The hour before the hour of ``now``
    and every later hour are never dropped: the upkeep keeps that previous
    hour for rows of a clock that is a little late, and a cap that dropped
    it would make the next upkeep create it again. So the cap can leave the
    table above ``max_size`` by up to two hours of writes. ``max_size`` 0
    drops nothing.
    """
    if max_size == 0:
        return []
    first_kept_hour = int(now // SECONDS_PER_HOUR) - PARTITIONS_BEHIND_HOURS
    total = sum(sizes.get(name, 0) for name in measured)
    candidates = sorted(
        (hour, name)
        for name in measured
        if (hour := parse_partition_hour(table, name)) is not None and hour < first_kept_hour
    )
    drop: list[str] = []
    for _, name in candidates:
        if total <= max_size:
            break
        drop.append(name)
        total -= sizes.get(name, 0)
    return drop


@dataclass(frozen=True)
class UpkeepStatus:
    """The last upkeep a writer ran: when (epoch seconds), whether it held the table's try-lock, and its changes.

    A writer that lost the try-lock changed nothing, so its lists are empty.
    """

    at: float
    held_lock: bool
    created: tuple[str, ...] = ()
    dropped: tuple[str, ...] = ()
    dropped_for_size: tuple[str, ...] = ()


@dataclass(frozen=True)
class WriterStatus:
    """What one writer did since the process started, as one immutable value safe to read from any thread.

    ``slot_rows`` and ``slot_bytes`` are the rows and the text of the chunk
    in flight (the queue holds the rest), counted as the queue counts them;
    ``rows_written`` and the three ``dropped_*`` counts are the rows of
    the writer's own outcomes. Rows the queue drops as ``queue_full`` are
    counted by the recorder, which appends to the queue.
    """

    started: bool = False
    slot_rows: int = 0
    slot_bytes: int = 0
    rows_written: int = 0
    dropped_rejected: int = 0
    dropped_no_partition: int = 0
    dropped_shutdown: int = 0
    breaker_open: bool = False
    last_upkeep: UpkeepStatus | None = None


@dataclass(frozen=True)
class PostgresStageStatus:
    """Stage P of one recorder as the UI shows it: the writer's status and the queue in front of it.

    ``queue_rows`` and ``queue_bytes`` are the rows and the text of the queue
    alone (the chunk in flight is ``writer.slot_rows`` and ``writer.slot_bytes``); ``dropped_queue_full``
    counts the rows the queue dropped since the process started.
    """

    writer: WriterStatus
    queue_rows: int
    queue_bytes: int
    dropped_queue_full: int


# The ``WriterStatus`` field that counts the rows dropped for each reason.
_DROPPED_FIELDS = {
    DROP_REASON_REJECTED: 'dropped_rejected',
    DROP_REASON_NO_PARTITION: 'dropped_no_partition',
    DROP_REASON_SHUTDOWN: 'dropped_shutdown',
}


class PostgresLoopThread:
    """A daemon thread that runs its own event loop; stage P and its pool live there.

    Other loops post coroutines with :meth:`submit` or :meth:`run`. A daemon,
    so a thread that never answers cannot keep the process from exiting: the
    stop waits for it with a bound and then leaves it.
    """

    def __init__(self, name: str) -> None:
        self._loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self._serve, name=name, daemon=True)
        self.thread.start()

    def _serve(self) -> None:
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_forever()
            # Coroutines still on the loop (a call its caller gave up on) are
            # cancelled, so none is destroyed while pending.
            pending = asyncio.all_tasks(self._loop)
            for task in pending:
                task.cancel()
            if pending:
                self._loop.run_until_complete(asyncio.wait(pending, timeout=LOOP_DRAIN_SECONDS))
            self._loop.run_until_complete(self._loop.shutdown_asyncgens())
            self._loop.run_until_complete(self._loop.shutdown_default_executor())
        finally:
            self._loop.close()

    def submit(self, factory: Callable[[], Coroutine[Any, Any, Any]]) -> concurrent.futures.Future:
        """Run ``factory()`` on the thread's loop; cancelling the returned future cancels the coroutine."""
        coroutine = factory()
        try:
            return asyncio.run_coroutine_threadsafe(coroutine, self._loop)
        except RuntimeError:
            # The loop is closed: the coroutine never runs, so close it here.
            coroutine.close()
            raise

    async def run(self, factory: Callable[[], Coroutine[Any, Any, Any]], timeout: float | None) -> Any:
        """Run ``factory()`` on the thread and wait at most ``timeout`` seconds for its result.

        A timeout or a cancellation of the waiting task cancels the coroutine
        on the thread.
        """
        return await asyncio.wait_for(asyncio.wrap_future(self.submit(factory)), timeout)

    async def wait(self, factory: Callable[[], Coroutine[Any, Any, Any]], timeout: float | None) -> Any:
        """Run ``factory()`` on the thread and wait at most ``timeout`` seconds; the coroutine is never cancelled.

        After a timeout the coroutine goes on to its end on the thread, for
        work that must still count what it does, such as the final stage.
        """
        future = asyncio.wrap_future(self.submit(factory))
        done, _ = await asyncio.wait({future}, timeout=timeout)
        if not done:
            raise TimeoutError
        return future.result()

    async def stop(self, timeout: float) -> bool:
        """Stop the loop and wait at most ``timeout`` seconds for the thread; False when it did not end."""
        if self.thread.is_alive():
            try:
                self._loop.call_soon_threadsafe(self._loop.stop)
            except RuntimeError:
                pass
        await asyncio.to_thread(self.thread.join, max(timeout, 0.0))
        return not self.thread.is_alive()


class PostgresWriter:
    """Stage P of one recorder: the start, the COPY of each chunk, the upkeep and the final stage at stop.

    Every method runs on one loop (the stage P thread's), one at a time:
    :meth:`run_once` and :meth:`final` take the writer's own lock.
    ``open_pool`` replaces :func:`drakkar.providers.postgres.open_pool`;
    ``None`` looks that function up when the pool opens. ``now_wall`` gives
    the epoch time of an upkeep in the status.
    """

    def __init__(
        self,
        settings: RecorderPostgresSettings,
        *,
        cluster: str,
        worker: str,
        instance: str,
        queue: PostgresQueue,
        caller: ResilientCaller,
        open_pool: OpenPool | None = None,
        now_monotonic: Callable[[], float] = time.monotonic,
        now_wall: Callable[[], float] = time.time,
    ) -> None:
        self._settings = settings
        self._cluster = cluster
        self._worker = worker
        self._instance = instance
        self._queue = queue
        self._caller = caller
        self._open_pool = open_pool
        self._now_monotonic = now_monotonic
        self._now_wall = now_wall
        self._pool: Any = None
        self._started = False
        self._slot: PendingChunk | None = None
        # The error text of the last final COPY error of the chunk in the slot.
        self._slot_error = ''
        # The text size of the chunk in the slot, as the queue counted it.
        self._slot_bytes = 0
        # True after a start and after a check violation: the next run upkeeps first.
        self._upkeep_requested = False
        self._last_upkeep_at: float | None = None
        # The retention warning is written once per process; a process has one writer.
        self._retention_mismatch_logged = False
        self._lock = asyncio.Lock()
        self._copy_rejected_log = RateLimitedLog(now_monotonic)
        self._drop_log = RateLimitedLog(now_monotonic)
        # Rows dropped per reason that no log line reported yet: a line the
        # rate limit holds back keeps its rows for the next line.
        self._unlogged_drops: dict[str, int] = {}
        # Replaced, never changed in place: another thread reads the reference.
        self._status = WriterStatus()

    @property
    def status(self) -> WriterStatus:
        """The last status the writer published; safe to read from any thread."""
        return self._status

    def _publish_status(self, **changes: Any) -> None:
        """Replace the status with one that holds the current start, slot and breaker state plus ``changes``."""
        self._status = replace(
            self._status,
            started=self._started,
            slot_rows=len(self._slot.pairs) if self._slot is not None else 0,
            slot_bytes=self._slot_bytes if self._slot is not None else 0,
            breaker_open=self._caller.breaker_open,
            **changes,
        )

    # --- the run ---

    async def start(self) -> bool:
        """Open the pool and make the table available, as one ``start`` operation; True once started."""
        result = await self._caller.call('start', self._start_attempt)
        if not result.ok:
            return False
        self._started = True
        self._upkeep_requested = True
        logger.info(
            'provider_started',
            category=LOG_CATEGORY,
            feature=RECORDER_FEATURE,
            provider=POSTGRES_PROVIDER,
        )
        return True

    async def _start_attempt(self) -> None:
        if self._pool is None:
            opener = self._open_pool if self._open_pool is not None else postgres_providers.open_pool
            # One connection: stage P runs one statement at a time, and a pool
            # replaces a connection that broke.
            entry = self._settings.connection.model_copy(update={'pool_min': 1, 'pool_max': 1})
            self._pool = await opener(entry)
        settings = self._settings
        await ensure_recorder_table(
            self._pool,
            schema=settings.schema,
            table=settings.table,
            retention_hours=settings.retention_hours,
            manage_schema=settings.connection.manage_schema,
            connection=settings.connection_name,
            prefix=settings.table_prefix,
        )

    async def run_once(self) -> None:
        """One stage P run; see the module docstring."""
        async with self._lock:
            try:
                if not self._started and not await self.start():
                    return
                if self.upkeep_due():
                    self._upkeep_requested = False
                    await self.upkeep()
                    self._last_upkeep_at = self._now_monotonic()
                while self._fill_slot():
                    if await self._write_slot() not in WRITTEN_OUTCOMES:
                        return
            finally:
                self._set_queue_gauge()

    def upkeep_due(self) -> bool:
        """True after a start or a check violation, and when the upkeep interval has passed since the last one."""
        if self._upkeep_requested:
            return True
        return (
            self._last_upkeep_at is not None
            and self._now_monotonic() - self._last_upkeep_at >= PARTITION_MAINTENANCE_SECONDS
        )

    async def upkeep(self) -> None:
        """The partition upkeep, as one ``partitions`` operation; it never raises.

        A failure is the caller's ``provider_call_failed`` line; the run goes
        on with its COPYs either way.
        """
        result = await self._caller.call('partitions', self._upkeep_attempt)
        if result.ok:
            self._publish_upkeep(result.value)
        if result.ok and result.value:
            actions: UpkeepActions = result.value
            # Counted only here, after the commit: a rolled-back upkeep dropped nothing.
            recorder_postgres_partitions_dropped.labels(reason=PARTITION_DROP_REASON_RETENTION).inc(len(actions.drop))
            recorder_postgres_partitions_dropped.labels(reason=PARTITION_DROP_REASON_SIZE).inc(
                len(actions.drop_for_size)
            )
            logger.info(
                'recorder_postgres_partitions_changed',
                category=LOG_CATEGORY,
                table=self._settings.qualified_table,
                created=sorted(name for name, _, _ in actions.create),
                dropped=sorted(actions.drop),
                dropped_for_size=sorted(actions.drop_for_size),
            )

    def _publish_upkeep(self, actions: UpkeepActions | None) -> None:
        """Publish a committed upkeep; ``None`` is one that found the try-lock held by another writer."""
        at = self._now_wall()
        if actions is None:
            self._publish_status(last_upkeep=UpkeepStatus(at=at, held_lock=False))
            return
        self._publish_status(
            last_upkeep=UpkeepStatus(
                at=at,
                held_lock=True,
                created=tuple(name for name, _, _ in actions.create),
                dropped=tuple(actions.drop),
                dropped_for_size=tuple(actions.drop_for_size),
            )
        )

    async def _upkeep_attempt(self) -> UpkeepActions | None:
        """One upkeep transaction; ``None`` when another writer holds the table's try-lock.

        The try-lock is a transaction lock, so the loser only commits. The
        server's clock sets the hours, so every writer of the table agrees on
        them whatever its own clock says. The retention drops follow the
        retention stored in the table comment, never this worker's own
        setting; the size drops follow this worker's own ``max_size``, so the
        workers that share a table should give it one value.
        """
        try:
            return await self._upkeep_statements()
        except asyncpg.PostgresError as exc:
            self._restart_when_table_missing(exc)
            raise

    async def _upkeep_statements(self) -> UpkeepActions | None:
        """The statements of one upkeep transaction; see :meth:`_upkeep_attempt`."""
        settings = self._settings
        qualified = settings.qualified_table
        async with borrow_transaction(self._pool) as conn:
            if not await conn.fetchval(RECORDER_STATEMENT_TEMPLATES['try_lock'], qualified):
                return None
            await conn.execute(
                RECORDER_STATEMENT_TEMPLATES['set_local'], SETTING_LOCK_TIMEOUT, SET_LOCAL_VALUES[SETTING_LOCK_TIMEOUT]
            )
            now = await conn.fetchval(RECORDER_STATEMENT_TEMPLATES['server_now'])
            stored = parse_retention_comment(
                await conn.fetchval(RECORDER_STATEMENT_TEMPLATES['retention_read'], qualified)
            )
            await self._check_retention(stored)
            listed = [row[0] for row in await conn.fetch(RECORDER_STATEMENT_TEMPLATES['partitions_list'], qualified)]
            actions = upkeep_actions(settings.table, now, stored, listed)
            for name, from_, to in actions.create:
                await conn.execute(
                    render_recorder(
                        PARTITION_DDL_TEMPLATES['create'],
                        schema=settings.schema,
                        table=settings.table,
                        partition=name,
                        from_=from_,
                        to=to,
                    )
                )
            await self._drop_partitions(conn, actions.drop)
            if settings.max_size > 0:
                measured = measured_partitions(settings.table, listed, actions.drop)
                rows = await conn.fetch(RECORDER_STATEMENT_TEMPLATES['partition_sizes'], settings.schema, measured)
                sizes = {row[0]: row[1] for row in rows}
                actions = replace(
                    actions, drop_for_size=size_drops(settings.table, now, measured, sizes, settings.max_size)
                )
                await self._drop_partitions(conn, actions.drop_for_size)
        return actions

    async def _drop_partitions(self, conn: Any, names: Sequence[str]) -> None:
        """Drop the partitions ``names`` in order, inside the upkeep's transaction."""
        settings = self._settings
        for name in names:
            await conn.execute(
                render_recorder(
                    PARTITION_DDL_TEMPLATES['drop'], schema=settings.schema, table=settings.table, partition=name
                )
            )

    def _restart_when_table_missing(self, exc: asyncpg.PostgresError) -> None:
        """Make the next run start the writer again after a missing-table error, so the table is made available."""
        # asyncpg sets ``sqlstate`` on each error class, but its type stubs do not declare it.
        if getattr(exc, 'sqlstate', None) == SQLSTATE_UNDEFINED_TABLE:
            self._started = False

    async def _check_retention(self, stored: int | None) -> None:
        """Warn once when the table stores no valid retention or one that differs from the setting."""
        configured = self._settings.retention_hours
        if self._retention_mismatch_logged or stored == configured:
            return
        self._retention_mismatch_logged = True
        logger.warning(
            'recorder_postgres_retention_mismatch',
            category=LOG_CATEGORY,
            table=self._settings.qualified_table,
            stored_retention_hours=stored,
            configured_retention_hours=configured,
        )

    def _fill_slot(self) -> bool:
        """Form a chunk into an empty slot; True when the slot holds one."""
        if self._slot is not None:
            return True
        pairs = self._queue.take_chunk(self._settings.batch_rows, POSTGRES_CHUNK_MAX_BYTES)
        if not pairs:
            return False
        records = [
            build_record(
                row,
                cluster=self._cluster,
                worker=self._worker,
                instance=self._instance,
                store_completed_output=self._settings.store_completed_output,
                max_output=self._settings.max_output,
            )
            for _, row in pairs
        ]
        self._slot = PendingChunk(pairs=pairs, records=records)
        self._slot_error = ''
        self._slot_bytes = sum(row_text_bytes(row) for _, row in pairs)
        return True

    async def _write_slot(self, *, attempts: int | None = None, deadline_seconds: float | None = None) -> CopyOutcome:
        """COPY the chunk in the slot as one ``copy`` operation and act on the outcome."""
        slot = self._slot
        assert slot is not None
        result = await self._caller.call(
            'copy', lambda: self._copy(slot.records), attempts=attempts, deadline_seconds=deadline_seconds
        )
        if not result.ok:
            return CopyOutcome.KEPT
        outcome, sqlstate, error = result.value
        if outcome in WRITTEN_OUTCOMES:
            recorder_postgres_rows_written.inc(len(slot.pairs))
            self._slot = None
            self._publish_status(rows_written=self._status.rows_written + len(slot.pairs))
            return outcome
        await self._reject(slot, sqlstate, error)
        return outcome

    async def _copy(self, records: list[tuple]) -> CopyAnswer:
        """One COPY attempt in one transaction; an error that the rows cause comes back as a value.

        The error is caught outside the transaction block, after its rollback.
        A data or integrity error of the server is final and no breaker
        failure. A value the client cannot encode for its column (the client
        encodes every record before it sends it) is a final error too, with
        no SQLSTATE: the same records would fail the same way on every retry.
        Every other error leaves the attempt, so the caller retries a
        transient one and counts each as a failed operation; the chunk stays.

        The transaction sets no server limit on its statements: the attempt
        time limit of the caller bounds the COPY, not a ``statement_timeout``
        set for the role or the database.
        """
        settings = self._settings
        try:
            async with borrow_transaction(self._pool) as conn:
                await conn.execute(
                    RECORDER_STATEMENT_TEMPLATES['set_local'],
                    SETTING_SYNCHRONOUS_COMMIT,
                    SET_LOCAL_VALUES[SETTING_SYNCHRONOUS_COMMIT],
                )
                await conn.execute(
                    RECORDER_STATEMENT_TEMPLATES['set_local'], STATEMENT_TIMEOUT_SETTING, COPY_STATEMENT_TIMEOUT
                )
                await conn.copy_records_to_table(
                    settings.table, schema_name=settings.schema, columns=COPY_COLUMNS, records=records
                )
        except asyncpg.PostgresError as exc:
            # asyncpg sets ``sqlstate`` on each error class, but its type stubs do not declare it.
            sqlstate = getattr(exc, 'sqlstate', None)
            if sqlstate == SQLSTATE_UNIQUE_VIOLATION:
                # An earlier attempt of this same record set landed and its answer was lost.
                return CopyOutcome.ALREADY_WRITTEN, sqlstate, ''
            if not is_recorder_final_sqlstate(sqlstate):
                self._restart_when_table_missing(exc)
                raise
            return CopyOutcome.REJECTED, sqlstate, describe_error(exc)
        except CLIENT_ENCODE_ERRORS as exc:
            return CopyOutcome.REJECTED, None, describe_error(exc)
        return CopyOutcome.WRITTEN, None, ''

    async def _reject(self, slot: PendingChunk, sqlstate: str | None, error: str) -> None:
        """Count a final COPY error against the chunk; drop it at the limit."""
        slot.final_failures += 1
        slot.last_sqlstate = sqlstate
        self._slot_error = error
        if sqlstate == SQLSTATE_CHECK_VIOLATION:
            # A row with no partition: the next run creates partitions first.
            self._upkeep_requested = True
        await self._copy_rejected_log.emit(
            COPY_REJECTED_LOG_KEY,
            logger.awarning,
            'recorder_postgres_copy_rejected',
            category=LOG_CATEGORY,
            sqlstate=sqlstate,
            error=error,
            rows=len(slot.pairs),
            final_failures=slot.final_failures,
        )
        if slot.final_failures < POSTGRES_CHUNK_FINAL_FAILURES:
            return
        reason = DROP_REASON_NO_PARTITION if sqlstate == SQLSTATE_CHECK_VIOLATION else DROP_REASON_REJECTED
        self._slot = None
        await self._drop(reason, len(slot.pairs), error)

    # --- the final stage P and the close ---

    async def final(self, deadline_seconds: float | None) -> None:
        """The final stage P at stop: no upkeep, one attempt per chunk within ``deadline_seconds``.

        Stops at the first chunk that is not written (time spent, an open
        breaker, a failure) and drops every row left in the slot and the
        queue with ``shutdown``. A writer that never started makes no start
        attempt: it counts one ``copy`` as ``not_started`` when rows wait.
        """
        deadline_at = None if deadline_seconds is None else self._now_monotonic() + deadline_seconds
        try:
            async with asyncio.timeout(deadline_seconds):
                await self._lock.acquire()
        except TimeoutError:
            await self._drop_everything_left()
            return
        try:
            if not self._started:
                if self._slot is not None or len(self._queue):
                    self._caller.skip('copy', OUTCOME_NOT_STARTED)
                return
            while self._fill_slot():
                left = None if deadline_at is None else deadline_at - self._now_monotonic()
                if await self._write_slot(attempts=1, deadline_seconds=left) not in WRITTEN_OUTCOMES:
                    return
        finally:
            try:
                await self._drop_everything_left()
            finally:
                self._lock.release()

    async def _drop_everything_left(self) -> None:
        """Drop the slot and the queue with ``shutdown``, one log line for all of them."""
        rows = 0
        if self._slot is not None:
            rows += len(self._slot.pairs)
            self._slot = None
        while pairs := self._queue.take_chunk(len(self._queue) + 1, 2**62):
            rows += len(pairs)
        self._set_queue_gauge()
        if rows:
            await self._drop(DROP_REASON_SHUTDOWN, rows, '')

    async def close(self) -> None:
        """Close the write pool; a close that is cancelled terminates its connections."""
        pool, self._pool = self._pool, None
        if pool is None:
            return
        try:
            await postgres_providers.close_pool(pool)
        except asyncio.CancelledError:
            pool.terminate()
            raise

    # --- metrics and logs ---

    def _set_queue_gauge(self) -> None:
        """Set the queue gauges (rows of the queue plus the slot, text of the queue) and publish the status."""
        slot_rows = len(self._slot.pairs) if self._slot is not None else 0
        recorder_postgres_queue_rows.set(len(self._queue) + slot_rows)
        recorder_postgres_queue_bytes.set(self._queue.bytes)
        self._publish_status()

    async def _drop(self, reason: str, rows: int, error: str) -> None:
        """Count dropped rows and log them, at most one line per 60 s per reason."""
        recorder_postgres_dropped_rows.labels(reason=reason).inc(rows)
        status_field = _DROPPED_FIELDS[reason]
        self._publish_status(**{status_field: getattr(self._status, status_field) + rows})
        unlogged = self._unlogged_drops.get(reason, 0) + rows
        self._unlogged_drops[reason] = unlogged
        written = False

        async def log_and_note(event: str, **fields: Any) -> None:
            nonlocal written
            written = True
            logger.warning(event, **fields)

        await self._drop_log.emit(
            reason,
            log_and_note,
            'recorder_postgres_rows_dropped',
            category=LOG_CATEGORY,
            reason=reason,
            rows=unlogged,
            error=error,
        )
        if written:
            self._unlogged_drops[reason] -= unlogged
