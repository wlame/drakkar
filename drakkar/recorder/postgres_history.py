"""The UI reads of the ``postgres`` recorder table: the history search and the rows of a slice download.

``GET /api/v1/debug/trace?history=1`` on a worker whose recorder keeps its
history in Postgres reads the rows of the worker's own cluster from the
events table, never the sealed files. The search runs on the API server's
loop with a connection of its own (no pool), makes one attempt, and is
bounded as a whole by ``recorder.search.timeout``: the client side by
``asyncio.timeout``, the server side by ``statement_timeout``.

``GET /api/v1/debug/slice`` reads the rows of one time range the same way,
with a server-side cursor, batch by batch, bounded by the time left of the
build. ``GET /api/v1/debug/databases/postgres`` reads the partitions of the
table and computes what the next upkeep drops. The three reads share one
small set of slots (:func:`postgres_read_slot`).
"""

from __future__ import annotations

import asyncio
import math
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Literal

import asyncpg

from drakkar.providers.postgres import (
    RECORDER_STATEMENT_TEMPLATES,
    SET_LOCAL_VALUES,
    SETTING_JIT,
    STATEMENT_TIMEOUT_SETTING,
    parse_partition_hour,
    parse_retention_comment,
    partition_bounds,
    render_recorder,
)
from drakkar.providers.resilience import describe_error
from drakkar.recorder.postgres import RecorderPostgresSettings, replace_nul
from drakkar.recorder.postgres_writer import measured_partitions, size_drops, upkeep_actions

MILLISECONDS_PER_SECOND = 1000
# The smallest statement_timeout sent: 0 would mean no limit on the server.
MIN_STATEMENT_TIMEOUT_MS = 1
# The longest a search may take; a larger or non-finite
# ``recorder.search.timeout`` uses this limit.
MAX_SEARCH_SECONDS = 3600.0
# UI reads of the table (history searches and slice reads together) that may
# hold a connection at once in one worker process. The debug UI has no
# authentication, so this keeps a burst of requests from using up the
# connections of a database that many writers share.
POSTGRES_HISTORY_MAX_CONCURRENT = 2

HistoryFailureReason = Literal['timeout', 'error', 'busy']
Connect = Callable[..., Awaitable[Any]]
# Told the error text of a close that failed after a complete search.
CloseFailed = Callable[[str], Awaitable[None]]


class PostgresHistoryError(Exception):
    """A UI read of the table that failed (``reason`` ``'error'``), ran out of time (``'timeout'``) or found no free slot (``'busy'``).

    ``error`` is a log-friendly text without credentials.
    """

    def __init__(self, reason: HistoryFailureReason, error: str) -> None:
        super().__init__(f'{reason}: {error}')
        self.reason: HistoryFailureReason = reason
        self.error = error


# UI reads now holding a slot. Read and changed only on the API server's event
# loop with no await in between, so a plain counter is enough and nothing ever
# blocks.
_running_reads = 0


@contextmanager
def postgres_read_slot() -> Iterator[None]:
    """Hold one of the :data:`POSTGRES_HISTORY_MAX_CONCURRENT` slots of UI table reads, or raise ``busy`` at once.

    Shared by the history search and the slice read. The slot is given
    back on every exit, a cancel included.
    """
    global _running_reads
    if _running_reads >= POSTGRES_HISTORY_MAX_CONCURRENT:
        raise PostgresHistoryError('busy', f'{POSTGRES_HISTORY_MAX_CONCURRENT} table reads are already running')
    _running_reads += 1
    try:
        yield
    finally:
        _running_reads -= 1


def search_limit_seconds(max_seconds: float) -> float:
    """The time limit of a search: ``max_seconds``, or :data:`MAX_SEARCH_SECONDS` when it is larger or not finite."""
    if not math.isfinite(max_seconds) or max_seconds > MAX_SEARCH_SECONDS:
        return MAX_SEARCH_SECONDS
    return max_seconds


def statement_timeout_ms(max_seconds: float) -> str:
    """The server-side time limit of a search, in whole milliseconds (rounded down, at least 1), as text.

    The limit is capped first (:func:`search_limit_seconds`).
    """
    limit = search_limit_seconds(max_seconds)
    return str(max(MIN_STATEMENT_TIMEOUT_MS, math.floor(limit * MILLISECONDS_PER_SECOND)))


async def search_postgres_history(
    settings: RecorderPostgresSettings,
    cluster: str,
    partition: int,
    offset: int,
    *,
    max_seconds: float,
    on_close_failed: CloseFailed,
    connect: Connect | None = None,
) -> list[dict]:
    """Trace ``(partition, offset)`` through the events table rows of ``cluster``; rows in time order.

    Each row holds the 20 event columns plus ``worker_name``. One
    connection is opened for the search and closed after it. The
    transaction sets ``statement_timeout`` (the same limit in whole
    milliseconds) and turns JIT off before the trace statement.

    A close that fails or runs out of time after the rows are committed
    does not fail the search: the connection is terminated and
    ``on_close_failed`` is awaited with the error text (no credentials).

    ``connect`` defaults to :func:`asyncpg.connect`, looked up at call time.
    At most :data:`POSTGRES_HISTORY_MAX_CONCURRENT` UI table reads run at
    once in the process; another one does not wait and opens no connection.
    Raises :class:`PostgresHistoryError` when the search fails, does not end
    within ``max_seconds`` (connect included), or finds no free slot
    (``reason`` ``'busy'``).
    """
    with postgres_read_slot():
        return await _search(settings, cluster, partition, offset, max_seconds, on_close_failed, connect)


async def _search(
    settings: RecorderPostgresSettings,
    cluster: str,
    partition: int,
    offset: int,
    max_seconds: float,
    on_close_failed: CloseFailed,
    connect: Connect | None,
) -> list[dict]:
    """One search with a connection of its own; the caller holds the slot."""
    connect = connect if connect is not None else asyncpg.connect
    statement = render_recorder(RECORDER_STATEMENT_TEMPLATES['trace'], schema=settings.schema, table=settings.table)
    set_local = RECORDER_STATEMENT_TEMPLATES['set_local']
    # The writer stores the cluster name with every NUL replaced; the search
    # must ask for the same text.
    stored_cluster = replace_nul(cluster)
    deadline = asyncio.get_running_loop().time() + search_limit_seconds(max_seconds)
    conn: Any = None
    close_error: str | None = None
    try:
        async with asyncio.timeout_at(deadline):
            conn = await connect(dsn=settings.connection.dsn)
            # Started and committed by hand: on a failure or a time-out nothing
            # more is sent (a ROLLBACK would wait for the server); closing the
            # socket below ends the transaction on the server.
            transaction = conn.transaction()
            await transaction.start()
            await conn.execute(set_local, STATEMENT_TIMEOUT_SETTING, statement_timeout_ms(max_seconds))
            await conn.execute(set_local, SETTING_JIT, SET_LOCAL_VALUES[SETTING_JIT])
            rows = await conn.fetch(statement, stored_cluster, partition, offset)
            await transaction.commit()
        close_error = await _close_after_search(conn, deadline)
    except TimeoutError as exc:
        raise PostgresHistoryError('timeout', describe_error(exc)) from exc
    except Exception as exc:
        raise PostgresHistoryError('error', describe_error(exc)) from exc
    finally:
        # Never waits: a connection left open by a failure, a time-out, a
        # cancel or a close that did not end is closed at once.
        if conn is not None and not conn.is_closed():
            conn.terminate()
    if close_error is not None:
        await on_close_failed(close_error)
    return [dict(row) for row in rows]


async def _close_after_search(conn: Any, deadline: float) -> str | None:
    """Close the connection of a search that already has its rows, within the search's time limit.

    Returns ``None`` after a clean close, else the error text of the close
    that failed or ran out of time. The rows are committed and complete, so
    such a close does not fail the search: the caller terminates the
    connection, which ends it on the server anyway, and reports the text. A
    cancel still leaves as a cancel.
    """
    try:
        async with asyncio.timeout_at(deadline):
            await conn.close()
    except Exception as exc:
        return describe_error(exc)
    return None


@dataclass(frozen=True)
class SliceQuery:
    """The parameters of the ``slice`` statement; every filter is ``None`` when not asked for.

    ``exclude_instance`` and ``exclude_from_id`` leave out the rows of one
    process from one id up (rows newer than a read of its memory store).
    """

    cluster: str
    from_ts: float
    to_ts: float
    worker: str | None
    partition: int | None
    offset_from: int | None
    offset_to: int | None
    task_id: str | None
    events: list[str] | None
    label_key: str | None
    label_value: str | None
    exclude_instance: str | None
    exclude_from_id: int | None
    limit: int

    def args(self) -> tuple[Any, ...]:
        """The 14 bind values, in statement order; the cluster with every NUL replaced, as the writer stores it."""
        return (
            replace_nul(self.cluster),
            self.from_ts,
            self.to_ts,
            self.worker,
            self.partition,
            self.offset_from,
            self.offset_to,
            self.task_id,
            self.events,
            self.label_key,
            self.label_value,
            self.exclude_instance,
            self.exclude_from_id,
            self.limit,
        )


def time_left_ms(deadline: float, now: float) -> str:
    """The time from ``now`` to ``deadline`` in whole milliseconds (rounded down, at least 1), as text."""
    return str(max(MIN_STATEMENT_TIMEOUT_MS, math.floor((deadline - now) * MILLISECONDS_PER_SECOND)))


async def read_postgres_slice(
    settings: RecorderPostgresSettings,
    query: SliceQuery,
    *,
    deadline: float,
    first_rows: int,
    on_batch: Callable[[list[Any]], Awaitable[int]],
    on_close_failed: CloseFailed,
    connect: Connect | None = None,
) -> None:
    """Read the ``slice`` rows of ``query`` with a server-side cursor; each batch goes to ``on_batch``.

    The first fetch asks for ``first_rows`` rows; ``on_batch`` answers how
    many rows the next fetch asks for (0 stops the read). ``deadline`` is a
    time of the running loop. It bounds every network wait (connect, the
    statements, each fetch, the commit) but never ``on_batch``: a batch being
    written is always finished, so its thread never outlives the read. The
    transaction sets ``statement_timeout`` to the time left (and turns JIT
    off) before the cursor opens. Takes one of the shared UI read slots for
    the whole read.

    Raises :class:`PostgresHistoryError`: ``busy`` with no free slot,
    ``timeout`` when the deadline or the server's statement timeout ended the
    read (the batches given so far stay valid), ``error`` for any other
    failure. A close that fails after a complete read is reported through
    ``on_close_failed`` and does not fail the read.
    """
    with postgres_read_slot():
        await _read_slice(settings, query, deadline, first_rows, on_batch, on_close_failed, connect)


async def _within[T](deadline: float, waiting: Awaitable[T]) -> T:
    """Await ``waiting`` until ``deadline`` (a time of the running loop)."""
    async with asyncio.timeout_at(deadline):
        return await waiting


async def _read_slice(
    settings: RecorderPostgresSettings,
    query: SliceQuery,
    deadline: float,
    first_rows: int,
    on_batch: Callable[[list[Any]], Awaitable[int]],
    on_close_failed: CloseFailed,
    connect: Connect | None,
) -> None:
    """One slice read with a connection of its own; the caller holds the slot."""
    connect = connect if connect is not None else asyncpg.connect
    statement = render_recorder(RECORDER_STATEMENT_TEMPLATES['slice'], schema=settings.schema, table=settings.table)
    set_local = RECORDER_STATEMENT_TEMPLATES['set_local']
    loop = asyncio.get_running_loop()
    conn: Any = None
    close_error: str | None = None
    try:
        conn = await _within(deadline, connect(dsn=settings.connection.dsn))
        # Started and committed by hand, as in the history search.
        transaction = conn.transaction()
        await _within(deadline, transaction.start())
        timeout_ms = time_left_ms(deadline, loop.time())
        await _within(deadline, conn.execute(set_local, STATEMENT_TIMEOUT_SETTING, timeout_ms))
        await _within(deadline, conn.execute(set_local, SETTING_JIT, SET_LOCAL_VALUES[SETTING_JIT]))
        cursor = await _within(deadline, conn.cursor(statement, *query.args()))
        count = first_rows
        while count > 0:
            rows = await _within(deadline, cursor.fetch(count))
            if not rows:
                break
            next_count = await on_batch(list(rows))
            if len(rows) < count:
                break
            count = next_count
        await _within(deadline, transaction.commit())
        close_error = await _close_after_search(conn, deadline)
    except (TimeoutError, asyncpg.QueryCanceledError) as exc:
        # The only statement timeout this read sets is the time left of the
        # build, so a cancel by the server is the build's time limit too.
        raise PostgresHistoryError('timeout', describe_error(exc)) from exc
    except Exception as exc:
        raise PostgresHistoryError('error', describe_error(exc)) from exc
    finally:
        if conn is not None and not conn.is_closed():
            conn.terminate()
    if close_error is not None:
        await on_close_failed(close_error)


@dataclass(frozen=True)
class PartitionRow:
    """One hour partition of the table: its name, its hour bounds (epoch seconds) and its size (``None`` when gone)."""

    name: str
    from_ts: int
    to_ts: int
    size_bytes: int | None


@dataclass(frozen=True)
class TablePartitions:
    """The partitions of the recorder table at the server time ``server_now``, and what the next upkeep drops.

    ``retention_drops`` and ``size_drops`` follow the rules of the upkeep
    (:func:`~drakkar.recorder.postgres_writer.upkeep_actions` and
    :func:`~drakkar.recorder.postgres_writer.size_drops`) with the stored
    retention and this worker's ``max_size``.
    """

    server_now: float
    stored_retention_hours: int | None
    partitions: list[PartitionRow]
    retention_drops: list[str]
    size_drops: list[str]

    @property
    def total_bytes(self) -> int:
        """The sum of the known partition sizes."""
        return sum(row.size_bytes or 0 for row in self.partitions)


async def read_table_partitions(
    settings: RecorderPostgresSettings,
    *,
    max_seconds: float,
    on_close_failed: CloseFailed,
    connect: Connect | None = None,
) -> TablePartitions:
    """Read the partitions of the recorder table with four existing statements; no other SQL is sent.

    One connection of its own (no pool) and one of the shared UI read slots,
    as the history search. In one transaction: ``statement_timeout`` (the
    capped ``max_seconds`` in whole milliseconds) and JIT off, then
    ``server_now``, ``retention_read``, ``partitions_list`` and
    ``partition_sizes``. Only the names that are partitions of this table
    (``<table>_p`` plus a UTC hour) become rows, ascending.

    Raises :class:`PostgresHistoryError` as the history search does.
    """
    with postgres_read_slot():
        return await _read_partitions(settings, max_seconds, on_close_failed, connect)


async def _read_partitions(
    settings: RecorderPostgresSettings,
    max_seconds: float,
    on_close_failed: CloseFailed,
    connect: Connect | None,
) -> TablePartitions:
    """One partitions read with a connection of its own; the caller holds the slot."""
    connect = connect if connect is not None else asyncpg.connect
    statements = RECORDER_STATEMENT_TEMPLATES
    qualified = settings.qualified_table
    deadline = asyncio.get_running_loop().time() + search_limit_seconds(max_seconds)
    conn: Any = None
    close_error: str | None = None
    try:
        async with asyncio.timeout_at(deadline):
            conn = await connect(dsn=settings.connection.dsn)
            # Started and committed by hand, as in the history search.
            transaction = conn.transaction()
            await transaction.start()
            await conn.execute(statements['set_local'], STATEMENT_TIMEOUT_SETTING, statement_timeout_ms(max_seconds))
            await conn.execute(statements['set_local'], SETTING_JIT, SET_LOCAL_VALUES[SETTING_JIT])
            now = float(await conn.fetchval(statements['server_now']))
            stored = parse_retention_comment(await conn.fetchval(statements['retention_read'], qualified))
            listed = [row[0] for row in await conn.fetch(statements['partitions_list'], qualified)]
            names = measured_partitions(settings.table, listed, [])
            sizes = {
                row[0]: int(row[1]) for row in await conn.fetch(statements['partition_sizes'], settings.schema, names)
            }
            await transaction.commit()
        close_error = await _close_after_search(conn, deadline)
    except TimeoutError as exc:
        raise PostgresHistoryError('timeout', describe_error(exc)) from exc
    except Exception as exc:
        raise PostgresHistoryError('error', describe_error(exc)) from exc
    finally:
        if conn is not None and not conn.is_closed():
            conn.terminate()
    if close_error is not None:
        await on_close_failed(close_error)
    return _table_partitions(settings, now, stored, names, sizes)


def _table_partitions(
    settings: RecorderPostgresSettings, now: float, stored: int | None, names: list[str], sizes: dict[str, int]
) -> TablePartitions:
    """The rows and the next drops of one read, computed as the upkeep computes them."""
    rows = [
        PartitionRow(name, *partition_bounds(hour), size_bytes=sizes.get(name))
        for name in names
        if (hour := parse_partition_hour(settings.table, name)) is not None
    ]
    retention = upkeep_actions(settings.table, now, stored, names).drop
    measured = measured_partitions(settings.table, names, retention)
    by_size = size_drops(settings.table, now, measured, sizes, settings.max_size)
    return TablePartitions(
        server_now=now,
        stored_retention_hours=stored,
        partitions=rows,
        retention_drops=retention,
        size_drops=by_size,
    )
