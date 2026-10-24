"""The recorder memory store: one in-memory SQLite database per recorder.

The database lives on SQLite's ``memdb`` VFS under a name that every
connection of the process can open (``file:/drakkar-recorder-<instance>-<seq>``).
It exists while at least one connection to it is open and is gone after the
last one closes. A by-name open after that silently creates a new, empty
database, so every by-name open goes through the open guard of
:class:`MemoryStore`, and the writer connection, opened first and closed
last, keeps the database alive.

Every read of the store has a time limit (:data:`MEMORY_READ_LIMIT_SECONDS`)
well under the writer's busy timeout: memdb uses rollback-journal locking, so
a long read would otherwise hold off a memory commit until it fails.
"""

from __future__ import annotations

import asyncio
import contextlib
import math
import sqlite3
import threading
import time
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from typing import Any

import aiosqlite

from drakkar.recorder.helpers import BUSY_TIMEOUT_MS, close_quietly, open_connection
from drakkar.recorder.schema import SCHEMA_EVENTS, SCHEMA_WORKER_CONFIG

MIB = 1024 * 1024

# How long one read statement of the memory store may run before it is
# stopped. Under the writer's busy timeout (5 s), so a reader can delay a
# memory commit but never make it fail.
MEMORY_READ_LIMIT_SECONDS = 2.0
# How long close() waits for by-name connections still counted by the guard.
MEMORY_CLOSE_WAIT_SECONDS = 2.0
# Limit of a whole stepped snapshot of the memory store, and the pages it
# copies per backup step (it holds the read lock only for one step).
SNAPSHOT_LIMIT_SECONDS = 30.0
SNAPSHOT_STEP_PAGES = 1024
# Eviction deletes at most this many rows per batch, at most this many
# batches per pass, and runs at most once per this interval on the read path.
EVICT_BATCH_ROWS = 5000
EVICT_MAX_BATCHES = 64
EVICT_MIN_INTERVAL_SECONDS = 1.0
# The read limit is checked every this many SQLite virtual machine steps.
PROGRESS_HANDLER_STEPS = 10_000

MEMDB_MISSING_MESSAGE = (
    'recorder memory store: this SQLite library has no memdb VFS; '
    'SQLite 3.36 or later built with deserialize support is required'
)

# SQLite primary result codes (the low byte of an extended code).
_SQLITE_BUSY = 5
_SQLITE_LOCKED = 6
_SQLITE_INTERRUPT = 9
_PRIMARY_CODE_MASK = 0xFF

_store_seq_lock = threading.Lock()
_store_seq = 0


class MemoryStoreUnavailableError(RuntimeError):
    """The SQLite library cannot hold the memory store (no memdb VFS)."""


def memory_store_uri(instance: str, seq: int, *, read_only: bool = False) -> str:
    """The SQLite URI of the memory store of recorder ``instance``, number ``seq`` in this process.

    The leading ``/`` makes the database shared by name across the
    connections of the process; ``read_only`` adds ``mode=ro``.
    """
    uri = f'file:/drakkar-recorder-{instance}-{seq}?vfs=memdb'
    return f'{uri}&mode=ro' if read_only else uri


def next_store_seq() -> int:
    """The next store number of this process, from 1; two recorders never share a store."""
    global _store_seq
    with _store_seq_lock:
        _store_seq += 1
        return _store_seq


@dataclass(frozen=True)
class MemoryCoverage:
    """The id range the memory store holds: ``floor_id .. last_id``, with no hole.

    ``last_id`` is the last id handed out; when the table is empty,
    ``floor_id`` is ``last_id + 1``. ``evicted`` says whether any row was
    removed, and ``evicted_max_ts`` is the largest ``ts`` removed so far.
    """

    floor_id: int
    last_id: int
    evicted: bool
    evicted_max_ts: float

    @property
    def rows(self) -> int:
        """Rows in the events table of the store."""
        return self.last_id - self.floor_id + 1


@dataclass(frozen=True)
class PrefixDeletion:
    """What :func:`delete_id_prefix` removed: the row count, the largest ``ts`` among them, and the error that ended it.

    ``rollback_error`` is set when the rollback after ``error`` failed too:
    the connection may then still hold the open transaction of the failed
    batch, and the caller must roll it back before its next write.
    """

    deleted: int
    max_ts: float | None
    error: sqlite3.Error | None = None
    rollback_error: sqlite3.Error | None = None
    batches: int = 0


async def delete_id_prefix(conn: aiosqlite.Connection, below_id: int) -> PrefixDeletion:
    """Delete every event row with ``id < below_id``, in committed batches of at most :data:`EVICT_BATCH_ROWS`.

    Before each batch, ``MAX(ts)`` of the rows about to go is read, so the
    caller can keep the largest removed ``ts``. A failed statement rolls
    back the open batch and ends the deletion; the batches committed before
    it stay deleted and are counted in the result, with the error. The
    failed batch is never counted: it is rolled back, or, when the rollback
    fails as well, still open, which ``rollback_error`` reports. ``batches``
    counts the committed batches.
    """
    deleted = 0
    batches = 0
    max_ts: float | None = None
    try:
        while True:
            async with conn.execute('SELECT MIN(id) FROM events') as cursor:
                row = await cursor.fetchone()
            lowest = row[0] if row else None
            if lowest is None or lowest >= below_id:
                break
            bound = min(below_id, lowest + EVICT_BATCH_ROWS)
            async with conn.execute('SELECT MAX(ts) FROM events NOT INDEXED WHERE id < ?', (bound,)) as cursor:
                row = await cursor.fetchone()
            batch_max_ts = row[0] if row else None
            async with conn.execute('DELETE FROM events WHERE id < ?', (bound,)) as cursor:
                batch_deleted = cursor.rowcount
            await conn.commit()
            deleted += batch_deleted
            batches += 1
            if batch_max_ts is not None:
                max_ts = batch_max_ts if max_ts is None else max(max_ts, batch_max_ts)
    except sqlite3.Error as exc:
        try:
            await conn.rollback()
        except sqlite3.Error as rollback_exc:
            return PrefixDeletion(deleted, max_ts, exc, rollback_exc, batches)
        return PrefixDeletion(deleted, max_ts, exc, batches=batches)
    return PrefixDeletion(deleted, max_ts, batches=batches)


async def read_page_numbers(conn: aiosqlite.Connection) -> tuple[int, int, int]:
    """The ``page_count``, ``freelist_count`` and ``page_size`` of the database behind ``conn``."""
    numbers = []
    for pragma in ('page_count', 'freelist_count', 'page_size'):
        async with conn.execute(f'PRAGMA {pragma}') as cursor:
            row = await cursor.fetchone()
        numbers.append(int(row[0]) if row else 0)
    return numbers[0], numbers[1], numbers[2]


EVICTION_REASONS = ('window', 'rows', 'bytes')
# Reason of a failed read of the floor, the oldest ts or the page numbers at
# the end of a pass.
EVICTION_FLOOR_REASON = 'floor'


@dataclass(frozen=True)
class EvictionResult:
    """What one eviction pass did and measured.

    ``coverage`` is the id range the store holds after the pass.
    ``evicted_by_reason`` has a row count for each of
    :data:`EVICTION_REASONS`. ``used_bytes`` (pages in use times the page
    size), ``allocated_bytes`` (every page) and ``oldest_ts`` (``None`` when
    the table is empty) are measured at the end of a pass that did not fail;
    after a failure they are 0, 0 and ``None``. ``failed_reason`` names the
    part of the pass whose statement failed, with ``error``, and
    ``rollback_error`` when the rollback after it failed too (the connection
    may then hold an open transaction).
    """

    coverage: MemoryCoverage
    evicted_by_reason: dict[str, int]
    used_bytes: int
    allocated_bytes: int
    oldest_ts: float | None
    failed_reason: str | None
    error: sqlite3.Error | None = None
    rollback_error: sqlite3.Error | None = None


@dataclass(frozen=True)
class _PassFailure:
    reason: str
    error: sqlite3.Error
    rollback_error: sqlite3.Error | None


@dataclass
class _EvictionTally:
    """The running state of one pass: the floor after the deletes so far, rows deleted, batches run."""

    coverage: MemoryCoverage
    floor_id: int
    evicted_max_ts: float
    deleted: dict[str, int]
    batches: int = 0

    @classmethod
    def starting_from(cls, coverage: MemoryCoverage) -> _EvictionTally:
        return cls(coverage, coverage.floor_id, coverage.evicted_max_ts, dict.fromkeys(EVICTION_REASONS, 0))

    @property
    def rows(self) -> int:
        return self.coverage.last_id - self.floor_id + 1

    def take(self, reason: str, deletion: PrefixDeletion) -> None:
        """Count a prefix deletion; the store is one contiguous range, so the floor moves by the rows deleted."""
        self.deleted[reason] += deletion.deleted
        self.floor_id += deletion.deleted
        self.batches += deletion.batches
        if deletion.max_ts is not None:
            self.evicted_max_ts = max(self.evicted_max_ts, deletion.max_ts)

    def coverage_with_floor(self, floor_id: int) -> MemoryCoverage:
        return MemoryCoverage(
            floor_id=floor_id,
            last_id=self.coverage.last_id,
            evicted=self.coverage.evicted or any(self.deleted.values()),
            evicted_max_ts=self.evicted_max_ts,
        )


async def run_eviction_pass(
    conn: aiosqlite.Connection,
    coverage: MemoryCoverage,
    *,
    now: float,
    window_seconds: int,
    max_rows: int,
    max_bytes: int,
) -> EvictionResult:
    """Evict the oldest rows of the memory store by the time window, then the row cap, then the data bytes.

    Every delete removes an id prefix through :func:`delete_id_prefix`, so
    the store stays one contiguous id range. The window removes the rows
    before the first row (in id order) whose ``ts`` is at least
    ``now - window_seconds``; a late row after it stays until the prefix
    passes it. The row cap removes the oldest rows above ``max_rows``. The
    byte budget compares pages in use times the page size with
    ``max_bytes`` (the page count alone never falls after a delete) and
    removes 1 % of the rows (1 to :data:`EVICT_BATCH_ROWS`) per batch until
    it fits, the table is empty, or the pass ran :data:`EVICT_MAX_BATCHES`
    batches. A failed statement rolls back and ends the pass; the next pass
    starts again with the window. No vacuum: freed pages are reused.
    """
    tally = _EvictionTally.starting_from(coverage)
    failure = (
        await _evict_by_window(conn, tally, now - window_seconds)
        or await _evict_by_row_cap(conn, tally, max_rows)
        or await _evict_by_bytes(conn, tally, max_bytes)
    )
    if failure is not None:
        return _failed_pass(tally, failure)
    return await _finish_pass(conn, tally)


async def _evict_by_window(conn: aiosqlite.Connection, tally: _EvictionTally, cutoff: float) -> _PassFailure | None:
    try:
        # A scan in id order that stops at the first row to keep.
        async with conn.execute(
            'SELECT id FROM events NOT INDEXED WHERE ts >= ? ORDER BY id LIMIT 1', (cutoff,)
        ) as cursor:
            row = await cursor.fetchone()
    except sqlite3.Error as exc:
        return await _failure_after_rollback(conn, 'window', exc)
    boundary_id = row[0] if row else tally.coverage.last_id + 1
    return await _delete_below(conn, tally, 'window', boundary_id)


async def _evict_by_row_cap(conn: aiosqlite.Connection, tally: _EvictionTally, max_rows: int) -> _PassFailure | None:
    excess = tally.rows - max_rows
    if excess <= 0:
        return None
    return await _delete_below(conn, tally, 'rows', tally.floor_id + excess)


async def _evict_by_bytes(conn: aiosqlite.Connection, tally: _EvictionTally, max_bytes: int) -> _PassFailure | None:
    while tally.rows > 0 and tally.batches < EVICT_MAX_BATCHES:
        try:
            page_count, freelist_count, page_size = await read_page_numbers(conn)
        except sqlite3.Error as exc:
            return await _failure_after_rollback(conn, 'bytes', exc)
        if (page_count - freelist_count) * page_size <= max_bytes:
            return None
        batches_before = tally.batches
        count = min(EVICT_BATCH_ROWS, max(1, tally.rows // 100))
        failure = await _delete_below(conn, tally, 'bytes', tally.floor_id + count)
        if failure is not None:
            return failure
        if tally.batches == batches_before:
            # Nothing was below the bound: never spin on a range the store does not hold.
            return None
    return None


async def _delete_below(
    conn: aiosqlite.Connection, tally: _EvictionTally, reason: str, below_id: int
) -> _PassFailure | None:
    if below_id <= tally.floor_id:
        return None
    deletion = await delete_id_prefix(conn, below_id)
    tally.take(reason, deletion)
    if deletion.error is not None:
        return _PassFailure(reason, deletion.error, deletion.rollback_error)
    return None


async def _failure_after_rollback(conn: aiosqlite.Connection, reason: str, exc: sqlite3.Error) -> _PassFailure:
    try:
        await conn.rollback()
    except sqlite3.Error as rollback_exc:
        return _PassFailure(reason, exc, rollback_exc)
    return _PassFailure(reason, exc, None)


def _failed_pass(tally: _EvictionTally, failure: _PassFailure) -> EvictionResult:
    return EvictionResult(
        coverage=tally.coverage_with_floor(tally.floor_id),
        evicted_by_reason=tally.deleted,
        used_bytes=0,
        allocated_bytes=0,
        oldest_ts=None,
        failed_reason=failure.reason,
        error=failure.error,
        rollback_error=failure.rollback_error,
    )


async def _finish_pass(conn: aiosqlite.Connection, tally: _EvictionTally) -> EvictionResult:
    """Read the floor, the oldest ``ts`` and the page numbers; an empty table has ``floor_id = last_id + 1``."""
    try:
        # Two statements: each MIN alone is one index or rowid lookup, both
        # in one SELECT would scan the table.
        async with conn.execute('SELECT MIN(id) FROM events') as cursor:
            row = await cursor.fetchone()
        lowest_id = row[0] if row else None
        async with conn.execute('SELECT MIN(ts) FROM events') as cursor:
            row = await cursor.fetchone()
        oldest_ts = row[0] if row else None
        page_count, freelist_count, page_size = await read_page_numbers(conn)
    except sqlite3.Error as exc:
        return _failed_pass(tally, await _failure_after_rollback(conn, EVICTION_FLOOR_REASON, exc))
    floor_id = tally.coverage.last_id + 1 if lowest_id is None else lowest_id
    return EvictionResult(
        coverage=tally.coverage_with_floor(floor_id),
        evicted_by_reason=tally.deleted,
        used_bytes=(page_count - freelist_count) * page_size,
        allocated_bytes=page_count * page_size,
        oldest_ts=oldest_ts,
        failed_reason=None,
    )


def _primary_code(exc: sqlite3.Error) -> int | None:
    code = getattr(exc, 'sqlite_errorcode', None)
    return None if code is None else code & _PRIMARY_CODE_MASK


def is_transient_memory_error(exc: sqlite3.Error) -> bool:
    """Whether a memory store error is a lock that a later attempt can pass (busy or locked)."""
    return _primary_code(exc) in (_SQLITE_BUSY, _SQLITE_LOCKED)


def is_read_interrupted(exc: sqlite3.Error) -> bool:
    """Whether a read was stopped by the read limit (``SQLITE_INTERRUPT``)."""
    return _primary_code(exc) == _SQLITE_INTERRUPT


def _read_deadline_handler(deadline: float) -> Callable[[], int]:
    """A progress handler that stops the running statement once ``deadline`` (monotonic) has passed."""

    def handler() -> int:
        return 1 if time.monotonic() > deadline else 0

    return handler


def arm_read_limit(conn: sqlite3.Connection) -> None:
    """Give the next statement on a by-name connection the read limit, counted from now."""
    conn.set_progress_handler(
        _read_deadline_handler(time.monotonic() + MEMORY_READ_LIMIT_SECONDS), PROGRESS_HANDLER_STEPS
    )


class MemoryStore:
    """The memory store of one recorder: a writer, a read-only reader and the open guard.

    The writer is also the keeper: opened first and closed last, it keeps
    the database alive while the reader and by-name connections come and go.
    """

    def __init__(self, instance: str, *, store_config: bool) -> None:
        """Name the store without opening it; ``store_config`` adds the ``worker_config`` table."""
        self._store_config = store_config
        self._seq = next_store_seq()
        self._uri = memory_store_uri(instance, self._seq)
        self._reader_uri = memory_store_uri(instance, self._seq, read_only=True)
        self._writer: aiosqlite.Connection | None = None
        self._reader: aiosqlite.Connection | None = None
        # By-name opens run on worker threads and close() runs on the loop,
        # so the guard is a thread lock: the open flag and the count of
        # by-name connections still open.
        self._guard = threading.Condition()
        self._is_open = False
        self._open_by_name_count = 0
        # One read at a time on the reader: the deadline armed by a read
        # must belong to that read's statement, not to the next one queued.
        self._read_lock = asyncio.Lock()

    @property
    def uri(self) -> str:
        """The SQLite URI of the store (read-write)."""
        return self._uri

    @property
    def is_open(self) -> bool:
        """Whether the store is open: opened and not yet closing."""
        with self._guard:
            return self._is_open

    @property
    def writer(self) -> aiosqlite.Connection:
        """The writer connection; it also keeps the store alive."""
        if self._writer is None:
            raise RuntimeError('recorder memory store is not open')
        return self._writer

    @property
    def reader(self) -> aiosqlite.Connection:
        """The read-only connection of the UI reads."""
        if self._reader is None:
            raise RuntimeError('recorder memory store is not open')
        return self._reader

    async def open(self) -> None:
        """Create the store with its tables and open the reader.

        Raises :class:`MemoryStoreUnavailableError` when the SQLite library
        has no memdb VFS. The store has the ``events`` table, and
        ``worker_config`` when ``store_config``; it never has ``worker_state``.
        """
        try:
            self._writer = await open_connection(self._uri, uri=True)
        except sqlite3.OperationalError as exc:
            if 'no such vfs' in str(exc):
                raise MemoryStoreUnavailableError(MEMDB_MISSING_MESSAGE) from exc
            raise
        try:
            await self._writer.execute(f'PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}')
            await self._writer.executescript(SCHEMA_EVENTS)
            if self._store_config:
                await self._writer.executescript(SCHEMA_WORKER_CONFIG)
            await self._writer.commit()
            self._reader = await open_connection(self._reader_uri, uri=True)
            await self._reader.execute(f'PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}')
        except BaseException:
            await self._close_connections()
            raise
        with self._guard:
            self._is_open = True

    async def read(self, query: str, params: Sequence[Any] = ()) -> tuple[list[str], list[Any]]:
        """Run one read statement on the reader under the read limit; return the column names and rows.

        A statement stopped by the limit raises :class:`sqlite3.OperationalError`
        for which :func:`is_read_interrupted` is true.
        """
        reader = self.reader
        async with self._read_lock:
            deadline = time.monotonic() + MEMORY_READ_LIMIT_SECONDS
            await reader.set_progress_handler(_read_deadline_handler(deadline), PROGRESS_HANDLER_STEPS)
            async with reader.execute(query, params) as cursor:
                rows = list(await cursor.fetchall())
                columns = [column[0] for column in cursor.description or ()]
        return columns, rows

    @contextlib.contextmanager
    def open_by_name(self) -> Iterator[sqlite3.Connection | None]:
        """Open a short-lived read-only connection to the store by name, through the open guard.

        Yields ``None`` once the store is closed (or before it opened), so no
        read ever creates a new, empty store. The connection has the busy
        timeout and the read-limit progress handler; call
        :func:`arm_read_limit` before each statement that the limit should
        bound. Blocking: call it from a worker thread.
        """
        with self._guard:
            if not self._is_open:
                conn = None
            else:
                # Connected under the guard: close() cannot close the keeper
                # between the flag check and the open, so the open always
                # finds the live store.
                conn = sqlite3.connect(self._reader_uri, uri=True, check_same_thread=False)
                self._open_by_name_count += 1
        if conn is None:
            yield None
            return
        try:
            conn.execute(f'PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}')
            # Present but not armed: a statement is bounded only after
            # arm_read_limit, so a long snapshot keeps its own limit.
            conn.set_progress_handler(_read_deadline_handler(math.inf), PROGRESS_HANDLER_STEPS)
            yield conn
        finally:
            conn.close()
            with self._guard:
                self._open_by_name_count -= 1
                self._guard.notify_all()

    def _refuse_new_opens(self) -> None:
        """Set the open flag false: from now on by-name opens get nothing."""
        # Held only while a by-name open checks the flag and connects, so
        # taking it on the loop is brief.
        with self._guard:
            self._is_open = False

    def _wait_for_open_by_name(self, wait_seconds: float) -> bool:
        """Wait for the counted by-name connections to close; False when the wait ran out."""
        with self._guard:
            return self._guard.wait_for(lambda: self._open_by_name_count == 0, timeout=wait_seconds)

    async def close(self, wait_seconds: float | None = None) -> None:
        """Close the store: refuse new by-name opens, wait for open ones, close the reader, then the writer.

        The wait for by-name connections is at most ``wait_seconds``
        (``None``: :data:`MEMORY_CLOSE_WAIT_SECONDS`; never below 0). A
        connection still open after it keeps the store alive until it
        closes, and then the store is gone. The reader and the writer close
        even when the caller is cancelled during the wait. Safe to call when
        the store never opened.
        """
        limit = MEMORY_CLOSE_WAIT_SECONDS if wait_seconds is None else max(wait_seconds, 0.0)
        # On the loop, before anything can be cancelled: once the
        # connections close below, no by-name open may find the flag true.
        self._refuse_new_opens()
        try:
            await asyncio.to_thread(self._wait_for_open_by_name, limit)
        finally:
            # Shielded: a second cancellation cannot leave a connection open.
            await asyncio.shield(self._close_connections())

    async def _close_connections(self) -> None:
        # Reader first: the writer is the keeper of the database.
        if self._reader is not None:
            reader, self._reader = self._reader, None
            await close_quietly(reader)
        if self._writer is not None:
            writer, self._writer = self._writer, None
            await close_quietly(writer)

    async def used_bytes(self) -> int:
        """Bytes of data in the store: pages in use times the page size."""
        page_count, freelist_count, page_size = await self._page_numbers()
        return (page_count - freelist_count) * page_size

    async def allocated_bytes(self) -> int:
        """Database size of the store, free pages included."""
        page_count, _, page_size = await self._page_numbers()
        return page_count * page_size

    async def _page_numbers(self) -> tuple[int, int, int]:
        return await read_page_numbers(self.writer)
