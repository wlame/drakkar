"""Build a slice file: read the sources of a plan in batches and write them into one SQLite file.

The builder never runs SQLite on the API server's loop: every SQLite read
and write is one thread call (:func:`drakkar.concurrency.run_in_thread_to_end`,
whose cancel waits for the thread, so no connection is closed under a thread
that still uses it). The Postgres rows are fetched on the loop with a
server-side cursor, and each batch is written by a thread call.

Each SQLite source is read in keyset batches by ``id`` up to the largest id
it held when the build opened it, each batch one statement with no
transaction across batches: the cut is fixed, and the recorder's checkpoint
is never held back for the whole build. A batch ends at
:data:`SLICE_BATCH_ROWS` rows or :data:`SLICE_CHUNK_BYTES` text bytes,
whichever comes first, so one batch is bounded in memory; the file and RAM
size limits are checked after each batch.

The memory store is read first. A later source leaves out the rows the
store gave (the same process and id; for an own sealed file, the same id
and time), so each row is written once even when the
store evicts rows while the build runs.

A limit never fails a build. The rows cap, the file size cap, the time limit
and the sealed-file budget end the build (or skip files) and mark the file
truncated; the caller names it ``-partial``.
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import io
import json
import os
import secrets
import sqlite3
import time
from collections.abc import Awaitable, Callable, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, BinaryIO

import structlog

from drakkar.concurrency import run_in_daemon_thread_to_end, run_in_thread_to_end
from drakkar.dbfiles import secure_db_file
from drakkar.merge import MERGED_SCHEMA
from drakkar.recorder.archive import is_own_sealed_name, parse_db_start_ts
from drakkar.recorder.history import list_sealed, meta_path, read_meta
from drakkar.recorder.memstore import MEMORY_READ_LIMIT_SECONDS, PROGRESS_HANDLER_STEPS
from drakkar.recorder.postgres import RecorderPostgresSettings
from drakkar.recorder.postgres_history import PostgresHistoryError, SliceQuery, read_postgres_slice
from drakkar.recorder.schema import (
    EVENT_COLUMNS,
    SCHEMA_EVENTS,
    SCHEMA_WORKER_CONFIG,
    SCHEMA_WORKER_STATE,
    WORKER_CONFIG_COLUMNS,
    WORKER_STATE_COLUMNS,
)
from drakkar.recorder.sealing import own_db_files
from drakkar.slices import (
    NOTE_SOURCE_UNREADABLE,
    SCOPE_CLUSTER,
    SLICE_BATCH_ROWS,
    SLICE_CHUNK_BYTES,
    SLICE_MAX_FILE_BYTES,
    SLICE_MAX_RAM_BYTES,
    SLICE_PG_FIRST_FETCH_ROWS,
    SLICE_SCHEMA_VERSION,
    SLICE_SOURCES_TABLE_DDL,
    SLICE_TABLE_DDL,
    SLICE_TIME_LIMIT_SECONDS,
    SOURCE_KIND_BACKLOG,
    SOURCE_KIND_LIVE,
    SOURCE_KIND_MEMORY,
    SOURCE_KIND_POSTGRES,
    SOURCE_KIND_SEALED,
    TRUNCATED_BYTES,
    TRUNCATED_FILES,
    TRUNCATED_ROWS,
    TRUNCATED_TIME,
    SlicePlan,
    SliceRequest,
    slice_file_name,
)
from drakkar.sqlitebatch import NO_ID, fetch_rows, row_bytes
from drakkar.timefmt import format_rfc3339_micro

logger = structlog.get_logger()

# The event columns of a slice, without ``id``: the slice numbers its rows from 1.
SLICE_EVENT_COLUMNS: tuple[str, ...] = EVENT_COLUMNS[1:]
_OUTPUT_COLUMNS = frozenset({'stdout', 'stderr'})
# SQLite's result code of a statement stopped by a progress handler.
_SQLITE_INTERRUPT = 9
_SIDECARS = ('', '-journal', '-wal', '-shm')
# Exclusive, owner-only, never through a link: the temp file holds recorder data.
_TEMP_FLAGS = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
# Sealed files never change: opened read-only and immutable.
_SEALED_URI_OPTIONS = 'mode=ro&immutable=1'
_LIVE_URI_OPTIONS = 'mode=ro'
# The ``reason`` of ``debug_slice_in_ram``.
IN_RAM_REASON_NO_FOLDER = 'storage_dir_unset'
IN_RAM_REASON_FOLDER_UNUSABLE = 'storage_dir_unusable'
# The ``source_file`` of the ``workers`` rows of a cluster slice built from the table.
POSTGRES_SOURCE_FILE = 'postgres'
_NO_ID = NO_ID
# Ids asked at once from the temp table of memory keys (under SQLite's variable limit).
_KEY_LOOKUP_CHUNK = 500
# How a source leaves out the rows the memory store gave: by id and time (an
# own sealed file), or not at all (``None``).
DEDUPE_BY_ID_AND_TS = 'id_ts'

MemoryOpener = Callable[[], AbstractContextManager[sqlite3.Connection | None]]
Flush = Callable[[], Awaitable[str | None]]
# Told the reason and the error text of a failed Postgres read that still gives a file (a timeout or a close).
PostgresReadLog = Callable[[str, str], Awaitable[None]]


@dataclass(frozen=True)
class SliceWorker:
    """What a build needs to know of the worker that builds it, read once per request.

    ``open_memory`` opens a by-name connection to the memory store through
    its open guard (``None`` inside when the store is closed).
    """

    worker: str
    instance: str
    cluster: str
    provider: str
    storage_dir: str
    history_dir: str
    live_path: str
    max_files: int
    open_memory: MemoryOpener | None = None
    postgres: RecorderPostgresSettings | None = None


@dataclass
class SliceBudget:
    """The limits of one build and the first one that cut it.

    ``truncated`` is ``rows``, ``bytes`` or ``time`` once a limit ended the
    build, and ``files`` when only the sealed-file budget skipped files.
    """

    max_rows: int
    max_bytes: int
    deadline: float
    clock: Callable[[], float] = time.monotonic
    rows: int = 0
    truncated: str | None = None

    @property
    def stopped(self) -> bool:
        """Whether a limit ended the build."""
        return self.truncated in (TRUNCATED_ROWS, TRUNCATED_BYTES, TRUNCATED_TIME)

    def stop(self, reason: str) -> None:
        """End the build for ``reason``; the first limit that ends it is kept."""
        if not self.stopped:
            self.truncated = reason

    def skip_files(self) -> None:
        """Mark that the sealed-file budget skipped files, unless a limit already ended the build."""
        if self.truncated is None:
            self.truncated = TRUNCATED_FILES

    def remaining_rows(self) -> int:
        """Rows the file may still take."""
        return max(0, self.max_rows - self.rows)

    def time_left(self) -> float:
        """Seconds left of the build, 0 when spent."""
        return max(0.0, self.deadline - self.clock())

    def out_of_time(self) -> bool:
        """Whether the build's time is spent."""
        return self.clock() >= self.deadline


@dataclass(frozen=True)
class BuiltSlice:
    """A finished slice: an open handle at the start of the file, its attachment name and what it holds."""

    handle: BinaryIO
    name: str
    rows: int
    truncated: str | None
    size_bytes: int


class _SourceTimedOut(Exception):
    """A source statement was stopped by the time limit."""


class _SourceUnreadable(Exception):
    """A source could not be read (missing, corrupt, another schema)."""


def _deadline_handler(deadline: float, clock: Callable[[], float]) -> Callable[[], int]:
    return lambda: int(clock() >= deadline)


def _source_error(exc: sqlite3.Error) -> Exception:
    """A source error as a stop by the progress handler, or as an unreadable source."""
    if getattr(exc, 'sqlite_errorcode', 0) & 0xFF == _SQLITE_INTERRUPT:
        return _SourceTimedOut()
    return _SourceUnreadable(str(exc))


def _run_source(conn: sqlite3.Connection, sql: str, params: Sequence[Any] = ()) -> list[tuple]:
    """One statement on a source connection; a stop by the progress handler and every other error are told apart."""
    try:
        return conn.execute(sql, params).fetchall()
    except sqlite3.Error as exc:
        raise _source_error(exc) from exc


def _fetch_chunk(conn: sqlite3.Connection, sql: str, params: Sequence[Any], max_bytes: int) -> tuple[list[tuple], bool]:
    """:func:`~drakkar.sqlitebatch.fetch_rows` on a source; a stop by the progress handler and every other error are told apart."""
    try:
        return fetch_rows(conn, sql, params, max_bytes)
    except sqlite3.Error as exc:
        raise _source_error(exc) from exc


def _quote(column: str) -> str:
    return f'"{column}"'


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    """The column names of ``table`` on a source; empty when it has no such table."""
    return {row[1] for row in _run_source(conn, f'PRAGMA table_info({table})')}


def _select_list(present: set[str], wanted: Sequence[str], *, nulled: frozenset[str] = frozenset()) -> str:
    """The SELECT list of ``wanted``: a column the source lacks, or one in ``nulled``, is read as NULL."""
    return ', '.join(_quote(col) if col in present and col not in nulled else 'NULL' for col in wanted)


def sqlite_filters(request: SliceRequest) -> tuple[str, list[Any]]:
    """The WHERE terms of a request on a SQLite ``events`` table, and their parameters.

    The label term reads ``$."<key>"``; the key has no ``"`` and no ``\\`` (a
    request rule), so the path needs no escaping. A row whose ``labels`` is
    not JSON never matches and never fails the statement.
    """
    terms = ['ts >= ?', 'ts < ?']
    params: list[Any] = [request.from_ts, request.to_ts]
    if request.partition is not None:
        terms.append('"partition" = ?')
        params.append(request.partition)
    if request.offset_from is not None:
        terms.append('"offset" >= ?')
        params.append(request.offset_from)
    if request.offset_to is not None:
        terms.append('"offset" <= ?')
        params.append(request.offset_to)
    if request.task_id is not None:
        terms.append('task_id = ?')
        params.append(request.task_id)
    if request.events:
        terms.append(f'event IN ({", ".join("?" * len(request.events))})')
        params.extend(request.events)
    if request.label_key is not None:
        terms.append('CASE WHEN json_valid(labels) THEN json_extract(labels, ?) END = ?')
        params.extend([f'$."{request.label_key}"', request.label_value])
    return ' AND '.join(terms), params


def _source_config(conn: sqlite3.Connection) -> dict[str, Any]:
    """The ``worker_config`` row of a source as a dict; empty when it has none."""
    if not _table_columns(conn, 'worker_config'):
        return {}
    try:
        cursor = conn.execute('SELECT * FROM worker_config WHERE id = 1')
        row = cursor.fetchone()
    except sqlite3.Error as exc:
        raise _source_error(exc) from exc
    if row is None:
        return {}
    return {column[0]: value for column, value in zip(cursor.description, row, strict=True)}


def _parse_meta_ts(text: Any) -> float | None:
    if not isinstance(text, str):
        return None
    try:
        return datetime.fromisoformat(text).timestamp()
    except ValueError:
        return None


@dataclass(frozen=True)
class _FileSource:
    """One file a build reads: its path, kind, worker, and how it leaves out the rows the memory store gave."""

    path: str
    kind: str
    worker: str | None = None
    dedupe: str | None = None


# --- The target file ---


class _Target:
    """The slice database: a private temp file in the own folder, or a database in RAM."""

    def __init__(self, conn: sqlite3.Connection, path: str | None) -> None:
        self.conn = conn
        self.path = path

    @property
    def in_ram(self) -> bool:
        return self.path is None

    def size_bytes(self) -> int:
        page_count = self.conn.execute('PRAGMA page_count').fetchone()[0]
        page_size = self.conn.execute('PRAGMA page_size').fetchone()[0]
        return int(page_count) * int(page_size)

    def open_result(self) -> tuple[BinaryIO, int]:
        """Close the database and hand back an open handle to its bytes and their size; the temp file is unlinked."""
        if self.path is None:
            data = self.conn.serialize()
            self.conn.close()
            return io.BytesIO(data), len(data)
        self.conn.execute('PRAGMA journal_mode=DELETE')
        self.conn.close()
        try:
            handle = open(self.path, 'rb')  # noqa: SIM115 - the stream closes it after the last chunk
            return handle, os.fstat(handle.fileno()).st_size
        finally:
            self._unlink()

    def discard(self) -> None:
        """Close the database and remove the temp file and its sidecars."""
        with contextlib.suppress(sqlite3.Error):
            self.conn.close()
        self._unlink()

    def _unlink(self) -> None:
        if self.path is None:
            return
        for suffix in _SIDECARS:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(self.path + suffix)


def slice_temp_path(folder: str, worker: str) -> str:
    """A new temp path ``<folder>/.<worker>-slice-<16 hex>.download``; the start sweep of the own folder matches it."""
    return os.path.join(folder, f'.{worker}-slice-{secrets.token_hex(8)}.download')


def _open_temp_target(folder: str, worker: str) -> _Target:
    """Create the private temp file in ``folder`` and connect to it. Raises :class:`OSError` when it cannot be created."""
    path = slice_temp_path(folder, worker)
    os.close(os.open(path, _TEMP_FLAGS, 0o600))
    try:
        secure_db_file(path)
        conn = sqlite3.connect(path, check_same_thread=False)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(path)
        raise
    # A crash leaves a temp file the start sweep removes; no journal is needed.
    conn.execute('PRAGMA journal_mode=OFF')
    conn.execute('PRAGMA synchronous=OFF')
    return _Target(conn, path)


def open_target(storage_dir: str, worker: str) -> tuple[_Target, str | None]:
    """The target of a build and, for a build in RAM, the reason. Blocking.

    With ``storage.dir`` set the slice is built in a temp file of the own
    folder; without it, or when no temp file can be made there, in RAM.
    """
    reason = IN_RAM_REASON_NO_FOLDER
    if storage_dir:
        try:
            return _open_temp_target(storage_dir, worker), None
        except OSError:
            reason = IN_RAM_REASON_FOLDER_UNUSABLE
    return _Target(sqlite3.connect(':memory:', check_same_thread=False), None), reason


# --- The writer ---


class SliceWriter:
    """Writes events, configs, states and the two slice tables into the target. Blocking; one call at a time."""

    def __init__(self, target: _Target, request: SliceRequest, budget: SliceBudget) -> None:
        self._target = target
        self._conn = target.conn
        self._request = request
        self._budget = budget
        self._cluster_scope = request.scope == SCOPE_CLUSTER
        self._postgres_workers: dict[str, int] = {}
        self.has_worker_config = False
        self.has_memory_keys = False
        event_columns = ['worker_id', *SLICE_EVENT_COLUMNS] if self._cluster_scope else list(SLICE_EVENT_COLUMNS)
        self._event_insert = (
            f'INSERT INTO events ({", ".join(_quote(c) for c in event_columns)}) '
            f'VALUES ({", ".join("?" * len(event_columns))})'
        )
        state_table = 'worker_states' if self._cluster_scope else 'worker_state'
        state_columns = ['worker_id', *WORKER_STATE_COLUMNS] if self._cluster_scope else list(WORKER_STATE_COLUMNS)
        self._state_insert = (
            f'INSERT INTO {state_table} ({", ".join(state_columns)}) VALUES ({", ".join("?" * len(state_columns))})'
        )

    def create_schema(self) -> None:
        """The recorder file schema (worker scope) or the merge result schema (cluster scope), plus the slice tables.

        A temp table (in the temp database, never in the file) keeps the id
        and time of every memory row written, so later sources can leave
        them out.
        """
        if self._cluster_scope:
            self._conn.executescript(MERGED_SCHEMA)
        else:
            self._conn.executescript(SCHEMA_EVENTS + SCHEMA_WORKER_CONFIG + SCHEMA_WORKER_STATE)
        self._conn.execute(SLICE_TABLE_DDL)
        self._conn.execute(SLICE_SOURCES_TABLE_DDL)
        self._conn.execute('CREATE TEMP TABLE slice_memory_keys (id INTEGER PRIMARY KEY, ts REAL NOT NULL)')
        self._conn.commit()

    def insert_events(self, rows: Sequence[Sequence[Any]], worker_id: int | None) -> None:
        """Write event rows (the slice columns, no id) and count them; check the file size after the batch.

        In a cluster slice, ``worker_id`` is the worker of every row; with
        ``None`` each row carries its own ``worker_id`` first.
        """
        if worker_id is not None:
            self._conn.executemany(self._event_insert, ((worker_id, *row) for row in rows))
        else:
            self._conn.executemany(self._event_insert, rows)
        self._conn.commit()
        self._budget.rows += len(rows)
        if self._target.size_bytes() >= self._budget.max_bytes:
            self._budget.stop(TRUNCATED_BYTES)

    def remember_memory_rows(self, keys: Sequence[tuple[int, float]]) -> None:
        """Keep the ``(id, ts)`` of memory rows written, for the sources read after the store."""
        self._conn.executemany('INSERT OR IGNORE INTO temp.slice_memory_keys (id, ts) VALUES (?, ?)', keys)
        self._conn.commit()
        self.has_memory_keys = self.has_memory_keys or bool(keys)

    def memory_keys_among(self, ids: Sequence[int]) -> dict[int, float]:
        """The ids among ``ids`` that a memory row had, with that row's time."""
        found: dict[int, float] = {}
        for start in range(0, len(ids), _KEY_LOOKUP_CHUNK):
            chunk = ids[start : start + _KEY_LOOKUP_CHUNK]
            found.update(
                self._conn.execute(
                    f'SELECT id, ts FROM temp.slice_memory_keys WHERE id IN ({", ".join("?" * len(chunk))})', chunk
                ).fetchall()
            )
        return found

    def insert_states(self, rows: Sequence[Sequence[Any]], worker_id: int | None) -> None:
        """Write ``worker_state`` rows (worker scope) or ``worker_states`` rows (cluster scope)."""
        if worker_id is not None:
            self._conn.executemany(self._state_insert, ((worker_id, *row) for row in rows))
        else:
            self._conn.executemany(self._state_insert, rows)
        self._conn.commit()

    def set_worker_config(self, config: dict[str, Any]) -> None:
        """Write the ``worker_config`` row of a worker-scope slice; a later call replaces it (newest source wins)."""
        values = [config.get(col) for col in WORKER_CONFIG_COLUMNS]
        self._conn.execute(
            f'INSERT OR REPLACE INTO worker_config (id, {", ".join(WORKER_CONFIG_COLUMNS)}) '
            f'VALUES (1, {", ".join("?" * len(values))})',
            values,
        )
        self._conn.commit()
        self.has_worker_config = True

    def add_worker(self, config: dict[str, Any], source_file: str) -> int:
        """Write one ``workers`` row of a cluster-scope slice; its id."""
        columns = [*WORKER_CONFIG_COLUMNS, 'source_file']
        values = [config.get(col) for col in WORKER_CONFIG_COLUMNS] + [source_file]
        cursor = self._conn.execute(
            f'INSERT INTO workers ({", ".join(columns)}) VALUES ({", ".join("?" * len(columns))})', values
        )
        self._conn.commit()
        row_id = cursor.lastrowid
        if row_id is None:  # pragma: no cover - an INSERT always gives a rowid
            raise sqlite3.DatabaseError('workers row has no id')
        return row_id

    def postgres_worker_id(self, worker: str, cluster: str) -> int:
        """The ``workers`` row of a worker found in the table (cluster scope), written on first sight."""
        row_id = self._postgres_workers.get(worker)
        if row_id is None:
            row_id = self.add_worker({'worker_name': worker, 'cluster_name': cluster}, POSTGRES_SOURCE_FILE)
            self._postgres_workers[worker] = row_id
        return row_id

    def add_source(self, kind: str, name: str | None, worker: str | None, rows: int) -> None:
        """One row of ``slice_sources``."""
        self._conn.execute(
            'INSERT INTO slice_sources (kind, name, worker, rows) VALUES (?, ?, ?, ?)', (kind, name, worker, rows)
        )
        self._conn.commit()

    def finish(self, *, worker: str, cluster: str, notes: list[str], created_at: float) -> None:
        """Write the ``slice`` row."""
        self._conn.execute(
            'INSERT INTO slice (id, schema, created_at, scope, worker, cluster, from_ts, to_ts, filters, rows, '
            'truncated, notes) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            (
                SLICE_SCHEMA_VERSION,
                format_rfc3339_micro(datetime.fromtimestamp(created_at, tz=UTC)),
                self._request.scope,
                worker,
                cluster or None,
                self._request.from_ts,
                self._request.to_ts,
                self._request.filters_json(),
                self._budget.rows,
                self._budget.truncated,
                json.dumps(notes, separators=(',', ':')),
            ),
        )
        self._conn.commit()


# --- The build ---


@dataclass
class _Build:
    """The state of one build: the request, the worker, the writer, the budget, the notes."""

    request: SliceRequest
    worker: SliceWorker
    writer: SliceWriter
    budget: SliceBudget
    notes: list[str] = field(default_factory=list)
    # Memory rows written, and the largest id the store held when it was read.
    memory_rows: int = 0
    memory_cut: int | None = None

    @property
    def cluster_scope(self) -> bool:
        return self.request.scope == SCOPE_CLUSTER

    def _note(self, note: str) -> None:
        if note not in self.notes:
            self.notes.append(note)

    # -- events in batches --

    def _left_out(self, rows: list[tuple], dedupe: str | None) -> list[tuple]:
        """``rows`` (``id`` and ``ts`` first) without the rows the memory store gave."""
        if dedupe is None or not self.writer.has_memory_keys or not rows:
            return rows
        known = self.writer.memory_keys_among([row[0] for row in rows])
        return [row for row in rows if known.get(row[0]) != row[1]]

    def _copy_batches(
        self,
        fetch: Callable[[int, int], tuple[list[tuple], bool]],
        worker_id: int | None,
        *,
        dedupe: str | None = None,
        remember: bool = False,
    ) -> int:
        """Copy the rows ``fetch(after_id, limit)`` gives (``id`` and ``ts`` first) in keyset batches; the rows written.

        ``fetch`` also says whether the byte bound ended its batch early.
        ``dedupe`` leaves out the rows the memory store gave; ``remember``
        keeps the ``(id, ts)`` of each batch written (the memory read).
        """
        written = 0
        after = _NO_ID
        while not self.budget.stopped:
            if self.budget.out_of_time():
                self.budget.stop(TRUNCATED_TIME)
                break
            remaining = self.budget.remaining_rows()
            limit = min(SLICE_BATCH_ROWS, remaining + 1)
            rows, cut_by_bytes = fetch(after, limit)
            if not rows:
                break
            kept = self._left_out(rows, dedupe)
            take = kept[:remaining]
            if take:
                self.writer.insert_events([row[1:] for row in take], worker_id)
                if remember:
                    self.writer.remember_memory_rows([(row[0], row[1]) for row in take])
                written += len(take)
            if len(kept) > remaining:
                self.budget.stop(TRUNCATED_ROWS)
                break
            if len(rows) < limit and not cut_by_bytes:
                break
            after = rows[-1][0]
        return written

    def _event_fetcher(
        self, conn: sqlite3.Connection, max_id: int, arm: Callable[[], None] | None = None
    ) -> Callable[[int, int], tuple[list[tuple], bool]]:
        present = _table_columns(conn, 'events')
        if 'id' not in present:
            raise _SourceUnreadable('no events table')
        nulled = frozenset() if self.request.output else _OUTPUT_COLUMNS
        select = _select_list(present, SLICE_EVENT_COLUMNS, nulled=nulled)
        where, params = sqlite_filters(self.request)
        sql = f'SELECT id, {select} FROM events WHERE id > ? AND id <= ? AND {where} ORDER BY id LIMIT ?'

        def fetch(after: int, limit: int) -> tuple[list[tuple], bool]:
            if arm is not None:
                arm()
            return _fetch_chunk(conn, sql, [after, max_id, *params, limit], SLICE_CHUNK_BYTES)

        return fetch

    def _copy_states(self, conn: sqlite3.Connection, worker_id: int | None) -> None:
        """Copy the ``worker_state`` rows whose ``updated_at`` is in the range, in keyset batches."""
        present = _table_columns(conn, 'worker_state')
        if 'updated_at' not in present:
            return
        sql = (
            f'SELECT id, {_select_list(present, WORKER_STATE_COLUMNS)} FROM worker_state '
            'WHERE id > ? AND updated_at >= ? AND updated_at < ? ORDER BY id LIMIT ?'
        )
        after = _NO_ID
        while not self.budget.stopped:
            if self.budget.out_of_time():
                self.budget.stop(TRUNCATED_TIME)
                return
            rows = _run_source(conn, sql, [after, self.request.from_ts, self.request.to_ts, SLICE_BATCH_ROWS])
            if rows:
                self.writer.insert_states([row[1:] for row in rows], worker_id)
            if len(rows) < SLICE_BATCH_ROWS:
                return
            after = rows[-1][0]

    # -- memory --

    def _arm(self, conn: sqlite3.Connection) -> None:
        """Bound the next memory statement by the memory read limit and by the build's deadline."""
        clock = self.budget.clock
        deadline = min(clock() + MEMORY_READ_LIMIT_SECONDS, self.budget.deadline)
        conn.set_progress_handler(_deadline_handler(deadline, clock), PROGRESS_HANDLER_STEPS)

    def copy_memory(self, *, read: bool) -> None:
        """Copy the memory store rows (with ``read``) and its config (worker scope). Blocking.

        The rows go first, so the config comes from the store unless a file
        read later replaces it. The ``(id, ts)`` of every row written is kept
        for the sources read after the store.
        """
        opener = self.worker.open_memory
        if opener is None:
            return
        with opener() as conn:
            if conn is None:
                if read:
                    self.writer.add_source(SOURCE_KIND_MEMORY, None, self.worker.worker, 0)
                return
            written = 0
            try:
                if read:
                    written = self._copy_memory_rows(conn)
                if not self.cluster_scope:
                    self._arm(conn)
                    config = _source_config(conn)
                    if config:
                        self.writer.set_worker_config(config)
            except _SourceTimedOut:
                self.budget.stop(TRUNCATED_TIME)
            except _SourceUnreadable:
                self._note(NOTE_SOURCE_UNREADABLE)
            if read:
                self.writer.add_source(SOURCE_KIND_MEMORY, None, self.worker.worker, written)

    def _copy_memory_rows(self, conn: sqlite3.Connection) -> int:
        self._arm(conn)
        max_id = _run_source(conn, 'SELECT MAX(id) FROM events')[0][0]
        self.memory_cut = max_id
        if max_id is None:
            return 0
        fetch = self._event_fetcher(conn, max_id, arm=lambda: self._arm(conn))
        self.memory_rows = self._copy_batches(fetch, None, remember=True)
        return self.memory_rows

    # -- files --

    def _open_source(self, path: str, uri_options: str) -> sqlite3.Connection:
        if not os.path.isfile(path):
            raise FileNotFoundError(path)
        try:
            conn = sqlite3.connect(f'file:{path}?{uri_options}', uri=True, isolation_level=None)
        except sqlite3.Error as exc:
            raise _SourceUnreadable(str(exc)) from exc
        conn.set_progress_handler(_deadline_handler(self.budget.deadline, self.budget.clock), PROGRESS_HANDLER_STEPS)
        return conn

    def _file_worker(self, conn: sqlite3.Connection, source: _FileSource) -> tuple[str, dict[str, Any]]:
        """The worker name of a file and its ``worker_config`` row (empty when it has none)."""
        config = _source_config(conn)
        name = source.worker or config.get('worker_name') or os.path.basename(source.path)
        return str(name), config

    def copy_file(self, source: _FileSource, *, history_dir: str, done: set[str]) -> None:
        """Copy one recorder file: its events, its states and, in worker scope, its config.

        A backlog or live file that is gone (sealed meanwhile) is read from
        the history folder under the same name, unless that copy was read
        already. An unreadable file is noted and skipped.
        """
        name = os.path.basename(source.path)
        uri_options = _SEALED_URI_OPTIONS if source.kind == SOURCE_KIND_SEALED else _LIVE_URI_OPTIONS
        try:
            conn = self._open_source(source.path, uri_options)
        except FileNotFoundError:
            moved = os.path.join(history_dir, name) if history_dir else ''
            if not moved or moved in done or not os.path.isfile(moved):
                return
            self.copy_file(_FileSource(moved, SOURCE_KIND_SEALED, source.worker), history_dir='', done=done)
            return
        except _SourceUnreadable:
            self._unreadable(source, name)
            return
        done.add(source.path)
        written = 0
        try:
            if _table_columns(conn, 'workers'):
                raise _SourceUnreadable('a merge result, not a recorder file')
            worker, config = self._file_worker(conn, source)
            worker_id = self.writer.add_worker(config or {'worker_name': worker}, name) if self.cluster_scope else None
            if not self.cluster_scope and config:
                self.writer.set_worker_config(config)
            max_row = _run_source(conn, 'SELECT MAX(id) FROM events')
            max_id = max_row[0][0] if max_row and max_row[0][0] is not None else 0
            written = self._copy_batches(self._event_fetcher(conn, max_id), worker_id, dedupe=source.dedupe)
            self._copy_states(conn, worker_id)
            self.writer.add_source(source.kind, name, worker, written)
        except _SourceTimedOut:
            self.budget.stop(TRUNCATED_TIME)
            self.writer.add_source(source.kind, name, source.worker, written)
        except _SourceUnreadable:
            self._unreadable(source, name, written)
        finally:
            conn.close()

    def _unreadable(self, source: _FileSource, name: str, written: int = 0) -> None:
        self._note(NOTE_SOURCE_UNREADABLE)
        self.writer.add_source(source.kind, name, source.worker, written)

    def _sealed_range(self, path: str) -> tuple[float | None, float | None, dict[str, Any] | None]:
        """The first and last event time of a sealed file, from its meta file or from the file."""
        meta = read_meta(meta_path(path))
        if meta is not None:
            return _parse_meta_ts(meta.get('first_event_ts')), _parse_meta_ts(meta.get('last_event_ts')), meta
        try:
            conn = self._open_source(path, _SEALED_URI_OPTIONS)
        except (FileNotFoundError, _SourceUnreadable):
            return None, None, None
        try:
            low, high = _run_source(conn, 'SELECT MIN(ts), MAX(ts) FROM events')[0]
        except (_SourceUnreadable, _SourceTimedOut):
            return None, None, None
        finally:
            conn.close()
        return low, high, None

    def _sealed_dedupe(self) -> str | None:
        """How an own sealed file leaves out the memory rows: by id and time.

        The meta ``instance`` names the process that sealed the file, not the
        one that wrote its rows: a start seals the files an earlier process of
        the same name left, and both processes number their rows from 1. A row
        of this process has the same id and time as its memory row; a row of
        another process almost never has both.
        """
        return None if self.cluster_scope else DEDUPE_BY_ID_AND_TS

    def sealed_sources(self, unsealed_names: set[str]) -> list[_FileSource]:
        """The sealed files to read, oldest first: newest first up to the file budget, each overlapping the range.

        Worker scope reads the own files of the cluster folder, cluster scope
        every file there. A name still listed as unsealed is left to that read.
        Files past the budget are skipped and mark the slice ``files``.
        """
        folder = self.worker.history_dir
        if not folder:
            return []
        paths = sorted(list_sealed(folder), key=parse_db_start_ts, reverse=True)
        chosen: list[_FileSource] = []
        for path in paths:
            name = os.path.basename(path)
            if name in unsealed_names:
                continue
            if not self.cluster_scope and not is_own_sealed_name(name, self.worker.worker):
                continue
            # A file is named when it opens, before its first event.
            if parse_db_start_ts(path) >= self.request.to_ts:
                continue
            if self.budget.out_of_time():
                self.budget.stop(TRUNCATED_TIME)
                break
            low, high, meta = self._sealed_range(path)
            if low is None or high is None or low >= self.request.to_ts or high < self.request.from_ts:
                continue
            if len(chosen) >= self.worker.max_files:
                self.budget.skip_files()
                break
            worker = meta.get('worker') if meta else None
            chosen.append(
                _FileSource(
                    path, SOURCE_KIND_SEALED, worker if isinstance(worker, str) else None, self._sealed_dedupe()
                )
            )
        chosen.reverse()
        return chosen

    def copy_files(self, plan: SlicePlan) -> None:
        """Copy the file sources of a plan, oldest first: sealed, then backlog, then live."""
        unsealed: list[_FileSource] = []
        if plan.backlog or plan.live:
            live = os.path.normpath(self.worker.live_path) if self.worker.live_path else ''
            for path in own_db_files(self.worker.storage_dir, self.worker.worker) if self.worker.storage_dir else []:
                if path != live and plan.backlog:
                    unsealed.append(_FileSource(path, SOURCE_KIND_BACKLOG, self.worker.worker))
            if live and plan.live:
                unsealed.append(_FileSource(live, SOURCE_KIND_LIVE, self.worker.worker))
        names = {os.path.basename(source.path) for source in unsealed}
        sealed = self.sealed_sources(names) if plan.sealed else []
        done: set[str] = set()
        for source in [*sealed, *unsealed]:
            if self.budget.stopped:
                break
            self.copy_file(source, history_dir=self.worker.history_dir, done=done)

    # -- postgres --

    def postgres_query(self) -> SliceQuery:
        """The parameters of the ``slice`` statement for this build.

        Worker scope reads this worker's rows. After a memory read, the rows
        of this process newer than the store's cut are left out (they were
        recorded after the read), and the limit leaves room for the rows the
        store gave already, which the build drops by key.
        """
        own_instance = None
        cut_from = None
        if not self.cluster_scope and self.memory_cut is not None:
            own_instance, cut_from = self.worker.instance, self.memory_cut + 1
        return SliceQuery(
            cluster=self.worker.cluster,
            from_ts=self.request.from_ts,
            to_ts=self.request.to_ts,
            worker=None if self.cluster_scope else self.worker.worker,
            partition=self.request.partition,
            offset_from=self.request.offset_from,
            offset_to=self.request.offset_to,
            task_id=self.request.task_id,
            events=list(self.request.events) or None,
            label_key=self.request.label_key,
            label_value=self.request.label_value,
            exclude_instance=own_instance,
            exclude_from_id=cut_from,
            limit=self.budget.remaining_rows() + self.memory_rows + 1,
        )

    def _postgres_values(self, row: Any) -> tuple:
        return tuple(
            None if (not self.request.output and col in _OUTPUT_COLUMNS) else row[col] for col in SLICE_EVENT_COLUMNS
        )

    def _postgres_kept(self, rows: Sequence[Any]) -> list[Any]:
        """The table rows without the rows of this process the memory store gave."""
        if not self.writer.has_memory_keys:
            return list(rows)
        own = [row['id'] for row in rows if row['instance'] == self.worker.instance]
        known = self.writer.memory_keys_among(own) if own else {}
        return [row for row in rows if not (row['instance'] == self.worker.instance and row['id'] in known)]

    def write_postgres_rows(self, rows: Sequence[Any], counts: dict[str | None, int]) -> int:
        """Write one batch of table rows in their order; the rows of the next fetch, 0 when the build wants no more.

        The batch is written in parts of at most :data:`SLICE_CHUNK_BYTES`,
        each part followed by the size check, and the next fetch is sized
        from the bytes per row of this one. Blocking.
        """
        remaining = self.budget.remaining_rows()
        kept = self._postgres_kept(rows)
        part: list[tuple] = []
        part_bytes = 0
        batch_bytes = 0
        for row in kept[:remaining]:
            event = self._postgres_values(row)
            worker = row['worker']
            counts[worker] = counts.get(worker, 0) + 1
            size = row_bytes(event)
            batch_bytes += size
            if self.cluster_scope:
                # The rows of a cluster slice carry their worker_id first, in table order.
                event = (self.writer.postgres_worker_id(worker, self.worker.cluster), *event)
            part.append(event)
            part_bytes += size
            if part_bytes >= SLICE_CHUNK_BYTES:
                self.writer.insert_events(part, None)
                part, part_bytes = [], 0
                if self.budget.stopped:
                    return 0
        if part:
            self.writer.insert_events(part, None)
        if len(kept) > remaining:
            self.budget.stop(TRUNCATED_ROWS)
        elif self.budget.out_of_time():
            self.budget.stop(TRUNCATED_TIME)
        if self.budget.stopped:
            return 0
        per_row = max(1, batch_bytes // max(1, len(kept))) if kept else 1
        return max(1, min(SLICE_BATCH_ROWS, SLICE_CHUNK_BYTES // per_row))

    def add_postgres_sources(self, counts: dict[str | None, int]) -> None:
        if not counts:
            counts = {None if self.cluster_scope else self.worker.worker: 0}
        for worker, rows in sorted(counts.items(), key=lambda item: item[0] or ''):
            self.writer.add_source(SOURCE_KIND_POSTGRES, None, worker, rows)


async def _copy_postgres(build: _Build, *, connect: Any, on_failed: PostgresReadLog) -> None:
    """Copy the table rows of a plan: the cursor on this loop, each batch written by a thread call.

    A read cut by the build's time (the client limit or the server's
    ``statement_timeout``) keeps its rows and marks the slice ``time``. Any
    other failure, and no free read slot, raises :class:`PostgresHistoryError`.
    """
    settings = build.worker.postgres
    if settings is None:
        return
    counts: dict[str | None, int] = {}
    # A failure to write the slice file is not a failure of the table read:
    # it ends the read and is raised again below (a 500, not a 503).
    write_errors: list[BaseException] = []

    async def on_batch(rows: list[Any]) -> int:
        try:
            return await run_in_thread_to_end(build.write_postgres_rows, rows, counts)
        except (OSError, sqlite3.Error) as exc:
            write_errors.append(exc)
            return 0

    try:
        await read_postgres_slice(
            settings,
            build.postgres_query(),
            deadline=asyncio.get_running_loop().time() + build.budget.time_left(),
            first_rows=SLICE_PG_FIRST_FETCH_ROWS,
            on_batch=on_batch,
            on_close_failed=lambda error: on_failed('close', error),
            connect=connect,
        )
    except PostgresHistoryError as exc:
        if exc.reason != 'timeout':
            raise
        build.budget.stop(TRUNCATED_TIME)
        await on_failed('timeout', exc.error)
    if write_errors:
        raise write_errors[0]
    await run_in_thread_to_end(build.add_postgres_sources, counts)


async def build_slice(
    request: SliceRequest,
    worker: SliceWorker,
    plan: SlicePlan,
    *,
    flush: Flush | None = None,
    flush_postgres: Flush | None = None,
    flush_memory: Flush | None = None,
    on_postgres_failed: PostgresReadLog | None = None,
    connect: Any = None,
    clock: Callable[[], float] = time.monotonic,
    now: Callable[[], float] = time.time,
) -> BuiltSlice:
    """Build the slice of ``request`` on ``worker`` along ``plan``; the open file and what it holds.

    Asks for the flush the plan needs first (a flush that does not end in
    time, or leaves rows waiting, adds its note). Then the memory store, the
    files and the table, in that order. Raises :class:`PostgresHistoryError`
    when the table cannot be read, and :class:`OSError` or
    :class:`sqlite3.Error` when the slice file cannot be written; the temp
    file is removed then, also when the build is cancelled.
    """
    notes: list[str] = []
    flusher = (
        flush if plan.flush else flush_postgres if plan.flush_postgres else flush_memory if plan.flush_memory else None
    )
    if flusher is not None:
        note = await flusher()
        if note is not None:
            notes.append(note)
    target, ram_reason = await run_in_thread_to_end(
        open_target, worker.storage_dir, worker.worker, on_cancelled_result=lambda opened: opened[0].discard()
    )
    try:
        if ram_reason is not None:
            await logger.ainfo('debug_slice_in_ram', category='debug', reason=ram_reason)
        budget = SliceBudget(
            max_rows=request.max_rows,
            max_bytes=SLICE_MAX_RAM_BYTES if target.in_ram else SLICE_MAX_FILE_BYTES,
            deadline=clock() + SLICE_TIME_LIMIT_SECONDS,
            clock=clock,
        )
        build = _Build(
            request=request, worker=worker, writer=SliceWriter(target, request, budget), budget=budget, notes=notes
        )
        await run_in_thread_to_end(build.writer.create_schema)
        if not build.cluster_scope:
            await run_in_thread_to_end(functools.partial(build.copy_memory, read=plan.memory))
        if (plan.sealed or plan.backlog or plan.live) and not budget.stopped:
            # The sealed files are on the shared folder, which may be a hung mount.
            await run_in_daemon_thread_to_end(build.copy_files, plan)
        if plan.postgres and not budget.stopped:
            await _copy_postgres(build, connect=connect, on_failed=on_postgres_failed or _ignore_failure)
        finish = functools.partial(
            build.writer.finish, worker=worker.worker, cluster=worker.cluster, notes=notes, created_at=now()
        )
        await run_in_thread_to_end(finish)
        # The name before the file is opened: nothing can fail between the open and the answer.
        name = slice_file_name(
            scope=request.scope,
            worker=worker.worker,
            cluster=worker.cluster,
            from_ts=request.from_ts,
            to_ts=request.to_ts,
            truncated=budget.truncated,
            compress=request.compress,
        )
        handle, size = await run_in_thread_to_end(
            target.open_result, on_cancelled_result=lambda opened: opened[0].close()
        )
    except BaseException:
        await run_in_thread_to_end(target.discard)
        raise
    return BuiltSlice(handle=handle, name=name, rows=budget.rows, truncated=budget.truncated, size_bytes=size)


async def _ignore_failure(_reason: str, _error: str) -> None:
    return None
