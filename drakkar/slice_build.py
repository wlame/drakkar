"""Build a slice file: read the sources of a plan in batches and write them into one SQLite file.

The builder never runs SQLite on the API server's loop: every SQLite read
and write is one call of :func:`asyncio.to_thread`. The Postgres rows are
fetched on the loop with a server-side cursor, and each batch is written by
a thread call. Each source is read in keyset batches of
:data:`SLICE_BATCH_ROWS` rows by ``id`` up to the largest id it held when the
build opened it, each batch one statement with no transaction across
batches: the cut is fixed, and the recorder's checkpoint is never held back
for the whole build.

A limit never fails a build. The rows cap, the file size cap, the time limit
and the sealed-file budget end the build (or skip files) and mark the file
truncated; the caller names it ``-partial``.
"""

from __future__ import annotations

import asyncio
import contextlib
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

from drakkar.dbfiles import secure_db_file
from drakkar.merge import MERGED_SCHEMA
from drakkar.recorder.archive import is_own_sealed_name, parse_db_start_ts
from drakkar.recorder.history import list_sealed, meta_path, read_meta
from drakkar.recorder.memstore import MEMORY_READ_LIMIT_SECONDS, PROGRESS_HANDLER_STEPS, MemoryCoverage
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
    SLICE_MAX_FILE_BYTES,
    SLICE_MAX_RAM_BYTES,
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
_NO_ID = -(2**63)

MemoryOpener = Callable[[], AbstractContextManager[sqlite3.Connection | None]]
Flush = Callable[[], Awaitable[str | None]]
# Told the reason and the error text of a failed Postgres read that still gives a file (a timeout or a close).
PostgresReadLog = Callable[[str, str], Awaitable[None]]


@dataclass(frozen=True)
class SliceWorker:
    """What a build needs to know of the worker that builds it, read once per request.

    ``open_memory`` opens a by-name connection to the memory store through
    its open guard (``None`` inside when the store is closed);
    ``coverage`` reads the id range the store holds.
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
    coverage: Callable[[], MemoryCoverage | None] = lambda: None
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


def _run_source(conn: sqlite3.Connection, sql: str, params: Sequence[Any] = ()) -> list[tuple]:
    """One statement on a source connection; a stop by the progress handler and every other error are told apart."""
    try:
        return conn.execute(sql, params).fetchall()
    except sqlite3.Error as exc:
        if getattr(exc, 'sqlite_errorcode', 0) & 0xFF == _SQLITE_INTERRUPT:
            raise _SourceTimedOut from exc
        raise _SourceUnreadable(str(exc)) from exc


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
        if getattr(exc, 'sqlite_errorcode', 0) & 0xFF == _SQLITE_INTERRUPT:
            raise _SourceTimedOut from exc
        raise _SourceUnreadable(str(exc)) from exc
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
    """One file a build reads: its path, kind, worker, and the rows of this process it must leave out."""

    path: str
    kind: str
    worker: str | None = None
    below_id: int | None = None


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
        """The recorder file schema (worker scope) or the merge result schema (cluster scope), plus the slice tables."""
        if self._cluster_scope:
            self._conn.executescript(MERGED_SCHEMA)
        else:
            self._conn.executescript(SCHEMA_EVENTS + SCHEMA_WORKER_CONFIG + SCHEMA_WORKER_STATE)
        self._conn.execute(SLICE_TABLE_DDL)
        self._conn.execute(SLICE_SOURCES_TABLE_DDL)
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
    coverage: MemoryCoverage | None
    notes: list[str] = field(default_factory=list)
    reads_memory: bool = False

    @property
    def cluster_scope(self) -> bool:
        return self.request.scope == SCOPE_CLUSTER

    # -- events in batches --

    def _copy_batches(self, fetch: Callable[[int, int], list[tuple]], worker_id: int | None) -> int:
        """Copy the rows ``fetch(after_id, limit)`` gives (``id`` first) in keyset batches; the rows written."""
        written = 0
        after = _NO_ID
        while not self.budget.stopped:
            if self.budget.out_of_time():
                self.budget.stop(TRUNCATED_TIME)
                break
            remaining = self.budget.remaining_rows()
            limit = min(SLICE_BATCH_ROWS, remaining + 1)
            rows = fetch(after, limit)
            if not rows:
                break
            take = rows[:remaining]
            if take:
                self.writer.insert_events([row[1:] for row in take], worker_id)
                written += len(take)
            if len(rows) > remaining:
                self.budget.stop(TRUNCATED_ROWS)
                break
            if len(rows) < limit:
                break
            after = rows[-1][0]
        return written

    def _event_fetcher(
        self, conn: sqlite3.Connection, max_id: int, *, below_id: int | None
    ) -> Callable[[int, int], list[tuple]]:
        present = _table_columns(conn, 'events')
        if 'id' not in present:
            raise _SourceUnreadable('no events table')
        nulled = frozenset() if self.request.output else _OUTPUT_COLUMNS
        select = _select_list(present, SLICE_EVENT_COLUMNS, nulled=nulled)
        where, params = sqlite_filters(self.request)
        if below_id is not None:
            where += ' AND id < ?'
            params.append(below_id)
        sql = f'SELECT id, {select} FROM events WHERE id > ? AND id <= ? AND {where} ORDER BY id LIMIT ?'
        return lambda after, limit: _run_source(conn, sql, [after, max_id, *params, limit])

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
            written = self._copy_batches(self._event_fetcher(conn, max_id, below_id=source.below_id), worker_id)
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
        if NOTE_SOURCE_UNREADABLE not in self.notes:
            self.notes.append(NOTE_SOURCE_UNREADABLE)
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
            below_id = None
            if (
                self.coverage is not None
                and meta is not None
                and meta.get('instance') == self.worker.instance
                and not self.cluster_scope
            ):
                below_id = self.coverage.floor_id
            chosen.append(_FileSource(path, SOURCE_KIND_SEALED, worker if isinstance(worker, str) else None, below_id))
        chosen.reverse()
        return chosen

    def copy_files(self, plan: SlicePlan, *, dedupe: bool) -> None:
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
        if not dedupe:
            sealed = [_FileSource(s.path, s.kind, s.worker) for s in sealed]
        done: set[str] = set()
        for source in [*sealed, *unsealed]:
            if self.budget.stopped:
                break
            self.copy_file(source, history_dir=self.worker.history_dir, done=done)

    # -- memory --

    def copy_memory(self, *, read: bool) -> None:
        """Copy the memory store rows (with ``read``), and its config when no file gave one. Blocking."""
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
                if not self.cluster_scope and not self.writer.has_worker_config:
                    self._arm(conn)
                    config = _source_config(conn)
                    if config:
                        self.writer.set_worker_config(config)
            except _SourceTimedOut:
                self.budget.stop(TRUNCATED_TIME)
            except _SourceUnreadable:
                if NOTE_SOURCE_UNREADABLE not in self.notes:
                    self.notes.append(NOTE_SOURCE_UNREADABLE)
            if read:
                self.writer.add_source(SOURCE_KIND_MEMORY, None, self.worker.worker, written)

    def _arm(self, conn: sqlite3.Connection) -> None:
        """Bound the next memory statement by the memory read limit and by the build's deadline."""
        clock = self.budget.clock
        deadline = min(clock() + MEMORY_READ_LIMIT_SECONDS, self.budget.deadline)
        conn.set_progress_handler(_deadline_handler(deadline, clock), PROGRESS_HANDLER_STEPS)

    def _copy_memory_rows(self, conn: sqlite3.Connection) -> int:
        self._arm(conn)
        max_id = _run_source(conn, 'SELECT MAX(id) FROM events')[0][0] or 0
        self._arm(conn)
        fetch = self._event_fetcher(conn, max_id, below_id=None)

        def armed(after: int, limit: int) -> list[tuple]:
            self._arm(conn)
            return fetch(after, limit)

        return self._copy_batches(armed, None)

    # -- postgres --

    def postgres_query(self) -> SliceQuery:
        """The parameters of the ``slice`` statement for this build."""
        own_instance = None
        floor_id = None
        if not self.cluster_scope and self.coverage is not None and self.reads_memory:
            own_instance, floor_id = self.worker.instance, self.coverage.floor_id
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
            exclude_from_id=floor_id,
            limit=self.budget.remaining_rows() + 1,
        )

    def write_postgres_rows(self, rows: Sequence[Any], counts: dict[str | None, int]) -> bool:
        """Write one batch of table rows in their order; whether the build wants more. Blocking."""
        remaining = self.budget.remaining_rows()
        values: list[tuple] = []
        for row in rows[:remaining]:
            event = tuple(
                None if (not self.request.output and col in _OUTPUT_COLUMNS) else row[col]
                for col in SLICE_EVENT_COLUMNS
            )
            worker = row['worker']
            counts[worker] = counts.get(worker, 0) + 1
            if self.cluster_scope:
                # The rows of a cluster slice carry their worker_id first, in table order.
                event = (self.writer.postgres_worker_id(worker, self.worker.cluster), *event)
            values.append(event)
        if values:
            self.writer.insert_events(values, None)
        if len(rows) > remaining:
            self.budget.stop(TRUNCATED_ROWS)
        elif self.budget.out_of_time():
            self.budget.stop(TRUNCATED_TIME)
        return not self.budget.stopped

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
    loop = asyncio.get_running_loop()
    deadline = loop.time() + build.budget.time_left()

    # A failure to write the slice file is not a failure of the table read:
    # it ends the read and is raised again below (a 500, not a 503).
    write_errors: list[BaseException] = []

    async def on_batch(rows: list[Any]) -> bool:
        try:
            return await asyncio.to_thread(build.write_postgres_rows, rows, counts)
        except (OSError, sqlite3.Error) as exc:
            write_errors.append(exc)
            return False

    try:
        await read_postgres_slice(
            settings,
            build.postgres_query(),
            deadline=deadline,
            batch_rows=SLICE_BATCH_ROWS,
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
    await asyncio.to_thread(build.add_postgres_sources, counts)


async def build_slice(
    request: SliceRequest,
    worker: SliceWorker,
    plan: SlicePlan,
    *,
    flush: Flush | None = None,
    flush_memory: Flush | None = None,
    on_postgres_failed: PostgresReadLog | None = None,
    connect: Any = None,
    clock: Callable[[], float] = time.monotonic,
    now: Callable[[], float] = time.time,
) -> BuiltSlice:
    """Build the slice of ``request`` on ``worker`` along ``plan``; the open file and what it holds.

    Asks for the recorder flush first when the plan reads the files, or for
    the flush of the memory store when it reads the store (a flush that
    does not end in time adds its note). Raises
    :class:`PostgresHistoryError` when the table cannot be read, and
    :class:`OSError` or :class:`sqlite3.Error` when the slice file cannot be
    written; the temp file is removed then.
    """
    notes: list[str] = []
    flusher = flush if plan.flush else flush_memory if plan.flush_memory else None
    if flusher is not None:
        note = await flusher()
        if note is not None:
            notes.append(note)
    target, ram_reason = await asyncio.to_thread(open_target, worker.storage_dir, worker.worker)
    if ram_reason is not None:
        await logger.ainfo('debug_slice_in_ram', category='debug', reason=ram_reason)
    budget = SliceBudget(
        max_rows=request.max_rows,
        max_bytes=SLICE_MAX_RAM_BYTES if target.in_ram else SLICE_MAX_FILE_BYTES,
        deadline=clock() + SLICE_TIME_LIMIT_SECONDS,
        clock=clock,
    )
    build = _Build(
        request=request,
        worker=worker,
        writer=SliceWriter(target, request, budget),
        budget=budget,
        coverage=worker.coverage(),
        notes=notes,
        reads_memory=plan.memory,
    )
    try:
        await asyncio.to_thread(build.writer.create_schema)
        if plan.sealed or plan.backlog or plan.live:
            await asyncio.to_thread(build.copy_files, plan, dedupe=plan.dedupe_memory)
        if plan.postgres and not budget.stopped:
            await _copy_postgres(build, connect=connect, on_failed=on_postgres_failed or _ignore_failure)
        if not build.cluster_scope:
            await asyncio.to_thread(build.copy_memory, read=plan.memory and not budget.stopped)
        await asyncio.to_thread(
            build.writer.finish, worker=worker.worker, cluster=worker.cluster, notes=notes, created_at=now()
        )
        handle, size = await asyncio.to_thread(target.open_result)
    except BaseException:
        await asyncio.to_thread(target.discard)
        raise
    name = slice_file_name(
        scope=request.scope,
        worker=worker.worker,
        cluster=worker.cluster,
        from_ts=request.from_ts,
        to_ts=request.to_ts,
        truncated=budget.truncated,
        compress=request.compress,
    )
    return BuiltSlice(handle=handle, name=name, rows=budget.rows, truncated=budget.truncated, size_bytes=size)


async def _ignore_failure(_reason: str, _error: str) -> None:
    return None
