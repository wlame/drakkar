"""Helpers of the slice tests: recorder files written by the test and a fake Postgres connection with a cursor.

``write_recorder_file`` writes a recorder file with the recorder's own DDL
and explicit event ids, and ``write_meta_file`` its meta file, so the
builder reads files shaped like the ones the recorder seals.

``SliceFakePostgres.connect`` stands in for :func:`asyncpg.connect`. Its
connection answers ``set_local`` and opens a cursor over ``rows`` (dicts
with the columns of the ``slice`` statement); ``error`` makes the cursor
fetch raise, ``hang`` makes it wait until cancelled, and ``close_error``
makes the graceful close raise. Every call lands in ``calls``.
"""

from __future__ import annotations

import asyncio
import json
import os
import sqlite3
from collections.abc import Iterable, Sequence
from typing import Any

from drakkar.recorder.schema import (
    EVENT_COLUMNS,
    INSERT_EVENT_SQL,
    SCHEMA_EVENTS,
    SCHEMA_WORKER_CONFIG,
    SCHEMA_WORKER_STATE,
)
from drakkar.timefmt import format_epoch_micro

T0 = 1_762_000_000.0  # 2025-11-01 12:26:40 UTC
HANG_SECONDS = 3600


def event(row_id: int, ts: float, **values: Any) -> tuple:
    """One event row in ``EVENT_COLUMNS`` order; the defaults make a ``consumed`` row of partition 0."""
    row = dict.fromkeys(EVENT_COLUMNS)
    row.update(id=row_id, ts=ts, dt=format_epoch_micro(ts), event='consumed', partition=0, origin='kafka')
    row.update(values)
    return tuple(row[column] for column in EVENT_COLUMNS)


def worker_config_row(worker: str, cluster: str = 'search') -> dict[str, Any]:
    return {'id': 1, 'worker_name': worker, 'cluster_name': cluster, 'created_at': T0, 'created_at_dt': 'dt'}


def write_recorder_file(
    path: str,
    rows: Iterable[tuple],
    *,
    worker: str | None = 'w-1',
    states: Sequence[float] = (),
    wal: bool = False,
) -> None:
    """A recorder file at ``path`` with ``rows``, a ``worker_config`` row (unless ``worker`` is None) and states."""
    conn = sqlite3.connect(path)
    try:
        if wal:
            conn.execute('PRAGMA journal_mode=WAL')
        conn.executescript(SCHEMA_EVENTS + SCHEMA_WORKER_CONFIG + SCHEMA_WORKER_STATE)
        conn.executemany(INSERT_EVENT_SQL, list(rows))
        if worker is not None:
            config = worker_config_row(worker)
            conn.execute(
                f'INSERT INTO worker_config ({", ".join(config)}) VALUES ({", ".join("?" * len(config))})',
                list(config.values()),
            )
        for updated_at in states:
            conn.execute(
                'INSERT INTO worker_state (uptime_seconds, updated_at, updated_at_dt) VALUES (?, ?, ?)',
                (1.0, updated_at, format_epoch_micro(updated_at)),
            )
        conn.commit()
    finally:
        conn.close()


def write_meta_file(path: str, *, worker: str, instance: str, first: float | None, last: float | None) -> None:
    """The meta file of a sealed file at ``path``."""
    meta = {
        'schema': 1,
        'worker': worker,
        'instance': instance,
        'cluster': 'search',
        'first_event_ts': None if first is None else format_epoch_micro(first),
        'last_event_ts': None if last is None else format_epoch_micro(last),
    }
    with open(path + '.meta.json', 'w', encoding='utf-8') as fh:
        json.dump(meta, fh)


def slice_rows(path: str, query: str = 'SELECT * FROM events ORDER BY id') -> list[tuple]:
    conn = sqlite3.connect(f'file:{path}?mode=ro', uri=True)
    try:
        return conn.execute(query).fetchall()
    finally:
        conn.close()


def pg_row(row_id: int, ts: float, *, worker: str = 'w-1', instance: str = 'aaaa', **values: Any) -> dict:
    """One answer row of the ``slice`` statement."""
    row = dict(zip(EVENT_COLUMNS, event(row_id, ts, **values), strict=True))
    row.update(worker=worker, instance=instance)
    return row


class SliceFakePostgres:
    """The server side of the fake: the rows the cursor answers and every call sent."""

    def __init__(self, rows: Sequence[dict] = ()) -> None:
        self.rows = list(rows)
        self.calls: list[tuple[Any, ...]] = []
        self.error: BaseException | None = None
        self.hang = False
        self.close_error: BaseException | None = None
        self.connections: list[FakeSliceConnection] = []

    async def connect(self, *, dsn: str) -> FakeSliceConnection:
        self.calls.append(('connect', dsn))
        conn = FakeSliceConnection(self)
        self.connections.append(conn)
        return conn

    def cursor_calls(self) -> list[tuple[Any, ...]]:
        return [call for call in self.calls if call[0] == 'cursor']


class _FakeTransaction:
    def __init__(self, server: SliceFakePostgres) -> None:
        self._server = server

    async def start(self) -> None:
        self._server.calls.append(('begin',))

    async def commit(self) -> None:
        self._server.calls.append(('commit',))


class _FakeCursor:
    def __init__(self, server: SliceFakePostgres, limit: int) -> None:
        self._server = server
        self._rows = server.rows[:limit]

    async def fetch(self, count: int) -> list[dict]:
        self._server.calls.append(('fetch', count))
        if self._server.hang:
            await asyncio.sleep(HANG_SECONDS)
        if self._server.error is not None:
            raise self._server.error
        batch, self._rows = self._rows[:count], self._rows[count:]
        return batch


class FakeSliceConnection:
    """One connection: ``transaction``, ``execute``, ``cursor``, ``close``, ``terminate``, ``is_closed``."""

    def __init__(self, server: SliceFakePostgres) -> None:
        self._server = server
        self._closed = False
        self.terminated = False

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction(self._server)

    async def execute(self, sql: str, *args: Any) -> str:
        self._server.calls.append(('execute', sql, args))
        return 'SELECT 1'

    async def cursor(self, sql: str, *args: Any) -> _FakeCursor:
        self._server.calls.append(('cursor', sql, args))
        return _FakeCursor(self._server, args[-1])

    async def close(self) -> None:
        self._server.calls.append(('close',))
        if self._server.close_error is not None:
            raise self._server.close_error
        self._closed = True

    def terminate(self) -> None:
        self.terminated = True
        self._closed = True

    def is_closed(self) -> bool:
        return self._closed


def temp_files(folder: str) -> list[str]:
    """The dot names in ``folder``: temp files a build may leave behind."""
    return sorted(name for name in os.listdir(folder) if name.startswith('.'))
