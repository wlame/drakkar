"""Slice downloads: the request checks, the source plan, the names, and the builder over every source.

Hermetic: recorder files and meta files written by the test in ``tmp_path``,
a real memory store (the memdb VFS, in this process), and a fake Postgres
connection with a cursor (``tests/slice_fakes.py``).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import re
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

import asyncpg
import pytest
from structlog.testing import capture_logs

from drakkar import slice_build, slices
from drakkar.config import PostgresConnectionConfig, ResilienceConfig
from drakkar.ownerlock import download_temp_pattern
from drakkar.providers.postgres import RECORDER_STATEMENT_TEMPLATES, render_recorder
from drakkar.recorder import postgres_history
from drakkar.recorder.memstore import MemoryStore
from drakkar.recorder.postgres import RecorderPostgresSettings
from drakkar.recorder.postgres_history import PostgresHistoryError, postgres_read_slot
from drakkar.recorder.schema import INSERT_EVENT_SQL, SCHEMA_EVENTS
from drakkar.slice_build import SliceBudget, SliceWorker, build_slice, sqlite_filters
from drakkar.slices import SliceRequestError, SliceSources, available_sources, parse_slice_request, plan_slice
from tests.slice_fakes import (
    T0,
    SliceFakePostgres,
    event,
    pg_row,
    slice_rows,
    temp_files,
    worker_config_row,
    write_meta_file,
    write_recorder_file,
)

FIXTURE = json.loads((Path(__file__).parent / 'fixtures' / 'slice_contract.json').read_text(encoding='utf-8'))
WORKER = 'w-1'
INSTANCE = 'aaaa'
CLUSTER = 'search'
SQLITE_SOURCES = SliceSources(worker=('files', 'history', 'memory'), cluster=('history',))
POSTGRES_SOURCES = SliceSources(worker=('memory', 'postgres'), cluster=('postgres',))
OFF_SOURCES = SliceSources(worker=('memory',), cluster=())
DSN = 'postgresql://drakkar:secret@db:5432/drakkar'


# --- The contract fixture ---


def _sources_of(case: dict) -> SliceSources:
    available = case.get('available', FIXTURE['available'])
    return SliceSources(worker=tuple(available['worker']), cluster=tuple(available['cluster']))


@pytest.mark.parametrize('case', FIXTURE['validation_cases'], ids=lambda case: case['name'])
def test_parse_slice_request_follows_the_pinned_cases(case):
    if case['error'] is None:
        parse_slice_request(case['query'], _sources_of(case))
        return
    with pytest.raises(SliceRequestError) as raised:
        parse_slice_request(case['query'], _sources_of(case))
    assert str(raised.value) == case['error']


def test_messages_are_pinned_in_check_order():
    assert FIXTURE['messages'] == [
        slices.MSG_FROM_TO,
        slices.MSG_TO_AFTER_FROM,
        slices.MSG_RANGE,
        slices.MSG_SCOPE,
        slices.MSG_SOURCE,
        slices.MSG_PARTITION,
        slices.MSG_OFFSETS_NEED_PARTITION,
        slices.MSG_OFFSETS_WHOLE,
        slices.MSG_OFFSETS_ORDER,
        slices.MSG_TASK_ID,
        slices.MSG_EVENTS_COUNT,
        slices.MSG_EVENT,
        slices.MSG_LABEL_PAIR,
        slices.MSG_LABEL_KEY,
        slices.MSG_LABEL_VALUE,
        slices.MSG_OUTPUT,
        slices.MSG_MAX_ROWS,
        slices.MSG_COMPRESS,
    ]
    assert FIXTURE['no_source_text'] == slices.NO_SOURCE_TEXT


def test_the_pinned_cases_reach_every_message():
    errors = {case['error'] for case in FIXTURE['validation_cases']}
    for message in FIXTURE['messages']:
        prefix = message.split('{', 1)[0]
        assert any(error and error.startswith(prefix) for error in errors), message


@pytest.mark.parametrize('case', FIXTURE['filters_cases'], ids=lambda case: case['filters'][:40])
def test_filters_json_follows_the_pinned_cases(case):
    assert parse_slice_request(case['query'], SQLITE_SOURCES).filters_json() == case['filters']


@pytest.mark.parametrize('case', FIXTURE['file_names'], ids=lambda case: case['name'])
def test_slice_file_name_follows_the_pinned_cases(case):
    name = slices.slice_file_name(
        scope=case['scope'],
        worker=case['worker'],
        cluster=case['cluster'],
        from_ts=case['from'],
        to_ts=case['to'],
        truncated=case['truncated'],
        compress=case['compress'],
    )
    assert name == case['name']
    # Never the rule of a worker's own recorder files.
    assert not re.search(r'\d{4}-\d{2}-\d{2}__\d{2}_\d{2}_\d{2}\.db$', name)


def test_ddl_answers_headers_and_limits_are_pinned():
    assert FIXTURE['ddl'] == {'slice': slices.SLICE_TABLE_DDL, 'slice_sources': slices.SLICE_SOURCES_TABLE_DDL}
    assert FIXTURE['file_name_time_format'] == slices.FILE_NAME_TIME_FORMAT
    assert FIXTURE['schema'] == slices.SLICE_SCHEMA_VERSION
    assert FIXTURE['answers'] == {
        'cluster_unavailable': {'status': 409, 'error': slices.CLUSTER_UNAVAILABLE_ERROR},
        'busy': {'status': 429, 'error': slices.BUSY_ERROR, 'retry_after_seconds': slices.SLICE_RETRY_AFTER_SECONDS},
        'open_bytes_full': {
            'status': 429,
            'error': slices.OPEN_BYTES_ERROR,
            'retry_after_seconds': slices.SLICE_RETRY_AFTER_SECONDS,
        },
        'open_count_full': {
            'status': 429,
            'error': slices.OPEN_COUNT_ERROR,
            'retry_after_seconds': slices.SLICE_RETRY_AFTER_SECONDS,
        },
        'postgres_failed': {'status': 503, 'error': slices.POSTGRES_FAILED_ERROR},
        'failed': {'status': 500, 'error_prefix': slices.FAILED_ERROR_PREFIX},
    }
    assert FIXTURE['headers'] == {'rows': slices.HEADER_ROWS, 'truncated': slices.HEADER_TRUNCATED}
    assert FIXTURE['notes'] == [
        slices.NOTE_FLUSH_TIMED_OUT,
        slices.NOTE_FLUSH_FAILED,
        slices.NOTE_SOURCE_UNREADABLE,
        slices.NOTE_POSTGRES_QUEUE_WAITING,
    ]
    assert FIXTURE['limits'] == {
        'max_range_seconds': 604800,
        'max_rows': 1_000_000,
        'default_max_rows': 200_000,
        'max_file_bytes': 1024**3,
        'max_ram_bytes': 64 * 1024**2,
        'time_limit_seconds': 60.0,
        'batch_rows': 5000,
        'chunk_bytes': 8 * 1024**2,
        'postgres_first_fetch_rows': 100,
        'postgres_flush_seconds': 3.0,
        'open_max_bytes': 2 * 1024**3,
        'open_max_count': 16,
        'send_idle_seconds': 120.0,
        'min_epoch': 0,
        'max_epoch': 253402300799,
        'flush_wait_seconds': 5.0,
        'max_events': 40,
        'builds_per_worker': 1,
    }
    assert FIXTURE['limits']['max_rows'] == slices.SLICE_MAX_ROWS
    assert FIXTURE['limits']['max_file_bytes'] == slices.SLICE_MAX_FILE_BYTES
    assert FIXTURE['limits']['max_ram_bytes'] == slices.SLICE_MAX_RAM_BYTES
    assert FIXTURE['limits']['time_limit_seconds'] == slices.SLICE_TIME_LIMIT_SECONDS
    assert FIXTURE['limits']['batch_rows'] == slices.SLICE_BATCH_ROWS
    assert FIXTURE['limits']['chunk_bytes'] == slices.SLICE_CHUNK_BYTES
    assert FIXTURE['limits']['open_max_bytes'] == slices.SLICE_OPEN_MAX_BYTES
    assert FIXTURE['limits']['open_max_count'] == slices.SLICE_OPEN_MAX_COUNT
    assert FIXTURE['limits']['max_epoch'] == slices.MAX_EPOCH


def test_the_slice_tables_create_on_sqlite():
    with contextlib.closing(sqlite3.connect(':memory:')) as conn:
        conn.execute(slices.SLICE_TABLE_DDL)
        conn.execute(slices.SLICE_SOURCES_TABLE_DDL)
        assert [row[1] for row in conn.execute('PRAGMA table_info(slice)')][-3:] == ['rows', 'truncated', 'notes']


def test_the_postgres_slice_statement_renders_schema_qualified():
    rendered = render_recorder(RECORDER_STATEMENT_TEMPLATES['slice'], schema='drakkar', table='fast_recorder_events')
    assert 'FROM drakkar.fast_recorder_events WHERE cluster = $1 AND ts >= $2 AND ts < $3' in rendered
    assert rendered.endswith('ORDER BY ts, worker, instance, id LIMIT $14')
    assert '$15' not in rendered


# --- Request details ---


def test_parse_slice_request_defaults_to_every_available_source():
    request = parse_slice_request({'from': '1', 'to': '2'}, SQLITE_SOURCES)
    assert request.sources == ('files', 'history', 'memory')
    assert request.scope == 'worker'
    assert request.max_rows == slices.SLICE_DEFAULT_MAX_ROWS
    assert request.output is True


def test_parse_slice_request_keeps_the_event_order_without_repeats():
    request = parse_slice_request({'from': '1', 'to': '2', 'events': 'task_failed,consumed,task_failed'}, OFF_SOURCES)
    assert request.events == ('task_failed', 'consumed')


@pytest.mark.parametrize(
    ('provider', 'memory', 'history', 'postgres', 'worker', 'cluster'),
    [
        ('sqlite', True, True, False, ('files', 'history', 'memory'), ('history',)),
        ('sqlite', False, False, False, ('files',), ()),
        ('postgres', True, False, True, ('memory', 'postgres'), ('postgres',)),
        ('postgres', True, False, False, ('memory',), ()),
        ('none', True, False, False, ('memory',), ()),
        ('none', False, False, False, (), ()),
    ],
)
def test_available_sources_per_mode(provider, memory, history, postgres, worker, cluster):
    sources = available_sources(provider=provider, has_memory=memory, has_history=history, has_postgres=postgres)
    assert sources == SliceSources(worker=worker, cluster=cluster)


@pytest.mark.parametrize(
    ('provider', 'sources', 'scope', 'expected'),
    [
        ('sqlite', SQLITE_SOURCES, None, slices.SlicePlan(flush=True, sealed=True, backlog=True, live=True)),
        ('sqlite', SQLITE_SOURCES, 'memory', slices.SlicePlan(flush_memory=True, memory=True)),
        (
            'sqlite',
            SQLITE_SOURCES,
            'memory,history',
            slices.SlicePlan(flush_memory=True, sealed=True, memory=True, dedupe_memory=True),
        ),
        ('sqlite', SQLITE_SOURCES, 'files,memory', slices.SlicePlan(flush=True, backlog=True, live=True)),
        (
            'postgres',
            POSTGRES_SOURCES,
            None,
            slices.SlicePlan(flush_postgres=True, postgres=True, memory=True, dedupe_memory=True),
        ),
        ('postgres', POSTGRES_SOURCES, 'postgres', slices.SlicePlan(flush_postgres=True, postgres=True)),
        ('none', OFF_SOURCES, None, slices.SlicePlan(flush_memory=True, memory=True)),
    ],
)
def test_plan_slice_per_mode(provider, sources, scope, expected):
    query = {'from': '1', 'to': '2'}
    if scope is not None:
        query['sources'] = scope
    assert plan_slice(parse_slice_request(query, sources), provider) == expected


@pytest.mark.parametrize(
    ('provider', 'sources', 'expected'),
    [
        ('sqlite', SQLITE_SOURCES, slices.SlicePlan(sealed=True)),
        ('postgres', POSTGRES_SOURCES, slices.SlicePlan(flush_postgres=True, postgres=True)),
    ],
)
def test_plan_slice_for_the_cluster(provider, sources, expected):
    request = parse_slice_request({'from': '1', 'to': '2', 'scope': 'cluster'}, sources)
    assert plan_slice(request, provider) == expected


def test_sqlite_filters_quote_the_label_key_in_the_path():
    request = parse_slice_request(
        {'from': '1', 'to': '2', 'label_key': 'a.b c', 'label_value': 'x', 'events': 'consumed'}, OFF_SOURCES
    )
    where, params = sqlite_filters(request)
    assert 'event IN (?)' in where
    assert params[-2:] == ['$."a.b c"', 'x']


def test_slice_budget_keeps_the_first_limit():
    budget = SliceBudget(max_rows=1, max_bytes=1, deadline=10.0, clock=lambda: 0.0)
    budget.skip_files()
    assert budget.truncated == 'files'
    budget.stop('rows')
    budget.stop('time')
    budget.skip_files()
    assert budget.truncated == 'rows'


# --- The builder ---


def _request(query: dict | None = None, sources: SliceSources = SQLITE_SOURCES) -> slices.SliceRequest:
    return parse_slice_request({'from': str(T0), 'to': str(T0 + 900), **(query or {})}, sources)


def _folders(tmp_path) -> tuple[str, str]:
    own = tmp_path / 'storage' / CLUSTER / WORKER
    history = tmp_path / 'shared' / 'history' / CLUSTER
    own.mkdir(parents=True)
    history.mkdir(parents=True)
    return str(own), str(history)


def _worker(tmp_path, *, provider: str = 'sqlite', live: str = '', max_files: int = 256, **values: Any) -> SliceWorker:
    own, history = _folders(tmp_path) if not (tmp_path / 'storage').exists() else _existing(tmp_path)
    fields: dict[str, Any] = {
        'worker': WORKER,
        'instance': INSTANCE,
        'cluster': CLUSTER,
        'provider': provider,
        'storage_dir': own,
        'history_dir': history,
        'live_path': live,
        'max_files': max_files,
    }
    fields.update(values)
    return SliceWorker(**fields)


def _existing(tmp_path) -> tuple[str, str]:
    return str(tmp_path / 'storage' / CLUSTER / WORKER), str(tmp_path / 'shared' / 'history' / CLUSTER)


def _save(built: slice_build.BuiltSlice, tmp_path) -> str:
    path = str(tmp_path / built.name.removesuffix('.gz'))
    with built.handle as handle, open(path, 'wb') as out:
        out.write(handle.read())
    return path


def _offsets(path: str) -> list[tuple]:
    return slice_rows(path, 'SELECT id, "offset" FROM events ORDER BY id')


def _sources(path: str) -> list[tuple]:
    return slice_rows(path, 'SELECT kind, name, worker, rows FROM slice_sources ORDER BY rowid')


def _slice_row(path: str) -> dict:
    conn = sqlite3.connect(path)
    try:
        cursor = conn.execute('SELECT * FROM slice')
        row = cursor.fetchone()
        return {col[0]: value for col, value in zip(cursor.description, row, strict=True)}
    finally:
        conn.close()


async def _build(request, worker, provider: str | None = None, **kwargs) -> slice_build.BuiltSlice:
    plan = plan_slice(request, provider or worker.provider)
    return await build_slice(request, worker, plan, **kwargs)


def _own_file(folder: str, stamp: str) -> str:
    return os.path.join(folder, f'{WORKER}-{stamp}.db')


async def test_build_reads_sealed_then_backlog_then_live_with_ids_from_one(tmp_path):
    own, history = _folders(tmp_path)
    sealed = _own_file(history, '2025-11-01__12_00_00')
    write_recorder_file(sealed, [event(1, T0 + 1, offset=1), event(2, T0 + 2, offset=2)])
    write_meta_file(sealed, worker=WORKER, instance='old', first=T0 + 1, last=T0 + 2)
    backlog = _own_file(own, '2025-11-01__12_20_00')
    write_recorder_file(backlog, [event(1, T0 + 3, offset=3)], states=[T0 + 3, T0 - 100])
    live = _own_file(own, '2025-11-01__12_30_00')
    write_recorder_file(live, [event(1, T0 + 4, offset=4), event(2, T0 + 5000, offset=99)], wal=True)
    other = os.path.join(history, 'w-2-2025-11-01__12_00_00.db')
    write_recorder_file(other, [event(1, T0 + 1, offset=50)], worker='w-2')
    flushed = []

    async def flush() -> None:
        flushed.append(True)

    built = await _build(_request(), _worker(tmp_path, live=live), flush=flush)
    path = _save(built, tmp_path)

    assert flushed == [True]
    assert _offsets(path) == [(1, 1), (2, 2), (3, 3), (4, 4)]
    assert _sources(path) == [
        ('sealed', os.path.basename(sealed), WORKER, 2),
        ('backlog', os.path.basename(backlog), WORKER, 1),
        ('live', os.path.basename(live), WORKER, 1),
    ]
    assert slice_rows(path, 'SELECT worker_name FROM worker_config') == [(WORKER,)]
    assert slice_rows(path, 'SELECT updated_at FROM worker_state') == [(T0 + 3,)]
    row = _slice_row(path)
    assert row['scope'] == 'worker' and row['worker'] == WORKER and row['cluster'] == CLUSTER
    assert row['rows'] == 4 and row['truncated'] is None and json.loads(row['notes']) == []
    assert row['filters'] == _request().filters_json()
    assert built.rows == 4 and built.truncated is None
    assert built.name == 'w-1-slice-2025-11-01_12-26-40__2025-11-01_12-41-40.db'
    assert temp_files(own) == []
    # The recorder schema: every tool that opens a recorder file opens a slice.
    tables = {row[0] for row in slice_rows(path, "SELECT name FROM sqlite_master WHERE type = 'table'")}
    assert {'events', 'worker_config', 'worker_state', 'slice', 'slice_sources'} <= tables


async def test_build_notes_a_flush_that_did_not_end(tmp_path):
    async def flush() -> str:
        return slices.NOTE_FLUSH_TIMED_OUT

    built = await _build(_request({'sources': 'files'}), _worker(tmp_path), flush=flush)

    assert json.loads(_slice_row(_save(built, tmp_path))['notes']) == ['flush_timed_out']


async def test_build_finds_a_backlog_file_sealed_during_the_build_by_its_name(tmp_path, monkeypatch):
    own, history = _folders(tmp_path)
    name = f'{WORKER}-2025-11-01__12_20_00.db'
    write_recorder_file(os.path.join(history, name), [event(1, T0 + 3, offset=3)])
    # Listed in the own folder when the build started, moved to the history folder since.
    monkeypatch.setattr(slice_build, 'own_db_files', lambda _folder, _worker: [os.path.join(own, name)])

    built = await _build(_request(), _worker(tmp_path))
    path = _save(built, tmp_path)

    assert _offsets(path) == [(1, 3)]
    assert _sources(path) == [('sealed', name, WORKER, 1)]


async def test_build_skips_sealed_files_outside_the_range_by_meta_and_opens_files_without_meta(tmp_path):
    _own, history = _folders(tmp_path)
    before = _own_file(history, '2025-11-01__10_00_00')
    write_recorder_file(before, [event(1, T0 - 10, offset=1)])
    write_meta_file(before, worker=WORKER, instance='old', first=T0 - 20, last=T0 - 10)
    empty = _own_file(history, '2025-11-01__11_00_00')
    write_recorder_file(empty, [])
    write_meta_file(empty, worker=WORKER, instance='old', first=None, last=None)
    no_meta = _own_file(history, '2025-11-01__12_00_00')
    write_recorder_file(no_meta, [event(1, T0 + 1, offset=2)])
    after_name = _own_file(history, '2025-11-01__13_00_00')  # named after the range ends
    write_recorder_file(after_name, [event(1, T0 + 1, offset=3)])

    built = await _build(_request({'sources': 'history'}), _worker(tmp_path))
    path = _save(built, tmp_path)

    assert _offsets(path) == [(1, 2)]
    assert _sources(path) == [('sealed', os.path.basename(no_meta), WORKER, 1)]


async def test_build_reads_at_most_max_files_sealed_files_newest_first_and_marks_files(tmp_path):
    _own, history = _folders(tmp_path)
    for hour, offset in ((10, 1), (11, 2), (12, 3)):
        path = _own_file(history, f'2025-11-01__{hour}_00_00')
        write_recorder_file(path, [event(1, T0 + offset, offset=offset)])
        write_meta_file(path, worker=WORKER, instance='old', first=T0 + offset, last=T0 + offset)

    built = await _build(_request({'sources': 'history'}), _worker(tmp_path, max_files=2))
    path = _save(built, tmp_path)

    assert _offsets(path) == [(1, 2), (2, 3)]
    assert built.truncated == 'files'
    assert built.name.endswith('-partial.db')
    assert _slice_row(path)['truncated'] == 'files'


async def test_build_notes_an_unreadable_file_and_goes_on(tmp_path):
    own, _history = _folders(tmp_path)
    broken = _own_file(own, '2025-11-01__12_10_00')
    Path(broken).write_bytes(b'not a database' * 100)
    merged = _own_file(own, '2025-11-01__12_15_00')
    conn = sqlite3.connect(merged)
    conn.execute('CREATE TABLE workers (id INTEGER)')
    conn.close()
    good = _own_file(own, '2025-11-01__12_20_00')
    write_recorder_file(good, [event(1, T0 + 1, offset=7)])

    built = await _build(_request({'sources': 'files'}), _worker(tmp_path))
    path = _save(built, tmp_path)

    assert _offsets(path) == [(1, 7)]
    assert json.loads(_slice_row(path)['notes']) == ['source_unreadable']
    assert [row[0:2] for row in _sources(path)] == [
        ('backlog', os.path.basename(broken)),
        ('backlog', os.path.basename(merged)),
        ('backlog', os.path.basename(good)),
    ]


async def test_build_applies_every_filter_and_nulls_the_output(tmp_path):
    own, _history = _folders(tmp_path)
    live = _own_file(own, '2025-11-01__12_20_00')
    write_recorder_file(
        live,
        [
            event(
                1,
                T0 + 1,
                partition=3,
                offset=10,
                task_id='t-1',
                event='task_failed',
                labels='{"a.b c": "x"}',
                stdout='out',
                stderr='err',
            ),
            event(2, T0 + 2, partition=3, offset=10, task_id='t-1', event='task_failed', labels='not json'),
            event(3, T0 + 3, partition=3, offset=10, task_id='t-2', event='task_failed', labels='{"a.b c": "x"}'),
            event(4, T0 + 4, partition=4, offset=10, task_id='t-1', event='task_failed', labels='{"a.b c": "x"}'),
            event(5, T0 + 5, partition=3, offset=30, task_id='t-1', event='task_failed', labels='{"a.b c": "x"}'),
            event(6, T0 + 6, partition=3, offset=10, task_id='t-1', event='consumed', labels='{"a.b c": "x"}'),
        ],
    )
    query = {
        'sources': 'files',
        'partition': '3',
        'offset_from': '5',
        'offset_to': '20',
        'task_id': 't-1',
        'events': 'task_failed',
        'label_key': 'a.b c',
        'label_value': 'x',
        'output': '0',
    }

    built = await _build(_request(query), _worker(tmp_path, live=live))
    path = _save(built, tmp_path)

    assert slice_rows(path, 'SELECT id, ts, stdout, stderr FROM events') == [(1, T0 + 1, None, None)]


async def test_build_cuts_at_max_rows_and_marks_rows(tmp_path):
    own, _history = _folders(tmp_path)
    write_recorder_file(_own_file(own, '2025-11-01__12_10_00'), [event(i, T0 + i, offset=i) for i in range(1, 4)])
    write_recorder_file(_own_file(own, '2025-11-01__12_20_00'), [event(1, T0 + 5, offset=5)])

    built = await _build(_request({'sources': 'files', 'max_rows': '3'}), _worker(tmp_path))
    path = _save(built, tmp_path)

    assert _offsets(path) == [(1, 1), (2, 2), (3, 3)]
    assert built.truncated == 'rows'
    assert _slice_row(path)['rows'] == 3
    assert built.name.endswith('-partial.db')


async def test_build_reads_in_batches(tmp_path, monkeypatch):
    monkeypatch.setattr(slice_build, 'SLICE_BATCH_ROWS', 2)
    own, _history = _folders(tmp_path)
    write_recorder_file(_own_file(own, '2025-11-01__12_10_00'), [event(i, T0 + i, offset=i) for i in range(1, 6)])

    built = await _build(_request({'sources': 'files'}), _worker(tmp_path))

    assert [row[1] for row in _offsets(_save(built, tmp_path))] == [1, 2, 3, 4, 5]
    assert built.truncated is None


async def test_build_cuts_at_the_file_size_and_marks_bytes(tmp_path, monkeypatch):
    monkeypatch.setattr(slice_build, 'SLICE_MAX_FILE_BYTES', 1)
    monkeypatch.setattr(slice_build, 'SLICE_BATCH_ROWS', 2)
    own, _history = _folders(tmp_path)
    write_recorder_file(_own_file(own, '2025-11-01__12_10_00'), [event(i, T0 + i, offset=i) for i in range(1, 6)])

    built = await _build(_request({'sources': 'files'}), _worker(tmp_path))

    assert built.truncated == 'bytes'
    assert len(_offsets(_save(built, tmp_path))) == 2


class SteppingClock:
    """A monotonic clock that moves ``step`` seconds on every read."""

    def __init__(self, step: float) -> None:
        self.now = 0.0
        self.step = step

    def __call__(self) -> float:
        self.now += self.step
        return self.now


async def test_build_stops_at_the_time_limit_and_marks_time(tmp_path, monkeypatch):
    monkeypatch.setattr(slice_build, 'SLICE_BATCH_ROWS', 1)
    own, _history = _folders(tmp_path)
    write_recorder_file(_own_file(own, '2025-11-01__12_10_00'), [event(i, T0 + i, offset=i) for i in range(1, 50)])

    built = await _build(_request({'sources': 'files'}), _worker(tmp_path), clock=SteppingClock(5.0))

    built.handle.close()
    assert built.truncated == 'time'
    assert 0 < built.rows < 49
    assert built.name.endswith('-partial.db')


async def test_a_source_statement_stopped_by_the_deadline_marks_time(tmp_path, monkeypatch):
    own, _history = _folders(tmp_path)
    write_recorder_file(_own_file(own, '2025-11-01__12_10_00'), [event(i, T0 + i, offset=i) for i in range(1, 2000)])
    # The progress handler answers "stop" on its first call: the deadline has passed inside the statement.
    monkeypatch.setattr(slice_build, 'PROGRESS_HANDLER_STEPS', 1)
    monkeypatch.setattr(slice_build, '_deadline_handler', lambda _deadline, _clock: lambda: 1)

    built = await _build(_request({'sources': 'files'}), _worker(tmp_path))

    built.handle.close()
    assert built.truncated == 'time'
    assert built.rows == 0


def _stop_after_the_first_batch(monkeypatch, *, how: str) -> None:
    """Make the second batch of a source fail: stopped by the real progress handler, or unreadable."""
    first_done = [False]
    real_fetch = slice_build._fetch_chunk
    monkeypatch.setattr(slice_build, 'PROGRESS_HANDLER_STEPS', 1)
    monkeypatch.setattr(slice_build, '_deadline_handler', lambda _deadline, _clock: lambda: int(first_done[0]))

    def fetch(conn, sql, params, max_bytes):
        if first_done[0] and how == 'unreadable':
            raise slice_build._SourceUnreadable('database disk image is malformed')
        rows = real_fetch(conn, sql, params, max_bytes)
        first_done[0] = True
        return rows

    monkeypatch.setattr(slice_build, '_fetch_chunk', fetch)


@pytest.mark.parametrize(
    ('how', 'truncated', 'notes'), [('stopped', 'time', []), ('unreadable', None, ['source_unreadable'])]
)
async def test_a_file_source_stopped_after_its_first_batch_lists_the_rows_it_wrote(
    tmp_path, monkeypatch, how, truncated, notes
):
    monkeypatch.setattr(slice_build, 'SLICE_BATCH_ROWS', 2)
    own, _history = _folders(tmp_path)
    backlog = _own_file(own, '2025-11-01__12_10_00')
    write_recorder_file(backlog, [event(i, T0 + i, offset=i) for i in range(1, 6)])
    _stop_after_the_first_batch(monkeypatch, how=how)

    built = await _build(_request({'sources': 'files'}), _worker(tmp_path))
    path = _save(built, tmp_path)

    assert _offsets(path) == [(1, 1), (2, 2)]
    assert _sources(path) == [('backlog', os.path.basename(backlog), WORKER, 2)]
    assert _slice_row(path)['rows'] == 2 and built.rows == 2
    assert built.truncated == truncated
    assert json.loads(_slice_row(path)['notes']) == notes


async def test_a_memory_read_stopped_after_its_first_batch_lists_the_rows_it_wrote(tmp_path, monkeypatch):
    monkeypatch.setattr(slice_build, 'SLICE_BATCH_ROWS', 2)
    store = await _memory_store([event(i, T0 + i, offset=i) for i in range(1, 6)])
    _stop_after_the_first_batch(monkeypatch, how='stopped')
    try:
        worker = _worker(tmp_path, provider='none', open_memory=store.open_by_name)
        built = await _build(_request(sources=OFF_SOURCES), worker)
    finally:
        await store.close()
    path = _save(built, tmp_path)

    assert built.truncated == 'time'
    assert _offsets(path) == [(1, 1), (2, 2)]
    assert _sources(path) == [('memory', None, WORKER, 2)]
    assert _slice_row(path)['rows'] == 2


async def test_build_without_a_folder_runs_in_ram_and_logs_it(tmp_path):
    store = await _memory_store([event(1, T0 + 1, offset=1)], config=True)
    try:
        worker = _worker(tmp_path, provider='none', storage_dir='', open_memory=store.open_by_name)
        with capture_logs() as logs:
            built = await _build(_request(sources=OFF_SOURCES), worker)
    finally:
        await store.close()
    path = _save(built, tmp_path)

    assert [entry for entry in logs if entry['event'] == 'debug_slice_in_ram'] == [
        {'event': 'debug_slice_in_ram', 'log_level': 'info', 'category': 'debug', 'reason': 'storage_dir_unset'}
    ]
    assert _offsets(path) == [(1, 1)]
    assert _sources(path) == [('memory', None, WORKER, 1)]
    # Without a file source the config comes from the memory store, as the recorder writes it.
    assert slice_rows(path, 'SELECT worker_name FROM worker_config') == [(WORKER,)]


async def test_build_in_ram_stops_at_the_ram_cap(tmp_path, monkeypatch):
    monkeypatch.setattr(slice_build, 'SLICE_MAX_RAM_BYTES', 1)
    store = await _memory_store([event(i, T0 + i, offset=i) for i in range(1, 4)])
    try:
        worker = _worker(tmp_path, provider='none', storage_dir='', open_memory=store.open_by_name)
        built = await _build(_request(sources=OFF_SOURCES), worker)
    finally:
        await store.close()

    built.handle.close()
    assert built.truncated == 'bytes'


async def test_build_with_an_unusable_folder_runs_in_ram(tmp_path):
    with capture_logs() as logs:
        built = await _build(
            _request({'sources': 'memory'}, OFF_SOURCES),
            _worker(tmp_path, provider='none', storage_dir=str(tmp_path / 'missing'), open_memory=None),
        )
    built.handle.close()

    assert [entry['reason'] for entry in logs if entry['event'] == 'debug_slice_in_ram'] == ['storage_dir_unusable']


async def test_build_names_its_temp_file_for_the_start_sweep(tmp_path, monkeypatch):
    own, _history = _folders(tmp_path)
    seen: list[str] = []
    real = slice_build._open_temp_target

    def spy(folder, worker):
        target = real(folder, worker)
        seen.append(os.path.basename(target.path))
        assert oct(os.stat(target.path).st_mode & 0o777) == '0o600'
        return target

    monkeypatch.setattr(slice_build, '_open_temp_target', spy)
    built = await _build(_request({'sources': 'files'}), _worker(tmp_path))
    built.handle.close()

    (name,) = seen
    assert re.fullmatch(rf'\.{WORKER}-slice-[0-9a-f]{{16}}\.download', name)
    assert download_temp_pattern(WORKER).fullmatch(name)
    assert temp_files(own) == []


def test_temp_target_settings_that_fail_remove_the_temp_file(tmp_path, monkeypatch):
    own, _history = _folders(tmp_path)
    real_connect = sqlite3.connect

    class RefusingSettings(sqlite3.Connection):
        def execute(self, sql, *args):
            if sql.startswith('PRAGMA'):
                raise sqlite3.OperationalError('disk I/O error')
            return super().execute(sql, *args)

    monkeypatch.setattr(
        slice_build.sqlite3, 'connect', lambda *args, **kwargs: real_connect(*args, factory=RefusingSettings, **kwargs)
    )
    with pytest.raises(sqlite3.OperationalError):
        slice_build._open_temp_target(own, WORKER)

    assert temp_files(own) == []


async def test_a_failed_write_removes_the_temp_file(tmp_path, monkeypatch):
    own, _history = _folders(tmp_path)
    write_recorder_file(_own_file(own, '2025-11-01__12_10_00'), [event(1, T0 + 1, offset=1)])

    def broken(self, rows, worker_id):
        raise sqlite3.OperationalError('database or disk is full')

    monkeypatch.setattr(slice_build.SliceWriter, 'insert_events', broken)
    with pytest.raises(sqlite3.OperationalError):
        await _build(_request({'sources': 'files'}), _worker(tmp_path))

    assert temp_files(own) == []


# --- Memory and the rule against double rows ---


async def _memory_store(rows: list[tuple], *, config: bool = False) -> MemoryStore:
    store = MemoryStore(INSTANCE, store_config=True)
    await store.open()
    await store.writer.executemany(INSERT_EVENT_SQL, rows)
    if config:
        values = worker_config_row(WORKER)
        await store.writer.execute(
            f'INSERT INTO worker_config ({", ".join(values)}) VALUES ({", ".join("?" * len(values))})',
            list(values.values()),
        )
    await store.writer.commit()
    return store


async def test_memory_only_on_sqlite_reads_the_store_and_no_file(tmp_path):
    own, _history = _folders(tmp_path)
    write_recorder_file(_own_file(own, '2025-11-01__12_10_00'), [event(1, T0 + 1, offset=1)])
    store = await _memory_store([event(5, T0 + 5, offset=5)])
    try:
        worker = _worker(tmp_path, open_memory=store.open_by_name)
        built = await _build(_request({'sources': 'memory'}), worker, flush=_no_flush, flush_memory=_memory_flushed)
    finally:
        await store.close()
    path = _save(built, tmp_path)

    assert _offsets(path) == [(1, 5)]
    assert _sources(path) == [('memory', None, WORKER, 1)]


async def _no_flush() -> None:
    raise AssertionError('a memory-only slice asked for a flush of the file')


async def _memory_flushed() -> None:
    return None


async def test_memory_and_history_leave_out_the_rows_the_store_holds(tmp_path):
    _own, history = _folders(tmp_path)
    sealed = _own_file(history, '2025-11-01__12_00_00')
    # Rows 4 and 5 of this process are in the sealed file and still in the memory store.
    write_recorder_file(sealed, [event(i, T0 + i, offset=i) for i in range(3, 6)])
    write_meta_file(sealed, worker=WORKER, instance=INSTANCE, first=T0 + 3, last=T0 + 5)
    older = _own_file(history, '2025-11-01__11_00_00')
    # A file of an earlier process with the same ids: nothing of it is left out.
    write_recorder_file(older, [event(4, T0 + 0.5, offset=40)])
    write_meta_file(older, worker=WORKER, instance='earlier', first=T0 + 0.5, last=T0 + 0.5)
    store = await _memory_store([event(i, T0 + i, offset=i) for i in range(4, 8)])
    try:
        worker = _worker(tmp_path, open_memory=store.open_by_name)
        built = await _build(_request({'sources': 'memory,history'}), worker)
    finally:
        await store.close()
    path = _save(built, tmp_path)

    # The store first, then the sealed files oldest first, each row once.
    assert [row[1] for row in _offsets(path)] == [4, 5, 6, 7, 40, 3]
    assert [row[0] for row in _offsets(path)] == [1, 2, 3, 4, 5, 6]


async def test_files_and_memory_on_sqlite_read_each_row_once(tmp_path):
    own, _history = _folders(tmp_path)
    live = _own_file(own, '2025-11-01__12_10_00')
    write_recorder_file(live, [event(i, T0 + i, offset=i) for i in range(1, 4)])
    # After the flush the memory store holds the same rows with the same ids.
    store = await _memory_store([event(i, T0 + i, offset=i) for i in range(1, 4)])
    try:
        worker = _worker(tmp_path, live=live, open_memory=store.open_by_name)
        built = await _build(_request({'sources': 'files,memory'}), worker)
    finally:
        await store.close()

    assert [row[1] for row in _offsets(_save(built, tmp_path))] == [1, 2, 3]


async def test_a_memory_read_stopped_by_its_limit_marks_time(tmp_path, monkeypatch):
    store = await _memory_store([event(i, T0 + i, offset=i) for i in range(1, 50)])
    monkeypatch.setattr(slice_build, 'PROGRESS_HANDLER_STEPS', 1)
    monkeypatch.setattr(slice_build, '_deadline_handler', lambda _deadline, _clock: lambda: 1)
    try:
        worker = _worker(tmp_path, provider='none', open_memory=store.open_by_name)
        built = await _build(_request(sources=OFF_SOURCES), worker)
    finally:
        await store.close()

    built.handle.close()
    assert built.truncated == 'time'


async def test_a_closed_memory_store_gives_an_empty_memory_source(tmp_path):
    store = await _memory_store([event(1, T0 + 1, offset=1)])
    await store.close()

    built = await _build(
        _request(sources=OFF_SOURCES), _worker(tmp_path, provider='none', open_memory=store.open_by_name)
    )
    path = _save(built, tmp_path)

    assert _offsets(path) == []
    assert _sources(path) == [('memory', None, WORKER, 0)]


# --- The whole cluster on sqlite ---


async def test_cluster_slice_reads_every_sealed_file_into_the_merge_schema(tmp_path):
    _own, history = _folders(tmp_path)
    first = os.path.join(history, 'w-1-2025-11-01__12_00_00.db')
    write_recorder_file(first, [event(1, T0 + 1, offset=1)], states=[T0 + 1])
    write_meta_file(first, worker='w-1', instance=INSTANCE, first=T0 + 1, last=T0 + 1)
    second = os.path.join(history, 'w-2-2025-11-01__12_10_00.db')
    write_recorder_file(second, [event(1, T0 + 2, offset=2), event(2, T0 + 3, offset=3)], worker='w-2')
    write_meta_file(second, worker='w-2', instance='bbbb', first=T0 + 2, last=T0 + 3)

    request = _request({'scope': 'cluster'})
    built = await _build(request, _worker(tmp_path))
    path = _save(built, tmp_path)

    assert built.name == 'search-cluster-slice-2025-11-01_12-26-40__2025-11-01_12-41-40.db'
    assert slice_rows(path, 'SELECT id, worker_name, source_file FROM workers ORDER BY id') == [
        (1, 'w-1', os.path.basename(first)),
        (2, 'w-2', os.path.basename(second)),
    ]
    assert slice_rows(path, 'SELECT id, worker_id, "offset" FROM events ORDER BY id') == [
        (1, 1, 1),
        (2, 2, 2),
        (3, 2, 3),
    ]
    assert slice_rows(path, 'SELECT worker_id, updated_at FROM worker_states') == [(1, T0 + 1)]
    assert _slice_row(path)['scope'] == 'cluster'
    assert [row[2] for row in _sources(path)] == ['w-1', 'w-2']


# --- Postgres ---


def _settings() -> RecorderPostgresSettings:
    return RecorderPostgresSettings(
        connection_name='main',
        connection=PostgresConnectionConfig(dsn=DSN, schema='drakkar'),
        table_prefix='fast',
        retention_hours=6,
        store_completed_output=False,
        max_output=0,
        max_size=0,
        batch_rows=5000,
        queue_max_bytes=128 * 1024 * 1024,
        resilience=ResilienceConfig(),
    )


def _pg_worker(tmp_path, **values: Any) -> SliceWorker:
    return _worker(tmp_path, **{'provider': 'postgres', 'storage_dir': '', 'postgres': _settings(), **values})


async def test_postgres_and_memory_read_each_row_once(tmp_path):
    # Row 4 of this process is in the table and in the store: it is written once.
    fake = SliceFakePostgres(
        [pg_row(1, T0 + 1, offset=1, instance='old'), pg_row(3, T0 + 2, offset=2), pg_row(4, T0 + 4, offset=4)]
    )
    store = await _memory_store([event(4, T0 + 4, offset=4), event(5, T0 + 5, offset=5)], config=True)
    try:
        worker = _pg_worker(tmp_path, open_memory=store.open_by_name)
        built = await _build(_request(sources=POSTGRES_SOURCES), worker, connect=fake.connect)
    finally:
        await store.close()
    path = _save(built, tmp_path)

    (cursor,) = fake.cursor_calls()
    statement = render_recorder(RECORDER_STATEMENT_TEMPLATES['slice'], schema='drakkar', table='fast_recorder_events')
    assert cursor[1] == statement
    # The table leaves out the rows of this process newer than the store's cut (id 5), and the limit
    # leaves room for the two rows the store gave.
    assert cursor[2] == (CLUSTER, T0, T0 + 900, WORKER, None, None, None, None, None, None, None, INSTANCE, 6, 200001)
    assert [row[1] for row in _offsets(path)] == [4, 5, 1, 2]
    assert _sources(path) == [('memory', None, WORKER, 2), ('postgres', None, WORKER, 2)]
    executes = [call for call in fake.calls if call[0] == 'execute']
    assert [call[2][0] for call in executes] == ['statement_timeout', 'jit']
    assert 1 <= int(executes[0][2][1]) <= 60000
    assert fake.calls[0] == ('connect', DSN)
    assert ('commit',) in fake.calls and ('close',) in fake.calls


async def test_a_memory_read_unreadable_after_its_first_batch_still_leaves_room_in_the_table_limit(
    tmp_path, monkeypatch
):
    # The store gives rows 1 and 2, then cannot be read; the table holds rows 1 to 4 of this process.
    monkeypatch.setattr(slice_build, 'SLICE_BATCH_ROWS', 2)
    fake = SliceFakePostgres([pg_row(i, T0 + i, offset=i, instance=INSTANCE) for i in range(1, 5)])
    store = await _memory_store([event(i, T0 + i, offset=i) for i in range(1, 5)])
    _stop_after_the_first_batch(monkeypatch, how='unreadable')
    try:
        worker = _pg_worker(tmp_path, open_memory=store.open_by_name)
        built = await _build(_request({'max_rows': '3'}, POSTGRES_SOURCES), worker, connect=fake.connect)
    finally:
        await store.close()
    path = _save(built, tmp_path)

    # One row left in the budget, plus the two memory rows the table read drops by key, plus one.
    assert fake.cursor_calls()[0][2][-1] == 4
    assert [row[1] for row in _offsets(path)] == [1, 2, 3]
    assert built.truncated == 'rows'
    assert _sources(path) == [('memory', None, WORKER, 2), ('postgres', None, WORKER, 1)]
    assert json.loads(_slice_row(path)['notes']) == ['source_unreadable']


async def test_postgres_alone_leaves_out_nothing(tmp_path):
    fake = SliceFakePostgres([pg_row(1, T0 + 1, offset=1)])
    request = _request(
        {'sources': 'postgres', 'events': 'consumed', 'label_key': 'k', 'label_value': 'v'}, POSTGRES_SOURCES
    )

    built = await _build(request, _pg_worker(tmp_path), connect=fake.connect)
    built.handle.close()

    args = fake.cursor_calls()[0][2]
    assert args[8] == ['consumed'] and args[9:11] == ('k', 'v')
    assert args[11:13] == (None, None)


async def test_postgres_rows_cut_at_max_rows(tmp_path):
    fake = SliceFakePostgres([pg_row(i, T0 + i, offset=i) for i in range(1, 6)])

    built = await _build(
        _request({'sources': 'postgres', 'max_rows': '3'}, POSTGRES_SOURCES), _pg_worker(tmp_path), connect=fake.connect
    )
    path = _save(built, tmp_path)

    assert fake.cursor_calls()[0][2][-1] == 4
    assert [row[1] for row in _offsets(path)] == [1, 2, 3]
    assert built.truncated == 'rows'


async def test_postgres_rows_are_fetched_in_batches_and_output_can_be_nulled(tmp_path, monkeypatch):
    monkeypatch.setattr(slice_build, 'SLICE_BATCH_ROWS', 2)
    monkeypatch.setattr(slice_build, 'SLICE_PG_FIRST_FETCH_ROWS', 2)
    fake = SliceFakePostgres([pg_row(i, T0 + i, offset=i, stdout='o', stderr='e') for i in range(1, 6)])

    built = await _build(
        _request({'sources': 'postgres', 'output': '0'}, POSTGRES_SOURCES), _pg_worker(tmp_path), connect=fake.connect
    )
    path = _save(built, tmp_path)

    assert [call for call in fake.calls if call[0] == 'fetch'] == [('fetch', 2)] * 3
    assert slice_rows(path, 'SELECT COUNT(*), COUNT(stdout), COUNT(stderr) FROM events') == [(5, 0, 0)]


async def test_postgres_cluster_slice_writes_one_workers_row_per_worker(tmp_path):
    fake = SliceFakePostgres(
        [pg_row(1, T0 + 1, worker='w-2'), pg_row(1, T0 + 2, worker='w-1'), pg_row(2, T0 + 3, worker='w-2')]
    )

    built = await _build(_request({'scope': 'cluster'}, POSTGRES_SOURCES), _pg_worker(tmp_path), connect=fake.connect)
    path = _save(built, tmp_path)

    assert fake.cursor_calls()[0][2][3] is None
    assert slice_rows(path, 'SELECT id, worker_name, cluster_name, source_file FROM workers ORDER BY id') == [
        (1, 'w-2', CLUSTER, 'postgres'),
        (2, 'w-1', CLUSTER, 'postgres'),
    ]
    assert slice_rows(path, 'SELECT worker_id FROM events ORDER BY id') == [(1,), (2,), (1,)]
    assert _sources(path) == [('postgres', None, 'w-1', 1), ('postgres', None, 'w-2', 2)]


async def test_an_empty_postgres_read_lists_its_source(tmp_path):
    fake = SliceFakePostgres([])

    built = await _build(_request({'scope': 'cluster'}, POSTGRES_SOURCES), _pg_worker(tmp_path), connect=fake.connect)

    assert _sources(_save(built, tmp_path)) == [('postgres', None, None, 0)]


@pytest.mark.parametrize(
    'error', [TimeoutError(), asyncpg.QueryCanceledError('canceling statement due to statement timeout')]
)
async def test_a_postgres_read_cut_by_time_keeps_its_rows_and_marks_time(tmp_path, monkeypatch, error):
    monkeypatch.setattr(slice_build, 'SLICE_BATCH_ROWS', 1)
    monkeypatch.setattr(slice_build, 'SLICE_PG_FIRST_FETCH_ROWS', 1)
    fake = SliceFakePostgres([pg_row(i, T0 + i, offset=i) for i in range(1, 4)])

    calls = {'n': 0}

    async def fake_connect(*, dsn):
        conn = await fake.connect(dsn=dsn)
        original = conn.cursor

        async def cursor(sql, *args):
            cur = await original(sql, *args)
            inner = cur.fetch

            async def fetch(count):
                calls['n'] += 1
                if calls['n'] == 2:
                    raise error
                return await inner(count)

            cur.fetch = fetch
            return cur

        conn.cursor = cursor
        return conn

    logged: list[tuple[str, str]] = []

    async def on_failed(reason: str, text: str) -> None:
        logged.append((reason, text))

    built = await _build(
        _request({'sources': 'postgres'}, POSTGRES_SOURCES),
        _pg_worker(tmp_path),
        connect=fake_connect,
        on_postgres_failed=on_failed,
    )
    path = _save(built, tmp_path)

    assert [row[1] for row in _offsets(path)] == [1]
    assert built.truncated == 'time'
    assert [reason for reason, _ in logged] == ['timeout']
    assert fake.connections[0].terminated


async def test_a_failed_postgres_read_raises_and_removes_the_temp_file(tmp_path):
    fake = SliceFakePostgres([pg_row(1, T0 + 1)])
    fake.error = OSError('connection reset')
    own, _history = _folders(tmp_path)

    with pytest.raises(PostgresHistoryError) as raised:
        await _build(
            _request({'sources': 'postgres'}, POSTGRES_SOURCES),
            _pg_worker(tmp_path, storage_dir=own),
            connect=fake.connect,
        )

    assert raised.value.reason == 'error'
    assert temp_files(own) == []


async def test_a_postgres_read_finds_no_slot_while_two_reads_run(tmp_path):
    fake = SliceFakePostgres([pg_row(1, T0 + 1)])
    with postgres_read_slot(), postgres_read_slot(), pytest.raises(PostgresHistoryError) as raised:
        await _build(_request({'sources': 'postgres'}, POSTGRES_SOURCES), _pg_worker(tmp_path), connect=fake.connect)

    assert raised.value.reason == 'busy'
    assert fake.calls == []


async def test_the_history_search_and_the_slice_share_the_read_slots(tmp_path):
    fake = SliceFakePostgres([pg_row(1, T0 + 1)])
    fake.hang = True
    first = asyncio.ensure_future(
        _build(_request({'sources': 'postgres'}, POSTGRES_SOURCES), _pg_worker(tmp_path), connect=fake.connect)
    )
    second = asyncio.ensure_future(
        _build(_request({'sources': 'postgres'}, POSTGRES_SOURCES), _pg_worker(tmp_path), connect=fake.connect)
    )
    try:
        while len([c for c in fake.calls if c[0] == 'fetch']) < 2:
            await asyncio.sleep(0.01)
        with pytest.raises(PostgresHistoryError) as raised:
            await postgres_history.search_postgres_history(
                _settings(), CLUSTER, 3, 42, max_seconds=5.0, on_close_failed=_unused, connect=fake.connect
            )
        assert raised.value.reason == 'busy'
    finally:
        first.cancel()
        second.cancel()
        await asyncio.gather(first, second, return_exceptions=True)


async def _unused(_error: str) -> None:
    raise AssertionError('unused')


async def test_a_slice_close_failure_after_a_complete_read_is_logged_and_kept(tmp_path):
    fake = SliceFakePostgres([pg_row(1, T0 + 1)])
    fake.close_error = OSError('close failed')
    logged: list[str] = []

    async def on_failed(reason: str, _text: str) -> None:
        logged.append(reason)

    built = await _build(
        _request({'sources': 'postgres'}, POSTGRES_SOURCES),
        _pg_worker(tmp_path),
        connect=fake.connect,
        on_postgres_failed=on_failed,
    )

    assert built.rows == 1
    assert logged == ['close']
    built.handle.close()


async def test_a_write_failure_during_the_postgres_read_is_a_write_failure(tmp_path, monkeypatch):
    fake = SliceFakePostgres([pg_row(1, T0 + 1)])

    def broken(self, rows, worker_id):
        raise sqlite3.OperationalError('database or disk is full')

    monkeypatch.setattr(slice_build.SliceWriter, 'insert_events', broken)
    with pytest.raises(sqlite3.OperationalError):
        await _build(_request({'sources': 'postgres'}, POSTGRES_SOURCES), _pg_worker(tmp_path), connect=fake.connect)


def test_time_left_ms_is_whole_milliseconds_at_least_one():
    assert postgres_history.time_left_ms(10.0, 7.5004) == '2499'
    assert postgres_history.time_left_ms(10.0, 11.0) == '1'


# --- Edges of the sources ---


async def test_a_file_without_config_or_state_tables_still_gives_its_events(tmp_path):
    own, _history = _folders(tmp_path)
    path = _own_file(own, '2025-11-01__12_10_00')
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA_EVENTS)
    conn.executemany(INSERT_EVENT_SQL, [event(1, T0 + 1, offset=1)])
    conn.commit()
    conn.close()

    built = await _build(_request({'sources': 'files'}), _worker(tmp_path))
    saved = _save(built, tmp_path)

    assert _offsets(saved) == [(1, 1)]
    assert slice_rows(saved, 'SELECT COUNT(*) FROM worker_config') == [(0,)]


async def test_a_file_without_an_events_table_is_unreadable(tmp_path):
    own, _history = _folders(tmp_path)
    path = _own_file(own, '2025-11-01__12_10_00')
    sqlite3.connect(path).execute('CREATE TABLE other (x)').connection.close()

    built = await _build(_request({'sources': 'files'}), _worker(tmp_path))

    assert json.loads(_slice_row(_save(built, tmp_path))['notes']) == ['source_unreadable']


async def test_sealed_files_with_a_bad_meta_time_or_a_broken_body_are_skipped(tmp_path):
    _own, history = _folders(tmp_path)
    bad_meta = _own_file(history, '2025-11-01__11_00_00')
    write_recorder_file(bad_meta, [event(1, T0 + 1, offset=1)])
    with open(bad_meta + '.meta.json', 'w', encoding='utf-8') as fh:
        json.dump({'schema': 1, 'worker': WORKER, 'first_event_ts': 'yesterday', 'last_event_ts': 3}, fh)
    broken = _own_file(history, '2025-11-01__12_00_00')
    Path(broken).write_bytes(b'not a database' * 100)

    built = await _build(_request({'sources': 'history'}), _worker(tmp_path))

    assert _sources(_save(built, tmp_path)) == []


async def test_history_without_a_shared_folder_reads_no_sealed_file(tmp_path):
    built = await _build(_request({'sources': 'history'}), _worker(tmp_path, history_dir=''))

    assert built.rows == 0
    built.handle.close()


async def test_state_rows_are_copied_in_batches(tmp_path, monkeypatch):
    monkeypatch.setattr(slice_build, 'SLICE_BATCH_ROWS', 2)
    own, _history = _folders(tmp_path)
    live = _own_file(own, '2025-11-01__12_10_00')
    write_recorder_file(live, [event(1, T0 + 1)], states=[T0 + 1, T0 + 2, T0 + 3, T0 + 4, T0 + 5])

    built = await _build(_request({'sources': 'files'}), _worker(tmp_path, live=live))

    assert slice_rows(_save(built, tmp_path), 'SELECT COUNT(*) FROM worker_state') == [(5,)]


async def test_a_temp_file_that_cannot_be_secured_falls_back_to_ram_and_is_removed(tmp_path, monkeypatch):
    own, _history = _folders(tmp_path)

    def refuse(_path):
        raise PermissionError('not allowed')

    monkeypatch.setattr(slice_build, 'secure_db_file', refuse)
    with capture_logs() as logs:
        built = await _build(_request({'sources': 'files'}), _worker(tmp_path))
    built.handle.close()

    assert [entry['reason'] for entry in logs if entry['event'] == 'debug_slice_in_ram'] == ['storage_dir_unusable']
    assert temp_files(own) == []


async def test_a_worker_without_a_memory_opener_reads_no_memory(tmp_path):
    built = await _build(_request(sources=OFF_SOURCES), _worker(tmp_path, provider='none', open_memory=None))

    assert _sources(_save(built, tmp_path)) == []


async def test_a_postgres_read_stops_between_batches_at_the_time_limit(tmp_path, monkeypatch):
    monkeypatch.setattr(slice_build, 'SLICE_BATCH_ROWS', 1)
    monkeypatch.setattr(slice_build, 'SLICE_PG_FIRST_FETCH_ROWS', 1)
    fake = SliceFakePostgres([pg_row(i, T0 + i, offset=i) for i in range(1, 30)])

    built = await _build(
        _request({'sources': 'postgres'}, POSTGRES_SOURCES),
        _pg_worker(tmp_path),
        connect=fake.connect,
        clock=SteppingClock(10.0),
    )
    built.handle.close()

    assert built.truncated == 'time'
    assert 0 < built.rows < 29


async def test_a_postgres_timeout_without_a_log_hook_still_marks_time(tmp_path):
    fake = SliceFakePostgres([pg_row(1, T0 + 1)])
    fake.error = TimeoutError()

    built = await _build(
        _request({'sources': 'postgres'}, POSTGRES_SOURCES), _pg_worker(tmp_path), connect=fake.connect
    )
    built.handle.close()

    assert built.truncated == 'time'


async def test_a_postgres_plan_without_settings_reads_no_table(tmp_path):
    request = _request({'sources': 'postgres'}, POSTGRES_SOURCES)

    built = await build_slice(
        request, _worker(tmp_path, provider='postgres', storage_dir=''), plan_slice(request, 'postgres')
    )
    built.handle.close()

    assert built.rows == 0


# --- Fix round 1: order, keys, byte bounds, thread safety ---


async def _delete_memory_rows(store: MemoryStore, ids: list[int]) -> None:
    """Evict rows from the store, as its eviction pass does."""
    await store.writer.execute(f'DELETE FROM events WHERE id IN ({", ".join("?" * len(ids))})', ids)
    await store.writer.commit()


async def test_rows_evicted_from_the_store_during_the_table_read_are_neither_lost_nor_doubled(tmp_path):
    # After the flush of the write stage the table holds rows 1 to 7 of this process; the store holds 4 to 7.
    table = [pg_row(i, T0 + i, offset=i, instance=INSTANCE) for i in range(1, 8)]
    fake = SliceFakePostgres(table)
    store = await _memory_store([event(i, T0 + i, offset=i) for i in range(4, 8)])
    real_connect = fake.connect

    async def connect(*, dsn):
        conn = await real_connect(dsn=dsn)
        original = conn.cursor

        async def cursor(sql, *args):
            # The store evicts rows 4 and 5 while the table is read.
            await _delete_memory_rows(store, [4, 5])
            return await original(sql, *args)

        conn.cursor = cursor
        return conn

    try:
        worker = _pg_worker(tmp_path, open_memory=store.open_by_name)
        built = await _build(_request(sources=POSTGRES_SOURCES), worker, connect=connect)
    finally:
        await store.close()
    offsets = [row[1] for row in _offsets(_save(built, tmp_path))]

    assert sorted(offsets) == [1, 2, 3, 4, 5, 6, 7]


async def test_rows_evicted_from_the_store_during_its_own_read_come_from_the_table(tmp_path, monkeypatch):
    monkeypatch.setattr(slice_build, 'SLICE_BATCH_ROWS', 2)
    table = [pg_row(i, T0 + i, offset=i, instance=INSTANCE) for i in range(1, 8)]
    fake = SliceFakePostgres(table)
    store = await _memory_store([event(i, T0 + i, offset=i) for i in range(1, 8)])
    real_fetch = slice_build._fetch_chunk
    calls: list[int] = []

    def evicting_fetch(conn, sql, params, max_bytes):
        rows = real_fetch(conn, sql, params, max_bytes)
        calls.append(1)
        if len(calls) == 1:
            # Between the first and the second memory batch the store evicts rows 3 and 4.
            writer = sqlite3.connect(store.uri, uri=True)
            writer.execute('DELETE FROM events WHERE id IN (3, 4)')
            writer.commit()
            writer.close()
        return rows

    monkeypatch.setattr(slice_build, '_fetch_chunk', evicting_fetch)
    try:
        worker = _pg_worker(tmp_path, open_memory=store.open_by_name)
        built = await _build(_request(sources=POSTGRES_SOURCES), worker, connect=fake.connect)
    finally:
        await store.close()
    offsets = [row[1] for row in _offsets(_save(built, tmp_path))]

    assert sorted(offsets) == [1, 2, 3, 4, 5, 6, 7]


async def test_a_leftover_file_sealed_by_this_process_keeps_rows_that_collide_by_id_only(tmp_path):
    """A start seals the file an earlier process left: the meta names this instance, the rows are the earlier process's."""
    _own, history = _folders(tmp_path)
    leftover = _own_file(history, '2025-11-01__11_00_00')
    # The earlier process numbered its rows from 1 too, at other times.
    write_recorder_file(leftover, [event(i, T0 + i + 0.5, offset=100 + i) for i in range(1, 4)])
    write_meta_file(leftover, worker=WORKER, instance=INSTANCE, first=T0 + 1.5, last=T0 + 3.5)
    store = await _memory_store([event(i, T0 + i, offset=i) for i in range(1, 4)])
    try:
        built = await _build(_request({'sources': 'memory,history'}), _worker(tmp_path, open_memory=store.open_by_name))
    finally:
        await store.close()

    assert [row[1] for row in _offsets(_save(built, tmp_path))] == [1, 2, 3, 101, 102, 103]


async def test_an_own_sealed_file_without_meta_leaves_out_the_store_rows_by_id_and_time(tmp_path):
    _own, history = _folders(tmp_path)
    # Rows 4 and 5 are the store's rows (same id and time); another process's row 6 has another time.
    write_recorder_file(
        _own_file(history, '2025-11-01__12_00_00'),
        [
            event(3, T0 + 3, offset=3),
            event(4, T0 + 4, offset=4),
            event(5, T0 + 5, offset=5),
            event(6, T0 + 0.5, offset=60),
        ],
    )
    store = await _memory_store([event(i, T0 + i, offset=i) for i in range(4, 8)])
    try:
        built = await _build(_request({'sources': 'memory,history'}), _worker(tmp_path, open_memory=store.open_by_name))
    finally:
        await store.close()

    assert [row[1] for row in _offsets(_save(built, tmp_path))] == [4, 5, 6, 7, 3, 60]


async def test_a_batch_ends_at_its_byte_bound_and_the_read_goes_on(tmp_path, monkeypatch):
    monkeypatch.setattr(slice_build, 'SLICE_CHUNK_BYTES', 1000)
    own, _history = _folders(tmp_path)
    write_recorder_file(
        _own_file(own, '2025-11-01__12_10_00'), [event(i, T0 + i, offset=i, stdout='o' * 900) for i in range(1, 6)]
    )
    sizes: list[int] = []
    real_fetch = slice_build._fetch_chunk

    def counting_fetch(conn, sql, params, max_bytes):
        rows, cut = real_fetch(conn, sql, params, max_bytes)
        sizes.append(len(rows))
        return rows, cut

    monkeypatch.setattr(slice_build, '_fetch_chunk', counting_fetch)
    built = await _build(_request({'sources': 'files'}), _worker(tmp_path))

    assert [row[1] for row in _offsets(_save(built, tmp_path))] == [1, 2, 3, 4, 5]
    # Two rows of about 950 bytes pass the bound of 1000: each batch holds at most two.
    assert max(sizes) <= 2
    assert built.truncated is None


async def test_a_ram_build_stops_within_one_bounded_batch_of_its_cap(tmp_path, monkeypatch):
    monkeypatch.setattr(slice_build, 'SLICE_CHUNK_BYTES', 5000)
    monkeypatch.setattr(slice_build, 'SLICE_MAX_RAM_BYTES', 1)
    store = await _memory_store([event(i, T0 + i, offset=i, stdout='o' * 1000) for i in range(1, 200)])
    try:
        built = await _build(
            _request(sources=OFF_SOURCES),
            _worker(tmp_path, provider='none', storage_dir='', open_memory=store.open_by_name),
        )
    finally:
        await store.close()
    built.handle.close()

    assert built.truncated == 'bytes'
    # One batch of at most 5000 bytes, so at most 5 rows of about 1 KiB.
    assert built.rows <= 5


async def test_postgres_batches_are_written_in_byte_bounded_parts_and_the_next_fetch_shrinks(tmp_path, monkeypatch):
    monkeypatch.setattr(slice_build, 'SLICE_CHUNK_BYTES', 2000)
    monkeypatch.setattr(slice_build, 'SLICE_PG_FIRST_FETCH_ROWS', 4)
    fake = SliceFakePostgres([pg_row(i, T0 + i, offset=i, stdout='o' * 1000) for i in range(1, 9)])
    parts: list[int] = []
    real_insert = slice_build.SliceWriter.insert_events

    def counting_insert(self, rows, worker_id):
        parts.append(len(rows))
        real_insert(self, rows, worker_id)

    monkeypatch.setattr(slice_build.SliceWriter, 'insert_events', counting_insert)
    built = await _build(
        _request({'sources': 'postgres'}, POSTGRES_SOURCES), _pg_worker(tmp_path), connect=fake.connect
    )
    built.handle.close()

    assert built.rows == 8
    assert max(parts) <= 2
    fetches = [call[1] for call in fake.calls if call[0] == 'fetch']
    assert fetches[0] == 4 and all(count <= 2 for count in fetches[1:])


async def test_a_batch_being_written_at_the_deadline_is_finished_and_counted(tmp_path, monkeypatch):
    monkeypatch.setattr(slice_build, 'SLICE_TIME_LIMIT_SECONDS', 0.2)
    monkeypatch.setattr(slice_build, 'SLICE_PG_FIRST_FETCH_ROWS', 2)
    monkeypatch.setattr(slice_build, 'SLICE_BATCH_ROWS', 2)
    fake = SliceFakePostgres([pg_row(i, T0 + i, offset=i) for i in range(1, 7)])
    real_insert = slice_build.SliceWriter.insert_events
    finished: list[int] = []

    def slow_insert(self, rows, worker_id):
        time.sleep(0.4)  # past the build's deadline
        real_insert(self, rows, worker_id)
        finished.append(len(rows))

    monkeypatch.setattr(slice_build.SliceWriter, 'insert_events', slow_insert)
    built = await _build(
        _request({'sources': 'postgres'}, POSTGRES_SOURCES), _pg_worker(tmp_path), connect=fake.connect
    )
    path = _save(built, tmp_path)

    assert built.truncated == 'time'
    # The batch in flight was written to its end, and the file, the count and the header agree.
    assert finished == [2]
    assert built.rows == 2
    assert slice_rows(path, 'SELECT COUNT(*) FROM events') == [(2,)]
    assert _slice_row(path)['rows'] == 2


async def test_a_build_cancelled_during_a_thread_waits_for_it_and_removes_the_temp_file(tmp_path, monkeypatch):
    own, _history = _folders(tmp_path)
    write_recorder_file(_own_file(own, '2025-11-01__12_10_00'), [event(1, T0 + 1, offset=1)])
    entered = threading.Event()
    gate = threading.Event()
    errors: list[BaseException] = []
    real_insert = slice_build.SliceWriter.insert_events

    def blocked_insert(self, rows, worker_id):
        entered.set()
        gate.wait(5)
        try:
            real_insert(self, rows, worker_id)
        except BaseException as exc:
            errors.append(exc)
            raise

    monkeypatch.setattr(slice_build.SliceWriter, 'insert_events', blocked_insert)
    task = asyncio.ensure_future(_build(_request({'sources': 'files'}), _worker(tmp_path)))
    await asyncio.to_thread(entered.wait, 5)
    task.cancel()
    asyncio.get_running_loop().call_later(0.1, gate.set)
    with pytest.raises(asyncio.CancelledError):
        await task

    # The insert ran on an open connection; only then was the target closed and removed.
    assert errors == []
    assert temp_files(own) == []


async def test_a_build_cancelled_while_its_target_opens_removes_the_temp_file(tmp_path, monkeypatch):
    own, _history = _folders(tmp_path)
    entered = threading.Event()
    gate = threading.Event()
    real_open = slice_build.open_target

    def slow_open(storage_dir, worker):
        target = real_open(storage_dir, worker)
        entered.set()
        gate.wait(5)
        return target

    monkeypatch.setattr(slice_build, 'open_target', slow_open)
    task = asyncio.ensure_future(_build(_request({'sources': 'files'}), _worker(tmp_path)))
    await asyncio.to_thread(entered.wait, 5)
    task.cancel()
    asyncio.get_running_loop().call_later(0.1, gate.set)
    with pytest.raises(asyncio.CancelledError):
        await task

    assert temp_files(own) == []


async def test_a_postgres_plan_asks_for_the_flush_of_the_write_stage(tmp_path):
    fake = SliceFakePostgres([])
    asked: list[str] = []

    async def flush_postgres() -> str:
        asked.append('postgres')
        return slices.NOTE_POSTGRES_QUEUE_WAITING

    async def other() -> None:
        raise AssertionError('another flush was asked for')

    built = await _build(
        _request(sources=POSTGRES_SOURCES),
        _pg_worker(tmp_path),
        connect=fake.connect,
        flush=other,
        flush_memory=other,
        flush_postgres=flush_postgres,
    )

    assert asked == ['postgres']
    assert json.loads(_slice_row(_save(built, tmp_path))['notes']) == ['postgres_queue_waiting']
