"""``GET /api/v1/debug/databases`` (contract v1.30): ``parts``, and the ``worker``, ``recorder`` and ``storage`` blocks.

Hermetic: real recorders that are never started (or a stub where the test
needs a memory store or a writer), folders and SQLite files written by the
test, a mocked app with a real config.
"""

from __future__ import annotations

import asyncio
import contextlib
import gzip
import io
import json
import os
import re
import sqlite3
import time
from collections.abc import Iterator
from types import SimpleNamespace
from unittest.mock import MagicMock

import asyncpg
import pytest
from httpx import ASGITransport, AsyncClient
from structlog.testing import capture_logs

from drakkar import slice_build
from drakkar.cache.sql import SCHEMA_CACHE_ENTRIES
from drakkar.config import DrakkarConfig, PostgresConnectionConfig, RecorderConfig, ResilienceConfig, UIConfig
from drakkar.fleet.service import FleetRegistry
from drakkar.ownerlock import take_own_lock
from drakkar.providers.postgres import RECORDER_STATEMENT_TEMPLATES, partition_name
from drakkar.recorder import EventRecorder
from drakkar.recorder.postgres import RecorderPostgresSettings
from drakkar.recorder.postgres_history import postgres_read_slot, statement_timeout_ms
from drakkar.recorder.postgres_writer import PostgresStageStatus, UpkeepStatus, WriterStatus
from drakkar.slice_build import BuiltSlice, build_slice
from drakkar.slices import SliceSources, parse_slice_request, plan_slice
from drakkar.uiserver import databases_status, routes_databases, routes_debug
from drakkar.uiserver.databases_status import count_cache_entries, parse_parts, recorder_block, storage_block
from drakkar.uiserver.server import create_ui_app
from tests.conftest import kafka_connections
from tests.slice_fakes import SliceFakePostgres, pg_row

CLUSTER = 'search'
WORKER = 'w-1'
DSN = 'postgresql://drakkar:secret-password@db:5432/drakkar'
NOW = 1_760_000_000.0


def _config(*, storage_dir: str = '', shared_dir: str = '', autodiscover: bool = False, **recorder) -> DrakkarConfig:
    values: dict = {
        'connections': {**kafka_connections(), 'postgres': {'main': {'dsn': DSN, 'schema': 'drakkar'}}},
        'sources': {'kafka': {'enabled': True}},
        'storage': {'dir': storage_dir, 'shared_dir': shared_dir},
        'ui': {'enabled': True, 'release': {'enabled': False}},
        'recorder': {'enabled': bool(storage_dir) or recorder.get('provider') == 'postgres', **recorder},
    }
    if autodiscover:
        values['autodiscover'] = {'enabled': True, 'provider': 'filesystem'}
    return DrakkarConfig(**values)


def _postgres_settings() -> RecorderPostgresSettings:
    return RecorderPostgresSettings(
        connection_name='main',
        connection=PostgresConnectionConfig(dsn=DSN, schema='drakkar'),
        table_prefix='fast',
        retention_hours=6,
        store_completed_output=False,
        max_output=0,
        max_size=64 * 1024 * 1024,
        batch_rows=5000,
        queue_max_bytes=128 * 1024 * 1024,
        resilience=ResilienceConfig(),
    )


def _recorder(*, persistence: str = 'none', storage_dir: str = '', shared_dir: str = '', cluster: str = CLUSTER):
    return EventRecorder(
        RecorderConfig(),
        worker_name=WORKER,
        cluster_name=cluster,
        persistence=persistence,
        storage_dir=storage_dir,
        shared_dir=shared_dir,
        instance='0123456789abcdef',
        postgres=_postgres_settings() if persistence == 'postgres' else None,
    )


@contextlib.asynccontextmanager
async def _client(recorder: EventRecorder, config: DrakkarConfig, *, fleet=None):
    drakkar_app = MagicMock()
    drakkar_app._config = config
    drakkar_app.fleet = fleet
    app = create_ui_app(UIConfig(enabled=True), recorder, drakkar_app)
    async with AsyncClient(transport=ASGITransport(app=app), base_url='http://test') as client:
        yield client


# --- parts ---


async def test_databases_without_parts_answers_the_worker_and_every_part(tmp_path):
    async with _client(_recorder(), _config()) as client:
        resp = await client.get('/api/v1/debug/databases')

    assert resp.status_code == 200
    assert set(resp.json()) == {'worker', 'own', 'history', 'recorder', 'storage'}


async def test_databases_parts_answers_only_those_parts_and_never_lists_the_history(tmp_path, monkeypatch):
    def refuse(_shared):
        raise AssertionError('the history folder was listed')

    monkeypatch.setattr(routes_debug, '_history_answer', refuse)
    recorder = _recorder(shared_dir=str(tmp_path / 'shared'))

    async with _client(recorder, _config()) as client:
        resp = await client.get('/api/v1/debug/databases', params={'parts': 'own,storage,recorder'})

    assert resp.status_code == 200
    assert set(resp.json()) == {'worker', 'own', 'storage', 'recorder'}


@pytest.mark.parametrize(
    ('parts', 'unknown'),
    [('disk', 'disk'), ('', ''), ('own,', ''), ('own,files,disk', 'files')],
    ids=['unknown', 'empty', 'trailing_comma', 'first_unknown_named'],
)
async def test_databases_unknown_part_answers_400(parts, unknown):
    async with _client(_recorder(), _config()) as client:
        resp = await client.get('/api/v1/debug/databases', params={'parts': parts})

    assert resp.status_code == 400
    assert resp.json() == {'error': f"unknown part '{unknown}'; known: history, own, recorder, storage"}


def test_parse_parts_accepts_repeats():
    assert parse_parts('own,own,storage') == frozenset({'own', 'storage'})
    assert parse_parts(None) == frozenset({'own', 'history', 'storage', 'recorder'})


# --- worker ---


@pytest.mark.parametrize(('cluster', 'expected'), [(CLUSTER, CLUSTER), ('', None)])
async def test_worker_block_names_the_worker_its_cluster_and_instance(cluster, expected):
    async with _client(_recorder(cluster=cluster), _config()) as client:
        resp = await client.get('/api/v1/debug/databases', params={'parts': 'own'})

    assert resp.json()['worker'] == {'name': WORKER, 'cluster': expected, 'instance': '0123456789abcdef'}


# --- recorder ---


async def test_recorder_block_of_a_sqlite_worker_with_a_cluster_archive(tmp_path, monkeypatch):
    monkeypatch.setattr(databases_status, 'archive_next_slot', lambda config, now: 1_760_003_600)
    storage = tmp_path / 'storage'
    config = _config(
        storage_dir=str(storage),
        shared_dir=str(tmp_path / 'shared'),
        autodiscover=True,
        sqlite={'rotation_interval': '2h'},
        archive={'window': '24h', 'due_after': '48h', 'retention': '30d', 'merged_retention': '7d'},
    )
    recorder = _recorder(persistence='sqlite', storage_dir=str(storage / CLUSTER / WORKER))

    async with _client(recorder, config) as client:
        resp = await client.get('/api/v1/debug/databases', params={'parts': 'recorder'})

    assert resp.json()['recorder'] == {
        'enabled': True,
        'provider': 'sqlite',
        'memory': None,
        'sqlite': {
            'rotation_interval_seconds': 7200,
            'archive': {
                'enabled': True,
                'scope': 'cluster',
                'next_slot_ts': 1_760_003_600,
                'window_seconds': 86400,
                'due_after_seconds': 172800,
                'retention_days': 30,
                'merged_retention_days': 7,
            },
        },
        'postgres': None,
    }


def test_recorder_block_without_autodiscover_archives_the_own_files_at_the_next_slot(tmp_path):
    config = _config(storage_dir=str(tmp_path), archive={'interval': '30m'})
    recorder = _recorder(persistence='sqlite', storage_dir=str(tmp_path / CLUSTER / WORKER))

    archive = recorder_block(config, recorder, now=7200.0)['sqlite']['archive']

    assert (archive['scope'], archive['next_slot_ts']) == ('worker', 9000)


def test_recorder_block_with_archiving_off_has_no_scope_and_no_slot(tmp_path):
    config = _config(storage_dir=str(tmp_path), archive={'enabled': False})
    recorder = _recorder(persistence='sqlite', storage_dir=str(tmp_path / CLUSTER / WORKER))

    archive = recorder_block(config, recorder, now=NOW)['sqlite']['archive']

    assert (archive['enabled'], archive['scope'], archive['next_slot_ts']) == (False, None, None)


async def test_recorder_block_of_a_worker_with_the_recorder_off():
    async with _client(_recorder(), _config()) as client:
        resp = await client.get('/api/v1/debug/databases', params={'parts': 'recorder'})

    assert resp.json()['recorder'] == {
        'enabled': False,
        'provider': 'none',
        'memory': None,
        'sqlite': None,
        'postgres': None,
    }


def test_recorder_block_shows_the_memory_bounds_of_an_open_memory_store():
    config = _config(memory={'window': '90s', 'rows_per_executor': 200, 'max_size': '32MiB'})
    stub = SimpleNamespace(persistence='none', memory_store=SimpleNamespace(is_open=True))

    block = recorder_block(config, stub, now=NOW)

    assert block['memory'] == {
        'window_seconds': 90,
        'max_rows': 200 * config.executor.max_executors,
        'max_bytes': 32 * 1024 * 1024,
    }


async def test_recorder_block_of_a_postgres_worker_before_start_has_the_table_and_no_writer():
    config = _config(provider='postgres')
    async with _client(_recorder(persistence='postgres'), config) as client:
        resp = await client.get('/api/v1/debug/databases', params={'parts': 'recorder'})

    assert 'secret-password' not in resp.text
    assert resp.json()['recorder']['postgres'] == {
        'connection': 'main',
        'schema': 'drakkar',
        'table': 'fast_recorder_events',
        'initial_retention_hours': 6,
        'max_size_bytes': 64 * 1024 * 1024,
        'store_completed_output': False,
        'writer': None,
    }


def test_recorder_block_of_a_postgres_worker_shows_its_writer():
    writer = WriterStatus(
        started=True,
        slot_rows=7,
        slot_bytes=100,
        rows_written=1200,
        dropped_rejected=2,
        dropped_no_partition=3,
        dropped_shutdown=4,
        breaker_open=True,
        last_upkeep=UpkeepStatus(
            at=NOW,
            held_lock=True,
            created=('fast_recorder_events_p2026101509',),
            dropped=('fast_recorder_events_p2026101401',),
            dropped_for_size=('fast_recorder_events_p2026101402',),
        ),
    )
    stub = SimpleNamespace(
        persistence='postgres',
        memory_store=None,
        postgres_settings=_postgres_settings(),
        postgres_stage_status=lambda: PostgresStageStatus(
            writer=writer, queue_rows=10, queue_bytes=2048, dropped_queue_full=5
        ),
    )

    block = recorder_block(_config(provider='postgres'), stub, now=NOW)

    assert block['postgres']['writer'] == {
        'started': True,
        'queue_rows': 17,
        'queue_bytes': 2148,
        'rows_written': 1200,
        'dropped': {'queue_full': 5, 'rejected': 2, 'no_partition': 3, 'shutdown': 4},
        'breaker_open': True,
        'last_upkeep': {
            'at': '2025-10-09T08:53:20.000000Z',
            'held_lock': True,
            'created': ['fast_recorder_events_p2026101509'],
            'dropped': ['fast_recorder_events_p2026101401'],
            'dropped_for_size': ['fast_recorder_events_p2026101402'],
        },
    }


def test_recorder_block_of_a_postgres_worker_before_its_first_upkeep():
    stub = SimpleNamespace(
        persistence='postgres',
        memory_store=None,
        postgres_settings=_postgres_settings(),
        postgres_stage_status=lambda: PostgresStageStatus(
            writer=WriterStatus(), queue_rows=0, queue_bytes=0, dropped_queue_full=0
        ),
    )

    writer = recorder_block(_config(provider='postgres'), stub, now=NOW)['postgres']['writer']

    assert (writer['started'], writer['last_upkeep']) == (False, None)


# --- storage ---


@contextlib.contextmanager
def _cache_file(path, rows: int) -> Iterator[None]:
    """A cache file in WAL mode with ``rows`` entries; the open writer keeps its ``-wal`` and ``-shm`` files."""
    conn = sqlite3.connect(str(path))
    try:
        conn.execute('PRAGMA journal_mode=WAL')
        conn.executescript(SCHEMA_CACHE_ENTRIES)
        conn.executemany(
            'INSERT INTO cache_entries VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
            [(f'k{i}', 'local', 'v', 1, 0, 0, None, WORKER) for i in range(rows)],
        )
        conn.commit()
        yield
    finally:
        conn.close()


def _write(path, size: int) -> None:
    path.write_bytes(b'x' * size)


async def test_storage_block_lists_the_own_folder_files_and_its_download_temps(tmp_path):
    storage = tmp_path / 'storage'
    own = storage / CLUSTER / WORKER
    own.mkdir(parents=True)
    lock = take_own_lock(str(own))
    _write(own / f'{WORKER}.watchdog', 5)
    _write(own / f'{WORKER}-2026-10-14__09_00_00.db', 100)
    _write(own / f'.{WORKER}-2026-10-14__09_00_00.db.0123456789abcdef.download', 30)
    _write(own / f'.{WORKER}-2026-10-14__09_00_00.db.0123456789abcdef.download-wal', 12)
    _write(own / 'notes.txt', 3)
    config = _config(storage_dir=str(storage))
    recorder = _recorder(persistence='sqlite', storage_dir=str(own))
    try:
        with _cache_file(own / f'{WORKER}-cache.db', rows=3):
            cache_parts = [own / f'{WORKER}-cache.db{suffix}' for suffix in ('', '-wal', '-shm')]
            cache_size = sum(part.stat().st_size for part in cache_parts)
            async with _client(recorder, config) as client:
                resp = await client.get('/api/v1/debug/databases', params={'parts': 'storage'})
    finally:
        lock.release()

    block = resp.json()['storage']
    assert (block['dir_set'], block['own_folder']) == (True, f'{CLUSTER}/{WORKER}')
    assert (block['shared_dir_set'], block['shared_is_storage_dir']) == (False, True)
    assert [(f['name'], f['kind'], f['entries']) for f in block['files']] == [
        ('.owner.lock', 'owner_lock', None),
        (f'{WORKER}-cache.db', 'cache', 3),
        (f'{WORKER}.watchdog', 'watchdog', None),
    ]
    assert block['files'][1]['size_bytes'] == cache_size
    assert block['files'][2]['size_bytes'] == 5
    assert block['files'][2]['modified_at'].endswith('Z')
    assert block['temp_files'] == {'count': 2, 'size_bytes': 42}
    assert block['siblings'] == []


async def test_storage_block_lists_the_sibling_folders_with_their_lock_and_files(tmp_path):
    storage = tmp_path / 'storage'
    cluster_dir = storage / CLUSTER
    own = cluster_dir / WORKER
    own.mkdir(parents=True)
    held = cluster_dir / 'w-2'
    free = cluster_dir / 'w-3'
    no_lock = cluster_dir / 'w-4'
    for folder in (held, free, no_lock):
        folder.mkdir()
    holder = take_own_lock(str(held))
    take_own_lock(str(free)).release()
    _write(held / 'w-2-2026-10-14__09_00_00.db', 10)
    _write(held / 'w-2.watchdog', 4)
    os.utime(held / 'w-2.watchdog', ns=(1_760_000_000_000_000_000, 1_760_000_000_000_000_000))
    os.utime(held / 'w-2-2026-10-14__09_00_00.db', ns=(1_700_000_000_000_000_000, 1_700_000_000_000_000_000))
    (cluster_dir / 'not a worker').mkdir()
    (cluster_dir / '.orphan-w-9-0123456789abcdef').mkdir()
    _write(cluster_dir / 'stray-file', 1)
    os.symlink(held, cluster_dir / 'w-5')
    fleet = MagicMock(spec=FleetRegistry)
    fleet.name_online.side_effect = {'w-2': True, 'w-3': False, 'w-4': None}.get
    config = _config(storage_dir=str(storage), shared_dir=str(tmp_path / 'shared'), autodiscover=True)
    recorder = _recorder(persistence='sqlite', storage_dir=str(own))
    try:
        async with _client(recorder, config, fleet=fleet) as client:
            resp = await client.get('/api/v1/debug/databases', params={'parts': 'storage'})
    finally:
        holder.release()

    block = resp.json()['storage']
    assert (block['shared_dir_set'], block['shared_is_storage_dir']) == (True, False)
    assert block['siblings'] == [
        {
            'worker': 'w-2',
            'files': 2,
            'size_bytes': 14,
            'newest_modified_at': '2025-10-09T08:53:20.000000Z',
            'in_use': True,
            'online': True,
        },
        {'worker': 'w-3', 'files': 0, 'size_bytes': 0, 'newest_modified_at': None, 'in_use': False, 'online': False},
        {'worker': 'w-4', 'files': 0, 'size_bytes': 0, 'newest_modified_at': None, 'in_use': None, 'online': None},
    ]


async def test_storage_block_without_a_registry_has_no_online_verdict(tmp_path):
    storage = tmp_path / 'storage'
    (storage / CLUSTER / WORKER).mkdir(parents=True)
    (storage / CLUSTER / 'w-2').mkdir()
    # A registry object without autodiscover on is not asked.
    fleet = MagicMock(spec=FleetRegistry)
    recorder = _recorder(persistence='sqlite', storage_dir=str(storage / CLUSTER / WORKER))

    async with _client(recorder, _config(storage_dir=str(storage)), fleet=fleet) as client:
        resp = await client.get('/api/v1/debug/databases', params={'parts': 'storage'})

    assert [sibling['online'] for sibling in resp.json()['storage']['siblings']] == [None]
    fleet.name_online.assert_not_called()


async def test_storage_block_without_storage_dir_has_no_folder():
    async with _client(_recorder(), _config()) as client:
        resp = await client.get('/api/v1/debug/databases', params={'parts': 'storage'})

    assert resp.json()['storage'] == {
        'dir_set': False,
        'own_folder': '',
        'shared_dir_set': False,
        'shared_is_storage_dir': False,
        'files': [],
        'temp_files': {'count': 0, 'size_bytes': 0},
        'siblings': None,
    }


def test_storage_block_with_a_missing_own_folder_lists_no_files(tmp_path):
    recorder = _recorder(persistence='sqlite', storage_dir=str(tmp_path / CLUSTER / WORKER))

    block = storage_block(_config(storage_dir=str(tmp_path)), recorder, worker_online=None)

    assert (block['files'], block['temp_files'], block['siblings']) == ([], {'count': 0, 'size_bytes': 0}, [])


def test_storage_block_with_a_cluster_folder_it_cannot_list_has_null_siblings(tmp_path, monkeypatch):
    own = tmp_path / CLUSTER / WORKER
    own.mkdir(parents=True)

    def refuse(_cluster_dir, _worker):
        raise PermissionError('denied')

    monkeypatch.setattr(databases_status, 'sibling_folders', refuse)
    recorder = _recorder(persistence='sqlite', storage_dir=str(own))

    block = storage_block(_config(storage_dir=str(tmp_path)), recorder, worker_online=None)

    assert block['siblings'] is None


def test_storage_block_skips_a_sibling_that_went_away(tmp_path, monkeypatch):
    own = tmp_path / CLUSTER / WORKER
    own.mkdir(parents=True)
    monkeypatch.setattr(
        databases_status, 'sibling_folders', lambda _dir, _worker: [(str(tmp_path / CLUSTER / 'w-gone'), 'w-gone')]
    )
    recorder = _recorder(persistence='sqlite', storage_dir=str(own))

    block = storage_block(_config(storage_dir=str(tmp_path)), recorder, worker_online=None)

    assert block['siblings'] == []


def test_count_cache_entries_answers_none_past_its_time_limit(tmp_path):
    path = tmp_path / 'w-1-cache.db'
    with _cache_file(path, rows=30_000):
        assert count_cache_entries(str(path)) == 30_000
        assert count_cache_entries(str(path), limit_seconds=0.0) is None


@pytest.mark.parametrize('content', [None, b'not a database at all, just text' * 100], ids=['missing', 'garbage'])
def test_count_cache_entries_answers_none_for_a_file_it_cannot_read(tmp_path, content):
    path = tmp_path / 'w-1-cache.db'
    if content is not None:
        path.write_bytes(content)

    assert count_cache_entries(str(path)) is None


def test_count_cache_entries_answers_none_when_it_cannot_connect(tmp_path, monkeypatch):
    def refuse(*_args, **_kwargs):
        raise sqlite3.OperationalError('unable to open database file')

    monkeypatch.setattr(databases_status.sqlite3, 'connect', refuse)

    assert count_cache_entries(str(tmp_path / 'w-1-cache.db')) is None


def test_recorder_block_of_a_postgres_worker_without_settings_has_no_table():
    stub = SimpleNamespace(persistence='postgres', memory_store=None, postgres_settings=None)

    assert recorder_block(_config(provider='postgres'), stub, now=NOW)['postgres'] is None


# --- GET /api/v1/debug/slice ---

SLICE = '/api/v1/debug/slice'


def _slice_drakkar_config() -> DrakkarConfig:
    config = DrakkarConfig(connections=kafka_connections(), sources={'kafka': {'enabled': True}})
    config.ui.release.enabled = False
    return config


@pytest.fixture
async def started_slice_recorder(tmp_path):
    """Start a recorder built in the test (``sqlite`` or ``none``) with a few recorded events; stop it at the end."""
    recorders: list[EventRecorder] = []

    async def start(persistence: str, *, events: int = 3) -> EventRecorder:
        storage = tmp_path / 'storage'
        storage.mkdir(exist_ok=True)
        shared = tmp_path / 'shared'
        shared.mkdir(exist_ok=True)
        rec = EventRecorder(
            RecorderConfig(flush_interval='60s'),
            worker_name=WORKER,
            cluster_name=CLUSTER,
            persistence=persistence,
            storage_dir=str(storage) if persistence == 'sqlite' else '',
            shared_dir=str(shared) if persistence == 'sqlite' else '',
        )
        await rec.start()
        recorders.append(rec)
        await rec.write_config(_slice_drakkar_config())
        for offset in range(events):
            rec._record({'ts': time.time(), 'event': 'consumed', 'partition': 0, 'offset': offset})
        return rec

    yield start
    for rec in recorders:
        await rec.stop()


def _slice_client(recorder: EventRecorder) -> AsyncClient:
    drakkar_app = MagicMock()
    drakkar_app._config = _slice_drakkar_config()
    app = create_ui_app(UIConfig(enabled=True), recorder, drakkar_app)
    return AsyncClient(transport=ASGITransport(app=app), base_url='http://test')


def _range(**extra: str) -> dict[str, str]:
    now = time.time()
    return {'from': str(now - 600), 'to': str(now + 60), **extra}


def _dot_files(folder) -> list[str]:
    return sorted(name for name in os.listdir(folder) if name.startswith('.'))


def _save_body(content: bytes, tmp_path) -> str:
    path = str(tmp_path / 'downloaded-slice.db')
    with open(path, 'wb') as out:
        out.write(content)
    return path


async def test_slice_answers_the_file_as_an_attachment_with_its_headers(tmp_path, started_slice_recorder):
    rec = await started_slice_recorder('sqlite', events=3)

    async with _slice_client(rec) as client:
        resp = await client.get(SLICE, params=_range())

    assert resp.status_code == 200
    assert resp.headers['content-type'] == 'application/x-sqlite3'
    assert re.fullmatch(
        r'attachment; filename="w-1-slice-\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}__\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}\.db"',
        resp.headers['content-disposition'],
    )
    assert resp.headers['cache-control'] == 'no-store, private'
    assert resp.headers['content-length'] == str(len(resp.content))
    # The flush ran first: events recorded but not yet written are in the file.
    assert resp.headers['x-drakkar-slice-rows'] == '3'
    assert 'x-drakkar-slice-truncated' not in resp.headers
    path = _save_body(resp.content, tmp_path)
    conn = sqlite3.connect(path)
    try:
        assert conn.execute('SELECT id, "offset" FROM events ORDER BY id').fetchall() == [(1, 0), (2, 1), (3, 2)]
        assert conn.execute('SELECT worker_name, cluster_name FROM worker_config').fetchone() == (WORKER, CLUSTER)
        assert conn.execute('SELECT kind FROM slice_sources').fetchall() == [('live',)]
        assert conn.execute('PRAGMA journal_mode').fetchone() == ('delete',)
    finally:
        conn.close()
    # The temp file was unlinked before the first byte was sent.
    assert _dot_files(rec.storage_dir) == []


async def test_slice_marks_a_cut_file_in_its_name_and_headers(started_slice_recorder):
    rec = await started_slice_recorder('sqlite', events=3)

    async with _slice_client(rec) as client:
        resp = await client.get(SLICE, params=_range(max_rows='2'))

    assert resp.status_code == 200
    assert resp.headers['x-drakkar-slice-rows'] == '2'
    assert resp.headers['x-drakkar-slice-truncated'] == 'rows'
    assert resp.headers['content-disposition'].endswith('-partial.db"')


async def test_slice_streams_gzip_without_a_length(tmp_path, started_slice_recorder):
    rec = await started_slice_recorder('sqlite', events=2)

    async with _slice_client(rec) as client:
        resp = await client.get(SLICE, params=_range(compress='gzip'))

    assert resp.status_code == 200
    assert resp.headers['content-type'] == 'application/gzip'
    assert 'content-length' not in resp.headers
    assert resp.headers['content-disposition'].endswith('.db.gz"')
    path = _save_body(gzip.decompress(resp.content), tmp_path)
    with contextlib.closing(sqlite3.connect(path)) as conn:
        assert conn.execute('SELECT COUNT(*) FROM events').fetchone() == (2,)


async def test_slice_on_a_worker_without_a_folder_is_built_in_ram(tmp_path, started_slice_recorder):
    rec = await started_slice_recorder('none', events=2)

    with capture_logs() as logs:
        async with _slice_client(rec) as client:
            resp = await client.get(SLICE, params=_range())

    assert resp.status_code == 200
    assert resp.headers['x-drakkar-slice-rows'] == '2'
    assert [entry['reason'] for entry in logs if entry['event'] == 'debug_slice_in_ram'] == ['storage_dir_unset']


async def test_slice_without_filters_answers_400_before_409_and_429(started_slice_recorder, monkeypatch):
    rec = await started_slice_recorder('none', events=1)
    release = asyncio.Event()
    monkeypatch.setattr(routes_databases, 'build_slice', _blocked_build(release))

    async with _slice_client(rec) as client:
        first = asyncio.ensure_future(client.get(SLICE, params=_range()))
        await _until_building()
        bare = await client.get(SLICE)
        cluster_and_broken = await client.get(SLICE, params=_range(scope='cluster', compress='zstd'))
        cluster = await client.get(SLICE, params=_range(scope='cluster'))
        busy = await client.get(SLICE, params=_range())
        release.set()
        await first

    # The UI's support probe sends no filters: the route exists when it answers 400.
    assert (bare.status_code, bare.json()) == (400, {'error': 'from and to must be finite numbers of epoch seconds'})
    assert (cluster_and_broken.status_code, cluster_and_broken.json()) == (400, {'error': 'compress must be gzip'})
    assert (cluster.status_code, cluster.json()) == (
        409,
        {'error': 'scope cluster needs the shared history or the Postgres table'},
    )
    assert (busy.status_code, busy.json()) == (429, {'error': 'a slice is already being built on this worker'})
    assert busy.headers['retry-after'] == '5'


_BUILDING: list[bool] = []


def _blocked_build(release: asyncio.Event, handles: list[io.BytesIO] | None = None):
    """A build that waits for ``release``, then answers a small file."""

    async def build(request, worker, plan, **_kwargs):
        _BUILDING.append(True)
        try:
            await release.wait()
        finally:
            _BUILDING.pop()
        handle = io.BytesIO(b'slice')
        if handles is not None:
            handles.append(handle)
        return BuiltSlice(handle=handle, name='w-1-slice.db', rows=0, truncated=None, size_bytes=5)

    return build


async def _until_building() -> None:
    for _ in range(500):
        if _BUILDING:
            return
        await asyncio.sleep(0.01)
    raise AssertionError('the build did not start')


async def test_slice_builds_one_at_a_time_and_frees_the_slot_when_the_build_ends(started_slice_recorder, monkeypatch):
    rec = await started_slice_recorder('none', events=1)
    release = asyncio.Event()
    monkeypatch.setattr(routes_databases, 'build_slice', _blocked_build(release))

    async with _slice_client(rec) as client:
        first = asyncio.ensure_future(client.get(SLICE, params=_range()))
        await _until_building()
        second = await client.get(SLICE, params=_range())
        release.set()
        done = await first
        third = await client.get(SLICE, params=_range())

    assert second.status_code == 429
    assert done.status_code == 200 and done.content == b'slice'
    assert third.status_code == 200


async def test_a_request_that_leaves_mid_build_keeps_the_slot_until_the_build_ends(started_slice_recorder, monkeypatch):
    rec = await started_slice_recorder('none', events=1)
    release = asyncio.Event()
    handles: list[io.BytesIO] = []
    monkeypatch.setattr(routes_databases, 'build_slice', _blocked_build(release, handles))

    async with _slice_client(rec) as client:
        leaving = asyncio.ensure_future(client.get(SLICE, params=_range()))
        await _until_building()
        leaving.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await leaving
        while_building = await client.get(SLICE, params=_range())
        release.set()
        for _ in range(100):
            if handles and handles[0].closed:
                break
            await asyncio.sleep(0.01)
        after = await client.get(SLICE, params=_range())

    assert while_building.status_code == 429
    # The file of the request that left is closed once its build ends.
    assert handles[0].closed
    assert after.status_code == 200


async def test_a_stream_closed_early_closes_its_file_at_once_and_leaves_no_temp_file(tmp_path, started_slice_recorder):
    rec = await started_slice_recorder('sqlite', events=2)
    request = parse_slice_request(_range(), routes_databases.slice_sources_of(rec))
    built = await build_slice(request, routes_databases.slice_worker_of(rec), plan_slice(request, rec.persistence))
    released: list[bool] = []
    response = routes_databases._slice_response(built, None, lambda: released.append(True))

    assert _dot_files(rec.storage_dir) == []
    first_chunk = await response.body_iterator.__anext__()
    await response.body_iterator.aclose()

    assert first_chunk.startswith(b'SQLite format 3')
    # Closed by the stream itself, not later by the garbage collector.
    assert built.handle.closed
    assert _dot_files(rec.storage_dir) == []


async def test_a_client_that_takes_no_chunk_ends_the_stream_and_frees_its_bytes(tmp_path, monkeypatch):
    monkeypatch.setattr(routes_databases, 'SLICE_SEND_IDLE_SECONDS', 0.05)
    handle = io.BytesIO(b'x' * 10)
    released: list[bool] = []
    built = BuiltSlice(handle=handle, name='w-1-slice.db', rows=0, truncated=None, size_bytes=10)
    response = routes_databases._slice_response(built, None, lambda: released.append(True))
    sent: list[dict] = []

    async def stalled_send(message):
        sent.append(message)
        if message['type'] == 'http.response.body':
            await asyncio.sleep(3600)

    async def receive():
        await asyncio.sleep(3600)

    with pytest.raises(Exception):  # noqa: B017 - the server turns the timeout into a disconnect
        await response({'type': 'http', 'asgi': {'spec_version': '2.4'}}, receive, stalled_send)

    assert sent[0]['type'] == 'http.response.start'
    assert handle.closed
    assert released == [True]


def test_open_slices_count_until_release_and_release_once():
    open_slices = routes_databases.OpenSlices()
    release = open_slices.hold(routes_databases.SLICE_OPEN_MAX_BYTES - 1)
    assert not open_slices.is_full()
    second = open_slices.hold(1)
    assert open_slices.is_full()
    second()
    second()
    assert open_slices.bytes == routes_databases.SLICE_OPEN_MAX_BYTES - 1
    release()
    assert open_slices.bytes == 0


async def test_slice_answers_429_while_slices_being_sent_hold_the_open_bytes(started_slice_recorder, monkeypatch):
    rec = await started_slice_recorder('none', events=1)
    seen: list[routes_databases.OpenSlices] = []

    class Recorded(routes_databases.OpenSlices):
        def __init__(self) -> None:
            super().__init__()
            seen.append(self)

    monkeypatch.setattr(routes_databases, 'OpenSlices', Recorded)
    async with _slice_client(rec) as client:
        done = await client.get(SLICE, params=_range())
        (open_slices,) = seen
        # The whole body was sent: its bytes left the count.
        after_send = open_slices.bytes
        open_slices.hold(routes_databases.SLICE_OPEN_MAX_BYTES)
        full = await client.get(SLICE, params=_range())

    assert done.status_code == 200
    assert after_send == 0
    assert (full.status_code, full.json()) == (
        429,
        {'error': 'slices still being sent hold 2 GiB on this worker; wait for one to end'},
    )
    assert full.headers['retry-after'] == '5'


async def test_a_slice_that_cannot_be_written_answers_500_and_logs(started_slice_recorder, monkeypatch):
    rec = await started_slice_recorder('sqlite', events=2)

    def broken(self, rows, worker_id):
        raise sqlite3.OperationalError('database or disk is full')

    monkeypatch.setattr(slice_build.SliceWriter, 'insert_events', broken)
    with capture_logs() as logs:
        async with _slice_client(rec) as client:
            resp = await client.get(SLICE, params=_range())

    assert (resp.status_code, resp.json()) == (500, {'error': 'Slice failed: database or disk is full'})
    (line,) = [entry for entry in logs if entry['event'] == 'debug_slice_failed']
    assert line['log_level'] == 'warning' and line['error_type'] == 'OperationalError'
    assert _dot_files(rec.storage_dir) == []


@pytest.mark.parametrize(('outcome', 'note'), [('hang', 'flush_timed_out'), ('raise', 'flush_failed')])
async def test_slice_notes_a_flush_that_did_not_end(tmp_path, started_slice_recorder, monkeypatch, outcome, note):
    rec = await started_slice_recorder('sqlite', events=1)
    monkeypatch.setattr(routes_databases, 'SLICE_FLUSH_WAIT_SECONDS', 0.05)

    async def stuck(coro, _loop):
        coro.close()
        if outcome == 'raise':
            raise RuntimeError('stage failed')
        await asyncio.sleep(3600)

    monkeypatch.setattr(routes_databases, 'dispatch_to_loop', stuck)
    async with _slice_client(rec) as client:
        resp = await client.get(SLICE, params=_range(sources='files'))

    assert resp.status_code == 200
    with contextlib.closing(sqlite3.connect(_save_body(resp.content, tmp_path))) as conn:
        assert json.loads(conn.execute('SELECT notes FROM slice').fetchone()[0]) == [note]


def test_slice_sources_and_worker_of_a_recorder_per_mode(tmp_path):
    sqlite_rec = _recorder(persistence='sqlite', storage_dir=str(tmp_path), shared_dir=str(tmp_path / 'shared'))
    postgres_rec = _recorder(persistence='postgres')
    off = _recorder()

    assert routes_databases.slice_sources_of(sqlite_rec) == SliceSources(
        worker=('files', 'history'), cluster=('history',)
    )
    assert routes_databases.slice_sources_of(postgres_rec) == SliceSources(worker=('postgres',), cluster=('postgres',))
    assert routes_databases.slice_sources_of(off) == SliceSources(worker=(), cluster=())
    worker = routes_databases.slice_worker_of(sqlite_rec)
    assert (worker.worker, worker.instance, worker.cluster, worker.provider) == (
        WORKER,
        '0123456789abcdef',
        CLUSTER,
        'sqlite',
    )
    assert worker.history_dir == str(tmp_path / 'shared' / 'history' / CLUSTER)
    assert worker.max_files == RecorderConfig().search.max_files
    assert routes_databases.slice_worker_of(postgres_rec).postgres is postgres_rec.postgres_settings


async def test_slice_of_the_postgres_table(tmp_path, monkeypatch):
    fake = SliceFakePostgres([pg_row(1, time.time() - 5, worker=WORKER), pg_row(2, time.time() - 4, worker='w-2')])
    monkeypatch.setattr(asyncpg, 'connect', fake.connect)

    async with _client(_recorder(persistence='postgres'), _config(provider='postgres')) as client:
        resp = await client.get(SLICE, params=_range(scope='cluster'))

    assert resp.status_code == 200
    assert resp.headers['x-drakkar-slice-rows'] == '2'
    assert resp.headers['content-disposition'].startswith('attachment; filename="search-cluster-slice-')
    assert DSN not in resp.text
    assert fake.cursor_calls()[0][2][0] == CLUSTER


@pytest.mark.parametrize('cause', ['error', 'busy'])
async def test_a_failed_postgres_read_answers_503_and_logs(monkeypatch, cause):
    fake = SliceFakePostgres([])
    fake.error = OSError('connection refused')
    monkeypatch.setattr(asyncpg, 'connect', fake.connect)

    with capture_logs() as logs:
        async with _client(_recorder(persistence='postgres'), _config(provider='postgres')) as client:
            if cause == 'busy':
                with postgres_read_slot(), postgres_read_slot():
                    resp = await client.get(SLICE, params=_range())
            else:
                resp = await client.get(SLICE, params=_range())

    assert (resp.status_code, resp.json()) == (503, {'error': 'postgres read failed'})
    (line,) = [entry for entry in logs if entry['event'] == 'ui_read_postgres_failed']
    assert line['endpoint'] == SLICE and line['reason'] == cause and line['category'] == 'debug'
    assert 'secret-password' not in line['error']


@pytest.mark.parametrize(('waiting', 'notes'), [(3, ['postgres_queue_waiting']), (0, []), (None, [])])
async def test_a_postgres_slice_names_rows_still_waiting_for_the_write_stage(tmp_path, monkeypatch, waiting, notes):
    fake = SliceFakePostgres([])
    monkeypatch.setattr(asyncpg, 'connect', fake.connect)
    rec = _recorder(persistence='postgres')
    asked: list[float] = []

    async def flush_postgres(timeout: float) -> int | None:
        asked.append(timeout)
        return waiting

    rec.flush_postgres = flush_postgres  # ty: ignore[invalid-assignment]
    async with _client(rec, _config(provider='postgres')) as client:
        resp = await client.get(SLICE, params=_range())

    assert resp.status_code == 200
    assert asked == [routes_databases.SLICE_POSTGRES_FLUSH_SECONDS]
    with contextlib.closing(sqlite3.connect(_save_body(resp.content, tmp_path))) as conn:
        assert json.loads(conn.execute('SELECT notes FROM slice').fetchone()[0]) == notes


# --- GET /debug/databases/postgres ---

PARTITIONS_ENDPOINT = '/api/v1/debug/databases/postgres'
MIB = 1024 * 1024


class _PartitionsConnection:
    """Answers the four statements of the partitions read; every call lands in ``calls``."""

    def __init__(self, fake: _PartitionsFake) -> None:
        self._fake = fake
        self.closed = False

    def transaction(self) -> SimpleNamespace:
        async def start() -> None:
            self._fake.calls.append(('BEGIN',))

        async def commit() -> None:
            self._fake.calls.append(('COMMIT',))

        return SimpleNamespace(start=start, commit=commit)

    async def execute(self, sql: str, *args):
        self._fake.calls.append((sql, *args))

    async def fetchval(self, sql: str, *args):
        self._fake.calls.append((sql, *args))
        statements = RECORDER_STATEMENT_TEMPLATES
        if sql == statements['server_now']:
            if self._fake.hang:
                await asyncio.sleep(3600)
            return self._fake.now
        assert sql == statements['retention_read']
        return self._fake.comment

    async def fetch(self, sql: str, *args):
        self._fake.calls.append((sql, *args))
        statements = RECORDER_STATEMENT_TEMPLATES
        if sql == statements['partitions_list']:
            return [(name,) for name in self._fake.listed]
        assert sql == statements['partition_sizes']
        return [(name, self._fake.sizes[name]) for name in args[1] if name in self._fake.sizes]

    async def close(self) -> None:
        if self._fake.close_error is not None:
            raise self._fake.close_error
        self.closed = True

    def is_closed(self) -> bool:
        return self.closed

    def terminate(self) -> None:
        self.closed = True


class _PartitionsFake:
    """Stands in for :func:`asyncpg.connect` in the partitions read."""

    def __init__(self, *, now: float, listed: list[str], sizes: dict[str, int], comment: str | None) -> None:
        self.now = now
        self.listed = listed
        self.sizes = sizes
        self.comment = comment
        self.calls: list[tuple] = []
        self.dsns: list[str] = []
        self.connect_error: Exception | None = None
        self.close_error: Exception | None = None
        self.hang = False

    async def connect(self, *, dsn: str) -> _PartitionsConnection:
        self.dsns.append(dsn)
        if self.connect_error is not None:
            raise self.connect_error
        return _PartitionsConnection(self)


TABLE = 'fast_recorder_events'
HOUR = 491_000  # an hour since the epoch (2026-01-05 08:00 UTC)


def _partitions_fake() -> _PartitionsFake:
    """Hours HOUR-8 to HOUR+3 of 10 MiB each and a child table of another name; retention 6 h stored."""
    hours = range(HOUR - 8, HOUR + 4)
    listed = [partition_name(TABLE, hour) for hour in hours] + [f'{TABLE}_old']
    sizes = {partition_name(TABLE, hour): 10 * MIB for hour in hours} | {f'{TABLE}_old': 1}
    return _PartitionsFake(now=HOUR * 3600 + 100.0, listed=listed, sizes=sizes, comment='retention_hours=6')


async def test_postgres_partitions_answers_the_rows_and_the_next_drops(monkeypatch):
    fake = _partitions_fake()
    monkeypatch.setattr(asyncpg, 'connect', fake.connect)

    async with _client(_recorder(persistence='postgres'), _config(provider='postgres')) as client:
        resp = await client.get(PARTITIONS_ENDPOINT)

    assert resp.status_code == 200
    body = resp.json()
    names = [partition_name(TABLE, hour) for hour in range(HOUR - 8, HOUR + 4)]
    assert body['server_now'] == HOUR * 3600 + 100.0
    assert body['stored_retention_hours'] == 6
    assert [row['name'] for row in body['partitions']] == names
    assert body['partitions'][0] == {
        'name': names[0],
        'from_ts': (HOUR - 8) * 3600,
        'to_ts': (HOUR - 7) * 3600,
        'size_bytes': 10 * MIB,
    }
    assert body['total_bytes'] == 120 * MIB
    assert body['next_drops'] == {
        'retention': names[:2],
        # 100 MiB are left after the retention; the oldest go until 64 MiB or less remain.
        'size': names[2:6],
    }
    assert DSN not in resp.text
    assert fake.dsns == [DSN]


async def test_postgres_partitions_sends_only_the_four_existing_statements_under_the_search_limit(monkeypatch):
    fake = _partitions_fake()
    monkeypatch.setattr(asyncpg, 'connect', fake.connect)

    async with _client(_recorder(persistence='postgres'), _config(provider='postgres')) as client:
        await client.get(PARTITIONS_ENDPOINT)

    statements = RECORDER_STATEMENT_TEMPLATES
    names = [partition_name(TABLE, hour) for hour in range(HOUR - 8, HOUR + 4)]
    assert fake.calls == [
        ('BEGIN',),
        (statements['set_local'], 'statement_timeout', statement_timeout_ms(RecorderConfig().search.timeout)),
        (statements['set_local'], 'jit', 'off'),
        (statements['server_now'],),
        (statements['retention_read'], f'drakkar.{TABLE}'),
        (statements['partitions_list'], f'drakkar.{TABLE}'),
        (statements['partition_sizes'], 'drakkar', names),
        ('COMMIT',),
    ]


async def test_postgres_partitions_without_a_stored_retention_drops_nothing_by_retention(monkeypatch):
    fake = _partitions_fake()
    fake.comment = 'set by hand'
    monkeypatch.setattr(asyncpg, 'connect', fake.connect)

    async with _client(_recorder(persistence='postgres'), _config(provider='postgres')) as client:
        body = (await client.get(PARTITIONS_ENDPOINT)).json()

    assert body['stored_retention_hours'] is None
    assert body['next_drops']['retention'] == []
    assert len(body['next_drops']['size']) == 6


async def test_postgres_partitions_of_a_partition_gone_during_the_read_has_no_size(monkeypatch):
    fake = _partitions_fake()
    gone = partition_name(TABLE, HOUR)
    del fake.sizes[gone]
    monkeypatch.setattr(asyncpg, 'connect', fake.connect)

    async with _client(_recorder(persistence='postgres'), _config(provider='postgres')) as client:
        body = (await client.get(PARTITIONS_ENDPOINT)).json()

    assert next(row for row in body['partitions'] if row['name'] == gone)['size_bytes'] is None
    assert body['total_bytes'] == 110 * MIB


async def test_postgres_partitions_on_a_worker_of_another_provider_answers_404():
    async with _client(_recorder(), _config()) as client:
        resp = await client.get(PARTITIONS_ENDPOINT)

    assert resp.status_code == 404
    assert resp.json() == {'error': 'the recorder provider is not postgres'}


async def test_postgres_partitions_answers_503_and_logs_when_the_read_fails(monkeypatch):
    fake = _partitions_fake()
    fake.connect_error = OSError('connection refused')
    monkeypatch.setattr(asyncpg, 'connect', fake.connect)

    with capture_logs() as logs:
        async with _client(_recorder(persistence='postgres'), _config(provider='postgres')) as client:
            resp = await client.get(PARTITIONS_ENDPOINT)

    assert resp.status_code == 503
    assert resp.json() == {'error': 'postgres read failed'}
    failed = [entry for entry in logs if entry['event'] == 'ui_read_postgres_failed']
    assert [(entry['endpoint'], entry['reason']) for entry in failed] == [(PARTITIONS_ENDPOINT, 'error')]
    assert DSN not in json.dumps(failed)


async def test_postgres_partitions_answers_503_busy_when_both_read_slots_are_taken(monkeypatch):
    fake = _partitions_fake()
    monkeypatch.setattr(asyncpg, 'connect', fake.connect)

    with capture_logs() as logs, postgres_read_slot(), postgres_read_slot():
        async with _client(_recorder(persistence='postgres'), _config(provider='postgres')) as client:
            resp = await client.get(PARTITIONS_ENDPOINT)

    assert resp.status_code == 503
    assert fake.dsns == []
    assert [entry['reason'] for entry in logs if entry['event'] == 'ui_read_postgres_failed'] == ['busy']


async def test_postgres_partitions_answers_503_when_the_read_runs_out_of_time(monkeypatch):
    fake = _partitions_fake()
    fake.hang = True
    monkeypatch.setattr(asyncpg, 'connect', fake.connect)
    config = _config(provider='postgres', search={'timeout': '1s'})
    recorder = EventRecorder(
        config.recorder,
        worker_name=WORKER,
        cluster_name=CLUSTER,
        persistence='postgres',
        instance='0123456789abcdef',
        postgres=_postgres_settings(),
    )

    with capture_logs() as logs:
        async with _client(recorder, config) as client:
            resp = await client.get(PARTITIONS_ENDPOINT)

    assert resp.status_code == 503
    assert [entry['reason'] for entry in logs if entry['event'] == 'ui_read_postgres_failed'] == ['timeout']


async def test_postgres_partitions_keeps_a_complete_read_whose_close_fails(monkeypatch):
    fake = _partitions_fake()
    fake.close_error = OSError('reset by peer')
    monkeypatch.setattr(asyncpg, 'connect', fake.connect)

    with capture_logs() as logs:
        async with _client(_recorder(persistence='postgres'), _config(provider='postgres')) as client:
            resp = await client.get(PARTITIONS_ENDPOINT)

    assert resp.status_code == 200
    assert [entry['reason'] for entry in logs if entry['event'] == 'ui_read_postgres_failed'] == ['close']
