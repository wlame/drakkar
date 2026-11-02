"""``GET /api/v1/debug/databases`` (contract v1.30): ``parts``, and the ``worker``, ``recorder`` and ``storage`` blocks.

Hermetic: real recorders that are never started (or a stub where the test
needs a memory store or a writer), folders and SQLite files written by the
test, a mocked app with a real config.
"""

from __future__ import annotations

import contextlib
import os
import sqlite3
from collections.abc import Iterator
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

from drakkar.cache.sql import SCHEMA_CACHE_ENTRIES
from drakkar.config import DrakkarConfig, PostgresConnectionConfig, RecorderConfig, ResilienceConfig, UIConfig
from drakkar.fleet.service import FleetRegistry
from drakkar.ownerlock import take_own_lock
from drakkar.recorder import EventRecorder
from drakkar.recorder.postgres import RecorderPostgresSettings
from drakkar.recorder.postgres_writer import PostgresStageStatus, UpkeepStatus, WriterStatus
from drakkar.uiserver import databases_status, routes_debug
from drakkar.uiserver.databases_status import count_cache_entries, parse_parts, recorder_block, storage_block
from drakkar.uiserver.server import create_ui_app
from tests.conftest import kafka_connections

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
        'queue_bytes': 2048,
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
