"""The debug routes over sealed history (contract v1.23 part 2).

``/debug/databases`` answers ``{own, history}``; downloads and merges take
paths relative to ``shared.dir`` (``history/<cluster>/<name>``,
``merged/<name>``); archives list every cluster folder; the trace reads the
sealed history only with ``history=1``.
"""

import gzip
import os
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

from drakkar.config import DrakkarConfig
from drakkar.recorder import EventRecorder
from drakkar.recorder.archive import archive_file_name
from drakkar.recorder.history import meta_path, serialize_json
from drakkar.recorder.schema import SCHEMA_EVENTS, SCHEMA_WORKER_CONFIG
from drakkar.uiserver.server import create_ui_app
from tests.conftest import make_ui_config

CLUSTER = 'search'
LIVE_NAME = 'w-1-2026-10-14__09_00_00.db'
DAY = 86400.0


def _recorder_db(path, *, worker: str = 'w-1', events: int = 2, cluster: str = CLUSTER) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(str(path))
    db.executescript(SCHEMA_WORKER_CONFIG)
    db.execute(
        'INSERT INTO worker_config (id, worker_name, cluster_name, created_at, created_at_dt) VALUES (1, ?, ?, ?, ?)',
        [worker, cluster, 0, ''],
    )
    db.executescript(SCHEMA_EVENTS)
    for i in range(events):
        db.execute(
            'INSERT INTO events (ts, dt, event, partition, offset) VALUES (?, ?, ?, ?, ?)',
            [1_760_000_000.0 + i, '', 'consumed', 3, 77 + i],
        )
    db.commit()
    db.close()
    return str(path)


def _meta(worker: str) -> dict:
    return {
        'schema': 1,
        'worker': worker,
        'instance': '1111111111111111',
        'cluster': CLUSTER,
        'first_event_ts': '2025-10-09T08:53:20.000000Z',
        'last_event_ts': '2025-10-09T08:53:21.000000Z',
        'sealed_at': '2026-10-14T09:00:00.000000Z',
        'size_bytes': 8192,
        'tables': {'events': 2, 'worker_config': 1},
        'event_types': {'consumed': 2},
    }


def _sealed(shared, cluster: str, name: str, *, worker: str = 'w-1', with_meta: bool = True) -> str:
    path = _recorder_db(shared / 'history' / cluster / name, worker=worker)
    if with_meta:
        with open(meta_path(path), 'wb') as handle:
            handle.write(serialize_json(_meta(worker)))
    return path


def _archive(shared, cluster: str, end: float) -> str:
    folder = shared / 'history' / cluster
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / archive_file_name(cluster, end - DAY, end)
    with gzip.open(path, 'wb') as gz:
        gz.write(b'archived bytes')
    return str(path)


@pytest.fixture
def storage(tmp_path):
    folder = tmp_path / 'storage'
    folder.mkdir()
    return folder


@pytest.fixture
def shared(tmp_path):
    return tmp_path / 'shared'


def _app(tmp_path, *, memory_only: bool = False, merge_enabled: bool = True):
    config = make_ui_config(enabled=True, port=8080, merge_enabled=merge_enabled)
    recorder = EventRecorder(
        config,
        worker_name='w-1',
        cluster_name=CLUSTER,
        storage_dir='' if memory_only else str(tmp_path / 'storage'),
        shared_dir=str(tmp_path / 'shared'),
    )
    recorder._db_path = str(tmp_path / 'storage' / LIVE_NAME)
    drakkar_app = MagicMock()
    drakkar_app._config = DrakkarConfig(sources={'kafka': {'enabled': True}})
    drakkar_app._config.ui.release.enabled = False
    drakkar_app.fleet = None
    return create_ui_app(config, recorder, drakkar_app), recorder


@pytest.fixture
async def client(tmp_path, storage):
    app, _recorder = _app(tmp_path)
    async with AsyncClient(transport=ASGITransport(app=app), base_url='http://test') as c:
        yield c


# --- /debug/databases ---


async def test_memory_only_databases_answer_is_empty(tmp_path):
    app, _recorder = _app(tmp_path, memory_only=True)
    async with AsyncClient(transport=ASGITransport(app=app), base_url='http://test') as c:
        resp = await c.get('/api/v1/debug/databases')

    assert resp.json() == {'own': [], 'history': {'folders': []}}


async def test_databases_lists_own_files_with_measured_stats(client, storage):
    _recorder_db(storage / LIVE_NAME, events=3)
    _recorder_db(storage / 'w-1-2026-10-14__08_00_00.db', events=1)
    _recorder_db(storage / 'w-2-2026-10-14__08_00_00.db', worker='w-2')

    own = (await client.get('/api/v1/debug/databases')).json()['own']

    assert [(row['filename'], row['kind'], row['event_count']) for row in own] == [
        ('w-1-2026-10-14__08_00_00.db', 'backlog', 1),
        (LIVE_NAME, 'live', 3),
    ]
    assert own[1]['worker_name'] == 'w-1'
    assert own[1]['cluster_name'] == CLUSTER
    assert own[1]['event_counts'] == {'consumed': 3}
    assert own[1]['first_event_ts'] == 1_760_000_000.0
    assert own[1]['size_bytes'] == os.path.getsize(storage / LIVE_NAME)


async def test_databases_lists_each_history_folder_then_merged(client, shared):
    with_meta = _sealed(shared, CLUSTER, 'w-1-2026-10-13__08_00_00.db')
    _sealed(shared, CLUSTER, 'w-2-2026-10-13__09_00_00.db', worker='w-2', with_meta=False)
    archive = _archive(shared, CLUSTER, datetime(2026, 10, 12, tzinfo=UTC).timestamp())
    _sealed(shared, 'index', 'w-9-2026-10-13__08_00_00.db', worker='w-9')
    (shared / 'history' / CLUSTER / 'w-3-2026-10-13__10_00_00.db.meta.json').write_bytes(b'{"schema":1}')
    (shared / 'history' / CLUSTER / '.w-4-2026-10-13__10_00_00.db.0123456789abcdef.incoming').write_bytes(b'x')
    (shared / 'history' / CLUSTER / 'w-5-2026-10-13__10_00_00.db.unreadable').write_bytes(b'x')
    merged = _recorder_db(shared / 'merged' / 'merged-2026-10-14__10_00_00-0123456789abcdef.db')

    folders = (await client.get('/api/v1/debug/databases')).json()['history']['folders']

    assert [(f['folder'], f['kind'], f['cluster']) for f in folders] == [
        ('history/index', 'history', 'index'),
        (f'history/{CLUSTER}', 'history', CLUSTER),
        ('merged', 'merged', None),
    ]
    files = folders[1]['files']
    assert [(f['name'], f['kind']) for f in files] == [
        (os.path.basename(archive), 'archive'),
        ('w-1-2026-10-13__08_00_00.db', 'sealed'),
        ('w-2-2026-10-13__09_00_00.db', 'sealed'),
    ]
    assert files[1]['path'] == f'history/{CLUSTER}/w-1-2026-10-13__08_00_00.db'
    assert files[1]['meta'] == _meta('w-1')
    assert files[1]['size_bytes'] == os.path.getsize(with_meta)
    assert files[0]['meta'] is None
    assert files[2]['meta'] is None
    assert folders[2]['files'][0]['path'] == f'merged/{os.path.basename(merged)}'
    assert folders[2]['files'][0]['kind'] == 'merged'
    mtime = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=os.stat(with_meta).st_mtime_ns // 1000)
    assert files[1]['modified_at'] == mtime.strftime('%Y-%m-%dT%H:%M:%S.%fZ')


# --- /debug/download/{path} ---


async def test_download_serves_a_sealed_file_an_archive_and_a_merge_result(client, shared):
    sealed = _sealed(shared, CLUSTER, 'w-1-2026-10-13__08_00_00.db')
    archive = _archive(shared, CLUSTER, datetime(2026, 10, 12, tzinfo=UTC).timestamp())
    merged = _recorder_db(shared / 'merged' / 'merged-2026-10-14__10_00_00-0123456789abcdef.db')

    cases = [
        (f'history/{CLUSTER}/{os.path.basename(sealed)}', sealed, 'application/x-sqlite3'),
        (f'history/{CLUSTER}/{os.path.basename(archive)}', archive, 'application/gzip'),
        (f'merged/{os.path.basename(merged)}', merged, 'application/x-sqlite3'),
    ]
    for path, source, media_type in cases:
        resp = await client.get(f'/api/v1/debug/download/{path}')
        assert resp.status_code == 200, path
        assert resp.content == Path(source).read_bytes()
        assert resp.headers['content-type'] == media_type
        assert os.path.basename(source) in resp.headers['content-disposition']


@pytest.mark.parametrize(
    'path',
    [
        f'history/{CLUSTER}/%2e%2e/%2e%2e/storage/{LIVE_NAME}',
        f'history/{CLUSTER}/.w-1-2026-10-13__08_00_00.db',
        'history/.hidden/w-1-2026-10-13__08_00_00.db',
        f'storage/{LIVE_NAME}',
        f'history/{CLUSTER}/sub/w-1-2026-10-13__08_00_00.db',
        f'history/{CLUSTER}/w-1-2026-10-13__08_00_00.db.meta.json',
        f'history/{CLUSTER}/notes.txt',
        'merged/w-1-2026-10-13__08_00_00.db.gz',
        f'history/{CLUSTER}/missing-2026-10-13__08_00_00.db',
        f'history/{CLUSTER}/w-1-2026-10-13__08_00_00.db%22%3B',
        '%2Fetc%2Fpasswd',
    ],
)
async def test_download_refuses_any_other_path(client, shared, storage, path):
    _sealed(shared, CLUSTER, 'w-1-2026-10-13__08_00_00.db')
    _recorder_db(storage / LIVE_NAME)

    resp = await client.get(f'/api/v1/debug/download/{path}')

    assert resp.status_code == 404


async def test_download_refuses_a_symlink_that_leaves_the_shared_folder(client, shared, tmp_path):
    outside = tmp_path / 'outside.db'
    outside.write_bytes(b'not yours')
    folder = shared / 'history' / CLUSTER
    folder.mkdir(parents=True)
    os.symlink(outside, folder / 'w-1-2026-10-13__08_00_00.db')
    os.symlink(tmp_path, shared / 'history' / 'escape')

    for path in (f'history/{CLUSTER}/w-1-2026-10-13__08_00_00.db', 'history/escape/outside.db'):
        resp = await client.get(f'/api/v1/debug/download/{path}')
        assert resp.status_code == 404, path


async def test_memory_only_download_answers_404(tmp_path, shared):
    _sealed(shared, CLUSTER, 'w-1-2026-10-13__08_00_00.db')
    app, _recorder = _app(tmp_path, memory_only=True)
    async with AsyncClient(transport=ASGITransport(app=app), base_url='http://test') as c:
        resp = await c.get(f'/api/v1/debug/download/history/{CLUSTER}/w-1-2026-10-13__08_00_00.db')

    assert resp.status_code == 404


# --- /debug/merge ---


async def test_merge_writes_its_result_into_the_merged_folder(client, shared):
    first = _sealed(shared, CLUSTER, 'w-1-2026-10-13__08_00_00.db')
    second = _sealed(shared, 'index', 'w-9-2026-10-13__08_00_00.db', worker='w-9')
    paths = [f'history/{CLUSTER}/{os.path.basename(first)}', f'history/index/{os.path.basename(second)}']

    resp = await client.post('/api/v1/debug/merge', json={'filenames': paths})

    assert resp.status_code == 200
    body = resp.json()
    assert body['path'] == f'merged/{body["filename"]}'
    assert body['filename'].startswith('merged-') and body['filename'].endswith('.db')
    assert body['worker_count'] == 2
    assert sorted(p.name for p in (shared / 'merged').iterdir()) == [body['filename']]
    assert oct((shared / 'merged').stat().st_mode & 0o777) == oct(0o700)
    downloaded = await client.get(f'/api/v1/debug/download/{body["path"]}')
    assert downloaded.status_code == 200


async def test_merge_accepts_a_merge_result_as_a_source(client, shared):
    first = _sealed(shared, CLUSTER, 'w-1-2026-10-13__08_00_00.db')
    merged = _recorder_db(shared / 'merged' / 'merged-2026-10-14__10_00_00-0123456789abcdef.db')
    paths = [f'history/{CLUSTER}/{os.path.basename(first)}', f'merged/{os.path.basename(merged)}']

    resp = await client.post('/api/v1/debug/merge', json={'filenames': paths})

    assert resp.status_code == 200


@pytest.mark.parametrize(
    ('bad', 'status'),
    [
        (f'history/{CLUSTER}/ARCHIVE', 400),
        (f'history/{CLUSTER}/../{CLUSTER}/w-1-2026-10-13__08_00_00.db', 400),
        (f'storage/{LIVE_NAME}', 400),
        (f'history/{CLUSTER}/missing-2026-10-13__08_00_00.db', 404),
    ],
)
async def test_merge_refuses_a_path_it_must_not_read(client, shared, bad, status):
    first = _sealed(shared, CLUSTER, 'w-1-2026-10-13__08_00_00.db')
    archive = _archive(shared, CLUSTER, datetime(2026, 10, 12, tzinfo=UTC).timestamp())
    bad = bad.replace('ARCHIVE', os.path.basename(archive))

    resp = await client.post(
        '/api/v1/debug/merge', json={'filenames': [f'history/{CLUSTER}/{os.path.basename(first)}', bad]}
    )

    assert resp.status_code == status
    assert not (shared / 'merged').exists()


async def test_memory_only_merge_answers_404(tmp_path, shared):
    _sealed(shared, CLUSTER, 'w-1-2026-10-13__08_00_00.db')
    _sealed(shared, CLUSTER, 'w-2-2026-10-13__08_00_00.db', worker='w-2')
    app, _recorder = _app(tmp_path, memory_only=True)
    paths = [f'history/{CLUSTER}/w-1-2026-10-13__08_00_00.db', f'history/{CLUSTER}/w-2-2026-10-13__08_00_00.db']
    async with AsyncClient(transport=ASGITransport(app=app), base_url='http://test') as c:
        resp = await c.post('/api/v1/debug/merge', json={'filenames': paths})

    assert resp.status_code == 404
    assert not (shared / 'merged').exists()


# --- /debug/archives ---


async def test_archives_lists_every_cluster_folder_with_its_path(client, shared):
    mine = _archive(shared, CLUSTER, datetime(2026, 10, 12, tzinfo=UTC).timestamp())
    theirs = _archive(shared, 'index', datetime(2026, 10, 13, tzinfo=UTC).timestamp())

    archives = (await client.get('/api/v1/debug/archives')).json()['archives']

    assert [(a['name'], a['folder'], a['path']) for a in archives] == [
        (os.path.basename(theirs), 'history/index', f'history/index/{os.path.basename(theirs)}'),
        (os.path.basename(mine), f'history/{CLUSTER}', f'history/{CLUSTER}/{os.path.basename(mine)}'),
    ]
    assert archives[0]['cluster'] == 'index'


async def test_the_archive_download_route_is_gone(client, shared):
    archive = _archive(shared, CLUSTER, datetime(2026, 10, 12, tzinfo=UTC).timestamp())

    resp = await client.get(f'/api/v1/debug/archives/{os.path.basename(archive)}')

    # No route serves it any more; the page catch-all answers instead.
    assert resp.status_code != 200
    assert b'archived bytes' not in resp.content


# --- /debug/trace ---


async def test_trace_with_history_searches_the_sealed_files_of_its_cluster(tmp_path, shared):
    _sealed(shared, CLUSTER, 'w-2-2026-10-13__08_00_00.db', worker='w-2')
    _sealed(shared, 'index', 'w-9-2026-10-13__08_00_00.db', worker='w-9')
    app, recorder = _app(tmp_path)
    recorder.cross_trace = AsyncMock(side_effect=AssertionError('the own trace must not run'))

    async with AsyncClient(transport=ASGITransport(app=app), base_url='http://test') as c:
        resp = await c.get('/api/v1/debug/trace', params={'partition': 3, 'offset': 77, 'history': 1})

    assert resp.status_code == 200
    assert [(row['worker_name'], row['offset']) for row in resp.json()] == [('w-2', 77)]


async def test_trace_with_history_on_a_memory_only_worker_finds_nothing(tmp_path, shared):
    _sealed(shared, CLUSTER, 'w-2-2026-10-13__08_00_00.db', worker='w-2')
    app, _recorder = _app(tmp_path, memory_only=True)

    async with AsyncClient(transport=ASGITransport(app=app), base_url='http://test') as c:
        resp = await c.get('/api/v1/debug/trace', params={'partition': 3, 'offset': 77, 'history': 1})

    assert resp.json() == []


async def test_shared_paths_are_resolved_off_the_ui_loop(tmp_path, storage, shared, monkeypatch):
    """``shared.dir`` is a network mount by design: resolving a path there
    (realpath, an lstat per component) must never run on the loop that also
    answers /healthz and /readyz."""
    import threading
    from pathlib import Path as RealPath

    first = _sealed(shared, CLUSTER, 'w-1-2026-10-13__08_00_00.db')
    second = _sealed(shared, CLUSTER, 'w-2-2026-10-13__08_00_00.db', worker='w-2')
    loop_thread = threading.get_ident()
    on_loop: list[str] = []
    real_resolve = RealPath.resolve

    def watched_resolve(self, strict=False):
        if threading.get_ident() == loop_thread:
            on_loop.append(str(self))
        return real_resolve(self, strict=strict)

    monkeypatch.setattr(RealPath, 'resolve', watched_resolve)
    app, _recorder = _app(tmp_path)
    paths = [f'history/{CLUSTER}/{os.path.basename(first)}', f'history/{CLUSTER}/{os.path.basename(second)}']
    async with AsyncClient(transport=ASGITransport(app=app), base_url='http://test') as c:
        await c.get(f'/api/v1/debug/download/{paths[0]}')
        await c.get('/api/v1/debug/archives')
        await c.post('/api/v1/debug/merge', json={'filenames': paths})

    assert on_loop == []
