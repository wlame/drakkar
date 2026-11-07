"""``GET /api/v1/debug/download-live``: a consistent snapshot of the live recorder file or the memory store.

The file snapshot is made with the SQLite backup API on its own read-only
connection (never the recorder's reader, which the UI reads through), into
a private temp file in ``storage.dir`` that is gone once the answer starts.
The memory snapshot goes through the open guard of the memory store: a
stepped backup into a temp file in ``storage.dir`` when it is set, a copy
in RAM otherwise.
"""

from __future__ import annotations

import asyncio
import contextlib
import gzip
import io
import os
import re
import sqlite3
import threading
import time
from unittest.mock import MagicMock

import pytest
from httpx import ASGITransport, AsyncClient
from structlog.testing import capture_logs

from drakkar.cache.engine import CacheEngine
from drakkar.cache.sql import SCHEMA_CACHE_ENTRIES
from drakkar.config import CacheConfig, DrakkarConfig, RecorderConfig, UIConfig
from drakkar.recorder import EventRecorder, memstore
from drakkar.recorder.schema import SCHEMA_EVENTS
from drakkar.uiserver import routes_debug, streaming
from drakkar.uiserver.server import create_ui_app
from tests.conftest import kafka_connections

LIVE_NAME = 'w-1-2026-10-14__09_00_00.db'


def _live_file(path, events: int) -> sqlite3.Connection:
    """A live recorder file in WAL mode; the returned writer keeps it open, like the recorder."""
    writer = sqlite3.connect(str(path), check_same_thread=False)
    writer.execute('PRAGMA journal_mode=WAL')
    writer.executescript(SCHEMA_EVENTS)
    for i in range(events):
        writer.execute('INSERT INTO events (ts, dt, event) VALUES (?, ?, ?)', (1_760_000_000.0 + i, '', 'consumed'))
    writer.commit()
    return writer


def _client(tmp_path, *, memory_only: bool = False) -> tuple[AsyncClient, EventRecorder]:
    config = UIConfig(enabled=True)
    recorder = EventRecorder(
        RecorderConfig(),
        worker_name='w-1',
        cluster_name='search',
        persistence='none' if memory_only else 'sqlite',
        storage_dir='' if memory_only else str(tmp_path / 'storage'),
        shared_dir=str(tmp_path / 'shared'),
    )
    if not memory_only:
        recorder._db_path = str(tmp_path / 'storage' / LIVE_NAME)
    drakkar_app = MagicMock()
    drakkar_app._config = DrakkarConfig(connections=kafka_connections(), sources={'kafka': {'enabled': True}})
    drakkar_app._config.ui.release.enabled = False
    app = create_ui_app(config, recorder, drakkar_app)
    return AsyncClient(transport=ASGITransport(app=app), base_url='http://test'), recorder


@pytest.fixture
def storage(tmp_path):
    folder = tmp_path / 'storage'
    folder.mkdir()
    return folder


async def test_the_snapshot_holds_the_committed_rows_and_leaves_no_temp_file(tmp_path, storage):
    writer = _live_file(storage / LIVE_NAME, events=3)
    client, _recorder = _client(tmp_path)
    try:
        async with client:
            resp = await client.get('/api/v1/debug/download-live')
    finally:
        writer.close()

    assert resp.status_code == 200
    assert resp.headers['content-type'] == 'application/x-sqlite3'
    assert resp.headers['content-disposition'] == f'attachment; filename="{LIVE_NAME}"'
    assert resp.headers['cache-control'] == 'no-store, private'
    snapshot = tmp_path / 'snapshot.db'
    snapshot.write_bytes(resp.content)
    # A rollback-journal file: it opens on its own, with no -wal beside it.
    assert resp.content[18:20] == b'\x01\x01'
    with contextlib.closing(sqlite3.connect(f'file:{snapshot}?mode=ro', uri=True)) as conn:
        assert conn.execute('SELECT COUNT(*) FROM events').fetchone() == (3,)
    assert sorted(p.name for p in storage.iterdir() if p.name.startswith('.')) == []


async def test_the_writer_keeps_writing_while_the_snapshot_is_taken(tmp_path, storage, monkeypatch):
    writer = _live_file(storage / LIVE_NAME, events=1)
    real_backup = routes_debug._backup_into

    def backup_while_writing(source, target):
        # WAL readers never block the writer: a write lands mid-backup.
        writer.execute('INSERT INTO events (ts, dt, event) VALUES (1.0, "", "consumed")')
        writer.commit()
        real_backup(source, target)

    monkeypatch.setattr(routes_debug, '_backup_into', backup_while_writing)
    client, _recorder = _client(tmp_path)
    try:
        async with client:
            resp = await client.get('/api/v1/debug/download-live')
    finally:
        writer.close()

    assert resp.status_code == 200


async def test_a_failed_backup_removes_its_temp_file(tmp_path, storage, monkeypatch):
    writer = _live_file(storage / LIVE_NAME, events=1)

    def broken(source, target):
        raise sqlite3.OperationalError('disk I/O error')

    monkeypatch.setattr(routes_debug, '_backup_into', broken)
    client, _recorder = _client(tmp_path)
    try:
        async with client:
            resp = await client.get('/api/v1/debug/download-live')
    finally:
        writer.close()

    assert resp.status_code == 500
    assert sorted(p.name for p in storage.iterdir() if p.name.startswith('.')) == []


async def test_the_route_never_uses_the_recorders_reader(tmp_path, storage):
    writer = _live_file(storage / LIVE_NAME, events=1)
    client, recorder = _client(tmp_path)
    recorder._reader_db = MagicMock(side_effect=AssertionError('the reader was used'))
    try:
        async with client:
            resp = await client.get('/api/v1/debug/download-live')
    finally:
        writer.close()

    assert resp.status_code == 200
    assert recorder._reader_db.method_calls == []


MEMORY_NAME_RE = re.compile(r'^w-1-memory-\d{4}-\d{2}-\d{2}__\d{2}_\d{2}_\d{2}\.db$')


def _consumed(offset: int) -> dict:
    return {'ts': time.time(), 'event': 'consumed', 'partition': 0, 'offset': offset}


@pytest.fixture
async def started(tmp_path):
    """Start recorders built in the test (``none`` or ``sqlite``) and stop them at the end."""
    recorders: list[EventRecorder] = []

    async def start(persistence: str, *, storage_dir: str = '', events: int = 3) -> EventRecorder:
        config = RecorderConfig(flush_interval='60s')
        rec = EventRecorder(
            config,
            worker_name='w-1',
            cluster_name='search',
            persistence=persistence,
            storage_dir=storage_dir,
        )
        await rec.start()
        recorders.append(rec)
        await rec.write_config(_drakkar_config())
        for offset in range(events):
            rec._record(_consumed(offset))
        await rec.flush()
        return rec

    yield start
    for rec in recorders:
        await rec.stop()


def _drakkar_config() -> DrakkarConfig:
    config = DrakkarConfig(connections=kafka_connections(), sources={'kafka': {'enabled': True}})
    config.ui.release.enabled = False
    return config


def _client_of(rec: EventRecorder) -> AsyncClient:
    drakkar_app = MagicMock()
    drakkar_app._config = _drakkar_config()
    app = create_ui_app(UIConfig(), rec, drakkar_app)
    return AsyncClient(transport=ASGITransport(app=app), base_url='http://test')


def _open_snapshot(data: bytes, tmp_path) -> sqlite3.Connection:
    path = tmp_path / 'downloaded.db'
    path.write_bytes(data)
    return sqlite3.connect(f'file:{path}?mode=ro', uri=True)


def _assert_recorder_copy(conn: sqlite3.Connection, events: int) -> None:
    assert conn.execute('SELECT COUNT(*) FROM events').fetchone() == (events,)
    assert conn.execute('SELECT worker_name, cluster_name FROM worker_config').fetchone() == ('w-1', 'search')
    assert conn.execute('PRAGMA journal_mode').fetchone() == ('delete',)


@pytest.mark.parametrize('persistence', ['sqlite', 'none'])
async def test_download_live_default_source(tmp_path, storage, started, persistence):
    rec = await started(persistence, storage_dir=str(storage) if persistence == 'sqlite' else '', events=2)

    async with _client_of(rec) as client:
        resp = await client.get('/api/v1/debug/download-live')

    assert resp.status_code == 200
    disposition = resp.headers['content-disposition']
    if persistence == 'sqlite':
        assert disposition == f'attachment; filename="{os.path.basename(rec.db_path)}"'
    else:
        name = disposition.removeprefix('attachment; filename="').removesuffix('"')
        assert MEMORY_NAME_RE.fullmatch(name)
    assert resp.headers['content-type'] == 'application/x-sqlite3'
    assert resp.headers['cache-control'] == 'no-store, private'
    assert resp.headers['content-length'] == str(len(resp.content))
    with contextlib.closing(_open_snapshot(resp.content, tmp_path)) as conn:
        _assert_recorder_copy(conn, events=2)


async def test_download_live_source_memory_on_a_file_worker_serves_the_memory_store(tmp_path, storage, started):
    rec = await started('sqlite', storage_dir=str(storage), events=2)

    async with _client_of(rec) as client:
        resp = await client.get('/api/v1/debug/download-live', params={'source': 'memory'})

    assert resp.status_code == 200
    assert 'w-1-memory-' in resp.headers['content-disposition']
    with contextlib.closing(_open_snapshot(resp.content, tmp_path)) as conn:
        _assert_recorder_copy(conn, events=2)
    assert sorted(p.name for p in storage.iterdir() if p.name.startswith('.')) == []


async def test_download_live_source_file_without_a_file_answers_404(started):
    rec = await started('none')

    async with _client_of(rec) as client:
        resp = await client.get('/api/v1/debug/download-live', params={'source': 'file'})

    assert resp.status_code == 404
    assert resp.json() == {'error': 'No live recorder file on this worker'}


async def test_download_live_source_memory_without_a_store_answers_404(tmp_path):
    # Never started: the recorder has no memory store.
    client, _recorder = _client(tmp_path, memory_only=True)
    async with client:
        default = await client.get('/api/v1/debug/download-live')
        memory = await client.get('/api/v1/debug/download-live', params={'source': 'memory'})

    assert (default.status_code, memory.status_code) == (404, 404)
    assert default.json() == memory.json() == {'error': 'No memory store on this worker'}


async def test_download_live_unknown_source_answers_400(tmp_path):
    client, _recorder = _client(tmp_path, memory_only=True)
    async with client:
        resp = await client.get('/api/v1/debug/download-live', params={'source': 'disk'})

    assert resp.status_code == 400
    assert resp.json() == {'error': 'source must be memory, file or cache'}


async def test_stepped_memory_snapshot_with_storage_dir(tmp_path, storage, started):
    rec = await started('none', events=4)

    with rec.open_memory_reader() as conn:
        handle, name = routes_debug._snapshot_memory_stepped(conn, str(storage), 'w-1')
    with handle:
        data = handle.read()

    assert MEMORY_NAME_RE.fullmatch(name)
    with contextlib.closing(_open_snapshot(data, tmp_path)) as copy:
        _assert_recorder_copy(copy, events=4)
    assert os.listdir(storage) == []


async def test_serialized_memory_snapshot_without_storage_dir(tmp_path, started):
    rec = await started('none', events=4)

    with rec.open_memory_reader() as conn:
        data = routes_debug._snapshot_memory_serialized(conn)
        # The read transaction is over: the connection is free again.
        assert not conn.in_transaction

    with contextlib.closing(_open_snapshot(data, tmp_path)) as copy:
        _assert_recorder_copy(copy, events=4)


def _writer_by_name(rec: EventRecorder) -> sqlite3.Connection:
    """A read-write connection to the memory store, as a second writer between backup steps."""
    conn = sqlite3.connect(rec.memory_store.uri, uri=True, check_same_thread=False)
    conn.execute(f'PRAGMA busy_timeout = {memstore.BUSY_TIMEOUT_MS}')
    return conn


def _write_and_evict(conn: sqlite3.Connection) -> None:
    """Append the next id and delete the oldest one, as a memory stage and an eviction would."""
    low, high = conn.execute('SELECT MIN(id), MAX(id) FROM events').fetchone()
    conn.execute('INSERT INTO events (id, ts, dt, event) VALUES (?, ?, ?, ?)', (high + 1, time.time(), '', 'consumed'))
    conn.execute('DELETE FROM events WHERE id = ?', (low,))
    conn.commit()


async def test_stepped_snapshot_restarts_under_eviction_and_stays_consistent(tmp_path, storage, started, monkeypatch):
    rec = await started('none', events=50)
    monkeypatch.setattr(memstore, 'SNAPSHOT_STEP_PAGES', 1)
    writer = _writer_by_name(rec)
    real_check = routes_debug._snapshot_step_check
    steps: list[int] = []

    def check_with_writes(deadline):
        check = real_check(deadline)

        def step(status, remaining, total):
            steps.append(remaining)
            # A write plus an eviction between the second and third steps,
            # and between the third and fourth: each restarts the copy.
            if len(steps) in (2, 3):
                _write_and_evict(writer)
            check(status, remaining, total)

        return step

    monkeypatch.setattr(routes_debug, '_snapshot_step_check', check_with_writes)
    try:
        with rec.open_memory_reader() as conn:
            handle, _name = routes_debug._snapshot_memory_stepped(conn, str(storage), 'w-1')
    finally:
        writer.close()
    with handle:
        data = handle.read()

    assert len(steps) > steps[0] + 1  # the copy started again at least once
    with contextlib.closing(_open_snapshot(data, tmp_path)) as copy:
        low, high, count = copy.execute('SELECT MIN(id), MAX(id), COUNT(*) FROM events').fetchone()
        assert (low, high, count) == (3, 52, 50)
        assert copy.execute('PRAGMA integrity_check').fetchone() == ('ok',)


async def test_stepped_snapshot_over_limit_answers_500(tmp_path, storage, started, monkeypatch):
    rec = await started('none', storage_dir=str(storage), events=50)
    monkeypatch.setattr(memstore, 'SNAPSHOT_STEP_PAGES', 1)
    monkeypatch.setattr(memstore, 'SNAPSHOT_LIMIT_SECONDS', 0.1)
    writer = _writer_by_name(rec)
    real_check = routes_debug._snapshot_step_check

    def check_with_a_write_every_step(deadline):
        check = real_check(deadline)

        def step(status, remaining, total):
            _write_and_evict(writer)
            check(status, remaining, total)

        return step

    monkeypatch.setattr(routes_debug, '_snapshot_step_check', check_with_a_write_every_step)
    try:
        async with _client_of(rec) as client:
            resp = await client.get('/api/v1/debug/download-live', params={'source': 'memory'})
    finally:
        writer.close()

    assert resp.status_code == 500
    assert resp.json() == {'error': 'Snapshot failed: the memory store copy did not finish in 0.1 s'}
    assert os.listdir(storage) == []


async def test_download_live_before_the_first_file_serves_the_memory_store(tmp_path, storage, started):
    rec = await started('sqlite', storage_dir=str(storage), events=2)
    # A file worker whose live file does not exist (yet); the real path is
    # put back for the stop, which seals it.
    real_path, rec._db_path = rec._db_path, str(storage / LIVE_NAME)
    try:
        async with _client_of(rec) as client:
            default = await client.get('/api/v1/debug/download-live')
            file = await client.get('/api/v1/debug/download-live', params={'source': 'file'})
    finally:
        rec._db_path = real_path

    assert default.status_code == 200
    assert 'w-1-memory-' in default.headers['content-disposition']
    with contextlib.closing(_open_snapshot(default.content, tmp_path)) as conn:
        _assert_recorder_copy(conn, events=2)
    assert file.status_code == 404
    assert file.json() == {'error': 'No live recorder file on this worker'}


async def test_memory_download_falls_back_to_ram_when_the_storage_folder_is_missing(tmp_path, started):
    missing = tmp_path / 'not-created'
    rec = await started('none', storage_dir=str(missing), events=3)

    async with _client_of(rec) as client:
        resp = await client.get('/api/v1/debug/download-live')

    assert resp.status_code == 200
    name = resp.headers['content-disposition'].removeprefix('attachment; filename="').removesuffix('"')
    assert MEMORY_NAME_RE.fullmatch(name)
    with contextlib.closing(_open_snapshot(resp.content, tmp_path)) as conn:
        _assert_recorder_copy(conn, events=3)
    assert not missing.exists()


async def test_memory_download_in_ram_logs_why(tmp_path, started):
    missing = tmp_path / 'not-created'
    unusable = await started('none', storage_dir=str(missing), events=1)
    unset = await started('none', events=1)

    with capture_logs() as cap:
        for rec in (unusable, unset):
            async with _client_of(rec) as client:
                assert (await client.get('/api/v1/debug/download-live')).status_code == 200

    lines = [e for e in cap if e['event'] == 'debug_download_live_in_ram']
    assert [(e['log_level'], e['category'], e['source'], e['reason']) for e in lines] == [
        ('info', 'debug', 'memory', 'storage_dir_unusable'),
        ('info', 'debug', 'memory', 'storage_dir_unset'),
    ]


async def test_concurrent_memory_downloads_never_copy_at_the_same_time(tmp_path, started, monkeypatch):
    rec = await started('none', events=2)
    real_snapshot = routes_debug._snapshot_memory
    copies_running = 0
    most_at_once = 0
    count_lock = threading.Lock()

    def slow_snapshot(recorder):
        nonlocal copies_running, most_at_once
        with count_lock:
            copies_running += 1
            most_at_once = max(most_at_once, copies_running)
        try:
            # Long enough that two unguarded copies would overlap.
            time.sleep(0.2)
            return real_snapshot(recorder)
        finally:
            with count_lock:
                copies_running -= 1

    monkeypatch.setattr(routes_debug, '_snapshot_memory', slow_snapshot)

    async with _client_of(rec) as client:
        first, second = await asyncio.gather(
            client.get('/api/v1/debug/download-live?source=memory'),
            client.get('/api/v1/debug/download-live?source=memory'),
        )

    assert (first.status_code, second.status_code) == (200, 200)
    assert most_at_once == 1
    for resp in (first, second):
        with contextlib.closing(_open_snapshot(resp.content, tmp_path)) as conn:
            _assert_recorder_copy(conn, events=2)


async def test_memory_download_answers_500_after_waiting_the_limit_for_another_snapshot(started, monkeypatch):
    rec = await started('none', events=1)
    monkeypatch.setattr(memstore, 'SNAPSHOT_LIMIT_SECONDS', 0.1)
    real_snapshot = routes_debug._snapshot_memory
    first_copy_started = threading.Event()
    first_copy_may_end = threading.Event()

    def held_snapshot(recorder):
        first_copy_started.set()
        first_copy_may_end.wait(timeout=10)
        return real_snapshot(recorder)

    monkeypatch.setattr(routes_debug, '_snapshot_memory', held_snapshot)

    async with _client_of(rec) as client:
        first = asyncio.create_task(client.get('/api/v1/debug/download-live?source=memory'))
        assert await asyncio.to_thread(first_copy_started.wait, 5)
        with capture_logs() as cap:
            second = await client.get('/api/v1/debug/download-live?source=memory')
        first_copy_may_end.set()
        first_resp = await first

    assert (second.status_code, second.json()) == (
        500,
        {'error': 'Snapshot failed: another memory snapshot is still running'},
    )
    failed = [e for e in cap if e['event'] == 'debug_download_live_failed']
    assert [(e['source'], e['error']) for e in failed] == [('memory', 'another memory snapshot is still running')]
    assert first_resp.status_code == 200


async def test_a_memory_download_whose_client_left_keeps_the_slot_until_its_copy_ends(started, monkeypatch):
    rec = await started('none', events=1)
    real_snapshot = routes_debug._snapshot_memory
    first_copy_started = threading.Event()
    first_copy_may_end = threading.Event()
    copies_running = 0
    most_at_once = 0
    count_lock = threading.Lock()
    calls = 0

    def counted_snapshot(recorder):
        nonlocal copies_running, most_at_once, calls
        with count_lock:
            calls += 1
            is_first = calls == 1
            copies_running += 1
            most_at_once = max(most_at_once, copies_running)
        try:
            if is_first:
                first_copy_started.set()
                first_copy_may_end.wait(timeout=10)
            return real_snapshot(recorder)
        finally:
            with count_lock:
                copies_running -= 1

    monkeypatch.setattr(routes_debug, '_snapshot_memory', counted_snapshot)

    async with _client_of(rec) as client:
        first = asyncio.create_task(client.get('/api/v1/debug/download-live?source=memory'))
        assert await asyncio.to_thread(first_copy_started.wait, 5)
        # The client of the first download goes away mid-copy.
        first.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await first
        second = asyncio.create_task(client.get('/api/v1/debug/download-live?source=memory'))
        await asyncio.sleep(0.05)
        first_copy_may_end.set()
        second_resp = await second

    assert second_resp.status_code == 200
    assert most_at_once == 1


# --- compress=gzip (contract v1.30) ---


async def test_download_live_compressed_file_snapshot_is_a_gzip_stream_of_the_copy(tmp_path, storage, started):
    rec = await started('sqlite', storage_dir=str(storage), events=3)

    async with _client_of(rec) as client:
        resp = await client.get('/api/v1/debug/download-live', params={'source': 'file', 'compress': 'gzip'})

    assert resp.status_code == 200
    assert resp.headers['content-type'] == 'application/gzip'
    assert resp.headers['content-disposition'] == f'attachment; filename="{os.path.basename(rec.db_path)}.gz"'
    assert resp.headers['cache-control'] == 'no-store, private'
    assert 'content-length' not in resp.headers
    with contextlib.closing(_open_snapshot(gzip.decompress(resp.content), tmp_path)) as conn:
        _assert_recorder_copy(conn, events=3)
    assert [p.name for p in storage.iterdir() if p.name.startswith('.')] == []


async def test_download_live_compressed_memory_snapshot(tmp_path, started):
    rec = await started('none', events=2)

    async with _client_of(rec) as client:
        resp = await client.get('/api/v1/debug/download-live', params={'source': 'memory', 'compress': 'gzip'})

    assert resp.status_code == 200
    name = resp.headers['content-disposition'].removeprefix('attachment; filename="').removesuffix('.gz"')
    assert MEMORY_NAME_RE.fullmatch(name)
    with contextlib.closing(_open_snapshot(gzip.decompress(resp.content), tmp_path)) as conn:
        _assert_recorder_copy(conn, events=2)


@pytest.mark.parametrize('compress', ['zip', '', 'GZIP'])
async def test_download_live_other_compress_answers_400_before_the_source_check(tmp_path, compress):
    client, _recorder = _client(tmp_path, memory_only=True)
    async with client:
        resp = await client.get('/api/v1/debug/download-live', params={'source': 'disk', 'compress': compress})

    assert (resp.status_code, resp.json()) == (400, {'error': 'compress must be gzip'})


def test_gzip_chunks_closes_the_file_when_the_client_leaves_mid_stream(monkeypatch):
    monkeypatch.setattr(streaming, 'CHUNK_BYTES', 4)
    handle = io.BytesIO(b'0123456789abcdef')
    chunks = streaming.gzip_chunks(handle)

    next(chunks)
    chunks.close()

    assert handle.closed


def test_gzip_chunks_of_a_whole_file_decompress_to_its_bytes(monkeypatch):
    monkeypatch.setattr(streaming, 'CHUNK_BYTES', 7)
    data = os.urandom(100) + b'z' * 1000
    handle = io.BytesIO(data)

    assert gzip.decompress(b''.join(streaming.gzip_chunks(handle))) == data
    assert handle.closed


# --- source=cache (contract v1.30) ---

CACHE_NAME_RE = re.compile(r'^w-1-cache-\d{4}-\d{2}-\d{2}__\d{2}_\d{2}_\d{2}\.db$')


def _cache_engine(storage, rows: int = 3, *, create: bool = True) -> CacheEngine:
    """A cache engine that is never started, pointing at a cache file in WAL mode written here."""
    engine = CacheEngine(CacheConfig(enabled=True), str(storage), 'w-1', 'search')
    path = storage / 'w-1-cache.db'
    if create:
        conn = sqlite3.connect(str(path))
        conn.execute('PRAGMA journal_mode=WAL')
        conn.executescript(SCHEMA_CACHE_ENTRIES)
        conn.executemany(
            'INSERT INTO cache_entries VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
            [(f'k{i}', 'local', 'v', 1, 0, 0, None, 'w-1') for i in range(rows)],
        )
        conn.commit()
        conn.close()
    engine._db_path = str(path)
    return engine


def _client_with_cache(rec: EventRecorder, engine: CacheEngine | None) -> AsyncClient:
    drakkar_app = MagicMock()
    drakkar_app._config = _drakkar_config()
    drakkar_app.cache_engine = engine
    app = create_ui_app(UIConfig(), rec, drakkar_app)
    return AsyncClient(transport=ASGITransport(app=app), base_url='http://test')


async def test_download_live_cache_serves_a_copy_of_the_cache_file(tmp_path, storage, started):
    rec = await started('none', events=1)

    async with _client_with_cache(rec, _cache_engine(storage, rows=3)) as client:
        resp = await client.get('/api/v1/debug/download-live', params={'source': 'cache'})

    assert resp.status_code == 200
    assert resp.headers['content-type'] == 'application/x-sqlite3'
    assert resp.headers['cache-control'] == 'no-store, private'
    name = resp.headers['content-disposition'].removeprefix('attachment; filename="').removesuffix('"')
    assert CACHE_NAME_RE.fullmatch(name)
    with contextlib.closing(_open_snapshot(resp.content, tmp_path)) as conn:
        assert conn.execute('SELECT COUNT(*) FROM cache_entries').fetchone() == (3,)
        assert conn.execute('PRAGMA journal_mode').fetchone() == ('delete',)
    assert [p.name for p in storage.iterdir() if p.name.startswith('.')] == []


async def test_download_live_cache_compressed(tmp_path, storage, started):
    rec = await started('none', events=1)

    async with _client_with_cache(rec, _cache_engine(storage, rows=2)) as client:
        resp = await client.get('/api/v1/debug/download-live', params={'source': 'cache', 'compress': 'gzip'})

    assert resp.status_code == 200
    assert resp.headers['content-disposition'].endswith('.db.gz"')
    with contextlib.closing(_open_snapshot(gzip.decompress(resp.content), tmp_path)) as conn:
        assert conn.execute('SELECT COUNT(*) FROM cache_entries').fetchone() == (2,)


@pytest.mark.parametrize('case', ['no_engine', 'no_file_yet', 'memory_only_cache'])
async def test_download_live_cache_without_a_cache_file_answers_404(storage, started, case):
    rec = await started('none', events=1)
    engines = {
        'no_engine': None,
        'no_file_yet': _cache_engine(storage, create=False),
        'memory_only_cache': CacheEngine(CacheConfig(enabled=True), '', 'w-1', 'search'),
    }

    async with _client_with_cache(rec, engines[case]) as client:
        resp = await client.get('/api/v1/debug/download-live', params={'source': 'cache'})

    assert (resp.status_code, resp.json()) == (404, {'error': 'No cache on this worker'})


async def test_download_live_cache_copy_that_fails_answers_500(storage, started, monkeypatch):
    rec = await started('none', events=1)

    def failing_copy(_source, _target):
        raise sqlite3.DatabaseError('file is not a database')

    monkeypatch.setattr(routes_debug, '_vacuum_into', failing_copy)

    async with _client_with_cache(rec, _cache_engine(storage)) as client:
        with capture_logs() as cap:
            resp = await client.get('/api/v1/debug/download-live', params={'source': 'cache'})

    assert (resp.status_code, resp.json()) == (500, {'error': 'Snapshot failed: file is not a database'})
    failed = [e for e in cap if e['event'] == 'debug_download_live_failed']
    assert [(e['source'], e['error']) for e in failed] == [('cache', 'file is not a database')]


async def test_a_cache_copy_past_its_limit_answers_500_and_leaves_no_temp_file(storage, started, monkeypatch):
    rec = await started('none', events=1)
    monkeypatch.setattr(routes_debug, 'CACHE_SNAPSHOT_LIMIT_SECONDS', 0.0)

    async with _client_with_cache(rec, _cache_engine(storage, rows=3000)) as client:
        resp = await client.get('/api/v1/debug/download-live', params={'source': 'cache'})

    assert (resp.status_code, resp.json()) == (500, {'error': 'Snapshot failed: the cache copy did not finish in 0 s'})
    assert [p.name for p in storage.iterdir() if p.name.startswith('.')] == []


async def test_the_cache_writer_commits_while_the_copy_runs_and_the_copy_stays_consistent(
    tmp_path, storage, started, monkeypatch
):
    rec = await started('none', events=1)
    engine = _cache_engine(storage, rows=3000)
    real_check = routes_debug._cache_copy_check
    writes: list[str] = []

    def check_that_writes(deadline):
        check = real_check(deadline)

        def step() -> int:
            if not writes:
                # The cache engine's writer, on its own connection, with no busy wait.
                writer = sqlite3.connect(engine.db_path, timeout=0)
                try:
                    writer.execute("INSERT INTO cache_entries VALUES ('late', 'local', 'v', 1, 0, 0, NULL, 'w-1')")
                    writer.commit()
                    writes.append('committed')
                finally:
                    writer.close()
            return check()

        return step

    monkeypatch.setattr(routes_debug, '_cache_copy_check', check_that_writes)

    async with _client_with_cache(rec, engine) as client:
        resp = await client.get('/api/v1/debug/download-live', params={'source': 'cache'})

    assert resp.status_code == 200
    assert writes == ['committed']
    with contextlib.closing(_open_snapshot(resp.content, tmp_path)) as conn:
        assert conn.execute('SELECT COUNT(*) FROM cache_entries').fetchone() == (3000,)
    with contextlib.closing(sqlite3.connect(engine.db_path)) as live:
        assert live.execute('SELECT COUNT(*) FROM cache_entries').fetchone() == (3001,)


def _hold_cache_copies(monkeypatch) -> tuple[threading.Event, threading.Event]:
    """Make each cache copy wait for ``may_end``; ``started`` is set when the first one runs."""
    real_copy = routes_debug._snapshot_cache_file
    started_event = threading.Event()
    may_end = threading.Event()

    def held_copy(cache_path, worker_name):
        started_event.set()
        may_end.wait(timeout=10)
        return real_copy(cache_path, worker_name)

    monkeypatch.setattr(routes_debug, '_snapshot_cache_file', held_copy)
    return started_event, may_end


async def test_a_memory_download_runs_while_a_cache_copy_runs(storage, started, monkeypatch):
    rec = await started('none', events=2)
    copy_started, copy_may_end = _hold_cache_copies(monkeypatch)

    async with _client_with_cache(rec, _cache_engine(storage)) as client:
        cache = asyncio.create_task(client.get('/api/v1/debug/download-live?source=cache'))
        assert await asyncio.to_thread(copy_started.wait, 5)
        memory = await client.get('/api/v1/debug/download-live?source=memory')
        copy_may_end.set()
        cache_resp = await cache

    assert (memory.status_code, cache_resp.status_code) == (200, 200)


async def test_a_second_cache_download_answers_500_while_the_first_copy_holds_its_slot(storage, started, monkeypatch):
    rec = await started('none', events=1)
    monkeypatch.setattr(memstore, 'SNAPSHOT_LIMIT_SECONDS', 0.1)
    copy_started, copy_may_end = _hold_cache_copies(monkeypatch)

    async with _client_with_cache(rec, _cache_engine(storage)) as client:
        first = asyncio.create_task(client.get('/api/v1/debug/download-live?source=cache'))
        assert await asyncio.to_thread(copy_started.wait, 5)
        second = await client.get('/api/v1/debug/download-live?source=cache')
        copy_may_end.set()
        first_resp = await first

    assert (second.status_code, second.json()) == (
        500,
        {'error': 'Snapshot failed: another cache snapshot is still running'},
    )
    assert first_resp.status_code == 200


async def test_open_for_stream_closes_the_file_of_a_request_cancelled_during_the_open(tmp_path, monkeypatch):
    opened = io.BytesIO(b'data')
    open_started = threading.Event()
    open_may_end = threading.Event()

    def slow_open(_path):
        open_started.set()
        open_may_end.wait(timeout=10)
        return opened

    monkeypatch.setattr(routes_debug, '_open_binary', slow_open)
    task = asyncio.create_task(routes_debug._open_for_stream(tmp_path / 'f.db'))
    assert await asyncio.to_thread(open_started.wait, 5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    open_may_end.set()
    for _ in range(100):
        if opened.closed:
            break
        await asyncio.sleep(0.01)

    assert opened.closed


def _recorded_snapshot_holders(monkeypatch) -> list[streaming.OpenDownloads]:
    """Record the holder of snapshots still being sent that the debug routes build."""
    seen: list[streaming.OpenDownloads] = []

    class Recorded(streaming.OpenDownloads):
        def __init__(self, **kwargs) -> None:
            super().__init__(**kwargs)
            seen.append(self)

    monkeypatch.setattr(routes_debug, 'OpenDownloads', Recorded)
    return seen


@pytest.mark.parametrize(
    ('fill', 'error'),
    [
        ('bytes', 'snapshots still being sent hold 2 GiB on this worker; wait for one to end'),
        ('count', '16 snapshots are still being sent on this worker; wait for one to end'),
    ],
)
async def test_download_live_answers_429_while_snapshots_being_sent_fill_a_bound(
    storage, started, monkeypatch, fill, error
):
    rec = await started('sqlite', storage_dir=str(storage), events=2)
    seen = _recorded_snapshot_holders(monkeypatch)

    async with _client_of(rec) as client:
        done = await client.get('/api/v1/debug/download-live')
        (open_snapshots,) = seen
        # The whole body was sent: it left the count.
        after_send = (open_snapshots.bytes, open_snapshots.count)
        if fill == 'bytes':
            open_snapshots.hold(routes_debug.SNAPSHOT_OPEN_MAX_BYTES)
        else:
            for _ in range(routes_debug.SNAPSHOT_OPEN_MAX_COUNT):
                open_snapshots.hold(0)
        full = await client.get('/api/v1/debug/download-live', params={'source': 'cache'})
        bad_source = await client.get('/api/v1/debug/download-live', params={'source': 'disk'})

    assert done.status_code == 200
    assert after_send == (0, 0)
    assert (full.status_code, full.json()) == (429, {'error': error})
    assert full.headers['retry-after'] == '5'
    # The source check comes first.
    assert bad_source.status_code == 400


async def test_a_second_file_download_answers_500_while_the_first_copy_holds_its_slot(storage, started, monkeypatch):
    rec = await started('sqlite', storage_dir=str(storage), events=1)
    monkeypatch.setattr(memstore, 'SNAPSHOT_LIMIT_SECONDS', 0.1)
    real_copy = routes_debug._snapshot_live_file
    copy_started = threading.Event()
    copy_may_end = threading.Event()

    def held_copy(live_path):
        copy_started.set()
        copy_may_end.wait(timeout=10)
        return real_copy(live_path)

    monkeypatch.setattr(routes_debug, '_snapshot_live_file', held_copy)
    async with _client_of(rec) as client:
        first = asyncio.create_task(client.get('/api/v1/debug/download-live?source=file'))
        assert await asyncio.to_thread(copy_started.wait, 5)
        second = await client.get('/api/v1/debug/download-live?source=file')
        copy_may_end.set()
        first_resp = await first

    assert (second.status_code, second.json()) == (
        500,
        {'error': 'Snapshot failed: another file snapshot is still running'},
    )
    assert first_resp.status_code == 200


async def test_a_snapshot_client_that_takes_no_chunk_ends_the_stream_and_leaves_the_count(monkeypatch):
    monkeypatch.setattr(routes_debug, 'DOWNLOAD_SEND_IDLE_SECONDS', 0.05)
    handle = io.BytesIO(b'x' * 10)
    open_snapshots = streaming.OpenDownloads(max_bytes=100, max_count=4)
    response = routes_debug._snapshot_response(handle, 'w-1.db', open_snapshots)
    held_while_sending = (open_snapshots.bytes, open_snapshots.count)
    sent: list[dict] = []

    async def stalled_send(message):
        sent.append(message)
        if message['type'] == 'http.response.body':
            await asyncio.sleep(3600)

    async def receive():
        await asyncio.sleep(3600)

    with pytest.raises(Exception):  # noqa: B017 - the server turns the timeout into a disconnect
        await response({'type': 'http', 'asgi': {'spec_version': '2.4'}}, receive, stalled_send)

    assert held_while_sending == (10, 1)
    assert sent[0]['type'] == 'http.response.start'
    assert handle.closed
    assert (open_snapshots.bytes, open_snapshots.count) == (0, 0)
