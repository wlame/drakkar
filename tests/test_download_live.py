"""``GET /api/v1/debug/download-live``: a consistent snapshot of the live recorder file.

The snapshot is made with the SQLite backup API on its own read-only
connection (never the recorder's reader, which the UI reads through), into
a private temp file in ``storage.dir`` that is gone once the answer starts.
"""

from __future__ import annotations

import contextlib
import os
import sqlite3
from unittest.mock import MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

from drakkar.config import DrakkarConfig
from drakkar.recorder import EventRecorder
from drakkar.recorder.schema import SCHEMA_EVENTS
from drakkar.uiserver import routes_debug
from drakkar.uiserver.server import create_ui_app
from tests.conftest import make_ui_config

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
    config = make_ui_config(enabled=True, port=8080)
    recorder = EventRecorder(
        config,
        worker_name='w-1',
        cluster_name='search',
        storage_dir='' if memory_only else str(tmp_path / 'storage'),
        shared_dir=str(tmp_path / 'shared'),
    )
    if not memory_only:
        recorder._db_path = str(tmp_path / 'storage' / LIVE_NAME)
    drakkar_app = MagicMock()
    drakkar_app._config = DrakkarConfig(sources={'kafka': {'enabled': True}})
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


async def test_memory_only_answers_404(tmp_path):
    client, _recorder = _client(tmp_path, memory_only=True)
    async with client:
        resp = await client.get('/api/v1/debug/download-live')

    assert resp.status_code == 404
    assert resp.json() == {'error': 'No live recorder file on this worker'}


async def test_no_live_file_yet_answers_404(tmp_path, storage):
    client, _recorder = _client(tmp_path)
    async with client:
        resp = await client.get('/api/v1/debug/download-live')

    assert resp.status_code == 404
    assert not os.listdir(storage)
