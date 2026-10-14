"""Sealing building blocks: rollback journal, meta, the move into history, leftovers."""

import errno
import os
import shutil
import sqlite3
import threading
import time
from datetime import UTC, datetime

import aiosqlite
import pytest

from drakkar.recorder import sealing
from drakkar.recorder.history import read_meta
from drakkar.recorder.schema import SCHEMA_EVENTS, SCHEMA_WORKER_CONFIG, SCHEMA_WORKER_STATE
from drakkar.recorder.sealing import (
    SealError,
    find_leftovers,
    own_db_files,
    publish_sealed,
    seal_closed_file,
    seal_connection,
)

SEALED_AT = datetime(2026, 10, 14, 9, 0, 0, tzinfo=UTC)
IDENTITY = {'worker': 'w-1', 'instance': '1111111111111111', 'cluster': 'search'}


def _make_wal_file(path, events: int) -> sqlite3.Connection:
    """A recorder-shaped file in WAL mode with ``events`` rows; the connection stays open (WAL not checkpointed)."""
    conn = sqlite3.connect(path)
    conn.execute('PRAGMA journal_mode=WAL')
    for schema in (SCHEMA_EVENTS, SCHEMA_WORKER_CONFIG, SCHEMA_WORKER_STATE):
        conn.executescript(schema)
    for i in range(events):
        conn.execute('INSERT INTO events (ts, dt, event) VALUES (?, ?, ?)', (1760342400.0 + i, '', 'consumed'))
    conn.commit()
    return conn


def _journal_mode(path) -> str:
    conn = sqlite3.connect(f'file:{path}?mode=ro', uri=True)
    try:
        return conn.execute('PRAGMA journal_mode').fetchone()[0]
    finally:
        conn.close()


# --- seal_connection / seal_closed_file ---


async def test_seal_connection_switches_to_a_rollback_journal_and_closes(tmp_path):
    path = tmp_path / 'w-1-2026-10-14__08_00_00.db'
    _make_wal_file(path, 3).close()
    conn = await aiosqlite.connect(str(path))
    async with conn.execute('PRAGMA journal_mode=WAL'):
        pass

    meta = await seal_connection(conn, db_path=str(path), sealed_at=SEALED_AT, **IDENTITY)

    assert sorted(p.name for p in tmp_path.iterdir()) == ['w-1-2026-10-14__08_00_00.db']
    assert _journal_mode(path) == 'delete'
    assert meta['tables']['events'] == 3
    assert meta['size_bytes'] == path.stat().st_size
    assert meta['worker'] == 'w-1'


async def test_seal_connection_waits_out_a_short_lived_reader(tmp_path, monkeypatch):
    """A reader in this process (the databases page, a trace) blocks the
    journal switch while it is open; the seal retries instead of failing."""
    monkeypatch.setattr(sealing, 'SEAL_RETRY_DELAYS', (0.1, 0.1, 0.2, 0.4))
    path = tmp_path / 'w-1-2026-10-14__08_00_00.db'
    _make_wal_file(path, 2).close()
    conn = await aiosqlite.connect(str(path))
    async with conn.execute('PRAGMA journal_mode=WAL'):
        pass
    reader = sqlite3.connect(f'file:{path}?mode=ro', uri=True, check_same_thread=False)
    reader.execute('SELECT COUNT(*) FROM events').fetchone()
    closer = threading.Timer(0.25, reader.close)
    closer.start()

    meta = await seal_connection(conn, db_path=str(path), sealed_at=SEALED_AT, **IDENTITY)

    closer.join()
    assert _journal_mode(path) == 'delete'
    assert meta['tables']['events'] == 2


async def test_seal_connection_refuses_while_another_connection_holds_the_file(tmp_path, monkeypatch):
    monkeypatch.setattr(sealing, 'SEAL_RETRY_DELAYS', (0.01, 0.01))
    path = tmp_path / 'w-1-2026-10-14__08_00_00.db'
    other = _make_wal_file(path, 1)
    other.execute('BEGIN')
    other.execute('SELECT COUNT(*) FROM events').fetchone()
    conn = await aiosqlite.connect(str(path))

    try:
        with pytest.raises((SealError, sqlite3.OperationalError)):
            await seal_connection(conn, db_path=str(path), sealed_at=SEALED_AT, **IDENTITY)
    finally:
        other.close()


async def test_seal_closed_file_replays_a_crashed_wal(tmp_path):
    crashed = tmp_path / 'source.db'
    writer = _make_wal_file(crashed, 5)
    # Copy the main file and its WAL while the writer still holds them: the
    # same bytes a killed process leaves behind.
    leftover = tmp_path / 'w-1-2026-10-14__07_00_00.db'
    shutil.copyfile(crashed, leftover)
    shutil.copyfile(f'{crashed}-wal', f'{leftover}-wal')
    writer.close()

    meta = await seal_closed_file(str(leftover), sealed_at=SEALED_AT, **IDENTITY)

    assert meta['tables']['events'] == 5
    assert not os.path.exists(f'{leftover}-wal')
    assert _journal_mode(leftover) == 'delete'


async def test_seal_closed_file_never_creates_a_missing_file(tmp_path):
    missing = tmp_path / 'w-1-2026-10-14__07_00_00.db'

    with pytest.raises(sqlite3.OperationalError):
        await seal_closed_file(str(missing), sealed_at=SEALED_AT, **IDENTITY)

    assert not missing.exists()


async def test_publish_in_background_settles_with_the_publish_result(tmp_path):
    source = tmp_path / 'w-1-2026-10-14__08_00_00.db'
    source.write_bytes(b'sealed bytes')
    folder = tmp_path / 'history' / 'search'

    dest, crossed = await sealing.publish_in_background(str(source), {'schema': 1}, str(folder))

    assert (dest, crossed) == (str(folder / source.name), False)


async def test_publish_in_background_passes_the_error_on(tmp_path):
    with pytest.raises(FileNotFoundError):
        await sealing.publish_in_background(str(tmp_path / 'absent.db'), {'schema': 1}, str(tmp_path / 'h'))


# --- publish_sealed ---


def test_publish_sealed_writes_the_meta_then_moves_the_file(tmp_path):
    storage = tmp_path / 'storage'
    storage.mkdir()
    source = storage / 'w-1-2026-10-14__08_00_00.db'
    source.write_bytes(b'sealed bytes')
    folder = tmp_path / 'shared' / 'history' / 'search'

    dest, crossed = publish_sealed(str(source), {'schema': 1, 'worker': 'w-1'}, str(folder))

    assert (dest, crossed) == (str(folder / 'w-1-2026-10-14__08_00_00.db'), False)
    assert not source.exists()
    assert (folder / 'w-1-2026-10-14__08_00_00.db').read_bytes() == b'sealed bytes'
    assert read_meta(str(folder / 'w-1-2026-10-14__08_00_00.db.meta.json')) == {'schema': 1, 'worker': 'w-1'}
    assert oct((folder).stat().st_mode & 0o777) == oct(0o700)


def _cross_filesystem_rename(src, dest):
    raise OSError(errno.EXDEV, 'Invalid cross-device link')


def test_publish_sealed_copies_across_filesystems(tmp_path, monkeypatch):
    monkeypatch.setattr(sealing, '_rename', _cross_filesystem_rename)
    source = tmp_path / 'w-1-2026-10-14__08_00_00.db'
    source.write_bytes(b'x' * 300_000)
    folder = tmp_path / 'history' / 'search'

    _dest, crossed = publish_sealed(str(source), {'schema': 1}, str(folder))

    assert crossed is True
    assert not source.exists()
    assert (folder / source.name).read_bytes() == b'x' * 300_000
    assert sorted(p.name for p in folder.iterdir()) == [source.name, f'{source.name}.meta.json']


def test_an_interrupted_cross_filesystem_copy_keeps_the_source_and_no_temp(tmp_path, monkeypatch):
    monkeypatch.setattr(sealing, '_rename', _cross_filesystem_rename)

    def broken_copy(src, dst, length=0):
        dst.write(b'partial')
        raise OSError(errno.ENOSPC, 'No space left on device')

    monkeypatch.setattr(sealing.shutil, 'copyfileobj', broken_copy)
    source = tmp_path / 'w-1-2026-10-14__08_00_00.db'
    source.write_bytes(b'sealed bytes')
    folder = tmp_path / 'history' / 'search'

    with pytest.raises(OSError, match='No space'):
        publish_sealed(str(source), {'schema': 1}, str(folder))

    assert source.read_bytes() == b'sealed bytes'
    assert not [p for p in folder.iterdir() if p.name.endswith('.incoming')]


def test_publish_sealed_passes_other_rename_errors_on(tmp_path, monkeypatch):
    def denied(src, dest):
        raise PermissionError(errno.EACCES, 'Permission denied')

    monkeypatch.setattr(sealing, '_rename', denied)
    source = tmp_path / 'w-1-2026-10-14__08_00_00.db'
    source.write_bytes(b'sealed bytes')

    with pytest.raises(PermissionError):
        publish_sealed(str(source), {'schema': 1}, str(tmp_path / 'history' / 'search'))

    assert source.exists()


# --- own files and leftovers ---


def _touch(path, age_seconds: float = 0.0) -> None:
    path.write_bytes(b'')
    stamp = time.time() - age_seconds
    os.utime(path, (stamp, stamp))


def test_own_db_files_matches_only_this_workers_exact_names(tmp_path):
    for name in [
        'a-2026-10-14__08_00_00.db',
        'a-2026-10-14__07_00_00.db',
        'a-b-2026-10-14__08_00_00.db',
        'a-2026-10-14__08_00_00.db-wal',
        'a-cache.db',
        'a-live.db',
        'merged-2026-10-14__08_00_00-0123456789abcdef.db',
    ]:
        _touch(tmp_path / name)

    names = [os.path.basename(p) for p in own_db_files(str(tmp_path), 'a')]

    assert names == ['a-2026-10-14__07_00_00.db', 'a-2026-10-14__08_00_00.db']


def test_find_leftovers_applies_the_floor_to_the_file_and_its_wal(tmp_path):
    _touch(tmp_path / 'w-1-2026-10-14__05_00_00.db', age_seconds=900)
    _touch(tmp_path / 'w-1-2026-10-14__06_00_00.db', age_seconds=900)
    _touch(tmp_path / 'w-1-2026-10-14__06_00_00.db-wal', age_seconds=10)
    _touch(tmp_path / 'w-1-2026-10-14__07_00_00.db', age_seconds=10)
    _touch(tmp_path / 'w-1-2026-10-14__08_00_00.db', age_seconds=900)

    found = find_leftovers(str(tmp_path), 'w-1', exclude=str(tmp_path / 'w-1-2026-10-14__08_00_00.db'), now=time.time())

    assert [os.path.basename(p) for p in found] == ['w-1-2026-10-14__05_00_00.db']


def test_find_leftovers_of_a_missing_folder_is_empty(tmp_path):
    assert find_leftovers(str(tmp_path / 'absent'), 'w-1', exclude='', now=time.time()) == []
