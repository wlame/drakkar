"""Tests for recorder DB naming, live symlink, and shutdown cleanup.

The recorder generates timestamped filenames from db_dir + worker_name,
maintains a {worker}-live.db symlink, and removes it on stop().
"""

import os
from pathlib import Path

from drakkar.config import DebugConfig
from drakkar.recorder import EventRecorder, list_db_files, live_link_path, make_db_path

WORKER = 'worker-1'


def _make_config(tmp_path, **overrides) -> DebugConfig:
    defaults = {
        'enabled': True,
        'db_dir': str(tmp_path),
        'retention_hours': 24,
        'retention_max_events': 100_000,
        'flush_interval_seconds': 60,
    }
    defaults.update(overrides)
    return DebugConfig(**defaults)


# --- Filename generation ---


def test_filename_contains_worker_name():
    path = make_db_path('/shared', 'my-worker')
    assert 'my-worker-' in Path(path).name


def test_filename_contains_timestamp():
    path = make_db_path('/tmp', 'w1')
    name = Path(path).name
    assert '__' in name  # YYYY-MM-DD__HH_MM_SS


def test_filename_no_drakkar_or_debug_prefix():
    path = make_db_path('/tmp', 'worker-1')
    name = Path(path).name
    assert not name.startswith('drakkar')
    assert 'debug' not in name


def test_filename_uses_db_dir():
    path = make_db_path('/custom/dir', 'w1')
    assert path.startswith('/custom/dir/')


def test_filename_ends_with_db():
    path = make_db_path('/tmp', 'w1')
    assert path.endswith('.db')


def test_different_workers_produce_different_filenames():
    p1 = make_db_path('/tmp', 'worker-1')
    p2 = make_db_path('/tmp', 'worker-2')
    assert p1 != p2
    assert 'worker-1' in p1
    assert 'worker-2' in p2


# --- Live symlink ---


async def test_start_creates_live_symlink(tmp_path):
    config = _make_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER)
    await rec.start()

    link = Path(live_link_path(str(tmp_path), WORKER))
    assert link.is_symlink()
    assert os.readlink(link) == os.path.basename(rec.db_path)

    await rec.stop()


async def test_live_symlink_name_is_worker_dash_live(tmp_path):
    config = _make_config(tmp_path)
    rec = EventRecorder(config, worker_name='my-cool-worker')
    await rec.start()

    link = tmp_path / 'my-cool-worker-live.db'
    assert link.is_symlink()

    await rec.stop()


async def test_live_symlink_resolves_to_existing_db(tmp_path):
    config = _make_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER)
    await rec.start()

    link = Path(live_link_path(str(tmp_path), WORKER))
    resolved = link.resolve()
    assert resolved.exists()
    assert resolved == Path(rec.db_path).resolve()

    await rec.stop()


async def test_stop_removes_live_symlink(tmp_path):
    config = _make_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER)
    await rec.start()

    link = Path(live_link_path(str(tmp_path), WORKER))
    assert link.is_symlink()

    await rec.stop()
    assert not link.exists()


async def test_stop_preserves_db_file(tmp_path):
    config = _make_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER)
    await rec.start()

    db_file = rec.db_path
    assert os.path.exists(db_file)

    await rec.stop()
    assert os.path.exists(db_file)  # file itself remains


async def test_stop_flushes_before_removing_symlink(tmp_path):
    config = _make_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER)
    await rec.start()

    from drakkar.models import SourceMessage

    msg = SourceMessage(topic='t', partition=0, offset=0, value=b'{}', timestamp=1000)
    rec.record_consumed(msg)
    # event is in buffer, not flushed

    await rec.stop()

    # verify the event was flushed to the DB before shutdown
    import aiosqlite

    async with (
        aiosqlite.connect(rec.db_path) as db,
        db.execute('SELECT COUNT(*) FROM events') as cur,
    ):
        row = await cur.fetchone()
        assert row[0] == 1


# --- Rotation updates symlink ---


async def test_rotation_updates_live_symlink(tmp_path):
    config = _make_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER)
    await rec.start()

    link = Path(live_link_path(str(tmp_path), WORKER))
    first_target = os.readlink(link)

    import asyncio

    await asyncio.sleep(1.1)  # ensure different timestamp
    await rec._rotate()

    second_target = os.readlink(link)
    assert second_target != first_target
    assert second_target == os.path.basename(rec.db_path)

    await rec.stop()


# --- File listing ---


def testlist_db_files_excludes_live_symlink(tmp_path):
    (tmp_path / 'worker-1-2026-03-16__14_00_00.db').touch()
    (tmp_path / 'worker-1-2026-03-16__15_00_00.db').touch()
    live = tmp_path / 'worker-1-live.db'
    live.symlink_to('worker-1-2026-03-16__15_00_00.db')

    files = list_db_files(str(tmp_path), 'worker-1')
    assert len(files) == 2
    assert all('live' not in f for f in files)


def testlist_db_files_excludes_other_workers(tmp_path):
    (tmp_path / 'worker-1-2026-03-16__14_00_00.db').touch()
    (tmp_path / 'worker-2-2026-03-16__14_00_00.db').touch()

    files = list_db_files(str(tmp_path), 'worker-1')
    assert len(files) == 1
    assert 'worker-1' in files[0]


# --- Memory-only mode ---


async def test_no_symlink_when_db_dir_empty():
    config = DebugConfig(enabled=True, db_dir='')
    rec = EventRecorder(config, worker_name=WORKER)
    await rec.start()
    assert rec.db_path == ''
    await rec.stop()
