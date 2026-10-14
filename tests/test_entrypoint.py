"""Tests for recorder DB naming and what a clean stop leaves behind.

The recorder generates timestamped filenames from storage.dir + worker
name; a clean stop seals the live file into the history folder.
"""

import os
from pathlib import Path

from drakkar.config import UIConfig
from drakkar.recorder import EventRecorder, make_db_path
from tests.conftest import make_ui_config

WORKER = 'worker-1'


def _make_config(tmp_path, **overrides) -> UIConfig:
    defaults = {
        'enabled': True,
        'rotation_interval_hours': 1,
        'flush_interval_seconds': 60,
    }
    defaults.update(overrides)
    return make_ui_config(**defaults)


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


def test_filename_uses_storage_dir():
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


async def test_stop_keeps_the_db_file_in_the_history_folder(tmp_path):
    config = _make_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER, storage_dir=str(tmp_path))
    await rec.start()

    db_file = rec.db_path
    assert os.path.exists(db_file)

    await rec.stop()
    assert not os.path.exists(db_file)
    assert os.path.exists(os.path.join(rec.history_dir, os.path.basename(db_file)))


async def test_stop_flushes_before_sealing(tmp_path):
    config = _make_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER, storage_dir=str(tmp_path))
    await rec.start()

    from drakkar.models import SourceMessage

    msg = SourceMessage(topic='t', partition=0, offset=0, value=b'{}', timestamp=1000)
    rec.record_consumed(msg)
    # event is in buffer, not flushed

    await rec.stop()

    # verify the event was flushed to the DB before shutdown
    import aiosqlite

    sealed = os.path.join(rec.history_dir, os.path.basename(rec.db_path))
    async with (
        aiosqlite.connect(sealed) as db,
        db.execute('SELECT COUNT(*) FROM events') as cur,
    ):
        row = await cur.fetchone()
        assert row[0] == 1


# --- Memory-only mode ---


async def test_no_db_file_when_storage_dir_empty():
    config = make_ui_config(enabled=True)
    rec = EventRecorder(config, worker_name=WORKER, storage_dir='')
    await rec.start()
    assert rec.db_path == ''
    await rec.stop()
