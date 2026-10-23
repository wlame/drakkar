"""The recorder's explicit persistence mode: folders do not depend on the file."""

import os
from unittest.mock import MagicMock

import pytest
from structlog.testing import capture_logs

from drakkar.recorder import EventRecorder
from drakkar.recorder import core as core_mod
from tests.conftest import make_ui_config

CLUSTER = 'search'


def _recorder(persistence: str, *, storage_dir: str = '', shared_dir: str = '', **ui) -> EventRecorder:
    return EventRecorder(
        make_ui_config(enabled=True, store_output=False, flush_interval_seconds=60, **ui),
        worker_name='w-1',
        cluster_name=CLUSTER,
        persistence=persistence,
        storage_dir=storage_dir,
        shared_dir=shared_dir,
    )


def test_none_recorder_with_shared_dir_resolves_shared_and_history_dirs(tmp_path):
    recorder = _recorder('none', shared_dir=str(tmp_path / 'shared'))

    assert not recorder.has_file
    assert recorder.persistence == 'none'
    assert recorder.shared_dir == str(tmp_path / 'shared')
    assert recorder.history_dir == str(tmp_path / 'shared' / 'history' / CLUSTER)


def test_none_recorder_with_storage_dir_only_shares_through_storage_dir(tmp_path):
    recorder = _recorder('none', storage_dir=str(tmp_path / 'storage'))

    assert recorder.shared_dir == str(tmp_path / 'storage')
    assert recorder.history_dir == str(tmp_path / 'storage' / 'history' / CLUSTER)


def test_none_recorder_without_any_folder_has_no_shared_folders():
    recorder = _recorder('none')

    assert recorder.shared_dir == ''
    assert recorder.history_dir == ''


async def test_none_recorder_with_storage_dir_writes_no_recorder_file(tmp_path):
    storage = tmp_path / 'storage'
    storage.mkdir()
    recorder = _recorder('none', storage_dir=str(storage))

    await recorder.start()
    await recorder.stop()

    assert os.listdir(storage) == []
    assert recorder.db_path == ''


def test_sqlite_recorder_without_storage_dir_raises():
    with pytest.raises(ValueError, match='recorder: persistence sqlite needs a storage_dir'):
        _recorder('sqlite')


async def test_sqlite_recorder_opens_its_file(tmp_path):
    storage = tmp_path / 'storage'
    storage.mkdir()
    recorder = _recorder('sqlite', storage_dir=str(storage))

    await recorder.start()
    try:
        assert recorder.has_file
        assert recorder.persistence == 'sqlite'
        assert len(os.listdir(storage)) >= 1
    finally:
        await recorder.stop()


async def test_none_recorder_skips_folder_warnings_rotation_state_and_leftover_sealing(tmp_path, monkeypatch):
    storage = tmp_path / 'storage'
    storage.mkdir()
    storage.chmod(0o777)  # would raise the world-writable warning with a file
    monkeypatch.setattr(core_mod, 'detect_network_fs', MagicMock(return_value=('nfs', '/mnt')))
    leftover = storage / 'w-1-2026-10-13__08_00_00.db'
    leftover.write_bytes(b'')
    os.utime(leftover, (1, 1))
    recorder = _recorder('none', storage_dir=str(storage), store_state=True)

    with capture_logs() as logs:
        await recorder.start()
        recorder.start_leftover_sealing()
        await recorder._sync_state()
        await recorder.stop()

    events = {entry['event'] for entry in logs}
    assert not {e for e in events if e.startswith('recorder_storage_dir')}
    assert 'recorder_archiving_disabled' not in events
    assert recorder._rotation_task is None
    assert recorder._state_task is None
    assert recorder._leftover_task is None
    assert recorder._db is None
    assert leftover.exists()
