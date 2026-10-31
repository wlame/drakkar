"""Orphan folders: the leftover pass seals the own folder of a worker name that is dead."""

import os
import time
from pathlib import Path

import pytest
from structlog.testing import capture_logs

import drakkar.recorder.core as recorder_core
from drakkar.config import RecorderConfig
from drakkar.ownerlock import OWNER_LOCK_NAME, take_own_lock
from drakkar.recorder import EventRecorder
from drakkar.recorder.history import read_meta
from drakkar.recorder.orphans import folder_is_idle, remove_orphan_files, sibling_folders
from tests.test_recorder_sealing import OLD_ENOUGH_SECONDS, _leftover

WORKER = 'w-1'
ORPHAN = 'w-old'
INSTANCE = '1111111111111111'
ORPHAN_FILE = f'{ORPHAN}-2026-11-01__10_00_00.db'


def _age(path: Path, seconds: float = OLD_ENOUGH_SECONDS) -> None:
    stamp = time.time() - seconds
    os.utime(path, (stamp, stamp))


def _orphan_folder(cluster_dir: Path, worker: str = ORPHAN, *, events: int = 3) -> Path:
    """An own folder a dead worker left: its lock file, a recorder file with rows in the WAL, a cache file, a crash marker."""
    folder = cluster_dir / worker
    folder.mkdir(parents=True)
    take_own_lock(str(folder)).release()
    _leftover(folder, f'{worker}-2026-11-01__10_00_00.db', events, age_seconds=OLD_ENOUGH_SECONDS)
    for name in (f'{worker}-cache.db', f'{worker}-cache.db-wal', f'{worker}.watchdog', f'.{worker}-x.db.ab.download'):
        (folder / name).write_text('')
        _age(folder / name)
    return folder


@pytest.fixture
def cluster_dir(tmp_path) -> Path:
    path = tmp_path / 'storage' / 'search'
    (path / WORKER).mkdir(parents=True)
    return path


def _recorder(tmp_path, cluster_dir: Path, worker_online=lambda name: False) -> EventRecorder:
    return EventRecorder(
        RecorderConfig(flush_interval='60s', capture={'output': False}),
        worker_name=WORKER,
        cluster_name='search',
        persistence='sqlite',
        storage_dir=str(cluster_dir / WORKER),
        shared_dir=str(tmp_path / 'shared'),
        instance=INSTANCE,
        sibling_dir=str(cluster_dir),
        worker_online=worker_online,
    )


def _events(logs: list[dict], name: str) -> list[dict]:
    return [entry for entry in logs if entry['event'] == name]


# --- the recorder ---


async def test_seal_pending_dead_name_seals_its_files_under_the_owners_name_and_removes_the_folder(
    tmp_path, cluster_dir
):
    folder = _orphan_folder(cluster_dir)
    asked: list[str] = []
    rec = _recorder(tmp_path, cluster_dir, worker_online=lambda name: asked.append(name) or False)

    with capture_logs() as logs:
        await rec._seal_pending()

    history = tmp_path / 'shared' / 'history' / 'search'
    assert sorted(path.name for path in history.iterdir()) == [ORPHAN_FILE, f'{ORPHAN_FILE}.meta.json']
    meta = read_meta(str(history / f'{ORPHAN_FILE}.meta.json'))
    assert (meta['worker'], meta['instance'], meta['cluster']) == (ORPHAN, INSTANCE, 'search')
    assert meta['tables']['events'] == 3
    assert not folder.exists()
    assert asked == [ORPHAN]
    (sealed,) = _events(logs, 'recorder_orphan_folder_sealed')
    assert sealed['log_level'] == 'info'
    assert (sealed['folder'], sealed['worker'], sealed['files'], sealed['folder_removed']) == (
        str(folder),
        ORPHAN,
        1,
        True,
    )
    assert rec._backlog == []


async def test_seal_orphans_folder_whose_lock_is_held_is_left_alone(tmp_path, cluster_dir):
    folder = _orphan_folder(cluster_dir)
    rec = _recorder(tmp_path, cluster_dir)
    owner = take_own_lock(str(folder))
    try:
        await rec._seal_orphans()
    finally:
        owner.release()

    assert (folder / ORPHAN_FILE).exists()


async def test_seal_orphans_folder_with_a_recent_file_is_left_alone(tmp_path, cluster_dir):
    folder = _orphan_folder(cluster_dir)
    (folder / f'{ORPHAN}-cache.db-wal').write_text('fresh')
    rec = _recorder(tmp_path, cluster_dir)

    await rec._seal_orphans()

    assert (folder / ORPHAN_FILE).exists()


@pytest.mark.parametrize('verdict', [True, None], ids=['online', 'unknown'])
async def test_seal_orphans_name_not_proved_dead_is_left_alone(tmp_path, cluster_dir, verdict):
    folder = _orphan_folder(cluster_dir)
    rec = _recorder(tmp_path, cluster_dir, worker_online=lambda name: verdict)

    with capture_logs() as logs:
        await rec._seal_orphans()

    assert (folder / ORPHAN_FILE).exists()
    assert not _events(logs, 'recorder_orphan_folder_found')


async def test_seal_orphans_without_a_registry_only_logs_the_folder_once(tmp_path, cluster_dir):
    folder = _orphan_folder(cluster_dir)
    rec = _recorder(tmp_path, cluster_dir, worker_online=None)

    with capture_logs() as logs:
        await rec._seal_orphans()
        await rec._seal_orphans()

    assert (folder / ORPHAN_FILE).exists()
    (found,) = _events(logs, 'recorder_orphan_folder_found')
    assert found['log_level'] == 'warning'
    assert found['category'] == 'recorder'
    assert (found['folder'], found['worker']) == (str(folder), ORPHAN)


async def test_seal_orphans_file_that_cannot_be_sealed_keeps_the_folder_and_no_backlog(tmp_path, cluster_dir):
    folder = _orphan_folder(cluster_dir)
    (folder / ORPHAN_FILE).write_bytes(b'not a database' * 100)
    (folder / f'{ORPHAN_FILE}-wal').unlink()
    _age(folder / ORPHAN_FILE)
    rec = _recorder(tmp_path, cluster_dir)

    with capture_logs() as logs:
        await rec._seal_orphans()

    assert (folder / ORPHAN_FILE).exists()
    (failed,) = _events(logs, 'recorder_seal_failed')
    assert (failed['stage'], failed['orphan_worker']) == ('seal', ORPHAN)
    assert rec._backlog == []
    assert not _events(logs, 'recorder_orphan_folder_sealed')


async def test_seal_orphans_publish_failure_keeps_the_folder(tmp_path, cluster_dir, monkeypatch):
    folder = _orphan_folder(cluster_dir)

    def failing_publish(path, meta, history):
        raise OSError(28, 'no space left on device')

    monkeypatch.setattr(recorder_core, 'publish_in_background', failing_publish)
    rec = _recorder(tmp_path, cluster_dir)

    with capture_logs() as logs:
        await rec._seal_orphans()

    assert folder.exists()
    (failed,) = _events(logs, 'recorder_seal_failed')
    assert failed['stage'] == 'publish'


async def test_seal_orphans_unknown_file_keeps_the_folder_and_its_lock_file(tmp_path, cluster_dir):
    folder = _orphan_folder(cluster_dir)
    (folder / 'notes.txt').write_text('')
    _age(folder / 'notes.txt')
    rec = _recorder(tmp_path, cluster_dir)

    with capture_logs() as logs:
        await rec._seal_orphans()

    assert sorted(path.name for path in folder.iterdir()) == [OWNER_LOCK_NAME, 'notes.txt']
    (sealed,) = _events(logs, 'recorder_orphan_folder_sealed')
    assert sealed['folder_removed'] is False


async def test_seal_orphans_never_touches_the_own_folder(tmp_path, cluster_dir):
    own = cluster_dir / WORKER
    take_own_lock(str(own)).release()
    (own / 'notes.txt').write_text('')
    _age(own / 'notes.txt')
    rec = _recorder(tmp_path, cluster_dir)

    await rec._seal_orphans()

    assert (own / 'notes.txt').exists()


# --- the helpers ---


def test_sibling_folders_lists_only_other_worker_folders(tmp_path):
    for name in ('w-1', 'w-2', 'a.b', 'w-3'):
        (tmp_path / name).mkdir()
    (tmp_path / 'w-4').write_text('a file')
    (tmp_path / 'w-5').symlink_to(tmp_path / 'w-2')

    assert sibling_folders(str(tmp_path), 'w-1') == [(str(tmp_path / 'w-2'), 'w-2'), (str(tmp_path / 'w-3'), 'w-3')]


def test_sibling_folders_missing_cluster_folder_has_none(tmp_path):
    assert sibling_folders(str(tmp_path / 'missing'), 'w-1') == []


def test_folder_is_idle_ignores_the_lock_file_and_sees_a_fresh_file(tmp_path):
    (tmp_path / OWNER_LOCK_NAME).write_text('')
    (tmp_path / 'old.db').write_text('')
    _age(tmp_path / 'old.db')

    assert folder_is_idle(str(tmp_path), now=time.time(), floor_seconds=300)

    (tmp_path / 'new.db').write_text('')
    assert not folder_is_idle(str(tmp_path), now=time.time(), floor_seconds=300)


def test_folder_is_idle_missing_folder_is_not_idle(tmp_path):
    assert not folder_is_idle(str(tmp_path / 'gone'), now=time.time(), floor_seconds=300)


def test_remove_orphan_files_folder_recreated_in_the_meantime_is_kept(tmp_path, monkeypatch):
    folder = tmp_path / ORPHAN
    folder.mkdir()
    (folder / OWNER_LOCK_NAME).write_text('')

    def racing_rmdir(path):
        raise OSError(66, 'Directory not empty')

    monkeypatch.setattr(os, 'rmdir', racing_rmdir)

    assert remove_orphan_files(str(folder), ORPHAN) is False
