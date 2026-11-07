"""Orphan folders: the leftover pass seals the own folder of a worker name that is not running."""

import asyncio
import os
import threading
import time
from pathlib import Path

import pytest
from prometheus_client import REGISTRY
from structlog.testing import capture_logs

from drakkar.config import RecorderConfig
from drakkar.ownerlock import OWNER_LOCK_NAME, open_own_folder, take_own_lock, try_sibling_lock, try_staging_lock
from drakkar.recorder import EventRecorder, orphans
from drakkar.recorder.history import read_meta
from drakkar.recorder.orphans import (
    UNKNOWN_VERDICT_REPORT_SECONDS,
    OrphanSealer,
    check_idle,
    remove_orphan_files,
    sibling_folders,
    staging_folders,
)
from tests.test_recorder_sealing import OLD_ENOUGH_SECONDS, _leftover

WORKER = 'w-1'
ORPHAN = 'w-old'
INSTANCE = '1111111111111111'
ORPHAN_FILE = f'{ORPHAN}-2026-11-01__10_00_00.db'
STAGING = f'.orphan-{ORPHAN}-0123456789abcdef'


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


class _Clock:
    def __init__(self) -> None:
        self.now = 100.0


def _sealer(tmp_path, cluster_dir: Path, worker_online=lambda name: False, clock: _Clock | None = None):
    clock = clock or _Clock()
    return OrphanSealer(
        cluster_dir=str(cluster_dir),
        own_worker=WORKER,
        worker_online=worker_online,
        history_dir=lambda: str(tmp_path / 'shared' / 'history' / 'search'),
        instance=INSTANCE,
        cluster='search',
        now_monotonic=lambda: clock.now,
    )


def _events(logs: list[dict], name: str) -> list[dict]:
    return [entry for entry in logs if entry['event'] == name]


def _orphan_failures() -> float:
    return REGISTRY.get_sample_value('drakkar_recorder_orphan_failures_total') or 0.0


def _history(tmp_path) -> Path:
    return tmp_path / 'shared' / 'history' / 'search'


# --- sealing through the recorder ---


async def test_seal_pending_dead_name_seals_its_files_under_the_owners_name_and_removes_the_folder(
    tmp_path, cluster_dir
):
    folder = _orphan_folder(cluster_dir)
    asked: list[str] = []
    rec = _recorder(tmp_path, cluster_dir, worker_online=lambda name: asked.append(name) or False)

    with capture_logs() as logs:
        await rec._seal_pending()

    history = _history(tmp_path)
    assert sorted(path.name for path in history.iterdir()) == [ORPHAN_FILE, f'{ORPHAN_FILE}.meta.json']
    meta = read_meta(str(history / f'{ORPHAN_FILE}.meta.json'))
    assert (meta['worker'], meta['instance'], meta['cluster']) == (ORPHAN, INSTANCE, 'search')
    assert meta['tables']['events'] == 3
    assert not folder.exists()
    assert sorted(path.name for path in cluster_dir.iterdir()) == [WORKER]
    assert asked == [ORPHAN]
    (sealed,) = _events(logs, 'recorder_orphan_folder_sealed')
    assert sealed['log_level'] == 'info'
    assert (sealed['folder'], sealed['worker'], sealed['files'], sealed['folder_removed']) == (
        str(folder),
        ORPHAN,
        1,
        True,
    )
    (leftover,) = _events(logs, 'recorder_leftover_sealed')
    assert leftover['orphan_worker'] == ORPHAN
    assert rec._backlog == []


async def test_seal_orphans_releases_the_folder_before_the_slow_seal(tmp_path, cluster_dir, monkeypatch):
    """A worker of that name that starts while the files are sealed gets its folder."""
    folder = _orphan_folder(cluster_dir)
    real_seal = orphans.seal_closed_file
    restarted: list[bool] = []

    async def seal_while_the_owner_restarts(path, **identity):
        lock = open_own_folder(str(cluster_dir.parent), str(folder), ORPHAN)
        restarted.append(lock.held)
        lock.release()
        return await real_seal(path, **identity)

    monkeypatch.setattr(orphans, 'seal_closed_file', seal_while_the_owner_restarts)
    rec = _recorder(tmp_path, cluster_dir)

    await rec._seal_orphans()

    assert restarted == [True]
    assert (_history(tmp_path) / ORPHAN_FILE).exists()


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
    assert (found['folder'], found['worker'], found['reason']) == (str(folder), ORPHAN, 'no_registry')


async def test_seal_orphans_unknown_file_stays_in_the_staging_folder_with_its_lock_file(tmp_path, cluster_dir):
    folder = _orphan_folder(cluster_dir)
    (folder / 'notes.txt').write_text('')
    _age(folder / 'notes.txt')
    rec = _recorder(tmp_path, cluster_dir)

    with capture_logs() as logs:
        await rec._seal_orphans()
        await rec._seal_orphans()

    assert not folder.exists()
    ((staging, _worker),) = staging_folders(str(cluster_dir))
    assert sorted(path.name for path in Path(staging).iterdir()) == [OWNER_LOCK_NAME, 'notes.txt']
    (sealed,) = _events(logs, 'recorder_orphan_folder_sealed')
    assert (sealed['files'], sealed['folder_removed']) == (1, False)


async def test_seal_orphans_never_touches_the_own_folder(tmp_path, cluster_dir):
    own = cluster_dir / WORKER
    take_own_lock(str(own)).release()
    (own / 'notes.txt').write_text('')
    _age(own / 'notes.txt')
    rec = _recorder(tmp_path, cluster_dir)

    await rec._seal_orphans()

    assert (own / 'notes.txt').exists()


# --- failures stay with their folder ---


async def test_seal_orphans_unreadable_sibling_is_logged_and_the_next_one_is_sealed(tmp_path, cluster_dir):
    blocked = _orphan_folder(cluster_dir, 'w-a')
    later = _orphan_folder(cluster_dir, 'w-b')
    rec = _recorder(tmp_path, cluster_dir)
    failures_before = _orphan_failures()
    blocked.chmod(0o000)
    try:
        with capture_logs() as logs:
            await rec._seal_orphans()
    finally:
        blocked.chmod(0o700)

    assert not later.exists()
    assert (_history(tmp_path) / 'w-b-2026-11-01__10_00_00.db').exists()
    (failed,) = _events(logs, 'recorder_orphan_folder_failed')
    assert failed['log_level'] == 'warning'
    assert (failed['folder'], failed['worker'], failed['stage']) == (str(blocked), 'w-a', 'lock')
    assert failed['error_type'] == 'PermissionError'
    assert failed['suppressed'] == 0
    assert _orphan_failures() == failures_before + 1


async def test_seal_orphans_failure_of_one_folder_is_logged_once_a_minute_and_counted_each_time(
    tmp_path, cluster_dir, monkeypatch
):
    _orphan_folder(cluster_dir)

    def no_lock(folder):
        raise PermissionError(13, 'Permission denied')

    monkeypatch.setattr(orphans, 'try_sibling_lock', no_lock)
    sealer = _sealer(tmp_path, cluster_dir)
    failures_before = _orphan_failures()

    with capture_logs() as logs:
        await sealer.run(OLD_ENOUGH_SECONDS / 3)
        await sealer.run(OLD_ENOUGH_SECONDS / 3)

    assert len(_events(logs, 'recorder_orphan_folder_failed')) == 1
    assert _orphan_failures() == failures_before + 2


async def test_seal_pending_unreadable_cluster_folder_never_fails_the_own_pass(tmp_path, cluster_dir, monkeypatch):
    own_leftover = _leftover(cluster_dir / WORKER, f'{WORKER}-2026-11-01__09_00_00.db', 1, age_seconds=900)

    def unreadable(cluster, own):
        raise PermissionError(13, 'Permission denied')

    monkeypatch.setattr(orphans, 'sibling_folders', unreadable)
    rec = _recorder(tmp_path, cluster_dir)

    with capture_logs() as logs:
        await rec._seal_pending()

    assert not os.path.exists(own_leftover)
    assert (_history(tmp_path) / os.path.basename(own_leftover)).exists()
    (failed,) = _events(logs, 'recorder_orphan_folder_failed')
    assert (failed['folder'], failed['stage']) == (str(cluster_dir), 'list')


async def test_seal_orphans_file_that_cannot_be_sealed_stays_staged_and_no_backlog(tmp_path, cluster_dir):
    folder = _orphan_folder(cluster_dir)
    (folder / ORPHAN_FILE).write_bytes(b'not a database' * 100)
    (folder / f'{ORPHAN_FILE}-wal').unlink()
    _age(folder / ORPHAN_FILE)
    rec = _recorder(tmp_path, cluster_dir)
    seal_failures_before = REGISTRY.get_sample_value('drakkar_recorder_seal_failures_total') or 0.0

    with capture_logs() as logs:
        await rec._seal_orphans()

    ((staging, worker),) = staging_folders(str(cluster_dir))
    assert worker == ORPHAN
    assert (Path(staging) / ORPHAN_FILE).exists()
    (failed,) = _events(logs, 'recorder_orphan_folder_failed')
    assert (failed['stage'], failed['worker']) == ('seal', ORPHAN)
    assert not _events(logs, 'recorder_seal_failed')
    assert REGISTRY.get_sample_value('drakkar_recorder_seal_failures_total') == seal_failures_before
    assert rec._backlog == []
    assert not _events(logs, 'recorder_orphan_folder_sealed')


async def test_seal_orphans_publish_failure_keeps_the_staged_file(tmp_path, cluster_dir, monkeypatch):
    _orphan_folder(cluster_dir)

    def failing_publish(path, meta, history):
        raise OSError(28, 'no space left on device')

    monkeypatch.setattr(orphans, 'publish_in_background', failing_publish)
    rec = _recorder(tmp_path, cluster_dir)

    with capture_logs() as logs:
        await rec._seal_orphans()

    ((staging, _worker),) = staging_folders(str(cluster_dir))
    assert (Path(staging) / ORPHAN_FILE).exists()
    (failed,) = _events(logs, 'recorder_orphan_folder_failed')
    assert failed['stage'] == 'publish'


async def test_seal_orphans_cleanup_failure_frees_the_staged_files_for_the_next_pass(
    tmp_path, cluster_dir, monkeypatch
):
    _orphan_folder(cluster_dir)
    real_remove = orphans.remove_orphan_files

    def failing_remove(folder, worker):
        raise OSError(5, 'Input/output error')

    monkeypatch.setattr(orphans, 'remove_orphan_files', failing_remove)
    sealer = _sealer(tmp_path, cluster_dir)
    await sealer.run(OLD_ENOUGH_SECONDS / 3)
    monkeypatch.setattr(orphans, 'remove_orphan_files', real_remove)

    await sealer.run(OLD_ENOUGH_SECONDS / 3)

    assert (_history(tmp_path) / ORPHAN_FILE).exists()
    assert staging_folders(str(cluster_dir)) == []


# --- staging folders ---


def _staging_folder(cluster_dir: Path) -> Path:
    staging = cluster_dir / STAGING
    staging.mkdir()
    take_own_lock(str(staging)).release()
    _leftover(staging, ORPHAN_FILE, 2, age_seconds=OLD_ENOUGH_SECONDS)
    return staging


async def test_run_finishes_a_staging_folder_whose_sealer_died(tmp_path, cluster_dir):
    staging = _staging_folder(cluster_dir)
    sealer = _sealer(tmp_path, cluster_dir)

    with capture_logs() as logs:
        await sealer.run(OLD_ENOUGH_SECONDS / 3)

    assert not staging.exists()
    meta = read_meta(str(_history(tmp_path) / f'{ORPHAN_FILE}.meta.json'))
    assert meta['worker'] == ORPHAN
    (sealed,) = _events(logs, 'recorder_orphan_folder_sealed')
    assert (sealed['folder'], sealed['files'], sealed['folder_removed']) == (str(staging), 1, True)


async def test_run_leaves_a_staging_folder_whose_sealer_still_works(tmp_path, cluster_dir):
    staging = _staging_folder(cluster_dir)
    sealer = _sealer(tmp_path, cluster_dir)
    working = take_own_lock(str(staging))
    try:
        await sealer.run(OLD_ENOUGH_SECONDS / 3)
    finally:
        working.release()

    assert (staging / ORPHAN_FILE).exists()


async def test_seal_orphans_rename_failure_creates_nothing_and_logs_no_sealed_line(tmp_path, cluster_dir, monkeypatch):
    folder = _orphan_folder(cluster_dir)

    def cross_device(source, destination):
        raise OSError(18, 'Invalid cross-device link')

    monkeypatch.setattr(orphans, 'rename_folder', cross_device)
    sealer = _sealer(tmp_path, cluster_dir)
    failures_before = _orphan_failures()

    with capture_logs() as logs:
        await sealer.run(OLD_ENOUGH_SECONDS / 3)
        await sealer.run(OLD_ENOUGH_SECONDS / 3)

    assert sorted(path.name for path in cluster_dir.iterdir()) == [WORKER, ORPHAN]
    assert (folder / ORPHAN_FILE).exists()
    (failed,) = _events(logs, 'recorder_orphan_folder_failed')
    assert failed['stage'] == 'stage'
    assert _orphan_failures() == failures_before + 2
    assert not _events(logs, 'recorder_orphan_folder_sealed')
    take_own_lock(str(folder)).release()


async def test_run_crash_after_the_rename_keeps_the_wal_with_its_database(tmp_path, cluster_dir):
    """The staging folder of a sealer that died after the rename still holds the file and its WAL."""
    folder = _orphan_folder(cluster_dir, events=4)
    staging = cluster_dir / STAGING
    os.rename(folder, staging)
    assert (staging / f'{ORPHAN_FILE}-wal').exists()

    await _sealer(tmp_path, cluster_dir).run(OLD_ENOUGH_SECONDS / 3)

    meta = read_meta(str(_history(tmp_path) / f'{ORPHAN_FILE}.meta.json'))
    assert meta['tables']['events'] == 4
    assert not staging.exists()


async def test_run_finishes_a_staging_folder_without_a_lock_file(tmp_path, cluster_dir):
    staging = cluster_dir / STAGING
    staging.mkdir()
    _leftover(staging, ORPHAN_FILE, 1, age_seconds=OLD_ENOUGH_SECONDS)

    await _sealer(tmp_path, cluster_dir).run(OLD_ENOUGH_SECONDS / 3)

    assert (_history(tmp_path) / ORPHAN_FILE).exists()
    assert not staging.exists()


async def test_run_empty_staging_folder_is_removed_without_a_sealed_line(tmp_path, cluster_dir):
    staging = cluster_dir / STAGING
    staging.mkdir()

    with capture_logs() as logs:
        await _sealer(tmp_path, cluster_dir).run(OLD_ENOUGH_SECONDS / 3)

    assert not staging.exists()
    assert not _events(logs, 'recorder_orphan_folder_sealed')


async def test_run_cancelled_during_the_rename_keeps_the_lock_until_the_rename_ends(tmp_path, cluster_dir, monkeypatch):
    folder = _orphan_folder(cluster_dir)
    started = threading.Event()
    go_on = threading.Event()
    real_rename = orphans.rename_folder

    def slow_rename(source, destination):
        started.set()
        go_on.wait(5)
        real_rename(source, destination)

    monkeypatch.setattr(orphans, 'rename_folder', slow_rename)
    task = asyncio.create_task(_sealer(tmp_path, cluster_dir).run(OLD_ENOUGH_SECONDS / 3))
    await asyncio.to_thread(started.wait, 5)
    task.cancel()
    await asyncio.sleep(0.05)

    assert not task.done()
    assert try_sibling_lock(str(folder)) is None
    go_on.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    ((staging, _worker),) = staging_folders(str(cluster_dir))
    lock = try_staging_lock(staging)
    assert lock is not None
    lock.release()


# --- what the pass reports ---


async def test_run_verdict_unknown_for_an_hour_is_logged_once(tmp_path, cluster_dir):
    folder = _orphan_folder(cluster_dir)
    clock = _Clock()
    sealer = _sealer(tmp_path, cluster_dir, worker_online=lambda name: None, clock=clock)

    with capture_logs() as logs:
        await sealer.run(OLD_ENOUGH_SECONDS / 3)
        clock.now += UNKNOWN_VERDICT_REPORT_SECONDS - 1
        await sealer.run(OLD_ENOUGH_SECONDS / 3)
        assert not _events(logs, 'recorder_orphan_folder_found')
        clock.now += 1
        await sealer.run(OLD_ENOUGH_SECONDS / 3)
        await sealer.run(OLD_ENOUGH_SECONDS / 3)

    (found,) = _events(logs, 'recorder_orphan_folder_found')
    assert (found['folder'], found['reason']) == (str(folder), 'registry_unknown')
    assert (folder / ORPHAN_FILE).exists()


async def test_run_file_ahead_of_the_clock_is_logged_once_and_keeps_the_folder(tmp_path, cluster_dir):
    folder = _orphan_folder(cluster_dir)
    future = time.time() + 3600
    os.utime(folder / f'{ORPHAN}.watchdog', (future, future))
    sealer = _sealer(tmp_path, cluster_dir)

    with capture_logs() as logs:
        await sealer.run(OLD_ENOUGH_SECONDS / 3)
        await sealer.run(OLD_ENOUGH_SECONDS / 3)

    (ahead,) = _events(logs, 'recorder_orphan_folder_clock_ahead')
    assert (ahead['folder'], ahead['file']) == (str(folder), f'{ORPHAN}.watchdog')
    assert ahead['seconds_ahead'] > 3500
    assert (folder / ORPHAN_FILE).exists()


async def test_run_cancelled_while_taking_a_lock_releases_it_later(tmp_path, cluster_dir, monkeypatch):
    folder = _orphan_folder(cluster_dir)
    entered = threading.Event()
    go_on = threading.Event()
    taken = threading.Event()

    def slow_lock(path):
        entered.set()
        go_on.wait(5)
        lock = try_sibling_lock(path)
        taken.set()
        return lock

    monkeypatch.setattr(orphans, 'try_sibling_lock', slow_lock)
    task = asyncio.create_task(_sealer(tmp_path, cluster_dir).run(OLD_ENOUGH_SECONDS / 3))
    # Cancel only once the pass is inside the lock call, or nothing is tested.
    assert await asyncio.to_thread(entered.wait, 5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    go_on.set()
    assert await asyncio.to_thread(taken.wait, 5)
    for _ in range(100):
        await asyncio.sleep(0.01)
        lock = try_sibling_lock(str(folder))
        if lock is not None:
            lock.release()
            return
    pytest.fail('the lock taken after the cancellation was never released')


# --- the helpers ---


def test_sibling_folders_lists_only_other_worker_folders(tmp_path):
    for name in ('w-1', 'w-2', 'a.b', 'w-3', STAGING):
        (tmp_path / name).mkdir()
    (tmp_path / 'w-4').write_text('a file')
    (tmp_path / 'w-5').symlink_to(tmp_path / 'w-2')

    assert sibling_folders(str(tmp_path), 'w-1') == [(str(tmp_path / 'w-2'), 'w-2'), (str(tmp_path / 'w-3'), 'w-3')]


def test_sibling_folders_missing_cluster_folder_has_none(tmp_path):
    assert sibling_folders(str(tmp_path / 'missing'), 'w-1') == []
    assert staging_folders(str(tmp_path / 'missing')) == []


def test_staging_folders_lists_only_real_staging_folders(tmp_path):
    (tmp_path / STAGING).mkdir()
    (tmp_path / '.orphan-w-2-notatoken').mkdir()
    (tmp_path / '.orphan-w-3-0123456789abcdef').write_text('a file')
    (tmp_path / 'w-4').mkdir()

    assert staging_folders(str(tmp_path)) == [(str(tmp_path / STAGING), ORPHAN)]


def test_check_idle_ignores_the_lock_file_and_sees_a_fresh_file(tmp_path):
    (tmp_path / OWNER_LOCK_NAME).write_text('')
    (tmp_path / 'old.db').write_text('')
    _age(tmp_path / 'old.db')

    assert check_idle(str(tmp_path), now=time.time(), floor_seconds=300).idle

    (tmp_path / 'new.db').write_text('')
    assert not check_idle(str(tmp_path), now=time.time(), floor_seconds=300).idle


def test_check_idle_missing_folder_is_not_idle(tmp_path):
    assert not check_idle(str(tmp_path / 'gone'), now=time.time(), floor_seconds=300).idle


def test_remove_orphan_files_folder_recreated_in_the_meantime_is_kept(tmp_path, monkeypatch):
    folder = tmp_path / ORPHAN
    folder.mkdir()
    (folder / OWNER_LOCK_NAME).write_text('')

    def racing_rmdir(path):
        raise OSError(66, 'Directory not empty')

    monkeypatch.setattr(os, 'rmdir', racing_rmdir)

    assert remove_orphan_files(str(folder), ORPHAN) is False
