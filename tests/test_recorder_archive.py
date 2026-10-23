"""Tests for the recorder archive engine: window math and the archive pass.

The pass works on one cluster history folder, ``<shared>/history/<cluster>/``,
where every ``*.db`` is a sealed file with a meta file beside it. One worker
of the cluster holds the task lock ``tasks/<cluster>/history.archive.lock``
while it archives.
"""

import asyncio
import functools
import gzip
import json
import os
import re
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path

import pytest
import structlog
from structlog.testing import capture_logs

from drakkar.cluster_tasks import (
    SLOT_ELECTION_FAILED,
    SLOT_HELD_ELSEWHERE,
    SLOT_LATE,
    SLOT_WON,
    ClusterElection,
    SlotContext,
    SlotLease,
    run_cluster_task,
)
from drakkar.config import HistoryArchiveConfig, ResilienceConfig
from drakkar.metrics import recorder_archive_failures
from drakkar.providers.resilience import ResilientCaller
from drakkar.recorder import EventRecorder
from drakkar.recorder import archive as archive_mod
from drakkar.recorder import core as core_mod
from drakkar.recorder.archive import (
    archive_file_name,
    assign_windows,
    list_archives,
    parse_db_start_ts,
    run_archive_pass,
    warn_if_archives_unbounded,
)
from drakkar.recorder.election import ElectionContext, FilesystemElection, ThreadFence
from drakkar.recorder.history import meta_path, task_lock_dir
from drakkar.recorder.task_lock import LOCK_SUFFIX, TaskLock
from drakkar.schedule import ACQUIRE_WINDOW_SECONDS, ARCHIVE_TASK_NAME, MIN_HOLD_SECONDS, task_scope
from tests.archive_helpers import (
    CLUSTER,
    DAY,
    HOUR,
    SteppingClock,
    due_start,
    history_folder,
    make_sealed_db,
    run_won_slot,
    sealed_db_name,
)
from tests.conftest import WAIT_FOR_TIMEOUT_SECONDS, make_ui_config, wait_for
from tests.fleet_fakes import ElectionStore, MemoryElection

OTHER_CLUSTER = 'index-fleet'
INSTANCE = '1111111111111111'
ARCHIVE_LOCK_FILE = ARCHIVE_TASK_NAME + LOCK_SUFFIX


def _lock_path(tmp_path: Path, cluster: str = CLUSTER) -> Path:
    """The archive task's lock file of ``cluster`` under ``<tmp_path>/shared``."""
    return Path(task_lock_dir(str(tmp_path / 'shared'), cluster)) / ARCHIVE_LOCK_FILE


def _other_holder(tmp_path: Path) -> TaskLock:
    """The archive lock of another worker, its folder created; the caller acquires it."""
    folder = _lock_path(tmp_path).parent
    folder.mkdir(parents=True, exist_ok=True)
    return TaskLock(
        str(folder),
        ARCHIVE_TASK_NAME,
        worker='worker-2',
        instance='2222222222222222',
        stale_seconds=7200,
        scope=task_scope(CLUSTER, ARCHIVE_TASK_NAME),
    )


def _filesystem_election(tmp_path, cluster: str = CLUSTER, *, worker: str = 'worker-1') -> ClusterElection:
    """The worker's election service over the filesystem provider of ``<tmp_path>/shared``."""
    context = ElectionContext(
        worker=worker,
        instance=INSTANCE,
        cluster=cluster,
        shared_dir=str(tmp_path / 'shared'),
        logger=structlog.get_logger(),
        options={},
    )
    caller = ResilientCaller(ResilienceConfig(), feature='election', provider='filesystem', category='periodic')
    return ClusterElection(FilesystemElection(), caller, provider_name='filesystem', context=context)


class _ScriptedFence:
    """A fence whose checks answer ``held``; with ``held`` False it reads as lost for ``reason``."""

    def __init__(self, *, held: bool, reason: str | None = None, provider_name: str = 'scripted') -> None:
        self.token = '0123456789abcdef'
        self.provider_name = provider_name
        self.loss_reason = reason
        self._held = held

    def is_held(self) -> bool:
        return self._held


def _make_archive(folder: Path, cluster: str, end: float, *, window: float = DAY) -> Path:
    """Drop a pre-existing archive file whose window ends at ``end``."""
    path = folder / archive_file_name(cluster, end - window, end)
    path.write_bytes(b'not-really-gzip')
    return path


def _make_config(**overrides) -> HistoryArchiveConfig:
    values = {'enabled': True, 'window_hours': 24}
    values.update(overrides)
    return HistoryArchiveConfig(**values)


def _age(path: Path, seconds: float) -> None:
    stamp = time.time() - seconds
    os.utime(path, (stamp, stamp))


# --- filename helpers ---------------------------------------------------


def test_parse_db_start_ts_reads_the_filename_timestamp_as_utc(tmp_path):
    path = tmp_path / 'worker-1-2026-07-15__13_45_30.db'
    path.write_bytes(b'')

    expected = datetime(2026, 7, 15, 13, 45, 30, tzinfo=UTC).timestamp()
    assert parse_db_start_ts(str(path)) == expected


def test_parse_db_start_ts_handles_worker_names_containing_dashes(tmp_path):
    path = tmp_path / 'search-worker-7-2026-07-15__13_45_30.db'
    path.write_bytes(b'')

    expected = datetime(2026, 7, 15, 13, 45, 30, tzinfo=UTC).timestamp()
    assert parse_db_start_ts(str(path)) == expected


def test_parse_db_start_ts_falls_back_to_mtime_when_name_is_unparsable(tmp_path):
    path = tmp_path / 'hand-made.db'
    path.write_bytes(b'')
    os.utime(path, (1_700_000_000, 1_700_000_000))

    assert parse_db_start_ts(str(path)) == 1_700_000_000


def test_archive_file_name_uses_utc_minute_precision():
    start = datetime(2026, 7, 14, tzinfo=UTC).timestamp()
    end = datetime(2026, 7, 15, tzinfo=UTC).timestamp()

    assert archive_file_name('a/b c', start, end) == 'a_b_c-2026-07-14_00-00__2026-07-15_00-00.db.gz'


# --- list_archives --------------------------------------------------


def test_list_archives_empty_when_folder_unset():
    assert list_archives('') == []


def test_list_archives_empty_when_folder_has_nothing(tmp_path):
    assert list_archives(str(tmp_path)) == []


def test_list_archives_parses_name_into_fields(tmp_path):
    start = datetime(2026, 7, 14, tzinfo=UTC).timestamp()
    end = datetime(2026, 7, 15, tzinfo=UTC).timestamp()
    name = archive_file_name(CLUSTER, start, end)
    (tmp_path / name).write_bytes(b'x' * 10)

    (info,) = list_archives(str(tmp_path))

    assert (info.name, info.cluster, info.from_ts, info.to_ts, info.size_bytes) == (name, CLUSTER, start, end, 10)


def test_list_archives_sorts_newest_first_by_to_ts(tmp_path):
    now = time.time()
    older = _make_archive(tmp_path, CLUSTER, now - 3 * DAY)
    newer = _make_archive(tmp_path, CLUSTER, now - DAY)

    assert [info.name for info in list_archives(str(tmp_path))] == [newer.name, older.name]


@pytest.mark.parametrize(
    'name',
    [
        f'.{CLUSTER}-2026-07-14_00-00__2026-07-15_00-00.db.gz.0123456789abcdef.tmp',
        f'{CLUSTER}-2026-07-14_00-00__2026-07-15_00-00.db.gz.unreadable',
        'worker-1-2026-07-14__00_00_00.db',
        f'{CLUSTER}-2026-13-14_00-00__2026-07-15_00-00.db.gz',
    ],
    ids=['dot-temp', 'unreadable', 'raw-db', 'impossible-date'],
)
def test_list_archives_lists_only_archive_names(tmp_path, name):
    (tmp_path / name).write_bytes(b'x')

    assert list_archives(str(tmp_path)) == []


# --- window assignment --------------------------------------------------


def _file_at(tmp_path, start: float) -> str:
    path = tmp_path / sealed_db_name('worker-1', start)
    path.write_bytes(b'')
    return str(path)


def test_assign_windows_assigns_a_late_evening_file_to_its_own_day(tmp_path):
    day = datetime(2026, 7, 14, tzinfo=UTC).timestamp()
    path = _file_at(tmp_path, day + 23 * HOUR + 59 * 60)

    (window,) = assign_windows([path], DAY, DAY, now=day + 3 * DAY)

    assert (window.start, window.end) == (day, day + DAY)


def test_assign_windows_returns_the_window_exactly_at_its_due_time(tmp_path):
    day = datetime(2026, 7, 14, tzinfo=UTC).timestamp()
    path = _file_at(tmp_path, day + HOUR)

    assert len(assign_windows([path], DAY, 2 * DAY, now=day + DAY + 2 * DAY)) == 1


def test_assign_windows_skips_the_window_one_second_before_it_is_due(tmp_path):
    day = datetime(2026, 7, 14, tzinfo=UTC).timestamp()
    path = _file_at(tmp_path, day + HOUR)

    assert assign_windows([path], DAY, 2 * DAY, now=day + DAY + 2 * DAY - 1) == []


def test_assign_windows_ignores_how_recently_a_sealed_file_was_written(tmp_path):
    """Every file in the history folder is sealed: no mtime check holds a window back."""
    day = datetime(2026, 7, 14, tzinfo=UTC).timestamp()
    path = _file_at(tmp_path, day + HOUR)
    os.utime(path, None)

    assert len(assign_windows([path], DAY, DAY, now=day + 3 * DAY)) == 1


def test_assign_windows_groups_separate_days_into_separate_windows(tmp_path):
    day = datetime(2026, 7, 14, tzinfo=UTC).timestamp()
    first = _file_at(tmp_path, day + HOUR)
    second = _file_at(tmp_path, day + DAY + HOUR)

    windows = assign_windows([second, first], DAY, DAY, now=day + 5 * DAY)

    assert [[c.path for c in w.files] for w in windows] == [[first], [second]]


# --- archive pass -------------------------------------------------------


async def _run(tmp_path, cfg=None, *, cluster=CLUSTER, merged_retention_days=0, worker='worker-1', stop=None) -> str:
    """Run one archive slot as the slot loop does; the slot's outcome.

    The slot takes the lease on the filesystem election; when it wins, the
    pass runs in a thread behind a fence, so the fence's checks run on this
    loop. The slot releases the lease at the end.
    """
    archive = cfg or _make_config()
    election = _filesystem_election(tmp_path, cluster, worker=worker)
    assert (await election.ensure_started(ACQUIRE_WINDOW_SECONDS)).ok
    lease = SlotLease(election, scope=task_scope(cluster, ARCHIVE_TASK_NAME), lease_seconds=archive.lease_seconds)
    outcome, _ = await lease.acquire(ACQUIRE_WINDOW_SECONDS)
    try:
        if outcome == SLOT_WON:
            fence = ThreadFence(lease, election.caller, asyncio.get_running_loop(), provider_name='filesystem')
            await asyncio.to_thread(
                functools.partial(
                    run_archive_pass,
                    str(tmp_path / 'shared'),
                    cluster=cluster,
                    instance=INSTANCE,
                    archive=archive,
                    merged_retention_days=merged_retention_days,
                    fence=fence,
                    stop=stop,
                )
            )
    finally:
        await lease.release()
        await election.stop(None)
    return outcome


def _run_fenced(tmp_path, fence: _ScriptedFence, cfg=None) -> None:
    """Run one pass directly, its lease checks answered by ``fence``."""
    run_archive_pass(
        str(tmp_path / 'shared'),
        cluster=CLUSTER,
        instance=INSTANCE,
        archive=cfg or _make_config(),
        merged_retention_days=0,
        fence=fence,  # type: ignore[arg-type]
    )


def _archives(folder: Path) -> list[str]:
    return sorted(p.name for p in folder.glob('*.db.gz'))


def _dotfiles(folder: Path) -> list[str]:
    return sorted(p.name for p in folder.iterdir() if p.name.startswith('.'))


async def test_run_archive_pass_merges_a_due_window_and_removes_sources_and_meta_files(tmp_path):
    folder = history_folder(tmp_path)
    now = time.time()
    start = due_start(now)
    first = make_sealed_db(folder, 'worker-1', start, event_count=2)
    second = make_sealed_db(folder, 'worker-2', start + HOUR, event_count=3)

    await _run(tmp_path)

    for source in (first, second):
        assert not Path(source).exists()
        assert not Path(meta_path(source)).exists()
    window_start = (start // DAY) * DAY
    assert _archives(folder) == [archive_file_name(CLUSTER, window_start, window_start + DAY)]
    assert _events_in_archive(folder / _archives(folder)[0]) == 5
    assert {row[0] for row in _archive_workers(folder / _archives(folder)[0])} == {'worker-1', 'worker-2'}
    assert _dotfiles(folder) == []


async def test_run_archive_pass_logs_the_created_archive(tmp_path):
    folder = history_folder(tmp_path)
    make_sealed_db(folder, 'worker-1', due_start(time.time()))

    with capture_logs() as cap:
        await _run(tmp_path)

    (created,) = [e for e in cap if e['event'] == 'recorder_archive_created']
    assert created['cluster'] == CLUSTER
    assert created['file_count'] == 1
    assert created['raw_bytes'] > 0
    assert created['compressed_bytes'] > 0


async def test_run_archive_pass_leaves_a_window_that_is_not_due(tmp_path):
    folder = history_folder(tmp_path)
    path = make_sealed_db(folder, 'worker-1', time.time() - HOUR)

    await _run(tmp_path)

    assert Path(path).exists()
    assert _archives(folder) == []
    assert not _lock_path(tmp_path).exists()


async def test_run_archive_pass_without_a_history_folder_creates_nothing(tmp_path):
    _run_fenced(tmp_path, _ScriptedFence(held=True))

    assert not (tmp_path / 'shared').exists()


async def test_run_archive_pass_never_takes_a_file_still_being_copied_in(tmp_path):
    folder = history_folder(tmp_path)
    start = due_start(time.time())
    make_sealed_db(folder, 'worker-1', start)
    incoming = folder / f'.{sealed_db_name("worker-2", start)}.0123456789abcdef.incoming'
    incoming.write_bytes(b'half a copy')

    await _run(tmp_path)

    assert incoming.exists()
    assert len(_archives(folder)) == 1


async def test_run_archive_pass_sets_aside_a_source_the_merge_cannot_read(tmp_path):
    folder = history_folder(tmp_path)
    start = due_start(time.time())
    healthy = make_sealed_db(folder, 'worker-1', start)
    corrupt = folder / sealed_db_name('worker-2', start + HOUR)
    corrupt.write_bytes(b'this file is not a SQLite database at all' * 8)

    with capture_logs() as cap:
        await _run(tmp_path)

    set_aside = folder / (corrupt.name + '.unreadable')
    assert not corrupt.exists()
    assert set_aside.read_bytes().startswith(b'this file is not a SQLite database')
    assert not Path(healthy).exists()
    assert len(_archives(folder)) == 1
    (skipped,) = [e for e in cap if e['event'] == 'recorder_archive_source_skipped']
    assert (skipped['name'], skipped['renamed_to']) == (corrupt.name, set_aside.name)


# --- fold-in --------------------------------------------------------------


def _events_in_archive(path: Path) -> int:
    """Unpack an archive and count the events it carries."""
    unpacked = path.parent.parent / f'unpacked-{path.name}.db'
    with gzip.open(path, 'rb') as gz:
        unpacked.write_bytes(gz.read())
    db = sqlite3.connect(str(unpacked))
    try:
        return db.execute('SELECT COUNT(*) FROM events').fetchone()[0]
    finally:
        db.close()
        unpacked.unlink()


def _archive_workers(path: Path) -> list[tuple]:
    """Unpack an archive; return (worker_name, source_file, event count) per worker."""
    unpacked = path.parent.parent / f'unpacked-{path.name}.db'
    with gzip.open(path, 'rb') as gz:
        unpacked.write_bytes(gz.read())
    db = sqlite3.connect(str(unpacked))
    try:
        return db.execute(
            'SELECT w.worker_name, w.source_file, COUNT(e.id) FROM workers w '
            'LEFT JOIN events e ON e.worker_id = w.id GROUP BY w.id ORDER BY w.worker_name'
        ).fetchall()
    finally:
        db.close()
        unpacked.unlink()


async def test_run_archive_pass_folds_a_published_archive_into_the_next_one(tmp_path):
    """A source that outlived its archive must not overwrite it."""
    folder = history_folder(tmp_path)
    start = due_start(time.time())
    first = make_sealed_db(folder, 'worker-1', start, event_count=2)
    make_sealed_db(folder, 'worker-2', start + HOUR, event_count=3)
    await _run(tmp_path)
    (name,) = _archives(folder)
    published = (folder / name).read_bytes()

    # A pass that died between publishing and deleting leaves a source behind.
    make_sealed_db(folder, 'worker-1', start, event_count=2)

    with capture_logs() as cap:
        await _run(tmp_path)

    assert not Path(first).exists()
    assert not Path(meta_path(first)).exists()
    assert (folder / name).read_bytes() == published
    assert [e['event'] for e in cap if e['event'].startswith('recorder_archive_')] == [
        'recorder_archive_sources_reclaimed'
    ]


async def test_run_archive_pass_adds_new_sources_to_a_published_archive(tmp_path):
    folder = history_folder(tmp_path)
    start = due_start(time.time())
    make_sealed_db(folder, 'worker-1', start, event_count=2)
    await _run(tmp_path)
    (name,) = _archives(folder)
    late = make_sealed_db(folder, 'worker-3', start + 2 * HOUR, event_count=4)

    await _run(tmp_path)

    assert not Path(late).exists()
    assert _archives(folder) == [name]
    assert _events_in_archive(folder / name) == 6


async def test_run_archive_pass_deletes_already_archived_sources_when_folding(tmp_path):
    """Sources skipped as already-archived are healthy — delete, never quarantine."""
    folder = history_folder(tmp_path)
    start = due_start(time.time())
    make_sealed_db(folder, 'worker-1', start, event_count=2)
    make_sealed_db(folder, 'worker-2', start + HOUR, event_count=3)
    await _run(tmp_path)
    (name,) = _archives(folder)
    make_sealed_db(folder, 'worker-1', start, event_count=2)
    make_sealed_db(folder, 'worker-2', start + HOUR, event_count=3)
    make_sealed_db(folder, 'worker-3', start + 2 * HOUR, event_count=4)

    with capture_logs() as cap:
        await _run(tmp_path)

    assert list(folder.glob('*.db')) == []
    assert list(folder.glob('*.unreadable')) == []
    assert [e for e in cap if e['event'] == 'recorder_archive_source_skipped'] == []
    assert _events_in_archive(folder / name) == 9


async def test_run_archive_pass_keeps_worker_attribution_across_folds(tmp_path):
    folder = history_folder(tmp_path)
    start = due_start(time.time())
    make_sealed_db(folder, 'worker-1', start, event_count=2)
    make_sealed_db(folder, 'worker-2', start + HOUR, event_count=3)
    await _run(tmp_path)
    (name,) = _archives(folder)
    make_sealed_db(folder, 'worker-3', start + 2 * HOUR, event_count=4)
    await _run(tmp_path)
    make_sealed_db(folder, 'worker-4', start + 3 * HOUR, event_count=1)
    await _run(tmp_path)

    assert _archives(folder) == [name]
    assert _archive_workers(folder / name) == [
        ('worker-1', sealed_db_name('worker-1', start), 2),
        ('worker-2', sealed_db_name('worker-2', start + HOUR), 3),
        ('worker-3', sealed_db_name('worker-3', start + 2 * HOUR), 4),
        ('worker-4', sealed_db_name('worker-4', start + 3 * HOUR), 1),
    ]
    assert _dotfiles(folder) == []


# --- the archive lock ---------------------------------------------------


async def test_an_archive_slot_held_by_another_worker_runs_no_pass(tmp_path):
    folder = history_folder(tmp_path)
    source = make_sealed_db(folder, 'worker-1', due_start(time.time()))
    holder = _other_holder(tmp_path)
    assert holder.acquire()
    try:
        assert await _run(tmp_path) == SLOT_HELD_ELSEWHERE

        assert Path(source).exists()
        assert _archives(folder) == []
        assert holder.is_ours()
    finally:
        holder.release()


async def test_run_archive_pass_takes_over_a_lock_left_by_a_dead_worker(tmp_path):
    folder = history_folder(tmp_path)
    source = make_sealed_db(folder, 'worker-1', due_start(time.time()))
    lock = _lock_path(tmp_path)
    lock.parent.mkdir(parents=True)
    lock.write_text(json.dumps({'acquired_at': '', 'instance': 'dead', 'token': 'dead', 'worker': 'worker-2'}))
    _age(lock, HistoryArchiveConfig().lease_seconds + 60)

    await _run(tmp_path)

    assert not Path(source).exists()
    assert len(_archives(folder)) == 1
    assert not lock.exists()


async def test_run_archive_pass_names_its_work_files_with_the_lock_token(tmp_path, monkeypatch):
    folder = history_folder(tmp_path)
    make_sealed_db(folder, 'worker-1', due_start(time.time()))
    seen: dict = {}
    real_merge = archive_mod.merge_databases

    def spy(paths, output_path):
        seen['output'] = os.path.basename(output_path)
        seen['token'] = json.loads(_lock_path(tmp_path).read_text())['token']
        return real_merge(paths, output_path)

    monkeypatch.setattr(archive_mod, 'merge_databases', spy)

    await _run(tmp_path)

    assert seen['output'].endswith(f'.{seen["token"]}.merge.db')
    assert re.fullmatch(r'[0-9a-f]{16}', seen['token'])


async def test_run_archive_pass_keeps_the_merge_temp_owner_only(tmp_path, monkeypatch):
    folder = history_folder(tmp_path)
    make_sealed_db(folder, 'worker-1', due_start(time.time()))
    seen: dict = {}
    real_compress = archive_mod._compress

    def spy(source_path, dest_path):
        seen['merge_mode'] = os.stat(source_path).st_mode & 0o777
        seen['archive_mode'] = os.stat(dest_path).st_mode & 0o777
        return real_compress(source_path, dest_path)

    monkeypatch.setattr(archive_mod, '_compress', spy)

    await _run(tmp_path)

    assert seen == {'merge_mode': 0o600, 'archive_mode': 0o600}
    assert os.stat(folder / _archives(folder)[0]).st_mode & 0o777 == 0o600


async def test_a_fenced_pass_publishes_nothing_and_deletes_nothing(tmp_path, monkeypatch):
    """A holder that lost its lock (it stalled past the stale age and a peer
    took over) must not publish or delete: the new holder owns the folder."""
    folder = history_folder(tmp_path)
    start = due_start(time.time())
    source = make_sealed_db(folder, 'worker-1', start)
    real_merge = archive_mod.merge_databases

    def taken_over_during_merge(paths, output_path):
        result = real_merge(paths, output_path)
        body = {'acquired_at': '', 'instance': 'peer', 'token': 'f' * 16, 'worker': 'worker-2'}
        _lock_path(tmp_path).write_text(json.dumps(body))
        return result

    monkeypatch.setattr(archive_mod, 'merge_databases', taken_over_during_merge)
    failures_before = recorder_archive_failures._value.get()

    with capture_logs() as cap:
        await _run(tmp_path)

    assert Path(source).exists()
    assert Path(meta_path(source)).exists()
    assert _archives(folder) == []
    assert _dotfiles(folder) == []
    assert json.loads(_lock_path(tmp_path).read_text())['token'] == 'f' * 16
    (fenced,) = [e for e in cap if e['event'] == 'recorder_archive_fenced']
    window_start = (start // DAY) * DAY
    assert fenced['name'] == archive_file_name(CLUSTER, window_start, window_start + DAY)
    assert recorder_archive_failures._value.get() == failures_before + 1


async def test_a_fenced_pass_names_the_provider(tmp_path, monkeypatch):
    folder = history_folder(tmp_path)
    make_sealed_db(folder, 'worker-1', due_start(time.time()))
    real_merge = archive_mod.merge_databases

    def taken_over_during_merge(paths, output_path):
        result = real_merge(paths, output_path)
        body = {
            'acquired_at': '',
            'instance': 'peer',
            'provider': 'filesystem',
            'token': 'f' * 16,
            'worker': 'worker-2',
        }
        _lock_path(tmp_path).write_text(json.dumps(body))
        return result

    monkeypatch.setattr(archive_mod, 'merge_databases', taken_over_during_merge)

    with capture_logs() as cap:
        await _run(tmp_path)

    (fenced,) = [e for e in cap if e['event'] == 'recorder_archive_fenced']
    assert (fenced['provider'], fenced['reason']) == ('filesystem', 'taken_over')
    assert fenced['detail'] == 'another worker took the archive lock over; this pass published and deleted nothing'


async def test_a_pass_whose_lease_cannot_be_confirmed_is_fenced_as_unconfirmed(tmp_path, monkeypatch):
    folder = history_folder(tmp_path)
    source = make_sealed_db(folder, 'worker-1', due_start(time.time()))

    async def no_answer(self, scope: str, token: str) -> bool:
        raise ValueError('the election gave no answer')

    monkeypatch.setattr(FilesystemElection, 'is_held', no_answer)

    with capture_logs() as cap:
        await _run(tmp_path)

    (fenced,) = [e for e in cap if e['event'] == 'recorder_archive_fenced']
    assert fenced['reason'] == 'unconfirmed'
    assert fenced['detail'] == (
        'the election provider could not confirm the lease; this pass published and deleted nothing'
    )
    assert Path(source).exists()
    assert _archives(folder) == []
    assert not _lock_path(tmp_path).exists()


async def test_run_archive_pass_fenced_logs_recorder_archive_fenced_and_counts_a_failure(tmp_path):
    folder = history_folder(tmp_path)
    start = due_start(time.time())
    source = make_sealed_db(folder, 'worker-1', start)
    failures_before = recorder_archive_failures._value.get()

    with capture_logs() as cap:
        _run_fenced(tmp_path, _ScriptedFence(held=False, reason='unconfirmed', provider_name='scripted-fence'))

    (fenced,) = [e for e in cap if e['event'] == 'recorder_archive_fenced']
    window_start = (start // DAY) * DAY
    assert fenced == {
        'event': 'recorder_archive_fenced',
        'log_level': 'warning',
        'category': 'recorder',
        'provider': 'scripted-fence',
        'reason': 'unconfirmed',
        'cluster': CLUSTER,
        'name': archive_file_name(CLUSTER, window_start, window_start + DAY),
        'detail': 'the election provider could not confirm the lease; this pass published and deleted nothing',
    }
    assert recorder_archive_failures._value.get() == failures_before + 1
    assert Path(source).exists()
    assert _archives(folder) == []
    assert _dotfiles(folder) == []


async def test_run_archive_pass_sweeps_with_sweep_after_seconds(tmp_path):
    folder = history_folder(tmp_path)
    old_work_file = folder / WORK_FILES[0]
    young_work_file = folder / WORK_FILES[3]
    for path in (old_work_file, young_work_file):
        path.write_bytes(b'left by a dead pass')
    _age(old_work_file, 700)
    _age(young_work_file, 500)

    _run_fenced(tmp_path, _ScriptedFence(held=True), _make_config(sweep_after_seconds=600))

    assert not old_work_file.exists()
    assert young_work_file.exists()


async def test_run_archive_pass_keeps_earlier_windows_when_a_later_one_fails(tmp_path, monkeypatch):
    folder = history_folder(tmp_path)
    now = time.time()
    first_start = due_start(now, days_back=4)
    second_start = due_start(now, days_back=3)
    first_source = make_sealed_db(folder, 'worker-1', first_start)
    second_source = make_sealed_db(folder, 'worker-1', second_start)
    real_merge = archive_mod.merge_databases
    calls = 0

    def flaky(paths, output_path):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError('second window blew up')
        return real_merge(paths, output_path)

    monkeypatch.setattr(archive_mod, 'merge_databases', flaky)

    await _run(tmp_path)

    assert not Path(first_source).exists()
    assert Path(second_source).exists()
    window_start = (first_start // DAY) * DAY
    assert _archives(folder) == [archive_file_name(CLUSTER, window_start, window_start + DAY)]


async def test_a_failed_window_is_logged_and_counted(tmp_path, monkeypatch):
    folder = history_folder(tmp_path)
    source = make_sealed_db(folder, 'worker-1', due_start(time.time()))

    def boom(paths, output_path):
        raise OSError('merge blew up')

    monkeypatch.setattr(archive_mod, 'merge_databases', boom)
    failures_before = recorder_archive_failures._value.get()

    with capture_logs() as cap:
        await _run(tmp_path)

    assert Path(source).exists()
    (failure,) = [e for e in cap if e['event'] == 'recorder_archive_failed']
    assert failure['cluster'] == CLUSTER
    assert recorder_archive_failures._value.get() == failures_before + 1


async def test_a_clean_pass_counts_no_failure(tmp_path):
    folder = history_folder(tmp_path)
    make_sealed_db(folder, 'worker-1', due_start(time.time()))
    failures_before = recorder_archive_failures._value.get()

    await _run(tmp_path)

    assert recorder_archive_failures._value.get() == failures_before


async def test_a_stop_ends_the_pass_after_the_current_window(tmp_path, monkeypatch):
    folder = history_folder(tmp_path)
    now = time.time()
    first_start = due_start(now, days_back=4)
    first_source = make_sealed_db(folder, 'worker-1', first_start)
    second_source = make_sealed_db(folder, 'worker-1', due_start(now, days_back=3))
    stop = threading.Event()
    real_merge = archive_mod.merge_databases

    def merge_then_stop(paths, output_path):
        # The worker starts its shutdown while the first window merges.
        stop.set()
        return real_merge(paths, output_path)

    monkeypatch.setattr(archive_mod, 'merge_databases', merge_then_stop)
    failures_before = recorder_archive_failures._value.get()

    await _run(tmp_path, stop=stop)

    window_start = (first_start // DAY) * DAY
    assert _archives(folder) == [archive_file_name(CLUSTER, window_start, window_start + DAY)]
    assert not Path(first_source).exists()
    assert Path(second_source).exists()
    assert recorder_archive_failures._value.get() == failures_before


async def test_run_archive_pass_keeps_sources_when_compression_fails(tmp_path, monkeypatch):
    folder = history_folder(tmp_path)
    source = make_sealed_db(folder, 'worker-1', due_start(time.time()))

    def boom(*args, **kwargs):
        raise OSError('disk full')

    monkeypatch.setattr(archive_mod.gzip, 'open', boom)

    await _run(tmp_path)

    assert Path(source).exists()
    assert _archives(folder) == []
    assert _dotfiles(folder) == []


# --- sweep ---------------------------------------------------------------


WORK_FILES = [
    f'.{CLUSTER}-2020-01-01_00-00__2020-01-02_00-00.db.gz.0123456789abcdef.merge.db',
    f'.{CLUSTER}-2020-01-01_00-00__2020-01-02_00-00.db.gz.0123456789abcdef.merge.db-journal',
    f'.{CLUSTER}-2020-01-01_00-00__2020-01-02_00-00.db.gz.0123456789abcdef.merge.db-wal',
    f'.{CLUSTER}-2020-01-01_00-00__2020-01-02_00-00.db.gz.0123456789abcdef.tmp',
    f'.{CLUSTER}-2020-01-01_00-00__2020-01-02_00-00.db.gz.0123456789abcdef.previous.db',
    '.worker-2-2020-01-01__00_00_00.db.0123456789abcdef.incoming',
    '.clock.2222222222222222',
]


@pytest.mark.parametrize('name', WORK_FILES)
async def test_the_sweep_removes_a_work_file_older_than_the_sweep_age(tmp_path, name):
    folder = history_folder(tmp_path)
    work_file = folder / name
    work_file.write_bytes(b'left by a dead pass')
    _age(work_file, 101)

    with capture_logs() as cap:
        await _run(tmp_path, _make_config(sweep_after_seconds=100))

    assert not work_file.exists()
    assert [e['name'] for e in cap if e['event'] == 'recorder_archive_temp_swept'] == [name]


async def test_history_sweep_still_removes_old_archive_lock_stale_copies(tmp_path):
    # Workers before the task lock family set a taken-over lock aside in the
    # history folder; the sweep still cleans those copies up. The old lock
    # itself stays until an operator deletes it.
    folder = history_folder(tmp_path)
    stale_copy = folder / '.archive.lock.stale.0123456789abcdef'
    old_lock = folder / '.archive.lock'
    for path in (stale_copy, old_lock):
        path.write_bytes(b'left by an older worker')
        _age(path, 101)

    with capture_logs() as cap:
        await _run(tmp_path, _make_config(sweep_after_seconds=100))

    assert not stale_copy.exists()
    assert old_lock.exists()
    assert [e['name'] for e in cap if e['event'] == 'recorder_archive_temp_swept'] == [stale_copy.name]


@pytest.mark.parametrize('name', WORK_FILES)
async def test_the_sweep_keeps_a_work_file_younger_than_the_sweep_age(tmp_path, name):
    folder = history_folder(tmp_path)
    work_file = folder / name
    work_file.write_bytes(b'still being written')
    _age(work_file, 50)

    await _run(tmp_path, _make_config(sweep_after_seconds=100))

    assert work_file.exists()


async def test_the_sweep_removes_an_old_meta_file_whose_database_is_missing(tmp_path):
    folder = history_folder(tmp_path)
    orphan = folder / 'worker-2-2020-01-01__00_00_00.db.meta.json'
    orphan.write_bytes(b'{"schema":1}')
    _age(orphan, 101)

    await _run(tmp_path, _make_config(sweep_after_seconds=100))

    assert not orphan.exists()


async def test_the_sweep_keeps_a_fresh_meta_file_whose_database_is_still_on_its_way(tmp_path):
    """Sealing writes the meta file first and moves the database next."""
    folder = history_folder(tmp_path)
    orphan = folder / 'worker-2-2020-01-01__00_00_00.db.meta.json'
    orphan.write_bytes(b'{"schema":1}')
    _age(orphan, 50)

    await _run(tmp_path, _make_config(sweep_after_seconds=100))

    assert orphan.exists()


async def test_the_sweep_keeps_an_old_meta_file_next_to_its_database(tmp_path):
    folder = history_folder(tmp_path)
    source = make_sealed_db(folder, 'worker-1', time.time() - HOUR)
    _age(Path(meta_path(source)), 101)

    await _run(tmp_path, _make_config(sweep_after_seconds=100))

    assert Path(meta_path(source)).exists()


# --- expiry ---------------------------------------------------------------


async def test_run_archive_pass_expires_only_this_clusters_old_archives(tmp_path):
    folder = history_folder(tmp_path)
    now = time.time()
    make_sealed_db(folder, 'worker-1', due_start(now, days_back=3))
    old_mine = _make_archive(folder, CLUSTER, now - 10 * DAY)
    recent_mine = _make_archive(folder, CLUSTER, now - 1 * DAY)
    old_theirs = _make_archive(folder, OTHER_CLUSTER, now - 10 * DAY)

    with capture_logs() as cap:
        await _run(tmp_path, _make_config(retention_days=3))

    assert not old_mine.exists()
    assert recent_mine.exists()
    assert old_theirs.exists()
    assert len(_archives(folder)) == 3
    assert [e['name'] for e in cap if e['event'] == 'recorder_archive_expired'] == [old_mine.name]


async def test_run_archive_pass_keeps_every_archive_when_retention_is_zero(tmp_path):
    folder = history_folder(tmp_path)
    old_mine = _make_archive(folder, CLUSTER, time.time() - 100 * DAY)

    await _run(tmp_path, _make_config(retention_days=0))

    assert old_mine.exists()


# --- merged results -------------------------------------------------------


def _merged_file(tmp_path, name: str, age_days: float) -> Path:
    folder = tmp_path / 'shared' / 'merged'
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / name
    path.write_bytes(b'merged')
    _age(path, age_days * DAY)
    return path


async def test_run_archive_pass_prunes_merge_results_past_their_retention(tmp_path):
    history_folder(tmp_path)
    old = _merged_file(tmp_path, 'merged-2026-09-01__10_00_00-0123456789abcdef.db', 40)
    young = _merged_file(tmp_path, 'merged-2026-10-10__10_00_00-0123456789abcdef.db', 3)
    in_progress = _merged_file(tmp_path, '.merged-2026-09-01__10_00_00-0123456789abcdef.tmp', 40)

    with capture_logs() as cap:
        await _run(tmp_path, merged_retention_days=32)

    assert not old.exists()
    assert young.exists()
    assert in_progress.exists()
    assert [e['name'] for e in cap if e['event'] == 'recorder_merged_pruned'] == [old.name]


async def test_run_archive_pass_keeps_merge_results_when_their_retention_is_zero(tmp_path):
    history_folder(tmp_path)
    old = _merged_file(tmp_path, 'merged-2026-01-01__10_00_00-0123456789abcdef.db', 300)

    await _run(tmp_path, merged_retention_days=0)

    assert old.exists()


# --- archive footprint --------------------------------------------------


async def test_archive_pass_reports_only_this_clusters_footprint(tmp_path):
    folder = history_folder(tmp_path)
    now = time.time()
    mine_a = _make_archive(folder, CLUSTER, now - 3 * DAY)
    mine_b = _make_archive(folder, CLUSTER, now - 2 * DAY)
    extended = _make_archive(folder, f'{CLUSTER}-hot', now - 2 * DAY)
    extended.write_bytes(b'x' * 4096)

    await _run(tmp_path, _make_config(retention_days=0))

    assert archive_mod.recorder_archive_files._value.get() == 2
    assert archive_mod.recorder_archive_bytes._value.get() == mine_a.stat().st_size + mine_b.stat().st_size


async def test_archive_pass_reports_a_zero_footprint_without_a_folder(tmp_path):
    archive_mod.recorder_archive_bytes.set(999)
    archive_mod.recorder_archive_files.set(9)

    await _run(tmp_path, _make_config(retention_days=0))

    assert archive_mod.recorder_archive_files._value.get() == 0
    assert archive_mod.recorder_archive_bytes._value.get() == 0


# --- unbounded-retention warning ----------------------------------------


def test_warn_if_archives_unbounded_warns_when_retention_is_zero():
    with capture_logs() as cap:
        warn_if_archives_unbounded(_make_config(retention_days=0), '/shared/history/search')

    (warning,) = [e for e in cap if e['event'] == 'recorder_archives_unbounded']
    assert warning['log_level'] == 'warning'
    assert warning['folder'] == '/shared/history/search'
    assert 'history.archive.retention_days' in warning['message']
    assert 'drakkar_recorder_archive_bytes' in warning['message']


@pytest.mark.parametrize(
    'cfg',
    [_make_config(retention_days=30), _make_config(enabled=False, retention_days=0), HistoryArchiveConfig()],
    ids=['retention-set', 'archiving-disabled', 'defaults'],
)
def test_warn_if_archives_unbounded_is_silent_otherwise(cfg):
    with capture_logs() as cap:
        warn_if_archives_unbounded(cfg, '/shared/history/search')

    assert [e for e in cap if e['event'] == 'recorder_archives_unbounded'] == []


# --- the recorder's archive task ----------------------------------------


def _make_recorder(tmp_path, *, archive: HistoryArchiveConfig | None = None) -> EventRecorder:
    """A recorder of ``worker-1`` in ``CLUSTER``."""
    (tmp_path / 'storage').mkdir(exist_ok=True)
    config = make_ui_config(enabled=True, flush_interval_seconds=60)
    return EventRecorder(
        config,
        worker_name='worker-1',
        cluster_name=CLUSTER,
        storage_dir=str(tmp_path / 'storage'),
        shared_dir=str(tmp_path / 'shared'),
        instance=INSTANCE,
        archive=archive,
        merged_retention_days=7,
    )


def _memory_election(provider_name: str = 'memory') -> tuple[ClusterElection, MemoryElection]:
    """The worker's election service over an in-memory provider, and the provider."""
    provider = MemoryElection(ElectionStore())
    context = ElectionContext(
        worker='worker-1', instance=INSTANCE, cluster=CLUSTER, shared_dir='', logger=structlog.get_logger(), options={}
    )
    caller = ResilientCaller(ResilienceConfig(), feature='election', provider=provider_name, category='periodic')
    return ClusterElection(provider, caller, provider_name=provider_name, context=context), provider


async def _start_won_archive_slot(rec: EventRecorder, election: ClusterElection) -> asyncio.Task:
    """Take the archive slot's lease and run its body in a task, as a won slot does; the lease is not released."""
    archive_task = rec.archive_cluster_task()
    assert archive_task is not None
    assert (await election.ensure_started(ACQUIRE_WINDOW_SECONDS)).ok
    lease = SlotLease(election, scope=task_scope(CLUSTER, ARCHIVE_TASK_NAME), lease_seconds=archive_task.lease_seconds)
    assert (await lease.acquire(ACQUIRE_WINDOW_SECONDS))[0] == SLOT_WON
    return asyncio.create_task(archive_task.run(SlotContext(slot=0, lease=lease)))


def test_the_archive_task_is_the_system_cluster_task_history_archive(tmp_path):
    archive = HistoryArchiveConfig(cron='0 * * * *', lease_seconds=900)
    archive_task = _make_recorder(tmp_path, archive=archive).archive_cluster_task()

    assert archive_task is not None
    assert (archive_task.name, archive_task.system, archive_task.on_error) == (ARCHIVE_TASK_NAME, True, 'continue')
    assert (archive_task.interval, archive_task.cron, archive_task.lease_seconds) == (None, '0 * * * *', 900)
    assert _make_recorder(tmp_path).archive_cluster_task().interval == 3600


async def test_archive_slot_won_runs_the_pass_with_a_fence_and_records_history_archive_row(tmp_path, monkeypatch):
    archive = HistoryArchiveConfig(lease_seconds=900)
    rec = _make_recorder(tmp_path, archive=archive)
    election, _provider = _memory_election('memory-won')
    seen: dict = {}
    rows: list[dict] = []

    def spy(shared_dir, **kwargs) -> None:
        seen.update(kwargs, shared_dir=shared_dir, thread=threading.current_thread().ident)
        # Checked from the pass's own thread, as the pass checks before each publish.
        seen['held'] = kwargs['fence'].is_held()

    monkeypatch.setattr(core_mod, 'run_archive_pass', spy)
    monkeypatch.setattr(rec, 'record_periodic_run', lambda *args, **row: rows.append(row))
    archive_task = rec.archive_cluster_task()
    assert archive_task is not None
    # A whole hour, so the first slot of the 3600 s default is the next second.
    slot = 1_800_000_000
    clock = SteppingClock(slot - 1, limit=slot + MIN_HOLD_SECONDS)

    loop_task = asyncio.create_task(
        run_cluster_task(archive_task, election, cluster=CLUSTER, recorder=rec, clock=clock)
    )
    try:
        await wait_for(lambda: bool(rows))
    finally:
        loop_task.cancel()
        await asyncio.wait({loop_task})
        await rec.stop()

    assert seen['shared_dir'] == str(tmp_path / 'shared')
    assert (seen['cluster'], seen['instance']) == (CLUSTER, INSTANCE)
    assert seen['archive'] is rec._archive
    assert seen['merged_retention_days'] == 7
    assert seen['stop'] is rec._archive_stop
    assert seen['thread'] != threading.current_thread().ident
    fence = seen['fence']
    assert isinstance(fence, ThreadFence)
    assert (fence.provider_name, seen['held']) == ('memory-won', True)
    (row,) = rows
    assert (row['name'], row['status'], row['system'], row['slot']) == (ARCHIVE_TASK_NAME, 'ok', True, slot)


async def test_archive_slot_cancelled_returns_at_once_and_the_pass_goes_on(tmp_path, monkeypatch):
    in_window = threading.Event()
    window_may_end = threading.Event()

    def stuck_pass(shared_dir, **kwargs) -> None:
        in_window.set()
        window_may_end.wait(WAIT_FOR_TIMEOUT_SECONDS)

    monkeypatch.setattr(core_mod, 'run_archive_pass', stuck_pass)
    rec = _make_recorder(tmp_path)
    election, _provider = _memory_election()
    slot_run = await _start_won_archive_slot(rec, election)
    await wait_for(in_window.is_set)

    slot_run.cancel()
    await asyncio.wait_for(asyncio.wait({slot_run}), timeout=WAIT_FOR_TIMEOUT_SECONDS)
    pass_future = rec._archive_pass_future
    try:
        assert slot_run.cancelled()
        assert pass_future is not None
        assert not pass_future.done()
    finally:
        window_may_end.set()
        await rec.stop()
    assert pass_future.done()


@pytest.mark.parametrize('outcome', [SLOT_HELD_ELSEWHERE, SLOT_LATE, SLOT_ELECTION_FAILED])
async def test_archive_slot_not_won_observes_the_footprint_gauges(tmp_path, outcome):
    folder = history_folder(tmp_path)
    existing = _make_archive(folder, CLUSTER, time.time() - 2 * DAY)
    archive_mod.recorder_archive_bytes.set(0)
    archive_mod.recorder_archive_files.set(0)
    rec = _make_recorder(tmp_path)
    archive_task = rec.archive_cluster_task()
    assert archive_task is not None and archive_task.on_skip is not None

    archive_task.on_skip(outcome)
    footprint = rec._footprint_future
    assert footprint is not None
    try:
        await asyncio.wrap_future(footprint)
    finally:
        await rec.stop()

    assert archive_mod.recorder_archive_files._value.get() == 1
    assert archive_mod.recorder_archive_bytes._value.get() == existing.stat().st_size


async def test_archive_footprint_is_not_queued_twice_while_a_pass_runs(tmp_path, monkeypatch):
    in_window = threading.Event()
    window_may_end = threading.Event()
    observing = threading.Event()
    observation_may_end = threading.Event()
    observations: list[tuple[str, str]] = []

    def stuck_pass(shared_dir, **kwargs) -> None:
        in_window.set()
        window_may_end.wait(WAIT_FOR_TIMEOUT_SECONDS)

    def slow_observation(shared_dir: str, cluster: str) -> None:
        observations.append((shared_dir, cluster))
        observing.set()
        observation_may_end.wait(WAIT_FOR_TIMEOUT_SECONDS)

    monkeypatch.setattr(core_mod, 'run_archive_pass', stuck_pass)
    monkeypatch.setattr(core_mod, 'observe_archive_footprint', slow_observation)
    rec = _make_recorder(tmp_path)
    archive_task = rec.archive_cluster_task()
    assert archive_task is not None and archive_task.on_skip is not None
    election, _provider = _memory_election()
    slot_run = await _start_won_archive_slot(rec, election)
    await wait_for(in_window.is_set)
    try:
        # A pass runs: the skip of another task's slot queues nothing behind it.
        archive_task.on_skip(SLOT_HELD_ELSEWHERE)
        assert rec._footprint_future is None
        window_may_end.set()
        await slot_run
        archive_task.on_skip(SLOT_HELD_ELSEWHERE)
        await wait_for(observing.is_set)
        # An observation runs: a second skip queues no second one.
        archive_task.on_skip(SLOT_LATE)
    finally:
        window_may_end.set()
        observation_may_end.set()
        await rec.stop()

    assert observations == [(str(tmp_path / 'shared'), CLUSTER)]


async def test_a_failed_footprint_observation_is_logged(tmp_path, monkeypatch):
    def broken_observation(shared_dir: str, cluster: str) -> None:
        raise OSError('the mount went away')

    monkeypatch.setattr(core_mod, 'observe_archive_footprint', broken_observation)
    rec = _make_recorder(tmp_path)
    archive_task = rec.archive_cluster_task()
    assert archive_task is not None and archive_task.on_skip is not None

    with capture_logs() as logs:
        archive_task.on_skip(SLOT_HELD_ELSEWHERE)
        footprint = rec._footprint_future
        assert footprint is not None
        await wait_for(footprint.done)
        await rec.stop()

    (failed,) = [entry for entry in logs if entry['event'] == 'recorder_background_job_failed']
    assert (failed['job'], failed['error']) == ('archive_footprint', 'the mount went away')


async def test_no_footprint_observation_after_the_recorder_stopped(tmp_path):
    rec = _make_recorder(tmp_path)
    archive_task = rec.archive_cluster_task()
    assert archive_task is not None and archive_task.on_skip is not None
    await rec.stop()

    archive_task.on_skip(SLOT_HELD_ELSEWHERE)

    assert rec._footprint_future is None


async def test_an_archive_slot_won_after_the_recorder_stopped_runs_no_pass(tmp_path, monkeypatch):
    passes: list[dict] = []
    monkeypatch.setattr(core_mod, 'run_archive_pass', lambda shared_dir, **kwargs: passes.append(kwargs))
    rec = _make_recorder(tmp_path)
    archive_task = rec.archive_cluster_task()
    assert archive_task is not None
    election, _provider = _memory_election()
    await rec.stop()

    await run_won_slot(archive_task, election)

    assert passes == []
    assert rec._archive_executor is None


async def test_the_archive_pass_needs_no_default_executor_thread_of_its_own(tmp_path):
    # The pass waits on provider calls and log writes that run in the default
    # executor; with one worker there, the pass must not be sitting in it.
    folder = history_folder(tmp_path)
    source = make_sealed_db(folder, 'worker-1', due_start(time.time()))
    one_thread = ThreadPoolExecutor(max_workers=1)
    asyncio.get_running_loop().set_default_executor(one_thread)
    rec = _make_recorder(tmp_path)
    election = _filesystem_election(tmp_path)
    archive_task = rec.archive_cluster_task()
    assert archive_task is not None
    try:
        await asyncio.wait_for(run_won_slot(archive_task, election), timeout=WAIT_FOR_TIMEOUT_SECONDS)
    finally:
        await rec.stop()
        await election.stop(None)
        one_thread.shutdown(wait=True)

    assert len(_archives(folder)) == 1
    assert not Path(source).exists()
    assert not _lock_path(tmp_path).exists()


class _FakeClock:
    """A replacement for the recorder's sleep: each sleep waits for a tick the test gives.

    ``entered`` counts the sleeps begun, which tells a test that the loop has
    handled the last tick and is waiting again.
    """

    def __init__(self) -> None:
        self._gates: dict[float, asyncio.Queue] = {}
        self.entered: dict[float, int] = {}

    async def sleep(self, seconds: float) -> None:
        self.entered[seconds] = self.entered.get(seconds, 0) + 1
        await self._gates.setdefault(seconds, asyncio.Queue()).get()

    async def tick(self, seconds: float) -> None:
        """Wake one sleep of this length and wait until its loop sleeps again."""
        await wait_for(lambda: self.entered.get(seconds, 0) >= 1)
        before = self.entered[seconds]
        self._gates.setdefault(seconds, asyncio.Queue()).put_nowait(None)
        await wait_for(lambda: self.entered[seconds] > before)


async def test_rotation_starts_no_archive_pass(tmp_path, monkeypatch):
    clock = _FakeClock()
    rec = _make_recorder(tmp_path, archive=HistoryArchiveConfig(enabled=True))
    rec._sleep = clock.sleep
    counts = {'rotations': 0, 'sealing': 0, 'passes': 0}

    async def rotate() -> None:
        counts['rotations'] += 1

    monkeypatch.setattr(rec, '_rotate', rotate)
    monkeypatch.setattr(rec, '_start_sealing', lambda: counts.__setitem__('sealing', counts['sealing'] + 1))
    monkeypatch.setattr(
        core_mod, 'run_archive_pass', lambda *a, **k: counts.__setitem__('passes', counts['passes'] + 1)
    )
    await rec.start()
    try:
        await clock.tick(rec._store.rotation_interval_hours * 3600)

        assert counts == {'rotations': 1, 'sealing': 1, 'passes': 0}
        assert rec._archive_executor is None
    finally:
        await rec.stop()


async def test_a_recorder_stop_asks_a_running_archive_pass_to_end(tmp_path, monkeypatch):
    started = threading.Event()
    seen: dict = {}

    def watching_pass(*args, stop=None, **kwargs) -> None:
        seen['stop'] = stop
        started.set()
        if stop is not None:
            stop.wait(10)

    monkeypatch.setattr(core_mod, 'run_archive_pass', watching_pass)
    rec = _make_recorder(tmp_path)
    election, _provider = _memory_election()
    await rec.start()
    slot_run = await _start_won_archive_slot(rec, election)
    try:
        await wait_for(started.is_set)
    finally:
        await rec.stop()
    await asyncio.wait_for(slot_run, timeout=WAIT_FOR_TIMEOUT_SECONDS)

    assert seen['stop'] is not None
    assert seen['stop'].is_set()


# --- the stop's wait for a pass in progress ------------------------------


async def test_a_pass_that_fails_after_stop_asked_it_to_end_is_logged(tmp_path, monkeypatch):
    in_window = threading.Event()

    def failing_pass(shared_dir, *, stop, **kwargs) -> None:
        in_window.set()
        stop.wait(WAIT_FOR_TIMEOUT_SECONDS)
        raise RuntimeError('window merge failed')

    monkeypatch.setattr(core_mod, 'run_archive_pass', failing_pass)
    rec = _make_recorder(tmp_path)
    election, _provider = _memory_election()
    await rec.start()
    slot_run = await _start_won_archive_slot(rec, election)
    await wait_for(in_window.is_set)
    # The worker cancels its slot loops before it stops the recorder.
    slot_run.cancel()
    await asyncio.wait({slot_run})

    with capture_logs() as logs:
        await rec.stop(timeout=WAIT_FOR_TIMEOUT_SECONDS)

    (failed,) = [entry for entry in logs if entry['event'] == 'recorder_background_job_failed']
    assert (failed['job'], failed['error']) == ('archive', 'window merge failed')


def _stuck_pass_in_window(monkeypatch) -> tuple[threading.Event, threading.Event]:
    """Install an archive pass whose window does not end when stop() asks, like a merge on a slow mount.

    Returns the event the pass sets once it is in its window, and the event
    that lets the window end.
    """
    in_window = threading.Event()
    window_may_end = threading.Event()

    def stuck_pass(shared_dir, **kwargs) -> None:
        in_window.set()
        window_may_end.wait(WAIT_FOR_TIMEOUT_SECONDS)

    monkeypatch.setattr(core_mod, 'run_archive_pass', stuck_pass)
    return in_window, window_may_end


async def _recorder_with_a_stuck_pass(tmp_path, monkeypatch) -> tuple[EventRecorder, threading.Event]:
    """A started recorder whose archive pass is stuck in a window and whose slot loop is cancelled, as at shutdown."""
    in_window, window_may_end = _stuck_pass_in_window(monkeypatch)
    rec = _make_recorder(tmp_path)
    election, _provider = _memory_election()
    await rec.start()
    slot_run = await _start_won_archive_slot(rec, election)
    await wait_for(in_window.is_set)
    slot_run.cancel()
    await asyncio.wait({slot_run})
    return rec, window_may_end


async def test_a_pass_that_outlives_the_shutdown_time_is_left_and_logged(tmp_path, monkeypatch):
    rec, window_may_end = await _recorder_with_a_stuck_pass(tmp_path, monkeypatch)

    try:
        started = time.monotonic()
        with capture_logs() as logs:
            await rec.stop(timeout=0.3)
        stop_seconds = time.monotonic() - started
    finally:
        window_may_end.set()

    assert stop_seconds < 0.9
    assert [entry['event'] for entry in logs].count('recorder_archive_pass_abandoned') == 1
    # The abandoned pass's thread must not outlive the test.
    pass_future = rec._archive_pass_future
    assert pass_future is not None
    await wait_for(pass_future.done)


async def test_the_stop_waits_for_a_pass_in_progress_until_only_the_reserve_is_left(tmp_path, monkeypatch):
    rec, window_may_end = await _recorder_with_a_stuck_pass(tmp_path, monkeypatch)

    try:
        started = time.monotonic()
        with capture_logs() as logs:
            await rec.stop(timeout=1.0, reserve_seconds=0.5)
        stop_seconds = time.monotonic() - started
    finally:
        window_may_end.set()

    # The wait ended when 0.5 s of the 1 s were left, not with the shutdown time.
    assert stop_seconds < 1.0
    (abandoned,) = [entry for entry in logs if entry['event'] == 'recorder_archive_pass_abandoned']
    assert abandoned['category'] == 'recorder'
    # At most the time left less the reserve; the seal before the wait took a little of it.
    assert 0.2 <= abandoned['waited_seconds'] <= 0.55
    assert abandoned['detail'] == 'the pass is fenced at its next check and its lease expires on its own'
    pass_future = rec._archive_pass_future
    assert pass_future is not None
    await wait_for(pass_future.done)


async def test_a_reserve_larger_than_the_time_left_abandons_the_pass_at_once(tmp_path, monkeypatch):
    rec, window_may_end = await _recorder_with_a_stuck_pass(tmp_path, monkeypatch)

    try:
        with capture_logs() as logs:
            await rec.stop(timeout=1.0, reserve_seconds=5.0)
    finally:
        window_may_end.set()

    (abandoned,) = [entry for entry in logs if entry['event'] == 'recorder_archive_pass_abandoned']
    assert abandoned['waited_seconds'] < 0.1
    pass_future = rec._archive_pass_future
    assert pass_future is not None
    await wait_for(pass_future.done)


async def test_a_stop_without_a_deadline_waits_until_the_pass_in_progress_ends(tmp_path, monkeypatch):
    rec, window_may_end = await _recorder_with_a_stuck_pass(tmp_path, monkeypatch)

    with capture_logs() as logs:
        stopping = asyncio.create_task(rec.stop(reserve_seconds=5.0))
        await asyncio.sleep(0.3)
        # Still waiting for the window: no deadline, so no reserve to keep.
        assert not stopping.done()
        window_may_end.set()
        await asyncio.wait_for(stopping, timeout=WAIT_FOR_TIMEOUT_SECONDS)

    assert 'recorder_archive_pass_abandoned' not in [entry['event'] for entry in logs]
    pass_future = rec._archive_pass_future
    assert pass_future is not None and pass_future.done()


async def test_start_notifies_once_when_archiving_is_disabled(tmp_path):
    rec = _make_recorder(tmp_path, archive=HistoryArchiveConfig(enabled=False))

    with capture_logs() as cap:
        await rec.start()
        await rec.stop()

    (notice,) = [e for e in cap if e['event'] == 'recorder_archiving_disabled']
    assert notice['log_level'] == 'info'
    assert notice['folder'] == rec.history_dir


async def test_start_stays_quiet_when_archiving_is_enabled(tmp_path):
    rec = _make_recorder(tmp_path)

    with capture_logs() as cap:
        await rec.start()
        await rec.stop()

    assert [e for e in cap if e['event'] == 'recorder_archiving_disabled'] == []


async def test_sealed_sources_are_archived_while_a_reader_holds_a_lock(tmp_path):
    """Sources are read with immutable=1: a stray reader's lock does not stall the window."""
    folder = history_folder(tmp_path)
    source = make_sealed_db(folder, 'worker-1', due_start(time.time()))
    reader = sqlite3.connect(source)
    reader.execute('BEGIN EXCLUSIVE')
    try:
        await _run(tmp_path)
    finally:
        reader.rollback()
        reader.close()

    assert len(_archives(folder)) == 1
