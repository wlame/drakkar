"""Tests for the recorder archive engine: window math and the archive pass.

The pass works on one cluster history folder, ``<shared>/history/<cluster>/``,
where every ``*.db`` is a sealed file with a meta file beside it. One worker
of the cluster holds ``.archive.lock`` while it archives.
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
from typing import Any

import pytest
import structlog
from structlog.testing import capture_logs

from drakkar.config import BreakerConfig, ConnectionsConfig, HistoryArchiveConfig, ResilienceConfig
from drakkar.metrics import recorder_archive_failures
from drakkar.providers.connections import Connections
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
from drakkar.recorder.archive_lock import LOCK_NAME, ArchiveLock
from drakkar.recorder.election import ElectionContext, ElectionLease, FilesystemElection
from drakkar.recorder.history import history_dir, meta_path, sanitize_cluster
from tests.archive_helpers import (
    CLUSTER,
    DAY,
    HOUR,
    due_start,
    election_calls,
    history_folder,
    make_sealed_db,
    sealed_db_name,
)
from tests.conftest import WAIT_FOR_TIMEOUT_SECONDS, make_ui_config, wait_for
from tests.fleet_fakes import ElectionStore, MemoryElection

OTHER_CLUSTER = 'index-fleet'
INSTANCE = '1111111111111111'


async def make_lease(
    tmp_path,
    cluster: str = CLUSTER,
    *,
    worker: str = 'worker-1',
    lease_seconds: float = HistoryArchiveConfig().lock_stale_seconds,
) -> ElectionLease:
    """A lease on the filesystem election of ``cluster``, its calls run on the test's event loop."""
    election = FilesystemElection()
    await election.start(
        ElectionContext(
            worker=worker,
            instance=INSTANCE,
            cluster=cluster,
            shared_dir=str(tmp_path / 'shared'),
            logger=structlog.get_logger(),
            options={},
        )
    )
    caller = ResilientCaller(ResilienceConfig(), feature='election', provider='filesystem', category='recorder')
    return ElectionLease(
        election,
        caller,
        asyncio.get_running_loop(),
        scope=sanitize_cluster(cluster),
        lease_seconds=lease_seconds,
        provider_name='filesystem',
    )


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


async def _run(tmp_path, cfg=None, *, cluster=CLUSTER, merged_retention_days=0, worker='worker-1', stop=None):
    """Run one pass in a thread, as the recorder does, so its lease can call the provider on this loop."""
    archive = cfg or _make_config()
    lease = await make_lease(tmp_path, cluster, worker=worker, lease_seconds=archive.lock_stale_seconds)
    await asyncio.to_thread(
        functools.partial(
            run_archive_pass,
            str(tmp_path / 'shared'),
            cluster=cluster,
            instance=INSTANCE,
            archive=archive,
            merged_retention_days=merged_retention_days,
            lease=lease,
            stop=stop,
        )
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
    assert not (folder / LOCK_NAME).exists()


async def test_run_archive_pass_without_a_history_folder_creates_nothing(tmp_path):
    await _run(tmp_path)

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


async def test_run_archive_pass_skips_while_another_worker_holds_the_lock(tmp_path):
    folder = history_folder(tmp_path)
    source = make_sealed_db(folder, 'worker-1', due_start(time.time()))
    holder = ArchiveLock(str(folder), worker='worker-2', instance='2222222222222222', stale_seconds=7200)
    assert holder.acquire()
    try:
        await _run(tmp_path)

        assert Path(source).exists()
        assert _archives(folder) == []
        assert holder.is_ours()
    finally:
        holder.release()


async def test_run_archive_pass_takes_over_a_lock_left_by_a_dead_worker(tmp_path):
    folder = history_folder(tmp_path)
    source = make_sealed_db(folder, 'worker-1', due_start(time.time()))
    lock = folder / LOCK_NAME
    lock.write_text(json.dumps({'acquired_at': '', 'instance': 'dead', 'token': 'dead', 'worker': 'worker-2'}))
    _age(lock, HistoryArchiveConfig().lock_stale_seconds + 60)

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
        seen['token'] = json.loads((folder / LOCK_NAME).read_text())['token']
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
        (folder / LOCK_NAME).write_text(json.dumps(body))
        return result

    monkeypatch.setattr(archive_mod, 'merge_databases', taken_over_during_merge)
    failures_before = recorder_archive_failures._value.get()

    with capture_logs() as cap:
        await _run(tmp_path)

    assert Path(source).exists()
    assert Path(meta_path(source)).exists()
    assert _archives(folder) == []
    assert _dotfiles(folder) == [LOCK_NAME]
    assert json.loads((folder / LOCK_NAME).read_text())['token'] == 'f' * 16
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
        (folder / LOCK_NAME).write_text(json.dumps(body))
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
    assert not (folder / LOCK_NAME).exists()


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
    assert not (folder / LOCK_NAME).exists()


async def test_a_failed_window_is_logged_counted_and_releases_the_lock(tmp_path, monkeypatch):
    folder = history_folder(tmp_path)
    source = make_sealed_db(folder, 'worker-1', due_start(time.time()))

    def boom(paths, output_path):
        raise OSError('merge blew up')

    monkeypatch.setattr(archive_mod, 'merge_databases', boom)
    failures_before = recorder_archive_failures._value.get()

    with capture_logs() as cap:
        await _run(tmp_path)

    assert not (folder / LOCK_NAME).exists()
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
    assert not (folder / LOCK_NAME).exists()
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
    '.archive.lock.stale.0123456789abcdef',
]


@pytest.mark.parametrize('name', WORK_FILES)
async def test_the_sweep_removes_a_work_file_older_than_the_stale_age(tmp_path, name):
    folder = history_folder(tmp_path)
    work_file = folder / name
    work_file.write_bytes(b'left by a dead pass')
    _age(work_file, 101)

    with capture_logs() as cap:
        await _run(tmp_path, _make_config(lock_stale_seconds=100))

    assert not work_file.exists()
    assert [e['name'] for e in cap if e['event'] == 'recorder_archive_temp_swept'] == [name]


@pytest.mark.parametrize('name', WORK_FILES)
async def test_the_sweep_keeps_a_work_file_younger_than_the_stale_age(tmp_path, name):
    folder = history_folder(tmp_path)
    work_file = folder / name
    work_file.write_bytes(b'still being written')
    _age(work_file, 50)

    await _run(tmp_path, _make_config(lock_stale_seconds=100))

    assert work_file.exists()


async def test_the_sweep_removes_an_old_meta_file_whose_database_is_missing(tmp_path):
    folder = history_folder(tmp_path)
    orphan = folder / 'worker-2-2020-01-01__00_00_00.db.meta.json'
    orphan.write_bytes(b'{"schema":1}')
    _age(orphan, 101)

    await _run(tmp_path, _make_config(lock_stale_seconds=100))

    assert not orphan.exists()


async def test_the_sweep_keeps_a_fresh_meta_file_whose_database_is_still_on_its_way(tmp_path):
    """Sealing writes the meta file first and moves the database next."""
    folder = history_folder(tmp_path)
    orphan = folder / 'worker-2-2020-01-01__00_00_00.db.meta.json'
    orphan.write_bytes(b'{"schema":1}')
    _age(orphan, 50)

    await _run(tmp_path, _make_config(lock_stale_seconds=100))

    assert orphan.exists()


async def test_the_sweep_keeps_an_old_meta_file_next_to_its_database(tmp_path):
    folder = history_folder(tmp_path)
    source = make_sealed_db(folder, 'worker-1', time.time() - HOUR)
    _age(Path(meta_path(source)), 101)

    await _run(tmp_path, _make_config(lock_stale_seconds=100))

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


async def test_archive_pass_reports_the_footprint_while_another_worker_holds_the_lock(tmp_path):
    folder = history_folder(tmp_path)
    existing = _make_archive(folder, CLUSTER, time.time() - 2 * DAY)
    archive_mod.recorder_archive_bytes.set(0)
    archive_mod.recorder_archive_files.set(0)
    holder = ArchiveLock(str(folder), worker='worker-2', instance='2222222222222222', stale_seconds=7200)
    assert holder.acquire()
    try:
        await _run(tmp_path, _make_config(retention_days=0))
    finally:
        holder.release()

    assert archive_mod.recorder_archive_files._value.get() == 1
    assert archive_mod.recorder_archive_bytes._value.get() == existing.stat().st_size


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


# --- recorder wiring ----------------------------------------------------


def _make_recorder(tmp_path, *, archive: HistoryArchiveConfig | None = None, **election: Any) -> EventRecorder:
    """A recorder of ``worker-1`` in ``CLUSTER``; ``election`` holds the election keyword arguments."""
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
        **election,
    )


async def test_archive_pass_runs_off_the_event_loop_on_the_shared_folder(tmp_path, monkeypatch):
    archive = HistoryArchiveConfig(lock_stale_seconds=900)
    rec = _make_recorder(tmp_path, archive=archive)
    seen: dict = {}

    def spy(shared_dir, **kwargs) -> None:
        seen.update(kwargs, shared_dir=shared_dir, thread=threading.current_thread().ident)

    monkeypatch.setattr(core_mod, 'run_archive_pass', spy)

    try:
        await rec._archive_pass()
    finally:
        await rec.stop()

    assert seen['shared_dir'] == str(tmp_path / 'shared')
    assert (seen['cluster'], seen['instance']) == (CLUSTER, INSTANCE)
    assert seen['archive'] is rec._archive
    assert seen['merged_retention_days'] == 7
    assert seen['thread'] != threading.current_thread().ident
    lease = seen['lease']
    assert (lease.scope, lease.lease_seconds, lease.provider_name) == (sanitize_cluster(CLUSTER), 900, 'filesystem')


async def test_the_default_lease_keeps_its_lock_in_the_clusters_history_folder(tmp_path, monkeypatch):
    rec = _make_recorder(tmp_path)
    seen: dict = {}
    monkeypatch.setattr(core_mod, 'run_archive_pass', lambda shared_dir, **kwargs: seen.update(kwargs))
    lock = Path(history_dir(str(tmp_path / 'shared'), CLUSTER)) / LOCK_NAME
    try:
        await rec._archive_pass()
        lease = seen['lease']

        # Before the stop: a stopped filesystem election refuses every call.
        assert await asyncio.to_thread(lease.acquire) is True
        try:
            body = json.loads(lock.read_bytes())
            assert (body['worker'], body['instance'], body['token']) == ('worker-1', INSTANCE, lease.token)
        finally:
            await asyncio.to_thread(lease.release)
    finally:
        await rec.stop()
    assert not lock.exists()


async def test_the_archive_pass_needs_no_default_executor_thread_of_its_own(tmp_path):
    # The pass waits on provider calls and log writes that run in the default
    # executor; with one worker there, the pass must not be sitting in it.
    folder = history_folder(tmp_path)
    source = make_sealed_db(folder, 'worker-1', due_start(time.time()))
    one_thread = ThreadPoolExecutor(max_workers=1)
    asyncio.get_running_loop().set_default_executor(one_thread)
    rec = _make_recorder(tmp_path)
    try:
        await asyncio.wait_for(rec._archive_pass(), timeout=WAIT_FOR_TIMEOUT_SECONDS)
    finally:
        await rec.stop()
        one_thread.shutdown(wait=True)

    assert len(_archives(folder)) == 1
    assert not Path(source).exists()
    assert not (folder / LOCK_NAME).exists()


@pytest.mark.parametrize('storage', [True, False], ids=['archiving-disabled', 'memory-only'])
async def test_archive_pass_does_nothing_when_off(tmp_path, monkeypatch, storage):
    election = MemoryElection(ElectionStore())
    rec = _make_recorder(
        tmp_path,
        archive=HistoryArchiveConfig(enabled=not storage),
        election=election,
        election_provider_name='memory-off',
    )
    if not storage:
        rec._storage_dir = ''
    calls = 0

    def spy(*args, **kwargs) -> None:
        nonlocal calls
        calls += 1

    monkeypatch.setattr(core_mod, 'run_archive_pass', spy)
    counted_before = {op: election_calls('memory-off', op, 'not_started') for op in ('acquire', 'stop')}

    try:
        await rec._archive_pass()
    finally:
        await rec.stop()

    assert calls == 0
    # The election is not in use: it never starts and no skipped call is counted.
    assert election.calls == {}
    assert {op: election_calls('memory-off', op, 'not_started') for op in ('acquire', 'stop')} == counted_before


async def test_a_blocked_archive_pass_never_holds_up_rotation(tmp_path, monkeypatch):
    names = iter(range(1, 100))
    monkeypatch.setattr(
        core_mod,
        'make_db_path',
        lambda folder, worker: os.path.join(folder, f'{worker}-2026-10-14__08_00_{next(names):02d}.db'),
    )
    unblock = threading.Event()
    passes = 0

    def blocked_pass(*args, **kwargs) -> None:
        nonlocal passes
        passes += 1
        unblock.wait(10)

    monkeypatch.setattr(core_mod, 'run_archive_pass', blocked_pass)
    rec = _make_recorder(tmp_path)
    rec._store = rec._store.model_copy(update={'rotation_interval_hours': 0.02 / 3600})
    await rec.start()
    try:
        first = rec.db_path
        await wait_for(lambda: passes == 1)
        seen = {first}
        await wait_for(lambda: seen.add(rec.db_path) or len(seen) >= 4)

        assert passes == 1
    finally:
        unblock.set()
        await rec.stop()


class _FakeClock:
    """A replacement for the recorder's sleep: each sleep waits for a tick the test gives.

    Sleeps are keyed by their length, so the rotation loop and the pass timer
    can be driven apart. ``entered`` counts the sleeps begun, which tells a
    test that the loop has handled the last tick and is waiting again.
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


def _ticking_recorder(tmp_path, monkeypatch, *, pass_interval_seconds: int, enabled: bool = True):
    """A recorder driven by a fake clock, with rotation, sealing and the pass start counted."""
    clock = _FakeClock()
    rec = _make_recorder(
        tmp_path,
        archive=HistoryArchiveConfig(enabled=enabled, pass_interval_seconds=pass_interval_seconds),
    )
    rec._sleep = clock.sleep
    counts = {'rotations': 0, 'sealing': 0, 'passes': 0}

    async def rotate() -> None:
        counts['rotations'] += 1

    monkeypatch.setattr(rec, '_rotate', rotate)
    monkeypatch.setattr(rec, '_start_sealing', lambda: counts.__setitem__('sealing', counts['sealing'] + 1))
    monkeypatch.setattr(rec, '_start_archive_pass', lambda: counts.__setitem__('passes', counts['passes'] + 1))
    return rec, clock, counts


def _rotation_seconds(rec) -> float:
    return rec._store.rotation_interval_hours * 3600


async def test_pass_interval_zero_starts_a_pass_after_each_rotation(tmp_path, monkeypatch):
    rec, clock, counts = _ticking_recorder(tmp_path, monkeypatch, pass_interval_seconds=0)
    await rec.start()
    try:
        await clock.tick(_rotation_seconds(rec))

        assert counts == {'rotations': 1, 'sealing': 1, 'passes': 1}
        assert rec._pass_timer_task is None
    finally:
        await rec.stop()


async def test_pass_interval_starts_passes_on_its_own_timer(tmp_path, monkeypatch):
    rec, clock, counts = _ticking_recorder(tmp_path, monkeypatch, pass_interval_seconds=20)
    await rec.start()
    try:
        await clock.tick(20)
        await clock.tick(20)
        assert counts['passes'] == 2
        assert counts['rotations'] == 0

        await clock.tick(_rotation_seconds(rec))

        # The rotation still rotates and seals; it starts no pass.
        assert counts == {'rotations': 1, 'sealing': 1, 'passes': 2}
    finally:
        await rec.stop()


async def test_pass_interval_tick_during_a_running_pass_starts_no_second_pass(tmp_path, monkeypatch):
    unblock = threading.Event()
    passes = 0

    def blocked_pass(*args, **kwargs) -> None:
        nonlocal passes
        passes += 1
        unblock.wait(10)

    monkeypatch.setattr(core_mod, 'run_archive_pass', blocked_pass)
    clock = _FakeClock()
    rec = _make_recorder(tmp_path, archive=HistoryArchiveConfig(pass_interval_seconds=20))
    rec._sleep = clock.sleep
    await rec.start()
    try:
        await clock.tick(20)
        await wait_for(lambda: passes == 1)
        await clock.tick(20)
        await clock.tick(20)
        assert passes == 1

        unblock.set()
        await wait_for(lambda: rec._archive_task.done())
        await clock.tick(20)
        await wait_for(lambda: passes == 2)
    finally:
        unblock.set()
        await rec.stop()


async def test_pass_interval_with_archive_disabled_runs_no_timer(tmp_path, monkeypatch):
    rec, clock, counts = _ticking_recorder(tmp_path, monkeypatch, pass_interval_seconds=20, enabled=False)
    await rec.start()
    try:
        await clock.tick(_rotation_seconds(rec))

        assert rec._pass_timer_task is None
        assert 20 not in clock.entered
        assert counts['passes'] == 1
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
    await rec.start()
    try:
        rec._start_archive_pass()
        await wait_for(started.is_set)
    finally:
        await rec.stop()

    assert seen['stop'] is not None
    assert seen['stop'].is_set()


# --- the recorder's election ----------------------------------------------


class _FailingStartElection(MemoryElection):
    """A memory election whose first ``failures`` starts raise."""

    def __init__(self, failures: int) -> None:
        super().__init__(ElectionStore())
        self._failures = failures

    async def start(self, ctx: ElectionContext) -> None:
        self.calls['start'] += 1
        if self.calls['start'] <= self._failures:
            raise RuntimeError('election service unavailable')
        self.started_with = ctx


class _HangingElection(MemoryElection):
    """A memory election whose ``start`` or ``stop`` hangs until cancelled."""

    def __init__(self, *, hang_in: str) -> None:
        super().__init__(ElectionStore())
        self._hang_in = hang_in
        self.hanging = asyncio.Event()

    async def _hang(self) -> None:
        self.hanging.set()
        await asyncio.Event().wait()

    async def start(self, ctx: ElectionContext) -> None:
        await super().start(ctx)
        if self._hang_in == 'start':
            await self._hang()

    async def stop(self) -> None:
        await super().stop()
        if self._hang_in == 'stop':
            await self._hang()


async def test_the_recorder_starts_its_election_once_with_the_worker_context(tmp_path, monkeypatch):
    election = MemoryElection(ElectionStore())
    connections = Connections(ConnectionsConfig())
    monkeypatch.setattr(core_mod, 'run_archive_pass', lambda shared_dir, **kwargs: None)
    rec = _make_recorder(
        tmp_path,
        election=election,
        election_provider_name='memory-once',
        election_options={'address': 'consul:8500'},
        connections=connections,
    )

    with capture_logs() as logs:
        try:
            await rec._archive_pass()
            await rec._archive_pass()
        finally:
            await rec.stop()

    assert election.calls['start'] == 1
    ctx = election.started_with
    assert ctx is not None
    assert (ctx.worker, ctx.instance, ctx.cluster) == ('worker-1', INSTANCE, CLUSTER)
    assert ctx.shared_dir == str(tmp_path / 'shared')
    assert ctx.options == {'address': 'consul:8500'}
    assert ctx.connections is connections
    started = [entry for entry in logs if entry['event'] == 'provider_started']
    assert started == [
        {
            'event': 'provider_started',
            'log_level': 'info',
            'category': 'recorder',
            'feature': 'election',
            'provider': 'memory-once',
        }
    ]


async def test_a_failed_election_start_skips_the_pass_and_the_next_pass_retries(tmp_path, monkeypatch):
    election = _FailingStartElection(failures=1)
    leases: list[ElectionLease] = []
    monkeypatch.setattr(core_mod, 'run_archive_pass', lambda shared_dir, **kwargs: leases.append(kwargs['lease']))
    rec = _make_recorder(tmp_path, election=election, election_provider_name='memory-retry')
    skipped_before = election_calls('memory-retry', 'acquire', 'not_started')

    try:
        await rec._archive_pass()
        assert leases == []
        assert election_calls('memory-retry', 'acquire', 'not_started') == skipped_before + 1
        await rec._archive_pass()
    finally:
        await rec.stop()

    assert election.calls['start'] == 2
    assert [lease.provider_name for lease in leases] == ['memory-retry']
    assert election.calls['stop'] == 1


async def test_the_election_breaker_spans_archive_passes(tmp_path, monkeypatch):
    election = _FailingStartElection(failures=99)
    monkeypatch.setattr(core_mod, 'run_archive_pass', lambda shared_dir, **kwargs: None)
    resilience = ResilienceConfig(retries=0, breaker=BreakerConfig(failure_threshold=1, cooldown_seconds=3600))
    caller = ResilientCaller(resilience, feature='election', provider='memory-breaker', category='recorder')
    rec = _make_recorder(tmp_path, election=election, election_caller=caller, election_provider_name='memory-breaker')
    skipped_before = election_calls('memory-breaker', 'start', 'skipped_open')

    try:
        await rec._archive_pass()
        await rec._archive_pass()
    finally:
        await rec.stop()

    assert election.calls['start'] == 1
    assert caller.breaker_open
    assert election_calls('memory-breaker', 'start', 'skipped_open') == skipped_before + 1


async def test_stop_never_stops_an_election_that_did_not_start(tmp_path):
    election = MemoryElection(ElectionStore())
    rec = _make_recorder(tmp_path, election=election, election_provider_name='memory-unstarted')
    skipped_before = election_calls('memory-unstarted', 'stop', 'not_started')

    await rec.start()
    await rec.stop()

    assert election.calls['stop'] == 0
    assert election_calls('memory-unstarted', 'stop', 'not_started') == skipped_before + 1


async def test_a_stop_while_the_election_start_hangs_ends_at_once_and_skips_the_provider_stop(tmp_path):
    election = _HangingElection(hang_in='start')
    rec = _make_recorder(tmp_path, election=election, election_provider_name='memory-hanging-start')
    await rec.start()
    rec._start_archive_pass()
    await wait_for(election.hanging.is_set)

    await asyncio.wait_for(rec.stop(), timeout=WAIT_FOR_TIMEOUT_SECONDS)

    assert election.calls['stop'] == 0


async def test_the_election_stop_gets_one_attempt_inside_the_shutdown_time_left(tmp_path, monkeypatch):
    election = _HangingElection(hang_in='stop')
    monkeypatch.setattr(core_mod, 'run_archive_pass', lambda shared_dir, **kwargs: None)
    rec = _make_recorder(tmp_path, election=election, election_provider_name='memory-hanging-stop')
    timeouts_before = election_calls('memory-hanging-stop', 'stop', 'timeout')
    await rec.start()
    await rec._archive_pass()

    started = time.monotonic()
    await rec.stop(timeout=1.0)

    # The default per-attempt limit is 5 s; the shutdown time left cuts it.
    assert time.monotonic() - started < 4
    assert election.calls['stop'] == 1
    assert election_calls('memory-hanging-stop', 'stop', 'timeout') == timeouts_before + 1


@pytest.mark.parametrize('started', [True, False], ids=['started', 'never-started'])
async def test_a_second_stop_neither_stops_the_election_nor_counts_a_stop(tmp_path, monkeypatch, started):
    election = MemoryElection(ElectionStore())
    monkeypatch.setattr(core_mod, 'run_archive_pass', lambda shared_dir, **kwargs: None)
    name = f'memory-second-stop-{started}'
    rec = _make_recorder(tmp_path, election=election, election_provider_name=name)
    await rec.start()
    if started:
        await rec._archive_pass()
    await rec.stop()
    counted = {outcome: election_calls(name, 'stop', outcome) for outcome in ('ok', 'not_started')}

    await rec.stop()

    assert election.calls['stop'] == (1 if started else 0)
    assert {outcome: election_calls(name, 'stop', outcome) for outcome in ('ok', 'not_started')} == counted


class _OrderRecordingElection(MemoryElection):
    """A memory election that also records the order of its calls."""

    def __init__(self) -> None:
        super().__init__(ElectionStore())
        self.order: list[str] = []

    async def start(self, ctx: ElectionContext) -> None:
        self.order.append('start')
        await super().start(ctx)

    async def acquire(self, scope: str, token: str, lease_seconds: float) -> bool:
        self.order.append('acquire')
        return await super().acquire(scope, token, lease_seconds)

    async def is_held(self, scope: str, token: str) -> bool:
        self.order.append('is_held')
        return await super().is_held(scope, token)

    async def release(self, scope: str, token: str) -> None:
        self.order.append('release')
        await super().release(scope, token)

    async def stop(self) -> None:
        self.order.append('stop')
        await super().stop()


async def test_the_election_stops_only_after_the_pass_in_progress_made_its_last_call(tmp_path, monkeypatch):
    election = _OrderRecordingElection()
    in_window = threading.Event()

    def pass_in_a_window(shared_dir, *, lease, stop, **kwargs) -> None:
        lease.acquire()
        in_window.set()
        stop.wait(WAIT_FOR_TIMEOUT_SECONDS)
        # The rest of the window, after stop() asked the pass to end.
        time.sleep(0.2)
        lease.is_held()
        lease.release()

    monkeypatch.setattr(core_mod, 'run_archive_pass', pass_in_a_window)
    rec = _make_recorder(tmp_path, election=election, election_provider_name='memory-ordered')
    await rec.start()
    rec._start_archive_pass()
    await wait_for(in_window.is_set)

    await rec.stop(timeout=WAIT_FOR_TIMEOUT_SECONDS)

    assert election.order == ['start', 'acquire', 'is_held', 'release', 'stop']


async def test_a_pass_that_fails_after_stop_asked_it_to_end_is_logged(tmp_path, monkeypatch):
    in_window = threading.Event()

    def failing_pass(shared_dir, *, stop, **kwargs) -> None:
        in_window.set()
        stop.wait(WAIT_FOR_TIMEOUT_SECONDS)
        raise RuntimeError('window merge failed')

    monkeypatch.setattr(core_mod, 'run_archive_pass', failing_pass)
    rec = _make_recorder(tmp_path, election=MemoryElection(ElectionStore()), election_provider_name='memory-failing')
    await rec.start()
    rec._start_archive_pass()
    await wait_for(in_window.is_set)

    with capture_logs() as logs:
        await rec.stop(timeout=WAIT_FOR_TIMEOUT_SECONDS)

    (failed,) = [entry for entry in logs if entry['event'] == 'recorder_background_job_failed']
    assert (failed['job'], failed['error']) == ('archive', 'window merge failed')


async def test_a_pass_that_outlives_the_shutdown_time_is_left_and_the_provider_stop_skipped(tmp_path, monkeypatch):
    election = MemoryElection(ElectionStore())
    in_window = threading.Event()

    def slow_pass(shared_dir, **kwargs) -> None:
        in_window.set()
        # A window that does not end when stop() asks.
        time.sleep(1.0)

    monkeypatch.setattr(core_mod, 'run_archive_pass', slow_pass)
    rec = _make_recorder(tmp_path, election=election, election_provider_name='memory-slow-pass')
    timeouts_before = election_calls('memory-slow-pass', 'stop', 'timeout')
    await rec.start()
    rec._start_archive_pass()
    await wait_for(in_window.is_set)

    started = time.monotonic()
    await rec.stop(timeout=0.3)

    assert time.monotonic() - started < 0.9
    # No shutdown time is left for the provider stop: it is counted, not made.
    assert election.calls['stop'] == 0
    assert election_calls('memory-slow-pass', 'stop', 'timeout') == timeouts_before + 1
    # The abandoned pass's thread must not outlive the test.
    pass_future = rec._archive_pass_future
    assert pass_future is not None
    await wait_for(pass_future.done)


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


async def test_the_stop_waits_for_a_pass_in_progress_until_only_the_reserve_is_left_and_still_stops_the_election(
    tmp_path, monkeypatch
):
    election = MemoryElection(ElectionStore())
    in_window, window_may_end = _stuck_pass_in_window(monkeypatch)
    rec = _make_recorder(tmp_path, election=election, election_provider_name='memory-stuck-pass')
    await rec.start()
    rec._start_archive_pass()
    await wait_for(in_window.is_set)

    try:
        started = time.monotonic()
        with capture_logs() as logs:
            await rec.stop(timeout=1.0, election_reserve_seconds=0.5)
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
    # The provider stop still made its attempt with the time that was left.
    assert election.calls['stop'] == 1
    pass_future = rec._archive_pass_future
    assert pass_future is not None
    await wait_for(pass_future.done)


async def test_a_reserve_larger_than_the_time_left_abandons_the_pass_at_once_and_still_stops_the_election(
    tmp_path, monkeypatch
):
    election = MemoryElection(ElectionStore())
    in_window, window_may_end = _stuck_pass_in_window(monkeypatch)
    rec = _make_recorder(tmp_path, election=election, election_provider_name='memory-reserve-too-large')
    await rec.start()
    rec._start_archive_pass()
    await wait_for(in_window.is_set)

    try:
        with capture_logs() as logs:
            await rec.stop(timeout=1.0, election_reserve_seconds=5.0)
    finally:
        window_may_end.set()

    (abandoned,) = [entry for entry in logs if entry['event'] == 'recorder_archive_pass_abandoned']
    assert abandoned['waited_seconds'] < 0.1
    assert election.calls['stop'] == 1
    pass_future = rec._archive_pass_future
    assert pass_future is not None
    await wait_for(pass_future.done)


async def test_a_stop_without_a_deadline_waits_until_the_pass_in_progress_ends(tmp_path, monkeypatch):
    election = _OrderRecordingElection()
    in_window, window_may_end = _stuck_pass_in_window(monkeypatch)
    rec = _make_recorder(tmp_path, election=election, election_provider_name='memory-no-deadline')
    await rec.start()
    rec._start_archive_pass()
    await wait_for(in_window.is_set)

    with capture_logs() as logs:
        stopping = asyncio.create_task(rec.stop(election_reserve_seconds=5.0))
        await asyncio.sleep(0.3)
        # Still waiting for the window: no deadline, so no reserve to keep.
        assert not stopping.done()
        window_may_end.set()
        await asyncio.wait_for(stopping, timeout=WAIT_FOR_TIMEOUT_SECONDS)

    assert 'recorder_archive_pass_abandoned' not in [entry['event'] for entry in logs]
    assert election.order[-1] == 'stop'


class _FailingStartFilesystemElection(FilesystemElection):
    """The filesystem election whose first ``failures`` starts raise."""

    def __init__(self, failures: int) -> None:
        super().__init__()
        self._failures = failures
        self.starts = 0

    async def start(self, ctx: ElectionContext) -> None:
        self.starts += 1
        if self.starts <= self._failures:
            raise RuntimeError('election service unavailable')
        await super().start(ctx)


async def test_an_election_back_after_its_breaker_opened_archives_without_a_restart(tmp_path):
    folder = history_folder(tmp_path)
    source = make_sealed_db(folder, 'worker-2', due_start(time.time()))
    clock = [1_000.0]
    resilience = ResilienceConfig(retries=0, breaker=BreakerConfig(failure_threshold=2, cooldown_seconds=30))
    caller = ResilientCaller(
        resilience,
        feature='election',
        provider='flaky-filesystem',
        category='recorder',
        now_monotonic=lambda: clock[0],
    )
    election = _FailingStartFilesystemElection(failures=2)
    rec = _make_recorder(tmp_path, election=election, election_caller=caller, election_provider_name='flaky-filesystem')
    refused_before = election_calls('flaky-filesystem', 'start', 'skipped_open')

    try:
        # Two failed starts open the breaker; the third pass is refused without a call.
        for _ in range(3):
            await rec._archive_pass()
        assert (election.starts, caller.breaker_open) == (2, True)
        assert election_calls('flaky-filesystem', 'start', 'skipped_open') == refused_before + 1
        assert _archives(folder) == []
        clock[0] += 31
        with capture_logs() as logs:
            await rec._archive_pass()
    finally:
        await rec.stop()

    # The probe start succeeded, and the same pass took the lock and archived.
    assert (election.starts, caller.breaker_open) == (3, False)
    assert [entry['event'] for entry in logs if entry['event'].startswith('provider_')] == [
        'provider_breaker_closed',
        'provider_started',
    ]
    assert len(_archives(folder)) == 1
    assert not Path(source).exists()
    assert not (folder / LOCK_NAME).exists()


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
