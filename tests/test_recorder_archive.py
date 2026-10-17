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

import pytest
import structlog
from structlog.testing import capture_logs

from drakkar.config import HistoryArchiveConfig, ResilienceConfig
from drakkar.metrics import recorder_archive_failures
from drakkar.providers.resilience import ResilientCaller
from drakkar.recorder import SCHEMA_EVENTS, SCHEMA_WORKER_CONFIG, EventRecorder, format_dt
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
from drakkar.recorder.history import history_dir, meta_path, sanitize_cluster, write_meta
from tests.conftest import WAIT_FOR_TIMEOUT_SECONDS, make_ui_config, wait_for

HOUR = 3600.0
DAY = 24 * HOUR
CLUSTER = 'search-fleet'
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


def _db_name(worker: str, start: float) -> str:
    """Sealed recorder file name for a worker whose file opened at ``start``."""
    return f'{worker}-{datetime.fromtimestamp(start, tz=UTC).strftime("%Y-%m-%d__%H_%M_%S")}.db'


def _folder(tmp_path, cluster: str = CLUSTER) -> Path:
    folder = Path(history_dir(str(tmp_path / 'shared'), cluster))
    folder.mkdir(parents=True, exist_ok=True)
    return folder


def _make_db(folder: Path, worker: str, start: float, *, event_count: int = 2) -> str:
    """One sealed recorder file (rollback journal) with its meta file, as sealing leaves them."""
    path = folder / _db_name(worker, start)
    db = sqlite3.connect(str(path))
    db.executescript(SCHEMA_WORKER_CONFIG)
    db.execute(
        """INSERT INTO worker_config
           (id, worker_name, cluster_name, ip_address, debug_port, created_at, created_at_dt)
           VALUES (1, ?, ?, ?, ?, ?, ?)""",
        [worker, CLUSTER, '10.0.0.1', 8080, start, format_dt(start)],
    )
    db.executescript(SCHEMA_EVENTS)
    for i in range(event_count):
        ts = start + i
        db.execute(
            'INSERT INTO events (ts, dt, event, partition, offset) VALUES (?, ?, ?, ?, ?)',
            [ts, format_dt(ts), 'consumed', 0, i],
        )
    db.commit()
    db.close()
    write_meta(meta_path(str(path)), {'schema': 1, 'worker': worker})
    return str(path)


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
    path = tmp_path / _db_name('worker-1', start)
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


def _due_start(now: float, *, days_back: int = 3) -> float:
    """A file start time inside a window that is comfortably due at ``now``."""
    day_start = (now // DAY) * DAY
    return day_start - days_back * DAY + 10 * HOUR


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
    folder = _folder(tmp_path)
    now = time.time()
    start = _due_start(now)
    first = _make_db(folder, 'worker-1', start, event_count=2)
    second = _make_db(folder, 'worker-2', start + HOUR, event_count=3)

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
    folder = _folder(tmp_path)
    _make_db(folder, 'worker-1', _due_start(time.time()))

    with capture_logs() as cap:
        await _run(tmp_path)

    (created,) = [e for e in cap if e['event'] == 'recorder_archive_created']
    assert created['cluster'] == CLUSTER
    assert created['file_count'] == 1
    assert created['raw_bytes'] > 0
    assert created['compressed_bytes'] > 0


async def test_run_archive_pass_leaves_a_window_that_is_not_due(tmp_path):
    folder = _folder(tmp_path)
    path = _make_db(folder, 'worker-1', time.time() - HOUR)

    await _run(tmp_path)

    assert Path(path).exists()
    assert _archives(folder) == []
    assert not (folder / LOCK_NAME).exists()


async def test_run_archive_pass_without_a_history_folder_creates_nothing(tmp_path):
    await _run(tmp_path)

    assert not (tmp_path / 'shared').exists()


async def test_run_archive_pass_never_takes_a_file_still_being_copied_in(tmp_path):
    folder = _folder(tmp_path)
    start = _due_start(time.time())
    _make_db(folder, 'worker-1', start)
    incoming = folder / f'.{_db_name("worker-2", start)}.0123456789abcdef.incoming'
    incoming.write_bytes(b'half a copy')

    await _run(tmp_path)

    assert incoming.exists()
    assert len(_archives(folder)) == 1


async def test_run_archive_pass_sets_aside_a_source_the_merge_cannot_read(tmp_path):
    folder = _folder(tmp_path)
    start = _due_start(time.time())
    healthy = _make_db(folder, 'worker-1', start)
    corrupt = folder / _db_name('worker-2', start + HOUR)
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
    folder = _folder(tmp_path)
    start = _due_start(time.time())
    first = _make_db(folder, 'worker-1', start, event_count=2)
    _make_db(folder, 'worker-2', start + HOUR, event_count=3)
    await _run(tmp_path)
    (name,) = _archives(folder)
    published = (folder / name).read_bytes()

    # A pass that died between publishing and deleting leaves a source behind.
    _make_db(folder, 'worker-1', start, event_count=2)

    with capture_logs() as cap:
        await _run(tmp_path)

    assert not Path(first).exists()
    assert not Path(meta_path(first)).exists()
    assert (folder / name).read_bytes() == published
    assert [e['event'] for e in cap if e['event'].startswith('recorder_archive_')] == [
        'recorder_archive_sources_reclaimed'
    ]


async def test_run_archive_pass_adds_new_sources_to_a_published_archive(tmp_path):
    folder = _folder(tmp_path)
    start = _due_start(time.time())
    _make_db(folder, 'worker-1', start, event_count=2)
    await _run(tmp_path)
    (name,) = _archives(folder)
    late = _make_db(folder, 'worker-3', start + 2 * HOUR, event_count=4)

    await _run(tmp_path)

    assert not Path(late).exists()
    assert _archives(folder) == [name]
    assert _events_in_archive(folder / name) == 6


async def test_run_archive_pass_deletes_already_archived_sources_when_folding(tmp_path):
    """Sources skipped as already-archived are healthy — delete, never quarantine."""
    folder = _folder(tmp_path)
    start = _due_start(time.time())
    _make_db(folder, 'worker-1', start, event_count=2)
    _make_db(folder, 'worker-2', start + HOUR, event_count=3)
    await _run(tmp_path)
    (name,) = _archives(folder)
    _make_db(folder, 'worker-1', start, event_count=2)
    _make_db(folder, 'worker-2', start + HOUR, event_count=3)
    _make_db(folder, 'worker-3', start + 2 * HOUR, event_count=4)

    with capture_logs() as cap:
        await _run(tmp_path)

    assert list(folder.glob('*.db')) == []
    assert list(folder.glob('*.unreadable')) == []
    assert [e for e in cap if e['event'] == 'recorder_archive_source_skipped'] == []
    assert _events_in_archive(folder / name) == 9


async def test_run_archive_pass_keeps_worker_attribution_across_folds(tmp_path):
    folder = _folder(tmp_path)
    start = _due_start(time.time())
    _make_db(folder, 'worker-1', start, event_count=2)
    _make_db(folder, 'worker-2', start + HOUR, event_count=3)
    await _run(tmp_path)
    (name,) = _archives(folder)
    _make_db(folder, 'worker-3', start + 2 * HOUR, event_count=4)
    await _run(tmp_path)
    _make_db(folder, 'worker-4', start + 3 * HOUR, event_count=1)
    await _run(tmp_path)

    assert _archives(folder) == [name]
    assert _archive_workers(folder / name) == [
        ('worker-1', _db_name('worker-1', start), 2),
        ('worker-2', _db_name('worker-2', start + HOUR), 3),
        ('worker-3', _db_name('worker-3', start + 2 * HOUR), 4),
        ('worker-4', _db_name('worker-4', start + 3 * HOUR), 1),
    ]
    assert _dotfiles(folder) == []


# --- the archive lock ---------------------------------------------------


async def test_run_archive_pass_skips_while_another_worker_holds_the_lock(tmp_path):
    folder = _folder(tmp_path)
    source = _make_db(folder, 'worker-1', _due_start(time.time()))
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
    folder = _folder(tmp_path)
    source = _make_db(folder, 'worker-1', _due_start(time.time()))
    lock = folder / LOCK_NAME
    lock.write_text(json.dumps({'acquired_at': '', 'instance': 'dead', 'token': 'dead', 'worker': 'worker-2'}))
    _age(lock, HistoryArchiveConfig().lock_stale_seconds + 60)

    await _run(tmp_path)

    assert not Path(source).exists()
    assert len(_archives(folder)) == 1
    assert not lock.exists()


async def test_run_archive_pass_names_its_work_files_with_the_lock_token(tmp_path, monkeypatch):
    folder = _folder(tmp_path)
    _make_db(folder, 'worker-1', _due_start(time.time()))
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
    folder = _folder(tmp_path)
    _make_db(folder, 'worker-1', _due_start(time.time()))
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
    folder = _folder(tmp_path)
    start = _due_start(time.time())
    source = _make_db(folder, 'worker-1', start)
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
    folder = _folder(tmp_path)
    _make_db(folder, 'worker-1', _due_start(time.time()))
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


async def test_a_pass_whose_lease_cannot_be_confirmed_is_fenced_as_unconfirmed(tmp_path, monkeypatch):
    folder = _folder(tmp_path)
    source = _make_db(folder, 'worker-1', _due_start(time.time()))

    async def no_answer(self, scope: str, token: str) -> bool:
        raise ValueError('the election gave no answer')

    monkeypatch.setattr(FilesystemElection, 'is_held', no_answer)

    with capture_logs() as cap:
        await _run(tmp_path)

    (fenced,) = [e for e in cap if e['event'] == 'recorder_archive_fenced']
    assert fenced['reason'] == 'unconfirmed'
    assert Path(source).exists()
    assert _archives(folder) == []
    assert not (folder / LOCK_NAME).exists()


async def test_run_archive_pass_keeps_earlier_windows_when_a_later_one_fails(tmp_path, monkeypatch):
    folder = _folder(tmp_path)
    now = time.time()
    first_start = _due_start(now, days_back=4)
    second_start = _due_start(now, days_back=3)
    first_source = _make_db(folder, 'worker-1', first_start)
    second_source = _make_db(folder, 'worker-1', second_start)
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
    folder = _folder(tmp_path)
    source = _make_db(folder, 'worker-1', _due_start(time.time()))

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
    folder = _folder(tmp_path)
    _make_db(folder, 'worker-1', _due_start(time.time()))
    failures_before = recorder_archive_failures._value.get()

    await _run(tmp_path)

    assert recorder_archive_failures._value.get() == failures_before


async def test_a_stop_ends_the_pass_after_the_current_window(tmp_path, monkeypatch):
    folder = _folder(tmp_path)
    now = time.time()
    first_start = _due_start(now, days_back=4)
    first_source = _make_db(folder, 'worker-1', first_start)
    second_source = _make_db(folder, 'worker-1', _due_start(now, days_back=3))
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
    folder = _folder(tmp_path)
    source = _make_db(folder, 'worker-1', _due_start(time.time()))

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
    folder = _folder(tmp_path)
    work_file = folder / name
    work_file.write_bytes(b'left by a dead pass')
    _age(work_file, 101)

    with capture_logs() as cap:
        await _run(tmp_path, _make_config(lock_stale_seconds=100))

    assert not work_file.exists()
    assert [e['name'] for e in cap if e['event'] == 'recorder_archive_temp_swept'] == [name]


@pytest.mark.parametrize('name', WORK_FILES)
async def test_the_sweep_keeps_a_work_file_younger_than_the_stale_age(tmp_path, name):
    folder = _folder(tmp_path)
    work_file = folder / name
    work_file.write_bytes(b'still being written')
    _age(work_file, 50)

    await _run(tmp_path, _make_config(lock_stale_seconds=100))

    assert work_file.exists()


async def test_the_sweep_removes_an_old_meta_file_whose_database_is_missing(tmp_path):
    folder = _folder(tmp_path)
    orphan = folder / 'worker-2-2020-01-01__00_00_00.db.meta.json'
    orphan.write_bytes(b'{"schema":1}')
    _age(orphan, 101)

    await _run(tmp_path, _make_config(lock_stale_seconds=100))

    assert not orphan.exists()


async def test_the_sweep_keeps_a_fresh_meta_file_whose_database_is_still_on_its_way(tmp_path):
    """Sealing writes the meta file first and moves the database next."""
    folder = _folder(tmp_path)
    orphan = folder / 'worker-2-2020-01-01__00_00_00.db.meta.json'
    orphan.write_bytes(b'{"schema":1}')
    _age(orphan, 50)

    await _run(tmp_path, _make_config(lock_stale_seconds=100))

    assert orphan.exists()


async def test_the_sweep_keeps_an_old_meta_file_next_to_its_database(tmp_path):
    folder = _folder(tmp_path)
    source = _make_db(folder, 'worker-1', time.time() - HOUR)
    _age(Path(meta_path(source)), 101)

    await _run(tmp_path, _make_config(lock_stale_seconds=100))

    assert Path(meta_path(source)).exists()


# --- expiry ---------------------------------------------------------------


async def test_run_archive_pass_expires_only_this_clusters_old_archives(tmp_path):
    folder = _folder(tmp_path)
    now = time.time()
    _make_db(folder, 'worker-1', _due_start(now, days_back=3))
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
    folder = _folder(tmp_path)
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
    _folder(tmp_path)
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
    _folder(tmp_path)
    old = _merged_file(tmp_path, 'merged-2026-01-01__10_00_00-0123456789abcdef.db', 300)

    await _run(tmp_path, merged_retention_days=0)

    assert old.exists()


# --- archive footprint --------------------------------------------------


async def test_archive_pass_reports_only_this_clusters_footprint(tmp_path):
    folder = _folder(tmp_path)
    now = time.time()
    mine_a = _make_archive(folder, CLUSTER, now - 3 * DAY)
    mine_b = _make_archive(folder, CLUSTER, now - 2 * DAY)
    extended = _make_archive(folder, f'{CLUSTER}-hot', now - 2 * DAY)
    extended.write_bytes(b'x' * 4096)

    await _run(tmp_path, _make_config(retention_days=0))

    assert archive_mod.recorder_archive_files._value.get() == 2
    assert archive_mod.recorder_archive_bytes._value.get() == mine_a.stat().st_size + mine_b.stat().st_size


async def test_archive_pass_reports_the_footprint_while_another_worker_holds_the_lock(tmp_path):
    folder = _folder(tmp_path)
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


def _make_recorder(tmp_path, *, archive: HistoryArchiveConfig | None = None) -> EventRecorder:
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
    try:
        await rec._archive_pass()
    finally:
        await rec.stop()
    lease = seen['lease']
    lock = Path(history_dir(str(tmp_path / 'shared'), CLUSTER)) / LOCK_NAME

    assert await asyncio.to_thread(lease.acquire) is True
    try:
        body = json.loads(lock.read_bytes())
        assert (body['worker'], body['instance'], body['token']) == ('worker-1', INSTANCE, lease.token)
    finally:
        await asyncio.to_thread(lease.release)
    assert not lock.exists()


async def test_the_archive_pass_needs_no_default_executor_thread_of_its_own(tmp_path):
    # The pass waits on provider calls and log writes that run in the default
    # executor; with one worker there, the pass must not be sitting in it.
    folder = _folder(tmp_path)
    source = _make_db(folder, 'worker-1', _due_start(time.time()))
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
    rec = _make_recorder(tmp_path, archive=HistoryArchiveConfig(enabled=not storage))
    if not storage:
        rec._storage_dir = ''
    calls = 0

    def spy(*args, **kwargs) -> None:
        nonlocal calls
        calls += 1

    monkeypatch.setattr(core_mod, 'run_archive_pass', spy)

    try:
        await rec._archive_pass()
    finally:
        await rec.stop()

    assert calls == 0


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
    folder = _folder(tmp_path)
    source = _make_db(folder, 'worker-1', _due_start(time.time()))
    reader = sqlite3.connect(source)
    reader.execute('BEGIN EXCLUSIVE')
    try:
        await _run(tmp_path)
    finally:
        reader.rollback()
        reader.close()

    assert len(_archives(folder)) == 1
