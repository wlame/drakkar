"""Tests for the recorder archive engine (window math + archive pass)."""

import gzip
import json
import os
import sqlite3
import threading
import time
from datetime import UTC, datetime
from pathlib import Path

import pytest
from structlog.testing import capture_logs

from drakkar.config import UIRecorderConfig
from drakkar.recorder import SCHEMA_EVENTS, SCHEMA_WORKER_CONFIG, EventRecorder, format_dt
from drakkar.recorder import archive as archive_mod
from drakkar.recorder import core as core_mod
from drakkar.recorder.archive import (
    LOCK_STALE_SECONDS,
    archive_file_name,
    assign_windows,
    parse_db_start_ts,
    run_archive_pass,
    sanitize_cluster,
)
from tests.conftest import make_ui_config

HOUR = 3600.0
DAY = 24 * HOUR
CLUSTER = 'search-fleet'
OTHER_CLUSTER = 'index-fleet'


def _db_name(worker: str, start: float) -> str:
    """Raw recorder file name for a worker whose file opened at ``start``."""
    return f'{worker}-{datetime.fromtimestamp(start, tz=UTC).strftime("%Y-%m-%d__%H_%M_%S")}.db'


def _make_db(
    db_dir: Path,
    worker: str,
    start: float,
    cluster: str | None = CLUSTER,
    *,
    mtime: float | None = None,
    event_count: int = 2,
    sidecars: bool = True,
) -> str:
    """Create one synthetic raw recorder DB with real schema + events.

    ``mtime`` defaults to the file's start time, which is what the DUE
    check reads; ``sidecars`` also drops the ``-wal``/``-shm`` companions
    so their deletion can be asserted.
    """
    path = db_dir / _db_name(worker, start)
    db = sqlite3.connect(str(path))
    db.executescript(SCHEMA_WORKER_CONFIG)
    db.execute(
        """INSERT INTO worker_config
           (id, worker_name, cluster_name, ip_address, debug_port, created_at, created_at_dt)
           VALUES (1, ?, ?, ?, ?, ?, ?)""",
        [worker, cluster, '10.0.0.1', 8080, start, format_dt(start)],
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
    if sidecars:
        for suffix in ('-wal', '-shm'):
            (db_dir / (path.name + suffix)).write_bytes(b'')
    stamp = start if mtime is None else mtime
    os.utime(path, (stamp, stamp))
    return str(path)


def _make_archive(db_dir: Path, cluster: str, end: float, *, window: float = DAY) -> Path:
    """Drop a pre-existing archive file whose window ends at ``end``."""
    path = db_dir / archive_file_name(cluster, end - window, end)
    path.write_bytes(b'not-really-gzip')
    return path


def _lock_path(db_dir: Path, cluster: str = CLUSTER) -> Path:
    return db_dir / f'.archive-{sanitize_cluster(cluster)}.lock'


def _make_config(**overrides) -> UIRecorderConfig:
    values = {'archive_enabled': True, 'archive_window_hours': 24, 'rotation_interval_hours': 1}
    values.update(overrides)
    return UIRecorderConfig(**values)


# --- filename helpers ---------------------------------------------------


def test_parse_db_start_ts_reads_the_filename_timestamp_as_utc(tmp_path):
    path = tmp_path / 'worker-1-2026-08-08__23_00_00.db'
    path.write_bytes(b'')
    expected = datetime(2026, 8, 8, 23, 0, 0, tzinfo=UTC).timestamp()

    assert parse_db_start_ts(str(path)) == expected


def test_parse_db_start_ts_handles_worker_names_containing_dashes(tmp_path):
    path = tmp_path / 'search-worker-7-2026-01-02__03_04_05.db'
    path.write_bytes(b'')
    expected = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC).timestamp()

    assert parse_db_start_ts(str(path)) == expected


def test_parse_db_start_ts_falls_back_to_mtime_when_name_is_unparsable(tmp_path):
    path = tmp_path / 'handmade-copy.db'
    path.write_bytes(b'')
    os.utime(path, (1_700_000_000, 1_700_000_000))

    assert parse_db_start_ts(str(path)) == 1_700_000_000


@pytest.mark.parametrize(
    ('raw', 'expected'),
    [
        ('search-fleet', 'search-fleet'),
        ('search_fleet_1', 'search_fleet_1'),
        ('search fleet', 'search_fleet'),
        ('a/b:c', 'a_b_c'),
        ('', 'default'),
        (None, 'default'),
    ],
)
def test_sanitize_cluster_maps_unsafe_characters(raw, expected):
    assert sanitize_cluster(raw) == expected


def test_archive_file_name_uses_utc_minute_precision():
    start = datetime(2026, 8, 8, 0, 0, 0, tzinfo=UTC).timestamp()
    end = datetime(2026, 8, 9, 0, 0, 0, tzinfo=UTC).timestamp()

    assert archive_file_name('search fleet', start, end) == 'search_fleet-2026-08-08_00-00__2026-08-09_00-00.db.gz'


# --- window assignment --------------------------------------------------


def test_assign_windows_assigns_a_late_evening_file_to_its_own_day(tmp_path):
    start = datetime(2026, 8, 8, 23, 0, 0, tzinfo=UTC).timestamp()
    path = _make_db(tmp_path, 'worker-1', start)
    now = start + 3 * DAY

    windows = assign_windows([path], DAY, now, HOUR)

    assert len(windows) == 1
    assert windows[0].start == datetime(2026, 8, 8, 0, 0, 0, tzinfo=UTC).timestamp()
    assert windows[0].end == datetime(2026, 8, 9, 0, 0, 0, tzinfo=UTC).timestamp()
    assert [c.path for c in windows[0].files] == [path]
    assert windows[0].files[0].window_start == windows[0].start


def test_assign_windows_returns_the_window_exactly_at_end_plus_one_window(tmp_path):
    start = datetime(2026, 8, 8, 10, 0, 0, tzinfo=UTC).timestamp()
    path = _make_db(tmp_path, 'worker-1', start)
    end = datetime(2026, 8, 9, 0, 0, 0, tzinfo=UTC).timestamp()

    assert assign_windows([path], DAY, end + DAY, HOUR) != []


def test_assign_windows_skips_the_window_one_second_before_it_is_due(tmp_path):
    start = datetime(2026, 8, 8, 10, 0, 0, tzinfo=UTC).timestamp()
    path = _make_db(tmp_path, 'worker-1', start)
    end = datetime(2026, 8, 9, 0, 0, 0, tzinfo=UTC).timestamp()

    assert assign_windows([path], DAY, end + DAY - 1, HOUR) == []


def test_assign_windows_vetoes_a_window_whose_file_was_written_recently(tmp_path):
    start = datetime(2026, 8, 8, 10, 0, 0, tzinfo=UTC).timestamp()
    now = start + 5 * DAY
    old = _make_db(tmp_path, 'worker-1', start)
    # Second file of the same window, still being written a minute ago.
    fresh = _make_db(tmp_path, 'worker-2', start + HOUR, mtime=now - 60)

    assert assign_windows([old, fresh], DAY, now, HOUR) == []
    # Without the still-warm file the same window is due.
    assert len(assign_windows([old], DAY, now, HOUR)) == 1


def test_assign_windows_groups_separate_days_into_separate_windows(tmp_path):
    day_one = datetime(2026, 8, 8, 1, 0, 0, tzinfo=UTC).timestamp()
    day_two = datetime(2026, 8, 9, 1, 0, 0, tzinfo=UTC).timestamp()
    first = _make_db(tmp_path, 'worker-1', day_one)
    second = _make_db(tmp_path, 'worker-1', day_two)
    now = day_two + 5 * DAY

    windows = assign_windows([second, first], DAY, now, HOUR)

    assert [w.start for w in windows] == [
        datetime(2026, 8, 8, tzinfo=UTC).timestamp(),
        datetime(2026, 8, 9, tzinfo=UTC).timestamp(),
    ]


# --- archive pass -------------------------------------------------------


def _due_start(now: float, *, days_back: int = 3) -> float:
    """A file start time inside a window that is comfortably due at ``now``."""
    day_start = (now // DAY) * DAY
    return day_start - days_back * DAY + 10 * HOUR


def _run(tmp_path, cfg=None, cluster=CLUSTER, exclude_path=''):
    run_archive_pass(
        db_dir=str(tmp_path),
        worker_name='worker-1',
        cluster=cluster,
        cfg=cfg or _make_config(),
        exclude_path=exclude_path,
    )


def _archives(tmp_path) -> list[str]:
    return sorted(p.name for p in tmp_path.glob('*.db.gz'))


def test_run_archive_pass_merges_a_due_window_and_removes_the_sources(tmp_path):
    now = time.time()
    start = _due_start(now)
    first = _make_db(tmp_path, 'worker-1', start, event_count=2)
    second = _make_db(tmp_path, 'worker-2', start + HOUR, event_count=3)

    _run(tmp_path)

    assert not Path(first).exists()
    assert not Path(second).exists()
    for source in (first, second):
        for suffix in ('-wal', '-shm'):
            assert not Path(source + suffix).exists()
    names = _archives(tmp_path)
    assert len(names) == 1
    window_start = (start // DAY) * DAY
    assert names[0] == archive_file_name(CLUSTER, window_start, window_start + DAY)

    # The archive gunzips to a valid merged SQLite DB holding both workers.
    merged = tmp_path / 'unpacked.db'
    with gzip.open(tmp_path / names[0], 'rb') as gz:
        merged.write_bytes(gz.read())
    db = sqlite3.connect(str(merged))
    try:
        assert db.execute('SELECT COUNT(*) FROM events').fetchone()[0] == 5
        workers = {row[0] for row in db.execute('SELECT worker_name FROM workers')}
    finally:
        db.close()
    assert workers == {'worker-1', 'worker-2'}


def test_run_archive_pass_logs_the_created_archive(tmp_path):
    now = time.time()
    _make_db(tmp_path, 'worker-1', _due_start(now))

    with capture_logs() as cap:
        _run(tmp_path)

    created = [e for e in cap if e['event'] == 'recorder_archive_created']
    assert len(created) == 1
    assert created[0]['cluster'] == CLUSTER
    assert created[0]['file_count'] == 1
    assert created[0]['raw_bytes'] > 0
    assert created[0]['compressed_bytes'] > 0


def test_run_archive_pass_is_a_no_op_when_no_window_is_due(tmp_path):
    now = time.time()
    path = _make_db(tmp_path, 'worker-1', now - HOUR, mtime=now - HOUR)

    _run(tmp_path)

    assert Path(path).exists()
    assert _archives(tmp_path) == []
    assert not _lock_path(tmp_path).exists()


def test_run_archive_pass_never_touches_the_live_database(tmp_path):
    now = time.time()
    start = _due_start(now)
    live = _make_db(tmp_path, 'worker-1', start)
    rotated = _make_db(tmp_path, 'worker-1', start + HOUR)

    _run(tmp_path, exclude_path=live)

    assert Path(live).exists()
    assert not Path(rotated).exists()
    assert len(_archives(tmp_path)) == 1


def test_run_archive_pass_ignores_another_workers_in_flight_merge_file(tmp_path):
    now = time.time()
    start = _due_start(now)
    mine = _make_db(tmp_path, 'worker-1', start)
    # A peer worker's intermediate merge file, dot-prefixed and old.
    in_flight = tmp_path / '.index-fleet-2026-01-01_00-00__2026-01-02_00-00.db.gz.merge.db'
    in_flight.write_bytes(b'')
    os.utime(in_flight, (start, start))

    _run(tmp_path)

    assert in_flight.exists()
    assert not Path(mine).exists()


def test_run_archive_pass_leaves_other_clusters_files_alone(tmp_path):
    now = time.time()
    start = _due_start(now)
    mine = _make_db(tmp_path, 'worker-1', start)
    theirs = _make_db(tmp_path, 'worker-9', start + HOUR, cluster=OTHER_CLUSTER)

    _run(tmp_path)

    assert not Path(mine).exists()
    assert Path(theirs).exists()
    assert _archives(tmp_path) == [archive_file_name(CLUSTER, (start // DAY) * DAY, (start // DAY) * DAY + DAY)]


def test_run_archive_pass_groups_a_file_without_worker_config_under_default(tmp_path):
    now = time.time()
    start = _due_start(now)
    orphan = tmp_path / _db_name('worker-x', start)
    orphan.write_bytes(b'')
    os.utime(orphan, (start, start))

    _run(tmp_path, cluster='')

    assert not orphan.exists()
    assert len(_archives(tmp_path)) == 1
    assert _archives(tmp_path)[0].startswith('default-')


# --- election -----------------------------------------------------------


def test_run_archive_pass_skips_when_another_worker_holds_a_fresh_lock(tmp_path):
    now = time.time()
    source = _make_db(tmp_path, 'worker-1', _due_start(now))
    lock = _lock_path(tmp_path)
    lock.write_text(json.dumps({'pid': 999, 'worker': 'worker-2', 'ts': now}))

    _run(tmp_path)

    assert Path(source).exists()
    assert _archives(tmp_path) == []
    assert lock.exists()


def test_run_archive_pass_takes_over_a_stale_lock(tmp_path):
    now = time.time()
    source = _make_db(tmp_path, 'worker-1', _due_start(now))
    lock = _lock_path(tmp_path)
    lock.write_text(json.dumps({'pid': 999, 'worker': 'worker-2', 'ts': 0}))
    stale = now - LOCK_STALE_SECONDS - 1
    os.utime(lock, (stale, stale))

    _run(tmp_path)

    assert not Path(source).exists()
    assert len(_archives(tmp_path)) == 1
    assert not lock.exists()


def test_run_archive_pass_writes_owner_identity_into_the_lock(tmp_path, monkeypatch):
    now = time.time()
    _make_db(tmp_path, 'worker-1', _due_start(now))
    seen: dict = {}

    real_merge = archive_mod.merge_databases

    def spy(paths, output_path):
        seen['lock'] = json.loads(_lock_path(tmp_path).read_text())
        return real_merge(paths, output_path)

    monkeypatch.setattr(archive_mod, 'merge_databases', spy)

    _run(tmp_path)

    assert seen['lock']['pid'] == os.getpid()
    assert seen['lock']['worker'] == 'worker-1'
    assert seen['lock']['ts'] > 0


def test_run_archive_pass_releases_the_lock_after_a_failure(tmp_path, monkeypatch):
    now = time.time()
    source = _make_db(tmp_path, 'worker-1', _due_start(now))

    def boom(paths, output_path):
        raise OSError('merge blew up')

    monkeypatch.setattr(archive_mod, 'merge_databases', boom)

    with capture_logs() as cap:
        _run(tmp_path)

    assert not _lock_path(tmp_path).exists()
    assert Path(source).exists()
    assert _archives(tmp_path) == []
    failures = [e for e in cap if e['event'] == 'recorder_archive_failed']
    assert len(failures) == 1
    assert failures[0]['cluster'] == CLUSTER


def test_run_archive_pass_keeps_sources_when_compression_fails(tmp_path, monkeypatch):
    now = time.time()
    source = _make_db(tmp_path, 'worker-1', _due_start(now))

    def boom(*args, **kwargs):
        raise OSError('disk full')

    monkeypatch.setattr(archive_mod.gzip, 'open', boom)

    _run(tmp_path)

    assert Path(source).exists()
    for suffix in ('-wal', '-shm'):
        assert Path(source + suffix).exists()
    assert _archives(tmp_path) == []
    # No temporary or intermediate files survive the failure.
    assert [p.name for p in tmp_path.iterdir() if p.name.startswith('.')] == []


# --- expiry -------------------------------------------------------------


def test_run_archive_pass_expires_only_this_clusters_old_archives(tmp_path):
    now = time.time()
    _make_db(tmp_path, 'worker-1', _due_start(now, days_back=3))
    old_mine = _make_archive(tmp_path, CLUSTER, now - 10 * DAY)
    recent_mine = _make_archive(tmp_path, CLUSTER, now - 1 * DAY)
    old_theirs = _make_archive(tmp_path, OTHER_CLUSTER, now - 10 * DAY)

    with capture_logs() as cap:
        _run(tmp_path, cfg=_make_config(archive_retention_days=3))

    assert not old_mine.exists()
    assert recent_mine.exists()
    assert old_theirs.exists()
    # The archive this pass just wrote (window ended ~2 days ago) survives.
    assert len(_archives(tmp_path)) == 3
    expired = [e for e in cap if e['event'] == 'recorder_archive_expired']
    assert [e['name'] for e in expired] == [old_mine.name]


def test_run_archive_pass_keeps_every_archive_when_retention_is_zero(tmp_path):
    now = time.time()
    _make_db(tmp_path, 'worker-1', _due_start(now))
    old_mine = _make_archive(tmp_path, CLUSTER, now - 100 * DAY)

    _run(tmp_path, cfg=_make_config(archive_retention_days=0))

    assert old_mine.exists()


# --- recorder wiring ----------------------------------------------------


def _make_recorder(tmp_path, **recorder_overrides) -> EventRecorder:
    config = make_ui_config(enabled=True, db_dir=str(tmp_path), flush_interval_seconds=60, **recorder_overrides)
    return EventRecorder(config, worker_name='worker-1', cluster_name=CLUSTER)


async def test_archive_pass_runs_off_the_event_loop_with_the_live_db_excluded(tmp_path, monkeypatch):
    rec = _make_recorder(tmp_path)
    rec._db_path = str(tmp_path / 'worker-1-live-now.db')
    seen: dict = {}

    def spy(**kwargs) -> None:
        seen.update(kwargs)
        seen['thread'] = threading.current_thread().ident

    monkeypatch.setattr(core_mod, 'run_archive_pass', spy)

    await rec._archive_pass()

    assert seen['db_dir'] == str(tmp_path)
    assert seen['worker_name'] == 'worker-1'
    assert seen['cluster'] == CLUSTER
    assert seen['exclude_path'] == rec._db_path
    assert seen['cfg'] is rec._store
    assert seen['thread'] != threading.current_thread().ident


async def test_archive_pass_does_nothing_when_archiving_is_disabled(tmp_path, monkeypatch):
    rec = _make_recorder(tmp_path, archive_enabled=False)
    calls = 0

    def spy(**kwargs) -> None:
        nonlocal calls
        calls += 1

    monkeypatch.setattr(core_mod, 'run_archive_pass', spy)

    await rec._archive_pass()

    assert calls == 0


async def test_archive_pass_does_nothing_without_a_db_dir(tmp_path, monkeypatch):
    rec = _make_recorder(tmp_path)
    rec._store = rec._store.model_copy(update={'db_dir': ''})
    calls = 0

    def spy(**kwargs) -> None:
        nonlocal calls
        calls += 1

    monkeypatch.setattr(core_mod, 'run_archive_pass', spy)

    await rec._archive_pass()

    assert calls == 0


async def test_start_notifies_once_when_archiving_is_disabled(tmp_path):
    rec = _make_recorder(tmp_path, archive_enabled=False)

    with capture_logs() as cap:
        await rec.start()
        await rec.stop()

    notices = [e for e in cap if e['event'] == 'recorder_archiving_disabled']
    assert len(notices) == 1
    assert notices[0]['log_level'] == 'info'


async def test_start_stays_quiet_when_archiving_is_enabled(tmp_path):
    rec = _make_recorder(tmp_path)

    with capture_logs() as cap:
        await rec.start()
        await rec.stop()

    assert [e for e in cap if e['event'] == 'recorder_archiving_disabled'] == []
