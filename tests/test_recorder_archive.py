"""Tests for the recorder archive engine (window math + archive pass)."""

import fcntl
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
    list_archives,
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


# --- list_archives --------------------------------------------------


def test_list_archives_empty_when_db_dir_unset():
    assert list_archives('') == []


def test_list_archives_empty_when_db_dir_has_nothing(tmp_path):
    assert list_archives(str(tmp_path)) == []


def test_list_archives_parses_name_into_fields(tmp_path):
    start = datetime(2026, 8, 8, 0, 0, 0, tzinfo=UTC).timestamp()
    end = datetime(2026, 8, 9, 0, 0, 0, tzinfo=UTC).timestamp()
    path = _make_archive(tmp_path, CLUSTER, end)
    path.write_bytes(b'x' * 37)

    [entry] = list_archives(str(tmp_path))

    assert entry.name == archive_file_name(CLUSTER, start, end)
    assert entry.cluster == sanitize_cluster(CLUSTER)
    assert entry.from_ts == start
    assert entry.to_ts == end
    assert entry.size_bytes == 37


def test_list_archives_sorts_newest_first_by_to_ts(tmp_path):
    day0 = datetime(2026, 8, 6, 0, 0, 0, tzinfo=UTC).timestamp()
    day1 = day0 + DAY
    day2 = day0 + 2 * DAY
    oldest = _make_archive(tmp_path, CLUSTER, day1)
    newest = _make_archive(tmp_path, CLUSTER, day2)
    middle = _make_archive(tmp_path, OTHER_CLUSTER, day1 + HOUR)  # unused window end, still < day2

    names = [entry.name for entry in list_archives(str(tmp_path))]

    assert names == [newest.name, middle.name, oldest.name]


def test_list_archives_excludes_dot_prefixed_files(tmp_path):
    end = datetime(2026, 8, 9, 0, 0, 0, tzinfo=UTC).timestamp()
    real = _make_archive(tmp_path, CLUSTER, end)
    # A compress-temp for the same window: dot-prefixed, pid-stamped.
    (tmp_path / f'.{real.name}.4242.tmp').write_bytes(b'partial')
    # A stale lock file left behind by a dead pass.
    (tmp_path / f'.archive-{CLUSTER}.lock').write_bytes(b'{}')

    [entry] = list_archives(str(tmp_path))

    assert entry.name == real.name


def test_list_archives_excludes_unreadable_quarantine_files(tmp_path):
    end = datetime(2026, 8, 9, 0, 0, 0, tzinfo=UTC).timestamp()
    real = _make_archive(tmp_path, CLUSTER, end)
    (tmp_path / f'{real.name}.unreadable').write_bytes(b'quarantined')

    [entry] = list_archives(str(tmp_path))

    assert entry.name == real.name


def test_list_archives_excludes_raw_db_files(tmp_path):
    end = datetime(2026, 8, 9, 0, 0, 0, tzinfo=UTC).timestamp()
    real = _make_archive(tmp_path, CLUSTER, end)
    _make_db(tmp_path, 'worker-1', end - HOUR)

    [entry] = list_archives(str(tmp_path))

    assert entry.name == real.name


def test_list_archives_skips_a_name_with_an_impossible_date(tmp_path):
    # The name regex only checks digit counts, so month 13 matches it but
    # is not a date. One stray file must not fail the whole listing.
    end = datetime(2026, 8, 9, 0, 0, 0, tzinfo=UTC).timestamp()
    real = _make_archive(tmp_path, CLUSTER, end)
    bogus = tmp_path / f'{CLUSTER}-2026-13-08_00-00__2026-13-09_00-00.db.gz'
    bogus.write_bytes(b'not a real window')

    [entry] = list_archives(str(tmp_path))

    assert entry.name == real.name


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


def test_run_archive_pass_sets_aside_a_source_the_merge_cannot_read(tmp_path):
    now = time.time()
    start = _due_start(now)
    healthy = _make_db(tmp_path, 'worker-1', start, cluster=None, event_count=2)
    corrupt = tmp_path / _db_name('worker-2', start + HOUR)
    corrupt.write_bytes(b'this file is not a SQLite database at all' * 8)
    for suffix in ('-wal', '-shm'):
        (tmp_path / (corrupt.name + suffix)).write_bytes(b'sidecar')
    os.utime(corrupt, (start, start))

    with capture_logs() as cap:
        _run(tmp_path, cluster='')

    # The unreadable file survives under a name that leaves the candidate set.
    assert not corrupt.exists()
    set_aside = tmp_path / (corrupt.name + '.unreadable')
    assert set_aside.exists()
    assert set_aside.read_bytes().startswith(b'this file is not a SQLite database')
    # The write-ahead log moves with it — it may hold the newest rows.
    for suffix in ('-wal', '-shm'):
        assert not (tmp_path / (corrupt.name + suffix)).exists()
        assert (tmp_path / (set_aside.name + suffix)).exists()
    # The healthy source was archived and only then deleted.
    assert not Path(healthy).exists()
    assert len(_archives(tmp_path)) == 1
    skipped = [e for e in cap if e['event'] == 'recorder_archive_source_skipped']
    assert [e['name'] for e in skipped] == [corrupt.name]
    assert skipped[0]['renamed_to'] == set_aside.name
    created = [e for e in cap if e['event'] == 'recorder_archive_created']
    assert created[0]['file_count'] == 1


def test_run_archive_pass_folds_a_published_archive_into_the_next_one(tmp_path):
    """A source that outlived its archive must not overwrite it."""
    now = time.time()
    start = _due_start(now)
    first = _make_db(tmp_path, 'worker-1', start, event_count=2)
    _make_db(tmp_path, 'worker-2', start + HOUR, event_count=3)

    _run(tmp_path)
    name = _archives(tmp_path)[0]
    published = (tmp_path / name).read_bytes()

    # A pass that died between publishing and deleting leaves a source behind.
    _make_db(tmp_path, 'worker-1', start, event_count=2)
    assert Path(first).exists()

    with capture_logs() as cap:
        _run(tmp_path)

    assert not Path(first).exists()
    assert _archives(tmp_path) == [name]
    # Already covered by the archive, so the file is reclaimed, not remerged.
    assert (tmp_path / name).read_bytes() == published
    assert [e['event'] for e in cap if e['event'].startswith('recorder_archive_')] == [
        'recorder_archive_sources_reclaimed'
    ]
    assert _events_in_archive(tmp_path / name) == 5


def test_run_archive_pass_adds_new_sources_to_a_published_archive(tmp_path):
    now = time.time()
    start = _due_start(now)
    _make_db(tmp_path, 'worker-1', start, event_count=2)

    _run(tmp_path)
    name = _archives(tmp_path)[0]

    # A third worker's file for the same window settles only afterwards.
    late = _make_db(tmp_path, 'worker-3', start + 2 * HOUR, event_count=4)

    _run(tmp_path)

    assert not Path(late).exists()
    assert _archives(tmp_path) == [name]
    assert _events_in_archive(tmp_path / name) == 6


def test_run_archive_pass_deletes_already_archived_sources_when_folding(tmp_path):
    """Sources skipped as already-archived are healthy — delete, never quarantine."""
    now = time.time()
    start = _due_start(now)
    first = _make_db(tmp_path, 'worker-1', start, event_count=2)
    second = _make_db(tmp_path, 'worker-2', start + HOUR, event_count=3)

    _run(tmp_path)
    name = _archives(tmp_path)[0]

    # A pass died after the rename: both sources are back, and a new file
    # for the same window has settled since.
    _make_db(tmp_path, 'worker-1', start, event_count=2)
    _make_db(tmp_path, 'worker-2', start + HOUR, event_count=3)
    late = _make_db(tmp_path, 'worker-3', start + 2 * HOUR, event_count=4)

    with capture_logs() as cap:
        _run(tmp_path)

    assert not Path(first).exists()
    assert not Path(second).exists()
    assert not Path(late).exists()
    assert list(tmp_path.glob('*.unreadable')) == []
    assert [e for e in cap if e['event'] == 'recorder_archive_source_skipped'] == []
    assert _archives(tmp_path) == [name]
    # Both already-archived sources kept their single copy of each event.
    assert _events_in_archive(tmp_path / name) == 9


def test_run_archive_pass_leaves_no_temporaries_on_the_reclaim_path(tmp_path):
    now = time.time()
    start = _due_start(now)
    _make_db(tmp_path, 'worker-1', start, event_count=2)

    _run(tmp_path)
    _make_db(tmp_path, 'worker-1', start, event_count=2)
    _run(tmp_path)

    assert [p.name for p in tmp_path.iterdir() if p.name.startswith('.')] == []


def _events_in_archive(path: Path) -> int:
    """Unpack an archive and count the events it carries."""
    unpacked = path.parent / f'unpacked-{path.name}.db'
    with gzip.open(path, 'rb') as gz:
        unpacked.write_bytes(gz.read())
    db = sqlite3.connect(str(unpacked))
    try:
        return db.execute('SELECT COUNT(*) FROM events').fetchone()[0]
    finally:
        db.close()
        unpacked.unlink()


# --- election -----------------------------------------------------------


def test_run_archive_pass_skips_while_another_worker_holds_the_lock(tmp_path):
    now = time.time()
    source = _make_db(tmp_path, 'worker-1', _due_start(now))
    lock = _lock_path(tmp_path)
    # A live holder: the lock is an flock, not the file's existence, so the
    # test takes the real thing on its own descriptor.
    holder = os.open(str(lock), os.O_CREAT | os.O_WRONLY, 0o600)
    fcntl.flock(holder, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        _run(tmp_path)
    finally:
        os.close(holder)

    assert Path(source).exists()
    assert _archives(tmp_path) == []
    assert lock.exists()


def test_run_archive_pass_takes_over_a_lock_file_left_by_a_dead_worker(tmp_path):
    now = time.time()
    source = _make_db(tmp_path, 'worker-1', _due_start(now))
    lock = _lock_path(tmp_path)
    # No process holds the flock — a dead worker's file, whatever its age.
    lock.write_text(json.dumps({'pid': 999, 'worker': 'worker-2', 'ts': 0}))
    stale = now - LOCK_STALE_SECONDS - 1
    os.utime(lock, (stale, stale))

    _run(tmp_path)

    assert not Path(source).exists()
    assert len(_archives(tmp_path)) == 1
    assert not lock.exists()


def test_run_archive_pass_stamps_its_pid_on_the_temporary_files(tmp_path, monkeypatch):
    now = time.time()
    _make_db(tmp_path, 'worker-1', _due_start(now))
    seen: dict = {}
    real_merge = archive_mod.merge_databases

    def spy(paths, output_path):
        seen['output'] = os.path.basename(output_path)
        return real_merge(paths, output_path)

    monkeypatch.setattr(archive_mod, 'merge_databases', spy)

    _run(tmp_path)

    assert seen['output'].endswith(f'.{os.getpid()}.merge.db')


def test_run_archive_pass_keeps_the_merge_temp_owner_only(tmp_path, monkeypatch):
    now = time.time()
    _make_db(tmp_path, 'worker-1', _due_start(now))
    seen: dict = {}
    real_compress = archive_mod._compress

    def spy(source_path, dest_path):
        seen['merge_mode'] = os.stat(source_path).st_mode & 0o777
        seen['archive_mode'] = os.stat(dest_path).st_mode & 0o777
        return real_compress(source_path, dest_path)

    monkeypatch.setattr(archive_mod, '_compress', spy)

    _run(tmp_path)

    assert seen['merge_mode'] == 0o600
    assert seen['archive_mode'] == 0o600
    assert os.stat(tmp_path / _archives(tmp_path)[0]).st_mode & 0o777 == 0o600


def test_run_archive_pass_sweeps_temporaries_left_by_a_dead_pass(tmp_path):
    now = time.time()
    _make_db(tmp_path, 'worker-1', _due_start(now))
    orphan = tmp_path / f'.{CLUSTER}-2020-01-01_00-00__2020-01-02_00-00.db.gz.4242.merge.db'
    orphan.write_bytes(b'leftover')
    fresh = tmp_path / f'.{CLUSTER}-2020-01-01_00-00__2020-01-02_00-00.db.gz.4243.merge.db'
    fresh.write_bytes(b'still warm')
    stale = now - LOCK_STALE_SECONDS - 1
    os.utime(orphan, (stale, stale))

    with capture_logs() as cap:
        _run(tmp_path)

    assert not orphan.exists()
    assert fresh.exists()
    swept = [e for e in cap if e['event'] == 'recorder_archive_temp_swept']
    assert [e['name'] for e in swept] == [orphan.name]


def test_run_archive_pass_keeps_earlier_windows_when_a_later_one_fails(tmp_path):
    now = time.time()
    first_start = _due_start(now, days_back=4)
    second_start = _due_start(now, days_back=3)
    first_source = _make_db(tmp_path, 'worker-1', first_start)
    second_source = _make_db(tmp_path, 'worker-1', second_start)
    real_merge = archive_mod.merge_databases
    calls = 0

    def flaky(paths, output_path):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError('second window blew up')
        return real_merge(paths, output_path)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(archive_mod, 'merge_databases', flaky)
        _run(tmp_path)

    # First window: archived and cleaned up. Second: untouched, retried next tick.
    assert not Path(first_source).exists()
    assert Path(second_source).exists()
    window_start = (first_start // DAY) * DAY
    assert _archives(tmp_path) == [archive_file_name(CLUSTER, window_start, window_start + DAY)]
    assert not _lock_path(tmp_path).exists()


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
