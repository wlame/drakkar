"""Tests for the databases-page stats cache (drakkar.dbstats).

The invariants pinned here are the design's promises:

- the file LIST always mirrors the real directory (delete a file →
  gone next call; add one → present next call), regardless of what the
  cache says — the eventual-consistency requirement;
- rotated (immutable) files are scanned exactly once — the second
  collect() reuses the cache;
- a growing live DB is refreshed by a DELTA scan (only rows past the
  cached max id), and a replaced file (cursor going backwards) falls
  back to a full scan;
- the inline budget caps full scans per call, with honest
  stats_pending rows for the remainder, and the warmer finishes the job
  and purges rows for deleted files;
- symlinks: ``*-live.db`` marks its target in-use; ``*-cache.db``
  produces a kind='cache' row under the symlink's display name.

Everything runs on real temp-dir SQLite files (sqlite3 is stdlib and
local — no external service), matching the merge-module test style.
"""

from __future__ import annotations

import os
import sqlite3
import time

from drakkar.dbstats import DBSTATS_FILENAME, DbStatsCache, collect, warm_directory
from drakkar.merge import scan_db
from drakkar.recorder.schema import SCHEMA_EVENTS, SCHEMA_WORKER_CONFIG


def make_recorder_db(path: str, *, worker: str = 'w1', events: list[tuple[str, float]] = ()) -> None:
    """A minimal real recorder DB: worker_config + events rows."""
    db = sqlite3.connect(path)
    db.executescript(SCHEMA_EVENTS)
    db.executescript(SCHEMA_WORKER_CONFIG)
    db.execute(
        'INSERT INTO worker_config (id, worker_name, cluster_name, created_at, created_at_dt) VALUES (1, ?, ?, ?, ?)',
        [worker, 'c1', time.time(), 'dt'],
    )
    add_events(db, events)
    db.commit()
    db.close()


def add_events(db: sqlite3.Connection, events: list[tuple[str, float]]) -> None:
    for name, ts in events:
        db.execute('INSERT INTO events (ts, dt, event) VALUES (?, ?, ?)', [ts, 'dt', name])


def make_cache_db(path: str, *, entries: int) -> None:
    db = sqlite3.connect(path)
    db.execute('CREATE TABLE cache_entries (key TEXT PRIMARY KEY, value TEXT)')
    for i in range(entries):
        db.execute('INSERT INTO cache_entries VALUES (?, ?)', [f'k{i}', 'v'])
    db.commit()
    db.close()


def bump_mtime(path: str) -> None:
    """Force a visible mtime change even on coarse-resolution filesystems."""
    st = os.stat(path)
    os.utime(path, ns=(st.st_atime_ns, st.st_mtime_ns + 1_000_000))


class TestScanDbClassification:
    def test_recorder_db_kind_and_cursor(self, tmp_path):
        p = str(tmp_path / 'w1-x.db')
        make_recorder_db(p, events=[('consumed', 1.0), ('arranged', 2.0)])
        stats = scan_db(p)
        assert stats.kind == 'recorder'
        assert stats.event_count == 2
        assert stats.max_event_id == 2
        assert stats.cache_entry_count is None

    def test_cache_db_kind_and_entry_count(self, tmp_path):
        p = str(tmp_path / 'w1-cache.db.actual')
        make_cache_db(p, entries=7)
        stats = scan_db(p)
        assert stats.kind == 'cache'
        assert stats.cache_entry_count == 7
        assert not stats.has_events

    def test_foreign_schema_is_unknown(self, tmp_path):
        p = str(tmp_path / 'other.db')
        db = sqlite3.connect(p)
        db.execute('CREATE TABLE foo (x)')
        db.commit()
        db.close()
        assert scan_db(p).kind == 'unknown'


class TestCollectListing:
    def test_listing_mirrors_the_directory_immediately(self, tmp_path):
        """The eventual-consistency requirement: existence is never cached."""
        cache = DbStatsCache(str(tmp_path))
        a = str(tmp_path / 'w1-a.db')
        make_recorder_db(a, events=[('consumed', 1.0)])

        rows = collect(str(tmp_path), cache, inline_scan_limit=-1)
        assert [r.stats.filename for r in rows] == ['w1-a.db']

        b = str(tmp_path / 'w1-b.db')
        make_recorder_db(b, events=[('consumed', 2.0)])
        os.remove(a)
        rows = collect(str(tmp_path), cache, inline_scan_limit=-1)
        # Deleted file gone, new file present — on the very next call,
        # even though the cache still holds a row for the deleted one.
        assert [r.stats.filename for r in rows] == ['w1-b.db']

    def test_dbstats_cache_file_itself_never_appears(self, tmp_path):
        cache = DbStatsCache(str(tmp_path))
        make_recorder_db(str(tmp_path / 'w1-a.db'))
        collect(str(tmp_path), cache, inline_scan_limit=-1)
        assert (tmp_path / DBSTATS_FILENAME).exists()
        rows = collect(str(tmp_path), cache, inline_scan_limit=-1)
        assert all(r.stats.filename != DBSTATS_FILENAME for r in rows)

    def test_immutable_file_is_scanned_once_then_cached(self, tmp_path, monkeypatch):
        cache = DbStatsCache(str(tmp_path))
        make_recorder_db(str(tmp_path / 'w1-a.db'), events=[('consumed', 1.0)])
        collect(str(tmp_path), cache, inline_scan_limit=-1)

        # A second collect must not full-scan again: poison scan_db.
        import drakkar.dbstats as dbstats_mod

        def explode(path):
            raise AssertionError(f'unexpected full scan of {path}')

        monkeypatch.setattr(dbstats_mod, 'scan_db', explode)
        rows = collect(str(tmp_path), cache, inline_scan_limit=-1)
        assert rows[0].stats.event_count == 1
        assert not rows[0].stats_pending


class TestDeltaScan:
    def test_growing_db_is_refreshed_incrementally(self, tmp_path, monkeypatch):
        cache = DbStatsCache(str(tmp_path))
        p = str(tmp_path / 'w1-live-target.db')
        make_recorder_db(p, events=[('consumed', 1.0), ('consumed', 2.0)])
        collect(str(tmp_path), cache, inline_scan_limit=-1)

        # Append events and change identity; a delta (not a full scan)
        # must pick them up.
        db = sqlite3.connect(p)
        add_events(db, [('arranged', 3.0), ('consumed', 4.0)])
        db.commit()
        db.close()
        bump_mtime(p)

        import drakkar.dbstats as dbstats_mod

        monkeypatch.setattr(dbstats_mod, 'scan_db', lambda path: (_ for _ in ()).throw(AssertionError('full scan')))
        rows = collect(str(tmp_path), cache, inline_scan_limit=-1)
        (row,) = rows
        assert row.stats.event_count == 4
        assert row.stats.event_counts == {'consumed': 3, 'arranged': 1}
        assert row.stats.last_event_ts == 4.0
        assert row.stats.max_event_id == 4

    def test_replaced_file_falls_back_to_full_scan(self, tmp_path):
        cache = DbStatsCache(str(tmp_path))
        p = str(tmp_path / 'w1-x.db')
        make_recorder_db(p, events=[('consumed', 1.0), ('consumed', 2.0), ('consumed', 3.0)])
        collect(str(tmp_path), cache, inline_scan_limit=-1)

        # Replace with a SMALLER db — max id went backwards.
        os.remove(p)
        make_recorder_db(p, events=[('produced', 9.0)])
        bump_mtime(p)

        rows = collect(str(tmp_path), cache, inline_scan_limit=-1)
        (row,) = rows
        assert row.stats.event_counts == {'produced': 1}
        assert row.stats.event_count == 1


class TestInlineBudgetAndWarmer:
    def test_budget_caps_full_scans_and_marks_pending(self, tmp_path):
        cache = DbStatsCache(str(tmp_path))
        for i in range(4):
            make_recorder_db(str(tmp_path / f'w1-{i}.db'), events=[('consumed', float(i + 1))])

        rows = collect(str(tmp_path), cache, inline_scan_limit=2)
        scanned = [r for r in rows if not r.stats_pending]
        pending = [r for r in rows if r.stats_pending]
        assert len(scanned) == 2
        assert len(pending) == 2
        # Pending rows still carry identity: name, path, size.
        assert all(r.stats.size_bytes > 0 for r in pending)
        assert all(r.stats.event_count == 0 for r in pending)

    def test_warmer_completes_and_purges(self, tmp_path):
        cache = DbStatsCache(str(tmp_path))
        for i in range(3):
            make_recorder_db(str(tmp_path / f'w1-{i}.db'), events=[('consumed', 1.0)])
        collect(str(tmp_path), cache, inline_scan_limit=0)  # everything pending

        cached_count, purged = warm_directory(str(tmp_path), cache)
        assert cached_count == 3
        assert purged == 0
        rows = collect(str(tmp_path), cache, inline_scan_limit=0)
        assert all(not r.stats_pending for r in rows)

        os.remove(str(tmp_path / 'w1-0.db'))
        _, purged = warm_directory(str(tmp_path), cache)
        assert purged == 1

    def test_zero_budget_never_scans_inline(self, tmp_path, monkeypatch):
        cache = DbStatsCache(str(tmp_path))
        make_recorder_db(str(tmp_path / 'w1-a.db'))

        import drakkar.dbstats as dbstats_mod

        monkeypatch.setattr(dbstats_mod, 'scan_db', lambda path: (_ for _ in ()).throw(AssertionError('scan')))
        rows = collect(str(tmp_path), cache, inline_scan_limit=0)
        assert rows[0].stats_pending


class TestSymlinks:
    def test_live_symlink_marks_its_target_in_use(self, tmp_path):
        cache = DbStatsCache(str(tmp_path))
        current = str(tmp_path / 'w1-2026.db')
        rotated = str(tmp_path / 'w1-2025.db')
        make_recorder_db(current)
        make_recorder_db(rotated)
        os.symlink(current, str(tmp_path / 'w1-live.db'))

        rows = {r.stats.filename: r for r in collect(str(tmp_path), cache, inline_scan_limit=-1)}
        assert rows['w1-2026.db'].live_for == 'w1'
        assert rows['w1-2025.db'].live_for == ''
        # The symlink itself is not a row.
        assert 'w1-live.db' not in rows

    def test_cache_symlink_becomes_a_cache_row_under_its_stable_name(self, tmp_path):
        cache = DbStatsCache(str(tmp_path))
        target = str(tmp_path / 'w1-cache.db.actual')
        make_cache_db(target, entries=5)
        os.symlink(target, str(tmp_path / 'w1-cache.db'))
        # Worker w1's recorder is live too — its cache row counts as in use.
        current = str(tmp_path / 'w1-2026.db')
        make_recorder_db(current)
        os.symlink(current, str(tmp_path / 'w1-live.db'))

        rows = {r.stats.filename: r for r in collect(str(tmp_path), cache, inline_scan_limit=-1)}
        row = rows['w1-cache.db']
        assert row.stats.kind == 'cache'
        assert row.stats.cache_entry_count == 5
        assert row.live_for == 'w1'
        # Worker identity comes from the symlink name — cache DBs have no
        # worker_config table to read it from.
        assert row.stats.worker_name == 'w1'

    def test_cache_row_not_in_use_without_a_live_recorder(self, tmp_path):
        cache = DbStatsCache(str(tmp_path))
        target = str(tmp_path / 'w1-cache.db.actual')
        make_cache_db(target, entries=1)
        os.symlink(target, str(tmp_path / 'w1-cache.db'))

        rows = {r.stats.filename: r for r in collect(str(tmp_path), cache, inline_scan_limit=-1)}
        assert rows['w1-cache.db'].live_for == ''

    def test_dangling_symlink_is_ignored(self, tmp_path):
        cache = DbStatsCache(str(tmp_path))
        os.symlink(str(tmp_path / 'gone.db'), str(tmp_path / 'w1-live.db'))
        assert collect(str(tmp_path), cache, inline_scan_limit=-1) == []


class TestMemoryOnlyMode:
    def test_empty_db_dir_creates_no_stray_cache_file(self, tmp_path, monkeypatch):
        """db_dir='' (memory-only recorder): collect must not open the
        cache at all — that would create ./.dbstats.db in the CWD."""
        monkeypatch.chdir(tmp_path)
        assert collect('', DbStatsCache(''), inline_scan_limit=4) == []
        assert not (tmp_path / DBSTATS_FILENAME).exists()


class TestCacheResilience:
    def test_corrupt_cache_file_self_heals(self, tmp_path, monkeypatch):
        cache = DbStatsCache(str(tmp_path))
        make_recorder_db(str(tmp_path / 'w1-a.db'), events=[('consumed', 1.0)])
        (tmp_path / DBSTATS_FILENAME).write_bytes(b'this is not sqlite at all')

        rows = collect(str(tmp_path), cache, inline_scan_limit=-1)
        assert rows[0].stats.event_count == 1
        assert not rows[0].stats_pending

        # The corrupt file was replaced by a working cache: the next call
        # reuses it without a full scan.
        import drakkar.dbstats as dbstats_mod

        monkeypatch.setattr(dbstats_mod, 'scan_db', lambda path: (_ for _ in ()).throw(AssertionError('scan')))
        rows = collect(str(tmp_path), cache, inline_scan_limit=-1)
        assert rows[0].stats.event_count == 1


class TestWarmerLeavesPeerLiveDatabasesAlone:
    """The warmer refreshes only the worker's OWN live database.

    Every worker sharing a ``db_dir`` used to delta-scan every other
    worker's live DB on each sweep. Those rows carry stdout/stderr, so a
    sweep touches many pages, and the cost is N-squared across the fleet
    for a page nobody may open — over a shared or NFS-mounted directory
    that is a lot of network reads per minute. Each worker now contributes
    its own file's statistics to the shared cache; peers read them from
    there.
    """

    @staticmethod
    def _live_pair(tmp_path, worker: str, events: int) -> str:
        """A recorder DB plus the ``<worker>-live.db`` symlink marking it in use."""
        target = str(tmp_path / f'{worker}-2026-08-23__10_00_00.db')
        make_recorder_db(target, worker=worker, events=[('consumed', float(i + 1)) for i in range(events)])
        os.symlink(target, str(tmp_path / f'{worker}-live.db'))
        return target

    def _grow(self, path: str, events: int) -> None:
        db = sqlite3.connect(path)
        add_events(db, [('consumed', 99.0) for _ in range(events)])
        db.commit()
        db.close()

    def test_sweep_delta_scans_only_the_own_live_db(self, tmp_path, monkeypatch):
        import drakkar.dbstats as dbstats_mod

        cache = DbStatsCache(str(tmp_path))
        mine = self._live_pair(tmp_path, 'me', 2)
        peer = self._live_pair(tmp_path, 'peer', 2)
        warm_directory(str(tmp_path), cache)  # no own db yet: full sweep caches both

        # Both files grow, so both would otherwise be re-read next sweep.
        self._grow(mine, 3)
        self._grow(peer, 3)

        touched: list[str] = []
        real_delta = dbstats_mod._delta_scan
        monkeypatch.setattr(
            dbstats_mod,
            '_delta_scan',
            lambda path, cached: (touched.append(path), real_delta(path, cached))[1],
        )
        monkeypatch.setattr(
            dbstats_mod, 'scan_db', lambda path: (_ for _ in ()).throw(AssertionError(f'scanned {path}'))
        )

        warm_directory(str(tmp_path), cache, own_live_db=mine)
        assert touched == [mine]

    def test_peer_rows_survive_the_purge_pass(self, tmp_path):
        """A skipped peer must still count as present, or its cached stats
        would be purged and the next page load would full-scan it."""
        cache = DbStatsCache(str(tmp_path))
        mine = self._live_pair(tmp_path, 'me', 1)
        peer = self._live_pair(tmp_path, 'peer', 1)
        warm_directory(str(tmp_path), cache)

        self._grow(peer, 5)
        cached_count, purged = warm_directory(str(tmp_path), cache, own_live_db=mine)
        assert purged == 0
        assert cached_count == 2
        assert peer in cache.load_all()

    def test_page_load_still_refreshes_peer_rows_on_demand(self, tmp_path):
        """Only the background sweep skips peers; a viewer gets fresh counts."""
        cache = DbStatsCache(str(tmp_path))
        self._live_pair(tmp_path, 'me', 1)
        peer = self._live_pair(tmp_path, 'peer', 1)
        warm_directory(str(tmp_path), cache)

        self._grow(peer, 4)
        rows = collect(str(tmp_path), cache, inline_scan_limit=-1)
        peer_row = next(r for r in rows if r.stats.path == peer)
        assert peer_row.stats.event_count == 5

    def test_without_an_own_db_the_sweep_refreshes_everything(self, tmp_path):
        """Default behaviour is unchanged — the merge CLI and tests rely on it."""
        cache = DbStatsCache(str(tmp_path))
        peer = self._live_pair(tmp_path, 'peer', 1)
        warm_directory(str(tmp_path), cache)

        self._grow(peer, 4)
        warm_directory(str(tmp_path), cache)
        assert cache.load_all()[peer].stats.event_count == 5

    def test_an_uncached_peer_live_db_is_left_pending_not_scanned(self, tmp_path, monkeypatch):
        """A peer this worker has never seen waits for that peer's own warmer.

        The row still appears (the directory is the source of truth), so the
        purge pass keeps it and a page load fills it in.
        """
        import drakkar.dbstats as dbstats_mod

        cache = DbStatsCache(str(tmp_path))
        mine = self._live_pair(tmp_path, 'me', 1)
        peer = self._live_pair(tmp_path, 'peer', 3)

        monkeypatch.setattr(
            dbstats_mod,
            'scan_db',
            lambda path: (
                (_ for _ in ()).throw(AssertionError(f'scanned peer {path}')) if path == peer else scan_db(path)
            ),
        )
        cached_count, purged = warm_directory(str(tmp_path), cache, own_live_db=mine)
        assert cached_count == 2
        assert purged == 0
        assert peer not in cache.load_all()
