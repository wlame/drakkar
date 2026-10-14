"""Statistics of a worker's own recorder files for the databases page (drakkar.dbstats).

Own files are measured on request and kept in memory: the live file grows,
and its events table is append-only with a monotonic id, so a refresh
aggregates only the rows past the cached cursor. A replaced file (the
cursor goes backwards) falls back to a full scan. Sealed files never come
here; their meta file carries their statistics.
"""

from __future__ import annotations

import os
import sqlite3
import time

import drakkar.dbstats as dbstats_mod
from drakkar.dbstats import OwnFileStats
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


def _no_full_scan(path):
    raise AssertionError(f'full scan of {path}')


def test_measure_reports_the_stats_of_each_file_in_order(tmp_path):
    first = str(tmp_path / 'w1-2026-10-14__08_00_00.db')
    second = str(tmp_path / 'w1-2026-10-14__09_00_00.db')
    make_recorder_db(first, events=[('consumed', 1.0), ('consumed', 2.0)])
    make_recorder_db(second, events=[('arranged', 5.0)])

    stats = OwnFileStats().measure([first, second])

    assert [(s.filename, s.event_count, s.event_counts) for s in stats] == [
        (os.path.basename(first), 2, {'consumed': 2}),
        (os.path.basename(second), 1, {'arranged': 1}),
    ]
    assert (stats[0].worker_name, stats[0].cluster_name, stats[0].first_event_ts) == ('w1', 'c1', 1.0)


def test_a_growing_file_is_refreshed_incrementally(tmp_path, monkeypatch):
    path = str(tmp_path / 'w1-2026-10-14__08_00_00.db')
    make_recorder_db(path, events=[('consumed', 1.0), ('consumed', 2.0)])
    own = OwnFileStats()
    own.measure([path])
    db = sqlite3.connect(path)
    add_events(db, [('arranged', 3.0), ('consumed', 4.0)])
    db.commit()
    db.close()
    monkeypatch.setattr(dbstats_mod, 'scan_db', _no_full_scan)

    (stats,) = own.measure([path])

    assert (stats.event_count, stats.event_counts) == (4, {'consumed': 3, 'arranged': 1})
    assert (stats.last_event_ts, stats.max_event_id) == (4.0, 4)


def test_a_replaced_file_falls_back_to_a_full_scan(tmp_path):
    path = str(tmp_path / 'w1-2026-10-14__08_00_00.db')
    make_recorder_db(path, events=[('consumed', 1.0), ('consumed', 2.0), ('consumed', 3.0)])
    own = OwnFileStats()
    own.measure([path])
    os.remove(path)
    make_recorder_db(path, events=[('produced', 9.0)])

    (stats,) = own.measure([path])

    assert (stats.event_count, stats.event_counts) == (1, {'produced': 1})


def test_a_vanished_file_is_skipped_and_forgotten(tmp_path):
    path = str(tmp_path / 'w1-2026-10-14__08_00_00.db')
    make_recorder_db(path, events=[('consumed', 1.0)])
    own = OwnFileStats()
    own.measure([path])
    os.remove(path)

    assert own.measure([path]) == []
    assert own.cached_paths() == []
