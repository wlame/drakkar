"""Merge multiple Drakkar debug SQLite databases into one.

The merged database contains:
- ``workers`` table: one row per source DB's worker_config
- ``events`` table: all events from all DBs, ordered by timestamp,
  with a ``worker_id`` foreign key linking each event to its worker
- ``worker_states`` table: all state snapshots with ``worker_id`` FK

This module is used by the debug UI merge feature and can also be
called programmatically.
"""

from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path

MERGED_SCHEMA = """
CREATE TABLE workers (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    worker_name          TEXT NOT NULL,
    cluster_name         TEXT,
    ip_address           TEXT,
    debug_port           INTEGER,
    debug_url            TEXT,
    kafka_brokers        TEXT,
    source_topic         TEXT,
    consumer_group       TEXT,
    binary_path          TEXT,
    max_workers          INTEGER,
    task_timeout_seconds INTEGER,
    max_retries          INTEGER,
    window_size          INTEGER,
    sinks_json           TEXT,
    env_vars_json        TEXT,
    created_at           REAL,
    created_at_dt        TEXT,
    source_file          TEXT NOT NULL
);

CREATE TABLE events (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    worker_id    INTEGER NOT NULL REFERENCES workers(id),
    ts           REAL NOT NULL,
    dt           TEXT NOT NULL,
    event        TEXT NOT NULL,
    partition    INTEGER,
    offset       INTEGER,
    task_id      TEXT,
    args         TEXT,
    stdout_size  INTEGER DEFAULT 0,
    stdout       TEXT,
    stderr       TEXT,
    exit_code    INTEGER,
    duration     REAL,
    output_topic TEXT,
    metadata     TEXT,
    pid          INTEGER
);

CREATE TABLE worker_states (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    worker_id           INTEGER NOT NULL REFERENCES workers(id),
    uptime_seconds      REAL,
    assigned_partitions TEXT,
    partition_count     INTEGER,
    pool_active         INTEGER,
    pool_max            INTEGER,
    total_queued        INTEGER,
    consumed_count      INTEGER,
    completed_count     INTEGER,
    failed_count        INTEGER,
    produced_count      INTEGER,
    committed_count     INTEGER,
    paused              INTEGER,
    updated_at          REAL NOT NULL,
    updated_at_dt       TEXT NOT NULL
);

CREATE INDEX idx_events_ts ON events(ts);
CREATE INDEX idx_events_dt ON events(dt);
CREATE INDEX idx_events_event ON events(event);
CREATE INDEX idx_events_task_id ON events(task_id);
CREATE INDEX idx_events_partition ON events(partition);
CREATE INDEX idx_events_worker ON events(worker_id);
CREATE INDEX idx_events_worker_ts ON events(worker_id, ts);
CREATE INDEX idx_states_worker ON worker_states(worker_id);
CREATE INDEX idx_states_updated ON worker_states(updated_at);
"""

_WORKER_CONFIG_COLUMNS = [
    'worker_name',
    'cluster_name',
    'ip_address',
    'debug_port',
    'debug_url',
    'kafka_brokers',
    'source_topic',
    'consumer_group',
    'binary_path',
    'max_workers',
    'task_timeout_seconds',
    'max_retries',
    'window_size',
    'sinks_json',
    'env_vars_json',
    'created_at',
    'created_at_dt',
]

_EVENT_COLUMNS = [
    'ts',
    'dt',
    'event',
    'partition',
    'offset',
    'task_id',
    'args',
    'stdout_size',
    'stdout',
    'stderr',
    'exit_code',
    'duration',
    'output_topic',
    'metadata',
    'pid',
]

_STATE_COLUMNS = [
    'uptime_seconds',
    'assigned_partitions',
    'partition_count',
    'pool_active',
    'pool_max',
    'total_queued',
    'consumed_count',
    'completed_count',
    'failed_count',
    'produced_count',
    'committed_count',
    'paused',
    'updated_at',
    'updated_at_dt',
]


@dataclass
class DbStats:
    """Statistics for a single source database file."""

    path: str
    filename: str
    worker_name: str = ''
    cluster_name: str = ''
    event_count: int = 0
    event_counts: dict[str, int] = field(default_factory=dict)
    first_event_ts: float | None = None
    last_event_ts: float | None = None
    has_events: bool = False
    has_config: bool = False
    has_state: bool = False
    size_bytes: int = 0


@dataclass
class MergeResult:
    """Result of a merge operation."""

    output_path: str
    worker_count: int = 0
    event_count: int = 0
    state_count: int = 0
    cluster_name: str = ''
    source_files: list[str] = field(default_factory=list)


def _dict_factory(cursor: sqlite3.Cursor, row: tuple) -> dict:
    """Row factory that returns dicts instead of tuples."""
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}


def _table_exists(db: sqlite3.Connection, table: str) -> bool:
    row = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        [table],
    ).fetchone()
    return row is not None


def scan_db(path: str) -> DbStats:
    """Read statistics from a single debug database file."""
    stats = DbStats(
        path=path,
        filename=os.path.basename(path),
        size_bytes=os.path.getsize(path),
    )
    try:
        db = sqlite3.connect(f'file:{path}?mode=ro', uri=True)
        db.row_factory = _dict_factory

        if _table_exists(db, 'worker_config'):
            stats.has_config = True
            row = db.execute('SELECT worker_name, cluster_name FROM worker_config WHERE id = 1').fetchone()
            if row:
                stats.worker_name = row['worker_name'] or ''
                stats.cluster_name = row['cluster_name'] or ''

        if _table_exists(db, 'events'):
            stats.has_events = True
            row = db.execute('SELECT COUNT(*) as cnt, MIN(ts) as first_ts, MAX(ts) as last_ts FROM events').fetchone()
            if row:
                stats.event_count = row['cnt'] or 0
                stats.first_event_ts = row['first_ts']
                stats.last_event_ts = row['last_ts']
            for row in db.execute('SELECT event, COUNT(*) as cnt FROM events GROUP BY event'):
                stats.event_counts[row['event']] = row['cnt']

        if _table_exists(db, 'worker_state'):
            stats.has_state = True

        db.close()
    except Exception:
        pass
    return stats


def scan_directory(db_dir: str) -> list[DbStats]:
    """Scan a directory for debug database files and return their stats.

    Excludes ``-live.db`` symlinks (they point to active DBs that may be
    written to) and returns results sorted by filename.
    """
    results: list[DbStats] = []
    if not db_dir or not os.path.isdir(db_dir):
        return results
    for entry in sorted(os.listdir(db_dir)):
        if not entry.endswith('.db'):
            continue
        full = os.path.join(db_dir, entry)
        # skip live symlinks — they point to active DBs
        if os.path.islink(full):
            continue
        if not os.path.isfile(full):
            continue
        results.append(scan_db(full))
    return results


def merge_databases(db_paths: list[str], output_path: str) -> MergeResult:
    """Merge multiple debug databases into a single file.

    Each source DB's ``worker_config`` becomes a row in the ``workers``
    table. Events and state snapshots reference their worker via FK.

    If all source DBs share the same ``cluster_name``, the result
    inherits that cluster.

    Args:
        db_paths: Paths to source .db files.
        output_path: Where to write the merged database.

    Returns:
        MergeResult with counts and metadata.
    """
    if os.path.exists(output_path):
        os.remove(output_path)

    out = sqlite3.connect(output_path)
    out.execute('PRAGMA journal_mode=WAL')
    out.execute('PRAGMA foreign_keys=ON')
    out.executescript(MERGED_SCHEMA)

    result = MergeResult(output_path=output_path)
    cluster_names: set[str] = set()

    # phase 1: insert all workers, collect worker_id mappings
    worker_map: dict[str, int] = {}  # db_path → worker_id

    for db_path in db_paths:
        basename = os.path.basename(db_path)
        result.source_files.append(basename)
        try:
            src = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True)
            src.row_factory = _dict_factory

            worker_id: int | None = None

            if _table_exists(src, 'worker_config'):
                row = src.execute('SELECT * FROM worker_config WHERE id = 1').fetchone()
                if row:
                    values = [row.get(col) for col in _WORKER_CONFIG_COLUMNS]
                    values.append(basename)
                    placeholders = ', '.join(['?'] * len(values))
                    cols = ', '.join([*_WORKER_CONFIG_COLUMNS, 'source_file'])
                    cursor = out.execute(f'INSERT INTO workers ({cols}) VALUES ({placeholders})', values)
                    worker_id = cursor.lastrowid
                    result.worker_count += 1
                    cluster = row.get('cluster_name')
                    if cluster:
                        cluster_names.add(cluster)
                    else:
                        cluster_names.add('')

            if worker_id is None:
                # no worker_config — create a placeholder worker row
                cursor = out.execute(
                    'INSERT INTO workers (worker_name, source_file) VALUES (?, ?)',
                    [Path(basename).stem, basename],
                )
                worker_id = cursor.lastrowid
                result.worker_count += 1
                cluster_names.add('')

            assert worker_id is not None
            worker_map[db_path] = worker_id
            src.close()
        except Exception:
            continue

    # phase 2: collect all events into a temp list, sort by ts, then insert
    all_events: list[tuple] = []

    for db_path in db_paths:
        if db_path not in worker_map:
            continue
        wid = worker_map[db_path]
        try:
            src = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True)
            src.row_factory = _dict_factory
            if _table_exists(src, 'events'):
                rows = src.execute('SELECT * FROM events ORDER BY ts').fetchall()
                for row in rows:
                    values = tuple(row.get(col) for col in _EVENT_COLUMNS)
                    all_events.append((wid, *values))
            src.close()
        except Exception:
            continue

    # sort all events by ts (index 1 in the tuple: worker_id, ts, ...)
    all_events.sort(key=lambda r: r[1] or 0)

    cols = ', '.join(['worker_id', *_EVENT_COLUMNS])
    placeholders = ', '.join(['?'] * (1 + len(_EVENT_COLUMNS)))
    out.executemany(f'INSERT INTO events ({cols}) VALUES ({placeholders})', all_events)
    result.event_count = len(all_events)

    # phase 3: merge worker_state rows
    for db_path in db_paths:
        if db_path not in worker_map:
            continue
        wid = worker_map[db_path]
        try:
            src = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True)
            src.row_factory = _dict_factory
            if _table_exists(src, 'worker_state'):
                rows = src.execute('SELECT * FROM worker_state ORDER BY updated_at').fetchall()
                cols = ', '.join(['worker_id', *_STATE_COLUMNS])
                placeholders = ', '.join(['?'] * (1 + len(_STATE_COLUMNS)))
                for row in rows:
                    values = [row.get(col) for col in _STATE_COLUMNS]
                    out.execute(f'INSERT INTO worker_states ({cols}) VALUES ({placeholders})', [wid, *values])
                    result.state_count += 1
            src.close()
        except Exception:
            continue

    out.commit()
    out.close()

    # determine cluster: if all sources share the same non-empty cluster
    non_empty = cluster_names - {''}
    if len(non_empty) == 1 and '' not in cluster_names:
        result.cluster_name = non_empty.pop()

    return result
