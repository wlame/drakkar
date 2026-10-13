"""Merge multiple Drakkar debug SQLite databases into one.

The merged database contains:
- ``workers`` table: one row per source DB's worker_config. A source that is
  itself a merge output (an archive folded into its successor, or a merged
  file picked again) contributes its own ``workers`` rows unchanged.
- ``events`` table: all events from all DBs, with a ``worker_id``
  foreign key linking each event to its worker. Rows are copied
  source-by-source; readers order via the ``ts`` index, so insertion
  order is not part of the contract.
- ``worker_states`` table: all state snapshots with ``worker_id`` FK

This module is used by the debug UI merge feature and can also be
called programmatically.
"""

from __future__ import annotations

import contextlib
import os
import secrets
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path

import structlog

from drakkar.dbfiles import secure_db_file

logger = structlog.get_logger()

MERGED_SCHEMA = """
CREATE TABLE workers (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    worker_name          TEXT NOT NULL,
    cluster_name         TEXT,
    ip_address           TEXT,
    debug_port           INTEGER,
    debug_url            TEXT,
    kafka_brokers        TEXT,
    -- source_topic/consumer_group mirror recorder/schema.py's worker_config
    -- columns of the same name, written from sources.kafka.topic /
    -- resolved_consumer_group. The column names predate that config path
    -- and stay as-is: this SQLite schema is a stable contract, not a
    -- mirror of the current config field names.
    source_topic         TEXT,
    consumer_group       TEXT,
    binary_path          TEXT,
    max_executors          INTEGER,
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
    pid          INTEGER,
    labels       TEXT,
    origin       TEXT,
    client_name  TEXT,
    request_id   TEXT
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
    health_state        TEXT,
    loop_lag_ms         REAL,
    throughput          TEXT,
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
    'max_executors',
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
    # Present in the live recorder schema (drakkar/recorder/schema.py).
    # ``row.get()`` yields None for source files written by older versions,
    # which INSERTs as NULL — hence nullable here even though the live
    # ``origin`` column is NOT NULL.
    'labels',
    'origin',
    'client_name',
    'request_id',
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
    # v1.15/v1.16 columns; row.get() yields None for databases written by
    # older versions, which INSERTs as NULL.
    'health_state',
    'loop_lag_ms',
    'throughput',
    'updated_at',
    'updated_at_dt',
]

# A merge output used as a merge source (an archive folded into its
# successor, or a merged file picked again in the UI) carries ``workers``
# rows instead of one ``worker_config`` row. Its rows are copied verbatim,
# source_file included, so every original worker keeps its events and
# states, and a folded archive still names the raw files it covers.
_MERGED_WORKER_COLUMNS = [*_WORKER_CONFIG_COLUMNS, 'source_file']


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
    # What the file IS: 'recorder' (per-worker event log), 'merged'
    # (drakkar-merge output — carries a ``workers`` table), 'cache'
    # (handler cache — carries ``cache_entries``), or 'unknown'
    # (unreadable / foreign schema). Lets the databases page group and
    # badge files instead of showing every .db as an events log.
    kind: str = 'unknown'
    # Highest events.id at scan time — the delta-scan cursor for files
    # that are still being written (see drakkar.dbstats). None when the
    # events table is absent or empty.
    max_event_id: int | None = None
    # Row count of ``cache_entries`` for kind='cache' files, else None.
    cache_entry_count: int | None = None


@dataclass
class MergeResult:
    """Result of a merge operation.

    ``source_files`` lists every basename the caller asked for;
    ``merged_files`` lists the full paths whose contents actually reached
    the output. The two differ when a source could not be read — the merge
    tolerates that and continues — so a caller that deletes what it merged
    (the recorder's archive pass) must consult ``merged_files``, never the
    input list.
    """

    output_path: str
    worker_count: int = 0
    event_count: int = 0
    state_count: int = 0
    cluster_name: str = ''
    source_files: list[str] = field(default_factory=list)
    merged_files: list[str] = field(default_factory=list)


def _dict_factory(cursor: sqlite3.Cursor, row: tuple) -> dict:
    """Row factory that returns dicts instead of tuples."""
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}


def _table_exists(db: sqlite3.Connection, table: str) -> bool:
    row = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        [table],
    ).fetchone()
    return row is not None


def _is_merged_db(db: sqlite3.Connection) -> bool:
    """True for a merge output: it carries a ``workers`` table (the rule ``scan_db`` uses)."""
    return _table_exists(db, 'workers')


def _copy_merged_workers(
    src: sqlite3.Connection,
    out: sqlite3.Connection,
    cluster_names: set[str],
) -> dict[int, int]:
    """Copy every ``workers`` row of a merged source; return its old id -> new id."""
    rows = src.execute('SELECT * FROM workers ORDER BY id').fetchall()
    cols = ', '.join(_MERGED_WORKER_COLUMNS)
    placeholders = ', '.join(['?'] * len(_MERGED_WORKER_COLUMNS))
    ids: dict[int, int] = {}
    for row in rows:
        cursor = out.execute(
            f'INSERT INTO workers ({cols}) VALUES ({placeholders})',
            [row.get(col) for col in _MERGED_WORKER_COLUMNS],
        )
        new_id = cursor.lastrowid
        assert new_id is not None
        ids[row['id']] = new_id
        cluster_names.add(row.get('cluster_name') or '')
    return ids


def _owner_ids(
    rows: list[dict],
    db_path: str,
    worker_map: dict[str, int],
    merged_ids: dict[str, dict[int, int]],
) -> list[int]:
    """The output ``worker_id`` of each source row, in row order.

    A recorder source has one worker, so every row maps to it. A merged
    source maps each row's own ``worker_id`` through the ids its workers got
    in the output. A row naming a worker the source does not list raises
    ``KeyError``; the caller then treats the whole source as incomplete.
    """
    ids = merged_ids.get(db_path)
    if ids is None:
        return [worker_map[db_path]] * len(rows)
    return [ids[row['worker_id']] for row in rows]


def scan_db(path: str) -> DbStats:
    """Read statistics from a single debug database file."""
    stats = DbStats(
        path=path,
        filename=os.path.basename(path),
        size_bytes=os.path.getsize(path),
    )
    # ``db`` is bound before the try so ``finally`` can close it on every
    # path. Closing at the end of the try body instead leaked the handle
    # whenever a file raised — and this runs per file on every poll of the
    # databases endpoint, so an unreadable file leaked a descriptor a second.
    db: sqlite3.Connection | None = None
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
            # Delta-scan cursor (see drakkar.dbstats): MAX on the integer
            # primary key is an O(1) b-tree descent, not a table scan.
            row = db.execute('SELECT MAX(id) as max_id FROM events').fetchone()
            if row:
                stats.max_event_id = row['max_id']

        if _table_exists(db, 'worker_state'):
            stats.has_state = True

        # Classify. Order matters only in that 'workers' (the merged-output
        # schema) also carries an events table — check it before defaulting
        # to 'recorder'.
        if _table_exists(db, 'workers'):
            stats.kind = 'merged'
        elif stats.has_events or stats.has_config:
            stats.kind = 'recorder'
        elif _table_exists(db, 'cache_entries'):
            stats.kind = 'cache'
            row = db.execute('SELECT COUNT(*) as cnt FROM cache_entries').fetchone()
            if row:
                stats.cache_entry_count = row['cnt'] or 0
    except Exception as e:
        # Partial stats are returned as-is (kind stays 'unknown') — but an
        # unreadable file is worth a line, not silence.
        logger.warning('db_scan_failed', path=path, error=str(e))
    finally:
        if db is not None:
            db.close()
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
        # Dot-prefixed names are pass-internal state, never a finished
        # database — archiving's in-flight merge temporaries
        # (``.<name>.<pid>.merge.db``) end in ``.db`` and would otherwise
        # show up here mid-merge.
        if entry.startswith('.'):
            continue
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
    A source that is itself a merge output keeps its own workers, events
    and states.

    If all source DBs share the same ``cluster_name``, the result
    inherits that cluster.

    A source that cannot be opened or read is skipped rather than failing
    the whole merge; ``MergeResult.merged_files`` reports which paths
    actually contributed.

    Args:
        db_paths: Paths to source .db files.
        output_path: Where to write the merged database.

    Returns:
        MergeResult with counts, metadata and the merged source paths.
    """
    if os.path.exists(output_path):
        os.remove(output_path)

    # Owner-only BEFORE connect, not after: the WAL pragma below makes SQLite
    # create -wal/-shm copying the main file's mode, so tightening afterwards
    # would leave both sidecars world-readable. Same ordering the recorder's
    # archive merge uses.
    secure_db_file(output_path)
    out = sqlite3.connect(output_path)
    out.execute('PRAGMA journal_mode=WAL')
    out.execute('PRAGMA foreign_keys=ON')
    out.executescript(MERGED_SCHEMA)

    result = MergeResult(output_path=output_path)
    cluster_names: set[str] = set()

    # phase 1: insert all workers, collect worker_id mappings
    worker_map: dict[str, int] = {}  # db_path → worker_id
    # Merged sources: db_path → (source worker id → output worker id).
    merged_ids: dict[str, dict[int, int]] = {}

    for db_path in db_paths:
        basename = os.path.basename(db_path)
        result.source_files.append(basename)
        src: sqlite3.Connection | None = None
        try:
            src = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True)
            src.row_factory = _dict_factory
            if _is_merged_db(src):
                merged_ids[db_path] = _copy_merged_workers(src, out, cluster_names)
                result.worker_count += len(merged_ids[db_path])
                continue

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
        except Exception as e:
            logger.warning('merge_source_skipped', path=db_path, phase='workers', error=str(e))
            continue
        finally:
            if src is not None:
                src.close()

    # Sources that opened in phase 1 but failed to hand over their rows
    # later. Tracked so ``merged_files`` never claims a partially-read
    # file: the archive pass deletes exactly what that list names.
    incomplete: set[str] = set()

    # phase 2: copy events per source. No cross-source buffering or global
    # sort — readers order via the ``ts`` index, so a globally sorted
    # insertion order buys nothing and buffering every source's rows at
    # once spiked memory on large merges. Peak memory is now one source.
    event_cols = ', '.join(['worker_id', *_EVENT_COLUMNS])
    event_placeholders = ', '.join(['?'] * (1 + len(_EVENT_COLUMNS)))
    insert_event_sql = f'INSERT INTO events ({event_cols}) VALUES ({event_placeholders})'

    for db_path in db_paths:
        if db_path not in worker_map and db_path not in merged_ids:
            continue
        src: sqlite3.Connection | None = None
        try:
            src = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True)
            src.row_factory = _dict_factory
            if _table_exists(src, 'events'):
                rows = src.execute('SELECT * FROM events ORDER BY ts').fetchall()
                owners = _owner_ids(rows, db_path, worker_map, merged_ids)
                batch = [
                    (owner, *(row.get(col) for col in _EVENT_COLUMNS)) for owner, row in zip(owners, rows, strict=True)
                ]
                out.executemany(insert_event_sql, batch)
                result.event_count += len(batch)
        except Exception as e:
            logger.warning('merge_source_skipped', path=db_path, phase='events', error=str(e))
            incomplete.add(db_path)
            continue
        finally:
            if src is not None:
                src.close()

    # phase 3: merge worker_state rows
    for db_path in db_paths:
        if db_path not in worker_map and db_path not in merged_ids:
            continue
        src: sqlite3.Connection | None = None
        try:
            src = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True)
            src.row_factory = _dict_factory
            # A recorder file keeps snapshots in ``worker_state``; a merge
            # output keeps them, already worker-tagged, in ``worker_states``.
            state_table = 'worker_states' if db_path in merged_ids else 'worker_state'
            if _table_exists(src, state_table):
                rows = src.execute(f'SELECT * FROM {state_table} ORDER BY updated_at').fetchall()
                owners = _owner_ids(rows, db_path, worker_map, merged_ids)
                cols = ', '.join(['worker_id', *_STATE_COLUMNS])
                placeholders = ', '.join(['?'] * (1 + len(_STATE_COLUMNS)))
                for owner, row in zip(owners, rows, strict=True):
                    values = [row.get(col) for col in _STATE_COLUMNS]
                    out.execute(f'INSERT INTO worker_states ({cols}) VALUES ({placeholders})', [owner, *values])
                    result.state_count += 1
        except Exception as e:
            logger.warning('merge_source_skipped', path=db_path, phase='states', error=str(e))
            incomplete.add(db_path)
            continue
        finally:
            if src is not None:
                src.close()

    result.merged_files = [
        path for path in db_paths if (path in worker_map or path in merged_ids) and path not in incomplete
    ]

    out.commit()
    out.close()

    # determine cluster: if all sources share the same non-empty cluster
    non_empty = cluster_names - {''}
    if len(non_empty) == 1 and '' not in cluster_names:
        result.cluster_name = non_empty.pop()

    return result


def merge_databases_atomic(db_paths: list[str], output_path: str) -> MergeResult:
    """Merge into a hidden temp file next to ``output_path``, then rename it into place.

    A reader of the folder never sees a half-written result: the temp name is
    dot-prefixed (every listing skips it) and the final name appears through
    one ``os.replace``. On any failure the temp file and its SQLite sidecars
    are removed and the error is re-raised.
    """
    folder, name = os.path.split(output_path)
    temp_path = os.path.join(folder, f'.{name}.{secrets.token_hex(8)}.tmp')
    try:
        result = merge_databases(db_paths, temp_path)
        os.replace(temp_path, output_path)
    except BaseException:
        for suffix in ('', '-wal', '-shm', '-journal'):
            with contextlib.suppress(FileNotFoundError):
                os.remove(temp_path + suffix)
        raise
    result.output_path = output_path
    return result
