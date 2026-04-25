"""SQLite schema + canned queries for the flight recorder.

Kept separate from the recorder runtime so the DDL can be inspected (or
``executescript``'d in test fixtures) without pulling in aiosqlite, the
flush loop, or the rotation logic.

Three tables back the recorder DB:

- ``events``        — append-only event log; the bulk of the recorder's writes.
- ``worker_config`` — single-row table holding the worker's startup config
  snapshot (used by peer discovery to learn cluster_name, debug URL, etc.).
- ``worker_state``  — periodic snapshot of counters / pool state for the
  debug UI's "what is this worker doing right now?" panels.
"""

from __future__ import annotations

SCHEMA_EVENTS = """
CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          REAL    NOT NULL,
    dt          TEXT    NOT NULL,
    event       TEXT    NOT NULL,
    partition   INTEGER,
    offset      INTEGER,
    task_id     TEXT,
    args        TEXT,
    stdout_size INTEGER DEFAULT 0,
    stdout      TEXT,
    stderr      TEXT,
    exit_code   INTEGER,
    duration    REAL,
    output_topic TEXT,
    metadata    TEXT,
    pid         INTEGER,
    labels      TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_partition_offset ON events(partition, offset);
CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts);
CREATE INDEX IF NOT EXISTS idx_events_dt ON events(dt);
CREATE INDEX IF NOT EXISTS idx_events_task_id ON events(task_id);
CREATE INDEX IF NOT EXISTS idx_events_type ON events(event);
CREATE INDEX IF NOT EXISTS idx_events_labels ON events(labels) WHERE labels IS NOT NULL;
"""

SCHEMA_WORKER_CONFIG = """
CREATE TABLE IF NOT EXISTS worker_config (
    id              INTEGER PRIMARY KEY CHECK (id = 1),
    worker_name     TEXT NOT NULL,
    cluster_name    TEXT,
    ip_address      TEXT,
    debug_port      INTEGER,
    debug_url       TEXT,
    kafka_brokers   TEXT,
    source_topic    TEXT,
    consumer_group  TEXT,
    binary_path     TEXT,
    max_executors     INTEGER,
    task_timeout_seconds INTEGER,
    max_retries     INTEGER,
    window_size     INTEGER,
    sinks_json      TEXT,
    env_vars_json   TEXT,
    created_at      REAL NOT NULL,
    created_at_dt   TEXT NOT NULL
);
"""

SCHEMA_WORKER_STATE = """
CREATE TABLE IF NOT EXISTS worker_state (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
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
CREATE INDEX IF NOT EXISTS idx_worker_state_updated ON worker_state(updated_at);
"""

# ``get_trace`` query: pulls every event for the given (partition, offset)
# plus all events for every task spawned from that source message. The
# json_each() join walks the ``source_offsets`` array stored on each
# task_started row; tasks are anchored by task_id thereafter.
_TRACE_QUERY = """
    SELECT * FROM events
    WHERE partition = ? AND (
        offset = ?
        OR task_id IN (
            SELECT e.task_id FROM events e, json_each(json_extract(e.metadata, '$.source_offsets')) j
            WHERE e.partition = ? AND e.event = 'task_started'
            AND j.value = ?
        )
    )
    ORDER BY id ASC
"""


# ``trace_by_label`` query: pulls every event for any task whose ``labels``
# JSON contains the requested key/value pair. Supports the debug UI's
# "trace by label" affordance — operators paste a request_id (or any
# user-defined label) and see every event tied to that ID across the
# replacement chain.
_LABEL_TRACE_QUERY = """
    SELECT * FROM events
    WHERE task_id IN (
        SELECT DISTINCT task_id FROM events
        WHERE labels IS NOT NULL
        AND json_extract(labels, ?) = ?
        AND task_id IS NOT NULL
    )
    ORDER BY id ASC
"""
