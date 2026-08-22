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

Webapp-release schema extension:
The ``events`` table grew three columns — ``origin``, ``client_name``,
``request_id`` — to track HTTP-origin tasks alongside Kafka-origin tasks
without a separate code path. These columns are added directly to the
``CREATE TABLE IF NOT EXISTS`` statement; there is intentionally NO
migration framework. Rationale:

* recorder DBs are observability-only, already rotated and disposable;
* ``debug.db_dir`` is documented as operator-disposable;
* a forward-only migration runner just to add three optional columns
  would be over-engineering for a feature that affects only debug data.

Pre-existing recorder DBs from older worker versions do NOT have these
columns. Operators delete those DBs on upgrade (recorder rotation
already produces fresh DBs per worker run). The startup-time
``PRAGMA table_info(events)`` check in ``EventRecorder.start()``
converts the otherwise-confusing ``OperationalError: no such column``
mid-request error into a clear startup failure with an actionable
upgrade path — see :class:`RecorderSchemaError` and ``docs/observability.md``
for the full upgrade story.
"""

from __future__ import annotations

# Column names that the webapp-release recorder schema requires on the
# ``events`` table. Used by ``EventRecorder.start()`` /
# ``EventRecorder._rotate()`` to validate at-open that the DB is
# compatible with the current code. Missing any of these raises
# :class:`RecorderSchemaError` so worker startup aborts with a clear
# upgrade-path message rather than failing at first webapp request.
WEBAPP_REQUIRED_EVENT_COLUMNS: tuple[str, ...] = (
    'origin',
    'client_name',
    'request_id',
)

# The PINNED recorder event-row shape, in DDL order. This is the contract
# for every surface that passes event rows through as JSON objects —
# ``/ws``, ``/api/v1/events``, ``/api/v1/trace``, ``/api/v1/trace-by-label``
# — and for cross-backend DB interop (a Go worker reads these columns from
# a Python-written file and vice versa). The list is mirrored in
# ``drakkar-ui/docs/api-contract-v1.md`` ("Recorder event row shape") and
# in the Go backend's ``internal/recorder/schema.go``; a parity test in
# each backend asserts the live table matches. Column PRESENCE is the
# contract — values stay event-type-dependent (nullable).
EVENT_COLUMNS: tuple[str, ...] = (
    'id',
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
    'labels',
    'origin',
    'client_name',
    'request_id',
)


class RecorderSchemaError(RuntimeError):
    """Raised at recorder open when the existing DB predates required columns.

    The webapp release added ``origin`` / ``client_name`` / ``request_id``
    columns to the ``events`` table (see module docstring). Pre-existing
    DBs from older worker versions lack those columns. The recorder
    runs ``PRAGMA table_info(events)`` immediately after opening the DB
    and raises this exception when the new columns are missing.

    The exception is intentionally left **uncaught** in the recorder
    layer so it propagates through ``AppLifecycle._async_run`` and
    aborts worker startup. Operators see the message in stderr/logs
    with the actionable next step: delete the per-worker DB(s) under
    ``db_dir`` and restart — fresh rotation-cycle DBs include the
    required columns automatically.
    """


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
    labels      TEXT,
    origin      TEXT    NOT NULL DEFAULT 'kafka',
    client_name TEXT,
    request_id  TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_partition_offset ON events(partition, offset);
CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts);
CREATE INDEX IF NOT EXISTS idx_events_dt ON events(dt);
CREATE INDEX IF NOT EXISTS idx_events_task_id ON events(task_id);
CREATE INDEX IF NOT EXISTS idx_events_type ON events(event);
CREATE INDEX IF NOT EXISTS idx_events_labels ON events(labels) WHERE labels IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_events_origin ON events(origin);
CREATE INDEX IF NOT EXISTS idx_events_request_id ON events(request_id) WHERE request_id IS NOT NULL;
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
    health_state        TEXT,
    loop_lag_ms         REAL,
    throughput          TEXT,
    updated_at          REAL NOT NULL,
    updated_at_dt       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_worker_state_updated ON worker_state(updated_at);
"""

# ``get_trace`` query: pulls every event for the given (partition, offset)
# plus all events for every task spawned from that source message. The
# json_each() join walks the ``source_offsets`` array stored on each
# task_started row; tasks are anchored by task_id thereafter.
#
# The third branch picks up WINDOW-scoped rows: handler annotations and
# ``offload`` events emitted from window-wide hooks (``arrange`` /
# ``on_window_complete``). Those rows describe a whole ``arrange()`` window
# rather than one message or task, so they carry neither an ``offset`` nor a
# ``task_id`` and the first two branches cannot see them. They instead record
# the window's message offsets in ``metadata.offsets``, which this branch
# walks. The ``event IN (...)`` filter matters: ``arranged`` rows carry an
# ``offsets`` array of the same shape and would otherwise be pulled into
# every trace.
#
# Parameter order: partition, offset, partition, offset, partition, offset.
_TRACE_QUERY = """
    SELECT * FROM events
    WHERE partition = ? AND (
        offset = ?
        OR task_id IN (
            SELECT e.task_id FROM events e, json_each(json_extract(e.metadata, '$.source_offsets')) j
            WHERE e.partition = ? AND e.event = 'task_started'
            AND j.value = ?
        )
        OR id IN (
            SELECT a.id FROM events a, json_each(json_extract(a.metadata, '$.offsets')) k
            WHERE a.partition = ? AND a.event IN ('annotation', 'offload')
            AND k.value = ?
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
