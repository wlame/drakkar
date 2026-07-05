# Local Databases & Worker Autodiscovery

This page is the **specification** of every SQLite file a Drakkar worker
creates, how it is written, and how workers find each other through a
shared directory. Both backends — Python (`py-drakkar`) and Go — implement
this spec **identically, byte-for-byte where it matters**: a file written
by one backend is read correctly by the other, and a mixed fleet sharing
one `db_dir` is a **supported deployment mode** (see
[Mixed fleets](#mixed-python-go-fleets-are-supported)).

Any change to schemas, encodings, pragmas, naming, or discovery rules is a
cross-backend contract change: land it on **both** backends, update this
page in **both** repos, and regenerate the cross-backend fixtures
(`just gen-db-fixtures` in each repo).

## The files

Every worker owns (writes) at most two databases, both living in the same
shared directory unless configured apart:

| File | Purpose | Directory config |
|---|---|---|
| `<worker>-<YYYY-MM-DD__HH_MM_SS>.db` | Flight recorder: event log + config snapshot + periodic state. Rotated. | `ui.recorder.db_dir` |
| `<worker>-live.db` → *(symlink)* | Stable pointer to the current recorder DB; peers read through it so rotation never breaks them. | same |
| `<worker>-cache.db.actual` | LWW key/value cache. Never rotated. | `cache.db_dir`, falling back to `ui.recorder.db_dir` |
| `<worker>-cache.db` → `.actual` *(symlink)* | Discovery pointer for the cache, deliberately shaped like the recorder's so one symlink-scan routine serves both. | same |
| `<worker>.watchdog` | Not SQLite: OOM/SIGKILL detection marker (`CLEAN_EXIT` written on graceful stop). | `ui.recorder.db_dir` |

Rules:

- **A worker only ever writes its own files.** Peers open other workers'
  files strictly read-only (`file:...?mode=ro`).
- Timestamp format in recorder filenames is exactly
  `%Y-%m-%d__%H_%M_%S` (UTC).
- DB files are created with `0600` permissions; both backends log a
  warning when `db_dir` is world-writable.
- `db_dir` is **disposable**: deleting it loses history/cache contents but
  never breaks a worker — fresh files are created on next start. This is
  also the documented upgrade path for schema changes (below).
- The merge tool output (`drakkar-merge` / the merge library) is a derived
  offline artifact with its own tables (`workers`, `events`,
  `worker_states`) and is not part of this runtime spec.

## Schemas

The DDL below is normative and **byte-identical in both backends**
(Python `drakkar/recorder/schema.py` + `drakkar/cache/sql.py`; Go
`internal/recorder/schema.go` + `internal/cache/sql.go`). The Go repo pins
its constants against these with unit tests.

### `events` (recorder)

```sql
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
```

plus eight indexes: `idx_events_partition_offset(partition, offset)`,
`idx_events_ts(ts)`, `idx_events_dt(dt)`, `idx_events_task_id(task_id)`,
`idx_events_type(event)`, partial `idx_events_labels(labels) WHERE labels
IS NOT NULL`, `idx_events_origin(origin)`, partial
`idx_events_request_id(request_id) WHERE request_id IS NOT NULL`.

This 20-column list is the **pinned event-row shape** — the same list the
`/api/v1` contract exposes through `/ws`, `/events`, `/trace`, and
`/trace-by-label` (`SELECT *` pass-through). It is pinned in code on both
backends (`EVENT_COLUMNS` / `EventColumns`) with tests asserting the live
table matches, and normatively in
`drakkar-ui/docs/api-contract-v1.md`. Column *presence* is contractual;
values stay event-type-dependent.

### `worker_config` (recorder, single row)

```sql
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
```

The `debug_port` / `debug_url` column names predate the `ui.*` config
rename and are **kept deliberately** for on-disk compatibility. Secrets
are redacted before insertion (broker credentials stripped, secret-named
env values replaced) — the file is downloadable via the operator UI.

### `worker_state` (recorder, periodic snapshots)

```sql
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
```

### `cache_entries` (cache)

```sql
CREATE TABLE IF NOT EXISTS cache_entries (
    key               TEXT    NOT NULL PRIMARY KEY,
    scope             TEXT    NOT NULL CHECK(scope IN ('local','cluster','global')),
    value             TEXT    NOT NULL,
    size_bytes        INTEGER NOT NULL,
    created_at_ms     INTEGER NOT NULL,
    updated_at_ms     INTEGER NOT NULL,
    expires_at_ms     INTEGER,
    origin_worker_id  TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_cache_expires
    ON cache_entries(expires_at_ms) WHERE expires_at_ms IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_cache_scope_updated
    ON cache_entries(scope, updated_at_ms);
```

## Connections & pragmas

Both backends follow the same connection discipline:

- **One writer connection per DB** (SQLite allows a single writer anyway;
  the Go side caps its `database/sql` pool at one connection to match the
  Python single-connection model).
- **A dedicated read-only reader connection** per DB for UI queries and
  cache `get()` fallback, so reads never queue behind flush commits.
- **`journal_mode=WAL`**, set on the writer at open (persisted in the DB
  header; readers inherit it). WAL is what lets peers read consistent
  snapshots while the owner commits, across processes and across
  backends — the WAL/SHM on-disk format is identical (the Go backend's
  pure-Go driver is transpiled from the same SQLite sources).
- **Explicit `busy_timeout` on every connection**, identical values in
  both backends:

| Connection | busy_timeout |
|---|---|
| Recorder writer, own reader, peer reads | 5000 ms |
| Cache writer, own reader | 10000 ms |
| Cache peer/cluster ephemeral reads | 5000 ms |

  The cache's own connections wait longer so a checkpoint can't fail a
  local flush; **peer** reads time out fast so a lock-wedged peer stays
  inside the per-peer failure isolation.
- No explicit WAL checkpoints, no `VACUUM`, driver-default `synchronous`
  and `foreign_keys` — on both backends.

## Encodings

- **JSON columns** (`args`, `metadata`, `labels`, `sinks_json`,
  `env_vars_json`, `assigned_partitions`): compact separators, **sorted
  keys**, raw UTF-8 (no `\uXXXX` escapes). Byte-identical across backends
  and across Python's orjson/stdlib encoder paths.
- **Datetimes embedded in JSON** use the canonical cross-backend format —
  UTC, RFC 3339, **fixed six-digit microseconds**, `Z` suffix:
  `2026-07-05T12:34:56.123456Z` (Python
  `drakkar.timefmt.format_rfc3339_micro`, Go
  `drakkar.FormatRFC3339Micro`). Fixed width is deliberate: no
  trailing-zero trimming, no `+00:00`/`Z` ambiguity, identical bytes from
  either backend.
- **`dt` / `created_at_dt` / `updated_at_dt` columns** keep their own
  display format `YYYY-MM-DD HH:MM:SS.mmm` (space separator,
  milliseconds) — also identical across backends.
- **`ts` columns** are `REAL` Unix seconds.
- **Cache `value` column**: JSON, but the exact bytes are
  **backend-specific and deliberately unspecified** — Python writes
  `json.dumps` defaults (spaces, insertion-order keys), Go writes
  `json.Marshal` (compact, sorted keys). Values are opaque: LWW compares
  `updated_at_ms` + `origin_worker_id`, never bytes, and both backends
  parse either form. `size_bytes` reflects the writer's own encoding.

## Write patterns & retention

**Recorder**: events buffer in memory (capped at
`ui.recorder.max_buffer`), flushed every
`ui.recorder.flush_interval_seconds` as one multi-row transaction. On
flush failure the batch is re-queued at the front (tail evictions are
counted) and dropped after `ui.recorder.max_flush_retries` consecutive
failures. Recording-side filters (`event_min_duration_ms`,
`output_min_duration_ms`, `store_output`) gate what enters the buffer —
they affect row presence, never layout.

**Rotation** (every `ui.recorder.rotation_interval_minutes`): flush →
create + initialize the new timestamped DB (WAL, busy_timeout, schema,
reader) → atomic swap → rewrite `worker_config` → repoint the live
symlink → close the old DB → delete files older than
`ui.recorder.retention_hours` (by mtime) → enforce a max file count of
`retention_max_events // 10000` (minimum 1), oldest first.

**Cache**: dirty entries flush every `cache.flush_interval_seconds`
(atomic dirty-map swap, single transaction, restore-on-failure); expired
rows are deleted every `cache.cleanup_interval_seconds`. The cache file
never rotates.

## Schema evolution

There are **no migrations**. Both backends create tables with
`CREATE TABLE IF NOT EXISTS` only, and `db_dir` is documented as
disposable. One forward-compatibility guard exists: at open, the recorder
verifies a pre-existing `events` table carries the webapp-release columns
(`origin`, `client_name`, `request_id`) and aborts startup with an
identical actionable message on both backends when they are missing
(delete the DB, restart). The cache has no version check; its schema is
additive-only by contract.

## Worker autodiscovery

A worker **advertises** itself by writing its single `worker_config` row
(requires `ui.recorder.store_config: true`, the default) and maintaining
the `<worker>-live.db` symlink. There is no separate registry.

The UI's workers list **discovers** siblings with this exact algorithm on
both backends:

1. Scan `db_dir/*-live.db`, **sorted** by path.
2. Keep only true symlinks (plain files are ignored).
3. Skip our own entry (basename minus suffix == own worker name).
4. Resolve the symlink; skip broken links silently.
5. Open the target read-only, confirm `worker_config` exists, read the
   `id = 1` row **mapped by column name** (column order never matters).

**Liveness semantics**: a peer is listed iff its symlink exists and
resolves. The symlink is removed on graceful shutdown; after a crash the
worker stays visible until retention deletes its file. There is no
timestamp-based staleness rule — by design, so a crashed worker's history
remains reachable.

**Cross-worker tracing** (`trace`, `trace-by-label`) searches: own live
DB → other workers' live DBs (sorted, first match wins) → rotated files
newest-first, filtered by matching `cluster_name`, annotating every event
with `worker_name`.

## Cache peer sync

Peers are discovered by scanning `db_dir/*-cache.db` symlinks (same
routine as recorder discovery — that is why the `.actual` indirection
exists). For each peer, every `cache.peer_sync.interval_seconds`:

1. **Cluster resolution**: read `cluster_name` from the peer's
   `<peer>-live.db` `worker_config` row; cache the answer in-process for
   300 s. Same cluster (including both-empty) → pull scopes
   `('cluster','global')`; different or unresolvable → `('global')` only.
   `local` never leaves its worker.
2. **Cursor pull**: `WHERE <scope> AND (expires_at_ms IS NULL OR
   expires_at_ms > now) AND updated_at_ms > cursor ORDER BY
   updated_at_ms ASC, key ASC LIMIT batch_size`, then drain same-
   millisecond ties keyed on `key >` continuation. Cursors advance
   identically on both backends.
3. **Apply through the LWW UPSERT** (below) and invalidate touched
   in-memory keys. The commit + metrics + invalidation trio is
   cancellation-atomic on both backends.
4. Per-peer failures are isolated; each cycle is bounded by
   `cycle_deadline_seconds` (default `interval × 0.9`).

The **LWW conflict rule** is one shared SQL statement used by both the
local flush path and the peer-apply path, byte-identical across backends:
incoming `updated_at_ms` strictly greater wins; on a tie the
lexicographically **smaller** `origin_worker_id` wins; otherwise the
stored row is kept. Two workers observing the same conflict — regardless
of backend — converge on the same survivor.

## Mixed Python + Go fleets are supported

Running Python and Go workers against one shared `db_dir` — recorder
discovery, cross-tracing, and cache peer sync included — is a
**supported deployment mode**. This is guaranteed by:

- byte-identical DDL, filenames, symlink protocols, JSON encodings, and
  the canonical datetime format (this page);
- identical busy-timeout behavior under cross-process WAL contention;
- **cross-backend round-trip tests in both repos**: each repo commits
  fixture DBs generated by the *other* backend's real engines and
  verifies discovery, tracing, the pinned event-row shape, and cache
  peer sync against them (Python `tests/test_cross_backend_db.py` +
  `tests/fixtures/go-db/`; Go `internal/crossbackend/` +
  `internal/cache/crossbackend_roundtrip_test.go`). Regenerate with
  `just gen-db-fixtures` in either repo after any spec change.

Requirements: mount the shared directory at the **same path** in every
worker, keep `ui.recorder.store_config: true` (discovery and
cluster-scoped cache sync depend on the `worker_config` row), and keep
both backends on the same contract version during the overlap.

## Watchdog file

`{db_dir}/{worker_id}.watchdog` is written at startup and marked
`CLEAN_EXIT` on graceful shutdown; a worker that finds its own stale
watchdog without the marker reports the previous run as killed
(OOM/SIGKILL detection). Same path and marker semantics on both backends;
disabled (with a log line) when `db_dir` is empty.
