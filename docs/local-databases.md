# Local Databases & Worker Autodiscovery

This page is the **specification** of every SQLite file a Drakkar worker
creates, how it is written, how it moves into the shared history, and how
workers find each other. It is written as a specification, byte-for-byte
where it matters, because these files outlive the process that wrote them:
the workers of a fleet read each other's sealed files in one shared folder,
and an archive is opened months later by tooling that was not running when
it was created.

Any change to schemas, encodings, pragmas, naming, or discovery rules is a
breaking change to that spec, not a refactor: existing files on a shared
volume were written by the old rules, and this page has to move with the
code.

## The files

A worker uses two folders:

- **`storage.dir`** holds the worker's own files. The live recorder file
  and the cache file are opened only by their owner, so this folder does
  not need to be shared. The recorder file exists only with
  `ui.recorder.persistence: sqlite`. Every worker with a recorder also keeps
  its recent events in an in-memory database, the memory store, which is no
  file. See [Recent Events in Memory](recent-events.md).
- **`shared.dir`** is the folder every worker of a fleet sees. It holds the
  recorder history of each cluster, the archives, the merge results and the
  [registry cards](fleet.md). When `shared.dir` is empty, the worker uses
  `storage.dir` for this too.

### Own files (`storage.dir`)

| File | Purpose |
|---|---|
| `<worker>-<YYYY-MM-DD__HH_MM_SS>.db` | Flight recorder: event log + config snapshot + periodic state. The **live file**: WAL mode, opened only by its owner. Rotated. Only with `persistence: sqlite`. |
| `<worker>-<YYYY-MM-DD__HH_MM_SS>.db` *(older ones)* | The **backlog**: own files that are closed but not yet moved into the history folder, because a seal or a move failed, or a clean stop had no time left. See [Sealing](#sealing-and-the-history-folder). |
| `<worker>-cache.db` | LWW key/value cache. A plain file, never rotated, opened only by its owner. Peers get its `cluster` and `global` rows over HTTP; see [Cache exchange](#cache-exchange). |
| `<worker>.watchdog` | Not SQLite: OOM/SIGKILL detection marker (`CLEAN_EXIT` written on graceful stop). |

### Shared files (`shared.dir`)

| Path | Purpose |
|---|---|
| `history/<cluster>/<worker>-<YYYY-MM-DD__HH_MM_SS>.db` | A **sealed** recorder file: rollback journal, no sidecars, never changes again. |
| `history/<cluster>/<worker>-<YYYY-MM-DD__HH_MM_SS>.db.meta.json` | The [meta file](#the-meta-file) of that sealed file: its statistics, written once by the owner. |
| `history/<cluster>/<cluster>-<from>__<to>.db.gz` | Archive: gzip-compressed merge of one UTC window's sealed files. See [Archiving](#archiving). |
| `tasks/<cluster>/<task>.lock` | The [task lock](#the-task-lock) of a cluster periodic task (the archive's is `history.archive.lock`), made by the default `filesystem` [election provider](#the-election-provider). It exists while one worker of the cluster holds the slot. The same folder holds the clock markers `.clock.<instance>`. |
| `merged/merged-<YYYY-MM-DD__HH_MM_SS>-<16 hex>.db` | The result of a merge started from the UI. Deleted after `history.merged_retention_days`; see [Merge results expire](#merge-results-expire). |
| `fleet/workers/<worker>.json` | Registry card of the default `filesystem` provider. See [Fleet registry](fleet.md). |

`<cluster>` is the sanitized cluster name: every character outside
`[a-zA-Z0-9_-]` becomes `_`, and an empty name becomes `default`. A worker
in cluster `search hot` writes into `history/search_hot/`. The meta file
keeps the cluster name as configured.

Rules:

- **The live file has one owner.** Only the worker that writes it opens
  it: one writer connection and one reader connection, both in that
  worker. Other workers read only sealed files, and they open them with
  `immutable=1`, which takes no locks.
- **Of the history folders, a worker writes only into the one of its own
  cluster.** It publishes its sealed files there. The worker that holds the
  lease of an archive slot (with the default provider: the task lock) also
  writes the archives there and deletes the sealed files it archived.
- Timestamp format in recorder filenames is exactly
  `%Y-%m-%d__%H_%M_%S` (UTC).
- DB files and meta files are created with `0600` permissions. The worker
  creates `storage.dir` with mode `0700` when it is missing, and logs a
  warning when `storage.dir` is world-writable. A cluster history folder,
  a cluster task lock folder `tasks/<cluster>/` and `merged/` are created
  with mode `0700`.
- `storage.dir` is **disposable**: deleting it loses own history/cache
  contents but never breaks a worker — fresh files are created on next
  start. This is also the documented upgrade path for schema changes
  (below).
- The merge tool output (`drakkar-merge` / the merge library) is a derived
  offline artifact with its own tables (`workers`, `events`,
  `worker_states`) and is not part of this runtime spec. A merge started
  from the UI lands in `<shared.dir>/merged/` and still carries fleet-wide
  event data, so it is created `0600` like every other database here.
  Merge results and archives are left with a rollback journal, so a merged
  file opens read-only without any `-shm` sidecar.

## Modes: which files exist when

The recorder and the cache are switched independently. The recorder file
needs `persistence: sqlite` and a folder; the memory store needs neither:

| Config | Effect |
|---|---|
| `ui.enabled: false` | No UI subsystem at all — no recorder files, no watchdog, no WS. The cache still runs if enabled and `storage.dir` is set. |
| `storage.dir: ''` | **Memory-only mode.** The persistence mode derives to `none`. No recorder files, no watchdog file, nothing sealed or published, no archive pass. The UI reads the [memory store](recent-events.md): History, Trace, Task Detail, the Databases tab (the memory row) and the memory snapshot download work, and they cover the memory window only. With a `shared.dir` the worker still reads the sealed history of its cluster. The workers list needs `shared.dir` (or another [registry provider](fleet.md)) to see peers. |
| `ui.recorder.persistence: none` with a `storage.dir` | Same recorder behaviour as memory-only mode. The cache file and the watchdog file stay in `storage.dir`. |
| `ui.recorder.persistence: postgres` | No recorder files, as in memory-only mode. The worker also writes its events to a table in Postgres, and the trace with history reads that table instead of the sealed files. `storage.dir` is optional; with a folder, the cache file and the watchdog file stay there. See [Recorder History in Postgres](recorder-postgres.md). |
| `ui.recorder.store_events: false` | No memory store, the `events` table stays empty and events are not buffered; the WS live view still streams. `worker_config` / `worker_state` are still written per their own flags. History, trace, and task queries return nothing. |
| `ui.recorder.store_config: false` | No `worker_config` row, so this worker's recorder files do not name it or its cluster inside the database. The meta file of each sealed file still names both. Discovery and the cache exchange do not depend on it: they use the [fleet registry](fleet.md). |
| `ui.recorder.store_state: false` | No periodic `worker_state` snapshots. |
| `cache.enabled: false` *(default)* | No cache files. The handler cache API is a no-op. |
| `cache.enabled: true` with `storage.dir: ''` | Startup warning `cache_engine_disabled_no_storage_dir` and the cache engine stays off: no cache file, nothing persisted, and no [cache exchange](#cache-exchange) in either direction. |

Any combination is valid: for example `store_config: true` with
`store_events: false` keeps each recorder file named after its worker and
cluster while writing no event history.

## What the events table stores — and the knobs that trim it

The `events` table is the long-term forensic record: one row per pipeline
event, columns type-dependent. The full event-type catalog (which event
fires when, with which fields) lives in
[Observability → Flight Recorder](observability.md#flight-recorder).
The rows that matter most for after-the-fact investigations:

- **`task_started`** carries `args`, `pid`, `labels`, and in `metadata`:
  `source_offsets`, the queue wait before a slot freed up
  (`queue_wait_ms`), and — when enabled — the task's **stdin** content
  (otherwise a `stdin_bytes` size marker).
- **`task_completed`** carries `duration`, `exit_code`,
  `stdout`/`stderr`, and the subprocess start latency (`spawn_ms`) in
  `metadata`.
- **`task_failed`** always stores the task's stdin (capped) plus the
  exception detail in `metadata`, regardless of `store_stdin` — a failed
  task is exactly the one you replay later.
- **`runtime_stall`** persists the event-loop stall duration **with the
  captured stack traces** ([Runtime Health](runtime-health.md)), so "the
  whole worker froze for 3 seconds at 04:12" stays answerable from the
  archive.
- **`resource_sample`** is a periodic snapshot (every
  `state_sync_interval_seconds`) of what the worker consumed: RSS, thread
  count, open file descriptors, CPU percent for the process and its
  *reaped* subprocesses, and the host network byte totals — so an archive
  alone can answer "which resource was the bottleneck at 04:12". Fields
  whose source the platform lacks are omitted, never zeroed.
- **`annotation`** rows are your own handler's diagnostics
  ([Annotations](annotations.md)).

Content-retention knobs (all under `ui.recorder.`; they gate row
*presence and payload*, never table layout):

| Knob | Default | What it trims |
|---|---|---|
| `store_output` | `true` | `false` drops `stdout`/`stderr` content from all task events. |
| `output_min_duration_ms` | `500` | Tasks faster than this keep their row but store no `args`/`stdout`/`stderr`. |
| `event_min_duration_ms` | `0` | Task events faster than this are not persisted at all (`0` = persist everything). |
| `store_stdin` | `false` | `true` stores each task's stdin in `task_started` metadata. Off, only the `stdin_bytes` size marker is written. Failed tasks always store stdin. |
| `stdin_max_bytes` | `65536` | Byte cap on stored stdin (`0` = unlimited); truncation is flagged as `stdin_truncated` in the metadata. |
| `annotations_enabled` + `annotation_max_bytes*` | `true` | Handler-annotation acceptance and size budgets. |

The UI names these gates when they bite: a Task Detail page missing stdin
or stdout says exactly which setting excluded it. The full annotated key
list, with env-var overrides, is in the
[Config Reference](config-reference.md).

## Schemas

The DDL below is normative: it is what `drakkar/recorder/schema.py` and
`drakkar/cache/sql.py` create, and what anything reading these files on a
shared volume may rely on.

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
`/trace-by-label` (`SELECT *` pass-through). It is pinned in code
(`EVENT_COLUMNS`) with tests asserting the live table matches, and
normatively in `drakkar-ui/docs/api-contract-v1.md`. Column *presence* is
contractual; values stay event-type-dependent.

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

A worker with [no Kafka source](sources.md) writes `source_topic` and
`consumer_group` as empty strings. The column set does not change, so a
peer reading the file needs no special case — read an empty value as "this
worker consumes no topic".

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
    health_state        TEXT,    -- 'healthy'/'degraded'/'stalled'; NULL = monitor off
    loop_lag_ms         REAL,    -- event-loop lag at the sync tick
    updated_at          REAL NOT NULL,
    updated_at_dt       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_worker_state_updated ON worker_state(updated_at);
```

The two runtime-health columns let a merged fleet database answer "which
worker was degraded when" without replaying events. Databases written by
older backends lack them; readers treat that as NULL.

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

The connection discipline is:

- **One writer connection per live DB** (SQLite allows a single writer
  anyway).
- **A dedicated read-only reader connection** per live DB for UI queries,
  cache `get()` fallback and the cache pages that peers pull, so reads
  never queue behind flush commits.
- **`journal_mode=WAL` on the live file**, set on the writer at open
  (persisted in the DB header; readers inherit it). WAL lets the reader
  connection read consistent snapshots while the writer commits.
- **A rollback journal on every file that leaves `storage.dir`.** Sealing
  switches a recorder file to `journal_mode=DELETE` before it moves into the
  history folder (see [Sealing](#sealing-and-the-history-folder)). Archives
  and merge results are left the same way. A sealed file never changes, so
  readers open it with `immutable=1`: no locks and no sidecar files.
- **Explicit `busy_timeout` on every connection**:

| Connection | busy_timeout |
|---|---|
| Recorder writer, own reader | 5000 ms |
| Cache writer, own reader | 10000 ms |

  The cache's connections wait longer so a checkpoint can't fail a local
  flush. No worker opens another worker's cache file.
- **`synchronous=NORMAL` on every WAL writer** — the
  recorder (including the connection rotation opens) and the handler cache.

  SQLite's default is `FULL`, which fsyncs the WAL on **every commit**.
  These stores commit often — the recorder on its flush interval, on each
  state sync and on any UI poll that forces a flush; the cache on its own
  flush interval — and a `storage.dir` can be a network mount, where one
  fsync costs tens of milliseconds and reads on the same connection queue
  behind it. `NORMAL` is the level SQLite's own documentation recommends
  under WAL: the WAL is synced at checkpoints instead.

  What you give up: `NORMAL` remains **fully safe across an application
  crash** — the process being killed, an OOM, a failed deploy — because the
  WAL is still written and replayed on the next open. It can lose the most
  recently committed transactions only when the **host** dies mid-write: a
  power cut or a kernel panic. For a flight recorder and a last-writer-wins
  cache, neither of which is a system of record, that is the correct trade.
  If your deployment needs stricter durability from these files, they are
  the wrong place to keep that data.

  Note that `synchronous` is per-**connection** and, unlike `journal_mode`,
  is not stored in the database header. The worker sets it on every writer
  connection it opens.
- No explicit WAL checkpoints, no `VACUUM`, driver-default `foreign_keys`.

## Encodings

- **JSON columns** (`args`, `metadata`, `labels`, `sinks_json`,
  `env_vars_json`, `assigned_partitions`): compact separators, **sorted
  keys**, raw UTF-8 (no `\uXXXX` escapes). Byte-identical across the
  orjson and stdlib encoder paths.
- **Datetimes embedded in JSON** use one canonical format — UTC,
  RFC 3339, **fixed six-digit microseconds**, `Z` suffix:
  `2026-07-05T12:34:56.123456Z` (`drakkar.timefmt.format_rfc3339_micro`).
  Fixed width is deliberate: no trailing-zero trimming and no
  `+00:00`/`Z` ambiguity, so the bytes are always the same shape.
- **`dt` / `created_at_dt` / `updated_at_dt` columns** keep their own
  display format `YYYY-MM-DD HH:MM:SS.mmm` (space separator,
  milliseconds).
- **`ts` columns** are `REAL` Unix seconds.
- **Cache `value` column**: JSON, but the exact bytes are **deliberately
  unspecified** — this worker writes `json.dumps` defaults (spaces,
  insertion-order keys), and a reader must accept compact or sorted-key
  forms too. Values are opaque: LWW compares `updated_at_ms` +
  `origin_worker_id`, never bytes. `size_bytes` reflects the writer's own
  encoding.

## Write patterns & archiving

**Recorder**: events wait in the intake buffer (capped at
`ui.recorder.max_buffer`). Every `ui.recorder.flush_interval_seconds` the
memory loop gives each row its id and writes it to the
[memory store](recent-events.md); with `sqlite` it then puts the rows in the
file queue (capped at `max_buffer` too). The file loop writes the file queue
in transactions of up to 5000 rows. A row has the same id in memory and in
the file. Ids continue across rotations, so a new file does not start at
id 1; they start again at 1 after a restart. On a file write failure the
chunk is re-queued at the front of the file queue (tail evictions are
counted) and dropped from the file after `ui.recorder.max_flush_retries`
consecutive failures. Recording-side filters (`event_min_duration_ms`,
`output_min_duration_ms`, `store_output`) gate what enters the buffer —
they affect row presence, never layout.

**Last-breath flush**: the most interesting events of a dying worker are
the unflushed last ones, so the buffer is salvaged on fatal exits that skip
the clean shutdown path. An `atexit` hook (armed at recorder start,
disarmed by a clean stop) writes the remaining buffer synchronously through
a direct SQLite connection — covering startup failures after the recorder
came up, unhandled exceptions, and stray `sys.exit` calls. It is
best-effort and logs `recorder_last_breath_flush` when it fires. With
`persistence: none` there is no file, and it writes nothing.
Neither can help against SIGKILL / the OOM killer — the watchdog file
covers *detecting* those, and the last periodic flush is what remains.

**Rotation** (every `ui.recorder.rotation_interval_hours`): flush →
create + initialize the new timestamped DB (WAL, busy_timeout, schema,
reader) → atomic swap → rewrite `worker_config` → close the old reader →
[seal](#sealing-and-the-history-folder) the old file. Rotation only seals:
moving the sealed file into the history folder is a background job.
Rotation itself deletes nothing — the [archive pass](#archiving) is the only
thing that ever removes a sealed recorder file.

**Cache**: dirty entries flush every `cache.flush_interval_seconds`
(atomic dirty-map swap, single transaction, restore-on-failure); expired
rows are deleted every `cache.cleanup_interval_seconds`. The cache file
never rotates.

## Sealing and the history folder

A live recorder file is opened only by its owner. When the owner is done
with the file, it **seals** it and moves it into the history folder of its
cluster, `<shared.dir>/history/<cluster>/`. A sealed file never changes
again. Any worker can read it without locks, and the archive pass needs no
check for files still in use.

### When a file is sealed

- **At rotation.** The old file is sealed right after the swap to the new
  file.
- **At a clean stop.** The live file is sealed after the final flush. The
  seal always runs. The move into the history folder runs only while
  shutdown time remains: the rest of `executor.drain_timeout_seconds` after
  draining. When no time remains, the sealed file stays in `storage.dir`,
  and the next start with the same worker name moves it as a
  [leftover](#leftovers).
- **Leftovers** of an earlier process, after the worker starts. See
  [Leftovers](#leftovers).

Memory-only mode (`storage.dir: ""`) and `persistence: none` create no
recorder files and seal nothing. A leftover file of an earlier `sqlite` run
under the same worker name stays in `storage.dir` and is not sealed. Stop a
worker cleanly before you switch it to `none`.

### How a file is sealed

1. On the last open connection to the file, the recorder switches the file
   to a rollback journal (`PRAGMA journal_mode=DELETE`). The switch fails
   when another connection still holds the file. Then the file stays in the
   backlog.
2. On the same connection, it computes the content of the
   [meta file](#the-meta-file), then closes the connection. The result is a
   file with no `-wal` or `-shm` sidecar, which never changes again.
3. It **publishes** the file: it writes `<name>.db.meta.json` into
   `<shared.dir>/history/<cluster>/`, then renames the database next to it.
   The meta file is written first. A crash between the two leaves a meta
   file without its database; readers ignore it, and the database stays in
   `storage.dir` to be sealed again.

When `storage.dir` and `shared.dir` are on different filesystems, the
rename fails with `EXDEV`. This is the usual fleet layout: a local disk per
pod plus a network mount. The worker then:

1. copies the file to a dot-prefixed `.<name>.<16 hex>.incoming` file in the
   history folder;
2. fsyncs the copy;
3. renames the copy to its final name;
4. fsyncs the folder;
5. deletes the source in `storage.dir`.

Dot-prefixed files never appear in listings and are never archived. The
archive pass removes an `.incoming` file that a stopped copy left behind
(see [the sweep](#the-sweep)).

### Background moves and the backlog

Rotation only seals. Moving the sealed file into the history folder runs in
a background job, one at a time, so a slow or hung shared mount never holds
up rotation or the pipeline. A file whose seal or move fails stays in
`storage.dir`, closed, in an in-memory **backlog**. The background job
retries it on every rotation tick. A backlog file that is gone (deleted by
hand) leaves the backlog.

| Metric | Meaning |
|---|---|
| `drakkar_recorder_unsealed_files` | Own recorder files closed but not yet sealed into the history folder. |
| `drakkar_recorder_seal_failures_total` | Recorder files whose sealing failed; they stay in the backlog and are retried. |

Suggested alert: `drakkar_recorder_unsealed_files > 0` for two rotation
intervals.

### Leftovers

A **leftover** is a recorder file of this worker name that an earlier
process left in `storage.dir`: after a crash, or after a clean stop with no
time left to move it. A file is a leftover when all of these hold:

- its name is exactly `<worker>-YYYY-MM-DD__HH_MM_SS.db` for this worker
  name, so worker `a` never touches the files of worker `a-b`;
- it is not the live file;
- nothing touched the file or its `-wal` for 300 seconds. A second process
  with the same worker name may still write a fresh file, and network
  mounts round modification times.

The worker looks for leftovers in the background after it starts: once at
start, once more after 300 seconds, and then on every rotation tick. It
opens a leftover normally, so SQLite replays the WAL of a crashed process.
Then it seals and moves the file like any other.

### Log events

| Event | When |
|---|---|
| `recorder_sealed` | A sealed file reached the history folder. Fields: `db_path`, `destination`, `crossed_filesystems`, `size_bytes`, `events`. |
| `recorder_leftover_sealed` | The same, for a leftover. |
| `recorder_seal_failed` | A seal or a move failed. Field `stage` is `seal` or `publish`. The file stays in the backlog. |
| `recorder_background_job_failed` | A background job failed with an unexpected error. Field `job` names it: `sealing`, `leftover_sealing`, `archive` (an archive pass) or `archive_footprint` (the footprint update of an archive slot that this worker did not win). A seal runs again at the next rotation tick, an archive pass at the next archive slot. |

## The meta file

Every sealed file has a meta file next to it: `<name>.db.meta.json`. The
owner writes it once, at seal time, from the connection that sealed the
file. Readers — the Databases page and the history listings — never open a
sealed file to learn its statistics. They read the meta file.

Schema 1:

| Key | Type | Value |
|---|---|---|
| `schema` | int | `1`. |
| `worker` | string | Name of the worker that wrote the file. |
| `instance` | string | 16 hex characters, new for every process. It tells two runs of one worker name apart. |
| `cluster` | string | Cluster name as configured (not sanitized). |
| `first_event_ts` | string or `null` | Time of the earliest event (`MIN(ts)` of `events`). `null` when the file has no events. |
| `last_event_ts` | string or `null` | Time of the latest event (`MAX(ts)` of `events`). `null` when the file has no events. |
| `sealed_at` | string | When the file was sealed. |
| `size_bytes` | int | Size of the file after the seal. |
| `tables` | object | Row count per table. SQLite's internal tables (`sqlite_*`) are left out. |
| `event_types` | object | Event count per event type. |

Encoding: sorted keys, compact separators, raw UTF-8, no trailing newline,
file mode `0600`. Times use the canonical datetime format from
[Encodings](#encodings). The bytes are pinned by
`tests/fixtures/history_meta.json`, so one meta object always serializes to
the same bytes, for any implementation. An example:

```json
{"cluster":"search","event_types":{"task_completed":120211,"task_started":120400},"first_event_ts":"2026-10-13T08:00:00.000000Z","instance":"9c41e0b27fd34a1e","last_event_ts":"2026-10-13T08:59:59.871000Z","schema":1,"sealed_at":"2026-10-13T09:00:02.410000Z","size_bytes":48211968,"tables":{"events":412093,"worker_config":1,"worker_state":360},"worker":"search-worker-3"}
```

## Archiving

Age-based and count-based retention (the old `retention_hours` /
`retention_max_events` keys) are gone. In their place, a periodic
**archive pass** folds each finished UTC time window's sealed files into
one compressed, merged database per cluster, and deletes only the sealed
files it successfully merged. A worker's pass works only inside the history
folder of its own cluster, `<shared.dir>/history/<cluster>/`. The window
math, file names and the protocol of the default lock
(`drakkar/recorder/archive.py`, `drakkar/recorder/task_lock.py`) are part
of the on-disk spec, so a fleet sharing one `shared.dir` archives correctly
no matter which worker does the work for a given cluster.

### When the pass runs: archive slots {#archive-slots}

The archive pass is the system [cluster periodic task](periodic-tasks.md)
`history.archive`. It runs only on a worker with `ui.enabled`,
`history.archive.enabled`, `ui.recorder.persistence` `sqlite` (derived or
set) and a `storage.dir`. A `none` worker does not take part in the archive
election. Its slots are the same on every worker of the cluster:

- `history.archive.interval` (default 3600): a slot at every whole multiple
  of that many seconds since 1970-01-01T00:00:00Z, so at the top of each
  UTC hour by default. A whole number of at least 30.
- or `history.archive.cron`: a five-field UTC cron expression in the
  [accepted subset](periodic-tasks.md#cron), for example `'15 */2 * * *'`.
  Give at most one of `interval` and `cron`.

At each slot every worker of the cluster asks the
[election provider](#the-election-provider) for the lease of the scope
`<cluster>/history.archive`. The worker that gets it runs the pass in the
background, on its own archive thread; the others skip the slot and lose
nothing, because any window still due is picked up at a later slot. Every
worker that does not win the slot updates the footprint gauges
(`drakkar_recorder_archive_bytes`, `drakkar_recorder_archive_files`) at
that slot, when its archive thread is idle. The rules of the slot (the
10-second acquire window, the 20-second minimum hold, no catch-up of missed
slots) are on [Periodic Tasks](periodic-tasks.md#cluster-tasks-the-slot-rule).

A worker runs the archive task when the UI is on (the recorder exists),
`history.archive.enabled` is true and `storage.dir` is set. The rotation
only rotates and seals; it starts no pass. A pass that runs longer than the
interval skips the slots that pass meanwhile.

```yaml
history:
  archive:
    interval: 1800               # a slot every 30 minutes; or cron: '0 */6 * * *'
    lease_seconds: 60            # how long the lease of a dead archiver lives
    sweep_after_seconds: 7200    # the age of work files the pass deletes
```

A worker that stops ends its pass after the window in progress and skips
expiry and pruning; the windows left stay due for the next pass of any
worker. At a clean stop the recorder lets the pass in progress end its
window. A pass ends only between windows, so the wait has no fixed limit.
While a worker registry runs, the wait keeps
`fleet.registry.resilience.timeout_seconds` of the time left for the card
withdraw that comes after the recorder: it ends when the time left is down
to that value, or at once when less is left. Without a registry it can use
all the time left. A pass that is still running then is left to end on its
own and logged as `recorder_archive_pass_abandoned` (fields
`waited_seconds` and `detail`). The election provider then stops with the
time that is left. A provider refuses every call after its stop, so the
abandoned pass is fenced at its next check, and its lease runs out.

### Defaults walkthrough

With `ui.recorder.rotation_interval_hours: 1` and `history.archive`
`enabled: true`, `window_hours: 24`, `due_after_hours: 48`,
`retention_days: 30` and `interval` not set (a slot every 3600 seconds; all
defaults):

- Every hour, rotation opens a new `<worker>-<timestamp>.db` file and seals
  the previous one into the history folder.
- Windows are UTC calendar days, `[00:00, 24:00)`. A window is **due** once
  the due delay (`due_after_hours`, 48h) has passed since it ended. Every
  file in the history folder is sealed, so there is no check for files
  still in use. With the defaults a window becomes due exactly 48h after it
  closes and is archived at the next archive slot after that, the next full
  UTC hour — that delay is the safety margin, not a bug. It also gives a
  late file of the window, such as a backlog file or a leftover, time to
  reach the history folder.
- Because of that delay, a single recorder file's lifespan on disk —
  counted from when it was *created*, not from when its window closed — can
  reach up to ~72h: up to 24h as part of the window it belongs to (a
  file created at the window's own start closes the window out 24h
  later), plus the ~48h due delay after the window closes before
  archiving actually runs.
- Once due, the worker that wins the archive slot merges the window's
  sealed files into one `<cluster>-<from>__<to>.db.gz` in the same folder,
  then deletes the sealed files and the meta files it merged.
- `retention_days: 30` deletes a `.db.gz` file once its window
  ended more than 30 days ago, so the archive footprint settles at about
  a month of history. `0` disables expiry entirely — nothing then deletes
  a `.db.gz` file, and the worker logs a `recorder_archives_unbounded`
  warning at startup saying so.

Steady state: up to three windows' worth of sealed `.db` files per worker in
the history folder at any time — the window currently being written plus
the two most recently closed ones, until the oldest of those finishes
archiving — and one `<cluster>-<from>__<to>.db.gz` per cluster per window
reaching back as far as retention allows.

### Windows are keyed by file start time, not event time

A window is `[k·W, (k+1)·W)` in UTC epoch seconds, width
`history.archive.window_hours`. A sealed file belongs to the window holding
its own **start** timestamp (parsed from the filename) and is never split
across two windows. Because a file keeps recording events until it rotates,
an archive can therefore carry a handful of events timestamped slightly
past its own window end — archives partition by when a file was *opened*,
not by when each event inside it happened.

### Only sealed files are archive candidates

Every `*.db` file in a cluster history folder is sealed: the live file never
leaves `storage.dir`, and a file reaches the history folder only after its
seal. So the pass needs no in-use check, no "settled" modification-time
check, and no opening of files to learn their cluster — the folder is the
cluster. It reads its sources with `immutable=1`, which takes no locks.

Merge results from the UI live in `<shared.dir>/merged/`, outside every
history folder, so they are never archive candidates. Dot-prefixed work
files are never candidates either.

### The task lock {#the-task-lock}

The task lock is the lease of the default election provider, `filesystem`
(see [The election provider](#the-election-provider)). It serves every
cluster periodic task, the archive included. The lock of task `<task>` of a
cluster is the file `<shared.dir>/tasks/<cluster>/<task>.lock`, so the
archive's lock is `tasks/<cluster>/history.archive.lock`. At each slot every
worker of the cluster tries to create it with an exclusive create, mode
`0600`. One worker wins and runs the slot; the others skip it. The worker
creates the folder `tasks/<cluster>/` with mode `0700` at its first
acquire; a missing parent folder `tasks/` is created with the default mode
of the process (its umask). The lock body is compact JSON with sorted keys,
pinned by `tests/fixtures/history_meta.json`:

```json
{"acquired_at":"2026-10-14T02:00:00.125000Z","instance":"9c41e0b27fd34a1e","provider":"filesystem","token":"5f0c1d2e3a4b6978","worker":"search-worker-3"}
```

The `provider` key names the election provider that made the lock, here
`filesystem`.

- **Refresh.** The holder refreshes the lock's modification time every
  third of the task's lease (`history.archive.lease_seconds` for the
  archive), at most every 60 seconds, while its run goes on. After the run
  and the minimum hold it deletes the lock, but only while the lock still
  holds its own token.
- **Takeover.** A lock that nobody refreshed for the lease length belongs
  to a dead holder. The next worker renames it aside to
  `<task>.lock.stale.<token>` (one rename wins), deletes it, and creates its
  own lock. Log: `periodic_lock_taken_over` (fields `provider`:
  `filesystem`, `scope`, `previous_worker`, `previous_instance`,
  `age_seconds`). When two workers take over at once, the slower one can
  move aside the fresh lock the faster one just created. It then sees that
  the lock is not stale, puts it back with a hard link (which never
  overwrites a newer lock) and stands down. If a third worker created a
  lock in that moment, the put-back fails (log:
  `periodic_lock_put_back_failed`) and the holder of the moved lock is
  fenced at its next check. A stale copy that a crash left behind stays in
  `tasks/<cluster>/`; nothing sweeps that folder, and the file is safe to
  delete.
- **The file server's clock.** Ages are measured on the file server's
  clock: the worker touches a marker file `.clock.<instance>` in the same
  folder and uses its modification time as "now". No host's wall clock is
  compared with another host's.
- **Fencing.** Right before it publishes an archive, and before it deletes
  any source, the archive pass checks that the lock still holds its token.
  When the token is no longer its own — another worker took the lock over —
  the pass publishes nothing and deletes nothing. Log:
  `recorder_archive_fenced` (see [Fencing](#fencing) for its fields).

The lock uses only an exclusive create, renames and modification times. It
does not use `flock`.

### The election provider

Which worker of a cluster runs a slot of a cluster periodic task — the
archive pass included — is decided by an **election provider**. It gives
out **leases**: one holder per *scope*, and the scope is
`<sanitized cluster>/<task name>`, for example `main/history.archive`. Each
slot takes the lease under a token of its own (16 hex characters, new for
every slot), refreshes it while the run goes on, and releases it after the
run and the minimum hold. The archive pass also checks it right before each
publish and each delete.

```yaml
periodic:
  election:
    provider: filesystem       # the default: the task lock above; or postgres, redis, consul
    # connection: main         # read by the postgres, redis and consul providers
    # resilience: {}           # time limit, retries and breaker; same keys as fleet.registry.resilience
history:
  archive:
    lease_seconds: 60          # the lease length of the archive slot, for every provider
```

- **One provider per cluster.** Every worker of a cluster must use the same
  provider. Workers on different providers cannot see each other's leases,
  so nothing then keeps two of them from running the same slot.
- **The lease length** is the task's `lease_seconds`
  (`history.archive.lease_seconds` for the archive; default 60, from 40 to
  86400) whichever provider is used. The holder refreshes the lease every
  third of that length, at most every 60 seconds. A holder that died stops
  refreshing, and once the lease length has passed another worker may take
  the scope at the next slot.
- **Providers.** `filesystem`, `postgres`, `redis` and `consul` are built in.
  The last three keep the lease in a service reached through a [named
  connection](fleet.md#connections). An unknown name
  fails startup with a message that lists the providers that exist, for
  example
  `periodic.election.provider 'etcd' is not known; known providers: consul, filesystem, postgres, redis`.
  The name is checked only when the worker runs at least one cluster task
  (the archive counts when it runs). With none, any name is accepted.
- **Start and stop.** The provider starts at the first slot that needs it,
  not at worker start (so with the `postgres` provider the tables are first
  checked or created then, with the `redis` provider the server is first
  sent a `PING` then, and with the `consul` provider the agent is first
  asked for the leader of its cluster then). The start must end inside the
  slot's acquire window; when it fails, the slot is skipped as
  `election_failed` and the next slot tries the start again. While the
  circuit breaker is open, a slot skips the start until the cooldown has
  passed. At a clean stop the provider stops once, after the recorder (see
  [Archive slots](#archive-slots) for the wait for a running pass), inside
  the shutdown time left.
- **Bounded calls.** The archive pass runs in a thread of its own, and
  every provider call runs on the event loop under the
  [`resilience`](fleet.md#resilience) block of `periodic.election`. The
  thread waits for one check at most as long as the longest the call can
  take (every attempt and every wait) plus one second. A hung provider
  therefore holds up the archive thread for a bounded time and never the
  event loop or the rotation.
- **A lease that cannot be confirmed counts as lost.** A call that fails,
  times out or is skipped by the open breaker reads as "not held". A refresh
  that does not answer `True` ends the refreshing and logs
  `periodic_lease_lost`, and every later check of that slot answers "not
  held" without a provider call.

#### The `postgres` election provider {#the-postgres-election-provider}

The lease is a row of `<schema>.task_locks`, in the schema of the
connection that `periodic.election.connection` names. It fits a
deployment that already runs Postgres. The sealed history folder
(`shared.dir`) is still a shared folder; only the choice of the worker that
runs a slot moves to the database.

```yaml
shared:
  dir: /mnt/drakkar-shared
connections:
  postgres:
    main:
      schema: drakkar
periodic:
  election:
    provider: postgres
    connection: main
```

The connection string comes from the environment:
`DK_CONNECTIONS__POSTGRES__MAIN__DSN`.

- **One row per scope.** `task_locks` has the columns `scope` (the
  primary key, for example `main/history.archive`), `token`, `worker`,
  `instance`, `acquired_at` and `refreshed_at`.
- **Acquire.** One transaction reads the row of the scope `FOR UPDATE` and
  then upserts it. The upsert writes only when the row is free, holds the
  token of this slot, or has a `refreshed_at` older than the task's
  `lease_seconds` on the database server's `now()`. So the lease length is
  judged on the server's clock, and no worker's wall clock is compared with
  another's. A worker that takes a free scope or its own token gets the
  lease; any other worker gets `False` and skips the slot.
- **Refresh and release.** `refresh` moves `refreshed_at` to the server's
  `now()`, and `release` deletes the row, each only while the row holds the
  caller's token. A holder that was taken over cannot extend or free the new
  holder's lease. `is_held` (the fencing check) is true only while the row
  holds the token.
- **Takeover.** When the upsert replaces another token's row, the worker logs
  `periodic_lock_taken_over` with `provider` set to `postgres`, and
  the fields `scope`, `previous_worker`, `previous_instance` and
  `age_seconds` (the age of the lease that ran out, on the server's clock).
  A fenced pass logs `recorder_archive_fenced` as described below.
- **The schema.** `manage_schema: true` creates the schema and both tables at
  the provider's first successful start, in one transaction behind an
  advisory lock. With `manage_schema: false` the start checks the table
  `task_locks` and, when it is missing, fails with a message that names
  `drakkar-schema postgres --schema=<name>`; the next slot tries the
  start again. The command prints the DDL for an operator to run; see
  [Fleet registry: postgres](fleet.md#postgres). With `--recorder`
  (and optional `--table-prefix=<p>` and `--retention-hours=<n>`) it also
  prints the recorder events table of
  [`persistence: postgres`](recorder-postgres.md#schema-management-and-ownership).
- **The old table.** An older build may have created a table
  `archive_locks` for the archive lease. Nothing reads it any more. Once no
  worker of that build runs, drop it:
  `DROP TABLE IF EXISTS <schema>.archive_locks;`.
- **An outage** is handled as for the registry: transient Postgres errors
  are retried under `periodic.election.resilience`, and the breaker
  opens after `failure_threshold` failed operations. Slots during an outage
  are skipped as `election_failed`. A lease that cannot be confirmed counts
  as lost (see above), so an archive pass during an outage stops without
  publishing or deleting. Its row stays until the lease length has passed,
  as for any holder that stops refreshing, and then a later slot takes it
  over.

#### The `redis` election provider {#the-redis-election-provider}

The lease is a key of the Redis database that the connection named by
`periodic.election.connection` points at. It fits a deployment that
already runs Redis. The sealed history folder (`shared.dir`) is still a shared
folder; only the choice of the worker that runs a slot moves to Redis.

```yaml
shared:
  dir: /mnt/drakkar-shared
connections:
  redis:
    main:
      key_prefix: "drakkar:"
periodic:
  election:
    provider: redis
    connection: main
```

The URL comes from the environment: `DK_CONNECTIONS__REDIS__MAIN__URL`. The
server, the database number and the `key_prefix` must be the same on every
worker of the cluster. The requirements for the server, the URL and the Redis
user are the same as for the registry; see [Fleet registry:
redis](fleet.md#redis) and [Security](security.md#provider-credentials).

- **One key per scope.** The key is `<key_prefix>task-locks:<scope>`, for
  example `drakkar:task-locks:main/history.archive`. Its value is the lock body, in the same
  encoding as the `filesystem` lock:

  ```json
  {"acquired_at":"2026-10-14T02:00:00.125000Z","instance":"9c41e0b27fd34a1e","provider":"redis","token":"5f0c1d2e3a4b6978","worker":"search-worker-3"}
  ```

  `acquired_at` comes from the clock of the worker that took the lease and is
  for information only.
- **The lease is the time to live of the key.** It is the task's
  `lease_seconds`. The Redis server judges it: the lease
  is alive while the key exists, and Redis deletes the key when the time to
  live ends. No worker's wall clock is compared with another's.
- **Acquire.** One script. It sets the body together with the time to live
  when the key is absent. For the token that already holds the key it moves
  the time to live and keeps the body, so `acquired_at` stays at its first
  value. For any other token it changes nothing, and the worker gets `False`
  and skips the slot.
- **Refresh and release.** `refresh` moves the time to live of the key to the
  lease length, and `release` deletes the key, each only while the body holds
  the caller's token. The check runs on the server inside the script. A holder
  that was taken over cannot extend or free the new holder's lease. `is_held`
  (the fencing check) reads the key and is true only while the body holds the
  token.
- **Takeover.** A lease that ran out is a key that Redis has deleted, so the
  next slot finds a free scope. There is no previous holder to read, and the
  `redis` provider writes no `periodic_lock_taken_over` line. Only the
  `filesystem` and `postgres` providers write it. A fenced pass logs
  `recorder_archive_fenced` as described below.
- **A lost key.** A Redis server that restarts without its data, a flush of
  the database, or eviction deletes the lease key before its time to live
  ends. Every key of the providers has a time to live, so any `volatile-*`
  and any `allkeys-*` `maxmemory` policy can evict the key. A database number
  of its own does not protect it, because the memory limit and eviction cover
  the whole server. The holder's next refresh then answers `False`, which
  ends the refreshing (`periodic_lease_lost`) but does not stop the run by
  itself. For the archive, the fencing check before the next publish or
  delete answers "not held" (it finds no key, or the refresh has already
  answered `False`), and that check stops the pass without publishing or
  deleting. It logs `recorder_archive_fenced` with `reason` `taken_over`,
  whose detail says that another worker took the archive lock over, although
  no worker did: the provider can only answer that the pass no longer holds
  the lease. The next slot takes the scope. Under the default policy,
  `noeviction`, a full server evicts nothing and answers a script that needs
  more memory with an error that names `OOM`. That error is final and counts
  as a failed operation for the breaker.
- **An outage** is handled as for the registry: connection errors and the
  server replies `BUSY`, `LOADING`, `MASTERDOWN` and `TRYAGAIN` are retried
  under `periodic.election.resilience`, and the breaker opens after
  `failure_threshold` failed operations. Any other reply, such as `WRONGPASS`
  or `NOPERM`, is final. Slots during an outage are skipped as
  `election_failed`. A lease that cannot be confirmed counts as lost (see
  above), so an archive pass during an outage stops without publishing or
  deleting. Its key stays until the lease length has passed, as for any
  holder that stops refreshing, and then a later slot takes the scope.

#### The `consul` election provider {#the-consul-election-provider}

The lease is a key of the Consul agent that the connection named by
`periodic.election.connection` points at, held by a session of the slot.
It fits a deployment that already runs Consul. The sealed history folder
(`shared.dir`) is still a shared folder; only the choice of the worker that
runs a slot moves to Consul.

```yaml
shared:
  dir: /mnt/drakkar-shared
connections:
  consul:
    main:
      address: http://127.0.0.1:8500
      key_prefix: drakkar
periodic:
  election:
    provider: consul
    connection: main
```

The ACL token comes from the environment: `DK_CONNECTIONS__CONSUL__MAIN__TOKEN`.
The Consul cluster, the datacenter and the `key_prefix` must be the same on
every worker of the cluster. The requirements for the agent, the address and the
token are the same as for the registry; see [Fleet registry:
consul](fleet.md#consul) and [Security](security.md#provider-credentials).

- **One key per scope.** The key is `<key_prefix>/task-locks/<scope>`, for
  example `drakkar/task-locks/main/history.archive`. Its value is the lock body, in the same
  encoding as the `filesystem` lock:

  ```json
  {"acquired_at":"2026-10-14T02:00:00.125000Z","instance":"9c41e0b27fd34a1e","provider":"consul","token":"5f0c1d2e3a4b6978","worker":"search-worker-3"}
  ```

  `acquired_at` comes from the clock of the worker that took the lease and is
  for information only.
- **The lease is the time to live of the session.** Each slot creates a
  session named `drakkar/task-lock/<scope>/<token>`, with `Behavior: delete`
  and `LockDelay: 0s`. Its time to live is half of the task's
  `lease_seconds`, rounded up, and at least 10 seconds (Consul accepts no
  less): 30 seconds with the default lease of 60. Consul ends a session no
  earlier than its time to live after the last renew, and at most twice its
  time to live after it. So the lease of a dead holder ends by
  `lease_seconds`, one second later when that value is odd (the time to live
  is rounded up). Consul keeps the time, so no worker's wall clock is
  compared with another's. The holder renews the session every third of
  `lease_seconds`, at most every 60 seconds. Consul may end the session as
  early as its time to live, half of `lease_seconds`, after the last renew.
  The renew interval is shorter than that time to live by a margin of a
  sixth of `lease_seconds` (a third against a half), and by more when the
  60-second cap applies. The lease floor of 40 seconds keeps that time to
  live at 20 seconds or more, the minimum hold of a slot.
- **Acquire.** The provider creates the session and writes the body to the key
  with `?acquire=<session>`. Consul grants this only to a session when no other
  session holds the key. When another session holds it, the provider destroys
  the new session at once, and the worker gets `False` and skips the slot. For a
  token that already holds the key, acquire renews its session and writes the
  same body again, so `acquired_at` keeps its first value; when that session
  has ended, a new one is created.
- **Refresh and release.** `refresh` renews the session, and answers `False`
  once Consul no longer knows it. `release` destroys the session, which deletes
  the key only while that session holds it. A holder that was taken over cannot
  free the new holder's lease. Consul answers `200` when it destroys a session
  that is already gone, so a release that is sent again after a failure is
  harmless. `is_held` (the fencing check) reads the key and is true only while
  the session that holds it is the slot's own.
- **Takeover.** A lease that ran out is a key that Consul has deleted, because
  of `Behavior: delete`. The next slot finds a free scope. There is no previous
  holder to read, and the `consul` provider writes no
  `periodic_lock_taken_over` line. Only the `filesystem` and `postgres`
  providers write it. A fenced pass logs `recorder_archive_fenced` as described
  below.
- **A lost lease.** A session can end before the lease length: the agent
  restarts without its data, the health check of the agent's node fails (Consul
  then ends every session of the node), or the holder could not renew within
  twice the time to live. The holder's next refresh then answers `False`,
  which ends the refreshing (`periodic_lease_lost`) but does not stop the run
  by itself. For the archive, the fencing check before the next publish or
  delete answers "not held" (it finds no key, or the refresh has already
  answered `False`), and that check stops the pass without publishing or
  deleting. It logs `recorder_archive_fenced` with `reason` `taken_over`,
  whose detail says that another worker took the archive lock over, although
  no worker did: the provider can only answer that the pass no longer holds
  the lease. The next slot takes the scope.
- **An outage** is handled as for the registry: timeouts, connection errors and
  the statuses `429`, `500`, `502`, `503` and `504` are retried under
  `periodic.election.resilience`, and the breaker opens after
  `failure_threshold` failed operations. Any other status, such as a `403`
  from a token without the rights, is final. Slots during an outage are
  skipped as `election_failed`. A lease that cannot be confirmed counts as
  lost (see above), so an archive pass during an outage stops without
  publishing or deleting. Its key stays until the session ends, at most twice
  its time to live after the last renew, as for any holder that stops
  refreshing, and then a later slot takes the scope.

#### Fencing

Before it publishes an archive and before it deletes any source, the
archive pass asks the provider whether its slot still holds the lease. The
other cluster tasks do not check their lease while they run (see
[Periodic Tasks](periodic-tasks.md#cluster-tasks-the-slot-rule)). If not, it publishes and
deletes nothing and counts in `drakkar_recorder_archive_failures_total`.
`recorder_archive_fenced` is logged with these fields: `provider`,
`cluster`, `name` (the archive about to be published) and `reason`:

| `reason` | Meaning |
|---|---|
| `taken_over` | The provider answered that this pass no longer holds the lease, usually because another worker took it over. With the `redis` provider it can also mean that the lease key was lost, and with the `consul` provider that the lease's session ended early; in both cases no worker took it over (see [The `redis` election provider](#the-redis-election-provider) and [The `consul` election provider](#the-consul-election-provider)). |
| `unconfirmed` | The provider could not confirm the lease: the call failed or timed out, or the breaker was open. The lease may still be held. |

Both reasons stop the pass the same way. The `detail` field of the line
follows the reason:

| `reason` | `detail` |
|---|---|
| `taken_over` | `another worker took the archive lock over; this pass published and deleted nothing` |
| `unconfirmed` | `the election provider could not confirm the lease; this pass published and deleted nothing` |

Every provider operation is counted in `drakkar_provider_calls_total` with
`feature="election"` and the `op` `start`, `acquire`, `refresh`, `is_held`,
`release` or `stop`; see [Fleet registry — Metrics](fleet.md#metrics).

### The sweep

With the lease held, the pass first deletes work files in the history
folder older than `history.archive.sweep_after_seconds` (default 7200, at
least 60), together with their `-wal`, `-shm` and `-journal` sidecars. The
sweep age is kept apart from the lease, because the sweep also deletes the
`.incoming` sealing copies of other workers, and a short lease must not
delete a slow copy:

| Pattern | What it is |
|---|---|
| `.*.merge.db`, `.*.tmp`, `.*.previous.db` | Work files of an archive pass that stopped. |
| `.*.incoming` | A cross-filesystem copy that stopped. |
| `.clock.*` | Age markers of the file server's clock. |
| `.archive.lock.stale.*` | Locks set aside by a takeover in earlier releases, which kept the archive lock in the history folder. |
| `<name>.db.meta.json` with no `<name>.db` | A meta file whose database is gone. A fresh one is kept: it is a move in progress, because the meta file is written before the database is renamed in. |

Each deletion logs `recorder_archive_temp_swept`.

### Merge, compress, publish, then delete

Each due window is merged with the same engine `POST /api/v1/debug/merge`
uses, gzip-compressed, and published under its final name with an atomic
rename. All of this happens on dot-prefixed work files named with the lease
token. Only after the archive is durably on disk (its directory entry
fsynced) are the sealed files it covers deleted, together with their meta
files — any failure before the rename leaves every source untouched for the
next pass to retry.

- A sealed file the merge cannot read is never deleted: it is renamed to
  `<name>.unreadable` so it stops blocking its window forever, while
  staying on disk for an operator to inspect. Its meta file stays, and the
  sweep removes it later as a meta file whose database is gone.
- An archive already present at the final name — left by an earlier pass
  that died between publishing and finishing its cleanup — is folded into
  the new one rather than overwritten: sources it already covers are
  recognized and simply deleted, never re-merged (which would duplicate
  their events).

A pass that took the lease and did not finish cleanly — a window failed, the
pass was fenced, or an unexpected error — counts in
`drakkar_recorder_archive_failures_total`. At the start of every pass, and
at every archive slot that a worker does not win, the worker refreshes
`drakkar_recorder_archive_bytes` and `drakkar_recorder_archive_files` from
the archives in its cluster's history folder.

### Merge results expire

The pass also deletes UI merge results in `<shared.dir>/merged/*.db` that
are older than `history.merged_retention_days` (default 32; `0` keeps
them). Ages are measured on the file server's clock. Each deletion logs
`recorder_merged_pruned`. This runs only inside the archive pass, so with
`history.archive.enabled: false` nothing deletes merge results
automatically.

### Opting out

`history.archive.enabled: false` turns the pass off entirely: the worker
does not run the `history.archive` task. Sealing still runs. With the pass off, **nothing removes sealed recorder files or merge
results automatically** — not the old age/count retention (removed), not
archiving. A startup log line (`recorder_archiving_disabled`, with the
history folder in its `folder` field) says so explicitly; bounding the
history folder becomes the operator's job (delete old files by hand, or run
an external job against them).

### Copy-pasteable config

```yaml
storage:
  dir: /var/lib/drakkar              # this worker's own files (local disk)
  shared_dir: /mnt/fleet             # history, archives and merge results; '' = storage.dir
recorder:
  sqlite:
    rotation_interval: 1h            # a whole number of hours
  archive:
    enabled: true                    # merge sealed files into windowed .db.gz archives
    window_hours: 24                 # one archive per cluster per UTC day; must be >= rotation_interval_hours
    due_after_hours: 48              # archive a window this long after it ends; must be >= rotation_interval_hours
    retention_days: 30               # delete archives older than this; 0 = keep forever; must cover due_after_hours + window_hours
    interval: 3600                   # seconds between archive slots, aligned to the epoch; or cron: '0 * * * *'
    lease_seconds: 60                # the lease of a dead archiver ends this long after its last refresh
    sweep_after_seconds: 7200        # archive work files older than this belong to a dead archiver
  merged_retention_days: 32          # delete UI merge results older than this; 0 = keep them
periodic:
  election:
    provider: filesystem             # which worker runs each slot: the task lock, or postgres, redis, consul
```

### Downloading archives

Archives are listed and downloaded from the same place as the other
history files: **Debug → Databases tab**.
`GET /api/v1/debug/archives` lists the archives of every cluster history
folder: name, cluster, window bounds and size, all parsed from the file
name and one `stat` (no file is opened), plus `folder` and `path`.
`GET /api/v1/debug/download/{path}` downloads one
(`Content-Type: application/gzip`); `path` is relative to `shared.dir`, for
example `history/search/search-2026-10-12_00-00__2026-10-13_00-00.db.gz`.
Archives are **read-only** in the UI: they never appear as merge
candidates, and there is no delete button. `POST /api/v1/debug/merge`
accepts only `.db` paths, so an archive path passed to it by hand answers
`400`.

A downloaded archive is a plain gzip file: `gunzip` it and the result is
an ordinary merged recorder SQLite database, readable with the `sqlite3`
CLI or any tool that already reads a merged `.db`.

### Gotchas

- **A cluster with no running worker is never archived or expired.** Each
  worker works only on the history folder of its own cluster. When you
  retire a cluster, its sealed files and archives stay until you delete
  them.
- **A corrupt file sitting at an archive's own final name stalls that
  window.** If `<cluster>-<from>__<to>.db.gz` itself is unreadable (junk
  written by something outside the recorder), every pass that considers
  that window tries to decompress it, fails, logs
  `recorder_archive_failed`, counts in
  `drakkar_recorder_archive_failures_total`, and retries at the next
  slot — indefinitely, without touching the sealed sources, which stay safe
  and undeleted on disk. Recovery is manual: remove or rename the bad
  file so the next pass can publish a clean one.

## The Databases page

`GET /api/v1/debug/databases` answers in two parts:

```json
{"own": [...], "history": {"folders": [...]}}
```

- **`own`** starts with one **memory row** when the worker has an open
  [memory store](recent-events.md): `kind: "memory"`, `filename`
  `<worker>-memory` (a label, not a file), `worker_name`, `cluster_name`,
  `event_count`, `event_counts` (the count per event type, `{}` when the
  2 second read limit stops the count), `first_event_ts`, `last_event_ts`
  and `size_bytes` (the data in use, not the RAM the store holds). The UI
  shows it with the title "Recent events in memory; not a file" and a
  download button. Then come this worker's own files in `storage.dir`: the
  live file (`kind: "live"`) and the backlog files (`kind: "backlog"`). They
  have no meta file, so their statistics are measured on request. The worker keeps
  the statistics in memory and refreshes them incrementally: the `events`
  table is append-only with a monotonic `id`, so a refresh of the live file
  reads only its new rows.
- **`history.folders`** has one entry per `history/<cluster>` folder,
  sorted by name, and then one for `merged`. Each file has `path` (relative
  to `shared.dir`), `name`, `kind` (`sealed`, `archive` or `merged`),
  `size_bytes`, `modified_at`, and `meta`: the meta object of a sealed
  file, `null` for the other kinds. No sealed file is opened to build this
  list.

A memory-only worker answers the memory row only in `own`. A worker with
neither a memory store nor a file answers `own: []`. `history.folders` is
listed whenever a shared folder resolves, in both modes. The normative shape
is in `drakkar-ui/docs/api-contract-v1.md`.

**Download.** `GET /api/v1/debug/download/{path}` takes a path relative to
`shared.dir` and serves only these shapes:

- `history/<cluster>/<name>.db` — a sealed file;
- `history/<cluster>/<archive>.db.gz` — an archive;
- `merged/<name>.db` — a merge result.

Any other path, an unsafe path segment, a symlink, a path that resolves
outside `shared.dir`, or a missing file answers `404`. Own files in
`storage.dir` are not downloadable by path: download a file once it is
sealed.

**Live file and memory store.** `GET /api/v1/debug/download-live` takes no
path. It takes `source=file` or `source=memory`; the default is `file` when
a live file is open, else `memory`. With `source=file` it answers a
consistent snapshot of the serving worker's live recorder file. The worker
makes the copy with the SQLite backup API, on its own read-only connection,
so the recorder keeps writing during the copy. The copy uses a rollback
journal, so it opens on its own. It goes to an owner-only temp file in
`storage.dir`, which is removed before the answer starts. With
`source=memory` it answers a snapshot of the memory store, named
`<worker>-memory-<YYYY-MM-DD__HH_MM_SS>.db`; see
[The snapshot download](recent-events.md#the-snapshot-download). The route
answers `404` `{"error": "No live recorder file on this worker"}` for
`source=file` with no live file, `404` `{"error": "No memory store on this
worker"}` for `source=memory` with no memory store, `400` `{"error":
"source must be memory or file"}` for another value, and `500` `{"error":
"Snapshot failed: ..."}` when the copy fails. It is behind `ui.auth_token`
like the other debug routes.

**Merge.** `POST /api/v1/debug/merge` takes the same `.db` paths (sealed
files and merge results) and writes
`merged/merged-<YYYY-MM-DD__HH_MM_SS>-<16 hex>.db` inside `shared.dir`. It
writes a temporary file and renames it, so the result appears only when it
is complete. `merged/` is created with mode `0700`. The answer carries an
extra `path` field with the result's path relative to `shared.dir`.

## Schema evolution

There are **no migrations**. Tables are created with
`CREATE TABLE IF NOT EXISTS` only, and `storage.dir` is documented as
disposable. One forward-compatibility guard exists: at open, the recorder
verifies a pre-existing `events` table carries the webapp-release columns
(`origin`, `client_name`, `request_id`) and aborts startup with an
actionable message when they are missing (delete the DB, restart). The cache has no version check; its schema is
additive-only by contract.

## Worker autodiscovery

Workers find each other through the [fleet registry](fleet.md), not
through these database files. With the default `filesystem` provider each
worker writes a small JSON card to `<shared.dir>/fleet/workers/<worker>.json`
(`<storage.dir>/fleet/...` when `shared.dir` is empty) and reads the cards
of its peers every `fleet.registry.refresh_seconds`. The workers list
answers from that in-memory snapshot. Liveness comes from the card's age on
the file server's clock: a peer is `online` while its card is no older than
`fleet.registry.offline_after_seconds` (default 60). See
[Fleet registry](fleet.md) for the card format and the rules.

**Message trace** (`GET /api/v1/debug/trace`) searches this worker's own
files by default: the live file and its closed own files in `storage.dir`
(with `none` and `postgres`, the memory store).
What `history=1` reads depends on the recorder mode:

| Mode | `trace?history=1` reads |
|---|---|
| `sqlite` | the sealed files of the history folder of the worker's own cluster, newest first |
| `none` | the same sealed files, when a shared folder resolves |
| `postgres` | the rows of the worker's own cluster in the Postgres table of its prefix, never the history folder |

The search of sealed files stops after `ui.trace.history_max_files` files
(default 256) or `ui.trace.history_max_seconds` (default 10), and it opens
sealed files with `immutable=1`. The Postgres search is complete or answers
503 `{"detail": "history search failed"}`, within
`ui.trace.history_max_seconds`; see
[The history search](recorder-postgres.md#the-history-search). **Trace by label** (`GET /api/v1/debug/trace-by-label`)
searches the live file only. Every returned event carries `worker_name`. No
worker opens another worker's live file.

## Cache exchange

No worker opens another worker's cache file. Every
`fleet.cache_exchange.interval_seconds` (default 30) a worker pulls the
`cluster` and `global` rows that other workers originated, over HTTP, from
`GET /api/v1/peer/cache/changes` at the URL on each peer's
[registry card](fleet.md#the-card). This section is the wire rule. Which
peers are pulled, the paging, the failure handling and the limits are on
the [Cache](cache.md#cache-exchange) page.

**Request.**
`GET <card url>/api/v1/peer/cache/changes?scopes=cluster,global&after_ms=<ms>&after_key=<key>&limit=<n>`

| Parameter | Rule |
|---|---|
| `scopes` | `cluster`, `global` or `cluster,global`. Required. |
| `after_ms`, `after_key` | The cursor: both or neither. Absent means "from the start". `after_ms` is an integer (an optional minus and ASCII digits, signed 64-bit). |
| `limit` | An integer from 1 to 1000. Default 500. |

A repeated parameter keeps its last value. The checks run in the order of
the table, and the first one that fails answers `400` `{"error": ...}`:

| Failure | `error` |
|---|---|
| `scopes` missing, empty, unknown, or with an empty item | `scopes must be cluster, global or both` |
| only one of `after_ms`, `after_key` | `after_ms and after_key must be given together` |
| `after_ms` not an integer | `after_ms must be an integer` |
| `limit` not an integer, or outside 1..1000 | `limit must be an integer from 1 to 1000` |

**Answer.** `200` `{"rows": [...], "more": <bool>}`. Each row has `key`,
`scope`, `value` (the stored JSON text, not parsed), `size_bytes`,
`created_at_ms`, `updated_at_ms`, `expires_at_ms` (or `null`) and
`origin_worker_id`. The served rows are exactly the rows that:

- the serving worker originated (`origin_worker_id` is its own name);
- have a requested scope;
- are not expired on the serving worker's clock;
- come after the cursor: `(updated_at_ms, key) > (after_ms, after_key)`.

They are ordered by `(updated_at_ms, key)`, at most `limit` of them. `more`
is `true` when more such rows remain. The `local` scope is never served,
and no worker serves the rows of another worker. The cursor compares the
pair, not `updated_at_ms` alone, so rows that share one `updated_at_ms` are
kept apart by `key`: a page boundary inside such a group neither loses nor
repeats a row. Each scope is one search of the `idx_cache_scope_updated`
index in the cursor order, which stops at the page size, so a page costs
about its own size however many rows the cache holds.

`404` `{"error": "The cache exchange is off on this worker"}` when the
cache, `storage.dir` or `fleet.cache_exchange` is off, or the cache engine
has stopped. The parameter checks run first, so a bad request answers
`400` even on such a worker. `503`
`{"error": "The cache did not answer in time"}` when the read does not
finish within 5 seconds.

The route needs no token, even with `ui.auth_token` set: workers call it on
each other and send none. See [Security posture](security.md).

**The card mark.** A worker that serves the exchange puts
`features.cache` on its registry card: for each scope, the
`{"ms": <updated_at_ms>, "key": <key>}` of the newest row it originated.
A scope with no such row is absent, so `{"cache": {}}` means "the exchange
is on, nothing to pull yet". `features` is `{}` when the worker does not
serve. The worker reads the marks from its file at start and raises them
after every committed flush. The card carries the new marks at its next
publish (every `fleet.registry.heartbeat_seconds`, default 10). A peer
asks for a page only when a mark is past its cursor for that worker.

The **LWW conflict rule** is one shared SQL statement used by both the
local flush path and the path that applies pulled rows: incoming
`updated_at_ms` strictly greater wins; on a tie the lexicographically
**smaller** `origin_worker_id` wins; otherwise the stored row is kept. Two
workers observing the same conflict converge on the same survivor.

## Sharing folders across workers

Running a fleet against one shared folder — history, archives, merge
results and the registry — is a **supported deployment mode**, and the
reason this page is written as a specification rather than as a
description. The cache does not use the shared folder: the
[cache exchange](#cache-exchange) runs over HTTP, and each cache file stays
in its worker's own `storage.dir`. Two layouts are common:

- **One host.** The workers of one host share one `storage.dir` and leave
  `shared.dir` empty. Each file name carries the worker name, and the
  history folders are created inside `storage.dir`.
- **A fleet across hosts.** Each worker has its own `storage.dir` on a
  local disk, and every worker mounts one shared folder as `shared.dir`.
  The live file and the cache file stay on the local disk. Only sealed
  files go to the shared mount, in a background job.

What makes sharing safe:

- fixed DDL, filenames, meta file and lock body bytes, JSON encodings, and
  the canonical datetime format (this page);
- sealed files never change, so readers take no locks;
- one archiver per archive slot and cluster, chosen by the election
  provider (by default an exclusive create in `tasks/<cluster>/`, with ages
  measured on the file server's clock) and a fence before every publish.

Requirements: every worker mounts the same shared folder; the mount path
may differ, because no file stores an absolute path. The filesystem must support an exclusive create (`O_EXCL`) and a
rename inside one folder: the task lock and the move of a sealed file
into the history folder rely on both. Keep `ui.recorder.store_config: true` (the default) so every
recorder file names the worker and cluster that wrote it inside the
database; discovery, and the peers and clusters of the cache exchange,
come from the [fleet registry](fleet.md).

Every worker trusts the shared folder. Anyone who can write it can plant
files that the workers archive and serve. See
[Security posture](security.md).

## Removed files

Earlier releases wrote five more files into `storage.dir`. The worker no
longer writes them:

| File | Replaced by |
|---|---|
| `<worker>-live.db` *(symlink)* | Nothing. No worker opens another worker's live file. |
| `<worker>-cache.db` *(symlink)* | Nothing. The cache file itself now has this name; peers pull rows over HTTP. See [Cache exchange](#cache-exchange). |
| `<worker>-cache.db.actual` | `<worker>-cache.db`, a plain file. |
| `.dbstats.db` | Meta files for sealed files, and in-memory statistics for own files. See [The Databases page](#the-databases-page). |
| `.archive-<cluster>.lock` (`flock`) | `tasks/<cluster>/history.archive.lock` in `shared.dir`. See [The task lock](#the-task-lock). |

The worker does not delete them. Delete them by hand; see the
[upgrade guide](upgrading.md#8-old-files). Delete the two cache files
**before** the first start: the new cache file has the name of the old
link, and a link that is still there is followed to the old `.actual`
file.

## Watchdog file

`{storage.dir}/{worker_id}.watchdog` is written at startup and marked
`CLEAN_EXIT` on graceful shutdown; a worker that finds its own stale
watchdog without the marker reports the previous run as killed
(OOM/SIGKILL detection). Disabled (with a log line) when `storage.dir` is empty.
