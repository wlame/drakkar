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
  is opened only by its owner.
- **`shared.dir`** is the folder every worker of a fleet sees. It holds the
  recorder history of each cluster, the archives, the merge results and the
  [registry cards](fleet.md). When `shared.dir` is empty, the worker uses
  `storage.dir` for this too.

### Own files (`storage.dir`)

| File | Purpose |
|---|---|
| `<worker>-<YYYY-MM-DD__HH_MM_SS>.db` | Flight recorder: event log + config snapshot + periodic state. The **live file**: WAL mode, opened only by its owner. Rotated. |
| `<worker>-<YYYY-MM-DD__HH_MM_SS>.db` *(older ones)* | The **backlog**: own files that are closed but not yet moved into the history folder, because a seal or a move failed, or a clean stop had no time left. See [Sealing](#sealing-and-the-history-folder). |
| `<worker>-cache.db.actual` | LWW key/value cache. Never rotated. |
| `<worker>-cache.db` → `.actual` *(symlink)* | Discovery pointer for the cache. Peers scan for it; see [Cache peer sync](#cache-peer-sync). |
| `<worker>.watchdog` | Not SQLite: OOM/SIGKILL detection marker (`CLEAN_EXIT` written on graceful stop). |

### Shared files (`shared.dir`)

| Path | Purpose |
|---|---|
| `history/<cluster>/<worker>-<YYYY-MM-DD__HH_MM_SS>.db` | A **sealed** recorder file: rollback journal, no sidecars, never changes again. |
| `history/<cluster>/<worker>-<YYYY-MM-DD__HH_MM_SS>.db.meta.json` | The [meta file](#the-meta-file) of that sealed file: its statistics, written once by the owner. |
| `history/<cluster>/<cluster>-<from>__<to>.db.gz` | Archive: gzip-compressed merge of one UTC window's sealed files. See [Archiving](#archiving). |
| `history/<cluster>/.archive.lock` | The [archive lock](#the-archive-lock) of the folder. It exists while one worker of the cluster runs an archive pass. |
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
  archive lock also writes the archives there and deletes the sealed files
  it archived.
- Timestamp format in recorder filenames is exactly
  `%Y-%m-%d__%H_%M_%S` (UTC).
- DB files and meta files are created with `0600` permissions. The worker
  creates `storage.dir` with mode `0700` when it is missing, and logs a
  warning when `storage.dir` is world-writable. A cluster history folder
  and `merged/` are created with mode `0700`.
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

The recorder and the cache are switched independently, and every
persistence flag needs a directory to write into:

| Config | Effect |
|---|---|
| `ui.enabled: false` | No UI subsystem at all — no recorder files, no watchdog, no WS. The cache still runs if enabled and `storage.dir` is set. |
| `storage.dir: ''` | **Memory-only mode.** No recorder files, no watchdog file, nothing sealed, no history, no archives. Live WebSocket streaming (the Live page) keeps working — events broadcast before the DB gate — but everything DB-backed is off: History, Trace, Task Detail lookups, the Databases tab, downloads. The workers list needs `shared.dir` (or another [registry provider](fleet.md)) to see peers. |
| `ui.recorder.store_events: false` | The `events` table stays empty and events are not buffered; the WS live view still streams. `worker_config` / `worker_state` are still written per their own flags. History, trace, and task queries return nothing. |
| `ui.recorder.store_config: false` | No `worker_config` row, so this worker's recorder files do not name it or its cluster inside the database. The meta file of each sealed file still names both. Discovery and the cache exchange do not depend on it: they use the [fleet registry](fleet.md). |
| `ui.recorder.store_state: false` | No periodic `worker_state` snapshots. |
| `cache.enabled: false` *(default)* | No cache files. The handler cache API is a no-op. |
| `cache.enabled: true` with `storage.dir: ''` | Startup warning + effective disable — the cache never runs memory-only. |

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
    throughput          TEXT,    -- three-window throughput JSON; NULL = feature off
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
- **A dedicated read-only reader connection** per live DB for UI queries
  and cache `get()` fallback, so reads never queue behind flush commits.
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
| Cache peer/cluster ephemeral reads | 5000 ms |

  The cache's own connections wait longer so a checkpoint can't fail a
  local flush; **peer** reads time out fast so a lock-wedged peer stays
  inside the per-peer failure isolation.
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

**Recorder**: events buffer in memory (capped at
`ui.recorder.max_buffer`), flushed every
`ui.recorder.flush_interval_seconds` as one multi-row transaction. On
flush failure the batch is re-queued at the front (tail evictions are
counted) and dropped after `ui.recorder.max_flush_retries` consecutive
failures. Recording-side filters (`event_min_duration_ms`,
`output_min_duration_ms`, `store_output`) gate what enters the buffer —
they affect row presence, never layout.

**Last-breath flush**: the most interesting events of a dying worker are
the unflushed last ones, so the buffer is salvaged on fatal exits that skip
the clean shutdown path. An `atexit` hook (armed at recorder start,
disarmed by a clean stop) writes the remaining buffer synchronously through
a direct SQLite connection — covering startup failures after the recorder
came up, unhandled exceptions, and stray `sys.exit` calls. It is
best-effort and logs `recorder_last_breath_flush` when it fires.
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

Memory-only mode (`storage.dir: ""`) creates no files and seals nothing.

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
| `recorder_background_job_failed` | A background job (sealing, leftover sealing, archive pass) failed with an unexpected error. Field `job` names it. The next rotation tick runs it again. |

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
math, file names and lock protocol (`drakkar/recorder/archive.py`,
`drakkar/recorder/archive_lock.py`) are part of the on-disk spec, so a
fleet sharing one `shared.dir` archives correctly no matter which worker
does the work for a given cluster.

### When the pass runs

Every worker runs the pass after each rotation tick, as a background job
(one at a time), when `history.archive.enabled` is true. A worker that
stops ends its pass after the window in progress and skips expiry and
pruning; the windows left stay due for the next pass of any worker. The
[archive lock](#the-archive-lock) lets one worker of the cluster work at a
time. The others skip the tick and lose nothing: any window still due is
picked up on a later tick.

### Defaults walkthrough

With `ui.recorder.rotation_interval_hours: 1` and `history.archive`
`enabled: true`, `window_hours: 24`, `due_after_hours: 48` and
`retention_days: 30` (all defaults):

- Every hour, rotation opens a new `<worker>-<timestamp>.db` file and seals
  the previous one into the history folder.
- Windows are UTC calendar days, `[00:00, 24:00)`. A window is **due** once
  the due delay (`due_after_hours`, 48h) has passed since it ended. Every
  file in the history folder is sealed, so there is no check for files
  still in use. With the defaults a window becomes due exactly 48h after it
  closes and is archived on the next rotation tick after that (up to 1h
  later, at the default hourly cadence) — that delay is the safety margin,
  not a bug. It also gives a late file of the window, such as a backlog file
  or a leftover, time to reach the history folder.
- Because of that delay, a single recorder file's lifespan on disk —
  counted from when it was *created*, not from when its window closed — can
  reach up to ~72h: up to 24h as part of the window it belongs to (a
  file created at the window's own start closes the window out 24h
  later), plus the ~48h due delay after the window closes before
  archiving actually runs.
- Once due, the worker that holds the archive lock merges the window's
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

### The archive lock

Every worker of the cluster tries to create
`history/<cluster>/.archive.lock` with an exclusive create, mode `0600`.
One worker wins and runs the pass; the others skip the tick. The lock body
is compact JSON with sorted keys, pinned by
`tests/fixtures/history_meta.json`:

```json
{"acquired_at":"2026-10-14T02:00:00.125000Z","instance":"9c41e0b27fd34a1e","token":"5f0c1d2e3a4b6978","worker":"search-worker-3"}
```

- **Refresh.** The holder refreshes the lock's modification time every 60
  seconds while its pass runs. At the end of the pass it deletes the lock,
  but only while the lock still holds its own token.
- **Takeover.** A lock older than `history.archive.lock_stale_seconds`
  (default 7200) belongs to a dead archiver. The next worker renames it
  aside to `.archive.lock.stale.<token>` (one rename wins), deletes it, and
  creates its own lock. Log: `recorder_archive_lock_taken_over`. When two
  workers take over at once, the slower one can move aside the fresh lock
  the faster one just created. It then sees that the lock is not stale,
  puts it back with a hard link (which never overwrites a newer lock) and
  stands down. If a third worker created a lock in that moment, the
  put-back fails (log: `recorder_archive_lock_put_back_failed`) and the
  holder of the moved lock is fenced at its next check.
- **The file server's clock.** Ages are measured on the file server's
  clock: the worker touches a marker file `.clock.<instance>` in the same
  folder and uses its modification time as "now". No host's wall clock is
  compared with another host's.
- **Fencing.** Right before it publishes an archive, and before it deletes
  any source, the holder reads the lock again. When the token is no longer
  its own — another worker took the lock over — the pass publishes nothing
  and deletes nothing. Log: `recorder_archive_fenced`.

The lock uses only an exclusive create, renames and modification times. It
does not use `flock`.

### The sweep

With the lock held, the pass first deletes work files older than
`lock_stale_seconds`, together with their `-wal`, `-shm` and `-journal`
sidecars:

| Pattern | What it is |
|---|---|
| `.*.merge.db`, `.*.tmp`, `.*.previous.db` | Work files of an archive pass that stopped. |
| `.*.incoming` | A cross-filesystem copy that stopped. |
| `.clock.*` | Age markers of the file server's clock. |
| `.archive.lock.stale.*` | Locks set aside by a takeover. |
| `<name>.db.meta.json` with no `<name>.db` | A meta file whose database is gone. A fresh one is kept: it is a move in progress, because the meta file is written before the database is renamed in. |

Each deletion logs `recorder_archive_temp_swept`.

### Merge, compress, publish, then delete

Each due window is merged with the same engine `POST /api/v1/debug/merge`
uses, gzip-compressed, and published under its final name with an atomic
rename. All of this happens on dot-prefixed work files named with the lock
token. Only after the archive is durably on disk (its directory entry
fsynced) are the sealed files it covers deleted, together with their meta
files — any failure before the rename leaves every source untouched for the
next tick to retry.

- A sealed file the merge cannot read is never deleted: it is renamed to
  `<name>.unreadable` so it stops blocking its window forever, while
  staying on disk for an operator to inspect. Its meta file stays, and the
  sweep removes it later as a meta file whose database is gone.
- An archive already present at the final name — left by an earlier pass
  that died between publishing and finishing its cleanup — is folded into
  the new one rather than overwritten: sources it already covers are
  recognized and simply deleted, never re-merged (which would duplicate
  their events).

A pass that took the lock and did not finish cleanly — a window failed, the
pass was fenced, or an unexpected error — counts in
`drakkar_recorder_archive_failures_total`. At the start of every pass,
before it tries the lock, each worker refreshes
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

`history.archive.enabled: false` turns the pass off entirely. Sealing still
runs. With the pass off, **nothing removes sealed recorder files or merge
results automatically** — not the old age/count retention (removed), not
archiving. A startup log line (`recorder_archiving_disabled`, with the
history folder in its `folder` field) says so explicitly; bounding the
history folder becomes the operator's job (delete old files by hand, or run
an external job against them).

### Renamed and removed keys

| Old key (removed) | Replaced by | Notes |
|---|---|---|
| `rotation_interval_minutes` | `rotation_interval_hours` | Renamed **and** the unit changed: `1` now means 1 hour, not 1 minute. |
| `retention_hours` | `history.archive.*` | Age-based deletion is gone; archiving replaces it. |
| `retention_max_events` | *(no replacement)* | Count-based deletion is gone entirely. |

These keys no longer exist, and no migration guard remains for them: a
config that still sets one is an unknown key inside `ui.recorder` and is
ignored. Delete them. The later move of the folder and archive keys to
`storage.dir` and `history.archive.*` does fail the load on the old names;
see the [upgrade guide](upgrading.md).

### Copy-pasteable config

```yaml
storage:
  dir: /var/lib/drakkar              # this worker's own files (local disk)
shared:
  dir: /mnt/fleet                    # history, archives and merge results; '' = storage.dir
ui:
  recorder:
    rotation_interval_hours: 1       # 1 = every hour (was rotation_interval_minutes)
history:
  archive:
    enabled: true                    # merge sealed files into windowed .db.gz archives
    window_hours: 24                 # one archive per cluster per UTC day; must be >= rotation_interval_hours
    due_after_hours: 48              # archive a window this long after it ends; must be >= rotation_interval_hours
    retention_days: 30               # delete archives older than this; 0 = keep forever; must cover due_after_hours + window_hours
    lock_stale_seconds: 7200         # an archive lock or work file older than this belongs to a dead archiver
  merged_retention_days: 32          # delete UI merge results older than this; 0 = keep them
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
  `drakkar_recorder_archive_failures_total`, and retries on the next
  tick — indefinitely, without touching the sealed sources, which stay safe
  and undeleted on disk. Recovery is manual: remove or rename the bad
  file so the next pass can publish a clean one.

## The Databases page

`GET /api/v1/debug/databases` answers in two parts:

```json
{"own": [...], "history": {"folders": [...]}}
```

- **`own`** lists this worker's own files in `storage.dir`: the live file
  (`kind: "live"`) and the backlog files (`kind: "backlog"`). They have no
  meta file, so their statistics are measured on request. The worker keeps
  the statistics in memory and refreshes them incrementally: the `events`
  table is append-only with a monotonic `id`, so a refresh of the live file
  reads only its new rows.
- **`history.folders`** has one entry per `history/<cluster>` folder,
  sorted by name, and then one for `merged`. Each file has `path` (relative
  to `shared.dir`), `name`, `kind` (`sealed`, `archive` or `merged`),
  `size_bytes`, `modified_at`, and `meta`: the meta object of a sealed
  file, `null` for the other kinds. No sealed file is opened to build this
  list.

Memory-only mode answers `{"own": [], "history": {"folders": []}}`. The
normative shape is in `drakkar-ui/docs/api-contract-v1.md`.

**Download.** `GET /api/v1/debug/download/{path}` takes a path relative to
`shared.dir` and serves only these shapes:

- `history/<cluster>/<name>.db` — a sealed file;
- `history/<cluster>/<archive>.db.gz` — an archive;
- `merged/<name>.db` — a merge result.

Any other path, an unsafe path segment, a symlink, a path that resolves
outside `shared.dir`, or a missing file answers `404`. Own files in
`storage.dir` are not downloadable: download a file once it is sealed.

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
`fleet.registry.offline_after_seconds` (default 30). See
[Fleet registry](fleet.md) for the card format and the rules.

**Message trace** (`GET /api/v1/debug/trace`) searches this worker's own
files by default: the live file and its closed own files in `storage.dir`.
With `history=1` it searches only the sealed files of the history folder
of the worker's own cluster, newest first. It stops after
`ui.trace.history_max_files` files (default 256) or
`ui.trace.history_max_seconds` (default 10), and it opens sealed files with
`immutable=1`. **Trace by label** (`GET /api/v1/debug/trace-by-label`)
searches the live file only. Every returned event carries `worker_name`. No
worker opens another worker's live file.

## Cache peer sync

Peers are discovered by scanning `storage.dir/*-cache.db` symlinks (that
is why the `.actual` indirection exists). For each peer, every `fleet.cache_exchange.interval_seconds`:

1. **Cluster resolution**: take the peer's cluster from the
   [fleet registry](fleet.md) snapshot. Same cluster (including
   both-empty) → pull scopes `('cluster','global')`; different, or a peer
   the registry does not list (or no registry at all) → `('global')` only.
   `local` never leaves its worker.
2. **Cursor pull**: `WHERE <scope> AND (expires_at_ms IS NULL OR
   expires_at_ms > now) AND updated_at_ms > cursor ORDER BY
   updated_at_ms ASC, key ASC LIMIT page_size`, then drain same-
   millisecond ties keyed on `key >` continuation.
3. **Apply through the LWW UPSERT** (below) and invalidate touched
   in-memory keys. The commit + metrics + invalidation trio is
   cancellation-atomic.
4. Per-peer failures are isolated; each cycle is bounded by
   `fleet.cache_exchange.cycle_deadline_seconds` (default `interval × 0.9`).

The **LWW conflict rule** is one shared SQL statement used by both the
local flush path and the peer-apply path: incoming `updated_at_ms`
strictly greater wins; on a tie the lexicographically **smaller**
`origin_worker_id` wins; otherwise the stored row is kept. Two workers
observing the same conflict converge on the same survivor.

## Sharing folders across workers

Running a fleet against one shared folder — history, archives, merge
results, the registry and the cache exchange — is a **supported deployment
mode**, and the reason this page is written as a specification rather than
as a description. Two layouts are common:

- **One host.** The workers of one host share one `storage.dir` and leave
  `shared.dir` empty. Each file name carries the worker name, and the
  history folders are created inside `storage.dir`.
- **A fleet across hosts.** Each worker has its own `storage.dir` on a
  local disk, and every worker mounts one shared folder as `shared.dir`.
  The live file stays on the local disk. Only sealed files go to the shared
  mount, in a background job.

The cache exchange finds its peers' cache files in `storage.dir` (see
[Cache peer sync](#cache-peer-sync)), so it needs workers that share
`storage.dir`.

What makes sharing safe:

- fixed DDL, filenames, meta file and lock body bytes, JSON encodings, and
  the canonical datetime format (this page);
- sealed files never change, so readers take no locks;
- one archiver per cluster folder, chosen by an exclusive create, with ages
  measured on the file server's clock and a fence before every publish;
- per-peer failure isolation in the cache exchange, so one lock-wedged
  worker cannot stall the others.

Requirements: every worker mounts the same shared folder; the mount path
may differ, because no file stores an absolute path. The filesystem must support an exclusive create (`O_EXCL`) and a
rename inside one folder: the archive lock and the move of a sealed file
into the history folder rely on both. Keep `ui.recorder.store_config: true` (the default) so every
recorder file names the worker and cluster that wrote it inside the
database; discovery and the cache exchange's cluster rule come from the
[fleet registry](fleet.md).

Every worker trusts the shared folder. Anyone who can write it can plant
files that the workers archive and serve. See
[Security posture](security.md).

## Removed files

Earlier releases wrote three more files into `storage.dir`. The worker no
longer writes or reads them:

| File | Replaced by |
|---|---|
| `<worker>-live.db` *(symlink)* | Nothing. No worker opens another worker's live file. |
| `.dbstats.db` | Meta files for sealed files, and in-memory statistics for own files. See [The Databases page](#the-databases-page). |
| `.archive-<cluster>.lock` (`flock`) | `history/<cluster>/.archive.lock`. See [The archive lock](#the-archive-lock). |

The worker does not delete them. Delete them by hand after the upgrade; see
the [upgrade guide](upgrading.md#8-old-files).

## Watchdog file

`{storage.dir}/{worker_id}.watchdog` is written at startup and marked
`CLEAN_EXIT` on graceful shutdown; a worker that finds its own stale
watchdog without the marker reports the previous run as killed
(OOM/SIGKILL detection). Disabled (with a log line) when `storage.dir` is empty.
