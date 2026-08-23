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
| `<cluster>-<from>__<to>.db.gz` | Archive: gzip-compressed merge of one UTC window's rotated-out recorder files for one cluster. Deleting a raw file's source is the only thing an archive pass does to `db_dir` besides creating this. | `ui.recorder.db_dir` |
| `<worker>-cache.db.actual` | LWW key/value cache. Never rotated. | `cache.db_dir`, falling back to `ui.recorder.db_dir` |
| `<worker>-cache.db` → `.actual` *(symlink)* | Discovery pointer for the cache, deliberately shaped like the recorder's so one symlink-scan routine serves both. | same |
| `<worker>.watchdog` | Not SQLite: OOM/SIGKILL detection marker (`CLEAN_EXIT` written on graceful stop). | `ui.recorder.db_dir` |
| `.dbstats.db` | Shared stats cache behind the Databases page — derived data only, disposable, dot-prefixed so it never lists as a database itself. See [The databases-page stats cache](#the-databases-page-stats-cache). | `ui.recorder.db_dir` |

See [Archiving](#archiving) below for the full mechanics of the `.db.gz` file.

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

## Modes: which files exist when

The recorder and the cache are switched independently, and every
persistence flag needs a directory to write into. Identical behavior on
both backends:

| Config | Effect |
|---|---|
| `ui.enabled: false` | No UI subsystem at all — no recorder files, no watchdog, no WS. The cache still runs if enabled and it can resolve a `db_dir`. |
| `ui.recorder.db_dir: ''` | **Memory-only mode.** No recorder files, no watchdog file, no archives. Live WebSocket streaming (the Live page) keeps working — events broadcast before the DB gate — but everything DB-backed is off: History, Trace, Task Detail lookups, peer autodiscovery, the Databases tab, downloads. |
| `ui.recorder.store_events: false` | The `events` table stays empty and events are not buffered; the WS live view still streams. `worker_config` / `worker_state` are still written per their own flags. History, trace, and task queries return nothing. |
| `ui.recorder.store_config: false` | No `worker_config` row — this worker becomes invisible to peer autodiscovery and to cluster-scoped cache sync. |
| `ui.recorder.store_state: false` | No periodic `worker_state` snapshots. |
| `cache.enabled: false` *(default)* | No cache files. The handler cache API is a no-op. |
| `cache.enabled: true` with no `db_dir` anywhere | Startup warning + effective disable — the cache never runs memory-only. |

Any combination is valid: for example `store_config: true` with
`store_events: false` keeps a worker discoverable while writing no event
history.

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
  archive. Both backends emit it; on Go the stacks are goroutine-dump
  groups.
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
- **`synchronous=NORMAL` on every WAL writer**, in both backends — the
  recorder (including the connection rotation opens), the handler cache and
  the `.dbstats` cache.

  SQLite's default is `FULL`, which fsyncs the WAL on **every commit**.
  These stores commit often — the recorder on its flush interval, on each
  state sync and on any UI poll that forces a flush; the cache on its own
  flush interval — and a `db_dir` is routinely a network mount, where one
  fsync costs tens of milliseconds and reads on the same connection queue
  behind it. `NORMAL` is the level SQLite's own documentation recommends
  under WAL: the WAL is synced at checkpoints instead.

  What you give up: `NORMAL` remains **fully safe across an application
  crash** — the process being killed, an OOM, a failed deploy — because the
  WAL is still written and replayed on the next open. It can lose the most
  recently committed transactions only when the **host** dies mid-write: a
  power cut or a kernel panic. For a flight recorder, a derived stats cache
  and a last-writer-wins cache, none of which is a system of record, that is
  the correct trade. If your deployment needs stricter durability from these
  files, they are the wrong place to keep that data.

  Note that `synchronous` is per-**connection** and, unlike `journal_mode`,
  is not stored in the database header. Python sets it on each writer
  connection; Go sets it as a DSN pragma, because `database/sql` may reopen
  a pooled connection and it would otherwise revert to the default.
- No explicit WAL checkpoints, no `VACUUM`, driver-default `foreign_keys` —
  on both backends.

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
the unflushed last ones, so both backends salvage the buffer on fatal
exits that skip the clean shutdown path. The Python backend registers an
`atexit` hook (armed at recorder start, disarmed by a clean stop) that
writes the remaining buffer synchronously through a direct SQLite
connection — covering startup failures after the recorder came up,
unhandled exceptions, and stray `sys.exit` calls. The Go backend flushes
the recorder from a panic guard on every pipeline goroutine that runs
user hook code, then re-panics so the crash keeps its original stack.
Both are best-effort and log `recorder_last_breath_flush` when they fire.
Neither can help against SIGKILL / the OOM killer — the watchdog file
covers *detecting* those, and the last periodic flush is what remains.

**Rotation** (every `ui.recorder.rotation_interval_hours`): flush →
create + initialize the new timestamped DB (WAL, busy_timeout, schema,
reader) → atomic swap → rewrite `worker_config` → repoint the live
symlink → close the old DB. Rotation itself deletes nothing — the
[archive pass](#archiving) is the only thing that ever removes a raw
recorder file.

**Cache**: dirty entries flush every `cache.flush_interval_seconds`
(atomic dirty-map swap, single transaction, restore-on-failure); expired
rows are deleted every `cache.cleanup_interval_seconds`. The cache file
never rotates.

## Archiving

Age-based and count-based retention (the old `retention_hours` /
`retention_max_events` keys) are gone. In their place, a periodic
**archive pass** folds each finished UTC time window's rotated-out raw
files into one compressed, merged database per cluster, and deletes only
the raw files it successfully merged. Both backends run this pass with
identical window math, file names, and lock protocol — Python
`drakkar/recorder/archive.py`, Go `internal/recorder/archive.go` — so a
mixed fleet sharing one `db_dir` archives correctly no matter which
backend does the work for a given cluster.

### Defaults walkthrough

With `rotation_interval_hours: 1`, `archive_enabled: true`,
`archive_window_hours: 24` and `archive_retention_days: 0` (all
defaults):

- Every hour, rotation opens a new raw `<worker>-<timestamp>.db` file.
- Windows are UTC calendar days, `[00:00, 24:00)`. A window is **due**
  only once (a) a full extra window (24h) has passed since it ended, and
  (b) every file assigned to it has gone untouched for at least one
  rotation interval — belt-and-braces against a writer that is still
  holding an old file open. With the defaults this means a window
  becomes due exactly 24h after it closes and is archived on the next
  rotation tick after that (up to 1h later, at the default hourly
  cadence) — that delay is the safety margin, not a bug.
- Because of that delay, a single raw file's lifespan on disk — counted
  from when it was *created*, not from when its window closed — can
  reach up to ~48h: up to 24h as part of the window it belongs to (a
  file created at the window's own start closes the window out 24h
  later), plus the ~24h due-delay after the window closes before
  archiving actually runs.
- Once due, whichever worker wins that tick's lock election merges the
  window's raw files into one `<cluster>-<from>__<to>.db.gz` and deletes
  the raw files it merged.
- `archive_retention_days: 0` keeps every archive forever — nothing ever
  deletes a `.db.gz` file.

Steady state: up to twice a window's worth of raw `.db` files per worker
at any time — the window currently being written plus the most recently
closed one, until the window before those finishes archiving — and one
`<cluster>-<from>__<to>.db.gz` per cluster per window reaching back as
far as retention allows — forever, by default.

### Windows are keyed by file start time, not event time

A window is `[k·W, (k+1)·W)` in UTC epoch seconds, width
`archive_window_hours`. A raw file belongs to the window holding its own
**start** timestamp (parsed from the filename) and is never split across
two windows. Because a file keeps recording events until it rotates, an
archive can therefore carry a handful of events timestamped slightly past
its own window end — archives partition by when a file was *opened*, not
by when each event inside it happened.

### Live databases are never archive candidates

Each running worker keeps a `<worker>-live.db` symlink pointing at the file
it is writing right now. An archive pass excludes every one of those
targets — its own and every peer's — so a worker can never merge away a
database another worker still holds open. This matters when workers in one
`db_dir` do not share a rotation setting: without the rule, a worker on
`rotation_interval_hours: 1` would judge a worker on `24` by its own
schedule and delete a file still in use.

Settledness is judged from both the database file and its `-wal` sidecar.
Under WAL the main file's modification time only moves on checkpoint, so a
continuously written database can look untouched for hours; the sidecar
moves on every write.

### Lock election in a shared `db_dir`

Workers sharing one `db_dir` each attempt the archive pass on every
rotation tick, but only one worker per **cluster** actually runs it: a
`flock`-based lock file (`.archive-<cluster>.lock`) elects the winner.
Losers skip the tick and lose nothing — any window still due is picked up
on the next tick. Ownership is decided by holding the OS lock, not by the
lock file's existence, so a crashed worker never leaves a stale election
behind; the kernel drops the lock the moment the holder's process dies.
Election relies on `flock` semantics being honored by the filesystem — on
a network filesystem (NFS/EFS) shared across hosts, verify the mount
supports `flock` before pointing multiple hosts' workers at one `db_dir`.

### Merge, compress, publish, then delete

Each due window is merged with the same engine `POST /api/debug/merge`
uses, gzip-compressed, and published under its final name with an atomic
rename. Only after the archive is durably on disk (its directory entry
fsynced) are the raw files it covers deleted — any failure before the
rename leaves every raw source untouched for the next tick to retry.

- A raw file the merge cannot read (corrupt, still mid-write, whatever)
  is never deleted: it is renamed to `<name>.unreadable` (its `-wal` /
  `-shm` sidecars move with it) so it stops blocking its window forever,
  while staying on disk for an operator to inspect.
- An archive already present at the final name — left by an earlier pass
  that died between publishing and finishing its cleanup — is folded into
  the new one rather than overwritten: sources it already covers are
  recognized and simply deleted, never re-merged (which would duplicate
  their events).

### Opting out

`ui.recorder.archive_enabled: false` turns the pass off entirely. With it
off, **nothing removes raw recorder files automatically** — not the old
age/count retention (removed), not archiving. A startup log line
(`recorder_archiving_disabled`) says so explicitly; bounding `db_dir`
becomes the operator's job (delete old files by hand, or run an external
job against them).

### Renamed and removed keys

| Old key (removed) | Replaced by | Notes |
|---|---|---|
| `rotation_interval_minutes` | `rotation_interval_hours` | Renamed **and** the unit changed: `1` now means 1 hour, not 1 minute. A config still setting the old key fails to load, naming the replacement. |
| `retention_hours` | `archive_enabled`, `archive_window_hours`, `archive_retention_days` | Age-based deletion is gone; archiving replaces it. A config still setting the old key fails to load, naming the replacement fields. |
| `retention_max_events` | *(no replacement)* | Count-based deletion is gone entirely. A config still setting the old key fails to load. |

The two backends catch a removed key differently: Python rejects the
key's mere *presence*, any value included, while Go's catcher fires only
on a *non-zero* value — a contrived `retention_hours: 0` loads silently
on Go instead of failing. This is not a real gap: the old schema required
`>= 1` on both backends, so no config that ever validated could have
carried a zero there.

### Copy-pasteable config

```yaml
ui:
  recorder:
    db_dir: /shared/drakkar-recorder
    rotation_interval_hours: 1       # 1 = every hour (was rotation_interval_minutes)
    archive_enabled: true            # merge rotated-out files into windowed .db.gz archives
    archive_window_hours: 24         # one archive per cluster per UTC day; must be >= rotation_interval_hours
    archive_retention_days: 0        # 0 = keep archives forever; else must be >= 2 x the window, in days
```

### Downloading archives

Archives are listed and downloaded from the same place as raw databases:
**Debug → Databases tab → Archives**, backed by
`GET /api/v1/debug/archives` (name, cluster, window bounds, size — all
parsed from the file name, no file is opened) and
`GET /api/v1/debug/archives/{name}` (download,
`Content-Type: application/gzip`). Archives are **read-only** in the UI:
they never appear as merge candidates, and there is no delete button.
`POST /api/debug/merge` does not specifically reject an archive name
passed to it by hand — filename hardening only rejects path traversal and
unsafe characters — but the merge engine cannot open gzip bytes as
SQLite, so it silently drops that source from the result, the same
"a source that cannot be opened or read is skipped, not fatal" behavior
it already has for any bad input.

A downloaded archive is a plain gzip file: `gunzip` it and the result is
an ordinary merged recorder SQLite database, readable with the `sqlite3`
CLI or any tool that already reads a merged `.db`.

### Gotchas

- **Retention only runs when a new window becomes due.** An idle or
  retired cluster that stops producing raw files never triggers another
  archive pass, so its existing archives are never re-evaluated against
  `archive_retention_days` — they are kept forever, regardless of the
  setting, until some other window in that cluster becomes due again.
- **A corrupt file sitting at an archive's own final name stalls that
  window.** If `<cluster>-<from>__<to>.db.gz` itself is unreadable (junk
  written by something outside the recorder, or a previous pass that died
  mid-write), every pass that considers that window tries to decompress
  it, fails, logs `recorder_archive_failed`, and retries on the next
  tick — indefinitely, without touching the raw sources, which stay safe
  and undeleted on disk. Recovery is manual: remove or rename the bad
  file so the next pass can publish a clean one.

## The databases-page stats cache

`GET /api/debug/databases` used to open every `.db` file and aggregate its
whole events table per page load. It is now backed by
`<db_dir>/.dbstats.db` (table `db_stats_v1` — identical schema on both
backends; a mixed fleet shares the file):

- **The directory stays the source of truth.** The file *list* is a live
  scan on every request — a file you delete from `db_dir` by hand
  disappears from the page immediately, and the cache only supplies
  derived statistics, keyed by `(path, mtime_ns, size_bytes)`. Rotated
  files are immutable, so their key never changes and they are scanned
  exactly once, ever.
- **Three feeders**: rotation precomputes the just-closed file's stats;
  a warmer loop (`ui.recorder.dbstats_warm_interval_seconds`, default 60)
  sweeps for anything missing and purges rows whose files are gone; a
  page request itself scans at most
  `ui.recorder.dbstats_inline_scan_limit` (default 4) cold files inline —
  the rest render as `stats_pending` rows and fill in as the warmer
  catches up (the UI re-polls automatically).
- **Live DBs are delta-scanned**: the events table is append-only with a
  monotonic `id`, so refreshing the file a worker is writing costs only
  its new rows, not a full `GROUP BY`.
- **Each worker warms only its own live DB.** A live file changes
  constantly, so a sweep that refreshed every live file made each worker
  re-read every co-located worker's growing database once a minute —
  N&nbsp;&times;&nbsp;N reads across the fleet, over a directory that is
  usually network-mounted, for a page nobody may have open. Because the
  cache is shared, every worker refreshing its own file keeps the whole
  page warm. A peer's row can therefore lag by up to that peer's warm
  interval in the background; **opening the page still refreshes every
  row on demand**, so what you look at is current.
- **Disposable and self-healing**: delete `.dbstats.db` and everything
  rescans; a corrupt file is discarded and rebuilt automatically. Writes
  are idempotent, so concurrent workers share it with WAL + busy-timeout
  and no coordination.

The page also marks **in-use files** (`live_for` — resolved from the
`*-live.db` / `*-cache.db` symlinks; note a stale symlink after a
SIGKILL marks its file in-use until the worker restarts) and now lists
**cache databases** as typed rows (`kind: cache`, entry count, shown
under the stable symlink name).

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
resolves. The symlink is removed on graceful shutdown; after a crash or
an OOM kill the worker stays visible until archiving removes the file it
points at (or, with `archive_enabled: false`, indefinitely — see
[Archiving](#archiving)). Listed is not the same as alive: each entry
carries `last_seen_ts` (the newest `worker_state.updated_at` heartbeat,
written every `ui.recorder.state_sync_interval_seconds`; falls back to
`MAX(events.ts)` when the peer runs `store_state: false`; `null` when
neither exists) and `online` — `true` iff that heartbeat is no older
than `ui.workers_offline_after_seconds` (default 30). A crashed worker
therefore stays listed — its history remains reachable — but flips to
offline once its heartbeat goes stale. The worker answering the request
always reports itself online. Size the threshold to at least 2–3x the
largest `state_sync_interval_seconds` in the fleet so healthy workers
never flap offline between heartbeats.

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
