# Shared Directory

This directory is mounted into all Drakkar integration test workers as `/shared`.

## Purpose

Stores per-worker SQLite files of two kinds:

1. **Flight-recorder databases** — rotating event logs (consumed / arranged /
   task lifecycle / produced / committed events). Managed by the recorder
   subsystem (`debug.db_dir`).
2. **Cache databases** — write-behind stores for the framework cache
   (`self.cache`). Managed by the cache subsystem (`cache.db_dir`, which
   defaults to `debug.db_dir` so both live side-by-side here).

Both subsystems publish a stable symlink that other workers scan for peer
discovery — the recorder's `-live.db` symlinks feed the Message Trace
fallback across cluster workers, and the cache's `-cache.db` symlinks feed
the periodic peer-sync loop that propagates cluster/global-scope entries.

## File naming

### Recorder

```
{worker_name}-{YYYY-MM-DD__HH_MM_SS}.db        # one per startup / rotation
{worker_name}-live.db                          # symlink → current file
```

A new timestamped file is created on each startup and on periodic rotation
(default every hour). The `-live.db` symlink always points at the current
writable file; on graceful shutdown the symlink is removed while the
historical file stays.

### Cache

```
{worker_name}-cache.db.actual                  # the real SQLite file
{worker_name}-cache.db                         # symlink → .actual file
```

The cache does **not** rotate files — a single `.actual` file per worker
accumulates entries and prunes on TTL expiration. The stable
`{worker_name}-cache.db` path is reserved as a symlink so peer workers can
discover it via the same glob pattern used for recorder DBs (just with a
different suffix). Cache peer-sync uses the symlink path to open a
read-only connection and pull new rows since its per-peer cursor.

Because this directory is a host mount, **both kinds of databases persist
across restarts**.

## Rotation

- **Recorder**: rotates automatically. Files older than `retention_hours`
  (default 24h) are deleted; file count is capped based on
  `retention_max_events`. A new file is created on each startup and
  periodic rotation.
- **Cache**: does not rotate. Entries are pruned inline by the cache's
  periodic cleanup loop (expired rows removed every `cleanup_interval_seconds`,
  default 60s). A full reset is a manual `rm` (see Cleanup below).

## Example contents

```
shared/
├── README.md
├── worker-1-2026-03-23__14_55_00.db               # recorder: older run
├── worker-1-2026-03-23__16_10_30.db               # recorder: current run
├── worker-1-live.db -> worker-1-2026-03-23__16_10_30.db
├── worker-1-cache.db.actual                       # cache: actual file
├── worker-1-cache.db -> worker-1-cache.db.actual  # cache: stable symlink
├── worker-2-2026-03-23__14_55_01.db
├── worker-2-live.db -> worker-2-2026-03-23__14_55_01.db
├── worker-2-cache.db.actual
├── worker-2-cache.db -> worker-2-cache.db.actual
└── ...
```

After shutdown (live symlinks removed; cache symlinks remain because the
cache subsystem removes only its own symlink on graceful stop, and the
`.actual` file is the source of truth):

```
shared/
├── README.md
├── worker-1-2026-03-23__14_55_00.db
├── worker-1-2026-03-23__16_10_30.db
├── worker-1-cache.db.actual
├── worker-2-2026-03-23__14_55_01.db
├── worker-2-cache.db.actual
└── ...
```

## Querying

### Recorder (events)

```bash
# While running — use the live symlink
sqlite3 integration/shared/worker-1-live.db "SELECT count(*) FROM events"

# After shutdown — find the latest file
ls -t integration/shared/worker-1-*.db | head -1

# Query a specific run (exclude cache files — their names contain "-cache.")
sqlite3 integration/shared/worker-1-2026-03-23__14_55_00.db ".tables"
```

### Cache (key/value entries)

```bash
# While running — follow the symlink
sqlite3 integration/shared/worker-1-cache.db "SELECT count(*) FROM cache_entries"

# Inspect scope distribution
sqlite3 integration/shared/worker-1-cache.db \
  "SELECT scope, count(*) FROM cache_entries GROUP BY scope"

# Peek the 10 most-recently-updated entries
sqlite3 integration/shared/worker-1-cache.db \
  "SELECT key, scope, length(value), origin_worker_id FROM cache_entries
   ORDER BY updated_at_ms DESC LIMIT 10"
```

The same data is also browsable in the Debug UI (`/debug` → Cache tab).

## Cleanup

```bash
# Remove everything (recorder + cache)
rm -f integration/shared/*.db integration/shared/*.db.actual

# Remove only recorder history, keep caches
rm -f integration/shared/*-[0-9]*.db integration/shared/*-live.db

# Remove only caches, keep recorder
rm -f integration/shared/*-cache.db integration/shared/*-cache.db.actual

# Remove only one worker's data of both kinds
rm -f integration/shared/worker-1-*.db integration/shared/worker-1-*.db.actual
```
