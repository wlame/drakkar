# Shared Directory

This directory is mounted into all Drakkar integration test workers as `/shared`.

## Purpose

Every worker's `storage.dir` is `/shared`, and `shared.dir` is empty, so the
shared folder is `/shared` too. The directory holds:

1. **Flight-recorder databases** — rotating event logs (consumed / arranged /
   task lifecycle / produced / committed events). The live file of each
   worker sits at the top level. When a file rotates (or a worker stops
   cleanly), the worker seals it and moves it into
   `history/<cluster>/`, next to a `.db.meta.json` file with its statistics.
2. **Archives** — one `<cluster>-<from>__<to>.db.gz` per cluster and UTC day,
   in the same `history/<cluster>/` folder.
3. **Merge results** — files merged from the Debug UI, in `merged/`.
4. **Cache databases** — write-behind stores for the framework cache
   (`self.cache`). The cache file lives in `storage.dir`, so it sits at the
   top level too.
5. **Registry cards** — one JSON file per worker in `fleet/workers/`, used
   for the workers list.

Only the cache publishes a symlink. Peers scan for the `-cache.db` symlinks
to find each other's cache files for the periodic peer-sync loop that
propagates cluster/global-scope entries. No worker opens another worker's
live recorder file: the Message Trace reads the sealed files in the history
folder.

`<cluster>` is the cluster name with every character outside
`[a-zA-Z0-9_-]` replaced by `_`. The harness clusters are `main cluster`
(worker-1, worker-2, worker-3), `fast cluster` (fast-worker-1,
fast-worker-2) and `http cluster` (http-worker), so the folders are
`history/main_cluster/`, `history/fast_cluster/` and `history/http_cluster/`.

## File naming

### Recorder

```
{worker_name}-{YYYY-MM-DD__HH_MM_SS}.db                          # live file (top level)
history/{cluster}/{worker_name}-{YYYY-MM-DD__HH_MM_SS}.db        # sealed file
history/{cluster}/{worker_name}-{YYYY-MM-DD__HH_MM_SS}.db.meta.json
history/{cluster}/{cluster}-{from}__{to}.db.gz                   # archive
merged/merged-{YYYY-MM-DD__HH_MM_SS}-{16 hex}.db                 # UI merge result
```

A new timestamped file is created on each startup and on periodic rotation
(default every hour). The live file is in WAL mode, so it has `-wal` and
`-shm` sidecars while the worker runs. A sealed file has no sidecars and
never changes again.

### Cache

```
{worker_name}-cache.db.actual                  # the real SQLite file
{worker_name}-cache.db                         # symlink → .actual file
```

The cache does **not** rotate files — a single `.actual` file per worker
accumulates entries and prunes on TTL expiration. The stable
`{worker_name}-cache.db` path is reserved as a symlink so peer workers can
discover it with a glob. Cache peer-sync uses the symlink path to open a
read-only connection and pull new rows since its per-peer cursor.

Because this directory is a host mount, **all of these files persist
across restarts**.

## Rotation

- **Recorder**: rotates automatically. Files rotate hourly and are sealed
  into `history/<cluster>/`. Once a 24h window's data is old enough (48
  hours after the window closes, by default), one worker of the cluster
  merges it into a compressed `<cluster>-<from>__<to>.db.gz` archive and
  removes the sealed files it merged. Archives appear in the Debug →
  Databases tab for download.
- **Cache**: does not rotate. Entries are pruned inline by the cache's
  periodic cleanup loop (expired rows removed every `cleanup_interval_seconds`,
  default 60s). A full reset is a manual `rm` (see Cleanup below).

## Example contents

```
shared/
├── README.md
├── worker-1-2026-10-14__09_00_00.db                 # recorder: live file
├── worker-1-2026-10-14__09_00_00.db-wal
├── worker-1-2026-10-14__09_00_00.db-shm
├── worker-1-cache.db.actual                         # cache: actual file
├── worker-1-cache.db -> worker-1-cache.db.actual    # cache: stable symlink
├── worker-1.watchdog
├── worker-2-2026-10-14__09_00_01.db
├── ...
├── history/
│   ├── main_cluster/
│   │   ├── worker-1-2026-10-14__08_00_00.db         # sealed
│   │   ├── worker-1-2026-10-14__08_00_00.db.meta.json
│   │   ├── worker-2-2026-10-14__08_00_01.db
│   │   ├── worker-2-2026-10-14__08_00_01.db.meta.json
│   │   └── main_cluster-2026-10-11_00-00__2026-10-12_00-00.db.gz
│   ├── fast_cluster/
│   └── http_cluster/
├── merged/
│   └── merged-2026-10-14__09_30_12-5f0c1d2e3a4b6978.db
└── fleet/
    └── workers/
        ├── worker-1.json
        └── ...
```

After a clean shutdown, each worker's last live file is sealed and moved
into `history/<cluster>/` (when the shutdown had time left for the move).
The cache files stay at the top level.

## Querying

### Recorder (events)

```bash
# The live file of one worker (the newest top-level file with its name)
ls -t integration/shared/worker-1-[0-9]*.db | head -1

# Sealed files of a cluster, and the statistics of one without opening it
ls integration/shared/history/main_cluster/
cat integration/shared/history/main_cluster/worker-1-2026-10-14__08_00_00.db.meta.json

# Query a sealed file read-only
sqlite3 "file:integration/shared/history/main_cluster/worker-1-2026-10-14__08_00_00.db?immutable=1" \
  "SELECT event, count(*) FROM events GROUP BY event"
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

Stop the workers first.

```bash
# Remove everything (recorder + history + merge results + cache + cards)
rm -rf integration/shared/history integration/shared/merged integration/shared/fleet
rm -f integration/shared/*.db integration/shared/*.db-wal integration/shared/*.db-shm \
  integration/shared/*.db.actual* integration/shared/*.watchdog

# Remove only recorder history, keep caches
# (the "__" of the timestamp keeps the glob off the "-cache.db" names)
rm -rf integration/shared/history integration/shared/merged
rm -f integration/shared/*__*.db integration/shared/*__*.db-wal integration/shared/*__*.db-shm

# Remove only caches, keep recorder
rm -f integration/shared/*-cache.db integration/shared/*-cache.db.actual*

# Remove only one worker's own files of both kinds (its sealed files stay in history/)
rm -f integration/shared/worker-1-*.db integration/shared/worker-1-*.db-wal \
  integration/shared/worker-1-*.db-shm integration/shared/worker-1-*.db.actual*
```
