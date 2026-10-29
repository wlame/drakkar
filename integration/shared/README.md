# Shared Directory

This directory is mounted into all Drakkar integration test workers as `/shared`.

## Purpose

Every worker's `storage.dir` and `storage.shared_dir` are both `/shared`. The
directory holds:

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
   top level too. It is private to its worker even here: the harness
   shares the folder only because it is also the fleet's shared folder.
5. **Registry cards** — one JSON file per worker in `fleet/workers/`, used
   for the workers list and the cache exchange.
6. **Cluster task locks** — with the `filesystem` election, one
   `<task>.lock` file per cluster task in `tasks/<cluster>/`, held by the
   worker that runs the current slot of that task: `history.archive.lock`
   (the archive) and `harness_cluster_tick.lock` (the harness handler's
   cluster tick). The folder also holds the `.clock.<instance>` markers the
   lock uses to read the file server's clock. The http-worker inherits the
   tick from the main handler and runs it alone in `tasks/http_cluster/`.

No worker opens another worker's files at the top level. The cache
exchange propagates cluster/global-scope entries over HTTP: every
`cache.exchange.interval_seconds` each worker reads the registry
cards and pulls new rows from the other workers with
`GET /api/v1/peer/cache/changes` at the URL on their card. The Message
Trace reads the sealed files in the history folder.

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
{worker_name}-cache.db                         # the SQLite file (WAL: -wal, -shm sidecars)
```

The cache does **not** rotate files — a single file per worker
accumulates entries and prunes on TTL expiration. Only its owner opens it.
Other workers get its cluster/global rows over HTTP, page by page after a
per-peer cursor.

Because this directory is a host mount, **all of these files persist
across restarts**.

## Rotation

- **Recorder**: rotates automatically. Files rotate hourly and are sealed
  into `history/<cluster>/`. In the harness, windows are one hour long and
  due one hour after they close (`history.archive` in
  `worker/drakkar.yaml`; the defaults are 24 hours and 48 hours). The
  archive is a cluster task with a slot every 30 seconds
  (`recorder.archive.interval`): at each slot one of worker-2 and worker-3
  takes the lease and looks for a due window. worker-1 turns the archive off
  (`DK_HISTORY__ARCHIVE__ENABLED` in `docker-compose.yml`). The lease of a
  holder that died ends 40 seconds after its last refresh
  (`recorder.archive.lease`). When a window is due, one worker of
  the cluster merges it into a compressed `<cluster>-<from>__<to>.db.gz` archive and
  removes the sealed files it merged. Archives appear in the Debug →
  Databases tab for download.
- **Cache**: does not rotate. Entries are pruned inline by the cache's
  periodic cleanup loop (expired rows removed every `cache.cleanup_interval`,
  default 60s). A full reset is a manual `rm` (see Cleanup below).

## Example contents

```
shared/
├── README.md
├── worker-1-2026-10-14__09_00_00.db                 # recorder: live file
├── worker-1-2026-10-14__09_00_00.db-wal
├── worker-1-2026-10-14__09_00_00.db-shm
├── worker-1-cache.db                                # cache file
├── worker-1-cache.db-wal
├── worker-1-cache.db-shm
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
├── fleet/
│   └── workers/
│       ├── worker-1.json
│       └── ...
└── tasks/                                           # filesystem election only
    ├── main_cluster/
    │   ├── history.archive.lock                     # while a slot holds it
    │   ├── harness_cluster_tick.lock
    │   └── .clock.5f0c1d2e3a4b6978
    └── http_cluster/
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
# Count the rows
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
# Remove everything (recorder + history + merge results + cache + cards + task locks)
rm -rf integration/shared/history integration/shared/merged integration/shared/fleet \
  integration/shared/tasks
rm -f integration/shared/*.db integration/shared/*.db-wal integration/shared/*.db-shm \
  integration/shared/*.watchdog

# Remove only recorder history, keep caches
# (the "__" of the timestamp keeps the glob off the "-cache.db" names)
rm -rf integration/shared/history integration/shared/merged
rm -f integration/shared/*__*.db integration/shared/*__*.db-wal integration/shared/*__*.db-shm

# Remove only caches, keep recorder
rm -f integration/shared/*-cache.db integration/shared/*-cache.db-wal integration/shared/*-cache.db-shm

# Remove only one worker's own files of both kinds (its sealed files stay in history/)
rm -f integration/shared/worker-1-*.db integration/shared/worker-1-*.db-wal \
  integration/shared/worker-1-*.db-shm

# Files an older release left: the .actual cache files
rm -f integration/shared/*-cache.db.actual*
```

An older release made `{worker_name}-cache.db` a link to a
`{worker_name}-cache.db.actual` file. Delete both before the workers start
(the "Remove only caches" line removes the link): a link that is still
there is followed to the old file.
