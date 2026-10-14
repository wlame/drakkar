# Upgrading: storage, history and fleet settings

This release moves the settings for files, archives, worker discovery and
cache exchange into four new config sections: `storage`, `shared`,
`history` and `fleet`. Some old keys moved, a few were removed. The old
keys are not read any more: a config that still uses one **fails to load**
with a message that lists every old key it found.

Follow the steps below in order. Plan the upgrade **with all workers
stopped**: a worker of this release does not read the files an older
release wrote, and an older and a newer worker cannot run side by side.

## 1. List the keys you use today

Search your YAML files **and** your environment (Kubernetes manifests,
`.env` files, CI variables) for these keys. As environment variables they
are `DK_` plus the YAML path with `__` between the levels.

| YAML path | Environment variable |
|---|---|
| `ui.recorder.db_dir` | `DK_UI__RECORDER__DB_DIR` |
| `ui.recorder.archive_enabled` | `DK_UI__RECORDER__ARCHIVE_ENABLED` |
| `ui.recorder.archive_window_hours` | `DK_UI__RECORDER__ARCHIVE_WINDOW_HOURS` |
| `ui.recorder.archive_retention_days` | `DK_UI__RECORDER__ARCHIVE_RETENTION_DAYS` |
| `ui.recorder.dbstats_warm_interval_seconds` | `DK_UI__RECORDER__DBSTATS_WARM_INTERVAL_SECONDS` |
| `ui.recorder.dbstats_inline_scan_limit` | `DK_UI__RECORDER__DBSTATS_INLINE_SCAN_LIMIT` |
| `ui.workers_offline_after_seconds` | `DK_UI__WORKERS_OFFLINE_AFTER_SECONDS` |
| `cache.db_dir` | `DK_CACHE__DB_DIR` |
| `cache.peer_sync.enabled` | `DK_CACHE__PEER_SYNC__ENABLED` |
| `cache.peer_sync.interval_seconds` | `DK_CACHE__PEER_SYNC__INTERVAL_SECONDS` |
| `cache.peer_sync.batch_size` | `DK_CACHE__PEER_SYNC__BATCH_SIZE` |
| `cache.peer_sync.timeout_seconds` | `DK_CACHE__PEER_SYNC__TIMEOUT_SECONDS` |
| `cache.peer_sync.cycle_deadline_seconds` | `DK_CACHE__PEER_SYNC__CYCLE_DEADLINE_SECONDS` |

## 2. Choose a layout

- **One host.** All workers run on one machine (or you run one worker).
  Set only `storage.dir`. Workers of one host can share it: each file name
  carries the worker name.
- **A fleet across hosts.** Give every worker its own `storage.dir` on a
  local disk (for example a per-pod volume), and mount one shared folder
  on every worker as `shared.dir` (for example a `ReadWriteMany` volume).
  Each worker writes its own files only to `storage.dir`. The shared folder
  holds the worker registry, the history, the archives and the merge
  results.

`shared.dir` empty means "the same folder as `storage.dir`".

## 3. Rename the moved keys

| Old YAML path | New YAML path | Old environment variable | New environment variable |
|---|---|---|---|
| `ui.recorder.db_dir` | `storage.dir` (and `shared.dir` for a fleet across hosts) | `DK_UI__RECORDER__DB_DIR` | `DK_STORAGE__DIR` (`DK_SHARED__DIR`) |
| `ui.recorder.archive_enabled` | `history.archive.enabled` | `DK_UI__RECORDER__ARCHIVE_ENABLED` | `DK_HISTORY__ARCHIVE__ENABLED` |
| `ui.recorder.archive_window_hours` | `history.archive.window_hours` | `DK_UI__RECORDER__ARCHIVE_WINDOW_HOURS` | `DK_HISTORY__ARCHIVE__WINDOW_HOURS` |
| `ui.recorder.archive_retention_days` | `history.archive.retention_days` | `DK_UI__RECORDER__ARCHIVE_RETENTION_DAYS` | `DK_HISTORY__ARCHIVE__RETENTION_DAYS` |
| `ui.workers_offline_after_seconds` | `fleet.registry.offline_after_seconds` | `DK_UI__WORKERS_OFFLINE_AFTER_SECONDS` | `DK_FLEET__REGISTRY__OFFLINE_AFTER_SECONDS` |
| `cache.peer_sync.enabled` | `fleet.cache_exchange.enabled` | `DK_CACHE__PEER_SYNC__ENABLED` | `DK_FLEET__CACHE_EXCHANGE__ENABLED` |
| `cache.peer_sync.interval_seconds` | `fleet.cache_exchange.interval_seconds` | `DK_CACHE__PEER_SYNC__INTERVAL_SECONDS` | `DK_FLEET__CACHE_EXCHANGE__INTERVAL_SECONDS` |
| `cache.peer_sync.batch_size` | `fleet.cache_exchange.page_size` | `DK_CACHE__PEER_SYNC__BATCH_SIZE` | `DK_FLEET__CACHE_EXCHANGE__PAGE_SIZE` |
| `cache.peer_sync.timeout_seconds` | `fleet.cache_exchange.timeout_seconds` | `DK_CACHE__PEER_SYNC__TIMEOUT_SECONDS` | `DK_FLEET__CACHE_EXCHANGE__TIMEOUT_SECONDS` |
| `cache.peer_sync.cycle_deadline_seconds` | `fleet.cache_exchange.cycle_deadline_seconds` | `DK_CACHE__PEER_SYNC__CYCLE_DEADLINE_SECONDS` | `DK_FLEET__CACHE_EXCHANGE__CYCLE_DEADLINE_SECONDS` |

Two rules changed together with the keys:

- `history.archive.retention_days` must now cover the due delay plus one
  window: `retention_days * 24 >= due_after_hours + window_hours` (or be
  `0`). The default 30 days covers the defaults (48 + 24 hours).
- `page_size` accepts 1 to 1000.

## 4. Delete the removed keys

| Removed key | Why |
|---|---|
| `cache.db_dir` | The cache file lives in `storage.dir`. |
| `ui.recorder.dbstats_warm_interval_seconds` | Database statistics are no longer cached in a shared file. |
| `ui.recorder.dbstats_inline_scan_limit` | Same as above. |

## 5. Review the new keys

These keys are new. Their defaults work for most deployments.

| Key | Default | What it does |
|---|---|---|
| `storage.dir` | `/tmp/drakkar` | This worker's own files. The worker creates the folder with mode `0700` when it is missing. `''` = memory-only. |
| `shared.dir` | `''` | The folder all workers see. `''` = `storage.dir`. |
| `history.archive.due_after_hours` | `48` | A window is archived this many hours after it ends. |
| `history.archive.lock_stale_seconds` | `7200` | An archive lock or work file older than this belongs to a dead archiver. |
| `history.merged_retention_days` | `32` | UI merge results older than this are deleted. `0` = never. |
| `fleet.registry.provider` | `filesystem` | How workers find each other: `filesystem`, `static`, `none`, or a plugin. Needs `ui.enabled`. |
| `fleet.registry.heartbeat_seconds` | `10` | How often a worker publishes its card. |
| `fleet.registry.refresh_seconds` | `15` | How often a worker reads the other cards. |
| `fleet.registry.prune_after_seconds` | `86400` | Cards older than this are deleted. |
| `fleet.registry.timeout_seconds` | `5` | Time limit for one registry operation. |
| `fleet.registry.filesystem.dir` | `''` | Card folder. `''` = `<shared.dir>/fleet`. |
| `fleet.registry.static.peers` | `[]` | Fixed peer list for the `static` provider. |
| `fleet.cache_exchange.max_concurrent_peers` | `4` | Peers pulled from in parallel. |
| `ui.trace.history_max_files` | `256` | Most history files one trace request reads. |
| `ui.trace.history_max_seconds` | `10` | Time limit for one trace request over history. |

See [Configuration](configuration.md) for every field.

## 6. Kubernetes example

Before:

```yaml
env:
  - name: DK_UI__RECORDER__DB_DIR
    value: /var/lib/drakkar
  - name: DK_CACHE__PEER_SYNC__INTERVAL_SECONDS
    value: "30"
volumeMounts:
  - name: recorder
    mountPath: /var/lib/drakkar
```

After, for a fleet across hosts (a per-pod volume for own files, one
`ReadWriteMany` volume for the fleet):

```yaml
env:
  - name: DK_STORAGE__DIR
    value: /var/lib/drakkar
  - name: DK_SHARED__DIR
    value: /mnt/fleet
  - name: DK_FLEET__CACHE_EXCHANGE__INTERVAL_SECONDS
    value: "30"
volumeMounts:
  - name: own-files          # emptyDir or a per-pod PVC
    mountPath: /var/lib/drakkar
  - name: fleet              # ReadWriteMany PVC, the same on every pod
    mountPath: /mnt/fleet
```

## 7. Check the result

1. Start one worker. If a key is still old, the load fails with one
   message that lists every old key, for example:

    ```
    config uses keys that moved or were removed (YAML paths; as env vars they are DK_ plus the path with "__"; see docs/upgrading.md):
      ui.recorder.db_dir -> storage.dir
      cache.db_dir: removed, the cache file lives in storage.dir
    ```

2. Read the startup line `drakkar_starting`. Its summary shows
   `storage=<folder>`, `shared=<folder>` when the shared folder differs,
   and `fleet=<provider>` (`fleet=off` when the UI is off or no folder
   resolves).
3. Start the other workers. The workers list in the UI shows every peer.

## 8. Old files

Upgrade with all workers stopped. The new release does not read the files
an older release wrote: delete the old folder, or point `storage.dir` and
`shared.dir` at empty folders. A rolling upgrade from the older release is
not supported.
