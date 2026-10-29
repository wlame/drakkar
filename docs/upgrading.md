# Upgrading: storage, history, fleet and provider settings

This page has two guides, one for each move:

- **To release 2.4.0, from an older release** (steps 1 to 8 below). Release
  2.4.0 moved the settings for files, archives, worker discovery and cache
  exchange into four new config sections: `storage`, `shared`, `history` and
  `fleet`. Some old keys moved, a few were removed. The old keys are not
  read any more: a config that still uses one **fails to load** with a
  message that lists every old key it found.
- **From 2.4.0 to this release**
  ([Upgrading from 2.4.0: provider settings](#from-2-4-0)). This release
  puts the calls to a registry or election provider under shared rules and
  adds the `connections` section. It also gives `@periodic` a schedule that
  every worker computes the same way and a `cluster` scope, and turns the
  archive pass into a cluster periodic task: the archive keys and the
  archive lock changed. Some keys moved, one default changed, and some log
  events were replaced. The recorder keeps recent events in memory and has a
  persistence choice; see
  [The recorder memory store](#recorder-memory-store). A new recorder mode
  keeps the history in Postgres; see
  [The recorder history in Postgres](#recorder-history-in-postgres). The
  DLQ now holds the original messages, and every sink declares a failure
  policy; see [The DLQ record](#dlq-record).

Coming from 2.4.0, read only the second guide. Coming from an older release,
do the first guide, then the second.

Follow steps 1 to 8 in order. Plan the move to 2.4.0 **with all workers
stopped**: a worker of 2.4.0 does not read the files an older release
wrote, and an older and a newer worker cannot run side by side.

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
  Each worker writes its live files only to `storage.dir`. The shared
  folder holds the worker registry, the history, the archives and the merge
  results. A finished recorder file is sealed and moved from `storage.dir`
  into `<shared.dir>/history/<cluster>/` in the background.

`shared.dir` empty means "the same folder as `storage.dir`".

The cache file is no longer shared. Each worker keeps
`<storage.dir>/<worker>-cache.db` to itself, and workers pull each other's
`cluster` and `global` cache rows over HTTP, from the URL on each
[registry card](fleet.md). So `storage.dir` does not need to be shared for
the cache any more, but every worker must reach the UI port of its peers.

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

`fleet.registry.offline_after_seconds` has a new default in this release:
60 seconds, not 30. See [the provider settings section](#from-2-4-0).

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
| `storage.dir` | `/tmp/drakkar` | This worker's own files. The worker creates the folder with mode `0700` when it is missing. `''` = memory-only (recorder persistence `none`). |
| `shared.dir` | `''` | The folder all workers see. `''` = `storage.dir`. |
| `history.archive.due_after_hours` | `48` | A window is archived this many hours after it ends. |
| `history.archive.lock_stale_seconds` | `7200` | An archive lock or work file older than this belongs to a dead archiver. The second guide replaces it with `history.archive.lease_seconds` and `history.archive.sweep_after_seconds`. |
| `history.merged_retention_days` | `32` | UI merge results older than this are deleted. `0` = never. |
| `fleet.registry.provider` | `filesystem` | How workers find each other: `filesystem`, `postgres`, `redis` or `consul`. Needs `ui.enabled`. |
| `fleet.registry.heartbeat_seconds` | `10` | How often a worker publishes its card. |
| `fleet.registry.refresh_seconds` | `15` | How often a worker reads the other cards. |
| `fleet.registry.prune_after_seconds` | `86400` | Cards older than this are deleted. |
| `fleet.registry.resilience.timeout_seconds` | `5` | Time limit for one attempt of a registry operation. The `resilience` block also holds retries and a circuit breaker; see [Fleet registry](fleet.md#resilience). |
| `fleet.cache_exchange.max_concurrent_peers` | `4` | Peers pulled from in parallel. |
| `ui.trace.history_max_files` | `256` | Most history files one trace request reads. |
| `ui.trace.history_max_seconds` | `10` | Time limit for one trace request over history. |

See [Configuration](configuration.md) for every field, and
[Fleet registry](fleet.md) for how workers find each other.

Two existing keys changed what they cover:

- **`ui.allowed_ws_origins`** also decides the cross-origin reads that the
  UI sends to every worker of the cluster (trace, label trace, databases
  list). **With `ui.auth_token` set, list the origin of every worker
  here**, for example `["http://10.0.3.17:8080", "http://10.0.3.18:8080"]`.
  Otherwise the UI of one worker cannot read the others and lists them as
  unreachable. See [Cross-origin reads](security.md#cross-origin-reads).
- **`ui.auth_token`** does not protect the new routes under
  `/api/v1/peer/`: workers call them on each other without a token. Anyone
  who can reach the UI port can read the `cluster` and `global` cache rows
  of a worker that serves the cache exchange. See
  [The public peer route](security.md#the-public-peer-route).

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
   message that lists every unknown key, for example:

    ```
    config has unknown keys (YAML paths; as env vars they are DK_ plus the path with "__"; see docs/config-reference.md):
      cache.db_dir
      ui.recorder.db_dir
    ```

2. Read the startup line `drakkar_starting`. Its summary shows
   `storage=<folder>`, `shared=<folder>` when the shared folder differs,
   and `fleet=<provider>` (`fleet=off` when the UI is off or no folder
   resolves). When the election provider (`periodic.election.provider`) is
   not `filesystem`, it also shows `election=<provider>` after `fleet=`.
3. Start the other workers. The workers list in the UI shows every peer.
   With the cache on, `drakkar_fleet_peer_requests_total{outcome="ok"}`
   grows on each worker once a peer has new `cluster` or `global` rows.
   Other outcomes mean that a peer could not be reached at the URL on its
   card, or answered with an error; the log event
   `fleet_peer_request_failed` names the peer and the URL.
4. Wait for one rotation (`ui.recorder.rotation_interval_hours`, default 1
   hour), or stop one worker cleanly. Then look in
   `<shared.dir>/history/<cluster>/` (`<storage.dir>/history/<cluster>/`
   when `shared.dir` is empty). It holds a sealed recorder file
   `<worker>-<YYYY-MM-DD__HH_MM_SS>.db` and its meta file
   `<worker>-<YYYY-MM-DD__HH_MM_SS>.db.meta.json`. `<cluster>` is the
   cluster name with every character outside `[a-zA-Z0-9_-]` replaced by
   `_`, or `default` when the cluster name is empty. If the folder stays
   empty, check `drakkar_recorder_unsealed_files` and the log event
   `recorder_seal_failed`. See
   [Sealing](local-databases.md#sealing-and-the-history-folder).

## 8. Old files

Upgrade with all workers stopped. The new release does not read the files
an older release wrote and does not migrate them: after the upgrade the
history folders start empty. Delete the old folder, or point `storage.dir`
and `shared.dir` at empty folders. A rolling upgrade from the older release
is not supported.

The worker does not delete the old files. Delete them yourself:

| Old file | What it was |
|---|---|
| `<worker>-<YYYY-MM-DD__HH_MM_SS>.db` (and its `-wal`, `-shm`) | Rotated recorder files. |
| `<worker>-live.db` | Link to the live recorder file. Workers no longer open each other's live files. |
| `<worker>-cache.db` (a link) and `<worker>-cache.db.actual` (and its `-wal`, `-shm`) | The old cache link and cache file. The cache file is now a plain `<worker>-cache.db`, private to its worker. Delete both **before** the first start: the worker does not remove them, and an old link with that name is followed to the old `.actual` file. |
| `.dbstats.db` | Statistics cache of the Databases page. Sealed files now carry a meta file. |
| `.archive-<cluster>.lock` | The old archive lock. Release 2.4.0 kept the lock in `history/<cluster>/.archive.lock`, and this release keeps it in `tasks/<cluster>/history.archive.lock`; see [Old lock files](#old-lock-files). |
| `<cluster>-<from>__<to>.db.gz` in `storage.dir` | Old archives. The archive pass now works only inside `<shared.dir>/history/<cluster>/`. |
| `merged-*.db` in `storage.dir` | Old UI merge results. New ones go to `<shared.dir>/merged/`. |

!!! note "Old recorder files with the worker's own name"
    An old rotated file named `<worker>-<YYYY-MM-DD__HH_MM_SS>.db` matches
    the [leftover](local-databases.md#leftovers) rule for that worker name.
    If it is still in `storage.dir` when that worker starts, the worker
    seals it and moves it into the history folder like a file of its own.
    Delete the old files before the first start if you do not want them in
    the history.

## Upgrading from 2.4.0: provider settings {#from-2-4-0}

The calls from a worker to a registry provider and to an election provider
now follow one set of rules: a time limit per attempt, retries, and a
circuit breaker. The new `connections` section declares the shared services
that providers use. Periodic tasks have new schedules and a cluster scope,
and the archive pass is now the cluster periodic task `history.archive`
([step 6](#periodic-tasks-and-the-archive)). Some keys moved, one default
changed, and some log events were replaced.

Between 2.4.0 and this release these formats did not change: the sealed
recorder file and its meta file, and the cache exchange route. The registry
card gains `features.periodic`, which a 2.4.0 worker ignores, so a worker of
one release can read the cards and the sealed files that a worker of the
other writes. **The archive lock moved**, so a 2.4.0 worker and a worker of
this release do not see each other's lock and must not archive at the same
time; see [step 6](#periodic-tasks-and-the-archive). Each worker applies its
own settings, too: a worker that still has the 2.4.0 default of
`offline_after_seconds` (30) and a worker with the new default (60) call the
same peer offline at different times.

### 1. Move the registry timeout

| Old YAML path | New YAML path | Old environment variable | New environment variable |
|---|---|---|---|
| `fleet.registry.timeout_seconds` | `fleet.registry.resilience.timeout_seconds` | `DK_FLEET__REGISTRY__TIMEOUT_SECONDS` | `DK_FLEET__REGISTRY__RESILIENCE__TIMEOUT_SECONDS` |

The old key fails the load as an unknown key, as in step 7 above.

The value now limits **one attempt**. By default an operation makes up to
three attempts (`resilience.retries: 2`) with waits of 0.2 s and 0.4 s
between them. Set `retries: 0` for one attempt per operation, as in 2.4.0.

### 2. Check the offline limit

`fleet.registry.offline_after_seconds` now defaults to `60` (it was `30`),
and it must be at least `10` for every provider. A value you set explicitly
still loads and keeps its meaning, for example `30` with the default
heartbeat of `10`. Only the default moved: with it, a worker that stopped
publishing shows as offline after 60 seconds, not after 30. A value below
`10` fails the load with
`fleet.registry.offline_after_seconds (9) must be >= 10`.

### 3. Update log filters and alerts

| Old log event | Now |
|---|---|
| `fleet_registry_publish_failed`, `fleet_registry_list_failed`, `fleet_registry_withdraw_failed` | `provider_call_failed` with `feature="registry"` and `op` set to `publish`, `list` or `withdraw`. At most once a minute per operation; `suppressed` says how many failures it stands for. |
| `fleet_registry_start_failed` | `provider_call_failed` with `op="start"`. The worker no longer replaces a provider that fails to start with `none`: it tries the start again at the next heartbeat. |
| `recorder_archive_lock_taken_over` | `periodic_lock_taken_over`, in category `periodic`. Fields `provider`, `scope` (such as `main/history.archive`), `previous_worker`, `previous_instance`, `age_seconds`. |
| `recorder_archive_lock_put_back_failed` | `periodic_lock_put_back_failed`, in category `periodic`. Fields `scope`, `folder`, `error`. |

`fleet_registry_stop_failed` and `fleet_registry_loop_failed` keep their
names. `fleet_registry_stop_failed` now means that the registry's own stop
raised; a provider whose `stop` fails shows as `provider_call_failed` with
`op="stop"`.

The metric `drakkar_fleet_registry_errors_total` changed its meaning. It
counts operations that failed after their retries (one count per operation)
and loop iterations that raised. Its `op` label has two more values, `start`
and `stop`. The three metrics `drakkar_provider_calls_total`,
`drakkar_provider_call_duration_seconds` and `drakkar_provider_breaker_open`
are new. See [Fleet registry](fleet.md#metrics).

### 4. Know what else changed

- **Config errors do not print the input of the failed section.** A load
  error names the key and the rule, but no longer adds a cut of the section
  that failed, so a wrong `connections` block cannot print a connection
  string. The messages of the framework itself still quote a value that is
  not a secret, for example a schema name or an out-of-range number.
- **The lock body has a `provider` key.** For the default election
  provider it is `"provider":"filesystem"`.
- **`recorder_archive_fenced` has two more fields,** `provider` and
  `reason` (`taken_over` or `unconfirmed`). See
  [Fencing](local-databases.md#fencing).
- **The summary line** shows ` election=<provider>` after `fleet=` when
  `periodic.election.provider` is not `filesystem`.
- **The `redis` package must be 7.2 or later.** The minimum was 5.2.0. The
  `redis` providers need it, and the Redis sink uses the same package.
  Installing `py-drakkar` brings a matching version; check it only when you
  pin `redis` yourself.
- **New sections and keys** need no action. `connections` is empty by
  default. `fleet.registry.connection`, `periodic.election` and the
  `resilience` blocks have working defaults.

### 5. If you wrote a plugin

Plugins are not supported. The entry-point groups `drakkar.sinks`,
`drakkar.registry_providers` and `drakkar.election_providers` are not read.
`sinks.custom`, `fleet.registry.plugin_options` and
`periodic.election.plugin_options` are not config keys, and `CustomPayload`
and `CollectResult.custom` do not exist. A sink or a registry or election
provider that you need must be one of the built-in ones: sinks `kafka`,
`postgres`, `mongo`, `http`, `redis` and `filesystem`; providers `filesystem`,
`postgres`, `redis` and `consul`.

### 6a. Task cost, speed and throughput

The `throughput` section (`cost_label`, `min_cost`) is not a config key; delete
it and any `DK_THROUGHPUT__*` variable. The `cost` and `speed` keys of
`task_completed` metadata and of `recent-tasks` rows, the `throughput` frame on
`/ws` and the `worker_state.throughput` column do not exist. Remove
dashboards and alerts on `drakkar_task_speed`, `drakkar_throughput` and
`drakkar_task_rate`.

### 6b. Periodic task overrides and registry providers

`periodic.tasks` is not a config key; delete it and any
`DK_PERIODIC__TASKS__*` variable. To change the schedule or the lease of a
task, change the arguments of its `@periodic` decorator. To stop a task from
running, do not declare it.

The registry providers `static` and `none` do not exist. A config that names
one fails startup (with the UI on) with
`fleet.registry.provider '<name>' is not known; known providers: consul, filesystem, postgres, redis`.
Delete `fleet.registry.static.peers`. A worker that needs no registry runs
with `ui.enabled: false`, or with the `filesystem` provider and no
`storage.dir` and no `shared.dir`: the registry is then off, and the workers
list shows only the current worker.

These keys of `fleet.registry` are not config keys either; delete them:

- `filesystem.dir`. The card folder is `<shared.dir>/fleet`, or
  `<storage.dir>/fleet` when `shared.dir` is empty. The worker no longer
  checks the folder at startup: a folder that cannot be written only fails
  the publishes, which are counted and retried.
- `consul.mode` and `consul.service_name`. The `consul` provider has one
  mode, which is the one `publish` selected.
- `redis` and `postgres`. These blocks had no keys.

### 6. Periodic tasks and the archive {#periodic-tasks-and-the-archive}

**The decorator.** `@periodic` takes `interval` instead of `seconds`. Write
`@periodic(30)` or `@periodic(interval=30)`; `@periodic(seconds=30)` now
raises `TypeError` when the handler module is imported. The decorator also
checks the method name: 1 to 64 characters of `a-z`, `0-9` and `_`, not
starting with a digit. A method named `refreshRates` must be renamed. A
worker-scope task with an interval runs as before. See
[Periodic Tasks](periodic-tasks.md) for `cron=` and `scope='cluster'`.

**The archive keys.** The archive pass no longer runs after each rotation:
it runs at archive slots, at the top of each UTC hour by default.

| Old YAML path | New YAML path | New environment variable |
|---|---|---|
| `history.archive.lock_stale_seconds` | `history.archive.lease_seconds` (the lease, default 60, 40 to 86400) and `history.archive.sweep_after_seconds` (the age of work files to sweep, default 7200, at least 60) | `DK_HISTORY__ARCHIVE__LEASE_SECONDS`, `DK_HISTORY__ARCHIVE__SWEEP_AFTER_SECONDS` |
| `history.archive.pass_interval_seconds` | `history.archive.interval` (seconds between slots, at least 30) or `history.archive.cron` (five-field UTC cron) | `DK_HISTORY__ARCHIVE__INTERVAL`, `DK_HISTORY__ARCHIVE__CRON` |
| `history.archive.election` (and every key below it) | `periodic.election` | `DK_PERIODIC__ELECTION__<KEY>` |

Release 2.4.0 has only the first of these keys; the other two exist only in
configs written for an older build of this release.

Each old key fails the load as an unknown key.

The old `lock_stale_seconds` was both the lease and the sweep age. If you
set it, set `sweep_after_seconds` to the old value and leave
`lease_seconds` at its default: the lease now only says how long the lock of
a dead archiver lives.

**Roll out with archiving off, or stop the fleet.** A 2.4.0 worker takes the
archive lock `history/<cluster>/.archive.lock`; a worker of this release
takes `tasks/<cluster>/history.archive.lock` (or the lease of another
election provider). The two cannot see each other's lock, so a worker of
each kind could archive one window at the same time. Do one of these:

- Stop every worker of the fleet, upgrade, and start them again.
- Or set `history.archive.enabled: false` (`DK_HISTORY__ARCHIVE__ENABLED=false`)
  on every worker for the rolling restart, and turn it back on once every
  worker runs this release. Due windows wait; nothing is lost, and the next
  archive slot after you turn it on picks them up.

A 2.4.0 card carries no definition hash, so the new workers count a 2.4.0
peer as unknown and log no warning about it. While archiving is off on every
worker, the new workers also hash the same definitions. When you turn
archiving back on one worker at a time, a worker with it on and a worker
with it off hash different definitions (`history.archive` is one of them),
so they log `periodic_cluster_mismatch` and
`drakkar_periodic_cluster_mismatch_peers` is above zero until the last worker
has archiving on again. That is expected.

#### Old lock files {#old-lock-files}

Nothing reads the old locks any more, and the worker does not delete them.
Once no 2.4.0 worker runs, delete:

| Old file or table | What it was |
|---|---|
| `<shared.dir>/history/<cluster>/.archive.lock` | The archive lock of 2.4.0. |
| `<shared.dir>/history/<cluster>/.archive.lock.stale.<token>` | A lock set aside by a takeover. The archive pass also deletes these once they are older than `history.archive.sweep_after_seconds`. |
| Postgres table `<schema>.archive_locks` | The archive lease of the `postgres` election provider in an older build. Drop it with `DROP TABLE IF EXISTS <schema>.archive_locks;`. The provider now creates `task_locks`. |

**Redis and Consul keys.** The `redis` election provider now keeps its leases
under `<key_prefix>task-locks:<cluster>/<task>`, and the `consul` provider
under `<key_prefix>/task-locks/<cluster>/<task>`, with sessions named
`drakkar/task-lock/<cluster>/<task>/<token>`. A Redis user or a Consul
token limited to the key prefix needs no change. A lock key of an older
build ends by itself.

### 7. The recorder memory store {#recorder-memory-store}

The recorder now keeps its recent events in an in-memory database, and
`ui.recorder.persistence` (`none` or `sqlite`) says whether it also writes
recorder files. See [Recent Events in Memory](recent-events.md).

- **Memory-only configs keep working.** A config with `storage.dir: ''` and
  no `persistence` key derives `none`. Such a worker is now queryable: the
  History, Trace and Task Detail pages, the memory row of the Databases tab
  and the memory snapshot download read the memory store. They cover the
  memory window only (two minutes by default).
- **`persistence: sqlite` needs a folder.** With an empty `storage.dir` the
  config does not load:

    ```
    ui.recorder.persistence is sqlite but storage.dir is empty; set storage.dir to a folder, or set ui.recorder.persistence to none (or remove it) to keep recorder events in memory only
    ```

- **Stop a worker cleanly before you switch it to `none`.** A `none` worker
  seals nothing, so a recorder file that an earlier `sqlite` run left in
  `storage.dir` stays there and never reaches the history folder. A clean
  stop seals and moves the live file first.
- **Ids no longer start at 1 per file.** The recorder gives each row its id
  in the process. Ids continue across rotations and start again at 1 after a
  restart. A client that polls `/api/v1/events` with `after_id` now sees new
  rows right after a rotation.
- **Give every worker of a cluster the same persistence.** A `none` worker
  does not run the archive, so its cluster task hash differs from the
  `sqlite` workers and they log `periodic_cluster_mismatch`.
- **Plan RAM.** With the default budget of 256 MiB, plan up to about 600 MiB
  per worker for the store. See [RAM to plan](recent-events.md#ram-to-plan).
- **`EventRecorder.reader_db` is gone.** Use `file_reader_db` for the live
  file, or `memory_store` for the memory store.

### 8. The recorder history in Postgres {#recorder-history-in-postgres}

`ui.recorder.persistence` accepts a third value, `postgres`: every worker
writes its events to one table in Postgres, and the trace with history reads
that table. Nothing changes until you set it; it is never derived. See
[Recorder History in Postgres](recorder-postgres.md).

- **Switching a worker from `sqlite` to `postgres`.** Stop the worker
  cleanly first: a clean stop seals and moves its live file. A file that a
  worker left behind without a clean stop stays in `storage.dir`. The sealed
  files stay in the history folder; merge, file downloads and the archives
  list still show them, but `trace?history=1` on a `postgres` worker no
  longer searches them. There is no migration of old files into the table.
- **Switch a whole cluster at once.** A cluster that mixes `postgres` and
  `sqlite` workers shows a cluster task mismatch (the archive runs on the
  `sqlite` side only), and its history is split between the files and the
  table.
- **Declare the connection.** `ui.recorder.postgres.connection` (default
  `main`) must name an entry under `connections.postgres`, or the config does
  not load.
- **The role must own the events table.** The worker creates and drops its
  partitions. With an operator-managed schema, add `--recorder` to the
  `drakkar-schema` command and give the table to the worker role.
- **The index set is fixed.** The table has three indexes and its primary
  key. A later `CREATE INDEX` on a filled partitioned table builds on every
  partition under a lock that blocks every writer. A later index change
  needs a new `table_prefix`, or a manual `CREATE INDEX CONCURRENTLY` on each
  partition.
- **Contract v1.26.** With `history=1`, a `postgres` worker answers the rows
  of its cluster from the table, or `503 {"detail": "history search
  failed"}`. A UI of this release shows "(the history search failed)"; an
  older UI shows the same note for a failed history search.

### 9. The DLQ record {#dlq-record}

A DLQ record is now the failed source message itself: the original key and
value, byte for byte, its own headers, then `dk-` headers that say where and
why it failed (see [The DLQ record](sinks.md#the-dlq-record)). The JSON
document with `original_payloads` is gone. Every sink also declares what
happens when its delivery fails (see [Failure policy](sinks.md#failure-policy)).
Do these steps in order.

1. **Drain or archive the old DLQ topic first.** Records written by an older
   worker are JSON documents with no headers; records written by this release
   are the original messages with `dk-` headers. The new replay tool never
   republishes an old record: it skips every record without a
   `dk-record-version` header and counts it as `skipped_old_format`. Replay
   the old records with the tool of the older release before you upgrade,
   copy them away, or delete them; or move the new workers to a new
   `dlq.topic`. Old records left in the topic stay there until its retention
   removes them.
2. **Change every consumer of the DLQ topic.** A tool that parsed the JSON
   document must read the record instead: the value is the original message
   value (it may not be JSON or UTF-8), the failure facts are headers
   (`dk-stage`, `dk-error`, `dk-sink`, `dk-attempts`, `dk-source-topic`,
   `dk-source-partition`, `dk-source-offset`, ...). One record is one source
   message, not a list of payloads.
3. **Code that calls `read_dlq_entries`** now gets `DLQEntry(key, value,
   headers, partition, offset)` records, not decoded JSON objects.
   `DLQMessage` and `ParseFailurePayload` are removed; a parse failure is a
   record with `dk-stage: parse`.
4. **Use the new replay tool.** `scripts/replay_dlq.py` republishes the key,
   the value and the source headers of each record, and takes `--stage`. Its
   summary line has `skipped_old_format` instead of `skipped_invalid`, and its
   report lines name `dlq_partition=` and `dlq_offset=`.
5. **Check `sinks.http.<name>.max_retries`.** It is now the shared retry
   budget of every sink: the default is 2 (it was 3, and unused), and a value
   above 10 fails the load with
   `sinks.http.<name>.max_retries must be a whole number from 0 to 10`.
6. **Review your `on_delivery_error` hook.**
    - The hook is called after every failed attempt, with `attempt`,
      `retries_left` and `declared_policy`. `RETRY` is honoured up to the
      sink's `max_retries` (it was a fixed 3 attempts); then the declared
      `on_failure` applies.
    - The default hook returns the sink's declared `on_failure` (`dlq` unless
      you set `skip`), as before for a sink you did not configure.
    - A hook that raises no longer stops the delivery: the worker logs
      `delivery_error_hook_failed` and applies the declared `on_failure`. It
      no longer reaches `dlq.on_send_failure`. The HTTP source follows the
      same rule (it sent such a request to the DLQ before).
    - With the circuit open, the hook is not called even when the worker has
      no DLQ; the declared `on_failure` decides.
7. **Know what "the message is done" means.** After the first DLQ record of
   a message, no later result of that message is delivered, also to sinks that
   did not fail, and `on_message_complete` still runs but its result is not
   delivered. A sink that may lose its payloads without stopping the message
   should declare `on_failure: skip`.
8. **Window results cannot reach the DLQ.** A failed delivery of an
   `on_window_complete` result went to the DLQ before (as the payload JSON).
   A window result has no source message, so `on_failure: dlq` now acts as
   `skip` there and logs `window_delivery_not_dlq`. A handler that needs the
   DLQ for an aggregate delivers it from `on_message_complete` instead, or
   sets `on_failure: skip` on that sink knowingly.
9. **The HTTP response summary.** In the per-type `sinks` summary of an HTTP
    request, the error markers `dlq_unconfigured` and `dlq_send_failed: ...`
    are gone. A sink that did not start reports `<type>/<name>: not started`,
    and a sink that failed with no DLQ record reports
    `<type>/<name>: failed, no DLQ record`. `dlq` counts only when the
    request's DLQ record was written.
10. **Update dashboards and alerts.** `drakkar_dlq_dropped_payloads_total` now
    counts source messages, once each, not payloads. New:
    `drakkar_dlq_records_total{stage}` and
    `drakkar_sink_deliveries_not_started_total{sink_type,sink_name,reason}`.
    The help texts of `drakkar_sink_dlq_messages_total`,
    `drakkar_dlq_dropped_payloads_total`,
    `drakkar_dlq_unconfigured_drops_total` and
    `drakkar_sink_deliveries_skipped_total` changed. The log events
    `dlq_message_sent` and `dlq_send_failed` carry `stage`, `sink` and
    `value_bytes` instead of payload fields. `dlq_payload_serialization_fallback`
    is gone (nothing is serialized any more). The HTTP source's
    `webapp_on_delivery_error_failed` and `webapp_sink_dlq_send_failed` are
    gone: the shared `delivery_error_hook_failed`, `message_dlq`,
    `dlq_send_failed` and `dlq_failure_payloads_dropped` cover those cases.
11. **Protect the DLQ topic like the source topic.** It now holds the original
    messages; see [Security](security.md#the-dlq-topic-holds-original-messages).

## Config keys are strict {#strict-keys}

Before this release, a nested section dropped a key it did not know, and the
value stopped applying with no message. Now every unknown key fails the load, in
the YAML file and in a `DK_*` variable, and one message lists them all:

```
config has unknown keys (YAML paths; as env vars they are DK_ plus the path with "__"; see docs/config-reference.md):
  sinks.kafka.results.max_retires (did you mean 'max_retries'?)
  ui.recorder.db_dir
```

1. **Delete or correct each key in the message.** A key that was ignored
   before, for example `rotation_interval_minutes`, must go. This includes the
   keys that used to give a "moved" message and the top-level `webapp:` section,
   which is `sources.http`.
2. **Check variables for hyphenated names.** A variable now reaches the existing
   name: `DK_SINKS__POSTGRES__MAIN_DB__POOL_MAX` changes `sinks.postgres.main-db`.
   Before, it made a second instance `main_db`. If two names in the YAML match one
   variable (`main-db` and `main_db`), the load fails: rename one.
3. **Set `schema` of a Postgres connection by that key.** In Python code,
   `PostgresConnectionConfig(schema='audit')` is the only form; the attribute
   stays `schema_name`.
4. **Config models refuse unknown keywords** when you build one in code.
   `DrakkarConfig(**other.model_dump())` fails with a Postgres connection, because
   the dump keys it `schema_name`. Use `drakkar.configmeta.dump_by_yaml_key`.

## Services, sources, sinks and the DLQ {#services}

A service is now declared once under `connections`. The Kafka source, the DLQ and
every Kafka, Postgres, Mongo and Redis sink name a connection, and the connection
must be declared. Do these steps in order. An old key fails the load, so the load
error tells you what is left.

1. Move the root `kafka:` section to `connections.kafka.main`. Every key keeps its
   name. Every config with the Kafka source on needs this entry, even an empty one
   (`main: {}`), because the source and the DLQ name `main` by default.
2. Give each Kafka sink or DLQ that has its own `brokers` a connection of its own,
   and name it with `connection`. Move its `brokers` and `security` to that entry.
   A sink's or the DLQ's `client_config` stays where it is. It is now merged after
   the connection's, so a sink or DLQ with no `brokers` gains the properties of
   `connections.kafka.main` and wins on a key that both set.
3. Move each `dsn`, `uri` and `url` of a Postgres, Mongo or Redis sink to an entry
   under `connections.postgres`, `connections.mongo` or `connections.redis`, and
   name the entry with `connection` on the sink. Sinks that used one server can
   share one entry. A Postgres connection entry also holds `schema`, `manage_schema`
   and its own `pool_min` and `pool_max`, for the autodiscovery tables only. A
   sink's `pool_min` and `pool_max` stay on the sink.
4. Rename the keys in this table, and write each duration or size with a unit. The
   environment variables follow the YAML paths.

| Old path | New path | Example |
|---|---|---|
| `kafka.<key>` | `connections.kafka.main.<key>` | |
| `DK_KAFKA__BROKERS` | `DK_CONNECTIONS__KAFKA__MAIN__BROKERS` | |
| `DK_KAFKA__SECURITY__SASL_PASSWORD` | `DK_CONNECTIONS__KAFKA__MAIN__SECURITY__SASL_PASSWORD` | |
| `connections.redis.<name>.key_prefix` | `connections.redis.<name>.state_prefix` | |
| `connections.consul.<name>.key_prefix` | `connections.consul.<name>.state_prefix` | |
| `sources.kafka.max_poll_interval_ms` | `sources.kafka.max_poll_interval` | `300000` is `5m` |
| `sources.kafka.session_timeout_ms` | `sources.kafka.session_timeout` | `45000` is `45s` |
| `sources.kafka.heartbeat_interval_ms` | `sources.kafka.heartbeat_interval` | `3000` is `3s` |
| `sources.kafka.startup_align_enabled` | `sources.kafka.startup_align.enabled` | |
| `sources.kafka.startup_min_wait_seconds` | `sources.kafka.startup_align.min_wait` | `4.0` is `4s` |
| `sources.kafka.startup_align_interval_seconds` | `sources.kafka.startup_align.interval` | `10` is `10s` |
| `sources.http.sinks_enabled` | `sources.http.deliver_to_sinks` | |
| `sources.http.request_timeout_seconds` | `sources.http.request_timeout` | `30.0` is `30s` |
| `sources.http.max_body_bytes` | `sources.http.max_body` | `10485760` is `10MiB` |
| `sinks.delivery_timeout_seconds` | `sinks.defaults.delivery_timeout` | `30.0` is `30s` |
| `sinks.circuit_breaker.failure_threshold` | `sinks.defaults.breaker_failures` | |
| `sinks.circuit_breaker.cooldown_seconds` | `sinks.defaults.breaker_cooldown` | `30.0` is `30s` |
| `sinks.kafka.<name>.flush_timeout_seconds` | `sinks.kafka.<name>.flush_timeout` | `30.0` is `30s` |
| `sinks.kafka.<name>.brokers`, `.security` | `connections.kafka.<entry>.brokers`, `.security`, and `sinks.kafka.<name>.connection: <entry>` | |
| `sinks.postgres.<name>.dsn` | `connections.postgres.<entry>.dsn`, and `sinks.postgres.<name>.connection: <entry>` | |
| `sinks.mongo.<name>.uri` | `connections.mongo.<entry>.uri`, and `sinks.mongo.<name>.connection: <entry>` | |
| `sinks.redis.<name>.url` | `connections.redis.<entry>.url`, and `sinks.redis.<name>.connection: <entry>` | |
| `sinks.http.<name>.timeout_seconds` | `sinks.http.<name>.request_timeout` | `30` is `30s` |
| `dlq.flush_timeout_seconds` | `dlq.flush_timeout` | `30.0` is `30s` |
| `dlq.brokers`, `dlq.security` | `connections.kafka.<entry>.brokers`, `.security`, and `dlq.connection: <entry>` | |

5. Check the two new DLQ keys. `dlq.enabled` is on by default. With `dlq.enabled:
   false` a `dlq` outcome follows `dlq.on_send_failure`, so `stall` holds the offset.
   `dlq.provider` accepts only `kafka`.
6. If a handler changes `sinks.circuit_breaker` or `sinks.delivery_timeout_seconds`
   in `on_startup`, set `sinks.defaults` in the config file instead. Each sink's
   policy is resolved when the worker is constructed. A change of `sinks.defaults`
   or of a sink's `on_failure`, `max_retries`, `delivery_timeout`,
   `breaker_failures`, `breaker_cooldown` or `depends_on` in `on_startup` is logged
   as `on_startup_config_change_ignored` and does nothing. A sink added in
   `on_startup` still gets its policy from the config it was added to.
7. Check tools that read `worker_config.kafka_brokers`. It holds the brokers of the
   source's connection, and `''` on a worker with the Kafka source off.

In code, these names changed: `KafkaConfig` is `KafkaConnectionConfig`,
`CircuitBreakerConfig` is gone, and `KafkaSink`, `PostgresSink`, `MongoSink` and
`RedisSink` take the connection as their third argument.

## The flight recorder and the shared folder {#recorder}

The recorder is now a root section, `recorder:`, and it does not depend on the UI.
`ui.enabled` controls only the UI. Do these steps in order. An old key fails the
load, so the load error tells you what is left.

1. **Check workers with `ui.enabled: false`. They now record.** Such a worker used
   to have no recorder at all. It now writes recorder files into `storage.dir`
   (with the default `/tmp/drakkar` too), seals them and runs the archive task,
   with no memory store. To keep the worker as it was, set `recorder.enabled: false`.
2. **Replace `ui.recorder.persistence`.** There is no derive rule any more:

    | The old value | The new keys |
    |---|---|
    | unset, with `storage.dir` set | nothing: `recorder.enabled` defaults to `true`, `recorder.provider` to `sqlite` |
    | unset, with an empty `storage.dir` | `recorder.enabled: false`. This is new: with `sqlite` an empty `storage.dir` now fails the load |
    | `none` | `recorder.enabled: false` |
    | `sqlite` | `recorder.provider: sqlite` |
    | `postgres` | `recorder.provider: postgres` |

3. **Check a Trace search of a worker with the recorder off.** With `none`, a
   `trace?history=1` request used to read the sealed files of the history folder. With
   `recorder.enabled: false` it reads no folder and no table: it answers the memory rows.
4. **Rename the keys in this table**, and write each duration or size with a unit. A
   number with no unit fails the load, except `0`. The environment variables follow
   the YAML paths: `DK_UI__RECORDER__MEMORY__WINDOW_SECONDS` is
   `DK_RECORDER__MEMORY__WINDOW`.

| The old key | The new key | The new value of an old one |
|---|---|---|
| `shared.dir` | `storage.shared_dir` | |
| `ui.expose_env_vars` | `recorder.capture.env_vars` | |
| `ui.trace.history_max_files` | `recorder.search.max_files` | |
| `ui.trace.history_max_seconds` | `recorder.search.timeout` | `10.0` is `10s` |
| `ui.recorder.flush_interval_seconds` | `recorder.flush_interval` | `5` is `5s`; whole seconds |
| `ui.recorder.max_buffer` | `recorder.max_buffer_rows` | |
| `ui.recorder.memory.window_seconds` | `recorder.memory.window` | `120` is `2m`; whole seconds |
| `ui.recorder.memory.rows_per_executor` | `recorder.memory.rows_per_executor` | |
| `ui.recorder.memory.max_mib` | `recorder.memory.max_size` | `256` is `256MiB` |
| `ui.recorder.store_events` | `recorder.capture.events` | |
| `ui.recorder.store_config` | `recorder.capture.config` | |
| `ui.recorder.store_output` | `recorder.capture.output` | |
| `ui.recorder.store_stdin` | `recorder.capture.stdin` | |
| `ui.recorder.stdin_max_bytes` | `recorder.capture.max_stdin` | `65536` is `64KiB` |
| `ui.recorder.event_min_duration_ms` | `recorder.capture.min_event_duration` | `0` stays `0`; `100` is `100ms` |
| `ui.recorder.output_min_duration_ms` | `recorder.capture.min_output_duration` | `500` is `500ms` |
| `ui.recorder.annotations_enabled` | `recorder.annotations.enabled` | |
| `ui.recorder.annotation_max_bytes` | `recorder.annotations.max_size` | `16384` is `16KiB` |
| `ui.recorder.annotation_max_bytes_per_call` | `recorder.annotations.max_size_per_call` | `262144` is `256KiB` |
| `ui.recorder.annotation_log_max_bytes` | `recorder.annotations.max_log_size` | `2048` is `2KiB` |
| `ui.recorder.rotation_interval_hours` | `recorder.sqlite.rotation_interval` | `1` is `1h`; whole hours |
| `ui.recorder.max_flush_retries` | `recorder.sqlite.max_flush_retries` | |
| `ui.recorder.store_state` | `recorder.sqlite.state_snapshots` | |
| `ui.recorder.state_sync_interval_seconds` | `recorder.sqlite.state_interval` | `10` is `10s`; whole seconds |
| `history.archive.enabled` | `recorder.archive.enabled` | |
| `history.archive.window_hours` | `recorder.archive.window` | `24` is `24h`; whole hours |
| `history.archive.due_after_hours` | `recorder.archive.due_after` | `48` is `48h`; whole hours |
| `history.archive.retention_days` | `recorder.archive.retention` | `30` is `30d`; whole days |
| `history.merged_retention_days` | `recorder.archive.merged_retention` | `32` is `32d`; whole days |
| `history.archive.interval` | `recorder.archive.interval` | `3600` is `1h`; whole seconds |
| `history.archive.cron` | `recorder.archive.cron` | |
| `history.archive.lease_seconds` | `recorder.archive.lease` | `60` is `1m`; whole seconds |
| `history.archive.sweep_after_seconds` | `recorder.archive.sweep_after` | `7200` is `2h`; whole seconds |
| `ui.recorder.postgres.connection` | `recorder.postgres.connection` | |
| `ui.recorder.postgres.table_prefix` | `recorder.postgres.table_prefix` | |
| `ui.recorder.postgres.retention_hours` | `recorder.postgres.initial_retention` | `6` is `6h`; whole hours |
| `ui.recorder.postgres.store_output` | `recorder.postgres.store_completed_output` | |
| `ui.recorder.postgres.batch_rows` | `recorder.postgres.batch_rows` | |
| `ui.recorder.postgres.queue_max_mib` | `recorder.postgres.max_queue` | `128` is `128MiB` |

5. **Check the Postgres default.** `initial_retention` defaults to `24h`; the old key
   defaulted to 6 hours. It only matters when a worker creates the table: an existing
   table keeps the value in its comment. `drakkar-schema` keeps its own default of 6
   hours when you give no `--retention-hours`.
6. **Rename the keys of every resilience block**, in `recorder.postgres.resilience`,
   `fleet.registry.resilience` and `periodic.election.resilience`. The `breaker` block
   is gone.

| The old key | The new key | The new value of an old one |
|---|---|---|
| `timeout_seconds` | `timeout` | `5.0` is `5s` |
| `retries` | `max_retries` | |
| `backoff_seconds` | `backoff` | `0.2` is `200ms` |
| `backoff_max_seconds` | `max_backoff` | `5.0` is `5s` |
| `breaker.failure_threshold` | `breaker_failures` | |
| `breaker.cooldown_seconds` | `breaker_cooldown` | `30.0` is `30s` |

7. **Update log queries.** `recorder_started` has no `ignored_keys` field. It has no
   memory fields on a worker with no memory store.
8. **Check a process that reads the Postgres table's comment or the archive messages.**
   The load errors of the archive now start with `recorder.archive: `.

The new layout of a worker with no UI that records into its folder:

```yaml
storage:
  dir: /var/lib/drakkar
  shared_dir: /mnt/fleet
ui:
  enabled: false
recorder:
  sqlite:
    rotation_interval: 1h
  archive:
    retention: 30d
```

In code, `EventRecorder` takes a `RecorderConfig` as its first argument, and the
constructor keywords `archive` and `merged_retention_days` are gone: both come from
`config.archive`. `UIConfig.recorder`, `UIRecorderConfig`, `UITraceConfig`,
`SharedConfig`, `HistoryConfig` and `BreakerConfig` are gone; `HistoryArchiveConfig` is
`RecorderArchiveConfig`.

## Autodiscover {#autodiscover}

The worker registry and the election of cluster tasks are now one optional
feature, `autodiscover`, with one provider. It is **off by default**. The root
sections `fleet` and `periodic` are gone; a config that uses them fails the load.

**What to do:**

1. If your workers use the workers list, the cache exchange or a cluster task,
   turn the feature on: `autodiscover.enabled: true` and `autodiscover.provider`.
   A worker with it off publishes no card, reads no peers, runs no election and
   pulls no cache rows, and the workers list shows only this worker.
2. Choose one provider. You can no longer run the registry on one provider and the
   election on another. If your two providers differed, move both to the one
   whose service every worker reaches.
3. With `filesystem`, set `storage.shared_dir`. It no longer falls back to
   `storage.dir`: `autodiscover.provider filesystem needs storage.shared_dir` fails
   the load. Set `storage.shared_dir` to the folder you used before, and the card
   files and locks stay where they are.
4. A handler with `@periodic(..., scope='cluster')` needs the feature on, or the
   worker does not start. Turn the feature on, or give the task `scope='worker'`.
5. An empty `cluster_name` with the feature on logs `cluster_name_empty` once at
   startup. It changes nothing else: the lock scope stays `default`.
6. Move the keys:

| Before | Now |
|---|---|
| `fleet.registry.provider`, `periodic.election.provider` | `autodiscover.provider`, default `''`, required when enabled |
| `fleet.registry.connection`, `periodic.election.connection` | `autodiscover.connection` |
| `fleet.registry.heartbeat_seconds: 10` | `autodiscover.heartbeat: 10s` |
| `fleet.registry.refresh_seconds: 15` | `autodiscover.refresh: 15s` |
| `fleet.registry.offline_after_seconds: 60` | `autodiscover.offline_after: 1m` |
| `fleet.registry.prune_after_seconds: 86400` | `autodiscover.prune_after: 24h` |
| `fleet.registry.resilience.*`, `periodic.election.resilience.*` | `autodiscover.resilience.*`, one block; the registry and the election keep separate breakers |
| `fleet.cache_exchange.*` | `cache.exchange.*`, the same keys under the `cache` section |
| `periodic.tasks.*` | Removed. Change the `@periodic` declaration in code. |
| `fleet.registry.static.*`, `fleet.registry.filesystem.dir`, `fleet.registry.consul.*`, `fleet.registry.redis`, `fleet.registry.postgres`, `*.plugin_options` | Removed. |

As environment variables: `DK_FLEET__REGISTRY__*` and `DK_PERIODIC__ELECTION__*`
become `DK_AUTODISCOVER__*`, and `DK_FLEET__CACHE_EXCHANGE__*` becomes
`DK_CACHE__EXCHANGE__*`.

**What stays the same:** the `fleet_workers` and `task_locks` tables, the Redis
keys, the Consul paths, the card, the lock body, and the metrics and their labels
`feature="registry"` and `feature="election"`. A cluster keeps its cards and leases
through this change when it keeps its provider and its folder or connection.

**Messages and events:**

- A provider name outside the four now fails the load, with or without `enabled`:
  `autodiscover.provider must be one of consul, filesystem, postgres, redis, got '<v>'`.
- The log events `periodic_cluster_check_off` and `fleet_registry_disabled_no_dir`
  are gone. Both states now stop the worker with a message.
- With `autodiscover` off, an enabled `cache.exchange` logs
  `cache_exchange_off_no_autodiscover` once, and the peer route answers as it does
  for a switched-off exchange.

## The archive scope {#archive-scope}

The archive pass now has two scopes. See
[The two scopes](local-databases.md#archive-scopes).

- With `autodiscover.enabled` and `storage.shared_dir` set, a pass covers the whole
  cluster, as before. It runs with an election, and it writes
  `<cluster>-<from>__<to>.db.gz`.
- In every other case, a pass covers only this worker's files. It runs at the same slots
  with no election and no lease, and it writes `<cluster>-<worker>-<from>__<to>.db.gz`.
  `history.archive` is then not in the definition hash, and the worker counts nothing in
  `drakkar_periodic_cluster_slots_total` for it.

**What to do:**

1. Find workers that have `autodiscover` off, or that have no `storage.shared_dir`. Their
   archive now takes the worker's own sealed files in `<dir>/history/<cluster>/`, where
   `<dir>` is `storage.shared_dir`, else `storage.dir`. To keep the sealed files instead,
   set `recorder.archive.enabled: false`.
2. If a tool reads the archive folder, let it also accept the name with a worker. The name
   does not tell the cluster, because cluster names and worker names can both hold `-`.
   Take the cluster from the folder. The `cluster` field of the archive rows in
   `/api/v1/debug/databases` is now the folder name.
3. Delete a line such as `recorder: {archive: {enabled: false}}` that you added only because
   the workers have no shared folder. Each worker can now archive its own files.

**The own scope needs a stable worker name.** A pass finds its files by the worker name.
Sealed files and archives of a retired name stay until an operator deletes them. Set the
variable that `worker_name_env` names (default `WORKER_ID`) to a name that does not change
across restarts. A worker with no name gets a generated one that changes at each start, and
it logs `archive_own_scope_unstable_worker_name` at startup in this scope. Its sealed files
of earlier starts are never archived.

**Archives that stay in the folder after a change of scope:** a pass deletes only the
archives of its own scope. If a worker changes scope, delete its old archives by hand once
you no longer need them. Their sealed files are already gone.

**Log events and metrics:** `recorder_archive_pass_abandoned` has a second `detail` text for
the own scope, `the pass ends after the window in progress`. The help texts of
`drakkar_recorder_archive_bytes`, `drakkar_recorder_archive_files` and
`drakkar_recorder_archive_failures_total` changed.

**In code:** `archive_task_runs` is `archive_scope`, which answers `cluster`, `worker` or
`None`. `PeriodicRoster.runs_archive` is `PeriodicRoster.archive_scope`. `run_archive_pass`
takes `only_worker`, and needs `fence` or `only_worker`: with neither it raises `ValueError`.
`archive_file_name` takes `worker`.
