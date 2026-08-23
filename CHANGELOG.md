# Changelog

All notable changes to this project are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `sinks.delivery_timeout_seconds` (default 30.0): a budget for one sink
  delivery — framework-internal transient retries included — and for one
  sink close during shutdown. Sinks whose driver accepts one also apply it
  as their own transport timeout (asyncpg `command_timeout`, redis-py
  `socket_timeout`, PyMongo `socketTimeoutMS`), which were all unset. A sink
  whose server stopped answering on a still-open TCP connection previously
  blocked its partition forever: the circuit breaker only counts a failure
  when a call returns, so `/readyz` kept reporting ready while the worker
  did no work, and every rebalance then spent the whole drain budget
  waiting. A timeout is now a transient delivery failure naming the sink and
  the budget.

### Fixed

- The Postgres sink capped a multi-row `INSERT` at 65535 bind parameters,
  the wire-protocol limit, but asyncpg refuses a statement above 32767. Any
  batch past that failed and silently fell back to one statement per row —
  1000 rows of 33 columns is inside the fan-out this project targets, so
  large batches quietly stopped being batches. The cap is now the driver's.
  A batch that falls back is also reported: a `sink_batch_fallback_per_row`
  warning carrying the batch error, and a
  `drakkar_sink_batch_fallbacks_total` counter.

- The flight recorder writes its buffer in chunks of 5000 rows instead of
  one statement per flush, and hands the event loop back between chunks.
  Building the row tuples for a whole flush ran on the loop and stalled it
  for tens of milliseconds at high event rates. The chunk is now also the
  unit of loss: a chunk that keeps failing is dropped after
  `max_flush_retries`, where previously the entire buffer was — a
  fifteen-second SQLite stall could cost every event held.

- The flight recorder starts a flush early, without waiting out
  `flush_interval_seconds`, once its buffer is half full. A burst is written
  out instead of filling `max_buffer` and evicting events.

- An archive pass could merge and delete a *peer's* live database in a
  shared `db_dir`. Source selection excluded only symlinks and the pass's
  own database, and settledness read only the main file's mtime, which
  under WAL moves on checkpoint rather than on every write — so a worker
  rotating hourly saw a worker rotating daily as settled. The victim kept
  writing to an unlinked inode and disappeared from autodiscovery and cache
  peer sync. Every `<worker>-live.db` symlink target is now excluded
  whoever owns it, and settledness also reads the `-wal` sidecar.

- The flight recorder no longer buffers events it can never write. In
  memory-only mode (`db_dir` empty) and with `store_events: false` there is
  no flush loop, so the ring buffer filled to `max_buffer` and then counted
  every further event in `drakkar_recorder_dropped_events_total` — a
  permanently firing alert, and up to `max_buffer` events of dead memory
  holding whole subprocess stdout/stderr. The live WebSocket stream is
  unaffected.

- Every DLQ write waited about one second: `DLQSink.send()` produced the
  message but never flushed the producer, so the message sat in the client
  buffer until its inactivity timer fired. The DLQ is on the hot failure
  paths, so a sink outage throttled each partition to roughly one batch per
  second and ate the rebalance drain budget. `send()` now flushes, and it
  also checks the delivery report — a broker refusal arrives inside the
  report rather than as an exception, so a failed DLQ write used to be
  reported as confirmed and the source offsets committed past it.

- A worker whose startup failed after the flight recorder or the cache had
  opened its SQLite connection stayed alive forever instead of exiting.
  The startup sequence now runs the same shutdown path as the poll loop,
  so an unreachable broker or sink makes the process exit non-zero and the
  orchestrator restart it.

### Removed

- The `net_io` WebSocket heartbeat: the worker no longer samples network
  RX/TX rates (`/proc/net/dev`) or NFS read/write byte rates
  (`/proc/self/mountstats` `bytes:`), and `resource_sample` no longer
  carries `rx_bytes_total` / `tx_bytes_total`. Per-mount NFS RTT and
  retransmit health in `resource_sample` is unchanged (contract v1.19).

### Changed

- Throughput windows are now 1, 5, and 30 seconds only — the 60 s and
  300 s windows are gone from the `throughput` WS frame, the
  `worker_state.throughput` JSON, and the `window` label of
  `drakkar_throughput` / `drakkar_task_rate`. The Live timeline draws the
  5 s window by default (contract v1.19).

## [1.17.1] - 2026-08-23

### Changed

- Update dependencies: confluent-kafka 2.14.0 -> 2.15.0, pytest 9.0.3 ->
  9.1.1, pydantic 2.13.0 -> 2.13.4, pydantic-settings 2.14.2 -> 2.15.0
  (floors raised to match).

## [1.17.0] - 2026-08-23

### Added

- Worker-liveness detection (contract v1.18): each entry in the workers
  list now carries `last_seen_ts` (newest `worker_state.updated_at`
  heartbeat, falling back to the newest event timestamp; `null` when
  neither exists) and `online`. A worker is online when its heartbeat is
  no older than the new `ui.workers_offline_after_seconds` (default 30,
  env `DK_UI__WORKERS_OFFLINE_AFTER_SECONDS`); the worker answering the
  request is always online. Crashed or OOM-killed workers, whose
  `-live.db` symlink is never removed, now show as offline instead of
  looking healthy forever. Peer scan failures during discovery are
  logged (`recorder_peer_scan_failed`) instead of skipped silently.

- User-defined app config (contract v1.17): a handler declares its own
  Pydantic model (`app_config_model` / `app_env_prefix` class attributes)
  and the framework loads it from the reserved `app:` section of the same
  `drakkar.yaml`, plus env overrides under the handler's own prefix
  (`MYAPP_SCORING__URL`), with the framework precedence (defaults → YAML →
  env). Validated fail-fast at startup and exposed as `self.app_config`
  before `on_startup` runs. The config-reference endpoint gains a runtime
  `Application` group built from the model — descriptions, env names,
  defaults, and nesting like framework fields, secrets (`SecretStr` or the
  `drakkar_secret` marker) masked. `DK_APP__*` env vars are rejected with
  a pointer to the handler prefix; a non-empty `app:` section with no
  declared model logs `app_config_ignored`. Standalone loading via the new
  public `load_app_config()`. New docs page `docs/app-config.md`.

### Fixed

- Recorder: a flush interrupted by cancellation (UI timeout, shutdown) now
  re-queues its batch instead of losing it; all SQLite error types go
  through the same retry/drop accounting; `stop()` always completes its
  teardown.
- Recorder: `cross_trace_by_label` now scans peer databases off the event
  loop with the same scan budget as `cross_trace`.
- File sink: writes happen on a worker thread, not on the event loop; a bad
  path anywhere in a batch now fails before any byte is written.
- Sinks: deliveries that exhaust their retries now go to the DLQ under the
  configured `dlq.on_send_failure` policy instead of being dropped
  silently. A sink that is not connected raises instead of silently
  confirming. `deliver_all` settles every sink group before raising.
- Sinks: `RedisPayload` now rejects `script`/`keys`/`args` on non-script
  operations instead of silently ignoring them.
- Postgres sink: the connect log now redacts the full DSN, including
  password-style query parameters.
- Cache: one writer lock serializes flush, cleanup, and peer-sync commits;
  a failed flush rolls back its open transaction. A read no longer
  overwrites a newer concurrent write (or resurrects a delete) when it
  warms memory from SQLite. A deadline-cancelled peer-sync commit is
  awaited by the next cycle and by `stop()`.
- Pipeline: an exception from `on_window_complete` no longer skips the
  window's recorder event and offset commit. The shutdown drain processes
  the backlog in `window_size` chunks instead of one unbounded window.
- Archive merge: merged databases now carry `labels`, `origin`,
  `client_name`, and `request_id`, so archived events keep label tracing
  and webapp attribution. Merge and dbstats failures are logged instead of
  swallowed silently.
- Event loop: peer discovery scans, webapp start/stop waits, UI-server
  thread join, and watchdog file I/O all run on worker threads. The UI
  server now verifies its port bind at startup and fails loudly, matching
  the Go backend.
- The runtime-health routes now honor `ui.auth_token` like every other
  API route (they were the only router without the bearer-token gate).
- Documentation accuracy pass: correct stale claims (executor `PriorityGate`
  and process-group kill, precomputed-task metrics, webapp 429 body and
  ports, periodic metric names, recorder flush buckets, worker_state
  columns, cache browser defaults, filesystem sink example), repair three
  broken anchors, and document previously missing config sections
  (`throughput:`, `io:`, `offload:`, consume pause, probe details,
  annotations, custom sinks, DLQ security) across configuration.md,
  config-reference.md, features.md, handler.md, observability.md, and the
  docs home page.

### Changed

- Rewrite the README as a short GitHub-first overview: features, one
  quick-start example, and links into the docs site. Details that lived
  only in the README now live in the docs.
- Replace the architecture diagram on the docs home page with a clearer
  pipeline view: window, user hooks, subprocess pool, sinks, DLQ, and
  watermark commit.
- Condense AGENTS.md to the non-derivable facts: cross-backend contract,
  invariants, commands, and footguns.
- Test-suite cleanup: remove tests that re-tested libraries or duplicated
  other tests, replace wall-clock timing assertions with deterministic
  synchronization, and drop seven seconds of real sleeps from the rotation
  tests.

## [1.16.0] - 2026-08-21

### Added

- `io.max_threads`: size asyncio's default `to_thread` executor — the one
  pool every blocking call from handlers and the framework shares. Python
  caps it at `min(32, cpu_count + 4)`, which silently limits blocking-I/O
  fan-out on many-core hosts; 0 (the default) keeps Python's sizing. New
  docs page "Threads & Pools" maps the event loop, the executor
  subprocesses, the blocking-I/O pool, and the offload pool — which
  mechanism fits which work, and how to read saturation of each from the
  observability stack.

## [1.15.0] - 2026-08-21

### Changed

- `offload.max_threads` now defaults to `0` = automatic sizing:
  `ceil(executor.max_executors / 4)` with a minimum of 2 (pool 8 -> 2 threads,
  9 -> 3, 13 -> 4), so bigger executor pools get proportional offload
  headroom without a second knob. An explicit value still wins untouched.

### Added

- Task cost, speed, and throughput (contract v1.16, opt-in via
  `throughput.cost_label`): name a numeric task label that correlates
  with each task's computational hardness — file size, a computed score,
  any unit. Counted completions gain `cost` and `speed` (cost/duration)
  in their `task_completed` metadata and on `GET /api/v1/recent-tasks`
  rows; a per-second broadcast-only `throughput` WS frame carries
  sliding-window aggregates (throughput, task rate, task count) for the
  fixed window set 1/5/30/60/300 s; new Prometheus series
  `drakkar_task_speed`, `drakkar_throughput{window}`,
  `drakkar_task_rate{window}`; `worker_state` rows snapshot the windows
  as JSON in the new `throughput` column, so merged fleet databases
  replay throughput history. `throughput.min_cost` excludes tasks whose
  fixed overhead would fabricate misleading speeds; failed and
  precomputed tasks are never counted. New docs page: Throughput. The
  integration demo's main worker enables it with the scan target's byte
  size as the cost.

## [1.14.1] - 2026-08-20

### Fixed

- The first opt-in runtime probe now fires immediately after start. Before,
  the probe scheduler compared `time.monotonic()` — which counts from boot
  on Linux — against a zero baseline, so on a freshly started host the
  first `runtime_probe` event silently waited until host uptime exceeded
  `runtime_health.probe_interval_seconds`.

## [1.14.0] - 2026-08-20

### Added

- Host-pressure sampling (contract v1.15): `resource_sample` events now
  carry load averages, PSI pressure percentages, cgroup CPU-throttle
  deltas, and per-NFS-mount health (`ops`, average `rtt_ms`, `retrans`
  per interval). These answer "which resource is the host fighting for"
  during an incident and replay from recorded databases. New docs page:
  Host Pressure.
- Runtime lag episodes (contract v1.15): every degraded/stalled span now
  produces one `runtime_lag_episode` event with stack samples aggregated
  across the whole span, loop-thread CPU time, host evidence, and a
  verdict — `blocked`, `cpu_bound`, `starved`, or `inconclusive`. The
  `/runtime/health` snapshot reports the open episode with a running
  verdict, so the UI can show the diagnosis during the incident. This
  removes the old blind spot where diffuse starvation showed a `stalled`
  badge but "No stalls recorded".
- Opt-in runtime stack probes: set
  `runtime_health.probe_interval_seconds` above 0 to record a
  `runtime_probe` event with the loop thread's stack every interval — a
  low-rate flight-recorder profiler for production tuning.
- `worker_state` rows now carry `health_state` and `loop_lag_ms`, so a
  merged fleet database shows which worker was degraded at which time.
- The recorder warns at startup (`recorder_db_dir_network_fs`) when
  `ui.recorder.db_dir` resolves to a network filesystem — SQLite there
  risks lock corruption and makes the recorder share fate with the
  network path.
- New config keys `runtime_health.episode_max_seconds` (default 300) and
  `runtime_health.probe_interval_seconds` (default 0 = off).
- Timed consume pause (contract v1.14, opt-in via
  `ui.consume_pause.enabled`): the Live page gains preset buttons
  (15s/1m/5m/15m by default, configurable) that pause message intake for
  a bounded period, with a countdown banner and a Resume button. The
  pause uses partition pause/resume — the consumer group is never left,
  so no rebalance — auto-resumes at its deadline, coordinates with
  backpressure (never overrides it), and never touches stall-paused
  partitions. New gauge `drakkar_consume_pause_active`. Enabled in the
  integration demo's main cluster.
- Kafka Read API (contract v1.13): `GET /api/v1/debug/kafka/*` fetches
  one message by (partition, offset) or streams a time window as NDJSON.
  Topics are addressed by configured alias only (`source`, `dlq`, or a
  Kafka sink instance name) — raw topic names never appear in the API.
  Reads use assign()-only consumers: no consumer group is joined, no
  offsets move. Gated by `ui.auth_token` and the new
  `ui.kafka_read_enabled` flag (default true); startup logs a warning
  when Kafka security is on but the UI has no auth token.
- New cache scope `CacheScope.MEMORY`: the entry lives in worker memory
  only and is never flushed to SQLite. TTL, `peek`, `get`, and `in` work
  as usual. A memory-scoped `set` records a delete tombstone, so it also
  purges any disk row an earlier wider-scoped write left for the same
  key. Memory-scoped entries are lost on LRU eviction and worker
  restart, and the `/debug/cache` entries browser does not list them.

### Changed

- The cache docs page now opens with a quick-reference table of the five
  handler-facing methods and the rules of thumb for choosing between
  `get`, `peek`, and `in`.

### Fixed

- `GET /api/v1/live/overview` now uses a bounded main-loop dispatch: when
  the loop is wedged it answers with empty task maps and real pool
  numbers instead of hanging. Before the fix, the UI header could show
  "Pool: N / 0 slots" during an incident because the overview request —
  the only source of the pool maximum — never returned.

## [1.13.0] - 2026-08-14

### Added

- The Databases page is fast now: statistics are served from a shared
  `<db_dir>/.dbstats.db` cache keyed by `(path, mtime, size)` instead of
  full-scanning every file per page load. Rotated (immutable) files are
  scanned once ever — at rotation time; a background warmer
  (`ui.recorder.dbstats_warm_interval_seconds`) fills anything missing
  and purges entries for externally deleted files; the file *list* is
  always a live directory scan, so hand-deleted files disappear from the
  page immediately. Live DBs refresh via an incremental delta scan (only
  events past the cached max id). Cold files beyond
  `ui.recorder.dbstats_inline_scan_limit` per request render as
  "scanning…" rows and fill in automatically (contract v1.12).
- The Databases page now marks the file each worker is writing right now
  (`live_for` — an "in use" highlight resolved from the `-live.db`
  symlinks) and lists handler cache databases as typed rows (kind
  `cache`, entry count, shown under their stable `<worker>-cache.db`
  name). Every row carries a `kind`: recorder / merged / cache / unknown.

- NFS throughput readout: the `net_io` WS frame now carries optional
  `nfs_read_mib_s` / `nfs_write_mib_s` rates (plus byte totals) sampled
  from `/proc/self/mountstats`, and the Live page shows them next to the
  Net readout. This closes a container blind spot: kernel-NFS traffic
  moves through the *host's* network interfaces, so the namespace-scoped
  RX/TX counters never see it — a worker could read a GiB/s from NFS
  while `Net: RX` sat near zero. The mountstats counters follow the
  mount namespace and do see bind-mounted NFS volumes. Fields appear
  only when an NFS mount is visible (contract v1.11).

- `handler.offload()`: run CPU-bound hook work on a small thread pool
  instead of the event loop. A heavy computation in `arrange()` or an
  aggregation hook no longer stalls the whole worker — polling, task
  completions, sink flushes, and the UI keep running while it computes.
  Configured by the new `offload.max_threads` setting (default 2). Adds
  the `drakkar_offload_running` / `drakkar_offload_queued` gauges, the
  `drakkar_offload_duration_seconds` histogram, one `offload`
  flight-recorder event per call (visible in message traces and the
  History filter), and an `offload` object on `GET /api/v1/live/overview`
  (contract v1.10). See the new `docs/offload.md` page. The integration
  worker's `arrange()` uses it for its whole window-planning pass and
  serves as the worked example.
- The cache sync operations (`set` / `peek` / `delete` / `in`) are now
  thread-safe, so offloaded functions can use them. The async `get()`
  stays loop-only; the docs show the warm-then-peek pattern.

### Changed

- The probe-details "write cap exceeded" error now names the tripped cap,
  its configured limit, and the config key to raise —
  `ui.probe_details.max_writes` and `ui.probe_details.max_total_bytes`
  produce distinct messages. Before, both caps shared one message, so an
  operator could raise the wrong knob (or misspell its environment
  variable) and see no change.

## [1.12.0] - 2026-08-12

### Added

- Resource samples for postmortems: the recorder writes a
  `resource_sample` event every `state_sync_interval_seconds` with RSS,
  thread count, open file descriptors, CPU percent for the worker and its
  reaped subprocesses, and host network byte totals. The rows ride the
  ordinary events table, so rotation, archiving, and the WS stream carry
  them — an archive alone can show which resource moved during an outage.
  Fields whose source the platform lacks are omitted.
- Last-breath flush: a fatal exit that skips the clean shutdown path
  (startup failure after the recorder came up, an unhandled exception, a
  stray `sys.exit`) now salvages the recorder's unflushed buffer through a
  synchronous `atexit` hook, so the final seconds before a crash are
  written instead of lost. Best-effort; SIGKILL/OOM still cannot be
  intercepted.
- Host network throughput in the live UI: the recorder now broadcasts a
  WS-only `net_io` frame every `state_sync_interval_seconds` with RX/TX
  rates in MiB/s, read from `/proc/net/dev` (all non-loopback interfaces).
  The rates are host-wide for the network namespace — the worker plus its
  subprocesses, and any neighbours sharing the namespace. The frame is
  never written to the events table, and platforms without `/proc/net/dev`
  send nothing.
- Documentation: the Local Databases page now answers "which local SQLite
  databases exist, what is in them, and how do I configure them" in one
  place. New sections cover the memory-only mode (`ui.recorder.db_dir:
  ''`), what each `store_*` flag controls, and a table of the
  content-retention knobs (`store_stdin`, `stdin_max_bytes`,
  `output_min_duration_ms`, `event_min_duration_ms`). The Observability
  event catalog gained the `partition_stalled`, `runtime_health`, and
  `runtime_stall` rows and now lists the `queue_wait_ms` / `spawn_ms` /
  stdin metadata fields; its recorder config example shows all keys.

## [1.11.0] - 2026-08-11

### Added

- `metrics.task_label_histograms` config: a list of task label keys whose
  numeric values feed a new `drakkar_task_label_value` Prometheus histogram
  (one time series per key). Use it to track distributions of things your
  tasks label themselves with, such as an input file size. Values that do
  not parse as finite numbers are skipped.
- Subprocess spawn timing: each task now measures how long starting its
  subprocess took (fork/exec plus the event-loop scheduling delay around
  it). The figure is on `ExecutorResult.spawn_seconds`, in a new
  `drakkar_executor_spawn_seconds` histogram, and rides as `spawn_ms` in
  the `task_completed` event metadata for the live UI. Spawn time growing
  toward the task duration means the worker process — not the task binary —
  is the bottleneck.
- Queue-wait timing: each task records how long it waited for a free
  executor slot before any work began — a new
  `drakkar_executor_queue_wait_seconds` histogram, plus `queue_wait_ms` in
  the `task_started` event metadata for the live UI. Long waits with a
  busy pool mean the pool (or the CPUs behind it) is the bottleneck.
- Stdin capture: `ui.recorder.store_stdin` (default off) stores each
  task's stdin content, capped at `ui.recorder.stdin_max_bytes`, in the
  `task_started` event metadata. Failed tasks always store their stdin
  (capped) on the `task_failed` event, regardless of the flag — a failure
  without its input is half a fingerprint. No events-table schema change:
  the content rides the existing metadata JSON.
- Documentation: `docs/debugging-bottlenecks.md`, a runbook for hunting
  down a worker that periodically looks stuck — which signals to read in
  which order, and what each combination means.
- Host-capacity check at startup: the worker reads how many CPUs it can
  actually use (affinity mask capped by any cgroup CPU quota) and logs an
  `executor_pool_exceeds_cpus` warning when `executor.max_executors` is
  larger. New `drakkar_host_effective_cpus` and `drakkar_executor_pool_max`
  gauges make the same comparison alertable.

### Changed

- `drakkar_executor_duration_seconds` gains sub-second buckets down to
  10 ms. The old 100 ms floor put most real tasks in one bucket, which made
  percentile queries useless.
- Locked dependency versions moved up: prometheus-client 0.26.0 (runtime)
  and ruff 0.16.2 (development only). The declared version floors in
  `pyproject.toml` are unchanged.

## [1.10.0] - 2026-08-11

### Added

- `ui.timeline` config: `history_factor` and `max_age_minutes` control how
  much task history the timeline keeps, `color_rules` map task labels and
  fields to bar colors (first match wins), and `labels` name which task
  label fills the tag, caption, highlight, filter, and marker roles.
  `GET /api/recent-tasks` now accepts a `limit` query parameter (default:
  `history_factor` times the executor pool size, itself capped at 100000
  since neither factor has its own ceiling), clamps `minutes` down to
  `max_age_minutes`, and reports a `stdout_size` per task and a formal
  `truncated` flag. `GET /api/v1/identity` now reports the full `timeline`
  config.
- Declarative UI enrichment for probe-details fields and table columns:
  `probe_field()` and `Column` gain `link_template` (clickable values, with
  `{value}` / `{row.<field>}` / `{<base>}` template tokens), `badge_colors`
  (a new `view='badge'` that renders a value as a colored pill), `format`
  (`duration_ms` / `bytes` / `timestamp` / `number` display formatting), and
  `hint` (a hover tooltip). Documented in docs/ui-enrichment.md.
- `columns` on a `table` / `tables` / `tree` field: pick and order a subset
  of the row model's columns, and attach per-column enrichment via
  `Column(...)` instead of the row model's default full column set.
- `detail` on a `table` / `tables` / `tree` field: a row-click side panel
  (`Detail` / `Element` / `Link`) showing a richer view of one row —
  string/keyvalue/table blocks plus external links.
- `ui.link_bases` config: named URL bases (e.g. `{jira: 'https://jira.internal.example.com'}`)
  that probe-details link templates resolve against. `GET /api/v1/identity` now
  reports `link_bases` (empty object when unset).
- Startup warning when a probe-details template references a link base that
  `ui.link_bases` does not configure. Not a startup error — the affected
  links just render as plain text — but now visible in logs instead of
  discovered by clicking a dead link.
- Declared UI pages: a handler lists custom dashboard pages on a new
  `ui_pages` attribute, each a set of widgets reading from a built-in data
  source (events, annotations, tasks, metrics) with no new data endpoint
  and no client-side code. Validated at startup like probe-details, served
  on `GET /api/v1/pages`, and rendered by the UI at `/p/<slug>` with one
  nav entry per page. Documented in docs/ui-pages.md.
- Custom cell renderers: `ui.custom_renderers_path` points to a
  deployment-provided JavaScript module, served at `GET
  /api/v1/ui/renderers.js` and loaded once by the UI at startup. The
  module maps names to functions that build a cell's content directly, for
  presentation the built-in links/badges/formats can't express. New
  `view='custom'` plus `renderer='name'` on `probe_field()` and `Element`,
  and a `renderer='name'` option on `Column` (also usable on page table
  columns), name which function renders that field, column, or
  detail-panel element. A broken or missing renderer never blanks a cell
  or crashes the page — it falls back to the field's normal text. `GET
  /api/v1/identity` now reports `custom_renderers` (true when the module is
  configured). Documented in docs/ui-enrichment.md.
- The Python integration test harness (`integration/worker/`) now
  exercises the full UI customization surface end to end: link templates,
  badges, formats, hints, a `columns` subset, a `detail` panel using every
  element kind, a `matchBar` and a `patternChip` custom renderer
  (`custom-renderers.js`), and a declared `scan-activity` page combining a
  `TasksSource` table, a `MetricsSource` stat tile, an `EventsSource`
  table, and an `AnnotationsSource` table. `ui.link_bases` and
  `ui.custom_renderers_path` are configured in `drakkar.yaml` accordingly.
  Every scan task now also carries six labels (`file_name`, `module`,
  `file_size`, `file_size_bytes`, `lines`, `request`) feeding a worked
  `ui.timeline` block: color rules on empty output, big files, and
  vendored paths, plus all five tag/caption/highlight/filter/marker roles
  bound to those labels.
- Documentation: the Documentation table and Key Features list on the docs
  home page now list Annotations, Probe User Details, UI Enrichment,
  Declared UI Pages, Runtime Health, and Sink Write Operations, all of
  which already had a nav entry but no table row. The FAQ's Message Probe
  entry now mentions the User-defined tab, plus a new "Can I customize the
  debug UI?" entry. `docs/ui-enrichment.md` gains a `bytes`/`timestamp`/`number`
  format example and a custom-renderer detail-element example;
  `docs/ui-pages.md` shows the `ui.link_bases` config its opening example
  relies on; `docs/probe-user-details.md`'s "See also" links forward to
  both.
- Documentation: `docs/ui-customization-cookbook.md`, a five-step worked
  example building one handler from a plain probe-details tab through
  links/badges/formats, a detail popup, a declared page, and a custom
  renderer. Linked from `docs/ui-enrichment.md` and `docs/ui-pages.md`.
- Documentation: `docs/ui-timeline.md` covers the new `ui.timeline` config
  end to end — the history-depth model, the color-rule condition grammar
  and palette, the five label roles, per-browser role overrides, and a
  walkthrough of the integration harness's worked example. Linked from the
  docs home page's Documentation table and Key Features list, with a new
  FAQ entry ("Can I change task colors or keep more history on the
  timeline?"); `docs/config-reference.md`'s `ui:` block now includes the
  `timeline` fields.
- Automatic recorder archiving: rotated-out database files are merged per
  time window into one compressed archive, `<cluster>-<from>__<to>.db.gz`
  in `db_dir`, and the merged raw files are then deleted. Windows are UTC
  and `archive_window_hours` wide (default 24, so one archive per cluster
  per day), and a window is only archived once it ended a full window ago
  and none of its files were written in the last rotation interval.
  Workers that share a `db_dir` elect one archiver per cluster with a lock
  file, and each worker archives only its own cluster. A raw file is
  deleted only after the archive that contains it is on disk; a file the
  merge cannot read keeps its data and is renamed to `<name>.unreadable`.
  The merge, the compression and the file deletion run in a thread, so the
  pipeline never waits for them. Set `ui.recorder.archive_retention_days`
  to delete old archives too — it must cover at least two archive windows,
  or the worker refuses to start. `archive_enabled: false` turns the whole
  pass off, and the worker then logs at startup that it deletes no
  recorder files. `GET /api/v1/debug/archives` lists archive files
  (name/cluster/window bounds/size, parsed from the file name — no file is
  opened) and `GET /api/v1/debug/archives/{name}` downloads one, mirroring
  the existing database listing and download routes.

### Changed

- **Breaking (config):** `ui.recorder.rotation_interval_minutes` is renamed
  to `rotation_interval_hours`. The unit also changes: `1` now means 1 hour,
  not 1 minute. Update any config that sets this field.
- Integration harness: both workers now declare the same `CLUSTER_ID`, so
  cluster features of the debug UI (worker switcher groups, the timeline
  cluster view) are testable in the compose setup.
- Locked dependency versions moved up: redis 8.1.0 and structlog 26.1.0
  (runtime), ruff 0.16.1 and mkdocs-material 9.7.7 (development only). The
  declared version floors in `pyproject.toml` are unchanged.

### Removed

- **Breaking (config):** `ui.recorder.retention_hours` and
  `ui.recorder.retention_max_events` are removed. Age-based and count-based
  pruning of rotated database files is gone — archiving replaces deletion,
  and a raw file is now removed only after it is merged into an archive.
  The new `archive_enabled`, `archive_window_hours` and
  `archive_retention_days` fields control that behavior.

### Fixed

- UI enrichment now rejects three previously-silent boot-time mistakes:
  a duplicate column name in `columns=[...]`, an empty `badge_colors={}`
  on a `Column`, and an empty `elements=[]` on a `Detail` all raise
  `ProbeDetailsConfigError` instead of passing through to a degraded UI.
- Corrected docs/ui-enrichment.md: percent-encoding applies only to the
  `{value}`/`{row.*}` substitutions in a link template, not the base
  itself (inserted verbatim); an unmapped badge value with no `'*'`
  fallback renders as a neutral pill, not plain text.
- The recorder now survives an unexpected error in its background loops:
  flush, database rotation, and worker-state sync. Before, one error ended
  the loop for the life of the process: the worker stopped writing events,
  stopped rotating its database, or stopped recording its state, and wrote
  no log line about it. Each loop now logs the error and continues on the
  next tick. A loop that ends anyway logs
  `recorder_background_task_died`.
- `GET /api/recent-tasks` now reports a degraded read instead of freezing
  the page. When the recorder data cannot be read, the endpoint returns
  the normal object with an extra `unavailable: true` flag, not a bare
  array that made clients fail on `payload.tasks`. The UI server also logs
  why the read failed — no reader connection, or the main loop did not
  answer in time.

## [1.9.0] - 2026-08-08

### Added

- Runtime health monitor: continuous event-loop lag tracking with stall
  introspection. A heartbeat task measures lag every `tick_seconds`; a
  sampler thread captures stack traces of the code blocking the loop when
  the heartbeat goes silent past `stall_seconds`. State transitions and
  10-second samples persist as `runtime_health` flight-recorder events;
  each stall persists as a `runtime_stall` event with the captured
  stacks. New endpoints `GET /api/v1/runtime/health` (snapshot + lag
  history; answers even during a stall) and
  `GET /api/v1/debug/runtime/units` (task census grouped by suspension
  point). New metrics `drakkar_loop_lag_seconds`,
  `drakkar_runtime_health_state`, `drakkar_runtime_stalls_total`. New
  `runtime_health:` config section, documented in docs/runtime-health.md.
  Near-zero overhead when healthy: one clock read, one comparison, and
  one ring-buffer write per tick.

### Changed

- **Breaking (wire format):** `tables` probe-details fields now travel as an
  ordered array of `[group, rows]` pairs instead of a JSON object keyed by
  group. A JSON object cannot pin group order: JavaScript clients enumerate
  integer-like keys numerically first, and the Go backend sorted map keys.
  The pair array shows sub-tables in first-append order on every backend.
  The handler-side API (`probe.append(field, row, group=...)`) is unchanged.

### Fixed

- `probe.append` on a `tables` field now rejects a non-string `group` with a
  `ProbeError`. Before, an int group and its string form (`123` / `'123'`)
  collided as JSON keys at serialization time and one sub-table's rows
  silently replaced the other's.

## [1.8.0] - 2026-08-07

### Added

- The `task_completed` WebSocket frame now carries `stdout_lines` next to
  `stdout_size`. The field travels on the socket only — the pinned events-table
  row shape is unchanged. The Live page shows a Stdout column (lines and
  bytes, green when not empty) next to Stdin.
- New `tree` view for probe user details. A flat `list[RowModel]` field with
  `group_by=('file', 'section')` renders as a collapsible tree in the Message
  Probe's User-defined tab: one level per named row field (up to 4), a
  sortable table of the remaining columns at each leaf. Rows are added with
  plain `probe.append(field, row)` — the grouping keys travel inside the row,
  so group order is append order on every backend.

### Fixed

- The periodic scheduler tests now wait for their condition instead of a
  fixed wall-clock window. A slow CI runner could fail the suite when the
  first timer tick did not arrive inside the window.

## [1.7.0] - 2026-08-07

### Added

- New `tables` view for probe user details. A field typed
  `dict[str, list[RowModel]]` renders one sub-table per key in the Message
  Probe's User-defined tab. Use `probe.append(field, row, group='...')` to
  add rows; each group becomes its own sortable table, in first-append
  order. This supports a run-time number of tables — for example, one table
  per input file.
- The probe-details write caps are now settings: `ui.probe_details.max_writes`
  (default 10,000) and `ui.probe_details.max_total_bytes` (default 5 MB).

## [1.6.0] - 2026-08-07

### Added

- **Configs tab groundwork.** `drakkar/configmeta.py` generates a canonical,
  machine-readable description of every config field (path, env var,
  description, type, default, secret flag, docs anchor) from the pydantic
  config tree, committed as `drakkar/uiserver/config-metadata.json` for
  drakkar-go to vendor. Credential-bearing fields (`ui.auth_token`, Kafka
  SASL/TLS-key passwords, Postgres DSN, Mongo URI, Redis URL, HTTP sink
  headers, webapp client tokens) are now marked `drakkar_secret` in their
  field schema. `GET /api/v1/config-reference` joins that catalogue with
  the worker's live config values: dynamic per-instance fields (sink
  instances, including the nested Mongo per-instance statements) expand
  into one entry per configured instance plus the unexpanded template entry,
  so an unconfigured sink type still shows its possible keys. Every
  secret-flagged field with a non-empty live value is masked to `••••••`
  before it leaves the process; `webapp.clients` tokens are masked
  per-element since that field has no per-client path of its own.
- The message probe reports each task's command-line arguments (`args`), and
  its `binary_path` only when the handler overrode the configured binary.
- **Handlers can register a probe details model.** Set `probe_details_model`
  on your handler and describe each field with `probe_field(section=...,
  view=...)`; `probe.set` / `probe.append` / `probe.update` fill it in from
  any hook, and are no-ops outside a probe. The report gets a `user_details`
  object, rendered as a new User-defined tab in the Message Probe. The
  `integration/worker` example handler now shows this in practice: cache
  lookup tiers, per-task match analysis, pattern ranking, and sink-routing
  decisions.

## [1.5.0] - 2026-08-04

### Changed

- **Breaking.** `/ws` sends one message per batch, not one per event:
  `{"dropped": N, "events": [...]}`. Clients that read a single event per
  message must be updated.
- `/ws` accepts `?events=a,b` and streams only those event types.
- `/ws` never sends `stdout` or `stderr`. Read them from `/api/v1/task/{id}`.
- `/api/v1/recent-tasks` returns `truncated`, and caps `minutes` at 60. The
  scan is limited to `ui.max_rows * 3` events, newest first.
- The built-in UI stops polling while the browser tab is hidden.

### Fixed

- A busy worker could lose its live connection with `1011 keepalive ping
  timeout`. Captured output no longer travels on the socket, so the keepalive
  ping is not queued behind it.
- Dropped live events were silent. The server counts them and tells the client,
  which then resyncs instead of drifting. New metric:
  `drakkar_recorder_ws_dropped_events_total`.
- Each event was encoded once per connected client. It is now encoded once and
  shared.
- Deferred start events used one timer per task. One sweep now serves them all.
- Scanning a database file leaked a descriptor when the file could not be read.
  The databases endpoint is polled, so the leak grew over time.

## [1.4.2] - 2026-08-02

### Added

- **Handlers can expose their own diagnostics in the UI.** `self.annotate(target,
  kind, data)` attaches a structured payload to a source message, an executor
  task, or a whole `arrange()` window from inside any hook, and it shows up on
  that entity's trace in the debug UI. It answers the question the framework's
  own events cannot — not *what* happened, but *why the handler decided it*:
  the candidates a hook considered, the flag that shaped a task's arguments,
  the alternative it rejected.

  The scope comes from the target, so no coordinates are ever passed: a
  `SourceMessage` anchors to that message, an `ExecutorTask` to that task, and
  `None` to the window. The framework resolves partition, window, and offsets
  from an ambient hook context it binds around every hook call.

  Annotations are ordinary rows in the flight recorder's `events` table under a
  new `annotation` event type — no schema column was added, so the pinned
  cross-backend event-row shape is unchanged. Recorder rotation and retention
  expire them like every other event, which is what makes them suitable for
  data that is worth keeping for a day and not worth keeping forever.

  Emission is best-effort and can never affect processing: `annotate()` does
  not raise, does not block, and only appends to the recorder's existing
  buffer. A payload that exceeds a budget is **dropped whole rather than
  truncated** — a half-written structured document still parses and still looks
  complete, so it misleads whoever reads it more effectively than a missing
  record does. Two budgets apply per hook invocation:
  `ui.recorder.annotation_max_bytes` (16 KiB) bounds one payload, and
  `ui.recorder.annotation_max_bytes_per_call` (256 KiB) bounds the total a
  single hook call can add, so one handler annotating a wide window cannot
  exhaust `retention_max_events` and evict every other event. Every drop
  increments `drakkar_recorder_annotations_dropped_total{reason}` and is logged
  with the payload attached; the log falls silent after five drops in one
  invocation while the counter keeps going, so alerting belongs on the metric.
  Set `ui.recorder.annotations_enabled: false` to turn the feature off and
  leave `self.annotate(...)` a no-op.

  See `docs/annotations.md`.

- The `arranged` recorder event's metadata now carries `window_id`, so an
  `arrange()` window can be correlated with the window-scoped annotations
  emitted from the same call.

## [1.4.1] - 2026-08-02

### Changed

- **Maintenance release: dependency and CI updates, no functional change.**
  The locked versions the test suite and CI resolve move to `redis` 8.0.1,
  `pymongo` 4.17.0, `fastapi` 0.141.1, `pytest-asyncio` 1.4.0, and `ty`
  0.0.65. The declared floors in `[project.dependencies]` are unchanged, so
  what an installed py-drakkar resolves is unaffected.

  Two of those needed a change to absorb. FastAPI 0.141 includes routers
  behind a lazy wrapper that resolves only when a request arrives, which the
  OpenAPI route-parity test had to learn — the served surface is identical,
  but the test walked `app.routes` and could no longer see routes reached
  through an include. ty 0.0.65 additionally flags a narrowing gap in the
  Redis sink's sorted-mapping helper, where an `isinstance(value, dict)`
  check leaves the keys typed `object`; the invariant is now stated with an
  explicit cast. Neither alters runtime behaviour.

- The GitHub Actions used by the CI, docs and release workflows moved to
  `checkout@v7`, `deploy-pages@v5`, `upload-pages-artifact@v5` and
  `upload-artifact@v7`.

## [1.4.0] - 2026-08-02

### Added

- **The Mongo sink writes more than inserts.** `MongoPayload.op` selects
  `insert` (the default, so existing handlers are unaffected), `update_one`,
  `update_many`, `upsert`, `delete_one`, `delete_many`, or `statement`. The
  update ops take a `filter` serialized to an equality predicate and assign
  `data` through `$set`; `upsert` is insert-or-set on the same filter. One
  and many stay explicit rather than hiding behind a flag, because the blast
  radius differs by orders of magnitude.

  `filter` is required and may never be empty, guarded twice: the payload
  validator rejects a missing one at construction, and the build step rejects
  one that *dumps* empty. An empty Mongo filter matches every document, so
  `delete_many({})` would empty a collection outright.

- **Operator-authored MQL, invoked by name.** Statements declared under
  `sinks.mongo.<instance>.statements` are compiled at startup and run by a
  payload with `op='statement'` and bound `params`. This is the escape hatch
  for anything the declarative fields cannot express — `$inc`, `$push`,
  computed pipeline updates. Unlike the Postgres and Redis equivalents a
  statement is a structured model rather than a string, because MQL is data:
  it carries its own collection, op, filter, and update.

  Values bind through `":name"` placeholders — whole values only, never a
  fragment of a longer string, never a key, and with their type preserved so
  a numeric field still matches. `"::name"` escapes a literal leading colon.
  `$where` and `$function` are rejected at config load at any depth,
  including inside aggregation-pipeline stages, because both execute
  server-side JavaScript.

- The message probe now reports which operation a Mongo payload plans
  (`extras.op`, plus `extras.filter` for the ops that carry one); a statement
  reports its name as the record's `destination`.

- **The Redis sink issues more than SET.** `RedisPayload.op` selects one
  write command per data type — `set` (the default, so existing handlers
  are unaffected), `delete`, `expire`, `incrby`, `hset`, `hdel`, `push`,
  `trim`, `sadd`, `srem`, `zadd` — with the fields each one needs. A field
  the chosen op does not use is a validation error rather than a silently
  ignored value, and a required collection may not be empty.

- **Operator-authored Lua, invoked by name.** Scripts declared under
  `sinks.redis.<instance>.scripts` are registered at startup and run by a
  payload with `op='script'`, its `keys` and `args` passed as `KEYS` and
  `ARGV`. This is the escape hatch for multi-step or conditional logic, and
  the only way to get server-side atomicity — a pipeline is not a
  transaction. Values are never interpolated into the body, so message
  content cannot alter what runs, and DLQ entries and logs carry the script
  name rather than Lua that could leak row data. Every entry of `keys` is
  key-prefixed, so a script cannot reach outside its sink's namespace.

- `RedisSink.client` exposes the `redis.asyncio` client after connect,
  mirroring `PostgresSink.pool`. Reads stay out of the sink itself, so a
  read-modify-write cycle goes through this.

- The message probe now reports which command a Redis payload plans
  (`extras.op`); a script reports its name as the record's `destination`.

### Changed

- **Mongo deliveries are one ordered bulk write per collection run, and
  nothing is re-sent.** `bulk_write(ordered=True)` replaces `insert_many`:
  execution order equals payload order, execution stops at the first failure,
  and `writeErrors[*].index` names the offending payload exactly. A run can
  now carry heterogeneous operations, which `insert_many` could not express.

- **Mongo payloads batch only with adjacent same-collection neighbours.**
  Payloads were previously bucketed globally, which could execute a payload
  before its predecessor — harmless for inserts, a silently lost write once
  updates and deletes exist.

- `MongoSink` decides retry-safety per batch: updates, upserts and deletes
  converge, so those batches get the transient fast-retry, while any `insert`
  or `statement` payload vetoes it.

- PyMongo's `ConnectionFailure` and `NetworkTimeout` are remapped to the
  builtin `ConnectionError`/`TimeoutError` the sink manager matches, with the
  original chained. They inherit only from `PyMongoError`, so a dropped Mongo
  connection had never been eligible for the fast-retry — the same latent
  defect the Redis sink had. Nothing depended on it while the sink was
  unconditionally non-idempotent; per-batch retry makes it live.

- **Redis pipeline failures are attributed positionally and nothing is
  re-sent.** The pipeline now runs with `raise_on_error=False`, so a
  per-command error names its own payload while the commands that
  succeeded are left alone. The previous behaviour re-sent the batch, which
  is what made a non-idempotent command unsafe to batch at all.

- `RedisSink` decides retry-safety per batch: a batch containing `incrby`,
  `push`, or `script` is not fast-retried, because those accumulate or are
  opaque. Everything else converges and stays retry-safe.

- Redis mapping arguments are emitted in sorted key order (`hset` fields,
  `zadd` members) so both backends issue identical commands;
  caller-supplied lists (`hdel` fields, `sadd`/`srem` members) keep their
  order. New shared corpus: `tests/fixtures/redis_commands.json`.

### Removed

- **The Mongo `_id`-stripping fallback**, and the duplicate writes it
  knowingly accepted. It existed only because the per-document replay re-sent
  documents the failed batch had already written, and PyMongo writes a
  generated `_id` back into every document it is handed — so a resent
  document raised a duplicate-key error on the FIRST document rather than the
  guilty one. Positional attribution removes the replay, so the workaround
  and its cost are both gone. This supersedes the fix shipped in 1.3.0 rather
  than reverting it.

### Fixed

- **Dead-lettered payloads lost their body.** Every sink payload declares
  `data` as `BaseModel`, and pydantic serializes against the declared type
  rather than the instance — so `model_dump_json()` emitted `"data": {}`.
  `DLQSink` serializes payloads exactly that way, which meant every
  dead-lettered record reached the DLQ topic without the data it exists to
  preserve, and `scripts/replay_dlq.py` would have replayed blank rows. All
  six payload types were affected, plus `PostgresPayload`'s `where` and
  `params`. Nothing warned: a user's model genuinely is a `BaseModel`, so
  pydantic considered it correctly serialized.

  The bodies are now annotated `SerializeAsAny`, which restores duck-typed
  serialization, and each payload type has a round-trip test plus one through
  the real `DLQMessage.serialize()` path. The Go backend was never affected —
  it marshals the concrete value — so this also closes an undocumented
  divergence on a surface the parity contract calls byte-stable.

### Added

- **The Postgres sink writes more than inserts.** `PostgresPayload.op`
  selects `insert` (the default, so existing handlers are unaffected),
  `update`, `upsert`, or `statement`. An `update` takes a `where` model
  serialized to an ANDed equality predicate, where a `None` value renders
  `IS NULL` rather than `= NULL`. An `upsert` takes `conflict` columns and
  an optional `update_columns` subset, and renders `DO NOTHING` when every
  inserted column belongs to the conflict target.

- **Operator-authored SQL, invoked by name.** Statements declared under
  `sinks.postgres.<instance>.statements` are compiled once at startup from
  `:name` placeholders to positional parameters and invoked by a payload
  with `op='statement'` and bound `params`. This is the escape hatch for
  SQL the declarative fields cannot express — value-dependent expressions
  and guarded predicates. Parameters are always bound, so message content
  can never reach the statement text, and DLQ entries and logs carry the
  statement name rather than SQL that could leak row data. New docs page:
  `docs/sink-write-operations.md`.

- The message probe now reports which operation a Postgres payload plans
  (`extras.op`, plus `extras.where` and `extras.conflict` for the
  operations that carry them); a named statement reports its name as the
  record's `destination`.

### Changed

- **Postgres payloads now batch only with adjacent same-shaped
  neighbours**, so the order statements reach the database always matches
  the order the handler returned them. Payloads were previously bucketed
  globally, which could execute a payload before its predecessor — harmless
  for inserts, a silently lost write once updates exist.

- `PostgresSink` decides retry-safety per batch: a batch of only `update`
  and `upsert` payloads gets the transient fast-retry, while any `insert`
  or `statement` payload vetoes it. Operator SQL is opaque to the
  framework, so it is never assumed idempotent.

- A `data` model that serializes to an empty mapping is now rejected when
  the statement is built, instead of reaching the database as a syntax
  error.

- **Postgres columns are now emitted in sorted order** rather than in the
  order the payload model declares its fields, and bound values follow the
  same sort. This closes a long-standing difference with the Go backend,
  which decodes payload data into a map with no field order to preserve and
  has always sorted. The emitted SQL is semantically unchanged — columns and
  values stay aligned — but the statement text differs, which matters if you
  assert on it or read it in query logs. `conflict` and an explicit
  `update_columns` keep the order you wrote them in.

## [1.3.1] - 2026-08-02

### Fixed

- **The Redis sink never retried a dropped connection, despite declaring
  itself safe to.** `RedisSink` sets `idempotent = True` so the framework
  retries it on transient errors, but the check matched Python's builtin
  `ConnectionError` and `TimeoutError` while `redis-py` raises its own
  classes, which inherit from `RedisError` instead. No Redis connection
  failure ever qualified. The sink now translates those two errors to
  their builtin equivalents, so a connection reset or timeout gets the
  bounded fast-retry before reaching `on_delivery_error`, and a Redis
  worker rides out a blip that previously surfaced as a delivery failure.
  The Go backend classifies errors structurally and has always retried,
  so this also removes a behavioural difference between the two backends.

  Command errors such as `WRONGTYPE` are deliberately left untranslated —
  retrying one would fail identically every time.

- **A failed Redis pipeline was silently retried key by key.** Any
  exception from the batched write was discarded and the whole batch
  re-sent as individual `SET`s. That masked real errors — including a
  defect that meant the batched path was never exercised by the test
  suite at all — and would double-apply any future command that
  accumulates rather than replaces. Pipeline failures now propagate, and
  transient ones are handled by the retry above.

### Changed

- The Redis sink's `idempotent` comment claimed that setting `EX` as part
  of `SET` prevented a retry from refreshing an already-written key. The
  opposite is true: a retried `SET … EX 3600` restarts the expiry window.
  The comment now records the real behaviour and why relative TTLs are
  still preferred over absolute deadlines.

## [1.3.0] - 2026-08-01

### Added

- **Kafka transport security.** `kafka.security` configures SASL (PLAIN,
  SCRAM-SHA-256/512, GSSAPI, OAUTHBEARER) and TLS including mutual TLS,
  so the framework can now reach managed and secured clusters —
  Confluent Cloud, AWS MSK, Aiven, Redpanda Cloud, and self-managed
  clusters behind SASL or TLS. It applies to every Kafka client: the
  consumer, Kafka sinks, the DLQ producer, and the DLQ replay reader.

  The default is `PLAINTEXT` and emits no client properties at all, so a
  worker that configures nothing connects exactly as before. Incoherent
  combinations (a SASL protocol with no mechanism, SCRAM without
  credentials, a mechanism on a non-SASL protocol, a TLS key without its
  certificate) now fail at startup instead of surfacing as an opaque
  librdkafka connection error at first poll.

  Passwords are `SecretStr` and never appear in `repr()` or
  `model_dump()`. Prefer the environment overrides — for example
  `DK_KAFKA__SECURITY__SASL_PASSWORD` — over YAML literals; `DK_*`
  variables are already withheld from executor subprocesses.

  A Kafka sink or DLQ whose `brokers` field is empty inherits the
  consumer's brokers *and* its security together. Setting `brokers`
  makes that client self-contained; if it then carries no security while
  the consumer is secured, startup logs a `kafka_security_mismatch`
  warning naming it.

  See [Kafka security](docs/configuration.md#kafka-security-kafkasecurity).

- **`kafka.client_config`** — a raw librdkafka escape hatch, merged after
  `security` so it wins, for properties the typed block does not model.
  Four keys are rejected at startup because each backs a delivery
  invariant: `enable.auto.commit`, `partition.assignment.strategy`,
  `group.id`, `bootstrap.servers`. The same field exists on Kafka sinks
  and the DLQ.

- A `kafka_security` startup log line reports the negotiated protocol and
  mechanism (never credentials). The one-line config summary is
  deliberately unchanged, to preserve byte-parity with the Go backend.

- CI now scans dependencies for known vulnerabilities on every run
  (`pip-audit` against the installed environment), backed by a weekly
  Dependabot job that tracks both Python package and GitHub Actions
  updates.
- A scheduled nightly workflow runs the full Docker-based integration
  harness against real Kafka, Postgres, Mongo, and Redis.

- **HTTP sink body encodings.** `sinks.http.<name>.encoding` selects the
  request body format: `json` (the default, unchanged), `form`
  (`application/x-www-form-urlencoded`), or `multipart`
  (`multipart/form-data`, fields only). For the form encodings the payload
  model is flattened to fields sorted by name, with non-string values
  rendered as compact JSON. Both backends emit byte-identical bodies, with
  two documented exceptions: floats render in each language's native form
  (`42.0` in Python, `42` in Go), and a `json`-encoded payload containing
  U+2028 or U+2029 differs because Go's JSON encoder escapes those two
  characters unconditionally (recorded as divergences #25 and #26 in the
  Go backend).

### Changed

- **An HTTP sink that sets a `Content-Type` header now fails at startup.**
  The `encoding` setting owns the Content-Type, so a `Content-Type` header
  is now rejected even when it agrees with the body it would have
  produced. For `encoding: json` — the default, and the only encoding
  that existed before this change — a header of `Content-Type:
  application/json` was previously correct and worked; so was
  `application/json; charset=utf-8`, which is now unrepresentable, since
  the `charset` parameter can no longer be expressed at all. Per RFC
  8259, UTF-8 is JSON's default charset, so receivers should be
  unaffected. Remove the header, or set `encoding` to the format you
  intended.

- **The MongoDB sink now uses PyMongo's async client instead of the
  deprecated `motor` driver.** No configuration change is required, and
  `motor` is no longer a dependency.
- The test suite now installs `httpx2` alongside `httpx` so Starlette's
  `TestClient` stops warning about the older client. This is a test-only
  change — production code (`drakkar/sinks/http.py`,
  `drakkar/uihost/fetch.py`) still uses `httpx`.
- Fixed-duration sleeps in the test suite that only waited for a
  condition were replaced with condition polling, and a flaky
  echo-duration assertion now bounds against measured wall-clock time
  instead of a near-vacuous positivity check. This does not meaningfully
  change suite runtime; it removes a source of intermittent failures.
- The minimum test coverage floor rose from 75% to 95%.

### Fixed

- The Kubernetes reference manifests in `deploy/k8s/` configured a retired
  `debug:` config section and a `DK_DEBUG__AUTH_TOKEN` environment
  variable, either of which prevents a worker from starting. They now use
  `ui:` and `DK_UI__AUTH_TOKEN`, and a test loads every shipped manifest
  through the real config loader.
- Reaping a subprocess after `SIGKILL` is now bounded at 5 seconds, so a
  process wedged in uninterruptible I/O can no longer hang worker
  shutdown.
- The README's trust-model section now describes all three
  `kafka.on_parse_error` policies (`skip`, `dlq`, `raise`) instead of
  only the default, so the documented behavior for an unparseable
  message matches what actually happens.
- The MongoDB sink's per-document fallback (used when a batch insert
  fails) now strips the `_id` PyMongo writes back into a document's
  dictionary before resending it. Previously that leftover `_id` made the
  retry collide with the document Mongo had already inserted, so the
  fallback reported the wrong document as the failure and gave up before
  reaching the one that actually failed. On a partly-failed batch the
  error now identifies the document that really caused it, and every
  document ahead of it is delivered — at the cost of documents the failed
  batch had already written being written again under a new `_id`.
  Documents after the failing one are still not attempted in that call,
  unchanged from the pre-batching behavior.

### Security

- **`executor.env_inherit_deny` additionally withholds `*PASSWD*` and
  `*SALT*` from subprocess environments.** If a handler binary relies on
  a parent environment variable matching either pattern, pass it
  explicitly via `executor.env` or `ExecutorTask.env`.
- The flight recorder redacts a broader set of secret-looking variable
  names before writing them to its debug database — `*AUTH*`,
  `*PRIVATE*`, `*CERT*`, `*SALT*`, `*PASSWD*`, and `*KEY*` anywhere in the
  name, rather than only as a `_KEY` suffix.
- The reference Kubernetes deployment now runs unprivileged (non-root
  UID/GID, all Linux capabilities dropped, a `RuntimeDefault` seccomp
  profile) with a read-only root filesystem.
- Upgraded the dependencies flagged by the new CVE scan.

## [1.0.0] - 2026-07-03

First stable release. Earlier 0.x releases were pre-stable development
snapshots and are not individually documented here.
