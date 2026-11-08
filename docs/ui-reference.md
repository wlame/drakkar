# Operator UI reference

This page lists the values that the operator UI shows, page by page and tab
by tab. For each value it says what the value means, where the UI reads it,
how often it changes on screen, and which Prometheus metric carries the same
value. "UI only" means that no metric carries it.

The pages that explain a feature in depth are linked from each section. This
page does not repeat them.

Every value comes from the worker that serves the UI, unless the section says
otherwise. With [autodiscover](fleet.md) on, the worker switcher in the header
opens the UI of another worker.

## Values that are easy to misread

| Value | What it is | What it is not |
|-------|------------|----------------|
| **Consumer Lag** (Dashboard tile) and **Lag** (All Partitions) | Broker lag: the high watermark of each partition minus the committed offset, from the broker. Metric `drakkar_consumer_lag`. | Not the work this worker holds. A worker that has taken many messages and not committed them can show a small lag. |
| **Pending** (All Partitions) | Pending offsets: messages this worker has taken from a partition and not yet committed. Metric `drakkar_offset_lag`. | Not broker lag. The metric name `drakkar_offset_lag` keeps its old name; it counts pending offsets. |
| **Queue** (All Partitions) | Messages polled from Kafka that wait in the partition queue for `arrange()`. Metric `drakkar_partition_queue_size`. | Not the tasks that wait for an executor slot. Those are the **waiting** count of the Live page header. |
| **CPU throttled** (Runtime tab, Host) | The share of the last sample interval (10 seconds by default) in which the cgroup CPU quota kept the container descheduled. | Not an average over a long time. One value covers one interval only. |
| **Avg RTT / op** (Runtime tab, NFS table) | The average server round trip of one NFS operation in the last sample interval. | Not the round trip of the last operation, and not a peak. |
| **PSI** values and **Load** (Runtime tab, Host) | Values the kernel computes for every process it counts in `/proc/pressure` and `/proc/loadavg`. On a plain container that is the whole host. | Never the pressure or load of this worker alone. Other workloads on the host move them. |
| **Heartbeat age** and **Current lag** (Runtime tab) | Lag is how late the event loop ran the last heartbeat, measured when the heartbeat runs. Heartbeat age is the time since the last heartbeat ran, read off the event loop. | During a hard stall the loop runs no heartbeat, so the lag cannot grow; the heartbeat age grows. Read the heartbeat age to see a stall that is happening now. |
| **Window peak** (Runtime tab) | The largest lag of one second over the whole lag window (`runtime_health.history_window`, 15 minutes by default). | Not the same window as `drakkar_loop_lag_peak_seconds`, which covers the last 60 seconds. |
| **Total Events**, **Consumed**, **Completed**, **Failed**, **Produced** (Dashboard tiles) | Counters since the worker started. | Not counts of stored rows. The per-partition counts in All Partitions are counts of stored rows. |
| **OK** and **Errors** (Periodic Tasks tab) | Counts over the newest 500 periodic run rows of the recorder, all tasks together. | Not totals since the start. For totals use `drakkar_periodic_task_runs_total`. |
| **Sched p99** and **Goroutines** (Runtime tab, Host) | Shown only when the backend sends these keys in its host sample. | Without them the cards are hidden; no card means no value, not zero. |

## Header

The header is the same on every page.

| Label | Meaning | Source and updates | Metric |
|-------|---------|--------------------|--------|
| Cluster badge | The cluster name of the worker. | `GET /api/v1/identity` `cluster`, once at load. | UI only (the `cluster` target label in Prometheus, if you set one) |
| Version badge | The version of the UI bundle. The panel adds the backend name and version and the served UI version. | The UI build and `/api/v1/identity`, once at load. | `drakkar_worker` (`version` label) |
| Sink icons | Links to the external UI of each sink that has a `ui_url`. | `/api/v1/sinks`. | UI only |
| Docs link | The operator docs site of the deployment (see [Operator Docs Site](ui-docs.md)). | `/api/v1/identity` `docs`. | UI only |
| Worker switcher | The workers of the registry, grouped by cluster, with their state (when it is not `running`), backend and version, and `offline`. Shown only with autodiscover on. | `GET /api/v1/workers`, every 10 seconds while the page is visible. | `drakkar_fleet_peers{state}` (counts only) |

## Dashboard (`/`)

Source: `GET /api/v1/dashboard` every 5 seconds, `GET /api/v1/partitions`
every 5 seconds, `GET /api/v1/sinks` every 2 seconds, and
`GET /api/v1/recent-tasks` every 10 seconds. A section whose poll fails three
times in a row shows "stale since" with the time of its last good answer.

### Tiles

| Label | Meaning | Metric |
|-------|---------|--------|
| Uptime | Time since the worker started. | `time() - process_start_time_seconds` (Linux) |
| Partitions | Partitions assigned to this worker now. | `drakkar_assigned_partitions` |
| Executor Pool | Tasks running now / the size of the pool (`executor.max_executors`). | `drakkar_executor_pool_active` / `drakkar_executor_pool_max` |
| Total Events | The sum of the consumed, completed, failed, produced and committed counters since the worker started. A commit counts once per commit. | UI only |
| Consumer Lag | Broker lag summed over the assigned partitions: high watermark minus committed offset. The worker asks the broker at each poll. See [Operator UI](observability.md#operator-ui) for the rule when the broker reports no committed offset. The tile uses the broker's committed offset, while `drakkar_consumer_lag` uses the commits of this worker only, so a partition that this worker has not committed yet has a value here and no metric sample. | `sum(drakkar_consumer_lag)` |
| Consumed | Messages consumed since the worker started. | `drakkar_messages_consumed_total` |
| Completed | Tasks whose subprocess exited with code 0, since the worker started. | `drakkar_executor_tasks_total{status="completed"}` |
| Failed | Tasks that failed, since the worker started. | `drakkar_executor_tasks_total{status="failed"}` |
| Produced | Payloads delivered to Kafka sinks, since the worker started. | `drakkar_sink_payloads_delivered_total{sink_type="kafka"}` |

With `ui.prometheus.url` set, a tile label has a small link to a Prometheus
graph of the same value for this worker.

### WebApp tile

Shown when `sources.http.enabled` is true. See [Webapp](webapp.md).

| Label | Meaning | Metric |
|-------|---------|--------|
| In-flight | HTTP requests the runner processes now. | `drakkar_webapp_inflight` |
| Success (60s) | Requests completed in the last 60 seconds, counted from recorder events. | Partial: `increase(drakkar_webapp_requests_total{status="ok"}[1m])` |
| Errors (60s) | Requests that timed out or whose result was dropped after a timeout, in the last 60 seconds. | Partial: `drakkar_webapp_requests_total{status="timeout"}` and `drakkar_webapp_dropped_after_timeout_total` |
| Rejected (60s) | Requests refused by the rate limit or the authentication, in the last 60 seconds. | Partial: `drakkar_webapp_requests_total{status=~"rate_limited\|auth_failed"}` |
| Clients | The configured clients and their requests-per-minute caps. | `drakkar_webapp_rpm_limit` |

### Task Stats

The strip that the Live timeline also shows: `n`, `avg`, `p50`, `p90` and
`p99` of the task durations, computed in the browser over the finished tasks
of the last `ui.timeline.max_age` minutes. A second row does the same for a
numeric task label that you pick.

| Label | Meaning | Metric |
|-------|---------|--------|
| duration | Subprocess run time of the tasks in the window. | Partial: `drakkar_executor_duration_seconds` (all tasks, not the window) |
| a label key | Values of that task label. | Partial: `drakkar_task_label_value{label}`, for the labels in `metrics.task_label_histograms` |

### Assigned Partitions, Prometheus Metrics, Links

The partition numbers assigned now, then the graph links of
`ui.prometheus`, grouped by category, the cluster-wide links, and
`ui.custom_links`. These are links, not values.

### All Partitions

One row per partition that the recorder has rows for, also partitions that
this worker no longer holds.

| Column | Meaning | Metric |
|--------|---------|--------|
| Partition | The partition number. | -- |
| Status | `live` when this worker holds the partition now, else `history`. | UI only |
| Last Consumed, Last Committed | Time of the newest `consumed` and `committed` row of the partition. | UI only |
| Committed | The committed offset, from the broker; the worker's own last commit when the broker reports none. | UI only |
| High WM | The high watermark, from the broker. | UI only |
| Lag | High WM minus Committed. The page uses the broker's committed offset, while `drakkar_consumer_lag` uses the commits of this worker only, so a partition that this worker has not committed yet has a value here and no metric sample. | `drakkar_consumer_lag{partition}` (refreshed every 10 seconds from cached watermarks, so it can differ a little) |
| Queue | Messages waiting in the partition queue. | `drakkar_partition_queue_size{partition}` |
| Pending | Offsets taken and not yet committed. | `drakkar_offset_lag{partition}` |
| Consumed, Completed, Failed | Counts of the `consumed`, `task_completed` and `task_failed` rows that the recorder holds for the partition. With a recorder file, the rows of the live file; with the recorder off or on `postgres`, the rows of the memory window. See [what reads memory](recent-events.md#what-reads-memory-and-what-reads-the-file). | Partial: `drakkar_messages_consumed_total{partition}` (since start) |

### Sinks

One row per configured sink.

| Column | Meaning | Metric |
|--------|---------|--------|
| Type, Name | The sink type and instance name. | -- |
| Deliveries | Delivery calls that succeeded, since the worker started. One call carries one or more payloads. | UI only |
| Payloads | Payloads in those calls. | `drakkar_sink_payloads_delivered_total` |
| Errors | Failed delivery attempts, plus deliveries skipped by an open circuit. | Partial: `drakkar_sink_deliver_errors_total` (failed attempts only) |
| Retries | Retries asked for by `on_delivery_error`. | `drakkar_sink_delivery_retries_total` |
| Last Delivery, Duration | Time and duration of the last successful delivery call. | Partial: `drakkar_sink_deliver_duration_seconds` |
| Last Error | The text of the last error, with URL credentials removed. `circuit open` when the circuit skipped a delivery. | Partial: `drakkar_sink_circuit_open` |

## Live (`/live`)

Source: the WebSocket `/ws` (task, arrange, annotation, runtime and
`resource_sample` events, as they happen), plus a resync every 5 seconds
(`/api/v1/recent-tasks`, `/api/v1/live/overview` and the result lists). The
**Frozen** button stops the screen from moving; data still arrives.

### Page header

| Label | Meaning | Source and updates | Metric |
|-------|---------|--------------------|--------|
| WS | The state of the WebSocket: `connecting`, `connected`, `disconnected`, `unauthorized` or `forbidden origin`. | The browser. | UI only |
| Pause consuming / Consuming paused | The [debug pause](consume-pause.md) control, and while a pause is active its banner with the time left. Shown with `ui.consume_pause.enabled`. | `GET /api/v1/debug/consume-pause`; while paused, polled again. | `drakkar_consume_pause_active` |
| Offload: running / max busy, queued | Calls that run on the [offload pool](offload.md) now / the pool size (`threads.offload`), and calls that wait for a pool thread. | `/api/v1/live/overview`, every 5 seconds. | `drakkar_offload_running`, `drakkar_offload_queued` |
| Pool: active / max slots, waiting | Tasks running now / `executor.max_executors`, and tasks that wait for a free slot. | Task events of the WebSocket, and `/api/v1/live/overview` every 5 seconds. | `drakkar_executor_pool_active`, `drakkar_executor_pool_max`, `drakkar_executor_pool_waiting` |

### Arrange tab

One row per `arrange()` call. Source: `arranged` events (the newest ones at
page load, then the WebSocket).

| Column | Meaning | Metric |
|--------|---------|--------|
| Partition | The partition of the window. | -- |
| Msgs | Messages in the window. | Partial: `drakkar_window_messages` |
| Tasks | The tasks the call made, by state: done, running, failed, unknown, then the total. | UI only |
| Arrange | Duration of the `arrange()` call. | Partial: `drakkar_handler_duration_seconds{hook="arrange"}` |
| Time | The time of the `arranged` event. | -- |
| Labels | The [message labels](handler.md#message_label) of the window. | UI only |

A click opens a side panel with the message offsets, the
[annotations](annotations.md) and the task ids with their states.

### Executors tab

The timeline: one lane per executor slot, one bar per task. Tasks shorter
than `ui.ws_min_duration` are not sent over the WebSocket, except failed
tasks. Bar colors, label roles and the history depth are explained in
[Timeline Tuning](ui-timeline.md); handler events on the timeline in
[Custom Timeline Events](ui-timeline-events.md). The Task Stats strip below
the timeline is the one of the [Dashboard](#task-stats). With autodiscover on,
**Cluster view** stacks the timeline of every worker of the cluster.

The hover strip of a bar:

| Label | Meaning | Metric |
|-------|---------|--------|
| Task | The task id. | -- |
| Slot | The executor slot of the task. | UI only |
| PID | The process id of the subprocess. | UI only |
| Ident | `p<partition>:o<first offset>` for a Kafka task, `<client>:<request id>` for an HTTP task. | -- |
| P / Origin | The partition, or `http` for an HTTP task. | -- |
| Status | `running`, `completed` or `failed`. | `drakkar_executor_tasks_total{status}` (counts) |
| Duration | Subprocess run time, from spawn to exit, measured off the event loop. `running` while it runs. | `drakkar_executor_duration_seconds` |
| Wait | Time the task waited for a free executor slot. | `drakkar_executor_queue_wait_seconds` |
| Spawn | Time to start the subprocess. | `drakkar_executor_spawn_seconds` |
| Run | Shown when there is something to add: the run time that the handler's `task_run_seconds` hook reported next to the process time, and "worker noticed the exit N ms late" when the delay is above 50 ms. See [Task run time](observability.md#task-run-time-and-the-exit-notice). | `drakkar_executor_exit_notice_seconds` |
| Start, End | The edges of the drawn bar. | -- |
| CLI | The arguments of the subprocess. | UI only |
| Stdin, Stdout | Size of the input and of the output (and the line count of the input while the WebSocket is connected). | Partial: `drakkar_pending_bytes` holds them while the task is pending |
| label chips, `env:` chips | The task labels and the environment variables of the task. | UI only |

### Task Results, Message Results, Window Results tabs

Shown when the handler defines `on_task_complete`, `on_message_complete` or
`on_window_complete`. Source: `/api/v1/live/task-results`,
`/api/v1/live/message-results` and `/api/v1/live/window-results`, every 5
seconds and when you open the tab. A click opens the count of sink outputs
per sink.

| Tab | Column | Meaning | Metric |
|-----|--------|---------|--------|
| Task Results | Exec | Subprocess run time. | `drakkar_executor_duration_seconds` |
| Task Results | Hook | Duration of `on_task_complete`. | `drakkar_handler_duration_seconds{hook="on_task_complete"}` |
| Task Results | Outputs | Sink payloads the hook returned. | UI only |
| Task Results | Status | `completed`, or `failed` with the exit code. | -- |
| Message Results | Hook | Duration of `on_message_complete`. | `drakkar_handler_duration_seconds{hook="on_message_complete"}` |
| Message Results | End-to-end | Time from the poll of the message to the end of `on_message_complete`: arrange, waits, subprocesses and hooks. | UI only |
| Message Results | Tasks | Tasks of the message that succeeded / failed / were replaced. See [Fan-out](fan-out.md). | UI only |
| Window Results | Window #, Duration, Tasks, Outputs | The window id, the duration of `on_window_complete`, the tasks of the window and the payloads returned. | `drakkar_handler_duration_seconds{hook="on_window_complete"}` |

### Runtime tab

The event-loop monitor. Source: `GET /api/v1/runtime/health` (served from the
monitor's memory, so it answers during a stall) and the newest 100
`runtime_stall` and `runtime_lag_episode` events. The tab reloads when it
opens and each time the WebSocket brings a `runtime_health`, `runtime_stall`
or `runtime_lag_episode` event. With `runtime_health.enabled: false` the tab
says so. See [Runtime Health](runtime-health.md).

| Label | Meaning | Metric |
|-------|---------|--------|
| State | `healthy`, `degraded` or `stalled`. It reads `stalled` while the heartbeat is silent for at least `runtime_health.stall`. See [States](runtime-health.md#states). | `drakkar_runtime_health_state` (0, 1, 2) |
| Current lag | How late the event loop ran the last heartbeat. | Partial: `drakkar_loop_lag_seconds` (histogram) |
| Window peak | The largest lag per second over the lag window. | Partial: `drakkar_loop_lag_peak_seconds` (last 60 seconds only) |
| Heartbeat age | Time since the last heartbeat ran. | `drakkar_runtime_heartbeat_age_seconds` |
| Sparkline | The largest lag of each second of the lag window. | Partial: `drakkar_loop_lag_peak_seconds` |
| Episode in progress | The verdict, the time so far, the peak lag, the stack samples and the CPU time of the event loop in an open lag episode. See [Lag episodes and verdicts](runtime-health.md#lag-episodes-and-verdicts). | UI only (closed episodes: `drakkar_runtime_lag_episodes_total{verdict}`) |
| Top blocking sites: Samples, Stalls, Total stall time, Last seen | The code locations seen in the captured stacks of the recent stalls. Total stall time is an upper bound, because the sites of one stall share its duration. | UI only |
| Stalls & episodes: When, Kind, Duration, Verdict, Stacks, Units | One row per stall or episode. A row marked `*` is known from the monitor's memory only; its event has not reached the recorder. | Stalls: `drakkar_runtime_stalls_total`; episodes: `drakkar_runtime_lag_episodes_total` |
| Unit census: Count, Name, Suspended at, Example | The asyncio tasks of the worker, grouped by where they wait. Only when you press **Sample now** (`GET /api/v1/debug/runtime/units`). A timeout means the event loop does not run now. | UI only |

**Host.** The newest host sample (`resource_sample` event, every
`recorder.sqlite.state_interval` with a recorder file, else every 10 seconds).
A card is shown only when the host gives its value. What each source means is
in [Host Pressure](host-pressure.md#what-is-sampled).

| Label | Meaning | Metric |
|-------|---------|--------|
| Load 1m / 5m | Load average of the whole host. | `drakkar_host_load{period}` |
| PSI cpu | Percent of the last 10 seconds in which some runnable task waited for a CPU. | `drakkar_host_pressure_percent{resource="cpu",kind="some"}` |
| PSI io some / full | Percent of time in which some (or all) non-idle tasks waited for I/O. | `drakkar_host_pressure_percent{resource="io"}` |
| PSI mem | Percent of time in which some task waited for memory. The `full` value is in the event but not on the card. | `drakkar_host_pressure_percent{resource="memory",kind="some"}` |
| CPU throttled | Throttled time of the sample interval as a percent of the interval, at most 100. | `100 * rate(drakkar_cgroup_cpu_throttled_seconds_total[...])` over your own range |
| Sched p99 | Shown only when the backend sends it. | UI only |
| Goroutines | Shown only when the backend sends it. | UI only |
| NFS mount, Avg RTT / op, Retransmits, Ops | For each NFS mount, over the sample interval: the average round trip of one operation (red from 100 ms), the requests sent again (red above 0), and the operations. | `rate(drakkar_nfs_rtt_seconds_total[5m]) / rate(drakkar_nfs_operations_total[5m])`, `drakkar_nfs_retransmits_total`, `drakkar_nfs_operations_total` |

## History (`/history`)

The newest recorder rows, at most `ui.max_rows`, filtered by partition, event
type and origin (`all`, `kafka`, `http`). Source: `GET /api/v1/events`, when
the page opens and when you press **Apply**; it does not refresh by itself.
With the recorder off the heading says that the rows cover the memory window.

| Column | Meaning | Metric |
|--------|---------|--------|
| Time, Event, Partition, Offset, Task ID | The row's time, event type and coordinates. An offset links to its trace. | -- |
| Duration | The duration the event carries (a task run, a hook, a flush). | UI only |
| Exit | The exit code of a task event; red when it is not 0. | UI only |
| Args / Metadata | The task arguments, or the metadata JSON of the event. A `resource_sample` row shows the full host sample here, also the values the Runtime tab does not show (`rss_bytes`, `threads`, `open_fds`, `cpu_self_pct`, `cpu_children_pct`). | Some of them: `process_resident_memory_bytes`, `drakkar_process_threads`, `process_open_fds` |

## Task detail (`/task/<id>`)

One task, rebuilt from its recorder rows. Source: `GET /api/v1/task/<id>`,
once when the page opens. With a recorder file the rows come from the file;
with the recorder off or on `postgres`, only the rows still in the memory
window.

| Label | Meaning | Metric |
|-------|---------|--------|
| Status | `running`, `completed` or `failed`. | -- |
| Partition, or Client and Request ID | The partition of a Kafka task, or the client and the request id of an HTTP task. | -- |
| Duration | Subprocess run time. | `drakkar_executor_duration_seconds` |
| Exit Code | The exit code; red when it is not 0. | UI only |
| PID | The process id of the subprocess. | UI only |
| Started, Finished | Time of the start and of the end event. | -- |
| Stdout Size | Size of the output, also when its content was not stored. | UI only |
| Labels, Environment Variables, Source Message Offsets, CLI | What the handler gave the task. | UI only |
| Stdin, Stdout, Stderr | The content when the recorder stored it, else its size and the key that controls it (`recorder.capture.*`). | UI only |
| Event Timeline | Every recorder row of the task, with annotations. | -- |

## Debug (`/debug`)

The heading is the one-line config summary of the worker. Each tab loads its
data when it opens; tabs without a poll have a **Refresh** button.

### Metrics

Every metric family of the worker's Prometheus registry with its type, help
text and current values, split into **framework** and **user** metrics.
Source: `GET /api/v1/debug/metrics`, when the tab opens and on **Refresh**.
The tab hides the `process_*`, `python_*` and `gc_*` families of the client
library; see [Process and Python runtime metrics](observability.md#process-and-python-runtime-metrics).
The values are the metrics themselves.

### Periodic Tasks

One row per periodic task name found in the newest 500 `periodic_run` rows of
the recorder. Source: `GET /api/v1/debug/periodic`, on open and on
**Refresh**. See [Periodic Tasks](periodic-tasks.md).

| Column | Meaning | Metric |
|--------|---------|--------|
| Task | The task name; `system` marks a framework task (`cache.flush`, `history.archive`, ...). | -- |
| Last Run, Duration, Status | Time, duration and outcome of the newest run. | Partial: `drakkar_periodic_task_duration_seconds{name}` |
| OK, Errors | Runs of the task among those 500 rows, by outcome. | Partial: `drakkar_periodic_task_runs_total{name,status}` (since start) |
| Recent | One dot per run of the newest 20, green for `ok`, red for `error`. | UI only |

A cluster task counts only on the worker that ran the slot.

### Message Trace

Finds every recorder row of one message (`partition:offset`) or of one task
label value. Source: `GET /api/v1/debug/trace` and
`/api/v1/debug/trace-by-label`, on demand. **search history** adds the sealed
history of the cluster, or the Postgres table of the recorder. See
[Cross-Worker Trace](observability.md#cross-worker-trace).

| Column | Meaning | Metric |
|--------|---------|--------|
| Time, Worker, Event, Task ID | The row and the worker that wrote it. | -- |
| Gap | Time since the previous row of the trace; red above 1 second. A large gap names the slow stage. | UI only |
| Duration | The duration the event carries. | UI only |
| Details | The main metadata of the row, or an annotation. | -- |

### Message Probe

Runs one message through the handler with no side effects and shows the
report: the parsed input, the arrange result, each task with its stdin,
stdout, stderr and exit code, the sink outputs it would write, the cache
calls, and the errors. It runs only when you press **Run**
(`POST /api/v1/debug/probe`). A message can come from the source topic, the
DLQ or a sink topic through the [Kafka Read API](kafka-read.md). The
**User-defined** tab is the handler's own view, see
[Probe User Details](probe-user-details.md). All values are UI only.

### DLQ

Shown when the worker serves the [Kafka Read API](kafka-read.md) and has a
DLQ topic. Lists the records of the DLQ topic in a time range (columns time,
`p:offset`, key, size, payload); a click shows the `dk-` headers as
**DLQ facts**, the payload and the source headers. Source: a stream of the
Kafka Read API, on demand. The record format is in
[The DLQ record](sinks.md#the-dlq-record). The listing is UI only; the
records written are counted by `drakkar_sink_dlq_messages_total` and
`drakkar_dlq_records_total{stage}`.

### Cache

Shown when `cache.enabled` is true. See [Cache](cache.md).

| Label | Meaning | Source and updates | Metric |
|-------|---------|--------------------|--------|
| Entries (memory), Bytes (memory) | Entries in the in-memory cache, and the sum of their sizes. | `GET /api/v1/debug/cache/stats`, every 5 seconds. | `drakkar_cache_entries_in_memory`, `drakkar_cache_bytes_in_memory` |
| Entries (DB), Bytes (DB) | Rows in the cache file, and the sum of their sizes. Refreshed by the `cache.cleanup` task (every minute by default). | Same. | `drakkar_cache_entries_in_db`, `drakkar_cache_bytes_in_db` |
| Key, Scope, Size, Updated, Expires, Origin | The rows of the cache file, filtered and paged. Entries of scope `memory` never reach the file and are not listed. | `GET /api/v1/debug/cache/entries`, on **Apply** and paging. | UI only |

### Databases

Where the worker keeps its data, and downloads of it. The four views (Files,
Postgres, Slice, Autodiscover) are described in
[The Databases View](databases-view.md). The view reads its data when it
opens and on **Refresh**; nothing polls. The values that a metric also
carries:

| View and value | Meaning | Metric |
|----------------|---------|--------|
| Files, memory row: events | Rows in the recorder memory store. | `drakkar_recorder_memory_rows` |
| Files, memory row: size | Data in use in the memory store (pages in use times the page size). The memory the store holds is larger; see [RAM to plan](recent-events.md#ram-to-plan). | `drakkar_recorder_memory_bytes` (the allocated size, `drakkar_recorder_memory_allocated_bytes`, is not shown) |
| Files, history: archives | The `.db.gz` archives and their sizes. | `drakkar_recorder_archive_files`, `drakkar_recorder_archive_bytes` |
| Files, backlog files | Closed recorder files not yet sealed. | `drakkar_recorder_unsealed_files` |
| Postgres, writer: rows waiting, written, dropped by reason | The state of this worker's writer to the recorder table. See [The Databases tab](recorder-postgres.md#the-databases-tab). | `drakkar_recorder_postgres_queue_rows`, `drakkar_recorder_postgres_rows_written_total`, `drakkar_recorder_postgres_dropped_rows_total{reason}` |
| Postgres, partitions: Size, Next upkeep | The size of each hour partition, and whether the next upkeep drops it. | Dropped partitions: `drakkar_recorder_postgres_partitions_dropped_total{reason}` |
| Autodiscover, workers: Liveness | `online`, `offline` or unknown, for each registry card. See [The Autodiscover view](fleet.md#autodiscover-view). | `drakkar_fleet_peers{state}` (counts only) |
| Autodiscover, leases | The holder of each cluster task lease, its age and the time left. | UI only (slots: `drakkar_periodic_cluster_slots_total{name,outcome}`) |

### Configs

Every config key of the worker: path, current value (secrets masked), the
`DK_` environment name and the description, grouped by section, with the
number of keys that differ from their default. The `app` group is the
deployment's own config. Source: `GET /api/v1/config-reference`, once when
the tab opens. All values are UI only. See [Config Reference](config-reference.md).

## Declared pages (`/p/<slug>`)

Pages that the handler declares with `ui_pages`. Their widgets show built-in
data sources; stat widgets refresh every 30 seconds. See
[Declared UI Pages](ui-pages.md).
