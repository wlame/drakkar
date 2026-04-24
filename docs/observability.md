# Observability

Drakkar provides four integrated observability layers: Prometheus metrics for alerting and dashboards, structured logging for event streams, a browser-based debug UI for real-time inspection, and a SQLite flight recorder for post-mortem analysis. All four are enabled by default and work together -- the debug UI reads from the flight recorder and links out to Prometheus graphs. See [Configuration](configuration.md) for the full YAML reference.

---

## Prometheus Metrics

Metrics are exposed via `prometheus_client` and served on a dedicated HTTP endpoint. The server starts automatically when `metrics.enabled` is `True` (the default).

```yaml
metrics:
  enabled: true
  port: 9090
```

### Metrics Reference

#### Worker Identity

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `drakkar_worker` | Info | `worker_id`, `version`, `consumer_group` | Worker instance identity, published once at startup |

#### Messages

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `drakkar_messages_consumed_total` | Counter | `partition` | Total messages consumed from the source Kafka topic |

#### Executor

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `drakkar_executor_tasks_total` | Counter | `status` (`started`, `completed`, `failed`) | Total executor tasks by outcome |
| `drakkar_executor_duration_seconds` | Histogram | -- | Task execution duration. Buckets: 0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300 |
| `drakkar_executor_pool_active` | Gauge | -- | Number of tasks currently running in the executor pool |
| `drakkar_executor_timeouts_total` | Counter | -- | Total tasks that exceeded `task_timeout_seconds` and were killed |
| `drakkar_task_retries_total` | Counter | -- | Total tasks retried after failure (via [on_error](handler.md#on_error) returning RETRY) |

#### Batches

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `drakkar_batch_duration_seconds` | Histogram | -- | Window/batch processing duration (arrange through last task completion). Buckets: 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600 |

#### Partitions

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `drakkar_partition_queue_size` | Gauge | `partition` | Messages waiting in the partition's internal queue |
| `drakkar_offset_lag` | Gauge | `partition` | Pending (uncommitted) offsets per partition |
| `drakkar_backpressure_active` | Gauge | -- | Whether the consumer is paused due to [backpressure](performance.md#backpressure). `1` = paused, `0` = flowing |
| `drakkar_total_queued` | Gauge | -- | Total messages buffered in all partition queues plus in-flight tasks |
| `drakkar_assigned_partitions` | Gauge | -- | Number of partitions currently assigned to this worker |
| `drakkar_executor_idle_slot_seconds_total` | Counter | -- | Accumulated slot-seconds of executor idle time while messages are waiting in queues. If 2 slots sit idle for 3 seconds while messages are queued, this increments by 6. Only counts when messages are in queues (not yet dispatched), not when the worker has nothing to do. |
| `drakkar_consumer_idle_seconds_total` | Counter | -- | Accumulated seconds with no messages available from Kafka and nothing in queues. Measures time the worker has genuinely nothing to do (consumer lag is zero). Does not count time when the consumer is paused due to backpressure. |

#### Consumer

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `drakkar_consumer_errors_total` | Counter | -- | Total Kafka consumer poll errors |

#### Offsets

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `drakkar_offsets_committed_total` | Counter | `partition` | Total offset commit operations |

#### Rebalancing

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `drakkar_rebalance_events_total` | Counter | `type` (`assign`, `revoke`) | Total Kafka rebalance events |

#### Sinks

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `drakkar_sink_deliver_duration_seconds` | Histogram | `sink_type`, `sink_name` | Duration of sink delivery operations. Buckets: 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5 |
| `drakkar_sink_deliver_errors_total` | Counter | `sink_type`, `sink_name` | Total sink delivery failures |
| `drakkar_sink_payloads_delivered_total` | Counter | `sink_type`, `sink_name` | Total payloads delivered to sinks |
| `drakkar_sink_delivery_retries_total` | Counter | `sink_type`, `sink_name` | Total sink delivery retry attempts |
| `drakkar_sink_deliveries_skipped_total` | Counter | `sink_type`, `sink_name` | Deliveries skipped via `on_delivery_error` returning SKIP |
| `drakkar_tasks_precomputed_total` | Counter | -- | Tasks whose result was supplied by the handler via `ExecutorTask.precomputed`, bypassing the subprocess. Framework is agnostic to the reason (cache hit, lookup, deterministic shortcut). Compare to `drakkar_executor_tasks_total{status="completed"}` for the short-circuit rate. |
| `drakkar_sink_dlq_messages_total` | Counter | -- | Total messages sent to the [dead letter queue](sinks.md#dead-letter-queue) |
| `drakkar_dlq_send_failures_total` | Counter | -- | Total failed attempts to send messages to the DLQ. When both the primary sink and DLQ fail, the payload is lost — alert on this counter. |
| `drakkar_sink_circuit_open` | Gauge | `sink_type`, `sink_name` | Per-sink [circuit breaker](sinks.md#circuit-breaker) state: `0.0` closed, `0.5` half-open, `1.0` open. Gauges are zero-initialized at sink registration so a never-tripped sink still appears in scrape output. Sustained `1.0` on a sink means its downstream has been down longer than the cooldown can recover from. |
| `drakkar_sink_circuit_trips_total` | Counter | `sink_type`, `sink_name` | Transitions *into* the open state per sink — both the initial failure-threshold trip and every half-open probe failure. A flapping circuit surfaces as a rising rate on this counter, not a single trip plus silent reopens. Alert on `rate(...[5m]) > 0` paired with non-zero `drakkar_sink_circuit_open`. |

#### Handler Hooks

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `drakkar_handler_duration_seconds` | Histogram | `hook` (`arrange`, `on_task_complete`, `on_message_complete`, `on_error`, `on_window_complete`, etc.) | Duration of user handler hook execution. Buckets: 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 30 |

#### Recorder

Emitted only when [`debug.enabled=true`](configuration.md#debug-flight-recorder-debug). Track flight-recorder health -- buffer depth, drops under load, and flush latency. Alert on sustained non-zero `dropped_events_total` or p99 `flush_duration_seconds` exceeding `flush_interval_seconds`.

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `drakkar_recorder_buffer_size` | Gauge | -- | Current depth of the in-memory event buffer. Updated on every `_record()` call and after each flush. A sustained value near `max_buffer` signals the flush loop can't keep up with incoming events -- raise `flush_interval_seconds` or `max_buffer`, or investigate disk latency. |
| `drakkar_recorder_dropped_events_total` | Counter | -- | Events evicted from the ring buffer because it was full at record time. Non-zero means event history has gaps; scale the buffer up or speed up flushes. This was previously a silent failure mode -- the counter makes it alertable. |
| `drakkar_recorder_flush_duration_seconds` | Histogram | -- | Duration of the full flush body (`executemany` + `commit`). Exposes disk-I/O latency tail. Buckets: 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1. Alert on p99 exceeding `flush_interval_seconds` -- flushes overlapping the next scheduled flush starve the buffer. |

#### Cache

Emitted only when [`cache.enabled=true`](cache.md). Memory gauges are maintained as running sums — Prometheus scrape reads a single int, never walks the in-memory dict. DB gauges are refreshed by the `cache.cleanup` loop (default every 60s).

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `drakkar_cache_hits_total` | Counter | `source` (`memory`, `db`) | Cache reads served from memory or from the DB fallback. `peek()` is **not** counted — only `get()` increments this. |
| `drakkar_cache_misses_total` | Counter | -- | Cache reads that returned `None` (key absent or expired in both memory and DB). |
| `drakkar_cache_writes_total` | Counter | `scope` (`local`, `cluster`, `global`) | Total `Cache.set()` calls, labelled by the entry's scope. |
| `drakkar_cache_deletes_total` | Counter | -- | Total `Cache.delete()` calls regardless of whether the key was present. Note: delete is local-only — see [sharp edge](cache.md#delete-is-local-only-the-main-sharp-edge). |
| `drakkar_cache_evictions_total` | Counter | -- | Entries popped from the in-memory dict due to the LRU cap (`max_memory_entries`). Sustained high rate signals cap undersizing. |
| `drakkar_cache_flush_entries_total` | Counter | `op` (`set`, `delete`) | Entries drained from the dirty map to SQLite per flush cycle, by op type. Measures flush throughput, not rows actually modified (LWW may reject a SET). |
| `drakkar_cache_cleanup_removed_total` | Counter | -- | Rows removed from the SQLite DB by the cleanup loop (entries whose TTL elapsed). A sudden spike means many entries expiring together. |
| `drakkar_cache_sync_entries_fetched_total` | Counter | `peer` | Rows pulled from a peer worker's cache DB by the sync loop, per peer. |
| `drakkar_cache_sync_entries_upserted_total` | Counter | `peer` | Rows the sync loop attempted to UPSERT into the local DB. Equals or less than `sync_entries_fetched_total` when LWW rejects some. |
| `drakkar_cache_sync_errors_total` | Counter | `peer` | Per-peer failures during the sync cycle (connection refused, corrupt DB, timeout). One increment per failed cycle; sync loop keeps running. |
| `drakkar_cache_peer_sync_timeouts_total` | Counter | -- | Worker-level count of peer-sync cycles that exceeded `cache.peer_sync.cycle_deadline_seconds` (or the derived `interval_seconds * 0.9` default). One tick per cycle that was cut short by the deadline. A sustained non-zero rate signals a slow-peer or peer-count-vs-interval imbalance — either tune the deadline up, stagger peers, or reduce the number of peers pulled per cycle. |
| `drakkar_cache_entries_in_memory` | Gauge | -- | Entries currently in the in-memory dict. Running sum, adjusted per set/delete/evict/cleanup. |
| `drakkar_cache_bytes_in_memory` | Gauge | -- | Sum of `size_bytes` across in-memory entries. Running sum (see above). |
| `drakkar_cache_entries_in_db` | Gauge | -- | Rows in the local `<worker>-cache.db`. Refreshed by the cleanup loop. |
| `drakkar_cache_bytes_in_db` | Gauge | -- | Sum of `size_bytes` across DB rows. Refreshed by the cleanup loop. |

Cache flush/sync/cleanup loop durations are captured by the existing
`periodic_task_duration{name="cache.flush|cache.sync|cache.cleanup"}`
histogram — no dedicated cache-only timing histograms.

### User-Defined Metrics

You can declare Prometheus metrics as class attributes on your handler -- see [Custom Prometheus Metrics](handler.md#custom-prometheus-metrics) for the handler-side setup. The framework auto-discovers them via `discover_handler_metrics()`, which scans the handler's class hierarchy (MRO) for any `prometheus_client` metric instances (`Counter`, `Gauge`, `Histogram`, `Summary`, `Info`).

```python
from prometheus_client import Counter, Histogram
from drakkar.handler import BaseDrakkarHandler

class MyHandler(BaseDrakkarHandler[MyInput, MyOutput]):
    # These are automatically discovered and shown in the debug UI
    items_parsed = Counter(
        'myapp_items_parsed_total',
        'Total items parsed from executor output',
    )
    parse_duration = Histogram(
        'myapp_parse_duration_seconds',
        'Time spent parsing executor output',
    )

    async def on_task_complete(self, result):
        with self.parse_duration.time():
            items = parse_output(result.stdout)
        self.items_parsed.inc(len(items))
        ...
```

User metrics are displayed alongside framework metrics on the debug UI's metrics page (`/debug`), tagged with source `user` to distinguish them from the built-in `drakkar_*` metrics.

### Scrape Configuration

Metrics are exposed at `:9090/metrics` by default (configurable via `metrics.port`).

=== "Static Targets"

    ```yaml
    # prometheus.yml
    scrape_configs:
      - job_name: drakkar
        static_configs:
          - targets:
              - 'worker-1:9090'
              - 'worker-2:9090'
              - 'worker-3:9090'
    ```

=== "Kubernetes Service Discovery"

    ```yaml
    # prometheus.yml
    scrape_configs:
      - job_name: drakkar
        kubernetes_sd_configs:
          - role: pod
        relabel_configs:
          - source_labels: [__meta_kubernetes_pod_label_app]
            regex: drakkar
            action: keep
          - source_labels: [__meta_kubernetes_pod_ip]
            target_label: __address__
            replacement: '${1}:9090'
    ```

---

## Structured Logging

Drakkar uses [structlog](https://www.structlog.org/) for structured, context-rich logging. By default, log output goes to **stderr** (standard Unix convention), but you can redirect to stdout or a file.

### Configuration

```yaml
logging:
  level: INFO       # DEBUG, INFO, WARNING, ERROR, CRITICAL
  format: json      # json | console
  output: stderr    # stderr | stdout | file path
```

- **`json`** (default) -- produces one JSON object per line, suitable for ingestion by Elasticsearch, Loki, Datadog, or any log aggregator.
- **`console`** -- human-readable colored output for local development.

### Output Destination

The `output` field controls where logs are written:

| Value | Behavior |
|-------|----------|
| `stderr` (default) | Standard error stream |
| `stdout` | Standard output stream |
| File path | Append to the specified file (created if missing, parent directories created automatically) |

File paths support template variables:

| Variable | Substitution |
|----------|-------------|
| `{worker_id}` | Worker name (from `WORKER_ID` env var or config) |
| `{cluster_name}` | Cluster name (from config or env var) |

```yaml
# Examples
logging:
  output: stdout

logging:
  output: /var/log/drakkar/{worker_id}.log

logging:
  output: /var/log/{cluster_name}/{worker_id}.jsonl
```

Environment variable override: `DRAKKAR_LOGGING__OUTPUT=/var/log/drakkar/worker.log`

### Automatic Context Fields

Every log line includes these fields, bound once at startup:

| Field | Source | Example |
|-------|--------|---------|
| `timestamp` | ISO 8601 timestamp | `2026-04-06T12:34:56.789Z` |
| `level` | Log level | `info` |
| `service_name` | Hardcoded | `drakkar` |
| `worker_id` | `worker_name_env` env var | `worker-3` |
| `consumer_group` | `kafka.consumer_group` config | `drakkar-workers` |
| `module` | Python module name | `drakkar.partition` |

### Per-Context Binding

The framework binds additional fields for specific operations using `structlog.contextvars`. These appear on all log lines within that context:

- `partition` -- bound during partition processing
- `hook` -- bound during handler hook execution (e.g., `arrange`, `on_task_complete`, `on_message_complete`)
- `task_id` -- bound during task execution and result handling

### Usage in Handler Code

```python
import structlog

logger = structlog.get_logger()

class MyHandler(BaseDrakkarHandler[MyInput, MyOutput]):
    async def arrange(self, messages, pending):
        await logger.ainfo(
            "Arranging batch",
            category='handler',
            message_count=len(messages),
            pending_count=len(pending.pending_task_ids),
        )
        ...
```

Output (JSON format):

```json
{
  "timestamp": "2026-04-06T12:34:56.789Z",
  "level": "info",
  "event": "Arranging batch",
  "service_name": "drakkar",
  "worker_id": "worker-3",
  "consumer_group": "drakkar-workers",
  "module": "myapp.handler",
  "category": "handler",
  "message_count": 50,
  "pending_count": 12
}
```

---

## Debug UI

The debug web UI is a FastAPI application served in a separate thread so that CPU-intensive executor tasks on the main event loop do not block the interface. Enabled by default on port 8080.

```yaml
debug:
  enabled: true
  port: 8080
```

### Pages

#### `/` -- Dashboard

The landing page with a high-level overview of worker health:

- **Partition tiles** -- one tile per assigned partition showing queue depth and status.
- **Pool utilization** -- active tasks vs. max workers.
- **Event counters** -- consumed, completed, failed, produced, committed totals.
- **Consumer lag** -- total lag across all assigned partitions.
- **Custom links** -- configurable via `debug.custom_links` with template variables (`{worker_id}`, `{cluster_name}`, `{metrics_port}`, `{debug_port}`).
- **Prometheus graph links** -- when `debug.prometheus_url` is configured, each stat card links directly to a Prometheus graph filtered to this worker. Worker-scoped links are grouped by category (Throughput, Latency, Health, Errors). Cluster-wide links appear when `debug.prometheus_cluster_label` is set.

#### `/partitions` -- Partition Overview

Per-partition stats table:

- Queue size (messages waiting)
- Pending offsets (uncommitted)
- Consumer lag (from Kafka high watermark)
- Committed offset and high watermark
- Consumed, completed, and failed event counts
- Live/dead status indicator

#### `/partitions/{id}` -- Partition Detail

Paginated event browser for a single partition. Shows all recorded events (consumed, arranged, task_started, task_completed, task_failed, committed) in reverse chronological order with 50 events per page.

#### `/sinks` -- Sink Delivery Stats

Overview of all configured sinks with live delivery statistics:

- Delivered count and payload count
- Error count and retry count
- Last delivery timestamp and duration
- Last error message and timestamp
- Links to external sink UIs (when `ui_url` is configured on a sink)

#### `/live` -- Live Pipeline View

Real-time view of the processing pipeline, fed by WebSocket (`/ws`). Organized in tabbed sections:

- **Arrange** -- active [arrange()](handler.md#arrange-required) calls with partition, duration, and [message labels](handler.md#message_label).
- **Executors Timeline** -- visual timeline of running, pending, and recently finished tasks. Slot-based layout matching `max_executors`. Tasks shorter than `ws_min_duration_ms` are hidden (except failures, which are always shown). See [Duration Thresholds](#duration-thresholds) for threshold tuning.
- **Collect** -- recently completed tasks with exit codes and durations.

Pool utilization bar shows active, waiting, and available slot counts.

#### `/history` -- Event Browser

Filterable, paginated event browser across all partitions:

- Partition filter dropdown
- Event type toggles (consumed, arranged, task_started, task_completed, task_failed, committed, etc.)
- 100 events per page with forward/backward pagination

#### `/debug` -- Debug Tools

Multi-purpose debug page with:

- **Metrics viewer** -- live snapshot of all registered Prometheus metrics (framework and user), accessible via the `/api/debug/metrics` JSON endpoint.
- **Periodic tasks** -- status of all `@periodic` handler methods: last run time, duration, success/error status, total counts, and a sparkline of recent runs.
- **Cross-worker message trace** -- two search modes:
    - **By partition:offset** -- enter `partition:offset` (e.g., `5:42`) to trace a message through the full pipeline.
    - **By label** -- search inputs are auto-generated for each [label](handler.md#task-labels) key found in the database (e.g., `request_id`, `pattern`). Enter a value to find all tasks and events matching that label. Useful for tracing a specific request across partitions and workers.
    Both modes search the current worker first, then other live workers in the cluster, then rotated historical DB files.
- **Database management** -- list all debug database files with event counts and sizes, download individual files, merge multiple files into one, and view per-file breakdown by event type.
- **Message Probe** -- replay a single pasted message through the handler pipeline (`arrange` → executor → `on_task_complete` → `on_message_complete` → `on_window_complete`) with **zero footprint**: no sink writes, no offset commits, no event-recorder rows, no cache writes, no peer sync. The report shows every task's stdout/stderr/exit code, each hook's returned `CollectResult`, the payloads that *would have been* written to each sink, every cache call, a timeline waterfall, and any exceptions raised at any stage. Posts to `/api/debug/probe` with a JSON body containing the message's `value` (and optional `key`, `partition`, `offset`, `topic`, `timestamp`, `use_cache`).

#### `/task/{id}` -- Task Detail

Detailed view of a single task's lifecycle:

- **Status** -- running, completed, or failed
- **Labels** -- user-defined [labels](handler.md#task-labels) from [ExecutorTask](executor.md#executortask)`.labels`
- **Duration** -- wall-clock execution time
- **PID** -- OS process ID
- **CLI** -- reconstructed command line (`binary_path` + `args`)
- **stdout/stderr** -- captured process output (subject to `output_min_duration_ms` threshold)
- **Source offsets** -- which Kafka offsets this task covers
- **Event timeline** -- chronological list of all events for this task_id (started, completed/failed, task_complete)

---

## Flight Recorder

The flight recorder is a SQLite-based event log that captures the full processing lifecycle. It provides the data backing for the debug UI and survives worker restarts (within retention limits).

### Configuration

```yaml
debug:
  db_dir: /tmp                      # Directory for SQLite files; '' disables disk persistence
  store_events: true                # Write processing events to the events table
  store_config: true                # Write worker config (enables autodiscovery)
  store_state: true                 # Periodic state snapshots
  flush_interval_seconds: 5         # Buffer flush interval
  max_buffer: 50000                 # In-memory event buffer size
  rotation_interval_minutes: 60     # Rotate to a new DB file every N minutes
  retention_hours: 24               # Delete DB files older than this
  retention_max_events: 100000      # Cap total events across DB files
  store_output: true                # Include stdout/stderr in event records
```

### Database Schema

Each database file can contain up to three tables, controlled by the `store_events`, `store_config`, and `store_state` flags:

#### `events` -- Processing Events

Stores processing events across the full pipeline lifecycle.

Key columns: `ts`, `dt`, `event`, `partition`, `offset`, `task_id`, `args`, `stdout`, `stderr`, `exit_code`, `duration`, `output_topic`, `metadata` (JSON), `pid`, `labels` (JSON).

Indexed on `(partition, offset)`, `ts`, `dt`, `task_id`, `event`, and `labels` (partial, where not null).

**Event types:**

| Event | When recorded | Key fields |
|-------|--------------|------------|
| `consumed` | Message polled from Kafka | `partition`, `offset` |
| `arranged` | `arrange()` completes for a window | `partition`, `metadata` (message_count, task_count, offsets, message_labels) |
| `task_started` | Subprocess launched (after semaphore acquired) | `task_id`, `partition`, `args`, `pid`, `labels`, `metadata` (source_offsets, slot) |
| `task_completed` | Subprocess finished with exit 0 | `task_id`, `duration`, `exit_code`, `stdout`, `stderr`, `pid`, `labels` |
| `task_failed` | Subprocess failed (non-zero exit, timeout, crash) | `task_id`, `duration`, `exit_code`, `pid`, `labels`, `metadata` (exception) |
| `task_complete` | `on_task_complete()` hook finishes for one task | `task_id`, `partition`, `duration`, `metadata` (output_message_count) |
| `message_complete` | `on_message_complete()` hook finishes for one source message | `partition`, `offset`, `duration`, `metadata` (task_count, succeeded, failed, replaced, output_message_count) |
| `window_complete` | `on_window_complete()` hook finishes for one arrange() window | `partition`, `duration`, `metadata` (window_id, task_count, output_message_count) |
| `produced` | Kafka payload delivered to output topic | `partition`, `offset`, `output_topic` |
| `sink_delivered` | Sink delivery succeeds | `metadata` (sink_type, sink_name, payload_count, duration) |
| `sink_error` | Sink delivery fails | `metadata` (sink_type, sink_name, error, attempt) |
| `committed` | Kafka offset committed | `partition`, `offset` |
| `assigned` | Partition assigned during rebalance | `partition` |
| `revoked` | Partition revoked during rebalance | `partition` |
| `periodic_run` | Periodic task execution completes | `task_id` (task name), `duration`, `exit_code` (0=ok, 1=error), `metadata` (status, error) |

Fields subject to [duration thresholds](#duration-thresholds): `args`, `stdout`, `stderr` are omitted for fast tasks below `output_min_duration_ms`. Events below `event_min_duration_ms` are not stored at all.

#### `worker_config` -- Autodiscovery

Single-row table written at startup (and after each rotation). Contains the full worker identity and configuration: `worker_name`, `cluster_name`, `ip_address`, `debug_port`, `debug_url`, `kafka_brokers`, `source_topic`, `consumer_group`, `binary_path`, `max_executors`, `task_timeout_seconds`, `max_retries`, `window_size`, `sinks_json`, `env_vars_json`.

This table is what enables the worker autodiscovery feature -- other workers scan for it in shared `db_dir`.

!!! warning "Secrets are redacted before they reach disk"
    The recorder SQLite file is downloadable via the debug UI. To avoid
    publishing credentials through that path, three redactions are applied
    before any env data is written:

    - **`kafka_brokers`** — embedded credentials are stripped from SASL
      URIs. `SASL_SSL://alice:s3cret@host:9094` becomes
      `SASL_SSL://***:***@host:9094`. Host and port remain visible.
    - **`worker_config.env_vars_json`** — values for env var names matching
      secret patterns (`*PASSWORD*`, `*SECRET*`, `*TOKEN*`, `*_KEY`,
      `*API_KEY*`, `*CREDENTIAL*`, `*_DSN`) are replaced with `***`. For
      other names, any URL-shaped value has its embedded `user:pass@`
      credentials stripped.
    - **per-task `env` metadata** — `task.env` values written by your
      handler in `arrange()` are sanitized with the same patterns before
      the `task_started` event is recorded. The original task object is
      not mutated; only the recorded copy is redacted (the subprocess
      still receives the real values). This closes the last env-related
      leak into the debug UI.

    The contract is "aggressive redact, accept false positives":
    `PASSWORD_RESET_URL` is redacted because it matches `*PASSWORD*` even
    though a reset URL isn't a credential. Rename the var if you need the
    value visible. The `ExecutorConfig.env` (framework-level) values are
    **never written** to the recorder at all — they only reach the
    subprocess environment.

    This protects operators who add secret-named vars to `expose_env_vars`
    without thinking, DSNs passed through otherwise-innocuous var names
    (`UPSTREAM_URL`, `CACHE_URL`, etc.), and per-task env values that
    handlers assemble from inbound message payloads.

#### `worker_state` -- Periodic Snapshots

Appended every `state_sync_interval_seconds` (default: 10). Captures a point-in-time snapshot: `uptime_seconds`, `assigned_partitions`, `partition_count`, `pool_active`, `pool_max`, `total_queued`, cumulative counters (`consumed_count`, `completed_count`, `failed_count`, `produced_count`, `committed_count`), and `paused` flag.

### File Layout and Rotation

Database files follow the naming pattern:

```
{db_dir}/{worker_name}-{timestamp}.db
```

For example: `/tmp/worker-1-2026-04-06__14_55_00.db`

A symlink `{worker_name}-live.db` always points to the currently active database file while the worker is running:

```
/tmp/worker-1-live.db -> worker-1-2026-04-06__14_55_00.db
```

The symlink is removed on graceful shutdown.

**Rotation**: every `rotation_interval_minutes` (default: 60), the recorder opens a new timestamped DB file before closing the old one (no query gap). The worker config is re-written to the new file and the live symlink is updated.

**Retention**: after rotation, files older than `retention_hours` (default: 24) are deleted. An additional cap limits the total number of files based on `retention_max_events / 10,000`.

### Buffer Mechanics

Events are not written to SQLite immediately. They accumulate in an
in-memory ring buffer (`collections.deque` with `maxlen=max_buffer`,
default 50,000). A background task flushes the buffer to disk every
`flush_interval_seconds` (default: 5) as a batch INSERT. This design:

- Avoids per-event disk I/O (thousands of events/sec would bottleneck SQLite)
- Keeps the main event loop fast (recording an event is a deque append, ~1us)
- Provides a short window of recent events even when `db_dir` is empty (in-memory only mode)

If the buffer fills before the next flush (e.g., during a burst),
oldest events are evicted (ring buffer behavior). Drops are no longer
silent — the `drakkar_recorder_dropped_events_total` counter
([see Recorder metrics](#recorder)) increments on every evicted
event, and the `drakkar_recorder_buffer_size` gauge shows the current
depth. Alert on sustained non-zero drops and raise `max_buffer` or
shorten `flush_interval_seconds` when they appear.

On shutdown, the buffer is flushed one final time before closing the
database.

### Merging Databases

The debug UI's database section (`/debug`) allows selecting multiple
database files and merging them into a single file. This is useful for:

- **Post-mortem analysis** -- combine files from multiple workers and
  time periods into one queryable database
- **Cross-worker correlation** -- merged files contain events from
  all selected workers, with `worker_name` preserved in the
  `worker_config` table
- **Archival** -- download a merged file for offline analysis with
  any SQLite tool

The merge process copies all events from selected files into a new
timestamped file (`merged-{timestamp}.db`), deduplicates by
`(ts, event, partition, offset, task_id)`, and creates a combined
`worker_config` table listing all source workers. The merged file
appears in the database list and can be downloaded.

---

## Duration Thresholds

Drakkar provides four independent duration thresholds that control what gets logged, broadcast, and stored. These are essential for high-throughput workers where fast tasks would otherwise flood every observability channel.

```yaml
debug:
  log_min_duration_ms: 500
  ws_min_duration_ms: 500
  event_min_duration_ms: 0
  output_min_duration_ms: 500
```

### `log_min_duration_ms` (default: 500)

Controls which completed/failed tasks produce a structlog entry. Tasks finishing faster than this threshold are silent in the log stream. Keeps logs focused on slow or problematic tasks.

### `ws_min_duration_ms` (default: 500)

Controls which tasks appear in the live UI (`/live` page). When a task starts, its WebSocket broadcast is deferred. If the task completes before the threshold, neither the start nor the completion event is sent to connected browsers -- the task is invisible in the live view.

**Exception**: failed tasks are always sent to WebSocket regardless of duration. This ensures failures are never hidden.

### `event_min_duration_ms` (default: 0)

Controls which tasks are persisted to the SQLite flight recorder. When set to a value greater than 0, tasks completing faster than the threshold are not written to the `events` table. Set to `0` to store all events (the default).

This is useful for workers that process millions of fast tasks where full event recording would consume excessive disk space.

### `output_min_duration_ms` (default: 500)

Controls whether `args`, `stdout`, and `stderr` are stored for a task. Tasks completing faster than this threshold have their output omitted from the event record -- the event itself is still recorded (subject to `event_min_duration_ms`), but without the potentially large output fields.

Reduces storage for fast tasks where the output is uninteresting.

---

## Worker Autodiscovery

A shared `db_dir` between workers is **not required** for Drakkar to
function. Each worker operates independently -- its own Kafka consumer,
executor pool, sinks, and flight recorder work fine with a local
directory.

You can also disable the flight recorder database entirely by setting
`db_dir: ""` (empty string). The debug web UI and WebSocket live
streaming still work (events are held in memory only), but these
features are disabled: event history (`/history`), message trace,
label search, database download/merge, periodic task history, and
worker autodiscovery. The live pipeline view (`/live`) and dashboard
counters continue working since they read from in-memory state.

When workers share the same `db_dir` (e.g., a shared NFS mount,
Kubernetes PVC, or Docker volume), several additional debug features
become available:

- **Worker switcher** -- jump between worker UIs from a dropdown in the
  nav bar, without remembering ports or IP addresses
- **Cross-worker message trace** -- trace a message across all workers
  in the cluster by partition:offset or by label value
- **Database merge** -- combine flight recorder files from multiple
  workers into a single SQLite file for post-mortem analysis
- **Cluster-wide database browser** -- see all workers' DB files in
  one view, sorted and filtered

Without a shared `db_dir`, each worker's debug UI only sees its own
data. All other functionality (processing, sinks, metrics, logging)
works identically.

### How It Works

1. Each worker creates a timestamped SQLite file in `db_dir`
   (e.g., `worker-1-2026-04-08__14_30_00.db`) and maintains a
   `{worker_name}-live.db` symlink pointing to the current file.
2. Workers with `store_config: true` (default) write their
   configuration -- worker name, IP address, debug port, cluster
   name, Kafka settings -- to the `worker_config` table at startup
   and after each DB rotation.
3. The debug UI scans `db_dir` for `*-live.db` symlinks belonging
   to other workers.
4. For each live symlink, the UI reads `worker_config` to get the
   worker's name, address, and cluster membership.
5. Workers are grouped by `cluster_name` -- only workers in the same
   cluster appear together. Unclustered workers show in a separate
   group.
6. On graceful shutdown, the `*-live.db` symlink is removed so
   stopped workers don't appear in the dropdown.

### Worker Switcher

The debug UI navigation bar includes a worker dropdown that lists all
discovered workers, grouped by cluster. Clicking a worker navigates
to its debug UI (using `debug_url` if configured, otherwise
`http://{ip_address}:{debug_port}/`). The dropdown refreshes every
10 seconds.

### Setup

The minimal config for autodiscovery:

```yaml
# all workers must share the same db_dir path
debug:
  db_dir: "/shared"        # mount the same volume on all workers
  store_config: true       # default, writes worker_config table
  # optional: set cluster_name to group workers
cluster_name: "my-cluster"
```

In Kubernetes, use a shared PVC:

```yaml
volumes:
  - name: debug-shared
    persistentVolumeClaim:
      claimName: drakkar-debug

# mount on all worker pods at the same path
volumeMounts:
  - name: debug-shared
    mountPath: /shared
```

In Docker Compose, use a named volume:

```yaml
volumes:
  shared: {}

services:
  worker-1:
    volumes:
      - shared:/shared
  worker-2:
    volumes:
      - shared:/shared
```

### Cross-Worker Trace

The trace feature on the `/debug` page searches for a message's lifecycle
across all workers in the cluster. Two search modes are available:

**By partition:offset** -- given a partition and offset, finds the
consumed event and all related events (task_started, task_completed,
task_failed, task_complete, message_complete, produced, committed).

**By label** -- given a label key and value (e.g., `request_id=abc-123`),
finds all tasks whose [labels](handler.md#task-labels) match and returns
their full event lifecycle. This is useful when you know a business
identifier but not which partition or offset it landed on. Label search
inputs appear automatically on the `/debug` page for each label key
found in the database.

Both modes search in the same order:

1. The current worker's live database.
2. Other workers' live databases (same cluster).
3. Rotated (historical) database files in `db_dir`, newest first.

Each returned event carries a `worker_name` field identifying which
worker processed it. Labels are stored as JSON in the `labels` column
with a partial index (`WHERE labels IS NOT NULL`) for efficient
filtering.

---

## Periodic Tasks

The `@periodic` decorator schedules handler methods to run at fixed intervals alongside the main processing loop. See also [Periodic Tasks](handler.md#periodic-tasks) in the handler reference.

### Declaration

```python
from drakkar.periodic import periodic

class MyHandler(BaseDrakkarHandler[MyInput, MyOutput]):
    @periodic(seconds=60)
    async def refresh_cache(self):
        """Reload lookup table from database every minute."""
        self.lookup = await load_lookup_table()

    @periodic(seconds=300, on_error='stop')
    async def health_check(self):
        """Check downstream service health. Stop if it fails."""
        await ping_downstream()
```

### Behavior

- **Discovery**: at startup, the framework inspects the handler instance for methods decorated with `@periodic`. Only `async` functions are accepted -- decorating a sync function raises `TypeError`.
- **Scheduling**: each periodic task runs in its own `asyncio.Task` within the main event loop. The loop is: `sleep(seconds)` then `await coro_fn()`. The next interval starts only after the current invocation finishes, preventing overlapping runs.
- **Lifecycle**: periodic tasks are started after [on_ready()](handler.md#on_ready) completes and cancelled during graceful shutdown (before partition processors are drained).
- **Error handling**:
    - `on_error='continue'` (default) -- the exception is logged, and the task continues scheduling on the next interval.
    - `on_error='stop'` -- the exception is logged, and this specific periodic task is permanently cancelled. Other periodic tasks and the main processing loop are unaffected.
