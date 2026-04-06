# Observability

Drakkar provides four integrated observability layers: Prometheus metrics for alerting and dashboards, structured logging for event streams, a browser-based debug UI for real-time inspection, and a SQLite flight recorder for post-mortem analysis. All four are enabled by default and work together -- the debug UI reads from the flight recorder and links out to Prometheus graphs.

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
| `drakkar_task_retries_total` | Counter | -- | Total tasks retried after failure (via `on_error` returning RETRY) |

#### Batches

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `drakkar_batch_duration_seconds` | Histogram | -- | Window/batch processing duration (arrange through last task completion). Buckets: 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600 |

#### Partitions

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `drakkar_partition_queue_size` | Gauge | `partition` | Messages waiting in the partition's internal queue |
| `drakkar_offset_lag` | Gauge | `partition` | Pending (uncommitted) offsets per partition |
| `drakkar_backpressure_active` | Gauge | -- | Whether the consumer is paused due to backpressure. `1` = paused, `0` = flowing |
| `drakkar_total_queued` | Gauge | -- | Total messages buffered in all partition queues plus in-flight tasks |
| `drakkar_assigned_partitions` | Gauge | -- | Number of partitions currently assigned to this worker |

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
| `drakkar_sink_dlq_messages_total` | Counter | -- | Total messages sent to the dead letter queue |

#### Handler Hooks

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `drakkar_handler_duration_seconds` | Histogram | `hook` (`arrange`, `collect`, `on_error`, `on_window_complete`, etc.) | Duration of user handler hook execution. Buckets: 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 30 |

### User-Defined Metrics

You can declare Prometheus metrics as class attributes on your handler. The framework auto-discovers them via `discover_handler_metrics()`, which scans the handler's class hierarchy (MRO) for any `prometheus_client` metric instances (`Counter`, `Gauge`, `Histogram`, `Summary`, `Info`).

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

    async def collect(self, result):
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

Drakkar uses [structlog](https://www.structlog.org/) for structured, context-rich logging. All log output goes to stderr.

### Configuration

```yaml
logging:
  level: INFO       # DEBUG, INFO, WARNING, ERROR, CRITICAL
  format: json      # json | console
```

- **`json`** (default) -- produces one JSON object per line, suitable for ingestion by Elasticsearch, Loki, Datadog, or any log aggregator.
- **`console`** -- human-readable colored output for local development.

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
- `hook` -- bound during handler hook execution (e.g., `arrange`, `collect`)
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

- **Arrange** -- active `arrange()` calls with partition, duration, and message labels.
- **Executors Timeline** -- visual timeline of running, pending, and recently finished tasks. Slot-based layout matching `max_workers`. Tasks shorter than `ws_min_duration_ms` are hidden (except failures, which are always shown).
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
- **Cross-worker message trace** -- enter a partition and offset to trace a message across all workers sharing the same `db_dir`. Searches the current worker first, then other live workers, then rotated historical DB files.
- **Database management** -- list all debug database files with event counts and sizes, download individual files, merge multiple files into one, and view per-file breakdown by event type.

#### `/task/{id}` -- Task Detail

Detailed view of a single task's lifecycle:

- **Status** -- running, completed, or failed
- **Labels** -- user-defined labels from `ExecutorTask.labels`
- **Duration** -- wall-clock execution time
- **PID** -- OS process ID
- **CLI** -- reconstructed command line (`binary_path` + `args`)
- **stdout/stderr** -- captured process output (subject to `output_min_duration_ms` threshold)
- **Source offsets** -- which Kafka offsets this task covers
- **Event timeline** -- chronological list of all events for this task_id (started, completed/failed, collect_completed)

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

Stores every processing event (consumed, arranged, task_started, task_completed, task_failed, collect_completed, produced, committed, assigned, revoked, sink_delivered, sink_error).

Key columns: `ts`, `dt`, `event`, `partition`, `offset`, `task_id`, `args`, `stdout`, `stderr`, `exit_code`, `duration`, `output_topic`, `metadata` (JSON), `pid`, `labels` (JSON).

Indexed on `(partition, offset)`, `ts`, `dt`, `task_id`, and `event` for fast queries from the debug UI.

#### `worker_config` -- Autodiscovery

Single-row table written at startup (and after each rotation). Contains the full worker identity and configuration: `worker_name`, `cluster_name`, `ip_address`, `debug_port`, `debug_url`, `kafka_brokers`, `source_topic`, `consumer_group`, `binary_path`, `max_workers`, `task_timeout_seconds`, `max_retries`, `window_size`, `sinks_json`, `env_vars_json`.

This table is what enables the worker autodiscovery feature -- other workers scan for it in shared `db_dir`.

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

When workers share the same `db_dir` (e.g., a shared NFS mount or a Kubernetes PVC), they can discover each other automatically.

### How It Works

1. Workers with `store_config: true` write their configuration to the `worker_config` table in their SQLite database at startup and after each rotation.
2. The debug UI scans `db_dir` for `*-live.db` symlinks belonging to other workers.
3. For each live symlink found, the UI reads the `worker_config` table to get the worker's name, IP address, debug port, and cluster membership.
4. Workers are grouped by `cluster_name` -- only workers in the same cluster are shown together. Unclustered workers appear in a separate group.

### Worker Switcher

The debug UI navigation bar includes a worker dropdown that lists all discovered workers. Clicking a worker navigates to its debug UI (using `debug_url` if configured, otherwise `http://{ip_address}:{debug_port}/`).

### Cross-Worker Trace

The trace feature on the `/debug` page searches for a message's lifecycle across all workers in the cluster. Given a partition and offset, it searches:

1. The current worker's live database.
2. Other workers' live databases (same cluster).
3. Rotated (historical) database files in `db_dir`, newest first.

Each returned event carries a `worker_name` field identifying which worker processed it.

---

## Periodic Tasks

The `@periodic` decorator schedules handler methods to run at fixed intervals alongside the main processing loop.

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
- **Lifecycle**: periodic tasks are started after `on_ready()` completes and cancelled during graceful shutdown (before partition processors are drained).
- **Error handling**:
    - `on_error='continue'` (default) -- the exception is logged, and the task continues scheduling on the next interval.
    - `on_error='stop'` -- the exception is logged, and this specific periodic task is permanently cancelled. Other periodic tasks and the main processing loop are unaffected.
