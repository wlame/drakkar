# Configuration

Drakkar configuration is built on [pydantic-settings](https://docs.pydantic-dev.github.io/pydantic-settings/) and supports YAML files, environment variable overrides, and inline Python objects. Every option has a sensible default so you can start with a minimal config and grow from there.

---

## Configuration Loading

Drakkar resolves its configuration from three sources, applied in order. Later sources override earlier ones:

1. **Built-in defaults** -- every field has a default value in the Pydantic models.
2. **YAML file** -- structured configuration loaded from disk.
3. **Environment variables** -- `DRAKKAR_`-prefixed env vars, deep-merged on top of YAML.

### YAML File Resolution

The YAML file path is determined by the first match:

1. The `config_path` argument passed to `DrakkarApp`.
2. The `DRAKKAR_CONFIG` environment variable.
3. If neither is set, Drakkar runs with env-only configuration (all defaults + env overrides).

```python
# Option 1: explicit path
app = DrakkarApp(handler=MyHandler(), config_path='drakkar.yaml')

# Option 2: env var (set DRAKKAR_CONFIG=/etc/drakkar/config.yaml)
app = DrakkarApp(handler=MyHandler())

# Option 3: inline config object (no file needed)
from drakkar.config import DrakkarConfig, KafkaConfig, ExecutorConfig

app = DrakkarApp(
    handler=MyHandler(),
    config=DrakkarConfig(
        kafka=KafkaConfig(brokers='kafka:9092', source_topic='my-events'),
        executor=ExecutorConfig(max_executors=8),
    ),
)
```

!!! note
    When you pass a `config` object directly, no YAML file or env var loading occurs -- you have full control.

### Environment Variable Overrides

Environment variables use the `DRAKKAR_` prefix with `__` (double underscore) as the nesting delimiter:

```bash
# Override kafka.brokers
export DRAKKAR_KAFKA__BROKERS=kafka-prod:9092

# Override executor.max_executors
export DRAKKAR_EXECUTOR__MAX_EXECUTORS=16

# Override debug.port
export DRAKKAR_DEBUG__PORT=9000
```

Env vars are parsed into a nested dict structure and deep-merged on top of the YAML values. Leaf values from env vars always win over YAML values; nested dicts are merged recursively.

### Loading Order Summary

```
defaults --> YAML file --> environment variables (deep merge)
```

---

## Root Config (`DrakkarConfig`)

Top-level settings that control worker identity and cluster grouping.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `worker_name_env` | `str` | `'WORKER_ID'` | Name of the environment variable that holds the worker name. Used in logs, metrics, and the debug UI. If the env var is empty or unset, falls back to `drakkar-{hex_id}`. |
| `cluster_name` | `str` | `''` | Logical cluster name for grouping workers in the debug UI. Workers with the same cluster name are displayed together and can cross-trace messages. |
| `cluster_name_env` | `str` | `''` | Name of an environment variable that holds the cluster name. If set and non-empty, overrides the static `cluster_name` value. |

```yaml
worker_name_env: HOSTNAME
cluster_name: search-cluster
cluster_name_env: DRAKKAR_CLUSTER
```

---

## Kafka Source (`kafka:`)

Settings for the Kafka consumer that reads input messages.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `brokers` | `str` | `'localhost:9092'` | Kafka bootstrap servers (comma-separated for multiple brokers). Also used as the fallback for sink and DLQ brokers when they are left empty. |
| `source_topic` | `str` | `'input-events'` | The Kafka topic to consume messages from. |
| `consumer_group` | `str` | `'drakkar-workers'` | Consumer group ID. All workers in the same group share partition assignments. |
| `max_poll_records` | `int` | `100` | Maximum number of messages returned per poll batch. Higher values improve throughput; lower values reduce latency. |
| `max_poll_interval_ms` | `int` | `300000` | Maximum time (ms) between poll calls before Kafka considers the consumer dead and triggers a rebalance. Increase this if your tasks take a long time. |
| `session_timeout_ms` | `int` | `45000` | Session timeout (ms) for group membership. If the broker does not receive a heartbeat within this window, the consumer is removed from the group. |
| `heartbeat_interval_ms` | `int` | `3000` | Interval (ms) between heartbeats sent to the broker. Should be less than `session_timeout_ms / 3`. |

```yaml
kafka:
  brokers: kafka-1:9092,kafka-2:9092
  source_topic: search-requests
  consumer_group: search-workers
  max_poll_records: 200
  max_poll_interval_ms: 600000
  session_timeout_ms: 60000
  heartbeat_interval_ms: 5000
```

---

## Executor Pool (`executor:`)

Controls the subprocess executor pool that runs user-defined binaries.

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `binary_path` | `str \| None` | `None` | min length 1 if set | Default binary path for all tasks. If `None`, each [ExecutorTask](executor.md#executortask) returned by [arrange()](handler.md#arrange-required) must provide its own `binary_path`, otherwise the task fails with a clear error. See [Binary Path Resolution](executor.md#binary-path-resolution). |
| `env` | `dict[str, str]` | `{}` | | Environment variables passed to all executor subprocesses. Merged on top of the parent process env. Per-task `ExecutorTask.env` overrides these on conflict. See [Environment Variables](executor.md#environment-variables). |
| `max_executors` | `int` | `4` | >= 1 | Maximum number of concurrent subprocesses. Controls the `asyncio.Semaphore` size -- tasks beyond this limit wait in a queue. See [Concurrency and Backpressure](executor.md#concurrency-and-backpressure). |
| `task_timeout_seconds` | `int` | `120` | >= 1 | Wall-clock timeout (seconds) per subprocess. If a process exceeds this, it is killed and treated as a failure. |
| `window_size` | `int` | `100` | >= 1 | Maximum number of messages collected per [arrange()](handler.md#arrange-required) [window](executor.md#windowing). Larger windows allow more batching in `arrange()`; smaller windows reduce latency. |
| `max_retries` | `int` | `3` | >= 0 | Maximum number of retry attempts per failed task (0 = no retries). A task can run up to `max_retries + 1` times total. |
| `drain_timeout_seconds` | `int` | `5` | >= 1 | Maximum time (seconds) to wait for in-flight tasks during shutdown or partition revocation. |
| `backpressure_high_multiplier` | `int` | `32` | >= 1 | Multiplier for the pause threshold. When total queued messages reach `max_executors * backpressure_high_multiplier`, Kafka consumption is paused. |
| `backpressure_low_multiplier` | `int` | `4` | >= 1 | Multiplier for the resume threshold. When total queued messages drop to `max(1, max_executors * backpressure_low_multiplier)`, Kafka consumption resumes. |

### Backpressure Formula

[Backpressure](performance.md#backpressure) prevents unbounded memory growth by pausing Kafka consumption when too many messages are buffered:

```
high_watermark = max_executors * backpressure_high_multiplier
low_watermark  = max(1, max_executors * backpressure_low_multiplier)
```

With defaults (`max_executors=4`):

- **High watermark** = 4 * 32 = **128** -- pause consumption
- **Low watermark** = max(1, 4 * 4) = **16** -- resume consumption

The gap between high and low watermarks prevents rapid pause/resume oscillation (hysteresis).

```yaml
executor:
  binary_path: /usr/local/bin/my-processor
  max_executors: 8
  task_timeout_seconds: 300
  window_size: 50
  max_retries: 5
  drain_timeout_seconds: 10
  backpressure_high_multiplier: 16
  backpressure_low_multiplier: 2
```

---

## Sinks (`sinks:`)

Sinks define where processed results are delivered. See [Sinks](sinks.md) for payload models, [routing](sinks.md#routing), and the [delivery lifecycle](sinks.md#delivery-lifecycle). Each sink type is a dictionary mapping instance names to their configuration. You can configure multiple instances of the same type (e.g., two Kafka sinks writing to different topics).

### Kafka Sink (`sinks.kafka.<name>`)

Produces messages to a Kafka topic.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `topic` | `str` | *(required)* | Target Kafka topic for output messages. |
| `brokers` | `str` | `''` | Kafka brokers for this sink. If empty, inherits from `kafka.brokers` (same cluster as the source). |
| `ui_url` | `str` | `''` | URL to a web UI for this sink (e.g., Kafka UI, Kowl). Displayed as a link in the debug dashboard. |

### PostgreSQL Sink (`sinks.postgres.<name>`)

Inserts rows into a PostgreSQL database via an asyncpg connection pool.

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `dsn` | `str` | *(required)* | | PostgreSQL connection string (e.g., `postgresql://user:pass@host:5432/db`). |
| `pool_min` | `int` | `2` | >= 1 | Minimum number of connections in the pool. |
| `pool_max` | `int` | `10` | >= 1 | Maximum number of connections in the pool. |
| `ui_url` | `str` | `''` | | URL to a database management UI (e.g., pgAdmin). |

### MongoDB Sink (`sinks.mongo.<name>`)

Inserts documents into a MongoDB database via motor AsyncIOMotorClient.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `uri` | `str` | *(required)* | MongoDB connection URI (e.g., `mongodb://host:27017`). |
| `database` | `str` | *(required)* | Target database name. |
| `ui_url` | `str` | `''` | URL to a MongoDB management UI (e.g., Mongo Express). |

### HTTP Sink (`sinks.http.<name>`)

Sends JSON payloads to an HTTP endpoint.

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `url` | `str` | *(required)* | | Target URL for HTTP requests. |
| `method` | `str` | `'POST'` | | HTTP method to use. |
| `timeout_seconds` | `int` | `30` | >= 1 | Request timeout in seconds. |
| `headers` | `dict[str, str]` | `{}` | | Additional HTTP headers sent with each request. |
| `max_retries` | `int` | `3` | >= 0 | Maximum retry attempts for failed HTTP requests. |
| `ui_url` | `str` | `''` | | URL to a related web UI. |

### Redis Sink (`sinks.redis.<name>`)

Sets key-value pairs in Redis.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `url` | `str` | `'redis://localhost:6379/0'` | Redis connection URL. |
| `key_prefix` | `str` | `''` | Prefix prepended to all keys (e.g., `cache:` produces keys like `cache:my-key`). |
| `ui_url` | `str` | `''` | URL to a Redis management UI (e.g., RedisInsight). |

### Filesystem Sink (`sinks.filesystem.<name>`)

Appends JSONL lines to files on disk.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `base_path` | `str` | `''` | Optional base directory. Individual payloads specify their own full paths. |
| `ui_url` | `str` | `''` | URL to a file browser or related UI. |

### Example: Multiple Named Sinks

```yaml
sinks:
  kafka:
    search-results:
      topic: search-results
    analytics:
      topic: analytics-events
      brokers: kafka-analytics:9092

  postgres:
    main-db:
      dsn: postgresql://user:pass@db:5432/myapp
      pool_min: 5
      pool_max: 20
      ui_url: http://pgadmin.internal:5050

  mongo:
    logs:
      uri: mongodb://mongo:27017
      database: app_logs

  http:
    webhook:
      url: https://api.example.com/webhook
      method: POST
      timeout_seconds: 15
      headers:
        Authorization: "Bearer ${API_TOKEN}"
      max_retries: 5

  redis:
    cache:
      url: redis://redis:6379/0
      key_prefix: "result:"
      ui_url: http://redis-insight.internal:8001

  filesystem:
    archive:
      base_path: /data/archive
```

---

## Dead Letter Queue (`dlq:`)

Failed sink deliveries can be routed to a [DLQ](sinks.md#dead-letter-queue) Kafka topic. The DLQ captures the original payloads, error details, and metadata for later inspection or reprocessing.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `topic` | `str` | `''` | DLQ Kafka topic name. If empty, auto-derived as `{source_topic}_dlq` (e.g., `input-events_dlq`). |
| `brokers` | `str` | `''` | Kafka brokers for the DLQ. If empty, inherits from `kafka.brokers`. |

```yaml
dlq:
  topic: failed-events
  brokers: kafka-prod:9092
```

---

## Metrics (`metrics:`)

Prometheus metrics endpoint configuration.

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `enabled` | `bool` | `true` | | Enable or disable the Prometheus metrics HTTP server. |
| `port` | `int` | `9090` | 1--65535 | Port for the Prometheus metrics endpoint. |

```yaml
metrics:
  enabled: true
  port: 9090
```

---

## Logging (`logging:`)

Structured logging configuration via structlog.

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `level` | `str` | `'INFO'` | | Log level: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`. |
| `format` | `str` | `'json'` | `json` or `console` | Output format. `json` produces machine-readable structured logs. `console` produces colorized human-readable output for local development. |
| `output` | `str` | `'stderr'` | | Log destination: `stderr`, `stdout`, or a file path. File paths support `{worker_id}` and `{cluster_name}` template variables. Parent directories are created automatically. |

```yaml
logging:
  level: INFO
  format: json
  output: stderr  # or stdout, or /var/log/drakkar/{worker_id}.log
```

---

## Debug / Flight Recorder (`debug:`)

The debug subsystem provides a [flight recorder](observability.md#flight-recorder) (SQLite-backed event log), a [web UI dashboard](observability.md#debug-ui), WebSocket live streaming, and [worker autodiscovery](observability.md#worker-autodiscovery). This is the largest configuration section.

### Core Settings

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `enabled` | `bool` | `true` | | Enable or disable the entire debug feature. Set to `false` to skip the flight recorder, web UI, and all associated overhead. |
| `host` | `str` | `'127.0.0.1'` | | Bind address for the debug server. Default `127.0.0.1` (localhost only). Use `0.0.0.0` to expose on all interfaces. |
| `port` | `int` | `8080` | 1--65535 | Port for the debug web UI (FastAPI server). |
| `auth_token` | `str` | `''` | | Bearer token for sensitive debug endpoints (database download, merge, database listing). When empty, no authentication is required. When set, protected endpoints require `Authorization: Bearer <token>` header or `?token=<token>` query parameter. Read-only pages (dashboard, live, partitions, sinks, history) are always accessible. |
| `debug_url` | `str` | `''` | | External URL for the debug UI. Used when workers discover each other -- if set, this URL is advertised instead of the auto-detected `http://{ip}:{port}`. Useful behind load balancers or Kubernetes ingresses. |
| `db_dir` | `str` | `'/tmp'` | | Directory for SQLite database files. Set to `''` to run without any disk persistence (in-memory only, WebSocket streaming still works). Use a shared filesystem (e.g., NFS, EFS) for cross-worker autodiscovery and merge. |

### Persistence Flags

These flags control which tables are created in the SQLite database. All require `db_dir` to be non-empty. Any combination is valid.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `store_events` | `bool` | `true` | Write processing events (consumed, task_started, task_completed, task_failed, etc.) to the `events` table. Disable to reduce disk I/O on high-throughput workers. |
| `store_config` | `bool` | `true` | Write worker configuration to the `worker_config` table. This enables autodiscovery -- other workers sharing the same `db_dir` can find and link to this worker. |
| `store_state` | `bool` | `true` | Periodically snapshot worker state (uptime, partitions, pool utilization, queue depth, counters) to the `worker_state` table. |
| `state_sync_interval_seconds` | `int` | `10` | >= 1 | Interval (seconds) between worker state snapshots. |

### Exposed Environment Variables

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `expose_env_vars` | `list[str]` | `[]` | List of environment variable names to capture and store in `worker_config.env_vars_json`. Useful for recording deployment metadata (e.g., `['GIT_SHA', 'DEPLOY_ENV', 'K8S_POD_NAME']`). |

### Database Rotation and Retention

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `rotation_interval_minutes` | `int` | `60` | >= 1 | How often (minutes) to rotate the SQLite database file. On rotation, a new timestamped file is created and the old one is finalized. A `-live.db` symlink always points to the current file. |
| `retention_hours` | `int` | `24` | >= 1 | Delete rotated database files older than this many hours. |
| `retention_max_events` | `int` | `100000` | >= 100 | Upper bound on total events across all DB files. Also controls the maximum number of retained DB files (`max_files = retention_max_events / 10000`). |

### Output Storage

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `store_output` | `bool` | `true` | | Include subprocess stdout/stderr in event records. Disable to save disk space when output is large or not needed for debugging. |
| `flush_interval_seconds` | `int` | `5` | >= 1 | How often (seconds) the in-memory event buffer is flushed to SQLite. |
| `max_buffer` | `int` | `50000` | >= 1000 | Maximum number of events held in the in-memory buffer. When full, oldest events are dropped (ring buffer). |
| `max_ui_rows` | `int` | `5000` | >= 100 | Maximum number of rows returned to the debug web UI in list views. |

### Duration Thresholds

These thresholds control which events are recorded, logged, and streamed. See [Duration Thresholds](observability.md#duration-thresholds) for detailed behavior and [Performance Tuning](performance.md) for recommendations. They help reduce noise in high-throughput systems where most tasks complete quickly.

| Field | Type | Default | Constraints | Description |
|-------|------|---------|-------------|-------------|
| `log_min_duration_ms` | `int` | `500` | >= 0 | Minimum task duration (ms) to emit a `slow_task_completed` or `slow_task_failed` log message. Set to `0` to log all tasks. |
| `ws_min_duration_ms` | `int` | `500` | >= 0 | Minimum task duration (ms) to broadcast via WebSocket to the live UI. Fast tasks that complete under this threshold are invisible in the live view (reduces UI noise). Failed tasks always appear regardless. Set to `0` to show all tasks. |
| `event_min_duration_ms` | `int` | `0` | >= 0 | Minimum task duration (ms) to persist to the SQLite database. Set above `0` to skip storing fast tasks entirely. |
| `output_min_duration_ms` | `int` | `500` | >= 0 | Minimum task duration (ms) to include stdout/stderr in the persisted event record. Tasks under this threshold are recorded but without output data. |

### Prometheus Integration

These settings add clickable Prometheus graph links to the debug dashboard.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `prometheus_url` | `str` | `''` | Base URL of your Prometheus server (e.g., `http://prometheus:9090`). If empty, no Prometheus links are shown in the debug UI. |
| `prometheus_rate_interval` | `str` | `'5m'` | Rate interval used in PromQL `rate()` expressions for dashboard links (e.g., `1m`, `5m`, `15m`). |
| `prometheus_worker_label` | `str` | `''` | PromQL label filter for worker-scoped queries. Supports template variables: `{worker_id}`, `{cluster_name}`, `{metrics_port}`, `{debug_port}`. If empty, defaults to `instance="{hostname}:{metrics_port}"`. Example: `worker_id="{worker_id}"`. |
| `prometheus_cluster_label` | `str` | `''` | PromQL label filter for cluster-wide queries. Supports the same template variables. Example: `cluster="{cluster_name}"`. If empty, cluster-wide links are not shown. |

### Custom Links

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `custom_links` | `list[dict[str, str]]` | `[]` | List of custom links displayed in the debug dashboard navigation. Each entry is a dict with `name` and `url` keys. URL values support template variables: `{worker_id}`, `{cluster_name}`, `{metrics_port}`, `{debug_port}`. |

```yaml
debug:
  enabled: true
  port: 8080
  debug_url: https://debug.example.com/worker-1
  db_dir: /shared/drakkar-debug
  store_events: true
  store_config: true
  store_state: true
  state_sync_interval_seconds: 10
  expose_env_vars:
    - GIT_SHA
    - DEPLOY_ENV
    - K8S_POD_NAME
  rotation_interval_minutes: 60
  retention_hours: 48
  retention_max_events: 200000
  store_output: true
  flush_interval_seconds: 5
  max_buffer: 50000
  max_ui_rows: 5000
  log_min_duration_ms: 1000
  ws_min_duration_ms: 500
  event_min_duration_ms: 100
  output_min_duration_ms: 1000
  prometheus_url: http://prometheus:9090
  prometheus_rate_interval: 5m
  prometheus_worker_label: 'worker_id="{worker_id}"'
  prometheus_cluster_label: 'cluster="{cluster_name}"'
  custom_links:
    - name: Grafana Dashboard
      url: http://grafana:3000/d/drakkar?var-worker={worker_id}
    - name: Kibana Logs
      url: http://kibana:5601/app/discover#/?_a=(query:(match_phrase:(worker_id:'{worker_id}')))
```

---

## Full Example YAML

A complete `drakkar.yaml` with all sections filled in:

```yaml
# --- Worker identity ---
worker_name_env: HOSTNAME          # read worker name from $HOSTNAME
cluster_name: search-cluster       # static cluster name
cluster_name_env: DRAKKAR_CLUSTER  # override cluster from env if set

# --- Kafka source ---
kafka:
  brokers: kafka-1:9092,kafka-2:9092,kafka-3:9092
  source_topic: search-requests
  consumer_group: search-workers
  max_poll_records: 200
  max_poll_interval_ms: 600000     # 10 minutes (long-running tasks)
  session_timeout_ms: 60000        # 60 seconds
  heartbeat_interval_ms: 5000      # 5 seconds

# --- Executor pool ---
executor:
  binary_path: /usr/local/bin/search-engine
  max_executors: 8
  task_timeout_seconds: 300        # 5 minutes per task
  window_size: 50
  max_retries: 3
  drain_timeout_seconds: 10
  backpressure_high_multiplier: 16 # pause at 8 * 16 = 128 queued
  backpressure_low_multiplier: 2   # resume at max(1, 8 * 2) = 16 queued

# --- Output sinks ---
sinks:
  kafka:
    search-results:
      topic: search-results
    analytics:
      topic: analytics-events
      brokers: kafka-analytics:9092  # separate cluster
      ui_url: http://kafka-ui.internal:8080

  postgres:
    main-db:
      dsn: postgresql://app:secret@db:5432/search
      pool_min: 5
      pool_max: 20
      ui_url: http://pgadmin.internal:5050

  mongo:
    logs:
      uri: mongodb://mongo:27017
      database: search_logs
      ui_url: http://mongo-express.internal:8081

  http:
    webhook:
      url: https://api.example.com/results
      method: POST
      timeout_seconds: 15
      headers:
        Authorization: "Bearer my-api-token"
        X-Source: drakkar
      max_retries: 5
      ui_url: https://api.example.com/docs

  redis:
    cache:
      url: redis://redis:6379/0
      key_prefix: "search:"
      ui_url: http://redis-insight.internal:8001

  filesystem:
    archive:
      base_path: /data/archive
      ui_url: http://filebrowser.internal:8080

# --- Dead letter queue ---
dlq:
  topic: search-requests-dlq       # explicit DLQ topic
  brokers: ""                       # inherit from kafka.brokers

# --- Prometheus metrics ---
metrics:
  enabled: true
  port: 9090

# --- Logging ---
logging:
  level: INFO
  format: json                      # "json" for production, "console" for dev
  output: stderr                    # stderr | stdout | file path (supports {worker_id}, {cluster_name})

# --- Debug flight recorder ---
debug:
  enabled: true
  port: 8080
  debug_url: ""                     # set if behind a reverse proxy
  db_dir: /shared/drakkar-debug     # shared filesystem for autodiscovery
  store_events: true
  store_config: true
  store_state: true
  state_sync_interval_seconds: 10
  expose_env_vars:
    - GIT_SHA
    - DEPLOY_ENV
    - K8S_POD_NAME
  rotation_interval_minutes: 60
  retention_hours: 48
  retention_max_events: 200000
  store_output: true
  flush_interval_seconds: 5
  max_buffer: 50000
  max_ui_rows: 5000
  log_min_duration_ms: 1000
  ws_min_duration_ms: 500
  event_min_duration_ms: 0
  output_min_duration_ms: 500
  prometheus_url: http://prometheus:9090
  prometheus_rate_interval: 5m
  prometheus_worker_label: 'worker_id="{worker_id}"'
  prometheus_cluster_label: 'cluster="{cluster_name}"'
  custom_links:
    - name: Grafana
      url: http://grafana:3000/d/drakkar?var-worker={worker_id}
    - name: Logs
      url: http://kibana:5601/app/discover#/?_a=(query:(match_phrase:(worker_id:'{worker_id}')))
```

---

## Environment Variable Reference

Common environment variables and their corresponding YAML paths:

| Environment Variable | YAML Path | Example Value |
|---------------------|-----------|---------------|
| `DRAKKAR_CONFIG` | *(file path, not a config field)* | `/etc/drakkar/config.yaml` |
| `DRAKKAR_WORKER_NAME_ENV` | `worker_name_env` | `HOSTNAME` |
| `DRAKKAR_CLUSTER_NAME` | `cluster_name` | `search-cluster` |
| `DRAKKAR_CLUSTER_NAME_ENV` | `cluster_name_env` | `K8S_CLUSTER` |
| `DRAKKAR_KAFKA__BROKERS` | `kafka.brokers` | `kafka:9092` |
| `DRAKKAR_KAFKA__SOURCE_TOPIC` | `kafka.source_topic` | `my-events` |
| `DRAKKAR_KAFKA__CONSUMER_GROUP` | `kafka.consumer_group` | `my-workers` |
| `DRAKKAR_KAFKA__MAX_POLL_RECORDS` | `kafka.max_poll_records` | `200` |
| `DRAKKAR_KAFKA__MAX_POLL_INTERVAL_MS` | `kafka.max_poll_interval_ms` | `600000` |
| `DRAKKAR_KAFKA__SESSION_TIMEOUT_MS` | `kafka.session_timeout_ms` | `60000` |
| `DRAKKAR_KAFKA__HEARTBEAT_INTERVAL_MS` | `kafka.heartbeat_interval_ms` | `5000` |
| `DRAKKAR_EXECUTOR__BINARY_PATH` | `executor.binary_path` | `/usr/bin/my-tool` |
| `DRAKKAR_EXECUTOR__MAX_EXECUTORS` | `executor.max_executors` | `16` |
| `DRAKKAR_EXECUTOR__TASK_TIMEOUT_SECONDS` | `executor.task_timeout_seconds` | `300` |
| `DRAKKAR_EXECUTOR__WINDOW_SIZE` | `executor.window_size` | `50` |
| `DRAKKAR_EXECUTOR__MAX_RETRIES` | `executor.max_retries` | `5` |
| `DRAKKAR_EXECUTOR__DRAIN_TIMEOUT_SECONDS` | `executor.drain_timeout_seconds` | `10` |
| `DRAKKAR_EXECUTOR__BACKPRESSURE_HIGH_MULTIPLIER` | `executor.backpressure_high_multiplier` | `16` |
| `DRAKKAR_EXECUTOR__BACKPRESSURE_LOW_MULTIPLIER` | `executor.backpressure_low_multiplier` | `2` |
| `DRAKKAR_DLQ__TOPIC` | `dlq.topic` | `my-events-dlq` |
| `DRAKKAR_DLQ__BROKERS` | `dlq.brokers` | `kafka:9092` |
| `DRAKKAR_METRICS__ENABLED` | `metrics.enabled` | `true` |
| `DRAKKAR_METRICS__PORT` | `metrics.port` | `9090` |
| `DRAKKAR_LOGGING__LEVEL` | `logging.level` | `DEBUG` |
| `DRAKKAR_LOGGING__FORMAT` | `logging.format` | `console` |
| `DRAKKAR_LOGGING__OUTPUT` | `logging.output` | `/var/log/drakkar/worker.log` |
| `DRAKKAR_DEBUG__ENABLED` | `debug.enabled` | `false` |
| `DRAKKAR_DEBUG__PORT` | `debug.port` | `9000` |
| `DRAKKAR_DEBUG__DB_DIR` | `debug.db_dir` | `/shared/debug` |
| `DRAKKAR_DEBUG__STORE_EVENTS` | `debug.store_events` | `true` |
| `DRAKKAR_DEBUG__STORE_CONFIG` | `debug.store_config` | `true` |
| `DRAKKAR_DEBUG__STORE_STATE` | `debug.store_state` | `true` |
| `DRAKKAR_DEBUG__STORE_OUTPUT` | `debug.store_output` | `false` |
| `DRAKKAR_DEBUG__PROMETHEUS_URL` | `debug.prometheus_url` | `http://prometheus:9090` |

!!! tip
    The `DRAKKAR_CONFIG` variable is special -- it sets the YAML file path, not a config field. It is excluded from the env-var-to-config merge.
