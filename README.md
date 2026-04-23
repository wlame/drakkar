# Drakkar
## Poll -> Execute -> Sink

Kafka subprocess orchestration framework with pluggable output sinks. Consumes messages from Kafka, runs CPU-intensive external binaries in a managed subprocess pool, and delivers results to any combination of Kafka, PostgreSQL, MongoDB, Redis, HTTP, and filesystem.

Workers are the Drakkars, executors are the Vikings.

## What it does

```
Kafka source topic
    |
    v
[ Drakkar Worker ]
    |
    +-- poll messages (per-partition pipelines)
    +-- arrange() -> executor tasks (user hook)
    +-- run external binary via subprocess pool
    +-- on_task_complete() -> sink payloads (user hook)
    +-- on_message_complete() -> aggregate per source message (user hook, optional)
    +-- deliver to configured sinks (Kafka, Postgres, Mongo, Redis, HTTP, files)
    +-- commit offsets (watermark-based, only after all sinks confirm)
    |
    v
Configured sinks (any combination)
```

- **Per-partition independent pipelines** with offset watermark tracking
- **Pluggable sinks** -- configure any combination of Kafka, PostgreSQL, MongoDB, Redis, HTTP, filesystem
- **Dead letter queue** -- failed deliveries go to a DLQ Kafka topic with error metadata
- **Cooperative-sticky rebalancing** -- non-revoked partitions continue without interruption
- **Backpressure** via Kafka pause/resume -- memory stays bounded regardless of consumer lag
- **Subprocess executor pool** with semaphore-based concurrency limiting
- **Typed message models** -- define Pydantic schemas for input/output, get auto-deserialization
- **Cache (optional)** -- `self.cache` key/value store with memory + write-behind SQLite + eventually-consistent peer sync across workers ([docs](docs/cache.md))
- **Built-in debug UI** (FastAPI) with executor timeline, partition lag, message tracing
- **Flight recorder** -- SQLite event log with retention and rotation
- **Prometheus metrics** -- pipeline, executor, and per-sink metrics
- **Structured JSON logging** -- ECS-compatible, ready for Elastic

## Quick start

```bash
uv init my-processor && cd my-processor
uv add py-drakkar
```

### 1. Define your message models

```python
# models.py
from pydantic import BaseModel

class InputMessage(BaseModel):
    request_id: str
    data: str
    priority: int = 1

class ProcessedResult(BaseModel):
    request_id: str
    result: str
    processed: bool

class ResultSummary(BaseModel):
    request_id: str
    status: str
```

### 2. Implement your handler

```python
# handler.py
import structlog
from prometheus_client import Counter

from drakkar import (
    BaseDrakkarHandler, CollectResult, DeliveryAction, DeliveryError,
    ErrorAction, ExecutorTask, KafkaPayload, PostgresPayload,
    RedisPayload, make_task_id,
)
from models import InputMessage, ProcessedResult, ResultSummary

logger = structlog.get_logger()

# custom Prometheus metric (user-defined)
items_processed = Counter('app_items_processed_total', 'Total processed items')

class MyHandler(BaseDrakkarHandler[InputMessage, ProcessedResult]):
    async def arrange(self, messages, pending):
        tasks = []
        for msg in messages:
            # msg.payload is an InputMessage instance (auto-deserialized)
            tasks.append(ExecutorTask(
                task_id=make_task_id("proc"),
                args=["--input", msg.payload.data],
                source_offsets=[msg.offset],
                metadata={"request_id": msg.payload.request_id},
            ))
        return tasks

    async def on_task_complete(self, result):
        output = ProcessedResult(
            request_id=result.task.metadata["request_id"],
            result=result.stdout.strip(),
            processed=result.exit_code == 0,
        )
        summary = ResultSummary(
            request_id=output.request_id,
            status="done" if output.processed else "failed",
        )

        # custom Prometheus metric
        items_processed.inc()

        # async structured logging
        await logger.ainfo(
            "item_processed",
            category="handler",
            request_id=output.request_id,
            processed=output.processed,
        )

        # route to sinks based on business logic
        sinks = CollectResult(
            kafka=[KafkaPayload(data=output, key=output.request_id.encode())],
            postgres=[PostgresPayload(table="results", data=summary)],
        )

        # conditional: cache successful results in Redis
        if output.processed:
            sinks.redis.append(
                RedisPayload(key=f"result:{output.request_id}", data=summary, ttl=3600)
            )

        return sinks

    async def on_error(self, task, error):
        await logger.awarning(
            "task_failed", category="handler",
            task_id=task.task_id, exit_code=error.exit_code,
        )
        return ErrorAction.RETRY

    async def on_delivery_error(self, error: DeliveryError):
        await logger.awarning(
            "delivery_failed", category="handler",
            sink=error.sink_name, error=error.error,
        )
        # retry transient failures, DLQ for permanent ones
        if error.sink_type in ("http", "redis"):
            return DeliveryAction.RETRY
        return DeliveryAction.DLQ
```

### 3. Configure

```yaml
# drakkar.yaml
kafka:
  brokers: "localhost:9092"
  source_topic: "input-events"
  consumer_group: "my-workers"

executor:
  binary_path: "/usr/local/bin/my-processor"
  max_executors: 8
  task_timeout_seconds: 120
  window_size: 20

sinks:
  kafka:
    results:
      topic: "output-results"
  postgres:
    main:
      dsn: "postgresql://user:pass@localhost:5432/mydb"
  redis:
    cache:
      url: "redis://localhost:6379/0"
      key_prefix: "app:"

dlq:
  topic: ""  # auto-derived: input-events_dlq

metrics:
  port: 9090

debug:
  port: 8080
```

### 4. Run

```python
# main.py
from drakkar import DrakkarApp
from handler import MyHandler

app = DrakkarApp(
    handler=MyHandler(),
    config_path="drakkar.yaml",
)
app.run()
```

Worker name is read from the `WORKER_ID` environment variable by default (configurable via `worker_name_env` in config).

## Security & trust model

Drakkar has an explicit trust model that operators should understand before production deployment. These assumptions are inherent to the framework's architecture and aren't weaknesses per se -- they're the trust boundaries.

1. **Handler binary is fully trusted.** `executor.binary_path` is operator-configured; message bytes flow to the binary's stdin without sanitization. The binary runs with the worker's privileges (plus any env overrides from `ExecutorConfig.env` or per-task `env`).
2. **Peer workers sharing `db_dir` are fully trusted.** The cache and recorder peer-sync mechanisms have no cryptographic authentication of peer writes. Anyone who can write to the shared directory can inject cache entries or event rows that your workers will read. Treat `db_dir` as a shared-trust boundary.
3. **The debug UI is an operator tool, not a public surface.** The default `debug.host='127.0.0.1'` is load-bearing for out-of-the-box security. If you set `debug.host='0.0.0.0'` (or any non-loopback address), you MUST set `debug.auth_token` to a 32+ character random value -- Drakkar fails at startup with `InsecureDebugConfigError` if you don't. When a token is set, the WebSocket stream also validates the `Origin` header (against `debug.allowed_ws_origins` when configured, otherwise against the request's `Host` header). Even with auth, the debug UI exposes subprocess stdout/stderr, per-task env (after redaction), cache contents, and live event streams; restrict access to operators only.
4. **Kafka producers are trusted for availability, not correctness.** Drakkar deserializes message payloads via `handler.deserialize_message`; parse errors silently set `msg.payload=None` rather than DLQ-ing the message or raising. A malicious producer cannot execute code in the worker, but can cause handlers to see unexpected `None` payloads unless your handler validates.
5. **Per-task `env` is redacted before it reaches the recorder.** Handler-written values in `task.env` are sanitized on the way to the recorder DB: names matching `*PASSWORD*`/`*SECRET*`/`*TOKEN*`/`*_KEY`/`*API_KEY*`/`*CREDENTIAL*`/`*_DSN` become `***`; other URL-shaped values have embedded credentials stripped. Non-matching names pass through, so rename or avoid per-task env for secrets whose names don't trigger a pattern. `ExecutorConfig.env` (framework-level) is **never written** to the recorder at all -- it only reaches the subprocess environment, making it the safer slot for stable credentials.

See the [FAQ](docs/faq.md) for deeper discussion of each assumption and links to the implementation.

## Handler hooks

| Hook | When | Purpose |
|---|---|---|
| `on_startup(config)` | Before components start | Modify config (e.g., auto-detect CPU count) |
| `on_ready(config, db_pool)` | After sinks connected | Initialize state from DB, run migrations |
| `arrange(messages, pending)` | Window of messages received | Transform messages into executor tasks |
| `on_task_complete(result)` | Each task completes | Process per-task result into sink payloads |
| `on_message_complete(group)` | All tasks for one source message finish | Aggregate per-message results (fan-out → fan-in) |
| `on_window_complete(results, messages)` | All tasks in a window done | Aggregate results across a window |
| `on_error(task, error)` | Task fails | Return `RETRY`, `SKIP`, or replacement tasks |
| `on_delivery_error(error)` | Sink delivery fails | Return `DLQ` (default), `RETRY`, or `SKIP` |
| `on_assign(partitions)` | Partitions assigned | Initialize per-partition state |
| `on_revoke(partitions)` | Partitions revoked | Cleanup per-partition state |

See [`docs/handler.md`](docs/handler.md) for hook semantics and [`docs/fan-out.md`](docs/fan-out.md) for the fan-out → fan-in pattern with `on_message_complete`.

## Periodic tasks

Use the `@periodic` decorator to schedule recurring background coroutines on the handler. They run in the same async loop alongside the poll loop, start after `on_ready()`, and are cancelled on shutdown. Overlapping runs are prevented -- the next interval starts only after the current invocation finishes.

```python
from drakkar import BaseDrakkarHandler, periodic

class MyHandler(BaseDrakkarHandler):
    async def on_ready(self, config, db_pool):
        self.db_pool = db_pool

    @periodic(seconds=60)
    async def refresh_cache(self):
        async with self.db_pool.acquire() as conn:
            self.cache = await conn.fetch("SELECT * FROM lookup")

    @periodic(seconds=30, on_error="stop")
    async def health_ping(self):
        await http_post("https://health.example.com/ping")
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `seconds` | `float` | required | Interval between runs |
| `on_error` | `"continue" \| "stop"` | `"continue"` | `"continue"` logs and retries next interval; `"stop"` logs and cancels the task |

## Sinks

Configure any combination in the `sinks:` section. Each type supports multiple named instances.

| Sink | Payload | Serialization |
|------|---------|---------------|
| `KafkaPayload` | `data: BaseModel`, `key: bytes` | `data.model_dump_json().encode()` -> value |
| `PostgresPayload` | `data: BaseModel`, `table: str` | `data.model_dump()` -> column mapping |
| `MongoPayload` | `data: BaseModel`, `collection: str` | `data.model_dump()` -> BSON document |
| `HttpPayload` | `data: BaseModel` | `data.model_dump_json()` -> POST body |
| `RedisPayload` | `data: BaseModel`, `key: str`, `ttl: int?` | `data.model_dump_json()` -> string value |
| `FilePayload` | `data: BaseModel`, `path: str` | `data.model_dump_json() + "\n"` -> JSONL line |

**Routing**: if you have multiple sinks of the same type, set `sink="name"` on the payload. With a single sink per type, the framework routes automatically.

**Error handling**: on delivery failure, `on_delivery_error()` is called. Default action: write to DLQ. The DLQ topic is auto-derived as `{source_topic}_dlq`.

## Typed messages

Define Pydantic models for your input/output and use them as type parameters:

```python
class MyHandler(BaseDrakkarHandler[InputModel, OutputModel]):
    async def arrange(self, messages, pending):
        for msg in messages:
            msg.payload  # InputModel instance, auto-deserialized
            msg.value    # raw bytes, always available as fallback
```

Non-generic `BaseDrakkarHandler` (no type params) works too -- you get raw bytes in `msg.value`.

## Scaling

Run multiple instances with the same `consumer_group`. Kafka's cooperative-sticky rebalancing distributes partitions across workers.

```bash
WORKER_ID=worker-1 python main.py
WORKER_ID=worker-2 python main.py
```

## Configuration

All config fields support environment variable override with `DRAKKAR_` prefix and `__` for nesting:

```bash
DRAKKAR_KAFKA__BROKERS=kafka:9092
DRAKKAR_EXECUTOR__MAX_EXECUTORS=16
DRAKKAR_DEBUG__PORT=8081
```

## Observability

### Debug UI

Enabled by default at `:8080`. Pages:

- `/` -- dashboard with partition tiles, pool utilization, event counters
- `/partitions` -- per-partition stats
- `/live` -- tabbed live view: Arrange (with filter, progress bars, and a right-side batch-detail sidebar), Executors (timeline), Collect
- `/debug` -- tabbed tools: Metrics, Periodic Tasks, Message Trace, Cache (when enabled), Databases, Message Probe. Deep-link `#trace/<partition>/<offset>` opens the Trace tab pre-filled.
- `/history` -- filterable event browser with partition and event type toggles
- `/task/{task_id}` -- task detail with PID, duration, CLI command, stdout/stderr

Optional: set `kafka.ui_url` and `kafka.ui_cluster_name` in config to render a small Kafka-UI icon next to every `<partition:offset>` link; clicking the icon opens the corresponding message in Kafka-UI (provectus).

### Message Probe

The **Message Probe** tab on `/debug` lets you paste a raw message value and run it end-to-end through the live handler's full pipeline -- `arrange` -> subprocess executor -> `on_task_complete` -> `on_message_complete` -> `on_window_complete` -- without touching any production state. The report shows the parsed `SourceMessage`, every generated task with stdin/stdout/stderr/exit code/duration, each hook's returned `CollectResult`, the sink payloads that **would have been** produced (grouped by sink type), every cache call made during the run, a timeline waterfall, and any exceptions with full tracebacks. Click any task row to open a right-side sidebar with the full scrollable stdin/stdout/stderr (handles 15k+ lines).

**Safety guarantees** (enforced by tests): no sink writes, no offset commits, no event-recorder rows, no cache writes, no peer sync -- zero footprint on the live system. Cache reads are opt-in via the **Use cache (read-only)** checkbox; when enabled, the probe forwards reads to the live cache but still silently suppresses writes. The `handler.cache` swap is always restored in a `finally` block, even if a hook raises.

Paste your message, click Run, and see the full behavior instead of inferring it from flight-recorder rows.

### Prometheus metrics

Exposed at `:9090/metrics`. Key metrics:

- `drakkar_messages_consumed_total{partition}`
- `drakkar_executor_tasks_total{status}`, `drakkar_executor_duration_seconds`
- `drakkar_sink_payloads_delivered_total{sink_type, sink_name}`
- `drakkar_sink_deliver_errors_total{sink_type, sink_name}`
- `drakkar_sink_deliver_duration_seconds{sink_type, sink_name}`
- `drakkar_sink_dlq_messages_total`
- `drakkar_backpressure_active`, `drakkar_total_queued`
- `drakkar_offset_lag{partition}`, `drakkar_assigned_partitions`
- `drakkar_handler_duration_seconds{hook}`
- `drakkar_worker_info` (worker_id, version, consumer_group)

### Structured logging

JSON to stderr, ECS-compatible. Every log line includes `service_name`, `worker_id`, `consumer_group`, `category`, and `timestamp`.

Use `structlog.get_logger()` for async logging in your handlers:

```python
import structlog
logger = structlog.get_logger()

# in any async hook
await logger.ainfo("my_event", category="handler", custom_field="value")
```

## Integration test

A full docker-compose example lives in `integration/` with all 6 sink types:

```bash
cd integration
docker compose up --build
```

Services and web UIs:

| URL | Service |
|-----|---------|
| `http://localhost:8081` | Worker 1 debug UI (primary workers, shared consumer group) |
| `http://localhost:8082` | Worker 2 debug UI |
| `http://localhost:8083` | Worker 3 debug UI |
| `http://localhost:8084` | Fast-worker 1 debug UI (separate consumer group, `on_window_complete` aggregation) |
| `http://localhost:8085` | Fast-worker 2 debug UI |
| `http://localhost:8087` | Redis Commander |
| `http://localhost:8088` | Kafka UI |
| `http://localhost:8089` | MongoDB Express |
| `http://localhost:9099` | Prometheus |

The integration scenario:
- 3 primary workers consuming from a 50-partition topic (main pipeline)
- 2 fast-workers on a separate consumer group demonstrating `on_window_complete` window aggregation
- Each result goes to Kafka + Postgres + MongoDB + Redis (always)
- Two Postgres sink instances (`postgres.main` + `postgres.hot`) showing multiple-sinks-of-same-type routing
- Framework cache (`self.cache`) is enabled with `scope=CLUSTER` — peer sync propagates entries between primary workers (see the Cache tab in `/debug`)
- High-match results (>20) trigger HTTP webhook
- Very high-match results (>50) write to JSONL file
- 5% simulated executor failures with retry via `on_error()`
- Failed deliveries route to DLQ or retry based on sink type

Debug recorder databases and per-worker cache databases both live in `integration/shared/`, which is mounted into every worker container as `/shared`. Recorder files are per-worker timestamped (`worker-1-2026-03-23__14_55_00.db` with a `{worker}-live.db` symlink). Cache files are single per-worker (`worker-1-cache.db.actual` with a `{worker}-cache.db` symlink used for peer discovery). See `integration/shared/README.md` for details.

## Development

```bash
uv sync --extra=dev
uv run pytest --cov=drakkar
uvx ruff check drakkar/ tests/
uv run ty check drakkar/
```

## License

MIT
