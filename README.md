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
    +-- collect() -> sink payloads (user hook)
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

    async def collect(self, result):
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
  max_workers: 8
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

## Handler hooks

| Hook | When | Purpose |
|---|---|---|
| `on_startup(config)` | Before components start | Modify config (e.g., auto-detect CPU count) |
| `on_ready(config, db_pool)` | After sinks connected | Initialize state from DB, run migrations |
| `arrange(messages, pending)` | Window of messages received | Transform messages into executor tasks |
| `collect(result)` | Each task completes | Process result into sink payloads |
| `on_window_complete(results, messages)` | All tasks in a window done | Aggregate results across a window |
| `on_error(task, error)` | Task fails | Return `RETRY`, `SKIP`, or replacement tasks |
| `on_delivery_error(error)` | Sink delivery fails | Return `DLQ` (default), `RETRY`, or `SKIP` |
| `on_assign(partitions)` | Partitions assigned | Initialize per-partition state |
| `on_revoke(partitions)` | Partitions revoked | Cleanup per-partition state |

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
DRAKKAR_EXECUTOR__MAX_WORKERS=16
DRAKKAR_DEBUG__PORT=8081
```

## Observability

### Debug UI

Enabled by default at `:8080`. Pages:

- `/` -- dashboard with partition tiles, pool utilization, event counters
- `/partitions` -- per-partition stats
- `/live` -- tabbed live view: Arrange, Executors (timeline), Collect, Trace
- `/history` -- filterable event browser with partition and event type toggles
- `/trace/{partition}/{offset}` -- full lifecycle of a single message
- `/task/{task_id}` -- task detail with PID, duration, CLI command, stdout/stderr

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
| `http://localhost:8081` | Worker 1 debug UI |
| `http://localhost:8082` | Worker 2 debug UI |
| `http://localhost:8083` | Worker 3 debug UI |
| `http://localhost:8088` | Kafka UI |
| `http://localhost:8089` | MongoDB Express |
| `http://localhost:8087` | Redis Commander |

The integration scenario:
- 3 workers consuming from 50-partition topic
- Each result goes to Kafka + Postgres + MongoDB + Redis (always)
- High-match results (>20) trigger HTTP webhook
- Very high-match results (>50) write to JSONL file
- 5% simulated executor failures with retry via `on_error()`
- Failed deliveries route to DLQ or retry based on sink type

Debug databases are stored in `integration/shared/` with per-worker filenames and automatic timestamping (e.g. `worker-1-2026-03-23__14_55_00.db`). A `{worker}-live.db` symlink points to the current database while running. Files persist across restarts and are rotated automatically. See `integration/shared/README.md` for details.

## Development

```bash
uv sync --extra=dev
uv run pytest --cov=drakkar
uvx ruff check drakkar/ tests/
uv run ty check drakkar/
```

## License

MIT
