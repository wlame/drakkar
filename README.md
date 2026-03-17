# Drakkar

Kafka subprocess orchestration framework. Consumes messages from a Kafka topic, runs CPU-intensive external binaries in a managed subprocess pool, and produces results to an output topic with PostgreSQL persistence.

Workers are the Drakkars, vikings are the subprocess runners.

## What it does

```
Kafka source topic
    |
    v
[ Drakkar Worker ]
    |
    +-- poll messages (per-partition pipelines)
    +-- arrange() -> viking tasks (user hook)
    +-- run external binary via subprocess pool
    +-- collect() -> output messages + DB rows (user hook)
    +-- produce to target topic
    +-- commit offsets (watermark-based)
    |
    v
Kafka target topic + PostgreSQL
```

- **Per-partition independent pipelines** with offset watermark tracking
- **Cooperative-sticky rebalancing** -- non-revoked partitions continue without interruption
- **Backpressure** via Kafka pause/resume -- memory stays bounded regardless of consumer lag
- **Subprocess viking pool** with semaphore-based concurrency limiting
- **Typed message models** -- define Pydantic schemas for input/output, get auto-deserialization
- **Built-in debug UI** (FastAPI) with viking timeline, partition lag, message tracing
- **Flight recorder** -- SQLite event log with retention and rotation
- **Prometheus metrics** -- 21 metrics covering the full pipeline
- **Structured JSON logging** -- ECS-compatible, ready for Elastic

## Quick start

```bash
uv init my-processor && cd my-processor
uv add drakkar
```

### 1. Define your message models

```python
# models.py
from pydantic import BaseModel

class InputMessage(BaseModel):
    request_id: str
    data: str
    priority: int = 1

class OutputMessage(BaseModel):
    request_id: str
    result: str
    processed: bool
```

### 2. Implement your handler

```python
# handler.py
from drakkar import (
    BaseDrakkarHandler, CollectResult, DBRow, ErrorAction,
    VikingTask, OutputMessage, make_task_id,
)
from models import InputMessage, OutputMessage as MyOutput

class MyHandler(BaseDrakkarHandler[InputMessage, MyOutput]):
    async def arrange(self, messages, pending):
        tasks = []
        for msg in messages:
            # msg.payload is an InputMessage instance (auto-deserialized)
            tasks.append(VikingTask(
                task_id=make_task_id("proc"),
                args=["--input", msg.payload.data],
                source_offsets=[msg.offset],
                metadata={"request_id": msg.payload.request_id},
            ))
        return tasks

    async def collect(self, result):
        output = MyOutput(
            request_id=result.task.metadata["request_id"],
            result=result.stdout.strip(),
            processed=result.exit_code == 0,
        )
        return CollectResult(
            output_messages=[OutputMessage.from_model(output)],
            db_rows=[DBRow(
                table="results",
                data={"request_id": output.request_id, "status": "done"},
            )],
        )

    async def on_error(self, task, error):
        return ErrorAction.SKIP
```

### 3. Configure

```yaml
# drakkar.yaml
kafka:
  brokers: "localhost:9092"
  source_topic: "input-events"
  target_topic: "output-results"
  consumer_group: "my-workers"

viking:
  binary_path: "/usr/local/bin/my-processor"
  max_vikings: 8
  task_timeout_seconds: 120
  window_size: 20

postgres:
  dsn: "postgresql://user:pass@localhost:5432/mydb"

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
    worker_id="worker-1",
)
app.run()
```

## Handler hooks

| Hook | When | Purpose |
|---|---|---|
| `on_startup(config)` | Before components start | Modify config (e.g., auto-detect CPU count) |
| `arrange(messages, pending)` | Window of messages received | Transform messages into viking tasks |
| `collect(result)` | Each task completes | Process result into output messages + DB rows |
| `on_window_complete(results, messages)` | All tasks in a window done | Aggregate results across a window |
| `on_error(task, error)` | Task fails | Return `RETRY`, `SKIP`, or replacement tasks |
| `on_assign(partitions)` | Partitions assigned | Initialize per-partition state |
| `on_revoke(partitions)` | Partitions revoked | Cleanup per-partition state |

## Typed messages

Define Pydantic models for your input/output and use them as type parameters:

```python
class MyHandler(BaseDrakkarHandler[InputModel, OutputModel]):
    async def arrange(self, messages, pending):
        for msg in messages:
            msg.payload  # InputModel instance, auto-deserialized
            msg.value    # raw bytes, always available as fallback
```

Serialize output with `OutputMessage.from_model(pydantic_instance)`.

Non-generic `BaseDrakkarHandler` (no type params) works too -- you get raw bytes in `msg.value`.

## Scaling

Run multiple instances with the same `consumer_group`. Kafka's cooperative-sticky rebalancing distributes partitions across workers. Each worker processes its assigned partitions independently.

```bash
# Machine 1
WORKER_ID=worker-1 python main.py

# Machine 2
WORKER_ID=worker-2 python main.py

# Machine N...
```

## Configuration

All config fields support environment variable override with `DRAKKAR_` prefix and `__` for nesting:

```bash
DRAKKAR_KAFKA__BROKERS=kafka:9092
DRAKKAR_VIKING__MAX_VIKINGS=16
DRAKKAR_DEBUG__PORT=8081
```

## Observability

### Debug UI

Enabled by default at `:8080`. Pages:

- `/` -- dashboard with partition lag bars, pool utilization, event counters
- `/partitions` -- per-partition stats with committed offset, high watermark, lag
- `/vikings` -- viking timeline (scrollable, zoomable), running/pending/finished tasks
- `/history` -- filterable event browser with partition and event type toggles
- `/trace/{partition}/{offset}` -- full lifecycle of a single message
- `/task/{task_id}` -- task detail with PID, duration, CLI command, stdout/stderr

### Prometheus metrics

Exposed at `:9090/metrics`. Key metrics:

- `drakkar_messages_consumed_total`, `drakkar_messages_produced_total`
- `drakkar_viking_tasks_total{status}`, `drakkar_viking_duration_seconds`
- `drakkar_backpressure_active`, `drakkar_total_queued`
- `drakkar_offset_lag{partition}`, `drakkar_assigned_partitions`
- `drakkar_consumer_errors_total`, `drakkar_producer_errors_total`
- `drakkar_db_rows_written_total`, `drakkar_db_errors_total`
- `drakkar_handler_duration_seconds{hook}`
- `drakkar_info` (worker_id, version, consumer_group)

### Structured logging

JSON to stderr, ECS-compatible. Every log line includes `service_name`, `worker_id`, `consumer_group`, `category`, and `timestamp`.

## Integration test

A full docker-compose example lives in `integration/`:

```bash
cd integration
docker compose up --build
```

This runs 3 workers + Kafka + PostgreSQL + Kafka UI + a load-generating producer. Open:

- `http://localhost:8081` -- Worker 1 debug UI
- `http://localhost:8082` -- Worker 2 debug UI
- `http://localhost:8083` -- Worker 3 debug UI
- `http://localhost:8088` -- Kafka UI

## Development

```bash
uv sync --extra=dev
uv run pytest --cov=drakkar
```

## License

MIT
