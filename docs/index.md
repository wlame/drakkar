# Drakkar

**Kafka subprocess orchestration for Python 3.13+**

Drakkar consumes messages from Kafka, runs CPU-intensive external binaries in a managed subprocess pool, and delivers results to any combination of six sink types. Workers are the Drakkars, executors are the Vikings.

## Architecture

```
                         ┌────────────────────────────────────────────────────┐
                         │              Drakkar Worker                        │
                         │                                                    │
  ┌─────────────┐        │  poll   ┌──────────┐  tasks  ┌──────────────────┐  │
  │ Kafka source│───────►│────────►│ arrange()│────────►│ Executor Pool    │  │
  │    topic    │        │         │ (user)   │         │ (subprocess mgmt)│  │
  └─────────────┘        │         └──────────┘         └────────┬─────────┘  │
                         │                                       │            │
                         │         ┌──────────┐  results         │            │
                         │         │ collect()│◄────────────────┘            │
                         │         │ (user)   │                              │
                         │         └─────┬────┘                               │
                         │               │ payloads                           │
                         └───────────────┼────────────────────────────────────┘
                                         │
               ┌─────────────────────────┼─────────────────────────┐
               │            │            │           │             │
               ▼            ▼            ▼           ▼             ▼
          ┌────────┐  ┌──────────┐  ┌───────┐  ┌───────┐  ┌──────────┐
          │ Kafka  │  │ Postgres │  │ Mongo │  │ Redis │  │   HTTP   │
          └────────┘  └──────────┘  └───────┘  └───────┘  └──────────┘
                                                               ┌───────┐
                                                               │ Files │
                                                               └───────┘
```

Each partition runs an independent pipeline: **poll -> arrange -> execute -> collect -> deliver -> commit**. A shared executor pool with semaphore-based concurrency limits subprocess parallelism across all partitions.

## Key Features

- **Per-partition pipelines** -- independent processing with watermark-based offset tracking
- **Pluggable sinks** -- Kafka, PostgreSQL, MongoDB, Redis, HTTP, filesystem; any combination, multiple instances per type
- **Dead letter queue** -- failed deliveries route to a DLQ topic with error metadata
- **Backpressure** -- Kafka pause/resume keeps memory bounded regardless of consumer lag
- **Typed messages** -- Pydantic models for input/output with auto-deserialization
- **Debug UI** -- built-in FastAPI dashboard with executor timeline, partition lag, message tracing
- **Prometheus metrics** -- pipeline, executor, and per-sink counters/histograms
- **Structured logging** -- JSON/ECS-compatible via structlog, ready for Elastic
- **Periodic tasks** -- `@periodic` decorator for recurring background coroutines
- **Task labels** -- custom `message_label()` for human-readable log/UI identifiers
- **Error hooks** -- `on_error` for executor failures, `on_delivery_error` for sink failures (retry, skip, or DLQ)

## Quick Start

### Install

```bash
uv init my-processor && cd my-processor
uv add py-drakkar
```

### Define a handler

```python
# handler.py
from pydantic import BaseModel
from drakkar import (
    BaseDrakkarHandler, CollectResult, ExecutorTask,
    KafkaPayload, PostgresPayload, make_task_id,
)

class JobInput(BaseModel):
    job_id: str
    command: str

class JobOutput(BaseModel):
    job_id: str
    result: str

class MyHandler(BaseDrakkarHandler[JobInput, JobOutput]):
    async def arrange(self, messages, pending):
        return [
            ExecutorTask(
                task_id=make_task_id('job'),
                args=['--cmd', msg.payload.command],
                source_offsets=[msg.offset],
                metadata={'job_id': msg.payload.job_id},
            )
            for msg in messages
        ]

    async def collect(self, result):
        output = JobOutput(
            job_id=result.task.metadata['job_id'],
            result=result.stdout.strip(),
        )
        return CollectResult(
            kafka=[KafkaPayload(data=output, key=output.job_id.encode())],
            postgres=[PostgresPayload(table='results', data=output)],
        )
```

### Configure

```yaml
# drakkar.yaml
kafka:
  brokers: "localhost:9092"
  source_topic: "jobs"
  consumer_group: "my-workers"

executor:
  binary_path: "/usr/local/bin/my-tool"
  max_workers: 8
  task_timeout_seconds: 60

sinks:
  kafka:
    output:
      topic: "job-results"
  postgres:
    main:
      dsn: "postgresql://user:pass@localhost:5432/mydb"
```

All config fields support env var override with `DRAKKAR_` prefix and `__` for nesting (e.g. `DRAKKAR_EXECUTOR__MAX_WORKERS=16`).

### Run

```python
# main.py
from drakkar import DrakkarApp
from handler import MyHandler

app = DrakkarApp(handler=MyHandler(), config_path='drakkar.yaml')
app.run()
```

```bash
WORKER_ID=worker-1 python main.py
```

Scale horizontally by running multiple instances with the same `consumer_group`. Kafka's cooperative-sticky rebalancing distributes partitions across workers.

## Documentation

| Page | Contents |
|------|----------|
| [Handler](handler.md) | `BaseDrakkarHandler` hooks: `arrange`, `collect`, `on_error`, `on_window_complete`, lifecycle hooks |
| [Configuration](configuration.md) | Full YAML reference, env var overrides, `DrakkarConfig` model |
| [Sinks](sinks.md) | Sink types, payload models, routing, multi-instance setup |
| [Executor](executor.md) | Subprocess pool, concurrency, timeouts, retries, binary resolution |
| [Observability](observability.md) | Debug UI pages, Prometheus metrics, structured logging setup |
| [Data Flow](data-flow.md) | End-to-end pipeline walkthrough: poll through commit |
