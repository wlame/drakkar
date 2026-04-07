# Drakkar

**Kafka subprocess orchestration for Python 3.13+**

Drakkar consumes messages from Kafka, runs CPU-intensive external binaries in a managed subprocess pool, and delivers results to any combination of six sink types. Workers are the Drakkars, executors are the Vikings.

## Architecture

<div class="diagram-light" markdown>
```mermaid
flowchart LR
    K["Kafka\nsource topic"] --> P["poll"]

    subgraph worker ["Drakkar Worker"]
        P --> A["arrange()\n<i>user hook</i>"]
        subgraph pool ["Executor Pool"]
            direction TB
            S0["#0 ▰▰▰▰▰▰▰▰▰▰▰▰"]
            S1["#1 ▰▰▰▰▰▰"]
            S2["#2 ▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰"]
            S3["#3 ▰▰▰▰"]
            S0 ~~~ S1 ~~~ S2 ~~~ S3
        end
        A -- "tasks" --> pool
        pool -- "results" --> C["collect()\n<i>user hook</i>"]
    end

    C --> SK["Kafka"]
    C --> SP["Postgres"]
    C --> SM["Mongo"]
    C --> SR["Redis"]
    C --> SH["HTTP"]
    C --> SF["Files"]

    style worker fill:#f0fdfa,stroke:#0d9488,stroke-width:2px,color:#1a1a1a
    style pool fill:#f5f3ee,stroke:#94a3b8,stroke-width:1px,color:#6b7280
    style P fill:#f8fafc,stroke:#64748b,color:#1a1a1a
    style K fill:#e0f2fe,stroke:#0284c7,color:#1a1a1a
    style A fill:#fef3c7,stroke:#d97706,color:#1a1a1a
    style C fill:#fef3c7,stroke:#d97706,color:#1a1a1a
    style S0 fill:#34d399,stroke:#059669,color:#064e3b
    style S1 fill:#fbbf24,stroke:#d97706,color:#451a03
    style S2 fill:#34d399,stroke:#059669,color:#064e3b
    style S3 fill:#f87171,stroke:#dc2626,color:#450a0a
    style SK fill:#d1fae5,stroke:#059669,color:#1a1a1a
    style SP fill:#d1fae5,stroke:#059669,color:#1a1a1a
    style SM fill:#d1fae5,stroke:#059669,color:#1a1a1a
    style SR fill:#d1fae5,stroke:#059669,color:#1a1a1a
    style SH fill:#d1fae5,stroke:#059669,color:#1a1a1a
    style SF fill:#d1fae5,stroke:#059669,color:#1a1a1a
```
</div>

<div class="diagram-dark" markdown>
```mermaid
flowchart LR
    K["Kafka\nsource topic"] --> P["poll"]

    subgraph worker ["Drakkar Worker"]
        P --> A["arrange()\n<i>user hook</i>"]
        subgraph pool ["Executor Pool"]
            direction TB
            S0["#0 ▰▰▰▰▰▰▰▰▰▰▰▰"]
            S1["#1 ▰▰▰▰▰▰"]
            S2["#2 ▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰"]
            S3["#3 ▰▰▰▰"]
            S0 ~~~ S1 ~~~ S2 ~~~ S3
        end
        A -- "tasks" --> pool
        pool -- "results" --> C["collect()\n<i>user hook</i>"]
    end

    C --> SK["Kafka"]
    C --> SP["Postgres"]
    C --> SM["Mongo"]
    C --> SR["Redis"]
    C --> SH["HTTP"]
    C --> SF["Files"]

    style worker fill:#1a3a3a,stroke:#2dd4bf,stroke-width:2px,color:#e2e8f0
    style pool fill:#1e293b,stroke:#64748b,stroke-width:1px,color:#94a3b8
    style P fill:#1e293b,stroke:#64748b,color:#e2e8f0
    style K fill:#172554,stroke:#60a5fa,color:#e2e8f0
    style A fill:#422006,stroke:#f59e0b,color:#fef3c7
    style C fill:#422006,stroke:#f59e0b,color:#fef3c7
    style S0 fill:#065f46,stroke:#34d399,color:#d1fae5
    style S1 fill:#78350f,stroke:#fbbf24,color:#fef3c7
    style S2 fill:#065f46,stroke:#34d399,color:#d1fae5
    style S3 fill:#7f1d1d,stroke:#f87171,color:#fee2e2
    style SK fill:#052e16,stroke:#4ade80,color:#d1fae5
    style SP fill:#052e16,stroke:#4ade80,color:#d1fae5
    style SM fill:#052e16,stroke:#4ade80,color:#d1fae5
    style SR fill:#052e16,stroke:#4ade80,color:#d1fae5
    style SH fill:#052e16,stroke:#4ade80,color:#d1fae5
    style SF fill:#052e16,stroke:#4ade80,color:#d1fae5
```
</div>

Each partition runs an independent pipeline: **poll &rarr; arrange &rarr; execute &rarr; collect &rarr; deliver &rarr; commit**. A shared executor pool with semaphore-based concurrency limits subprocess parallelism across all partitions.

## Key Features

- **[Per-partition pipelines](data-flow.md#phase-3-window-collection-and-arrangement)** -- independent processing with watermark-based [offset tracking](handler.md#offset-commit-logic)
- **[Pluggable sinks](sinks.md)** -- Kafka, PostgreSQL, MongoDB, Redis, HTTP, filesystem; any combination, multiple instances per type
- **[Dead letter queue](sinks.md#dead-letter-queue)** -- failed deliveries route to a DLQ topic with error metadata
- **[Backpressure](performance.md#backpressure)** -- Kafka pause/resume keeps memory bounded regardless of consumer lag
- **[Typed messages](handler.md#typed-messages)** -- Pydantic models for input/output with auto-deserialization
- **[Debug UI](observability.md#debug-ui)** -- built-in FastAPI dashboard with executor timeline, partition lag, message tracing
- **[Prometheus metrics](observability.md#prometheus-metrics)** -- pipeline, executor, and per-sink counters/histograms
- **[Structured logging](observability.md#structured-logging)** -- JSON/ECS-compatible via structlog, ready for Elastic
- **[Periodic tasks](handler.md#periodic-tasks)** -- `@periodic` decorator for recurring background coroutines
- **[Task labels](handler.md#task-labels)** -- custom [message_label()](handler.md#message_label) for human-readable log/UI identifiers
- **[Error hooks](handler.md#on_error)** -- [on_error](handler.md#on_error) for executor failures, [on_delivery_error](handler.md#on_delivery_error) for sink failures (retry, skip, or DLQ)

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
  max_executors: 8
  task_timeout_seconds: 60

sinks:
  kafka:
    output:
      topic: "job-results"
  postgres:
    main:
      dsn: "postgresql://user:pass@localhost:5432/mydb"
```

All config fields support env var override with `DRAKKAR_` prefix and `__` for nesting (e.g. `DRAKKAR_EXECUTOR__MAX_EXECUTORS=16`).

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
| [Performance](performance.md) | Per-task overhead, bottleneck analysis, tuning recommendations |
| [Config Calculator](calculator.md) | Interactive calculator for recommended config values |
| [Integration Tests](integration.md) | Docker Compose test environment, chaos test scenario |
| [Data Flow](data-flow.md) | End-to-end pipeline walkthrough: poll through commit |
