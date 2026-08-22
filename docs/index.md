# Drakkar

**Kafka subprocess orchestration for Python 3.13+**

Drakkar consumes messages from Kafka, runs CPU-intensive external binaries in a managed subprocess pool, and delivers results to any combination of six sink types. Workers are the Drakkars, executors are the Vikings.

## Architecture

<div class="diagram-light" markdown>
```mermaid
flowchart LR
    K["Kafka\nsource topic"] -- "poll" --> W

    subgraph worker ["Drakkar worker — one pipeline per partition"]
        W["window of\nmessages"] --> A["arrange()\n<i>your code</i>"]
        A -- "tasks" --> P["subprocess pool\nruns your binary"]
        P -- "results" --> T["on_task_complete()\n<i>your code</i>"]
    end

    T -- "payloads" --> sinks
    subgraph sinks ["sinks — any combination"]
        direction TB
        SK["Kafka"] ~~~ SP["Postgres"] ~~~ SM["MongoDB"]
        SR["Redis"] ~~~ SH["HTTP"] ~~~ SF["files"]
    end
    sinks -- "all confirmed" --> CO["commit offsets\n(watermark)"]
    sinks -- "failed delivery" --> DLQ["DLQ topic"]

    style worker fill:#f0fdfa,stroke:#0d9488,stroke-width:2px,color:#1a1a1a
    style sinks fill:#f8fafc,stroke:#94a3b8,stroke-width:1px,color:#6b7280
    style K fill:#e0f2fe,stroke:#0284c7,color:#1a1a1a
    style W fill:#f8fafc,stroke:#64748b,color:#1a1a1a
    style A fill:#fef3c7,stroke:#d97706,color:#1a1a1a
    style P fill:#f5f3ee,stroke:#64748b,color:#1a1a1a
    style T fill:#fef3c7,stroke:#d97706,color:#1a1a1a
    style CO fill:#e0f2fe,stroke:#0284c7,color:#1a1a1a
    style DLQ fill:#fee2e2,stroke:#dc2626,color:#450a0a
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
    K["Kafka\nsource topic"] -- "poll" --> W

    subgraph worker ["Drakkar worker — one pipeline per partition"]
        W["window of\nmessages"] --> A["arrange()\n<i>your code</i>"]
        A -- "tasks" --> P["subprocess pool\nruns your binary"]
        P -- "results" --> T["on_task_complete()\n<i>your code</i>"]
    end

    T -- "payloads" --> sinks
    subgraph sinks ["sinks — any combination"]
        direction TB
        SK["Kafka"] ~~~ SP["Postgres"] ~~~ SM["MongoDB"]
        SR["Redis"] ~~~ SH["HTTP"] ~~~ SF["files"]
    end
    sinks -- "all confirmed" --> CO["commit offsets\n(watermark)"]
    sinks -- "failed delivery" --> DLQ["DLQ topic"]

    style worker fill:#1a3a3a,stroke:#2dd4bf,stroke-width:2px,color:#e2e8f0
    style sinks fill:#1e293b,stroke:#64748b,stroke-width:1px,color:#94a3b8
    style K fill:#172554,stroke:#60a5fa,color:#e2e8f0
    style W fill:#1e293b,stroke:#64748b,color:#e2e8f0
    style A fill:#422006,stroke:#f59e0b,color:#fef3c7
    style P fill:#1e293b,stroke:#64748b,color:#e2e8f0
    style T fill:#422006,stroke:#f59e0b,color:#fef3c7
    style CO fill:#172554,stroke:#60a5fa,color:#e2e8f0
    style DLQ fill:#7f1d1d,stroke:#f87171,color:#fee2e2
    style SK fill:#065f46,stroke:#34d399,color:#d1fae5
    style SP fill:#065f46,stroke:#34d399,color:#d1fae5
    style SM fill:#065f46,stroke:#34d399,color:#d1fae5
    style SR fill:#065f46,stroke:#34d399,color:#d1fae5
    style SH fill:#065f46,stroke:#34d399,color:#d1fae5
    style SF fill:#065f46,stroke:#34d399,color:#d1fae5
```
</div>

Each partition runs an independent pipeline: **poll &rarr; arrange &rarr; execute &rarr; on_task_complete &rarr; on_message_complete &rarr; deliver &rarr; commit** (with `on_window_complete` firing at window boundaries for coarser aggregation). A shared executor pool with semaphore-based concurrency limits subprocess parallelism across all partitions.

## Key Features

- **[Per-partition pipelines](data-flow.md#phase-3-window-collection-and-arrangement)** -- independent processing with watermark-based [offset tracking](handler.md#offset-commit-logic)
- **[Pluggable sinks](sinks.md)** -- Kafka, PostgreSQL, MongoDB, Redis, HTTP, filesystem; any combination, multiple instances per type
- **[Dead letter queue](sinks.md#dead-letter-queue)** -- failed deliveries route to a DLQ topic with error metadata
- **[Backpressure](performance.md#backpressure)** -- Kafka pause/resume keeps memory bounded regardless of consumer lag
- **[Typed messages](handler.md#typed-messages)** -- Pydantic models for input/output with auto-deserialization
- **[Cache (optional)](cache.md)** -- `self.cache` key/value store with memory + write-behind SQLite + eventually-consistent peer sync across workers
- **[Operator UI](observability.md#operator-ui)** -- the versioned [drakkar-ui](https://github.com/wlame/drakkar-ui) SPA (fetched from GitHub Releases at startup, one shared cache for Python and Go workers) with executor timeline, partition lag, message tracing; built-in fallback pages when offline
- **[UI customization](ui-enrichment.md)** -- a handler-defined [Message Probe tab](probe-user-details.md), [links/badges/formats/detail panels](ui-enrichment.md) on any probe field or table column, and [declared dashboard pages](ui-pages.md) -- all opt-in, no client-side code
- **[Timeline tuning](ui-timeline.md)** -- configurable history depth, first-match-wins bar color rules from task labels/fields, and label roles (tag, caption, highlight, filter, marker) for the Live timeline
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

    async def on_task_complete(self, result):
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

All config fields support env var override with `DK_` prefix and `__` for nesting (e.g. `DK_EXECUTOR__MAX_EXECUTORS=16`).

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
| [Handler](handler.md) | `BaseDrakkarHandler` hooks: `arrange`, `on_task_complete`, `on_message_complete`, `on_window_complete`, `on_error`, lifecycle hooks |
| [Cache](cache.md) | `self.cache` API, scope rules, peer sync, LWW semantics, "delete is local-only" sharp edge |
| [Fan-out](fan-out.md) | One message → many tasks → one aggregate. `MessageGroup`, `on_message_complete`, replacement-chain tracing. |
| [Annotations](annotations.md) | `self.annotate(...)` — handler-authored diagnostics surfaced per message/task/window in the UI |
| [Probe User Details](probe-user-details.md) | `probe_details_model` — a handler-defined tab in the Message Probe, `probe_field()` views and write caps |
| [UI Enrichment](ui-enrichment.md) | Links, badges, formats, hints, and detail panels for probe-details fields and table columns; custom cell renderers |
| [Declared UI Pages](ui-pages.md) | `ui_pages` — a handler's own dashboard page built from built-in data sources, no client-side code |
| [Timeline Tuning](ui-timeline.md) | `ui.timeline` — history depth, first-match-wins color rules, and tag/caption/highlight/filter/marker label roles |
| [Configuration](configuration.md) | Full YAML reference, env var overrides, `DrakkarConfig` model |
| [Features & Enable Order](features.md) | Which switch enables what, dependency rules, tiered rollout order |
| [Sinks](sinks.md) | Sink types, payload models, routing, multi-instance setup |
| [Sink Write Operations](sink-write-operations.md) | Declarative write operations plus the raw-SQL/command escape hatch, per sink type |
| [Executor](executor.md) | Subprocess pool, concurrency, timeouts, retries, binary resolution |
| [Webapp](webapp.md) | Synchronous HTTP pipeline, auth, rate limits, status codes |
| [Observability](observability.md) | Operator UI pages, Prometheus metrics, structured logging setup |
| [Runtime Health](runtime-health.md) | Event-loop lag monitor: heartbeat, stall sampler, state badge, Prometheus metrics |
| [Local Databases](local-databases.md) | SQLite spec: schemas, discovery, peer sync, mixed-fleet support |
| [Performance](performance.md) | Per-task overhead, bottleneck analysis, tuning recommendations |
| [Config Calculator](calculator.md) | Interactive calculator for recommended config values |
| [Integration Tests](integration.md) | Docker Compose test environment, chaos test scenario |
| [Data Flow](data-flow.md) | End-to-end pipeline walkthrough: poll through commit |
| [Config Reference](config-reference.md) | Annotated `drakkar.yaml` with every field + env override |
| [Deployment](deployment.md) | Kubernetes probes, rolling restarts |
| [Development](development.md) | uv setup, just recipes, docs build, CI |
| [FAQ](faq.md) | Operator Q&A across all subsystems |
