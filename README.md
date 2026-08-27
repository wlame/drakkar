# Drakkar

**Kafka → subprocess pool → sinks, for Python 3.13+.**

Drakkar is an orchestration framework for CPU-heavy stream processing: it consumes messages from Kafka, turns them into invocations of an external binary run in a managed subprocess pool, and delivers the results to any combination of Kafka, PostgreSQL, MongoDB, Redis, HTTP, and files. You write a handler with a few async hooks; the framework owns polling, windowing, backpressure, delivery, offset commits, and observability.

Workers are the Drakkars, executors are the Vikings.

```mermaid
flowchart LR
    K["Kafka<br>source topic"] -- "poll" --> W
    subgraph worker ["Drakkar worker — one pipeline per partition"]
        W["window of<br>messages"] --> A["arrange()<br>your code"]
        A -- "tasks" --> P["subprocess pool<br>runs your binary"]
        P -- "results" --> T["on_task_complete()<br>your code"]
    end
    T -- "payloads" --> S["sinks — any combination<br>Kafka · Postgres · MongoDB · Redis · HTTP · files"]
    S -- "failed delivery" --> DLQ["DLQ topic"]
    S -- "all confirmed" --> CO["commit offsets<br>(watermark)"]
```

<!-- TODO(wlame): screenshot or GIF of the Live executor timeline goes here.
<p align="center"><img src="docs/media/timeline.gif" alt="Live executor timeline" width="800"></p>
-->

**[Documentation](https://wlame.github.io/drakkar)** — full guides for every feature below.

> [!IMPORTANT]
> **Drakkar is an internal tool.** Its operator UI and API are built for a trusted,
> private network and must not be exposed to an untrusted one. See
> [Security posture](#security-posture) below.

## Features

- **Per-partition pipelines** with watermark offset tracking — commits happen only after every sink confirmed
- **Pluggable sinks** — Kafka, PostgreSQL, MongoDB, Redis, HTTP, filesystem; multiple named instances per type, third-party sinks via entry points
- **Dead letter queue** with replay tooling; `on_delivery_error()` decides retry / skip / DLQ per failure
- **Backpressure** via Kafka pause/resume — memory stays bounded regardless of lag
- **Typed messages** — Pydantic models as type parameters, auto de/serialization
- **Operator UI** — live executor timeline, partition lag, message tracing, and a Message Probe that runs a pasted message through the full pipeline with zero footprint on production state
- **UI customization** — handler-defined probe tabs, links/badges/formats on any field, declared dashboard pages — all server-side, no client code
- **Cache** (optional) — `self.cache` key/value store with write-behind SQLite and peer sync across workers
- **Offload** — `await self.offload(fn, ...)` keeps CPU-bound hook work off the event loop
- **Webapp** (optional) — the same handler pipeline exposed as a synchronous HTTP endpoint with auth and rate limits
- **Observability** — Prometheus metrics, ECS-compatible structured logging, flight recorder (SQLite event log), runtime-health and host-pressure monitors, task cost/throughput stats
- **Kubernetes-ready** — `/healthz` and `/readyz` probes, reference manifests, crash/OOM detection on restart

The operator UI is [drakkar-ui](https://github.com/wlame/drakkar-ui), a versioned SPA the worker fetches at startup and caches on disk — so co-located workers share one download, and the UI ships on its own release cadence.

## Quick start

```bash
uv init my-processor && cd my-processor
uv add py-drakkar
```

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
        # window of Kafka messages -> subprocess tasks
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
        # subprocess output -> sink payloads
        output = JobOutput(
            job_id=result.task.metadata['job_id'],
            result=result.stdout.strip(),
        )
        return CollectResult(
            kafka=[KafkaPayload(data=output, key=output.job_id.encode())],
            postgres=[PostgresPayload(table='results', data=output)],
        )
```

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
    job_results_out:
      topic: "job-results"
  postgres:
    main_db:
      dsn: "postgresql://user:pass@localhost:5432/mydb"
```

```python
# main.py
from drakkar import DrakkarApp
from handler import MyHandler

DrakkarApp(handler=MyHandler(), config_path='drakkar.yaml').run()
```

```bash
WORKER_ID=worker-1 python main.py
```

Every config field can be overridden by environment variables (`DK_` prefix, `__` for nesting: `DK_EXECUTOR__MAX_EXECUTORS=16`). Scale horizontally by running more workers in the same consumer group; cooperative-sticky rebalancing spreads partitions without stopping the others.

More hooks are available for aggregation and error handling — `on_message_complete` (fan-out → fan-in), `on_window_complete`, `on_error`, `@periodic` background tasks, lifecycle hooks. See the [handler guide](https://wlame.github.io/drakkar/handler/).

## Try it

A full docker-compose environment with Kafka, all six sink types, five workers, and a load generator lives in [`integration/`](integration/):

```bash
cd integration
docker compose up --build
```

Then open `http://localhost:8081` for the first worker's UI. See the [integration guide](https://wlame.github.io/drakkar/integration/).

## Security posture

**Drakkar is built to run inside a system you own.** The operator UI, the
`/api/v1` JSON and WebSocket surface, and the optional HTTP ingress assume a
trusted, private network. Do not expose them to the public internet, to a
shared corporate network you do not control, or to a user population wider
than the engineers operating the service.

Drakkar does ship security controls — an opt-in UI bearer token with a
WebSocket origin check, per-client tokens and rate limits on the HTTP
ingress, path containment so the download endpoint can only ever serve
recorder databases, secret masking in the config view and the flight
recorder, and bounds on request size, header size and connection lifetime.
Turn them on. But treat every one of them as **defence in depth, not a
security perimeter**: they exist to reduce blast radius and catch mistakes,
not to withstand a determined attacker who already has network access.

A worker has no user model, no roles and no tenancy. Anyone who can reach a
port and present the one configured token is an operator of that worker. The
real boundary sits where the context is:

- **your network** decides who reaches the ports at all — this is the control that matters;
- **your ingress** owns TLS, SSO or mTLS, and connection-level limits;
- **your application** authorises your end users and validates their input;
- **Drakkar** avoids shipping footguns behind that line.

Even inside a private network the UI is an operator tool that shows task
output, arguments, redacted environment, cache contents and live event
streams — worth a token, or a bastion, if any of that is sensitive.

Full detail, including what Drakkar deliberately does **not** provide (TLS,
CSRF protection, an audit trail, a handler sandbox):
**[Security posture](https://wlame.github.io/drakkar/security/)**. To report
a vulnerability, see [`SECURITY.md`](SECURITY.md).

## Development

[`just`](https://github.com/casey/just) is the dev entrypoint; CI runs the same recipes.

```bash
just install     # uv sync with dev + perf extras
just test        # unit tests (hermetic, no network)
just ci          # format check -> lint -> types -> tests + coverage gate
just docs-serve  # live-reload docs at http://127.0.0.1:8000
just --list      # everything else (integration env, chaos test, DLQ replay, ...)
```

See [`docs/development.md`](docs/development.md) and [`CONTRIBUTING.md`](CONTRIBUTING.md).

## License

MIT — see [`LICENSE`](LICENSE).
