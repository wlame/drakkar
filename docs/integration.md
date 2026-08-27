# Integration Tests

The `integration/` directory in the repository contains a full Docker
Compose environment with all six [sink types](sinks.md), multiple worker clusters,
realistic load patterns, and a chaos test scenario. It is intended for
**development and testing only** -- not as a production deployment
reference. Credentials are hardcoded, services run without
authentication, and all data is ephemeral.

---

## Quick Start

```bash
cd integration
docker-compose up --build -d
```

This starts 14 services. First run pulls images and builds workers
(~2 minutes). Subsequent runs start in seconds.

To stop everything:

```bash
docker-compose down -v
```

---

## Services & Ports

### Infrastructure

| Service | Port | Description |
|---------|------|-------------|
| **Kafka** | 9092 | Single-node KRaft broker (no Zookeeper) |
| **PostgreSQL** | 5432 | `drakkar` database with `search_results` table |
| **MongoDB** | 27017 | Document archive |
| **Redis** | 6379 | Cached search summaries (1h TTL) |
| **Webhook** | 9000 | HTTP echo server for webhook sink |
| **Prometheus** | [localhost:9099](http://localhost:9099) | Scrapes all worker metrics |

### Web UIs

| URL | Service |
|-----|---------|
| [localhost:8088](http://localhost:8088) | Kafka UI -- topics, messages, consumer groups |
| [localhost:8089](http://localhost:8089) | MongoDB Express -- collections, documents |
| [localhost:8087](http://localhost:8087) | Redis Commander -- keys, TTLs |

### Main Cluster (ripgrep search)

3 workers, consumer group `drakkar-integration`, 50-partition topic.

| Worker | Operator UI | Metrics | Webapp | Config |
|--------|----------|---------|--------|--------|
| worker-1 | [localhost:8081](http://localhost:8081) | [localhost:9090](http://localhost:9090/metrics) | [localhost:8091](http://localhost:8091/process) | 4 executors, all 6 sinks, **webapp enabled** |
| worker-2 | [localhost:8082](http://localhost:8082) | [localhost:9091](http://localhost:9091/metrics) | -- | 4 executors, all 6 sinks |
| worker-3 | [localhost:8083](http://localhost:8083) | [localhost:9093](http://localhost:9093/metrics) | -- | 4 executors, all 6 sinks |

Webapp port follows the integration convention **UI port + 10**
(worker-1 UI `8081` → webapp `8091`). The framework default outside
the integration cluster is `8090`; the integration `drakkar.yaml`
overrides it to keep the port plan consistent across services.

Only worker-1 has `DK_WEBAPP__ENABLED=true`; worker-2 and worker-3 are
Kafka-only. This is intentional -- it demonstrates the
[load-balancer caveat](webapp.md#load-balancer-caveat) for the
synchronous HTTP endpoint: requests land where they're sent, no
framework-level redistribution.

### Fast Cluster (symbol counting)

2 workers, consumer group `drakkar-fast`, same source topic.

| Worker | Operator UI | Metrics | Config |
|--------|----------|---------|--------|
| fast-worker-1 | [localhost:8084](http://localhost:8084) | [localhost:9094](http://localhost:9094/metrics) | 2 executors, Kafka-only sink |
| fast-worker-2 | [localhost:8085](http://localhost:8085) | [localhost:9095](http://localhost:9095/metrics) | 2 executors, Kafka-only sink |

---

## What It Does

### Message Producer

The `producer` container sends 10,000 messages to `search-requests` in
phases that exercise different load patterns:

| Phase | Messages | Rate | Purpose |
|-------|----------|------|---------|
| 0. Slow drip | 10 | 0.2/sec | Worker startup, first messages |
| 1. Warm-up | 500 | 15/sec | Steady ramp |
| 2. Pause | -- | 15s silence | Consumer idle behavior |
| 3. Burst | 200 | max speed | Burst handling, backpressure |
| 4. Steady | 2,000 | 20/sec | Normal throughput |
| 5. Pause | -- | 10s silence | Second idle period |
| 6. Burst | 300 | max speed | Second burst |
| 7. Cool-down | remaining | 10/sec | Drain |
| 8. Flood | 5,000 | max speed | Massive consumer lag |

3% of messages are "slow outliers" with `repeat: 150-250` (the ripgrep
binary runs the search that many times), creating tasks that take
minutes instead of seconds. This exercises timeout handling and
mixed-duration workloads.

### Main Cluster Processing

Each search message flows through:

1. [arrange()](handler.md#arrange-required) -- creates a ripgrep task with CLI args
2. Executor runs `/usr/local/bin/run-rg` (ripgrep wrapper)
3. [on_task_complete()](handler.md#on_task_complete) routes results based on match count:
   - **Always**: Kafka (full result), Postgres (metrics row), MongoDB (archive), Redis (cached summary)
   - **match_count > 20**: HTTP webhook notification
   - **match_count > 50**: JSONL file log
4. 5% simulated executor failures with retry via [on_error()](handler.md#on_error)
5. Failed deliveries route to [DLQ](sinks.md#dead-letter-queue) or retry based on sink type

### Fast Cluster Processing

Same source topic, different consumer group. Each message:

1. `arrange()` -- creates a symbol-count task
2. Executor runs `count-symbols.sh` (character counter)
3. `on_task_complete()` -- sends count to `symbol-counts` Kafka topic

Fast tasks finish in milliseconds, demonstrating [duration threshold](observability.md#duration-thresholds)
filtering and high-throughput behavior.

### Webapp Pipeline (worker-1 only)

Worker-1 also runs the [synchronous HTTP webapp](webapp.md). The
endpoint accepts a `RankRequest` body and returns a framework-built
`WebReport` JSON containing the user's `RankResponse` plus per-task and
sink stats. See `integration/worker/handler.py` for the
`arrange_http_request` / `on_http_request_complete` implementation and
the `task_priority` override that pushes HTTP-origin tasks ahead of
Kafka tasks at the executor gate.

Two clients are configured (in `integration/worker/drakkar.yaml`):

| Client | Token | RPM cap |
|--------|-------|---------|
| `anonymous` | (empty) | 4 |
| `tenant-A` | `integration-tenant-a-token-do-not-use-in-prod` | 60 |

A `load_generator` container drives a request every 10 seconds using
the anonymous client (so you can watch the rate-limit kick in at the
4-rpm cap). A second `load_generator_tenant_a` service is gated behind
the `tenant` Compose profile and uses the higher-rpm tenant token.

---

## Testing the Webapp

### Hands-on with `curl`

While the stack is running, send a request as **tenant-A** (the
60-rpm cap leaves plenty of headroom for manual testing; the
anonymous client is typically saturated by the load_generator):

```bash
curl -sS -X POST http://localhost:8091/process \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer integration-tenant-a-token-do-not-use-in-prod' \
  -d '{"request_id": "manual-1", "score": 42}' | jq .
```

You should see a 200 response with the framework-built `WebReport`
envelope (`request_id`, `client`, `started_at`, `duration_ms`,
`status`, `result`, `tasks`, `task_summary`, `cache`, `sinks`,
`timeline`).

To **provoke** a 429 deliberately on tenant-A: hit the endpoint at
>1 req/sec for ~60 seconds. The 60-rpm window fills and the 61st
request gets 429 with a `Retry-After` header and the `hint` field
documented in [webapp.md](webapp.md#status-codes).

### Watching the load_generator

```bash
# Default profile -- anonymous traffic only
docker-compose logs -f load_generator

# With the tenant-A profile
docker-compose --profile tenant up -d load_generator_tenant_a
docker-compose logs -f load_generator_tenant_a
```

Each iteration logs the request body and the response status. You'll
see 200 responses up to the rpm cap, then 429s once the sliding window
fills.

### What to check in the UI

Open [worker-1's UI](http://localhost:8081):

- **Dashboard `/`** -- a "WebApp" tile appears alongside the partition
  tiles, showing in-flight requests, per-client rpm caps, and 60s
  success/error counts.
- **Live `/live`** -- HTTP-origin tasks render with a distinct purple
  highlight and the identifier `<client>:<request_id[:8]>` instead of
  the Kafka `p<partition>:o<offset>` form.
- **History `/history`** -- the **Origin** filter (top right) lets you
  view kafka-only / http-only / all events. Webapp-specific event
  types (`webapp_request_received`, `webapp_request_completed`,
  `webapp_request_timeout`, `webapp_request_rate_limited`,
  `webapp_request_auth_failed`, `webapp_request_dropped_after_timeout`)
  are visible here.
- **Task detail `/task/{id}`** -- clicking an HTTP-origin task shows
  Client / Request ID (instead of Partition / Offset), the truncated
  request body, and the final response.

### What to check in metrics

The webapp metrics live at
[localhost:9090/metrics](http://localhost:9090/metrics) alongside the
existing executor and sink counters. Filter for `drakkar_webapp_`:

| Metric | Type | Labels |
|--------|------|--------|
| `drakkar_webapp_requests_total` | Counter | `{client, status}` |
| `drakkar_webapp_request_duration_seconds` | Histogram | `{client, status}` |
| `drakkar_webapp_inflight` | Gauge | -- |
| `drakkar_webapp_dropped_after_timeout_total` | Counter | `{client}` |
| `drakkar_webapp_rpm_limit` | Gauge | `{client}` (informational) |

A useful Prometheus query while the load_generator runs:

```promql
sum by (client, status) (rate(drakkar_webapp_requests_total[1m]))
```

### Demonstrating priority scheduling

The integration handler's `task_priority` override returns a
priority key of `(0, ...)` for `task.origin == 'http'` tasks and
`(1, source_offset)` for Kafka tasks. Under sustained Kafka load, a
manual `curl` request lands in the executor gate ahead of the queued
Kafka tasks. The effect is visible in `/live` -- HTTP tasks dequeue
near-instantly even when the Kafka partition queues are full.

To exercise this end-to-end:

1. Start the stack and the producer (default).
2. Wait until pool utilization on worker-1 is near 100% from
   Kafka traffic.
3. Hit `/process` with `curl` (or just leave the `load_generator`
   running).
4. Watch `/live` on worker-1: HTTP-origin tasks (purple) are scheduled
   immediately; Kafka tasks remain queued behind them.

### Toggling sinks_enabled

The webapp's sinks-enabled mode is OFF by default in the integration
config. To exercise it:

```bash
# In integration/docker-compose.yml -> worker-1 -> environment
DK_WEBAPP__SINKS_ENABLED: "true"
```

Restart worker-1. Now each HTTP request also flows through the
handler's `on_message_complete` and writes to Kafka / Postgres /
MongoDB / Redis like a Kafka-source message. The framework adds a
`sinks: { ... }` summary to the response body documenting per-sink
attempted/delivered/dlq counts.

---

## Operator UI Pages

Open any worker's UI to explore:

### Dashboard (`/`)

Partition tiles with queue depth and lag, executor pool utilization bar,
event counters, Prometheus graph links (click any metric card).
Custom links configured via `custom_links` in config.

### Partitions (`/partitions`)

Per-partition breakdown: queue size, pending offsets, committed offset,
consumer lag. Click a partition for its event history.

### Sinks (`/sinks`)

Per-sink delivery stats: delivered count, error count, retry count,
last delivery time and duration.

### Live Pipeline (`/live`)

Three tabs, all WebSocket-powered:

- **Arrange** -- recent [arrange()](handler.md#arrange-required) calls with [message labels](handler.md#message_label), task counts, durations
- **Executors** -- scrollable timeline with task bars (green=completed, yellow=running, red=failed). Hover for task detail. Zoom in/out with +/- buttons.
- **Collect** -- recent [on_task_complete()](handler.md#on_task_complete) completions with output counts

The pool utilization bar and running/finished task tables update in
real time. [Task labels](handler.md#task-labels) (`request_id`, `pattern`) appear in the Labels
column and hover detail.

### History (`/history`)

Filterable event browser. Toggle event types (consumed, arranged,
task_started, task_completed, task_failed, etc.) and filter by
partition. Paginated.

### Debug (`/debug`)

Three collapsible sections:

- **Metrics** -- all Prometheus metrics (framework + user), filterable by source
- **Message Trace** -- enter `partition:offset` to trace a message through the full pipeline, including cross-worker search
- **Databases** -- SQLite [flight recorder](observability.md#flight-recorder) files, sortable/filterable, downloadable. Select multiple and merge into one file.

### Task Detail (`/task/{id}`)

Full task lifecycle: status, labels, partition, duration, PID, CLI
command, source offsets (linked to trace), stdout/stderr output, event
timeline.

---

## Kafka Topics

| Topic | Partitions | Producers | Consumers |
|-------|-----------|-----------|-----------|
| `search-requests` | 50 | producer | main cluster, fast cluster |
| `search-results` | 50 | main cluster | -- |
| `search-requests_dlq` | 50 | main cluster (DLQ) | -- |
| `symbol-counts` | 50 | fast cluster | -- |

Browse topics in the [Kafka UI](http://localhost:8088).

---

## Worker Autodiscovery

All workers write to `/shared` (mounted as a volume). The [operator UI](observability.md#operator-ui)
scans this directory for `*-live.db` symlinks and discovers peer
workers via [worker autodiscovery](observability.md#worker-autodiscovery). Use the worker dropdown in the top-right nav to switch between
workers without remembering ports.

---

## Chaos Test

The `chaos-test.sh` script simulates cascading worker failures and
recoveries:

```bash
cd integration
docker-compose up --build -d
./chaos-test.sh
```

### Scenario: Rolling Outage

| Time | Event | Live Workers |
|------|-------|-------------|
| t1 | ~3000/10000 tasks processed | w1, w2, w3 |
| t1 + 30s | worker-2 stops | w1, w3 |
| t1 + 150s | worker-3 stops | w1 only |
| t1 + 390s | worker-2 starts, worker-1 stops | w2 only |
| t1 + 450s | worker-1 starts | w1, w2 |
| t1 + 510s | worker-3 starts | w1, w2, w3 |

Total runtime: ~10 minutes. The script polls each worker's
`/api/v1/dashboard` every 5 seconds and prints per-worker completed
task counts and partition counts, so you can watch rebalancing happen.

**What to observe:**

- Partition reassignment in `/history` (filter by `assigned`/`revoked` events)
- Pool utilization spike on surviving worker in `/live`
- Consumer lag changes in `/partitions`

The script does not stop at "watch it happen". When the outage sequence
finishes it runs the delivery verification below and **exits non-zero if
anything was lost, duplicated past the cap, or reordered** -- so a chaos run
is a pass/fail gate, not an invitation to read three dashboards.

---

## Verifying delivery

`verify_delivery.py` is what makes the harness a test rather than a demo. It
rebuilds the exact set of request ids the producer sent -- they are numbered
`req-000001`..`req-<TOTAL_MESSAGES>`, not random -- and checks it against
what the sinks hold:

| Check | What a failure means |
|-------|----------------------|
| `no_loss` | A produced request has no `request_summaries` row. An offset was committed past a message whose sinks never confirmed. |
| `no_phantom` | A summary row exists for an id nobody sent. A corrupted key path, or a database still holding rows from an earlier run. |
| `update_not_reordered` | A request above the notify threshold still reads `notified = false`. The handler appends `UPDATE notified` immediately after the `UPSERT` that creates the row, and the upsert never writes that column -- so an `UPDATE` that ran first matched nothing and lost the write. This is the on-disk signature of the Postgres sink executing payloads out of order. |
| `task_rows_present` | A rollup reports successful tasks but wrote no `search_results` rows. The per-request summary was delivered while the per-task writes behind it were not. |
| `duplication_bounded` | `search_results` rows per request passed the cap. At-least-once permits duplicates -- the flood phase redelivers every request on purpose -- but this many means messages are being replayed in a loop. |

Run it once the producer has finished and the pipeline has drained:

```bash
just verify-delivery 5000              # the TOTAL_MESSAGES the producer ran with
just verify-delivery 300 --dsn=...     # other flags pass straight through
```

It waits for the `request_summaries` count to stop moving before checking,
because a request still in flight is indistinguishable from a lost one. Pass
`--wait-seconds=0` to check immediately.

The scheduled `Integration` workflow runs the same recipe against a small
300-message corpus. The checks compare against the exact produced id set
rather than a sample, so a small corpus is as conclusive as a large one --
it just finishes inside the job's budget.

---

## Tuning for Experiments

### Adjust message volume

`TOTAL_MESSAGES` is read from the environment, so export it before starting
the harness rather than editing the file:

```bash
TOTAL_MESSAGES=20000 just integration-up
just verify-delivery 20000
```

Pass the same number to `verify-delivery`: request ids run `req-000001`
through `req-<TOTAL_MESSAGES>`, and that is how the verification knows what
to expect.

### Change executor concurrency

Override per-worker via environment:

```yaml
# docker-compose.yml → worker-1 → environment
DK_EXECUTOR__MAX_EXECUTORS: "8"
```

### Test with fewer partitions

Edit `integration/infra/init-kafka.sh`:

```bash
--partitions 10   # instead of 50
```

### Simulate slow tasks

Increase the slow-outlier percentage in `integration/infra/producer.py`:

```python
# line 86: change 0.03 to 0.15 for 15% slow tasks
if random.random() < 0.15:
```

### Change rebalance speed

The main cluster uses `session_timeout_ms: 10000` (10s) for fast
rebalance detection. Increase for production-like behavior:

```yaml
# integration/worker/drakkar.yaml
session_timeout_ms: 45000
```

### Disable duration thresholds

To see ALL tasks in the UI (including fast ones):

```yaml
# integration/worker/drakkar.yaml or fast-worker/drakkar.yaml
ui:
  ws_min_duration_ms: 0
  log_min_duration_ms: 0
  recorder:
    event_min_duration_ms: 0
    output_min_duration_ms: 0
```

### Add auth to the UI

Protect database downloads with a token:

```yaml
ui:
  auth_token: "my-secret-token"
```

Then access protected endpoints with `?token=my-secret-token` or
`Authorization: Bearer my-secret-token` header.
