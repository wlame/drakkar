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

| Worker | Debug UI | Metrics | Config |
|--------|----------|---------|--------|
| worker-1 | [localhost:8081](http://localhost:8081) | [localhost:9090](http://localhost:9090/metrics) | 4 executors, all 6 sinks |
| worker-2 | [localhost:8082](http://localhost:8082) | [localhost:9091](http://localhost:9091/metrics) | 4 executors, all 6 sinks |
| worker-3 | [localhost:8083](http://localhost:8083) | [localhost:9093](http://localhost:9093/metrics) | 4 executors, all 6 sinks |

### Fast Cluster (symbol counting)

2 workers, consumer group `drakkar-fast`, same source topic.

| Worker | Debug UI | Metrics | Config |
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
3. [collect()](handler.md#collect) routes results based on match count:
   - **Always**: Kafka (full result), Postgres (metrics row), MongoDB (archive), Redis (cached summary)
   - **match_count > 20**: HTTP webhook notification
   - **match_count > 50**: JSONL file log
4. 5% simulated executor failures with retry via [on_error()](handler.md#on_error)
5. Failed deliveries route to [DLQ](sinks.md#dead-letter-queue) or retry based on sink type

### Fast Cluster Processing

Same source topic, different consumer group. Each message:

1. `arrange()` -- creates a symbol-count task
2. Executor runs `count-symbols.sh` (character counter)
3. `collect()` -- sends count to `symbol-counts` Kafka topic

Fast tasks finish in milliseconds, demonstrating [duration threshold](observability.md#duration-thresholds)
filtering and high-throughput behavior.

---

## Debug UI Pages

Open any worker's debug UI to explore:

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
- **Collect** -- recent [collect()](handler.md#collect) completions with output counts

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

All workers write to `/shared` (mounted as a volume). The [debug UI](observability.md#debug-ui)
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
`/api/dashboard` every 5 seconds and prints per-worker completed
task counts and partition counts, so you can watch rebalancing happen.

**What to observe:**

- Partition reassignment in `/history` (filter by `assigned`/`revoked` events)
- Pool utilization spike on surviving worker in `/live`
- Consumer lag changes in `/partitions`
- No message loss: total processed across all workers should equal total produced

---

## Tuning for Experiments

### Adjust message volume

Override `TOTAL_MESSAGES` on the producer:

```yaml
# docker-compose.yml → producer → environment
TOTAL_MESSAGES: "20000"
```

### Change executor concurrency

Override per-worker via environment:

```yaml
# docker-compose.yml → worker-1 → environment
DRAKKAR_EXECUTOR__MAX_WORKERS: "8"
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

### Disable debug thresholds

To see ALL tasks in the debug UI (including fast ones):

```yaml
# integration/worker/drakkar.yaml or fast-worker/drakkar.yaml
debug:
  ws_min_duration_ms: 0
  event_min_duration_ms: 0
  output_min_duration_ms: 0
  log_min_duration_ms: 0
```

### Add auth to debug UI

Protect database downloads with a token:

```yaml
debug:
  auth_token: "my-secret-token"
```

Then access protected endpoints with `?token=my-secret-token` or
`Authorization: Bearer my-secret-token` header.
