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

This starts 23 services, including six workers in three clusters. (A
twenty-fourth, `load_generator_tenant_a`, is gated behind the `tenant` Compose
profile.) First run pulls images and builds workers (~2 minutes).
Subsequent runs start in seconds.

To stop everything:

```bash
docker-compose down -v
```

### Profiles

`just integration-up` takes a profile that sets the size of the run:

| Profile | Messages | Use it for |
|---------|----------|------------|
| `full` (default) | 5000, repeat counts as given | The long soak. A run with a service-backed provider takes about two hours. |
| `quick` | 1000, repeat counts scaled by 0.2 (`REPEAT_SCALE=0.2`) | Development checks and the run matrix. Every producer phase stays, with less work in each. |

```bash
just integration-up provider=redis profile=quick
```

A value of `TOTAL_MESSAGES` or `REPEAT_SCALE` that is already in your
environment wins over the profile. The scenario scripts read the message
total from the running producer, so a profile cannot disagree with the
checks.

---

## Services & Ports

### Infrastructure

| Service | Port | Description |
|---------|------|-------------|
| **Kafka** | 9092 | Single-node KRaft broker (no Zookeeper) |
| **PostgreSQL** | 5432 | `drakkar` database with `search_results` table |
| **PostgreSQL (fleet)** | 5434 | `drakkar_fleet` database: the main cluster's worker registry and periodic election when the harness runs with the `postgres` provider |
| **Redis (fleet)** | 6380 | Database 3: the main cluster's worker registry and periodic election when the harness runs with the `redis` provider. The `default` user is off; the only user is `drakkar` (password `drakkar`) |
| **Consul (fleet)** | 8500 | A single Consul agent in dev mode: the main cluster's worker registry and periodic election when the harness runs with the `consul` provider. ACLs are on and deny by default; the one-shot `consul-fleet-init` creates the workers' token |
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
| worker-1 | [localhost:8081](http://localhost:8081) | [localhost:9090](http://localhost:9090/metrics) | [localhost:8091](http://localhost:8091/process) | 4 executors, all 6 sinks, **webapp enabled**, recorder in memory only |
| worker-2 | [localhost:8082](http://localhost:8082) | [localhost:9091](http://localhost:9091/metrics) | -- | 4 executors, all 6 sinks |
| worker-3 | [localhost:8083](http://localhost:8083) | [localhost:9093](http://localhost:9093/metrics) | -- | 4 executors, all 6 sinks |

Webapp port follows the integration convention **UI port + 10**
(worker-1 UI `8081` → webapp `8091`). The framework default outside
the integration cluster is `8090`; the integration `drakkar.yaml`
overrides it to keep the port plan consistent across services.

Only worker-1 has `DK_SOURCES__HTTP__ENABLED=true`; worker-2 and worker-3
are Kafka-only. This is intentional -- it demonstrates the
[load-balancer caveat](webapp.md#load-balancer-caveat) for the
synchronous HTTP endpoint: requests land where they're sent, no
framework-level redistribution.

### Fast Cluster (symbol counting)

2 workers, consumer group `drakkar-fast`, same source topic. Both keep
their recorder history in Postgres (`DK_UI__RECORDER__PERSISTENCE:
"postgres"`): the table `drakkar.fast_recorder_events` on `postgres-fleet`,
with the table prefix `fast` and a retention of 2 hours. `postgres-fleet`
starts with every provider.

| Worker | Operator UI | Metrics | Config |
|--------|----------|---------|--------|
| fast-worker-1 | [localhost:8084](http://localhost:8084) | [localhost:9094](http://localhost:9094/metrics) | 2 executors, Kafka-only sink |
| fast-worker-2 | [localhost:8085](http://localhost:8085) | [localhost:9095](http://localhost:9095/metrics) | 2 executors, Kafka-only sink |

### HTTP Cluster (the same search, over HTTP)

One worker, no consumer group. `http-worker` runs with
`sources.kafka.enabled: false`, so it is the harness's
[HTTP-only worker](sources.md#http-only).

| Worker | Operator UI | Metrics | HTTP source | Config |
|--------|----------|---------|-------------|--------|
| http-worker | [localhost:8086](http://localhost:8086) | [localhost:9096](http://localhost:9096/metrics) | [localhost:8092](http://localhost:8092/process) | 2 executors, one Kafka sink, **no Kafka source**, DLQ off |

Its Compose healthcheck polls `/readyz` rather than `/healthz`, which is
exactly the HTTP-only readiness rule: the worker turns ready once its socket
is bound and every sink is connected, because it has no partitions to wait
for. Its
`dlq.topic` is empty and its Kafka source is off, so it also exercises
the `dlq=off` path — its startup summary line reads
`sources=[http:8092] ... dlq=off`.

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

**The HTTP mirror.** After producing a message, the producer POSTs the
same JSON body to `http-worker` with probability `HTTP_MIRROR_RATIO`
(default `0.01`, so about 1%). It logs one line per mirrored request:

```
mirror: req-000123 -> 200
```

Requests are **mirrored, not diverted**: the message still goes to Kafka,
so the main and fast clusters see it as usual. An HTTP error or a timeout
(5 s) on the mirror never fails the producer run. `HTTP_MIRROR_URL` and
`HTTP_MIRROR_TOKEN` point it at `http://http-worker:8092/process` with the
`mirror` client's bearer token.

### Main Cluster Processing

Each search message flows through:

1. [arrange()](handler.md#arrange-required) -- creates a ripgrep task with CLI args
2. Executor runs `/usr/local/bin/run-rg` (ripgrep wrapper). A matching line longer
   than 500 characters is cut, so one result stays well under the Kafka message limit.
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

### HTTP Cluster Processing

`http-worker` reuses the main cluster's ripgrep handler
(`integration/worker/http_handler.py`, selected by `WORKER_HANDLER=http-search`;
its config is `integration/http-worker/drakkar.yaml`). Its
`arrange_http_request` calls the same task-building code `arrange` uses, so
a mirrored request runs the identical search. The response carries a
`SearchResponse` (request id, per-task match counts, succeeded and failed
counts), and because `sources.http.sinks_enabled` is on, the result also
goes to the `search-results-http` Kafka topic.

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

## Comparing the two paths

The mirror makes the two input paths comparable on the same request. Both
topics are keyed by request id, so one id can be read from each:

1. Find a mirrored id in the producer log: `docker-compose logs producer |
   grep mirror` prints lines like `mirror: req-000123 -> 200`.
2. Open the [Kafka UI](http://localhost:8088) and read `search-results` —
   the Kafka path's output for that id, written by the main cluster.
3. Read `search-results-http` — the HTTP path's output for the same id,
   written by `http-worker`.

The search itself is the same, so the two results should agree on the match
counts. What differs is everything around it: the Kafka row carries a real
partition and offset, the HTTP row carries the synthetic `partition = -1`
and a per-worker sequence number.

!!! note "What the traces anchor on"
    On `http-worker`, a request trace in the operator UI carries the
    handler's own annotations only where the **synthetic offset** lines up.
    The handler mirrors the runner's per-request sequence number as that
    offset, so the two agree for requests the runner numbered — the trace
    is anchored on a counter, not on a Kafka coordinate.

!!! warning "Nothing verifies the mirror"
    `verify_delivery.py` does not check the mirrored 1%. It rebuilds the
    produced id set and checks the **Kafka** path's sinks. The mirror is a
    demonstration of the HTTP-only worker, not an assertion; a mirrored
    request that silently failed would not fail the harness.

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
DK_SOURCES__HTTP__SINKS_ENABLED: "true"
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

### Partitions and sinks tables (on `/`)

The Dashboard also holds the per-partition table (queue size, pending
offsets, committed offset, consumer lag) and the per-sink table (delivered
count, error count, retry count, last delivery time and duration).

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

### Operator Docs Site (`/docs/`)

The handbook the worker serves for itself (see
[Operator Docs Site](ui-docs.md)) -- hand-written HTML under
`integration/worker/docs-site/`, not an mkdocs build, to show that any
prebuilt static directory works. The nav entry opens it in a new tab;
`ui.docs.anchors` also puts a book icon on the `module` label chips of a
task detail page, on the `archive_results_db` row of the sinks table, and
on the `scan_window_processing_range` entry in the Live timeline's event
list. Each icon opens that page in a drawer beside the live view.

---

## Kafka Topics

| Topic | Partitions | Producers | Consumers |
|-------|-----------|-----------|-----------|
| `search-requests` | 50 | producer | main cluster, fast cluster |
| `search-results` | 50 | main cluster | -- |
| `search-requests_dlq` | 50 | main cluster (DLQ) | -- |
| `symbol-counts` | 50 | fast cluster | -- |
| `search-results-http` | 50 | http cluster | -- |

Browse topics in the [Kafka UI](http://localhost:8088).

---

## Worker Autodiscovery

All workers write to `/shared` (mounted as a volume). Each worker
publishes its card to the [fleet registry](fleet.md) and reads the cards
of the others, so the [operator UI](observability.md#operator-ui) lists
every worker. Use the worker dropdown in the top-right nav to switch
between workers without remembering ports.

The fast workers and the HTTP worker always keep their cards in the
registry folder in `/shared`. The main cluster (`worker-1`..`worker-3`)
takes its registry and periodic-election provider from the argument of
`just integration-up`:

```bash
just integration-up                      # filesystem: cards in /shared
just integration-up provider=postgres    # postgres: cards in the postgres-fleet database
just integration-up provider=redis       # redis: cards in database 3 of redis-fleet
just integration-up provider=consul      # consul: cards in the KV store of consul-fleet
```

The argument sets `HARNESS_PROVIDER`, which `docker-compose.yml` passes to
the three workers as `DK_FLEET__REGISTRY__PROVIDER` and
`DK_PERIODIC__ELECTION__PROVIDER`. The periodic election decides which
worker runs each slot of a cluster task: the archive and the handler's
`harness_cluster_tick`. With `postgres`, both use the
[connection](fleet.md#connections) `connections.postgres.main`: its DSN in
`drakkar.yaml` has no password, and `docker-compose.yml` supplies the
full DSN through `DK_CONNECTIONS__POSTGRES__MAIN__DSN`. The worker
creates the `drakkar` schema and its tables in `drakkar_fleet` on first
use.

With `redis`, both use the connection `connections.redis.main`: its URL
in `drakkar.yaml` has no password, and `docker-compose.yml` supplies the
full URL through `DK_CONNECTIONS__REDIS__MAIN__URL`. The keys start with
`drakkar:` (the `key_prefix`) and are in database 3 (the path of the URL).
`redis-fleet` starts with the `default` user off and one user, `drakkar`,
that has exactly the rights that
[Security](security.md#provider-credentials) gives the providers:
`~drakkar:* +@read +@write +@scripting +@connection +time`. A run with
`redis` therefore also shows that these rights are enough.

With `consul`, both use the connection `connections.consul.main`: its
entry in `drakkar.yaml` has the `address` and the `key_prefix` (`drakkar`)
and no token, and `docker-compose.yml` supplies the ACL token through
`DK_CONNECTIONS__CONSUL__MAIN__TOKEN`. `consul-fleet` runs with ACLs on and
the default policy `deny`. Before the workers start, `consul-fleet-init`
(the script `integration/infra/init-consul.sh`) creates the policy
`drakkar-fleet` and a token of that policy for the workers. The policy has
exactly the two rules that
[Security](security.md#provider-credentials) gives the providers:
`key_prefix "drakkar/" { policy = "write" }` and
`session_prefix "" { policy = "write" }`. A run with `consul` therefore
also shows that these rights are enough. The management token of the
agent and the workers' token are fixed UUIDs in `docker-compose.yml`. They
are harness values, not secrets. The agent keeps its state in memory: a
new `consul-fleet` container starts empty, and the next
`just integration-up` runs `consul-fleet-init` again, which finds or
creates the policy and the token.

The main cluster's registry timings are short, so that the scenarios
below finish in minutes: a card is published every 2 s and read every
3 s, a worker whose card is 10 s old is offline, and a card 30 s old is
deleted. With `consul` a card is not listed offline: the session of the
card has a TTL of 10 s, and Consul deletes the card with the session about
20 s after the last publish. Each call to the provider has 2 s and one
retry, and three failed operations open the
[circuit breaker](fleet.md#resilience) for 10 s. The periodic election
uses the same resilience block (`periodic.election.resilience`).

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
- Consumer lag changes in the partitions table on `/`

The script does not stop at "watch it happen". When the outage sequence
finishes it runs the delivery verification below and **exits non-zero if
anything was lost, duplicated past the cap, or reordered** -- so a chaos run
is a pass/fail gate, not an invitation to read three dashboards.

---

## Provider outage scenarios

Six scripts check how the main cluster's registry and cluster tasks
behave when their provider, the network or a worker fails, and how the
recorder memory store works with and without a recorder file. Start the harness with a
provider, wait until the workers are healthy, then run the scenarios with
the same provider:

```bash
just integration-up provider=redis
just integration-scenarios provider=redis
just integration-down
```

`just integration-scenarios` runs the seven scripts in the order below and
stops at the first failure. Each script prints one `VERDICT` line and
exits 0 on a pass (or when it does not apply to the provider) and 1 on a
failure. You can also run one script alone, for example
`HARNESS_PROVIDER=redis integration/scenario-secrets.sh`.

`just integration-scenarios` first runs `integration/wait-ready.sh`. It
returns when worker-1 lists worker-2 and worker-3 online, and it fails after
300 seconds and prints the last list it saw. The scenarios start only after
that.

A run with `postgres`, `redis` or `consul` and the `full` profile takes about two hours: the paced phases of
the producer take about 7 minutes, and the main cluster then needs an hour or more to
work off the backlog before `just verify-delivery` can check it. The outage
scenario allows up to two hours for that drain. With `filesystem` the outage
scenario does not apply, and the other five take a few minutes each (the
cluster-tasks scenario watches its slots for about five minutes).

| Script | What it does | What it proves |
|--------|--------------|----------------|
| `scenario-service-outage.sh` | Pauses the service of the provider (`postgres-fleet`, `redis-fleet` or `consul-fleet`) for 90 s, twice: the second pause starts after the recovery from the first. A paused container keeps its connections open and answers nothing. When `consul-fleet` resumes, it ends at once every session whose TTL ran out during the pause, so the workers create new sessions. | For each pause: within 40 s the registry breaker of worker-1 opens (`drakkar_provider_breaker_open{feature="registry",provider="<provider>"}` is 1), within 60 s of the unpause it is closed and worker-1 lists worker-2 and worker-3 online, and no worker restarts. The second pause shows that the first one left nothing behind that keeps the next calls from reaching their time limit. Once the producer has printed `PRODUCER_DONE` and the main cluster has worked off its backlog (consumer lag 0, an hour or more after the flood phase), `just verify-delivery` passes. With `filesystem` it does not apply: there is no service to pause. |
| `scenario-worker-cut-off.sh` | Disconnects worker-3 from the Compose network for 90 s. | With `postgres` or `redis`, worker-1 lists worker-3 offline within 30 s, because worker-3 cannot reach the provider's service and its card gets old. (The check waits for the verdict: a card 30 s old is deleted, and worker-3 would then be missing from the list rather than offline.) With `consul`, worker-3 is missing from worker-1's list, or listed offline, within 30 s (the check accepts both): it cannot renew its session, and Consul deletes the card when the session ends. After the reconnect, worker-3 finds that its session has ended and publishes with a new one. With `filesystem` worker-3 stays online, because its card goes through the shared folder and not the network. worker-3's completed-task counter does not go down, worker-3 is online again within 20 s of the reconnect, and no worker restarts. |
| `scenario-unclean-exit.sh` | Kills worker-2, which cannot withdraw its card. With `consul` it starts worker-2 again at once. | With `filesystem`, `postgres` or `redis`: within 20 s worker-1 lists worker-2 offline, and within 45 s of the kill the card is deleted. After a start, worker-2 is online within 20 s with a new `instance`. With `consul`: the card is still held by the session of the killed worker, which lives until its TTL runs out. The new worker-2 takes the card over: within 10 s of the start (one heartbeat, one refresh and 5 s) worker-1 lists it with a new `instance`, and the agent log shows that the new worker-2 destroyed the old session, which did not end by its TTL. When the host starts the worker too slowly for that, the scenario fails and names the measured times. The second half tests the archive election; see [The election half](#the-election-half). |
| `scenario-secrets.sh` | Reads the config reference and the metrics page of each main-cluster worker, and their logs. | None of them contains the password of the Postgres DSN or of the Redis URL (`drakkar:drakkar@`, `postgresql://drakkar:`, `redis://drakkar:`) or the Consul token, and the config reference lists the three connections, with `connections.consul.main.token` masked. Runs with every provider. |
| `scenario-cluster-tasks.sh` | Watches 8 slots of `harness_cluster_tick`, a cluster task of the harness handler with a slot every 30 s of epoch time, and 15 s after the last of them. Each run logs `harness_cluster_tick` with its slot. Then it reads `/api/v1/workers` on worker-2. | No slot ran on two workers, and at least 7 of the 8 slots ran exactly once (a slot whose lease does not come within its 10 s acquire window is skipped, so one missed slot is allowed). worker-2 lists worker-1 with `cluster_tasks_match: false` (worker-1 turns the archive off, so its cluster task definitions differ), and worker-3 and itself with `true`. worker-3 matches only because worker-2 and worker-3 run the archive with the same settings and the same handler: a config change on one of them makes the check fail. Runs with every provider. |
| `scenario-recorder-memory.sh` | Reads `/api/v1/debug/databases`, `/api/v1/events`, the metrics and `/api/v1/debug/download-live` of worker-1, which keeps its recorder events in memory only (`DK_UI__RECORDER__PERSISTENCE: "none"`, a memory window of 60 s), and of worker-2, which keeps the recorder file. When the shared history folder is still empty (a fresh harness, the script run alone), it first plants one small due window with `integration/plant_history.py`. | worker-1 lists exactly one own database, the memory store, and no live file, and it lists the shared history folders. Its events answer has rows. Once it has run for 90 s, `drakkar_recorder_memory_rows` is above 0, `drakkar_recorder_memory_evicted_rows_total{reason="window"}` is above 0, `drakkar_recorder_memory_oldest_age_seconds` is at most 75 (the window, one flush interval and slack), and `drakkar_recorder_memory_allocated_bytes` is at least `drakkar_recorder_memory_bytes`. Its `download-live` without `source` answers a SQLite database (the memory store), and with `source=file` 404. worker-2 lists a memory row and a live row, and both `source=file` and `source=memory` download a SQLite database. The verdict line carries worker-1's memory numbers and the count of `ui_read_memory_timed_out` lines in the logs of the three workers. Runs with every provider. |
| `scenario-recorder-postgres.sh` | Reads the table `drakkar.fast_recorder_events` with `psql` in `postgres-fleet`, and the history search, the metrics and `/api/v1/debug/databases` of fast-worker-1. It creates a partition of the hour 5 hours ago by hand and restarts fast-worker-1, then pauses `postgres-fleet` for 60 s and, later, for 15 s. | The table exists, its comment is `retention_hours=2`, and the partitions of the current hour to 3 hours ahead exist. The rows of the cluster `fast cluster` grow over 30 s, with the names of both fast workers. `/api/v1/debug/trace?history=1` for one consumed message answers a non-empty list: every row has `worker_name`, one row is `consumed`, and no row has a `cluster` key. Within 60 s of the restart the hand-made partition is gone and fast-worker-1 logged `recorder_postgres_partitions_changed` with it. During the 60 s pause `drakkar_recorder_postgres_queue_rows` rises above 0; within 60 s of the unpause it falls below 5000 (`batch_rows`) and `drakkar_recorder_postgres_rows_written_total` rises; `drakkar_recorder_postgres_dropped_rows_total` does not change. During the 15 s pause the history search answers 503 `{"detail": "history search failed"}` within 12 s (`ui.trace.history_max_seconds` and 2 s). fast-worker-1 lists exactly one own database, the memory store. Runs with every provider. |

The scenarios change the harness (a killed and restarted worker, a paused
service), so run `just integration-down` before the next run.

### Drain and delivery check

`integration/verify-after-drain.sh` waits until the producer printed
`PRODUCER_DONE` and the consumer lag of the main cluster is 0. Then it runs
`just verify-delivery` with the message total of the producer. The outage
scenario calls it at its end. The run matrix calls it for every provider,
because the outage scenario does not apply to `filesystem`.

### The election half

The second half of `scenario-unclean-exit.sh` tests the election of the
archive task with the provider of the run. It uses these archive settings
of `integration/worker/drakkar.yaml` (`history.archive`):

| Key | Value | Effect |
|-----|-------|--------|
| `window_hours` | 1 | A window is one hour. |
| `due_after_hours` | 1 | A window is due one hour after it ends. |
| `retention_days` | 1 | Archives are kept one day. |
| `interval` | 30 | An archive slot comes every 30 s of epoch time. At each slot one worker of the cluster wins the lease, looks for due windows and archives them. |
| `lease_seconds` | 40 | The lease of a holder that died ends 40 s after its last refresh; the next slot after that is won by another worker. |

Worker-1 is the observer of every scenario and sets
`DK_HISTORY__ARCHIVE__ENABLED: "false"` in `docker-compose.yml`, so it never
runs the archive and never holds its lock. It also keeps its recorder events in memory only
(`DK_UI__RECORDER__PERSISTENCE: "none"`), so it writes no recorder file
and seals nothing; worker-2 and worker-3 keep the file, seal it and archive
it. The lock is in
`tasks/main_cluster/history.archive.lock` in the shared folder
(`filesystem`), in the `drakkar.task_locks` table (`postgres`), under the key
`drakkar:task-locks:main_cluster/history.archive` (`redis`) or under the key
`drakkar/task-locks/main_cluster/history.archive` (`consul`).

The script plants a window with `integration/plant_history.py`: 50 sealed
recorder files of the main cluster, with real meta files, in one hour window
that ended 12 hours ago, so it is due at once. The size is set so that one
archive pass takes between 8 and 25 s, which is less than the 40 s lease.

The winner of a slot holds the lock for its whole pass and at least 20 s,
also when nothing is due. A held lock alone therefore does not show the pass
of the planted window, and the script times the plant against the slots:

1. It waits until 2 s after a slot boundary S0. The winner of S0 has listed
   the history folder by then.
2. It plants the window. When the plant ends later than 2 s before the next
   boundary S1, the script fails and names the plant's duration: the winner
   of S1 may have listed a partial window.
3. At S1 + 1 s it fails when the archive is already published (the winner of
   S0 acquired late and listed the plant). Otherwise the worker that holds
   the lock in two polls in a row is the winner of S1, which archives the
   planted window. It must be worker-2 or worker-3.
4. It kills that worker at once and checks that the archive is not published
   yet and every planted file is still there. If the kill comes after the
   archive is published, the script fails and names the planted size.
5. Within 150 s of the kill another worker must hold the lock, publish the
   archive and delete every planted file. The dead holder's lease ends up to
   40 s after the kill, the next slot comes within 30 s, and the pass takes up
   to about 40 s under load. With `filesystem` and `postgres` the new holder
   logs `periodic_lock_taken_over` with its provider and the archive's scope.
6. No slot of `harness_cluster_tick` ran twice from the plant to the end of
   the half.

On a Linux host the containers create the history and task folders as root
with mode 0700, so planting from the host needs write access there (a
`sudo chown` of `integration/shared/history` is enough; the script does not
write into `integration/shared/tasks`). macOS Docker hosts are not affected.

### Run matrix

```bash
just integration-matrix profile=quick
```

For `filesystem`, `postgres`, `redis` and `consul`, in this order, the recipe
clears `integration/shared/` (except `README.md`; it is a bind mount that
`integration-down` does not clear), starts the harness with
`integration-up`, waits with `wait-ready.sh`, runs the seven scenarios and
`verify-after-drain.sh`, and runs `integration-down`, also after a failure. It
goes on after a failing step, prints a table with PASS, FAIL or N/A for each
provider and step, and exits non-zero when any step failed. `N/A` means the
scenario does not apply to the provider.

---

## Verifying delivery

`verify_delivery.py` is what makes the harness a test rather than a demo. It
rebuilds the exact set of request ids the producer sent -- they are numbered
`req-000001`..`req-<TOTAL_MESSAGES>`, not random -- and checks it against
what the sinks hold.

The [HTTP mirror](#comparing-the-two-paths) does not affect it. Mirrored
requests are sent to Kafka as well, so the produced id set is unchanged, and
the mirror writes to its own topic rather than to the sinks these checks
read. Nothing here verifies the mirrored 1%:

| Check | What a failure means |
|-------|----------------------|
| `no_loss` | A produced request has no `request_summaries` row. An offset was committed past a message whose sinks never confirmed. |
| `no_phantom` | A summary row exists for an id nobody sent. A corrupted key path, or a database still holding rows from an earlier run. |
| `update_not_reordered` | A request above the notify threshold still reads `notified = false`. The handler appends `UPDATE notified` immediately after the `UPSERT` that creates the row, and the upsert never writes that column -- so an `UPDATE` that ran first matched nothing and lost the write. This is the on-disk signature of the Postgres sink executing payloads out of order. |
| `task_rows_present` | A rollup reports successful tasks but wrote no `search_results` rows. The per-request summary was delivered while the per-task writes behind it were not. A task that several requests share (the same pattern and file in one window) writes one row for each of them. |
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
