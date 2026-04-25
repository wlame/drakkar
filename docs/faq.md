# Frequently Asked Questions

A living Q&A page drawing from real operator questions and common first-time-reader confusions. Answers are deliberately short — click through for depth. Contributions welcome.

---

## About Drakkar

### What is Drakkar?

A Python framework for building Kafka → subprocess → multi-sink pipelines. You write a handler describing how to turn input messages into executor-task specs; the framework polls Kafka, runs your binaries with backpressure, captures results, and fans them out to one or more sinks (Kafka, Postgres, Mongo, HTTP, Redis, Filesystem). See [Architecture](index.md#architecture) and [Data Flow](data-flow.md).

### What workloads is Drakkar designed for?

**Streaming CPU-bound work done by external CLI tools or compiled binaries**, with async Python handling the I/O on either side. The shape of a good fit is:

- **Ingest**: messages arrive on Kafka (async I/O).
- **Prep** (`arrange()`): optional async lookups — cache, small DB queries, feature-flag checks — that build per-task payloads without blocking the loop.
- **Work**: each task spawns a subprocess running your CPU-heavy binary (ripgrep, a Rust/Go/C++ analyzer, a numerical tool). This is where the CPU goes — outside Python, so the GIL is irrelevant and N subprocesses truly use N cores.
- **Collect** (`on_task_complete` / `on_message_complete` / `on_window_complete`): async, builds sink records.
- **Emit**: records fan out in parallel to one or more sinks (async I/O).

The `ExecutorPool` semaphore sits at the center, throttling concurrent subprocesses to `max_executors` so you saturate CPU without oversubscribing it. Backpressure pauses the Kafka consumer when the pipeline is already full. See [Performance recommendations](performance.md#configuration-recommendations) and [Executor System](executor.md).

### What is Drakkar NOT a good fit for?

- **Pure async-Python workloads with no subprocess stage** — a plain `aiokafka` consumer + async sinks is simpler; Drakkar's subprocess machinery becomes pure overhead.
- **Very short tasks** where the per-task subprocess launch overhead (~10ms) dominates the actual work. If your task is ≤ 1ms of real work, use [precomputed tasks](handler.md#precomputed-task-results-skip-the-subprocess) or in-process logic. See [Bottleneck: Subprocess Launch](performance.md#bottleneck-subprocess-launch).
- **Ultra-low-latency pipelines** (single-digit ms end-to-end) — windowing and batching cost is real.
- **Exactly-once transactional streams** — Drakkar is at-least-once with DLQ safety; see the delivery section below.

### How does Drakkar compare to Celery, Faust, Kafka Streams, Benthos?

Drakkar is narrower and more opinionated than any of them — the sweet spot is **Kafka input + CPU-heavy external binary + multi-sink fan-out** with strict per-partition ordering. Rough shape of how the others differ:

| Tool | Input sources | Work stage | Python-native? | Notes |
|------|--------------|-----------|----------------|-------|
| **Drakkar** | Kafka only | **Subprocess pool** (external binary via stdin/stdout) | Yes (3.13+) | Built-in DLQ, circuit breakers, flight recorder, peer-sync cache, live debug UI. Offsets commit per-partition after *all* sinks ack. |
| **Celery** | Broker-agnostic (Redis, RabbitMQ, SQS, Kafka via plugin) | In-process Python function | Yes | General task queue; no native Kafka partition ordering; one broker at a time per worker. |
| **Faust** | Kafka only | In-process async Python (streams & tables) | Yes | Stream-processing DSL (agents, tables, windows). No first-class subprocess executor; you bring your own. |
| **Kafka Streams** | Kafka only | In-process JVM code | No (Java/Scala) | Stateful stream processor with changelog-backed stores. No Python. |
| **Benthos** (now Bento/Redpanda Connect) | ~50 sources | Config-driven processors (Go) | No | YAML pipeline DSL; excellent fan-out; no user-written Python; handlers are Bloblang/WASM. |

Use Drakkar when your handler binary is Rust/Go/C++/a CLI you don't own, you need per-partition ordering, and you want multi-sink delivery (Kafka + Postgres + HTTP + ...) with at-least-once guarantees and a DLQ. Use one of the others when your work is pure in-process Python (Faust), pure Go/config (Benthos), or not Kafka-first (Celery).

### What languages can my handler's executor binary be written in?

Any language. The executor launches a subprocess and communicates via stdin/stdout/exit-code — the binary only needs to read a JSON payload from stdin and write results to stdout. See [Executor System](executor.md).

### Why Python 3.13+?

The floor is driven by **PEP 695 generic syntax** used across the codebase (e.g. `async def get[T: BaseModel](...)` in `drakkar/cache_memory.py`). That syntax parses only on 3.12+, and Drakkar stabilized it on 3.13 where the type parameter machinery is mature and ty / mypy handle it cleanly. Python 3.14 is also supported (the package classifiers list both). Dropping the 3.13 floor would mean rewriting those signatures to the `TypeVar` / `Generic[T]` form — doable, but not planned pre-1.0.

---

## Hardware and scaling

### How much CPU does a worker need? How many cores should I allocate?

Depends on task shape (fast/slow), executor pool size, and input throughput. Use the [Config Calculator](calculator.md) to get a starting point, then tune via [Performance Recommendations](performance.md#configuration-recommendations). Rule of thumb: one Python process can saturate ~1 core for the event loop + expect (max_executors × task CPU share) for subprocess work.

### How much memory per worker?

There's no single number — memory is dominated by three tunables plus your subprocess. A useful back-of-envelope:

- **Buffered Kafka messages**: `max_poll_records` × average message size × (number of partitions × `window_size`) in worst case. Defaults (`max_poll_records=100`, `window_size` small) keep this in the tens of MB.
- **In-memory cache**: capped by `cache.max_memory_entries` (default `10_000`). Each entry is a Python dict + serialized Pydantic JSON; budget roughly 1-10 KB/entry for typical payloads. Set `max_memory_entries: null` for unbounded (the config layer emits a startup warning if you do — monitor RSS).
- **Flight recorder buffer**: small, bounded by `recorder.flush_interval_seconds` × event rate. Events are buffered in memory and flushed to SQLite; a full buffer is tens of MB.
- **Subprocess children**: `max_executors` live processes, each using whatever your binary consumes. This usually dwarfs the Python RSS.

For a typical worker (max_executors=80, cache enabled with defaults, recorder on), the **Python process** sits in the 150-400 MB range. The subprocess pool adds whatever your binary needs, times `max_executors`. Measure under load; there's no substitute. Raw capacity knobs are documented in [Configuration](configuration.md) and the [Cache memory cap](cache.md) page.

### Can I run multiple workers on one host?

Yes. Each worker needs a unique `worker_id` (set via the env var named in `worker_name_env`, default `WORKER_ID`). Recorder and cache DBs are per-worker, so their paths must differ — typically achieved by including the worker_id in the directory path.

### How do I scale horizontally?

Run more workers in the same Kafka consumer group. The group rebalances partitions across all members; parallelism is capped at the partition count. See [Scaling Horizontally](performance.md#scaling-horizontally) and [Architecture](index.md#architecture).

### What's the maximum throughput per worker?

Roughly **4,000-8,000 tasks/sec/worker** before a single event loop becomes the bottleneck — see [Bottleneck: Event Loop](performance.md#bottleneck-event-loop) for the breakdown of where the GIL time goes. In practice you hit this ceiling with sub-10ms tasks and `max_executors > 100`; for longer tasks (≥ 30ms) you'll saturate on CPU / downstream sinks well before Python orchestration becomes the limit.

Knobs that move the ceiling up (all documented in [performance.md](performance.md)):

- **Precomputed tasks** (cache hits skip the subprocess entirely) remove the per-task launch cost — the ceiling becomes whatever your sinks can absorb.
- **Batching messages per task** (one subprocess launch for N messages via stdin) amortizes the 1-5ms launch cost.
- **Larger `window_size`** enables larger batches in `arrange()`.
- **Off-thread JSON encoding** and **`orjson`** are available to cut recorder overhead (see [Available optimization: `orjson`](performance.md#available-optimization-orjson-opt-in)).

Use the [Config Calculator](calculator.md) for a starting point, then measure in staging. Target metrics live in [Monitoring Throughput](performance.md#monitoring-throughput).

### What benefit does separating pipelines by partition give?

Kafka partitions become independent processing lanes: each partition has its own `PartitionProcessor` with its own window, message tracker, and offset tracker ([Data Flow — Phase 2](data-flow.md#phase-2-partition-assignment-and-revocation)). Benefits:
- Ordering preserved within a partition (key-based grouping stays intact).
- A slow message on partition 3 doesn't block partitions 0/1/2/4.
- Offsets commit per-partition, so retries don't disturb neighbours.

---

## Handler basics

### How do I write a handler?

Subclass `BaseDrakkarHandler[InputModel, OutputModel]` and implement at least `arrange()`. See [Quick Start — Define a handler](index.md#define-a-handler) and the full [Handler System](handler.md) reference.

### What hooks exist and when are they called?

`on_startup` → `on_ready` → per-message: `arrange` → (per task) `on_task_complete` → `on_message_complete` → (per window) `on_window_complete`. Error paths: `on_error`, `on_delivery_error`. Partition lifecycle: `on_assign`, `on_revoke`. See [Hook Reference](handler.md#hook-reference).

### What's a "precomputed" task?

A task whose result the handler already knows (cache hit, deterministic shortcut) — the executor skips the subprocess and emits synthetic `task_started`/`task_completed` events. Useful for memoization. See [Precomputed task results](handler.md#precomputed-task-results-skip-the-subprocess).

### When does a message's offset get committed to Kafka?

After **all** tasks generated from that message terminate (success or failure after retries exhausted) AND `on_message_complete` has run. Failures go through `on_error` → DLQ, not a "skipped commit". See [Offset Commit Semantics](fan-out.md#offset-commit-semantics).

### Can I build per-message aggregates from multiple tasks?

Yes — that's what `on_message_complete` is for. You get a `MessageGroup` with all the task outcomes. See [Fan-out: One Message → Many Tasks → One Aggregate](fan-out.md).

### Can one task cover multiple messages (fan-in)?

Yes. An `ExecutorTask` can list multiple `source_offsets`; it only completes the window when all its source messages' trackers have settled. See [Multi-message tasks (fan-IN)](fan-out.md#multi-message-tasks-fan-in).

---

## Cache and state

### What does the built-in cache give me?

Framework-provided `self.cache` on every handler: in-memory LRU + write-behind per-worker SQLite + optional cross-worker peer sync with last-write-wins. Survives restarts (per-worker persistence), optionally shares across the fleet. See [Cache — What it solves](cache.md#what-it-solves).

### How do workers discover each other's caches?

Via a **shared filesystem** — no peer list in config, no Kafka topic, no discovery protocol. Each worker writes its cache DB under a shared directory (`cache.db_dir`, falling back to `debug.db_dir`) and publishes a stable live symlink named `<worker>-cache.db` pointing at the current file. Peers `glob` the directory for `*-cache.db` symlinks, skip their own entry, resolve through `os.path.realpath` (so a peer rotating its DB doesn't break mid-read), and read rows from the peer's `cache_entries` table. See `drakkar/peer_discovery.py::discover_peer_dbs` for the implementation.

What happens if the filesystem isn't shared: peer sync finds zero peers and the cache operates single-worker. No error, no warning — it just behaves like a local cache. If you *want* cluster-wide cache, mount a shared volume (NFS, EFS, hostPath in k8s) and point every worker's `db_dir` at it.

Fallbacks when a peer DB is unreachable: the sync loop wraps each peer in `try/except`, logs a warning, increments `drakkar_cache_sync_errors_total{peer=...}`, and continues to the next peer. Broken symlinks (peer's DB file gone) are silently skipped — the peer may come back later with a fresh file. One bad peer cannot break the whole sync cycle. See [Cache — Peer unreachable](cache.md#peer-unreachable).

Trust boundary: the `db_dir` is shared-trust — anyone who can write to it can inject cache entries that peers will read. There is no per-write signature or auth. Restrict directory permissions to the worker user (see [Why are peer workers trusted?](#why-are-peer-workers-trusted)).

### What if two workers write the same cache key?

Last-write-wins based on `updated_at_ms`, with `origin_worker_id` as a lexicographic tiebreak when timestamps collide. Details in [Consistency model](cache.md#consistency-model).

### Is cache delete propagated across workers?

**No — delete is LOCAL only** in the current implementation. Other workers' copies stay until they expire or are overwritten. See [Delete is local-only (the main sharp edge)](cache.md#delete-is-local-only-the-main-sharp-edge).

### What's the difference between `CacheScope.LOCAL` and `CLUSTER`?

`LOCAL` lives only in the worker that wrote it. `CLUSTER` is peer-synced and readable by other workers in the same cluster. See [Choosing a scope](cache.md#choosing-a-scope).

### How does cache interaction with SQLite actually work?

Reads are in-memory LRU first; miss → fallback to `_reader_db` (separate aiosqlite connection so reads never queue behind a write). Writes go into `_dirty` dict and are flushed periodically to `_writer_db` as a batch. WAL mode so readers don't block writers. See [How it flows](cache.md#how-it-flows).

---

## Sinks and delivery

### What sinks ship with Drakkar?

Kafka, Postgres, Mongo, HTTP, Redis, Filesystem, and a dedicated DLQ (Kafka topic). See [Sinks](sinks.md) and the per-type config in [Configuration — Sinks](configuration.md#sinks-sinks).

### What delivery guarantee does Drakkar provide?

**At-least-once.** A failed sink write triggers DLQ + `on_delivery_error`; a failed task triggers `on_error` + offset commit only after a final outcome. Duplicates after crash/retry are possible — design your sinks to be idempotent or tolerate duplicates. See [Delivery and error handling](sinks.md#delivery-and-error-handling).

### Does Drakkar support exactly-once?

No. Kafka EOS transactions aren't wired in; workloads that require exactly-once should key their output writes idempotently (Kafka sinks use producer-side dedup via message keys; Postgres/Mongo sinks can use `ON CONFLICT` / upsert semantics; HTTP sinks should send an idempotency key). There is no roadmap item for first-class transactional EOS — the design cost (coordinating the Kafka consumer group, the producer transaction, and arbitrary external sinks) conflicts with Drakkar's "many sinks, one pipeline" philosophy. If you need strict EOS between Kafka and Kafka only, use Kafka Streams or a transactional producer directly.

### What happens when a sink is unreachable?

The sink's internal retry policy kicks in; exhausted retries call `on_delivery_error` on your handler and route the record to the DLQ topic. See [Dead letter queue](sinks.md#dead-letter-queue).

### How do I add multiple sinks of the same type?

Name each in config (`sinks.kafka.results`, `sinks.kafka.audit`); your handler routes records by setting `sink='results'` on each payload. See [Multiple named sinks](sinks.md#multiple-named-sinks).

### How do I add a custom sink type?

There's no plugin registry yet — sink types are built into `DrakkarApp._build_sinks()` (see `drakkar/app.py`). Today the path to a custom sink is:

1. Subclass `BaseSink[YourPayloadT]` from `drakkar.sinks.base`; set the `sink_type` class attribute and implement `connect()`, `deliver(payloads)`, and `close()`.
2. Handle your own retries internally — the `SinkManager` records failures and trips a circuit breaker, but delegates retry policy to the sink. Design your `deliver()` to be safe to re-enter (idempotent writes) so that framework-level duplicates (at-least-once delivery) don't cause trouble downstream.
3. Register your sink instance with the framework's `SinkManager` in your bootstrap code (currently requires a small fork of `DrakkarApp._build_sinks` or a monkey-patch).

A stable plugin API (entry-points-based `SinkRegistry`) is on the Phase 4 roadmap — it will let you add sink types without forking the app bootstrap. Until then, an in-tree contribution is often the easier path if the sink type is generally useful.

---

## Deployment and operations

### How do I configure a worker?

YAML file + env overrides (`DRAKKAR_` prefix, `__` nesting). See [Configuration Loading](configuration.md#configuration-loading).

### How do I stagger a rolling deploy so my fleet doesn't cascade-rebalance?

Leave `kafka.startup_align_enabled: true` (default). Each worker delays its first `subscribe()` until the next shared wall-clock boundary, so 10 workers booting over 15 seconds all join the consumer group at the same moment. See [Staggered startup alignment](configuration.md#staggered-startup-alignment).

### How many workers should I run per consumer group?

Up to the number of Kafka partitions. Extra workers sit idle (Kafka only assigns one consumer per partition). See [Scaling Horizontally](performance.md#scaling-horizontally).

### Can I hot-reload config without restarting?

No. Config is loaded once at startup and held in the `DrakkarApp` instance for its lifetime — there is no SIGHUP handler and no config-watch loop. The signal handlers today (`drakkar/app.py`) are `SIGINT` and `SIGTERM`, both of which trigger graceful shutdown. To change config, rely on rolling deploys: Kafka's cooperative-sticky rebalance keeps non-revoked partitions running during the rollout, and `kafka.startup_align_enabled` (default on) prevents a fleet-wide cascade rejoin. See [Staggered startup alignment](configuration.md#staggered-startup-alignment).

### Where do logs go?

Stdout by default; configurable to file/syslog/JSON via structlog. See [Structured Logging](observability.md#structured-logging).

### How do I wire up Prometheus scraping?

Drakkar exposes metrics on `/metrics` at the configured port. See [Scrape Configuration](observability.md#scrape-configuration) and the full metric list in [Metrics Reference](observability.md#metrics-reference).

### How do I deploy in Kubernetes?

Drakkar does not ship blessed Helm charts or Kustomize overlays yet — a reference chart is on the roadmap. What the framework *does* provide to make k8s easy:

- **Graceful shutdown** on `SIGTERM` (drains in-flight windows, commits settled offsets, disconnects sinks).
- **Structured logs** on stdout (JSON via structlog) — ready for any k8s log collector.
- **Prometheus metrics** on a configurable port for `prometheus-operator` `ServiceMonitor`.
- **Stateful considerations**: each worker needs a unique `worker_id` (set via the env var named in `worker_name_env`, default `WORKER_ID`) and a per-worker DB path (recorder + cache SQLite files). Use a `StatefulSet` with a `volumeClaimTemplate` mounted at `debug.db_dir` / `cache.db_dir`, or use a shared `ReadWriteMany` volume (NFS, EFS) if you want cluster-wide peer sync.

Typical shape: one `StatefulSet` (or `Deployment` with a PVC per replica) × N replicas = N Kafka consumer-group members. Replica count ≤ partition count. Env-var overrides follow the `DRAKKAR_` prefix + `__` nesting convention (see [Configuration Loading](configuration.md#configuration-loading)).

---

## Debug UI and observability

### How does the debug web UI affect the running pipeline?

The UI runs on a separate thread with its own asyncio event loop, so most read-only endpoints don't interfere. Real effects to be aware of:

- **Message Probe** monkey-patches `handler.cache` process-wide while a probe runs — production hooks during that window see a `DebugCacheProxy` and their `cache.set()` calls are silently suppressed.
- **Probe consumes `ExecutorPool` slots** exactly like production messages.
- **GIL contention** between the UI thread and the main loop under heavy UI use.
- **aiosqlite connections** are per-thread workers; UI reads don't block pipeline writes thanks to SQLite WAL mode.

See [Observability — Debug UI](observability.md#debug-ui) for the endpoint inventory.

### Is the debug UI safe to expose to a team of operators?

Auth is **opt-in by default**. The UI is read-only by design (no endpoint stops a worker, replays Kafka messages, mutates sinks, or fakes pipeline data) and Drakkar is intended to run inside a private contour, so the framework starts unauthenticated when `debug.auth_token` is empty (the default) and emits a structured `debug_ui_unauthenticated` warning at startup naming the host:port and the two opt-in paths (`debug.auth_token` in YAML or `DRAKKAR_DEBUG__AUTH_TOKEN` env var).

For multi-operator setups, set a strong `debug.auth_token`:

1. **Bearer token on protected endpoints.** Once the token is set, `/api/debug/databases`, `/api/debug/merge`, `/debug/download/{filename}`, and `/api/debug/probe` all require an `Authorization: Bearer <token>` header (or `?token=<token>` query parameter). Comparison uses `secrets.compare_digest` to avoid timing side-channels.
2. **WebSocket auth + origin check.** With a token configured, the live-event WebSocket at `/ws` also requires the same `auth_token`; invalid tokens close with code 4401. The handshake validates the `Origin` header against `allowed_ws_origins` (explicit allowlist) or the `Host` header (same-origin fallback).
3. **Read-only pages remain accessible** — both before and after enabling auth. `/`, `/live`, `/partitions`, `/sinks`, `/history`, and the per-task pages serve flight-recorder content; gating is on the mutating / data-exposing endpoints listed above.

Even with auth, the read-only pages expose task stdout/stderr, task env (after [redaction](observability.md#flight-recorder)), cache contents, and live event streams. Restrict access to operators. Concurrent Message Probes serialize on an internal lock, and the probe temporarily replaces `handler.cache` — keep this in mind if you have many operators debugging the same worker simultaneously.

### Why does my worker emit a `debug_ui_unauthenticated` warning at startup?

You have `debug.enabled=true` and `debug.auth_token` is empty (the default). That's an intentional, supported configuration — Drakkar treats the debug UI as opt-in-auth because it is read-only and meant for private-network deployments. The warning is informational; the worker continues starting normally.

To silence it (i.e. require auth), pick one of:

- **Set `debug.auth_token`** to a strong random value (`python -c "import secrets; print(secrets.token_urlsafe(32))"`) — recommended whenever the UI is reachable from anywhere outside a fully-trusted operator network.
- **Set `debug.enabled=false`** — if the worker doesn't need the flight recorder at all (no observability cost reduction without removing the UI).

See [Authentication](configuration.md#authentication) for the field semantics and the implementation at `drakkar/app_security.py::warn_if_debug_unauthenticated`.

### What is the Message Probe tab?

Paste a raw Kafka message value; the framework runs it end-to-end through your handler (`arrange` → executor → `on_task_complete` → `on_message_complete` → `on_window_complete`) with zero intentional footprint: no sink writes, no offset commits, no recorder rows, no cache writes. Shows every task's stdin/stdout/stderr/exit code/duration plus the sink payloads that *would* have been produced. The full contract (headers, cache proxy behavior, concurrent-probe serialization) is documented on the [Debug UI page](observability.md#debug-ui).

### How do I trace a specific message through the pipeline?

Use `/debug` → **Message Trace** tab, search by `partition:offset` or by label value. The flight recorder stores every lifecycle event per message. See [Observability — Debug UI](observability.md#debug-ui).

### Do UI readers slow down the pipeline?

Mildly. Heavy read traffic increases SQLite WAL checkpoint frequency and burns a little Python GIL time. Under normal operator use (a few tabs refreshing) the effect is negligible. See [Bottleneck: Recorder and Debug UI](performance.md#bottleneck-recorder-and-debug-ui).

---

## Failure modes

### What happens if my handler crashes mid-message?

The exception is caught, logged, the task is marked failed, `on_error` fires on the handler. If `on_error` returns `RETRY` and `max_retries` isn't exhausted, the task runs again. If all retries fail, the task is terminal and the message's offset commits only after all its tasks settle. See [Error Handling — on_error Hook](executor.md#error-handling-on_error-hook).

### What if a subprocess hangs?

`executor.task_timeout_seconds` kills the subprocess when `asyncio.wait_for` raises `TimeoutError`; the executor's `finally` block calls `proc.kill()` + `proc.wait()` and the result is recorded with `exit_code=-1` and `stderr='task timed out'`. Then the normal `on_error` path handles it. See [Execution Flow — Enforce Timeout](executor.md#5-enforce-timeout).

### What if the Kafka broker goes down?

librdkafka (the underlying client) handles reconnects transparently — the consumer automatically rejoins when brokers recover. Drakkar does not expose a distinct "broker-down" state in the debug UI or Prometheus: there is no `drakkar_kafka_connected` gauge today. What you *will* see: `drakkar_messages_consumed_total` rate drops to zero while processing continues on already-polled messages. Pair a "no new messages" alert (`rate(drakkar_messages_consumed_total[5m]) == 0` while expecting traffic) with broker-level monitoring (Kafka's own JMX / kafka_exporter metrics) to catch broker issues. There is no hard fail-fast option — the design assumption is that brokers recover and transient disconnects should not restart the worker.

### What if a sink is unreachable for a long time?

Per-batch retries kick in first; exhausted retries fire `on_delivery_error` and route the record to the DLQ. After `failure_threshold` consecutive terminal failures (default `5`), the sink's [circuit breaker](sinks.md#circuit-breaker) trips and subsequent batches route **directly** to the DLQ without even attempting the sink — so you stop paying the retry-timeout cost on every batch while the downstream is down. After `cooldown_seconds` (default `30s`) the breaker promotes to half-open and sends a single probe; success closes it, failure reopens with a fresh cooldown. Watch `drakkar_sink_circuit_open` and `drakkar_sink_circuit_trips_total` to alert on the condition. See also [Dead letter queue](sinks.md#dead-letter-queue).

### How do I replay messages from the DLQ?

Drakkar does not run a built-in replay worker — replays are operator-driven. Use [`scripts/replay_dlq.py`](sinks.md#dlq-replay) as the reference tool: it reads DLQ entries and republishes their preserved `original_payloads` to a target Kafka topic (defaults to your source topic), with `--dry-run`, `--filter`, `--limit` for inspection and a consumer group that is unique per invocation so each run drains from the beginning. The script preserves the serialized payload bytes; it does NOT currently preserve original partition / key / headers — if downstream routing depends on those, point `--target-topic` at a retry topic whose producer re-derives them. See the [DLQ payload schema](sinks.md#dead-letter-queue) for the fields the script reads.

### A message is stuck in "in-flight" forever — what do I do?

Check the **Executors** tab on `/live` for stuck tasks; their `task_timeout_seconds` should eventually fire. When a task overruns the timeout, `asyncio.wait_for` raises `TimeoutError`, the executor's `finally` block calls `proc.kill()` + `proc.wait()`, and the task takes the normal `on_error` path. If the subprocess *itself* doesn't die after `proc.kill()`, the task coroutine stays pending — the debug server exposes `/api/debug/processors`, which includes a `stuck_tasks` list with a live coroutine stack (top 5 frames) per in-flight task; that's the rescue-visibility tool.

Rescue procedure when a task is truly wedged past the timeout:

1. Open `/live` → **Executors** tab and look for tasks whose `oldest_running_sec` exceeds `task_timeout_seconds`.
2. Hit `/api/debug/processors` to get the coroutine stack for each wedged task via the `stuck_tasks` field.
3. Check worker logs for the task's `task_id` — you'll usually find the downstream call (sink, cache, peer sync) that's blocked.
4. If the stack shows the subprocess already exited and the coroutine is wedged in sink or recorder code, a worker restart is the safest recovery — Kafka's cooperative-sticky rebalance will reassign the partition to another worker, which will re-poll the un-committed offset range and retry the work.
5. There is no in-UI "force complete" button today by design: marking a wedged task as succeeded could silently ack a half-applied side effect. Operators are expected to restart the worker instead.

---

## Architecture deep-dive

### Why is the executor subprocess-based instead of async-native or threaded?

Four reasons, all aligned with the "CPU in external binaries, Python orchestrates I/O" design goal:

1. **Language portability** — the worker binary can be Rust/Go/C++/a CLI tool you don't own; it just reads a JSON payload from stdin and writes results to stdout.
2. **Real parallelism** — N subprocesses truly use N cores. Python threads would still contend on the GIL for any CPU burst; `asyncio.to_thread` is only useful for blocking I/O.
3. **OS-level CPU isolation** — one task's bug (segfault, OOM, infinite loop) cannot corrupt the worker process.
4. **Clean timeouts and cancellation** — the executor just SIGKILLs a runaway subprocess. Cancelling in-process Python threads is notoriously unreliable.

The cost is ~10ms of launch overhead per task ([Bottleneck: Subprocess Launch](performance.md#bottleneck-subprocess-launch)) — a fair trade when your task does 10ms–10s of real CPU work. For sub-millisecond tasks, use [precomputed results](handler.md#precomputed-task-results-skip-the-subprocess) to skip the subprocess entirely.

### Why SQLite for the recorder and cache?

Zero-operational-cost embedded store with WAL-mode concurrency, good enough for the write volumes these components generate, and easy to inspect/backup/ship. The framework separates reader and writer connections so read endpoints never queue behind the writer's flush. See [Cache — How it flows](cache.md#how-it-flows).

### How many event loops does a worker run?

Two: the **main loop** (Kafka consumer + partition processors + executor pool + periodic tasks + sink manager) and the **debug UI loop** (uvicorn on a separate thread). asyncio primitives created on one are not safe to use on the other — in particular, the recorder's `aiosqlite` connection and the cache reader connection are bound to the main loop. The debug server uses a `_dispatch_to_main_loop` helper to marshal roughly 20 read-side endpoints (message trace, probe, task listings, cache inspection, etc.) back onto the main loop via `asyncio.run_coroutine_threadsafe`; pure-Python work (template rendering, counters, constants) stays on the UI loop.

### What happens during a Kafka rebalance?

Revoked partitions' `PartitionProcessor`s drain their in-flight windows then stop; newly-assigned partitions spawn fresh processors. `on_revoke` / `on_assign` hooks fire so handler code can flush partition-scoped state. See [Partition Assignment and Revocation](data-flow.md#phase-2-partition-assignment-and-revocation).

### How does backpressure work?

The poll loop compares in-flight message count to high/low watermarks (multiples of `max_executors`). Above high-watermark: pause the Kafka consumer so no new batch is polled. Below low-watermark: resume. See [Backpressure Formula](configuration.md#backpressure-formula) and [Backpressure deep-dive](performance.md#backpressure).

### What's the difference between the recorder DB and the cache DB?

They're separate SQLite files with separate schemas and separate reader/writer connections. The recorder stores per-message lifecycle events (flight recorder for the debug UI); the cache stores operator-written key/value pairs for memoization. See [Observability — Flight Recorder](configuration.md#debug-flight-recorder-debug) and [Cache](cache.md).

---

## Security and trust model

This section expands the five trust assumptions listed in the [project README](https://github.com/wlame/drakkar#security--trust-model) -- each one is an architectural trust boundary, not a latent bug. Read this before a production deploy.

### Why is the handler binary trusted?

Drakkar launches `executor.binary_path` as a subprocess and pipes the message bytes to its stdin without validation. There's no sandbox, no signature check, and no attempt to filter input — the binary is assumed to be operator-provided code you audit the same way you audit the rest of your deployment.

See `drakkar/executor.py::ExecutorPool._launch` for the launch code. The binary runs with the worker's OS privileges, plus any env overrides from `ExecutorConfig.env` or per-task `env`. If you need defense-in-depth against a compromised binary, run the worker under a restricted user / with seccomp / inside a container — Drakkar itself offers no in-process sandbox.

### Why are peer workers trusted?

The cache and recorder peer-sync mechanisms read other workers' SQLite files directly (see `drakkar/cache_engine.py::CacheEngine._sync_inner` and `drakkar/recorder.py::EventRecorder.cross_trace`). There's no per-write signature, no auth check, no schema-level integrity verification. Anyone who can write to the shared `db_dir` can inject cache entries or event rows that your workers will read.

Treat `db_dir` as a shared-trust boundary: any principal with write access to that directory has the same trust level as the workers themselves. On a shared filesystem (NFS, EFS), restrict directory permissions to the worker user.

### How is the debug UI protected?

Auth is opt-in (see [Is the debug UI safe](#is-the-debug-ui-safe-to-expose-to-a-team-of-operators) above):

1. **Default loopback bind** (`debug.host='127.0.0.1'`) — the UI is only reachable from the host out of the box, regardless of auth.
2. **Startup warning when unauthenticated.** With `debug.enabled=true` and an empty `auth_token`, the worker emits a `debug_ui_unauthenticated` structured warning at startup naming the unauthenticated bind and the two opt-in paths (YAML key + env var). The worker continues starting — Drakkar treats this as a private-contour-friendly default, not a misconfiguration.
3. **Bearer token + Origin check when `auth_token` is set.** Protected endpoints (database download, merge, probe) require `Authorization: Bearer <token>`; the WebSocket stream additionally validates the `Origin` header against `allowed_ws_origins` (or the `Host` header). See `drakkar/debug_server_helpers.py::origin_allowed` and `drakkar/debug_server.py::_token_matches`.

Read-only HTTP pages are not token-gated regardless — auth applies to mutating / data-exposing endpoints and to the WebSocket event stream. If you put the UI on a non-loopback host outside a private network, set a strong `auth_token` and consider a reverse proxy with TLS.

### Why doesn't Drakkar validate Kafka message payloads?

Parse errors in `handler.deserialize_message` silently set `msg.payload=None` rather than raising or DLQ-ing the message (see `drakkar/app.py::_deserialize`). A malicious producer can cause handlers to see unexpected `None` payloads, but cannot execute code in the worker. Your handler is responsible for validating the payload before using it — use Pydantic `model_validate` or raise explicitly from `arrange()` to route bad messages.

### What redactions apply to per-task env?

Two surfaces expose env vars:

- **The recorder's `worker_config` table** — framework-level `ExecutorConfig.env` is **never written** to the recorder (it's omitted from the JSON payload entirely). Environment variables listed in `expose_env_vars` are captured by name, and secret-shaped names (`*PASSWORD*`, `*SECRET*`, `*TOKEN*`, `*_KEY`, `*API_KEY*`, `*CREDENTIAL*`, `*_DSN`) are redacted to `***`. Non-matching values with embedded URL credentials (`user:pass@host`) have the credentials stripped.
- **The recorder's per-task `env` metadata** — `task.env` written by your handler is sanitized with the same secret-name patterns before being stored. The original task object is not mutated; only the recorded copy is redacted. See `drakkar/recorder_helpers.py::sanitize_env_value` for the regex.

The contract is "aggressive redact, accept false positives": `PASSWORD_RESET_URL` is redacted because it matches `*PASSWORD*`, even though a reset URL isn't a credential. Operators who need to expose these exact names should rename them — a leaked secret is a worse outcome than a logged URL.

---

## Contribute to this page

Missing a question you've asked or answered? Send a PR adding it here — even questions without a confident answer are useful: they mark the project's documentation debt visibly, and get resolved faster when they're written down.
