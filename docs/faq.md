# Frequently Asked Questions

A living Q&A page drawing from real operator questions and common first-time-reader confusions. Answers are deliberately short — click through for depth. Items marked **TBD** are open questions we haven't pinned down yet; contributions welcome.

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
- **Very short tasks** where the per-task subprocess launch overhead (~10ms) dominates the actual work. If your task is ≤ 1ms of real work, use [precomputed tasks](handler.md#precomputed-task-results) or in-process logic. See [Bottleneck: Subprocess Launch](performance.md#bottleneck-subprocess-launch).
- **Ultra-low-latency pipelines** (single-digit ms end-to-end) — windowing and batching cost is real.
- **Exactly-once transactional streams** — Drakkar is at-least-once with DLQ safety; see the delivery section below.

### How does Drakkar compare to Celery, Faust, Kafka Streams, Benthos?

**TBD** — we'll write a proper comparison matrix. The short pitch: Drakkar is narrower (Kafka-only input; subprocess executors first-class) and more opinionated about fan-in/fan-out/partition ordering.

### What languages can my handler's executor binary be written in?

Any language. The executor launches a subprocess and communicates via stdin/stdout/exit-code — the binary only needs to read a JSON payload from stdin and write results to stdout. See [Executor System](executor.md).

### Why Python 3.13+?

**TBD** — confirm whether the floor is motivated by specific asyncio improvements, typing features, or just baseline modernization.

---

## Hardware and scaling

### How much CPU does a worker need? How many cores should I allocate?

Depends on task shape (fast/slow), executor pool size, and input throughput. Use the [Config Calculator](calculator.md) to get a starting point, then tune via [Performance Recommendations](performance.md#configuration-recommendations). Rule of thumb: one Python process can saturate ~1 core for the event loop + expect (max_executors × task CPU share) for subprocess work.

### How much memory per worker?

**TBD** — depends heavily on max_poll_records, cache memory cap, and subprocess memory. Real numbers from production deployments are the right way to answer this.

### Can I run multiple workers on one host?

Yes. Each worker needs a unique `worker_id` (set via the env var named in `worker_name_env`, default `WORKER_ID`). Recorder and cache DBs are per-worker, so their paths must differ — typically achieved by including the worker_id in the directory path.

### How do I scale horizontally?

Run more workers in the same Kafka consumer group. The group rebalances partitions across all members; parallelism is capped at the partition count. See [Scaling Horizontally](performance.md#scaling-horizontally) and [Architecture](index.md#architecture).

### What's the maximum throughput per worker?

**TBD** — depends on task profile. Use the [Config Calculator](calculator.md) for order-of-magnitude; measure real throughput in staging. Target metrics are covered in [Monitoring Throughput](performance.md#monitoring-throughput).

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

A task whose result the handler already knows (cache hit, deterministic shortcut) — the executor skips the subprocess and emits synthetic `task_started`/`task_completed` events. Useful for memoization. See [Precomputed task results](handler.md#precomputed-task-results).

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

**TBD** — fill in: what discovery mechanism is used (shared filesystem? peer list in config? Kafka topic?), what happens if the filesystem isn't shared, fallbacks when a peer DB is unreachable. See partial notes in [Cache — Peer unreachable](cache.md#peer-unreachable). This needs a proper write-up.

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

Kafka, Postgres, Mongo, HTTP, Redis, Filesystem, and a dedicated DLQ (Kafka topic). See [Sinks](sinks.md) and the per-type config in [Configuration — Sinks](configuration.md#sinks).

### What delivery guarantee does Drakkar provide?

**At-least-once.** A failed sink write triggers DLQ + `on_delivery_error`; a failed task triggers `on_error` + offset commit only after a final outcome. Duplicates after crash/retry are possible — design your sinks to be idempotent or tolerate duplicates. See [Delivery and error handling](sinks.md#delivery-and-error-handling).

### Does Drakkar support exactly-once?

No. Kafka EOS transactions aren't wired in; workloads that require exactly-once should key their output writes idempotently. **TBD** — whether to offer an opt-in EOS mode is an open roadmap question.

### What happens when a sink is unreachable?

The sink's internal retry policy kicks in; exhausted retries call `on_delivery_error` on your handler and route the record to the DLQ topic. See [Dead letter queue](sinks.md#dead-letter-queue).

### How do I add multiple sinks of the same type?

Name each in config (`sinks.kafka.results`, `sinks.kafka.audit`); your handler routes records by setting `sink='results'` on each payload. See [Multiple named sinks](sinks.md#multiple-named-sinks).

### How do I add a custom sink type?

**TBD** — there's no stable plugin API yet. Today you'd subclass `BaseSink`. Worth documenting as the project hardens.

---

## Deployment and operations

### How do I configure a worker?

YAML file + env overrides (`DRAKKAR_` prefix, `__` nesting). See [Configuration Loading](configuration.md#configuration-loading).

### How do I stagger a rolling deploy so my fleet doesn't cascade-rebalance?

Leave `kafka.startup_align_enabled: true` (default). Each worker delays its first `subscribe()` until the next shared wall-clock boundary, so 10 workers booting over 15 seconds all join the consumer group at the same moment. See [Staggered startup alignment](configuration.md#staggered-startup-alignment).

### How many workers should I run per consumer group?

Up to the number of Kafka partitions. Extra workers sit idle (Kafka only assigns one consumer per partition). See [Scaling Horizontally](performance.md#scaling-horizontally).

### Can I hot-reload config without restarting?

**TBD** — probably no today. Document explicitly or add a SIGHUP handler if desired.

### Where do logs go?

Stdout by default; configurable to file/syslog/JSON via structlog. See [Structured Logging](observability.md#structured-logging).

### How do I wire up Prometheus scraping?

Drakkar exposes metrics on `/metrics` at the configured port. See [Scrape Configuration](observability.md#scrape-configuration) and the full metric list in [Metrics Reference](observability.md#metrics-reference).

### How do I deploy in Kubernetes?

**TBD** — no first-class k8s manifests yet. A blessed example chart would be useful.

---

## Debug UI and observability

### How does the debug web UI affect the running pipeline?

The UI runs on a separate thread with its own asyncio event loop, so most read-only endpoints don't interfere. Real effects to be aware of:

- **Message Probe** monkey-patches `handler.cache` process-wide while a probe runs — production hooks during that window see a `DebugCacheProxy` and their `cache.set()` calls are silently suppressed.
- **Probe consumes `ExecutorPool` slots** exactly like production messages.
- **GIL contention** between the UI thread and the main loop under heavy UI use.
- **aiosqlite connections** are per-thread workers; UI reads don't block pipeline writes thanks to SQLite WAL mode.

See [Observability — Debug UI](observability.md#debug-ui) for the endpoint inventory. **TBD** — we'll turn this answer into a proper subsection of that page.

### Is the debug UI safe to expose to a team of operators?

Drakkar layers three defenses:

1. **Default loopback binding.** `debug.host='127.0.0.1'` out of the box — the UI is only reachable from the host, not the network. Most development and local-operator flows work against the default.
2. **Fail-fast on insecure non-loopback binds.** If you set `debug.host='0.0.0.0'` (or any non-loopback address) without a `debug.auth_token`, the worker raises `InsecureDebugConfigError` at startup rather than silently exposing the UI. Either set a strong token or keep the loopback default. See [Insecure-startup failure](configuration.md#insecure-startup-failure).
3. **Auth + origin check on WebSocket.** The live-event WebSocket at `/ws` requires the same `auth_token` (bearer header or `?token=` query param); invalid tokens close with code 4401. When a token is configured, the handshake also validates the `Origin` header against `allowed_ws_origins` (explicit allowlist) or the `Host` header (same-origin fallback). Comparison uses `secrets.compare_digest`.

Even with all three layers, the read-only pages expose task stdout/stderr, task env (after [redaction](observability.md#flight-recorder)), cache contents, and live event streams. Restrict access to operators. Concurrent Message Probes serialize on an internal lock, and the probe temporarily replaces `handler.cache` — keep this in mind if you have many operators debugging the same worker simultaneously.

### Why does my worker fail with `InsecureDebugConfigError` on startup?

You set `debug.enabled=true` + a non-loopback `debug.host` + no `debug.auth_token`. That combination publishes the debug UI (task stdout, env, cache contents, WebSocket event stream) without authentication — a security bug we refuse to let slip into production.

Pick **one** of these remediations:

- **Set `debug.auth_token`** to a strong random value (e.g. `python -c "import secrets; print(secrets.token_urlsafe(32))"`) — recommended for production where the UI needs to be reachable from ops tooling.
- **Set `debug.host=127.0.0.1`** — UI only reachable from the host, ideal when operators SSH-tunnel or run a sidecar reverse proxy.
- **Set `debug.enabled=false`** — if the worker doesn't need the flight recorder at all.

See [Insecure-startup failure](configuration.md#insecure-startup-failure) for the full check and the implementation at `drakkar/app.py::_validate_debug_security`.

### What is the Message Probe tab?

Paste a raw Kafka message value; the framework runs it end-to-end through your handler (`arrange` → executor → `on_task_complete` → `on_message_complete` → `on_window_complete`) with zero intentional footprint: no sink writes, no offset commits, no recorder rows, no cache writes. Shows every task's stdin/stdout/stderr/exit code/duration plus the sink payloads that *would* have been produced. **TBD** — promote this explanation into `observability.md` under the `/debug` section.

### How do I trace a specific message through the pipeline?

Use `/debug` → **Message Trace** tab, search by `partition:offset` or by label value. The flight recorder stores every lifecycle event per message. See [Observability — Debug UI](observability.md#debug-ui).

### Do UI readers slow down the pipeline?

Mildly. Heavy read traffic increases SQLite WAL checkpoint frequency and burns a little Python GIL time. Under normal operator use (a few tabs refreshing) the effect is negligible. See [Bottleneck: Recorder and Debug UI](performance.md#bottleneck-recorder-and-debug-ui).

---

## Failure modes

### What happens if my handler crashes mid-message?

The exception is caught, logged, the task is marked failed, `on_error` fires on the handler. If `on_error` returns `RETRY` and `max_retries` isn't exhausted, the task runs again. If all retries fail, the task is terminal and the message's offset commits only after all its tasks settle. See [Error Handling — on_error Hook](executor.md#error-handling-on-error-hook).

### What if a subprocess hangs?

`executor.task_timeout_seconds` (default per-profile) kills the subprocess and reports exit code 124. Then the normal `on_error` path handles it. See [Execution Flow — Enforce Timeout](executor.md#5-enforce-timeout).

### What if the Kafka broker goes down?

librdkafka (the underlying client) handles reconnects transparently — the consumer automatically rejoins when brokers recover. **TBD** — document how Drakkar surfaces "disconnected" state in the dashboard and whether there's a hard fail-fast option.

### What if a sink is unreachable for a long time?

The sink's retry policy kicks in; exhausted retries route the record to the DLQ topic and fire `on_delivery_error`. See [Dead letter queue](sinks.md#dead-letter-queue).

### How do I replay messages from the DLQ?

**TBD** — there's no built-in replay worker today. Typically operators run a one-off script that consumes from the DLQ topic and re-produces to the source topic. Worth shipping a reference script.

### A message is stuck in "in-flight" forever — what do I do?

Check the **Executors** tab on `/live` for stuck tasks; their `task_timeout_seconds` should eventually fire. If a single task is wedged past the timeout and the subprocess didn't die, something unusual is happening — check the worker logs and consider restarting. **TBD** — document the specific rescue procedure.

---

## Architecture deep-dive

### Why is the executor subprocess-based instead of async-native or threaded?

Four reasons, all aligned with the "CPU in external binaries, Python orchestrates I/O" design goal:

1. **Language portability** — the worker binary can be Rust/Go/C++/a CLI tool you don't own; it just reads a JSON payload from stdin and writes results to stdout.
2. **Real parallelism** — N subprocesses truly use N cores. Python threads would still contend on the GIL for any CPU burst; `asyncio.to_thread` is only useful for blocking I/O.
3. **OS-level CPU isolation** — one task's bug (segfault, OOM, infinite loop) cannot corrupt the worker process.
4. **Clean timeouts and cancellation** — the executor just SIGKILLs a runaway subprocess. Cancelling in-process Python threads is notoriously unreliable.

The cost is ~10ms of launch overhead per task ([Bottleneck: Subprocess Launch](performance.md#bottleneck-subprocess-launch)) — a fair trade when your task does 10ms–10s of real CPU work. For sub-millisecond tasks, use [precomputed results](handler.md#precomputed-task-results) to skip the subprocess entirely.

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

This section expands the five trust assumptions listed in the [README](../README.md#security--trust-model) -- each one is an architectural trust boundary, not a latent bug. Read this before a production deploy.

### Why is the handler binary trusted?

Drakkar launches `executor.binary_path` as a subprocess and pipes the message bytes to its stdin without validation. There's no sandbox, no signature check, and no attempt to filter input — the binary is assumed to be operator-provided code you audit the same way you audit the rest of your deployment.

See `drakkar/executor.py::ExecutorPool._launch` for the launch code. The binary runs with the worker's OS privileges, plus any env overrides from `ExecutorConfig.env` or per-task `env`. If you need defense-in-depth against a compromised binary, run the worker under a restricted user / with seccomp / inside a container — Drakkar itself offers no in-process sandbox.

### Why are peer workers trusted?

The cache and recorder peer-sync mechanisms read other workers' SQLite files directly (see `drakkar/cache.py::CacheEngine._sync_loop` and `drakkar/recorder.py::cross_trace`). There's no per-write signature, no auth check, no schema-level integrity verification. Anyone who can write to the shared `db_dir` can inject cache entries or event rows that your workers will read.

Treat `db_dir` as a shared-trust boundary: any principal with write access to that directory has the same trust level as the workers themselves. On a shared filesystem (NFS, EFS), restrict directory permissions to the worker user.

### How is the debug UI protected?

Three layers (see [Is the debug UI safe](#is-the-debug-ui-safe-to-expose-to-a-team-of-operators) above):

1. Loopback default (`debug.host='127.0.0.1'`).
2. `InsecureDebugConfigError` at startup if you set a non-loopback host without an `auth_token`.
3. Token + Origin check on the WebSocket stream (`drakkar/debug_server.py::_origin_allowed` and `_token_matches`).

Read-only HTTP pages are not token-gated — auth applies to mutating endpoints (merge, probe) and to the WebSocket event stream. If you put the UI on a non-loopback host, set a strong `auth_token` and consider a reverse proxy with TLS.

### Why doesn't Drakkar validate Kafka message payloads?

Parse errors in `handler.deserialize_message` silently set `msg.payload=None` rather than raising or DLQ-ing the message (see `drakkar/app.py::_deserialize`). A malicious producer can cause handlers to see unexpected `None` payloads, but cannot execute code in the worker. Your handler is responsible for validating the payload before using it — use Pydantic `model_validate` or raise explicitly from `arrange()` to route bad messages.

### What redactions apply to per-task env?

Two surfaces expose env vars:

- **The recorder's `worker_config` table** — framework-level `ExecutorConfig.env` is **never written** to the recorder (it's omitted from the JSON payload entirely). Environment variables listed in `expose_env_vars` are captured by name, and secret-shaped names (`*PASSWORD*`, `*SECRET*`, `*TOKEN*`, `*_KEY`, `*API_KEY*`, `*CREDENTIAL*`, `*_DSN`) are redacted to `***`. Non-matching values with embedded URL credentials (`user:pass@host`) have the credentials stripped.
- **The recorder's per-task `env` metadata** — `task.env` written by your handler is sanitized with the same secret-name patterns before being stored. The original task object is not mutated; only the recorded copy is redacted. See `drakkar/recorder.py::_sanitize_env_value` for the regex.

The contract is "aggressive redact, accept false positives": `PASSWORD_RESET_URL` is redacted because it matches `*PASSWORD*`, even though a reset URL isn't a credential. Operators who need to expose these exact names should rename them — a leaked secret is a worse outcome than a logged URL.

---

## Contribute to this page

Missing a question you've asked or answered? Send a PR adding it here — even questions with **TBD** answers are useful: they mark the project's documentation debt visibly, and get resolved faster when they're written down.
