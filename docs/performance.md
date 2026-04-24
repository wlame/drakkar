# Performance Tuning

This page covers performance characteristics and tuning strategies for
high-throughput Drakkar deployments -- particularly when running fast
tasks (5-50ms) on machines with many CPU cores (32-128+).

---

## Per-Task Overhead Breakdown

The table below shows approximate overhead for a specific scenario:
**a binary that runs in ~30ms, on an 80-core machine with
`max_executors: 80`**. Your numbers will differ based on hardware, binary
startup cost, message size, and Pydantic model complexity -- but the
relative proportions hold.

| Step | What happens | Approximate cost |
|------|-------------|-----------------|
| **Kafka poll** | `consumer.consume()` returns batch of messages | ~1ms per batch (amortized across `max_poll_records` messages) |
| **Enqueue** | Message placed in partition asyncio.Queue, `record_consumed()` event, Prometheus counter increment | ~5us per message |
| **Window gather** | Drain up to `window_size` messages from the partition queue (non-blocking after first) | ~10us per window |
| **Deserialize** | `model_validate_json(msg.value)` for each message (if typed handler) | ~10-50us per message (depends on model complexity) |
| **arrange()** | Your hook -- builds ExecutorTask list | User-defined |
| **Record arranged** | JSON-encode metadata, write to event buffer | ~20us |
| **Semaphore acquire** | `asyncio.Semaphore` -- instant if slots available, blocks if pool full | 0 or unbounded wait |
| **Slot bookkeeping** | Pop slot from list, increment counters, record_task_started, deferred WS timer | ~30us |
| **Process launch** | `asyncio.create_subprocess_exec()` -- kernel fork+exec | **1-5ms** (the big one) |
| **Process runtime** | Your binary runs | ~30ms (actual work) |
| **Pipe read** | Read stdout/stderr from pipes, decode UTF-8 | ~10-50us (depends on output size) |
| **Pydantic model creation** | Build `ExecutorResult` model | ~10us |
| **record_task_completed** | JSON-encode, buffer event, deferred WS check | ~20us |
| **on_task_complete()** | Your hook -- builds CollectResult with payloads | User-defined |
| **Sink delivery** | Send payloads to Kafka/Postgres/Mongo/etc. | ~0.5-5ms per sink (network I/O) |
| **record_task_complete** | Buffer event | ~10us |
| **Offset tracking** | Mark offsets complete, check committable watermark | ~5us |
| **Offset commit** | `consumer.commit()` to Kafka | ~5-20ms (but async, non-blocking) |

### Where the time goes for a 30ms task

```
Process launch:      ~3ms         (9% of task wall time)
Actual work:         ~30ms        (the binary)
Python overhead:     ~210-390us   (bytecode + JSON encoding, GIL-held)
Sink delivery:       ~1-5ms       (network, per sink, async)
```

The **Python orchestration overhead is real** — not negligible, not
crippling. A more honest breakdown per task:

- **Python bytecode** (arrange, on_task_complete, subprocess completion
  handling, offset bookkeeping, Pydantic model construction): **150-250us**
- **JSON encoding** (structlog with contextvars, Prometheus label
  materialization, `json.dumps` inside the recorder): **60-140us**
- **Total per-task Python-side cost**: **210-390us**

This is GIL-held time — it runs on the single event-loop thread. For a
30ms task, that's still only ~1% of task wall time, but the absolute
cost matters at high throughput (see the next section).

Sink delivery is I/O-bound and runs concurrently with the next task's
execution, so its wall-clock cost doesn't pile onto the event loop the
way bytecode and JSON encoding do.

As tasks get faster (5-10ms), the process launch fraction grows to
30-50% -- see [Subprocess Launch](#bottleneck-subprocess-launch) below
for mitigation strategies.

---

## Bottleneck: Event Loop

Drakkar's main processing runs on **one asyncio event loop** in one
thread. The event loop handles:

- Kafka polling
- Message deserialization
- [arrange()](handler.md#arrange-required) / [on_task_complete()](handler.md#on_task_complete) / [on_message_complete()](handler.md#on_message_complete) / [on_window_complete()](handler.md#on_window_complete) / [on_error()](handler.md#on_error) calls
- Subprocess launch and completion callbacks
- [Flight recorder](observability.md#flight-recorder) event buffering
- [Prometheus metric](observability.md#prometheus-metrics) updates
- [Offset tracking](handler.md#offset-commit-logic) and commits

Note: the **[debug web server](observability.md#debug-ui) runs in a separate thread** with its own
Uvicorn event loop (`DebugServer` uses `threading.Thread`). It does not
compete for time on the main event loop. Communication between them uses
a thread-safe `queue.Queue` for WebSocket event broadcasting.

With 80 executors completing 30ms tasks, that's ~2,700 task
completions per second hitting the event loop. Each completion triggers
a cascade of Python bytecode and JSON encoding, all serialized on the
single event-loop thread (GIL-held):

1. Subprocess completion handling, pipe read, UTF-8 decode
2. Pydantic `ExecutorResult` construction
3. `record_task_completed()` — structlog (with contextvars), Prometheus
   label materialization, `json.dumps` into the recorder buffer
4. `on_task_complete()` (user code)
5. `record_task_complete()` — another structlog + JSON round-trip
6. Sink delivery dispatch (fast to queue; I/O itself is async)
7. Offset complete + committable scan
8. Prometheus metric updates, `asyncio.Task` cleanup

**Total Python-side cost per completion**: **210-390us**
(150-250us bytecode + 60-140us JSON encoding) **+ on_task_complete() duration**.

At 2,700 completions/sec, that's **570-1050ms/sec of GIL-held time —
57-100% of a single core**. Your `on_task_complete()` runs on top of
that. **At the advertised sweet spot (80 executors, 30ms tasks), the
event loop IS the bottleneck**, not a comfortable cushion.

**Budget for it.** Python orchestration is the primary reason a single
worker tops out around 4k-8k tasks/sec regardless of how many cores are
available. To push further, scale horizontally (more workers, each with
its own event loop) or reduce per-task overhead (see the mitigations
below and the [Recorder](#bottleneck-recorder-and-debug-ui) tuning
knobs).

The ceiling moves even lower when:

- `arrange()` or `on_task_complete()` do non-trivial CPU work (any
  time spent here is fully additive to the 210-390us baseline)
- `event_min_duration_ms: 0` is set at high throughput — every task
  pays the full JSON-encode cost into the recorder buffer
- Sink payloads are large enough that per-sink JSON encoding is
  non-trivial (group size x payload complexity)
- You run 200+ executors with sub-5ms tasks (>40,000 completions/sec is
  beyond a single event loop's reach, period)

### Future optimizations (Phase 3+)

Two concrete changes buy meaningful headroom on this bottleneck without
changing the execution model:

- **Off-thread JSON encoding.** Batch recorder events and hand the
  `json.dumps` work to a worker thread via `asyncio.to_thread` (or a
  dedicated encoder thread). The event loop keeps the Python-object
  buffer; JSON serialization happens off-thread and doesn't hold the GIL
  during the I/O-free encoding fast path.
- **Switch to `orjson`.** A drop-in replacement for `json.dumps` that
  is ~3-5x faster on typical recorder payloads. Keeps the encoding on
  the main thread but cuts the 60-140us per-task cost roughly in half.

Combined, these unlock the 4k-8k tasks/sec/worker ceiling. They are
scoped for a later phase — today, the honest guidance is "scale
horizontally once one worker saturates".

---

## Bottleneck: Subprocess Launch

For tasks under 20ms, the process launch overhead (1-5ms) becomes a
significant fraction of total time. This is a Linux kernel cost --
Drakkar cannot eliminate it.

### Mitigation strategies

**1. Batch multiple messages into one task.**
Instead of one subprocess per message, have `arrange()` combine N
messages into one task via stdin or args:

```python
async def arrange(self, messages, pending):
    # one task for the entire window
    batch = [msg.payload.model_dump() for msg in messages]
    return [ExecutorTask(
        task_id=make_task_id('batch'),
        args=['--batch'],
        stdin=json.dumps(batch),
        source_offsets=[msg.offset for msg in messages],
    )]
```

This trades latency for throughput: one process launch for 100 messages
instead of 100 launches. For 5ms tasks, this can improve throughput
10-50x.

**2. Use a long-running worker process instead of one-shot binaries.**
If your binary supports it, start it once and feed work via stdin/stdout
line protocol. Drakkar's [stdin support](executor.md#stdin-support) enables this pattern --
the process receives input on stdin and writes results to stdout.

**3. Increase `window_size`.**
Larger windows mean more messages per `arrange()` call, enabling larger
batches. With `window_size: 500` and batching in `arrange()`, you
amortize process launch across hundreds of messages.

**4. Skip the subprocess entirely with `PrecomputedResult`.**
When the handler already knows the answer — from a cache, lookup table,
or deterministic logic — attach a [`PrecomputedResult`](handler.md#precomputed-task-results-skip-the-subprocess)
to the task in `arrange()`. The framework skips the semaphore AND the
subprocess, invoking `on_task_complete` directly with the synthesised
result. On cache-hit-heavy workloads this removes the entire
process-launch cost from the hot path and the effective throughput
ceiling is no longer capped by `max_executors`. Track the short-circuit
rate via `drakkar_tasks_precomputed_total` (compare to
`drakkar_executor_tasks_total{status="completed"}` to see the fraction
served from cache).

**5. Memoize via the framework [cache](cache.md).**
Strategies 4 works best when combined with a real cache: `self.cache`
gives you an in-memory dict with LRU eviction, a write-behind SQLite
persistence layer (results survive worker restarts), and optional
peer-sync (results shared across workers in the same cluster, LWW
merged). Pair `self.cache.peek(key)` in `arrange()` with
`PrecomputedResult` on a hit, and `self.cache.set(key, ..., ttl=...)`
in `on_task_complete` on a subprocess run. Track the hit rate via
`drakkar_cache_hits_total{source="memory"|"db"}` and
`drakkar_cache_misses_total`. See the [Cache](cache.md) page for the
full story, scope rules, and the documented "delete is local-only"
sharp edge.

---

## Bottleneck: Window Serialization

Within a single partition, `arrange()` is called sequentially -- one
window at a time. While tasks from window N run concurrently, the
next `arrange()` call for window N+1 does not wait for N to complete --
it starts as soon as the next batch of messages is available.

However, if your `arrange()` does I/O (HTTP calls, DB lookups), it
blocks the partition pipeline for the duration of that call.

### Mitigation

- Keep `arrange()` fast -- do lookups in [on_ready()](handler.md#on_ready) and cache results
- If you need per-message lookups, do them in [on_task_complete()](handler.md#on_task_complete) instead (runs
  concurrently per task)
- Use more partitions to parallelize across partition processors

---

## Bottleneck: Recorder and Debug UI

With `event_min_duration_ms: 0` (default), every task writes events to
the SQLite buffer. See [Duration Thresholds](observability.md#duration-thresholds) for the full reference on these settings. At 2,700 tasks/sec (80 workers, 30ms tasks), that's
~8,000 events/sec (started + completed + consumed), flushed to SQLite
every `flush_interval_seconds`. The flush itself is a batch INSERT that
takes ~10-50ms for 10,000 rows.

The WebSocket broadcast is handled in the debug server's **separate
thread**, so it doesn't block the main event loop. However, the
`_record()` method on the main loop still JSON-encodes each event and
pushes it to the thread-safe queue (~20us per event).

### Mitigation

For high-throughput fast-task workloads:

```yaml
debug:
  event_min_duration_ms: 50    # skip DB writes for tasks < 50ms
  ws_min_duration_ms: 100      # skip live UI updates for tasks < 100ms
  output_min_duration_ms: 100  # skip stdout/stderr for fast tasks
  log_min_duration_ms: 100     # skip structlog for fast tasks
  store_output: false          # no stdout/stderr storage at all
```

This keeps the debug system useful (slow tasks, failures, and sink
deliveries are still recorded) while eliminating per-task overhead for
the fast majority.

---

## Bottleneck: Slot Bookkeeping

A minor detail that matters at extreme scale: after each task completes,
the executor pool runs `self._available_slots.sort()` (line 82 of
`executor.py`). With `max_executors: 80`, this sorts a list of up to
80 integers on every task completion. At 2,700 completions/sec, that's
2,700 sorts/sec -- still fast (80-element sort is ~1us), but it's
there.

---

## Backpressure

Drakkar uses Kafka consumer pause/resume to prevent unbounded memory
growth when the executor pool can't keep up with incoming messages.

### How it works

On every poll loop iteration, the framework calculates `total_queued` --
the sum of all partition queue sizes plus all in-flight task counts. Two
thresholds control the hysteresis:

```
high_watermark = max_executors x backpressure_high_multiplier
low_watermark  = max(1, max_executors x backpressure_low_multiplier)
```

With defaults (`max_executors: 4`, `high_multiplier: 32`,
`low_multiplier: 4`):

```
high_watermark = 4 x 32 = 128
low_watermark  = 4 x 4  = 16
```

**When `total_queued >= high_watermark`:** the consumer pauses all
assigned partitions (stops fetching new messages) and sets the
`drakkar_backpressure_active` metric to **1**.

**When `total_queued <= low_watermark`:** the consumer resumes all
partitions and sets `drakkar_backpressure_active` back to **0**.

The gap between high and low prevents rapid pause/resume flapping.
Messages already in partition queues continue processing while paused --
only new fetches from Kafka are stopped.

### Tuning

For high core count machines, the defaults may be too conservative:

| Scenario | Suggested values |
|----------|-----------------|
| 80 workers, 30ms tasks | `high_multiplier: 16`, `low_multiplier: 4` -- high=1280, low=320 |
| 80 workers, 5ms tasks | `high_multiplier: 8`, `low_multiplier: 2` -- high=640, low=160 |
| 4 workers, 10s tasks | `high_multiplier: 32`, `low_multiplier: 4` -- high=128, low=16 (defaults are fine) |

If `drakkar_backpressure_active == 1` is persistent, either increase
`max_executors`, add more horizontal workers, or increase the multipliers
to buffer more in memory.

---

## Configuration Recommendations

### Fast tasks (< 50ms), high core count

```yaml
executor:
  max_executors: 64              # match available cores, leave headroom
  window_size: 200             # larger windows for batching opportunity
  task_timeout_seconds: 30     # short timeout for fast tasks
  backpressure_high_multiplier: 16
  backpressure_low_multiplier: 4

kafka:
  max_poll_records: 500        # pull more messages per poll cycle

debug:
  ws_min_duration_ms: 100      # hide fast tasks from live UI
  event_min_duration_ms: 50    # don't store fast tasks in DB
  output_min_duration_ms: 100  # don't store stdout for fast tasks
  log_min_duration_ms: 100     # don't log fast tasks
  store_output: false          # skip stdout/stderr storage entirely
```

| Setting | Rationale |
|---------|-----------|
| `max_executors: 64` | On an 80-core machine, leave ~16 cores for the event loop, OS, Kafka consumer, and sink I/O. Going higher adds contention without benefit. |
| `window_size: 200` | With fast tasks, larger windows let `arrange()` batch more messages per subprocess. Even without batching, larger windows reduce `arrange()` call frequency. |
| `max_poll_records: 500` | Pull more messages per Kafka poll to keep partition queues full. Reduces poll overhead per message. |
| `ws_min_duration_ms: 100` | Fast tasks flood the WebSocket and live UI. Hiding them reduces event loop work and browser rendering cost. |
| `event_min_duration_ms: 50` | Skip SQLite writes for fast tasks. At high throughput, SQLite INSERT rate becomes a bottleneck. |
| `store_output: false` | Stdout/stderr for fast tasks is usually small and uninteresting. Skipping it eliminates JSON encoding and storage. |

### Slow tasks (> 1s), moderate core count

```yaml
executor:
  max_executors: 8
  window_size: 20
  task_timeout_seconds: 300

kafka:
  max_poll_records: 100

debug:
  ws_min_duration_ms: 0        # show all tasks in live UI
  event_min_duration_ms: 0     # store everything
  store_output: true           # stdout/stderr is valuable for debugging
```

For slow tasks, the overhead analysis inverts: process launch is
negligible (3ms vs 5000ms task), the event loop is idle most of the
time, and debug recording has no throughput impact. Optimize for
observability.

### Long-running tasks (~10s), low core count

```yaml
executor:
  max_executors: 16
  window_size: 5               # small windows -- few tasks in flight at once
  task_timeout_seconds: 120    # generous timeout, but not infinite
  max_retries: 2               # retry on transient failures
  drain_timeout_seconds: 30    # allow time for in-flight tasks on shutdown
  backpressure_high_multiplier: 8
  backpressure_low_multiplier: 2

kafka:
  max_poll_records: 20         # don't over-fetch -- each message = 10s of work
  session_timeout_ms: 60000    # longer session timeout to avoid rebalance during slow processing
  max_poll_interval_ms: 600000 # 10 min between polls -- tasks may run long

debug:
  ws_min_duration_ms: 0        # show everything in live UI
  event_min_duration_ms: 0     # store all events
  store_output: true           # stdout/stderr is critical for 10s tasks
  log_min_duration_ms: 0       # log every task
```

| Setting | Rationale |
|---------|-----------|
| `max_executors: 16` | With 10s tasks, 16 workers means ~1.6 tasks/sec throughput. Adding more workers only helps if you have enough CPU cores. |
| `window_size: 5` | Small windows avoid buffering too many 10s tasks. Each window takes ~10s to complete, so commit latency = 10s per window. |
| `max_poll_records: 20` | Don't fetch 500 messages when each takes 10s to process -- that's 83 minutes of work per poll. |
| `session_timeout_ms: 60000` | Default 45s may trigger rebalance if the event loop is busy with task completions. 60s gives more headroom. |
| `max_poll_interval_ms: 600000` | Kafka kicks a consumer out if it doesn't poll within this interval. With 10s tasks and window_size 5, a full window takes ~50s. 10 minutes provides ample margin. |
| `drain_timeout_seconds: 30` | On shutdown, wait up to 30s for 10s tasks to finish rather than killing them. |

---

## Scaling Horizontally

When a single worker's event loop or I/O bandwidth saturates, add more
workers with the same `consumer_group`. Kafka's cooperative-sticky
rebalancing redistributes partitions.

**Partition count is the parallelism ceiling.** If you have 10
partitions and 20 workers, 10 workers sit idle. For high-throughput
scenarios, ensure `partition_count >= worker_count`.

```bash
# scale to 4 workers
WORKER_ID=worker-1 python main.py &
WORKER_ID=worker-2 python main.py &
WORKER_ID=worker-3 python main.py &
WORKER_ID=worker-4 python main.py &
```

Each worker gets a subset of partitions and runs its own executor pool.
The total subprocess capacity across the cluster is
`worker_count x max_executors`.

---

## Monitoring Throughput

Key [Prometheus](observability.md#prometheus-metrics) queries for identifying bottlenecks (use the [Config Calculator](calculator.md) for initial tuning values):

```promql
# Messages consumed per second (are we keeping up?)
rate(drakkar_messages_consumed_total[1m])

# Task completion rate
rate(drakkar_executor_tasks_total{status="completed"}[1m])

# Backpressure (1 = consumer paused, 0 = flowing)
drakkar_backpressure_active

# Queue depth (growing = can't keep up)
drakkar_total_queued

# Task duration p95 (includes process launch overhead)
histogram_quantile(0.95, rate(drakkar_executor_duration_seconds_bucket[5m]))

# arrange() duration (should be << task duration)
histogram_quantile(0.95, rate(drakkar_handler_duration_seconds_bucket{hook="arrange"}[5m]))

# on_task_complete() duration (should be << task duration)
histogram_quantile(0.95, rate(drakkar_handler_duration_seconds_bucket{hook="on_task_complete"}[5m]))

# Executor idle waste — slot-seconds wasted while messages wait in queues
rate(drakkar_executor_idle_slot_seconds_total[5m])

# Consumer idle — seconds/second with nothing to do (lag is zero)
rate(drakkar_consumer_idle_seconds_total[5m])
```

If `backpressure_active == 1` consistently, you need more `max_executors`
or more horizontal workers. If `total_queued` grows unbounded, your
processing rate is lower than your production rate.

### Efficiency metrics

**`drakkar_executor_idle_slot_seconds_total`** measures wasted executor
capacity. It accumulates `idle_slots x dt` on every poll loop iteration,
but **only when messages are waiting in partition queues** (not yet
dispatched to executors). If the queues are empty, idle slots are
expected and don't count as waste.

A high `rate(drakkar_executor_idle_slot_seconds_total[5m])` means your
executor slots are free but messages sit in queues waiting for
`arrange()` to run. This points to `arrange()` being slow, or
`window_size` being too large (the framework waits to fill the window
before calling `arrange()`).

**`drakkar_consumer_idle_seconds_total`** measures time the worker has
genuinely nothing to do -- Kafka poll returned no messages, partition
queues are empty, and no tasks are in flight. A high rate means the
worker is over-provisioned or the source topic has low volume. This
metric is **not** incremented when the consumer is paused due to
backpressure (that's deliberate throttling, not idleness).
