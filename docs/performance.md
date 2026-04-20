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
Process launch:      ~3ms    (9% of task wall time)
Actual work:         ~30ms   (the binary)
Framework overhead:  ~0.2ms  (arrange + on_task_complete + recording + tracking)
Sink delivery:       ~1-5ms  (network, per sink, async)
```

For a 30ms task, process launch is ~9% overhead -- noticeable but
manageable. **The framework's own overhead** (arrange, on_task_complete,
recording, offset tracking) **is negligible at ~200us total.** Sink
delivery is I/O-bound and runs concurrently with the next task's
execution.

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
completions per second hitting the event loop. Each completion triggers:

1. Subprocess completion handling (~5us)
2. `record_task_completed()` (~20us)
3. `on_task_complete()` (user code)
4. `record_task_complete()` (~10us)
5. Sink delivery dispatch (~5us to queue, actual I/O is async)
6. Offset complete + committable scan (~5us)
7. Prometheus metric updates (~10us)
8. `asyncio.Task` cleanup (~5us)

Total event loop time per completion: **~60us + on_task_complete() duration**.
At 2,700/sec, that's ~160ms/sec of event loop time -- well within
budget for a single core. **The event loop is not the bottleneck** for
most workloads.

It becomes the bottleneck when:

- `arrange()` or `on_task_complete()` do heavy CPU work (avoid this -- keep them fast)
- You run 200+ executors with sub-5ms tasks (>40,000 completions/sec)
- Debug recording is enabled with `event_min_duration_ms: 0` and very high throughput

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
