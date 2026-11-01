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
| **Offset commit** | `consumer.commit()` to Kafka | ~5-20ms, **synchronous** — coalesced, so it costs at most one round trip per partition per 500 ms (see below) |

### Offset commits are batched

A commit is a **synchronous** broker round trip on a consumer-thread-pool
thread, serialized per partition by the commit lock. (An earlier version of
this page called it asynchronous. It never was.)

It does not, however, happen per message. Every completed message advances
the per-partition watermark, and the commit is deferred until either

- the watermark has moved **300 offsets**, or
- **500 ms** have passed since the oldest uncommitted advance,

whichever comes first. So the commit rate is bounded at ~2/s per partition
regardless of message rate, instead of tracking it — which mattered most for
low-fan-out handlers, where one message is one task and the commit rate was
the message rate.

Two things are deliberately **not** affected:

- **At-least-once is unchanged.** Deferring a commit can only make a
  restarted worker redo work it already did, which the guarantee already
  allows. Losing a message would require committing *early*, and nothing
  here does that.
- **Giving up a partition always flushes.** A rebalance revoke, a drain and
  a shutdown each force the pending commit before releasing the partition,
  so batching never turns into redelivery at handover.

The window bounds how much work a crash can cost: up to 500 ms of finished
messages get reprocessed instead of up to one. The framework uses the same
two thresholds.

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

Note: the **[API server](observability.md#operator-ui) runs in a separate thread** with its own
Uvicorn event loop (`APIServer` uses `threading.Thread`). It does not
compete for time on the main event loop. Communication between them uses
a thread-safe `queue.Queue` for WebSocket event broadcasting.

### Fan-out is chunked

A window's task count is whatever `arrange()` returns, so it is bounded by
the message shape rather than by any setting. The framework creates those
tasks 256 at a time and hands the loop back between chunks. Without that,
asyncio would run the first step of every coroutine before polling I/O again
— roughly 9 µs per task, which is a tenth of a second for a 10,000-task
window and close to a second for 100,000, during which nothing else runs at
all: no Kafka poll, no sink delivery, no UI frame, no health sample.

The chunk size is fixed, not configurable. What is still proportional to the
window's task count is **memory**, not loop time: every task is a parked
`asyncio.Task` with a queue entry until a pool slot frees up. Keep
`window_size` in proportion to the tasks per message — a handler emitting a
thousand tasks per message wants a window of a few messages, not the default
hundred.

### What the completion hooks cost

`on_window_complete` receives every result of its window, and
`on_message_complete` receives every task, result and error of its message.
A result carries the subprocess output. Holding it is what the hooks are
for — but the window only releases it when its **last** task settles, so a
handler that implements `on_window_complete` keeps the output of a whole
window resident. The stdin is not kept: when a task reaches its last outcome
the framework sets `task.stdin` to `None`, so these hooks see the tasks
without their stdin.

The framework therefore collects them only for the hooks a handler actually
implements. A handler that leaves both inherited pays nothing: the counters
still move, the recorder still reports how many tasks succeeded and failed,
and resident memory tracks the tasks in flight instead of the window.

If you implement one of them, the kept output counts in
`executor.max_pending_bytes` until the hook has run, so new windows wait
while it is held (see [Backpressure](#backpressure)). Read what you need in
`on_task_complete` — which is handed one result at a time and retains
nothing — and keep the aggregate hooks for counts and summaries.

Your task rate follows from the pool size: roughly
`max_executors / average task duration` — 8 slots at 20ms tasks is
~400 tasks/sec, 64 slots at 200ms tasks is ~320 tasks/sec. But the
rate has a ceiling that pool size cannot buy past: **plan for at most
~400 tasks per second per worker**. Beyond that, the single event loop
itself becomes the bottleneck, and adding slots only grows the queue.

The number is deliberately conservative. The per-completion budget
below measures framework overhead only — it does not include the
preparation work a real `arrange()` does to *create* those tasks
(parsing, grouping, building stdin), which also runs on the loop and
scales with the task count. Planning against ~400/sec leaves room for
that unmeasured cost; a handler with unusually lean hooks may sustain
more, but measure before counting on it.

Each completion triggers a cascade of Python bytecode and JSON
encoding, all serialized on the single event-loop thread (GIL-held):

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

At 400 completions/sec, that baseline is 84-156ms of GIL-held time per
second — 8-16% of a core — which leaves real headroom for your own hook
code, Kafka polling, offset commits, and recorder flushes. Push toward
2,700 completions/sec (80 executors on 30ms tasks) and the same math
gives **570-1050ms/sec: 57-100% of a single core** consumed before
`arrange()` and `on_task_complete()` run at all. The loop saturates,
consumer lag grows, and every subsystem sharing the loop degrades
together. This is why very short CPU-bound tasks in high volume are the
worst fit for the model: they maximize completions per second, which is
exactly the currency the loop runs out of first.

**The ceiling is soft — and it mostly moves down.** ~400 tasks/sec
assumes your hook code is cheap. The more CPU-bound work task
preparation and completion do on the loop — compiling or applying
regexes, nested loops over large collections, building large payloads
in `arrange()` or `on_task_complete()` — the more loop stalls you
introduce, and each stall delays *every* in-flight task's completion
handling, not just its own. Heavy hooks can drag the practical maximum
far below 400/sec. Keep per-call hook work in the tens of microseconds
and move anything heavier to
[`handler.offload()`](handler.md#offloading-cpu-bound-work).

The ceiling also drops when:

- `recorder.capture.min_event_duration: 0` is set at high throughput — every task
  pays the full JSON-encode cost into the recorder buffer
- Sink payloads are large enough that per-sink JSON encoding is
  non-trivial (group size x payload complexity)

**To go faster, add workers, not slots.** Each worker brings its own
event loop, so cluster throughput scales horizontally (see
[Scaling Horizontally](#scaling-horizontally)). On a large host, run
several workers side by side rather than one worker with a huge pool.

### Available optimization: `orjson` (opt-in)

Installing the `perf` extra swaps the recorder's JSON encoder for
[`orjson`](https://github.com/ijl/orjson), a C-based encoder that is
~3-5x faster than `json.dumps` on typical recorder payloads
(task args, metadata, labels). Install with:

```bash
pip install "drakkar[perf]"
```

No config change — the recorder detects `orjson` at import time and
transparently falls back to the stdlib `json` module when the extra is
not installed. **Output bytes are identical across both paths** (same
sort-key order, compact separators, UTF-8) so the on-disk recorder DB
stays stable whether or not the extra is present.

Recommended once a worker runs in the upper part of its envelope
(roughly **> 200 tasks/sec**); below that, the stdlib encoder is fast
enough that the gain is not measurable.

### Further headroom on the horizon

`orjson` has already shipped (see above). One more concrete change
would buy meaningful additional headroom on this bottleneck without
changing the execution model:

- **Off-thread JSON encoding.** Batch recorder events and hand the
  encode work to a worker thread via `asyncio.to_thread` (or a
  dedicated encoder thread). The event loop keeps the Python-object
  buffer; serialization happens off-thread and doesn't hold the GIL
  during the I/O-free encoding fast path.

Combined with `orjson`, this buys back part of the per-completion
budget — but it does not change the architecture. Orchestration stays
serialized on one loop, so treat these gains as extra headroom *within*
the ~400 tasks/sec envelope, not as a path to multiples of it. Today
the honest guidance is "install the `perf` extra, then scale
horizontally once one worker saturates".

### When Python is the wrong tool

If your workload is inherently high-rate, small, CPU-bound tasks —
thousands per second of 5-20ms work — no amount of tuning makes a
single Python event loop comfortable, and the right fix may be a
different runtime rather than more workers. Drakkar orchestrates every
task on one event loop per worker, and that loop runs on one core. If
you are hitting the per-worker ceiling with short CPU-bound tasks, move
that work into the subprocess (where it runs in parallel), batch more
messages per task, or run that part of the workload on a runtime that
schedules work across all cores.

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
persistence layer (results survive worker restarts), and an optional
cache exchange (results shared across workers over HTTP, LWW
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

## Bottleneck: Recorder and UI

With `recorder.capture.min_event_duration: 0` (default), every task writes events to
the SQLite buffer. See [Duration Thresholds](observability.md#duration-thresholds) for the full reference on these settings. At the ~400 tasks/sec per-worker ceiling, that's
~1,200 events/sec (started + completed + consumed), written every
`recorder.flush_interval`. Each write is a batch INSERT that takes ~10-50ms
for 10,000 rows.

The recorder writes in two stages. Stage M writes the events to the
in-memory [memory store](recent-events.md); with provider `sqlite`
stage F then writes them to the file. The two run on separate loops, so the
memory intake does not wait for the disk: a slow disk delays only the file,
and the recent events stay readable. The UI reads recent rows (time windows,
newest rows, polls) from memory while the store still holds them. Pages
keyed by task, message or label, and the whole-table pages, still read the
file with provider `sqlite`, so they still compete with the file writer for the disk.

A memory read holds a read lock on the store, and the memory writer waits
for it. Every memory read has a 2 second limit, under the writer's 5 second
busy timeout, so a slow read delays a memory write and never makes it fail.
The default memory store (a `2m` window, 3000 rows per executor and a
256 MiB budget) keeps each read small. See
[Sizing the row cap](recent-events.md#sizing-the-row-cap) and
[RAM to plan](recent-events.md#ram-to-plan).

The WebSocket broadcast is handled in the API server's **separate
thread**, so it doesn't block the main event loop. However, the
`_record()` method on the main loop still JSON-encodes each event and
pushes it to the thread-safe queue (~20us per event).

### Mitigation

For high-throughput fast-task workloads:

```yaml
logging:
  min_task_duration: 100ms     # skip structlog for fast tasks
ui:
  ws_min_duration: 100ms       # skip live UI updates for tasks < 100ms
recorder:
  capture:
    min_event_duration: 50ms   # skip DB writes for tasks < 50ms
    min_output_duration: 100ms   # skip stdout/stderr for fast tasks
    output: false                # no stdout/stderr storage at all
```

This keeps the recorder and UI useful (slow tasks, failures, and sink
deliveries are still recorded) while eliminating per-task overhead for
the fast majority.

### Recorder history in Postgres

With `recorder.provider: postgres`, stage P writes every row of the
memory store to a table in Postgres. It runs on a thread of its own with one
connection, so the main loop pays only for handing a chunk to the Postgres
queue; a slow or absent database never stalls the pipeline, and only the
bounds of that queue drop rows.

The cost moves to the database. A busy worker at about 5000 rows per second
writes about 2.5 MB per second, 9 GB per hour and 54 GB at a
retention of 6 hours, and the write-ahead log about doubles the disk writes.
See [Write volume](recorder-postgres.md#write-volume) for the table.

Two kinds of settings cut that volume:

- The recorder filters `recorder.capture.min_event_duration` and `recorder.capture.min_output_duration`
  act before the memory store. They cut every provider, and they also shorten
  the live view and the memory store.
- `recorder.postgres.store_completed_output: false` (the default) writes `stdout`
  and `stderr` as `NULL` in Postgres only; the memory store keeps them.
  With `true`, rows with output can be many KB each.

Put a busy cluster on its own `table_prefix` with a short
`recorder.postgres.initial_retention`, or keep it on `sqlite`. Watch
`drakkar_recorder_postgres_rows_written_total`,
`drakkar_recorder_postgres_queue_rows` and
`drakkar_recorder_postgres_dropped_rows_total{reason="queue_full"}`: a queue
that stays near `recorder.max_buffer_rows` means the database does not keep up.

---

## Bottleneck: Slot Bookkeeping

A minor detail that matters at extreme scale: after each task completes,
the executor pool runs `self._available_slots.sort()` (line 82 of
`executor.py`). With `max_executors: 80`, this sorts a list of up to
80 integers on every task completion. At 400 completions/sec, that's
400 sorts/sec -- still fast (80-element sort is ~1us), but it's
there.

---

## Backpressure

Backpressure keeps a worker's memory bounded when the executor pool cannot
keep up with the input. It works in two stages, each in its own unit:

1. **Window admission, in tasks and bytes.** A partition loop takes a new
   `arrange()` window only while the worker has room under both
   `executor.max_pending_tasks` and `executor.max_pending_bytes`.
2. **Kafka fetch, in messages.** The poll loop pauses all partitions when
   the messages waiting in partition queues reach
   `sources.kafka.max_queued_messages`, and resumes them at half of it.

The two stages chain: when the budget is full, loops stop taking windows,
the queues fill, and the fetch pauses by itself.

### Pending tasks and pending bytes

**Pending tasks** are the tasks created by `arrange()` or by an `on_error`
replacement and not yet settled: waiting for an executor slot, running, or
in `on_task_complete` and its sink delivery. A retry is the same task and
counts once.

**Pending bytes** are the stdin of those tasks, plus the stdout and stderr of
results kept for `on_message_complete` and `on_window_complete`. Results are
kept only when the handler overrides one of those hooks, and they count from
the moment a task settles until the hook has run. A result kept by both hooks
counts twice; that overestimates and pauses earlier, which is safe. Sizes
come from `sys.getsizeof`, the real memory of a Python string.

One budget serves the whole worker: every partition loop and the HTTP
source charge it. HTTP requests count in it, but never wait for it — a
budget full of Kafka work must not refuse HTTP requests.

The `drakkar_pending_tasks` and `drakkar_pending_bytes` gauges show the
actual numbers, not the limits.

### Reservation, and how far a limit can be passed

The framework sees a window's tasks only after `arrange()` has returned
them. So when a loop takes a window, it **reserves** the window's estimated
tasks and bytes at once, before `arrange()` runs: the number of messages
times the moving averages per message. The reservation counts against the
limits, so other loops see the room as taken while the window is arranged.
As the window fans out, its tasks are charged out of the reservation; what
is left is given back when the fan-out ends (also when `arrange()` raises or
returns no tasks).

A limit is passed only by the error of the estimate:

- With a steady fan-out the overshoot is about one message, for the whole
  worker — not one window per partition.
- With a fan-out that jumps — one message of 100 000 tasks after messages of
  10 — the overshoot is that one message. The framework cannot bound one
  message: `arrange()` builds its list before the framework sees it.
- Before the worker has measured any window there is no estimate, so only one
  window with no estimate is arranged at a time; it takes one message.

The reservation is not shown by the gauges: `drakkar_pending_tasks` and
`drakkar_pending_bytes` show the tasks and bytes that exist.

### Window size adapts to the room

Each partition keeps moving averages (weight 0.3 for the newest sample) of
what one message costs: tasks, stdin bytes, and the output bytes the message
and window hooks keep. The worker keeps the same averages over all
partitions. The next window takes

```
n = clamp(min(free_tasks / avg_tasks, free_bytes / avg_bytes), 1, window_size)
```

messages, where `avg_bytes` is stdin plus kept output per message and the
free room excludes what other windows reserved. A partition's first window
uses the worker's averages; the worker's first window takes one message.
`drakkar_window_messages` shows the window sizes that were taken.

A loop that has no room waits in a first-in, first-out list; a loop that
arrives while others wait queues behind them even if there is room. A
release that makes room wakes the oldest waiter, which checks the room again
and queues again at the front if another loop took it. The wait is cut every
second, so the loop still retries an outstanding commit and sees a stop. The time spent waiting
goes to `drakkar_window_admission_wait_seconds_total{limit}` (`tasks` or
`bytes`, the limit that was reached; `tasks` when both were).

### Memory per task

Measured with `tracemalloc` on Python 3.13:

| Object | Bytes |
|---|---|
| `ExecutorTask` without stdin (4 args, 1 label, 1 metadata key) | about 1 900 |
| stdin of 7 000 lines of 100 bytes | 700 041 |
| The task's coroutine chain and its slot in the priority gate | about 2 500 |
| `ExecutorResult` with 2 000 bytes of stdout | about 1 100 plus the stdout |

So a task costs about 4-5 KB without its stdin, and stdin is often 100 times
more. That is why the budget counts bytes as well as tasks. While a task
runs, its stdin exists up to three times (the string, the encoded bytes, and
the part the pipe still buffers): about 2 MB at peak for a 700 KB stdin.

When a task reaches its last outcome — after `on_task_complete` and its
delivery, or after `on_error` with no retry — the framework sets
`task.stdin` to `None`. A message or window hook that keeps the task no
longer keeps its stdin. `arrange()`, `on_error`, `on_task_complete` and the
retry run still see the stdin; `on_message_complete`, `on_window_complete`
and `on_http_request_complete` see `None`.

### Sizing

| Key | Default | Sizing |
|---|---|---|
| `executor.max_pending_tasks` | `2000` | At least `max_executors`. 2000 waiting tasks keep 64 slots busy for about 31 rounds, 256 slots for about 8. |
| `executor.max_pending_bytes` | `1GiB` | About a quarter of the container memory. With 700 KB of stdin per task, 1 GiB stops admission at about 1 500 tasks, so the byte limit is the one that binds. |
| `sources.kafka.max_queued_messages` | `1000` | At least `max_poll_records`. Polled messages cost only their Kafka payload; this bounds how many wait for a window. |

A worker with the defaults holds at most about 1 GiB of stdin and kept
output plus one message's worth (with 1000 tasks of 700 KB per message, one
message is about 700 MB, so the peak is about 1.7 GB), plus about 2 MB per
running task for the copies a running task makes. Size the container for
that peak, not for the limit alone.

### The Kafka pause

On every poll loop turn the framework counts the messages waiting in the
queues of live partitions. When that count reaches `max_queued_messages`, it
pauses all assigned partitions (no new fetches) and sets
`drakkar_backpressure_active` to **1**. When the count falls to half of it,
it resumes them and sets the gauge back to **0**. The poll loop keeps calling
the consumer while paused, so Kafka's `max.poll.interval` never expires.

- A **stalled** partition (`dlq.on_send_failure: stall`) and a **dead**
  partition (its loop died twice) stay paused through every resume. A dead
  partition's queue is not counted: nothing empties it.
- An operator [debug pause](consume-pause.md) blocks the resume while it is
  on.

### Stop and revoke

At stop or revoke, polled messages that are not in a window are dropped, not
arranged. Their offsets were never registered, so the final commit stays
below them and the next owner of the partition reads them again. Only
in-flight work drains. `drakkar_queued_messages_dropped_at_stop_total`
counts them.

If `drakkar_backpressure_active == 1` is persistent, either increase
`max_executors` or add horizontal workers. If
`drakkar_window_admission_wait_seconds_total` grows while executor slots are
idle, the pending limits are too small for the pool.

---

## Configuration Recommendations

### Fast tasks (< 50ms), high core count

```yaml
executor:
  max_executors: 16              # the ~400 tasks/sec loop ceiling binds before core count
  window_size: 200             # larger windows for batching opportunity
  task_timeout: 30s            # short timeout for fast tasks

sources:
  kafka:
    max_poll_records: 500        # pull more messages per poll cycle

logging:
  min_task_duration: 100ms     # don't log fast tasks
ui:
  ws_min_duration: 100ms       # hide fast tasks from live UI
recorder:
  capture:
    min_event_duration: 50ms   # don't store fast tasks in DB
    min_output_duration: 100ms   # don't store stdout for fast tasks
    output: false                # skip stdout/stderr storage entirely
```

| Setting | Rationale |
|---------|-----------|
| `max_executors: 16` | With fast tasks, the event-loop ceiling (~400 tasks/sec, see [Bottleneck: Event Loop](#bottleneck-event-loop)) binds before core count: ~400 x average task duration is all the slots one worker can drain. At p80=40ms that is ~16 slots — more would complete tasks faster than the loop can orchestrate. On a big machine, run several workers per host instead of one worker with a larger pool. |
| `window_size: 200` | With fast tasks, larger windows let `arrange()` batch more messages per subprocess. Even without batching, larger windows reduce `arrange()` call frequency. |
| `max_poll_records: 500` | Pull more messages per Kafka poll to keep partition queues full. Reduces poll overhead per message. |
| `ws_min_duration: 100ms` | Fast tasks flood the WebSocket and live UI. Hiding them reduces event loop work and browser rendering cost. |
| `recorder.capture.min_event_duration: 50ms` | Skip SQLite writes for fast tasks. At high throughput, SQLite INSERT rate becomes a bottleneck. |
| `recorder.capture.output: false` | Stdout/stderr for fast tasks is usually small and uninteresting. Skipping it eliminates JSON encoding and storage. |

### Slow tasks (> 1s), moderate core count

```yaml
executor:
  max_executors: 8
  window_size: 20
  task_timeout: 5m

sources:
  kafka:
    max_poll_records: 100

ui:
  ws_min_duration: 0           # show all tasks in live UI
recorder:
  capture:
    min_event_duration: 0      # store everything
    output: true               # stdout/stderr is valuable for debugging
```

For slow tasks, the overhead analysis inverts: process launch is
negligible (3ms vs 5000ms task), the event loop is idle most of the
time, and event recording has no throughput impact. Optimize for
observability.

### Long-running tasks (~10s), low core count

```yaml
executor:
  max_executors: 16
  window_size: 5               # small windows -- few tasks in flight at once
  task_timeout: 2m             # generous timeout, but not infinite
  max_retries: 2               # retry on transient failures
  drain_timeout: 30s           # allow time for in-flight tasks on shutdown

sources:
  kafka:
    max_poll_records: 20         # don't over-fetch -- each message = 10s of work
    session_timeout: 1m          # longer session timeout to avoid rebalance during slow processing
    max_poll_interval: 10m       # 10 min between polls -- tasks may run long

logging:
  min_task_duration: 0         # log every task
ui:
  ws_min_duration: 0           # show everything in live UI
recorder:
  capture:
    min_event_duration: 0      # store all events
    output: true               # stdout/stderr is critical for 10s tasks
```

| Setting | Rationale |
|---------|-----------|
| `max_executors: 16` | With 10s tasks, 16 workers means ~1.6 tasks/sec throughput. Adding more workers only helps if you have enough CPU cores. |
| `window_size: 5` | Small windows avoid buffering too many 10s tasks. Each window takes ~10s to complete, so commit latency = 10s per window. |
| `max_poll_records: 20` | Don't fetch 500 messages when each takes 10s to process -- that's 83 minutes of work per poll. |
| `sources.kafka.session_timeout: 1m` | Default 45s may trigger rebalance if the event loop is busy with task completions. 60s gives more headroom. |
| `sources.kafka.max_poll_interval: 10m` | Kafka kicks a consumer out if it doesn't poll within this interval. With 10s tasks and window_size 5, a full window takes ~50s. 10 minutes provides ample margin. |
| `drain_timeout: 30s` | On shutdown, wait up to 30s for 10s tasks to finish rather than killing them. |

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

Key [Prometheus](observability.md#prometheus-metrics) queries for identifying bottlenecks (use the [Config Wizard](calculator.md) for initial tuning values):

```promql
# Messages consumed per second (are we keeping up?)
rate(drakkar_messages_consumed_total[1m])

# Task completion rate
rate(drakkar_executor_tasks_total{status="completed"}[1m])

# Backpressure (1 = consumer paused, 0 = flowing)
drakkar_backpressure_active

# Pending tasks and bytes against executor.max_pending_tasks / max_pending_bytes
drakkar_pending_tasks
drakkar_pending_bytes

# Seconds partition loops waited for room, by the limit that was reached
rate(drakkar_window_admission_wait_seconds_total[5m])

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
