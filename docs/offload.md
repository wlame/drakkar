# Offloading CPU-bound Hook Work

Every handler hook runs on the worker's single asyncio event loop. That
loop also drives Kafka polling, the executor pool, sink flushes, offset
commits, the cache engine, and the debug UI's data feeds. A hook that
spends seconds in pure-Python computation — deeply nested loops deriving
task parameters in `arrange()`, result crunching in
`on_message_complete()` — freezes **all** of it for that long. On real
workloads an arrange step of 5–25 seconds is not unusual, and during
those seconds the worker looks dead: no polls, no task completions, no
sink deliveries, a stalled UI.

The [runtime health monitor](runtime-health.md) *detects* that state
(`runtime_stall` events with the blocking stack captured). This page is
the *remedy*.

## Quick start

Move the heavy computation into a plain synchronous function and await
`self.offload(...)` instead of calling it inline:

```python
class MyHandler(BaseDrakkarHandler[MyInput, MyOutput]):
    async def arrange(self, messages, pending):
        plan = await self.offload(self._build_task_plan, messages)
        return [ExecutorTask(...) for item in plan]

    def _build_task_plan(self, messages):
        # Plain sync function — the nested loops that used to stall the
        # loop live here now. Runs on the offload thread pool.
        plan = []
        for msg in messages:
            for candidate in msg.payload.candidates:
                ...
        return plan
```

`offload()` works in **any** hook — `arrange`, `on_task_complete`,
`on_message_complete`, `on_window_complete`, `on_error`, the webapp
hooks — and also outside them (`on_ready`, `@periodic` methods). The
function's return value comes back as the await result; exceptions
propagate unchanged.

## What it does — and does not — buy you

`offload()` runs the function on a small dedicated thread pool
(`offload.max_threads`; by default sized automatically from the executor
pool — `ceil(executor.max_executors / 4)`, minimum 2). Be clear about the
physics:

- **It does NOT make the computation faster.** Under the GIL,
  pure-Python bytecode is serialized no matter which thread runs it.
  A 20-second crunch still takes ~20 seconds.
- **It keeps the worker alive while the crunch runs.** Instead of one
  20-second stall, the event loop sees millisecond-scale GIL handoffs:
  polling continues, completed tasks are collected, sinks flush, offsets
  commit, other partitions make progress, the UI stays live.
- If the offloaded code releases the GIL (numpy, compiled extensions),
  threads DO run in parallel — then `max_threads` becomes a genuine
  parallelism knob. The same is true on free-threaded Python builds.

The pool is deliberately **not** the subprocess executor pool. Executor
tasks are external commands with offsets, priorities, retries, and sink
flow; an offloaded function is an inline computation whose result the
hook needs before it can continue. Keeping them separate keeps the
executor's scheduler and stats untouched.

## Using the cache inside an offloaded function

The sync cache operations are thread-safe and fully usable from
offloaded code:

```python
async def arrange(self, messages, pending):
    # 1. Warm DB-backed keys on the loop, where async get() works.
    for key in self._keys_for(messages):
        await self.cache.get(key)          # memory-miss falls through to DB

    # 2. Crunch off-loop; peek() reads the warmed memory entries.
    plan = await self.offload(self._build_task_plan, messages)
    return [ExecutorTask(...) for item in plan]

def _build_task_plan(self, messages):
    for msg in messages:
        cached = self.cache.peek(self._key_of(msg))    # OK: thread-safe
        self.cache.set(self._key_of(msg), ...)         # OK: thread-safe
        ...
```

The rules:

| Operation | Inside `offload()` | Notes |
|---|---|---|
| `self.cache.peek(key)` | ✅ | memory-only read |
| `self.cache.set(key, v)` | ✅ | lands in the dirty map; flushed/synced as usual |
| `self.cache.delete(key)` | ✅ | |
| `key in self.cache` | ✅ | |
| `await self.cache.get(key)` | ❌ loop-only | it is a coroutine — a sync function cannot await it, and its SQLite fallback is bound to the main loop. Warm keys **before** offloading (step 1 above). |

`self.annotate(...)` also works inside offloaded functions and anchors
to the same hook invocation — the hook context is copied into the
thread.

Everything else framework-owned (sinks, recorder queries, `pending`
internals) is loop-bound: treat the offloaded function as pure
computation over the arguments you pass it, plus the cache surface
above.

## Cancellation and shutdown

Awaiting `offload()` is cancellable like any await: when a partition is
revoked or the worker shuts down mid-call, the awaiting hook receives
`CancelledError`. A **queued** computation is cancelled outright. A
**running** one cannot be interrupted — Python threads are not killable —
so the thread finishes the current function in the background and the
result is discarded. If a very long crunch should stop early, have it
check a flag your handler sets (a `threading.Event` works well).

In unit tests `offload()` needs no running app: the default
implementation runs the function via `asyncio.to_thread` with the same
semantics, minus the shared pool, metrics, and recorder events.

## Configuration

```yaml
offload:
  max_threads: 0        # 0 = auto: ceil(executor.max_executors / 4), min 2
                        # env: DK_OFFLOAD__MAX_THREADS
```

The default (`0` = auto) sizes the pool from the executor pool —
`ceil(executor.max_executors / 4)` with a minimum of 2, so pool 8 → 2 threads,
9 → 3, 13 → 4 — a bigger subprocess fleet gets proportionally more
offload headroom without tuning a second knob. The knob bounds how many
offloaded computations run at once before newer calls queue — it is a
queueing knob, not a speed knob (see the GIL note above). Its sibling
`io.max_threads` sizes the separate blocking-I/O pool behind
`asyncio.to_thread` — see [Threads & Pools](threads.md) for the full map.
Set an explicit value when
several partitions routinely offload at the same time *and* the
`drakkar_offload_queued` gauge shows sustained waiting, or when the
offloaded code releases the GIL.

## Observability

Prometheus:

| Metric | Meaning |
|---|---|
| `drakkar_offload_running` | computations executing right now |
| `drakkar_offload_queued` | calls waiting for a free pool thread — the sizing signal |
| `drakkar_offload_duration_seconds{hook}` | execution time per call (queue wait excluded) |

Flight recorder: one `offload` event per call, recorded at completion
and anchored like an [annotation](annotations.md) — to the message, the
task, or the whole window depending on the hook. Metadata carries the
hook, the function's qualified name, queue wait, and `ok` / `error` /
`cancelled` status. The rows appear in message traces and in the History
page's `offload` filter.

Debug UI: the Live page header shows `Offload: R / N busy, Q queued`
whenever the backend reports a pool.

And the payoff metric: `runtime_stall` events for your hook should
disappear. If [Runtime Health](runtime-health.md) still reports stalls
after offloading, the captured stack now points at whatever *else*
blocks the loop.

## When not to reach for it

- **Sub-100ms computations.** The offload round-trip costs a thread
  handoff (tens of microseconds) — negligible for seconds-scale work,
  pure overhead for trivial work. If the loop-lag monitor never
  complains about your hook, leave it inline.
- **I/O-bound work.** Waiting on HTTP, DBs, or files belongs in native
  `async` code on the loop, not on an offload thread.
- **Work that needs true CPU parallelism.** A thread pool cannot give
  pure-Python code more than one core. If you need that, it is
  subprocess-shaped work — consider making it an `ExecutorTask`.

## The Go backend

The Go backend has no `offload()` and needs none: the Go runtime
preempts goroutines (~10ms quanta) and schedules them across
`GOMAXPROCS` OS threads, so a CPU-heavy `Arrange` never stalls the rest
of the worker. Mixed fleets share one YAML — Go accepts and validates
the `offload:` block without acting on it — and the UI hides the offload
readout for Go workers automatically. See the Go repo's `docs/offload.md`
for the full story.

## Worked example

The integration worker uses `offload()` in production shape:
[`integration/worker/handler.py`](https://github.com/wlame/drakkar/blob/main/integration/worker/handler.py)
moves its entire window-planning pass (`_build_scan_plan`) onto the pool —
the nested `patterns × file_paths` bucketing, per-file stat/read
syscalls, and the memory-tier `cache.peek` probes all run in one offload
call, while the SQLite-tier `await cache.get()` fallback stays on the
loop. It also shows the batching win: one offload call replaces what was
previously a per-pair `asyncio.to_thread` hop.

## See also

- [Runtime Health](runtime-health.md) — how stalls are detected and attributed
- [Cache](cache.md) — the full cache contract, including thread-safety
- [Annotations](annotations.md) — the anchoring model offload events reuse
- [Debugging Bottlenecks](debugging-bottlenecks.md) — finding what is actually slow
