# Threads & Pools

A Drakkar worker mixes four execution mechanisms — one event loop, a
subprocess pool, and two thread pools — plus a handful of service threads.
Each exists for a different kind of work, each has its own sizing rules,
and confusing them is the root of most "why is this slow" questions. This
page is the map.

## The map

| Mechanism | Runs | Size / knob | GIL | Parallel? |
|---|---|---|---|---|
| **Event loop** (main thread) | all `async` framework + handler code: arrange, hooks, sinks, recorder, scheduling | exactly 1 — not configurable | held while running Python | no — cooperative |
| **Executor subprocesses** | the tasks themselves (your binary) | `executor.max_executors` | separate processes: none | **yes** — real parallelism |
| **Blocking-I/O pool** — asyncio's default `to_thread` executor | every `asyncio.to_thread(...)` call: handler blocking I/O, plus the framework's archive passes and database-stats scans | `io.max_threads` (0 = Python's `min(32, cpu_count + 4)`) | released during blocking I/O | yes, for I/O waits |
| **Offload pool** | `handler.offload(...)` — CPU-bound Python in hooks | `offload.max_threads` (0 = auto: `ceil(max_executors / 4)`, min 2) | held for pure Python; released only by numpy-style extensions | mostly no (GIL) |

Plus service threads you never size: the runtime-health sampler (it must
observe the loop from outside — [Runtime Health](runtime-health.md)), the
debug UI server and webapp server (each on its own thread with its own
loop, reaching main-loop state only through bounded dispatches), the
Prometheus exporter, and one worker thread per aiosqlite connection.

## Choosing the mechanism

**The task's own work → a subprocess task.** This is the framework's
whole design: heavy per-unit work becomes an `ExecutorTask` and runs as a
process — true parallelism, isolation, timeouts, retries, per-task
observability. When in doubt, make it a task.

**Blocking I/O inside a hook → `asyncio.to_thread`.** Reading a file to
decide how to arrange, statting inputs for labels, any call that blocks on
a syscall. The thread just waits; the GIL is released; the loop stays
responsive. Do **not** do this inline in the hook — a single synchronous
read on a slow mount freezes every partition at once (that is exactly what
[Runtime Health](runtime-health.md) stall stacks catch).

**CPU-bound Python inside a hook → `handler.offload(...)`.** A heavy
pure-Python computation also freezes the loop, but `to_thread` is the
wrong home for it: it would occupy an I/O slot while holding the GIL. The
offload pool exists so this work is bounded, measured
(`drakkar_offload_*`), and recorded — see [Offload](offload.md). It makes
the loop responsive; it does not make the computation faster.

**Never block the event loop itself.** The loop is one thread shared by
every partition, sink, and the recorder. The runtime-health monitor tells
you when this rule was broken and by which call site.

## The blocking-I/O pool (`io.max_threads`)

Every `asyncio.to_thread` call in the process shares **one** pool:
asyncio's default executor. Python sizes it `min(32, cpu_count + 4)` —
which quietly means a 32-thread ceiling on any host with 28+ CPUs. If a
handler fans out blocking filesystem reads, that ceiling caps I/O
concurrency no matter how many cores the host has; and when the storage
behind those reads degrades, each blocked call holds a thread for its full
wall time, the pool saturates, and later calls queue invisibly. The unit
census on the Runtime tab shows the symptom: a pile of tasks suspended in
`to_thread`.

```yaml
io:
  max_threads: 0   # 0 = Python's default; set e.g. 128 to lift the cap
```

Sizing considerations:

- Blocking I/O **releases the GIL**, so large values are legitimate here —
  the threads sleep in syscalls. The costs are memory (~ stack per
  thread) and, more importantly, **pressure on the storage behind the
  calls**: 128 concurrent reads against an already-struggling NFS server
  make the server's queue worse, not better. Raising this knob fixes
  "the pool is my bottleneck on a healthy day"; it does nothing for "the
  storage is the bottleneck" — check [Host Pressure](host-pressure.md)'s
  per-mount RTT before reaching for it.
- The pool is shared with the framework's own background file work
  (archive compaction, database-stats scans). Those are occasional and
  short; handler fan-out dominates in practice.
- A reasonable starting point for I/O-heavy handlers on big hosts is a
  small multiple of `executor.max_executors` — the executor pool already
  expresses how much concurrent work the deployment intends.

## The offload pool (`offload.max_threads`)

The deliberate opposite: small, dedicated, for work that *holds* the GIL.
More threads do not make pure-Python computation faster — they time-slice
it — so the pool's job is bounding and measuring, with queue depth
(`drakkar_offload_queued`) as the tuning signal. By default it scales
gently with the executor pool: `ceil(executor.max_executors / 4)`, minimum
2\. Full story: [Offload](offload.md).

## Reading problems back from the observability stack

| Symptom | Likely mechanism | Where to look |
|---|---|---|
| `stalled` badge, stall stack names a handler line | blocking call ran **on the loop** | [Runtime Health](runtime-health.md) — move it to `to_thread` / `offload` |
| lag episode verdict `cpu_bound` | CPU-bound Python on the loop | episode stacks → `offload()` it |
| census shows hundreds suspended in `to_thread` | blocking-I/O pool saturated | `io.max_threads`, and [Host Pressure](host-pressure.md) RTT for the storage side |
| `drakkar_offload_queued` sustained > 0 | offload pool saturated | raise `offload.max_threads` only if the work releases the GIL or partitions genuinely overlap |
| tasks slow, loop healthy | the subprocesses themselves | timeline + [Throughput](throughput.md) speed, host pressure |

## See also

- [Executor](executor.md) — the subprocess pool
- [Offload](offload.md) · [Runtime Health](runtime-health.md) ·
  [Host Pressure](host-pressure.md) · [Config Reference](config-reference.md)
