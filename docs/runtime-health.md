# Runtime Health

Drakkar's pipeline lives or dies by its event loop: one synchronous call
that blocks for a second freezes every partition, every sink, and every
probe at once. The runtime health monitor watches for exactly that — and
when it happens, tells you **what** blocked the loop, not just that
something did.

It answers three questions:

1. **How healthy is the loop right now?** — continuous lag measurement,
   a state badge (`healthy` / `degraded` / `stalled`), Prometheus
   metrics, and a lag sparkline on the debug UI.
2. **What blocked it?** — during a stall, a sampler thread captures the
   stack trace of the code the loop thread is stuck in. The stacks land
   in the flight recorder, so post-mortems work days later.
3. **What is the loop carrying?** — an on-demand census groups live
   asyncio tasks by coroutine and suspension point: `37 × MyHandler.arrange
   — suspended at handler.py:412`.

## How it works

Two cooperating parts, because a blocked event loop cannot observe
itself — only a thread can:

- A **heartbeat task** sleeps `tick_seconds` (default 250 ms) and
  measures how late it wakes. That lateness *is* the event-loop lag —
  the time between "this coroutine became runnable" and "it actually
  ran", which is what every other coroutine in the worker experiences
  too. Each tick costs one clock read, one comparison, and one
  ring-buffer write.
- A **sampler thread** wakes at the same interval and compares the
  heartbeat's age against `stall_seconds` (default 1 s). While the
  heartbeat is silent — the loop is stuck *right now* — it captures the
  loop thread's current stack via `sys._current_frames()`. Repeated
  samples of the same location collapse into one entry with a count;
  at most `max_stall_stacks` distinct sites are kept per stall.

When the loop resumes, the heartbeat task packages the captured stacks
into one `runtime_stall` flight-recorder event and the stall appears on
the debug UI with its stacks expandable.

!!! note "Overhead when healthy"
    Nothing beyond the two ticks: no task scans, no stack walks, no
    database writes except one small `runtime_health` sample event every
    `sample_interval_seconds` (default 10 s). Stack capture and the task
    census only run during a stall or on an explicit request.

## States

| State | Meaning | Trigger |
|---|---|---|
| `healthy` | lag below `warn_lag_seconds` | — |
| `degraded` | the loop runs but late | lag ≥ `warn_lag_seconds` (default 100 ms) |
| `stalled` | the loop is not running | heartbeat silent ≥ `stall_seconds` (default 1 s) |

Recovery back to `healthy` needs several consecutive clean ticks
(hysteresis), so a loop hovering around the threshold emits one
transition, not dozens. Every transition is logged, recorded as a
`runtime_health` event, and reflected in the
`drakkar_runtime_health_state` gauge.

## What you see

**Debug UI → Live → Runtime tab**: state badge, current lag, a lag
sparkline (in-memory ring buffer, `history_window_seconds` long), the
recent-stall list with expandable stack traces, and a *Sample now*
button that runs the task census.

**Prometheus**:

| Metric | Type | Meaning |
|---|---|---|
| `drakkar_loop_lag_seconds` | histogram | lag per heartbeat tick |
| `drakkar_runtime_health_state` | gauge | 0 = healthy, 1 = degraded, 2 = stalled |
| `drakkar_runtime_stalls_total` | counter | stalls detected |

**Flight recorder**: `runtime_health` events (state transitions +
periodic samples) and `runtime_stall` events (duration, captured stacks,
task count) — with the same retention as every other event, so history
survives restarts.

**API**:

- `GET /api/v1/runtime/health` — snapshot + lag window. Served from
  monitor memory without touching the event loop, so it answers even
  *during* a stall.
- `GET /api/v1/debug/runtime/units` — the task census. It must run on
  the loop; a 503 means the dispatch timed out because the loop is not
  serving coroutines — which is itself a diagnosis.

## Configuration

```yaml
runtime_health:
  enabled: true
  tick_seconds: 0.25               # heartbeat + sampler interval
  warn_lag_seconds: 0.1            # degraded threshold
  stall_seconds: 1.0               # stalled threshold + stack capture trigger
  max_stall_stacks: 10             # distinct stacks kept per stall
  sample_interval_seconds: 10.0    # recorder history sample cadence
  history_window_seconds: 900      # in-memory sparkline window
```

Every field is environment-overridable (`DK_RUNTIME_HEALTH__*`); see the
[config reference](config-reference.md#runtime-health-runtime-health).

## Interpreting what you find

- **Stall stacks pointing at handler code** — an `arrange()` or hook
  doing synchronous I/O or heavy CPU work on the loop. Wrap the
  computation in [`self.offload(...)`](offload.md) — the framework's
  thread pool for exactly this case — or make it a subprocess task.
- **Stall stacks pointing at a library** — a client library with a
  blocking call path (DNS resolution, synchronous connect, compression).
  The stack names the exact call to wrap or replace.
- **Degraded without stalls** — many small blocks rather than one big
  one. The `drakkar_loop_lag_seconds` histogram shows how bad the tail
  is; lowering `stall_seconds` narrows the attribution blind spot at the
  cost of more sampling during rough patches.
- **A census full of one coroutine** — a fan-out that outran its
  backpressure. The suspension point shows what everything is waiting
  on.

## Limits

- A block shorter than `stall_seconds` shows up in the lag histogram
  but carries no stack — the sampler never saw it in the act.
- The census counts and locates coroutines; it does not measure their
  CPU time. For time-based profiling use a sampling profiler.

## Cross-backend note

The wire contract is backend-neutral: `unit_label` says what
`unit_count` counts (`tasks` here, `goroutines` on the Go backend), and
lag maps to Go's scheduler latency. The Go backend does not implement
the monitor yet; it accepts the `runtime_health:` config block so mixed
fleets share one config shape.

## See also

- [Offload](offload.md) — the remedy for stalls caused by CPU-bound hook code
- [Observability](observability.md) — metrics, flight recorder, logs
- [Config reference](config-reference.md#runtime-health-runtime-health)
