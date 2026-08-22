# Runtime Health

Drakkar's pipeline lives or dies by its event loop: one synchronous call
that blocks for a second freezes every partition, every sink, and every
probe at once. The runtime health monitor watches for exactly that — and
when it happens, tells you **what** blocked the loop, not just that
something did.

It answers four questions:

1. **How healthy is the loop right now?** — continuous lag measurement,
   a state badge (`healthy` / `degraded` / `stalled`), Prometheus
   metrics, and a lag sparkline on the debug UI.
2. **What blocked it?** — during a stall, a sampler thread captures the
   stack trace of the code the loop thread is stuck in. The stacks land
   in the flight recorder, so post-mortems work days later.
3. **Was it blocked at all — or starved?** — every degraded/stalled span
   becomes a [lag episode](#lag-episodes-and-verdicts) with stacks
   aggregated across the whole span and a verdict: `blocked`,
   `cpu_bound`, `starved`, or `inconclusive`. This covers the case a
   single-stall view cannot: diffuse slowness with no one culprit call.
4. **What is the loop carrying?** — an on-demand census groups live
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

## Lag episodes and verdicts

A single hard block produces a clean `runtime_stall` event with the
guilty stack. But the second failure shape — the one that used to render
as a `stalled` badge over "No stalls recorded" — is *diffuse*: thousands
of small delays, no single blocking call, usually because the **host**
is the problem (CPU starvation, cgroup throttling, a struggling NFS
server slowing every syscall).

An **episode** covers that shape. It opens when the monitor leaves
`healthy` and closes when it returns (or after `episode_max_seconds`,
default 300 — a longer incident flushes and continues in a fresh
episode, so evidence exists even if the process dies mid-incident).
While an episode is open:

- The sampler thread captures the loop thread's stack on **every**
  wakeup, not only while the heartbeat is stale, and aggregates the
  samples with the usual dedup-and-count.
- The monitor tracks the loop *thread's* CPU time
  (`/proc/self/task/<tid>/stat`) next to wall time — thread-level, so
  offload and recorder threads cannot pollute the ratio.

On close, one `runtime_lag_episode` event records duration, peak and
accumulated lag, the stall count, the aggregated stacks, CPU numbers,
best-effort host evidence (cgroup throttling during the episode, CPU
PSI, load) — and the **verdict**:

| Verdict | Signature | Reading |
|---|---|---|
| `blocked` | little loop CPU, one non-idle call site dominates the samples | a blocking call; the top stack names it |
| `cpu_bound` | loop CPU ≈ wall time | the loop itself computed through the lag; the top stack is the hot site — [offload](offload.md) it |
| `starved` | little loop CPU, no dominant site (throttle/PSI evidence strengthens it) | the process wanted CPU and did not get it — host-level contention, not your code; see [Host Pressure](host-pressure.md) |
| `inconclusive` | none of the above dominates | mixed or short episode; look at the raw numbers |

The `/runtime/health` snapshot carries `current_episode` (with a running
verdict) and `recent_episodes` **from monitor memory**, so the Runtime
tab shows the diagnosis *during* the incident even when the recorder's
database path is degraded.

## Stack probes (opt-in profiler)

Set `runtime_health.probe_interval_seconds` above 0 and the sampler
thread records a `runtime_probe` event with the loop thread's stack every
interval, healthy or not — a low-rate flight-recorder profiler. Useful to
answer "where does the loop actually spend its time" while tuning a
production workload, and to have baseline stacks on disk from *before* an
incident. Off by default (`0`): it writes events for as long as the
worker runs.

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
periodic samples), `runtime_stall` events (duration, captured stacks,
task count), `runtime_lag_episode` events (span, verdict, aggregated
stacks, CPU + host evidence), and opt-in `runtime_probe` events — with
the same retention as every other event, so history survives restarts.
The periodic `worker_state` snapshot also carries `health_state` and
`loop_lag_ms` columns, so a merged fleet database can answer "which
worker was degraded when" with one query.

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
  episode_max_seconds: 300.0       # flush cap for one lag episode
  probe_interval_seconds: 0.0      # opt-in stack probes; 0 = off
```

Every field is environment-overridable (`DK_RUNTIME_HEALTH__*`); see the
[config reference](config-reference.md#runtime-health-runtime_health).

## Interpreting what you find

- **Stall stacks pointing at handler code** — an `arrange()` or hook
  doing synchronous I/O or heavy CPU work on the loop. Wrap the
  computation in [`self.offload(...)`](offload.md) — the framework's
  thread pool for exactly this case — or make it a subprocess task.
- **Stall stacks pointing at a library** — a client library with a
  blocking call path (DNS resolution, synchronous connect, compression).
  The stack names the exact call to wrap or replace.
- **Degraded without stalls** — many small blocks rather than one big
  one. This is exactly what episodes exist for: check the episode's
  verdict. `starved` (with throttle/PSI evidence) means the host, not
  the code; `blocked`/`cpu_bound` name the site. The
  `drakkar_loop_lag_seconds` histogram shows how bad the tail is.
- **A census full of one coroutine** — a fan-out that outran its
  backpressure. The suspension point shows what everything is waiting
  on.

## Limits

- A block shorter than `stall_seconds`, outside any episode, shows up in
  the lag histogram but carries no stack — the sampler never saw it in
  the act. (Inside an episode, every sampler wakeup captures.)
- The census counts and locates coroutines; it does not measure their
  CPU time. For continuous time attribution, enable
  [stack probes](#stack-probes-opt-in-profiler) or use a sampling
  profiler.
- Episode CPU attribution needs `/proc/self/task/<tid>/stat`; where it
  is unreadable the verdict falls back to stack dominance and host
  evidence alone.

## Cross-backend note

The wire contract is backend-neutral: `unit_label` says what
`unit_count` counts (`tasks` here, `goroutines` on the Go backend). The
Go backend implements the same monitor with Go-shaped introspection:
heartbeat lag measures scheduler latency, episode stacks are aggregated
goroutine-dump groups, and episode CPU comes from process rusage (Go has
no single loop thread — a blocked goroutine does not stall the others,
so the verdicts that matter there are `starved` and `cpu_bound`).

## See also

- [Offload](offload.md) — the remedy for stalls caused by CPU-bound hook code
- [Observability](observability.md) — metrics, flight recorder, logs
- [Config reference](config-reference.md#runtime-health-runtime_health)
