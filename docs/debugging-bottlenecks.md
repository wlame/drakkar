# Debugging Bottlenecks

A runbook for one specific situation: **a worker that periodically looks
stuck** — the live timeline shows synchronized gaps where nothing runs for
seconds, or every task bar has the same width regardless of its input.
Both symptoms mean some shared resource is saturated; the question is
which one. This page lists the signals the framework records and what
each combination of them means.

## The mental model

A task's wall time decomposes into phases, each with its own signal:

```
arrange ──► queue wait ──► spawn ──► run (feed stdin, compute, drain output) ──► hooks ──► commit
            queue_wait_ms   spawn_ms   duration minus the first two
```

- `queue_wait_ms` and `spawn_ms` ride in the task's event metadata and
  show in the timeline hover; their histograms are
  `drakkar_executor_queue_wait_seconds` and `drakkar_executor_spawn_seconds`.
- The gaps BETWEEN tasks belong to the pipeline, not the executor:
  arrange, completion hooks, sink delivery, commits — all visible as
  events in the flight recorder and on the Live page's Arrange /
  Task Results tabs.

## The decision table

Read the signals in this order; stop at the first row that matches.

| Signal | Where to look | Meaning |
|---|---|---|
| `executor_pool_exceeds_cpus` warning at startup | worker log, or compare `drakkar_executor_pool_max` vs `drakkar_host_effective_cpus` | The pool is bigger than the CPUs behind it. Concurrent tasks time-share cores; wall times stretch and converge. Shrink the pool or grow the machine. |
| Loop lag high / stalls recorded | Runtime tab: state badge, lag sparkline, **Top blocking sites** | The worker's own event loop is the bottleneck. The aggregated stall sites name the exact code that blocked it — that code is your fix target. When the site is your own hook's CPU work, wrap it in [`self.offload(...)`](offload.md). |
| `spawn_ms` grows toward the task duration | timeline hover, `drakkar_executor_spawn_seconds` p99 | The parent process cannot start subprocesses fast enough (fork/exec on a congested loop, or a starved CPU). Usually accompanies the row above. |
| `queue_wait_ms` long while pool is busy | timeline hover, `drakkar_executor_queue_wait_seconds` + `drakkar_executor_pool_active` | Healthy executor, undersized pool: tasks queue because every slot is taken. Raise the pool — but only up to the effective CPU count. |
| `queue_wait_ms` long while pool is idle | same, with `pool_active` low | Work exists but slots stay empty — the worker process is too slow to schedule it. Check the Runtime tab for stalls; this is loop congestion, not pool sizing. |
| Tasks fast, but gaps between batches | Arrange tab durations; the trace view's per-stage deltas (`/debug` → Trace) | The stall is between executor phases: slow arrange (often input staging — network filesystems), heavy completion hooks, or slow sink delivery. The trace deltas name the stage. |
| Everything above looks healthy | `drakkar_consumer_idle_seconds_total`, consumer lag on the Dashboard | The worker is genuinely idle — the bottleneck is upstream (no messages) or in Kafka itself. |

## Artifacts for postmortems

When the incident is over, the flight-recorder database still holds:

- Every task's timings, labels, `queue_wait_ms`, `spawn_ms`, exit code —
  and its stdout/stderr when the task ran at least
  `ui.recorder.output_min_duration_ms` (failures always store output).
- The task's **stdin** when `ui.recorder.store_stdin` is on (capped at
  `stdin_max_bytes`); failed tasks store stdin regardless of the flag.
  Copy it from the task detail page into the message probe to replay the
  exact input.
- Every runtime stall with its captured stack traces
  (`runtime_stall` events) — the Runtime tab aggregates them by blocking
  site; the raw rows survive rotation and archiving like everything else.
- A `resource_sample` row every `state_sync_interval_seconds`: RSS,
  thread count, open fds, CPU percent for the worker and its reaped
  subprocesses, and host network byte totals. Chart these against the
  task timeline to see *which* resource moved when the system froze —
  RSS climbing toward the container limit, fds leaking, children CPU
  saturating the cores, or the network counters flatlining.
- The final seconds before a crash: fatal exits that skip the clean
  shutdown path trigger a best-effort **last-breath flush**
  (`recorder_last_breath_flush` in the log says it fired), so the buffer
  tail is written instead of lost. SIGKILL/OOM cannot be intercepted —
  there the watchdog marker plus the last periodic flush are what you
  have.

When data is missing on a task detail page, the page says which retention
setting excluded it — absence of data and never-recorded data are
different findings.

## See also

- [Observability](observability.md) — every metric, with PromQL for
  percentiles.
- [Runtime Health](runtime-health.md) — how lag tracking and stall
  sampling work.
