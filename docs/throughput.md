# Task Cost, Speed & Throughput

Duration alone says how long a task took — not how much it *did*. When
tasks vary widely in size, "the pipeline is slower today" is
indistinguishable from "today's tasks are bigger" until you normalize by
some measure of the work itself. This feature adds that measure as a
first-class, opt-in concept:

- **cost** — a number you attach to each task that correlates with its
  computational hardness. Any unit: bytes to parse, records to process, or
  a computed score of your own.
- **speed** — per task: `cost / duration`, in cost-units per second.
- **throughput** — per worker: the sum of the cost of tasks completed in
  the last N seconds divided by N, maintained for three sliding windows
  (1, 5, 30 s) side by side, with the matching **task rate**
  (tasks per second).

Everything is observational — nothing schedules, prioritizes, or throttles
on cost.

## Enabling it

Cost rides an ordinary task **label**. In `arrange()`, label each task
with a numeric value:

```python
tasks.append(ExecutorTask(
    task_id=make_task_id(...),
    args=[...],
    labels={'input_bytes': str(size)},   # labels are strings; numbers are parsed
))
```

Then name that label in backend config:

```yaml
throughput:
  cost_label: input_bytes   # '' (the default) = feature entirely off
  min_cost: 50000           # optional floor, see below
```

`cost_label` is a label *role*, exactly like the roles in
`ui.timeline.labels` and the keys in `metrics.task_label_histograms` —
one label can feed all three. The section lives in backend config (not
`ui.`) because the backend consumes it: Prometheus, the flight recorder,
and worker_state snapshots all report cost-derived numbers whether or not
a browser is open.

### Choosing a cost formula

Anything that correlates with how long the work *should* take. For file
processing, plain byte size is often enough. When processing time depends
on more than input size, compute a score in `arrange()` — for example an
input's size multiplied by a factor for how many patterns are applied to
it and their complexity — and label that. The unit is yours; the
framework never interprets it, only divides it by seconds. Pick one
formula and keep it stable, or your history stops being comparable.

### The minimum cost

`min_cost` excludes tasks whose fixed overhead (process spawn, small-file
reads) dwarfs the work itself — their speeds are dominated by that
overhead and drag every aggregate toward noise. Excluded tasks carry no
speed at all and enter no window: below the floor, "cost per second" is
not a meaningful number, so the framework refuses to fabricate one.

## Counting rules

A task is counted if and only if **all** of these hold:

| Rule | Why |
|---|---|
| `throughput.cost_label` is configured | the feature is opt-in |
| the task completed successfully | failed work is not throughput |
| it is not a precomputed fast-track task | no subprocess ran; a near-zero duration would fabricate an absurd speed |
| the label value parses as a finite number | tolerant like `metrics.task_label_histograms` |
| `cost >= min_cost` | the overhead floor above |
| `duration > 0` | division |

Excluded tasks carry no `cost`/`speed` keys anywhere — absent, never
zeroed.

## Where the numbers appear

**Per task** — counted completions gain `cost` and `speed` in their
`task_completed` event metadata (recorded, traced, streamed) and on
`GET /api/v1/recent-tasks` rows. The debug UI shows speed in the
timeline's task hover.

**Live, per second** — a broadcast-only `throughput` WebSocket frame
(never persisted) carries all three windows every second:

```json
{"event": "throughput", "metadata": {"windows": {
  "1":   {"throughput": 41250000.0, "task_rate": 9.0, "tasks": 9},
  "5":   {"throughput": 38700000.0, "task_rate": 8.4, "tasks": 42},
  "30":  {"...": "..."}
}}}
```

Quiet windows report zeros, so an idle or stalled worker draws as a real
dip on the UI's throughput track rather than a gap. The Live page renders
the track under the timeline, sharing its time axis, drawing the 5 s
window by default; the window chips (`1s` / `5s` / `30s`) and the current
reading sit in the timeline's gear popover, and switching is instant
because every frame carries all three windows.

**Prometheus**:

| Metric | Type | Meaning |
|---|---|---|
| `drakkar_task_speed` | histogram | per-task speed of counted completions |
| `drakkar_throughput{window="1|5|30"}` | gauge | windowed throughput, refreshed each second |
| `drakkar_task_rate{window=...}` | gauge | windowed completion rate (counted tasks only) |

Example queries: `drakkar_throughput{window="30"}` for the smoothed live
rate; `histogram_quantile(0.5, rate(drakkar_task_speed_bucket[10m]))` for
the median per-task speed over ten minutes — a falling median at constant
cost mix is the "same work, slower host" signal.

**worker_state** — each state-sync tick (default 10 s) snapshots the
three-window object as JSON into the new nullable `throughput` column.
Because worker_state rows are an append-only time series, a rotated,
archived, or merged fleet database replays throughput history with one
query — no event replay needed. NULL means the feature was off.

## Reading it during an incident

The pairing that motivated this feature: when every task slows down at
once, throughput falls while task **cost mix is unchanged** — and the
per-task speed distribution shifts down uniformly. Cross-reference the
[Runtime Health](runtime-health.md) episode verdict and the
[Host Pressure](host-pressure.md) samples from the same moment: a
`starved` episode with rising NFS RTT and a uniformly collapsed speed is
host contention, not your handler.

## Worked example

The integration demo's main worker labels every scan target with its
exact byte size and enables the feature with a 50 KB floor
(`integration/worker/drakkar.yaml`), so its throughput track reads as
"bytes scanned per second" and small files stay out of the aggregates.

## See also

- [Observability](observability.md) — the full event and metrics catalogue
- [Debugging Bottlenecks](debugging-bottlenecks.md)
- [Config reference](config-reference.md#throughput-throughput)
