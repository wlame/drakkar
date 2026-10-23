# Config Calculator

Interactive calculator that recommends Drakkar [configuration](configuration.md) based on your
hardware and workload characteristics. See [Performance Tuning](performance.md) for
detailed bottleneck analysis and tuning strategies.

!!! warning "Starting point, not a final answer"
    These are **estimated values** to use as a starting point for tuning.
    They are not calibrated against real production workloads. Every
    deployment is different — binary startup cost, message size, sink
    latency, and OS scheduling all affect optimal values. Use these
    recommendations as initial config, then iterate based on the
    [metrics to watch](#what-to-watch-after-deploying) section below.

<div id="calc" style="font-family: inherit;">

### Inputs

<div class="row">
  <label>CPU cores available</label>
  <input type="number" id="in-cpu" value="8" min="1" max="512">
  <span class="hint">total on machine</span>
</div>
<div class="row">
  <label>RAM available (GB)</label>
  <input type="number" id="in-ram" value="16" min="1" max="4096">
  <span class="hint">total on machine</span>
</div>
<div class="row">
  <label>Task duration p80 (ms)</label>
  <input type="number" id="in-p80" value="500" min="1" max="600000">
  <span class="hint">80th percentile</span>
</div>
<div class="row">
  <label>Sink count</label>
  <input type="number" id="in-sinks" value="2" min="1" max="10">
  <span class="hint">distinct sinks in on_task_complete()</span>
</div>
<div class="row">
  <label>Avg stdout size (KB)</label>
  <input type="number" id="in-stdout" value="2" min="0" max="10240">
  <span class="hint">per task output</span>
</div>
<div class="row">
  <label>Workers in cluster</label>
  <input type="number" id="in-workers" value="1" min="1" max="100">
  <span class="hint">horizontal instances</span>
</div>
<div class="row">
  <label>Kafka partitions</label>
  <input type="number" id="in-partitions" value="50" min="1" max="1000">
  <span class="hint">source topic</span>
</div>

<button onclick="drakkarCalc()">Calculate</button>

<div class="result" id="calc-result" style="display:none;">
  <pre id="calc-yaml"></pre>
</div>

<div class="explain" id="calc-explain" style="display:none;"></div>

</div>


---

## What to Watch After Deploying

After applying the recommended config, monitor these metrics to
understand how well the worker is performing and where to tune further.

### Is the worker keeping up?

| Metric | Good | Bad | Action |
|--------|------|-----|--------|
| `rate(drakkar_messages_consumed_total[5m])` | Stable or matches production rate | Dropping or zero | Check if consumer is paused (backpressure) or partitions were revoked |
| `drakkar_backpressure_active` | 0 most of the time | Stuck at 1 | Increase `max_executors` or add horizontal workers |
| `drakkar_total_queued` | Stable, not growing | Growing over time | Processing rate < production rate. Scale up or batch in `arrange()` |

### Is the executor pool sized correctly?

| Metric | Good | Bad | Action |
|--------|------|-----|--------|
| `drakkar_executor_pool_active` | 50-80% of `max_executors` on average | Pegged at `max_executors` constantly | Add more slots or more workers |
| `rate(drakkar_executor_idle_slot_seconds_total[5m])` | Near zero | High (slots idle while messages wait) | `arrange()` is the bottleneck — make it faster or reduce `window_size` |
| `rate(drakkar_consumer_idle_seconds_total[5m])` | Low when topic has data | High while topic has data | Worker is over-provisioned for this workload |

### Are tasks healthy?

| Metric | Good | Bad | Action |
|--------|------|-----|--------|
| `histogram_quantile(0.95, rate(drakkar_executor_duration_seconds_bucket[5m]))` | Close to expected p95 | Much higher than expected | Binary performance degraded, or resource contention |
| `rate(drakkar_executor_tasks_total{status="failed"}[5m])` | Near zero or matching expected failure rate | Spiking | Check `on_error()` logic, binary health, input data |
| `rate(drakkar_executor_timeouts_total[5m])` | Zero | Non-zero | Increase `task_timeout_seconds` or investigate stuck processes |
| `rate(drakkar_task_retries_total[5m])` | Low | High | Transient failures are frequent — check error patterns |

### Are sinks healthy?

| Metric | Good | Bad | Action |
|--------|------|-----|--------|
| `rate(drakkar_sink_deliver_errors_total[5m])` | Zero | Non-zero | Check sink connectivity, credentials, capacity |
| `histogram_quantile(0.95, rate(drakkar_sink_deliver_duration_seconds_bucket[5m]))` | Low (< 100ms for Kafka/Redis, < 500ms for Postgres/HTTP) | High or spiking | Sink is overloaded or network is degraded |
| `rate(drakkar_sink_dlq_messages_total[5m])` | Zero | Non-zero | Deliveries are failing and being routed to DLQ — investigate sink errors |
| `rate(drakkar_dlq_send_failures_total[5m])` | Zero | Non-zero | **Critical** — both sink AND DLQ failed. Data is being lost. |

### Is the handler code fast enough?

| Metric | Good | Bad | Action |
|--------|------|-----|--------|
| `histogram_quantile(0.95, rate(drakkar_handler_duration_seconds_bucket{hook="arrange"}[5m]))` | << task duration | Comparable to or exceeding task duration | `arrange()` is the bottleneck — cache lookups, reduce I/O |
| `histogram_quantile(0.95, rate(drakkar_handler_duration_seconds_bucket{hook="on_task_complete"}[5m]))` | << task duration | High | `on_task_complete()` is doing too much work — move heavy logic elsewhere |

### Iterating on config

1. Start with calculator values
2. Run under production-like load for 10+ minutes
3. Check the tables above
4. Adjust one parameter at a time, observe for another 10 minutes
5. Typical iteration cycle: `max_executors` first, then `window_size`,
   then duration thresholds last (they affect observability, not throughput)

---

## Principles

The calculator follows these rules to derive each parameter:

### Executor sizing

**`max_executors`** = available cores - 20% reserved. The reserved cores
handle the asyncio event loop, OS, Kafka consumer, and sink I/O. Each
executor slot runs one subprocess consuming one CPU core. Going beyond
available cores causes context switching without throughput gain.
Further capped by `partitions/workers * 4` since there is no point
having more slots than potential queued work.

**`window_size`** targets 2-5 seconds of aggregate work per window
(shorter for slow tasks, longer for fast). This balances two forces:
larger windows reduce `arrange()` call frequency and enable batching,
while smaller windows reduce commit latency since offsets only commit
after the slowest task in the window finishes.

### Kafka consumer

**`max_poll_records`** = enough messages per poll to feed one window
cycle across active partitions. Too low starves the pipeline (partition
queues empty between polls). Too high wastes memory on queued messages
that will not be processed for minutes.

**`session_timeout_ms`** controls how fast Kafka detects a dead worker.
Lower = faster rebalance, but more false positives if the event loop is
temporarily busy. Fast tasks (heartbeat-friendly) use 10s. Slow tasks
use 45-60s to avoid spurious rebalances.

**`max_poll_interval_ms`** must exceed the worst-case window duration
(all tasks hitting the timeout). If a window takes longer, Kafka
kicks the consumer out of the group and triggers rebalance.

### Backpressure

**`backpressure_high_multiplier`** and **`low_multiplier`** control the
pause/resume hysteresis. `high_watermark = max_executors * high_mult`.
Fast tasks drain quickly and need less buffer. Slow tasks need more
buffer but should not over-fetch (each message is minutes of work).
The gap between high and low prevents rapid pause/resume cycling.

### Duration thresholds

All four `*_min_duration_ms` thresholds follow the same principle:
**hide noise, preserve signal**. For fast workloads (p80 < 50ms), the
majority of tasks are routine -- only slow outliers and failures matter.
For slow workloads (p80 > 500ms), every task is worth observing. The
thresholds scale with p80.

**`store_output`** trades disk I/O for debuggability. For fast tasks
producing small stdout, the per-second write volume is high and the
data is rarely needed. For slow tasks, stdout often contains the only
clue about what went wrong.
