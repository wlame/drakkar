# Config Wizard

This page builds a starting [configuration](configuration.md) for your workers.
It asks for the hardware and the task timing of one worker, whether each
worker has a local disk for its own files, where the long event history
lives (recorder files or [Postgres](recorder-postgres.md)), then for the workers of one
cluster: how many, on one host or on several, and which services every worker
reaches. It prints one YAML block, the environment
variables that hold the secrets, and a link that keeps every answer. Open the
link again to get the same config. See [Performance Tuning](performance.md)
for detailed bottleneck analysis and tuning strategies, and
[Fleet registry](fleet.md) for the providers.

!!! warning "Starting point, not a final answer"
    These are **estimated values** to use as a starting point for tuning.
    They are not calibrated against real production workloads. Every
    deployment is different — binary startup cost, message size, sink
    latency, and OS scheduling all affect optimal values. Use these
    recommendations as initial config, then iterate based on the
    [metrics to watch](#what-to-watch-after-deploying) section below.

The printed config is not complete: add your brokers, topics, sinks and
worker names (see [Configuration](configuration.md)). It never holds a
password or a token. Set those in the worker's environment.

<noscript>
<p>This page needs JavaScript. Without it, see <a href="../configuration/">Configuration</a> and <a href="../fleet/">Fleet registry</a>.</p>
</noscript>

<div id="calc">
<div id="wizard-body" hidden>
<nav class="wizard-steps" aria-label="Wizard steps">
<button type="button" data-step="workload">Workload</button>
<button type="button" data-step="disk" hidden>Disk</button>
<button type="button" data-step="history" hidden>History</button>
<button type="button" data-step="hosts" hidden>Hosts</button>
<button type="button" data-step="services" hidden>Services</button>
<button type="button" data-step="choose" hidden>Providers</button>
<button type="button" data-step="result" hidden>Result</button>
</nav>
<p id="wizard-notice" class="wizard-notice" role="status" hidden></p>
<section data-step="workload">
<h3 tabindex="-1">Workload</h3>
<fieldset>
<legend>Hardware and task timing of one worker</legend>
<div class="row">
  <label for="in-cpu">CPU cores available</label>
  <input type="number" id="in-cpu" data-input="cpu" value="8" min="1" max="512" step="1" aria-describedby="hint-cpu error-cpu">
  <span class="hint" id="hint-cpu">total on machine</span>
  <span class="wizard-error" id="error-cpu"></span>
</div>
<div class="row">
  <label for="in-ram">RAM available (GB)</label>
  <input type="number" id="in-ram" data-input="ram" value="16" min="1" max="4096" step="1" aria-describedby="hint-ram error-ram">
  <span class="hint" id="hint-ram">total on machine</span>
  <span class="wizard-error" id="error-ram"></span>
</div>
<div class="row">
  <label for="in-p80">Task duration p80 (ms)</label>
  <input type="number" id="in-p80" data-input="p80" value="500" min="1" max="600000" step="1" aria-describedby="hint-p80 error-p80">
  <span class="hint" id="hint-p80">80th percentile</span>
  <span class="wizard-error" id="error-p80"></span>
</div>
<div class="row">
  <label for="in-sinks">Sink count</label>
  <input type="number" id="in-sinks" data-input="sinks" value="2" min="1" max="10" step="1" aria-describedby="hint-sinks error-sinks">
  <span class="hint" id="hint-sinks">distinct sinks in on_task_complete()</span>
  <span class="wizard-error" id="error-sinks"></span>
</div>
<div class="row">
  <label for="in-stdout">Avg stdout size (KB)</label>
  <input type="number" id="in-stdout" data-input="stdout" value="2" min="0" max="10240" step="1" aria-describedby="hint-stdout error-stdout">
  <span class="hint" id="hint-stdout">per task output</span>
  <span class="wizard-error" id="error-stdout"></span>
</div>
<div class="row">
  <label for="in-workers">Workers in cluster</label>
  <input type="number" id="in-workers" data-input="workers" value="1" min="1" max="100" step="1" aria-describedby="hint-workers error-workers">
  <span class="hint" id="hint-workers">horizontal instances</span>
  <span class="wizard-error" id="error-workers"></span>
</div>
<div class="row">
  <label for="in-partitions">Kafka partitions</label>
  <input type="number" id="in-partitions" data-input="partitions" value="50" min="1" max="1000" step="1" aria-describedby="hint-partitions error-partitions">
  <span class="hint" id="hint-partitions">source topic</span>
  <span class="wizard-error" id="error-partitions"></span>
</div>
</fieldset>
</section>
<section data-step="disk" hidden>
<h3 tabindex="-1">Disk</h3>
<div id="wizard-disk"></div>
</section>
<section data-step="history" hidden>
<h3 tabindex="-1">History</h3>
<div id="wizard-history"></div>
</section>
<section data-step="hosts" hidden>
<h3 tabindex="-1">Hosts</h3>
<div id="wizard-hosts"></div>
</section>
<section data-step="services" hidden>
<h3 tabindex="-1">Services</h3>
<div id="wizard-services"></div>
</section>
<section data-step="choose" hidden>
<h3 tabindex="-1">Providers</h3>
<div id="wizard-reg"></div>
<div id="wizard-elect"></div>
</section>
<section data-step="result" hidden>
<h3 tabindex="-1">Result</h3>
<div aria-live="polite">
<div id="wizard-warnings"></div>
<p id="wizard-status"></p>
</div>
<p id="wizard-rules-error" hidden>The rules file could not be loaded; the fleet part is not shown. The page does not change the link, so the link still holds every answer: reload it later to see the fleet part.</p>
<h4>YAML</h4>
<div class="result"><pre id="calc-yaml"></pre></div>
<button type="button" data-copy="calc-yaml">Copy YAML</button>
<div id="wizard-env-section">
<h4>Environment</h4>
<p id="wizard-env-intro">Set these in the worker's environment, never in the YAML file. See <a href="../security/#provider-credentials">Provider credentials</a>.</p>
<p id="wizard-env-none" hidden>This config needs no secret in the environment.</p>
<div id="wizard-env-block">
<div class="result"><pre id="wizard-env"></pre></div>
<button type="button" data-copy="wizard-env">Copy environment</button>
</div>
</div>
<h4>Link</h4>
<p><code id="wizard-link"></code></p>
<button type="button" data-copy="wizard-link">Copy link</button>
<div class="explain" id="calc-explain"></div>
</section>
<div class="wizard-buttons">
<button type="button" id="wizard-back" hidden>Back</button>
<button type="button" id="wizard-next">Next</button>
</div>
</div>
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

**`session_timeout`** controls how fast Kafka detects a dead worker.
Lower = faster rebalance, but more false positives if the event loop is
temporarily busy. Fast tasks (heartbeat-friendly) use 10s. Slow tasks
use 45-60s to avoid spurious rebalances.

**`max_poll_interval`** must exceed the worst-case window duration
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
