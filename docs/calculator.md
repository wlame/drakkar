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

<style>
#calc { max-width: 720px; }
#calc .row { display: flex; gap: 16px; margin-bottom: 12px; align-items: center; }
#calc .row label { min-width: 220px; font-size: 14px; color: var(--md-default-fg-color--light); }
#calc .row input, #calc .row select { flex: 1; padding: 6px 10px; border: 1px solid var(--md-default-fg-color--lightest); border-radius: 4px; background: var(--md-code-bg-color); color: var(--md-default-fg-color); font-family: 'JetBrains Mono', monospace; font-size: 13px; }
#calc .row .hint { font-size: 11px; color: var(--md-default-fg-color--lighter); min-width: 120px; text-align: right; }
#calc h3 { margin-top: 24px; }
#calc .result { margin-top: 24px; padding: 16px; border-radius: 6px; background: var(--md-code-bg-color); border: 1px solid var(--md-default-fg-color--lightest); }
#calc .result pre { margin: 0; font-size: 13px; line-height: 1.6; white-space: pre; overflow-x: auto; }
#calc .explain { margin-top: 16px; font-size: 13px; line-height: 1.7; }
#calc .explain dt { font-weight: 600; font-family: 'JetBrains Mono', monospace; font-size: 12px; color: var(--md-accent-fg-color); }
#calc .explain dd { margin: 0 0 10px 0; padding-left: 0; }
#calc button { padding: 8px 20px; border: none; border-radius: 4px; background: var(--md-accent-fg-color); color: #fff; font-size: 14px; cursor: pointer; margin-top: 8px; }
#calc button:hover { opacity: 0.9; }
</style>

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

<script>
function drakkarCalc() {
  var cpu = Math.max(1, +document.getElementById('in-cpu').value);
  var ram = Math.max(1, +document.getElementById('in-ram').value);
  var p80 = Math.max(1, +document.getElementById('in-p80').value);
  var sinkCount = Math.max(1, +document.getElementById('in-sinks').value);
  var stdoutKb = Math.max(0, +document.getElementById('in-stdout').value);
  var workers = Math.max(1, +document.getElementById('in-workers').value);
  var partitions = Math.max(1, +document.getElementById('in-partitions').value);

  var p80s = p80 / 1000;

  // max_executors: cores - 20% reserved, capped by partitions per worker
  var reservedCores = Math.max(2, Math.ceil(cpu * 0.2));
  var maxWorkers = Math.max(1, cpu - reservedCores);
  var partPerWorker = Math.ceil(partitions / workers);
  maxWorkers = Math.min(maxWorkers, partPerWorker * 4);
  maxWorkers = Math.max(1, maxWorkers);

  // window_size: target 2-5 seconds of work per window
  var windowTargetSec = p80s < 0.1 ? 5 : p80s < 1 ? 3 : 2;
  var windowSize = Math.max(1, Math.round(windowTargetSec / p80s));
  windowSize = Math.min(windowSize, 500);

  // task_timeout: 10x p80 (floor 30s, cap 600s)
  var taskTimeout = Math.max(30, Math.min(600, Math.round(p80s * 10)));

  // max_poll_records: fill one window cycle
  var pollRecords = Math.max(50, Math.min(1000, windowSize * Math.min(partPerWorker, 10)));

  // backpressure multipliers
  var highMult, lowMult;
  if (p80s < 0.1) { highMult = 8; lowMult = 2; }
  else if (p80s < 1) { highMult = 16; lowMult = 4; }
  else if (p80s < 10) { highMult = 32; lowMult = 4; }
  else { highMult = 8; lowMult = 2; }

  // drain_timeout: 2x p80 (floor 30s to match framework default, cap 120s)
  var drainTimeout = Math.max(30, Math.min(120, Math.ceil(p80s * 2)));

  var maxRetries = p80s < 1 ? 2 : 3;

  // Kafka timeouts
  var sessionTimeout = p80s < 1 ? 10000 : p80s < 10 ? 45000 : 60000;
  var heartbeatInterval = Math.max(1000, Math.round(sessionTimeout / 5));
  var worstWindowSec = windowSize * taskTimeout;
  var maxPollInterval = Math.max(300000, Math.min(900000, worstWindowSec * 1000 * 2));

  // debug thresholds
  // ws_min_duration_ms: floor at 30ms — below that, fast task WS events
  // cause buggy timeline rendering and browser performance issues.
  var wsMin, eventMin, outputMin, logMin, storeOutput;
  if (p80 < 50) {
    wsMin = Math.max(30, Math.round(p80 * 3)); eventMin = Math.round(p80 / 2);
    outputMin = Math.round(p80 * 2); logMin = Math.round(p80 * 2);
    storeOutput = false;
  } else if (p80 < 500) {
    wsMin = Math.max(30, Math.round(p80)); eventMin = 0;
    outputMin = Math.round(p80 / 2); logMin = Math.round(p80 / 2);
    storeOutput = true;
  } else {
    wsMin = 0; eventMin = 0; outputMin = 0; logMin = 0; storeOutput = true;
  }

  // estimates
  var perTaskMb = (5 + stdoutKb) / 1024;
  var highWatermark = maxWorkers * highMult;
  var peakQueuedMb = highWatermark * perTaskMb;
  var recorderBufferMb = 50000 * 0.5 / 1024;
  var baseMb = 150;
  var totalEstMb = baseMb + peakQueuedMb + recorderBufferMb;
  var tasksPerSec = maxWorkers / p80s;
  var tasksPerSecCluster = tasksPerSec * workers;

  // build YAML
  var y = '';
  y += 'kafka:\n';
  y += '  max_poll_records: ' + pollRecords + '\n';
  y += '  session_timeout_ms: ' + sessionTimeout + '\n';
  y += '  heartbeat_interval_ms: ' + heartbeatInterval + '\n';
  y += '  max_poll_interval_ms: ' + maxPollInterval + '\n';
  y += '\nexecutor:\n';
  y += '  max_executors: ' + maxWorkers + '\n';
  y += '  window_size: ' + windowSize + '\n';
  y += '  task_timeout_seconds: ' + taskTimeout + '\n';
  y += '  max_retries: ' + maxRetries + '\n';
  y += '  drain_timeout_seconds: ' + drainTimeout + '\n';
  y += '  backpressure_high_multiplier: ' + highMult + '\n';
  y += '  backpressure_low_multiplier: ' + lowMult + '\n';
  y += '\ndebug:\n';
  y += '  ws_min_duration_ms: ' + wsMin + '\n';
  y += '  event_min_duration_ms: ' + eventMin + '\n';
  y += '  output_min_duration_ms: ' + outputMin + '\n';
  y += '  log_min_duration_ms: ' + logMin + '\n';
  y += '  store_output: ' + storeOutput + '\n';

  document.getElementById('calc-yaml').textContent = y;
  document.getElementById('calc-result').style.display = '';

  // explanations
  var el = document.getElementById('calc-explain');
  el.style.display = '';
  // clear and rebuild via DOM
  el.textContent = '';

  function addH3(text) { var h = document.createElement('h3'); h.textContent = text; el.appendChild(h); }
  function addDl() { var dl = document.createElement('dl'); el.appendChild(dl); return dl; }
  function addDt(dl, text) { var dt = document.createElement('dt'); dt.textContent = text; dl.appendChild(dt); }
  function addDd(dl, text) { var dd = document.createElement('dd'); dd.textContent = text; dl.appendChild(dd); }

  addH3('How each parameter was calculated');
  var dl = addDl();

  addDt(dl, 'max_executors: ' + maxWorkers);
  addDd(dl, cpu + ' cores - ' + reservedCores + ' reserved (20%) = ' + (cpu - reservedCores) + ' available. Capped at ' + partPerWorker + ' partitions/worker * 4 = ' + (partPerWorker * 4) + '. Each slot runs one subprocess. More slots than cores causes context-switch overhead without throughput gain.');

  addDt(dl, 'window_size: ' + windowSize);
  addDd(dl, 'Target ' + windowTargetSec + 's of work per window. At p80=' + p80 + 'ms, that is ' + windowTargetSec + '/' + p80s.toFixed(3) + ' = ' + windowSize + ' messages. Larger windows reduce arrange() call overhead and enable batching. Smaller windows reduce commit latency \u2014 offsets commit only when the slowest task in the window finishes.');

  addDt(dl, 'task_timeout_seconds: ' + taskTimeout);
  addDd(dl, '10x p80 = ' + Math.round(p80s * 10) + 's (floor 30s, cap 600s). Tasks exceeding this are killed. Set above p99 to avoid killing valid tasks, but low enough to detect stuck processes.');

  addDt(dl, 'max_poll_records: ' + pollRecords);
  addDd(dl, 'window_size * min(partitions/worker, 10) = ' + windowSize + ' * ' + Math.min(partPerWorker, 10) + '. Enough messages per poll to keep partition queues fed for one window cycle. Too low starves the pipeline; too high wastes memory.');

  var bpText = p80s < 0.1 ? 'Fast tasks drain quickly \u2014 small buffer sufficient.' : p80s < 10 ? 'Moderate tasks need room to queue while in-flight work completes.' : 'Long tasks: small buffer prevents over-fetching messages that sit for minutes.';
  addDt(dl, 'backpressure high/low: ' + highMult + '/' + lowMult);
  addDd(dl, 'High watermark = max_executors * ' + highMult + ' = ' + (maxWorkers * highMult) + '. Low = max_executors * ' + lowMult + ' = ' + Math.max(1, maxWorkers * lowMult) + '. ' + bpText + ' The gap prevents pause/resume flapping.');

  addDt(dl, 'drain_timeout_seconds: ' + drainTimeout);
  addDd(dl, '2x p80 = ' + drainTimeout + 's. On shutdown, in-flight tasks get this long to finish. If exceeded, their offsets remain uncommitted and Kafka redelivers after restart.');

  var stText = p80s < 1 ? 'Fast tasks: 10s for quick dead-worker detection.' : p80s < 10 ? 'Moderate: 45s balances detection speed vs false positives.' : 'Slow tasks: 60s prevents spurious rebalances.';
  addDt(dl, 'session_timeout_ms: ' + sessionTimeout);
  addDd(dl, stText + ' Heartbeat every ' + heartbeatInterval + 'ms (session/5). Kafka declares consumer dead if no heartbeat for this duration.');

  addDt(dl, 'max_poll_interval_ms: ' + maxPollInterval);
  addDd(dl, 'Worst-case window: ' + windowSize + ' tasks * ' + taskTimeout + 's = ' + worstWindowSec + 's. 2x margin = ' + (worstWindowSec * 2) + 's. Kafka kicks the consumer if no poll() within this interval.');

  addDt(dl, 'ws_min_duration_ms: ' + wsMin);
  addDd(dl, wsMin > 0 ? 'Tasks faster than ' + wsMin + 'ms hidden from live UI. Floor is 30ms \u2014 below that, fast task WebSocket events cause buggy timeline rendering and browser performance issues. Failed tasks always shown regardless.' : '0 = show all tasks. With p80=' + p80 + 'ms, task throughput is low enough for the UI to handle.');

  addDt(dl, 'event_min_duration_ms: ' + eventMin);
  addDd(dl, eventMin > 0 ? 'Tasks faster than ' + eventMin + 'ms skip SQLite storage. At ~' + Math.round(tasksPerSec) + ' tasks/sec, filtering prevents DB bottleneck.' : '0 = store all events. SQLite handles ~' + Math.round(tasksPerSec) + ' tasks/sec comfortably.');

  addDt(dl, 'store_output: ' + storeOutput);
  addDd(dl, !storeOutput ? 'Disabled \u2014 fast task stdout is small and uninteresting. Saves ~' + Math.round(stdoutKb * tasksPerSec) + ' KB/sec of writes.' : 'Enabled \u2014 stdout/stderr valuable for debugging. Storage cost manageable at ' + Math.round(tasksPerSec) + ' tasks/sec.');

  addH3('Estimates');
  var dl2 = addDl();
  addDt(dl2, 'Throughput');
  addDd(dl2, '~' + tasksPerSec.toFixed(1) + ' tasks/sec per worker, ~' + tasksPerSecCluster.toFixed(1) + ' tasks/sec cluster (' + workers + ' workers * ' + maxWorkers + ' slots / ' + p80s.toFixed(3) + 's)');
  addDt(dl2, 'Memory (per worker)');
  addDd(dl2, '~' + Math.round(totalEstMb) + ' MB (' + baseMb + ' MB runtime + ' + Math.round(peakQueuedMb) + ' MB peak queue + ' + Math.round(recorderBufferMb) + ' MB recorder buffer)');
  addDt(dl2, 'Commit latency');
  addDd(dl2, '~' + (windowSize * p80s).toFixed(1) + 's worst case (' + windowSize + ' tasks * ' + p80 + 'ms each)');
  addDt(dl2, 'Backpressure threshold');
  addDd(dl2, 'Pauses at ' + (maxWorkers * highMult) + ' queued, resumes at ' + Math.max(1, maxWorkers * lowMult));
}
</script>

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
   then debug thresholds last (they affect observability, not throughput)

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

### Debug thresholds

All four `*_min_duration_ms` thresholds follow the same principle:
**hide noise, preserve signal**. For fast workloads (p80 < 50ms), the
majority of tasks are routine -- only slow outliers and failures matter.
For slow workloads (p80 > 500ms), every task is worth observing. The
thresholds scale with p80.

**`store_output`** trades disk I/O for debuggability. For fast tasks
producing small stdout, the per-second write volume is high and the
data is rarely needed. For slow tasks, stdout often contains the only
clue about what went wrong.
