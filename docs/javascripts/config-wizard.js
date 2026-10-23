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

  // Event-loop throughput ceiling: one Python event loop orchestrates
  // every completion, so past ~400 tasks/sec the loop is the bottleneck
  // and extra slots add queue, not throughput (see performance.md,
  // "Bottleneck: Event Loop"). Cap the pool at 400 x task duration.
  var LOOP_CEILING_TASKS_PER_SEC = 400;
  var loopCapSlots = Math.max(1, Math.round(LOOP_CEILING_TASKS_PER_SEC * p80s));
  var loopCapped = loopCapSlots < maxWorkers;
  if (loopCapped) { maxWorkers = loopCapSlots; }

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

  // UI duration thresholds
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
  y += 'sources:\n';
  y += '  kafka:\n';
  y += '    max_poll_records: ' + pollRecords + '\n';
  y += '    session_timeout_ms: ' + sessionTimeout + '\n';
  y += '    heartbeat_interval_ms: ' + heartbeatInterval + '\n';
  y += '    max_poll_interval_ms: ' + maxPollInterval + '\n';
  y += '\nexecutor:\n';
  y += '  max_executors: ' + maxWorkers + '\n';
  y += '  window_size: ' + windowSize + '\n';
  y += '  task_timeout_seconds: ' + taskTimeout + '\n';
  y += '  max_retries: ' + maxRetries + '\n';
  y += '  drain_timeout_seconds: ' + drainTimeout + '\n';
  y += '  backpressure_high_multiplier: ' + highMult + '\n';
  y += '  backpressure_low_multiplier: ' + lowMult + '\n';
  y += '\nui:\n';
  y += '  ws_min_duration_ms: ' + wsMin + '\n';
  y += '  log_min_duration_ms: ' + logMin + '\n';
  y += '  recorder:\n';
  y += '    event_min_duration_ms: ' + eventMin + '\n';
  y += '    output_min_duration_ms: ' + outputMin + '\n';
  y += '    store_output: ' + storeOutput + '\n';

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
  addDd(dl, loopCapped
    ? cpu + ' cores would allow more, but the event-loop ceiling binds first: ~' + LOOP_CEILING_TASKS_PER_SEC + ' tasks/sec * p80 ' + p80 + 'ms = ' + loopCapSlots + ' slots. More slots would complete tasks faster than the single Python event loop can orchestrate. To use the remaining cores, run more workers per host instead. CPU-bound work in arrange()/hooks lowers the real ceiling further.'
    : cpu + ' cores - ' + reservedCores + ' reserved (20%) = ' + (cpu - reservedCores) + ' available. Capped at ' + partPerWorker + ' partitions/worker * 4 = ' + (partPerWorker * 4) + '. Each slot runs one subprocess. More slots than cores causes context-switch overhead without throughput gain.');

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
  addDd(dl2, '~' + tasksPerSec.toFixed(1) + ' tasks/sec per worker, ~' + tasksPerSecCluster.toFixed(1) + ' tasks/sec cluster (' + workers + ' workers * ' + maxWorkers + ' slots / ' + p80s.toFixed(3) + 's)'
    + (loopCapped ? ' Per-worker rate sits at the ~' + LOOP_CEILING_TASKS_PER_SEC + ' tasks/sec event-loop ceiling — to go faster, add workers, not slots.' : ''));
  addDt(dl2, 'Memory (per worker)');
  addDd(dl2, '~' + Math.round(totalEstMb) + ' MB (' + baseMb + ' MB runtime + ' + Math.round(peakQueuedMb) + ' MB peak queue + ' + Math.round(recorderBufferMb) + ' MB recorder buffer)');
  addDt(dl2, 'Commit latency');
  addDd(dl2, '~' + (windowSize * p80s).toFixed(1) + 's worst case (' + windowSize + ' tasks * ' + p80 + 'ms each)');
  addDt(dl2, 'Backpressure threshold');
  addDd(dl2, 'Pauses at ' + (maxWorkers * highMult) + ' queued, resumes at ' + Math.max(1, maxWorkers * lowMult));
}
