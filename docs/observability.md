# Observability

Drakkar provides four integrated observability layers: Prometheus metrics for alerting and dashboards, structured logging for event streams, a browser-based operator UI for real-time inspection, and a SQLite flight recorder for post-mortem analysis. All four are enabled by default and work together -- the UI reads from the flight recorder and links out to Prometheus graphs. See [Configuration](configuration.md) for the full YAML reference.

---

## Prometheus Metrics

Metrics are exposed via `prometheus_client` and served on a dedicated HTTP endpoint. The server starts automatically when `metrics.enabled` is `True` (the default).

```yaml
metrics:
  enabled: true
  port: 9090
```

### Metrics Reference

#### Worker Identity

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `drakkar_worker` | Info | `worker_id`, `version`, `consumer_group` | Worker instance identity, published once at startup. The `consumer_group` label is **empty** on a worker with `sources.kafka.enabled: false` — it joins no group, so reporting the configured default would place it in every group-scoped dashboard query it has nothing to do with. |

#### Messages

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `drakkar_messages_consumed_total` | Counter | `partition` | Total messages consumed from the source Kafka topic |
| `drakkar_message_parse_failures_total` | Counter | `partition` | Source messages whose value failed `input_model` deserialization (handled per `sources.kafka.on_parse_error`) |
| `drakkar_delivery_stalled_offsets_total` | Counter | `partition` | Offsets left uncommitted because sink delivery (including the DLQ fallback) could not be confirmed. The watermark is stalled; messages are redelivered after restart. **Alert on this.** |
| `drakkar_suppressed_zombie_deliveries_total` | Counter | `partition` | Sink deliveries suppressed because the task finished after a revoke/shutdown drain timeout — the new partition owner re-processes those messages. A rising rate means `executor.drain_timeout` is too small for the workload. |
| `drakkar_messages_unassigned_dropped_total` | Counter | `partition` | Messages received from Kafka for a partition with no registered processor (a revoke raced the poll). The new owner redelivers them; this only signals the race happened. |

#### Executor

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `drakkar_executor_tasks_total` | Counter | `status` (`started`, `completed`, `failed`) | Total executor tasks by outcome |
| `drakkar_executor_duration_seconds` | Histogram | -- | Task subprocess run time in seconds, from spawn to exit, measured off the event loop (see [Task run time](#task-run-time-and-the-exit-notice)). Buckets: 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 300 |
| `drakkar_executor_exit_notice_seconds` | Histogram | -- | Time from a task subprocess exit to the moment the worker handled it; grows when the event loop is busy. A healthy worker stays in the lowest buckets. Buckets: 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5. Rides per task as `exit_notice_ms` in `task_completed` / `task_failed` metadata. |
| `drakkar_executor_exit_watch_fallbacks_total` | Counter | `reason` (`exited_before_watch`, `unavailable`) | Task exits timed on the event loop because the exit watcher could not watch the process. `exited_before_watch`: the child exited and was reaped before the watch started — a busy loop. `unavailable`: the host cannot watch (no backend for the platform, or the kernel refused). |
| `drakkar_executor_queue_wait_seconds` | Histogram | -- | Time a task waited for a free executor slot before its subprocess could start. Long waits with a busy pool = the pool (or the CPUs behind it) is the bottleneck; long waits with an idle pool = the worker process itself is too slow to start work. Also rides per task as `queue_wait_ms` in `task_started` metadata (timeline hover). |
| `drakkar_executor_spawn_seconds` | Histogram | -- | Time to start the task subprocess: fork/exec plus event-loop scheduling delay around it. Normally single-digit milliseconds; spawn approaching the task duration means the worker process — not the task binary — is the bottleneck. The same figure rides per task as `spawn_ms` in `task_completed` event metadata and shows in the timeline hover. |
| `drakkar_executor_pool_max` | Gauge | -- | Configured executor pool size (`executor.max_executors`) |
| `drakkar_host_effective_cpus` | Gauge | -- | CPUs this worker process can actually use: the affinity mask capped by any cgroup CPU quota, read once at startup. `drakkar_executor_pool_max` above this value means concurrent tasks time-share cores; startup also logs an `executor_pool_exceeds_cpus` warning then. |
| `drakkar_task_label_value` | Histogram | `label` | Numeric values of the task labels named in `metrics.task_label_histograms`, observed once per completed task. Log-spaced buckets from 0.001 to 1e9 (unitless — a label can hold bytes, line counts, ratios). Values that do not parse as finite numbers are skipped. |
| `drakkar_executor_pool_active` | Gauge | -- | Number of tasks currently running in the executor pool |
| `drakkar_executor_timeouts_total` | Counter | -- | Total tasks that exceeded `executor.task_timeout` and were killed |
| `drakkar_executor_output_truncated_total` | Counter | `stream` (`stdout`, `stderr`) | Output streams truncated to `executor.max_stdout` / `executor.max_stderr`. One increment per truncated stream per task. Divide by the `drakkar_executor_tasks_total` rate for the fraction of tasks being truncated. |
| `drakkar_task_retries_total` | Counter | -- | Total tasks retried after failure (via [on_error](handler.md#on_error) returning RETRY) |

Average and percentiles come from the histograms with standard PromQL —
task duration shown here, and the same shapes work for
`drakkar_task_label_value` by adding its `label` selector:

```promql
# average task duration over 5 minutes
rate(drakkar_executor_duration_seconds_sum[5m])
  / rate(drakkar_executor_duration_seconds_count[5m])

# p50 / p90 / p99 task duration
histogram_quantile(0.99,
  rate(drakkar_executor_duration_seconds_bucket[5m]))

# p90 of a configured task label (metrics.task_label_histograms)
histogram_quantile(0.90,
  rate(drakkar_task_label_value_bucket{label="file_size_bytes"}[5m]))
```

### Task run time and the exit notice

`duration` (and `drakkar_executor_duration_seconds`) is how long the task's
subprocess ran: from just before the spawn to its exit. The worker reads the
exit time when the kernel reports the exit, on a small thread of its own (a
Linux pidfd, a macOS kqueue), not when its event loop gets to the exit. A
loop that is busy for two seconds therefore no longer adds two seconds to
the duration of every task that ended in that time. The thread never reaps
the child: asyncio keeps the exit status.

Each `task_completed` and `task_failed` event of a task that ran a process
carries three timing keys in its metadata:

| Key | Meaning |
|---|---|
| `run_started_at` | Unix seconds of the spawn, 6 decimals. The run bar in the timeline is `[run_started_at, run_started_at + duration]`. |
| `exit_notice_ms` | Milliseconds from the exit to the moment the worker handled it. `0` when the worker handled the exit before the watcher reported it. Absent when the exit could not be timed off the loop. |
| `reported_run_seconds` | The run time the handler's optional [`task_run_seconds`](handler.md#task_run_seconds) hook reported. Present only when the hook gave a valid value. |

**`exit_notice_ms` is a loop-stall indicator.** It measures the worker, not
the task: a healthy worker handles an exit within a few milliseconds. Values
in the hundreds of milliseconds mean the event loop was busy (a slow hook,
a blocking call, garbage collection) while the task had already finished.
Read it from the histogram:

```promql
# share of task exits the worker handled more than 100 ms late
1 - (
  rate(drakkar_executor_exit_notice_seconds_bucket{le="0.1"}[5m])
  / rate(drakkar_executor_exit_notice_seconds_count[5m])
)
```

The watcher batches its wake-ups: after each batch it pauses 5 ms, so its
own cost stays near 2 % of one core at 1,200 task exits per second. An exit
that comes during the pause is stamped at the end of it, so a stamp can be
up to about 5 ms late; that never delays a task, because the task coroutine
does not wait for the watcher. When the loop handles an exit before the
watcher reports it, the loop's own reading is used (it is at least as
accurate).

On a platform without a watcher, or when the kernel refuses the watch (for
example a seccomp profile that forbids `pidfd_open`), the duration is
measured on the loop as before, `exit_notice_ms` is absent, and
`drakkar_executor_exit_watch_fallbacks_total{reason="unavailable"}` counts
every task. The worker logs `exit_watcher_started` (with the backend) or
`exit_watcher_off` at startup. A short task on a busy loop can exit and be
reaped before the watch starts; it is timed on the loop and counted as
`exited_before_watch`.

The Live page's timeline tab and the Dashboard show the same avg/p50/p90/p99
figures computed browser-side over the tasks currently in memory — no
Prometheus needed for a quick read.

#### Batches

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `drakkar_batch_duration_seconds` | Histogram | -- | Window/batch processing duration (arrange through last task completion). Buckets: 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 600 |

#### Partitions

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `drakkar_partition_queue_size` | Gauge | `partition` | Messages waiting in the partition's internal queue |
| `drakkar_offset_lag` | Gauge | `partition` | Pending (uncommitted) offsets per partition |
| `drakkar_partition_processor_deaths_total` | Counter | `partition`, `outcome` | Partition loops that exited on an unexpected error. `outcome="restarted"` came back; `outcome="dead"` gave up and now fails `/readyz`. Alert on any non-zero rate — see [Deployment](deployment.md#dead-partition-loops) |
| `drakkar_backpressure_active` | Gauge | -- | Whether Kafka fetching is paused because queued messages reached `sources.kafka.max_queued_messages` (see [backpressure](performance.md#backpressure)). `1` = paused, `0` = flowing |
| `drakkar_total_queued` | Gauge | -- | Total messages buffered in all partition queues plus in-flight tasks |
| `drakkar_pending_tasks` | Gauge | -- | Tasks created and not yet settled: waiting for an executor slot, running, or in `on_task_complete` and its delivery. The actual count; reservations are not in it. New windows wait while it plus the reserved tasks reach `executor.max_pending_tasks`; it can pass the limit by the admitted window's fan-out beyond its estimate (normally one message) |
| `drakkar_pending_bytes` | Gauge | -- | Bytes held for pending tasks and kept results: task stdin plus stdout and stderr kept for `on_message_complete` and `on_window_complete`. The actual count, reservations not included; new windows wait while it plus the reserved bytes reach `executor.max_pending_bytes` |
| `drakkar_window_admission_wait_seconds_total` | Counter | `limit` | Seconds partition loops waited for room under `executor.max_pending_tasks` (`limit="tasks"`) or `executor.max_pending_bytes` (`limit="bytes"`) before taking a window. `tasks` when both were reached; a wait behind other waiters while there is room counts under the limit that last blocked a loop; a wait for the one unmeasured window at worker start counts under `tasks` |
| `drakkar_window_messages` | Histogram | -- | Messages taken into one `arrange()` window. Buckets 1, 2, 5, 10, 20, 50, 100, 200, 500, 1000 |
| `drakkar_queued_messages_dropped_at_stop_total` | Counter | -- | Polled messages left unarranged at stop or revoke; the next owner of the partition reads them again |
| `drakkar_assigned_partitions` | Gauge | -- | Number of partitions currently assigned to this worker |
| `drakkar_executor_idle_slot_seconds_total` | Counter | -- | Accumulated slot-seconds of executor idle time while messages are waiting in queues. If 2 slots sit idle for 3 seconds while messages are queued, this increments by 6. Only counts when messages are in queues (not yet dispatched), not when the worker has nothing to do. |
| `drakkar_consumer_idle_seconds_total` | Counter | -- | Accumulated seconds with no messages available from Kafka and nothing in queues. Measures time the worker has genuinely nothing to do (consumer lag is zero). Does not count time when the consumer is paused due to backpressure. |

#### Consumer

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `drakkar_consumer_errors_total` | Counter | -- | Total Kafka consumer poll errors |

#### Offsets

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `drakkar_offsets_committed_total` | Counter | `partition` | Total offset commit operations |

#### Rebalancing

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `drakkar_rebalance_events_total` | Counter | `type` (`assign`, `revoke`) | Total Kafka rebalance events |

#### Sinks

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `drakkar_sink_deliver_duration_seconds` | Histogram | `sink_type`, `sink_name` | Duration of sink delivery operations. Buckets: 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5 |
| `drakkar_sink_deliver_errors_total` | Counter | `sink_type`, `sink_name` | Total sink delivery failures |
| `drakkar_sink_payloads_delivered_total` | Counter | `sink_type`, `sink_name` | Total payloads delivered to sinks |
| `drakkar_sink_batch_fallbacks_total` | Counter | `sink_type`, `sink_name` | Batched statements that failed and were retried one payload at a time. Delivery stays correct -- a failed batch wrote nothing -- but costs one round trip per payload instead of one per batch. A climbing rate means batching is not working: check the batch error in the paired `sink_batch_fallback_per_row` warning. |
| `drakkar_sink_delivery_retries_total` | Counter | `sink_type`, `sink_name` | Total sink delivery retry attempts |
| `drakkar_sink_deliveries_skipped_total` | Counter | `sink_type`, `sink_name` | Sink deliveries skipped: by the handler (`SKIP`), by the declared `on_failure: skip`, on an open circuit with `skip`, or for a [window result](sinks.md#window-results) that cannot reach the DLQ |
| `drakkar_sink_deliveries_not_started_total` | Counter | `sink_type`, `sink_name`, `reason` | Sink deliveries not started, by reason: `message_dlq` (the message is already on the DLQ) or `dependency_failed` (a [`depends_on`](sinks.md#sink-dependencies-depends_on) dependency failed without a DLQ record) |
| `drakkar_tasks_precomputed_total` | Counter | -- | Tasks whose result was supplied by the handler via `ExecutorTask.precomputed`, bypassing the subprocess. Framework is agnostic to the reason (cache hit, lookup, deterministic shortcut). Compare to `drakkar_executor_tasks_total{status="completed"}` for the short-circuit rate. |
| `drakkar_sink_dlq_messages_total` | Counter | -- | DLQ records written, one per source message, all stages (see [the DLQ record](sinks.md#the-dlq-record)) |
| `drakkar_dlq_records_total` | Counter | `stage` | DLQ records written by the stage that failed: `parse`, `execute` or `sink` |
| `drakkar_dlq_send_failures_total` | Counter | -- | Total failed attempts to send messages to the DLQ. What happens next depends on `dlq.on_send_failure`: `drop` (default) commits past the lost messages, `stall` leaves the offsets uncommitted and pauses the partition — alert on this counter either way. |
| `drakkar_dlq_unconfigured_drops_total` | Counter | -- | Payloads (sink path) or source messages (task path) dropped because no DLQ topic is configured (the Kafka source is disabled and `dlq.topic` is empty). A sink failure counts its payloads; a task that `on_error` sends to the DLQ counts its source messages. See [the DLQ rule](sources.md#the-dlq-without-the-kafka-source). The first drop also logs `dlq_send_dropped_unconfigured` at warning level. |
| `drakkar_dlq_dropped_payloads_total` | Counter | `partition` | Source messages lost because no DLQ record could be written and `dlq.on_send_failure` is `drop`. Each lost message counts once. The offset committed and the message is lost. **Alert on this.** |
| `drakkar_sink_circuit_open` | Gauge | `sink_type`, `sink_name` | Per-sink [circuit breaker](sinks.md#circuit-breaker) state: `0.0` closed, `0.5` half-open, `1.0` open. Gauges are zero-initialized at sink registration so a never-tripped sink still appears in scrape output. Sustained `1.0` on a sink means its downstream has been down longer than the cooldown can recover from. |
| `drakkar_sink_circuit_trips_total` | Counter | `sink_type`, `sink_name` | Transitions *into* the open state per sink — both the initial failure-threshold trip and every half-open probe failure. A flapping circuit surfaces as a rising rate on this counter, not a single trip plus silent reopens. Alert on `rate(...[5m]) > 0` paired with non-zero `drakkar_sink_circuit_open`. |

The sink delivery and DLQ log events (category `sink`, except where noted):

| Event | Level | Meaning |
|-------|-------|---------|
| `message_dlq` | warning | A source message was written to the DLQ; no later delivery of it starts. Fields `stage`, `sink`, `error`, `source_topic`, `partition`, `offset`, `request_id` (HTTP), `delivered_sinks`. |
| `message_already_dlq` | info | A failure of a message that already has its DLQ record; no second record is written. Fields `stage` and `partition`, plus `sink` for the `sink` stage and `task_id` for the `execute` stage. |
| `sink_not_started` | debug | A sink group did not start. Fields `sink`, `reason` (`message_dlq` or `dependency_failed`). |
| `sink_delivery_skipped` | warning | A delivery ended in `skip`. At most once a minute per sink, with `suppressed`. |
| `sink_retry_budget_spent` | warning | The hook answered `RETRY` with no retries left; the declared `on_failure` applied. Fields `sink`, `attempts`, `max_retries`, `on_failure`. At most once a minute per sink. |
| `delivery_error_hook_failed` | error | `on_delivery_error` raised; the declared `on_failure` applied. Fields `sink`, `attempt`, `on_failure`, `error`, with the exception. |
| `window_delivery_not_dlq` | warning | A [window result](sinks.md#window-results) delivery ended in `dlq`, which acts as `skip` there. At most once a minute per sink. |
| `sink_delivery_circuit_open` | warning | A delivery met an open circuit; the declared `on_failure` (field `on_failure`) decided without the hook. |
| `dlq_message_sent` | info | A DLQ record was confirmed by the broker. Fields `stage`, `sink`, `partition`, `value_bytes`. |
| `dlq_send_failed`, `dlq_flush_incomplete`, `dlq_delivery_report_failed` | critical | A DLQ write failed or was not confirmed in time. `dlq_send_failed` carries `stage`, `sink`, `partition`, `value_bytes`, `attempts`. |
| `dlq_failure_payloads_dropped` | critical | No record could be written and `dlq.on_send_failure` is `drop`. A sink or task failure carries `message_count`; a parse failure carries `partition`, `offset` and `reason` instead. |
| `execute_dlq_failed` | error | Category `partition`. The `execute` record of a task that `on_error` sent to the DLQ raised or was refused under `stall`. Fields `task_id`, `source_offsets`, `error`, `error_type`, `offset_action`. |

#### Handler Hooks

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `drakkar_handler_duration_seconds` | Histogram | `hook` (`arrange`, `on_task_complete`, `on_message_complete`, `on_error`, `on_window_complete`, etc.) | Duration of user handler hook execution. Buckets: 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 30 |
| `drakkar_handler_hook_errors_total` | Counter | `hook` | User hooks that raised instead of returning. The framework contains the failure and keeps the partition flowing, so this counter is the only signal that a handler is broken — **alert on any non-zero rate** |

#### Periodic tasks

Covers both user-defined [`@periodic`](#periodic-tasks) methods and the framework's system tasks (`cache.flush`, `cache.sync`, `cache.cleanup`, `history.archive`), keyed by task name. A cluster task's run counts only on the worker that won the slot.

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `drakkar_periodic_task_runs_total` | Counter | `name`, `status` (`ok`, `error`) | Periodic task executions by name and outcome. Alert on a non-zero `status="error"` rate — the wrapper logs and keeps scheduling by default, so this counter is the durable failure signal. |
| `drakkar_periodic_task_duration_seconds` | Histogram | `name` | Duration of periodic task executions. Buckets: 0.001, 0.01, 0.1, 0.5, 1, 5, 10, 30, 60. |
| `drakkar_periodic_cluster_slots_total` | Counter | `name`, `outcome` (`won`, `held_elsewhere`, `late`, `election_failed`) | Cluster periodic task slots by name and outcome. Across a healthy cluster, each slot counts `won` on one worker and `held_elsewhere` on the others. A growing `election_failed` rate means the election provider does not answer; `late` means the worker was blocked through the slot's 10-second acquire window. See [Periodic Tasks](periodic-tasks.md#cluster-tasks-the-slot-rule). |
| `drakkar_periodic_cluster_mismatch_peers` | Gauge | -- | Online peers of this worker's cluster whose cluster periodic task definitions differ. Non-zero while a rolling change is not finished; alert when it stays non-zero. See [Definition hash](periodic-tasks.md#definition-hash). |

#### Offload pool

CPU-bound hook work moved off the event loop via [`handler.offload()`](offload.md).

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `drakkar_offload_running` | Gauge | -- | Offloaded computations currently executing on the offload thread pool |
| `drakkar_offload_queued` | Gauge | -- | Offloaded calls waiting for a free pool thread. Sustained non-zero values mean `threads.offload` is undersized for how often hooks offload concurrently |
| `drakkar_offload_duration_seconds` | Histogram | `hook` | Execution time of offloaded computations, queue wait excluded. Buckets reach 120s — offloaded work is expected to be seconds-scale |

#### Recorder

Emitted only when the worker has a recorder (`recorder.enabled` or `ui.enabled`, see [Flight recorder](configuration.md#flight-recorder-recorder)). Track flight-recorder health -- buffer depth, drops under load, flush latency, and the memory store. The memory metrics are explained in [Recent Events in Memory](recent-events.md#metrics). Alert on sustained non-zero `dropped_events_total` or p99 `flush_duration_seconds` exceeding `recorder.flush_interval`.

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `drakkar_recorder_buffer_size` | Gauge | -- | Events that wait in the intake plus rows that wait in the file queue. Updated on every `_record()` call and after each stage. A sustained value near `recorder.max_buffer_rows` signals the loops can't keep up with incoming events -- raise `recorder.flush_interval` or `recorder.max_buffer_rows`, or investigate disk latency. |
| `drakkar_recorder_dropped_events_total` | Counter | -- | Rows evicted from the intake or the file queue because it was full. Non-zero means event history has gaps; scale the buffer up or speed up flushes. |
| `drakkar_recorder_flush_duration_seconds` | Histogram | -- | Duration of one stage F, the file write (`executemany` + `commit`). Exposes disk-I/O latency tail. Buckets: 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5. Alert on p99 exceeding `recorder.flush_interval` -- flushes overlapping the next scheduled flush starve the buffer. |
| `drakkar_recorder_flush_retries_total` | Counter | -- | Flush attempts that failed with `aiosqlite.OperationalError` and re-queued their batch at the front of the buffer. A non-zero `rate(...[5m])` signals a transient DB-health issue (WAL lock, disk pressure); if it climbs without stabilising, the recorder is about to start dropping batches. Pair with `drakkar_recorder_flush_batches_dropped_total` to distinguish "retrying" from "giving up". |
| `drakkar_recorder_flush_batches_dropped_total` | Counter | -- | Batches discarded after `recorder.sqlite.max_flush_retries` consecutive failures. This is silent recorder data loss — alert on any non-zero rate. Investigate disk space, filesystem health, and WAL contention. |
| `drakkar_recorder_requeue_overflow_total` | Counter | -- | Rows evicted from the tail of a full queue (`recorder.max_buffer_rows`) while a chunk is put back at its front. With a recorder file, a failed or cancelled file write puts its chunk back at the front of the file queue; a transient memory error (busy or locked) puts its chunk back at the front of the intake, and so does a cancelled memory write without a file. Rows recorded during the write can fill the queue. Any non-zero value is data loss; alert alongside `flush_batches_dropped_total`. |
| `drakkar_recorder_memory_rows` | Gauge | -- | Rows in the events table of the recorder memory store |
| `drakkar_recorder_memory_bytes` | Gauge | -- | Bytes of data in the recorder memory store: pages in use times the page size |
| `drakkar_recorder_memory_allocated_bytes` | Gauge | -- | Database size of the recorder memory store, free pages included; the memory it holds is at least this. The process RSS is larger; see [RAM to plan](recent-events.md#ram-to-plan). |
| `drakkar_recorder_memory_oldest_age_seconds` | Gauge | -- | Age in seconds of the oldest event in the recorder memory store, 0 when it is empty. Shows how much time the store really covers when the row cap or the budget binds first. |
| `drakkar_recorder_memory_evicted_rows_total` | Counter | `reason` | Rows removed from the recorder memory store, by reason: `window`, `rows`, `bytes`, `gap`. |
| `drakkar_recorder_memory_write_failures_total` | Counter | -- | Event chunks that could not be written to the recorder memory store. Final errors only; a busy or locked store is retried and logs `recorder_memory_write_retry`. |
| `drakkar_recorder_postgres_queue_rows` | Gauge | -- | Rows waiting to be written to the recorder Postgres table: the Postgres queue plus the chunk that stage P holds. Only with provider `postgres`. See [Recorder History in Postgres](recorder-postgres.md#while-postgres-is-down). |
| `drakkar_recorder_postgres_rows_written_total` | Counter | -- | Rows written to the recorder Postgres table. With the table size, it gives the real write volume. |
| `drakkar_recorder_postgres_dropped_rows_total` | Counter | `reason` | Rows that never reached the recorder Postgres table, by reason: `queue_full` (the Postgres queue passed a bound), `rejected` (a chunk failed twice with a final error), `no_partition` (the same, and the last error was `23514`), `shutdown` (left over at stop). Any non-zero rate is a gap in the Postgres history. |
| `drakkar_recorder_postgres_partitions_dropped_total` | Counter | `reason` | Recorder Postgres partitions dropped, by reason: `retention` (the hour is past the retention stored with the table), `size` (the table was above `recorder.postgres.max_size`). Counted when the upkeep commits. A steady `size` rate means the table is too small for its retention. |
| `drakkar_recorder_ws_dropped_events_total` | Counter | -- | Events dropped for a live WebSocket subscriber whose queue was full. Affects only that subscriber, which is signalled to resync from the DB, so the UI self-heals; sustained growth means live views cannot keep up with event volume. |
| `drakkar_recorder_archive_bytes` | Gauge | -- | Total size on disk of the compressed `.db.gz` archives of this cluster, and of this worker when it [archives its own files](local-databases.md#archive-scopes), in its history folder (`<dir>/history/<cluster>/`), refreshed on every archive pass including the ticks that archive nothing. Archives are the only recorder artifact nothing else reclaims: `recorder.archive.retention` bounds the archives of the current scope and worker name, and nothing while it is `0`. Archives of a retired worker name are neither counted nor expired. Alert against your volume budget. |
| `drakkar_recorder_archive_files` | Gauge | -- | Number of `.db.gz` archives of this cluster, and of this worker when it archives its own files, in its history folder. Read with `drakkar_recorder_archive_bytes` to tell a slow accumulation of small windows from a few very large ones. |
| `drakkar_recorder_unsealed_files` | Gauge | -- | Own recorder files closed but not yet sealed into the history folder: files waiting for the background move, plus the backlog of failed seals and moves. Alert when it stays above `0` for two rotation intervals: the history folder is unwritable, or another process holds the file. See [Sealing](local-databases.md#sealing-and-the-history-folder). |
| `drakkar_recorder_seal_failures_total` | Counter | -- | Recorder files whose sealing failed; they stay in the backlog and are retried. Counts a failed seal and a failed move into the history folder (log `recorder_seal_failed`, field `stage`). |
| `drakkar_recorder_orphan_failures_total` | Counter | -- | Orphan worker folders the leftover pass could not look at, move or seal; each is tried again at the next pass (log `recorder_orphan_folder_failed`, field `stage`). See [Orphan folders](local-databases.md#orphan-folders). |
| `drakkar_recorder_archive_failures_total` | Counter | -- | Archive passes that did not finish cleanly: a window failed, the pass was fenced (another worker took the lease over, or the provider could not confirm it; see `reason` of `recorder_archive_fenced`), or an unexpected error. |
| `drakkar_recorder_annotations_total` | Counter | -- | Handler annotations accepted and written to the recorder. See [Annotations](annotations.md). |
| `drakkar_recorder_annotations_dropped_total` | Counter | `reason` | Annotations discarded before reaching the recorder. `reason` is `oversize` (payload alone exceeded `recorder.annotations.max_size`), `budget_exhausted` (the hook invocation had spent `recorder.annotations.max_size_per_call`), `no_context` (called outside a framework-invoked hook — a handler bug), or `unserializable`. Payloads are dropped whole, never truncated. The accompanying warning log falls silent after five drops in one hook invocation to protect the log pipeline, so **alert on this counter, not on log volume**. |

The memory store logs `recorder_memory_write_retry`, `recorder_memory_write_failed`, `recorder_memory_eviction_failed` (with `rollback_error` when the rollback failed too), `recorder_memory_rollback_failed` and `recorder_memory_config_write_failed`, and a UI read stopped by the 2 second memory read limit logs `ui_read_memory_timed_out` (category `debug`). Each appears at most once per 60 seconds. See [Recent Events in Memory](recent-events.md#log-events). `recorder_flush_loop_iteration_failed` and `recorder_final_flush_failed` carry `stage` (`memory`, `file` or `postgres`); see [Buffer Mechanics](#buffer-mechanics). The log events of provider `postgres` are in [Recorder history in Postgres](#recorder-history-in-postgres).

#### Cache

Emitted only when [`cache.enabled=true`](cache.md). Memory gauges are maintained as running sums — Prometheus scrape reads a single int, never walks the in-memory dict. DB gauges are refreshed by the `cache.cleanup` loop (default every 60s).

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `drakkar_cache_hits_total` | Counter | `source` (`memory`, `db`) | Cache reads served from memory or from the DB fallback. `peek()` is **not** counted — only `get()` increments this. |
| `drakkar_cache_misses_total` | Counter | -- | Cache reads that returned `None` (key absent or expired in both memory and DB). |
| `drakkar_cache_writes_total` | Counter | `scope` (`local`, `cluster`, `global`) | Total `Cache.set()` calls, labelled by the entry's scope. |
| `drakkar_cache_deletes_total` | Counter | -- | Total `Cache.delete()` calls regardless of whether the key was present. Note: delete is local-only — see [sharp edge](cache.md#delete-is-local-only-the-main-sharp-edge). |
| `drakkar_cache_evictions_total` | Counter | -- | Entries popped from the in-memory dict due to the LRU cap (`max_memory_entries`). Sustained high rate signals cap undersizing. |
| `drakkar_cache_flush_entries_total` | Counter | `op` (`set`, `delete`) | Entries drained from the dirty map to SQLite per flush cycle, by op type. Measures flush throughput, not rows actually modified (LWW may reject a SET). |
| `drakkar_cache_flush_failures_total` | Counter | -- | Flush cycles that raised. Consecutive failures mean cache writes accumulate in memory (lost on shutdown if the final drain also fails); after 5 in a row the worker logs `cache_flush_failing_repeatedly` at ERROR. **Alert on a rising rate.** |
| `drakkar_cache_cleanup_removed_total` | Counter | -- | Rows removed from the SQLite DB by the cleanup loop (entries whose TTL elapsed). A sudden spike means many entries expiring together. |
| `drakkar_cache_sync_entries_fetched_total` | Counter | `peer` | Rows pulled from a peer over HTTP ([cache exchange](cache.md#cache-exchange)) and accepted, per peer. Counted after the page commits. Rows the worker rejected are not counted here; they go to `drakkar_cache_sync_errors_total`. |
| `drakkar_cache_sync_entries_upserted_total` | Counter | `peer` | Rows from a peer passed to the LWW upsert of the local DB, per peer. Counted with the same rows as `sync_entries_fetched_total`, so the two are equal; rows that LWW keeps unchanged are still counted. |
| `drakkar_cache_sync_errors_total` | Counter | `peer` | Per-peer exchange errors: one per failed request (timeout, refused connection, a status other than 200, an answer that is not the expected JSON), one per page that could not be applied locally, and one per rejected row (wrong scope or not the peer's own origin). The loop keeps running. |
| `drakkar_cache_peer_sync_timeouts_total` | Counter | -- | Worker-level count of exchange cycles that hit `cache.exchange.cycle_deadline` (or the derived `cache.exchange.interval * 0.9` default). One tick per cycle that was cut short by the deadline; the cut peers are not counted as failures. A sustained non-zero rate signals slow peers, or more peers than one interval can pull — raise the deadline and the interval, or raise `max_concurrent_peers`. |
| `drakkar_cache_entries_in_memory` | Gauge | -- | Entries currently in the in-memory dict. Running sum, adjusted per set/delete/evict/cleanup. |
| `drakkar_cache_bytes_in_memory` | Gauge | -- | Sum of `size_bytes` across in-memory entries. Running sum (see above). |
| `drakkar_cache_entries_in_db` | Gauge | -- | Rows in the local `<worker>-cache.db`. Refreshed by the cleanup loop. |
| `drakkar_cache_bytes_in_db` | Gauge | -- | Sum of `size_bytes` across DB rows. Refreshed by the cleanup loop. |

Cache flush/sync/cleanup loop durations are captured by the existing
`drakkar_periodic_task_duration_seconds{name="cache.flush|cache.sync|cache.cleanup"}`
histogram — no dedicated cache-only timing histograms.

Each HTTP request of the exchange is also counted in the fleet metrics
`drakkar_fleet_peer_requests_total{endpoint="cache_changes", outcome}`
(`ok`, `http_error`, `timeout`, `transport_error`, `bad_body`) and timed in
`drakkar_fleet_peer_request_duration_seconds{endpoint}`. See
[Fleet registry — Metrics](fleet.md#metrics). The exchange logs
`fleet_peer_request_failed`, `fleet_peer_backoff` and
`fleet_peer_rows_rejected` ([Fleet registry — Log events](fleet.md#log-events)),
`cache_peer_sync_failed` when a pulled page cannot be applied locally
(fields `peer`, `error`), and `cache_peer_sync_cycle_timeout` when a cycle
hits its deadline. With `autodiscover` off, an enabled exchange logs
`cache_exchange_off_no_autodiscover` (info, category `cache`) once at startup.

#### Fleet registry and provider operations

Every call from the worker to a provider is one **operation**: the registry makes `start`, `publish`, `list`, `withdraw` and `stop`, and the election of [cluster periodic tasks](periodic-tasks.md#the-election) (the archive included) makes `start`, `acquire`, `refresh`, `is_held`, `release` and `stop`. With [provider `postgres`](recorder-postgres.md) the recorder makes `start`, `copy` and `partitions` with `feature="recorder"` and `provider="postgres"`; `drakkar_provider_call_duration_seconds` of `op="copy"` is the COPY duration. Retries and the circuit breaker are explained in [Fleet registry — Resilience](fleet.md#resilience).

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `drakkar_provider_calls_total` | Counter | `feature` (`registry`, `election`, `recorder`), `provider`, `op`, `outcome` | Provider operations by outcome: `ok`, `error`, `timeout`, `skipped_open` (the breaker was open, nothing was called) or `not_started` (the provider has not started). A retried operation counts once, with the outcome of its last attempt. |
| `drakkar_provider_call_duration_seconds` | Histogram | `feature`, `provider`, `op` | Duration of one operation with its retries and the waits between them. Skipped and not started operations have no sample. Buckets: 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30. |
| `drakkar_provider_breaker_open` | Gauge | `feature`, `provider` | `1` while the circuit breaker of that provider is open. A breaker that stays open means the provider is down; the operations are skipped and the log carries `provider_breaker_opened`. Alert on it. |
| `drakkar_fleet_registry_errors_total` | Counter | `op` | Registry operations that failed or timed out, and loop iterations that raised. An operation counts once, retries included, and an operation with no attempt (open breaker, not started) does not count. `op` is `start`, `publish`, `list`, `withdraw` or `stop`. |
| `drakkar_fleet_registry_duration_seconds` | Histogram | `op` | Duration of registry operations, all attempts included. |

The other `drakkar_fleet_*` metrics (peers, snapshot age, duplicate worker
names, peer requests) are in [Fleet registry — Metrics](fleet.md#metrics).

The provider log events:

| Event | Level | Meaning |
|-------|-------|---------|
| `provider_started` | info | A provider started; after failed starts, logged once when a start succeeds. |
| `provider_call_failed` | warning | An operation failed after its retries. At most once a minute per operation; `suppressed` counts the failures the line stands for. |
| `provider_breaker_opened` | warning | A breaker opened (once per opening; a failed probe does not log it again). |
| `provider_breaker_closed` | info | A probe succeeded and the breaker closed. |
| `fleet_registry_iteration_failed` | error | A registry loop iteration raised outside a provider call. At most once a minute per loop, with `suppressed`. |
| `periodic_lock_taken_over`, `periodic_lock_put_back_failed` | warning | Lease takeovers of the election providers; see the [periodic task log events](#periodic-log-events). |

#### Webapp

Emitted only when [`sources.http.enabled=true`](webapp.md). Track per-client request volume, latency, capacity headroom, and drop rates. Status labels are drawn from a closed set documented in [Webapp → Status codes](webapp.md#status-codes): `ok | timeout | error | rate_limited | auth_failed | shutdown | not_ready | capacity`. The `client` label is bounded by the configured client list plus the fixed `unauthenticated` sentinel for auth-failed / pre-auth gate hits, so cardinality stays small.

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `drakkar_webapp_requests_total` | Counter | `client`, `status` | One increment per HTTP request, labelled by the matched client name and the terminal outcome. Replaces a separate `webapp_rate_limited_total` -- query `drakkar_webapp_requests_total{status='rate_limited'}` instead. |
| `drakkar_webapp_request_duration_seconds` | Histogram | `client`, `status` | Server-side wall-clock duration of HTTP requests. Buckets: 0.005, 0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10, 30 -- covers sub-second work and the default 30s `request_timeout` budget. The duration starts at route entry and ends at response emission, so it includes auth + rate-limit + body parsing as well as the runner. |
| `drakkar_webapp_inflight` | Gauge | -- | Number of HTTP requests currently being processed by the runner on T1. Incremented on runner entry, decremented in `finally`. **Alert on `drakkar_webapp_inflight > sources.http.max_concurrent` to spot semaphore-leak bugs** -- a permit accidentally held past response would surface here. |
| `drakkar_webapp_dropped_after_timeout_total` | Counter | `client` | Requests whose pipeline result was dropped on T1 after T2 had already returned a 504 to the caller. Incremented at each cancellation gate (post-execute and pre-`on_http_request_complete`). A high rate signals either a too-tight `request_timeout` or a slow subprocess pipeline -- correlate with `drakkar_executor_duration_seconds`. |
| `drakkar_webapp_rpm_limit` | Gauge | `client` | Configured rpm cap per client. **Informational gauge** set once at webapp startup and never updated thereafter (the cap is a config field; operators edit YAML and restart). Read alongside `drakkar_webapp_requests_total{status='rate_limited'}` to confirm deployed limits match the workload's expectations. |

#### Shutdown

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `drakkar_uncommitted_offsets_at_stop` | Gauge | -- | Snapshot at `_shutdown` start: count of Kafka offsets that were registered in-flight but not yet committed when shutdown began. Summed across all assigned partitions via each `OffsetTracker.pending_count`. Always set (even to `0`) so the gauge reflects the most recent shutdown rather than a stale prior value. |
| `drakkar_inflight_at_stop` | Gauge | -- | Snapshot at `_shutdown` start: number of in-flight executor subprocesses (`ExecutorPool.active_count`) running user code when shutdown began. Always set, including to `0`. |
| `drakkar_drain_timeout_hit_total` | Counter | -- | Incremented each time an input source's drain exceeded `executor.drain_timeout` before its in-flight work finished — the Kafka source's partition processors or the HTTP source's in-flight requests. A nonzero rate signals workers being killed mid-flight — either the timeout is too tight or handlers are stuck. |
| `drakkar_suspected_oom_kills_total` | Counter | -- | Incremented at startup when the previous run left a watchdog file at `<storage.dir>/<cluster>/<worker>/<worker>.watchdog` whose body lacked the `CLEAN_EXIT` marker. That signature means the prior process was killed before reaching the normal shutdown path — typically OOM-killer SIGKILL, kubelet pod-pressure eviction, or kernel panic. See *OOM / SIGKILL detection* below. |

Log events of the start and the stop:

| Event | Level | When |
|---|---|---|
| `ui_disabled_no_probes` | warning | `api.enabled` is `false`, so the worker serves no `/healthz` and `/readyz` for Kubernetes probes. Field `reason`; category `lifecycle`. Once at startup. |
| `queued_messages_dropped_at_stop` | info | A partition loop stopped (a stop or a revoke) with polled messages that were not in a window. They are dropped, not arranged; their offsets were never registered, so the next owner of the partition reads them again. Fields `partition`, `count`, `first_offset` and `hint`; category `partition`. Counted in `drakkar_queued_messages_dropped_at_stop_total`. |
| `partition_paused_on_death` | error | The poll loop paused partitions whose processing loop died. They stay paused until a restart or revoke, and their queues do not count toward `sources.kafka.max_queued_messages`. Field `partitions`; category `lifecycle`. `partition_death_pause_failed` (error, with `error`) when the pause call failed. |
| `messages_polled_at_stop_dropped` | info | A stop arrived while a Kafka poll was in flight and the poll returned messages. The worker does not hand them to the partitions, whose loops are leaving; their offsets are not committed, so the next owner of each partition reads them again. Fields `count` and `partitions` (a sorted list); category `kafka`. |

#### OOM / SIGKILL detection

Drakkar writes a small **watchdog file** at `<storage.dir>/<cluster>/<worker>/<worker>.watchdog`
during startup and removes it as the last step of a clean shutdown. The
file's presence and contents at the next startup let the framework
distinguish three termination modes:

| Watchdog state at startup | Previous run | Action |
|----|----|----|
| File **absent** | First run, or prior shutdown completed cleanly and unlinked the file. | No log, no metric. |
| File **present**, body == `CLEAN_EXIT` | Prior shutdown wrote the marker but a process death between the write and the unlink left the file behind. Still a clean exit. | No log, no metric. |
| File **present**, body empty (or anything else) | Prior process was killed before `_shutdown` reached `mark_clean` — almost always SIGKILL from the OOM-killer, a kubelet pod-pressure eviction, or a kernel panic. | Structured warning `previous_run_ended_unexpectedly` (category `watchdog`) with `worker_id` and `watchdog_path` fields, plus `drakkar_suspected_oom_kills_total` increment. |

The watchdog file is created in `AppLifecycle._async_run` once the
worker is committed to running (the actual `write()` happens just before
`subscribe()` returns and the poll loop starts) and deleted as soon as
the drain phase has been accounted for in `_shutdown`. A
`drain_timeout` is treated as a clean (if slow) shutdown for the
watchdog's purposes — its dedicated counter
`drakkar_drain_timeout_hit_total` already covers that case. Conflating
"drain was slow" with "process was SIGKILLed" would muddle dashboards,
so the OOM counter `drakkar_suspected_oom_kills_total` is reserved for
the genuinely-empty-body signature: process killed before reaching
`mark_clean`.

The file lives in `storage.dir` (default `/tmp/drakkar`), the same folder
used by the recorder and cache engine. If `storage.dir` is empty —
fully disk-less deployment — the watchdog is **disabled** for that run
(no file written, no metric increment), since the alternative of
falling back to the worker's CWD risks both breaking the "no on-disk
state" promise and writing into a read-only container volume. The
disable is logged at startup as `watchdog_disabled_no_storage_dir` so
operators can see the OOM signal is off.

If the watchdog write itself fails (read-only mount, permission denied,
no space), the worker logs a structured warning
`watchdog_write_failed` with `path` and `error`, then **disables the
watchdog for the rest of the run** and continues startup. The watchdog
is observability-only — losing the OOM signal is preferable to
crashing the worker over a misconfigured volume mount.

**Operator interpretation:** any nonzero rate on
`drakkar_suspected_oom_kills_total` should be correlated with
container-runtime pod-restart events. A spike usually maps to one of:
container memory limit too low, a memory leak in handler code, a
sibling pod evicting this worker, or a node-level OOM. Pair the metric
with `drakkar_uncommitted_offsets_at_stop` and
`drakkar_inflight_at_stop` from the previous run (Prometheus retains
the last-set value across restarts) to gauge how much in-flight work
was lost.

Some feature-specific metrics are documented on their own pages rather
than here: [runtime health](runtime-health.md#what-you-see) (event-loop
lag gauges and stall counters) and
[consume pause](consume-pause.md#observability).

### User-Defined Metrics

You can declare Prometheus metrics as class attributes on your handler -- see [Custom Prometheus Metrics](handler.md#custom-prometheus-metrics) for the handler-side setup. The framework auto-discovers them via `discover_handler_metrics()`, which scans the handler's class hierarchy (MRO) for any `prometheus_client` metric instances (`Counter`, `Gauge`, `Histogram`, `Summary`, `Info`).

```python
from prometheus_client import Counter, Histogram
from drakkar.handler import BaseDrakkarHandler

class MyHandler(BaseDrakkarHandler[MyInput, MyOutput]):
    # These are automatically discovered and shown in the operator UI
    items_parsed = Counter(
        'myapp_items_parsed_total',
        'Total items parsed from executor output',
    )
    parse_duration = Histogram(
        'myapp_parse_duration_seconds',
        'Time spent parsing executor output',
    )

    async def on_task_complete(self, result):
        with self.parse_duration.time():
            items = parse_output(result.stdout)
        self.items_parsed.inc(len(items))
        ...
```

User metrics are displayed alongside framework metrics on the UI's metrics page (`/debug`), tagged with source `user` to distinguish them from the built-in `drakkar_*` metrics.

### Scrape Configuration

Metrics are exposed at `:9090/metrics` by default (configurable via `metrics.port`).

=== "Static Targets"

    ```yaml
    # prometheus.yml
    scrape_configs:
      - job_name: drakkar
        static_configs:
          - targets:
              - 'worker-1:9090'
              - 'worker-2:9090'
              - 'worker-3:9090'
    ```

=== "Kubernetes Service Discovery"

    ```yaml
    # prometheus.yml
    scrape_configs:
      - job_name: drakkar
        kubernetes_sd_configs:
          - role: pod
        relabel_configs:
          - source_labels: [__meta_kubernetes_pod_label_app]
            regex: drakkar
            action: keep
          - source_labels: [__meta_kubernetes_pod_ip]
            target_label: __address__
            replacement: '${1}:9090'
    ```

---

## Structured Logging

Drakkar uses [structlog](https://www.structlog.org/) for structured, context-rich logging. By default, log output goes to **stderr** (standard Unix convention), but you can redirect to stdout or a file.

### Configuration

```yaml
logging:
  level: INFO       # DEBUG, INFO, WARNING, ERROR, CRITICAL
  format: json      # json | console
  output: stderr    # stderr | stdout | file path
```

- **`json`** (default) -- produces one JSON object per line, suitable for ingestion by Elasticsearch, Loki, Datadog, or any log aggregator.
- **`console`** -- human-readable colored output for local development.

### Output Destination

The `output` field controls where logs are written:

| Value | Behavior |
|-------|----------|
| `stderr` (default) | Standard error stream |
| `stdout` | Standard output stream |
| File path | Append to the specified file (created if missing, parent directories created automatically) |

File paths support template variables:

| Variable | Substitution |
|----------|-------------|
| `{worker_id}` | Worker name (from `WORKER_ID` env var or config) |
| `{cluster_name}` | Cluster name (from config or env var) |

```yaml
# Examples
logging:
  output: stdout

logging:
  output: /var/log/drakkar/{worker_id}.log

logging:
  output: /var/log/{cluster_name}/{worker_id}.jsonl
```

Environment variable override: `DK_LOGGING__OUTPUT=/var/log/drakkar/worker.log`

### Automatic Context Fields

Every log line includes these fields, bound once at startup:

| Field | Source | Example |
|-------|--------|---------|
| `timestamp` | ISO 8601 timestamp | `2026-04-06T12:34:56.789Z` |
| `level` | Log level | `info` |
| `service_name` | Hardcoded | `drakkar` |
| `worker_id` | `worker.name_env` env var, else `worker.name` | `worker-3` |
| `consumer_group` | `sources.kafka.consumer_group` config; **omitted entirely** when the Kafka source is disabled | `drakkar-workers` |
| `module` | Python module name | `drakkar.partition` |

### Per-Context Binding

The framework binds additional fields for specific operations using `structlog.contextvars`. These appear on all log lines within that context:

- `partition` -- bound during partition processing
- `hook` -- bound during handler hook execution (e.g., `arrange`, `on_task_complete`, `on_message_complete`)
- `task_id` -- bound during task execution and result handling

### Usage in Handler Code

```python
import structlog

logger = structlog.get_logger()

class MyHandler(BaseDrakkarHandler[MyInput, MyOutput]):
    async def arrange(self, messages, pending):
        await logger.ainfo(
            "Arranging batch",
            category='handler',
            message_count=len(messages),
            pending_count=len(pending.pending_task_ids),
        )
        ...
```

Output (JSON format):

```json
{
  "timestamp": "2026-04-06T12:34:56.789Z",
  "level": "info",
  "event": "Arranging batch",
  "service_name": "drakkar",
  "worker_id": "worker-3",
  "consumer_group": "drakkar-workers",
  "module": "myapp.handler",
  "category": "handler",
  "message_count": 50,
  "pending_count": 12
}
```

---

## Operator UI

The operator web UI is a FastAPI application served in a separate thread so that CPU-intensive executor tasks on the main event loop do not block the interface. Enabled by default on the API port 8080 (`api.port`); it needs `api.enabled`.

```yaml
api:
  enabled: true
  port: 8080
ui:
  enabled: true
```

### Pages

#### `/` -- Dashboard

The landing page with a high-level overview of worker health:

- **Partition tiles** -- one tile per assigned partition showing queue depth and status.
- **Pool utilization** -- active tasks vs. max workers.
- **Event counters** -- consumed, completed, failed, produced, committed totals.
- **Consumer lag** -- total lag across all assigned partitions.
- **Custom links** -- configurable via `ui.custom_links` with template variables (`{worker_id}`, `{cluster_name}`, `{metrics_port}`, `{debug_port}`).
- **Prometheus graph links** -- when `ui.prometheus.url` is configured, each stat card links directly to a Prometheus graph filtered to this worker. Worker-scoped links are grouped by category (Throughput, Latency, Health, Errors). Cluster-wide links appear when `ui.prometheus.cluster_label` is set.

The two tables below the tiles hold the per-partition and per-sink detail. There are no separate `/partitions` or `/sinks` pages.

**All Partitions** table, one row per partition the recorder has seen:

- Queue size (messages waiting)
- Pending offsets (uncommitted)
- Consumer lag (from Kafka high watermark)
- Committed offset and high watermark
- Last consumed and last committed time
- Consumed, completed, and failed event counts

Lag is the high watermark minus the committed offset. The committed offset
comes from the broker. If the broker does not report one for a partition, the
worker uses the last offset that it committed for that partition itself. When
neither is known, the committed offset shows as `-` (`null` in the API) and
lag counts from where the consumer starts to read under `auto.offset.reset`:
the low watermark for `earliest` (the default), the high watermark for
`latest`. The dashboard's total lag is the sum of these per-partition values.

**Sinks** table, one row per configured sink, with live delivery statistics:

- Sink type and name
- Delivered count and payload count
- Error count and retry count
- Last delivery timestamp and duration
- Last error message
- Links to external sink UIs (when `ui_url` is configured on a sink)

#### `/live` -- Live Pipeline View

Real-time view of the processing pipeline, fed by WebSocket (`/ws`). Organized in tabbed sections:

- **Arrange** -- active [arrange()](handler.md#arrange-required) calls with partition, duration, and [message labels](handler.md#message_label).
- **Executors Timeline** -- visual timeline of running, pending, and recently finished tasks. Slot-based layout matching `max_executors`. Tasks shorter than `ui.ws_min_duration` are hidden (except failures, which are always shown). See [Duration Thresholds](#duration-thresholds) for threshold tuning.
- **Collect** -- recently completed tasks with exit codes and durations.

Pool utilization bar shows active, waiting, and available slot counts.

#### `/history` -- Event Browser

Filterable, paginated event browser across all partitions:

- Partition filter dropdown
- Event type toggles (consumed, arranged, task_started, task_completed, task_failed, committed, etc.)
- 100 events per page with forward/backward pagination

#### `/debug` -- Debug Tools

Multi-purpose debug page with:

- **Metrics viewer** -- live snapshot of all registered Prometheus metrics (framework and user), accessible via the `/api/v1/debug/metrics` JSON endpoint.
- **Periodic tasks** -- status of all `@periodic` handler methods: last run time, duration, success/error status, total counts, and a sparkline of recent runs.
- **Cross-worker message trace** -- two search modes:
    - **By partition:offset** -- enter `partition:offset` (e.g., `5:42`) to trace a message through the full pipeline.
    - **By label** -- search inputs are auto-generated for each [label](handler.md#task-labels) key found in the database (e.g., `request_id`, `pattern`). Enter a value to find all tasks and events matching that label. Useful for tracing a specific request across the partitions of this worker.
    A partition:offset trace searches the current worker's own files, or the sealed history of its cluster on request. A label search searches the current worker's live file. With `recorder.enabled: false` both search the memory store instead of the own files. See [Cross-Worker Trace](#cross-worker-trace).
- **Database management** -- list this worker's memory store (one row, `kind: "memory"`), its own recorder files and the shared history folders (sealed files, archives, merge results) with event counts and sizes, download history files, merge several of them into one, and view per-file breakdown by event type. See [The Databases page](local-databases.md#the-databases-page).
- **Live file and memory download** -- `GET /api/v1/debug/download-live` answers a consistent snapshot of this worker's live recorder file (`source=file`) or of its memory store (`source=memory`). The default is `file` when a live file is open, else `memory`. A file copy is made with the SQLite backup API on its own read-only connection, in rollback-journal format, so it opens on its own. The temp copy is removed at once. `404` when the asked source does not exist, `400` for another `source` value, `500` `{"error": "Snapshot failed: ..."}` when the copy fails. Behind `ui.auth_token` like the other debug routes. See [The snapshot download](recent-events.md#the-snapshot-download).
- **Message Probe** -- replay a single pasted message through the handler pipeline (`arrange` → executor → `on_task_complete` → `on_message_complete` → `on_window_complete`) with **zero footprint**: no sink writes, no offset commits, no event-recorder rows, no cache writes, nothing for peers to pull. The report shows every task's stdout/stderr/exit code, each hook's returned `CollectResult`, the payloads that *would have been* written to each sink, every cache call, a timeline waterfall, and any exceptions raised at any stage. Posts to `/api/v1/debug/probe` with a JSON body containing the message's `value` (and optional `key`, `partition`, `offset`, `topic`, `timestamp`, `use_cache`).

#### `/task/{id}` -- Task Detail

Detailed view of a single task's lifecycle:

- **Status** -- running, completed, or failed
- **Labels** -- user-defined [labels](handler.md#task-labels) from [ExecutorTask](executor.md#executortask)`.labels`
- **Duration** -- wall-clock execution time
- **PID** -- OS process ID
- **CLI** -- reconstructed command line (`binary_path` + `args`)
- **stdout/stderr** -- captured process output (subject to `recorder.capture.min_output_duration` threshold)
- **Source offsets** -- which Kafka offsets this task covers
- **Event timeline** -- chronological list of all events for this task_id (started, completed/failed, task_complete)

### API

The pages above are backed by a JSON/WebSocket API under `/api/v1`. Endpoints useful beyond the pages themselves:

- `GET /api/v1/identity` -- worker identity for the SPA and external tooling. Since contract v1.2 the response also carries `backend` (`"python"` or `"go"`), `backend_version`, `ui_version`, and `ui_source`.
- `GET /api/v1/openapi.json` -- the vendored OpenAPI 3.1 spec describing the `/api/v1` surface. Served from `drakkar/uiserver/openapi.yaml`; the canonical source is `docs/openapi-v1.yaml` in the drakkar-ui repo, and `tests/test_openapi_parity.py` pins the served route table to the spec.
- `GET /api-docs` -- a self-hosted Swagger UI over that spec (no CDN assets). It moved off `/docs` in contract v1.22; that path now serves the operator docs site.

Both the spec and Swagger UI endpoints are auth-gated exactly like the rest of their route class when `ui.auth_token` is set.

### Managing the UI bundle cache

The `drakkar-ui` console script (installed with the package) manages the shared per-user bundle cache -- the same engine the worker runs at startup, so you can prime or inspect the cache without starting a worker:

```bash
drakkar-ui where                    # report the cache location + what would be served
drakkar-ui update                   # download the latest UI release into the cache
drakkar-ui fetch --version=v0.2.0   # download a specific release
```

All subcommands accept `--repo=owner/name`, `--cache-dir=DIR`, and `--api-base=URL` (GitHub Enterprise); a `GITHUB_TOKEN` in the environment raises rate limits and unlocks private repos. Cached versions are never re-downloaded -- release tags are immutable. `just drakkar-ui <args>` wraps the same command for repo-local use.

When neither the cache nor the release source can supply a bundle (an offline host whose cache is still empty), the worker **serves its JSON API and WebSocket without pages**: `/api/v1/...`, the Kubernetes probes and the event WebSocket are all unaffected, and every page request answers **503** naming the ways to supply a bundle. There is no built-in HTML fallback -- the UI is one versioned artifact, and a single download at any point in a worker's life fills a cache that every later start reads.

For an air-gapped deployment, stage the bundle once with `drakkar-ui fetch --version=vX.Y.Z` (into a cache directory the workers share), or point `ui.release.repo` at an internal mirror that publishes the same release assets. `ui.release.enabled: false` fetches no bundle, which leaves the same worker without pages. `ui.enabled: false` turns the UI off: the worker then serves only `/healthz`, `/readyz` and the peer route.

---

## Flight Recorder

The flight recorder is a SQLite-based event log that captures the full processing lifecycle. It provides the data backing for the operator UI. It keeps recent events in an in-memory database, the memory store, and with provider `sqlite` it also writes files that survive worker restarts (within retention limits). See [Recent Events in Memory](recent-events.md).

### Configuration

```yaml
storage:
  dir: /tmp/drakkar                   # This worker's own files
recorder:
  enabled: true                       # Durable recording; off keeps recent events in memory for the UI
  provider: sqlite                    # sqlite | postgres
  flush_interval: 5s                  # Tick of the memory loop and the file loop
  max_buffer_rows: 50000              # Capacity of the intake and of the file queue, each
  memory:
    window: 2m                        # Keep events in the memory store this long
    rows_per_executor: 3000           # Row cap = this x executor.max_executors
    max_size: 256MiB                  # Data budget of the memory store
  capture:
    events: true                      # Write processing events to the events table
    config: true                      # Write worker config into each file
    output: true                      # Include stdout/stderr in event records
    min_output_duration: 500ms        # Tasks faster than this store no args/stdout/stderr
    min_event_duration: 0             # Task events faster than this are not persisted at all
    stdin: false                      # Store each task's stdin in task_started metadata (failures always store it)
    max_stdin: 64KiB                  # Size cap for stored stdin; 0 = unlimited
  annotations:
    enabled: true                     # Accept handler-emitted annotations
    max_size: 16KiB                   # Largest single annotation; 0 = unlimited
    max_size_per_call: 256KiB         # Per hook invocation; 0 = unlimited
    max_log_size: 2KiB                # Cap on a dropped payload's log copy; 0 = unlimited
  sqlite:
    rotation_interval: 1h             # Rotate to a new DB file every N hours
    max_flush_retries: 3              # Failed-batch retries before the batch is dropped
    state_snapshots: true             # Periodic state snapshots
    state_interval: 10s               # Snapshot frequency for worker_state
```

The four `recorder.annotations` settings govern
[handler annotations](annotations.md) — diagnostic records your handler
attaches to a window, message, or task. They are stored as ordinary
`events` rows, so they rotate, archive, and (optionally) expire with
everything else — see [Archiving](#archiving) below.

### Database Schema

Each database file can contain up to three tables, controlled by the `recorder.capture.events`, `recorder.capture.config` and `recorder.sqlite.state_snapshots` flags:

#### `events` -- Processing Events

Stores processing events across the full pipeline lifecycle.

Key columns: `ts`, `dt`, `event`, `partition`, `offset`, `task_id`, `args`, `stdout`, `stderr`, `exit_code`, `duration`, `output_topic`, `metadata` (JSON), `pid`, `labels` (JSON), `origin` (`'kafka'` or `'http'`, default `'kafka'`), `client_name` (webapp client name; `NULL` for Kafka-origin rows), `request_id` (webapp framework id; `NULL` for Kafka-origin rows).

Indexed on `(partition, offset)`, `ts`, `dt`, `task_id`, `event`, `labels` (partial, where not null), `origin`, and `request_id` (partial, where not null).

**Event types:**

| Event | When recorded | Key fields |
|-------|--------------|------------|
| `consumed` | Message polled from Kafka | `partition`, `offset` |
| `arranged` | `arrange()` completes for a window | `partition`, `metadata` (message_count, task_count, offsets, message_labels, window_id) |
| `annotation` | A handler calls `self.annotate(...)` — see [Annotations](annotations.md) | `partition`, `offset` (message scope), `task_id` (task scope), `labels`, `metadata` (kind, scope, hook, window_id, offsets, data) |
| `task_started` | Subprocess launched (after semaphore acquired) | `task_id`, `partition`, `args`, `pid`, `labels`, `metadata` (source_offsets, slot, queue_wait_ms; stdin + stdin_truncated when `recorder.capture.stdin` is on, else stdin_bytes size marker) |
| `task_completed` | Subprocess finished with exit 0 | `task_id`, `duration` (spawn to exit, measured off the loop), `exit_code`, `stdout`, `stderr`, `pid`, `labels`, `metadata` (spawn_ms, run_started_at, exit_notice_ms, reported_run_seconds) |
| `task_failed` | Subprocess failed (non-zero exit, timeout, crash) | `task_id`, `duration`, `exit_code`, `pid`, `labels`, `metadata` (exception; stdin always stored, capped at `recorder.capture.max_stdin`; run_started_at, exit_notice_ms, reported_run_seconds when the task ran a process) |
| `task_complete` | `on_task_complete()` hook finishes for one task | `task_id`, `partition`, `duration`, `metadata` (output_message_count) |
| `message_complete` | `on_message_complete()` hook finishes for one source message | `partition`, `offset`, `duration`, `metadata` (task_count, succeeded, failed, replaced, output_message_count) |
| `window_complete` | `on_window_complete()` hook finishes for one arrange() window | `partition`, `duration`, `metadata` (window_id, task_count, output_message_count) |
| `produced` | Kafka payload delivered to output topic | `partition`, `offset`, `output_topic` |
| `sink_delivered` | Sink delivery succeeds | `metadata` (sink_type, sink_name, payload_count, duration) |
| `sink_error` | One sink delivery attempt fails (`attempt` 0: the circuit is open) | `metadata` (sink_type, sink_name, error, attempt, action: retry/skip/dlq/failed — `dlq` only when a record was written, `skip` also for a window result, `failed` when no record could be written) |
| `message_dlq` | A source message is written to the DLQ; no later delivery of it starts | `partition`, `offset`; for an HTTP request `origin='http'`, `request_id`, `partition=-1` and no `offset`; `metadata` (stage, sink, error, delivered_sinks, attempts) |
| `sink_not_started` | A result's delivery to a sink does not start: its message is on the DLQ, or a `depends_on` dependency failed | `partition`; for an HTTP request `origin='http'`, `request_id`, `partition=-1`; `metadata` (sink, reason: message_dlq/dependency_failed) |
| `committed` | Kafka offset committed | `partition`, `offset` |
| `assigned` | Partition assigned during rebalance | `partition` |
| `revoked` | Partition revoked during rebalance | `partition` |
| `partition_stalled` | Partition paused because an offset stalled (`dlq.on_send_failure=stall`) | `partition` |
| `runtime_health` | Event-loop health transition or periodic sample — see [Runtime Health](runtime-health.md) | `metadata` (kind: transition/sample, state, lag_ms, unit_count) |
| `runtime_stall` | Event loop resumed after a stall; carries the stack traces sampled while it lasted | `duration`, `metadata` (duration_ms, stacks: [{stack, location, count}], dropped_stacks, unit_count) |
| `runtime_lag_episode` | A degraded/stalled span ended (or hit `runtime_health.max_episode`); carries stacks aggregated across the whole span and a verdict — see [Runtime Health](runtime-health.md#lag-episodes-and-verdicts) | `duration`, `metadata` (duration_ms, peak_lag_ms, lag_sum_ms, cpu_ms, cpu_ratio, verdict: blocked/cpu_bound/starved/inconclusive, stall_count, sample_count, stacks, dropped_stacks, unit_count, plus optional evidence: cpu_throttled_ms, psi_cpu_some_avg10, load1) |
| `runtime_probe` | Opt-in stack probe (`runtime_health.stack_probe_enabled`) — a flight-recorder profiler sample, recorded regardless of health state | `metadata` (lag_ms, unit_count, stacks: [{stack, location, count}]) |
| `resource_sample` | Periodic snapshot of what the worker consumed — and what the host is fighting over ([Host Pressure](host-pressure.md)) — every `recorder.sqlite.state_interval` | `metadata` (rss_bytes, threads, open_fds, cpu_self_pct, cpu_children_pct, interval_s, load1, load5, psi_cpu_some_avg10, psi_io_some_avg10, psi_io_full_avg10, psi_mem_some_avg10, psi_mem_full_avg10, cpu_throttled_periods, cpu_throttled_ms, nfs_mounts: [{mount, ops, rtt_ms, retrans}] — each omitted when its source is unavailable; `cpu_children_pct` counts *reaped* subprocesses, so it is bursty; the first sample carries no cpu or delta fields) |
| `offload` | One `handler.offload()` call completed — CPU-bound hook work run on the offload thread pool; see [Offload](offload.md) | `partition` (NULL outside hooks), `offset` / `task_id` (annotation-style anchoring), `duration`, `metadata` (hook, function, queued, status: ok/error/cancelled, window_id, offsets, error) |
| `periodic_run` | Periodic task execution completes | `task_id` (task name), `duration`, `exit_code` (0=ok, 1=error), `metadata` (status, error; `system: true` for a framework task; `scope: "cluster"` and `slot` (the slot's epoch second) for a cluster run) |
| `webapp_request_received` | One HTTP request passed auth + rate-limit + body parsing and is about to dispatch to T1 | `origin='http'`, `client_name`, `request_id`, `metadata` (started_at, body_bytes) |
| `webapp_request_completed` | An HTTP request returned 200 to the caller | `origin='http'`, `client_name`, `request_id`, `duration`, `metadata` (status='ok', duration_ms) |
| `webapp_request_timeout` | T2's `asyncio.wait_for` tripped its `request_timeout` budget; client received a 504 | `origin='http'`, `client_name`, `request_id`, `duration`, `metadata` (duration_ms) |
| `webapp_request_rate_limited` | Per-client rpm window full; client received a 429 | `origin='http'`, `client_name`, `metadata` (rpm_limit, requests_in_window) |
| `webapp_request_auth_failed` | `Authorization` header missing or naming a non-configured token; client received a 401 | `origin='http'`, `metadata` (token_prefix -- redacted to first 4 chars) |
| `webapp_request_dropped_after_timeout` | T1 reached a cancellation gate after T2 had already 504'd. Marks pipeline work the framework managed to skip thanks to cooperative cancellation. | `origin='http'`, `client_name`, `request_id` |

For HTTP-origin rows, `partition=-1` is used as a synthetic partition value so they remain distinct from any real Kafka partition without requiring a separate table. Filter by `origin='http'` to see all webapp activity, or by `request_id=...` to walk the full lifecycle of one HTTP request (received → task_started → task_completed → completed/timeout/dropped).

Fields subject to [duration thresholds](#duration-thresholds): `args`, `stdout`, `stderr` are omitted for fast tasks below `recorder.capture.min_output_duration`. Events below `recorder.capture.min_event_duration` are not stored at all. Stdin content is gated by `recorder.capture.stdin` / `recorder.capture.max_stdin` rather than by duration — failed tasks always store it. See [Local Databases](local-databases.md#what-the-events-table-stores-and-the-knobs-that-trim-it) for the full retention-knob table.

!!! warning "Recorder upgrade story (delete pre-webapp DBs)"
    The webapp release added three columns to the `events` table --
    `origin`, `client_name`, `request_id` -- without a migration
    framework. Recorder DBs are observability-only, already rotated and
    disposable, so a one-shot delete on upgrade is preferable to a
    migration runner that exists only to add three optional columns.

    **On upgrade**, operators delete pre-existing per-worker recorder
    DBs in `storage.dir` before restarting workers. New rotation-cycle
    DBs include the new columns automatically.

    The framework detects the schema mismatch at startup: the recorder
    runs `PRAGMA table_info(events)` immediately after opening each DB
    and raises `RecorderSchemaError` when `origin` / `client_name` /
    `request_id` are missing. The exception propagates through
    `AppLifecycle._async_run` and aborts startup with the actionable
    next step in the message: delete the offending DB(s) under `storage.dir`
    and restart. Without this fail-fast check, pre-webapp DBs would
    surface a confusing `OperationalError: no such column` mid-request
    on the first HTTP event the recorder tried to write.

#### `worker_config` -- Worker identity {#worker_config-autodiscovery}

Single-row table written at startup (and after each rotation). Contains the full worker identity and configuration: `worker_name`, `cluster_name`, `ip_address`, `debug_port`, `debug_url`, `kafka_brokers`, `source_topic`, `consumer_group`, `binary_path`, `max_executors`, `task_timeout_seconds`, `max_retries`, `window_size`, `sinks_json`, `env_vars_json`. On a worker with the Kafka source disabled, `source_topic` and `consumer_group` are written **empty**; the column set does not change, so peers reading the file need no special case.

This table names the worker and cluster that wrote each file, for history, merges and archives.

!!! warning "Secrets are redacted before they reach disk"
    The recorder SQLite file is downloadable via the operator UI. To avoid
    publishing credentials through that path, three redactions are applied
    before any env data is written:

    - **`kafka_brokers`** — embedded credentials are stripped from SASL
      URIs. `SASL_SSL://alice:s3cret@host:9094` becomes
      `SASL_SSL://***:***@host:9094`. Host and port remain visible.
    - **`worker_config.env_vars_json`** — values for env var names matching
      secret patterns (`*PASSWORD*`, `*PASSWD*`, `*SECRET*`, `*TOKEN*`,
      `*KEY*`, `*API_KEY*`, `*CREDENTIAL*`, `*_DSN`, `*AUTH*`, `*PRIVATE*`,
      `*CERT*`, `*SALT*`) are replaced with `***`. For other names, any
      URL-shaped value has its embedded `user:pass@` credentials stripped.
    - **per-task `env` metadata** — `task.env` values written by your
      handler in `arrange()` are sanitized with the same patterns before
      the `task_started` event is recorded. The original task object is
      not mutated; only the recorded copy is redacted (the subprocess
      still receives the real values). This closes the last env-related
      leak into the UI.

    The contract is "aggressive redact, accept false positives":
    `PASSWORD_RESET_URL` is redacted because it matches `*PASSWORD*` even
    though a reset URL isn't a credential. Rename the var if you need the
    value visible. The `ExecutorConfig.env` (framework-level) values are
    **never written** to the recorder at all — they only reach the
    subprocess environment.

    These patterns are deliberately broader than
    `executor.env_inherit_deny` (see [Parent-env
    filtering](executor.md#parent-env-filtering-secrets-guard)): an
    over-matched redaction here just costs `***` in a downloadable debug
    DB, while an over-matched inheritance-deny pattern would withhold a
    variable a subprocess actually needs.

    This protects operators who add secret-named vars to `recorder.capture.env_vars`
    without thinking, DSNs passed through otherwise-innocuous var names
    (`UPSTREAM_URL`, `CACHE_URL`, etc.), and per-task env values that
    handlers assemble from inbound message payloads.

#### `worker_state` -- Periodic Snapshots

Appended every `recorder.sqlite.state_interval` (default `10s`). Captures a point-in-time snapshot: `uptime_seconds`, `assigned_partitions`, `partition_count`, `pool_active`, `pool_max`, `total_queued`, cumulative counters (`consumed_count`, `completed_count`, `failed_count`, `produced_count`, `committed_count`), `paused` flag, `health_state` and `loop_lag_ms` from the [runtime health monitor](runtime-health.md).

### File Layout and Rotation

The live database file follows the naming pattern:

```
{storage.dir}/{cluster}/{worker_name}/{worker_name}-{timestamp}.db
```

For example: `/tmp/drakkar/search/worker-1/worker-1-2026-04-06__14_55_00.db`. Only the worker that writes it opens it.

**Rotation**: every `recorder.sqlite.rotation_interval` (default `1h`), the recorder opens a new timestamped DB file before closing the old one (no query gap). The worker config is re-written to the new file. The old file is then **sealed**: switched to a rollback journal, given a meta file with its statistics, and closed. A background job moves it into the history folder of the worker's cluster:

```
{storage.shared_dir}/history/{cluster}/{worker_name}-{timestamp}.db
{storage.shared_dir}/history/{cluster}/{worker_name}-{timestamp}.db.meta.json
```

`storage.shared_dir` empty means `storage.dir`. `{cluster}` is the cluster name with every character outside `[a-zA-Z0-9_-]` replaced by `_` (`default` when it is empty). A clean stop seals the live file too, and moves it while shutdown time remains. A file whose seal or move fails stays in `storage.dir` and is retried on every rotation tick. The full rules — the cross-filesystem copy, the backlog, leftovers of a crashed process, the meta file format — are in [Local Databases → Sealing](local-databases.md#sealing-and-the-history-folder). Rotation itself deletes nothing — see [Archiving](#archiving), next.

### Archiving

A periodic
**archive pass** folds each finished UTC time window's sealed files into
one compressed, merged database per cluster
(`<cluster>-<from>__<to>.db.gz`, in the same cluster history folder), and
deletes only the sealed files it successfully merged — see
[Local Databases → Archiving](local-databases.md#archiving) for the full
byte-level spec. The short version:

- **Defaults** (`recorder.sqlite.rotation_interval: 1h`, and
  `recorder.archive` `enabled: true`, `window: 1d`,
  `due_after: 2d`, `retention: 30d`): files rotate hourly and
  are sealed into the history folder; a UTC day's window becomes due
  exactly 48h after it closes and is archived at the next archive slot
  after that (hourly by default), merging its sealed files into one `.db.gz` per cluster and deleting
  them and their meta files. That 48h due delay is the safety margin, not a
  bug — but it means a single recorder file's lifespan, counted from when
  it was *created* rather than from window close, can reach up to ~72h (up
  to 24h as part of its own window, plus the ~48h due delay after that
  window closes). Archives themselves are deleted once their window ended
  more than `retention` ago; `0` keeps them forever and warns at
  startup. Track the footprint with `drakkar_recorder_archive_bytes`.
- **The pass has [two scopes](local-databases.md#archive-scopes).** With
  `autodiscover.enabled` and `storage.shared_dir` set, one worker runs the pass
  at each archive slot over the whole cluster: the pass is the system
  [cluster task](periodic-tasks.md) `history.archive`, and every other worker
  updates the footprint gauges at that slot. In every other case each worker
  runs the pass at each slot over its own files, with no election, and writes
  `<cluster>-<worker>-<from>__<to>.db.gz`. The slots come from
  `recorder.archive.interval` (default `1h`, aligned to the epoch) or
  `recorder.archive.cron`. The pass runs in the background, on the history
  folder of its own cluster only. The rotation only seals; it starts no pass.
- **Windows key on file start time, not event time.** A sealed file
  belongs to the window holding its own start timestamp; an archive can
  carry a handful of events timestamped slightly past its own window end.
- **Only sealed files are candidates.** Every `*.db` in a cluster history
  folder is sealed and never changes, so the pass needs no in-use check and
  reads its sources with `immutable=1`.
- **One archiver per slot in the cluster scope.** At each slot every worker of the cluster asks
  the election provider for the lease of `<cluster>/history.archive`; one
  wins, and the others skip the slot and lose nothing. With the default
  `filesystem` provider the lease is the file
  `<storage.shared_dir>/tasks/<cluster>/history.archive.lock`, made with an
  exclusive create. A lease nobody refreshed for
  `recorder.archive.lease` (default `1m`) belongs to a dead archiver
  and is taken over (`periodic_lock_taken_over`, with the fields `provider`
  and `scope`); a fresh lock that two racing takers moved aside is put back,
  and `periodic_lock_put_back_failed` reports when a third worker's lock
  came first. Ages are measured on the file server's clock. Right before it
  publishes, and before it deletes a source, the holder checks that the
  lease is still its own; if another worker took it over, the pass
  publishes and deletes nothing (`recorder_archive_fenced`, with the fields
  `provider` and `reason`: `taken_over`, or `unconfirmed` when the election
  provider could not confirm the lease). `autodiscover` selects
  another provider (`postgres`, `redis` and `consul` are built in), and every
  worker of a cluster must use the same. With `redis` or `consul` an expired
  lease is a deleted key, so no `periodic_lock_taken_over` line is written;
  the next slot finds the scope free and takes it.
  See [Local Databases — The election provider](local-databases.md#the-election-provider).
- **Sources die last.** A sealed file is deleted only once its archive is
  durably on disk. A file the merge cannot read is renamed to
  `<name>.unreadable` instead of being deleted, and a pre-existing
  archive at the final name is folded into the new one rather than
  overwritten. A pass that took the lock and failed counts in
  `drakkar_recorder_archive_failures_total`.
- **Merge results expire.** The pass also deletes UI merge results in
  `<storage.shared_dir>/merged/` older than `recorder.archive.merged_retention`
  (default 32; `0` keeps them), logging `recorder_merged_pruned`.
- **Opting out** (`recorder.archive.enabled: false`) disables the pass
  entirely — nothing then deletes sealed recorder files or merge results
  automatically, and a startup log line (`recorder_archiving_disabled`,
  field `folder`) says so.

```yaml
storage:
  dir: /var/lib/drakkar
  shared_dir: /mnt/fleet             # history folders live here; '' = storage.dir
recorder:
  sqlite:
    rotation_interval: 1h            # a whole number of hours
  archive:
    enabled: true                    # merge sealed files into windowed .db.gz archives
    window: 24h                      # one archive per cluster per UTC day; must be >= rotation_interval
    due_after: 48h                   # archive a window this long after it ends
    retention: 30d                   # delete archives older than this; 0 = keep for ever
    merged_retention: 32d            # delete UI merge results older than this; 0 = keep them
```

Archives are downloaded from the same place as the other history files —
**Debug → Databases tab** — backed by `GET /api/v1/debug/archives` (the
archives of every cluster folder, each with `folder` and `path`) and
`GET /api/v1/debug/download/{path}`, where `path` is relative to
`storage.shared_dir` (for example
`history/search/search-2026-10-12_00-00__2026-10-13_00-00.db.gz`). See
[Merging Databases](#merging-databases) below for why archives never
appear as merge candidates. A downloaded archive is a plain gzip file:
`gunzip` it and the result is an ordinary merged recorder SQLite
database, readable with the `sqlite3` CLI or any tool that already reads
a merged `.db`.

Two operational gotchas worth knowing:

- **A cluster with no running worker is never archived or expired.** Each
  worker works only on its own cluster's history folder, so a retired
  cluster's sealed files and archives stay until you delete them.
- **A corrupt file at an archive's own final name stalls that window.**
  Every pass that considers the window fails to decompress it, logs
  `recorder_archive_failed`, and retries next tick — indefinitely,
  without touching the (safe, undeleted) sealed sources. Recovery is
  manual: remove or rename the bad file.

### Buffer Mechanics

Events are not written to SQLite immediately. They wait in the **intake**,
an in-memory buffer (`collections.deque` with `maxlen=recorder.max_buffer_rows`, default
50,000). Two background loops then write them, each on the tick of
`recorder.flush_interval` (default `5s`):

- **Stage M** (the memory loop, background task `memory_flush`) takes the
  intake in chunks, gives each row its id, and writes the chunk to the
  [memory store](recent-events.md). With provider `sqlite` it then puts
  the `(id, row)` pairs in the **file queue**, which also holds at most
  `recorder.max_buffer_rows` rows. After the write it runs one eviction pass.
- **Stage F** (the file loop, background task `file_flush`, `sqlite` only)
  writes the file queue to the recorder file in transactions of up to 5000
  rows.

The two loops have their own locks, so a slow or hung disk holds only stage
F. The memory store keeps taking events, and the UI keeps reading them. This
design:

- Avoids per-event disk I/O (thousands of events/sec would bottleneck SQLite)
- Keeps the main event loop fast (recording an event is a deque append, ~1us)
- Keeps the recent events readable even when `storage.dir` is empty or the
  disk stalls

When the intake or the file queue is full, its oldest rows are evicted.
Drops are not silent: `drakkar_recorder_dropped_events_total`
([see Recorder metrics](#recorder)) counts the evictions of both, and
`drakkar_recorder_buffer_size` shows the intake plus the file queue. While
the disk stalls, up to twice `recorder.max_buffer_rows` rows wait in memory: a full intake
and a full file queue. While stage M is held back (the memory store is busy
or locked), the intake fills, and its overflow counts as dropped. Alert on
sustained non-zero drops and raise `recorder.max_buffer_rows` or shorten
`recorder.flush_interval` when they appear.

Stage M writes in chunks of 5000 rows, and the loop is handed back between
chunks: building a row tuple costs one lookup per column, so an unchunked
write of tens of thousands of events would stall the loop for tens of
milliseconds and show up as loop lag in [runtime health](runtime-health.md).
Stage M also starts early, before `recorder.flush_interval` elapses, once the
intake is half full, and stage F once the file queue is half full. A burst
is written out rather than left to reach `recorder.max_buffer_rows` and evict.

Stage M starts early on **size** as well as on count, once the buffered
payload passes roughly 64 MiB. `recorder.max_buffer_rows` bounds the buffer by event count,
which says nothing about how much memory it holds: with `recorder.capture.output` on,
every task at or above `recorder.capture.min_output_duration` has its whole stdout and
stderr buffered, and `executor.max_stdout` is unlimited by default. A
handler producing hundreds of kilobytes per task therefore reaches gigabytes
of resident buffer long before it reaches 25,000 events. The size trigger is
not configurable: it is a safety floor rather than a tuning knob, and writing
early is never wrong. Nothing is truncated — the stored row keeps the whole
output, which is what the task-detail page shows.

Live WebSocket clients never receive captured output — the live views show
`stdout_size`, not the text — and the queued frames do not hold it either: the
output is dropped when the event is handed to the fan-out, not when it is
encoded. Otherwise a browser tab on a slow link could pin up to 10,000 events'
worth of full output in its subscriber queue, on top of the buffer above.

A memory chunk that fails because the store is busy or locked goes back to
the front of the intake with its ids and is written on the next tick
(`recorder_memory_write_retry`). Any other failed memory write deletes the
memory rows up to that chunk (`recorder_memory_write_failed`); with `sqlite`
the chunk still goes to the file, with `none` it is lost. See
[Recent Events in Memory](recent-events.md#log-events).

The file chunk is the unit of loss on disk. A file chunk that still fails
after `recorder.sqlite.max_flush_retries` consecutive attempts is dropped from the file and
`drakkar_recorder_flush_batches_dropped_total` ticks; the rest of the file
queue stays and is retried on the next tick. Size `recorder.max_buffer_rows` for how long a
database stall you want to ride out:

    recorder.max_buffer_rows >= event_rate x recorder.flush_interval x (recorder.sqlite.max_flush_retries + 1)

At 5000 events/s with the defaults (5 s interval, 3 retries) that is 100 000,
twice the default `recorder.max_buffer_rows`. Raise it if your workers must survive a
15-second SQLite stall — the usual cause is a network filesystem — without
losing a chunk.

With `recorder.enabled: false` there is no file
queue and no stage F: events are buffered and written to the memory store
only. With a worker whose `ui.enabled` is false and the recorder off, there is
no recorder object at all. With `recorder.capture.events: false` there is no memory store and no loop, so
events are streamed to the live WebSocket view and not buffered at all: the
buffer gauge stays at 0 and the drop counter never moves.

With provider `postgres` there is no stage F either. Stage M hands every
row it wrote to the Postgres queue, and stage P writes that queue to the
table on a thread of its own (see
[Recorder History in Postgres](recorder-postgres.md#while-postgres-is-down)).

A loop iteration that raises logs `recorder_flush_loop_iteration_failed`
with `stage` `memory`, `file` or `postgres`, and the loop goes on.

On shutdown, stage M and then stage F (or the final stage P) run one final
time before the database closes. A failure there logs
`recorder_final_flush_failed` with `stage` `memory`, `file` or `postgres`.
The memory store closes last.

### Recorder history in Postgres

With provider `postgres` the recorder also writes these log events
(category `recorder` unless named). The metrics are in the
[Recorder](#recorder) table above. See
[Recorder History in Postgres](recorder-postgres.md) for the mode.

| Event | Level | Fields | When |
|---|---|---|---|
| `recorder_postgres_rows_dropped` | warning | `reason` (`queue_full`, `rejected`, `no_partition`, `shutdown`), `rows`, `error` (empty for `queue_full` and `shutdown`), `suppressed` | Rows were dropped. At most one line per 60 seconds per reason; the rows of the lines left out are added to the next one. |
| `recorder_postgres_copy_rejected` | warning | `sqlstate`, `error`, `rows`, `final_failures`, `suppressed` | A COPY failed with a final error. The chunk stays for one more try, and is dropped at its second final error. At most one line per 60 seconds. |
| `recorder_postgres_partitions_changed` | info | `table` (schema-qualified), `created`, `dropped` (by the retention), `dropped_for_size` (by the size cap), each sorted partition names | An upkeep created or dropped partitions. |
| `recorder_postgres_retention_mismatch` | warning | `table`, `stored_retention_hours` (a number, or `null` when the table has no valid comment), `configured_retention_hours` | The table comment differs from `recorder.postgres.initial_retention`, or there is none. Once per process, at the first upkeep in which this worker holds the try-lock. |
| `ui_read_postgres_failed` (category `debug`) | warning | `endpoint`, `reason`, `error`, `suppressed` | A history search failed. `reason` is `timeout` (the time limit passed), `error` (the connection or the statement failed), `busy` (two searches were already running; the search did not start) or `close` (the search completed and answered its rows, but the connection did not close cleanly). Every reason but `close` answers 503. At most one line per 60 seconds. |

The recorder calls also log the provider events with `feature=recorder` and
`category=recorder`: `provider_started` (stage P started), `provider_call_failed`,
`provider_breaker_opened` and `provider_breaker_closed`. `recorder_started`
carries `postgres_connection` and `postgres_table` (schema-qualified).

### Merging Databases

The UI's database section (`/debug`) allows selecting multiple
history files — sealed files and earlier merge results — and merging them
into a single file. This is useful for:

- **Post-mortem analysis** -- combine files from multiple workers and
  time periods into one queryable database
- **Cross-worker correlation** -- merged files contain events from
  all selected workers, with `worker_name` preserved in the
  `worker_config` table
- **Archival** -- download a merged file for offline analysis with
  any SQLite tool

`POST /api/v1/debug/merge` takes paths relative to `storage.shared_dir`
(`history/<cluster>/<name>.db` or `merged/<name>.db`). The merge process
copies all events from the selected files into a new file
`<storage.shared_dir>/merged/merged-<YYYY-MM-DD__HH_MM_SS>-<16 hex>.db`; the
answer carries its `path`. It does not remove duplicates. The `workers`
table lists every source worker; a selected merged file keeps its own
workers, events and states. The file is written under a hidden temporary
name and appears in the database list only when it is complete. It can be
downloaded. `merged/` is created with mode `0700`, and the archive pass
deletes merge results older than `recorder.archive.merged_retention`
(default 32 days; `0` keeps them).

A worker's own files in `storage.dir` — the live file and the backlog —
are not merge sources and are not downloadable: use a file once it is
sealed into the history folder.

**Archives are never merge candidates.** The Databases tab's archives
(see [Archiving](#archiving) above) are read-only — no selection
checkboxes, no path into `/api/v1/debug/merge`. `POST /api/v1/debug/merge`
accepts only `.db` paths, so an archive path passed to it by hand answers
`400`.

---

## Duration Thresholds

Drakkar provides four independent duration thresholds that control what gets logged, broadcast, and stored. These are essential for high-throughput workers where fast tasks would otherwise flood every observability channel.

```yaml
logging:
  min_task_duration: 500ms
ui:
  ws_min_duration: 500ms
recorder:
  capture:
    min_event_duration: 0
    min_output_duration: 500ms
```

### `logging.min_task_duration` (default: 500ms)

Controls which completed/failed tasks produce a structlog entry. Tasks finishing faster than this threshold are silent in the log stream. Keeps logs focused on slow or problematic tasks.

### `ui.ws_min_duration` (default: 500ms)

Controls which tasks appear in the live UI (`/live` page). When a task starts, its WebSocket broadcast is deferred. If the task completes before the threshold, neither the start nor the completion event is sent to connected browsers -- the task is invisible in the live view.

**Exception**: failed tasks are always sent to WebSocket regardless of duration. This ensures failures are never hidden.

### `recorder.capture.min_event_duration` (default: 0)

Controls which tasks are persisted to the flight recorder. When set to a value greater than 0, tasks completing faster than the threshold are not written to the `events` table. Set to `0` to store all events (the default).

This is useful for workers that process millions of fast tasks where full event recording would consume excessive disk space.

### `recorder.capture.min_output_duration` (default: 500ms)

Controls whether `args`, `stdout`, and `stderr` are stored for a task. Tasks completing faster than this threshold have their output omitted from the event record -- the event itself is still recorded (subject to `min_event_duration`), but without the potentially large output fields.

Reduces storage for fast tasks where the output is uninteresting.

---

## Worker Autodiscovery

A shared folder between workers is **not required** for Drakkar to
function. Each worker operates independently -- its own Kafka consumer,
executor pool, sinks, and flight recorder work fine with a local
directory.

You can also disable the flight recorder database entirely by setting
`storage.dir: ""` (empty string). The web UI and WebSocket live
streaming still work (events are held in memory only), but these
features are disabled: event history (`/history`), message trace,
label search, database download/merge and periodic task history. Worker
discovery needs a card folder for the [fleet registry](fleet.md): with
`storage.dir: ""` set `storage.shared_dir` (or another registry provider) to keep
the workers list. The live pipeline view (`/live`) and dashboard counters
continue working since they read from in-memory state.

When workers share one `storage.shared_dir` (e.g., a shared NFS mount,
Kubernetes PVC, or Docker volume; with `storage.shared_dir` empty, one shared
`storage.dir`), several additional UI features become available:

- **Worker switcher** -- jump between worker UIs from a dropdown in the
  nav bar, without remembering ports or IP addresses (the switcher comes
  from the [fleet registry](fleet.md), whose default card folder is in
  the shared folder)
- **Cross-worker message trace** -- trace a message by partition:offset
  through the sealed history of every worker in the cluster
- **Database merge** -- combine sealed recorder files from multiple
  workers into a single SQLite file for post-mortem analysis
- **Cluster-wide database browser** -- see every cluster's history
  folder (sealed files, archives, merge results) in one view

Without a shared folder, each worker's UI only sees its own
data. All other functionality (processing, sinks, metrics, logging)
works identically.

### How It Works

Workers find each other through the [fleet registry](fleet.md):

1. Each worker publishes a small card -- worker name, cluster, the URL of
   its API server, state (`starting`, `running`, `draining`), release --
   right after its API server starts, at every state change, and every
   `autodiscover.heartbeat` (default 10).
2. With the `filesystem` provider the card is a file in
   `<storage.shared_dir>/fleet/workers/`, so workers that mount the same folder
   see each other.
3. Every `autodiscover.refresh` (default 15) a worker reads the
   other cards into memory. `GET /api/v1/workers` answers from that
   snapshot; it never opens a peer's database.
4. Workers are grouped by cluster name -- only workers in the same cluster
   appear together. Unclustered workers show in a separate group.
5. At a clean stop a worker deletes its card.

### Liveness

A crashed or OOM-killed worker never deletes its card, so it stays listed
until the card is pruned (`autodiscover.prune_after`, default one
day). To tell such workers apart from healthy ones, every row of
`/api/v1/workers` carries two liveness fields:

- `online` -- `true` when the peer's card is no older than
  `autodiscover.offline_after` (default 60), `false` when it is
  older, and `null` when the provider cannot tell (a worker whose own first card
  write has not succeeded yet). The worker
  serving the request always reports itself online. A provider whose
  service decides liveness itself gives its own verdict, which replaces the
  age rule.
- `last_seen_ts` -- the serving worker's current time minus the card's
  age, in epoch seconds; `null` when the age is unknown.

The age is measured on the file server's clock, so clock skew between
worker hosts does not matter (see [Liveness](fleet.md#liveness)). An
offline worker stays listed -- its recorded history is still reachable --
it is just marked as gone.

### Worker Switcher

The UI navigation bar includes a worker dropdown that lists all
discovered workers, grouped by cluster. Clicking a worker navigates to the
URL on its card: `api.public_url` if configured, otherwise
`http://<outbound ip>:<api.port>/`. A worker that is not `running` shows its
state, and every worker shows its release when the card carries it. The
dropdown refreshes every 10 seconds.

### Setup

The minimal config for worker discovery:

```yaml
# every worker mounts the same folder; the registry keeps its cards there
storage:
  shared_dir: "/shared"
# optional: set a cluster name to group workers
cluster:
  name: "my-cluster"
```


In Kubernetes, use a shared PVC:

```yaml
volumes:
  - name: recorder-shared
    persistentVolumeClaim:
      claimName: drakkar-recorder

# mount on all worker pods (the mount path may differ per pod)
volumeMounts:
  - name: recorder-shared
    mountPath: /shared
```

In Docker Compose, use a named volume:

```yaml
volumes:
  shared: {}

services:
  worker-1:
    volumes:
      - shared:/shared
  worker-2:
    volumes:
      - shared:/shared
```

### Cross-Worker Trace

The trace feature on the `/debug` page searches for a message's lifecycle
in the current worker's files and, on request, in the sealed history of
every worker in the cluster. Two search modes are available:

**By partition:offset** -- given a partition and offset, finds the
consumed event and all related events (task_started, task_completed,
task_failed, task_complete, message_complete, produced, committed).

**By label** -- given a label key and value (e.g., `request_id=abc-123`),
finds all tasks whose [labels](handler.md#task-labels) match and returns
their full event lifecycle. This is useful when you know a business
identifier but not which partition or offset it landed on. Label search
inputs appear automatically on the `/debug` page for each label key
found in the database.

What each mode searches:

- **By partition:offset**, by default: the current worker's own files --
  its live database and its closed files in `storage.dir` that are not yet
  sealed into the history folder.
- **By partition:offset with `history=1`**: only the sealed files in the
  history folder of the worker's own cluster
  (`<storage.shared_dir>/history/<cluster>/`), newest first. The search stops after
  `recorder.search.max_files` files (default 256) or
  `recorder.search.timeout` (default `10s`). Sealed files never change,
  so the search opens them with `immutable=1` and takes no locks.
- **By partition:offset with `history=1` on a provider `postgres`
  worker**: the rows of the worker's own cluster in the Postgres table of
  its prefix, never the history folder. The answer is complete or a 503
  `{"detail": "history search failed"}`; see
  [Recorder History in Postgres](recorder-postgres.md#the-history-search).
- **By label**: the current worker's live database only.

No worker opens another worker's live database. Events of other workers
come from their sealed files, or from the workers themselves: the UI
sends the trace, label-trace and databases requests to every worker of
the cluster that is not offline, and lists the workers that did not
answer as unreachable. Those reads are cross-origin. With `ui.auth_token`
set, list the origin of every worker in `ui.allowed_origins`, or only
the serving worker answers (see
[Cross-origin reads](security.md#cross-origin-reads)).

Each returned event carries a `worker_name` field identifying which
worker processed it. Labels are stored as JSON in the `labels` column
with a partial index (`WHERE labels IS NOT NULL`) for efficient
filtering.

---

## Periodic Tasks

The `@periodic` decorator runs handler methods on a schedule, next to the
main processing loop: on every worker (`scope='worker'`, the default) or on
one worker of the cluster per slot (`scope='cluster'`). The schedules, the
slot rule, the overrides and the definition hash are on
[Periodic Tasks](periodic-tasks.md); this section lists what a periodic
task makes visible.

```python
from drakkar.periodic import periodic

class MyHandler(BaseDrakkarHandler[MyInput, MyOutput]):
    @periodic(60)
    async def reload_country_lookup_table(self):
        """Reload the lookup table from the database every minute, on every worker."""
        self.country_lookup = await load_country_lookup()

    @periodic(cron='0 * * * *', scope='cluster', on_error='stop')
    async def refresh_shared_exchange_rates(self):
        """Refresh the shared table once an hour, on one worker of the cluster. Stop if it fails."""
        await copy_exchange_rates_into_reporting_table()
```

### Behavior

- **Discovery**: at startup, after `on_startup` and before the recorder,
  the registry and the sinks start, the framework finds the methods
  decorated with `@periodic`. Only `async` functions are accepted --
  decorating a sync function raises `TypeError`.
- **Scheduling**: each periodic task runs in its own `asyncio.Task` within
  the main event loop. Runs of one task never overlap on one worker.
- **Lifecycle**: periodic tasks are started after [on_ready()](handler.md#on_ready)
  completes and cancelled during graceful shutdown (before partition
  processors are drained). A cluster task's lease is not released at
  shutdown; it ends by itself.
- **Error handling**:
    - `on_error='continue'` (default) -- the exception is logged, and the task continues scheduling on the next interval.
    - `on_error='stop'` -- the exception is logged, and this specific periodic task is permanently cancelled. Other periodic tasks and the main processing loop are unaffected.

### Log events {#periodic-log-events}

All in category `periodic`.

| Event | Level | Meaning |
|-------|-------|---------|
| `periodic_task_started` | info | A task's loop started. Fields `periodic_task`, `scope` (`worker` or `cluster`), and `interval_seconds` or `cron`. |
| `periodic_task_failed` | error | A run raised; carries the exception. |
| `periodic_task_stopped` | warning | A run raised and the task has `on_error='stop'`: its loop ended. |
| `periodic_loop_ended` | error | A task's loop ended on an error outside a run (the slot math, a skipped slot, the lease setup); the task does not run again on this worker until it restarts. Fields `name`, `error`; carries the exception. |
| `periodic_slot_skipped` | debug or warning | A cluster slot this worker does not run. Fields `name`, `slot` (epoch second), `outcome`. Debug for `held_elsewhere` (the normal case on every worker but one). Warning for `late` (field `late_seconds`) and `election_failed` (field `error`: the operation and its outcome, such as `start timeout`, `acquire error` or `acquire skipped_open`). At most one warning line per task per 60 seconds; `suppressed` counts the lines left out since the last one. |
| `periodic_lease_lost` | warning | A refresh of a running slot's lease did not confirm it. Fields `name`, `slot`, `reason` (`taken_over`: another worker holds it; `unconfirmed`: the provider did not answer). Once per slot; the run goes on. |
| `periodic_lease_refresh_failed` | error | The lease refresher of a running slot raised outside a provider call. Field `scope`. |
| `periodic_skip_hook_failed` | error | The framework's hook for a skipped slot raised (for the archive: starting the footprint update). Fields `name`, `slot`, `outcome`. The slot is skipped either way. |
| `periodic_cluster_mismatch` | warning | An online peer of this worker's cluster has another [definition hash](periodic-tasks.md#definition-hash). Fields `peer`, `own_hash`, `peer_hash`. Once per peer and hash; again when the peer leaves the registry listing and comes back. |
| `cluster_name_empty` | warning | `autodiscover` is on and the cluster name is empty, so the lock scope of cluster tasks is `default`. Field `scope`; category `lifecycle`. Once at startup. |
| `periodic_lock_taken_over` | warning | A worker took over the lease of a cluster task (the archive included) whose holder died. Fields `provider`, `scope`, `previous_worker`, `previous_instance`, `age_seconds`. Written by the `filesystem` and `postgres` election providers only: the `redis` and `consul` providers write no such line, because an expired lease is a key that the server deleted. |
| `periodic_lock_put_back_failed` | warning | Two workers took over one `filesystem` lock at once, and a third worker's lock came first, so the lock moved aside could not be put back. Fields `scope`, `folder`, `error`. The holder of the moved lock is fenced at its next check. |
| `periodic_election_stop_failed` | warning | Stopping the election at shutdown raised. Field `error`. The rest of the shutdown still runs. |

The election's provider calls are counted and logged like every provider
call (`drakkar_provider_calls_total{feature="election"}`, `provider_started`
and `provider_call_failed` in category `periodic`); see
[Fleet registry and provider operations](#fleet-registry-and-provider-operations).
