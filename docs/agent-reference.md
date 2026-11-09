# Agent Reference

This page is generated from the agent knowledge registry by `drakkar-agent reference`. Do not edit it by hand: run `just agent-reference`.

A worker with `agent.enabled: true` serves the same entries on `GET /api/v1/agent/manual`. See [Agent Surface](agent.md) for the routes, the config keys and the handler notes.

PromQL queries keep the placeholders `{{worker}}` and `{{cluster}}`. A worker fills them from `ui.prometheus.worker_label` and `ui.prometheus.cluster_label`.

Registry version: `628b58b14c3606fa`.

## Concepts

### `concept:pipeline`

A worker reads messages, turns them into tasks, runs the tasks as subprocesses, delivers the results to sinks and commits the offsets.

A worker has one control loop. The steps of one message are:

1. A source reads the message (the Kafka source or the HTTP source).
2. The partition loop puts the message in a window.
3. The `arrange` hook of the handler turns the window into tasks.
4. The executor pool runs each task as a subprocess, at most `executor.max_executors` at one time.
5. The result hooks (`on_task_complete`, `on_message_complete`, `on_window_complete`) turn the output into sink payloads.
6. The sinks deliver the payloads. A failure goes to the DLQ or is skipped, by the sink policy.
7. The worker commits the offset of the message when every task and delivery of the message is done.

The framework owns steps 1, 2, 4, 6 and 7. The handler owns steps 3 and 5. The work itself runs in the subprocess.

**Principles:**
- A hook must not block; a blocking call delays its partition and can stop every partition of the worker.
- The worker commits an offset only after the message is fully done.
- Every limit of the pipeline is a config key; read the effective config before you change one.

**Related:** `concept:window`, `concept:arrange`, `concept:task`, `concept:offsets`, `concept:sinks-and-delivery`, `concept:at-least-once`, `route:GET /api/v1/dashboard`

### `concept:window`

A window is a group of 1 to executor.window_size messages of one partition that the arrange hook gets in one call.

Each partition loop takes messages from its queue into a window. The size of a window depends on the free room of the pending budget: during the first 3 windows of a partition a window holds one message; after that the worker sizes the window from the average tasks and bytes per message, between 1 and `executor.window_size`.

A partition loop does not wait for a window to finish before it takes the next one. Many windows of one partition can be in flight at one time. The offsets of a window are committed in order, so a slow message holds the commit of the messages after it.

**Principles:**
- A window never holds messages of two partitions.
- The window size is an upper bound, not a target; small windows are normal when the budget is nearly full.

**Related:** `concept:pending-budget`, `concept:arrange`, `config:executor.window_size`, `metric:drakkar_window_messages`, `event:arranged`, `event:window_complete`

### `concept:arrange`

The arrange hook of the handler maps a window of messages to a list of tasks; it decides how much work each message makes.

The worker calls `arrange` once per window with the messages and a view of the tasks that are pending on the partition. The hook returns a list of tasks. Each task names the source offsets it covers. A message with no task is done at once.

The time of the hook is in `drakkar_handler_duration_seconds{hook="arrange"}` and in the `arranged` event. A slow `arrange` slows the whole partition, and a blocking `arrange` stops the event loop.

**Principles:**
- The arrange hook must not block; wait for I/O without blocking the worker, and move heavy CPU work into tasks or the offload pool.
- Every task must name the source offsets it covers, or the offsets are committed before the work is done.

**Related:** `concept:window`, `concept:task`, `metric:drakkar_handler_duration_seconds`, `event:arranged`

### `concept:task`

A task is one subprocess run with its arguments, optional stdin, labels and the source offsets it covers.

The executor pool runs tasks with at most `executor.max_executors` subprocesses at one time. A task that waits for a slot is counted in `drakkar_executor_pool_waiting`.

A task ends in one of these ways: exit code 0 (completed), another exit code, a timeout after `executor.task_timeout` (the worker kills the process group), or a launch failure. On a failure the `on_error` hook decides: retry (up to `executor.max_retries`), skip, send the message to the DLQ, or replace the task with other tasks. With no hook the answer is `executor.on_failure`.

A task can carry a precomputed result; then no subprocess runs.

**Principles:**
- A task is settled only after its result hooks and their deliveries end; until then it counts in the pending budget.
- The output of a task is kept in memory until its hooks end; cap it with executor.max_stdout and executor.max_stderr.

**Related:** `concept:pending-budget`, `config:executor.max_executors`, `config:executor.task_timeout`, `config:executor.on_failure`, `metric:drakkar_executor_tasks_total`, `event:task_started`, `event:task_completed`, `event:task_failed`

### `concept:offsets`

Each partition keeps a watermark; the worker commits the highest offset below which every message is done.

The worker registers the offsets of a window as pending when it takes the window. A message is done when all its tasks and deliveries are done. The watermark of a partition is the offset of the oldest pending message. The worker commits that watermark to Kafka.

`drakkar_offset_lag` is the number of pending offsets of this worker. `drakkar_consumer_lag` is the broker lag: the high watermark of the partition minus the committed offset.

A message that never finishes holds the watermark: all later messages of the partition stay uncommitted, even when they are done.

**Principles:**
- One stuck message stops the commits of its partition.
- The worker never commits past a message that is not done.

**Related:** `concept:at-least-once`, `metric:drakkar_offset_lag`, `metric:drakkar_consumer_lag`, `metric:drakkar_offsets_committed_total`, `event:committed`, `recipe:lag-growing`

### `concept:at-least-once`

Every message is processed at least once; after a crash, a revoke or a drain timeout, messages after the last commit are processed again.

The worker commits an offset only after the message is done. When a worker stops before a commit, the next owner of the partition reads the messages after the last committed offset again. So a sink can receive the same result twice.

On a revoke or a stop, the worker drains: it waits up to `executor.drain_timeout` for the work in flight, then commits. Work that does not finish in time is cancelled and its results are not delivered (`drakkar_suppressed_zombie_deliveries_total`), because the next owner processes those messages again.

**Principles:**
- Sinks must accept a repeated result; make writes idempotent (upserts, keys).
- Do not raise executor.drain_timeout above what the Kafka rebalance allows (sources.kafka.max_poll_interval).

**Related:** `concept:offsets`, `config:executor.drain_timeout`, `metric:drakkar_suppressed_zombie_deliveries_total`, `metric:drakkar_drain_timeout_hit_total`, `metric:drakkar_delivery_stalled_offsets_total`

### `concept:pending-budget`

One budget per worker limits the pending tasks and the pending bytes; a partition loop takes a new window only when the budget has room.

The budget counts the tasks that are created and not settled (waiting for a slot, running, or in their hooks and delivery), and the bytes they hold: task stdin plus the output of results that hooks or deliveries still use. The limits are `executor.max_pending_tasks` and `executor.max_pending_bytes`.

A partition loop checks the room and reserves the estimate of its next window before it calls `arrange`. While the budget is full, loops wait in a first-in, first-out list; the wait is counted in `drakkar_window_admission_wait_seconds_total`. The HTTP source uses the same budget.

A full budget is the normal state of a busy worker. It is a problem only when the executor pool is not busy at the same time.

**Principles:**
- The budget bounds the memory of the worker; raising it raises the memory the worker can use.
- A full budget with an idle executor pool means hooks or deliveries are slow, not the subprocesses.

**Related:** `concept:backpressure`, `config:executor.max_pending_tasks`, `config:executor.max_pending_bytes`, `metric:drakkar_pending_tasks`, `metric:drakkar_pending_bytes`, `metric:drakkar_window_admission_wait_seconds_total`, `rule:budget-saturated`

### `concept:backpressure`

When the partition queues hold sources.kafka.max_queued_messages messages, the worker pauses Kafka fetching until they fall to half.

The Kafka source counts the messages that wait in the queues of live partition loops. At `sources.kafka.max_queued_messages` it pauses fetching for every assigned partition and sets `drakkar_backpressure_active` to 1. It resumes when the count is at or below half of the limit and no operator debug pause holds the source.

The poll call goes on while the source is paused, so the Kafka group does not evict the worker. Backpressure is the normal way a busy worker slows its input: a full pending budget fills the queues, and the full queues pause the fetch.

**Principles:**
- Backpressure protects the memory of the worker; it is not an error.
- Lag grows while backpressure is on; add workers or make tasks faster to reduce it.

**Related:** `concept:pending-budget`, `config:sources.kafka.max_queued_messages`, `metric:drakkar_backpressure_active`, `metric:drakkar_total_queued`, `recipe:lag-growing`

### `concept:sinks-and-delivery`

Sinks write the results to Kafka, Postgres, Mongo, Redis, HTTP or files; each sink has a failure policy, retries and a circuit breaker.

The result hooks return payloads for named sink instances. The worker delivers them per sink, in payload order. A failed delivery calls `on_delivery_error`, which may retry; the retries are bounded by `max_retries` of the sink. When the retries are spent the sink policy `on_failure` decides: `dlq` writes the source message to the DLQ, `skip` drops the payloads.

Each sink has a circuit breaker: after `breaker_failures` failed deliveries it opens for `breaker_cooldown`, and deliveries then follow `on_failure` at once. A sink can depend on other sinks (`depends_on`); it starts only after its dependencies delivered or skipped.

The defaults of every policy key are in `sinks.defaults`; an instance can set its own value.

**Principles:**
- Delivery order equals payload order inside one sink.
- A retry sends the whole group again; payloads that were already written are written again, so the target must accept repeated writes.
- An open circuit means the downstream system is down; fix it there, not in the worker.

**Related:** `concept:dlq`, `config:sinks.defaults.*`, `metric:drakkar_sink_deliver_errors_total`, `metric:drakkar_sink_circuit_open`, `event:sink_delivered`, `event:sink_error`, `rule:sink-breaker-open`, `recipe:sink-failing`

### `concept:dlq`

The DLQ is a Kafka topic that receives a failed source message unchanged, with dk- headers that describe the failure.

A DLQ record holds the key and value of the source message, its headers without the `dk-` ones, and 15 `dk-` headers: the stage (`parse`, `execute` or `sink`), the error text, the sink, the task, the attempts, the source coordinates, the worker and the time. One source message gets at most one record.

After a record is written, the message is done: no later delivery of it starts, and its offset is committed. When the record cannot be written, `dlq.on_send_failure` decides: `drop` loses the message and commits (`drakkar_dlq_dropped_payloads_total`), `stall` keeps the offset and pauses the partition.

The topic is `dlq.topic`, or `<source topic>_dlq` when empty. A replay tool reads the records and publishes them again.

**Principles:**
- A growing DLQ is data that needs an operator; read the dk-stage and dk-error headers.
- With dlq.on_send_failure drop, a DLQ outage loses messages; with stall, it stops the partition.

**Related:** `concept:sinks-and-delivery`, `config:dlq.on_send_failure`, `config:dlq.topic`, `metric:drakkar_sink_dlq_messages_total`, `metric:drakkar_dlq_records_total`, `event:message_dlq`, `recipe:dlq-filling`

### `concept:own-folder`

Each worker keeps its own files in &lt;storage.dir&gt;/&lt;cluster&gt;/&lt;worker&gt;/ and holds a lock on .owner.lock there while it runs.

The own folder holds the live recorder file, the cache file and the watchdog file of the worker. At start the worker takes an exclusive lock on `.owner.lock`; a second process with the same worker name cannot start.

`storage.dir` must be a local filesystem. With `storage.dir` empty the worker keeps no files: no recorder file, no cache file, no watchdog.

A folder of another worker whose lock is free and whose name is not online in the registry is an orphan: the worker seals its files into the history folder. Without autodiscover the worker only logs an orphan folder.

**Principles:**
- Never share one storage.dir folder between hosts over a network filesystem.
- Two workers must never have the same name.

**Related:** `concept:worker-name`, `concept:sealed-history`, `config:storage.dir`, `metric:drakkar_storage_free_bytes`, `metric:drakkar_recorder_orphan_failures_total`

### `concept:worker-name`

The worker name identifies the worker in folders, files, registry cards and metrics; it must be unique in the fleet.

The name is the value of the environment variable named by `worker.name_env` (default `WORKER_ID`) when it is set and not empty, else `worker.name`, else the host name made safe, else `drakkar-<8 hex>`. A configured name must be 1 to 128 letters, digits, `_` or `-`.

The cluster name comes from `cluster.name_env` or `cluster.name`. Workers with the same cluster name share the history folder, the cluster tasks and the cache exchange.

The registry counts other live instances that publish the same name (`drakkar_fleet_duplicate_worker_name_total`).

**Principles:**
- A generated name changes at each start; set a stable name for workers that keep files.
- Workers of one cluster must use the same cluster name.

**Related:** `concept:own-folder`, `config:worker.name`, `config:worker.name_env`, `config:cluster.name`, `metric:drakkar_fleet_duplicate_worker_name_total`

### `concept:recorder`

The flight recorder writes one event row per pipeline step to a memory store, a local SQLite file or a Postgres table.

The recorder keeps events such as `consumed`, `arranged`, `task_started`, `task_failed`, `sink_error` and `committed`. The operator UI and the agent routes read them to trace a message or a task.

`recorder.enabled` turns on durable recording; `recorder.provider` is `sqlite` (one file per rotation in the own folder) or `postgres` (one partitioned table). With the UI on, the recorder also keeps a memory store of the recent events.

The recorder never blocks the pipeline: when its queues are full it drops the oldest rows and counts them. A drop leaves a gap in the history, not in the processing.

**Principles:**
- Recorder drops are a loss of history, not of messages.
- Recording task output costs memory and disk; recorder.capture limits what is kept.

**Related:** `concept:memory-store`, `concept:sealed-history`, `config:recorder.enabled`, `config:recorder.provider`, `metric:drakkar_recorder_dropped_events_total`, `metric:drakkar_recorder_buffer_size`, `recipe:recorder-dropping`

### `concept:memory-store`

The memory store keeps the recent recorder events in memory for the UI, bounded by a time window, a row cap and a size cap.

The memory store exists only with `ui.enabled`. It holds the events of the last `recorder.memory.window`, at most `recorder.memory.rows_per_executor * executor.max_executors` rows and at most `recorder.memory.max_size` of data. The oldest rows are evicted first.

The UI reads recent events from it while it covers the asked range; older reads go to the recorder file or table. `drakkar_recorder_memory_oldest_age_seconds` shows how much time it covers.

With the recorder off and the UI on, the memory store is the only store.

**Principles:**
- The memory store uses RAM; its allocated size can stay near its peak after a load.
- A short window means less RAM and less history in the live views.

**Related:** `concept:recorder`, `config:recorder.memory.window`, `config:recorder.memory.max_size`, `metric:drakkar_recorder_memory_bytes`, `metric:drakkar_recorder_memory_allocated_bytes`, `metric:drakkar_recorder_memory_evicted_rows_total`

### `concept:sealed-history`

A closed recorder file is sealed and moved to the history folder, where every worker of the cluster can search it.

With provider `sqlite` the recorder rotates its file every `recorder.sqlite.rotation_interval`. The closed file is sealed (made one self-contained file with a meta file next to it) and moved to the history folder: `<storage.shared_dir>/history/<cluster>/` when a shared folder is set, else `<storage.dir>/history/<cluster>/`.

The history search of the Trace page reads the sealed files of the cluster. Files that fail to seal stay in the own folder and are tried again (`drakkar_recorder_unsealed_files`).

**Principles:**
- The history folder must be writable by every worker of the cluster.
- A sealed file is never changed; the archive merges and compresses old ones.

**Related:** `concept:archive`, `concept:own-folder`, `config:storage.shared_dir`, `config:recorder.sqlite.rotation_interval`, `metric:drakkar_recorder_unsealed_files`, `metric:drakkar_recorder_seal_failures_total`

### `concept:archive`

The archive merges the sealed recorder files of one time window into one compressed file and deletes the sources.

The archive runs as the system task `history.archive` at `recorder.archive.interval` or `recorder.archive.cron`. It takes the sealed files of each window of `recorder.archive.window` that is older than `recorder.archive.due_after`, merges them, compresses the result, publishes it and deletes the sources. Archives older than `recorder.archive.retention` are deleted.

With autodiscover and a shared folder the archive is a cluster task: one worker per slot archives the files of the whole cluster under a lease. Otherwise each worker archives only its own files.

**Principles:**
- Only recorder.archive.retention removes archives; set a retention that fits the volume.
- A pass that loses its lease stops before it changes more files.

**Related:** `concept:sealed-history`, `concept:cluster-tasks`, `config:recorder.archive.retention`, `config:recorder.archive.window`, `metric:drakkar_recorder_archive_bytes`, `metric:drakkar_recorder_archive_failures_total`, `recipe:disk-filling`

### `concept:autodiscover`

Autodiscover lets workers find each other through one provider (consul, filesystem, postgres or redis) that holds the registry cards and the election leases.

With `autodiscover.enabled` each worker publishes a card every `autodiscover.heartbeat` with its name, cluster, URL, state and features, and reads the cards of its peers every `autodiscover.refresh`. A peer whose card is older than `autodiscover.offline_after` is offline.

The same provider runs the election of the cluster tasks. Calls to the provider are retried and guarded by a circuit breaker (`autodiscover.resilience`). A provider that is down skips slots and keeps the last peer list; it never stops the pipeline.

With autodiscover off the worker publishes no card, runs no cluster task and pulls no cache rows.

**Principles:**
- The provider is a shared dependency of the cluster; watch its breaker.
- Every worker of one cluster must use the same provider and connection.

**Related:** `concept:cluster-tasks`, `concept:cache-exchange`, `config:autodiscover.enabled`, `config:autodiscover.provider`, `metric:drakkar_fleet_peers`, `metric:drakkar_provider_breaker_open`, `rule:peers-offline`, `recipe:peer-missing`

### `concept:cluster-tasks`

A cluster task runs on one worker of the cluster per slot; the workers elect the runner through a lease of the autodiscover provider.

A periodic task with scope `cluster` has slots computed the same way on every worker: an interval aligned to the epoch, or a UTC cron expression. At each slot every worker tries to take the lease of `<cluster>/<task>` within 10 seconds of the slot. The winner runs the task and holds the lease until the run ends and at least 20 seconds after the slot.

Each worker publishes a hash of its cluster task definitions in its card. Peers with another hash are counted in `drakkar_periodic_cluster_mismatch_peers`; during a rolling change the count is not zero for a while.

**Principles:**
- Clocks of the workers must differ by less than 10 seconds.
- Every worker of a cluster must declare the same cluster tasks with the same schedule.
- A lease held at stop is not released early; it ends by itself.

**Related:** `concept:autodiscover`, `metric:drakkar_periodic_cluster_slots_total`, `metric:drakkar_periodic_cluster_mismatch_peers`, `event:periodic_run`, `rule:cluster-tasks-mismatch`, `recipe:cluster-task-not-running`

### `concept:cache-exchange`

The cache exchange lets each worker pull the cluster and global cache rows that its peers wrote, over HTTP.

The cache is a local store of keys and values with a time to live and a scope: `local` (this worker only), `cluster` or `global`. Every `cache.exchange.interval` each worker asks its online peers for the rows they wrote since its last cursor (`GET /api/v1/peer/cache/changes`). The last write wins.

The exchange needs autodiscover to find the peers. It gives no ordering or freshness guarantee: a row can arrive one or more intervals late.

**Principles:**
- Use the cache for values that are safe to read late or not at all.
- A delete is local; it does not reach the peers.

**Related:** `concept:autodiscover`, `config:cache.exchange.interval`, `metric:drakkar_cache_sync_errors_total`, `metric:drakkar_cache_peer_sync_timeouts_total`, `route:GET /api/v1/peer/cache/changes`

### `concept:runtime-health`

The runtime health monitor measures how late the event loop runs, detects stalls and records the stacks that blocked it.

A heartbeat runs every `runtime_health.tick` and measures how late it wakes: that delay is the loop lag. The state is `healthy`, `degraded` (lag at or above `runtime_health.warn_lag`) or `stalled` (no heartbeat for `runtime_health.stall`). During a stall a separate thread captures the stack of the loop and writes a `runtime_stall` event.

Each span out of `healthy` becomes a lag episode with a verdict: `blocked` (one call blocked the loop), `cpu_bound` (the loop computed all the time), `starved` (the host gave the process no CPU) or `inconclusive`.

**Principles:**
- A stalled loop stops every partition, sink and probe of the worker.
- Read the verdict before you change code; a starved worker needs CPU, not a code change.

**Related:** `config:runtime_health.warn_lag`, `config:runtime_health.stall`, `metric:drakkar_runtime_health_state`, `metric:drakkar_loop_lag_peak_seconds`, `event:runtime_stall`, `event:runtime_lag_episode`, `rule:worker-stalled`, `recipe:worker-stalled`

### `concept:http-source`

The HTTP source takes work as HTTP requests; each request runs through the arrange, execute and result steps and answers the result.

With `sources.http.enabled` the worker listens on `sources.http.port`. Each request is checked (client token, rate limit, body size), turned into tasks by the handler's HTTP arrange hook, run in the executor pool and answered with the result. A request that takes longer than `sources.http.request_timeout` answers 504.

An HTTP request has no partition and no offset: its events use partition -1. The results go to the sinks only with `sources.http.deliver_to_sinks`. The HTTP source shares the executor pool and the pending budget with the Kafka source.

**Principles:**
- The worker does not run a failed or timed-out request again; the client decides to send it again.
- At most sources.http.max_concurrent requests run at one time; others answer at once.

**Related:** `config:sources.http.enabled`, `config:sources.http.request_timeout`, `metric:drakkar_webapp_requests_total`, `metric:drakkar_webapp_inflight`, `event:webapp_request_received`

### `concept:databases-view`

The Databases page of the UI lists the recorder files, the history folder, the archives and the storage use, with downloads and slices.

The page reads `GET /api/v1/debug/databases`. It shows the own files of the worker (the memory store, the live file, closed files), the sealed history of the cluster, the archives, the merge results, the storage and the recorder state.

From it an operator can download a sealed file, an archive, a snapshot of the live file or memory store, or a slice of the events of one time range, and merge files. Downloads are bounded so that they do not take the disk or the event loop.

**Principles:**
- A download reads files off the event loop; many large downloads at once still use disk and network.

**Related:** `concept:sealed-history`, `concept:archive`, `route:GET /api/v1/debug/databases`, `route:GET /api/v1/debug/slice`

### `concept:agent-surface`

The agent routes under /api/v1/agent explain a running worker to an AI agent; they are read-only and off by default.

With `agent.enabled` the API server answers six routes: the index, the manual (this knowledge), the effective config, the metrics with their meaning, the live state and the rule-based findings. They need only `api.enabled`, so they work with the UI off.

Set `agent.auth_token`: the config and the state show the worker to anyone who can reach the API port. Secret values are masked. Task output text is sent only with `agent.include_payloads`.

Start at the index, then call the diagnose route, then read the manual topics it names.

**Principles:**
- The agent routes never change the worker.
- Every answer is bounded in size; ask for parts to keep it small.

**Related:** `config:agent.enabled`, `config:agent.auth_token`, `config:agent.include_payloads`, `rule:agent-unauthenticated`, `route:GET /api/v1/agent`, `route:GET /api/v1/agent/diagnose`

## Config advice

### `config:worker.name`

The literal name of the worker.

**Advice:** Set a stable, unique name (or worker.name_env) for every worker that keeps files or joins a cluster; a generated name changes at each start and leaves orphan folders.

**Related:** `concept:worker-name`

### `config:worker.name_env`

The environment variable that holds the worker name.

**Advice:** In a container platform, point it at a variable that holds the pod or instance name, so each replica gets its own stable name.

**Related:** `concept:worker-name`

### `config:cluster.name`

The name of the cluster this worker belongs to.

**Advice:** Give every worker of one deployment the same cluster name; workers with another name do not share the history, the cluster tasks or the cache exchange.

**Related:** `concept:worker-name`, `concept:cluster-tasks`

### `config:sources.kafka.max_queued_messages`

The number of queued messages that pauses Kafka fetching.

**Advice:** Keep it above executor.window_size times the partitions of the worker so windows can fill; raise it only with memory to spare, because queued messages stay in memory.

**Related:** `concept:backpressure`

### `config:sources.kafka.max_poll_interval`

The longest time between two polls before Kafka evicts the consumer.

**Advice:** Keep it larger than executor.drain_timeout, because a revoke waits for the drain inside the rebalance.

**Related:** `concept:at-least-once`

### `config:sources.http.request_timeout`

The time limit of one HTTP request.

**Advice:** Set it above the p99 of the whole request (all tasks and hooks); a request past it answers 504 and its result is dropped.

**Related:** `concept:http-source`

### `config:sources.http.max_concurrent`

The most HTTP requests that run at one time.

**Advice:** Keep it near the number of requests the executor pool can serve at once; extra requests only wait for slots.

**Related:** `concept:http-source`

### `config:executor.max_executors`

The number of task subprocesses that run at one time.

**Advice:** Start at the CPU count of the container for CPU-bound tasks; go higher only for tasks that wait on I/O. Above drakkar_host_effective_cpus the tasks share cores.

**Related:** `concept:task`, `metric:drakkar_executor_pool_waiting`, `metric:drakkar_host_effective_cpus`

### `config:executor.task_timeout`

The wall-clock time limit of one task subprocess.

**Advice:** Set it above the p99 task duration with room; a timeout kills the whole process group and counts as a failure.

**Related:** `concept:task`, `metric:drakkar_executor_timeouts_total`

### `config:executor.max_stdout`

The most standard output kept per task.

**Advice:** Set a cap when tasks can write large output; the output is held in memory until the result hooks end, and 0 means no cap.

**Related:** `concept:pending-budget`, `metric:drakkar_executor_output_truncated_total`

### `config:executor.max_stderr`

The most standard error kept per task.

**Advice:** Set a cap when tasks can write large error text; 0 means no cap.

**Related:** `metric:drakkar_executor_output_truncated_total`

### `config:executor.window_size`

The most messages in one arrange window.

**Advice:** Raise it when arrange gains from seeing many messages at once (batching, deduplication); it does not raise throughput by itself, because the pending budget sizes the windows.

**Related:** `concept:window`

### `config:executor.max_retries`

The most retries of one failed task.

**Advice:** Retries run at once with no backoff; keep it low for failures that a retry cannot fix.

**Related:** `concept:task`, `metric:drakkar_task_retries_total`

### `config:executor.on_failure`

What the default on_error does with a task that failed for good.

**Advice:** Use dlq when a failed message must not be lost silently; skip drops the task and commits the message.

**Related:** `concept:dlq`, `concept:task`

### `config:executor.drain_timeout`

The longest wait for work in flight at a stop or a revoke.

**Advice:** Set it above the p99 task duration so a revoke rarely cancels work, and below sources.kafka.max_poll_interval.

**Related:** `concept:at-least-once`, `metric:drakkar_drain_timeout_hit_total`

### `config:executor.max_pending_tasks`

The most tasks that are created and not settled before new windows wait.

**Advice:** Raise it when the budget is full while the executor pool has free slots and memory allows; lower it to bound memory.

**Related:** `concept:pending-budget`, `rule:budget-saturated`

### `config:executor.max_pending_bytes`

The most bytes of task stdin and kept output before new windows wait.

**Advice:** Set it from the memory of the container minus the other users; it is the main memory bound of the pipeline.

**Related:** `concept:pending-budget`, `recipe:worker-memory-high`

### `config:sinks.defaults.*`

The default failure policy of every sink instance.

**Advice:** Set the policy once here and override it only for sinks that need another one; on_failure dlq keeps failed messages, skip drops them.

**Related:** `concept:sinks-and-delivery`

### `config:sinks.defaults.breaker_failures`

The consecutive failures that open the circuit of a sink.

**Advice:** Lower it to fail fast to the DLQ when the downstream is often down; raise it when single failures are common and short.

**Related:** `metric:drakkar_sink_circuit_open`

### `config:dlq.topic`

The Kafka topic of the DLQ.

**Advice:** Create the topic before the worker starts, with a retention long enough for an operator to read and replay the records.

**Related:** `concept:dlq`

### `config:dlq.on_send_failure`

What happens when no DLQ record can be written.

**Advice:** Use stall when no message may be lost, and drop when progress matters more; alert on drakkar_dlq_send_failures_total with either value.

**Related:** `concept:dlq`, `metric:drakkar_dlq_dropped_payloads_total`

### `config:storage.dir`

The local folder of the own folders of the workers.

**Advice:** Use a local disk with room for the recorder files of a few rotations; never a network filesystem shared between hosts.

**Related:** `concept:own-folder`, `recipe:disk-filling`

### `config:storage.shared_dir`

The folder that every worker of the cluster sees.

**Advice:** Set it to a volume mounted on every worker to share the history and archive the whole cluster at once.

**Related:** `concept:sealed-history`, `concept:archive`

### `config:recorder.enabled`

Durable recording of the pipeline events.

**Advice:** Keep it on for post-incident traces; turn it off only when disk and CPU are scarce and the memory store of the UI is enough.

**Related:** `concept:recorder`

### `config:recorder.provider`

Where durable recording goes, sqlite or postgres.

**Advice:** Use postgres when workers have no persistent local disk or when the cluster needs one searchable history; sqlite needs storage.dir.

**Related:** `concept:recorder`

### `config:recorder.max_buffer_rows`

The rows the recorder queues hold.

**Advice:** Raise it when drakkar_recorder_dropped_events_total grows during short bursts; it costs memory.

**Related:** `recipe:recorder-dropping`

### `config:recorder.memory.window`

How long the memory store keeps events.

**Advice:** Raise it for longer live views, lower it to save memory; the row cap and the size cap can bind first.

**Related:** `concept:memory-store`

### `config:recorder.memory.max_size`

The data budget of the memory store.

**Advice:** Lower it on small containers; the allocated memory can stay near the peak after a burst.

**Related:** `concept:memory-store`, `recipe:worker-memory-high`

### `config:recorder.capture.output`

Whether task events keep the stdout and stderr text.

**Advice:** Turn it off when task output is large or sensitive; the events then keep no stdout and stderr text.

**Related:** `concept:recorder`, `recipe:disk-filling`

### `config:recorder.archive.retention`

How long archives are kept.

**Advice:** Set it to the history an incident needs; 0 keeps archives for ever and the disk fills.

**Related:** `concept:archive`, `metric:drakkar_recorder_archive_bytes`

### `config:cache.max_memory_entries`

The most cache entries kept in memory.

**Advice:** Size it from the hot key set and the size of the values; 0 means no cap and can use all memory.

**Related:** `metric:drakkar_cache_evictions_total`

### `config:cache.exchange.interval`

The time between two cache exchange cycles.

**Advice:** Lower it for fresher peer rows at the cost of more requests; keep it above the time one cycle needs.

**Related:** `concept:cache-exchange`

### `config:autodiscover.enabled`

Whether the worker joins the registry and the election.

**Advice:** Turn it on for every worker of a cluster that uses cluster tasks, the cache exchange or the cluster views of the UI.

**Related:** `concept:autodiscover`

### `config:autodiscover.provider`

The provider of the registry and the election.

**Advice:** Use a service the cluster already depends on; filesystem needs storage.shared_dir on every worker.

**Related:** `concept:autodiscover`

### `config:autodiscover.offline_after`

The card age after which a peer is offline.

**Advice:** Keep it at least twice autodiscover.heartbeat so one late publish does not mark a peer offline.

**Related:** `rule:peers-offline`

### `config:runtime_health.warn_lag`

The loop lag that marks the runtime degraded.

**Advice:** Lower it to see smaller delays, raise it on workers whose hooks do expected short CPU bursts.

**Related:** `concept:runtime-health`, `rule:loop-lag-high`

### `config:runtime_health.stall`

The heartbeat age that marks the runtime stalled and captures stacks.

**Advice:** Keep the default unless stalls shorter than it matter for the workload.

**Related:** `concept:runtime-health`

### `config:agent.enabled`

Whether the agent routes answer.

**Advice:** Turn it on where an agent helps operators; set agent.auth_token at the same time.

**Related:** `concept:agent-surface`

### `config:agent.auth_token`

The bearer token of the agent routes.

**Advice:** Set a long random value from a secret store; without it anyone who reaches the API port reads the config and the state.

**Related:** `concept:agent-surface`, `rule:agent-unauthenticated`

### `config:agent.include_payloads`

Whether the state answer may hold task output text.

**Advice:** Keep it off when task output can hold personal or secret data; the agent then sees exit codes and error kinds only.

**Related:** `concept:agent-surface`

## Metrics

### `metric:drakkar_worker_info`

The identity of the worker, with the labels worker_id, version and consumer_group.

**Meaning:** Join other metrics with it to read the version of each worker; consumer_group is empty on a worker without the Kafka source.

**Healthy:** none

**PromQL:**
- Versions in the cluster: `count by (version) (drakkar_worker_info{ {{cluster}} })`

### `metric:drakkar_messages_consumed_total`

Messages read from the Kafka source topic, per partition.

**Meaning:** The input rate of the worker; compare it with the completed task rate to see whether the worker keeps up.

**Healthy:** A rate close to the produce rate of the topic.

**PromQL:**
- Input rate of this worker: `sum(rate(drakkar_messages_consumed_total{ {{worker}} }[5m]))`

**Related:** `concept:pipeline`, `recipe:lag-growing`

### `metric:drakkar_message_parse_failures_total`

Source messages whose value failed the input model of the handler, per partition.

**Meaning:** The producer sends data the handler cannot read; sources.kafka.on_parse_error decides what happens to them.

**Healthy:** 0; any rate means bad input.

**PromQL:**
- Parse failures per second: `sum(rate(drakkar_message_parse_failures_total{ {{worker}} }[5m]))`

**Related:** `concept:dlq`

### `metric:drakkar_delivery_stalled_offsets_total`

Offsets left uncommitted because neither the delivery nor the DLQ record could be confirmed.

**Meaning:** The watermark of the partition is stuck; the messages are processed again after a restart or rebalance.

**Healthy:** 0; alert on any increase.

**PromQL:**
- Stalled offsets in the last hour: `sum(increase(drakkar_delivery_stalled_offsets_total{ {{worker}} }[1h]))`

**Related:** `concept:dlq`, `event:partition_stalled`

### `metric:drakkar_suppressed_zombie_deliveries_total`

Deliveries dropped because their task finished after the drain of its partition timed out.

**Meaning:** The next owner of the partition processes these messages again; a steady rate means executor.drain_timeout is too short.

**Healthy:** 0, or rare increases at deploys.

**Related:** `concept:at-least-once`, `config:executor.drain_timeout`

### `metric:drakkar_messages_unassigned_dropped_total`

Messages polled for a partition that had no loop because a revoke raced the poll.

**Meaning:** The next owner reads them again; the counter only shows that the race happened.

**Healthy:** none

**Related:** `concept:at-least-once`

### `metric:drakkar_partition_queue_size`

Messages that wait in the queue of each partition loop.

**Meaning:** Messages polled and not yet in a window; a growing queue means the partition cannot take windows fast enough.

**Healthy:** Below sources.kafka.max_queued_messages; zero most of the time on an idle worker.

**PromQL:**
- Queue size per partition: `sum by (partition) (drakkar_partition_queue_size{ {{worker}} })`

**Related:** `concept:backpressure`

### `metric:drakkar_offset_lag`

Pending offsets of this worker per partition, taken and not yet committed.

**Meaning:** The work in flight of the worker; it is not the broker lag.

**Healthy:** Stable values; a value that only grows on one partition points at a stuck message.

**PromQL:**
- Pending offsets per partition: `sum by (partition) (drakkar_offset_lag{ {{worker}} })`

**Related:** `concept:offsets`

### `metric:drakkar_consumer_lag`

Broker lag per partition, the high watermark minus the offset this worker committed.

**Meaning:** How far the worker is behind the topic; the lag of the whole group is the sum over its workers.

**Healthy:** Small and not growing; a growing value means input is faster than processing.

**PromQL:**
- Lag of this worker: `sum(drakkar_consumer_lag{ {{worker}} })`
- Lag of the cluster: `sum(drakkar_consumer_lag{ {{cluster}} })`

**Related:** `concept:offsets`, `recipe:lag-growing`, `rule:lag-growing`

### `metric:drakkar_partition_processor_deaths_total`

Partition loops that ended on an unexpected error, with outcome restarted or dead.

**Meaning:** A dead loop makes no progress and fails the readiness probe.

**Healthy:** 0; alert on any increase.

**PromQL:**
- Dead partition loops: `sum(drakkar_partition_processor_deaths_total{outcome="dead", {{worker}} })`

**Related:** `recipe:partition-dead`, `rule:partition-dead`

### `metric:drakkar_backpressure_active`

1 while Kafka fetching is paused because the partition queues are full.

**Meaning:** The worker slows its input on purpose; it is normal under load.

**Healthy:** Flips between 0 and 1 under load; 1 all the time means the worker is the limit.

**PromQL:**
- Share of time with backpressure: `avg_over_time(drakkar_backpressure_active{ {{worker}} }[15m])`

**Related:** `concept:backpressure`

### `metric:drakkar_consume_pause_active`

1 while an operator debug pause holds consuming.

**Meaning:** Somebody paused the worker from the UI; it takes no new input until the pause ends.

**Healthy:** 0 outside of debugging.

**Related:** `rule:partition-paused-by-debug`

### `metric:drakkar_total_queued`

Messages in all partition queues plus tasks in flight.

**Meaning:** The total work held by the worker.

**Healthy:** Stable under a steady load.

**Related:** `concept:backpressure`

### `metric:drakkar_pending_tasks`

Tasks created and not yet settled, the task count of the pending budget.

**Meaning:** At executor.max_pending_tasks new windows wait; it can pass the limit by the fan-out of one window.

**Healthy:** Below executor.max_pending_tasks most of the time.

**PromQL:**
- Use of the task budget: `drakkar_pending_tasks{ {{worker}} }`

**Related:** `concept:pending-budget`, `rule:budget-saturated`

### `metric:drakkar_pending_bytes`

Bytes held for pending tasks and their results, the byte count of the pending budget.

**Meaning:** Task stdin plus kept output; it is the main memory use of the pipeline.

**Healthy:** Below executor.max_pending_bytes.

**PromQL:**
- Pending bytes: `drakkar_pending_bytes{ {{worker}} }`

**Related:** `concept:pending-budget`, `recipe:worker-memory-high`

### `metric:drakkar_window_admission_wait_seconds_total`

Seconds partition loops waited for room in the pending budget, by limit (tasks or bytes).

**Meaning:** The time the budget held the input; the label names the limit that bound.

**Healthy:** Low; a rate near the partition count means the loops wait most of the time.

**PromQL:**
- Waiting loops on average: `sum by (limit) (rate(drakkar_window_admission_wait_seconds_total{ {{worker}} }[5m]))`

**Related:** `concept:pending-budget`

### `metric:drakkar_window_messages`

Messages taken into one arrange window.

**Meaning:** Small windows under load mean the budget is nearly full.

**Healthy:** none

**PromQL:**
- Average window size: `rate(drakkar_window_messages_sum{ {{worker}} }[5m]) / rate(drakkar_window_messages_count{ {{worker}} }[5m])`

**Related:** `concept:window`

### `metric:drakkar_queued_messages_dropped_at_stop_total`

Polled messages left out of a window at a stop or revoke.

**Meaning:** Their offsets were never registered, so the next owner reads them again; no data is lost.

**Healthy:** none

**Related:** `concept:at-least-once`

### `metric:drakkar_assigned_partitions`

The number of partitions assigned to this worker.

**Meaning:** The share of the topic this worker processes.

**Healthy:** About the partition count divided by the worker count.

**PromQL:**
- Partitions per worker: `drakkar_assigned_partitions{ {{cluster}} }`

### `metric:drakkar_executor_idle_slot_seconds_total`

Slot-seconds of idle executor slots while messages wait in queues.

**Meaning:** The pool had room and the input was there, but no task was ready; arrange, hooks or the budget are the limit.

**Healthy:** Near 0 under load.

**Related:** `concept:pending-budget`

### `metric:drakkar_consumer_idle_seconds_total`

Seconds with no message from Kafka and nothing in the queues.

**Meaning:** Time the worker had nothing to do.

**Healthy:** none

**Related:** `concept:pipeline`

### `metric:drakkar_consumer_errors_total`

Errors returned by the Kafka poll.

**Meaning:** Broker or network problems seen by the consumer.

**Healthy:** 0 or rare.

**Related:** `concept:pipeline`

### `metric:drakkar_offsets_committed_total`

Offset commits per partition.

**Meaning:** A partition with input and no commits has a stuck watermark.

**Healthy:** A steady rate on every partition with input.

**PromQL:**
- Commits per partition: `sum by (partition) (rate(drakkar_offsets_committed_total{ {{worker}} }[5m]))`

**Related:** `concept:offsets`

### `metric:drakkar_rebalance_events_total`

Kafka rebalance events, by type assign or revoke.

**Meaning:** Each rebalance drains partitions and can process messages again.

**Healthy:** Increases only at deploys and scaling.

**Related:** `event:revoked`

### `metric:drakkar_executor_tasks_total`

Executor tasks by status (started, completed, failed).

**Meaning:** The output rate and the failure rate of the worker.

**Healthy:** Few failures compared with completions.

**PromQL:**
- Completed tasks per second: `sum(rate(drakkar_executor_tasks_total{status="completed", {{worker}} }[5m]))`
- Failure ratio: `sum(rate(drakkar_executor_tasks_total{status="failed", {{worker}} }[5m])) / sum(rate(drakkar_executor_tasks_total{status="started", {{worker}} }[5m]))`

**Related:** `concept:task`, `recipe:tasks-failing`

### `metric:drakkar_executor_duration_seconds`

The run time of task subprocesses, from spawn to exit.

**Meaning:** The work time of the task binary, measured off the event loop.

**Healthy:** Stable percentiles; compare them with executor.task_timeout.

**PromQL:**
- p90 task duration: `histogram_quantile(0.9, sum by (le) (rate(drakkar_executor_duration_seconds_bucket{ {{worker}} }[5m])))`

**Related:** `recipe:tasks-slow`

### `metric:drakkar_executor_exit_notice_seconds`

Time from a task exit to the moment the worker handled it.

**Meaning:** It measures the event loop, not the task; large values mean the loop was busy.

**Healthy:** Mostly 0 and below 0.01 s.

**PromQL:**
- Share of exits handled more than 100 ms late: `1 - sum(rate(drakkar_executor_exit_notice_seconds_bucket{le="0.1", {{worker}} }[5m])) / sum(rate(drakkar_executor_exit_notice_seconds_count{ {{worker}} }[5m]))`

**Related:** `concept:runtime-health`

### `metric:drakkar_executor_exit_watch_fallbacks_total`

Task runs timed on the event loop instead of by the exit watcher, by reason.

**Meaning:** exited_before_watch points at a busy loop, unavailable at the host, descendant_held_pipes at background processes of the task.

**Healthy:** 0 or a low rate.

**Related:** `concept:task`

### `metric:drakkar_executor_queue_wait_seconds`

Time a task waited for a free executor slot.

**Meaning:** Long waits with a full pool mean the pool or the CPU is the limit; long waits with an idle pool mean the worker process is slow.

**Healthy:** Low compared with the task duration.

**PromQL:**
- p90 queue wait: `histogram_quantile(0.9, sum by (le) (rate(drakkar_executor_queue_wait_seconds_bucket{ {{worker}} }[5m])))`

**Related:** `recipe:tasks-slow`

### `metric:drakkar_executor_spawn_seconds`

Time to start a task subprocess.

**Meaning:** Spawn close to the task duration means the worker process, not the task binary, is the limit.

**Healthy:** A few milliseconds.

**Related:** `recipe:tasks-slow`

### `metric:drakkar_executor_pool_max`

The configured size of the executor pool, executor.max_executors.

**Meaning:** The upper bound of drakkar_executor_pool_active.

**Healthy:** none

**Related:** `config:executor.max_executors`

### `metric:drakkar_executor_pool_active`

Task subprocesses that run now.

**Meaning:** Equal to the pool size means every slot is busy.

**Healthy:** none

**PromQL:**
- Pool use: `drakkar_executor_pool_active{ {{worker}} } / drakkar_executor_pool_max{ {{worker}} }`

**Related:** `rule:pool-saturated`

### `metric:drakkar_executor_pool_waiting`

Tasks that wait for a free executor slot.

**Meaning:** Values above 0 for a long time mean executor.max_executors limits the worker.

**Healthy:** 0 or short peaks.

**Related:** `rule:pool-saturated`, `config:executor.max_executors`

### `metric:drakkar_executor_timeouts_total`

Tasks killed because they ran longer than executor.task_timeout.

**Meaning:** Each timeout is a failure that on_error handles.

**Healthy:** 0 or rare.

**Related:** `config:executor.task_timeout`, `recipe:tasks-failing`

### `metric:drakkar_executor_output_truncated_total`

Task output streams cut to executor.max_stdout or executor.max_stderr, by stream.

**Meaning:** Result hooks saw only the start of the output.

**Healthy:** 0 unless the caps are set on purpose.

**Related:** `config:executor.max_stdout`

### `metric:drakkar_executor_priority_fn_errors_total`

Times the task priority hook of the handler raised.

**Meaning:** The task got the default priority; it still ran.

**Healthy:** 0; any rate is a handler bug.

**Related:** `concept:task`

### `metric:drakkar_task_retries_total`

Failed tasks that on_error retried.

**Meaning:** Retries run at once and use executor slots.

**Healthy:** Low compared with completed tasks.

**Related:** `config:executor.max_retries`

### `metric:drakkar_tasks_precomputed_total`

Tasks whose result the handler supplied, so no subprocess ran.

**Meaning:** The share of work served from a cache or a lookup.

**Healthy:** none

**Related:** `concept:task`

### `metric:drakkar_host_effective_cpus`

CPUs the worker process can use, the affinity mask capped by the cgroup quota.

**Meaning:** An executor pool larger than this makes CPU-bound tasks share cores.

**Healthy:** none

**Related:** `config:executor.max_executors`

### `metric:drakkar_task_label_value`

Numeric values of the task labels named in metrics.task_label_histograms, by label.

**Meaning:** The size of the inputs of tasks, such as bytes or line counts, to explain task duration.

**Healthy:** none

**PromQL:**
- p90 of one label: `histogram_quantile(0.9, sum by (le, label) (rate(drakkar_task_label_value_bucket{ {{worker}} }[5m])))`

### `metric:drakkar_batch_duration_seconds`

The time of one window, from arrange to the end of its last task.

**Meaning:** Long windows hold the commits of their partition.

**Healthy:** none

**Related:** `concept:window`

### `metric:drakkar_handler_duration_seconds`

The time of each handler hook, by hook.

**Meaning:** Hooks run on the event loop; a slow hook slows every partition.

**Healthy:** Milliseconds for each hook.

**PromQL:**
- p90 per hook: `histogram_quantile(0.9, sum by (le, hook) (rate(drakkar_handler_duration_seconds_bucket{ {{worker}} }[5m])))`

**Related:** `concept:arrange`

### `metric:drakkar_handler_hook_errors_total`

Handler hooks that raised instead of returning, by hook.

**Meaning:** The framework keeps the partition moving, so this counter and the hook's error log line are the signs of a broken hook.

**Healthy:** 0; alert on any rate.

**Related:** `concept:pipeline`

### `metric:drakkar_sink_deliver_duration_seconds`

The time of one delivery to a sink, by sink type and name.

**Meaning:** A slow sink holds the pending budget.

**Healthy:** Well below the delivery_timeout of the sink.

**PromQL:**
- p90 delivery time per sink: `histogram_quantile(0.9, sum by (le, sink_type, sink_name) (rate(drakkar_sink_deliver_duration_seconds_bucket{ {{worker}} }[5m])))`

**Related:** `recipe:sink-failing`

### `metric:drakkar_sink_deliver_errors_total`

Failed deliveries per sink.

**Meaning:** Each failure retries or ends in the on_failure policy of the sink.

**Healthy:** 0 or rare.

**PromQL:**
- Failures per sink: `sum by (sink_type, sink_name) (rate(drakkar_sink_deliver_errors_total{ {{worker}} }[5m]))`

**Related:** `recipe:sink-failing`

### `metric:drakkar_sink_payloads_delivered_total`

Payloads delivered per sink.

**Meaning:** The output rate per sink.

**Healthy:** none

**PromQL:**
- Payloads per second per sink: `sum by (sink_type, sink_name) (rate(drakkar_sink_payloads_delivered_total{ {{worker}} }[5m]))`

### `metric:drakkar_sink_batch_fallbacks_total`

Batched sink statements that failed and were sent again one payload at a time.

**Meaning:** Delivery stays correct but slower; a steady rate means batching does not work for that sink.

**Healthy:** 0 or rare.

**Related:** `concept:sinks-and-delivery`

### `metric:drakkar_sink_delivery_retries_total`

Delivery retries per sink.

**Meaning:** It counts the retries the handler asked for, which spend max_retries of the sink, and the quick in-call retries of an idempotent batch, which do not.

**Healthy:** Low.

**Related:** `concept:sinks-and-delivery`

### `metric:drakkar_sink_deliveries_skipped_total`

Deliveries skipped by the handler, by the skip policy, on an open circuit, or for a window result.

**Meaning:** Skipped payloads are not written anywhere.

**Healthy:** 0 unless skip is the chosen policy.

**Related:** `concept:sinks-and-delivery`

### `metric:drakkar_sink_deliveries_not_started_total`

Deliveries not started because the message is on the DLQ or a dependency failed, by reason.

**Meaning:** The result of the message did not reach that sink.

**Healthy:** Follows the DLQ rate.

**Related:** `event:sink_not_started`

### `metric:drakkar_sink_dlq_messages_total`

DLQ records written, one per source message, all stages.

**Meaning:** Messages that need an operator.

**Healthy:** 0 or a known low rate.

**PromQL:**
- DLQ records per minute: `sum(rate(drakkar_sink_dlq_messages_total{ {{worker}} }[5m])) * 60`

**Related:** `concept:dlq`, `rule:dlq-writing`

### `metric:drakkar_dlq_records_total`

DLQ records by the stage that failed (parse, execute, sink).

**Meaning:** The stage names the step to investigate.

**Healthy:** 0 or a known low rate.

**PromQL:**
- DLQ records by stage: `sum by (stage) (rate(drakkar_dlq_records_total{ {{worker}} }[5m]))`

**Related:** `recipe:dlq-filling`

### `metric:drakkar_dlq_send_failures_total`

Failed attempts to write a DLQ record.

**Meaning:** With dlq.on_send_failure drop messages are lost; with stall the partition stops.

**Healthy:** 0; alert on any rate.

**Related:** `config:dlq.on_send_failure`

### `metric:drakkar_dlq_unconfigured_drops_total`

Payloads or source messages dropped because no DLQ topic is configured.

**Meaning:** A worker without the Kafka source needs dlq.topic to keep failures.

**Healthy:** The value stays 0.

**Related:** `config:dlq.topic`

### `metric:drakkar_dlq_dropped_payloads_total`

Source messages lost because no DLQ record could be written and dlq.on_send_failure is drop.

**Meaning:** The offsets were committed; these messages are gone.

**Healthy:** 0; alert on any increase.

**Related:** `concept:dlq`

### `metric:drakkar_sink_circuit_open`

The circuit breaker state of each sink, 0 closed, 0.5 half open, 1 open.

**Meaning:** An open circuit sends deliveries straight to the on_failure policy.

**Healthy:** The value stays 0.

**PromQL:**
- Open sink circuits: `drakkar_sink_circuit_open{ {{worker}} } == 1`

**Related:** `rule:sink-breaker-open`

### `metric:drakkar_sink_circuit_trips_total`

Transitions of a sink circuit into the open state.

**Meaning:** A rising rate means the circuit flaps.

**Healthy:** The value stays 0.

**Related:** `rule:sink-breaker-open`

### `metric:drakkar_periodic_task_runs_total`

Periodic task runs by name and status (ok, error).

**Meaning:** System tasks such as cache.flush and history.archive are included.

**Healthy:** No error status.

**PromQL:**
- Failed periodic runs: `sum by (name) (increase(drakkar_periodic_task_runs_total{status="error", {{worker}} }[1h]))`

**Related:** `event:periodic_run`

### `metric:drakkar_periodic_task_duration_seconds`

The duration of periodic task runs, by name.

**Meaning:** A run longer than its interval delays the next run.

**Healthy:** Below the interval of the task.

**Related:** `concept:cluster-tasks`

### `metric:drakkar_periodic_cluster_slots_total`

Cluster task slots by name and outcome (won, held_elsewhere, late, election_failed).

**Meaning:** In a healthy cluster each slot counts won on one worker and held_elsewhere on the others.

**Healthy:** No late and no election_failed.

**PromQL:**
- Slots won in the cluster: `sum by (name) (increase(drakkar_periodic_cluster_slots_total{outcome="won", {{cluster}} }[1h]))`

**Related:** `recipe:cluster-task-not-running`

### `metric:drakkar_periodic_cluster_mismatch_peers`

Online peers of this cluster whose cluster task definitions differ.

**Meaning:** A rolling change is not finished, or workers run other code.

**Healthy:** 0 outside of deploys.

**Related:** `rule:cluster-tasks-mismatch`

### `metric:drakkar_offload_running`

Offloaded computations that run now.

**Meaning:** The use of the offload thread pool.

**Healthy:** none

**Related:** `config:threads.offload`

### `metric:drakkar_offload_queued`

Offloaded computations that wait for a free thread.

**Meaning:** Values above 0 for a long time mean threads.offload is too small.

**Healthy:** 0 or short peaks.

**Related:** `config:threads.offload`

### `metric:drakkar_offload_duration_seconds`

The run time of offloaded computations, by hook.

**Meaning:** The CPU work that hooks moved off the event loop.

**Healthy:** none

### `metric:drakkar_loop_lag_seconds`

The event loop lag of each heartbeat tick.

**Meaning:** How late every coroutine of the worker runs.

**Healthy:** Most ticks below runtime_health.warn_lag.

**PromQL:**
- p99 loop lag: `histogram_quantile(0.99, sum by (le) (rate(drakkar_loop_lag_seconds_bucket{ {{worker}} }[5m])))`

**Related:** `concept:runtime-health`

### `metric:drakkar_loop_lag_peak_seconds`

The largest lag of one heartbeat tick in the last 60 seconds.

**Meaning:** Compare it with runtime_health.warn_lag and runtime_health.stall.

**Healthy:** Below runtime_health.warn_lag.

**Related:** `rule:loop-lag-high`

### `metric:drakkar_runtime_health_state`

The runtime health state, 0 healthy, 1 degraded, 2 stalled.

**Meaning:** Read at scrape time, so a stall shows even while the loop does not run.

**Healthy:** The value stays 0.

**PromQL:**
- Workers not healthy: `drakkar_runtime_health_state{ {{cluster}} } > 0`

**Related:** `rule:worker-stalled`

### `metric:drakkar_runtime_heartbeat_age_seconds`

Seconds since the last heartbeat of the runtime health monitor.

**Meaning:** At or above runtime_health.stall the loop is stalled now.

**Healthy:** Below runtime_health.tick plus a little.

**Related:** `concept:runtime-health`

### `metric:drakkar_runtime_stalls_total`

Stalls of the event loop longer than runtime_health.stall.

**Meaning:** Each stall has its stacks in a runtime_stall event.

**Healthy:** The value stays 0.

**Related:** `event:runtime_stall`

### `metric:drakkar_runtime_lag_episodes_total`

Lag episodes that ended, by verdict (blocked, cpu_bound, starved, inconclusive).

**Meaning:** The verdict tells whether code or the host caused the lag.

**Healthy:** Rare.

**Related:** `event:runtime_lag_episode`

### `metric:drakkar_host_load`

The load average of the host, by period 1m and 5m.

**Meaning:** Runnable and waiting tasks of the whole host, not only this worker.

**Healthy:** Below the CPU count of the host.

### `metric:drakkar_host_pressure_percent`

Kernel pressure stall information by resource (cpu, io, memory) and kind (some, full).

**Meaning:** The share of time tasks waited for the resource.

**Healthy:** Low single digits.

**Related:** `recipe:tasks-slow`

### `metric:drakkar_cgroup_cpu_throttled_periods_total`

Periods in which the cgroup CPU quota descheduled the container.

**Meaning:** The container hits its CPU limit.

**Healthy:** 0 or a low rate.

**Related:** `recipe:tasks-slow`

### `metric:drakkar_cgroup_cpu_throttled_seconds_total`

Time the cgroup CPU quota kept the container descheduled.

**Meaning:** A rate near the CPU count of the quota means the container is CPU-capped.

**Healthy:** 0 or a low rate.

**Related:** `recipe:tasks-slow`

### `metric:drakkar_nfs_operations_total`

NFS operations on each mount, by every process of the host.

**Meaning:** The divisor of the average NFS round trip.

**Healthy:** none

### `metric:drakkar_nfs_rtt_seconds_total`

Total server round-trip time of the NFS operations on each mount.

**Meaning:** Divided by the operation rate it gives the average round trip.

**Healthy:** A few milliseconds per operation.

**PromQL:**
- Average NFS round trip: `rate(drakkar_nfs_rtt_seconds_total{ {{worker}} }[5m]) / rate(drakkar_nfs_operations_total{ {{worker}} }[5m])`

### `metric:drakkar_nfs_retransmits_total`

NFS requests sent again because the server did not answer in time.

**Meaning:** The NFS server or the network is overloaded.

**Healthy:** The value stays 0.

### `metric:drakkar_process_threads`

Threads of the worker process.

**Meaning:** A count that only grows points at a thread leak.

**Healthy:** Stable.

### `metric:drakkar_children_cpu_seconds_total`

CPU time of the task subprocesses that ended.

**Meaning:** The CPU the tasks used; read it as a rate over a few minutes.

**Healthy:** none

### `metric:drakkar_storage_free_bytes`

Bytes free on the filesystem that holds storage.dir.

**Meaning:** Recorder and cache files need this space.

**Healthy:** Above a few rotations of recorder files.

**Related:** `recipe:disk-filling`

### `metric:drakkar_recorder_buffer_size`

Events in the recorder intake plus rows in its file queue.

**Meaning:** A value near recorder.max_buffer_rows means the recorder cannot keep up.

**Healthy:** Near 0.

**Related:** `recipe:recorder-dropping`

### `metric:drakkar_recorder_dropped_events_total`

Recorder rows dropped because a queue was full.

**Meaning:** The event history has gaps; the processing is not affected.

**Healthy:** The value stays 0.

**PromQL:**
- Dropped events in the last hour: `increase(drakkar_recorder_dropped_events_total{ {{worker}} }[1h])`

**Related:** `rule:recorder-dropping`

### `metric:drakkar_recorder_file_queue_bytes`

Characters of the large fields of the rows that wait for the recorder file.

**Meaning:** Bounded by recorder.sqlite.max_queue.

**Healthy:** Low.

**Related:** `concept:recorder`

### `metric:drakkar_recorder_flush_duration_seconds`

The time of one write to the recorder file.

**Meaning:** The disk latency of the recorder.

**Healthy:** p99 below recorder.flush_interval.

**Related:** `recipe:recorder-dropping`

### `metric:drakkar_recorder_flush_retries_total`

Recorder file writes that failed and were queued again.

**Meaning:** The disk had a transient problem.

**Healthy:** 0 or rare.

**Related:** `concept:recorder`

### `metric:drakkar_recorder_flush_batches_dropped_total`

Recorder batches dropped after recorder.sqlite.max_flush_retries failures.

**Meaning:** History is lost.

**Healthy:** 0; alert on any increase.

**Related:** `recipe:recorder-dropping`

### `metric:drakkar_recorder_requeue_overflow_total`

Rows dropped from a full recorder queue while a failed write was put back.

**Meaning:** History is lost.

**Healthy:** The value stays 0.

**Related:** `concept:recorder`

### `metric:drakkar_recorder_memory_rows`

Rows in the recorder memory store.

**Meaning:** The size of the live history of the UI.

**Healthy:** none

**Related:** `concept:memory-store`

### `metric:drakkar_recorder_memory_bytes`

Bytes of data in the recorder memory store.

**Meaning:** Bounded by recorder.memory.max_size.

**Healthy:** Below recorder.memory.max_size.

**Related:** `concept:memory-store`

### `metric:drakkar_recorder_memory_allocated_bytes`

The database size of the recorder memory store, free pages included.

**Meaning:** A lower bound of the memory the store holds; it can stay near its peak.

**Healthy:** none

**Related:** `recipe:worker-memory-high`

### `metric:drakkar_recorder_memory_oldest_age_seconds`

The age of the oldest event in the recorder memory store.

**Meaning:** How much time the live history covers.

**Healthy:** Near recorder.memory.window.

**Related:** `concept:memory-store`

### `metric:drakkar_recorder_memory_evicted_rows_total`

Rows removed from the recorder memory store, by reason (window, rows, bytes, gap).

**Meaning:** rows or bytes mean a cap binds before the time window.

**Healthy:** Mostly reason window.

**Related:** `concept:memory-store`

### `metric:drakkar_recorder_memory_write_failures_total`

Event chunks that could not be written to the recorder memory store.

**Meaning:** The live history has gaps.

**Healthy:** The value stays 0.

**Related:** `concept:memory-store`

### `metric:drakkar_recorder_postgres_queue_rows`

Rows that wait for the recorder Postgres table.

**Meaning:** Grows while the database is slow or down.

**Healthy:** Near 0.

**Related:** `recipe:recorder-dropping`

### `metric:drakkar_recorder_postgres_queue_bytes`

Characters of text of the rows in the recorder Postgres queue.

**Meaning:** Bounded by recorder.postgres.max_queue.

**Healthy:** Low.

**Related:** `concept:recorder`

### `metric:drakkar_recorder_postgres_rows_written_total`

Rows written to the recorder Postgres table.

**Meaning:** The write volume of the Postgres history.

**Healthy:** none

**Related:** `concept:recorder`

### `metric:drakkar_recorder_postgres_dropped_rows_total`

Rows that never reached the recorder Postgres table, by reason.

**Meaning:** The Postgres history has gaps.

**Healthy:** The value stays 0.

**Related:** `rule:recorder-dropping`

### `metric:drakkar_recorder_postgres_partitions_dropped_total`

Recorder Postgres partitions dropped, by reason retention or size.

**Meaning:** A steady size rate means the table is too small for its retention.

**Healthy:** Only reason retention.

**Related:** `concept:recorder`

### `metric:drakkar_recorder_ws_dropped_events_total`

Events dropped for a live UI subscriber whose queue was full.

**Meaning:** Only that view lost events; it reloads by itself.

**Healthy:** 0 or rare.

**Related:** `concept:recorder`

### `metric:drakkar_recorder_archive_bytes`

The size of the compressed recorder archives in the history folder.

**Meaning:** Archives are removed only by recorder.archive.retention.

**Healthy:** Stable when a retention is set.

**PromQL:**
- Archive size: `max(drakkar_recorder_archive_bytes{ {{cluster}} })`

**Related:** `concept:archive`, `recipe:disk-filling`

### `metric:drakkar_recorder_archive_files`

The number of compressed recorder archives in the history folder.

**Meaning:** Read it with the archive size to tell many small archives from a few large ones.

**Healthy:** none

**Related:** `concept:archive`

### `metric:drakkar_recorder_unsealed_files`

Own recorder files closed and not yet sealed into the history folder.

**Meaning:** Above 0 for two rotations means the history folder cannot be written.

**Healthy:** 0 most of the time.

**Related:** `concept:sealed-history`

### `metric:drakkar_recorder_seal_failures_total`

Recorder files whose seal or move failed; they are tried again.

**Meaning:** The history folder or the file has a problem.

**Healthy:** The value stays 0.

**Related:** `concept:sealed-history`

### `metric:drakkar_recorder_orphan_failures_total`

Orphan worker folders that the leftover pass could not handle.

**Meaning:** Files of a gone worker stay in storage.dir.

**Healthy:** The value stays 0.

**Related:** `concept:own-folder`

### `metric:drakkar_recorder_archive_failures_total`

Archive passes that did not finish cleanly.

**Meaning:** A window failed, the lease was lost, or an error stopped the pass.

**Healthy:** 0 or rare.

**Related:** `concept:archive`

### `metric:drakkar_recorder_annotations_total`

Handler annotations written to the recorder.

**Meaning:** The volume of custom recorder payloads.

**Healthy:** none

**Related:** `event:annotation`

### `metric:drakkar_recorder_annotations_dropped_total`

Handler annotations dropped, by reason.

**Meaning:** no_context is a handler bug; oversize and budget_exhausted mean payloads are too large.

**Healthy:** The value stays 0.

**Related:** `event:annotation`

### `metric:drakkar_cache_hits_total`

Cache reads served from memory or from the database, by source.

**Meaning:** With the misses it gives the hit ratio.

**Healthy:** none

**PromQL:**
- Cache hit ratio: `sum(rate(drakkar_cache_hits_total{ {{worker}} }[5m])) / (sum(rate(drakkar_cache_hits_total{ {{worker}} }[5m])) + sum(rate(drakkar_cache_misses_total{ {{worker}} }[5m])))`

### `metric:drakkar_cache_misses_total`

Cache reads that found no live entry.

**Meaning:** Work the cache did not save.

**Healthy:** none

### `metric:drakkar_cache_writes_total`

Cache writes by scope (local, cluster, global).

**Meaning:** Cluster and global writes travel to the peers.

**Healthy:** none

**Related:** `concept:cache-exchange`

### `metric:drakkar_cache_deletes_total`

Cache deletes.

**Meaning:** A delete is local; peers keep their copy.

**Healthy:** none

**Related:** `concept:cache-exchange`

### `metric:drakkar_cache_evictions_total`

Entries removed from cache memory by the entry cap.

**Meaning:** A high rate means cache.max_memory_entries is small for the hot keys.

**Healthy:** Low.

**Related:** `config:cache.max_memory_entries`

### `metric:drakkar_cache_flush_entries_total`

Cache entries written from memory to the cache file, by operation.

**Meaning:** The write volume of the cache file.

**Healthy:** none

### `metric:drakkar_cache_flush_failures_total`

Cache flush cycles that failed.

**Meaning:** Writes stay in memory and are lost at stop if the last flush fails too.

**Healthy:** 0; alert on a rising rate.

**Related:** `metric:drakkar_storage_free_bytes`

### `metric:drakkar_cache_cleanup_removed_total`

Expired cache rows removed from the cache file.

**Meaning:** A spike means many entries expired together.

**Healthy:** none

### `metric:drakkar_cache_sync_entries_fetched_total`

Cache rows pulled from each peer and accepted.

**Meaning:** The volume of the cache exchange per peer.

**Healthy:** none

**Related:** `concept:cache-exchange`

### `metric:drakkar_cache_sync_entries_upserted_total`

Cache rows from each peer passed to the last-write-wins upsert.

**Meaning:** Equal to the fetched rows.

**Healthy:** none

**Related:** `concept:cache-exchange`

### `metric:drakkar_cache_sync_errors_total`

Cache exchange errors per peer, failed requests, failed pages and rejected rows.

**Meaning:** The worker misses rows of that peer.

**Healthy:** 0 or rare.

**Related:** `concept:cache-exchange`

### `metric:drakkar_cache_peer_sync_timeouts_total`

Cache exchange cycles cut by their deadline.

**Meaning:** Peers are slow, or there are more peers than one interval can pull.

**Healthy:** The value stays 0.

**Related:** `config:cache.exchange.interval`

### `metric:drakkar_cache_entries_in_memory`

Cache entries in memory.

**Meaning:** Bounded by cache.max_memory_entries.

**Healthy:** none

**Related:** `recipe:worker-memory-high`

### `metric:drakkar_cache_bytes_in_memory`

The size of the cached values in memory.

**Meaning:** Part of the memory of the worker, without object overhead.

**Healthy:** none

**Related:** `recipe:worker-memory-high`

### `metric:drakkar_cache_entries_in_db`

Rows in the cache file.

**Meaning:** Refreshed by the cleanup loop.

**Healthy:** none

### `metric:drakkar_cache_bytes_in_db`

The size of the values in the cache file.

**Meaning:** Refreshed by the cleanup loop.

**Healthy:** none

### `metric:drakkar_provider_calls_total`

Provider operations by feature, provider, operation and outcome.

**Meaning:** Outcomes other than ok mean the provider failed, timed out, was skipped by an open breaker or had not started.

**Healthy:** Mostly outcome ok.

**PromQL:**
- Failed provider operations: `sum by (feature, op, outcome) (rate(drakkar_provider_calls_total{outcome!="ok", {{worker}} }[5m]))`

**Related:** `concept:autodiscover`

### `metric:drakkar_provider_call_duration_seconds`

The time of one provider operation with its retries.

**Meaning:** Slow operations delay the registry, the election or the recorder table writes.

**Healthy:** Well below the resilience timeout.

**Related:** `concept:autodiscover`

### `metric:drakkar_provider_breaker_open`

1 while the circuit breaker of a provider is open.

**Meaning:** The provider is down; its operations are skipped.

**Healthy:** 0; alert while 1.

**Related:** `event:log:provider_breaker_opened`

### `metric:drakkar_fleet_registry_errors_total`

Registry operations that failed or timed out, by operation.

**Meaning:** The card of the worker or the peer list may be stale.

**Healthy:** 0 or rare.

**Related:** `concept:autodiscover`

### `metric:drakkar_fleet_registry_duration_seconds`

The time of registry operations.

**Meaning:** The latency of the provider for the registry.

**Healthy:** none

### `metric:drakkar_fleet_peers`

Peers in the last registry snapshot, by state (online, offline, unknown).

**Meaning:** This worker is not counted.

**Healthy:** No offline peers.

**PromQL:**
- Offline peers: `drakkar_fleet_peers{state="offline", {{worker}} }`

**Related:** `rule:peers-offline`

### `metric:drakkar_fleet_registry_snapshot_age_seconds`

Seconds since the last successful registry refresh.

**Meaning:** It grows while the provider does not answer.

**Healthy:** Below autodiscover.refresh plus a little.

**Related:** `recipe:peer-missing`

### `metric:drakkar_fleet_duplicate_worker_name_total`

Other live instances seen with the name of this worker.

**Meaning:** Two processes use one worker name; their files and cards collide.

**Healthy:** The value stays 0.

**Related:** `concept:worker-name`

### `metric:drakkar_fleet_peer_requests_total`

Requests this worker sent to peers, by endpoint and outcome.

**Meaning:** Outcomes other than ok mean a peer could not be reached.

**Healthy:** Mostly outcome ok.

**Related:** `concept:cache-exchange`

### `metric:drakkar_fleet_peer_request_duration_seconds`

The time of one request to a peer, by endpoint.

**Meaning:** Slow peers slow the cache exchange.

**Healthy:** none

**Related:** `concept:cache-exchange`

### `metric:drakkar_webapp_requests_total`

HTTP source requests by client and outcome status.

**Meaning:** Status values other than ok are timeouts, errors and refusals.

**Healthy:** Mostly status ok.

**PromQL:**
- Requests by status: `sum by (status) (rate(drakkar_webapp_requests_total{ {{worker}} }[5m]))`

**Related:** `concept:http-source`

### `metric:drakkar_webapp_request_duration_seconds`

The server time of HTTP source requests, by client and status.

**Meaning:** Compare the p99 with sources.http.request_timeout.

**Healthy:** p99 below sources.http.request_timeout.

**Related:** `config:sources.http.request_timeout`

### `metric:drakkar_webapp_inflight`

HTTP source requests that run now.

**Meaning:** Above sources.http.max_concurrent points at a permit that was not released.

**Healthy:** Up to sources.http.max_concurrent.

**Related:** `config:sources.http.max_concurrent`

### `metric:drakkar_webapp_dropped_after_timeout_total`

HTTP requests whose result came after their 504 answer.

**Meaning:** The work was done but nobody received it.

**Healthy:** 0 or rare.

**Related:** `event:webapp_request_dropped_after_timeout`

### `metric:drakkar_webapp_rpm_limit`

The configured requests-per-minute limit of each HTTP client.

**Meaning:** Read it with the rate_limited status of the requests.

**Healthy:** none

### `metric:drakkar_uncommitted_offsets_at_stop`

Offsets in flight and not committed when the last stop began.

**Meaning:** Messages that the next owner processes again.

**Healthy:** none

**Related:** `concept:at-least-once`

### `metric:drakkar_inflight_at_stop`

Task subprocesses that ran when the last stop began.

**Meaning:** Work that the stop had to drain or cancel.

**Healthy:** none

**Related:** `config:executor.drain_timeout`

### `metric:drakkar_drain_timeout_hit_total`

Drains of an input source that timed out before their work finished.

**Meaning:** Work was cancelled at a stop or a revoke; its messages are processed again.

**Healthy:** 0 or rare.

**Related:** `config:executor.drain_timeout`

### `metric:drakkar_suspected_oom_kills_total`

Starts after a previous run that was killed before its clean stop.

**Meaning:** Usually the memory limit of the container killed the worker.

**Healthy:** The value stays 0.

**Related:** `recipe:worker-memory-high`, `event:log:previous_run_ended_unexpectedly`

## Events

### `event:consumed`

The Kafka source took one message into the queue of its partition.

**When:** Once per message read from Kafka, before the message is in a window.

**Fields:**
- `offset`: The Kafka offset of the message.
- `partition`: The Kafka partition of the message.

**Related:** `concept:pipeline`, `metric:drakkar_messages_consumed_total`

### `event:arranged`

The arrange hook turned one window into tasks.

**When:** Once per window, after the arrange hook returned.

**Fields:**
- `duration`: Seconds the arrange hook took.
- `metadata`: The offsets of the window, the task ids, the counts and the window id.

**Action:** A long duration slows the partition; move slow work out of arrange.

**Related:** `concept:arrange`, `concept:window`

### `event:assigned`

Kafka assigned a partition to this worker.

**When:** Once per partition at each rebalance that gives the partition to this worker.

**Fields:**
- `partition`: The assigned partition.

**Related:** `metric:drakkar_rebalance_events_total`, `metric:drakkar_assigned_partitions`

### `event:revoked`

Kafka took a partition away from this worker.

**When:** Once per partition at each rebalance that removes it; the worker drains the partition before the rebalance goes on.

**Fields:**
- `partition`: The revoked partition.

**Action:** Frequent revokes mean the group is unstable; check session and poll timeouts and the worker restarts.

**Related:** `concept:at-least-once`, `metric:drakkar_rebalance_events_total`

### `event:committed`

The worker committed the watermark of a partition to Kafka.

**When:** After a commit of a new watermark.

**Fields:**
- `offset`: The committed offset.
- `partition`: The partition.

**Related:** `concept:offsets`, `metric:drakkar_offsets_committed_total`

### `event:partition_stalled`

A partition was paused because a DLQ record could not be written and dlq.on_send_failure is stall.

**When:** When the delivery of a message and its DLQ record both failed under the stall policy.

**Fields:**
- `partition`: The paused partition.

**Action:** Fix the DLQ topic or its connection; the partition stays paused until the worker restarts or loses it.

**Related:** `concept:dlq`, `config:dlq.on_send_failure`, `metric:drakkar_delivery_stalled_offsets_total`

### `event:message_complete`

Every task of one source message ended and the on_message_complete hook ran.

**When:** Once per source message that had tasks.

**Fields:**
- `duration`: Seconds from the first task start to the end of the last task.
- `metadata`: The task count, the succeeded, failed and replaced counts and the output message count.
- `offset`: The offset of the message.

**Related:** `concept:offsets`

### `event:window_complete`

Every task of one window ended and the on_window_complete hook ran.

**When:** Once per window that had tasks.

**Fields:**
- `duration`: Seconds the window took.
- `metadata`: The window id, the task count and the output message count.

**Related:** `concept:window`, `metric:drakkar_batch_duration_seconds`

### `event:task_started`

A task got an executor slot and its subprocess started.

**When:** When the subprocess of a task starts; a precomputed task has no start.

**Fields:**
- `args`: The arguments of the subprocess.
- `labels`: The labels the handler gave the task.
- `metadata`: The slot, the queue wait in milliseconds and the source offsets of the task.
- `task_id`: The id of the task.

**Related:** `concept:task`, `metric:drakkar_executor_queue_wait_seconds`

### `event:task_completed`

A task subprocess exited with code 0.

**When:** When the subprocess of a task ends with exit code 0.

**Fields:**
- `duration`: Seconds the subprocess ran, from spawn to exit.
- `metadata`: The run start time, the exit notice delay, the spawn time and the reported run time.
- `stdout_size`: The size of the standard output in bytes.

**Related:** `concept:task`, `metric:drakkar_executor_duration_seconds`

### `event:task_failed`

A task failed by a non-zero exit code, a timeout or a launch failure.

**When:** When the subprocess of a task fails; each attempt of a retried task writes one event.

**Fields:**
- `duration`: Seconds the subprocess ran.
- `exit_code`: The exit code of the subprocess, or null when it did not exit by itself.
- `metadata`: The exception text of a timeout or launch failure, the failure kind (kind is nonzero_exit, timeout, launch_failure or internal), the source offsets of a Kafka task (source_offsets), the task stdin (capped by recorder.capture.max_stdin) and the run timing.
- `stderr`: The standard error text when the recorder keeps output.

**Action:** Read the exit code, stderr and the exception text; follow the tasks-failing recipe.

**Related:** `recipe:tasks-failing`, `metric:drakkar_executor_tasks_total`

### `event:task_complete`

The on_task_complete hook of a task ran.

**When:** Once per successful task, after its result hook returned.

**Fields:**
- `duration`: Seconds the hook took.
- `metadata`: The number of output messages the hook returned.

**Related:** `metric:drakkar_handler_duration_seconds`

### `event:produced`

A payload was written to a Kafka sink topic.

**When:** Once per Kafka payload delivered.

**Fields:**
- `output_topic`: The sink that received the payload.

**Related:** `concept:sinks-and-delivery`

### `event:sink_delivered`

One sink delivered a group of payloads.

**When:** After a delivery to one sink succeeded.

**Fields:**
- `metadata`: The sink type and name, the payload count and the duration.

**Related:** `concept:sinks-and-delivery`, `metric:drakkar_sink_payloads_delivered_total`

### `event:sink_error`

A delivery to one sink failed.

**When:** After each failed delivery attempt and after its final decision.

**Fields:**
- `metadata`: The sink, the error text, the attempt and the action (retry, skip, dlq or failed).

**Action:** Follow the sink-failing recipe.

**Related:** `recipe:sink-failing`, `metric:drakkar_sink_deliver_errors_total`

### `event:message_dlq`

A source message was written to the DLQ; no later delivery of it starts.

**When:** Once per source message that gets a DLQ record, at the parse, execute or sink stage.

**Fields:**
- `metadata`: The stage, the sink, the error, the sinks that already delivered and the attempts.

**Action:** Read the stage and the error; follow the dlq-filling recipe.

**Related:** `concept:dlq`, `recipe:dlq-filling`

### `event:sink_not_started`

A sink delivery did not start because the message is on the DLQ or a dependency failed.

**When:** For each sink group of a message that was not started.

**Fields:**
- `metadata`: The sink and the reason (message_dlq or dependency_failed).

**Related:** `concept:sinks-and-delivery`, `metric:drakkar_sink_deliveries_not_started_total`

### `event:runtime_health`

The runtime health monitor changed state or wrote its periodic sample.

**When:** At each state change and every runtime_health.sample_interval.

**Fields:**
- `metadata`: The state, the lag numbers and the stall count.

**Related:** `concept:runtime-health`

### `event:runtime_stall`

The event loop was silent for longer than runtime_health.stall; the event holds the stacks it ran.

**When:** When the loop runs again after a stall.

**Fields:**
- `duration`: Seconds the loop was stalled.
- `metadata`: The captured stacks with their sample counts.

**Action:** The top stack names the code that blocked the loop; follow the worker-stalled recipe.

**Related:** `concept:runtime-health`, `recipe:worker-stalled`, `metric:drakkar_runtime_stalls_total`

### `event:runtime_lag_episode`

A span of degraded or stalled runtime health ended; the event holds its verdict and evidence.

**When:** When the runtime returns to healthy, or every runtime_health.max_episode during a long episode.

**Fields:**
- `metadata`: The duration, the peak lag, the verdict, the aggregated stacks, the loop CPU and the host evidence.

**Action:** Act on the verdict; blocked and cpu_bound point at code, starved points at the host.

**Related:** `concept:runtime-health`, `metric:drakkar_runtime_lag_episodes_total`

### `event:runtime_probe`

One stack sample of the event loop, taken while runtime_health.stack_probe_enabled is true.

**When:** Every runtime_health.stack_probe_interval.

**Fields:**
- `metadata`: The lag, the number of concurrency units and the stacks.

**Related:** `config:runtime_health.stack_probe_enabled`

### `event:resource_sample`

A sample of the host and the worker process resources.

**When:** Every recorder.sqlite.state_interval with a recorder file, else every 10 seconds.

**Fields:**
- `metadata`: Load, pressure, CPU throttling, NFS numbers, threads, free storage and the memory of the worker.

**Related:** `metric:drakkar_host_load`, `metric:drakkar_storage_free_bytes`

### `event:annotation`

The handler attached a custom payload to a message, a task or a window.

**When:** When a hook calls the annotation API and the payload fits the size limits.

**Fields:**
- `metadata`: The kind and the payload the handler gave.

**Related:** `metric:drakkar_recorder_annotations_total`, `metric:drakkar_recorder_annotations_dropped_total`

### `event:periodic_run`

A periodic task ran once.

**When:** After each run of a periodic task, system tasks included.

**Fields:**
- `metadata`: The name, the status, the duration and, for a cluster task, the scope and the slot.

**Related:** `concept:cluster-tasks`, `metric:drakkar_periodic_task_runs_total`

### `event:offload`

A hook ran one computation on the offload thread pool.

**When:** After each offloaded computation.

**Fields:**
- `metadata`: The hook, the function, the queue wait, the duration and the status.

**Related:** `metric:drakkar_offload_duration_seconds`

### `event:webapp_request_received`

The HTTP source accepted a request and started its pipeline.

**When:** Once per accepted HTTP request.

**Fields:**
- `client_name`: The client that sent it.
- `request_id`: The id of the request.

**Related:** `concept:http-source`

### `event:webapp_request_completed`

An HTTP request finished and was answered with its result.

**When:** Once per request that ended with status ok.

**Fields:**
- `duration`: Seconds the request took.

**Related:** `concept:http-source`, `metric:drakkar_webapp_requests_total`

### `event:webapp_request_timeout`

An HTTP request took longer than sources.http.request_timeout and was answered 504.

**When:** Once per request that timed out.

**Action:** Raise sources.http.request_timeout or make the tasks of the request faster.

**Related:** `config:sources.http.request_timeout`

### `event:webapp_request_rate_limited`

An HTTP request was refused with 429 because its client passed its requests-per-minute limit.

**When:** Once per refused request.

**Related:** `metric:drakkar_webapp_rpm_limit`

### `event:webapp_request_auth_failed`

An HTTP request was refused with 401 because its client token is missing or wrong.

**When:** Once per refused request; the event holds only a short prefix of the token.

**Related:** `concept:http-source`

### `event:webapp_request_dropped_after_timeout`

The pipeline of an HTTP request finished after its 504 answer, so its result was dropped.

**When:** Once per request whose result came too late.

**Related:** `metric:drakkar_webapp_dropped_after_timeout_total`

### `event:log:agent_surface_unauthenticated`

The agent routes are on and agent.auth_token is empty.

**When:** Once at startup.

**Fields:**
- `hint`: What to set to protect the routes.

**Action:** Set agent.auth_token.

**Related:** `concept:agent-surface`, `rule:agent-unauthenticated`

### `event:log:agent_notes_failed`

The agent_notes hook of the handler raised; the worker runs without handler notes.

**When:** Once at startup, after on_startup.

**Action:** Fix the agent_notes hook of the handler.

**Related:** `concept:agent-surface`

### `event:log:agent_notes_invalid`

A handler note breaks a rule (a key, a metric or a reference); that note is dropped and the other notes stay.

**When:** Once at startup, after on_startup.

**Fields:**
- `reasons`: The problems found in the notes.

**Action:** Fix the notes; every reference must name a framework entry or another note.

**Related:** `concept:agent-surface`

### `event:log:agent_rule_abandoned`

A handler diagnose rule ran longer than 1 second; the diagnose answer lists it as failed and does not wait for it.

**When:** When a handler rule passes its time limit.

**Fields:**
- `rule`: The name of the rule in the diagnose answer, app:&lt;key&gt;.

**Action:** Make the rule a fast, pure check of the snapshot.

**Related:** `concept:agent-surface`

### `event:log:partition_processor_died`

A partition loop ended on an unexpected error.

**When:** When the loop of a partition raises; it is restarted once, then marked dead.

**Action:** Read the error; follow the partition-dead recipe.

**Related:** `recipe:partition-dead`, `metric:drakkar_partition_processor_deaths_total`

### `event:log:previous_run_ended_unexpectedly`

The previous process with this worker name was killed before its clean stop.

**When:** Once at startup, when the watchdog file has no clean exit marker.

**Action:** Check the container memory limit and the kill events of the platform.

**Related:** `metric:drakkar_suspected_oom_kills_total`, `recipe:worker-memory-high`

### `event:log:provider_breaker_opened`

The circuit breaker of a provider opened; its operations are skipped until a probe succeeds.

**When:** Once per opening.

**Action:** Check the provider service (the autodiscover provider or the recorder Postgres database).

**Related:** `metric:drakkar_provider_breaker_open`

### `event:log:zombie_delivery_suppressed`

A task finished after the drain of its partition timed out; its result was not delivered.

**When:** Once per suppressed delivery.

**Action:** Raise executor.drain_timeout when this happens at each rebalance.

**Related:** `concept:at-least-once`, `metric:drakkar_suppressed_zombie_deliveries_total`

## Recipes

### `recipe:lag-growing`

Find out why the consumer lag grows and what limits the worker.

**Question:** Why does the consumer lag grow?

**Steps:**
1. Call /api/v1/agent/state?parts=budget,partitions. Why: It shows the lag per partition, the pending budget and the executor pool in one answer.
2. Compare rate(drakkar_messages_consumed_total) with rate(drakkar_executor_tasks_total{status="completed"}). Why: Input faster than output means the worker is too slow; low input means fetching is paused.
3. Read drakkar_executor_pool_waiting and drakkar_executor_pool_active. Why: A full pool with waiting tasks means the subprocesses are the limit.
4. Read drakkar_window_admission_wait_seconds_total and drakkar_backpressure_active. Why: Waits with an idle pool mean hooks or deliveries hold the budget.
5. Look for a partition with dead true or a lag much higher than the others. Why: One stuck partition shows as lag on that partition only.

**Causes:**
- The executor pool is full. Check: pool_active equals pool_max and pool_waiting stays above 0. Fix: Add workers, or raise executor.max_executors when the host has free CPU.
- Tasks became slower. Check: The p90 of drakkar_executor_duration_seconds rose. Fix: Follow the tasks-slow recipe.
- A slow hook or sink delivery holds the pending budget. Check: budget.tasks is at max_tasks while the pool is not full; drakkar_sink_deliver_duration_seconds or drakkar_handler_duration_seconds is high. Fix: Make the hook or the sink faster, or raise executor.max_pending_tasks when memory allows.
- One message holds the commit of its partition. Check: drakkar_offset_lag of one partition grows while the others commit. Fix: Trace the oldest pending offset of that partition with /api/v1/debug/trace.
- A partition loop is dead. Check: partitions[].dead is true. Fix: Follow the partition-dead recipe.

**Related:** `concept:offsets`, `concept:backpressure`, `concept:pending-budget`, `metric:drakkar_consumer_lag`, `rule:lag-growing`

### `recipe:worker-memory-high`

Find out what uses the memory of a worker container.

**Question:** Why does the worker use so much memory?

**Steps:**
1. Read process_resident_memory_bytes and the cgroup memory.current of the container. Why: The container also counts the task subprocesses and the page cache, which the worker process does not hold.
2. Read drakkar_pending_bytes. Why: It is the task stdin and output that the worker holds for pending tasks.
3. Read drakkar_recorder_memory_allocated_bytes. Why: It is a lower bound of the memory of the recorder memory store.
4. Read drakkar_cache_bytes_in_memory and drakkar_cache_entries_in_memory. Why: The cache keeps values in memory up to cache.max_memory_entries.
5. Read drakkar_recorder_buffer_size and drakkar_recorder_file_queue_bytes. Why: A slow disk makes the recorder queues grow.

**Causes:**
- Large task output is kept while hooks run. Check: drakkar_pending_bytes is near executor.max_pending_bytes. Fix: Lower executor.max_pending_bytes, or cap the output with executor.max_stdout and executor.max_stderr.
- The recorder memory store is large. Check: drakkar_recorder_memory_allocated_bytes is a large part of the resident memory. Fix: Lower recorder.memory.max_size or recorder.memory.window.
- The task subprocesses use the memory. Check: The cgroup anon memory minus the worker resident memory is large. Fix: Lower executor.max_executors or give the container more memory.
- The cache holds too many entries. Check: drakkar_cache_entries_in_memory is near cache.max_memory_entries with large values. Fix: Lower cache.max_memory_entries.

**Related:** `concept:pending-budget`, `concept:memory-store`, `metric:drakkar_pending_bytes`, `metric:drakkar_suspected_oom_kills_total`

### `recipe:tasks-slow`

Find out whether slow tasks come from the task binary, the host or the worker process.

**Question:** Why are tasks slow?

**Steps:**
1. Read the p90 of drakkar_executor_duration_seconds and drakkar_executor_queue_wait_seconds. Why: Duration is the run of the subprocess; queue wait is the time before it.
2. Read drakkar_executor_spawn_seconds and drakkar_executor_exit_notice_seconds. Why: Large values mean the worker process, not the binary, is slow.
3. Read drakkar_host_pressure_percent, drakkar_cgroup_cpu_throttled_seconds_total and drakkar_host_effective_cpus. Why: CPU pressure or a CPU quota slows every subprocess.
4. Compare slow tasks by their labels with drakkar_task_label_value. Why: Large inputs explain long runs.

**Causes:**
- The host has no free CPU. Check: drakkar_executor_pool_max is above drakkar_host_effective_cpus, or the CPU pressure is high. Fix: Lower executor.max_executors to the CPU count, or give the container more CPU.
- The event loop is busy. Check: drakkar_executor_exit_notice_seconds is often above 0.1 s and the loop lag is high. Fix: Follow the worker-stalled recipe.
- The inputs are larger. Check: drakkar_task_label_value of a size label grew. Fix: Split large inputs into more tasks in arrange.
- A shared filesystem is slow. Check: rate(drakkar_nfs_rtt_seconds_total) over rate(drakkar_nfs_operations_total) grew. Fix: Check the NFS server, or move the task files to a local disk.

**Related:** `concept:task`, `metric:drakkar_executor_duration_seconds`, `metric:drakkar_executor_queue_wait_seconds`

### `recipe:tasks-failing`

Find out why tasks fail and what happens to their messages.

**Question:** Why do tasks fail?

**Steps:**
1. Call /api/v1/agent/state?parts=recent_failures. Why: It lists the last failed tasks with their exit code and error kind.
2. Read rate(drakkar_executor_tasks_total{status="failed"}) and drakkar_executor_timeouts_total. Why: It tells whether failures are timeouts or exit codes.
3. Read drakkar_task_retries_total and drakkar_dlq_records_total{stage="execute"}. Why: It tells what the on_error hook did with the failures.
4. Open /api/v1/task/{task_id} for one failed task. Why: It shows the arguments, the stderr and the timing of the task.

**Causes:**
- The task binary is missing or not executable. Check: The error kind is launch_failure. Fix: Install the binary or set executor.binary_path.
- Tasks run longer than the timeout. Check: The error kind is timeout and drakkar_executor_timeouts_total grows. Fix: Raise executor.task_timeout or split the work into smaller tasks.
- The input data is bad. Check: The same exit code and stderr repeat for some inputs only. Fix: Send those messages to the DLQ from on_error and fix the producer.
- The host kills the subprocesses. Check: Exit codes are negative or 137 and the container is near its memory limit. Fix: Lower executor.max_executors or give the container more memory.

**Related:** `concept:task`, `event:task_failed`, `metric:drakkar_executor_tasks_total`, `config:executor.on_failure`

### `recipe:dlq-filling`

Find out why messages go to the DLQ and whether any are lost.

**Question:** Why do messages go to the DLQ?

**Steps:**
1. Read drakkar_dlq_records_total by stage. Why: The stage (parse, execute, sink) names the failing step.
2. Read records on the DLQ topic with /api/v1/debug/kafka/dlq/messages. Why: The dk-error and dk-sink headers give the error text and the sink.
3. Read drakkar_dlq_dropped_payloads_total and drakkar_dlq_send_failures_total. Why: Non-zero values mean messages were lost or the DLQ topic cannot be written.

**Causes:**
- Messages cannot be parsed. Check: The stage is parse and drakkar_message_parse_failures_total grows. Fix: Fix the producer or the input model.
- Tasks fail and on_error sends them to the DLQ. Check: The stage is execute. Fix: Follow the tasks-failing recipe.
- A sink fails. Check: The stage is sink and drakkar_sink_deliver_errors_total grows for one sink. Fix: Follow the sink-failing recipe.

**Related:** `concept:dlq`, `metric:drakkar_sink_dlq_messages_total`, `event:message_dlq`, `rule:dlq-writing`

### `recipe:sink-failing`

Find out why a sink fails to deliver and what happens to the results.

**Question:** Why does a sink fail?

**Steps:**
1. Call /api/v1/agent/state?parts=sinks. Why: It shows the breaker state and the delivered and failed counts per sink.
2. Read drakkar_sink_deliver_errors_total and drakkar_sink_circuit_trips_total per sink. Why: It names the failing sink and shows whether its breaker flaps.
3. Read sink_error events of that sink with /api/v1/events. Why: The metadata holds the error text and the action taken.
4. Read drakkar_provider_breaker_open and the connection of the sink in /api/v1/agent/config. Why: A wrong address or a down service fails every delivery.

**Causes:**
- The downstream system is down or slow. Check: Every delivery fails with a connection or timeout error, and the breaker opens. Fix: Fix the downstream system; the breaker closes by itself.
- The payloads do not fit the target. Check: The errors name a column, a type or a constraint. Fix: Fix the result hook or the target schema.
- The delivery timeout is too short. Check: drakkar_sink_deliver_duration_seconds is near the delivery_timeout of the sink. Fix: Raise delivery_timeout of the sink or make the target faster.

**Related:** `concept:sinks-and-delivery`, `metric:drakkar_sink_deliver_errors_total`, `event:sink_error`, `rule:sink-breaker-open`

### `recipe:worker-stalled`

Find out what blocks the event loop of a worker.

**Question:** Why is the worker stalled or slow to answer?

**Steps:**
1. Call /api/v1/agent/state?parts=health. Why: It shows the state, the heartbeat age and the open lag episode.
2. Read the last runtime_stall and runtime_lag_episode events. Why: They hold the stacks that ran during the stall and the verdict.
3. Call /api/v1/debug/runtime/units. Why: It groups the live concurrency units by where they wait.

**Causes:**
- A hook makes a blocking call. Check: The verdict is blocked and the top stack is in handler code. Fix: Replace the call with a non-blocking one, or run it off the event loop.
- A hook does heavy CPU work. Check: The verdict is cpu_bound. Fix: Move the work to the offload pool, or into the task subprocess.
- The host gives the worker no CPU. Check: The verdict is starved and the CPU pressure or throttling is high. Fix: Give the container more CPU or lower executor.max_executors.

**Related:** `concept:runtime-health`, `event:runtime_stall`, `rule:worker-stalled`, `rule:loop-lag-high`

### `recipe:partition-dead`

Find out why a partition loop died and how to get the partition moving again.

**Question:** Why does one partition make no progress?

**Steps:**
1. Call /api/v1/agent/state?parts=partitions. Why: It shows dead, paused and the lag per partition.
2. Read drakkar_partition_processor_deaths_total by outcome. Why: restarted means the loop came back once; dead means it gave up.
3. Read the partition_processor_died log lines. Why: They hold the error that ended the loop.
4. Check /readyz. Why: A dead partition fails the readiness probe.

**Causes:**
- A hook raised an error that the framework did not contain. Check: The log line names a handler frame. Fix: Fix the handler and restart the worker; the offsets of the crashed window are processed again.
- The partition is held by a DLQ write that failed with dlq.on_send_failure stall. Check: A partition_stalled event exists for the partition. Fix: Fix the DLQ topic; with dlq.on_send_failure stall the partition waits for it.

**Related:** `concept:offsets`, `metric:drakkar_partition_processor_deaths_total`, `rule:partition-dead`, `route:GET /readyz`

### `recipe:recorder-dropping`

Find out why the recorder drops events and how to stop the gaps.

**Question:** Why does the recorder drop events?

**Steps:**
1. Read drakkar_recorder_dropped_events_total and drakkar_recorder_buffer_size. Why: A full queue drops the oldest events.
2. Read the p99 of drakkar_recorder_flush_duration_seconds. Why: Flushes slower than recorder.flush_interval mean the disk is slow.
3. With provider postgres, read drakkar_recorder_postgres_dropped_rows_total by reason and drakkar_provider_breaker_open{feature="recorder"}. Why: queue_full means the table is not reached in time.

**Causes:**
- The disk of storage.dir is slow or full. Check: drakkar_storage_free_bytes is low or flushes are slow. Fix: Free disk space or move storage.dir to a faster disk.
- The event volume is larger than the queue. Check: The buffer stays near recorder.max_buffer_rows. Fix: Raise recorder.max_buffer_rows, or record less with recorder.capture.
- The Postgres table cannot be written. Check: The provider breaker of the recorder is open. Fix: Fix the database; rows that waited in the queue are written when it comes back.

**Related:** `concept:recorder`, `metric:drakkar_recorder_dropped_events_total`, `rule:recorder-dropping`

### `recipe:peer-missing`

Find out why a peer is missing or offline in the registry.

**Question:** Why does a worker not see one of its peers?

**Steps:**
1. Call /api/v1/agent/state?parts=autodiscover. Why: It counts the online and offline peers.
2. Call /api/v1/workers. Why: It lists the peers with their state and the age of their card.
3. Read drakkar_provider_breaker_open{feature="registry"} and drakkar_fleet_registry_snapshot_age_seconds. Why: A provider that does not answer keeps an old peer list.

**Causes:**
- The peer is down. Check: The card age of the peer grows past autodiscover.offline_after. Fix: Restart the peer; its card is removed after autodiscover.prune_after.
- The peer uses another cluster name or provider. Check: The cluster name in its card differs, or the peer is not listed at all. Fix: Use one cluster name and one autodiscover provider for the whole cluster.
- The provider does not answer. Check: The registry breaker is open. Fix: Fix the provider service or its connection.

**Related:** `concept:autodiscover`, `metric:drakkar_fleet_peers`, `rule:peers-offline`

### `recipe:cluster-task-not-running`

Find out why a cluster task does not run at its slots.

**Question:** Why does a cluster task not run?

**Steps:**
1. Read drakkar_periodic_cluster_slots_total by name and outcome on every worker. Why: Each slot should count won on one worker and held_elsewhere on the others.
2. Call /api/v1/debug/autodiscover/leases. Why: It shows which worker holds the lease of each cluster task.
3. Read drakkar_periodic_task_runs_total{status="error"}. Why: A run that won the slot can still fail.

**Causes:**
- The election provider does not answer. Check: The outcome election_failed grows. Fix: Fix the autodiscover provider.
- The worker was blocked through the acquire window. Check: The outcome late grows. Fix: Follow the worker-stalled recipe.
- The workers declare different tasks. Check: drakkar_periodic_cluster_mismatch_peers is above 0. Fix: Deploy the same task definitions on every worker of the cluster.

**Related:** `concept:cluster-tasks`, `metric:drakkar_periodic_cluster_slots_total`, `rule:cluster-tasks-mismatch`

### `recipe:disk-filling`

Find out which files fill the disk of a worker or of the shared folder.

**Question:** Why does the disk fill up?

**Steps:**
1. Read drakkar_storage_free_bytes. Why: It is the free space of the filesystem that holds storage.dir.
2. Call /api/v1/debug/databases. Why: It lists the own files, the history files, the archives and the merge results with their sizes.
3. Read drakkar_recorder_archive_bytes and drakkar_recorder_unsealed_files. Why: Archives grow without a retention, and unsealed files stay in the own folder.

**Causes:**
- Archives are kept for ever. Check: recorder.archive.retention is 0, or drakkar_recorder_archive_bytes grows steadily. Fix: Set recorder.archive.retention.
- Sealed files are not archived. Check: The history folder holds many sealed files older than recorder.archive.due_after. Fix: Check drakkar_recorder_archive_failures_total and the archive lease.
- Task output is recorded. Check: recorder.capture.output is true and tasks write large output. Fix: Raise recorder.capture.min_output_duration or turn off recorder.capture.output.

**Related:** `concept:archive`, `concept:own-folder`, `metric:drakkar_storage_free_bytes`

## Routes

### `route:GET /healthz`

The liveness probe of the worker.

**Use:** Check that the API server of the worker answers at all.

**Cost:** cheap

### `route:GET /readyz`

The readiness probe; it fails before startup ends, while a sink is not connected, or when a partition loop is dead.

**Use:** Check that the worker takes work; a failure names the reason.

**Cost:** cheap

**Related:** `recipe:partition-dead`

### `route:GET /api/v1/dashboard`

The aggregates of the Dashboard page, such as throughput, lag, pool use and sink counts.

**Use:** Get one overview of the worker in one call.

**Cost:** moderate

**Related:** `concept:pipeline`

### `route:GET /api/v1/identity`

The name, cluster, instance, versions and capabilities of the worker.

**Use:** Learn which worker answers and which features it has on.

**Cost:** cheap

### `route:GET /api/v1/partitions`

The pipeline state of each assigned partition.

**Use:** Find the partition that holds the lag or the commits.

**Cost:** moderate

**Related:** `concept:offsets`

### `route:GET /api/v1/task/{task_id}`

The events and details of one task.

**Use:** Read the arguments, output, exit code and timing of one task that a failure names.

**Cost:** moderate

**Related:** `recipe:tasks-failing`

### `route:GET /api/v1/sinks`

The delivery counters and breaker states of every sink.

**Use:** Find the sink that fails or is slow.

**Cost:** cheap

**Related:** `recipe:sink-failing`

### `route:GET /api/v1/workers`

This worker and its peers from the registry, with their state and card age.

**Use:** Check which workers of the cluster are online.

**Cost:** cheap

**Related:** `concept:autodiscover`

### `route:GET /api/v1/events`

Recorder event rows, filtered by type, partition and time.

**Use:** Read recent events of one kind, such as sink_error or task_failed.

**Cost:** moderate

**Related:** `concept:recorder`

### `route:GET /api/v1/recent-tasks`

The recent tasks for the timeline of the UI.

**Use:** See which tasks ran recently and how long they took.

**Cost:** moderate

### `route:GET /api/v1/timeline/events`

The custom timeline events of the visible time window.

**Use:** Read the handler-defined events that the timeline draws.

**Cost:** moderate

### `route:GET /api/v1/live/overview`

The live overview of the worker and the runtime settings of the UI.

**Use:** Get the live counts that the Live page starts from.

**Cost:** cheap

### `route:POST /api/v1/live/arrange-tasks`

Looks up many tasks by id in one call.

**Use:** Resolve a list of task ids to their tasks.

**Cost:** moderate

### `route:GET /api/v1/live/task-results`

Recent results per task.

**Use:** Read the recent task outcomes of the Live page.

**Cost:** moderate

### `route:GET /api/v1/live/message-results`

Recent results per source message.

**Use:** Read the recent message outcomes of the Live page.

**Cost:** moderate

### `route:GET /api/v1/live/window-results`

Recent results per window.

**Use:** Read the recent window outcomes of the Live page.

**Cost:** moderate

### `route:POST /api/v1/live/sink-breakdown`

Which output topics the results of given offsets went to.

**Use:** Find where the results of some source offsets were delivered.

**Cost:** moderate

### `route:GET /api/v1/runtime/health`

The runtime health snapshot with the state, lag, stalls and lag episodes.

**Use:** Read the health of the event loop with its recent episodes.

**Cost:** cheap

**Related:** `concept:runtime-health`

### `route:GET /api/v1/debug/processors`

The internal state of each partition loop, with stuck tasks.

**Use:** Inspect a partition that does not move.

**Cost:** moderate

**Related:** `recipe:partition-dead`

### `route:GET /api/v1/debug/databases`

The own recorder files, the shared history, the archives, the storage and the recorder state.

**Use:** See which recorder files exist and how much disk they use.

**Cost:** moderate

**Related:** `concept:databases-view`, `recipe:disk-filling`

### `route:POST /api/v1/debug/merge`

Merges recorder files into one file.

**Use:** Build one file of several recorder files for an offline look; it writes to disk.

**Cost:** heavy

### `route:GET /api/v1/debug/download/{path}`

Downloads a sealed file, an archive or a merge result.

**Use:** Take a recorder file for an offline look.

**Cost:** heavy

### `route:GET /api/v1/debug/download-live`

Downloads a snapshot of the live recorder file, the memory store or the cache.

**Use:** Take the current recorder events for an offline look.

**Cost:** heavy

### `route:GET /api/v1/debug/slice`

Downloads the recorder events of one time range as one file.

**Use:** Take the events of an incident without the whole history.

**Cost:** heavy

**Related:** `concept:databases-view`

### `route:GET /api/v1/debug/databases/postgres`

The hour partitions of the recorder Postgres table.

**Use:** Check the size and the hours of the Postgres history.

**Cost:** moderate

### `route:DELETE /api/v1/debug/files/{path}`

Deletes one merge result; it needs ui.delete_enabled.

**Use:** Remove a merge result that is no longer needed. It changes the disk.

**Cost:** cheap

### `route:GET /api/v1/debug/autodiscover`

The registry cards from the last refresh.

**Use:** Read the cards of the peers as the provider gave them.

**Cost:** cheap

**Related:** `concept:autodiscover`

### `route:GET /api/v1/debug/autodiscover/leases`

The holder of the lease of each cluster task.

**Use:** Find which worker runs a cluster task now.

**Cost:** moderate

**Related:** `recipe:cluster-task-not-running`

### `route:GET /api/v1/debug/archives`

The compressed recorder archives of the history folder.

**Use:** List the archives and their sizes.

**Cost:** moderate

**Related:** `concept:archive`

### `route:GET /api/v1/debug/trace`

All events of one message, by partition and offset.

**Use:** Follow one message through arrange, tasks, deliveries and commit.

**Cost:** moderate

### `route:GET /api/v1/debug/label-keys`

The distinct label keys of recent tasks.

**Use:** Learn which task labels exist before a trace by label.

**Cost:** cheap

### `route:GET /api/v1/debug/trace-by-label`

The events of the tasks with one label value.

**Use:** Follow the work of one business key, such as a customer id.

**Cost:** moderate

### `route:GET /api/v1/debug/metrics`

The Prometheus metric families of the worker as JSON.

**Use:** Read raw metric values; the agent metrics route adds their meaning.

**Cost:** moderate

### `route:GET /api/v1/debug/periodic`

The history of the periodic task runs.

**Use:** Check when a periodic task last ran and how it ended.

**Cost:** moderate

**Related:** `concept:cluster-tasks`

### `route:GET /api/v1/debug/runtime/units`

A census of the live concurrency units, grouped by where they wait.

**Use:** See what the event loop carries during a slowdown.

**Cost:** moderate

**Related:** `recipe:worker-stalled`

### `route:POST /api/v1/debug/probe`

Runs one message through the handler as a dry run.

**Use:** Test what the handler does with one message; it runs the tasks but writes nothing to sinks, offsets, the recorder or the cache.

**Cost:** heavy

### `route:GET /api/v1/debug/cache/entries`

Pages through the cache entries.

**Use:** Inspect what the cache holds.

**Cost:** moderate

### `route:GET /api/v1/debug/cache/entry/{key}`

One cache entry with its decoded value.

**Use:** Read one cache entry.

**Cost:** cheap

### `route:GET /api/v1/debug/cache/stats`

The memory and disk numbers of the cache.

**Use:** Check the size of the cache.

**Cost:** cheap

### `route:GET /api/v1/debug/consume-pause`

The state of the operator consume pause.

**Use:** Check whether an operator paused consuming.

**Cost:** cheap

### `route:POST /api/v1/debug/consume-pause`

Pauses consuming for a bounded time.

**Use:** Stop new input for a short time; it changes the worker, so only an operator should call it.

**Cost:** cheap

### `route:POST /api/v1/debug/consume-resume`

Resumes consuming now.

**Use:** End an operator pause; it changes the worker, so only an operator should call it.

**Cost:** cheap

**Related:** `rule:partition-paused-by-debug`

### `route:GET /api/v1/debug/kafka/topics`

The Kafka topic aliases this worker may read.

**Use:** Learn the aliases before reading a message or the DLQ.

**Cost:** cheap

### `route:GET /api/v1/debug/kafka/{alias}/message`

One Kafka message by partition and offset.

**Use:** Read the input or a DLQ record of one message.

**Cost:** moderate

### `route:GET /api/v1/debug/kafka/{alias}/messages`

The Kafka messages of one time window, streamed as lines of JSON.

**Use:** Read many records, such as the recent DLQ records.

**Cost:** heavy

**Related:** `recipe:dlq-filling`

### `route:GET /api/v1/config-reference`

Every config key with its description, default and live value.

**Use:** Read the config in the form of the UI; the agent config route adds origin and advice.

**Cost:** moderate

### `route:GET /api/v1/pages`

The UI pages the handler declared.

**Use:** List the custom pages of the deployment.

**Cost:** cheap

### `route:GET /api/v1/ui/renderers.js`

The custom cell renderers module of the deployment.

**Use:** Not needed by an agent; the UI loads it.

**Cost:** cheap

### `route:GET /api/v1/openapi.json`

The API document as JSON.

**Use:** Read the exact shapes of every route.

**Cost:** heavy

### `route:GET /api/v1/peer/cache/changes`

The cache rows this worker wrote after a cursor, for its peers.

**Use:** Not needed by an agent; peers call it for the cache exchange.

**Cost:** moderate

**Related:** `concept:cache-exchange`

### `route:GET /api/v1/agent`

The index of the agent routes, with the parts of each route and the approximate size of each part.

**Use:** Call it first; it names the routes, their parts and their size, so you can ask only for what you need.

**Cost:** cheap

**Related:** `concept:agent-surface`

### `route:GET /api/v1/agent/manual`

The knowledge of the framework, such as concepts, config advice, metrics, events, recipes, routes and rules.

**Use:** Read one topic with topic=&lt;kind&gt;:&lt;key&gt;, or one part with parts=; brief entries give only the key and the summary.

**Cost:** moderate

**Related:** `concept:agent-surface`

### `route:GET /api/v1/agent/config`

The effective config of the worker, with the origin, the default and the meaning of each key; secrets are masked.

**Use:** Check which value the worker really runs with and where the value came from.

**Cost:** moderate

**Related:** `concept:agent-surface`

### `route:GET /api/v1/agent/metrics`

The current samples of the framework and handler metrics, with the meaning of each metric.

**Use:** Read the value of a metric now, with names= or prefix= to keep the answer small.

**Cost:** moderate

**Related:** `concept:agent-surface`

### `route:GET /api/v1/agent/state`

One snapshot of the live state, such as health, the pending budget, partitions, sinks, the recorder and recent failures.

**Use:** See what the worker does now, with parts= to keep the answer small.

**Cost:** moderate

**Related:** `concept:agent-surface`

### `route:GET /api/v1/agent/diagnose`

The findings of the diagnose rules on one snapshot of the state and the metrics.

**Use:** Call it after the index to find problems; each finding names the manual topic that explains it.

**Cost:** moderate

**Related:** `concept:agent-surface`

## Rules

### `rule:worker-stalled`

The event loop of the worker does not run now.

**Severity:** critical

**Title:** The event loop is stalled

**Explain:** `concept:runtime-health`

**Suggestion:** Read the last runtime_stall event for the stack that blocks the loop, and move that call off the event loop.

**Inputs:**
- `health.state`
- `health.heartbeat_age_ms`

### `rule:loop-lag-high`

The event loop ran late by runtime_health.warn_lag or more in the last 60 seconds.

**Severity:** warning

**Title:** The event loop runs late

**Explain:** `concept:runtime-health`

**Suggestion:** Read the open lag episode and its verdict; offload CPU work for cpu_bound, find the blocking call for blocked, give the worker more CPU for starved.

**Inputs:**
- `health.window_peak_lag_ms`
- `health.state`
- `runtime_health.warn_lag`

### `rule:budget-saturated`

The pending tasks or the pending bytes reached their limit, so partition loops wait before they take new windows.

**Severity:** warning

**Title:** New windows wait for the pending budget

**Explain:** `concept:pending-budget`

**Suggestion:** When the executor pool is busy, add workers or make tasks faster; when it is idle, find the slow hook or sink delivery.

**Inputs:**
- `budget.tasks`
- `budget.max_tasks`
- `budget.bytes`
- `budget.max_bytes`

### `rule:pool-saturated`

Every executor slot is busy and tasks wait for a slot.

**Severity:** info

**Title:** The executor pool is full

**Explain:** `concept:task`

**Suggestion:** When the host has free CPU, raise executor.max_executors; otherwise add workers.

**Inputs:**
- `budget.pool_active`
- `budget.pool_max`
- `budget.pool_waiting`

### `rule:partition-dead`

A partition loop stopped after an unexpected error and its one restart; the partition makes no progress.

**Severity:** critical

**Title:** A partition loop is dead

**Explain:** `concept:offsets`

**Suggestion:** Read the partition_processor_died log line for the error, fix the cause and restart the worker.

**Inputs:**
- `partitions[].dead`
- `partitions[].partition`

### `rule:partition-paused-by-debug`

An operator paused consuming from the UI; the partition takes no new messages until the pause ends.

**Severity:** info

**Title:** Consuming is paused by an operator

**Explain:** `concept:backpressure`

**Suggestion:** Resume consuming from the Live page or with POST /api/v1/debug/consume-resume when the pause is no longer needed.

**Inputs:**
- `partitions[].paused_by`

### `rule:sink-breaker-open`

The circuit breaker of a sink is open; its deliveries follow the on_failure policy without a call to the sink.

**Severity:** critical

**Title:** A sink circuit breaker is open

**Explain:** `concept:sinks-and-delivery`

**Suggestion:** Check the downstream system of the sink and its connection; the breaker closes by itself after a successful probe.

**Inputs:**
- `sinks[].breaker_open`
- `sinks[].type`
- `sinks[].name`

### `rule:lag-growing`

The sum of the broker lag of the partitions grew since a diagnose call at least 60 seconds earlier.

**Severity:** warning

**Title:** The consumer lag grows

**Explain:** `recipe:lag-growing`

**Suggestion:** Compare the input rate with the completed task rate, then follow the lag-growing recipe.

**Inputs:**
- `partitions[].lag`

### `rule:recorder-dropping`

The recorder dropped events or Postgres rows since the previous diagnose call; the event history has gaps.

**Severity:** warning

**Title:** The recorder drops events

**Explain:** `recipe:recorder-dropping`

**Suggestion:** Check the disk or the Postgres table of the recorder, and the recorder queue limits.

**Inputs:**
- `drakkar_recorder_dropped_events_total`
- `drakkar_recorder_postgres_dropped_rows_total`

### `rule:dlq-writing`

The worker wrote DLQ records since the previous diagnose call.

**Severity:** info

**Title:** Messages go to the DLQ

**Explain:** `concept:dlq`

**Suggestion:** Read the dk-stage and dk-error headers of the new DLQ records to find the failing stage.

**Inputs:**
- `drakkar_sink_dlq_messages_total`

### `rule:peers-offline`

Some peers in the registry did not publish their card in time and count as offline.

**Severity:** warning

**Title:** Peers of the cluster are offline

**Explain:** `recipe:peer-missing`

**Suggestion:** Check that the offline workers run; a worker that was removed leaves its card until autodiscover.prune_after.

**Inputs:**
- `autodiscover.peers_offline`

### `rule:cluster-tasks-mismatch`

Online peers of this cluster declare other cluster tasks or other schedules than this worker.

**Severity:** warning

**Title:** Cluster task definitions differ in the cluster

**Explain:** `concept:cluster-tasks`

**Suggestion:** Finish the rolling change, or deploy the same handler code and config on every worker of the cluster.

**Inputs:**
- `autodiscover.cluster_tasks_match`

### `rule:agent-unauthenticated`

The agent routes need no token; anyone who reaches the API port can read the config and the state.

**Severity:** info

**Title:** The agent routes have no token

**Explain:** `concept:agent-surface`

**Suggestion:** Set agent.auth_token, or keep the API port on a private network.

**Inputs:**
- `agent.auth_token`
