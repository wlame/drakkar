# Drakkar Data Flow: Complete Processing Story

## Architecture Overview

Drakkar is a Python 3.13+ framework that consumes messages from a Kafka topic, fans them out to per-partition async processors, runs user-defined logic as subprocesses, and delivers results to one or more output sinks (Kafka, PostgreSQL, MongoDB, HTTP, Redis, filesystem). The architecture follows a pipeline model: **poll -> partition -> window -> arrange -> execute -> on_task_complete -> on_message_complete -> on_window_complete -> deliver -> commit**. A single Drakkar worker runs one async event loop (`asyncio.run`) that hosts all partition processors, a shared subprocess executor pool with semaphore-based concurrency control, a sink manager, a dead-letter queue (DLQ), optional Prometheus metrics, and an optional flight recorder with an operator web UI. The user implements a handler class with hooks (`arrange`, `on_task_complete`, `on_message_complete`, `on_window_complete`, `on_error`, `on_delivery_error`, plus lifecycle hooks like `on_startup`/`on_ready`/`on_assign`/`on_revoke`) that define the application-specific logic; everything else -- Kafka consumption, offset tracking, backpressure, retries, subprocess lifecycle, sink connections, and graceful shutdown -- is managed by the framework. See [Fan-out](fan-out.md) for a dedicated walkthrough of the per-task / per-message / per-window hook trio.

Configuration is loaded from a YAML file (path via `config_path` argument or `DK_CONFIG` env var) with environment variable overrides using the `DK_` prefix and `__` as a nesting delimiter (e.g., `DK_CONNECTIONS__KAFKA__MAIN__BROKERS`). Environment variables are deep-merged on top of YAML values. The root configuration object is `DrakkarConfig`, a Pydantic `BaseSettings` model. All config fields referenced below are part of this hierarchy.

---

## Phase 0: Worker Startup

### 0.1 Initialization

When `DrakkarApp.run()` is called, the framework:

1. Resolves the **worker identity**:
   - Reads the environment variable named by `worker.name_env` (default: `'WORKER_ID'`).
   - If it is unset or empty, uses `worker.name`; if that is empty too, a hex-encoded Python `id()` like `drakkar-7f3a2b`.
2. Resolves the **cluster name**:
   - If `cluster.name_env` is set and the corresponding env var is non-empty, uses that.
   - Otherwise falls back to `cluster.name` (default: `''`).
3. Configures **structured logging** via structlog using `logging.level` (default: `'INFO'`) and `logging.format` (default: `'json'`, also accepts `'console'`). The worker_id, consumer_group, and framework version are bound as context variables to every log line.
4. Calls the handler's `on_startup(config)` hook, which receives the full `DrakkarConfig` and may return a modified copy. This is the only point where the user can mutate configuration before the framework wires everything up. A few settings are consumed earlier, while `DrakkarApp` is constructed (the `app:` section and the worker/cluster name resolution above), so changing those here has no effect and the worker logs one `on_startup_config_change_ignored` warning naming them -- see [Handler](handler.md#on_startup).

### 0.2 Component Construction

After `on_startup`, the framework builds all components in this order:

1. **Validates sinks are configured** -- if `sinks.is_empty` is True (no sinks of any type defined) and results go to sinks (`sources.delivers_to_sinks`: the Kafka source is on, or `sources.http.deliver_to_sinks` is true), raises `SinkNotConfiguredError` immediately. An HTTP-only worker with `deliver_to_sinks: false` may have no sinks.

2. **Creates the ExecutorPool** with:
   - `executor.binary_path` (default: `None` -- each task must then provide its own `binary_path`)
   - `executor.max_executors` (default: `4`, min: `1`) -- controls the `asyncio.Semaphore` size
   - `executor.task_timeout` (default: `120`, min: `1`) -- per-subprocess wall-clock timeout

3. **Starts the Prometheus metrics server** on `metrics.port` (default: `9090`) if `metrics.enabled` (default: `True`). Publishes `worker_info` with worker_id, version, and consumer_group labels.

4. **Resolves the periodic tasks** ([Periodic Tasks](periodic-tasks.md)): finds the handler's methods decorated with `@periodic`. It splits the tasks into worker tasks and cluster tasks and computes the definition hash of the cluster tasks (`history.archive` included when the worker runs it), so the registry card carries it from its first publish. When the worker runs at least one cluster task, it also makes the election service from `autodiscover`: an unknown provider name, or the `filesystem` provider without a shared folder, fails startup. The provider itself starts later, at the first slot that needs it.

5. **Recorder, API server and autodiscover**, each only when its switch is on:
   - With `recorder.enabled` or `ui.enabled`, creates an `EventRecorder` with the recorder config, worker name, and cluster name. (With both off there is no recorder object.)
   - Sets up a state provider callback so the recorder can periodically snapshot worker state (uptime, partitions, pool utilization, queue depth) every `recorder.sqlite.state_interval` (default: `10`) seconds.
   - Starts the recorder (creates/opens the SQLite database, starts flush/rotation/state-sync background tasks).
     Once the worker runs, a background job seals and moves files an earlier process with the same worker name left in `storage.dir` ([leftovers](local-databases.md#leftovers)); startup never waits on it.
   - Writes the full worker configuration to the `worker_config` table (if `recorder.capture.config` is True, default: `True`), so every recorder file names the worker and cluster that wrote it.
   - Creates and starts the API server (FastAPI) on `api.port` (default: `8080`) when `api.enabled`. It always answers `/healthz`, `/readyz` and the peer route; with `ui.enabled` it also provides the web UI, JSON API, WebSocket event streaming, and database download/merge endpoints.
   - Starts the [fleet registry](fleet.md) service (when `autodiscover.enabled`): it publishes this worker's card with state `starting` (with the cluster task hash in `features.periodic`) and reads the peer cards in the background. After each read it compares the hash with the online peers of its cluster. The card moves to `running` once the sources start and to `draining` when shutdown begins; a clean stop withdraws it after the recorder stops and before the API server stops.

6. **Builds all sinks** from config by iterating each type's dict:
   - `sinks.kafka` -- creates a `KafkaSink` for each named entry, on the cluster of its `connection`
   - `sinks.postgres` -- creates a `PostgresSink` for each, on the database of its `connection`
   - `sinks.mongo` -- creates a `MongoSink` for each, on the server of its `connection`
   - `sinks.http` -- creates `HttpSink(name, config)` for each
   - `sinks.redis` -- creates a `RedisSink` for each, on the server of its `connection`
   - `sinks.filesystem` -- creates `FileSink(name, config)` for each
   Each is registered with the `SinkManager` under a `(sink_type, name)` key.

7. **Connects all sinks** by calling `connect()` on each in registration order. If any `connect()` raises, the worker crashes immediately (fail-fast design).

8. **Builds and connects the DLQ sink**:
   - Topic: `dlq.topic` if non-empty, otherwise `'{sources.kafka.topic}_dlq'` (default source topic: `'input-events'`). With the Kafka source disabled and `dlq.topic` empty, **no DLQ producer is built** -- see [the DLQ rule](sources.md#the-dlq-without-the-kafka-source).
   - Cluster: the brokers, `security` and `client_config` of `connections.kafka.<dlq.connection>`, with `dlq.client_config` merged after them.
   - Connects a dedicated Kafka producer for the DLQ.

9. **Builds the enabled input sources**, in the fixed order `kafka`, then `http`. A source whose `enabled` flag is false is not built at all. The Kafka source creates a `KafkaConsumer` (wrapping `confluent_kafka.aio.AIOConsumer`) with:
   - `bootstrap.servers`: the brokers of `connections.kafka.<sources.kafka.connection>` (default `'localhost:9092'`)
   - `group.id`: `sources.kafka.consumer_group` (default: `'drakkar-workers'`)
   - `enable.auto.commit`: `False` (offsets are committed manually by the framework)
   - `auto.offset.reset`: `'earliest'`
   - `partition.assignment.strategy`: `'cooperative-sticky'`
   - `max.poll.interval.ms`: `sources.kafka.max_poll_interval` (default: `300000` = 5 minutes)
   - `session.timeout.ms`: `sources.kafka.session_timeout` (default: `45000` = 45 seconds)
   - `heartbeat.interval.ms`: `sources.kafka.heartbeat_interval` (default: `3000` = 3 seconds)
   - Assign/revoke callbacks are wired to `_on_assign` and `_on_revoke`.

   The HTTP source builds the webapp server. See [Input Sources](sources.md).

10. **Exposes the PostgreSQL connection pool** for the handler: if any postgres sink exists, its `asyncpg.Pool` is extracted and passed to the next hook.

11. **Calls the handler's `on_ready(config, pg_pool)` hook**. The user can use this for database migrations, loading lookup tables, initializing caches, or any async setup that requires live connections.

12. **Starts the periodic tasks** resolved in step 4:
    - A **worker** task runs in its own `asyncio.Task`. With an interval it sleeps the interval, runs, and sleeps again from the end of the run; with a cron expression it sleeps until the next fire time. Runs of one task never overlap.
    - A **cluster** task runs a slot loop in its own `asyncio.Task`: at each slot it starts the election provider if needed, takes the lease of `<cluster>/<task>`, and runs the task only when it won the lease ([the slot rule](periodic-tasks.md#cluster-tasks-the-slot-rule)).
    - The system cluster task `history.archive` starts here too, when the recorder runs and archiving is on.
    - If the coroutine raises an exception:
      - `on_error='continue'` (default): logs the error and continues looping.
      - `on_error='stop'`: logs the error and exits the task permanently.

13. **Starts the sources** in the same fixed order and logs `sources_starting` / `source_started`. The Kafka source subscribes to the topic and enters the poll loop; the HTTP source binds its socket. A source that fails to start is fatal: the worker logs `source_start_failed`, tears down, and stops.

14. **Registers signal handlers** for `SIGINT` and `SIGTERM` that set `_running = False`, triggering graceful shutdown.

---

## Phase 1: Polling Messages from Kafka (Kafka source)

!!! note "HTTP-only workers skip this"
    Phases 1, 2 and 9 belong to the Kafka source. A worker with
    `sources.kafka.enabled: false` runs none of them: it has no poll loop,
    no partition assignment, and no offsets to commit. Its work enters at
    [the HTTP source path](#http-source-path) below and continues from
    Phase 3.

The main poll loop runs continuously while `_running` is True. Each iteration:

### 1.1 Check Backpressure

The framework counts the **queued messages**: the messages waiting in the
queues of live partition processors. A dead processor's queue is left out,
because nothing will empty it. Partition loops take windows only while the
[pending budget](#31-collecting-a-window) has room, so a full budget fills
the queues and the fetch pauses by itself.

- First, each partition whose loop died is paused in Kafka, once, and stays
  paused.
- **Pause condition**: not paused AND queued messages >=
  `sources.kafka.max_queued_messages` (default `1000`): calls
  `consumer.pause(all_assigned_partition_ids)`, sets the
  `drakkar_backpressure_active` gauge to `1` and `_paused = True`.
- **Resume condition**: paused AND queued messages <= half of
  `max_queued_messages` AND no operator debug pause: calls
  `consumer.resume(...)` for every partition that is neither stalled nor
  dead, sets the gauge to `0` and `_paused = False`.

The poll below runs on every turn, paused or not, so Kafka's
`max.poll.interval` never expires.

### 1.2 Poll a Batch

The consumer calls `consume(num_messages=count, timeout=1.0)` on the underlying `confluent_kafka.aio.AIOConsumer`, where `count` defaults to `sources.kafka.max_poll_records` (default: `100`).

- **If messages are returned**: each message is wrapped in a `SourceMessage` object containing `topic`, `partition`, `offset`, `key` (bytes or None), `value` (bytes), and `timestamp` (milliseconds, Kafka-provided).
- **If a `PARTITION_EOF` error is received**: silently ignored (normal condition when consumer reaches end of partition).
- **If any other Kafka error occurs**: increments the `consumer_errors` Prometheus counter and logs a warning. The message is skipped.

### 1.3 Dispatch to Partition Processors

Each `SourceMessage` is routed to its corresponding `PartitionProcessor` by `msg.partition`:

```
processor = self._processors.get(msg.partition)
if processor:
    processor.enqueue(msg)
```

The `enqueue()` call is non-blocking (`queue.put_nowait()`). It also:
- Records a `consumed` event in the flight recorder (if the UI is enabled).
- Increments the `messages_consumed` Prometheus counter (labeled by partition).
- Updates the `partition_queue_size` gauge.

If no processor exists for the partition (shouldn't happen under normal operation), the message is silently dropped.

### 1.4 Idle Backoff

If no messages were returned by the poll, the loop sleeps for **50ms** (`asyncio.sleep(0.05)`) to avoid busy-spinning.

---

### HTTP source path

A request that arrives on the HTTP source does not pass through Phases 1
and 2. The webapp runner synthesises one virtual `SourceMessage` with
`partition = -1` and a monotone per-worker offset. It does not use the
window or the partition processor. It calls `arrange_http_request()`, runs
the tasks it returns through the executor pool directly (all tasks of the
request at the same time), and delivers the result to the sinks itself.
The synthetic partition keeps HTTP work out of the partition
trackers in the recorder and the operator UI. There are no offsets to
commit, so Phase 9 does not apply either: the result goes back in the HTTP
response, and to the sinks when `sources.http.deliver_to_sinks` is true. See
[Webapp](webapp.md) for the request lifecycle and status codes.

---

## Phase 2: Partition Assignment and Revocation

### 2.1 On Assign (New Partitions)

When Kafka's cooperative-sticky rebalancer assigns new partitions to this worker:

1. Records an `assigned` event per partition in the flight recorder.
2. For each newly assigned partition ID (skipping already-known ones):
   - Creates a new `PartitionProcessor` with:
     - The partition ID
     - The user's handler instance
     - The shared `ExecutorPool`
     - `executor.window_size` (default: `100`, min: `1`) -- max messages per window
     - `executor.max_retries` (default: `3`, min: `0`) -- retry limit per failed task
     - Callbacks for sink delivery (`_handle_collect`) and offset commit (`_handle_commit`)
     - The flight recorder (if the UI is enabled)
   - Starts the processor's background `asyncio.Task` immediately.
3. Updates the `assigned_partitions` Prometheus gauge.
4. Calls the handler's `on_assign(partition_ids)` hook asynchronously (fire-and-forget with exception logging).

### 2.2 On Revoke (Lost Partitions)

When partitions are revoked (rebalance, scaling event):

The revoke callback **blocks until every revoked partition has drained and
committed**. `AIOConsumer` runs rebalance callbacks via
`run_coroutine_threadsafe(...).result()`, so librdkafka's rebalance thread
waits here — which is what holds the rebalance open until our offsets are
committed. Returning early would let the new owner start consuming from the
last committed offset while in-flight work here was still delivering the
same messages, duplicating everything between the last commit and the drain
end.

The wait is bounded, not open-ended: each stop sequence runs against one
`executor.drain_timeout` deadline covering the drain, the final commit
and the processor stop, and it suppresses deliveries when the drain expires,
so the callback always returns within roughly that budget. That bound
matters — `run_coroutine_threadsafe(...).result()` has no timeout of its own,
so an unbounded wait would wedge the consumer thread permanently, and a
callback that overruns `max.poll.interval.ms` gets the worker evicted from the
group. Teardown runs concurrently across partitions, so N revoked partitions
cost one drain timeout, not N.

1. Records a `revoked` event per partition.
2. For each revoked partition (concurrently, all awaited before returning):
   - Removes the processor from `_processors`.
   - Runs the stop sequence:
     1. Sets `processor._running = False`.
     2. Waits up to `executor.drain_timeout` (default: `30`, min: `1`) for in-flight work to complete.
     3. **Only if drain completed cleanly**, commits the offset watermark. If drain timed out, in-flight tasks may still be running and committing their offsets would silently skip them on reassign — safer to let at-least-once replay recover them.
     4. **On drain timeout**, the still-running tasks are marked as *zombies*: their late results are suppressed (no sink deliveries, no offset commits) because the new partition owner replays their messages from the last committed offset — delivering here would be a guaranteed double-write, and a late commit could clobber the new owner's progress. Suppressions are counted in `drakkar_suppressed_zombie_deliveries_total` and logged as `zombie_delivery_suppressed`. The same suppression applies to tasks that outlive the shutdown drain timeout.
     5. **On drain timeout**, the zombies are then cancelled, which kills each subprocess' process group and returns its executor slot immediately. Left running they would hold a slot until `executor.task_timeout`, queuing work from the partitions this worker still owns behind them. The cancellation is logged as `zombie_tasks_cancelled`. Their offsets stay uncommitted, exactly as in step 3.
     6. Calls `processor.stop()` with what is left of the deadline.
3. Updates the `assigned_partitions` gauge.
4. Calls the handler's `on_revoke(partition_ids)` hook on a background task. The hook is a user notification, not part of the commit contract, so a slow hook does not eat into the rebalance budget.

---

## Phase 3: Window Collection and Arrangement

Each `PartitionProcessor` runs its own async loop independently. It does not wait for one window to fully complete before starting the next -- multiple windows can be in-flight concurrently.

!!! note "Why one processor per partition?"
    Kafka tracks and commits offsets per partition, so each `PartitionProcessor` owns an offset watermark and a FIFO queue for its partition alone. Running one `arrange()` coroutine per partition keeps offset bookkeeping and in-partition ordering as local state, and makes rebalances a matter of starting or stopping one task per affected partition -- there is no cross-partition state to untangle when assignments change.

### 3.1 Collecting a Window

The processor waits for messages from its queue:

1. **Blocking wait for the first message**: `await asyncio.wait_for(queue.get(), timeout=1.0)`.
   - **If timeout (1 second, no messages)**: returns an empty list. The processor then attempts an offset commit for any previously completed work and loops back.
   - **If a message arrives**: it is held as the first message of the next window (it still counts as queued).
2. **Wait for room in the pending budget**: one budget per worker counts the pending tasks (created and not settled) and pending bytes (their stdin plus the output of their results while `on_task_complete`, `on_error` and the sink delivery run, or while `on_message_complete` and `on_window_complete` keep it). While either reaches its limit (`executor.max_pending_tasks`, `executor.max_pending_bytes`), the loop waits in a first-in, first-out list; a release that makes room wakes the oldest waiter. Each wait lasts at most 1 second, then the loop retries a commit and comes back with its place kept. A stop ends the wait at once.
3. **Non-blocking drain**: calls `queue.get_nowait()` in a tight loop until:
   - The queue is empty, OR
   - The window reaches the admitted size. During the partition's warm-up (its first 3 windows, whether they created tasks or not) that is one message, and the window reserves all the free room. After it: `min(free tasks / tasks per message, free bytes / bytes per message)`, clamped to 1 .. `executor.window_size`. The per-message numbers are moving averages of tasks, stdin bytes and kept output bytes (the output of a task in its own hooks and delivery is charged but not estimated) (weight 0.5 for a larger sample, 0.05 for a smaller one), from the partition's own windows or, before it has one, from the whole worker, raised to a floor of `max_pending_tasks / window_size` tasks and `max_pending_bytes / window_size` bytes. A window whose `arrange()` created no tasks is not measured. Before the worker measured a window with tasks, one window of one message is arranged at a time. The free room excludes what other windows reserved.
4. Updates the `partition_queue_size` gauge and observes `drakkar_window_messages`.

The resulting list of 1 to `window_size` messages forms a **window**. Right
after it is taken, with no await in between, the window **reserves** its
estimated tasks and bytes in the budget; the fan-out charges the real tasks
out of the reservation, and the rest is given back when the fan-out ends. With a
steady message shape the peak is therefore about the cap plus one message for
the whole worker. After the warm-up, a sudden jump in tasks per message can
pass the cap by the size of that jump (at most one window's excess), because
the fan-out is known only after `arrange()` returns. The
`drakkar_pending_tasks` and `drakkar_pending_bytes` gauges show the real
numbers.

### 3.2 Processing a Window

For each window:

1. **Increment the window counter** (unique per partition, monotonically increasing).

2. **Register all message offsets as PENDING** in the `OffsetTracker`. The tracker uses `bisect.insort()` to maintain a sorted list of offsets for efficient watermark calculation.

3. **Deserialize each message**: calls `handler.deserialize_message(msg)`. If the handler class was declared with a generic `InputT` type parameter (e.g., `class MyHandler(BaseDrakkarHandler[MyInput, MyOutput])`), the framework automatically calls `InputT.model_validate_json(msg.value)` and sets `msg.payload` to the parsed Pydantic model. If parsing fails, `msg.payload` is set to `None`. If no `InputT` is declared, the raw bytes remain in `msg.value`.

4. **Build PendingContext**: creates a snapshot of currently in-flight tasks for this partition:
   - `pending_tasks`: list of `ExecutorTask` objects currently being run
   - `pending_task_ids`: set of their task IDs for O(1) deduplication checks
   This allows `arrange()` to avoid creating duplicate tasks for messages that map to already-running work.

5. **Call `handler.arrange(messages, pending_ctx)`**: the core user hook that maps a window of messages to a list of `ExecutorTask` objects. The arrange hook receives:
   - `messages`: the window of `SourceMessage` objects (with `.payload` already parsed)
   - `pending`: the `PendingContext` for deduplication

   The hook returns `list[ExecutorTask]`, where each task has:
   - `task_id` (str): unique identifier (typically from `make_task_id()`)
   - `args` (list[str]): command-line arguments for the subprocess
   - `metadata` (dict): arbitrary data carried through the pipeline
   - `source_offsets` (list[int]): which Kafka offsets this task covers
   - `binary_path` (str | None): optional per-task binary override
   - `stdin` (str | None): optional string written to process stdin

   The framework records the `arrange_duration` in the `handler_duration` Prometheus histogram and in the flight recorder.

   !!! tip "Cache interaction in arrange()"
       If `cache.enabled=true` ([Cache](cache.md)), `arrange()` can consult `self.cache.peek(key)` (synchronous memory probe, zero I/O) or `await self.cache.get(key)` (memory → SQLite fallback) to short-circuit tasks. On a hit, attach the cached output to the task via [`precomputed=PrecomputedResult(...)`](handler.md#precomputed-task-results-skip-the-subprocess) — the framework skips the semaphore and subprocess entirely and feeds the result straight to `on_task_complete`.

6. **If arrange returns an empty list**: all message offsets are immediately marked COMPLETED, an offset commit is attempted, and the processor moves to the next window. No tasks are run.

7. **If arrange returns tasks**: for each task:
   - Check for duplicate `task_id` in pending tasks; log a warning if found.
   - Add to `_pending_tasks` dict.
   - Increment `_inflight_count`.
   - Create an `asyncio.Task` wrapping `_execute_and_track(task, window)`.
   - Track the async task handle in `_active_tasks` for cleanup.

   All tasks within a window are launched concurrently. The processor immediately returns to collecting the next window -- it does not wait for these tasks to finish.

---

## Phase 4: Subprocess Execution

### 4.1 Acquiring an Executor Slot

The `ExecutorPool` uses an `asyncio.Semaphore(max_executors)` to limit concurrent subprocess runs across all partitions.

1. The task enters the **waiting state**: `waiting_count` is incremented.
2. `async with self._semaphore:` -- blocks until a slot is available.
3. On acquiring the semaphore: `waiting_count` is decremented, `active_count` is incremented, and a **slot ID** (0 to max_executors-1) is popped from the available slots list.
4. If the flight recorder is enabled, a `task_started` event is recorded with the current `pool_active` and `pool_waiting` counts and the allocated slot number.

### 4.2 Launching the Subprocess

1. **Binary resolution**: The framework uses `task.binary_path` if set; otherwise falls back to `executor.binary_path` from config. If neither is set, the task fails immediately with `exit_code=-1` and a descriptive error.

2. **Process creation** via `asyncio.create_subprocess_exec()`:
   - First argument: the resolved binary path
   - Remaining arguments: `task.args` (passed as individual arguments, not via shell -- prevents shell injection)
   - `stdin`: `asyncio.subprocess.PIPE` if `task.stdin is not None`, else `None` (not connected)
   - `stdout`: `asyncio.subprocess.PIPE` (always captured)
   - `stderr`: `asyncio.subprocess.PIPE` (always captured)

3. **Communication**: `proc.communicate(input=stdin_bytes)` writes stdin (if any) and waits for the process to exit, capturing all stdout and stderr. When `executor.max_stdout` / `max_stderr` are set, the pipes are streamed instead: up to the cap is retained per stream, the rest is read and discarded, and the result's `stdout_truncated` / `stderr_truncated` flags are set.

4. **Timeout enforcement**: the entire capture call is wrapped in `asyncio.wait_for(timeout=executor.task_timeout)` (default: `120` seconds).

### 4.3 Possible Outcomes

**Outcome A -- Success (exit_code == 0)**:
- An `ExecutorResult` is created with:
  - `exit_code`: 0
  - `stdout`: process stdout decoded as UTF-8 (with `errors='replace'` for invalid bytes)
  - `stderr`: process stderr decoded as UTF-8
  - `duration_seconds`: wall-clock time rounded to 3 decimal places (measured with `time.monotonic()`)
  - `task`: the original `ExecutorTask` (carried through for context)
  - `pid`: the OS process ID
- The result is returned normally to the caller.

**Outcome B -- Non-zero exit code**:
- An `ExecutorResult` is created as above but with the actual non-zero `exit_code`.
- An `ExecutorTaskError` is raised, wrapping both:
  - `ExecutorError(task, exit_code, stderr, pid=pid)` -- error context for the handler
  - `ExecutorResult` -- full result including stdout/stderr for inspection

**Outcome C -- Timeout**:
- The process is killed (`proc.kill()` + `await proc.wait()`) in the `finally` block.
- A synthetic `ExecutorResult` is created with `exit_code=-1`, `stdout=''`, `stderr='task timed out'`.
- An `ExecutorTaskError` is raised with `ExecutorError(task, stderr='task timed out', exception='Timeout after {N}s', pid=...)`.
- The `executor_timeouts` Prometheus counter is incremented.

**Outcome D -- OS error (binary not found, permission denied, etc.)**:
- A synthetic `ExecutorResult` is created with `exit_code=-1`, `stdout=''`, `stderr=str(e)`.
- An `ExecutorTaskError` is raised with `ExecutorError(task, exception=str(e))` -- no `exit_code` or `pid` since the process never started.

### 4.4 Releasing the Executor Slot

In the `finally` block (guaranteed to run):
1. The slot ID is returned to the available slots list (re-sorted to maintain ascending order).
2. `active_count` is decremented.
3. The semaphore is released, allowing the next waiting task to proceed.

---

## Phase 5: Post-Execution -- Success Path

When a task succeeds (exit_code == 0):

### 5.1 Metrics and Recording

- `executor_tasks` counter incremented with `status='completed'`.
- `executor_duration` histogram observes the `duration_seconds`.
- Flight recorder records a `task_completed` event with pool utilization stats.

### 5.2 on_task_complete Hook

The framework calls `handler.on_task_complete(result)`:
- Receives the full `ExecutorResult` including stdout, stderr, exit_code, duration, and the original `ExecutorTask` (with its metadata dict).
- **Returns `CollectResult`**: a container with typed payload lists for each sink type:
  - `kafka`: list of `KafkaPayload(sink='', key=b'...', data=MyModel(...))`
  - `postgres`: list of `PostgresPayload(sink='', table='results', data=MyModel(...))`
  - `mongo`: list of `MongoPayload(sink='', collection='events', data=MyModel(...))`
  - `http`: list of `HttpPayload(sink='', data=MyModel(...))`
  - `redis`: list of `RedisPayload(sink='', key='cache:123', data=MyModel(...), ttl=3600)`
  - `files`: list of `FilePayload(sink='', path='output/results.jsonl', data=MyModel(...))`
- **Returns `None`**: no per-task sink delivery for this result. The result is still tracked in the `MessageGroup` and available to `on_message_complete`.

The hook's duration is recorded in the `handler_duration{hook="on_task_complete"}` histogram and a `task_complete` event lands in the flight recorder.

!!! tip "Cache interaction in on_task_complete()"
    This is where handlers typically **persist** results back into `self.cache` via `self.cache.set(key, result.stdout, ttl=...)` — the write lands in the in-memory dict immediately and is flushed to `<worker>-cache.db` on the next flush cycle. Skip the set when `result.pid is None` (precomputed fast-path) — those results came from the cache in the first place. See [Cache](cache.md) for the full API.

### 5.3 Sink Delivery

If `on_task_complete()` returned a non-None `CollectResult` with `has_outputs == True`:

1. **Validation**: `SinkManager.validate_collect(result)` checks that every payload's `sink` field resolves to a configured sink instance:
   - If `sink` is empty and exactly one sink of that type exists: resolves to the default.
   - If `sink` is empty and multiple sinks of that type exist: raises `AmbiguousSinkError`.
   - If `sink` names a non-existent instance: raises `SinkNotConfiguredError`.
   These errors crash the worker (fail-fast at validation, not at delivery time).

2. **Grouping**: payloads are grouped by `(sink_type, sink_name)` for batched delivery.

3. **Delivery per sink** (see Phase 8 for full delivery details).

The result is appended to the window's `results` list AND to the per-message `MessageGroup.results` list for every offset in `task.source_offsets`.

### 5.4 Message-Group Tracking Update

Per-task outcome updates the tracker for every source message this task belongs to (usually just one; multi-offset fan-in tasks update multiple):

- For each offset in `task.source_offsets`, decrement `tracker.remaining` and append the `ExecutorResult` to `tracker.results`.
- If `tracker.remaining == 0` for any message, its `MessageGroup` is complete — transition to [Phase 7: Message Completion](#phase-7-message-completion).

---

## Phase 6: Post-Execution -- Failure Path

When a task fails (`ExecutorTaskError` raised):

### 6.1 Metrics and Recording

- `executor_tasks` counter incremented with `status='failed'`.
- If the error's `kind` is `'timeout'`: `executor_timeouts` counter incremented (classification is by the structured `kind` field, never by parsing the exception text).
- Flight recorder records a `task_failed` event with error details.

### 6.2 on_error Hook

The framework calls `handler.on_error(task, error)`:
- `task`: the failed `ExecutorTask`
- `error`: an `ExecutorError` with:
  - `kind` (`'nonzero_exit' | 'timeout' | 'launch_failure' | 'internal'`): structured failure classification — non-zero exit code (default), exceeded `executor.task_timeout`, process could not be started, or synthesized by the framework with no subprocess outcome behind it
  - `exit_code` (int | None): the process exit code, or None if it never started / timed out
  - `stderr` (str): process stderr or error description
  - `exception` (str | None): exception message for timeout/launch failures, None for normal non-zero exits
  - `pid` (int | None): process ID, None if never started

The hook must return one of four types of responses:

**Response A -- `ErrorAction.RETRY`** (retry the same task):
- If `retry_count < executor.max_retries` (default: `3`, meaning up to 3 retries = 4 total attempts):
  - `task_retries` Prometheus counter incremented.
  - A new `asyncio.Task` is created wrapping `_execute_and_track(task, window, retry_count + 1)`.
  - The function returns immediately **without** decrementing `_inflight_count` or incrementing `window.completed_count` -- the retry reuses the same slot.
  - There is **no sleep or backoff** between retries; the task is immediately re-submitted to the executor pool.
- If `retry_count >= executor.max_retries` (retries exhausted):
  - Logs a `max_retries_exceeded` warning with the task_id, the retry count and `action` (`executor.on_failure`).
  - With `executor.on_failure: dlq`, writes the task's `execute` DLQ record, as Response D below.
  - The failed result is appended to the window's results list (kept only when `on_window_complete` is implemented).
  - Falls through to the finally block (task counted as completed).

**Response B -- `ErrorAction.SKIP`** (skip and continue):
- The default answer of `BaseDrakkarHandler.on_error()` while `executor.on_failure` is `skip` (its default).
- The failed result is appended to the window's results list (kept only when `on_window_complete` is implemented).
- Falls through to the finally block.

**Response D -- `ErrorAction.DLQ`** (send the message to the DLQ):
- The default answer of `BaseDrakkarHandler.on_error()` when `executor.on_failure` is `dlq`.
- The [DLQ record](sinks.md#the-dlq-record) of each source message of the task is written at once (`dk-stage: execute`, `dk-task-id`, `dk-attempts` = the task's attempts). A message that already has a record gets no second one (`message_already_dlq`).
- The task counts as failed; the message is done, so no later result of it is delivered. Tasks already running finish.
- A record that cannot be written follows `dlq.on_send_failure` and logs `execute_dlq_failed`.

**Response C -- `list[ExecutorTask]`** (replacement tasks):
- The handler returns new tasks to run instead of retrying the original.
- For each new task:
  - Added to `_pending_tasks`.
  - `window.task_ids` list extended.
  - `window.total_tasks` incremented (the window grows dynamically).
  - `_inflight_count` incremented.
  - A new `asyncio.Task` is created.
- The original task is counted as completed in the finally block.
- This allows patterns like: split a large failed task into smaller ones, or substitute a fallback binary.

### 6.3 Unexpected Exceptions

If any non-`ExecutorTaskError` exception occurs during task processing or hook calls:
- A synthetic `ExecutorResult` is created with `exit_code=-1`, `stderr=str(e)`, `duration_seconds=0`.
- Appended to the window's results list.
- The error is logged with full traceback.
- The task is counted as completed so the window can still progress.

### 6.4 Finally Block (All Paths)

After every task run (success, failure, or unexpected error), except when returning early for a retry:

1. Remove the task from `_pending_tasks`. If not found, log a warning (indicates a potential race or duplicate cleanup).
2. Decrement `_inflight_count`.
3. Update `executor_pool_active` gauge.
4. Increment `window.completed_count`.

---

## Phase 7: Message Completion

Per-source-message aggregation and per-message offset commit. Fires before any window-level hook, potentially many times within one window as individual messages finish.

### 7.1 Message Completion Check

Each terminal outcome (success, SKIP, retry-exhaustion, replacement) decrements `tracker.remaining` for every message the task contributes to. When a tracker hits zero, the message's `MessageGroup` is ready to fire.

A `MessageTracker.completion_fired` guard prevents double-firing in any pathological race.

### 7.2 on_message_complete Hook

When a message's tracker settles:

1. `tracker.finished_at` is stamped with `time.monotonic()`.
2. The framework builds a `MessageGroup(source_message, tasks, results, errors, started_at, finished_at)`.
3. Calls `handler.on_message_complete(group)`:
   - **Returns `CollectResult`**: the aggregate sink payloads for this request (e.g. one summary row per message).
   - **Returns `None`**: no per-message delivery (relies on `on_task_complete` or `on_window_complete`).
   - **Raises**: logged at ERROR level with full context; offset commit still proceeds (hook bugs must not stall the partition).
4. The hook duration is recorded in `handler_duration{hook="on_message_complete"}` and a `message_complete` event lands in the recorder with fields: `task_count`, `succeeded`, `failed`, `replaced`, `output_message_count`.

### 7.3 Message-Level Sink Delivery

If `on_message_complete()` returned a non-None `CollectResult`, it flows through the same validation → grouping → `SinkManager.deliver_all` path as `on_task_complete` output (see Phase 8). A delivery failure here is logged but does NOT block the offset from committing — sink retries go through `on_delivery_error` as usual.

### 7.4 Offset Completion and Commit

After `on_message_complete` returns (success or raise), the message's offset is marked complete on the partition's `OffsetTracker`, a commit is scheduled, and the tracker entry is removed.

This is **per-message commit granularity**: a fast-finishing message does not wait for slower messages in the same window.

**Commits are coalesced, not sent per message.** The commit itself is a synchronous broker round trip, so a completed message schedules one rather than making one: the partition sends when the watermark has moved 300 offsets, or 500 ms after the oldest uncommitted advance, whichever comes first. Deferring only ever costs redone work after a crash — the direction at-least-once already permits — and the revoke, drain and shutdown paths each force the pending commit before the partition is released, so a handover never redelivers because of batching. See [Performance](performance.md#offset-commits-are-batched).

When a partition is revoked or the worker shuts down, offsets for already-finished messages are committed by that forced flush; only in-flight messages' offsets remain uncommitted (expected, drives the at-least-once replay on restart).

---

## Phase 8: Window Completion and Sink Delivery

### 8.1 Window Completion Check

After each task's finally block, the framework checks `window.is_complete`:

```
completed_count >= total_tasks AND total_tasks > 0
```

Note: `total_tasks` can grow dynamically if `on_error` returns replacement tasks, so the window only completes when ALL tasks (including dynamically added ones) have finished.

By the time `window.is_complete` fires, every message in the window has already had its `on_message_complete` called and its offset committed — this phase is purely about the window-level aggregation hook.

### 8.2 on_window_complete Hook

When the window is complete:

1. `batch_duration` histogram observes the total window duration (from creation to last task completion).
2. Calls `handler.on_window_complete(results, source_messages)`:
   - `results`: all `ExecutorResult` objects from this window (successes and failures).
   - `source_messages`: the original messages that triggered this window.
   - **Returns `CollectResult`**: additional sink payloads (e.g., aggregated summaries).
   - **Returns `None`**: no additional delivery.

If a `CollectResult` is returned, it goes through the same sink delivery pipeline as `on_task_complete()` results.

### 8.3 Offset State at Window End

By the time `on_window_complete` fires, every message in the window has already had its offset marked COMPLETE (in Phase 7.4). The `offset_lag` gauge already reflects the post-window state. This section is kept for historical reference — the actual state transition now happens per-message, not per-window.

### 8.4 Sink Delivery Details

The `SinkManager.deliver_all()` method handles delivery to all sinks:

**Grouping**: payloads from the `CollectResult` are grouped by `(sink_type, sink_name)`. The mapping from `CollectResult` fields to sink types is:
- `result.kafka` -> `'kafka'` sinks
- `result.postgres` -> `'postgres'` sinks
- `result.mongo` -> `'mongo'` sinks
- `result.http` -> `'http'` sinks
- `result.redis` -> `'redis'` sinks
- `result.files` -> `'filesystem'` sinks

**Order of the groups.** Groups start in parallel. A group whose sink declares
[`depends_on`](sinks.md#sink-dependencies-depends_on) waits until each of its
dependencies with payloads in this result settled with success (delivered or
skipped); a dependency that ended the message on the DLQ, or failed without a
record, stops it (`sink_not_started`, reason `message_dlq` or
`dependency_failed`). A message that is already on the DLQ starts no group at
all.

**Per-sink delivery** (for each group):

If the sink's circuit is open, the sink's declared `on_failure` decides at
once, without the hook (`dlq` writes the record with `dk-attempts: 0`).
Otherwise a loop of hook-level attempts runs, with `attempt` starting at 1:

1. **Call `sink.deliver(payloads)`**. The serialization is sink-specific:

   - **KafkaSink**: each payload's `data` is serialized via `model_dump_json().encode()` as the Kafka message value; `key` is passed through. All payloads are produced in one batch, then the producer is flushed. The method raises `RuntimeError` if flush is incomplete, any future is None, or any result contains an error.
   - **PostgresSink**: acquires a single connection from the `asyncpg.Pool` (pool_min default: `2`, pool_max default: `10`). For each payload, `data` is serialized via `model_dump()` to a column->value dict, and an `INSERT INTO {table} ({columns}) VALUES ($1, $2, ...)` query is run. Table and column names are validated against SQL injection via a `^[a-zA-Z_][a-zA-Z0-9_]*$` regex.
   - **MongoSink**: for each payload, `data` is serialized via `model_dump()` to a document dict, then `collection.insert_one(document)` is called.
   - **HttpSink**: for each payload individually, `data` is encoded per the sink's `encoding` setting — `json` (default, `model_dump_json()`), `form`, or `multipart` — and the encoder supplies the matching `Content-Type`. An HTTP request is sent using the configured `method` (default: `'POST'`) to the configured `url`, plus any custom `headers`. Timeout is `timeout` (default: `30`). Non-2xx responses raise `httpx.HTTPStatusError` via `raise_for_status()`.
   - **RedisSink**: for each payload, `data` is serialized via `model_dump_json()` as the string value. The full key is `{config.key_prefix}{payload.key}`. If `payload.ttl` is set, `SET key value EX ttl`; otherwise, `SET key value` (no expiry).
   - **FileSink**: for each payload, `data` is serialized via `model_dump_json() + '\n'` (JSONL format). The file at `payload.path` is opened in append mode. Parent directory must exist.

2. **On success**:
   - SinkManager stats updated: `delivered_count += 1`, `delivered_payloads += len(payloads)`, timestamps recorded.
   - Flight recorder records a `sink_delivered` event.
   - `sink_payloads_delivered` and `sink_deliver_duration` Prometheus metrics updated.
   - Break from the retry loop.

3. **On failure** (exception from `deliver()`):
   - Stats updated: `error_count += 1`, `last_error` set (URL credentials redacted).
   - `sink_deliver_errors` Prometheus counter incremented.
   - A `DeliveryError` is created with `sink_name`, `sink_type`, `error`, the failed
     `payloads`, `attempt`, `retries_left` (the sink's `max_retries + 1 - attempt`) and
     `declared_policy` (the sink's `on_failure`).
   - The handler's `on_delivery_error(error)` hook is called. If it raises, the worker
     logs `delivery_error_hook_failed` and uses `declared_policy`.

**on_delivery_error returns one of three actions** (the default hook returns
`error.declared_policy`):

   - **`DeliveryAction.RETRY`** with `retries_left > 0`:
     - Flight recorder records a `sink_error` event with `action: retry`.
     - `sink_delivery_retries` counter incremented; `sink_delivery_retry` warning logged.
     - The whole group is delivered again (immediate retry, no backoff).

   - **`DeliveryAction.RETRY`** with `retries_left == 0`:
     - `sink_retry_budget_spent` warning (at most one line per sink per minute).
     - Treated as the declared `on_failure`.

   - **`DeliveryAction.SKIP`**:
     - `sink_deliveries_skipped` counter incremented; `sink_delivery_skipped` warning.
     - The payloads are dropped; `sink_error` recorded with `action: skip`.

   - **`DeliveryAction.DLQ`**:
     - For a task or message result, the [DLQ record](sinks.md#the-dlq-record) of each
       source message is written once, under the message's own lock: the original key,
       value and headers, then the `dk-` headers (`dk-stage: sink`, `dk-sink`,
       `dk-attempts`, `dk-delivered-sinks`, the source coordinates, the worker).
     - `sink_dlq_messages` and `dlq_records{stage="sink"}` counters incremented; a
       `message_dlq` warning logged and a `message_dlq` event recorded.
     - The message is done: groups of this result that have not started do not start,
       and no later result of the message is delivered.
     - For a window result there is no source message: the payloads are dropped as a
       skip, with a `window_delivery_not_dlq` warning.
     - If no record can be written, `dlq.on_send_failure` applies
       (see [When the DLQ itself fails](sinks.md#when-the-dlq-itself-fails)).
     - `sink_error` recorded with `action: dlq`, or `failed` when no record was written.

After successful delivery, the framework also records `produced` events in the flight recorder for Kafka payloads (counting output messages for observability).

---

## Phase 9: Offset Commit

### 9.1 Watermark Calculation

The `OffsetTracker` maintains a sorted list of registered offsets, each in state PENDING or COMPLETED. The `committable()` method returns the **highest consecutive completed offset + 1** from the beginning of the sorted list:

- Example: offsets [10, 11, 12, 13, 14] with states [COMPLETED, COMPLETED, COMPLETED, PENDING, COMPLETED]
  - Committable = 13 (offsets 10, 11, 12 are consecutive and completed; 13 is pending, blocking 14)
- Example: offsets [10, 11, 12] all COMPLETED
  - Committable = 13 (all completed, commit up to 13)
- Example: offset [10] PENDING
  - Committable = None (nothing to commit)

This watermark design ensures that Kafka offsets are only committed when ALL preceding messages have been fully processed and their results delivered to sinks.

### 9.2 Commit Triggers

Offset commits are attempted at these points:

1. **After every `on_message_complete`** — the most common trigger; the watermark may advance once per source message as each settles. The commit is batched: it is sent at once when the watermark has advanced by 300 offsets, otherwise 0.5 s after the first advance that was not yet committed.
2. **On idle iterations** (no messages received for 1 second) — catches any lagging commits.
3. **When arrange returns no tasks** — offsets are immediately committable after the empty-arrange `on_message_complete` calls.
4. **During shutdown** — final commit for each partition.
5. **During partition revocation** — commit before releasing the partition, only if drain completed cleanly.

### 9.3 Commit Flow

When `committable()` returns a non-None offset:

1. The framework calls `consumer.commit({partition_id: offset})`.
   - The consumer creates a `TopicPartition(topic, partition, offset)` and calls `commit(asynchronous=False)`.
   - The `offsets_committed` Prometheus counter is incremented (labeled by partition).
2. If the commit succeeds:
   - The flight recorder records a `committed` event.
   - `offset_tracker.acknowledge_commit(committed_offset)` removes all offsets below the committed value from tracking, bounding memory usage.
3. If the commit fails (exception):
   - A warning is logged with the partition, offset, and error.
   - The function returns early **without** acknowledging -- the next commit attempt will retry with the same or a higher offset.

---

## Phase 10: Graceful Shutdown

When `_running` is set to False (via SIGINT, SIGTERM, or programmatic shutdown):

**Shutdown is driven per source.** The worker first marks itself not ready,
then calls `signal_stop` on every enabled source and logs `sources_draining`.
Every source then drains **concurrently against one shared deadline**,
`executor.drain_timeout` — the Kafka source waits for its partition
processors to empty their queues and settle their in-flight tasks, the HTTP
source waits for its in-flight requests. The step ends with `sources_drained`,
or with the deadline expiring, which is what decides whether final offset
commits are safe. Whatever remains of the budget is what stopping the sources
gets; a source that fails to stop logs `source_stop_failed`. Only then do the
shared subsystems close, in the order below. One shared deadline means the
total shutdown time does not grow with the number of sources — the steps that
follow describe what the **Kafka** source does inside its share of it.

### Step 1: Cancel Periodic Tasks
- All periodic task `asyncio.Task` objects are cancelled, worker and cluster tasks alike. A cluster task that holds a lease does not release it: the lease ends by itself, so a worker whose clock is behind cannot run the slot again. An archive pass that already runs goes on in its thread until the recorder stops it (step 7).
- `asyncio.gather(*tasks, return_exceptions=True)` waits for them to finish.
- The list is cleared.

### Step 2: Signal Partition Processors to Stop
- `processor._running = False` for all processors. This causes each processor's main loop to exit after its current window collection.

### Step 3: Drain All Processors
- Waits up to `executor.drain_timeout` (default: `30`) for all processors with:
  - Non-empty queues, OR
  - Pending offset commits, OR
  - In-flight tasks

  to finish their work.
- Each processor drains by: dropping the queued messages that are not in a window (never arranged; their offsets were never registered, so the next owner reads them again; counted in `drakkar_queued_messages_dropped_at_stop_total`), waiting for in-flight tasks to complete, then doing a final commit.
- If the timeout expires, logs a warning but continues shutdown. The flag that tracks a clean drain is used by the next step to decide whether committing final offsets is safe. The still-running tasks are suppressed and then cancelled, so they release their executor slots and subprocesses rather than holding them for the rest of the pod's grace period.
- Processors whose loop has died are excluded: nothing will empty their queue or settle their pending offsets, so waiting on one would spend the whole budget and suppress every other partition's final commit with it.

### Step 4: Final Offset Commits
- **Only if Step 3 drained cleanly**, iterates processors and commits any remaining offsets via `committable()`.
- If drain timed out, final commits are skipped entirely. Tasks may still be running, so committing their watermark would silently skip them on next startup — preferring at-least-once duplication over silent loss.
- If a commit call itself fails, logs a warning. Those offsets will be re-processed on next startup (at-least-once semantics).

### Step 5: Stop All Processors
- Calls `processor.stop()` on all of them concurrently, which:
  - Waits up to 10 seconds for the processor's async task to exit naturally.
  - If it doesn't exit in 10 seconds, force-cancels the task.
  - Cancels any task coroutine the loop left behind — cancelling the loop does not reach them, and one still holding an executor slot has no owner left to read its result.
- Sequentially, a worker with N partitions would pay N stop timeouts back to back, which the pod's grace period usually outlasts — and it expires *after* the watchdog was marked clean, so the SIGKILL is recorded as a clean exit and the closes in steps 7-11 never run.
- Clears the processors dict.

### Step 6: Await Rebalance Background Tasks
- Any revoke-triggered `_stop_processor`, `on_assign`/`on_revoke` handler hook invocations, and backpressure `pause` calls scheduled as `asyncio.ensure_future` are awaited here (bounded by `executor.drain_timeout`).
- This runs **before** the consumer is closed so those tasks can still use `self._consumer` safely. Skipping this step caused use-after-close errors and missed final commits under revoke-then-shutdown sequences.

### Step 7: Stop the Flight Recorder
- If the UI is enabled: cancels background tasks (flush, rotation, state sync, sealing, archive pass), flushes any buffered events to SQLite, and seals the live file (rollback journal, meta file, close). The seal always runs. Moving the sealed file into `<storage.shared_dir>/history/<cluster>/` runs only while shutdown time remains (the rest of `executor.drain_timeout` after draining); otherwise the file stays in `storage.dir` and the next start moves it. See [Sealing](local-databases.md#sealing-and-the-history-folder).
- An archive pass still running is asked to stop after its current window; the recorder waits for it until only `autodiscover.resilience.timeout` of the shutdown time is left (all of it without a registry), then leaves it (`recorder_archive_pass_abandoned`).
- Then the election of cluster periodic tasks stops (one attempt, in the time left; a failure logs `periodic_election_stop_failed` and the shutdown goes on), the registry card is withdrawn, and the shared connections close.

### Step 8: Stop the UI Server
- If the UI is enabled: stops the FastAPI server.

### Step 9: Close All Sinks and DLQ
- `sink_manager.close_all()`: calls `close()` on each sink. Exceptions are caught and logged as warnings (never raised during shutdown).
  - KafkaSink: closes the Kafka producer, sets to None.
  - PostgresSink: closes the asyncpg pool, sets to None.
  - MongoSink: closes the PyMongo async client, sets to None.
  - HttpSink: calls `client.aclose()`, sets to None.
  - RedisSink: calls `client.aclose()`, sets to None.
  - FileSink: no-op (no persistent connection).
- DLQ sink closed separately (same pattern).

### Step 10: Close the Kafka Consumer
- Closes the confluent_kafka consumer, which triggers a final leave-group request to the Kafka broker.

---

## Concurrency Model Summary

- **One async event loop** per worker process.
- **One poll loop** fetches messages and dispatches to partition processors.
- **One processing loop per partition** collects windows and launches tasks.
- **Multiple windows per partition** can be in-flight concurrently -- the processor does not wait for window N to complete before starting window N+1.
- **All partitions share a single ExecutorPool** with a semaphore of size `executor.max_executors`. Tasks from any partition compete for the same slots.
- **Backpressure operates globally** -- one pending budget bounds the tasks and bytes of every partition and the HTTP source, and the Kafka pause/resume applies to all partitions at once, based on the queued messages of the live partitions.
- **Periodic tasks** run as independent async tasks in the same event loop.
- **Sink delivery** happens inline within the task's async context (not batched across partitions).

---

## Delivery Guarantees

- **At-least-once processing**: offsets are committed only after all tasks in a window complete AND their results are delivered to sinks (or handled by on_delivery_error). If the worker crashes before committing, messages will be re-consumed on restart.
- **No exactly-once**: the framework does not use Kafka transactions. In failure scenarios (crash between sink delivery and offset commit), messages may be processed and delivered more than once.
- **Ordering within a partition**: messages are processed in offset order within windows. However, tasks within a window run concurrently, so per-message ordering is not guaranteed within a window.
- **Cross-partition ordering**: no ordering guarantees between partitions.

---

## Configuration Reference

Every key, with its default, bounds and `DK_` variable, is on the
[Config Reference](config-reference.md); the rules behind the layout and the
deep tables are on [Configuration](configuration.md).
