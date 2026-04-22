# Drakkar Data Flow: Complete Processing Story

## Architecture Overview

Drakkar is a Python 3.13+ framework that consumes messages from a Kafka topic, fans them out to per-partition async processors, runs user-defined logic as subprocesses, and delivers results to one or more output sinks (Kafka, PostgreSQL, MongoDB, HTTP, Redis, filesystem). The architecture follows a pipeline model: **poll -> partition -> window -> arrange -> execute -> on_task_complete -> on_message_complete -> on_window_complete -> deliver -> commit**. A single Drakkar worker runs one async event loop (`asyncio.run`) that hosts all partition processors, a shared subprocess executor pool with semaphore-based concurrency control, a sink manager, a dead-letter queue (DLQ), optional Prometheus metrics, and an optional debug flight recorder with a web UI. The user implements a handler class with hooks (`arrange`, `on_task_complete`, `on_message_complete`, `on_window_complete`, `on_error`, `on_delivery_error`, plus lifecycle hooks like `on_startup`/`on_ready`/`on_assign`/`on_revoke`) that define the application-specific logic; everything else -- Kafka consumption, offset tracking, backpressure, retries, subprocess lifecycle, sink connections, and graceful shutdown -- is managed by the framework. See [Fan-out](fan-out.md) for a dedicated walkthrough of the per-task / per-message / per-window hook trio.

Configuration is loaded from a YAML file (path via `config_path` argument or `DRAKKAR_CONFIG` env var) with environment variable overrides using the `DRAKKAR_` prefix and `__` as a nesting delimiter (e.g., `DRAKKAR_KAFKA__BROKERS`). Environment variables are deep-merged on top of YAML values. The root configuration object is `DrakkarConfig`, a Pydantic `BaseSettings` model. All config fields referenced below are part of this hierarchy.

---

## Phase 0: Worker Startup

### 0.1 Initialization

When `DrakkarApp.run()` is called, the framework:

1. Resolves the **worker identity**:
   - Reads the environment variable named by `worker_name_env` (default: `'WORKER_ID'`).
   - If empty, falls back to a hex-encoded Python `id()` like `drakkar-7f3a2b`.
2. Resolves the **cluster name**:
   - If `cluster_name_env` is set and the corresponding env var is non-empty, uses that.
   - Otherwise falls back to `cluster_name` (default: `''`).
3. Configures **structured logging** via structlog using `logging.level` (default: `'INFO'`) and `logging.format` (default: `'json'`, also accepts `'console'`). The worker_id, consumer_group, and framework version are bound as context variables to every log line.
4. Calls the handler's `on_startup(config)` hook, which receives the full `DrakkarConfig` and may return a modified copy. This is the only point where the user can mutate configuration before the framework wires everything up.

### 0.2 Component Construction

After `on_startup`, the framework builds all components in this order:

1. **Validates sinks are configured** -- if `sinks.is_empty` is True (no sinks of any type defined), raises `SinkNotConfiguredError` immediately. At least one sink must be configured.

2. **Creates the ExecutorPool** with:
   - `executor.binary_path` (default: `None` -- each task must then provide its own `binary_path`)
   - `executor.max_executors` (default: `4`, min: `1`) -- controls the `asyncio.Semaphore` size
   - `executor.task_timeout_seconds` (default: `120`, min: `1`) -- per-subprocess wall-clock timeout

3. **Starts the Prometheus metrics server** on `metrics.port` (default: `9090`) if `metrics.enabled` (default: `True`). Publishes `worker_info` with worker_id, version, and consumer_group labels.

4. **If `debug.enabled`** (default: `True`):
   - Creates an `EventRecorder` with the debug config, worker name, and cluster name.
   - Sets up a state provider callback so the recorder can periodically snapshot worker state (uptime, partitions, pool utilization, queue depth) every `debug.state_sync_interval_seconds` (default: `10`) seconds.
   - Starts the recorder (creates/opens the SQLite database, starts flush/retention/state-sync background tasks).
   - Writes the full worker configuration to the `worker_config` table (if `debug.store_config` is True, default: `True`), enabling autodiscovery by other workers in the same cluster.
   - Creates and starts a `DebugServer` (FastAPI) on `debug.port` (default: `8080`), providing a web UI, JSON API, WebSocket event streaming, and database download/merge endpoints.

5. **Builds all sinks** from config by iterating each type's dict:
   - `sinks.kafka` -- creates `KafkaSink(name, config, brokers_fallback=kafka.brokers)` for each named entry
   - `sinks.postgres` -- creates `PostgresSink(name, config)` for each
   - `sinks.mongo` -- creates `MongoSink(name, config)` for each
   - `sinks.http` -- creates `HttpSink(name, config)` for each
   - `sinks.redis` -- creates `RedisSink(name, config)` for each
   - `sinks.filesystem` -- creates `FileSink(name, config)` for each
   Each is registered with the `SinkManager` under a `(sink_type, name)` key.

6. **Connects all sinks** by calling `connect()` on each in registration order. If any `connect()` raises, the worker crashes immediately (fail-fast design).

7. **Builds and connects the DLQ sink**:
   - Topic: `dlq.topic` if non-empty, otherwise `'{kafka.source_topic}_dlq'` (default source topic: `'input-events'`).
   - Brokers: `dlq.brokers` if non-empty, otherwise `kafka.brokers` (default: `'localhost:9092'`).
   - Connects a dedicated Kafka producer for the DLQ.

8. **Creates the Kafka consumer** (`KafkaConsumer` wrapping `confluent_kafka.aio.AIOConsumer`) with:
   - `bootstrap.servers`: `kafka.brokers` (default: `'localhost:9092'`)
   - `group.id`: `kafka.consumer_group` (default: `'drakkar-workers'`)
   - `enable.auto.commit`: `False` (offsets are committed manually by the framework)
   - `auto.offset.reset`: `'earliest'`
   - `partition.assignment.strategy`: `'cooperative-sticky'`
   - `max.poll.interval.ms`: `kafka.max_poll_interval_ms` (default: `300000` = 5 minutes)
   - `session.timeout.ms`: `kafka.session_timeout_ms` (default: `45000` = 45 seconds)
   - `heartbeat.interval.ms`: `kafka.heartbeat_interval_ms` (default: `3000` = 3 seconds)
   - Assign/revoke callbacks are wired to `_on_assign` and `_on_revoke`.

9. **Exposes the PostgreSQL connection pool** for the handler: if any postgres sink exists, its `asyncpg.Pool` is extracted and passed to the next hook.

10. **Calls the handler's `on_ready(config, pg_pool)` hook**. The user can use this for database migrations, loading lookup tables, initializing caches, or any async setup that requires live connections.

11. **Discovers and starts periodic tasks** by inspecting the handler for methods decorated with `@periodic(seconds=N)`. For each discovered method:
    - Creates an `asyncio.Task` that loops: sleep `seconds`, then `await coro_fn()`.
    - Overlap prevention: the next interval begins only after the current invocation finishes (no concurrent runs of the same periodic task).
    - If the coroutine raises an exception:
      - `on_error='continue'` (default): logs the error and continues looping.
      - `on_error='stop'`: logs the error and exits the task permanently.

12. **Subscribes to the Kafka topic** and enters the main poll loop.

13. **Registers signal handlers** for `SIGINT` and `SIGTERM` that set `_running = False`, triggering graceful shutdown.

---

## Phase 1: Polling Messages from Kafka

The main poll loop runs continuously while `_running` is True. Each iteration:

### 1.1 Check Backpressure

The framework calculates the **total queued count** as the sum of `queue_size + inflight_count` across all active partition processors.

Two watermarks control backpressure:

- **High watermark** = `executor.max_executors` (default: `4`) x `executor.backpressure_high_multiplier` (default: `32`) = **128** by default.
- **Low watermark** = max(1, `executor.max_executors` x `executor.backpressure_low_multiplier` (default: `4`)) = **16** by default.

**Pause condition**: If not currently paused AND total queued >= high watermark:
- Calls `consumer.pause(all_assigned_partition_ids)` -- Kafka stops delivering messages from all partitions.
- Sets `backpressure_active` Prometheus gauge to `1`.
- Sets `_paused = True`.

**Resume condition**: If currently paused AND total queued <= low watermark:
- Calls `consumer.resume(all_assigned_partition_ids)` -- Kafka resumes delivery.
- Sets `backpressure_active` gauge to `0`.
- Sets `_paused = False`.

The hysteresis between high and low watermarks prevents rapid pause/resume oscillation. Pause and resume always operate on ALL assigned partitions, not individual ones.

### 1.2 Poll a Batch

The consumer calls `consume(num_messages=count, timeout=1.0)` on the underlying `confluent_kafka.aio.AIOConsumer`, where `count` defaults to `kafka.max_poll_records` (default: `100`).

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
- Records a `consumed` event in the flight recorder (if debug enabled).
- Increments the `messages_consumed` Prometheus counter (labeled by partition).
- Updates the `partition_queue_size` gauge.

If no processor exists for the partition (shouldn't happen under normal operation), the message is silently dropped.

### 1.4 Idle Backoff

If no messages were returned by the poll, the loop sleeps for **50ms** (`asyncio.sleep(0.05)`) to avoid busy-spinning.

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
     - The flight recorder (if debug enabled)
   - Starts the processor's background `asyncio.Task` immediately.
3. Updates the `assigned_partitions` Prometheus gauge.
4. Calls the handler's `on_assign(partition_ids)` hook asynchronously (fire-and-forget with exception logging).

### 2.2 On Revoke (Lost Partitions)

When partitions are revoked (rebalance, scaling event):

1. Records a `revoked` event per partition.
2. For each revoked partition:
   - Removes the processor from `_processors`.
   - Initiates an async stop sequence:
     1. Sets `processor._running = False`.
     2. Waits up to `executor.drain_timeout_seconds` (default: `30`, min: `1`) for in-flight work to complete.
     3. **Only if drain completed cleanly**, commits the offset watermark. If drain timed out, in-flight tasks may still be running and committing their offsets would silently skip them on reassign — safer to let at-least-once replay recover them.
     4. Calls `processor.stop()`.
3. Updates the `assigned_partitions` gauge.
4. Calls the handler's `on_revoke(partition_ids)` hook asynchronously.

---

## Phase 3: Window Collection and Arrangement

Each `PartitionProcessor` runs its own async loop independently. It does not wait for one window to fully complete before starting the next -- multiple windows can be in-flight concurrently.

!!! note "Why one processor per partition?"
    Kafka tracks and commits offsets per partition, so each `PartitionProcessor` owns an offset watermark and a FIFO queue for its partition alone. Running one `arrange()` coroutine per partition keeps offset bookkeeping and in-partition ordering as local state, and makes rebalances a matter of starting or stopping one task per affected partition -- there is no cross-partition state to untangle when assignments change.

### 3.1 Collecting a Window

The processor waits for messages from its queue:

1. **Blocking wait for the first message**: `await asyncio.wait_for(queue.get(), timeout=1.0)`.
   - **If timeout (1 second, no messages)**: returns an empty list. The processor then attempts an offset commit for any previously completed work and loops back.
   - **If a message arrives**: becomes the first message in this window.
2. **Non-blocking drain**: calls `queue.get_nowait()` in a tight loop until:
   - The queue is empty, OR
   - The window reaches `executor.window_size` (default: `100`) messages.
3. Updates the `partition_queue_size` gauge after collection.

The resulting list of 1 to `window_size` messages forms a **window**.

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

3. **Communication**: `proc.communicate(input=stdin_bytes)` writes stdin (if any) and waits for the process to exit, capturing all stdout and stderr.

4. **Timeout enforcement**: the entire `communicate()` call is wrapped in `asyncio.wait_for(timeout=executor.task_timeout_seconds)` (default: `120` seconds).

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
- If the error contains "Timeout" in the exception message: `executor_timeouts` counter incremented.
- Flight recorder records a `task_failed` event with error details.

### 6.2 on_error Hook

The framework calls `handler.on_error(task, error)`:
- `task`: the failed `ExecutorTask`
- `error`: an `ExecutorError` with:
  - `exit_code` (int | None): the process exit code, or None if it never started / timed out
  - `stderr` (str): process stderr or error description
  - `exception` (str | None): exception message for timeout/launch failures, None for normal non-zero exits
  - `pid` (int | None): process ID, None if never started

The hook must return one of three types of responses:

**Response A -- `ErrorAction.RETRY`** (retry the same task):
- If `retry_count < executor.max_retries` (default: `3`, meaning up to 3 retries = 4 total attempts):
  - `task_retries` Prometheus counter incremented.
  - A new `asyncio.Task` is created wrapping `_execute_and_track(task, window, retry_count + 1)`.
  - The function returns immediately **without** decrementing `_inflight_count` or incrementing `window.completed_count` -- the retry reuses the same slot.
  - There is **no sleep or backoff** between retries; the task is immediately re-submitted to the executor pool.
- If `retry_count >= executor.max_retries` (retries exhausted):
  - Logs a `max_retries_exceeded` warning with the task_id and retry count.
  - The failed result is appended to the window's results list.
  - Falls through to the finally block (task counted as completed).

**Response B -- `ErrorAction.SKIP`** (skip and continue):
- The default behavior from `BaseDrakkarHandler.on_error()`.
- The failed result is appended to the window's results list.
- Falls through to the finally block.

**Response C -- `list[ExecutorTask]`** (replacement tasks):
- The handler returns new tasks to run instead of retrying the original.
- For each new task:
  - Added to `_pending_tasks`.
  - `window.tasks` list extended.
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

A `_MessageTracker.completion_fired` guard prevents double-firing in any pathological race.

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

After `on_message_complete` returns (success or raise), the message's offset is marked complete on the partition's `OffsetTracker`, a commit is attempted, and the tracker entry is removed.

This is **per-message commit granularity**: a fast-finishing message does not wait for slower messages in the same window. When a partition is revoked or the worker shuts down, offsets for already-finished messages are already committed; only in-flight messages' offsets remain uncommitted (expected, drives the at-least-once replay on restart).

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

**Per-sink delivery** (for each group):

A retry loop with `attempt` starting at 0:

1. **Call `sink.deliver(payloads)`**. The serialization is sink-specific:

   - **KafkaSink**: each payload's `data` is serialized via `model_dump_json().encode()` as the Kafka message value; `key` is passed through. All payloads are produced in one batch, then the producer is flushed. The method raises `RuntimeError` if flush is incomplete, any future is None, or any result contains an error.
   - **PostgresSink**: acquires a single connection from the `asyncpg.Pool` (pool_min default: `2`, pool_max default: `10`). For each payload, `data` is serialized via `model_dump()` to a column->value dict, and an `INSERT INTO {table} ({columns}) VALUES ($1, $2, ...)` query is run. Table and column names are validated against SQL injection via a `^[a-zA-Z_][a-zA-Z0-9_]*$` regex.
   - **MongoSink**: for each payload, `data` is serialized via `model_dump()` to a document dict, then `collection.insert_one(document)` is called.
   - **HttpSink**: for each payload individually, `data` is serialized via `model_dump_json()` as the request body. An HTTP request is sent using the configured `method` (default: `'POST'`) to the configured `url` with `Content-Type: application/json` plus any custom `headers`. Timeout is `timeout_seconds` (default: `30`). Non-2xx responses raise `httpx.HTTPStatusError` via `raise_for_status()`.
   - **RedisSink**: for each payload, `data` is serialized via `model_dump_json()` as the string value. The full key is `{config.key_prefix}{payload.key}`. If `payload.ttl` is set, `SET key value EX ttl`; otherwise, `SET key value` (no expiry).
   - **FileSink**: for each payload, `data` is serialized via `model_dump_json() + '\n'` (JSONL format). The file at `payload.path` is opened in append mode. Parent directory must exist.

2. **On success**:
   - SinkManager stats updated: `delivered_count += 1`, `delivered_payloads += len(payloads)`, timestamps recorded.
   - Flight recorder records a `sink_delivered` event.
   - `sink_payloads_delivered` and `sink_deliver_duration` Prometheus metrics updated.
   - Break from the retry loop.

3. **On failure** (exception from `deliver()`):
   - Stats updated: `error_count += 1`, `last_error` set.
   - `sink_deliver_errors` Prometheus counter incremented.
   - Flight recorder records a `sink_error` event.
   - A `DeliveryError` is created with `sink_name`, `sink_type`, `error` message, and the failed `payloads`.
   - The handler's `on_delivery_error(error)` hook is called.

**on_delivery_error returns one of three actions**:

   - **`DeliveryAction.RETRY`** (and `attempt < max_retries`, default max_retries: `3`):
     - `sink_delivery_retries` counter incremented.
     - Logs a warning.
     - Continues the retry loop (immediate retry, no backoff).

   - **`DeliveryAction.SKIP`**:
     - `sink_deliveries_skipped` counter incremented.
     - Logs a warning.
     - Breaks from the retry loop. Payloads are dropped.

   - **`DeliveryAction.DLQ`** (default from `BaseDrakkarHandler.on_delivery_error()`):
     - The framework calls `dlq_sink.send(error, partition_id)`.
     - The DLQ sink serializes the failed payloads into a JSON envelope:
       ```json
       {
         "original_payloads": ["<json_string_1>", "<json_string_2>"],
         "sink_name": "results",
         "sink_type": "kafka",
         "error": "Connection refused",
         "timestamp": 1743580800.123,
         "partition": 5,
         "attempt_count": 1
       }
       ```
       Each payload is serialized via `model_dump_json()`; if that fails (e.g., non-serializable model), falls back to `str()`.
     - The envelope is produced to the DLQ Kafka topic.
     - `sink_dlq_messages` Prometheus counter incremented.
     - Breaks from the retry loop.

   - **Retries exhausted** (RETRY action but `attempt >= max_retries`):
     - Falls through to the DLQ/break path. Logs a warning.
     - Breaks from the retry loop.

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

1. **After every `on_message_complete`** — the most common trigger; fires once per source message as each settles.
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

### Step 1: Cancel Periodic Tasks
- All periodic task `asyncio.Task` objects are cancelled.
- `asyncio.gather(*tasks, return_exceptions=True)` waits for them to finish.
- The list is cleared.

### Step 2: Signal Partition Processors to Stop
- `processor._running = False` for all processors. This causes each processor's main loop to exit after its current window collection.

### Step 3: Drain All Processors
- Waits up to `executor.drain_timeout_seconds` (default: `30`) for all processors with:
  - Non-empty queues, OR
  - Pending offset commits, OR
  - In-flight tasks

  to finish their work.
- Each processor drains by: processing remaining queued messages into windows, waiting for in-flight tasks to complete, then doing a final commit.
- If the timeout expires, logs a warning but continues shutdown. The flag that tracks a clean drain is used by the next step to decide whether committing final offsets is safe.

### Step 4: Final Offset Commits
- **Only if Step 3 drained cleanly**, iterates processors and commits any remaining offsets via `committable()`.
- If drain timed out, final commits are skipped entirely. Tasks may still be running, so committing their watermark would silently skip them on next startup — preferring at-least-once duplication over silent loss.
- If a commit call itself fails, logs a warning. Those offsets will be re-processed on next startup (at-least-once semantics).

### Step 5: Stop All Processors
- Calls `processor.stop()` on each, which:
  - Waits up to 10 seconds for the processor's async task to exit naturally.
  - If it doesn't exit in 10 seconds, force-cancels the task.
- Clears the processors dict.

### Step 6: Await Rebalance Background Tasks
- Any revoke-triggered `_stop_processor`, `on_assign`/`on_revoke` handler hook invocations, and backpressure `pause` calls scheduled as `asyncio.ensure_future` are awaited here (bounded by `drain_timeout_seconds`).
- This runs **before** the consumer is closed so those tasks can still use `self._consumer` safely. Skipping this step caused use-after-close errors and missed final commits under revoke-then-shutdown sequences.

### Step 7: Stop the Flight Recorder
- If debug is enabled: flushes any buffered events to SQLite, cancels background tasks (flush, retention, state sync), removes the `-live.db` symlink.

### Step 8: Stop the Debug Server
- If debug is enabled: stops the FastAPI server.

### Step 9: Close All Sinks and DLQ
- `sink_manager.close_all()`: calls `close()` on each sink. Exceptions are caught and logged as warnings (never raised during shutdown).
  - KafkaSink: closes the Kafka producer, sets to None.
  - PostgresSink: closes the asyncpg pool, sets to None.
  - MongoSink: closes the motor client, sets to None.
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
- **Backpressure operates globally** -- pause/resume applies to all partitions simultaneously based on the total queued count across all processors.
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

### `kafka` -- Kafka Consumer Settings

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `brokers` | str | `'localhost:9092'` | Kafka bootstrap servers |
| `source_topic` | str | `'input-events'` | Topic to consume from |
| `consumer_group` | str | `'drakkar-workers'` | Consumer group ID |
| `max_poll_records` | int | `100` | Max messages per poll batch |
| `max_poll_interval_ms` | int | `300000` | Max time between polls before Kafka considers consumer dead |
| `session_timeout_ms` | int | `45000` | Session timeout for group membership |
| `heartbeat_interval_ms` | int | `3000` | Heartbeat interval to the broker |

### `executor` -- Subprocess Executor Pool

| Field | Type | Default | Min | Description |
|-------|------|---------|-----|-------------|
| `binary_path` | str or None | `None` | 1 char | Default subprocess binary; None requires per-task override |
| `max_executors` | int | `4` | 1 | Concurrent subprocess limit (semaphore size) |
| `task_timeout_seconds` | int | `120` | 1 | Per-subprocess wall-clock timeout |
| `window_size` | int | `100` | 1 | Max messages per arrange() window |
| `max_retries` | int | `3` | 0 | Max retries per failed task (0 = no retries) |
| `drain_timeout_seconds` | int | `30` | 1 | Max wait for in-flight tasks during shutdown or partition revocation. On timeout, final commits are skipped (at-least-once replay). |
| `backpressure_high_multiplier` | int | `32` | 1 | Pause threshold = max_executors x this |
| `backpressure_low_multiplier` | int | `4` | 1 | Resume threshold = max(1, max_executors x this) |

### `sinks` -- Output Sink Instances

Each sink type is a dict mapping instance names to their config:

**`sinks.kafka.<name>`**:

| Field | Type | Default |
|-------|------|---------|
| `topic` | str | (required) |
| `brokers` | str | `''` (inherits `kafka.brokers`) |
| `ui_url` | str | `''` |

**`sinks.postgres.<name>`**:

| Field | Type | Default | Min |
|-------|------|---------|-----|
| `dsn` | str | (required) | |
| `pool_min` | int | `2` | 1 |
| `pool_max` | int | `10` | 1 |
| `ui_url` | str | `''` | |

**`sinks.mongo.<name>`**:

| Field | Type | Default |
|-------|------|---------|
| `uri` | str | (required) |
| `database` | str | (required) |
| `ui_url` | str | `''` |

**`sinks.http.<name>`**:

| Field | Type | Default | Min |
|-------|------|---------|-----|
| `url` | str | (required) | |
| `method` | str | `'POST'` | |
| `timeout_seconds` | int | `30` | 1 |
| `headers` | dict | `{}` | |
| `max_retries` | int | `3` | 0 |
| `ui_url` | str | `''` | |

**`sinks.redis.<name>`**:

| Field | Type | Default |
|-------|------|---------|
| `url` | str | `'redis://localhost:6379/0'` |
| `key_prefix` | str | `''` |
| `ui_url` | str | `''` |

**`sinks.filesystem.<name>`**:

| Field | Type | Default |
|-------|------|---------|
| `base_path` | str | `''` |
| `ui_url` | str | `''` |

### `dlq` -- Dead Letter Queue

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `topic` | str | `''` | DLQ topic; empty = `'{source_topic}_dlq'` |
| `brokers` | str | `''` | DLQ brokers; empty = inherits `kafka.brokers` |

### `metrics` -- Prometheus

| Field | Type | Default |
|-------|------|---------|
| `enabled` | bool | `True` |
| `port` | int | `9090` |

### `logging` -- Structured Logging

| Field | Type | Default |
|-------|------|---------|
| `level` | str | `'INFO'` |
| `format` | str | `'json'` (also: `'console'`) |

### `debug` -- Flight Recorder and Web UI

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `True` | Enable/disable entire debug feature |
| `port` | int | `8080` | Debug web UI port |
| `debug_url` | str | `''` | External URL for the debug UI |
| `db_dir` | str | `'/tmp'` | SQLite database directory; `''` = no disk persistence |
| `store_events` | bool | `True` | Write processing events to events table |
| `store_config` | bool | `True` | Write worker config (enables autodiscovery) |
| `store_state` | bool | `True` | Periodic state snapshots |
| `state_sync_interval_seconds` | int | `10` | Seconds between state snapshots |
| `rotation_interval_minutes` | int | `60` | When to roll over DB files |
| `retention_hours` | int | `24` | Delete DBs older than this |
| `retention_max_events` | int | `100000` | Max total events across DB files |
| `store_output` | bool | `True` | Include stdout/stderr in event records |
| `flush_interval_seconds` | int | `5` | Buffer flush interval |
| `max_buffer` | int | `50000` | In-memory event buffer size |
| `max_ui_rows` | int | `5000` | Max rows returned to UI queries |
| `log_min_duration_ms` | int | `500` | Min duration to log slow tasks |
| `ws_min_duration_ms` | int | `500` | Min duration to broadcast via WebSocket |
| `event_min_duration_ms` | int | `0` | Min duration to persist to DB |
| `output_min_duration_ms` | int | `500` | Min duration to include stdout/stderr |
| `prometheus_url` | str | `''` | Prometheus server URL for dashboard links |
| `prometheus_rate_interval` | str | `'5m'` | Rate interval for Prometheus queries |
| `prometheus_worker_label` | str | `''` | Worker label name in Prometheus |
| `prometheus_cluster_label` | str | `''` | Cluster label name in Prometheus |
| `custom_links` | list[dict] | `[]` | Custom links for debug UI |

### Root-Level Settings

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `worker_name_env` | str | `'WORKER_ID'` | Env var holding worker name |
| `cluster_name` | str | `''` | Logical cluster name |
| `cluster_name_env` | str | `''` | Env var overriding cluster_name |
