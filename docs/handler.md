# Handler System

The handler is the user-facing entry point into Drakkar. You subclass
`BaseDrakkarHandler`, override hooks, and the framework calls them at the
right time during the message-processing pipeline. See [Configuration](configuration.md) for the full YAML reference.

## Hook Reference

| Hook | When called | Frequency | Returns |
|------|-------------|-----------|---------|
| `on_startup(config)` | Before any components are created | Once per worker lifetime | Modified `DrakkarConfig` |
| `on_ready(config, db_pool)` | After sinks connected, before polling | Once per worker lifetime | `None` |
| `arrange(messages, pending)` | A window of messages is ready to process | Once per window per partition | `list[`[`ExecutorTask`](executor.md#executortask)`]` |
| `on_task_complete(result)` | A single task completes successfully (exit 0) | Once per successful task | [`CollectResult`](sinks.md#collectresult) `\| None` |
| `on_message_complete(group)` | All tasks derived from a single source message reached a terminal state | Once per source message | [`CollectResult`](sinks.md#collectresult) `\| None` |
| `on_window_complete(results, messages)` | All tasks in a window finished | Once per window per partition | [`CollectResult`](sinks.md#collectresult) `\| None` |
| `on_error(task, error)` | A single task fails (non-zero exit, timeout, crash) | Once per failed task | `ErrorAction \| list[`[`ExecutorTask`](executor.md#executortask)`]` |
| `on_delivery_error(error)` | A sink's deliver() raises an exception | Once per failed sink delivery batch | `DeliveryAction` |
| `on_assign(partitions)` | Kafka assigns partitions during rebalance | Once per rebalance event | `None` |
| `on_revoke(partitions)` | Kafka revokes partitions during rebalance | Once per rebalance event | `None` |
| `message_label(msg)` | Before logging each message in arrange | Once per message | `str` |

Only `arrange()` is required. All other hooks have safe defaults.

---

## BaseDrakkarHandler

```python
from drakkar import BaseDrakkarHandler
```

### Generic type parameters

`BaseDrakkarHandler` accepts two optional type parameters that control
automatic (de)serialization of messages:

```python
class MyHandler(BaseDrakkarHandler[MyInput, MyOutput]):
    ...
```

Both type arguments must be Pydantic `BaseModel` subclasses. At class
creation time (`__init_subclass__`), the framework inspects the generic
arguments and sets two class attributes:

| Attribute      | Value                               |
|----------------|-------------------------------------|
| `input_model`  | The `InputT` class, or `None`       |
| `output_model` | The `OutputT` class, or `None`      |

When `input_model` is set, every consumed message is automatically
deserialized before `arrange()` is called:

```python
# Framework calls this internally:
msg.payload = MyInput.model_validate_json(msg.value)
```

If deserialization fails, `msg.payload` is set to `None` and the raw
bytes remain available in `msg.value`.

### Non-generic usage

If you do not need typed deserialization, omit the type parameters:

```python
class RawHandler(BaseDrakkarHandler):
    async def arrange(self, messages, pending):
        for msg in messages:
            raw: bytes = msg.value  # no auto-deserialization
            ...
```

Both `input_model` and `output_model` will be `None`.

---

## Handler Hooks

Hooks are listed in lifecycle order. All hooks except `arrange()` have
no-op defaults, so you only override what you need.

### on_startup

```python
async def on_startup(self, config: DrakkarConfig) -> DrakkarConfig
```

Called once, before any components (sinks, executor, consumer) are
created. Receives the loaded config and **must return** a (possibly
modified) `DrakkarConfig`. This is the only point where config can be
changed at runtime.

```python
import os

async def on_startup(self, config: dk.DrakkarConfig) -> dk.DrakkarConfig:
    cpu_count = os.cpu_count() or 4
    config.executor.max_executors = cpu_count * 2
    return config
```

### on_ready

```python
async def on_ready(self, config: DrakkarConfig, db_pool: object) -> None
```

Called after all sinks are connected and the executor pool is created,
but before the consumer starts polling. Use it to initialize handler
state, run database migrations, or load lookup tables.

The `db_pool` argument is an `asyncpg.Pool` if at least one postgres
sink is configured; otherwise it is `None`.

```python
async def on_ready(self, config: dk.DrakkarConfig, db_pool) -> None:
    self.lookup_table = {}
    if db_pool:
        rows = await db_pool.fetch('SELECT id, name FROM categories')
        self.lookup_table = {r['id']: r['name'] for r in rows}
```

### arrange (required)

```python
async def arrange(
    self,
    messages: list[SourceMessage],
    pending: PendingContext,
) -> list[ExecutorTask]
```

The only **required** hook. Transforms source messages into subprocess
tasks. See [ExecutorTask](executor.md#executortask) for the full task model reference.

**Partition isolation.** Each call receives messages from exactly **one
Kafka partition**. Drakkar runs an independent pipeline per partition, so
`arrange()` never mixes messages from different partitions in a single
call. The maximum number of concurrent `arrange()` invocations equals
the number of partitions assigned to this worker -- one per partition at
a time.

**[Windowing](executor.md#windowing).** The framework collects up to `executor.window_size`
messages from the partition queue before calling `arrange()`. A window
may contain fewer messages if the queue drains before reaching the
limit. While the tasks from one window are executing, the next window
can already be collected and `arrange()` called again for the same
partition -- windows are processed concurrently within a partition.

**When it is called.** The partition processor polls messages into a
queue. Once enough messages accumulate (or a short timeout passes), they
are batched into a window, deserialized (if `input_model` is set), and
passed to `arrange()`. This happens continuously while the worker is
running and the partition is assigned.

`pending.pending_task_ids` is a `set[str]` of task IDs currently
in-flight for this partition. Use it for O(1) deduplication:

```python
async def arrange(self, messages, pending):
    tasks = []
    for msg in messages:
        req = msg.payload
        if req is None:
            continue

        task_id = dk.make_task_id('rg')
        if task_id in pending.pending_task_ids:
            continue

        tasks.append(dk.ExecutorTask(
            task_id=task_id,
            args=[req.pattern, req.file_path],
            metadata={'request_id': req.request_id},
            source_offsets=[msg.offset],
        ))
    return tasks
```

If `arrange()` returns an empty list, all message offsets are immediately
marked complete and committed.

#### Precomputed task results (skip the subprocess)

Sometimes the handler already knows what a task would output — from a
cache, a lookup table, deterministic logic, or any other shortcut.
Attach a [`PrecomputedResult`](#precomputedresult) to the task via the
`precomputed` field and the framework will skip the subprocess entirely:

```python
async def arrange(self, messages, pending):
    tasks = []
    for msg in messages:
        cached = self.cache.get(msg.payload.request_id)
        if cached is not None:
            # Short-circuit: handler supplies the outcome directly.
            tasks.append(dk.ExecutorTask(
                task_id=dk.make_task_id('direct'),
                source_offsets=[msg.offset],
                metadata={'request_id': msg.payload.request_id},
                precomputed=dk.PrecomputedResult(stdout=cached),
            ))
        else:
            tasks.append(dk.ExecutorTask(
                task_id=dk.make_task_id('run'),
                args=['--process', msg.payload.request_id],
                source_offsets=[msg.offset],
            ))
    return tasks
```

**Semantics:**

- The framework synthesises an `ExecutorResult` from the precomputed
  `stdout` / `stderr` / `exit_code` / `duration_seconds` and invokes
  `on_task_complete` immediately. Downstream hooks (`on_message_complete`,
  `on_window_complete`) see the result indistinguishably from a real
  subprocess outcome — except `result.pid is None` and
  `result.task.precomputed is not None` if they want to distinguish.
- **No pool slot is consumed.** Precomputed tasks bypass the executor
  semaphore entirely; a cache-hit-heavy workload is not capped by
  `max_executors`. A separate counter
  (`drakkar_tasks_precomputed_total`) tracks the volume.
- **Non-zero `exit_code` routes through `on_error`** exactly like a real
  subprocess failure — `RETRY`, `SKIP`, and replacement-list return
  values all work. This lets the handler cache error outcomes too
  (uncommon but valid).
- `args` is not required when `precomputed` is set (it defaults to `[]`
  and the subprocess never runs).
- **Framework-agnostic about the source.** `precomputed` says "a result
  was supplied in `arrange()`"; the framework does NOT interpret it as
  "cache hit" specifically. The debug-UI event metadata is marked
  `precomputed=true`, not `cached=true`.

**Observability:**

- `task_started` and `task_completed` events still fire (with
  `metadata.precomputed=true`) so the flight recorder timeline stays
  coherent.
- `executor_duration` histogram is **not** observed for precomputed
  tasks — their duration is artificial and would skew the distribution.
- `drakkar_tasks_precomputed_total` increments once per precomputed
  task; operators can chart it against
  `drakkar_executor_tasks_total{status="completed"}` to see the
  short-circuit rate.

**When to use it:**

| Situation | Fit |
|---|---|
| Previously computed result in Redis / in-memory LRU | Direct fit — attach to tasks in `arrange()` |
| Deterministic input → deterministic output (no binary needed) | Direct fit — compute in Python, wrap in PrecomputedResult |
| Lookup table / enrichment where subprocess would add nothing | Direct fit |
| Expensive subprocess that's rarely worth running | Direct fit — decide in `arrange()` |
| Async I/O you don't want to do in `arrange()` | Less good — moves cache lookup into arrange's hot path; consider prefetching in `on_ready()` instead |

### on_task_complete

```python
async def on_task_complete(self, result: ExecutorResult) -> CollectResult | None
```

Called after each task completes successfully (exit code 0). Runs in
the context of the same partition that produced the task via `arrange()`.
Multiple `on_task_complete()` calls from the same window may run concurrently
as tasks finish in any order.

Process the [ExecutorResult](executor.md#executorresult) and return a
[CollectResult](sinks.md#collectresult) with payloads for one or more
sinks, or `None` to skip per-task delivery (for example when you
aggregate in [`on_message_complete`](#on_message_complete) instead).
See [Sinks](sinks.md) for all available payload types.

The `result.task` field carries the original [ExecutorTask](executor.md#executortask), including
its `metadata` dict.

```python
async def on_task_complete(self, result: dk.ExecutorResult) -> dk.CollectResult | None:
    meta = result.task.metadata
    matches = result.stdout.strip().splitlines()

    output = SearchResult(
        request_id=meta['request_id'],
        match_count=len(matches),
        duration_seconds=result.duration_seconds,
    )

    summary = SearchSummary(
        request_id=meta['request_id'],
        match_count=len(matches),
    )

    sinks = dk.CollectResult(
        kafka=[dk.KafkaPayload(data=output, key=meta['request_id'].encode())],
        postgres=[dk.PostgresPayload(table='results', data=summary)],
        redis=[dk.RedisPayload(key=f'search:{meta["request_id"]}', data=summary, ttl=3600)],
    )

    # conditional routing based on business logic
    if len(matches) > 20:
        notification = AlertPayload(request_id=meta['request_id'], count=len(matches))
        sinks.http.append(dk.HttpPayload(data=notification))

    return sinks
```

### on_message_complete

```python
async def on_message_complete(self, group: MessageGroup) -> CollectResult | None
```

Called **once per source message**, after every task derived from that
message has reached a terminal state. Receives a
[MessageGroup](#messagegroup) summarising the whole fan-out — use for
N-in → 1-out aggregation where one message produces many subprocess
tasks but you want a single aggregated record as output.

Offsets are committed **after** this hook fires — any sink emissions
here are guaranteed delivered-or-failed before the consumer offset
advances for this message.

```python
async def on_message_complete(self, group: dk.MessageGroup) -> dk.CollectResult | None:
    req: SearchRequest = group.source_message.payload
    if req is None or group.is_empty:
        return None

    total_matches = sum(
        sum(1 for line in r.stdout.split('\n') if line)
        for r in group.results
    )

    aggregate = RequestSummary(
        request_id=req.request_id,
        total_tasks=group.total,
        succeeded=group.succeeded,
        failed=group.failed,
        total_matches=total_matches,
    )

    return dk.CollectResult(
        kafka=[dk.KafkaPayload(data=aggregate, key=req.request_id.encode(), sink='summaries')],
    )
```

See the [Fan-out](fan-out.md) page for a complete walkthrough, the full
`MessageGroup` schema with properties, error-path semantics,
replacement-chain tracing via `parent_task_id`, and offset commit
ordering.

### MessageGroup

Passed to `on_message_complete`. Contains every task scheduled for a
single source message, the terminal outcomes, and the wall-clock timing.

| Field | Type | Meaning |
|---|---|---|
| `source_message` | `SourceMessage` | the original Kafka input |
| `tasks` | `list[ExecutorTask]` | full history — includes replaced tasks |
| `results` | `list[ExecutorResult]` | terminal successes |
| `errors` | `list[ExecutorError]` | terminal failures (SKIP / retries exhausted) |
| `started_at`, `finished_at` | `float` | monotonic timestamps |

Convenience properties: `succeeded`, `failed`, `total`, `replaced`,
`all_succeeded`, `any_failed`, `is_empty`, `duration_seconds`.

Full details and examples on the [Fan-out](fan-out.md) page.

### PrecomputedResult

Attached to an `ExecutorTask` via `precomputed=` when the handler
already knows what the subprocess would have produced. See
[Precomputed task results](#precomputed-task-results-skip-the-subprocess)
above for the full story.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `stdout` | `str` | `''` | stdout the framework would have captured |
| `stderr` | `str` | `''` | stderr the framework would have captured |
| `exit_code` | `int` | `0` | non-zero routes through `on_error` like a real failure |
| `duration_seconds` | `float` | `0.0` | set if you want the UI / recorder to show a non-zero duration (e.g. reflect a cache lookup time) |

### The cache (`self.cache`)

When `cache.enabled=true` in config, every handler instance gains a
`self.cache` attribute — a framework-provided key/value cache,
memory-backed and periodically flushed to a per-worker SQLite file. It
pairs naturally with [PrecomputedResult](#precomputedresult) for the
classic memoization pattern:

```python
async def arrange(self, messages, pending):
    tasks = []
    for msg in messages:
        cache_key = f'search|{msg.payload.pattern}'

        # Fast path: synchronous memory peek (zero I/O)
        cached = self.cache.peek(cache_key)
        if cached is None:
            # Slower path: memory miss, check SQLite (local + peer-synced)
            cached = await self.cache.get(cache_key)

        if cached is not None:
            tasks.append(dk.ExecutorTask(
                task_id=dk.make_task_id('search'),
                source_offsets=[msg.offset],
                precomputed=dk.PrecomputedResult(stdout=cached),
            ))
        else:
            tasks.append(dk.ExecutorTask(
                task_id=dk.make_task_id('search'),
                args=[msg.payload.pattern],
                source_offsets=[msg.offset],
            ))
    return tasks

async def on_task_complete(self, result):
    if result.pid is not None:   # skip precomputed — already cached
        cache_key = f'search|{result.task.args[0]}'
        self.cache.set(cache_key, result.stdout, ttl=3600)
    # ... return CollectResult as usual
```

When `cache.enabled=false` (the default), `self.cache` is a no-op stub
— `peek`/`get` return `None`, `set`/`delete` silently discard. Handler
code can call the cache unconditionally.

Full API, cross-worker sync behavior, scope rules (`LOCAL` /
`CLUSTER` / `GLOBAL`), and the documented "delete is local-only" sharp
edge are covered on the dedicated [Cache](cache.md) page.

### on_window_complete

```python
async def on_window_complete(
    self,
    results: list[ExecutorResult],
    source_messages: list[SourceMessage],
) -> CollectResult | None
```

Called after **all** tasks in a window have finished (successes and
failures). The `results` and `source_messages` belong to the same
partition and the same window that was passed to `arrange()`. Use for
cross-task aggregation or batch-level outputs. Returns a [CollectResult](sinks.md#collectresult)
or `None`.

```python
async def on_window_complete(self, results, source_messages):
    succeeded = [r for r in results if r.exit_code == 0]
    if not succeeded:
        return None

    summary = WindowSummary(
        window_size=len(results),
        success_count=len(succeeded),
        avg_duration=sum(r.duration_seconds for r in succeeded) / len(succeeded),
    )
    return dk.CollectResult(
        postgres=[dk.PostgresPayload(table='window_stats', data=summary)],
    )
```

### on_error

```python
async def on_error(
    self,
    task: ExecutorTask,
    error: ExecutorError,
) -> ErrorAction | list[ExecutorTask]
```

Called when a subprocess task fails (non-zero exit, timeout, or launch
error). Runs in the context of the partition that owns the task.
Replacement tasks returned here are added to the same window and
partition. Return one of:

| Return value            | Behavior                                      |
|-------------------------|-----------------------------------------------|
| `ErrorAction.RETRY`     | Re-run the same task (up to `max_retries`)    |
| `ErrorAction.SKIP`      | Drop the task, continue processing (default)  |
| `list[ExecutorTask]`    | Spawn replacement tasks instead               |

The `ExecutorError` fields:

| Field       | Type         | Description                                       |
|-------------|--------------|---------------------------------------------------|
| `task`      | ExecutorTask | The task that failed                               |
| `exit_code` | int or None  | Process exit code; `None` if never started/timeout |
| `stderr`    | str          | Process stderr or error description                |
| `exception` | str or None  | Exception message for timeout/launch failures      |
| `pid`       | int or None  | Process ID; `None` if never started                |

```python
async def on_error(self, task, error):
    if error.exit_code == 1 and 'TRANSIENT' in error.stderr:
        return dk.ErrorAction.RETRY
    return dk.ErrorAction.SKIP
```

### on_delivery_error

```python
async def on_delivery_error(self, error: DeliveryError) -> DeliveryAction
```

Called when a sink's `deliver()` raises an exception. See [Delivery Lifecycle](sinks.md#delivery-lifecycle) for the full delivery flow. The `DeliveryError`
contains the sink name, sink type, error message, and the payloads that
failed.

| Return value           | Behavior                                           |
|------------------------|----------------------------------------------------|
| `DeliveryAction.DLQ`   | Write to [dead letter queue](sinks.md#dead-letter-queue) (default) |
| `DeliveryAction.RETRY` | Retry delivery (up to `max_retries` from config)   |
| `DeliveryAction.SKIP`  | Drop the payloads, continue                        |

```python
async def on_delivery_error(self, error):
    if error.sink_type in ('http', 'redis'):
        return dk.DeliveryAction.RETRY
    return dk.DeliveryAction.DLQ
```

### on_assign

```python
async def on_assign(self, partitions: list[int]) -> None
```

Called when new Kafka partitions are assigned to this worker during a
rebalance. Use for per-partition initialization (loading state, resetting
caches).

```python
async def on_assign(self, partitions):
    for p in partitions:
        self.partition_cache[p] = {}
```

### on_revoke

```python
async def on_revoke(self, partitions: list[int]) -> None
```

Called when partitions are revoked from this worker during a rebalance.
Use for cleanup (flushing buffers, saving state).

```python
async def on_revoke(self, partitions):
    for p in partitions:
        self.partition_cache.pop(p, None)
```

---

## Offset Commit Logic

Drakkar uses **watermark-based offset tracking** to guarantee at-least-once
delivery. Understanding this is important for designing [arrange()](#arrange-required) and
`source_offsets`.

### How it works

Each partition has an `OffsetTracker` that maintains a sorted list of
offsets and their state (`PENDING` or `COMPLETED`):

1. When messages enter `arrange()`, their offsets are registered as
   **PENDING**.
2. When a task finishes and all its sink payloads are delivered (or
   routed to [DLQ](sinks.md#dead-letter-queue)/skipped), the offsets from `task.source_offsets` are
   marked **COMPLETED**.
3. The framework asks: *what is the highest consecutive completed offset
   starting from the lowest tracked offset?* That value + 1 is committed
   to Kafka.

### Example

A window of 5 messages from partition 3 with offsets `[100, 101, 102, 103, 104]`:

```
Step 1: All registered as PENDING
  100=PENDING  101=PENDING  102=PENDING  103=PENDING  104=PENDING
  committable() → None (100 is not completed)

Step 2: Offset 104 finishes first (fastest subprocess)
  100=PENDING  101=PENDING  102=PENDING  103=PENDING  104=COMPLETED
  committable() → None (100 is still pending)

Step 3: Offsets 100 and 101 finish
  100=COMPLETED  101=COMPLETED  102=PENDING  103=PENDING  104=COMPLETED
  committable() → 102 (consecutive run: 100, 101 → commit 101+1)

Step 4: Offsets 102 and 103 finish
  100=COMPLETED  101=COMPLETED  102=COMPLETED  103=COMPLETED  104=COMPLETED
  committable() → 105 (all consecutive → commit 104+1)
```

The key property: **a fast task finishing before earlier tasks does not
advance the commit position**. Drakkar will never commit offset 105
while offset 100 is still in flight. This prevents message loss on
worker crash — Kafka will redeliver from the last committed offset.

### What this means for your handler

- **`source_offsets` must be accurate.** Every offset in `source_offsets`
  blocks the commit watermark until its task completes and all payloads
  are delivered. If you include an offset you don't actually process, it
  will stall commits for the entire partition.

- **Fewer tasks per window = lower commit latency.** A window of 100
  messages producing 100 tasks won't commit until the slowest task
  finishes. If commit latency matters, use a smaller `window_size`.

- **One task can cover multiple offsets.** If your `arrange()` batches
  several messages into one task, list all their offsets in
  `source_offsets`. They'll all be marked complete together when the task
  finishes.

- **Empty arrange = instant commit.** If `arrange()` returns `[]`, all
  offsets in the window are marked complete immediately and committed.

### When commits happen

Commits are attempted at two points:

1. **After each window completes** — when the last task in a window
   finishes and all sink deliveries succeed.
2. **On partition revocation** — pending offsets are drained (up to
   `executor.drain_timeout_seconds`). **Only if drain completes
   cleanly** is the highest committable offset committed before the
   partition is released. If drain times out, the final commit is
   skipped — in-flight tasks may still be running, and committing past
   them would silently skip their messages on reassign. Those messages
   will replay instead (at-least-once).
3. **On shutdown** — same drain-then-commit-if-clean behavior as
   revocation.

Commits are per-partition and asynchronous. They do not block the
processing loop.

---

## Typed Messages

Define Pydantic models for your input and output schemas, then pass them
as type parameters to `BaseDrakkarHandler`:

```python
from pydantic import BaseModel

class SearchRequest(BaseModel):
    request_id: str
    pattern: str
    file_path: str

class SearchResult(BaseModel):
    request_id: str
    match_count: int
    duration_seconds: float

class MyHandler(dk.BaseDrakkarHandler[SearchRequest, SearchResult]):
    async def arrange(self, messages, pending):
        for msg in messages:
            req: SearchRequest = msg.payload  # auto-deserialized
            ...
```

The framework calls `SearchRequest.model_validate_json(msg.value)`
before `arrange()`. If parsing fails, `msg.payload` is `None` and the
raw bytes remain in `msg.value`.

Access either form in your hooks:

```python
req = msg.payload        # SearchRequest instance (or None on parse error)
raw_bytes = msg.value    # always available
```

---

## message_label()

```python
def message_label(self, msg: SourceMessage) -> str
```

Override to provide a human-readable label used in structured log lines
and the debug web UI. The default returns `partition:offset`.

```python
def message_label(self, msg):
    if msg.payload:
        return f'{msg.partition}:{msg.offset} [{msg.payload.request_id[:8]}]'
    return f'{msg.partition}:{msg.offset}'
```

---

## Task Labels

[ExecutorTask](executor.md#executortask)`.labels` is a `dict[str, str]` of user-defined key-value
pairs displayed alongside task details in the [debug UI](observability.md#debug-ui). Set them in
`arrange()`:

```python
dk.ExecutorTask(
    task_id=dk.make_task_id('rg'),
    args=[req.pattern, req.file_path],
    metadata={'request_id': req.request_id},
    labels={
        'request_id': req.request_id,
        'pattern': req.pattern,
    },
    source_offsets=[msg.offset],
)
```

Labels appear on:

- The live timeline view
- The task detail page
- Running and finished task tables
- The debug trace view

Labels are purely for display. Use `metadata` for data you need in
`on_task_complete()`, `on_message_complete()`, or `on_error()`.

---

## Periodic Tasks

Decorate async handler methods with `@periodic(seconds=N)` to run them
on a recurring interval.

```python
from drakkar import periodic

class MyHandler(dk.BaseDrakkarHandler[MyInput, MyOutput]):

    @periodic(seconds=60)
    async def refresh_cache(self):
        self.lookup_table = await fetch_lookup_data()

    @periodic(seconds=30, on_error='stop')
    async def health_check(self):
        if not os.path.isdir('/data/corpus'):
            raise RuntimeError('Corpus directory missing')
```

### Behavior

- Periodic tasks start after [on_ready()](#on_ready) completes.
- They run in the same async event loop as the rest of the worker.
- Overlapping runs are prevented -- the next interval starts only after
  the current invocation finishes.
- All periodic tasks are cancelled during shutdown.

### Error handling

| `on_error` value       | Behavior on exception                      |
|------------------------|--------------------------------------------|
| `'continue'` (default) | Log the error, keep running on schedule   |
| `'stop'`               | Log the error, cancel this task permanently |

---

## Custom Prometheus Metrics

Declare `prometheus_client` metrics as class attributes on your handler.
The framework auto-discovers them and exposes them on the [debug UI](observability.md#debug-ui)
metrics page alongside built-in [Drakkar metrics](observability.md#prometheus-metrics).

```python
from prometheus_client import Counter, Gauge, Histogram

class MyHandler(dk.BaseDrakkarHandler[MyInput, MyOutput]):
    items_processed = Counter(
        'app_items_processed_total',
        'Total items processed',
    )
    active_sessions = Gauge(
        'app_active_sessions',
        'Currently active sessions',
    )
    match_count = Histogram(
        'app_match_count',
        'Matches per request',
        buckets=(0, 1, 5, 10, 50, 100, 500),
    )

    async def on_task_complete(self, result):
        self.items_processed.inc()
        self.match_count.observe(len(result.stdout.splitlines()))
        ...
```

The auto-discovery scans the handler's class hierarchy (MRO) for
attributes that are instances of `MetricWrapperBase` (Counter, Gauge,
Histogram, Summary, Info). Use an `app_` prefix to distinguish your
metrics from Drakkar's built-in `drakkar_` metrics in dashboards and
queries.
