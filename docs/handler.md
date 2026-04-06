# Handler System

The handler is the user-facing entry point into Drakkar. You subclass
`BaseDrakkarHandler`, override hooks, and the framework calls them at the
right time during the message-processing pipeline.

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
    config.executor.max_workers = cpu_count * 2
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

The only **required** hook. Called once per message window (1 to
`executor.window_size` messages from the same partition). Transforms
source messages into subprocess tasks.

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

### collect

```python
async def collect(self, result: ExecutorResult) -> CollectResult | None
```

Called after each task completes successfully. Process the executor
result and return a `CollectResult` with payloads for one or more sinks,
or `None` to skip delivery.

The `result.task` field carries the original `ExecutorTask`, including
its `metadata` dict.

```python
async def collect(self, result: dk.ExecutorResult) -> dk.CollectResult | None:
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

### on_window_complete

```python
async def on_window_complete(
    self,
    results: list[ExecutorResult],
    source_messages: list[SourceMessage],
) -> CollectResult | None
```

Called after **all** tasks in a window have finished (successes and
failures). Use for cross-task aggregation or batch-level outputs.
Returns a `CollectResult` or `None`.

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
error). Return one of:

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

Called when a sink's `deliver()` raises an exception. The `DeliveryError`
contains the sink name, sink type, error message, and the payloads that
failed.

| Return value           | Behavior                                           |
|------------------------|----------------------------------------------------|
| `DeliveryAction.DLQ`   | Write to dead letter queue (default)               |
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

`ExecutorTask.labels` is a `dict[str, str]` of user-defined key-value
pairs displayed alongside task details in the debug UI. Set them in
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
`collect()` or `on_error()`.

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

- Periodic tasks start after `on_ready()` completes.
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
The framework auto-discovers them and exposes them on the debug UI
metrics page alongside built-in Drakkar metrics.

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

    async def collect(self, result):
        self.items_processed.inc()
        self.match_count.observe(len(result.stdout.splitlines()))
        ...
```

The auto-discovery scans the handler's class hierarchy (MRO) for
attributes that are instances of `MetricWrapperBase` (Counter, Gauge,
Histogram, Summary, Info). Use an `app_` prefix to distinguish your
metrics from Drakkar's built-in `drakkar_` metrics in dashboards and
queries.
