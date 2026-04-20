# Sinks

Sinks are pluggable output destinations for processed results. After your [on_task_complete()](handler.md#on_task_complete),
[on_message_complete()](handler.md#on_message_complete), or [on_window_complete()](handler.md#on_window_complete) hook
returns a `CollectResult`, the framework routes each payload to the correct sink, serializes the data, and delivers it.

Drakkar ships with six sink types. You can configure any combination of them, and each type
supports multiple named instances (e.g., two separate Kafka topics or three Postgres databases).

## Delivery Lifecycle

| Event | When | What happens |
|-------|------|--------------|
| `connect()` | Worker startup, after [on_startup()](handler.md#on_startup) | Each configured sink opens its connection (Kafka producer, asyncpg pool, motor client, httpx client, Redis connection). If any fails, the worker crashes immediately. |
| `deliver(payloads)` | After each [on_task_complete()](handler.md#on_task_complete) or [on_message_complete()](handler.md#on_message_complete) or [on_window_complete()](handler.md#on_window_complete) returns payloads | The framework groups payloads by `(sink_type, sink_name)` and calls `deliver()` once per group. A single `on_task_complete()` returning payloads for 3 sinks produces 3 `deliver()` calls. |
| [on_delivery_error()](handler.md#on_delivery_error) | When `deliver()` raises an exception | Your handler decides: `DLQ` (default), `RETRY`, or `SKIP`. Retries re-call `deliver()` up to `executor.max_retries` times. |
| Offset commit | After **all** sinks confirm delivery for a window | Kafka offsets are committed only when every payload from the window has been successfully delivered (or routed to DLQ/skipped). No partial commits. |
| `close()` | Worker shutdown | Each sink closes its connection gracefully. Errors are logged but don't block shutdown. |

**Delivery frequency.** For each successful task, [on_task_complete()](handler.md#on_task_complete) is called once. If it returns
payloads for N sink groups (e.g., 1 Kafka + 1 Postgres + 1 Redis = 3 groups), the framework
makes N independent `deliver()` calls. With [on_window_complete()](handler.md#on_window_complete), one additional delivery
round happens per window. The Postgres pool exposed in [on_ready()](handler.md#on_ready) is the same pool used by
the Postgres sink -- you can query it directly for lookups, migrations, or health checks.

```yaml
sinks:
  kafka:
    results:
      topic: "search-results"
  postgres:
    main:
      dsn: "postgresql://user:pass@db:5432/app"
  http:
    webhook:
      url: "https://hooks.example.com/notify"
```

---

## Payload types

Every sink type has a corresponding payload model. You create payload instances inside
[on_task_complete()](handler.md#on_task_complete) and return them in a [CollectResult](#collectresult). The `data` field is always a Pydantic
`BaseModel` -- the framework handles serialization differently for each sink type.

### KafkaPayload

Produces a message to a Kafka topic.

| Field | Type | Description |
|-------|------|-------------|
| `sink` | `str` | Target sink instance name (empty string for default) |
| `key` | `bytes \| None` | Kafka message key, passed through as-is |
| `data` | `BaseModel` | Payload model |

**Serialization:** `data.model_dump_json().encode()` becomes the Kafka message value.
The `key` is passed through unchanged to the producer.

```python
from drakkar import KafkaPayload

KafkaPayload(
    data=search_result,
    key=b"request-abc",
)
```

### PostgresPayload

Inserts a row into a PostgreSQL table.

| Field | Type | Description |
|-------|------|-------------|
| `sink` | `str` | Target sink instance name (empty string for default) |
| `table` | `str` | Target table name |
| `data` | `BaseModel` | Payload model |

**Serialization:** `data.model_dump()` produces a `{column: value}` dict. The framework
builds an `INSERT INTO <table> (<columns>) VALUES (<placeholders>)` query. Column and
table names are validated against SQL injection.

```python
from drakkar import PostgresPayload

PostgresPayload(
    table='search_results',
    data=search_summary,
)
```

### MongoPayload

Inserts a document into a MongoDB collection.

| Field | Type | Description |
|-------|------|-------------|
| `sink` | `str` | Target sink instance name (empty string for default) |
| `collection` | `str` | Target MongoDB collection name |
| `data` | `BaseModel` | Payload model |

**Serialization:** `data.model_dump()` produces a dict, inserted via `insert_one`.

```python
from drakkar import MongoPayload

MongoPayload(
    collection='search_archive',
    data=search_result,
)
```

### HttpPayload

Sends a JSON request to an HTTP endpoint.

| Field | Type | Description |
|-------|------|-------------|
| `sink` | `str` | Target sink instance name (empty string for default) |
| `data` | `BaseModel` | Payload model |

**Serialization:** `data.model_dump_json()` becomes the request body with
`Content-Type: application/json`. Non-2xx responses raise an error routed through
[on_delivery_error()](handler.md#on_delivery_error).

```python
from drakkar import HttpPayload

HttpPayload(data=notification)
```

### RedisPayload

Sets a key-value pair in Redis.

| Field | Type | Description |
|-------|------|-------------|
| `sink` | `str` | Target sink instance name (empty string for default) |
| `key` | `str` | Redis key suffix |
| `data` | `BaseModel` | Payload model |
| `ttl` | `int \| None` | Optional expiry in seconds |

**Serialization:** `data.model_dump_json()` becomes the Redis string value. The full
Redis key is `{config.key_prefix}{payload.key}`. When `ttl` is set, the key expires
after that many seconds (`SET key value EX ttl`).

```python
from drakkar import RedisPayload

RedisPayload(
    key=f'search:{request_id}',
    data=search_summary,
    ttl=3600,  # 1 hour
)
```

### FilePayload

Appends a JSON line to a file on the local filesystem.

| Field | Type | Description |
|-------|------|-------------|
| `sink` | `str` | Target sink instance name (empty string for default) |
| `path` | `str` | File path (relative to the sink's `base_path` config) |
| `data` | `BaseModel` | Payload model |

**Serialization:** `data.model_dump_json() + '\n'` is appended to the file in JSONL
format. The file is created if it does not exist. The parent directory must already exist.

**Path containment:** `base_path` is required in the filesystem sink config. All payload
paths are resolved relative to `base_path` and canonicalized — the framework raises
`ValueError` if the resolved path escapes the base directory (prevents path traversal).

```python
from drakkar import FilePayload

FilePayload(
    path='high-match-results.jsonl',  # resolved relative to sink's base_path
    data=search_result,
)
```

---

## Routing

The `sink` field on every payload controls which configured sink instance receives it.

### Single sink per type

When only one sink of a given type is configured, leave `sink` as the empty string
(the default). The framework routes automatically:

```yaml
sinks:
  kafka:
    results:          # only one kafka sink
      topic: "output"
```

```python
# sink='' (default) routes to "results" automatically
KafkaPayload(data=output, key=b"abc")
```

### Multiple named sinks

When you have multiple sinks of the same type, set `sink` to the instance name:

```yaml
sinks:
  kafka:
    results:
      topic: "search-results"
    alerts:
      topic: "search-alerts"
```

```python
# Route to the "results" kafka sink
KafkaPayload(sink='results', data=full_result, key=b"abc")

# Route to the "alerts" kafka sink
KafkaPayload(sink='alerts', data=alert_data, key=b"abc")
```

### AmbiguousSinkError

If you have multiple sinks of the same type but leave `sink` empty, the framework
raises `AmbiguousSinkError` at delivery time:

```
AmbiguousSinkError: 2 'kafka' sinks configured (['results', 'alerts']),
but payload has empty sink name -- specify which one
```

This is caught during validation before any delivery attempt, so misconfiguration
surfaces immediately.

---

## Delivery and error handling

### Delivery flow

The `SinkManager` groups all payloads from a `CollectResult` by `(sink_type, sink_name)`,
then calls `deliver()` on each group. A single `on_task_complete()` call can route payloads to
any number of sinks in one shot.

### Error handling

When a sink's `deliver()` raises an exception, the framework calls your
[on_delivery_error()](handler.md#on_delivery_error) hook with a `DeliveryError` containing the sink name, type,
error message, and the failed payloads:

```python
class DeliveryError(BaseModel):
    sink_name: str    # e.g. "results"
    sink_type: str    # e.g. "kafka", "postgres", "http"
    error: str        # human-readable error message
    payloads: list[BaseModel]  # the payloads that failed
```

Your hook returns a `DeliveryAction`:

| Action | Behavior |
|--------|----------|
| `DeliveryAction.DLQ` | Write the failed payloads to the dead letter queue (default) |
| `DeliveryAction.RETRY` | Retry delivery, up to `executor.max_retries` attempts |
| `DeliveryAction.SKIP` | Drop the payloads and continue processing |

If `RETRY` is returned but retries are exhausted, the framework falls through to DLQ.

### Example

```python
async def on_delivery_error(self, error: dk.DeliveryError) -> dk.DeliveryAction:
    # Retry transient failures for HTTP and Redis
    if error.sink_type in ('http', 'redis'):
        return dk.DeliveryAction.RETRY

    # Skip filesystem errors (non-critical logging)
    if error.sink_type == 'filesystem':
        return dk.DeliveryAction.SKIP

    # Everything else goes to DLQ for investigation
    return dk.DeliveryAction.DLQ
```

---

## Dead letter queue

When delivery fails and your `on_delivery_error` hook returns `DeliveryAction.DLQ`
(or retries are exhausted), the framework writes the failed payloads to a Kafka-based
dead letter queue.

### Topic derivation

The DLQ topic is configured under the `dlq` key:

```yaml
dlq:
  topic: ""       # empty = auto-derived from source topic
  brokers: ""     # empty = inherits from kafka.brokers
```

When `topic` is empty, the framework derives it as `{source_topic}_dlq`. For example,
if `kafka.source_topic` is `search-requests`, the DLQ topic becomes
`search-requests_dlq`.

### Message format

Each DLQ message is a JSON document containing:

| Field | Description |
|-------|-------------|
| `original_payloads` | The failed payloads, each serialized as a JSON string |
| `sink_name` | Name of the sink that failed |
| `sink_type` | Type of the sink that failed |
| `error` | Error message from the failed delivery |
| `timestamp` | Unix epoch timestamp of the failure |
| `partition` | Source Kafka partition the message came from |
| `attempt_count` | Number of delivery attempts before writing to DLQ |

### Broker inheritance

When `dlq.brokers` is empty, the DLQ producer connects to the same brokers as the
main Kafka consumer (`kafka.brokers`). Set `dlq.brokers` explicitly to write DLQ
messages to a different Kafka cluster.

---

## Sink connections

Each sink follows a lifecycle of `connect()` at startup and `close()` at shutdown.

### Startup

`connect()` is called once during worker startup for every configured sink. If any
sink fails to connect, the worker crashes immediately with a clear error -- this
prevents silent failures where the pipeline runs but cannot deliver results.

### Shutdown

`close()` is called for every sink during worker shutdown. Close errors are logged
but never raised, so shutdown proceeds cleanly even if a connection is already lost.

### Sink-specific details

**PostgresSink** exposes the `asyncpg` connection pool via its `pool` property. This
is available in [on_ready()](handler.md#on_ready) for running migrations, loading lookup tables, or any
direct database access needed at startup.

**HttpSink** uses `httpx.AsyncClient` with configurable timeout and headers:

```yaml
sinks:
  http:
    webhook:
      url: "https://hooks.example.com/notify"
      method: "POST"
      timeout_seconds: 10
      headers:
        Authorization: "Bearer ${WEBHOOK_TOKEN}"
```

**RedisSink** uses `redis.asyncio` (via `from_url`), connecting to the URL specified
in config:

```yaml
sinks:
  redis:
    cache:
      url: "redis://redis:6379/0"
      key_prefix: "drakkar:"
```

**KafkaSink** inherits `kafka.brokers` when its own `brokers` field is empty, so you
only need to specify brokers once for sinks on the same cluster.

**MongoSink** uses motor's `AsyncIOMotorClient` for native asyncio support.

**FileSink** requires `base_path` (non-empty) and validates it exists at connect time. All payload paths are contained within `base_path` — traversal attempts raise `ValueError`.
Individual payload paths must have existing parent directories.

---

## CollectResult

`CollectResult` is the return type of [on_task_complete()](handler.md#on_task_complete), [on_message_complete()](handler.md#on_message_complete), and [on_window_complete()](handler.md#on_window_complete). It has
one list field per sink type. Populate whichever fields match your configured sinks.

### Complete example

This example routes a single executor result to all six sink types, with conditional
routing for HTTP (webhook only for high-match results) and filesystem (JSONL log only
for very high-match results):

```python
async def on_task_complete(self, result: dk.ExecutorResult) -> dk.CollectResult | None:
    matches = [line for line in result.stdout.strip().split('\n') if line]
    meta = result.task.metadata

    full_result = SearchResult(
        request_id=meta['request_id'],
        pattern=meta['pattern'],
        match_count=len(matches),
        duration_seconds=result.duration_seconds,
        matches=matches[:50],
    )

    summary = SearchSummary(
        request_id=meta['request_id'],
        pattern=meta['pattern'],
        match_count=len(matches),
        duration_seconds=result.duration_seconds,
    )

    sinks = dk.CollectResult(
        kafka=[dk.KafkaPayload(data=full_result, key=meta['request_id'].encode())],
        postgres=[dk.PostgresPayload(table='search_results', data=summary)],
        mongo=[dk.MongoPayload(collection='search_archive', data=full_result)],
        redis=[dk.RedisPayload(key=f'search:{meta["request_id"]}', data=summary, ttl=3600)],
    )

    # Conditional: HTTP webhook for high-match results only
    if len(matches) > 20:
        notification = SearchNotification(
            request_id=meta['request_id'],
            pattern=meta['pattern'],
            match_count=len(matches),
            message=f"High match count: {len(matches)} matches for '{meta['pattern']}'",
        )
        sinks.http.append(dk.HttpPayload(data=notification))

    # Conditional: JSONL file log for very high-match results
    if len(matches) > 50:
        sinks.files.append(dk.FilePayload(path='/tmp/high-match-results.jsonl', data=full_result))

    return sinks
```

Returning `None` from `on_task_complete()` skips delivery entirely for that result. The framework
only commits Kafka offsets after all sinks confirm delivery (or delivery errors are
handled through [on_delivery_error()](handler.md#on_delivery_error)). See [Offset Commit Logic](handler.md#offset-commit-logic) for details on watermark-based commit tracking.
