# Sinks

Sinks are output destinations for processed results. After your [on_task_complete()](handler.md#on_task_complete),
[on_message_complete()](handler.md#on_message_complete), or [on_window_complete()](handler.md#on_window_complete) hook
returns a `CollectResult`, the framework routes each payload to the correct sink, serializes the data, and delivers it.

Drakkar ships with six sink types. You can configure any combination of them, and each type
supports multiple named instances (e.g., two separate Kafka topics or three Postgres databases).

## Delivery Lifecycle

| Event | When | What happens |
|-------|------|--------------|
| `connect()` | Worker startup, after [on_startup()](handler.md#on_startup) | Every configured sink's `connect()` runs concurrently (`asyncio.gather`), so cold-start latency is bounded by the slowest sink, not the sum. If any fails, the worker crashes immediately. |
| `deliver(payloads)` | After each [on_task_complete()](handler.md#on_task_complete) or [on_message_complete()](handler.md#on_message_complete) or [on_window_complete()](handler.md#on_window_complete) returns payloads | The framework groups payloads by `(sink_type, sink_name)`, then dispatches one `deliver()` call per group **in parallel** via `asyncio.gather`. A group whose sink declares [`depends_on`](#sink-dependencies-depends_on) waits for those sinks first. One slow or failing sink does not block the others. Each group owns its own retry budget and circuit-breaker state. |
| [Circuit breaker](#circuit-breaker) | Before each `deliver()` call | If the sink's breaker is open, `deliver()` is skipped and the sink's declared [`on_failure`](#failure-policy) decides at once: `dlq` writes the message's [DLQ record](#the-dlq-record), `skip` drops the payloads. |
| [on_delivery_error()](handler.md#on_delivery_error) | When `deliver()` raises an exception | Your handler decides: `DLQ`, `RETRY`, or `SKIP`. The default answer is the sink's declared `on_failure`. `RETRY` re-calls `deliver()` up to the sink's `max_retries` (default 2). Only a hook that returns `RETRY` spends that budget: the default hook makes one attempt and then applies the declared policy. |
| Offset commit | After **all** sinks settled for the message | Kafka offsets are committed only when every payload of the message was delivered, skipped, or the message was written to the DLQ. No partial commits. |
| `close()` | Worker shutdown | Each sink closes its connection gracefully. Errors are logged but don't block shutdown. |

**Delivery frequency.** For each successful task, [on_task_complete()](handler.md#on_task_complete) is called once. If it returns
payloads for N sink groups (e.g., 1 Kafka + 1 Postgres + 1 Redis = 3 groups), the framework
makes N independent `deliver()` calls. With [on_window_complete()](handler.md#on_window_complete), one additional delivery
round happens per window. The Postgres pool exposed in [on_ready()](handler.md#on_ready) is the same pool used by
the Postgres sink -- you can query it directly for lookups, migrations, or health checks.

```yaml
connections:
  kafka:
    main:
      brokers: "kafka:9092"
  postgres:
    main:
      dsn: "postgresql://user:pass@db:5432/app"   # better: DK_CONNECTIONS__POSTGRES__MAIN__DSN

sinks:
  kafka:
    results:
      topic: "search-results"       # connection main
  postgres:
    main: {}                        # connection main
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

Inserts, updates, or upserts a row, or runs an operator-authored statement. The
`op` field selects which; it defaults to `insert`.

| Field | Type | Ops | Description |
|-------|------|-----|-------------|
| `sink` | `str` | all | Target sink instance name (empty string for default) |
| `op` | `PostgresOp` | all | `insert` (default), `update`, `upsert`, or `statement` |
| `table` | `str` | insert, update, upsert | Target table name |
| `data` | `BaseModel` | insert, update, upsert | Column→value map; the SET assignments for `update` |
| `where` | `BaseModel` | update | Equality predicate, ANDed. **Required** |
| `conflict` | `list[str]` | upsert | `ON CONFLICT` target columns. **Required** |
| `update_columns` | `list[str] \| None` | upsert | Columns to overwrite on conflict; defaults to every `data` column not in `conflict` |
| `statement` | `str` | statement | Name of a statement declared in sink config |
| `params` | `BaseModel` | statement | Values bound to the statement's `:name` placeholders |

A field the chosen `op` does not use is a validation error, not a silently
ignored value — `PostgresPayload(op='insert', where=key)` raises.

**Serialization:** `data.model_dump()` produces a `{column: value}` dict. Column
and table names are validated against SQL injection. Columns are emitted in
sorted order — not model-declaration order — so every write renders identical
SQL; see [Column order](sink-write-operations.md#postgres-column-order).

**Batching:** payloads batch only with *adjacent* same-shaped neighbours, so
execution order always matches payload order. `insert` and `upsert` runs go out
as one multi-row `INSERT` (chunked at 65535 bind parameters); `update` and
`statement` runs go out as one `executemany`. If a batch fails, the framework
retries it payload-by-payload so the surfaced error still names the offending
payload.

```python
from drakkar import PostgresOp, PostgresPayload

PostgresPayload(table='search_results', data=search_summary)

PostgresPayload(
    op=PostgresOp.UPDATE,
    table='jobs',
    data=JobStatus(status='done'),
    where=JobKey(id=42),
)
```

See [Sink Write Operations](sink-write-operations.md) for every operation, the
named-statement escape hatch, and the ordering and retry rules.

### MongoPayload

Performs one write operation against a MongoDB collection — or runs a named
MQL statement.

| Field | Type | Description |
|-------|------|-------------|
| `sink` | `str` | Target sink instance name (empty string for default) |
| `op` | `MongoOp` | Which operation to perform; defaults to `insert` |
| `collection` | `str` | Target collection. Required for every op except `statement`, which declares its own |
| `data` | `BaseModel \| None` | Insert and update ops: the document, or the `$set` assignments |
| `filter` | `BaseModel \| None` | Update, upsert and delete ops: the predicate. Required, and never empty |
| `statement` | `str` | `statement` only: the key under `sinks.mongo.<name>.statements` |
| `params` | `BaseModel \| None` | `statement` only: values bound to the template's `":name"` placeholders |

**Serialization:** `data.model_dump()` produces the document; `filter.model_dump()`
produces an equality predicate. A field the chosen `op` does not use is a
validation error rather than a silently ignored value.

**Batching:** payloads are grouped into consecutive runs of the same collection
and each run is sent as one `bulk_write(ordered=True)`, which carries
heterogeneous operations and names the failing payload positionally without
re-sending anything. A run of one uses the direct method (`insert_one`,
`update_one`, …).

```python
from drakkar import MongoOp, MongoPayload

MongoPayload(collection='search_archive', data=search_result)

MongoPayload(
    op=MongoOp.UPDATE_ONE,
    collection='jobs',
    data=JobStatus(status='done'),
    filter=JobKey(id=job_id),
)
```

The full operation table, the `statements:` config block, and the parameter
binding rules are in
[Sink write operations](sink-write-operations.md#mongo).

### HttpPayload

Sends a request to an HTTP endpoint, body format per the sink's `encoding` setting.

| Field | Type | Description |
|-------|------|-------------|
| `sink` | `str` | Target sink instance name (empty string for default) |
| `data` | `BaseModel` | Payload model |

**Serialization:** The request body format follows the sink's `encoding` setting — `json`
(the default), `form`, or `multipart`. The encoder supplies the
`Content-Type` along with the body, so the two always agree. See
[Body encoding](configuration.md#body-encoding-sinkshttpnameencoding). Non-2xx responses raise an error routed through
[on_delivery_error()](handler.md#on_delivery_error).

```python
from drakkar import HttpPayload

HttpPayload(data=notification)
```

### RedisPayload

Issues one Redis write command, or runs an operator-authored Lua script. The
`op` field selects which; it defaults to `set`.

| Field | Type | Ops | Description |
|-------|------|-----|-------------|
| `sink` | `str` | all | Target sink instance name (empty string for default) |
| `op` | `RedisOp` | all | Which command to issue; defaults to `set` |
| `key` | `str` | all but `script` | Redis key suffix. The full key is `{config.key_prefix}{key}` |
| `data` | `BaseModel` | set, push | Serialized via `model_dump_json()` as the stored value |
| `ttl` | `int \| None` | set (optional), expire (required) | Expiry in seconds |
| `fields` | `dict \| list` | hset (mapping), hdel (names) | Hash fields. Must be non-empty |
| `members` | `dict \| list` | zadd (member→score), sadd/srem (names) | Must be non-empty |
| `amount` | `int` | incrby | The increment; may be negative or zero |
| `side` | `'left' \| 'right'` | push | Which end; `None` means left |
| `start` / `stop` | `int` | trim | Range to keep |
| `script` | `str` | script | Name of a script declared in sink config |
| `keys` | `list[str]` | script | Passed as `KEYS`, each prefixed. Required and non-empty |
| `args` | `list` | script | Passed as `ARGV`, in order |

A field the chosen `op` does not use is a validation error, not a silently
ignored value — `RedisPayload(op='delete', amount=5)` raises.

**Batching:** one delivery is one pipeline, in payload order. A command that
fails is named by position; **nothing is ever re-sent**, which is what makes
`incrby` and `push` safe to batch.

```python
from drakkar import RedisOp, RedisPayload

RedisPayload(key=f'search:{request_id}', data=search_summary, ttl=3600)

RedisPayload(op=RedisOp.INCRBY, key=f'hits:{day}', amount=1)

RedisPayload(op=RedisOp.ZADD, key='leaderboard', members={user: score})
```

See [Sink Write Operations](sink-write-operations.md) for every command, the
named-script escape hatch, and the ordering and retry rules.

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
any number of sinks in one shot. Groups run in parallel unless a sink declares
[`depends_on`](#sink-dependencies-depends_on).

### Failure policy

Every sink instance, of every type, declares what
happens when its delivery fails for good:

```yaml
sinks:
  postgres:
    main_db:
      connection: main       # a name under connections.postgres
      on_failure: dlq        # dlq (default) | skip
      max_retries: 2         # 0 to 10: retries after the first attempt, spent only by a hook that returns RETRY
      depends_on: []         # sinks to deliver first, as '<type>/<name>'
```

| Key | Default | Meaning |
|-----|---------|---------|
| `on_failure` | `dlq` | What happens when the retry budget is spent. `dlq`: the message goes to the [DLQ](#dead-letter-queue) and is done. `skip`: the payloads of this sink are dropped and the message goes on. |
| `max_retries` | `2` | Retries after the first attempt, spent only by a hook that returns `RETRY` (0 to 10). The default hook makes one attempt and then applies `on_failure`. The budget belongs to one sink for one result. |
| `depends_on` | `[]` | Sinks that must deliver first, written `<type>/<name>`. See [Sink dependencies](#sink-dependencies-depends_on). |

Environment overrides follow the usual rule:
`DK_SINKS__POSTGRES__MAIN_DB__ON_FAILURE=skip`.

The quick transient retry of an idempotent batch (see
[Retry contract](#retry-contract)) is a separate internal budget. It runs
inside one attempt, does not call the hook, and does not spend `max_retries`.

The config load refuses a bad policy with one of these messages
(`<path>` is `sinks.<type>.<name>`):

```text
<path>.on_failure must be dlq or skip, got '<v>'
<path>.max_retries must be a whole number from 0 to 10
<path>.depends_on: '<ref>' is not a configured sink; write '<type>/<name>'
<path>.depends_on: a sink cannot depend on itself
sinks: depends_on forms a cycle: <a> -> <b> -> <a>
```

#### `on_failure: dlq` — the message is done

When a sink's delivery of a task result or a message result ends in `dlq`:

1. The [DLQ record](#the-dlq-record) of each source message of the result is
   written once. Two results of one message that fail at the same moment
   write one record, not two.
2. From then on no delivery of that message starts. Later task results and
   the message result are not delivered; each sink group is counted on
   `drakkar_sink_deliveries_not_started_total{reason="message_dlq"}`. Tasks
   that already run are not cancelled, and
   [on_message_complete()](handler.md#on_message_complete) still fires, but
   its result is not delivered.
3. Groups of the same result that are already in flight finish. If one of
   them fails later, no second record is written: the worker logs
   `message_already_dlq`.
4. The offset completes as usual, when every task of the message settled.

The record's `dk-delivered-sinks` header lists the sinks that delivered a
result of the message before the record was written. A message can reach the
DLQ with some of its sinks already delivered. Drakkar is at-least-once, so a
replay of that message delivers those sinks again; make them idempotent if
that matters.

#### `on_failure: skip`

The payloads of this sink are dropped. The worker counts them on
`drakkar_sink_deliveries_skipped_total`, logs `sink_delivery_skipped`
(a warning, at most one line per sink per minute) and records a `sink_error`
event with `action: skip`. The other sinks of the result deliver as usual,
and a sink that depends on a skipped sink still starts.

### Sink dependencies (`depends_on`)

By default every sink group of a result is delivered at the same time.
`depends_on` orders them inside one result. In this example sinks 1 and 3
are written first, in parallel, and sink 2 only after both delivered, so a
consumer of the event never reads before the row and the cache entry exist:

```yaml
sinks:
  postgres:
    main_db: {}              # sink 1
  kafka:
    order_events:            # sink 2: announces what sinks 1 and 3 wrote
      topic: "order-events"
      depends_on: [postgres/main_db, redis/order_cache]
  redis:
    order_cache: {}          # sink 3
```

The rules for one result:

1. A sink with no dependencies starts at once; all such sinks run in parallel.
2. A sink starts when every dependency **that has payloads in this result**
   settled with success: delivered, or skipped (by the hook or by
   `on_failure: skip`). A dependency with no payloads in this result counts
   as satisfied.
3. If a dependency ends in `dlq`, the message is done (see above): its
   dependents do not start, reason `message_dlq`.
4. If a dependency fails without a DLQ record (a window result, or a record
   that could not be written), its dependents do not start, reason
   `dependency_failed`.

A sink that does not start is logged `sink_not_started` (debug), counted on
`drakkar_sink_deliveries_not_started_total{sink_type,sink_name,reason}` and
recorded as a `sink_not_started` event.

`depends_on` does not pass data between sinks, does not wait for a sink of
another result, and does not retry a dependent when a dependency recovers
later. Inside one group the payload order is the delivery order, as always.
The load checks every reference, refuses a sink that depends on itself, and
refuses a cycle (the messages are listed under [Failure policy](#failure-policy)).

### Window results

An [on_window_complete()](handler.md#on_window_complete) result is delivered
after the messages of its window completed their offsets. It has no source
message, so it cannot send a message to the DLQ. For a window result
`on_failure: dlq` acts as `skip` with a warning: the worker logs
`window_delivery_not_dlq` (at most one line per sink per minute), counts the
payloads on `drakkar_sink_deliveries_skipped_total`, and the sinks that depend
on this one do not start (reason `dependency_failed`). `depends_on` orders the
groups of a window result as it does for any other result.

### Error handling

When a sink's `deliver()` raises an exception, the framework calls your
[on_delivery_error()](handler.md#on_delivery_error) hook with a `DeliveryError`
after every failed attempt:

```python
class DeliveryError(BaseModel):
    sink_name: str            # e.g. "results"
    sink_type: str            # e.g. "kafka", "postgres", "http"
    error: str                # human-readable error message, URL credentials redacted
    payloads: list[BaseModel] # the payloads that failed
    attempt: int              # the failed attempt, from 1
    retries_left: int         # retries left of the sink's max_retries
    declared_policy: DeliveryAction  # the sink's on_failure: DLQ or SKIP
```

Your hook returns a `DeliveryAction`:

| Action | Behavior |
|--------|----------|
| `DeliveryAction.DLQ` | Send the message to the dead letter queue: [the message is done](#on_failure-dlq-the-message-is-done) |
| `DeliveryAction.RETRY` | Re-deliver **the whole group** while `retries_left > 0` — at-least-once, see below |
| `DeliveryAction.SKIP` | Drop this sink's payloads and continue |

- The default `on_delivery_error` returns `error.declared_policy`, so a handler
  that does not override the hook gets the configured `on_failure`.
- `RETRY` with `retries_left == 0` is treated as the declared `on_failure` and
  logged `sink_retry_budget_spent` (a warning, at most one line per sink per
  minute). A hook that answers `RETRY` for ever still ends.
- A hook that raises is logged `delivery_error_hook_failed` (error, with the
  exception) and the declared `on_failure` applies. The exception does not
  leave the delivery.

#### `RETRY` is at-least-once over the whole group

A retry re-calls `deliver()` with the **original payload list**, not with
whatever part of it had not been applied yet. Partial application before the
raise is the normal case, not an edge case:

- the HTTP sink POSTs sequentially — the first N requests already succeeded;
- the Kafka sink already has broker acks for some of the messages;
- Postgres and Mongo commit earlier adjacent runs in autocommit before a
  later run fails;
- the filesystem sink has already appended the earlier lines.

So a retried group re-applies whatever landed the first time. Every retry
logs `sink_delivery_retry` with the payload count and a note saying so.

This is deliberate, and it matches the guarantee the layer above already
gives: Drakkar is **at-least-once end to end**. A partition revoke, a drain
timeout or a crash replays the whole message and re-delivers every payload
in it. A `RETRY` that tried to resume from the first unapplied payload would
promise an exactness that a rebalance would break a moment later.

**What that means for your handler.** Return `RETRY` when re-applying a
payload is harmless — an idempotent upsert, a keyed Kafka topic a consumer
dedups, a `SET` in Redis. For an operation that counts (`INCRBY`, `LPUSH`,
an append, a webhook that charges a card), prefer `DLQ`: the original
message is kept in the DLQ for a deliberate replay instead of being re-applied
blindly. See
[Retry contract](#retry-contract) for the related per-sink `idempotent` flag,
which governs the framework's own internal fast-retry.

### Circuit Breaker

Every sink has a per-instance circuit breaker that protects the
worker from hammering a persistently-failing downstream. The breaker
operates at the **terminal-outcome** level (retries already
spent, or the handler returned `DLQ`), so transient blips don't trip it.
A `SKIP` is the handler's choice, not an infrastructure failure, and does
not count.

**States and transitions:**

| State | Behavior | Transition |
|-------|----------|------------|
| `closed` | Every `deliver()` call goes through. Terminal failures increment `_consecutive_failures`; any success resets it to zero. | After `breaker_failures` consecutive failures → `open`. |
| `open` | `deliver()` is skipped for the cooldown window. The sink's declared `on_failure` decides without calling `on_delivery_error`: `skip` drops and counts the payloads, `dlq` writes the [DLQ record](#the-dlq-record) with `dk-error: circuit open` and `dk-attempts: 0`. An open breaker is an infrastructure signal, not a per-message decision. | After `breaker_cooldown` elapsed, the next incoming batch promotes to `half_open`. |
| `half_open` | A **single probe** delivery is allowed through. Parallel delivery attempts against the same sink observe the in-flight probe and skip — the `_probe_inflight` gate enforces single-flight semantics. | Probe success → `closed` with counters reset. Probe failure → `open` with a fresh cooldown. |

**Operator-visible signals:**

- `drakkar_sink_circuit_open{sink_type, sink_name}` — gauge with
  states encoded as `0.0` (closed), `0.5` (half-open), `1.0` (open).
  Zero-initialized at registration so a never-tripped sink still
  appears in scrape output.
- `drakkar_sink_circuit_trips_total{sink_type, sink_name}` — counter
  ticking on every transition *into* open (initial failure-threshold
  trip AND half-open probe failures). A flapping circuit shows up as
  a rising rate on this counter, not a single trip plus silent
  reopens.
- `SinkStats.last_error == 'circuit open'` — the operator UI renders
  this sentinel so operators can distinguish breaker-routed DLQ
  records from actual delivery failures.

The breaker defaults (`breaker_failures: 5`, `breaker_cooldown: 30s`)
are tuned for typical throughput. Pipelines with stricter latency
budgets can lower the threshold so one persistent outage trips the
breaker sooner; long-recovery downstreams may want a longer cooldown.
See [Configuration → Circuit Breaker](configuration.md#circuit-breaker)
for the full knobs.

#### Circuit open with no DLQ configured

When a sink with `on_failure: dlq` meets an open circuit and no record can be
written, the rules of [When the DLQ itself fails](#when-the-dlq-itself-fails)
apply. A worker whose DLQ is off by configuration (no Kafka source and no
`dlq.topic`) counts the payloads of the failed sink on
`drakkar_dlq_unconfigured_drops_total` and goes on; it never stalls. (A task
that `on_error` sends to the DLQ counts its source messages there instead.) A worker with a DLQ that cannot write follows
`dlq.on_send_failure`. Declare `on_failure: skip` for a sink whose losses
you accept, so a down sink does not reach that path at all.

### Retry contract

Every `BaseSink` subclass declares an `idempotent: bool` class attribute that
tells `SinkManager` whether duplicate delivery is safe. The flag drives a small
fast-retry loop on transient errors *before* the failure reaches your
`on_delivery_error` handler:

| `idempotent` value | SinkManager behavior on transient error |
|--------------------|------------------------------------------|
| `True`  | Retry `deliver()` up to 3 times with exponential backoff (50ms / 100ms / 200ms). If still failing, surface the error to `on_delivery_error`. |
| `False` (default) | Single attempt. Any error — transient or not — goes straight to `on_delivery_error`. |

The transient-error classification covers `ConnectionError`, `TimeoutError`,
and `asyncio.TimeoutError`. Every other exception (including `ValueError` and
`RuntimeError`) is treated as non-transient and is not retried even when the
sink is idempotent — those signal a bug in the payload or sink, and a quick
re-attempt won't change the outcome.

All fast-retries inside a single `deliver()` call count as **one delivery
attempt** from the circuit breaker's perspective — whether the sink retried
internally 1 or 3 times, the breaker sees exactly one terminal outcome
(success or failure) per batch.

#### When to set `idempotent = True`

Opt in when duplicate delivery is provably safe:

- **Broker-side dedup**: Kafka producer with `enable.idempotence=true` +
  stable message key (the default `KafkaSink` does NOT enable this and
  therefore keeps `idempotent = False`).
- **Write-replace semantics**: Redis `SET` of a fixed key, filesystem
  atomic-rename overwrite, S3 `PutObject` on a stable path.
- **Idempotency keys**: HTTP `POST`/`PUT` with an `Idempotency-Key` header
  the receiver honors (Stripe, Shopify, custom APIs).
- **Upsert semantics**: Postgres `INSERT ... ON CONFLICT DO UPDATE`, Mongo
  update-with-upsert on a deterministic `_id`.

#### When to keep `idempotent = False`

Keep the default when duplicate delivery has observable side effects:

- Plain HTTP POST without an idempotency key.
- Append-only file writes without a dedup key.
- Plain INSERT without a unique constraint.
- Any non-idempotent network call (payment API, notification dispatch, etc.).

#### Built-in sink defaults

| Sink | `idempotent` | Reason |
|------|--------------|--------|
| `KafkaSink` | `False` | Default producer config does not enable broker dedup. |
| `HttpSink` | `False` | HTTP POST may have side effects without an idempotency key. |
| `FileSink` | `False` | Append mode duplicates records on retry. |
| `DLQSink` | `False` | Same as `KafkaSink`; DLQ also uses its own `send()` path. |
| `PostgresSink` | per batch | `UPDATE`/`UPSERT` converge on retry; plain `INSERT` duplicates. See below. |
| `MongoSink` | per batch | Updates, upserts and deletes converge; a plain `insert` duplicates. See below. |
| `RedisSink` | per batch | Most commands converge; `incrby`/`push`/`script` do not. See below. |

`PostgresSink` overrides `batch_idempotent()` rather than setting the flag: a
batch of only `update` and `upsert` payloads is retry-safe, while any `insert`
or `statement` payload makes it unsafe (`attempts = attempts + 1` is not
idempotent, and the framework cannot inspect operator SQL to know).

`RedisSink` does the same from the other direction: most Redis writes replace
or converge, so a batch is retry-safe unless it contains `incrby` (accumulates),
`push` (appends a duplicate), or `script` (opaque Lua).

`MongoSink` follows the Redis pattern: `$set` against a fixed filter converges,
upsert converges, and removal converges, so a batch is retry-safe unless it
contains an `insert` (duplicates documents) or a `statement` (opaque MQL —
`$inc` accumulates).

Any sink can do this when retry-safety is a property of the payloads rather than
of the sink:

```python
def batch_idempotent(self, payloads):
    """Defaults to the class-level `idempotent` flag."""
    return all(p.safe for p in payloads)
```

A sink subclass inherits the safe default (`False`). Set the attribute explicitly
on your subclass when you know duplicates are safe:

```python
class StripeWebhookSink(HttpSink):
    """Webhook sink that attaches a Stripe idempotency key."""

    idempotent = True  # Stripe dedups by Idempotency-Key header
```

### Example

```python
async def on_delivery_error(self, error: dk.DeliveryError) -> dk.DeliveryAction:
    # Retry only where re-applying the whole group is harmless. Redis SETs
    # are; an HTTP POST that charges a card is not — see "RETRY is
    # at-least-once over the whole group" above.
    if error.sink_type == 'redis':
        return dk.DeliveryAction.RETRY

    # Skip filesystem errors (non-critical logging)
    if error.sink_type == 'filesystem':
        return dk.DeliveryAction.SKIP

    # Everything else goes to DLQ for investigation
    return dk.DeliveryAction.DLQ
```

---

## Dead letter queue

The dead letter queue is a Kafka topic that keeps the **original messages**
that the worker could not process. The framework writes one record there for
each failed source message:

- a message whose value fails `input_model` parsing, with
  `sources.kafka.on_parse_error: dlq` (stage `parse`);
- a task whose [on_error()](handler.md#on_error) returns `ErrorAction.DLQ`
  (stage `execute`);
- a sink delivery that ends in `dlq`: the sink's `on_failure`, the handler's
  `DeliveryAction.DLQ`, a spent retry budget, or an open circuit
  (stage `sink`).

A record is written once per message: after it, [the message is
done](#on_failure-dlq-the-message-is-done).

### Topic derivation

The DLQ topic is configured under the `dlq` key:

```yaml
dlq:
  topic: ""       # empty = derived from sources.kafka.topic, or no DLQ at all
                  #   when the Kafka source is disabled
  connection: main  # the Kafka cluster: a name under connections.kafka
```

When `topic` is empty and the Kafka source is enabled, the framework derives
the DLQ topic as `{sources.kafka.topic}_dlq`. For example, if
`sources.kafka.topic` is `search-requests`, the DLQ topic becomes
`search-requests_dlq`.

**With the Kafka source disabled there is nothing to derive from**, so an
empty `topic` means no DLQ producer is built at all: a DLQ send is dropped,
logged once per worker run as `dlq_send_dropped_unconfigured`, and counted in
`drakkar_dlq_unconfigured_drops_total`. Set `dlq.topic` explicitly on an
HTTP-only worker that must not lose failed requests — see
[the DLQ rule](sources.md#the-dlq-without-the-kafka-source).

### The DLQ record

A DLQ record is the failed source message itself. Its **key and value are the
source message's key and value, byte for byte**; a value that is not valid
UTF-8 or JSON is copied unchanged. The record's Kafka timestamp is the time
the DLQ producer wrote it.

The headers come in a fixed order. First the source message's own headers, in
their original order, without any header whose name starts with `dk-` (a
replayed message that fails again gets fresh facts, never two sets). Then the
`dk-` headers, all UTF-8 text, in this order:

| Header | Value |
|--------|-------|
| `dk-record-version` | `1` |
| `dk-stage` | `parse`, `execute` or `sink` |
| `dk-error` | The error text with URL credentials redacted, cut to 2000 bytes at a UTF-8 character boundary. The full text is in the log line. |
| `dk-sink` | `<sink_type>/<sink_name>` of the failed sink; `sink` stage only |
| `dk-task-id` | The failed task's id; `execute` stage only |
| `dk-attempts` | Attempts made, in decimal. `sink`: the hook-level attempts of that sink for this message (`0` for an open circuit). `execute`: the task's attempts. Absent for `parse`. |
| `dk-delivered-sinks` | The sinks that delivered a result of this message before the record was written, as sorted, comma-joined `<sink_type>/<sink_name>`; absent when none |
| `dk-source-topic` | The source topic |
| `dk-source-partition` | The source partition, in decimal |
| `dk-source-offset` | The source offset, in decimal |
| `dk-source-timestamp` | The source message's Kafka timestamp in milliseconds |
| `dk-request-id` | The request id; HTTP source only |
| `dk-worker` | The worker name |
| `dk-instance` | The worker instance id |
| `dk-failed-at` | The time of the write, RFC 3339 UTC with six fraction digits (`2026-11-05T10:15:30.123456Z`) |

A header whose value is absent is not written; `0` is a value. For the
`execute` stage `dk-error` reads `exit code <n>: <detail>`, where the detail is
the task's stderr, else the exception text, else the failure kind; without an
exit code it is the detail alone.

A failed request of the [HTTP source](webapp.md) has its own form: the value
is the raw request body as the client sent it, the key is absent, and
`dk-source-topic`, `dk-source-partition` and `dk-source-offset` are absent.
`dk-source-timestamp` is the time the request was received, and
`dk-request-id` names the request.

An example, a Postgres sink failure of a message from partition 1 of `orders`
(the source message had one header of its own, `x-trace`):

```text
key:    order-18
value:  {"id": 18}
headers:
  x-trace              1
  dk-record-version    1
  dk-stage             sink
  dk-error             connection to postgresql://***:***@db:5432/main refused
  dk-sink              postgres/main_db
  dk-attempts          3
  dk-delivered-sinks   kafka/results,redis/cache
  dk-source-topic      orders
  dk-source-partition  1
  dk-source-offset     77
  dk-source-timestamp  1793000000789
  dk-worker            worker-a
  dk-instance          0123456789abcdef
  dk-failed-at         2026-11-05T10:15:30.123456Z
```

**The record is a wire format.** The header names, their order and the value
rules are a contract (pinned by `tests/fixtures/dlq_contract.json`). Tooling
reads the failure facts by header name and replays the key, the value and the
source headers as they are. The [Kafka read API](kafka-read.md) returns the
headers of each record it reads from the DLQ topic.

Because the record holds the original message, give the DLQ topic the same
access rules as the source topic ([Security](security.md)).

### Cluster of the DLQ

The DLQ producer connects to the cluster of the connection `dlq.connection`
names (default `main`), with that connection's brokers, `security` and
`client_config`. Point `dlq.connection` at another entry of `connections.kafka`
to write DLQ records to a different Kafka cluster.

### When the DLQ itself fails

The DLQ is the last resort. If a record cannot be written, the message has
nowhere safe to go. That happens when the DLQ write fails (broker down, topic
missing, auth error) or is not confirmed in time, and when a payload names a
sink that is not configured (the result is rejected before any delivery). The
`dlq.on_send_failure` setting decides what happens next:

| Strategy | Behavior | Trade-off |
|----------|----------|-----------|
| `drop` (default) | The loss is logged at CRITICAL (`dlq_failure_payloads_dropped`: with `message_count` for a sink or task failure, with `offset` for a parse failure) and counted in `drakkar_dlq_dropped_payloads_total` (source messages); the message counts as processed and its offset commits. The pipeline keeps moving. | The message is **lost**. Alert on the counter. |
| `stall` | The affected offset is left uncommitted (counted in `drakkar_delivery_stalled_offsets_total`) and the partition is **paused** — no new messages are fetched from it until the worker restarts or the partition is reassigned. On restart, processing resumes from the last committed offset, so the messages are redelivered. | No data loss, but the partition accrues consumer lag until the downstream is fixed and the worker restarted. |

A write judged unconfirmed at the flush timeout can still land later, so a
message that comes back under `stall` may get a second record, and a loss
counted under `drop` may have been written.

Both strategies are loud: `drakkar_dlq_send_failures_total` ticks on every failed
DLQ write regardless of the strategy, and a CRITICAL `dlq_send_failed` log entry
carries the full context. Under `stall`, the pause is also recorded as a
`partition_stalled` event in the flight recorder and a `partition_paused_on_stall`
ERROR log.

A stall-paused partition is deliberately excluded from the backpressure
pause/resume cycle — backpressure recovery never silently un-pauses it. Choose
`stall` when downstream data must never be dropped (audit, billing); keep `drop`
when pipeline liveness matters more than occasional loss during a double outage.

This is the only `stall` in the product: a sink's own `on_failure` is `dlq` or
`skip`. A message that is lost under `drop` is counted once, however many of
its sinks fail later. The strategy applies at all three delivery points —
after `on_task_complete`, after `on_message_complete`, and after
`on_window_complete` — and the task is still reported as a terminal failure
to the handler in every case; only the offset decision changes. A raising
`on_delivery_error` hook does not reach this path: the sink's declared
`on_failure` applies (see [Error handling](#error-handling)). With the DLQ off
by configuration the loss is the counted unconfigured drop above, never a
stall.

### DLQ replay

`scripts/replay_dlq.py` (also `just replay-dlq`) is a reference operator tool
that reads DLQ records and republishes the original messages to a target
Kafka topic. Each message is written as it was: the same key, the same value,
and the source headers, without any `dk-` header. Use it after fixing the
cause of the failures: the script drives those messages back through Kafka so
the normal consumer picks them up again.

#### When to use it

- A downstream sink regressed, the bug has been fixed, and you want to
  reprocess the dead-lettered messages without editing offsets by hand.
- You want to inspect and filter the DLQ contents before replaying (pair
  `--dry-run` with `--filter` or `--stage` to count matches).
- You want to divert DLQ content to a shadow topic for offline analysis
  (set `--target-topic` to a scratch topic).

#### Usage

```bash
# Dry run, count the sink failures of one customer
uv run python scripts/replay_dlq.py \
    --dlq-config=config.yaml \
    --target-topic=search-requests \
    --dry-run \
    --stage=sink \
    --filter='customer_id:42'

# Real replay, bounded
uv run python scripts/replay_dlq.py \
    --dlq-config=config.yaml \
    --target-topic=search-requests \
    --limit=1000

# Different broker for the target
uv run python scripts/replay_dlq.py \
    --dlq-config=config.yaml \
    --target-topic=search-requests \
    --target-brokers=prod-kafka:9092
```

| Flag | Meaning |
|------|---------|
| `--dlq-config` | The worker's YAML config; the DLQ topic and brokers come from it, with the worker's own fallback rules. |
| `--target-topic` | The topic to republish to. Required unless `--dry-run`. |
| `--target-brokers` | Brokers for the producer. Defaults to the brokers of `dlq.connection`. |
| `--dry-run` | Read and count, publish nothing. |
| `--limit` | Stop after N records have been **read** (filtered records count too). |
| `--filter` | Publish only records whose value contains this text. The value is read as UTF-8; bytes that are not UTF-8 become U+FFFD and the rest still matches. |
| `--stage` | Publish only records of one failing stage: `parse`, `execute` or `sink` (the `dk-stage` header). A record without `dk-stage` matches no stage. |

Each record is flushed and confirmed by the broker before the next one is
read. The script prints `replayed N/M` to stderr every 1000 records and a
final summary line (`summary: read=… published=… would_publish=… filtered_out=… skipped_old_format=… errors=…`)
when it finishes. Ctrl+C is handled gracefully — the record in flight
completes and the producer is flushed before exit.

#### Caveats

- **An old-format record is never replayed**: a record without a
  `dk-record-version` header was written by an older release (a JSON
  document of payloads, not a source message). The script skips it, also in
  a `--dry-run`, and counts it as `skipped_old_format` in the summary line.
  It stays in the DLQ topic: drain or delete the old topic content before a
  replay if it must not stay (see [Upgrading](upgrading.md#dlq-record)).
- **Duplicate processing**: the script does NOT deduplicate against the
  source topic. A message that reached the DLQ after some of its sinks
  delivered (`dk-delivered-sinks`) delivers to those sinks again when it is
  replayed. If those sinks are not idempotent (see
  [`BaseSink.idempotent`](#retry-contract)), check them before replaying.
- **Filter is substring-only**: `--filter='customer_id:42'` is a plain
  substring check on the value text. It does not parse JSON or support
  path expressions. For structural filtering, pipe the DLQ topic through
  `kcat | jq` in advance and feed a pre-filtered topic to the script.
- **`--limit` does not resume**: `--limit=1000` stops after 1000 records
  have been READ (filter-rejected records still count). Running with
  `--limit` twice replays the first N records both times — the consumer
  group is throwaway (unique per invocation), so offsets are not preserved
  between runs.
- **The partition is the target's**: the target producer picks the partition
  from the key, as any producer does. The source partition and offset stay in
  the DLQ record (`dk-source-partition`, `dk-source-offset`) for reference.
- **An HTTP request is not replayed into Kafka by itself**: its record has no
  key and its value is the raw request body. Replaying it into a source topic
  works only when the Kafka source parses the same body.
- **Exit code 1 with partial success**: if a publish fails mid-run, the script
  exits 1 and prints to stderr the DLQ partition and offset of the failed
  record (`publish failed at dlq_partition=… dlq_offset=…`) and of the last
  record it published (`last successful dlq_partition=… dlq_offset=…`).
  Re-run with an adjusted filter to resume.

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

### Connection-state flag

`BaseSink` maintains an `is_connected: bool` read-only property backed by the
`mark_connected()` / `mark_disconnected()` methods. You do NOT call these from
your subclass — the `SinkManager` drives the flag: `connect_all()` calls
`mark_connected()` after a successful `connect()`, and `close_all()` calls
`mark_disconnected()` after `close()` runs (even when `close()` raises, so the
flag faithfully reflects the manager's view of the connection). Readiness
probes (`/readyz`) read `is_connected` to decide whether to report the sink
as ready. A sink only needs to implement `connect()` / `close()`;
the bookkeeping is handled for you.

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
      request_timeout: 10s
      headers:
        Authorization: "Bearer ${WEBHOOK_TOKEN}"
```

**RedisSink** exposes the `redis.asyncio` client via its `client` property, the
counterpart to `PostgresSink.pool`. It is the way to run a read-modify-write
cycle, since sinks discard results and read commands are deliberately absent.
Note the current limit: handlers are never handed sink instances, so this is
reachable only from a sink subclass. A general `self.sinks` accessor is
a separate design.

It uses `redis.asyncio` (via `from_url`), connecting to the URL of its
connection:

```yaml
sinks:
  redis:
    cache:
      connection: main          # the URL comes from connections.redis.main
      key_prefix: "drakkar:"
```

**KafkaSink** takes its brokers, `security` and `client_config` from its connection, so
you declare a cluster once for every sink that writes to it.

**MongoSink** uses PyMongo's `AsyncMongoClient` for native asyncio support, and
holds any operator-authored MQL under `statements`:

```yaml
sinks:
  mongo:
    main:
      connection: main          # the URI comes from connections.mongo.main
      database: app
      statements:
        record_attempt:
          collection: jobs
          op: update_one
          filter: { _id: ":id" }
          update:
            $set: { last_seen: ":now" }
            $inc: { attempts: 1 }
```

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
        # Relative to the sink's required base_path — absolute paths are rejected.
        sinks.files.append(dk.FilePayload(path='high-match-results.jsonl', data=full_result))

    return sinks
```

Returning `None` from `on_task_complete()` skips delivery entirely for that result. The framework
only commits Kafka offsets after all sinks confirm delivery (or delivery errors are
handled through [on_delivery_error()](handler.md#on_delivery_error)). See [Offset Commit Logic](handler.md#offset-commit-logic) for details on watermark-based commit tracking.
