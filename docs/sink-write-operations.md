# Sink Write Operations

A sink payload does not have to mean "insert a record". Stateful sinks accept an
operation discriminator that selects what the framework writes, and an escape
hatch for everything the declarative fields cannot express.

## The principle

> The declarative tier stays deliberately dumb. The escape hatch is always the
> datastore's own language, authored by the operator in configuration, invoked
> by name with bound parameters.

Two tiers, and nothing in between:

- **Declarative operations** cover the shapes almost every pipeline needs —
  insert, update, upsert. The handler describes *what* to write with typed
  models; the framework builds the statement.
- **Named statements** cover everything else. The SQL lives in sink
  configuration under a name; the handler invokes that name and supplies bound
  parameters.

Five properties follow from that shape, and they are the reason for it:

- **No injection surface.** Parameters are always bound, never interpolated, so
  message content can never reach the statement text.
- **Operator-reviewable.** A DBA reads SQL in YAML rather than in handler code.
- **Low-cardinality names** in DLQ entries, logs, and error messages — never
  query text, which can leak row data.
- **Changing the query is a config change**, not a code deploy.
- **Nothing new to learn.** The escape hatch is the datastore's own language.
  There is no framework query dialect to document or to hand-write twice.

## Postgres

`PostgresPayload.op` selects the operation and defaults to `insert`, so existing
handlers are unaffected.

| `op` | Builds | Required fields |
|------|--------|-----------------|
| `insert` (default) | `INSERT INTO t (…) VALUES (…)` | `table`, `data` |
| `update` | `UPDATE t SET … WHERE …` | `table`, `data`, `where` |
| `upsert` | `INSERT … ON CONFLICT (…) DO UPDATE SET …` | `table`, `data`, `conflict` |
| `statement` | operator-authored SQL, by name | `statement` |

A field the chosen `op` does not use is a validation error rather than a
silently ignored value — `PostgresPayload(op='insert', where=key)` raises.

### insert

```python
PostgresPayload(table='search_results', data=search_summary)
```

### update

```python
PostgresPayload(
    op=PostgresOp.UPDATE,
    table='jobs',
    data=JobStatus(status='done', finished_at=now),
    where=JobKey(id=42),
)
```

Renders `UPDATE "jobs" SET "finished_at" = $1, "status" = $2 WHERE "id" = $3`
(columns are sorted — see [Column order](#column-order)).

`where` is required and may not serialize to an empty mapping — an empty
predicate would rewrite every row in the table. A `None` value in `where`
renders `IS NULL`, not `= NULL`, which is never true and would match nothing.

An `UPDATE` matching zero rows is a silent no-op, exactly as it is when you
write the SQL yourself.

### upsert

```python
PostgresPayload(
    op=PostgresOp.UPSERT,
    table='sessions',
    data=Session(id=1, created_at=t0, last_seen=t1),
    conflict=['id'],
    update_columns=['last_seen'],   # created_at is preserved on conflict
)
```

Renders `INSERT INTO "sessions" (…) VALUES (…) ON CONFLICT ("id") DO UPDATE SET
"last_seen" = EXCLUDED."last_seen"`. Omit `update_columns` to overwrite every
non-conflict column. When every `data` column is a conflict column there is
nothing left to overwrite and the statement becomes `DO NOTHING`.

`conflict` columns need not appear in `data` — a unique index on a generated or
defaulted column is legitimate.

### statement — arbitrary SQL, operator-controlled

Anything the declarative fields cannot express — expressions that read the
current value, guarded predicates, optimistic concurrency — goes in a **named
statement**.

```yaml
sinks:
  postgres:
    primary_warehouse:
      dsn: "postgresql://user:pass@db:5432/app"
      statements:
        claim_job: |
          UPDATE jobs
             SET status = :status,
                 attempts = attempts + 1
           WHERE id = :id
             AND status = 'pending'
        bump_counter: |
          INSERT INTO counters (key, hits) VALUES (:key, 1)
          ON CONFLICT (key) DO UPDATE SET hits = counters.hits + 1
```

```python
PostgresPayload(
    op=PostgresOp.STATEMENT,
    statement='claim_job',
    params=ClaimParams(id=42, status='running'),
)
```

Statement names must match `^[a-z_][a-z0-9_]*$`, because they appear as
structured-log fields.

#### Placeholders

Placeholders are written `:name` and compiled once at startup to the positional
`$n` form asyncpg binds. A name used twice binds one value:

```sql
UPDATE t SET a = :v, b = :v WHERE id = :id   -->   SET a = $1, b = $1 WHERE id = $2
```

The compiler never mistakes a colon for a placeholder inside a string literal, a
quoted identifier, a dollar-quoted string, a `--` line comment, or a nested
`/* */` block comment. In code, `::text` is a cast, `arr[1:3]` is a slice, and
`:=` is an assignment — all copied through untouched. Writing a positional `$1`
yourself is an error, since the framework has no value to bind to it.

A missing *or* unexpected key in `params` is an error, so a typo in either the
payload model or the config surfaces immediately rather than binding silently.

#### What is validated, and when

Statements are **not** verified against the database at startup. `PREPARE`
cannot distinguish "your SQL is malformed" from "that column does not exist", so
validating there would couple worker startup to schema state.

| Problem | Surfaces at |
|---|---|
| Malformed placeholder syntax, bad statement name, empty SQL | startup, as a config error |
| Unknown statement name in a payload | delivery, via `on_delivery_error` |
| `params` missing or unexpected keys | delivery, via `on_delivery_error` |
| Missing column, missing table, constraint violation | delivery, via `on_delivery_error` |

That last row is the same behaviour an `INSERT` naming a missing table has
always had.

## Redis

`RedisPayload.op` selects the command and defaults to `set`, so existing
handlers are unaffected. One write verb per data type:

| `op` | Command | Required fields |
|------|---------|-----------------|
| `set` (default) | `SET pk <json> [EX ttl]` | `key`, `data` |
| `delete` | `DEL pk` | `key` |
| `expire` | `EXPIRE pk ttl` | `key`, `ttl` |
| `incrby` | `INCRBY pk amount` | `key`, `amount` |
| `hset` | `HSET pk f v [f v …]` | `key`, `fields` (mapping) |
| `hdel` | `HDEL pk f [f …]` | `key`, `fields` (list) |
| `push` | `LPUSH`/`RPUSH pk <json>` | `key`, `data` |
| `trim` | `LTRIM pk start stop` | `key`, `start`, `stop` |
| `sadd` | `SADD pk m [m …]` | `key`, `members` (list) |
| `srem` | `SREM pk m [m …]` | `key`, `members` (list) |
| `zadd` | `ZADD pk score member […]` | `key`, `members` (mapping) |
| `script` | `EVALSHA sha n pk… args…` | `script`, `keys` |

Reads are deliberately absent, for the same reason `SELECT` is absent from the
Postgres sink: a sink discards results. A read-modify-write cycle belongs in the
handler, through the sink's [`client` property](sinks.md#sink-specific-details).

A field the chosen `op` does not use is a validation error rather than a
silently ignored value, and a required collection may not be empty —
`hset` with `fields={}` would be a malformed command.

```python
RedisPayload(key=f'result:{request_id}', data=summary, ttl=3600)
RedisPayload(op=RedisOp.INCRBY, key=f'hits:{day}', amount=1)
RedisPayload(op=RedisOp.HSET, key=f'session:{sid}', fields={'ip': ip})
RedisPayload(op=RedisOp.ZADD, key='leaderboard', members={user: score})
```

`data` is a model only where the value IS a serialized object (`set`, `push`).
Hash fields and sorted-set members are frequently dynamic keys — a leaderboard
keyed by user id cannot be a model with static field names — so those take
plain typed mappings and lists instead.

### script — arbitrary Lua, operator-controlled

Multi-step or conditional logic goes in a **named script**. Lua also buys
something no declarative op can: **a script is atomic on the server**, while a
pipeline is not a transaction. An LPUSH-then-LTRIM pair issued as two ops can
interleave with another writer; the same pair inside a script cannot.

```yaml
sinks:
  redis:
    result_cache:
      url: "redis://redis:6379/0"
      key_prefix: "drakkar:"
      scripts:
        push_and_cap: |
          redis.call('LPUSH', KEYS[1], ARGV[1])
          redis.call('LTRIM', KEYS[1], 0, tonumber(ARGV[2]) - 1)
          return redis.call('LLEN', KEYS[1])
```

```python
RedisPayload(
    op=RedisOp.SCRIPT,
    script='push_and_cap',
    keys=['recent'],
    args=[summary.model_dump_json(), 100],
)
```

Script names must match `^[a-z_][a-z0-9_]*$`, because they appear as
structured-log fields. `keys` must be non-empty: a keyless script cannot be
routed under Redis Cluster, so declaring keys keeps scripts cluster-safe from
the start.

**Every entry of `keys` is prefixed**, not just the single-key ops' `key`. The
prefix is the sink instance's namespace, and a script given raw keys could write
outside it.

Values reach the script through `KEYS` and `ARGV` and are never interpolated
into the body, so message content cannot alter what runs. Scripts are **not**
validated against a live server — there is no Lua parser available without one,
and validating there would couple worker startup to Redis availability. A
broken script fails at delivery through `on_delivery_error`. Registration at
startup computes the SHA1 locally with no round trip, so it stays cheap and
survives a briefly unavailable Redis.

### Argument order

Mapping arguments (`hset` fields, `zadd` members) are emitted in **sorted key
order**; lists (`hdel` fields, `sadd`/`srem` members) keep the order you supply.
Order changes neither command's end state, but it does change the emitted
command, and the Go backend decodes a mapping into a map with no order to
preserve — so sorting is the only rule both backends can honour. This is the
same reasoning as [Postgres column order](#postgres-column-order) below.

`zadd` takes `members` as member→score, which is the natural shape and matches
the client's own signature; Redis receives `score member`, flipped during
rendering.

## Postgres column order

Columns are emitted in **sorted** order, not in the order the payload model
declares its fields:

```python
class Row(BaseModel):
    request_id: str
    answer: int

PostgresPayload(table='results', data=Row(...))
# INSERT INTO "results" ("answer", "request_id") VALUES ($1, $2)
```

Bound values follow the same sort, so columns and values always stay aligned.
The rule exists for cross-backend identity: the Go backend decodes payload data
into a map, which has no field order to preserve, so sorting is the only rule
both backends can honour unconditionally. It also makes the emitted SQL
independent of how a model happens to be written.

Two lists are *not* sorted, because they are the operator's own: `conflict`, and
an explicit `update_columns`. An `update_columns` left to default is derived from
the data columns and is therefore sorted with them.

## Batching and ordering

Execution order always equals payload order on both sinks. How they get there
differs, because the two datastores batch differently.

**Postgres** batches only with **adjacent** same-shaped neighbours. Grouping
globally would batch better but would execute a payload before its predecessor —
harmless for inserts, a silently lost write for updates.

| Run of | Sent as |
|---|---|
| `insert` / `upsert` | one multi-row `VALUES` statement, chunked at 65535 bind parameters |
| `update` / `statement` | one `executemany` — one prepared statement, N argument tuples |

When a batch fails, the framework retries it payload-by-payload so the surfaced
error names the offending payload. That fallback cannot double-write: a failed
`executemany` is atomic, and a failed multi-row `INSERT` wrote nothing.

**Redis** has no shape-grouping problem at all: a pipeline carries heterogeneous
commands and executes them in order, so one delivery is one pipeline and
ordering is preserved for free. There is no chunking either — batch size is
bounded by `executor.window_size` and Redis has no per-command parameter limit.

A failing Redis command is attributed **positionally**, and nothing is re-sent:
the pipeline returns one result per command, with per-command errors present as
values rather than raised, so the offending payload is named without repeating
the ones that succeeded. That is what makes `incrby` and `push` safe to batch.
A connection-level failure is different — there the framework cannot know what
was applied, so the error propagates with the whole batch.

## Retry safety

Retry-safety is a property of the *batch*, not of the sink, so both sinks answer
per delivery through [`batch_idempotent`](sinks.md#retry-contract) rather than
through the class-level `idempotent` flag.

**Postgres:**

| Batch contains | Retry-safe |
|---|---|
| only `update` and `upsert` | yes — both converge on re-delivery |
| any `insert` | no — a plain insert duplicates rows |
| any `statement` | no — the SQL is opaque to the framework |

**Redis** is mostly-idempotent by nature, so the veto list is short:

| Batch contains | Retry-safe |
|---|---|
| only `set`, `delete`, `expire`, `hset`, `hdel`, `sadd`, `srem`, `zadd`, `trim` | yes — each replaces or converges |
| any `incrby` | no — it accumulates |
| any `push` | no — it appends a duplicate element |
| any `script` | no — the Lua is opaque to the framework |

`attempts = attempts + 1` is not idempotent, and the framework cannot inspect
operator SQL or Lua to know whether a given one is. Marking individual
statements and scripts idempotent in configuration is a natural extension,
deliberately left out for now.

A TTL is the one place a retry is not exactly convergent: `SET … EX 3600`
restarts the expiry window, so a fast-retry can shift it by the backoff — a few
hundred milliseconds. The alternative, an absolute `EXAT` deadline computed by
the worker, would converge exactly but would take the timestamp from the
worker's clock, making worker/server skew shift real expiry times. Clock skew is
the worse hazard, so TTLs stay relative.

## Observability

The message probe (`POST /api/v1/debug/probe`) reports the planned operation on
both sinks: `extras.op` carries the discriminator, and `destination` is the
escape hatch's name — the statement for Postgres `op=statement`, the script for
Redis `op=script` — or the table/key for everything else. Postgres additionally
reports `extras.where` and `extras.conflict` for the operations that use them.

Delivery-failure logs name the operations and the statement or script names
involved, never the SQL or Lua text, which can carry row data.

No metric names or labels change on either sink.
