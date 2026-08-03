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

## Column order

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

Payloads batch only with **adjacent** same-shaped neighbours, so the order
statements reach the database always matches the order the handler returned
them. Grouping globally would batch better but would execute a payload before
its predecessor — harmless for inserts, a silently lost write for updates.

| Run of | Sent as |
|---|---|
| `insert` / `upsert` | one multi-row `VALUES` statement, chunked at 65535 bind parameters |
| `update` / `statement` | one `executemany` — one prepared statement, N argument tuples |

When a batch fails, the framework retries it payload-by-payload so the surfaced
error names the offending payload. That fallback cannot double-write: a failed
`executemany` is atomic, and a failed multi-row `INSERT` wrote nothing.

## Retry safety

Retry-safety is a property of the *batch*, not of the sink, so `PostgresSink`
answers per delivery through
[`batch_idempotent`](sinks.md#retry-contract) rather than through the
class-level `idempotent` flag:

| Batch contains | Retry-safe |
|---|---|
| only `update` and `upsert` | yes — both converge on re-delivery |
| any `insert` | no — a plain insert duplicates rows |
| any `statement` | no — the SQL is opaque to the framework |

`attempts = attempts + 1` is not idempotent, and the framework cannot inspect
operator SQL to know whether a given statement is. Marking individual statements
idempotent in configuration is a natural extension, deliberately left out for
now.

## Observability

The message probe (`POST /api/v1/debug/probe`) reports the planned operation:
`extras.op` carries the discriminator, `extras.where` and `extras.conflict`
appear for the operations that use them, and `destination` is the statement name
for a named statement or the table for everything else.

Delivery-failure logs name the operations and the statement names involved,
never the SQL text.
