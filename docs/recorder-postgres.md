# Recorder History in Postgres

With `ui.recorder.persistence: postgres`, every worker sends its recorder
events to one table in a central Postgres database. Many workers and many
clusters write to the same table. The trace search of the UI
(`trace?history=1`) reads that table instead of sealed recorder files. A
worker in this mode needs no local disk.

This page tells you what the mode does, how to set it up, how large the
database gets, and what happens when Postgres is down. For the memory store
that every mode keeps, see [Recent Events in Memory](recent-events.md).

## What the mode does

The mode does this:

- The worker keeps its recent events in the memory store, exactly as with
  `none`. Every UI read except the history search uses the memory store.
- Stage P, a write stage next to the memory stage, sends every row that
  the memory store takes to the table, with the same `id`, `ts`, `event` and
  worker name. So the UI joins memory rows and table rows without duplicates.
- Stage P keeps one partition per UTC hour, creates the next ones and drops
  the expired ones (see [Hourly partitions and the upkeep](#hourly-partitions-and-the-upkeep)).
- `trace?history=1` searches the table rows of the worker's own cluster.

The mode does not do this:

- It writes no recorder file. There is no rotation, no sealing, no publish
  into the history folder, and the `history.archive` cluster task does not
  run. `worker_state` is not written, and the last-breath flush at exit
  writes nothing.
- The history search does not read old sealed files. Merge, file downloads
  and the archives list still show the sealed files that are in the history
  folder.
- `trace-by-label` does not search the table. It reads the memory store.
- `/api/v1/debug/databases` shows no Postgres row, and there is no download
  of Postgres rows.

`postgres` is never derived from `storage.dir`. Write it in the config. It
loads with an empty `storage.dir`: the worker then has no cache file and no
crash marker, as with `none`.

| | `none` | `sqlite` | `postgres` |
|---|---|---|---|
| Memory store | yes | yes | yes |
| Local recorder file | no | yes | no |
| Rotation, sealing, publish into the history folder | no | yes | no |
| `history.archive` cluster task | no | yes | no |
| Rows in Postgres, partition upkeep | no | no | yes |
| `trace?history=1` reads | sealed files, when a shared folder resolves | sealed files | the Postgres table, rows of the own cluster |
| Other UI reads | memory | memory or file | memory |
| `storage.dir` | optional | required | optional |

## The keys

The DSN and the schema come from `connections.postgres.<name>` (see
[Fleet: connections](fleet.md#connections)). The recorder opens its own pool
from that entry. It does not share the pool of the registry or the election.

```yaml
recorder:
  provider: postgres            # sqlite | postgres
  postgres:
    connection: main            # name under connections.postgres
    table_prefix: ""            # "" -> <schema>.recorder_events; "heavy" -> <schema>.heavy_recorder_events
    initial_retention: 24h      # written into the table when this worker creates it
    store_completed_output: false  # send the stdout and stderr columns to Postgres
    batch_rows: 5000            # most rows in one COPY
    max_queue: 128MiB           # most text waiting for Postgres, counted in characters
    resilience: {}              # timeout, retries, backoff and breaker of the recorder calls
```

| Key | Type | Default | Bounds | Env |
|---|---|---|---|---|
| `recorder.provider` | `sqlite` or `postgres` | `sqlite` | one of the two | `DK_RECORDER__PROVIDER` |
| `recorder.postgres.connection` | string | `main` | a name under `connections.postgres` | `DK_RECORDER__POSTGRES__CONNECTION` |
| `recorder.postgres.table_prefix` | string | `''` | empty, or `^[a-z][a-z0-9_]{0,23}$` | `DK_RECORDER__POSTGRES__TABLE_PREFIX` |
| `recorder.postgres.initial_retention` | duration, whole hours | `24h` | `1h` to `168h` | `DK_RECORDER__POSTGRES__INITIAL_RETENTION` |
| `recorder.postgres.store_completed_output` | boolean | `false` | | `DK_RECORDER__POSTGRES__STORE_COMPLETED_OUTPUT` |
| `recorder.postgres.batch_rows` | whole number | 5000 | 100 to 50000 | `DK_RECORDER__POSTGRES__BATCH_ROWS` |
| `recorder.postgres.max_queue` | size | `128MiB` | `16MiB` to `1GiB` | `DK_RECORDER__POSTGRES__MAX_QUEUE` |
| `recorder.postgres.resilience.*` | the shared resilience block | `5s`, 2 retries, `200ms`, `5s`, breaker 5 / `30s` | its rules | `DK_RECORDER__POSTGRES__RESILIENCE__<KEY>` |

No key is secret; the DSN stays in `connections`. The whole-number keys
accept `30.0` and refuse `30.5`. The keys load and do nothing unless the mode
is `postgres`.

These existing keys also act with `postgres`:

- `ui.recorder.flush_interval_seconds` is also the tick of the Postgres loop.
- `ui.recorder.max_buffer` is also the row bound of the Postgres queue.
- `ui.trace.history_max_seconds` is also the time limit of the history
  search. `ui.trace.history_max_files` does not apply.

`max_flush_retries`, `rotation_interval_hours`, `store_state` and the whole
`history.archive` block load and are checked as usual, then ignored.
`recorder_started` lists `rotation_interval_hours` (and `store_state` when it
is on) in `ignored_keys`.

The config load fails when the connection is not declared:

```
ui.recorder.postgres.connection '<name>' is not defined under connections.postgres
```

This check runs after the `sqlite` folder check and before the history and
rotation check, whatever `ui.enabled` says. A bad prefix fails with:

```
ui.recorder.postgres.table_prefix '<value>' must be empty, or a lower-case letter followed by at most 23 lower-case letters, digits or underscores
```

The config summary line shows ` recorder=postgres` after ` storage=...` and
the optional ` shared=...`, with or without a `storage.dir`:

```
... storage=mem recorder=postgres fleet=...
```

## The table

The table is `<schema>.recorder_events`, or `<schema>.<prefix>_recorder_events`
with a prefix. It is partitioned by `ts`, one partition per UTC hour.
`drakkar-schema postgres --schema=drakkar --recorder` prints this DDL:

```sql
CREATE SCHEMA IF NOT EXISTS drakkar;

CREATE TABLE IF NOT EXISTS drakkar.recorder_events (
    cluster        text             NOT NULL,
    worker         text             NOT NULL,
    instance       text             NOT NULL,
    id             bigint           NOT NULL,
    ts             double precision NOT NULL,
    dt             text             NOT NULL,
    event          text             NOT NULL,
    "partition"    integer,
    "offset"       bigint,
    task_id        text,
    args           text,
    stdout_size    bigint,
    stdout         text,
    stderr         text,
    exit_code      integer,
    duration       double precision,
    output_topic   text,
    metadata       text,
    pid            integer,
    labels         text,
    origin         text             NOT NULL,
    client_name    text,
    request_id     text,
    trace_offsets  bigint[],
    PRIMARY KEY (instance, id, ts)
) PARTITION BY RANGE (ts);

COMMENT ON TABLE drakkar.recorder_events IS 'retention_hours=6';

CREATE INDEX IF NOT EXISTS recorder_events_partition_offset_idx ON drakkar.recorder_events ("partition", "offset");

CREATE INDEX IF NOT EXISTS recorder_events_task_id_idx ON drakkar.recorder_events (task_id);

CREATE INDEX IF NOT EXISTS recorder_events_trace_offsets_idx ON drakkar.recorder_events USING gin (trace_offsets) WITH (fastupdate = off) WHERE trace_offsets IS NOT NULL;
```

The columns:

- `cluster`, `worker`, `instance` identify the writer: the cluster name (`''`
  when it is empty), the worker name and the 16 hex characters of the worker
  process.
- The next 20 columns are the event columns of the recorder files, with the
  same names and order.
- `trace_offsets` is not an event column. Stage P fills it from the
  `metadata` of `task_started` (`source_offsets`), `annotation` and
  `offload` (`offsets`) rows, so the trace search has an index.

The primary key is `(instance, id, ts)`. An `id` is unique within one worker
process, and `instance` is random per process, so a key never repeats in the
fleet. The key also makes a retried write safe (see
[While Postgres is down](#while-postgres-is-down)).

Every text value that holds a NUL character (U+0000) is written with U+FFFD
in its place, because Postgres `text` refuses NUL. Postgres 14 or later is
needed.

### The fixed index set

The table has three indexes and the primary key, and no more. They serve the
trace search. Plan the index set before the table fills: a later
`CREATE INDEX` on a filled partitioned table builds on every partition under
a lock that blocks every writer. To change the indexes, use a new table
prefix, or run `CREATE INDEX CONCURRENTLY` on each partition by hand.

## Table prefixes and separate clusters

All workers on one prefix share one table and one retention. Give a cluster
its own prefix when its load or its expiry differs from the others:

- a busy cluster with a short retention on `heavy`, the quiet clusters on the
  empty prefix with a longer one;
- a cluster whose history must not share a table with another cluster.

The prefix is a lower-case letter followed by at most 23 lower-case letters,
digits or underscores. The table, partition and index names start with it:

| Thing | Prefix `''` | Prefix `heavy` |
|---|---|---|
| Events table | `recorder_events` | `heavy_recorder_events` |
| One partition (UTC hour) | `recorder_events_p2026110314` | `heavy_recorder_events_p2026110314` |
| Index names | `recorder_events_<suffix>` | `heavy_recorder_events_<suffix>` |

Clusters on one prefix never see each other's rows in the history search:
the search filters by the worker's own cluster. No worker can delete the
history of another cluster by its own setting (see [Retention](#retention)).

## Hourly partitions and the upkeep

The partition of an hour holds the rows with `ts` from the start of that UTC
hour up to, not including, the start of the next one. There is no default
partition.

Stage P keeps the partitions current. There is no cluster task for it. The
upkeep runs at the first stage P run after the start, then every 300 seconds,
and once more at the next run after a row found no partition. One upkeep is
one transaction:

1. A try-lock on the table (`pg_try_advisory_xact_lock`). When another
   worker holds it, the upkeep is done for this worker, and the next one is
   due in 300 seconds. So every writer of the table runs the upkeep, and one
   at a time does the work.
2. The lock time-out is set to 3 seconds.
3. The server clock gives the current hour, so every writer uses one clock.
4. The worker reads the stored retention from the table comment.
5. It creates every missing partition from one hour behind to three hours
   ahead.
6. With a stored retention of `R` hours, it drops every partition whose hour
   ended at least `R` hours ago. Without one, it drops nothing.
7. It commits and logs `recorder_postgres_partitions_changed` when it created
   or dropped a partition.

A partition name that is not the table name, `_p` and a valid UTC hour is not
the upkeep's, and the upkeep leaves it alone.

Lock cost: a create or a drop takes an `ACCESS EXCLUSIVE` lock on the table
until the upkeep commits. While the upkeep waits for that lock (for example
behind a long history search), every new COPY of every writer of that table
waits behind it. The lock time-out bounds this wait to **3 seconds per
upkeep**, about once an hour in the steady state (one create and one drop per
hour). A lock time-out (SQLSTATE `55P03`) ends that upkeep with an error; the
next one is due in 300 seconds. That error counts as a failed `partitions`
operation, and the breaker of the recorder calls is shared by the start, the
upkeep and the COPY, so many such errors in a row can open it.

## Retention

The table stores its retention in its comment, `retention_hours=<n>`. The
worker that creates the table writes its own `retention_hours` there. After
that the comment rules: the upkeep drops partitions only by the stored value,
never by a worker's setting. So the clusters on one prefix share one
retention, and no cluster can delete the history of another one.

A comment counts when it is exactly `retention_hours=<n>` with `n` from 1 to
168. Without such a comment (no comment, `retention_hours=0`, a trailing
space, any other text), the upkeep still creates partitions and drops none.

When the stored value differs from the worker's `retention_hours`, or there
is none, the worker logs `recorder_postgres_retention_mismatch` once per
process and goes on. It checks this only in an upkeep in which it holds the
try-lock, so the line can come late on a worker that seldom wins the lock.

To change the retention of a table, run:

```sql
COMMENT ON TABLE drakkar.recorder_events IS 'retention_hours=24';
```

The next upkeep drops by the new value.

Why at most 168 hours: there is one partition per hour, the history search
visits every partition, and each partition costs locks and planning time.

## Schema management and ownership

At the start, stage P checks that the table exists. When it exists, the start
is done; a running fleet takes no table lock when a worker starts.

When the table is missing:

- With `connections.postgres.<name>.manage_schema: true` (the default), the
  worker creates the schema, the table, its comment and its indexes in one
  transaction, under the same advisory lock that the provider tables use and
  with a lock time-out of 3 seconds. A worker that waited sees the table that
  another worker created and stops there.
- With `manage_schema: false`, the start fails with this message, and the
  next tick tries again:

```
table drakkar.recorder_events does not exist and connections.postgres.main.manage_schema is false; create it with: drakkar-schema postgres --schema=drakkar --recorder --retention-hours=24
```

The command gains ` --table-prefix=<prefix>` when the prefix is not empty. It
prints the worker's `initial_retention`, so an operator who follows the message
gets the retention of the config. The command's own default, with no
`--retention-hours` flag, is 6 hours.

Partitions are always the upkeep's work, in both modes. Postgres lets only
the owner of a table create or drop its partitions, so **the role of the
connection must own the events table**. When an administrator creates the
table, make the worker role its owner:

```bash
drakkar-schema postgres --schema=drakkar --recorder --retention-hours=24 | psql "$ADMIN_DSN"
psql "$ADMIN_DSN" -c 'ALTER TABLE drakkar.recorder_events OWNER TO drakkar_worker'
```

`drakkar-schema` takes three flags for the recorder table:

| Flag | Meaning |
|---|---|
| `--recorder` | After the provider DDL, print one empty line and the recorder DDL. |
| `--table-prefix=<p>` | The table prefix. Only with `--recorder`. |
| `--retention-hours=<n>` | The value of the table comment, 1 to 168, default 6. Only with `--recorder`. |

A prefix or retention flag without `--recorder`, a value that breaks its rule,
or `--recorder=<value>` is a usage error: exit code 2 and nothing on stdout.
The command cannot print partitions; they depend on the time.

## Write volume

Size the database before you turn the mode on. The numbers below assume about
1200 tasks per second on a busy worker and 3 to 5 rows per task, so up to 5000
rows per second per worker, and about 0.5 KB per row with
`store_output: false` (the row and its share of the key and the indexes).

| | rows per second | data per second | per hour | at the default 6 h | per day |
|---|---|---|---|---|---|
| one busy worker | 5000 | 2.5 MB | 9 GB | 54 GB | 216 GB |
| 3 busy workers | 15000 | 7.5 MB | 27 GB | 162 GB | 650 GB |
| 27 busy workers | 135000 | 67.5 MB | 243 GB | 1.5 TB | 5.8 TB |

The write-ahead log about doubles the disk writes. Each COPY runs with
`synchronous_commit = off`: the history is disposable, so a COPY does not
wait for a flush of the write-ahead log. A crash of the database server can
lose the last fraction of a second of commits, and nothing else.

So one database holds the history of a few busy workers at the default
retention (a few hundred GB for 3 to 5 busy workers), not of every worker of
a large fleet at full load. Put the busy clusters on their own prefix with a
short retention, or keep them on `sqlite`.

Two kinds of settings cut the volume:

- The recorder filters `ui.recorder.event_min_duration_ms` and
  `output_min_duration_ms` act before the memory store. They also shorten the
  live view and the memory store.
- `ui.recorder.postgres.store_output` acts on Postgres only (see below).

`drakkar_recorder_postgres_rows_written_total` and the size of the table give
the real numbers.

## Output columns: `store_output`

With `store_output: false` (the default), stage P writes `stdout` and
`stderr` as `NULL`. `stdout_size` stays. Nothing else changes: `metadata`
keeps its bytes, so a table row has the same `metadata` as the memory row.

Stdin inside `metadata` follows the existing recorder rules:
`ui.recorder.store_stdin` (default off) for `task_started`, and the capped
stdin of `task_failed` (at most `ui.recorder.stdin_max_bytes`, default 64
KiB). To keep stdin out of Postgres, keep `store_stdin` off and lower
`stdin_max_bytes`.

With `store_output: true`, rows with output can be many KB each. Size the
database again.

## Connections per worker

| Connection | Opened | Count |
|---|---|---|
| Write pool (stage P: COPY and upkeep, one at a time) | at the stage P start | 1 (a pool, so a broken connection is replaced) |
| History search | per search, closed after | 0 to 2 |
| Shared `connections` pool (registry, election, when they use Postgres) | by the providers | `connections.postgres.<name>.pool_min` to `pool_max` (default 1 to 4) |

A worker with the defaults uses at most 1 + 2 + 4 = 7 server connections,
usually 1 to 3. Size `max_connections` of the server from this table and the
number of workers.

## While Postgres is down

Rows wait in the **Postgres queue** between the memory store and stage P. The
queue has two bounds: `ui.recorder.max_buffer` rows (default 50000) and
`queue_max_mib` MiB of text (default 128), counted in characters of text. When a new row passes either bound,
the oldest rows are dropped. The memory stage never waits for stage P.

Stage P runs on a thread of its own with its own event loop and one
connection, so a slow or absent Postgres never stalls the main loop, the
memory store or the pipeline. It runs on every memory tick, on the
`flush_interval_seconds` tick, and when the queue passes half of either
bound. One run starts stage P when it has not started, runs the upkeep when
it is due, then writes chunks until the queue is empty. A chunk is at most
`batch_rows` rows and at most 4 MiB of text (counted in characters of text), and one COPY in one transaction.

A chunk stays in one slot outside the queue until it is written or dropped,
so a retry sends exactly the same rows. When an earlier attempt landed and
its answer was lost, the retry fails on the primary key (SQLSTATE `23505`),
and stage P counts the chunk as written. A row never appears twice.

| What happens | What stage P does |
|---|---|
| The start fails (Postgres down, missing table) | Rows wait in the queue. The next tick tries again. The worker keeps running and serves the UI. |
| A COPY fails with a network error, a time-out or a transient SQLSTATE after its retries | The chunk stays in its slot; the run stops; the next run tries the same chunk. It is never dropped for this. |
| The breaker of the recorder calls is open | Stage P skips at once, one probe per cooldown. No chunk is dropped. |
| A COPY fails with an error that its rows cause: the server refuses them as bad data (SQLSTATE class `22`) or for a constraint (class `23`, for example `23514`, a row with no partition), or the client cannot encode a value | The chunk stays and its count of final failures goes up by one; `recorder_postgres_copy_rejected` is logged. After `23514` the next run starts with an upkeep. At the second final failure the chunk is dropped. |
| A COPY fails with any other server error (for example `25006` on a read-only server, `42501` for a missing right, `42P01` for a missing table, or a disk error) | The chunk stays in its slot and is never dropped for this; `provider_call_failed` is logged. The failure counts toward the breaker as in an outage. |
| A COPY or the upkeep fails with `42P01` (the table was dropped) | The next run starts stage P again: with `manage_schema: true` it creates the table again, with `manage_schema: false` it logs the missing-table message (see [Schema management and ownership](#schema-management-and-ownership)). Then it writes the kept chunk. |
| The queue passes a bound | The oldest rows are dropped (`queue_full`). |
| The worker stops | Stage P writes what it can in the time left, one attempt per COPY and no upkeep. The rest is dropped (`shutdown`). |

A final COPY error is not counted by the breaker: a healthy server that
rejects two bad chunks does not open it. Any other failed COPY, start or
upkeep counts as usual.

The COPY transaction sets `statement_timeout` to `0` for itself, so a limit
set for the role or the database never cuts a COPY; the attempt time limit
of `ui.recorder.postgres.resilience.timeout_seconds` bounds it.

When Postgres comes back, the probe succeeds and the queue drains chunk after
chunk in one run. At the defaults the queue holds about 10 seconds of a
worker at 5000 rows per second, and many minutes of a quiet worker. A crash
of the worker loses the queue.

Metrics and log events of the write path:

| Name | Kind | Meaning |
|---|---|---|
| `drakkar_recorder_postgres_queue_rows` | gauge | Rows waiting to be written to the recorder Postgres table (the queue plus the chunk in the slot) |
| `drakkar_recorder_postgres_rows_written_total` | counter | Rows written to the recorder Postgres table |
| `drakkar_recorder_postgres_dropped_rows_total` | counter, `reason` | Rows that never reached the recorder Postgres table, by reason: `queue_full`, `rejected`, `no_partition` (the last error was `23514`), `shutdown` |
| `drakkar_provider_calls_total`, `drakkar_provider_call_duration_seconds`, `drakkar_provider_breaker_open` | provider metrics | with `feature="recorder"`, `provider="postgres"`, `op` = `start`, `copy`, `partitions` |
| `recorder_postgres_rows_dropped` | warning | `reason`, `rows`, `error` (empty for `queue_full` and `shutdown`); at most one line per 60 s per reason |
| `recorder_postgres_copy_rejected` | warning | `sqlstate`, `error`, `rows`, `final_failures`; at most one line per 60 s |

See [Observability](observability.md#recorder-history-in-postgres) for every
line and field.

## The history search

`GET /api/v1/debug/trace?partition=&offset=&history=1` on a `postgres` worker
reads the events table of the worker's own prefix, rows of the worker's own
cluster only. It never reads the history folder.

- **Complete or 503.** The answer is complete; no file budget cuts it. A
  failed or timed-out search answers `503 {"detail": "history search
  failed"}` and logs `ui_read_postgres_failed`. The UI then shows "(the
  history search failed)" instead of "No events found". Every other read
  keeps its usual empty answer on failure.
- **Time limit.** `ui.trace.history_max_seconds` bounds the whole search,
  connect included, on the worker and on the server (`statement_timeout`),
  so the server stops its work when the worker gives up. The server limit is
  at most 3600 seconds; a larger or non-finite value uses 3600.
- **At most two at once.** One worker runs at most two history searches at
  the same time. A third answers the same 503 at once and logs
  `ui_read_postgres_failed` with `reason=busy`.
- **One connection per search.** Each search opens one connection, runs one
  attempt (no retry, no breaker) with JIT off, and closes the connection. A
  complete search whose close fails still answers 200 with its rows and logs
  `reason=close`.
- **Rows.** The rows have the same shape as rows of sealed files: the 20 event
  columns plus `worker_name`, in time order. A history row lags the memory
  store by at most one tick plus one COPY.
- **Stable task ids.** Task ids are stable for the same content. So the trace
  joins tasks of the same content across the whole retention of the cluster,
  where a sealed file joined only inside itself.

The search has no filter on `ts`, so it plans and locks every partition, about 10 at
the default retention and about 172 at 168 hours. A long search holds its
locks, and an upkeep that waits for them holds every COPY of that table for
at most 3 seconds.

## A full example

A worker with no disk, its history in Postgres, the busy cluster on its own
prefix with 3 hours of history:

```yaml
worker_name: worker-1
cluster_name: payments

sources:
  kafka:
    enabled: true

storage:
  dir: ""                       # no disk: no cache file, no crash marker

connections:
  postgres:
    main:
      schema: drakkar           # the DSN comes from DK_CONNECTIONS__POSTGRES__MAIN__DSN
      manage_schema: true

ui:
  recorder:
    persistence: postgres
    postgres:
      connection: main
      table_prefix: heavy       # drakkar.heavy_recorder_events
      retention_hours: 3        # written into the comment when this worker creates the table
      store_output: false
  trace:
    history_max_seconds: 10
```

```bash
export DK_CONNECTIONS__POSTGRES__MAIN__DSN='postgresql://drakkar:secret@db:5432/drakkar'
```

The summary line shows `storage=mem recorder=postgres`, and `recorder_started`
names `postgres_connection=main` and `postgres_table=drakkar.heavy_recorder_events`.

To create the table as an administrator instead:

```bash
drakkar-schema postgres --schema=drakkar --recorder --table-prefix=heavy --retention-hours=3 | psql "$ADMIN_DSN"
```

The [config wizard](calculator.md) prints a starting config for this mode
when you answer "Postgres" to "Where should the long event history live?".
