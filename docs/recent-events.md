# Recent Events in Memory

A worker with the UI on keeps its recent recorder events in an in-memory
SQLite database, the **memory store**. The UI pages read their recent rows
from it. A worker can also keep a recorder file on disk (provider `sqlite`),
send its events to a table in Postgres (provider `postgres`), or keep nothing
else at all (`recorder.enabled: false`). This page tells you what the memory
store holds, how to size it, how much RAM to plan for it, and what each page
shows in each case.

The memory store exists when `ui.enabled` and `recorder.capture.events` are
both on (the defaults). With `capture.events: false` there is no memory store
and no event rows anywhere; the live WebSocket view still streams. A worker
with `ui.enabled: false` has no memory store either: it inserts nothing in
memory, and the memory gauges stay `0`.

## The recorder cases

`recorder.enabled` and `recorder.provider` select where the recorder keeps
events besides the memory store:

| | `enabled: false` | `sqlite` | `postgres` |
|---|---|---|---|
| Memory store (with `ui.enabled`) | yes | yes | yes |
| Recorder file in `storage.dir` | no | yes | no |
| Rotation, sealing, publish into the history folder | no | yes | no |
| `history.archive` pass | does not run | runs | does not run |
| Rows in a Postgres table, partition upkeep | no | no | yes |
| Trace with history (`history=1`) | the memory rows only | sealed files | the Postgres table, rows of the own cluster |
| Merge, file downloads, archives list | yes, when a shared folder resolves | yes | yes, when a shared folder resolves |
| `worker_state` rows | not written | written to the file only | not written |
| `worker_config` row | in memory | in memory and in the file | in memory |
| UI reads | memory | memory for recent rows, the file for the rest (see below) | memory |
| Last-breath flush at exit | nothing | into the file | nothing |
| `storage.dir` | optional (cache file, crash marker, snapshot temp file) | required | optional |

The `postgres` provider has its own page: [Recorder History in Postgres](recorder-postgres.md).

`recorder.enabled` defaults to `true` and `provider` to `sqlite`, and nothing is
derived from `storage.dir`. A worker with an empty `storage.dir`, the recorder
on and provider `sqlite` fails the load with this message:

```
recorder.provider is sqlite but storage.dir is empty; set storage.dir to a folder, or set recorder.enabled to false
```

So a worker that keeps its recent events in memory only, for example in a
container with no writable disk, sets `recorder.enabled: false`.

`storage.dir` keeps one meaning: the folder of this worker's own files. With
`recorder.enabled: false` and a folder set, the cache file and the crash marker
stay in that folder, and the recorder writes no recorder file there. The config
summary line then shows the token ` recorder=off` after ` storage=...` (and
the optional ` shared=...`).

The shared folder (`storage.shared_dir`, else `storage.dir`) does not depend on
the provider. A worker with `recorder.enabled: false` and a shared folder still
reads nothing of the sealed history in a trace (the search answers the memory
rows), but it merges files and downloads archives. It publishes nothing into
the history folder.

```yaml
recorder:
  enabled: false             # keep recent events in memory only
  memory:
    window: 2m               # keep events this long
    rows_per_executor: 3000  # row cap = rows_per_executor x executor.max_executors
    max_size: 256MiB         # data budget of the store
```

## The three bounds

The worker removes the oldest rows of the memory store after each write to
it, and on every memory tick when the worker is idle. It applies three bounds,
in this order:

1. **Time window**, `recorder.memory.window` (default `2m`, from `10s`
   to `24h`). Rows older than the window go.
2. **Row cap**, `recorder.memory.rows_per_executor` times
   `executor.max_executors` (default 3000 per executor, from 100 to 1000000).
   The oldest rows above the cap go.
3. **Data budget**, `recorder.memory.max_size` (default `256MiB`, from `16MiB` to
   `512MiB`). Rows go until the data in use (pages in use times the page size) fits
   the budget.

The worker deletes at most 5000 rows per transaction. The byte step stops
after 64 delete batches in one pass, and the next pass goes on. A read from
the UI starts a pass at most once per second.

The budget bounds the data, not the RAM. The SQLite in-memory database refuses
to grow past 1 GiB, so the budget stops at 512 MiB: 512 MiB, plus one write
stage, plus free pages, stays under that limit.

`window` is a whole number of seconds (`2m`, `90s`); `rows_per_executor` is a whole number; `max_size` is a size.

### Sizing the row cap

The row cap does not scale with the window. The rule of thumb counts about
four rows per task:

> rows per executor per window is about 4 x window / average task seconds

With the default window of `2m`, the default of 3000 fits tasks of
0.16 seconds (4 x 120 / 0.16 = 3000). With faster tasks, the row cap binds
first, and the store covers less than the window. For example, with 0.05
second tasks the default cap covers about 37 seconds. A longer window needs a
larger `rows_per_executor` in the same proportion.

The gauge `drakkar_recorder_memory_oldest_age_seconds` shows how much time the
store really covers.

## RAM to plan

The SQLite in-memory database grows its buffer by doubling and never gives
memory back. So per worker:

- the store buffer reaches up to about twice the largest database size it
  ever had, and never more than 1 GiB;
- the rollback pages of the open write transaction are in memory too;
- one growth step holds the old and the new buffer for a short time;
- the intake buffer and, with a durable store, its queue each hold up to
  `recorder.max_buffer_rows` rows, outputs included.

With the default budget of 256 MiB, plan up to about 600 MiB for the store.

Two gauges show the store size:

- `drakkar_recorder_memory_bytes` is the data in use (pages in use times the
  page size). Eviction keeps it near the budget.
- `drakkar_recorder_memory_allocated_bytes` is the database size, free pages
  included. The memory the store holds is at least this.

The process RSS is larger than the allocated size: the buffer can be up to
twice the largest size the database had, and the heap of the process holds the
rollback pages and the Python objects of the buffered rows.

## What reads memory and what reads the file

With the recorder off and with provider `postgres`, every UI read uses the
memory store. A page that asks for an older range gets a shorter answer. With
`postgres`, only the trace with history reads the Postgres table.

With `sqlite`, each read picks one source, memory or the live file. It never
merges the two:

| Read | Examples | Source with `sqlite` |
|---|---|---|
| Time window (`since`) | `/api/v1/recent-tasks` (the timeline), `/api/v1/timeline/events`, page tiles (60 s) | memory when the store still holds the whole range, else the file |
| Newest N rows | `/api/v1/events` without `after_id`, `/api/v1/live/window-results`, `/api/v1/live/task-results`, `/api/v1/live/message-results`, `/api/v1/debug/periodic` | memory when the store returns all N rows or has evicted nothing, else the file |
| Poll | `/api/v1/events` with `after_id` | memory when the store still holds every row after `after_id`, else the file |
| By key | `/api/v1/task/{id}`, `/api/v1/debug/trace`, `/api/v1/debug/trace-by-label`, `/api/v1/live/arrange-tasks`, `/api/v1/live/sink-breakdown` | the file |
| Whole table | `/api/v1/partitions`, the event pages | the file, without a flush |
| Label keys | `/api/v1/debug/label-keys` | always memory |

The timeline reads up to `ui.timeline.max_age` (default 60 minutes).
With the default window of `2m`, the default timeline reads the file.

A page with two queries reads both from the same source. A memory read has a
time limit of 2 seconds. A read stopped by the limit answers an empty page and
logs `ui_read_memory_timed_out`.

### What each page shows with the recorder off or on `postgres`

With the recorder off or on `postgres`, the pages cover the memory window only. The UI does not label it.

- The **timeline** shows the tasks of the window, not the last hour.
- The **partition counts** count the rows of the window.
- **`/debug/periodic`** totals count only the runs inside the window.
- **Task detail** shows only the rows of the task that are still in the
  window. A task whose older rows left the window shows its newer rows only.
- **Trace** finds the rows of the window. With `history=1`, a `sqlite` worker
  adds the sealed history of the cluster, and a `postgres` worker adds the rows
  of the cluster from the Postgres table. With the recorder off, `history=1`
  answers the memory rows only.
  **Trace by label** finds the rows of the window.

Rare events leave with the window too. An event that happens once an hour is
in the store for two minutes with the defaults. Use a `sqlite` or a `postgres`
worker, or the sealed history of other workers, to keep it longer.

## Row ids

The recorder gives each row its id. An id is unique and increases within one
worker process. It continues across file rotations and starts again at 1
after a restart. With `sqlite`, the row in the memory store and the row in the
file have the same id. With `postgres`, the row in the table has the same id.

## The snapshot download

`GET /api/v1/debug/download-live` takes `source=memory`, `source=file` or
`source=cache`. The default is `file` when a live file is open, else
`memory`. The Databases tab shows a download button on the memory row, on
the live row and on the cache file row, and a second button for the same
copy gzip-compressed (`compress=gzip`).

The memory snapshot is a recorder database of the same shape as a file,
named `<worker>-memory-<YYYY-MM-DD__HH_MM_SS>.db` (UTC time of the snapshot).

- With `storage.dir` set, the worker copies the store in steps of 1024 pages
  into a temporary file there. The file is owner-only and removed at once.
  The read lock is released between steps. A write between steps starts the
  copy again. A copy that does not finish in 30 seconds answers 500.
- With `storage.dir` empty, or when no temporary file can be made there, the
  worker copies the store in RAM. The download then holds one more copy of the
  store in memory, up to its used size plus free pages, while it runs.
  The worker logs `debug_download_live_in_ram` (info, with `reason`
  `storage_dir_unset` or `storage_dir_unusable`) for each such copy.

One memory snapshot runs at a time. Another memory download waits for it, up
to 30 seconds, and then answers 500 with the reason `another memory snapshot
is still running`. Parallel downloads therefore never make two copies at the
same time. A cache copy has a slot of its own, so it never makes a memory
download wait. A finished copy stays in memory until its download ends.

Answers:

| Case | Answer |
|---|---|
| `source=file` and no live file | `404 {"error": "No live recorder file on this worker"}` |
| `source=memory` and no open memory store | `404 {"error": "No memory store on this worker"}` |
| any other `source` value | `400 {"error": "source must be memory or file"}` |
| a failed copy | `500 {"error": "Snapshot failed: <reason>"}` |

The route is behind `ui.auth_token` like the other debug routes. See
[Security](security.md).

## Metrics

| Metric | Type | Meaning |
|---|---|---|
| `drakkar_recorder_memory_rows` | gauge | Rows in the events table of the recorder memory store |
| `drakkar_recorder_memory_bytes` | gauge | Bytes of data in the recorder memory store: pages in use times the page size |
| `drakkar_recorder_memory_allocated_bytes` | gauge | Database size of the recorder memory store, free pages included; the memory it holds is at least this |
| `drakkar_recorder_memory_oldest_age_seconds` | gauge | Age in seconds of the oldest event in the recorder memory store, 0 when it is empty |
| `drakkar_recorder_memory_evicted_rows_total` | counter, `reason` = `window`, `rows`, `bytes`, `gap` | Rows removed from the recorder memory store, by reason |
| `drakkar_recorder_memory_write_failures_total` | counter | Event chunks that could not be written to the recorder memory store |

The gauges are set after each eviction pass. After a failed pass only
`drakkar_recorder_memory_rows` is set; the byte, allocated and age gauges keep
the values of the last pass that measured them.

## Log events

Each line below appears at most once per 60 seconds and carries `suppressed`,
the number of lines left out since the last one.

| Event | Level | Fields | When |
|---|---|---|---|
| `recorder_memory_write_retry` | warning | `error`, `batch_size` | The store was busy or locked. The chunk goes back to the intake and is written on the next tick. Nothing is lost. |
| `recorder_memory_write_failed` | error | `error`, `batch_size` | Any other failed memory write. The worker deletes the memory rows up to that chunk. With provider `sqlite` the chunk still goes to the file; with the recorder off it is lost. |
| `recorder_memory_eviction_failed` | warning | `error`, `reason` (`window`, `rows`, `bytes`, `gap`, `floor`), `rollback_error` when the rollback failed too | An eviction pass or a gap delete failed. The next pass starts again. |
| `recorder_memory_rollback_failed` | warning | `error` | The rollback after a failed memory write failed. The next write rolls back first. |
| `recorder_memory_config_write_failed` | warning | `error` | The `worker_config` row could not be written to the store. The file write goes on. |
| `ui_read_memory_timed_out` (category `debug`) | warning | `endpoint`, `limit_seconds` | A memory read stopped at its 2 second limit. |

`recorder_started` carries `persistence` (`none` with the recorder off, else the
provider), `memory_window_seconds`, `memory_max_rows` and `memory_max_bytes`
(the last three only with a memory store).

See [Observability](observability.md#flight-recorder) for the recorder metrics
and the write stages, and [Local Databases](local-databases.md) for the files.
