# The Databases View

The **Databases** tab of the Debug page shows where a worker keeps its data
and lets you take data out of it: the recorder files and the memory store,
the own folder, the shared history of the cluster, the Postgres table of the
recorder, and the state of the registry and the cluster task leases. From
it you download whole files, or a **slice**: the recorder events of one time
range, filtered, built by the worker that holds them, also from another
worker and also for events that are not sealed yet.

This page describes the view. The routes it reads, their parameters and
their answers are in [The Databases page](local-databases.md#the-databases-page)
and [Slice download](local-databases.md#slice-download).

## Layout

The tab has, from top to bottom:

- the **mode strip**, one line that says what the serving worker runs, for
  example
  `w-2 · cluster search · recorder sqlite (memory 2m, rotation 1h) · archive: cluster scope, next 11:00 · autodiscover consul`:
  the worker, its cluster, the recorder provider with the memory window and
  the rotation interval (or the Postgres table), the archive scope and the
  next archive slot in your browser's time, and the autodiscover provider;
- the **view buttons**: **Files**, **Postgres**, **Slice** and
  **Autodiscover** (see [The four views](#the-four-views));
- **Refresh** and, with autodiscover on, the **worker picker** (see
  [Other workers](#other-workers));
- the **coverage bar** (Files and Slice views);
- the view itself.

A worker of an older release that does not send the `worker` and
`recorder` blocks shows no mode strip; a part of the view whose data or
route the worker does not have is hidden, and for another worker the view
says "not available on this worker".

## The four views

| View | Shown when | What it shows |
|---|---|---|
| Files | always | the files and the memory store of each worker you picked, its own folder, the other worker folders on this host, and the shared history |
| Postgres | the serving worker's recorder provider is `postgres` | the recorder table, this worker's writer, and the hour partitions |
| Slice | always | the form that builds slice downloads |
| Autodiscover | autodiscover is on | the registry cards and the leases of the cluster tasks |

A link that asks for a view the worker cannot show (for example
`db_view=postgres` on a worker with provider `sqlite`) opens the Files view.

## The address

The view keeps its state in the query string, in keys that start with
`db_`, so a reload, the Back button and a pasted link show the same view.
The tab itself stays in the hash (`#databases`), and every other query key
(the `token`, the keys of other pages) is kept.

| Key | Values | Default |
|---|---|---|
| `db_view` | `files`, `postgres`, `slice`, `autodiscover` | `files` |
| `db_worker` | a worker name, or `*` for every worker of the cluster | the serving worker |
| `db_history_worker` | a worker name: the Files view shows only that worker's sealed files in the shared history | every worker |
| `db_range` | `5m`, `15m`, `1h`, `6h`, `24h` | `15m` |
| `db_from`, `db_to` | epoch seconds, whole numbers; both together replace `db_range` | none |
| `db_scope` | `worker`, `cluster` | `worker` |
| `db_sources` | comma list of `memory`, `files`, `history`, `postgres` | every source the worker has |
| `db_types` | comma list of event types | all |
| `db_partition`, `db_offsets` | a partition; `<from>-<to>` offsets | none |
| `db_label_key` | a label key | none |

A change of the view or of the worker adds a history entry, so Back
undoes it; every other change replaces the current entry. A value that is
missing or not valid (an unknown view, a bad number, a bad event type name)
gives the default. A key that holds the default is left out of the
address.

The task id and the label value of the Slice view are **not** kept in the
address: they can hold user data, and an address ends up in browser
history, logs and referrer headers. You type them on each visit.

## Other workers

On open the tab reads only the serving worker; it never calls another
worker by itself and nothing polls. The worker picker chooses what else to
read:

- **This worker** (the default): no other worker is read.
- **One worker by name** (`db_worker=w-3`): that worker is read.
- **All workers of the cluster** (`db_worker=*`): every other worker of the
  cluster is read. A pasted link with `*` reads them once, on open.

Each other worker is asked only for its own part
(`/api/v1/debug/databases?parts=own,storage,recorder`); the shared history
is read once, from the serving worker. At most 6 workers are read at once,
each with a 5 second limit. A worker that is offline or has its UI off is
not read; it is named under the list with the reason, as is a worker that
does not answer or runs an older release. Downloads from another worker's rows go to
that worker's own address. **Refresh** reads again, the workers list
included.

Reading another worker is a cross-origin request. With `ui.auth_token` set,
list the origin of every worker in `ui.allowed_origins`, or the other
workers show as not answered (see [Cross-origin reads](security.md#cross-origin-reads)).

## The coverage bar

One time axis, one lane per source of the worker you picked: **Memory**,
**Live file**, **Backlog**, **Sealed** (its own sealed files in the history
folder) and **Archives** (the windows of the archives in the history
folder, read from their names). Each lane shows the spans that source
covers, so you see which source holds a time range. A source the worker
does not have has no lane.

Drag across the lanes to choose a range. The range goes into `db_from` and
`db_to`; from the Files view the drag also opens the Slice view.

## The Files view

**Own files of each worker.** One section per worker you picked:

- the **memory row**, "Recent events in memory; not a file", when the
  worker has a [memory store](recent-events.md): events, first and last
  event time, data size; a snapshot download and a gzip download;
- the **live file**, "Being written now": file name, events, first and
  last event time, size; a snapshot download and a gzip download;
- **backlog files**, "Closed; waiting to be sealed into the history
  folder": the leftover pass seals them; no action.

A worker with no own file says so.

**Own folder** (`<cluster>/<worker>` under `storage.dir`): the cache file
(size with its `-wal` and `-shm` files, entries, last change, a snapshot
download and a gzip download), the crash marker, the owner lock file, and
the count of download temp files. With `storage.dir` empty the section says
that the worker keeps no files of its own. See
[The own folder](local-databases.md#the-own-folder).

**Other folders on this host.** The other worker folders of
`<storage.dir>/<cluster>/`: worker name, files, size, newest change, the
lock state (held by a running process, free, or no lock file) and the
registry state (online, offline, or not known). The view only reads them.
A note says when such a folder goes away: the
[orphan pass](local-databases.md#orphan-folders) of any worker of the
cluster seals and removes a folder whose lock is free, whose files are
idle for 5 minutes, and whose worker is not online. With autodiscover off
nothing proves a worker is not online, so the pass only logs such a folder
and the folder stays. Testing the lock takes
a shared lock for a moment; an orphan pass that looks at the folder at that
moment skips it for one round.

**History.** One block per `history/<cluster>/` folder, then `merged/`.
A line above the list says when files leave (from `recorder.archive`:
when archives and merge results are deleted). Rows: sealed files (worker,
events from the meta file, first and last time, size), archives (window,
size) and merge results. A worker filter narrows the list. Actions:

- **Download**, and **↓gz** for a gzip stream of a sealed file or a merge
  result (an archive is already gzip);
- select two or more sealed files or merge results and **Merge** them into
  one file in `merged/`, then download it from the banner;
- **Delete** a merge result, only when `ui.delete_enabled` is true. The
  view asks for a confirmation that names the file.

## The Slice view

A form that builds slice downloads. The fields:

| Field | Address key | Notes |
|---|---|---|
| Range | `db_range`, or `db_from` and `db_to` | presets `5m` to `24h`, or **custom** with from and to; at most 7 days |
| Scope | `db_scope` | this worker (each worker, with all workers picked), or the whole cluster |
| Sources | `db_sources` | only the sources the worker has; not offered for the whole cluster |
| Event types | `db_types` | none checked means all |
| Partition and offsets | `db_partition`, `db_offsets` | offsets as `<from>-<to>`, for example `100-200` |
| Task id | not kept | one task |
| Label key and value | `db_label_key`; the value is not kept | one [label](handler.md#task-labels) value |
| Include stdout and stderr | not kept | off leaves them out of the file (`NULL`) |
| Compress (gzip) | not kept | the file comes as `.db.gz` |
| Max rows | not kept | 1 to 1000000, 200000 when empty |

Above the form a line says which source covers which part of the range,
for example "memory covers 10:58 to 11:00; live file 10:00 to 11:00". Below
it the view lists the limits, previews the file name (`-partial` is added
when a limit ends the build), and checks every field before it builds a
link, so a link the worker would refuse is not offered.

**Download** is a plain link that opens in a new tab: a finished download
leaves the tab empty, and an error answer stays readable there. A range
such as "last 15 minutes" is counted back from the moment of the click. The
worker builds the file first and sends it when it is ready, within at most
60 seconds; a banner says so meanwhile.

**Which links you get.** One slice is built by one worker, and no worker
calls another worker for a slice:

| Recorder provider | This worker | The whole cluster |
|---|---|---|
| `sqlite` | one link | one link per worker for its unsealed part (`scope=worker&sources=files` on that worker), listed first, then one link for the cluster file, which holds the sealed history of the cluster only. A file sealed between two downloads is then in both, never in none. |
| `postgres` | one link | one link: the table rows of the cluster, every worker. The newest seconds may be missing: rows reach the table within about 5 seconds. |
| recorder off | one link (the memory store) | not offered: there is no shared source. With all workers picked, one link per worker for its memory store. |

Each link is a separate click: a browser blocks many downloads started by
one click, and the work on the cluster follows your clicks.

**Limits.** A limit never fails a download: the worker ends the file with
the rows it has, marks it (`-partial` in the name, the header
`X-Drakkar-Slice-Truncated`, and the `slice` table in the file) and sends
it.

| Limit | Value |
|---|---|
| Range | 7 days |
| Rows | `max_rows`, 200000 by default, at most 1000000 |
| File size | 1 GiB, or 64 MiB for a worker without `storage.dir` (built in RAM) |
| Build time | 60 seconds |
| Sealed files | `recorder.search.max_files`, newest first |
| Builds at once | one per worker; a second request answers `429` |
| Slices still being sent | 2 GiB of finished slices, and 16 slices being built or sent, per worker; past either a request answers `429` |

Archives are never read by a slice: download the archive itself. What each
source holds, the tables of the file and where it is built are in
[Slice download](local-databases.md#slice-download).

## The Postgres view

Shown when the serving worker's recorder writes to a Postgres table (see
[Recorder History in Postgres](recorder-postgres.md)):

- **Table**: the table with its schema, the connection name, the retention
  a new table gets (`initial_retention`), `max_size` and whether completed
  output is stored. The DSN is never shown.
- **Writer of this worker**: whether it started, the rows waiting and
  written, the rows dropped by reason, and its last upkeep (when, whether it
  held the upkeep lock, the partitions it created and dropped).
- **Partitions**: one row per hour partition with its hour and its size,
  the stored retention against `initial_retention`, the total size against
  `max_size`, and which partitions the next upkeep drops, by retention or
  by size.

The table and the writer come from values the worker keeps in memory. The
partitions are read from the table when you open the view and when you
press **Refresh**, with one connection of their own; nothing polls them.
See [The Databases tab](recorder-postgres.md#the-databases-tab) and
[The partitions of the table](recorder-postgres.md#the-partitions-of-the-table).

The view has no Postgres action: retention, `max_size` and the partitions
belong to the upkeep and the database administrator.

## The Autodiscover view

Shown while autodiscover is on (see [Fleet registry](fleet.md)):

- **Workers**: one row per registry card of the last refresh, the own
  cluster first: worker, instance, state, backend, URL, start, heartbeat,
  age, liveness, the cluster task hash, the features and the store key of
  the card. A header line gives the provider, the connection name (never a
  DSN, URL or token), the last refresh and publish, and whether the breaker
  is open.
- **Leases**: one row per cluster task of this worker, `history.archive`
  included when it runs as one: the holder as the store reports it, when it
  acquired the lease, how long ago it refreshed it, the lease length, the
  time left, the next slot, the outcome of this worker's last slot, and the
  store key.

The workers come from the worker's memory. The leases are read from the
store when you open the view and when you press **Refresh**; a read never
takes, refreshes or frees a lease, and never counts against the election's
breaker. See [The Autodiscover view](fleet.md#autodiscover-view).

## What the view can change

| Action | Key | Default |
|---|---|---|
| Download a sealed file, an archive, a merge result, a snapshot of the live file, of the memory store or of the cache file, or a slice | behind `ui.auth_token` like every debug route | on |
| Merge sealed files or merge results into `merged/` | `ui.merge_enabled` | on |
| Delete one merge result | `ui.delete_enabled` | **off** |

Everything else is read-only on purpose:

| Not offered | Why |
|---|---|
| Delete a sealed file | The archive pass is the only thing that removes sealed files, and it may be reading the file. |
| Delete an archive | It is the only copy of old history; `recorder.archive.retention` removes archives. |
| Run the archive pass now | The pass runs in the slots of the cluster task `history.archive`; a run outside a slot could run beside the slot's winner. The mode strip shows the next slot; a shorter `recorder.archive.interval` runs it more often. |
| Seal a backlog file now | The leftover pass seals it within 300 seconds. |
| Delete another worker's folder | The orphan pass does it after three checks (lock, idle, not online); a delete from the UI could remove a live worker's files. |
| Release a task lock | A lease held at shutdown is never released early; the slot protocol depends on it. |
| Drop a Postgres partition or change the retention | The upkeep owns the partitions; retention and `max_size` are settings of the table. |

A download sends recorder data (`args`, `stdout`, `stderr` and labels
included) to anyone who reaches the UI; see
[Security](security.md#what-drakkar-does-provide).

## What cleans itself

| What | Removed by | When | Shown where |
|---|---|---|---|
| Sealed files | the archive pass, which folds them into an archive | windows older than `recorder.archive.due_after`, at the next slot | history line, mode strip |
| Archives | the archive pass | older than `recorder.archive.retention` | history line |
| Merge results | the archive pass | older than `recorder.archive.merged_retention` | history line |
| Download temp files of the own folder | the start of the worker | at start | own folder (count) |
| Orphan worker folders | the orphan pass | lock free, files idle 5 minutes, worker not online | other folders on this host |
| Postgres partitions | the upkeep | past the stored retention, or above `max_size` | Postgres view |

## The view per mode

| Recorder | `storage.dir` | Files view | Postgres view | Slice sources (this worker) | Whole cluster |
|---|---|---|---|---|---|
| `sqlite` | set | memory, live and backlog files, own folder, other folders, history | hidden | memory, files, history | sealed history of the cluster plus one link per worker |
| `postgres` | set | memory, own folder, other folders, older history if any | shown | memory, postgres | the table |
| `postgres` | empty | memory only | shown | memory, postgres (built in RAM) | the table (built in RAM) |
| off, memory store open | set or empty | memory, own folder when `storage.dir` is set | hidden | memory | not offered |
| off, `recorder.capture.events: false` | any | own folder only | hidden | none: the view says there is nothing to slice | not offered |

`sqlite` with an empty `storage.dir` fails the config load.

## The routes behind the view

| View | Route |
|---|---|
| all | `GET /api/v1/debug/databases` with `?parts=` ([reference](local-databases.md#the-databases-page)) |
| Files | `GET /api/v1/debug/download/{path}`, `GET /api/v1/debug/download-live`, `POST /api/v1/debug/merge`, `DELETE /api/v1/debug/files/{path}` |
| Slice | `GET /api/v1/debug/slice` ([reference](local-databases.md#slice-download)) |
| Postgres | `GET /api/v1/debug/databases/postgres` ([reference](recorder-postgres.md#the-partitions-of-the-table)) |
| Autodiscover | `GET /api/v1/debug/autodiscover`, `GET /api/v1/debug/autodiscover/leases` ([reference](fleet.md#autodiscover-view)) |
