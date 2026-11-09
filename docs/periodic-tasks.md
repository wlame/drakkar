# Periodic Tasks

A periodic task is an `async` method of your handler that the worker runs on
a schedule, next to the message pipeline. The schedule is an interval in
seconds or a cron expression in UTC. The **scope** says where the task runs:

- **`worker`** (the default): every worker runs the task. Use it for work
  that belongs to one worker, such as reloading a lookup table into memory.
- **`cluster`**: at each scheduled time (a **slot**), exactly one worker of
  the cluster runs the task. Use it for work that must happen once, such as
  refreshing a shared table, sending a daily report, or cleaning a bucket.

```python
import os

from drakkar import BaseDrakkarHandler, periodic


class MyHandler(BaseDrakkarHandler[MyInput, MyOutput]):
    @periodic(60)
    async def reload_country_lookup_table(self):
        self.country_lookup = await load_country_lookup()

    @periodic(cron='*/10 * * * *', scope='cluster')
    async def refresh_shared_exchange_rates(self):
        await copy_exchange_rates_into_reporting_table()

    @periodic(cron='0 6 * * MON-FRI', scope='cluster', lease_seconds=300)
    async def send_weekday_morning_report(self):
        await send_report_to_operations_mailbox()

    @periodic(30, on_error='stop')
    async def check_corpus_directory_exists(self):
        if not os.path.isdir('/data/corpus'):
            raise RuntimeError('Corpus directory missing')
```

A cluster task needs an [election provider](#the-election) that every worker
of the cluster can reach. The default provider uses the shared folder.

## The decorator

```python
periodic(
    interval: float | None = None,       # seconds; the first positional argument
    *,
    cron: str | None = None,             # five-field cron expression, UTC
    scope: 'worker' | 'cluster' = 'worker',
    lease_seconds: int | None = None,    # cluster tasks only
    on_error: 'continue' | 'stop' = 'continue',
)
```

Give exactly one of `interval` and `cron`. `@periodic(30)` and
`@periodic(interval=30)` are the same.

The decorator checks the declaration when Python applies it, that is when
the handler module is imported. A broken declaration raises
`PeriodicRuleError` (a `ValueError`) whose message names the method:

| Rule | Message (after `periodic task '<name>': `) |
|---|---|
| The method name is 1 to 64 characters of `a-z`, `0-9` and `_`, and does not start with a digit | `the name must be 1 to 64 characters of a-z, 0-9 and _, and must not start with a digit` |
| The scope is `worker` or `cluster` | `scope must be worker or cluster` |
| Exactly one schedule | `give exactly one of interval and cron` |
| A worker task's interval is greater than 0 (a float is fine) | `interval must be greater than 0` |
| A cluster task's interval is a whole number of seconds, at least 30 | `a cluster task's interval must be a whole number of seconds and at least 30` |
| A cron expression is in the [accepted subset](#cron) | one of the [cron messages](#cron-messages) |
| `lease_seconds` is given only for a cluster task | `lease_seconds is only for cluster tasks` |
| `lease_seconds` is a whole number from 40 to 86400 | `lease_seconds must be a whole number from 40 to 86400` |

The rules are checked in the order of the table. Decorating a function that
is not `async` raises `TypeError`.

## Schedules

Every time is the worker's own wall clock in UTC, cut to the whole second.

| Scope | `interval` | `cron` |
|---|---|---|
| `worker` | Sleep `interval`, run, sleep again from the end of the run. The workers are not aligned with each other, so a fleet does not call a shared service in the same second. | Sleep until the next fire time after now, run, then compute the next fire time from the end of the run. A fire time that passes during a run is skipped. |
| `cluster` | Slots at whole multiples of `interval` since 1970-01-01T00:00:00Z: the next slot after `now` is `(floor(now) // interval + 1) * interval`. Every worker computes the same slots. | Slots at the fire times of the expression. The next slot is the first fire time strictly after `floor(now)`. |

A slot is named by its epoch second. With `interval: 600` the slots are at
00:00, 00:10, 00:20 UTC and so on, whatever time the workers started.

A worker sleeps until the slot by its own clock. When the sleep ends early,
it sleeps again, so a slot never runs before its time. A slot never runs
twice on one worker, also when the clock steps back. Runs of one task never
overlap on one worker.

### Cron

A cron expression has five fields, in UTC: minute, hour, day of month, month
and day of week. Only one subset of cron is accepted. Inside it, cron
libraries agree on every fire time. Outside it, they read the same text in
different ways, so a fleet whose workers read one expression differently
would run a task at different times.

| Field | Values | Names |
|---|---|---|
| minute | `0`-`59` | |
| hour | `0`-`23` | |
| day of month | `1`-`31` | |
| month | `1`-`12` | `JAN`-`DEC`, any letter case |
| day of week | `0`-`6` (`0` is Sunday) | `SUN`-`SAT`, any letter case |

- The fields are separated by exactly one space: no space at the start or at
  the end, no double space, no tab, no line break. A tab or a line break is
  not a separator: it becomes part of a field, and the message depends on
  the field count that is left (see [Cron messages](#cron-messages)).
- Each field is a comma list. Each element is one of `*`, `*/s`, `v`,
  `a-b` (with `a` lower than `b`) or `a-b/s`.
- `v`, `a` and `b` are decimal numbers without a sign or a leading zero, or a
  three-letter name (month and day of week only).
- A step `s` is from 1 to the field's maximum minus its minimum (59 for the
  minute, 23 for the hour, 30 for the day of month, 11 for the month, 6 for
  the day of week).
- The day of month and the day of week cannot both be restricted: at least
  one of them is `*`.
- The expression must fire at least once: `0 0 30 2 *` (30 February) is
  refused.

Refused: a seconds or a year field, `@hourly` and the other `@` macros, `L`,
`W`, `#`, `?`, `H`, `R`, a range that wraps (`22-2`), a range of one value
(`5-5`, `5-5/2`), a step on a single value (`5/10`), an empty element
(`1,,2`), full names (`MONDAY`), `7` for Sunday, and a time-zone prefix.

Examples:

| Expression | Fires |
|---|---|
| `*/15 * * * *` | every 15 minutes |
| `0 */2 * * *` | every 2 hours, on the hour |
| `30 6 * * MON-FRI` | 06:30 UTC on weekdays |
| `0 0 1 * *` | 00:00 UTC on the first day of each month |
| `0 3 * JAN,JUL *` | 03:00 UTC every day of January and July |

The check of the subset is a grammar check written into the framework. A
cron library, [cronsim](https://pypi.org/project/cronsim/), computes the
fire times. Its version is pinned exactly (`cronsim==2.7`), because the
accepted expressions, the messages and a set of fire times (month ends,
29 February, the year end) are pinned by a test fixture. A library release
that computed one fire time differently would make workers of one cluster
disagree on their slots; with the exact pin, an upgrade that changes a
result fails a test instead.

#### Cron messages

The check stops at the first problem. It looks at the field count, then the
day rule, then each field from left to right.

| Problem | Message |
|---|---|
| A field count other than 5 when split on single spaces, or an empty field (a space at the start or the end, a double space) | `cron '<expr>' must have 5 fields separated by single spaces` |
| Day of month and day of week both restricted | `cron '<expr>' restricts both the day of month and the day of week; make one of them *` |
| An element of another shape, a tab or a line break inside a field included | `cron '<expr>': <field> element '<item>' is not one of *, */s, v, a-b, a-b/s` |
| A value out of range or an unknown name | `cron '<expr>': <field> value '<token>' is not one of <allowed>` |
| A range whose end is not above its start | `cron '<expr>': <field> range '<item>' must go from a lower to a higher value` |
| A step out of range | `cron '<expr>': <field> step '<step>' must be from 1 to <max>` |
| No fire time | `cron '<expr>' never fires` |

## Cluster tasks: the slot rule

Every worker runs one loop for each enabled cluster task. At each slot `S`:

1. The worker sleeps until `S` by its own clock.
2. When it wakes 10 seconds or more after `S` (the **acquire window** is
   10 seconds), it skips the slot as `late`. This happens when the event
   loop or the host was blocked through the slot.
3. When the election provider has not started yet, the worker starts it.
   A failed start skips the slot as `election_failed`.
4. The worker asks the provider for a **lease** on the task's scope
   `<cluster>/<task>` (for example `search_hot/refresh_shared_exchange_rates`;
   the cluster part is the sanitized cluster name). Steps 3 and 4 must end
   before `S + 10`. One worker gets the lease. The others skip the slot as
   `held_elsewhere`. An error or a timeout skips it as `election_failed`.
5. The worker with the lease runs the task. While the task runs, it refreshes
   the lease every third of `lease_seconds`, at most every 60 seconds.
6. After the run, the worker keeps the lease until at least `S + 20` (the
   **minimum hold** is 20 seconds), also when the run took one second. Then
   it releases the lease.
7. The next slot is computed from the current time. Slots that passed during
   a long run are skipped: there is no catch-up.

The window and the hold are fixed and the same on every worker, whatever the
task settings, so two workers with different settings for one task (in the
middle of a rolling change) still agree on them.

**Why one slot runs once.** A worker takes a lease only before `S + 10` by
its own clock, and the winner holds the lease until at least `S + 20` by its
own clock. A worker whose clock is behind by less than 10 seconds therefore
still finds the slot held. Keep the clocks of a cluster within 10 seconds of
each other (NTP does much better). With a larger difference a slot can run
twice.

**The lease.** `lease_seconds` (default 60, from 40 to 86400) is how long a
lease lives when nobody refreshes it. It matters only when the holder dies:
its lease ends `lease_seconds` after its last refresh, the slots in that
time are `held_elsewhere` for every worker, and the first slot after the
lease ended runs normally on another worker. The lease does not decide when
the next slot starts (the release does), so a lease longer than the interval
is fine. The floor of 40 seconds is twice the minimum hold: a lease must not
end by itself before `S + 20` when every refresh fails, and with the
`consul` provider the guaranteed life of a lease is its session's time to
live, which is half the lease (at least 10 seconds).

**A lost lease does not stop the run.** When a refresh answers that another
worker holds the scope, or the refresh fails, the worker logs
`periodic_lease_lost` and stops refreshing. The run goes on, because a
coroutine cannot be stopped safely in the middle. A later slot can then run
on another worker while the old run still works. Keep a cluster task shorter
than its interval, or make it safe to run twice. The archive pass is the one
task that checks its lease itself before each step that must not happen
twice ([fencing](local-databases.md#fencing)).

**Shutdown.** The loops of periodic tasks are cancelled at the start of the
shutdown. A lease held at that moment is not released: it ends by itself,
because an early release could let a worker whose clock is behind run the
slot again.

**The provider is down.** A cluster task never blocks the message pipeline.
When the provider does not answer, each slot is `election_failed` and the
worker goes on processing messages. The calls follow the
[`resilience` block](fleet.md#resilience) of `autodiscover`: retries,
and a circuit breaker that skips the provider for a cooldown after repeated
failures (those slots show `error: start skipped_open` or
`acquire skipped_open`). The first slot after the provider answers again
runs normally, with no restart.

## The election

One provider serves every cluster task of a worker, the archive included, and
the [worker registry](fleet.md). It is configured once, under `autodiscover`,
and it is off by default:

```yaml
storage:
  shared_dir: /mnt/drakkar-shared   # provider filesystem needs it
autodiscover:
  enabled: true
  provider: filesystem   # filesystem | postgres | redis | consul
  connection: main       # entry of connections.<provider>; read by postgres, redis and consul
  # resilience: {}       # time limit, retries and breaker
```

Every worker of a cluster must use the same provider. Where each provider
keeps the lock of task `<task>` of cluster `<cluster>`:

| Provider | Lock |
|---|---|
| `filesystem` | The file `<storage.shared_dir>/tasks/<cluster>/<task>.lock` |
| `postgres` | A row of the table `task_locks` (key column `scope`) in the schema of the connection |
| `redis` | The key `<state_prefix>task-locks:<cluster>/<task>` |
| `consul` | The key `<state_prefix>/task-locks/<cluster>/<task>`, held by a session named `drakkar/task-lock/<cluster>/<task>/<token>` |

The details of each provider are on
[Local databases](local-databases.md#the-election-provider).

The provider is made at startup only when the worker runs at least one
cluster task (an archive over a worker's [own files](local-databases.md#archive-scopes)
is not one), and it starts at the first slot that needs it, not at worker
start. Tasks whose first slots come in the same second start it once. It
stops at shutdown, after the recorder. Cluster tasks need no UI: a worker
with `ui.enabled: false` runs them too.

## Startup

The worker finds the handler's periodic tasks once at startup: after
`on_startup`, and before the recorder, the registry, the API server, the sinks
and the sources start. The loops start after `on_ready`, before the sources
take input.

Startup fails when a declared cluster task runs on a worker with `autodiscover`
off. The message names the first such task in sorted order:
`periodic task '<name>' has scope cluster, but autodiscover is off; set autodiscover.enabled and autodiscover.provider, or give the task scope worker`.

`on_startup` may change the `autodiscover` keys, so they are checked again after
it, with the messages of the config load. A provider that is configured but down
at startup is not a startup failure: its slots are `election_failed` until it
answers.

With `autodiscover` on and an empty cluster name, the worker logs
`cluster_name_empty` (warning; field `scope`, the value `default`) once at
startup: such workers share the lock scope `default`.

## Definition hash

Every worker computes a hash of the cluster tasks it runs: the enabled user
cluster tasks, and `history.archive` when the worker runs it
[over the whole cluster](local-databases.md#archive-scopes). Two workers of one cluster with different hashes run different
schedules or leases for their cluster tasks, which usually means a rolling
change that is not finished, or a config that differs by mistake.

- **The bytes.** A JSON array of one object per task, sorted by name:
  `{"name", "interval" or "cron", "lease_seconds"}`, with whole numbers as
  JSON integers and the cron expression as written. Keys are sorted, there
  are no spaces, and text is raw UTF-8.
- **The hash.** The first 16 hex characters of the SHA-256 of those bytes.
  A worker with no cluster task has the hash of `[]`, `4f53cda18c2baa0c`.

For example, a worker that runs only the archive with its defaults hashes
`[{"interval":3600,"lease_seconds":60,"name":"history.archive"}]`.

The worker publishes the hash in its [registry card](fleet.md#the-card) from
the first publish, as `features.periodic.cluster_hash`.

**The warning.** After each registry refresh the worker compares its hash
with the online peers of its own cluster. A peer whose card has no hash in
that form (an older release, or a malformed card) is unknown, not a
mismatch. For each peer with another hash the worker logs
`periodic_cluster_mismatch` (warning; fields `peer`, `own_hash`,
`peer_hash`) once per peer and hash. When the peer leaves the listing and
comes back with the same hash, the line is logged again. The gauge
`drakkar_periodic_cluster_mismatch_peers` counts the online peers with
another hash. The worker keeps running either way.

**The API.** Each row of `GET /api/v1/workers` has two more fields:

| Field | Value |
|---|---|
| `cluster_tasks_hash` | The worker's hash, or `null` when its card has none in the contract form. |
| `cluster_tasks_match` | `true` for the worker that answers. For a peer: `true` or `false` when both hashes are known and the peer is in the same cluster (also when the peer is offline), else `null`. |

`GET /api/v1/debug/periodic` gives each task group a `scope`: `cluster` when
its latest run was a cluster run, else `worker`.

## The archive: `history.archive`

The archive pass is a system task named `history.archive`. Its slots come
from `recorder.archive.interval` (default `1h`, a whole number of seconds and
at least 30) or `recorder.archive.cron`. A worker runs it when
`recorder.enabled` is true, `recorder.provider` is `sqlite`,
`recorder.archive.enabled` is true and `storage.dir` is set. Any other worker
does not run it and leaves `history.archive` out of its definition hash. It
still reads the sealed history of its cluster when a shared folder resolves.

The task has [two scopes](local-databases.md#archive-scopes):

- **The whole cluster**, with `autodiscover.enabled` and `storage.shared_dir`
  set. `history.archive` is a cluster task with the lease
  `recorder.archive.lease` (default `1m`). The worker that wins a slot runs
  the archive pass; every other worker updates the archive footprint gauges
  at that slot. The pass checks its lease before each publish and each delete.
  The task is part of the definition hash.
- **This worker's own files**, in every other case. The task runs at the same
  slots with no election and no lease. It is not a cluster task: it needs no
  `autodiscover`, it is not part of the definition hash, and it counts nothing
  in `drakkar_periodic_cluster_slots_total`. Its `periodic_run` row is a system
  row with no `scope`.

Different archive settings in one cluster are a misconfiguration. Workers that
run the cluster task hash differently from workers that do not, so the workers
list shows `cluster_tasks_match: false` and the workers log
`periodic_cluster_mismatch` (see [Definition hash](#definition-hash)). Give
every worker of a cluster the same settings.

See [Archiving](local-databases.md#archiving).

The three cache loops (`cache.flush`, `cache.cleanup`, `cache.sync`) are
worker-scope system tasks: every worker runs its own.

## Observability

| Name | Kind | Meaning |
|---|---|---|
| `drakkar_periodic_task_runs_total{name, status}` | counter | Runs by outcome (`ok`, `error`). A won cluster slot counts here too. |
| `drakkar_periodic_task_duration_seconds{name}` | histogram | Run duration. |
| `drakkar_periodic_cluster_slots_total{name, outcome}` | counter | Cluster slots by outcome: `won`, `held_elsewhere`, `late`, `election_failed`. |
| `drakkar_periodic_cluster_mismatch_peers` | gauge | Online peers of this worker's cluster with another definition hash. |
| `periodic_task_started` | log, info | A loop started. Fields `scope`, and `interval_seconds` or `cron`. |
| `periodic_slot_skipped` | log | A cluster slot this worker does not run. Debug for `held_elsewhere`; warning for `late` (field `late_seconds`) and `election_failed` (field `error`, such as `start timeout` or `acquire error`). At most one warning line per task per 60 seconds; `suppressed` counts the lines left out. |
| `periodic_lease_lost` | log, warning | The lease of a running slot is lost. Fields `name`, `slot`, `reason` (`taken_over` or `unconfirmed`). |
| `periodic_task_failed`, `periodic_task_stopped` | log | A run raised; with `on_error='stop'` the loop then ends. |
| `periodic_loop_ended` | log, error | A task's loop ended on an error outside a run; the task does not run again on this worker until it restarts. Fields `name`, `error`. |

The `periodic_run` row of the [flight recorder](observability.md#events-processing-events)
of a cluster run has the metadata keys `"scope": "cluster"` and
`"slot": <epoch second>`. The full list of log events is on
[Observability](observability.md#periodic-log-events).
