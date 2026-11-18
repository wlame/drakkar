# Detach from the Consumer Group

A running worker can leave its Kafka consumer group for a bounded time and
join it again later. It does not restart, and its health checks stay green.
While the worker is away, Kafka gives its partitions to the other workers of
the group. This is a **detach**. The return is an **attach**.

The handler can ask for a detach from its own code, for example when a probe
finds that the local disk is too slow. An operator can ask for one from the
Live page. Every detach has a deadline, and the framework attaches the worker
at the deadline when nothing did it sooner. A budget in the
[autodiscover](fleet.md) store limits how many workers of one group are away
at the same time.

The feature is **off by default**. A detach moves partitions between
workers, so a deployment turns it on deliberately.

---

## Pause or detach

The [consume pause](consume-pause.md) and the detach both stop the intake of
one worker. They do different things:

| | Consume pause | Detach |
|---|---|---|
| The partitions | Stay on this worker | Go to the other workers of the group |
| A rebalance | Never | One when the worker leaves, one when it returns |
| Consumer lag | Grows on the paused partitions | The other workers consume the partitions |
| Who can ask | An operator, on the Live page | The handler, or an operator on the Live page |
| Limit | 1 hour | `sources.kafka.detach.max_duration` (default 24 hours) |
| Needs | `ui.consume_pause.enabled` | `sources.kafka.detach.enabled` and `autodiscover` |

Use a pause to look at a worker for a short time while nothing moves. Use a
detach when the worker should not process messages for a while and the other
workers should do its work.

## When to use it

A worker can find that it works in a degraded mode: its disk or its network
is slow, or a dependency on its host does not answer. Without a detach it
has two choices. It can keep its partitions and hold the group back, or it
can stop and wait for a supervisor to start it again. A stop loses the
process state, and a restart on the same host does not help.

A detach is a third choice. The worker gives its partitions away, stays
alive, keeps measuring, and comes back when the cause has passed.

Base the decision on a property of **this worker**: the time of a disk
write, the round trip to a dependency, the time per task of this worker.
Do not base it on lag. The section [Risks](#risks) explains why.

## Turn it on

```yaml
autodiscover:
  enabled: true                 # the budget of detached workers is kept in its store
  provider: postgres            # or filesystem, redis, consul

sources:
  kafka:
    enabled: true
    consumer_group: search-workers
    detach:
      enabled: true
      max_detached: 1           # workers of this group away at once
      min_attached: 2           # workers of this group that must stay attached
      default_duration: 1h
      max_duration: 24h
      cooldown: 5m
      lease: 1m

ui:
  detach_enabled: true          # the Detach control on the Live page and the two POST routes
```

| Key | Default | Range | Meaning |
|---|---|---|---|
| `sources.kafka.detach.enabled` | `false` | | Let this worker leave its consumer group for a bounded time and join it again without a restart. |
| `sources.kafka.detach.max_detached` | `1` | 1 to 100 | How many workers of one consumer group may be detached at once. It is the number of slots in the store. |
| `sources.kafka.detach.min_attached` | `1` | 1 to 1000 | How many other workers of the group must stay attached. A group of one worker can never detach. |
| `sources.kafka.detach.default_duration` | `1h` | `10s` to `7d`, and at most `max_duration` | How long a detach lasts when the request gives no duration. |
| `sources.kafka.detach.max_duration` | `24h` | `10s` to `7d` | The longest detach that a request may ask for. |
| `sources.kafka.detach.cooldown` | `5m` | `0` (none) to `1d` | The shortest time between the return of a detach and the next handler detach. An operator detach does not wait for it. |
| `sources.kafka.detach.lease` | `1m` | whole seconds, `40s` to `1d` | The lease of the slot in the store. A detached worker attaches again when its lease was not confirmed for this long. |
| `ui.detach_enabled` | `false` | | Serve `POST /api/v1/debug/detach` and `POST /api/v1/debug/attach`, and show the Detach control on the Live page. |

Each key has an environment override that follows the path:
`DK_SOURCES__KAFKA__DETACH__ENABLED`, `DK_SOURCES__KAFKA__DETACH__MAX_DETACHED`,
`DK_SOURCES__KAFKA__DETACH__LEASE`, `DK_UI__DETACH_ENABLED`, and so on.

The load checks, which also run again after
[`on_startup`](handler.md#on_startup):

- `sources.kafka.detach.enabled needs autodiscover.enabled: the budget of detached workers is kept in the autodiscover store`
  (only with the Kafka source on).
- `sources.kafka.detach.default_duration (<v>) must be <= sources.kafka.detach.max_duration (<v>)`.
- `ui.detach_enabled needs sources.kafka.detach.enabled`.
- A value out of its range fails with the usual message, for example
  `sources.kafka.detach.lease must be from 40s to 1d, got 30s`.

**Use the same `max_detached` and `min_attached` on every worker of a
group.** Each worker publishes both values in its registry card. After each
registry refresh a worker logs `detach_config_mismatch` for an online peer of
its group whose values differ. With different values, each worker counts
with its own numbers, so the largest `max_detached` is the real limit.

## The handler API

Four members of `BaseDrakkarHandler`:

```python
result = await self.detach(duration=None, reason='slow disk')
result.detached   # bool: the worker is detached now
result.reason     # '' on success, else one value of the table below
result.until      # datetime (UTC) of the deadline, or None

result = await self.attach(reason='disk is fast again')
result.attached   # bool: the worker is a member of its group now
result.reason     # '' on success, else one value of the table below

state = self.detach_state
state.state       # 'attached', 'detaching', 'detached' or 'attaching'
state.origin      # DetachOrigin.HANDLER, DetachOrigin.OPERATOR, or None when attached
state.reason      # the text of the detach request
state.since       # datetime (UTC), or None when attached
state.until       # datetime (UTC) of the deadline; set only while the state is 'detached'

self.request_stop(reason='work directory is gone', exit_code=1)   # not awaited, returns at once
```

The types `DetachResult`, `AttachResult`, `DetachState` and `DetachOrigin` are
exported from `drakkar`.

**`detach(duration=None, reason='')`** leaves the consumer group.

- `duration` is in seconds. `None` means `default_duration`. A value below
  10 seconds or above `max_duration` raises `ValueError`.
- `reason` is free text. It is cut at 200 characters. It goes to the log,
  the recorder event, the UI and the agent state.
- The call returns when the revoke drain has ended and no partition is left
  on this worker, or with a refusal. It can take up to
  `executor.drain_timeout` plus 10 seconds, plus the calls to the store.
- A detach of a worker that is already detached by the same origin replaces
  the deadline: now plus the new duration. An operator detach of a worker
  that the handler detached takes over the origin and replaces the deadline.
  Both log `detach_extended` and publish the registry card again.
  A handler detach of a worker that an operator detached changes nothing and
  answers `already_detached`.

**`attach(reason='')`** joins the consumer group again.

- The call returns when the join has seen one poll and the slot is given
  back. It does not wait for an assignment, because an assignment can be
  empty.
- A handler attach does not end an operator detach. It answers
  `held_by_operator`.
- An attach of a worker that is attached changes nothing and answers
  `attached` true.

**`detach_state`** is one immutable object. Read it at any time, also from a
thread.

**`request_stop(reason='', exit_code=0)`** asks for a graceful stop of the
whole worker. See [Stop the worker](#stop-the-worker-request_stop).

A caller that is cancelled while it awaits `detach()` or `attach()` does not
stop the transition half way. The framework owns the transition: a cancelled
attach still finishes, and a cancelled detach that is still in progress is
undone.

### Example: leave the group while the disk is slow

A periodic task probes the disk every 30 seconds. When a write is slow, it
detaches the worker for 15 minutes. When the disk is fast again, it attaches
the worker before the deadline. When the framework refuses the detach, the
task logs the reason and asks again at the next probe. It never stops the
worker for a refusal.

```python
import asyncio
import os
import time
from pathlib import Path

import structlog

import drakkar as dk

logger = structlog.get_logger()

# The probe writes on the same disk as the working files of the tasks.
DISK_PROBE_FILE = Path('/data/work/.disk-probe')
DISK_PROBE_BYTES = 4 * 1024 * 1024
# A probe write that takes longer than this means the disk is too slow for the tasks.
SLOW_DISK_SECONDS = 2.0
DETACH_SECONDS = 15 * 60


def time_disk_write(path: Path) -> float:
    """Write and sync DISK_PROBE_BYTES to ``path``, remove the file, and return the seconds it took."""
    started = time.monotonic()
    with path.open('wb') as probe_file:
        probe_file.write(os.urandom(DISK_PROBE_BYTES))
        probe_file.flush()
        os.fsync(probe_file.fileno())
    path.unlink()
    return time.monotonic() - started


class SearchHandler(dk.BaseDrakkarHandler[SearchRequest, SearchResult]):
    @dk.periodic(30)
    async def leave_the_group_while_the_disk_is_slow(self):
        # The disk calls run on a thread: they must not stop the event loop.
        seconds = await asyncio.to_thread(time_disk_write, DISK_PROBE_FILE)
        disk_is_slow = seconds > SLOW_DISK_SECONDS
        state = self.detach_state

        if state.state == 'attached' and disk_is_slow:
            result = await self.detach(duration=DETACH_SECONDS, reason=f'disk write took {seconds:.1f}s')
            if not result.detached:
                # The next probe asks again; a refusal never stops the worker.
                await logger.awarning('slow_disk_detach_refused', reason=result.reason, write_seconds=seconds)
            return

        detached_by_handler = state.state == 'detached' and state.origin == dk.DetachOrigin.HANDLER
        if detached_by_handler and not disk_is_slow:
            await self.attach(reason=f'disk write took {seconds:.1f}s')
```

Notes on the example:

- The task stops the worker on no refusal. `too_few_workers` and
  `budget_full` mean that the group needs this worker now; a stop would take
  the worker away all the same. `store_unavailable` comes on every worker
  while the autodiscover store is down, so a stop on it would stop the whole
  group. `cooldown` and `leave_failed` pass by themselves.
- The task does not attach a worker that an operator detached. That attach
  would answer `held_by_operator`.
- When the disk is still slow at the deadline, the framework attaches the
  worker. The next probe asks for a detach again, which answers `cooldown`
  for the first 5 minutes; the worker keeps working in its group meanwhile.
- The disk call can hang. Then the task waits; only that task and one thread
  of the `threads.io` pool are held.

`request_stop` is for a worker that cannot work at all, for example when its
work directory is gone:

```python
WORK_DIR = Path('/data/work')


class SearchHandler(dk.BaseDrakkarHandler[SearchRequest, SearchResult]):
    @dk.periodic(30)
    async def stop_when_the_work_directory_is_gone(self):
        if not await asyncio.to_thread(WORK_DIR.is_dir):
            self.request_stop(reason=f'{WORK_DIR} is gone', exit_code=3)
```

`request_stop` checks neither the budget of detached workers nor
`min_attached`. A rule that stops every worker that cannot detach can stop a
whole group, so do not use a detach refusal as a reason to stop. A store
outage must never lead to a stop.

A shorter working example is the periodic task `detach_on_request` of the
harness handler (`integration/worker/handler.py`). It detaches the worker
for 20 seconds when a request file appears. The
[integration harness](integration.md) runs it in the scenario
`scenario-detach.sh`.

### Detach answers

`result.reason` of a detach that did not happen. The checks run in this
order, and the first one that applies answers.

| Reason | `detached` | When |
|---|---|---|
| `not_running` | false | The Kafka source has not finished its first poll, or the worker has not started. |
| `stopping` | false | The worker is stopping. Also when a stop comes while the detach waits for the revoke drain: the wait ends at once, and nothing subscribes again. |
| `disabled` | false | `sources.kafka.detach.enabled` is false, or the worker has no Kafka source. |
| `inside_pipeline_hook` | false | The call runs as part of a partition's work. See [Where a detach is allowed](#where-a-detach-is-allowed). |
| `join_failed` | false | The worker is returning to its group, and the join has not succeeded yet (state `attaching`). The framework finishes the attach first. |
| `already_detached` | **true** | An operator detach is active. Handler calls only. `until` is the operator's deadline. |
| `cooldown` | false | A handler call sooner than `cooldown` after the last return. |
| `too_few_workers` | false | Fewer than `min_attached` attached workers of the group would remain. |
| `budget_full` | false | Every slot of the budget is held by another worker. |
| `store_unavailable` | false | No slot was won, and at least one call to the store failed or made no attempt. Also when the slot lease was lost while the detach was in progress. |
| `leave_failed` | false | The revoke did not end within `executor.drain_timeout` plus 10 seconds, or the leave raised or was interrupted. The worker joins the group again and gives the slot back. When that join does not succeed, the framework tries it again every 5 seconds (`detach_join_failed`). |

Inside a Message Probe run, a third kind of answer exists: `probe`. See
[Message Probe runs](#message-probe-runs).

### Attach answers

| Reason | `attached` | When |
|---|---|---|
| `''` | true | The worker is a member of its group: the join succeeded, or the worker was attached already. |
| `not_running` | false | The Kafka source has not finished its first poll, or the worker has not started. |
| `stopping` | false | The worker is stopping. |
| `held_by_operator` | false | A handler attach of an operator detach. Only an operator, the deadline or a safety rule ends it. |
| `join_failed` | false | The join did not succeed within `executor.drain_timeout` plus 10 seconds, or an attach is already in progress and has not succeeded yet. The worker is still outside its group. The framework tries the join again every 5 seconds until it succeeds or the worker stops. |

While a join is tried again, the state stays `attaching` and the slot stays
held and refreshed, because the worker really is outside its group. The
state never reads `attached` for a join that did not succeed.

### Where a detach is allowed

A detach waits for the revoke drain, and the drain waits for the work in
progress of every partition. So a `detach()` that is awaited inside that work
would wait for itself.

A call from code that runs as part of a partition's work answers
`inside_pipeline_hook` at once and does nothing. That code is `arrange`,
`arrange_stream`, `on_task_complete`, `on_message_complete`,
`on_group_complete`, `on_window_complete`, `on_error` and
`on_delivery_error`. The rule also covers a call that such a hook awaits
through `asyncio.gather` or `asyncio.wait_for`, which run it in a new task.

A `detach()` is allowed from:

- a [periodic task](periodic-tasks.md), of either scope;
- `on_assign` and `on_revoke`, which run on background tasks;
- the hooks of the [HTTP source](webapp.md);
- a task that the handler started itself, once the hook that started it has
  returned.

One limit: a task that a hook creates and that first runs after that hook
returned is allowed, also when a later hook awaits it. That detach then
waits for the drain, the drain waits for the later hook, and the detach ends
with `leave_failed` after the leave bound. Do not await such a task from a
pipeline hook.

`attach()` and `request_stop()` have no such limit.

### Message Probe runs

The [Message Probe](observability.md#debug-debug-tools) of the debug UI runs
the handler hooks on a pasted message. A probe never changes the worker:

- `detach()` and `attach()` answer with the reason `probe` and change
  nothing. This value never reaches a metric, the recorder or the state of
  the routes.
- `request_stop()` does nothing and logs `stop_request_suppressed` (info,
  category `debug`, fields `reason` and `exit_code`).

Handler code can read the same mark with `probe.is_active()`; see
[Checking for a probe run](probe-user-details.md#checking-for-a-probe-run-probeis_active).

A task that handler code first starts during a probe keeps this behaviour
for its whole life, also after the probe has ended.

## How a detach runs

The states of the worker's Kafka source:

| State | Meaning |
|---|---|
| `attached` | Member of the group. The state at every start. |
| `detaching` | A slot is held, the worker left the group, and the revoke drain runs. |
| `detached` | Not a member. The slot lease is refreshed. A deadline is set. |
| `attaching` | The worker joins the group again, and then gives the slot back. |

One transition runs at a time. A second request waits for the transition in
progress and is then judged against the new state.

A detach runs these steps after the checks of the answer table:

1. **First size check.** The worker reads the peer cards of the last
   registry refresh. It counts the **attached peers**: the online workers of
   its cluster that publish its consumer group and publish `detached` as
   exactly `false`. With fewer attached peers than `min_attached`, the answer
   is `too_few_workers`. No call to the store is made.
2. **Slot.** The worker starts the election provider if no cluster task
   started it before. Then it tries to take slot 0, slot 1, and so on up to
   `max_detached - 1`, each call within 5 seconds, and stops at the first
   slot it wins.
3. **Second size check.** The worker reads the holder of every slot and
   counts the held slots, its own included. A read that fails counts as
   held. When the **group size** (the online workers of the group, this
   worker included) minus the held slots is below `min_attached`, the worker
   gives the slot back and answers `too_few_workers`. Two workers that detach
   at the same moment each wrote their slot before they read the others, so
   at least one of them sees both slots and gives its slot back. Both can
   give back; then both are refused, which is safe.
4. **Leave.** The worker starts the lease refresher of the slot, and the
   state is `detaching`. The registry card is published at once with
   `detached: true`, so peers count the worker as detached while its
   partitions drain. Then the worker calls `unsubscribe()` on its consumer
   and keeps polling. The client runs the revoke inside the next poll: `on_revoke`
   runs, the partition loops drain and commit within
   `executor.drain_timeout`, and the partitions move. The wait ends when no
   partition and no revoke is left. A worker that holds no partition leaves
   with its next poll.
5. **Detached.** The state is `detached`. The deadline timer starts, the
   registry card is published again, and the worker records
   `worker_detached`.

An attach runs these steps:

1. The deadline timer stops.
2. The worker calls `subscribe()` with the same callbacks and waits for one
   completed poll after it.
3. The worker gives the slot back. The release comes after the join, so
   another worker cannot take the slot and leave before this worker is on
   its way back.
4. The state is `attached`. The card is published at once, and the worker
   records `worker_attached` with the cause.

A detach that ends before the state `detached` (for example the leave passed
its bound) is undone: the worker joins the group again and gives the slot
back. An undone detach writes no `worker_detached` and no `worker_attached`
event and counts no attach. It logs `detach_undone`.

## How a detach ends

The `cause` of `worker_attached` and of `drakkar_attach_total`:

| Cause | Rule |
|---|---|
| `handler` | The handler called `attach()`, and the origin of the detach is `handler`. |
| `operator` | An operator attached the worker from the Live page or through `POST /api/v1/debug/attach`. It ends a detach of either origin. |
| `deadline` | The deadline passed. |
| `lease_lost` | The slot lease was lost. See below. |
| `too_few_workers` | After a registry refresh, fewer online workers of the group than `min_attached` are attached (this worker is not one of them). |

**The deadline.** Every detach has one: now plus the duration, at most
`max_duration`. The deadline exists only while the state is `detached`.
When the deadline timer fails or ends in an unexpected way, the worker
attaches at once.

**The lease.** While detached, the worker refreshes its slot lease every
third of `lease`, at most every 60 seconds.

- A refresh that finds another holder of the slot attaches the worker at
  once.
- A refresh that fails or does not answer is tried again at the next refresh
  time. The worker logs `detach_lease_unconfirmed` with the seconds left.
  It attaches when `lease` has passed since the last confirmed refresh.

A safety rule never waits behind a slow detach: a detach that is still in
progress is told to stop, and undoes itself.

**A stop.** A stop while detached needs no attach. The worker stops as usual
and makes one attempt to give the slot back, right after the sources
stopped: before the recorder stops, so the recorder's seal cannot spend the
time of that attempt, and before the election provider stops. A detach never survives a restart: a
started worker is always attached and holds no slot.

**A crash or a kill** leaves the slot held until its lease ends. Until then
the slot counts against the budget of the group.

## The budget of detached workers

The budget is `max_detached` lease slots in the store of the worker's
[election provider](local-databases.md#the-election-provider), the same
provider that runs the cluster periodic tasks. A worker must hold one slot to
be detached. The slots belong to the worker's cluster and consumer group:
the scope of slot `i` is
`<sanitized cluster>/detach.<group key>_<i>`, where the group key is `g` plus
the first 12 hex characters of the SHA-256 of the consumer group id. For
example `main/detach.g740b670895e9_0` for slot 0 of the group
`search-workers` in the cluster `main`. The stored lease has the shape of a
cluster task lock: with the `filesystem` provider it is the file
`<storage.shared_dir>/tasks/<cluster>/detach.<group key>_<i>.lock`.

The calls of a detach to the store have a caller and a circuit breaker of
their own, with the settings of `autodiscover.resilience`. So a store
failure seen by a detach never opens the breaker of the cluster tasks, and
an open breaker of the cluster tasks never refuses a detach.

The number of held slots is exact, because the store counts it. The size of
the group comes from the registry. The section
[The last-worker guard depends on the registry](#the-last-worker-guard-depends-on-the-registry)
says what that means.

Each worker with the Kafka source on publishes `features.kafka` in its
[registry card](fleet.md#the-card): `group`, `detached`, and, with the
feature on, `max_detached` and `min_attached`. A peer whose card has no
`features.kafka`, or a `detached` value that is not exactly `false`, does not
count as attached. A bad card can only refuse a detach, never allow one too
many.

## What keeps running while detached

- The poll loop keeps polling and gets nothing. The time does not count in
  `drakkar_consumer_idle_seconds_total`.
- There are no partitions, so `drakkar_consumer_lag` has no series for this
  worker, and the pending budget is empty.
- [Periodic tasks](periodic-tasks.md) of both scopes, the cache exchange, the
  recorder, the runtime health monitor, the [HTTP source](webapp.md) and the
  UI keep running.
- `/healthz` and `/readyz` answer as before: a detached worker is ready.
  See [Deployment](deployment.md#detached-workers-and-stop-requests).
- `on_revoke` runs at the leave for the partitions the worker held, and
  `on_assign` runs at the return. There is no other hook for the state.
  Read `self.detach_state`.

## The operator control

With `ui.detach_enabled` on, the Live page has a **Detach** control next to
the pause control. It offers the durations 5 minutes, 15 minutes and
1 hour (those within `max_duration`; with none of them in range, only
`max_duration`), and a reason field of at most 200 characters. While the
worker is detached, the control is an **Attach now** button. A refused
request shows its reason in one plain sentence.

A banner on every page shows a detached worker: the origin, the reason, and
the deadline in UTC with the time left. The banner shows also when the
buttons are off (`ui.detach_enabled: false`), so that a handler detach is
visible. While the worker leaves or joins its group, one quieter line says
so. When the worker does not answer, the banner keeps the last state and
says since when the state is unknown.

The worker switcher marks each detached worker of the registry with
`detached`. See the [Operator UI reference](ui-reference.md#live-live).

The routes:

| Route | What it does |
|---|---|
| `GET /api/v1/debug/detach` | The state, always 200, also with the feature off: `enabled`, `ui_enabled`, `state`, `origin`, `reason`, `since_ms`, `until_ms` (the deadline while `detached`, `null` in every other state), `max_duration_seconds`, `default_duration_seconds`, and `last_request`. |
| `POST /api/v1/debug/detach` `{"duration_seconds": N, "reason": "..."}` | Starts an operator detach and answers 202 with the state at once. |
| `POST /api/v1/debug/attach` | Starts an operator attach and answers 202 with the state at once. On an attached worker it changes nothing. |

A detach can take `executor.drain_timeout` and more, so the POST routes do
not wait for it. `last_request` gives the outcome when it is known: `origin`
(`handler` or `operator`), `action` (`detach` or `attach`), `outcome`
(`detached`, `attached` or a refusal reason) and `at_ms`. It is `null` before
the first request. The UI polls the state while it is `detaching` or
`attaching`.

The POST routes answer:

- `403` naming the key when `ui.detach_enabled` is false, or the worker has
  no Kafka source;
- `422` in the usual validation shape for a `duration_seconds` below
  10 seconds or above `max_duration`;
- `503` when the worker's main loop does not answer within 5 seconds, or is
  closed. Such a request is not started later; only one whose start was
  already running at that moment still takes effect, and `last_request`
  shows it. The worker logs `ui_detach_request_unanswered`.

```bash
curl -X POST 'http://worker:8080/api/v1/debug/detach' \
  -H 'Content-Type: application/json' -d '{"duration_seconds": 900, "reason": "disk check"}'
curl 'http://worker:8080/api/v1/debug/detach'
curl -X POST 'http://worker:8080/api/v1/debug/attach'
```

The optional `ui.auth_token` gates the three routes, like every UI route.
`/api/v1/workers` rows carry `detached` (`true`, `false`, or `null` when the
card does not say). The [agent surface](agent.md) has the state part
`detach` and the rule `worker-detached`.

## Stop the worker: `request_stop`

`self.request_stop(reason='', exit_code=0)` asks for a graceful stop, the
same as `SIGTERM`. It is for a worker that cannot work at all, not an answer
to a refused detach, and it works also with the detach feature off.

- It returns at once and is not awaited. It is safe from a hook, from
  `on_startup`, and from a thread (a function that runs through
  [`offload`](offload.md)).
- It logs `stop_requested` (warning, category `lifecycle`, fields `reason`
  and `exit_code`). Then every source stops taking input, the drain settles
  the work in progress, the sinks deliver, and the worker shuts down.
- After a clean shutdown the process exits with `exit_code`, a whole number
  from 0 to 255. Another value raises `ValueError`.
- The first call wins. A later call changes nothing and logs nothing.
- Before the worker runs it does nothing. Inside a Message Probe run it only
  logs `stop_request_suppressed`.
- It checks neither the budget of detached workers nor `min_attached`. A
  rule that stops every worker that cannot detach can stop a whole group,
  and a store outage must never lead to a stop.

What happens next is the job of the supervisor. Kubernetes starts the
container again (on the same node), with any exit code, under the default
`restartPolicy: Always`. A supervisor that restarts only on failure needs an
exit code other than 0.

## Metrics, events and log events

Metrics:

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `drakkar_detached` | Gauge | -- | `1` while this worker is detached from its consumer group. |
| `drakkar_detach_requests_total` | Counter | `origin`, `outcome` | Detach requests by origin (`handler`, `operator`) and outcome (`detached` or a refusal reason). A `probe` answer is not counted. |
| `drakkar_attach_total` | Counter | `cause` | Returns to the consumer group after a detach, by cause. An undone detach does not count. |

The calls to the store appear in the provider metrics
(`drakkar_provider_calls_total`, `drakkar_provider_call_duration_seconds`,
`drakkar_provider_breaker_open`) with `feature="detach"` and the operations
`acquire`, `holder`, `refresh`, `release`, and `start` when a detach is the
first user of the election provider. That start logs `provider_started` with
category `periodic`. See
[Fleet registry and provider operations](observability.md#fleet-registry-and-provider-operations).

Recorder events, with `partition` -1 like the other worker events, stored in
`events` and sent on `/ws`:

| Event | Metadata |
|---|---|
| `worker_detached` | `origin`, `reason`, `until` (the deadline), `slot` (the slot index) |
| `worker_attached` | `cause`, `detached_seconds` |

Log events, all in category `kafka` unless the table says otherwise:

| Event | Level | When and fields |
|---|---|---|
| `worker_detached` | warning | The worker is detached. `origin`, `reason`, `until`, `slot`. |
| `worker_attached` | warning | A detach ended. `cause`, `detached_seconds`, `reason` (the text of the attach request). |
| `detach_refused` | info | A detach request did not detach the worker. `origin`, `reason` (the answer). |
| `detach_extended` | info | A detach of a detached worker replaced the deadline or took over the origin. `origin`, `until` (the new deadline), `reason`. |
| `detach_undone` | info | A detach ended before the state `detached` and was taken back. `origin`, `outside_seconds`. |
| `detach_lease_unconfirmed` | warning | A refresh of the slot lease failed or did not answer. `scope`, `seconds_left`. |
| `detach_join_failed` | warning | A join of the group did not succeed. `error`. The join is tried again every 5 seconds. |
| `detach_join_retry_restarted` | error | The background retry of a join ended while the worker was still outside its group, and started again. `cause`. |
| `detach_config_mismatch` | warning | An online peer of the group has other limits. `peer`, `peer_max_detached`, `peer_min_attached`, `max_detached`, `min_attached`. Once per peer and value pair. |
| `detach_transition_failed` | error | A detach raised. `origin`; carries the exception. The worker returns to its group. |
| `attach_transition_failed` | error | An attach raised. `cause` when the attach came from a request or a retry; carries the exception. The join is tried again. |
| `detach_request_failed` | error | An operator request raised outside the transition; carries the exception. |
| `detach_deadline_timer_failed` | error | The deadline timer raised; the worker attaches at once. |
| `detach_lease_refresh_failed` | error | The lease refresher raised outside a store call. `scope`. The slot counts as lost. |
| `detach_lease_loss_report_failed` | error | The report of a lost slot raised. `scope`, `reason`. |
| `detach_slot_release_failed` | error | A release of the slot raised. The lease then ends by itself. |
| `registry_refresh_check_failed` | error | A check after a registry refresh raised. `check` (`detach`, category `kafka`; or `cluster_hash`, category `periodic`). |
| `ui_detach_request_unanswered` | warning | A POST route got no answer from the main loop (category `debug`). `error_type`, `error`. |
| `stop_requested` | warning | `request_stop` was called (category `lifecycle`). `reason`, `exit_code`. |
| `stop_request_suppressed` | info | `request_stop` was called inside a Message Probe run (category `debug`). `reason`, `exit_code`. |

## Measured in the integration harness

These numbers come from the [integration harness](integration.md): three
workers, 50 partitions, an `executor.drain_timeout` of 5 seconds, and the
`filesystem` store. They are an example, not a guarantee.

- A leave took 2 to 6 seconds.
- The other workers held every partition 3 to 8 seconds after the request.
- After an attach, the worker held partitions again 3 to 8 seconds later.
- After a detached worker was killed, its slot was free about one `lease`
  later.
- Under load, the leave reached the drain timeout. The tasks in progress of
  10 of 17 partitions were cancelled and ran again on the new owner.

## Risks

### Lag belongs to a partition, not to a worker

A rule "detach when my lag grows" moves a hot partition to a healthy
worker, whose lag then grows too. With a fallback "cannot detach, so stop",
this becomes a chain of restarts across the group. Base the rule on a
property of the worker: the time per task, a disk probe, a network probe.

### A detached worker has no lag metric of its own

A detached worker holds no partition, so `drakkar_consumer_lag` has no
series for it. A rule "attach when my lag is low" cannot work. The return
rule needs a measure that exists while the worker is detached, such as the
disk probe of the example.

### Other workers pay at the return

At the leave the other workers only gain partitions. At the attach,
partitions are taken from them: each one runs its revoke drain, and tasks
that do not finish within `executor.drain_timeout` are cancelled and run
again on the worker that gets the partition. `cooldown` limits how often a
handler can make this happen.

### A slow worker drains badly

A worker detaches because it is slow, and a slow worker is slow to drain.
Tasks that do not finish within the drain timeout are cancelled and run
again on the new owner. A message of [`arrange_stream`](fan-out.md#streaming-a-large-message)
that is still open runs again from its start.

### The last-worker guard depends on the registry

The number of detached workers is exact: the store counts it. The size of
the group comes from the registry, which can be up to
`autodiscover.refresh` plus `autodiscover.offline_after` old. A worker that
died a moment ago still counts as a member. When every attached worker dies
while one worker is detached, nobody consumes until the detached worker's
next registry refresh shows it, and the `too_few_workers` rule attaches it.

### A poison message moves

A partition whose loop is dead on this worker goes to another worker after a
detach. That worker meets the same message.

### A detached worker still wins cluster task slots

A detached worker runs its periodic tasks, and it takes part in the election
of cluster tasks. It can win a slot, the archive pass included. Write a
cluster task so that it does not depend on the Kafka partitions of the
worker that runs it.

### Kubernetes restarts a stopped container on the same node

`request_stop` moves the work to another host only where the scheduler does
that. Kubernetes starts the stopped container again on the same node, so a
slow node stays slow. Nomad moves an allocation to another node only after
its restart attempts. A restart does not fix a host problem; a detach keeps
the process alive and gives the partitions away instead.

## HTTP workers

The detach is for the Kafka source only. The [HTTP source](webapp.md) keeps
serving while the Kafka source is detached, and a worker with no Kafka source
answers `disabled`.

For an HTTP worker, the router in front decides where requests go. The
standard signal is a failing readiness check with a passing liveness check:
Kubernetes removes the pod from the Service endpoints, Consul and Nomad take
the instance out of the healthy set, and load balancers stop sending to it.
None of them restarts the worker for it, unless a restart on a failed check
is configured.

The framework does not make `/readyz` fail while detached. The result would
be less exact than for Kafka: traffic stops only after the probe period
times its failure threshold, so a call cannot report "traffic stopped". An
unready pod counts against `maxUnavailable` and a PodDisruptionBudget, and
can stall a rolling deploy. A deployment that uses `/readyz` as its liveness
probe would restart the worker. To take an HTTP worker out of rotation,
use the means of the router, or `request_stop`.

## Related pages

- [Consume Pause](consume-pause.md): stop intake without a rebalance.
- [Handler System](handler.md#detach-attach-and-stop): the handler members.
- [Fleet Registry](fleet.md): the autodiscover store and the registry card.
- [Periodic Tasks](periodic-tasks.md): where a handler detach usually runs.
- [Deployment](deployment.md#detached-workers-and-stop-requests): probes and
  exit codes.
- [Observability](observability.md): every metric and event.
