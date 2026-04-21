# Fan-out: One Message → Many Tasks → One Aggregate

A common pipeline shape: one incoming message describes work that must be
split into many subprocess invocations, and the useful output is a
**single aggregated record** over all those subprocess outcomes — not
one output per task.

Drakkar supports this natively via the `on_message_complete` hook and
the `MessageGroup` dataclass.

!!! info "When to use this page"
    If your pipeline is strictly 1-in / 1-out (one input → one subprocess
    → one output), you don't need any of this — use `on_task_complete`
    and ignore the rest of the page. This page is for pipelines where
    one input deliberately produces *several* subprocess tasks.

---

## The shape

```mermaid
flowchart LR
    M["SourceMessage<br/>(e.g. 3 patterns × 2 files)"] --> A[arrange]
    A --> T1[Task 1]
    A --> T2[Task 2]
    A --> T3[Task 3]
    A --> T4[Task 4]
    A --> T5[Task 5]
    A --> T6[Task 6]
    T1 --> C["on_task_complete<br/>(per-task detail)"]
    T2 --> C
    T3 --> C
    T4 --> C
    T5 --> C
    T6 --> C
    T1 & T2 & T3 & T4 & T5 & T6 -. all terminal .-> MC["on_message_complete<br/>(one aggregate)"]
    C --> S1[per-task sinks]
    MC --> S2[aggregate sinks]
```

- `arrange()` expands the input into N tasks (the fan-out)
- `on_task_complete(result)` fires **once per task** — typical per-task fanout
- `on_message_complete(group)` fires **once per source message**, after every
  task derived from it has reached a terminal state

The two hooks are independent. You can use both, either, or neither.

---

## The `MessageGroup` passed to `on_message_complete`

```python
class MessageGroup(BaseModel):
    source_message: SourceMessage           # the original Kafka message
    tasks: list[ExecutorTask]               # full history (see below)
    results: list[ExecutorResult]           # terminal successes
    errors: list[ExecutorError]             # terminal failures (SKIP / retries exhausted)
    started_at: float                       # monotonic, when arrange() scheduled first task
    finished_at: float                      # monotonic, when last task terminal'd

    # Convenience properties
    succeeded: int      # len(results)
    failed: int         # len(errors)
    total: int          # len(tasks) — includes REPLACED tasks (see below)
    replaced: int       # total - succeeded - failed
    all_succeeded: bool # True iff total > 0 and failed == 0
    any_failed: bool    # failed > 0
    is_empty: bool      # total == 0 (arrange() returned nothing)
    duration_seconds: float
```

### What counts as "terminal"?

A task is **terminal** when its outcome is decided:

| Outcome | Contributes to |
|---|---|
| Subprocess exit 0, `on_task_complete` ran | `results` |
| `on_error` returned `SKIP` | `errors` |
| `on_error` returned `RETRY`, retries exhausted | `errors` |
| `on_error` returned `list[ExecutorTask]` (replaced) | **neither** — the replacements take its place |
| Unexpected exception in `on_task_complete` | `errors` (synthesised) |

Replaced-original tasks are kept in `tasks` (full history for debugging)
but not counted in `results` or `errors`. The REPLACEMENTS eventually
land in `results` or `errors` as their own terminal outcomes.

You can always recover the replacement count:

```python
group.replaced == group.total - group.succeeded - group.failed
```

### Tracing the replacement chain

Every task the framework schedules in response to an `on_error` list-return
has its `parent_task_id` auto-populated with the failing task's `task_id`
(unless the handler explicitly set it). In `on_message_complete` you can
walk the chain upward to find the arrange()-produced root:

```python
async def on_message_complete(self, group):
    by_id = {t.task_id: t for t in group.tasks}
    for task in group.tasks:
        chain = [task]
        while chain[-1].parent_task_id:
            chain.append(by_id[chain[-1].parent_task_id])
        # chain[0] is this task; chain[-1] is the original arrange() task
```

---

## A minimal example

```python
from pydantic import BaseModel, Field
import drakkar as dk


class SearchRequest(BaseModel):
    request_id: str
    patterns: list[str] = Field(min_length=1)
    file_paths: list[str] = Field(min_length=1)


class PerTaskResult(BaseModel):
    request_id: str
    pattern: str
    file_path: str
    match_count: int


class RequestSummary(BaseModel):
    request_id: str
    total_tasks: int
    succeeded: int
    failed: int
    total_matches: int


class MyHandler(dk.BaseDrakkarHandler[SearchRequest, PerTaskResult]):
    async def arrange(self, messages, pending):
        # One message → patterns × files tasks. Every produced task
        # shares the message's source_offsets — this is what binds them
        # into a MessageGroup.
        tasks = []
        for msg in messages:
            req = msg.payload
            for pattern in req.patterns:
                for file_path in req.file_paths:
                    tasks.append(
                        dk.ExecutorTask(
                            task_id=dk.make_task_id('search'),
                            args=[pattern, file_path],
                            metadata={
                                'request_id': req.request_id,
                                'pattern': pattern,
                                'file_path': file_path,
                            },
                            source_offsets=[msg.offset],
                        )
                    )
        return tasks

    async def on_task_complete(self, result):
        """Fine-grained per-task output (optional)."""
        meta = result.task.metadata
        matches = sum(1 for line in result.stdout.split('\n') if line)
        per_task = PerTaskResult(
            request_id=meta['request_id'],
            pattern=meta['pattern'],
            file_path=meta['file_path'],
            match_count=matches,
        )
        return dk.CollectResult(
            kafka=[dk.KafkaPayload(data=per_task, key=meta['request_id'].encode())],
        )

    async def on_message_complete(self, group):
        """Called ONCE per SearchRequest, after all its tasks finished."""
        req: SearchRequest = group.source_message.payload
        if req is None or group.is_empty:
            return None

        total_matches = sum(
            sum(1 for line in r.stdout.split('\n') if line)
            for r in group.results
        )

        summary = RequestSummary(
            request_id=req.request_id,
            total_tasks=group.total,
            succeeded=group.succeeded,
            failed=group.failed,
            total_matches=total_matches,
        )

        # One aggregate record per request to a "summaries" topic.
        return dk.CollectResult(
            kafka=[
                dk.KafkaPayload(
                    data=summary,
                    key=req.request_id.encode(),
                    sink='summaries',
                ),
            ],
        )
```

---

## Offset commit semantics

Offsets are committed **per source message**, after `on_message_complete`
returns. This means:

- A fast message whose tasks finish quickly commits its offset immediately,
  even if another (slow) message in the same arrange() window is still
  in flight.
- If `on_message_complete` raises, the exception is logged and offsets
  commit anyway — the raise doesn't stall the partition.
- On crash / revoke before `on_message_complete` completes, the offset
  is NOT committed. The message replays on restart (at-least-once).
  Any partial side effects already delivered via `on_task_complete` are
  duplicated on replay; design downstream sinks to be idempotent (use
  `request_id` as a primary/dedup key).

!!! tip "At-least-once, not exactly-once"
    Replays can cause duplicates in the per-task sinks. The message-level
    aggregate in `on_message_complete` is at-least-once too: a crash
    between "aggregate delivered to Kafka" and "offset committed to Kafka"
    produces a duplicate aggregate on replay. If this matters, dedupe
    downstream by `(request_id, partition, offset)`.

---

## Choosing which hook(s) to implement

| Your shape | Use |
|---|---|
| 1 message → 1 task → 1 output | just `on_task_complete` |
| 1 message → N tasks → N outputs | just `on_task_complete` |
| 1 message → N tasks → **1 aggregate output** | `on_message_complete` only (return `None` from `on_task_complete`) |
| 1 message → N tasks → N detail + 1 aggregate | BOTH `on_task_complete` and `on_message_complete` |
| Multi-message batch metrics | `on_window_complete` (coarser than message-level) |

All three hooks coexist — they fire on the same underlying data but at
different granularities. Which to use is a choice about what you want
downstream consumers to see, not a framework constraint.

### `on_message_complete` vs `on_window_complete`

| | `on_message_complete` | `on_window_complete` |
|---|---|---|
| Fires for | one source message | one arrange() batch |
| Receives | `MessageGroup` | `list[ExecutorResult]`, `list[SourceMessage]` |
| Granularity | per-message | per-window (may span many messages) |
| Offset commit order | **before** commit | **after** some offsets may already be committed |
| Typical use | request-level aggregation | dashboard metrics, window-level logs |

---

## Error handling across the group

The group doesn't need every task to succeed to fire. The hook ALWAYS
fires when all tasks reach a terminal state, whether that's success or
failure. Decide what to emit based on the group's shape:

```python
async def on_message_complete(self, group):
    if group.is_empty:
        # arrange() returned nothing for this message — you may still
        # want to emit an audit record so the request isn't invisible.
        return self._emit_skipped(group.source_message)

    if group.all_succeeded:
        return self._emit_success_aggregate(group)

    if group.any_failed and group.succeeded == 0:
        # Every task failed — emit a dead-letter-style summary.
        return self._emit_total_failure(group)

    # Partial failure: some succeeded, some didn't.
    return self._emit_partial_aggregate(group)
```

### Replacement chains

`on_error` returning a replacement list is a common pattern for
"subdivide a failed task into smaller work." The replacements become
part of the same `MessageGroup` automatically — the group doesn't
complete until every replacement (and any *further* replacements from
their failures) settles.

```python
async def on_error(self, task, error):
    if error.exception and 'memory' in (error.exception or '').lower():
        # Split the file in half and try each part as a smaller task.
        return [
            dk.ExecutorTask(
                task_id=dk.make_task_id('half1'),
                args=[...],
                source_offsets=task.source_offsets,  # REQUIRED: inherit
            ),
            dk.ExecutorTask(
                task_id=dk.make_task_id('half2'),
                args=[...],
                source_offsets=task.source_offsets,
            ),
        ]
    return dk.ErrorAction.SKIP
```

!!! warning "Replacements must inherit `source_offsets`"
    A replacement task with empty or different `source_offsets` will not
    be tracked by the original message's `MessageGroup`. Copy
    `task.source_offsets` onto every replacement unless you deliberately
    want to detach it.

    The framework auto-populates `parent_task_id` so you can trace the
    chain later; `source_offsets` is the handler's responsibility.

---

## Multi-message tasks (fan-IN)

If a task has `source_offsets = [a, b, c]` (one task represents work for
three source messages), it participates in THREE `MessageGroup`s. Its
terminal outcome is reported to all of them. Each group only completes
when every task it has a stake in has reached a terminal state.

This is uncommon but legitimate — deduplication, batched external API
calls covering multiple messages at once, or cross-message aggregation
work that makes one subprocess cheaper than N. The tracking is
transparent; no new field is needed beyond the existing list-typed
`source_offsets`.

### Example: dedupe identical queries across a window

A window of messages sometimes contains duplicate (pattern, file_path)
combinations. Instead of running the same search subprocess many times,
combine them into one task and let its result feed every message that
asked the same question:

```python
async def arrange(self, messages, pending):
    # Group messages by the actual search key.
    groups: dict[tuple[str, str], list[SourceMessage]] = {}
    for msg in messages:
        key = (msg.payload.pattern, msg.payload.file_path)
        groups.setdefault(key, []).append(msg)

    tasks = []
    for (pattern, file_path), msgs in groups.items():
        tasks.append(
            dk.ExecutorTask(
                task_id=dk.make_task_id('rg'),
                args=[pattern, file_path],
                # ONE task for every message that asked the same question.
                # When the task completes, its result lands in EACH of those
                # messages' MessageGroups, so every request_id is answered.
                source_offsets=[m.offset for m in msgs],
                metadata={
                    'pattern': pattern,
                    'file_path': file_path,
                    'request_ids': [m.payload.request_id for m in msgs],
                },
            )
        )
    return tasks

async def on_message_complete(self, group):
    # Even though a shared task produced group.results[0], each group
    # fires independently so the handler gets one callback per message.
    # The request_id comes from the source_message, not the task.
    req = group.source_message.payload
    return dk.CollectResult(
        kafka=[
            dk.KafkaPayload(
                data=Aggregate(request_id=req.request_id, ...),
                key=req.request_id.encode(),
                sink='results',
            ),
        ],
    )
```

### Example: single validation task shared across the window

Some pipelines want to run a quick validation pass once per window (e.g.
a schema check, a rate-limit probe) whose outcome applies to every
message that arrive in that batch. Express this as a single task tied
to every offset:

```python
async def arrange(self, messages, pending):
    tasks = [
        dk.ExecutorTask(
            task_id='validate',
            args=['--check-health'],
            source_offsets=[m.offset for m in messages],
        ),
    ]
    for msg in messages:
        tasks.append(
            dk.ExecutorTask(
                task_id=dk.make_task_id('proc'),
                args=['--process', msg.payload.data],
                source_offsets=[msg.offset],
            )
        )
    return tasks
```

Each message's `MessageGroup` will have `total = 2`: the shared
validation result plus that message's own processing task. The shared
task's `ExecutorResult` appears in all groups' `results` lists — the
same instance, so treat it as read-only.

### Gotchas

1. **Same partition only.** All offsets in `source_offsets` must be
   from the current partition (a `PartitionProcessor` only knows about
   its own partition's messages). Cross-partition offsets are silently
   skipped because no tracker exists for them. If you need
   cross-partition fan-in, run a downstream worker that consumes the
   output of this one.

2. **Replacements must inherit the multi-offset list.** If your
   `on_error` returns replacement tasks for a fan-in task, each
   replacement should carry the same `source_offsets` (or an intentional
   subset) — otherwise some groups will hang waiting on a task that no
   longer exists:

   ```python
   async def on_error(self, task, error):
       return [
           dk.ExecutorTask(
               task_id=dk.make_task_id('retry'),
               args=[...],
               source_offsets=task.source_offsets,  # INHERIT
           ),
       ]
   ```

3. **Shared results are shared objects.** `group.results` may hold the
   same `ExecutorResult` instance for multiple message groups when a
   fan-in task contributed to each. Treat results as immutable; if you
   need to annotate, build a new model in `on_message_complete`.

4. **Silent skip for bogus offsets.** If `source_offsets` includes an
   offset that isn't currently tracked (e.g. from a previous window or
   a handler bug), the framework silently ignores it rather than
   crashing. The task still runs; its outcome just isn't reported to a
   non-existent group.

---

## What's NOT in scope (yet)

Grouping that spans **multiple source messages** (e.g. "wait for 5
related messages with the same business key, then aggregate across them")
is a different problem — it's stateful aggregation, and it interacts
with Kafka partitioning in ways that can't be hidden behind a handler
hook without serious trade-offs.

Drakkar's current stance: **do that downstream**. Emit your
`on_message_complete` aggregates to a Kafka topic, and run a second
Drakkar worker (or Kafka Streams / Flink / your own consumer) that
groups those aggregates by business key. That worker owns the group
state and the termination condition.

A future `get_source_group_id(msg)` + `on_source_group_complete(group)`
hook for same-partition grouping may land once the termination semantics
are worked out.

---

## Precomputed tasks in a fan-out group

Any task in the fan-out — shared or per-message — can supply a
[`PrecomputedResult`](handler.md#precomputed-task-results-skip-the-subprocess)
instead of running a subprocess. Mixing precomputed and real tasks in
the same `MessageGroup` works transparently: `group.results` may
contain results from both sources. Tell them apart via
`result.pid is None` (precomputed) versus a numeric pid (real subprocess).

Common shape: cache-hit per-message task plus a real shared validation
task. The fan-in lookup runs once (cheap), and each message's
`on_message_complete` sees both its own cached result and the shared
one.

---

## Events emitted by the recorder

Each hook completion produces a distinct event in the flight recorder,
so the debug UI, Prometheus queries, and downstream tooling can tell
the three stages apart:

| Event | Fires after | Grain |
|---|---|---|
| `task_complete` | `on_task_complete()` returns | per task |
| `message_complete` | `on_message_complete()` returns | per source message |
| `window_complete` | `on_window_complete()` returns | per arrange() window |

Each event carries `duration`, `output_message_count`, and stage-specific
fields (`task_id` on `task_complete`; `offset` + `task_count` +
`succeeded` + `failed` + `replaced` on `message_complete`; `window_id`
+ `task_count` on `window_complete`). Use these to build per-stage
dashboards ("how often do my requests fail entirely?") or to hunt slow
hooks.

!!! note "vs `task_completed`"
    `task_completed` (past tense, with a `-d`) is a SEPARATE event that
    marks the moment a subprocess exits cleanly — emitted by the
    executor, not the hook. The pipeline sees `task_completed` first,
    then (if `on_task_complete` is overridden) `task_complete` once the
    hook finishes. The naming is subtle but the distinction is what
    lets you debug "my subprocess is fast but my hook is slow" without
    guessing.

---

## Integration demo

A full end-to-end example with real Kafka, Postgres, Mongo, and Redis
is in `integration/worker/`:

- `models.py` — `SearchRequest` (with `patterns`, `file_paths` lists)
  and `SearchAggregate`
- `handler.py` — `arrange()` fans out patterns × files;
  `on_task_complete` emits per-task to Kafka/Mongo/Redis/Postgres
  archive; `on_message_complete` emits one aggregate per request to
  Kafka priority topic, and conditionally to the hot Postgres DB and
  a webhook

Run with:

```bash
cd integration
docker compose up --build
```

Then watch the debug UIs at `:8081`–`:8083` — each request fans out to
several subprocess tasks, and exactly one aggregate row per request
lands in the `hot_recent_matches` Postgres table.
