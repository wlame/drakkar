# Annotations: Exposing Your Own Diagnostics in the UI

Your handler knows things the framework never sees. Which candidate inputs
`arrange()` looked at before picking one. The flag in a request that made a
task get built differently. The alternative that was rejected and why.

That information is usually not worth writing to a sink, and not worth
logging on every run. But when someone opens one message in the debug UI
months later and asks "why did this produce *that*?", it is exactly what
they need.

`self.annotate(...)` puts it there.

```python
async def arrange(self, messages, pending):
    tasks = []
    for msg in messages:
        candidates = self.discover_inputs(msg)
        chosen = self.pick(candidates)
        self.annotate(msg, 'input_selection', {
            'candidates': candidates,
            'chosen': chosen,
            'reason': 'newest matching revision',
        })
        tasks.append(self.build_task(msg, chosen))
    return tasks
```

Open that message's trace in the UI and the annotation sits inline with its
`consumed`, `task_started`, and `task_completed` events.

---

## The three scopes

Scope comes from what you pass as the first argument. You never pass
partitions or offsets — the framework knows which entity the running hook
is working on and fills the anchors in for you.

```python
# this one message
self.annotate(msg, 'input_selection', {'candidates': paths})

# this one task
self.annotate(task, 'arg_derivation', {'template': tpl, 'flags': flags})

# the whole arrange() window
self.annotate(None, 'window_summary', {'deduplicated': 12})
```

| First argument | Appears on |
|---|---|
| a `SourceMessage` | that message's trace |
| an `ExecutorTask` | that task, and the trace of every message that produced it |
| `None` | the whole hook invocation — for `arrange()` and `on_window_complete`, every message in the window |

Annotations work from every hook the framework calls: `arrange`,
`on_task_complete`, `on_error`, `on_message_complete`, `on_window_complete`,
and the two webapp hooks.

!!! warning "Only inside hooks"
    Calling `annotate()` from a periodic task, a background coroutine, or
    your own `__init__` has no pipeline entity to attach to. The record is
    dropped and counted under `reason="no_context"`, and the first
    occurrence is logged loudly.

---

## Arguments

```python
def annotate(
    self,
    target: SourceMessage | ExecutorTask | None,
    kind: str,
    data: Mapping[str, Any] | None = None,
    *,
    labels: dict[str, str] | None = None,
) -> None
```

`kind` names what the annotation describes. Keep it a stable literal per
call site (`'input_selection'`, not `f'selection_{msg.id}'`) — it is what
you filter on in the UI.

`data` is any JSON-serializable payload. Values the encoder cannot
represent natively degrade to their `str()`.

`labels` are stored on the row exactly like
[`ExecutorTask.labels`](handler.md) — indexed and searchable, so you can
pull up every annotation carrying a given `request_id`.

---

## Where the data lives, and for how long

Annotations are rows in the flight recorder's `events` table, under the
`annotation` event type. That has three consequences worth knowing:

**They live and expire like any other event.** An annotation rotates into
a new raw file every `ui.recorder.rotation_interval_hours` along with
everything else, and once that file's window is archived
(`ui.recorder.archive_enabled`, default true — see
[Archiving](local-databases.md#archiving)) the raw file is deleted but
the annotation survives, compressed, inside the archive.
`ui.recorder.archive_retention_days` (default `30`) is the only thing
that expires archives, so by default an annotation survives about a
month after its window is archived. Set it to `0` to keep archives —
and the annotations inside them — indefinitely. With `archive_enabled: false`, raw
files — and the annotations inside them — are never deleted
automatically at all.

**They are debug data, not durable data.** Recorder DBs are disposable by
design — operators delete them, rotation replaces them. Never annotate
something you would be unhappy to lose. If it matters, it belongs in a sink.

**They are visible to anyone who can reach the debug UI**, which is
unauthenticated unless you set `ui.auth_token`. Do not annotate secrets or
personal data.

---

## Budgets, and what happens when you exceed them

Annotating must never affect processing, so `annotate()` never raises and
never blocks — it appends to the recorder's in-memory buffer and returns.

When a payload cannot be accepted it is **dropped whole, never truncated.**
A half-written structured document still parses and still looks complete,
so it misleads whoever reads it far more effectively than a missing record
does. You get all of it or none of it.

Two limits, both per hook invocation:

| Setting | Default | Guards |
|---|---|---|
| `ui.recorder.annotation_max_bytes` | 16 KiB | one unreasonable payload |
| `ui.recorder.annotation_max_bytes_per_call` | 256 KiB | total DB pressure from one hook call |

Set either to `0` to disable it. The second one matters more than it looks:
without it, a handler annotating every message of a wide window can flood
the in-memory buffer (`ui.recorder.max_buffer`) with low-value rows and
evict genuinely important events that have not been flushed yet, which
costs you the debug value of everything else.

Every drop increments
`drakkar_recorder_annotations_dropped_total{reason=...}` and is logged at
warning level with the payload attached, so nothing disappears silently:

| `reason` | Meaning |
|---|---|
| `oversize` | The payload alone exceeded `annotation_max_bytes`. |
| `budget_exhausted` | This hook invocation had already spent `annotation_max_bytes_per_call`. |
| `no_context` | Called outside a framework-invoked hook. |
| `unserializable` | The payload could not be JSON-encoded (a self-referential structure, typically). |

After five drops within one hook invocation the warning log goes quiet —
one `annotation_drops_suppressed` line, then silence — so a runaway handler
cannot flood your log pipeline. **The metric keeps counting**, so alert on
that rather than on log volume:

```
rate(drakkar_recorder_annotations_dropped_total[5m]) > 0
```

The logged copy of a dropped payload is itself capped at
`ui.recorder.annotation_log_max_bytes` (default 2 KiB, `0` for unlimited)
and marked `data_truncated: true` when cut. This is the one place
truncation happens — a log line is already a lossy human-facing artifact,
and log storage is usually metered per byte.

---

## Payloads too large to inline

If a diagnostic genuinely runs to megabytes, do not raise
`annotation_max_bytes` to fit it — you would be filling the event log with
one message's worth of data. Put the blob in [the cache](cache.md) with a
TTL and annotate the key:

```python
key = f'diag/{msg.partition}/{msg.offset}'
self.cache.set(key, huge_structure, ttl=3600)
self.annotate(msg, 'full_dump', {'cache_key': key})
```

The annotation stays small and always present; the payload expires on its
own TTL and is browsable at `/api/v1/debug/cache/entry/{key}`. Note the
trade-off: the two expire on different clocks, so a trace older than the
TTL will show the key with nothing behind it.

---

## Turning it off

```yaml
ui:
  recorder:
    annotations_enabled: false
```

`self.annotate(...)` then becomes a no-op call your handler can keep making
unconditionally. The same happens automatically when the recorder is not
running or `ui.recorder.store_events` is false — with no flush loop there is
nowhere for an annotation to land, so building one buys nothing.

---

## See also

- [Observability](observability.md) — the metrics and the trace views
- [Handler](handler.md) — `ExecutorTask.labels`, the string-only sibling of annotations
- [Cache](cache.md) — for payloads too large to inline
