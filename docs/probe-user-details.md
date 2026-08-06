# Probe User Details: Your Own Tab in the Message Probe

[Annotations](annotations.md) attach diagnostics to one message, one task, or
one window. Sometimes what you want to explain is bigger than any single one
of those — the whole story of how this run of the handler turned this
message into these tasks: which candidates it considered, the notes it left
itself along the way, the running counters it built up across every hook.

Register a Pydantic model on your handler and the Message Probe grows a
second, **User-defined** tab that renders exactly that story, laid out the
way you declared it — no JSON dump, no ad hoc `print()` you have to remember
to remove.

```python
async def arrange(self, messages, pending):
    probe.set(strategy_note='picking the highest-scoring candidate per message')
    ...
```

Outside a probe, that same line does nothing — see
[No-op outside a probe](#no-op-outside-a-probe) below.

---

## Registering the model

Declare a `BaseModel` whose fields all use `probe_field()`, and point your
handler's `probe_details_model` at it:

```python
from pydantic import BaseModel
from drakkar.probe import probe_field


class ScoredCandidateRow(BaseModel):
    candidate_id: str
    score: float


class MyProbeDetails(BaseModel):
    strategy_note: str | None = probe_field(section='Arrange', view='string', default=None)
    counters: dict[str, int] = probe_field(section='Arrange', view='keyvalue', default_factory=dict)
    context: dict = probe_field(section='Arrange', view='dict', default_factory=dict)
    candidates: list[ScoredCandidateRow] = probe_field(section='Tasks', view='table', default_factory=list)


class MyHandler(BaseDrakkarHandler[...]):
    probe_details_model = MyProbeDetails
```

That is the whole opt-in. Leave `probe_details_model` at its default of
`None` and the User-defined tab does not appear.

!!! warning "Validated at startup, not at first probe"
    `Drakkar(...)` calls `build_layout(handler.probe_details_model)` while
    constructing the app, so a model that breaks the rules below raises
    `ProbeDetailsConfigError` immediately — a code-owned mistake surfaces at
    boot, not three weeks later when someone finally opens the probe tab.

---

## `probe_field()` reference

Every field of a probe-details model must be declared with `probe_field()` —
a plain field with no annotation is itself a startup error, since the
framework has no section or view to put it under.

```python
def probe_field(
    *,
    section: str,
    view: Literal['string', 'keyvalue', 'dict', 'table'],
    label: str | None = None,
    default: Any = ...,
    default_factory: Callable[[], Any] | None = None,
) -> Any
```

- **`section`** groups fields into a titled block in the tab. Sections
  appear in first-declaration order; fields within a section keep their
  declaration order too.
- **`view`** picks how the field renders — see the table below.
- **`label`** overrides the auto-generated heading. Left unset, `counters`
  becomes "Counters" and `strategy_note` becomes "Strategy note" — the
  field name with underscores turned to spaces and the first letter
  capitalized.
- One of **`default`** / **`default_factory`** is required — same as any
  Pydantic field with a mutable default (`dict`, `list`) — because the
  framework constructs the empty instance itself at the start of every
  probe. A field with no default is a startup error.

### The four view kinds

| `view` | Field type | Renders as |
|---|---|---|
| `string` | a scalar (`str`, `int`, `float`, `bool`), optionally `\| None` | plain text |
| `keyvalue` | `dict[str, <scalar>]` | a flat key/value list |
| `dict` | `dict` | a JSON tree |
| `table` | `list[RowModel]`, where `RowModel` is a `BaseModel` | a collapsible, sortable table — one column per row-model field |

`table` columns come from the row model's own fields, prettified the same
way labels are — no separate column declaration needed. Getting the view
wrong for the field's type (a `table` over `list[str]`, a `keyvalue` over
`dict[str, list[int]]`) is also a startup error, not a silent fallback.

---

## Filling it in: `probe.set` / `probe.append` / `probe.update`

Three module-level verbs write into whatever probe is currently running,
from anywhere in your handler's call graph — hooks, helper methods, code
several calls deep. None of them take a probe handle; there is nothing to
thread through.

```python
from drakkar import probe


class MyHandler(BaseDrakkarHandler[...]):
    probe_details_model = MyProbeDetails

    async def arrange(self, messages, pending):
        tasks = []
        for msg in messages:
            candidates = self.score_candidates(msg)
            chosen = max(candidates, key=lambda c: c.score)
            probe.set(strategy_note=f'picked {chosen.candidate_id}, highest score')
            probe.update('context', message_count=len(messages))
            for c in candidates:
                probe.append('candidates', ScoredCandidateRow(candidate_id=c.candidate_id, score=c.score))
            tasks.append(self.build_task(msg, chosen))
        return tasks

    async def on_task_complete(self, result):
        probe.update('counters', tasks_completed=1)
        return None

    async def on_message_complete(self, group):
        probe.update('counters', messages_completed=1)
        return None
```

| Verb | Targets | Behavior |
|---|---|---|
| `probe.set(**fields)` | any field | Assigns each keyword as the field's whole new value. Validated against the model like any Pydantic assignment. |
| `probe.append(field, row)` | `table` fields only | Appends one row — either an instance of the row model or a `dict` that validates against it. |
| `probe.update(field, **entries)` | `keyvalue` / `dict` fields only | Merges keys into the existing value rather than replacing it — repeated calls accumulate. |

Calling the wrong verb for a field's view (`probe.append` on a `string`
field, `probe.update` on a `table`) does not raise into your handler — see
[Error semantics](#error-semantics) below.

---

## No-op outside a probe

`probe.set` / `probe.append` / `probe.update` are safe to call
unconditionally from production hook code — the same shape as calling
`logger.debug(...)` on every request. There is no `if probing:` guard to
remember, and nothing to strip out before shipping.

Outside an active probe, each verb checks one contextvar, finds it unset,
and returns — no model construction, no validation, no allocation beyond
the check itself. The cost in a production hot path is a single
`ContextVar.get()` per call.

---

## Stage badges

Every successful write is stamped with the hook stage that made it —
`arrange`, `task_complete:<id>`, `message_complete`, `window_complete` — the
same stage tag the [cache-call log](observability.md#flight-recorder)
already uses. The User-defined tab shows this as a small badge next to each
field, so when a field ends up with a value you didn't expect, you can see
at a glance which hook (and, for `task_complete`, which task) last touched
it — without cross-referencing the timeline yourself.

---

## Error semantics

Two different kinds of mistake here fail very differently on purpose:

| When | Example | What happens |
|---|---|---|
| **Startup** | missing `probe_field()` annotation, `view='table'` on `list[str]`, a field with no default | `ProbeDetailsConfigError` raised from `Drakkar(...)` construction — the app never starts. This is a code review problem, not a runtime one. |
| **Probe time** | `probe.append('strategy_note', ...)` (wrong view), a row that fails the row model's own validation, the write cap exceeded | Never raises into your handler and never crashes the probe. The failed write is recorded as a `ProbeError` and shown in the report's **Errors** panel, exactly like an exception your handler itself raised. |

The probe-time case matters most: a bad `probe.append()` call is a
diagnostics bug, not a processing bug, and it must never be able to turn a
successful `arrange()` into a failed probe. One bad field also can't take
down the rest — `to_user_details()` serializes each field independently,
so an unserializable value in one field shows as
`<unserializable: TypeError>` in that field alone.

---

## Caps

Two limits guard against a handler that (accidentally or not) writes
unbounded diagnostics during a single probe run:

| Limit | Value | What happens past it |
|---|---|---|
| Writes per probe | 10,000 | Every further `set` / `append` / `update` call is dropped. |
| Total serialized size per probe | 5 MB | Same — further writes are dropped. |

The first write past either cap records one `ProbeError` ("write cap
exceeded") so it's visible in the report; every write after that is
silently dropped rather than raising a fresh error each time. A probe is a
single replayed message — these numbers are generous headroom for real
handler logic, not a budget you should expect to plan around.

---

## See also

- [Annotations](annotations.md) — the message/task/window-scoped sibling of this feature
- [Observability](observability.md) — the Message Probe's other panels (timeline, cache calls, sink payloads)
- [Handler](handler.md) — the hooks you can call `probe.set` / `probe.append` / `probe.update` from
