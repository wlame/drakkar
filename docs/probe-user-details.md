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
    `DrakkarApp(...)` calls `build_layout(handler.probe_details_model)` while
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
    view: Literal['string', 'keyvalue', 'dict', 'table', 'tables', 'tree'],
    label: str | None = None,
    group_by: tuple[str, ...] | None = None,
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
- **`group_by`** (required for and exclusive to `view='tree'`) is the
  ordered tuple of row-model field names the UI groups by — outermost
  level first, at most 4 levels.
- One of **`default`** / **`default_factory`** is required — same as any
  Pydantic field with a mutable default (`dict`, `list`) — because the
  framework constructs the empty instance itself at the start of every
  probe. A field with no default is a startup error.

### The six view kinds

| `view` | Field type | Renders as |
|---|---|---|
| `string` | a scalar (`str`, `int`, `float`, `bool`), optionally `\| None` | plain text |
| `keyvalue` | `dict[str, <scalar>]` | a flat key/value list |
| `dict` | `dict` | a JSON tree |
| `table` | `list[RowModel]`, where `RowModel` is a `BaseModel` | a collapsible, sortable table — one column per row-model field |
| `tables` | `dict[str, list[RowModel]]` | one sub-table per dict key, all sharing the row model's columns — for a *runtime-determined* number of tables (one per input file, one per external call, …) |
| `tree` | `list[RowModel]` + `group_by=(...)` | a collapsible tree grouped by the named row fields (up to 4 levels), with a sortable table of the remaining columns at each leaf |

`table` and `tables` columns come from the row model's own fields,
prettified the same way labels are — no separate column declaration
needed. Getting the view wrong for the field's type (a `table` over
`list[str]`, a `keyvalue` over `dict[str, list[int]]`) is also a startup
error, not a silent fallback.

### Grouped tables: one table per file

The layout is computed once at startup, so the *fields* of your model are
fixed — but a `tables` field lets the number of rendered tables follow the
data. Each distinct `group` you append under becomes its own sub-table,
shown under the field's heading in first-append order:

```python
class ImportedFileRow(BaseModel):
    record_id: str
    line_count: int
    status: str


class FileImportDetails(BaseModel):
    per_file_records: dict[str, list[ImportedFileRow]] = probe_field(
        section='Files', view='tables', default_factory=dict
    )
```

```python
for input_file in discovered_files:
    for record in parse(input_file):
        probe.append(
            'per_file_records',
            ImportedFileRow(record_id=record.id, line_count=record.lines, status=record.status),
            group=input_file.name,
        )
```

A probe that processed three files renders three sub-tables titled by file
name; a probe that processed one renders one. Every sub-table sorts
independently.

On the wire the field travels as an ordered array of `[group, rows]` pairs
(not a JSON object keyed by group), so first-append order survives on every
backend and in every client — including group names that look like numbers,
which a JavaScript client would otherwise re-enumerate numerically. `group`
must be a non-empty `str`; anything else is rejected as a `ProbeError`, since
an int group and its string form would collide as JSON keys.

### Tree: multi-level grouping of flat rows

When one grouping level is not enough — file → section → rule, say — declare
a `tree` field. It stays a flat `list[RowModel]`; the `group_by` tuple names
which row fields form the grouping path (outermost first, at most 4 levels):

```python
class MatchRow(BaseModel):
    file: str          # level 1
    section: str       # level 2
    rule: str          # leaf column
    score: float       # leaf column


class MyDetails(BaseModel):
    matches: list[MatchRow] = probe_field(
        section='Files', view='tree', group_by=('file', 'section'), default_factory=list
    )
```

```python
probe.append('matches', MatchRow(file=f.name, section=sec, rule=r.name, score=r.score))
```

The grouping keys travel inside each row, so filling a tree is a plain
`probe.append` — no group argument. The UI groups client-side in append
order and renders one collapsible level per key; each leaf shows a sortable
table of the columns that are **not** grouping keys. Because the tree is a
projection of a flat list, group order is append order on both backends,
same as `tables`.

`group_by` must name existing row-model fields, without duplicates — both
checked at startup, like every other layout rule.

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
| `probe.append(field, row)` | `table` / `tree` fields | Appends one row — either an instance of the row model or a `dict` that validates against it. A tree row carries its grouping keys as ordinary fields. |
| `probe.append(field, row, group='...')` | `tables` fields | Same, but into the named sub-table, creating it on first use. `group` is required for a `tables` field and rejected for a plain `table`. |
| `probe.update(field, **entries)` | `keyvalue` / `dict` fields only | Merges keys into the existing value rather than replacing it — repeated calls accumulate. |

Calling the wrong verb for a field's view (`probe.append` on a `string`
field, `probe.update` on a `table`), or `probe.append` without a `group`
on a `tables` field, does not raise into your handler — see
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

Every successful write is stamped with the hook stage that made it — one of
`deserialize`, `message_label`, `arrange`, `task_complete:<id>`,
`message_complete`, `window_complete`, or `on_error:<task_id>`, matching
whichever handler-overridable hook was running when the verb was called.
This is the same stage tag the
[cache-call log](observability.md#flight-recorder) already uses. The
User-defined tab shows this as a small badge next to each field, so when a
field ends up with a value you didn't expect, you can see at a glance which
hook (and, for `task_complete` / `on_error`, which task) last touched it —
without cross-referencing the timeline yourself.

---

## Error semantics

Two different kinds of mistake here fail very differently on purpose:

| When | Example | What happens |
|---|---|---|
| **Startup** | missing `probe_field()` annotation, `view='table'` on `list[str]`, a field with no default | `ProbeDetailsConfigError` raised from `DrakkarApp(...)` construction — the app never starts. This is a code review problem, not a runtime one. |
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

| Limit | Default | Config key | What happens past it |
|---|---|---|---|
| Writes per probe | 10,000 | `ui.probe_details.max_writes` | Every further `set` / `append` / `update` call is dropped. |
| Total serialized size per probe | 5 MB | `ui.probe_details.max_total_bytes` | Same — further writes are dropped. |

The first write past either cap records one `ProbeError` naming the
tripped cap, its configured limit, and the config key to raise (e.g.
"max_writes cap exceeded (10000 writes) — … raise
`ui.probe_details.max_writes`"), so the report itself says which knob to
turn; every write after that is silently dropped rather than raising a
fresh error each time. A probe is a
single replayed message — the defaults are generous headroom for real
handler logic. Raise them in YAML (or via `DK_UI__PROBE_DETAILS__MAX_WRITES`
and `DK_UI__PROBE_DETAILS__MAX_TOTAL_BYTES`) when a probe legitimately
produces more — e.g. one `tables` row per record across many large inputs:

```yaml
ui:
  probe_details:
    max_writes: 50000
    max_total_bytes: 20000000
```

---

## See also

- [Annotations](annotations.md) — the message/task/window-scoped sibling of this feature
- [Observability](observability.md) — the Message Probe's other panels (timeline, cache calls, sink payloads)
- [Handler](handler.md) — the hooks you can call `probe.set` / `probe.append` / `probe.update` from
- [UI Enrichment](ui-enrichment.md) — links, badges, formats, hints, detail panels, and custom cell renderers for your `probe_field()` declarations
- [Declared UI Pages](ui-pages.md) — the sibling opt-in surface for a handler's own dashboard page, reusing the same `Column` enrichment
