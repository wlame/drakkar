# Custom Timeline Events: Marking Domain Events on the Live Timeline

[Timeline tuning](ui-timeline.md) colors and labels the bars the framework
already draws — one per task. Custom timeline events are different: your
handler declares its own event *types* — a deploy, an incident window, a
batch boundary — and emits *instances* of them from inside a hook, so an
operator watching the Live page sees your domain's events lined up against
the tasks that were running at the time.

```python
self.timeline_event('deploy', text='v2.1', values={'sha': 'ab12f'})
```

Each type draws as one of three shapes: a vertical **marker** line, a
shaded **range**, or a **flag** pin — and can react to a click by doing
nothing, opening a URL, or emphasizing the tasks it correlates with.

---

## Declaring a type

Every type a handler can emit is declared under `ui.timeline.events`, up to
50 entries, each `name` unique:

```yaml
ui:
  timeline:
    events:
      - name: deployment_marker
        kind: marker
        color: purple
        line: dotted
        label: "deploy {text}"
        action: link
        link: "{grafana}/d/deploys?from={ts_ms}"
```

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | `str` | required | Identifier handlers emit by; `[a-z_][a-z0-9_]*`, unique in the list. |
| `kind` | `marker \| range \| flag` | required | Visualization family: a vertical marker line, a background time range, or a flag pin. |
| `color` | `str` | required | Timeline color name or `#rrggbb` — same eight names as [color rules](ui-timeline.md#colors). |
| `line` | `solid \| bold \| dotted` | `solid` | Marker line style. Valid only for `kind: marker`; set on any other kind, config load fails. |
| `label` | `str` | `''` | Chip template drawn at the marker top / flag pin. Only `{text}` substitutes — no `values` keys, no `link_bases`. |
| `enabled` | `bool` | `true` | `false` makes every emission of this type a silent no-op — see [Disabled types](#disabled-types-and-drop-semantics). |
| `show` | `bool` | `true` | Initial visibility of this type's toggle in the timeline's settings popover. |
| `link` | `str` | `''` | URL template opened on click when `action: link`. Required iff `action: link`; set for any other action, config load fails. |
| `action` | `none \| link \| highlight \| filter` | `none` | Click behavior — see [Correlating with tasks](#correlating-with-tasks). |

An unknown field, a bad color, or a `kind`/`line`/`link`/`action` mismatch
fails config load at boot, naming the offending type — the same "loud at
load time, not silent at render time" rule [color rules](ui-timeline.md)
follow.

---

## Emitting instances

```python
def timeline_event(
    self,
    type_name: str,
    text: str = '',
    *,
    ts: datetime | None = None,
    end_ts: datetime | None = None,
    values: Mapping[str, Any] | None = None,
    match: TimelineMatch | None = None,
) -> None
```

- **`type_name`** — must match a declared `name`. Anything else is a drop
  (see below), never an exception.
- **`text`** — instance text; substitutes into the type's `label` and
  `link` templates.
- **`ts`** / **`end_ts`** — event start/end time. `ts` defaults to now.
  `end_ts` is required for `kind: range` types and rejected for `marker`
  and `flag`; it must not precede `ts`.
- **`values`** — extra instance data, available to the `link` template
  (not to `label`) as additional `{key}` substitutions.
- **`match`** — which tasks this instance correlates with, for
  `action: highlight` / `action: filter` types; see below.

Call it from any hook the framework invokes — `arrange`,
`on_task_complete`, `on_error`, `on_message_complete`,
`on_window_complete`, and the two webapp hooks — the same set
[`annotate()`](annotations.md) works from. It rides `annotate`'s own
recorder storage, budgets, and no-context handling unchanged: same `events`
table, same rotation/archive lifetime, same
`ui.recorder.annotation_max_bytes` / `annotation_max_bytes_per_call` caps,
same `ui.recorder.annotations_enabled` master switch. See
[Annotations](annotations.md#where-the-data-lives-and-for-how-long) for
what that means for retention and budgets — nothing here changes it.

### Cookbook

**A marker at a batch boundary** — one pin per Kafka batch, no click
behavior:

```python
async def arrange(self, messages, pending):
    self.timeline_event('ripgrep_batch_started_marker', text=str(messages[0].offset))
    ...
```

**A highlighted range over a request's processing window** — shows how
long one message's whole fan-out took, and clicking it emphasizes every
task it produced:

```python
async def on_message_complete(self, group):
    now = datetime.now(tz=UTC)
    self.timeline_event(
        'scan_window_processing_range',
        text=f'{len(group.results)} tasks',
        ts=now - timedelta(seconds=group.duration_seconds),
        end_ts=now,
        match=dk.TimelineMatch(offsets=((group.source_message.partition, group.source_message.offset),)),
    )
```

**A flag pin linking out to an external service** — opens Kafka-UI on this
batch's first message when clicked:

```python
async def arrange(self, messages, pending):
    self.timeline_event(
        'kafka_ui_batch_link_flag',
        text=str(messages[0].offset),
        values={'partition': str(messages[0].partition)},
    )
```

with a type declared as `action: link`, `link:
"{kafka_ui}/ui/clusters/.../messages?seekType=OFFSET&seekTo={partition}::{text}"`,
and `{kafka_ui}` resolved from [`ui.link_bases`](ui-enrichment.md#uilink_bases-the-environmentcode-split).

The [integration test harness](integration.md) runs all three — see
`integration/worker/drakkar.yaml`'s `ui.timeline.events` block and the
matching emissions in `integration/worker/handler.py`'s `arrange()` and
`on_message_complete()`.

---

## Correlating with tasks

`action: highlight` and `action: filter` types emphasize the tasks a click
correlates them with, dimming the rest — the same emphasis/dimming
[highlight and filter label roles](ui-timeline.md#label-roles) use.
`TimelineMatch` names which tasks, setting **exactly one** of three fields:

```python
dk.TimelineMatch(offsets=((partition, offset), ...))   # tasks from these (partition, offset) pairs
dk.TimelineMatch(label=(key, value))                    # tasks whose label[key] == value
dk.TimelineMatch(window_id=window_id)                   # accepted, but see below
```

Passing more than one field, or none, is a drop (`bad_shape`).

**Omit `match` and it auto-fills from the current hook's offsets** — the
partition and source-message offsets of whatever the running hook is
anchored to. This is enough for the common case (a hook already scoped to
one message or one window) and is why the marker and flag examples above
never pass `match` at all: their types use `action: none` / `action: link`,
which never need one. When no offsets are available in the hook context
either (a hook with genuinely nothing to correlate against), the emission
drops as `bad_shape`.

!!! warning "`window_id` never highlights anything"
    `TimelineMatch(window_id=...)` is accepted at emit time — it is a valid
    shape — but the Live page's task view carries no `window_id` field to
    match against, so a `window_id` match highlights and filters nothing.
    Use `offsets` (works from any hook) or `label` (works when every task
    you want to correlate shares a label value, like the `request` label
    in [Timeline Tuning's worked example](ui-timeline.md#worked-example)).

---

## Disabled types and drop semantics

`self.timeline_event(...)` never raises and never affects processing —
exactly like `annotate()`. What happens to a call depends on why it can't
be recorded:

- **`enabled: false`** on the declared type is silent: no metric, no log.
  This is deliberate operator config (an event type turned off for now),
  not a mistake, so it stays quiet the way [color rules](ui-timeline.md)
  and label roles do when unbound.
- **Everything else is loud.** An unknown `type_name`, or a malformed
  instance (wrong shape for the type's `kind`, an ill-formed `match`),
  increments `drakkar_recorder_annotations_dropped_total{reason=...}` — the
  same counter and metric [annotations](annotations.md#budgets-and-what-happens-when-you-exceed-them)
  use, since it is one shared drop budget — and logs one
  `timeline_event_dropped` error, every time, uncapped. Unlike annotation
  drops, these are not rate-limited: a bad `type_name` or malformed
  `match` is a rare config or code mistake, not a hot path a runaway hook
  could flood.

| `reason` | Meaning |
|---|---|
| `unknown_type` | `type_name` doesn't match any declared `name`. |
| `bad_shape` | Wrong `ts`/`end_ts` shape for the type's `kind`, or `match` set to zero or multiple fields, or auto-fill found nothing to correlate. |
| `oversize` / `budget_exhausted` / `no_context` / `unserializable` | From the shared annotator layer underneath — see [Annotations](annotations.md#budgets-and-what-happens-when-you-exceed-them). |

---

## Degradation

A `link` template that references a variable no instance can fill —
`{end_ts_ms}` on a `marker`/`flag` type, a `values` key an emission never
set, a `link_bases` name missing from `ui.link_bases` — renders that
instance **without a link** rather than a broken URL: the whole
substitution is all-or-nothing, resolved client-side per instance. Unlike
[probe-details link templates](ui-enrichment.md#unresolved-bases-a-warning-not-an-error),
there is no startup warning for this — the backend never cross-checks a
type's `link` template against `ui.link_bases` at config load, so a typo'd
or missing base degrades silently to "no link" on every affected instance.
`text` and every `values` entry are percent-encoded before substitution;
`ts_ms`, `end_ts_ms`, and `link_bases` values are inserted raw.

A worker with no declared types at all (empty `ui.timeline.events`, the
default) is a fully valid config — every `self.timeline_event(...)` call
just drops as `unknown_type`, the same low cost calling `self.annotate(...)`
has when nobody looks at the annotation. An older UI bundle that predates
this feature simply never asks for the declarations and draws no overlay;
a newer UI against an older backend with no `timeline.events` key on
identity behaves the same way.

---

## See also

- [Timeline Tuning](ui-timeline.md) for the task bars, color rules, and
  label roles this feature complements.
- [Annotations](annotations.md) for the shared storage, budgets, and
  best-effort drop policy this feature rides.
- [UI Enrichment](ui-enrichment.md#link-templates) for `ui.link_bases` and
  the same trusted-base/percent-encoded-value split link templates use.
- [Integration Tests](integration.md) for the worked example's full
  handler.
