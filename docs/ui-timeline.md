# Timeline Tuning: History Depth, Color Rules, and Label Roles

The Live view's timeline strip shows one bar per task, scrolling in real time. Out of the
box it keeps a fixed window of recent tasks colored by status. `ui.timeline` config lets an
operator tune how much history it keeps, recolor bars from task labels or built-in task
fields, and bind up to five task labels to on-screen roles (a small tag, a caption, a
numeric highlight, a substring filter, and vertical batch markers) — all without writing
any client-side code.

Every field below is validated at config load: an unknown operator, field, or color fails
worker boot with the offending value named in the error, rather than
silently never matching at render time.

## How much history the timeline keeps

Two independent bounds decide what the timeline shows: how many tasks, and how far back.
**Count governs** — the row cap is the primary limit, and the time window is a ceiling on
top of it, not the other way around.

- **Depth (row count).** `GET /api/v1/recent-tasks` defaults its `limit` to
  `history_factor` &times; the executor pool's `max_executors` (or &times;8 when no pool is
  attached yet), capped at 100000 since neither factor has a ceiling of its own. A caller
  can pass an explicit `limit` (1-100000) to override the derived default.
- **Age ceiling (time window).** The `minutes` query parameter accepts up to 1440 (24h),
  but is always clamped down to `ui.timeline.max_age_minutes` — a per-worker override,
  tighter by default at 60 minutes. Raising `minutes` past `max_age_minutes` has no effect
  until the config value itself is raised.

**Worked numbers.** With the framework defaults (`history_factor: 100`,
`executor.max_executors: 4`), the timeline keeps at most 100 &times; 4 = **400 tasks**,
going back at most **60 minutes**. This is deliberately smaller than the old fixed
event-based ceiling — the timeline is now sized for what an operator can actually scroll
through, and depth is a config knob rather than a hardcoded constant. Raise
`history_factor` for a worker with a small pool that still handles bursty traffic; raise
`max_age_minutes` (up to the 1440 ceiling) to look further back in time.

```yaml
ui:
  timeline:
    history_factor: 100    # depth = history_factor x executor.max_executors (or x8 if no pool), capped at 100000
    max_age_minutes: 60    # 1-1440; oldest task age the timeline shows
```

## Color rules

`color_rules` is a first-match-wins list: the first rule whose `when` conditions all match
(logical AND) colors the bar. No match falls through to an implicit default (below). At
most 50 rules are allowed — more than that fails config load with a boot error rather than
silently accepting a list no operator can realistically reason through.

### Condition grammar

Each condition sets exactly one of `label` or `field` — never both, never neither — plus an
`op` and, for every op except `exists`/`missing`, a `value`.

| op | needs `value`? | meaning |
|---|---|---|
| `eq` | yes | equals |
| `ne` | yes | not equals |
| `contains` | yes | substring match |
| `prefix` | yes | starts with |
| `gt` / `ge` | yes | greater than / greater-or-equal |
| `lt` / `le` | yes | less than / less-or-equal |
| `exists` | no | the label/field has a value |
| `missing` | no | the label/field is absent |

- **`label: <key>`** reads a task label — a handler-defined string, set via
  [`message_label()`](handler.md#task-labels) or `ExecutorTask.labels`. Any key is legal;
  there is no fixed vocabulary, and any op may target a label (labels carry no declared
  type — see [Gotchas](#gotchas) below for how numeric ops treat them).
- **`field: <name>`** reads a built-in task field, restricted to a fixed, typed vocabulary:

  | string fields (`eq`/`ne`/`contains`/`prefix`/`exists`/`missing` only) | numeric fields (`eq`/`ne`/`gt`/`ge`/`lt`/`le`/`exists`/`missing` only) |
  |---|---|
  | `status`, `origin`, `client_name` | `exit_code`, `duration`, `stdout_size`, `stdout_lines`, `stdin_size`, `stdin_lines`, `partition` |

  Applying `contains`/`prefix` to a numeric field, or `gt`/`ge`/`lt`/`le` to a string field,
  fails config load — the config layer catches the mismatch once at boot instead of the
  rule silently never matching.

A single-condition rule can write `when` as one mapping instead of a one-element list —
`when: {field: status, op: eq, value: failed}` is equivalent to
`when: [{field: status, op: eq, value: failed}]`. Multiple entries in the list are AND'd
together, so a rule needs every condition to hold, not just one.

### Colors

`color` is one of eight named colors, or a literal `#rrggbb` hex:

| name | hex | | name | hex |
|---|---|---|---|---|
| `green` | `#34d399` | | `gray` | `#9ca3af` |
| `red` | `#f87171` | | `lightgray` | `#d1d5db` |
| `yellow` | `#fbbf24` | | `purple` | `#a78bfa` |
| `blue` | `#60a5fa` | | `orange` | `#fb923c` |

Every rule shows up in the toolbar's color key — a row of small swatches (the three status
colors first, then one per rule). The swatches carry no text; hover one to read its name
(the rule's `name`, or its generated condition text when unnamed).

### Implicit fallback

When no configured rule matches, the bar falls back to a status color: **HTTP-origin tasks**
(`origin == 'http'`, from the [synchronous webapp pipeline](webapp.md)) draw `#9c27b0` — a
different purple than the named `purple` above, carried over from the timeline's previous
hardcoded styling — then everything else falls to a plain status color: green (`completed`),
red (`failed`), yellow (`running`).

```yaml
ui:
  timeline:
    color_rules:
      - name: failed tasks
        when: {field: status, op: eq, value: failed}
        color: red
      - name: slow (>5s)
        when: {field: duration, op: gt, value: 5}
        color: orange
```

## Label roles

`labels` binds up to five task-label keys to special on-screen roles. Any role left `''`
(the default) is unbound and has no effect. A role's key doesn't need to exist on every
task — tasks without the bound label simply skip that role.

- **`tag`** — a short chip drawn at the bar's **right edge**, truncated to 16 characters
  with an ellipsis. Tried first when both `tag` and `caption` are bound: if the bar is too
  narrow for the tag, **neither** the tag nor the caption draws — a bound tag that doesn't
  fit blocks the caption too, it doesn't fall back to caption-only.
- **`caption`** — text drawn at the bar's **left edge**, in whatever space is left over
  after the tag (if any), truncated to 32 characters. Second priority — a caption draws on
  its own only when no `tag` role is bound, or the task lacks the tag's label entirely (so
  there's no tag text to try fitting in the first place).
- **`highlight`** — adds a numeric input to the toolbar (`<key> >`). Typing a threshold
  emphasizes every task whose bound label, parsed as a number, exceeds it (a solid outline,
  full opacity) and dims every other task to low opacity. A task whose label doesn't parse
  as a number never matches. Clearing the input turns emphasis/dimming off entirely, so an
  untouched timeline looks exactly as before.
- **`filter`** — adds a text input to the toolbar (`<key> &ni;`). Typing text emphasizes
  every task whose bound label contains that substring (case-insensitive) and dims the
  rest, the same emphasis/dimming behavior as `highlight`. Both `highlight` and `filter` can
  be active at once — a task must satisfy whichever of them are non-empty.
- **`marker`** — draws a vertical pin above the strip for each distinct value of the bound
  label seen among currently-loaded tasks, positioned at that value's earliest start time.
  Pins landing within 12px of each other collapse into one merged pin listing every merged
  value, so a burst of near-simultaneous distinct values doesn't draw as unreadable
  overlapping labels.

## Browser overrides

Each viewer can override or disable any role for their own browser, independent of the
backend's `ui.timeline.labels` config and independent of other viewers. Click the gear icon
in the timeline toolbar to open the timeline settings; the role picker there lists every label key seen across
currently-loaded tasks, plus whichever key the backend currently binds to that role (kept
selectable even if no visible task currently carries it), so a bound key never disappears
from the picker. Choosing "(none)" explicitly disables a role even if the backend binds one;
**Reset** returns one role to the backend's binding; **Reset all** clears every override at
once.

Overrides are stored in the browser's `localStorage`, keyed by worker ID — they follow one
worker on one browser, are not synced across browsers or devices, and are lost if the
browser clears site storage. A small dot appears on the gear icon when any role is
overridden, and next to a role's name in the picker when that specific role differs from
the backend's default — so an operator can tell at a glance whether they're looking at the
worker's configured roles or their own local tweaks.

## Worked example

The [integration test harness](integration.md) labels every scan task with six values (see
`integration/worker/handler.py`) and configures this `ui.timeline` block in
`integration/worker/drakkar.yaml`, under the existing `ui:` section:

```yaml
  # Timeline tuning: history depth, first-match-wins bar color rules, and
  # which task label feeds each special role. tag=file_size shows the
  # human size on the bar; caption=file_name names it; highlight=lines
  # flags line count on hover; filter=module lets the UI filter by
  # scanned directory; marker=request draws one pin per Kafka batch
  # boundary (see handler.py's arrange()).
  timeline:
    history_factor: 100
    max_age_minutes: 60
    color_rules:
      - name: empty output
        when: {field: stdout_size, op: eq, value: 0}
        color: lightgray
      - name: big file
        when: {label: file_size_bytes, op: gt, value: 10240}
        color: blue
      - name: vendored code
        when: {label: module, op: contains, value: vendor}
        color: purple
    labels:
      tag: file_size
      caption: file_name
      highlight: lines
      filter: module
      marker: request
```

What this produces on screen, rule by rule (first match wins):

1. **`empty output`** — a task whose `stdout_size` field is exactly `0` draws
   **lightgray**. Because `stdout_size` stays `null` until a task completes (see
   [Gotchas](#gotchas)), this only ever matches a *finished* task that produced no output —
   never a still-running or a failed one.
2. **`big file`** — otherwise, a task whose `file_size_bytes` label exceeds `10240` (10KB)
   draws **blue**. A big file that also happened to produce zero output still shows
   lightgray from rule 1, since that rule is checked first.
3. **`vendored code`** — otherwise, a task whose `module` label contains `"vendor"` draws
   **purple**. Checked last, so it only shows for vendored-path scans that neither matched
   an empty output nor a big file.
4. **Implicit fallback** — every other task (the common case: a small, non-vendored file
   with real output) falls through to the plain status color, since this harness's scan
   tasks always have `origin == 'kafka'`: green while `completed`, red if `failed`, yellow
   while `running`.

And the five label roles:

- **`tag: file_size`** — the bar's right edge shows the human-readable size, e.g. `12.4K`.
- **`caption: file_name`** — the bar's left edge shows the scanned file's base name, e.g.
  `app.py`, space permitting.
- **`highlight: lines`** — the toolbar gains a `lines >` box; typing `500` emphasizes every
  task that scanned a file with more than 500 lines.
- **`filter: module`** — the toolbar gains a `module &ni;` box; typing `vendor` emphasizes
  every task whose scanned directory's module name contains `vendor` — the same substring
  the `vendored code` color rule checks, but as an interactive, viewer-driven filter rather
  than a fixed bar color.
- **`marker: request`** — one vertical pin per distinct `<partition>:<first-offset>` value,
  i.e. one pin per Kafka `arrange()` batch, since every task built from the same batch
  shares the same `request` label.

## Gotchas

- **Labels are strings, even when they look numeric.** `file_size_bytes` and `lines` are
  ordinary label values (Python `str`, Go `string`). `gt`/`ge`/`lt`/`le` require *both*
  sides to parse as numbers — a label value that doesn't parse makes the condition false
  rather than raising an error; it just never matches. Parsing uses JavaScript's
  `parseFloat`, which reads only a *prefix* of the string rather than validating the whole
  thing — `"12.4K"` parses as `12.4` and compares as such, which can be surprising; store a
  clean decimal string like `file_size_bytes` for reliable numeric rules. `eq`/`ne` are more
  forgiving: they compare numerically when both sides parse, but fall back to a plain string
  comparison when either side doesn't — so `{label: env, op: eq, value: staging}` still
  matches a non-numeric label value like `"staging"` by comparing text, not silently failing
  the way `gt`/`ge`/`lt`/`le` would.
- **`stdout_size` is `null` while a task is running, and stays `null` if it fails.** Only a
  `task_completed` event populates it — a `task_failed` task never gets one, so it stays
  `null` rather than reporting a stale or default value. `{field: stdout_size, op: eq,
  value: 0}` therefore matches only *completed, empty-output* tasks, never a running or a
  failed one.
- **Rules on WS-only fields color a task only once the page has observed it live.**
  `exit_code`, `stdout_lines`, `stdin_lines`, and `stdin_size` are delivered on WebSocket
  frames only — the `/recent-tasks` resync that (re)populates the timeline does not carry
  them, so a task loaded purely from a resync (e.g. one that finished before the page
  connected) has these fields `null` until a live WS event for it arrives. A rule keyed on
  one of them, like `{field: exit_code, op: eq, value: 1}`, therefore won't color such a
  task. `stdout_size` is the exception: the resync row carries it too, so a rule keyed on
  `stdout_size` works even for a task the page never saw live.
- **A task can change color when it completes.** While running, a task typically has no
  `exit_code`/`duration`/`stdout_size` yet, so it usually draws under the implicit status
  fallback (yellow). The moment it completes and those fields populate, a rule that reads
  one of them (like `empty output` above) can start matching — the bar recolors in place,
  without moving, the next time the timeline re-renders.
- **Markers derive from the currently-loaded tasks, not full history.** A marker pin exists
  only as long as at least one task carrying that label value is still within the loaded
  window (bounded by depth and `max_age_minutes`, same as bars). Once every task from a
  given batch ages out, its pin disappears along with them.

## See also

- [Observability — Operator UI](observability.md#operator-ui) for the rest of the Live view.
- [Handler — Task labels](handler.md#task-labels) for how to set labels a timeline rule or
  role can read.
- [Configuration](configuration.md#ui-flight-recorder-ui) and
  [Config Reference](config-reference.md#ui-flight-recorder-ui) for the full `ui.*` field
  list.
- [Integration Tests](integration.md) for the worked example's full handler.
