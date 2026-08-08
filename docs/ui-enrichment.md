# UI Enrichment: Links, Badges, Formats, and Detail Panels

[Probe User Details](probe-user-details.md) turns a handler-registered model
into a tab in the Message Probe. On its own, a `table` field renders every
row as plain text — useful, but a build id is more useful as a link to the
CI system that produced it, an outcome is more useful as a colored badge
than the string `"failed"`, and a duration in milliseconds is more useful
formatted than raw.

**UI enrichment** is a set of optional `probe_field()` and `Column`
arguments that add exactly that presentation, without changing what the
field *is* — a `table` field enriched with a link template is still a
`table` field; the enrichment only changes how one column renders.

```python
class BuildRow(BaseModel):
    build_id: str
    job_name: str
    duration_ms: int
    outcome: str


class MyProbeDetails(BaseModel):
    builds: list[BuildRow] = probe_field(
        section='Builds',
        view='table',
        default_factory=list,
        columns={
            'build_id': Column(link_template='{jenkins}/job/{row.job_name}/{value}', hint='Open build {value}'),
            'duration_ms': Column(format='duration_ms'),
            'outcome': Column(badge_colors={'passed': 'green', 'failed': 'red', '*': 'gray'}),
        },
    )
```

That declaration alone gets you a clickable build id, a human-readable
duration, and a colored outcome pill — no client-side code, no per-row
formatting logic in the handler.

---

## Link templates

A link template turns a scalar value (`string` view) or a table column into
a clickable link. The grammar has three token kinds:

| Token | Resolves to | Where it's valid |
|---|---|---|
| `{value}` | The field's (or column's) own value | Anywhere |
| `{row.<field>}` | Another field of the same row | Table/tables/tree columns and detail elements only |
| `{<base>}` | A named URL base from `ui.link_bases` | Anywhere |

```python
link_template='{jenkins}/job/{row.job_name}/{value}'
```

This resolves `{jenkins}` from config, `{row.job_name}` from the sibling
`job_name` field on the same row, and `{value}` from the column's own
`build_id`.

### `ui.link_bases`: the environment/code split

A template names *which* base to use (`{jenkins}`), never the base URL
itself — that lives in config, so the same handler code produces working
links in every environment without a redeploy:

```yaml
ui:
  link_bases:
    jenkins: 'https://jenkins.internal.example.com'
    jira: 'https://jira.internal.example.com'
```

- Base names are lower-case identifiers (`^[a-z][a-z0-9_]*$`).
- Values must start with `http://` or `https://`; a trailing `/` is
  stripped, so a template's own leading `/job/...` doesn't produce a
  doubled slash.
- Templates are validated at startup regardless of config — a malformed
  template, an unknown token shape, or a `{row.*}` reference to a
  non-existent row field is a `ProbeDetailsConfigError` that stops the app
  from starting. Whether a *named base* actually resolves is not checked
  here, because config varies per environment.

### Unresolved bases: a warning, not an error

A template that references a base your config doesn't define is not a
startup error — different environments legitimately configure different
subsets of `ui.link_bases`. Instead:

- `DrakkarApp(...)` logs **one** `probe_details_link_bases_missing` warning
  at startup, naming every missing base, so the gap is visible in logs
  instead of discovered by an operator clicking a dead link. The warning is
  skipped entirely when `ui.enabled` is `false` — no UI means no link is
  ever rendered.
- At render time, the UI resolves what it can and falls back to **plain
  text** for anything it can't — a value with an unresolved link never
  becomes a dead anchor.

Percent-encoding applies to the `{value}` and `{row.*}` substitutions in a
URL, so a value containing `/` or spaces can't break the link's path
structure. The base itself is inserted verbatim — it's a trusted URL prefix
from config, not row or user data, and encoding it would break every link
that relies on its own `/` structure. Display text — hints and detail
titles — substitutes the same tokens unencoded, since it's read, not
navigated.

---

## Badges

`view='badge'` renders a scalar as a colored pill instead of plain text.
`badge_colors` maps each expected value to one of six palette colors, plus
an optional `'*'` fallback for anything not explicitly listed:

| Color |
|---|
| `green` |
| `red` |
| `yellow` |
| `blue` |
| `gray` |
| `purple` |

```python
release_state: str = probe_field(
    section='Enrichment',
    view='badge',
    badge_colors={'shipped': 'green', 'blocked': 'red', '*': 'gray'},
    default='',
)
```

A value that matches no key and has no `'*'` fallback still renders as a
badge — a neutral pill in the base (uncolored) badge style, never plain
text and never a broken or blank pill.

Table columns get badges the same way, via `Column(badge_colors=...)`
instead of a top-level `view='badge'` field — see
[Column subsets](#column-subsets-and-per-column-options) below.

!!! warning "`badge_colors` and `link_template` are exclusive on one column"
    A `Column` (or top-level field) that declares **both** `badge_colors`
    and `link_template` renders as a badge — the link is silently not
    rendered. This is intentional, not validated as an error, but it means
    the two options don't compose: pick one presentation per column.

---

## Formats

`format` applies a client-side display transform to a `string`-view scalar
or table column, without changing the underlying value:

| `format` | Field type | Before (raw value) | After (rendered) |
|---|---|---|---|
| `duration_ms` | `int` (milliseconds) | `65000` | `1 m 5 s` |
| `bytes` | `int` (bytes) | `10485760` | `10.0 MiB` (binary units) |
| `timestamp` | `str` (ISO-8601) | `"2026-08-09T10:00:00Z"` | `2026-08-09 10:00:00.000` |
| `number` | `int` or `float` | `1234567` | `1,234,567` |

```python
duration_ms: int = probe_field(section='Builds', view='string', format='duration_ms', default=0)
```

Hovering a formatted value shows the raw underlying value in a tooltip, so
the exact number is always one hover away — formatting never hides data,
only presents it more readably.

---

## Hints

`hint` attaches a small tooltip to a value or column header, using the same
template grammar as link templates (`{value}` / `{row.<field>}` /
`{<base>}`), rendered unencoded as display text:

```python
Column(link_template='{jenkins}/job/{row.job_name}/{value}', hint='Open build {value} in Jenkins')
```

`hint` is valid on `string`-view fields and on table columns; it is not
valid on row-bearing top-level fields directly (`table`, `tables`, `tree`)
— put it on the relevant `Column` instead.

---

## Column subsets and per-column options

By default, a `table`/`tables`/`tree` field's columns are every field of
the row model, in declaration order, with an auto-generated label. `columns`
overrides that in two ways:

- **`columns=['build_id', 'duration_ms']`** — a subset, in the given order.
  Every name must be a real row-model field; unknown names are a startup
  error.
- **`columns={'build_id': Column(...), 'duration_ms': Column(...)}`** — the
  keys are the displayed subset (in dict order), and each `Column` carries
  that column's enrichment.

```python
class Column(BaseModel):
    label: str | None = None
    link_template: str | None = None
    badge_colors: dict[str, str] | None = None
    format: Literal['duration_ms', 'bytes', 'timestamp', 'number'] | None = None
    hint: str | None = None
```

`Column.label` overrides that one column's auto-generated heading, the same
way `probe_field(label=...)` does for a top-level field. Every other
`Column` option means exactly what it means at the top level, scoped to
that column.

---

## Detail panels

A `detail` on a `table`/`tables`/`tree` field turns each row into a
clickable entry (a **›** affordance) that opens a right-side panel showing a
richer view of that one row — built from `Detail`, `Element`, and `Link`:

```python
class Link(BaseModel):
    label: str
    template: str


class Element(BaseModel):
    view: Literal['string', 'keyvalue', 'table', 'links']
    field: str | None = None      # required for every view except 'links'
    label: str | None = None
    links: list[Link] | None = None  # required for view='links', forbidden otherwise


class Detail(BaseModel):
    title: str | None = None
    elements: list[Element]
```

A worked example — a build row whose detail panel shows the job name, its
labels, its steps as a sub-table, and a link out to the Jenkins job:

```python
class BuildStepRow(BaseModel):
    step_name: str
    status: str


class BuildRow(BaseModel):
    build_id: str
    job_name: str
    duration_ms: int
    outcome: str
    labels: dict[str, str]
    steps: list[BuildStepRow]


class MyProbeDetails(BaseModel):
    builds: list[BuildRow] = probe_field(
        section='Enrichment',
        view='table',
        default_factory=list,
        columns={
            'build_id': Column(link_template='{jenkins}/job/{row.job_name}/{value}', hint='Open build {value}'),
            'duration_ms': Column(format='duration_ms'),
            'outcome': Column(badge_colors={'passed': 'green', 'failed': 'red', '*': 'gray'}),
        },
        detail=Detail(
            title='Build {row.build_id}',
            elements=[
                Element(field='job_name', view='string'),
                Element(field='labels', view='keyvalue'),
                Element(field='steps', view='table'),
                Element(view='links', links=[Link(label='Jenkins job', template='{jenkins}/job/{row.job_name}')]),
            ],
        ),
    )
```

Clicking any row in the **Builds** table opens a panel titled with that
row's `build_id`, showing its job name as text, its labels as a flat
key/value list, its steps as a sub-table, and a link to the Jenkins job page
for that build.

Rules, enforced at startup:

- `field` names a field of the *row model* — checked against the row
  model's actual fields, the same way `columns` subset names are.
- `view='links'` elements carry `links` and no `field`; every other view
  carries `field` and no `links`.
- `title` uses the same template grammar as everything else, with
  `{row.<field>}` resolved from the clicked row.
- `link_template` and `detail` are mutually exclusive on the same field —
  a `string`-view field can have a link, and a row-bearing field can have a
  detail panel, but nothing has both.

---

## Validation: what fails at boot vs what degrades at render

The same two-tier error model as the rest of probe-details applies here,
with one addition specific to enrichment:

| When | Example | What happens |
|---|---|---|
| **Startup** | `badge_colors` on a non-`badge` view, an unknown badge color name, a malformed template, `columns` naming a field the row model doesn't have, a `detail` element's `field` not on the row model, `link_template` and `detail` both set | `ProbeDetailsConfigError` — the app never starts. |
| **Startup** | A duplicate column name in `columns=[...]` | `ProbeDetailsConfigError` — the app never starts. |
| **Startup** | An empty `badge_colors={}` on a `Column` | `ProbeDetailsConfigError` — the app never starts. |
| **Startup** | An empty `elements=[]` on a `Detail` | `ProbeDetailsConfigError` — the app never starts. |
| **Startup (warning only)** | A template references a base (`{jenkins}`) that `ui.link_bases` doesn't configure | One `probe_details_link_bases_missing` warning naming every missing base; the app still starts. |
| **Render time** | The warned-about missing base | The UI renders plain text instead of a broken link — never a dead anchor. |
| **Render time** | A badge value with no matching color and no `'*'` fallback | The UI renders a neutral (uncolored) pill instead of a colored one — never plain text or a blank/broken pill. |

---

## See also

- [Probe User Details](probe-user-details.md) — registering the model these
  options attach to, the six view kinds, and the `probe.set` / `probe.append`
  / `probe.update` fill API
- [Declared UI Pages](ui-pages.md) — a `table` widget's `columns` reuses
  this same `Column` verbatim, addressed by a declared page instead of a
  probe run
- [Configuration](configuration.md#ui-flight-recorder-ui) — the full `ui.*`
  config section, including `ui.link_bases`
- [Annotations](annotations.md) — the message/task/window-scoped sibling of
  probe details
