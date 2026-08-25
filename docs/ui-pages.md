# Declared UI Pages

The drakkar-ui bundle ships with a fixed set of pages — Dashboard, Live,
History, Debug. Sometimes that's not the view an operator actually wants:
a page showing just this handler's orders, or just its retry queue, laid
out exactly the way the domain calls for. **Declared UI pages** are that
view, without writing any UI code — a handler lists a handful of widgets,
each reading from a small set of built-in data sources, and the debug UI
turns the list into a page and a nav entry.

```python
from drakkar.probe import Column
from drakkar.uipages import AnnotationsSource, MetricsSource, Page, Widget


class MyHandler(BaseDrakkarHandler[...]):
    ui_pages = [
        Page(
            slug='orders',
            title='Orders',
            widgets=[
                Widget(
                    title='Recent orders',
                    view='table',
                    source=AnnotationsSource(kind_prefix='order.', limit=100),
                    columns={
                        'order_id': Column(link_template='{shop_admin}/orders/{value}'),
                        'status': Column(badge_colors={'paid': 'green', '*': 'gray'}),
                    },
                ),
                Widget(
                    title='Orders processed',
                    view='stat',
                    source=MetricsSource(metric='orders_processed_total'),
                    format='number',
                ),
            ],
        ),
    ]
```

That's the whole opt-in. `/p/orders` shows a table of recent `order.*`
annotations — with a clickable order id and a colored status pill — next
to a running total of a Prometheus metric. The table refreshes over the
live WebSocket as new annotations arrive; the stat tile polls on a flat
30-second interval instead, since a metric sum has no single event that
means "this changed."

For a guided end-to-end walkthrough that builds one example through every
enrichment option and a declared page, see the
[UI customization cookbook](ui-customization-cookbook.md).

The `{shop_admin}` token above resolves the same way it would in a
probe-details `link_template` — from `ui.link_bases`:

```yaml
ui:
  link_bases:
    shop_admin: 'https://admin.shop.example.com'
```

---

## Declaring pages

```python
class Page(BaseModel):
    slug: str
    title: str
    widgets: list[Widget]


class Widget(BaseModel):
    title: str
    view: Literal['table', 'keyvalue', 'string', 'badge', 'stat']
    source: Source
    columns: list[str] | dict[str, Column] | None = None
    field: str | None = None
    badge_colors: dict[str, str] | None = None
    format: str | None = None
```

Point your handler's `ui_pages` at a list of `Page`. Leave it at its
default of `None` (or `[]`) and no extra nav entries appear — the same
opt-in shape as `probe_details_model` for the Message Probe's User-defined
tab.

!!! warning "Validated at startup, not at first page load"
    `DrakkarApp(...)` calls `build_pages(handler.ui_pages)` while
    constructing the app, so a broken declaration raises
    `UIPagesConfigError` immediately — a code-owned mistake surfaces at
    boot, not three weeks later when someone finally opens the page.

Caps, enforced at startup:

- At most **20 pages**, each with at most **12 widgets**.
- `slug` must match `^[a-z][a-z0-9-]*$`, be at most 40 characters, and be
  unique across every declared page.

---

## Sources

`source.kind` selects which existing read API backs a widget — declared
pages add **no new data endpoint**, only new ways to project data the
debug UI already serves:

| Source | Fields | Reads | Row shape |
|---|---|---|---|
| `EventsSource` | `event_types: list[str]` (required, non-empty), `limit: int` (1-1000, default 200) | `GET /api/v1/events` filtered to `event_types` | one row per event |
| `AnnotationsSource` | `kind_prefix: str` (default `''`), `limit: int` (1-1000, default 200) | `GET /api/v1/events` filtered to `event_types=annotation`, then client-filtered to rows whose annotation `kind` starts with `kind_prefix` | one row per matching annotation: its JSON payload spread onto the row, plus `ts` and `kind` |
| `TasksSource` | `limit: int` (1-1000, default 200) | `GET /api/v1/live/task-results` | one row per task result |
| `MetricsSource` | `metric: str` (required, non-empty) | `GET /api/v1/debug/metrics`, summed over the named metric family's samples | no rows — scalar-only, for `view='stat'` widgets |

The `ts` and `kind` stamped onto an annotation row always win over any
`ts`/`kind` keys the JSON payload itself happens to carry, so a handler
can't shadow the recorded event time or its own filtered-on kind by
naming a payload field the same thing.

`AnnotationsSource` and `EventsSource` read the same endpoint; the
difference is `AnnotationsSource` fixes `event_types=['annotation']` for
you and adds the `kind_prefix` filter, since "this handler's annotations
of one kind" is the common case a raw `EventsSource` would need more
config to express.

---

## Widget views

`view` picks the presentation; `source`, `columns`, `field`,
`badge_colors`, and `format` pair up with it as follows:

| `view` | Shows | `columns` | `field` | `badge_colors` | `format` |
|---|---|---|---|---|---|
| `table` | Every source row, as a table | **required**, non-empty | forbidden | per-column, via `Column(badge_colors=...)` | per-column, via `Column(format=...)` |
| `keyvalue` | One row as flat key/value pairs | forbidden | optional (a nested field; omitted shows the whole row) | forbidden | forbidden |
| `string` | One field of the newest row, as text | forbidden | **required** | forbidden | forbidden |
| `badge` | One field of the newest row, as a colored pill | forbidden | **required** | **required** | forbidden |
| `stat` | A metric's summed value | forbidden | forbidden | forbidden | optional |

```python
# table — every row, order_id linked, status badged per-column
Widget(title='Recent orders', view='table', source=AnnotationsSource(kind_prefix='order.'),
       columns={'order_id': Column(link_template='{shop_admin}/orders/{value}'),
                'status': Column(badge_colors={'paid': 'green', '*': 'gray'})})

# keyvalue — the newest row's own fields, flattened
Widget(title='Latest order', view='keyvalue', source=AnnotationsSource(kind_prefix='order.', limit=1))

# string — one field of the newest row
Widget(title='Latest order id', view='string', field='order_id', source=AnnotationsSource(kind_prefix='order.', limit=1))

# badge — one field of the newest row, colored
Widget(title='Latest order status', view='badge', field='status',
       badge_colors={'paid': 'green', '*': 'gray'}, source=AnnotationsSource(kind_prefix='order.', limit=1))

# stat — a metric family, summed and formatted
Widget(title='Orders processed', view='stat', source=MetricsSource(metric='orders_processed_total'), format='number')
```

`view='stat'` requires a `MetricsSource`, and a `MetricsSource` requires
`view='stat'` — the two are paired both ways, since a metric has no rows
for any other view to render and every other source has no single scalar
for `stat` to show.

---

## Column enrichment reuse

A `table` widget's `columns` is the exact same `Column` used by
[probe-details table columns](ui-enrichment.md#column-subsets-and-per-column-options)
— `link_template`, `badge_colors`, `format`, `hint`, `renderer`, and
`label` all mean what they mean there, with the same template grammar
(`{value}` / `{row.<field>}` / `{<base>}`) and the same `ui.link_bases`
resolution. That includes
[custom cell renderers](ui-enrichment.md#custom-cell-renderers) — a page
table column can name a `renderer` the same way a probe-details column
does. Nothing about enrichment is reinvented for pages; a page widget just
addresses that machinery from a declared page instead of a probe run.

---

## Validation: what fails at boot vs what degrades at render

| When | Example | What happens |
|---|---|---|
| **Startup** | A `slug` that doesn't match `^[a-z][a-z0-9-]*$`, is too long, or duplicates another page's | `UIPagesConfigError` — the app never starts. |
| **Startup** | More than 20 pages, or more than 12 widgets on one page | `UIPagesConfigError` — the app never starts. |
| **Startup** | `view='table'` with `columns` omitted or empty, `columns` or `field` used where the view forbids it, `view='badge'` without `badge_colors`, `format` outside `view='stat'`, a `stat`/`MetricsSource` mismatch | `UIPagesConfigError` — the app never starts. |
| **Startup** | A malformed `link_template`/`hint`, an unknown badge color name, an empty `badge_colors={}` | `UIPagesConfigError` — the app never starts. |
| **Startup (warning only)** | A template references a base (`{shop_admin}`) that `ui.link_bases` doesn't configure | One `probe_details_link_bases_missing` warning naming every missing base (shared with probe-details layouts); the app still starts. |
| **Render time** | A column or `hint` template references `{row.<field>}` for a field the actual row doesn't have | The UI renders plain text for that value — never validated at startup, since page rows are dynamic dicts (event/annotation payloads), not a fixed model. |
| **Render time** | An older UI against a widget whose `view` or `source.kind` it doesn't recognize | A placeholder message names the unrecognized value, instead of a blank or broken widget — see [forward compatibility](#forward-compatibility) below. |

The `{row.*}` gap is the one place declared pages are looser than
probe-details: a probe-details row is a fixed Pydantic model, so
`{row.job_name}` can be checked against its fields at boot. A page's rows
come from events, annotations, or task results — dynamic dicts whose
actual keys depend on what the handler puts in them at runtime — so the
same check can't run until a row actually exists.

### Forward compatibility

`view` and `source.kind` are open strings on the wire, not a closed set —
a widget using a view or source kind added after your UI was built is
expected, not an error. An unsupported widget shows one of three
placeholders instead of failing silently:

- *"This widget needs a newer UI (unsupported view '…')."*
- *"This widget needs a newer UI (unsupported source '…')."*
- *"This widget is misconfigured: a 'stat' view needs a source with a
  'metric' field."* — the one placeholder that isn't a version-skew case;
  it means the view/source pairing itself is malformed.

---

## Routing and navigation

Each declared page routes at `/p/<slug>` and gets one nav entry
(`title`, linking to `/p/<slug>`), appended after the bundle's own pages in
declaration order. A slug with no matching page — never declared, or
removed since the UI last loaded — renders the same not-found page as any
other unmatched route.

---

## Mixed fleets

Declared pages are a **Python-only** feature today: `drakkar-go`
answers `GET /api/v1/pages` with an empty list unconditionally, the same
shape a Python worker returns when its handler declares no `ui_pages`.
A fleet mixing both backends behind one UI simply shows no extra nav
entries for the Go workers — never an error, never a broken page.

---

## See also

- [UI Enrichment](ui-enrichment.md) — the `Column` options a `table`
  widget's `columns` reuses verbatim: link templates, badges, formats,
  and hints
- [Probe User Details](probe-user-details.md) — the sibling User-defined
  probe tab, sharing the same startup-validation posture
- [Annotations](annotations.md) — the event kind `AnnotationsSource` reads
- [Configuration](configuration.md#ui-flight-recorder-ui) — `ui.link_bases`,
  the config that resolves a template's `{<base>}` tokens
