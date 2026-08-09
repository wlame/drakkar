# UI Customization Cookbook: A Worked Example

[Probe User Details](probe-user-details.md), [UI Enrichment](ui-enrichment.md),
and [Declared UI Pages](ui-pages.md) each cover one feature in isolation. This
page builds **one** small handler through five short steps instead, adding
one capability at a time, so you can see how they compose on a single running
example rather than five unrelated snippets.

The running example: a worker that scans submitted files for patterns —
`arrange()` schedules one task per `(pattern, file_path)` pair, and
`on_task_complete()` records what each scan found. Nothing about the example
is specific to scanning; the same five steps apply to any handler.

Every step is copy-pasteable on its own, and the model grows across steps —
by the end of step 5 you have one complete `ScanProbeDetails` model, one
declared page, and one renderers module.

---

## Step 1: A tab in the Message Probe

Register a model and fill it from a hook. This is the condensed version of
[Probe User Details](probe-user-details.md) — read that page for the full
`probe_field()` reference, the six view kinds, and the `probe.set` /
`probe.append` / `probe.update` verbs.

```python
from pydantic import BaseModel

from drakkar import probe, probe_field


class FileScanRow(BaseModel):
    file_path: str
    pattern: str
    match_count: int
    duration_ms: int
    outcome: str  # 'clean' | 'flagged'


class ScanProbeDetails(BaseModel):
    scan_note: str | None = probe_field(section='Scan', view='string', default=None)
    file_scans: list[FileScanRow] = probe_field(section='Scan', view='table', default_factory=list)


class ScanPipelineHandler(dk.BaseDrakkarHandler[...]):
    probe_details_model = ScanProbeDetails

    async def arrange(self, messages, pending):
        probe.set(scan_note=f'{len(messages)} messages queued for scanning')
        ...

    async def on_task_complete(self, result):
        # non-empty lines of stdout, e.g. ['line 12: TODO', 'line 40: TODO']
        matches = [line for line in result.stdout.splitlines() if line.strip()]
        probe.append(
            'file_scans',
            FileScanRow(
                file_path=result.task.metadata['file_path'],
                pattern=result.task.metadata['pattern'],
                match_count=len(matches),
                duration_ms=round(result.duration_seconds * 1000),
                outcome='flagged' if matches else 'clean',
            ),
        )
        ...
```

No YAML needed yet — `probe_details_model` has no external configuration to
resolve. Open the Message Probe on any replayed message and the
**User-defined** tab now shows a **Scan** section: `scan_note` as text,
`file_scans` as a plain table.

---

## Step 2: Links, badges, and a format

`file_scans` renders every column as plain text so far. Add a `columns`
dict to turn `file_path` into a link, `outcome` into a colored pill, and
`duration_ms` into a human-readable duration — see
[UI Enrichment](ui-enrichment.md#link-templates) for the full grammar.

```python
from drakkar.probe import Column


class ScanProbeDetails(BaseModel):
    scan_note: str | None = probe_field(section='Scan', view='string', default=None)
    file_scans: list[FileScanRow] = probe_field(
        section='Scan',
        view='table',
        default_factory=list,
        columns={
            'file_path': Column(
                link_template='{artifact_store}/files/{value}',
                hint='Open {value} in the artifact store',
            ),
            'duration_ms': Column(format='duration_ms'),
            'outcome': Column(badge_colors={'clean': 'green', 'flagged': 'red', '*': 'gray'}),
        },
    )
```

`{artifact_store}` is a named base, not a URL — configure it once per
environment:

```yaml
ui:
  link_bases:
    artifact_store: 'https://artifacts.internal.example.com'
```

`file_path` is now a clickable link into the artifact store, `outcome` a
green/red pill, and `duration_ms` reads as `1 m 5 s` instead of `65000`.

---

## Step 3: A detail popup

Clicking a row still does nothing. Add a `detail` panel that shows all four
non-custom element kinds — `string`, `keyvalue`, `table`, and `links` — with
a bit of synthetic diagnostic data (`scan_meta`, `sample_lines`) to give the
`keyvalue` and `table` elements something to show. See
[Detail panels](ui-enrichment.md#detail-panels) for the full rules.

```python
from typing import Any

from drakkar.probe import Detail, Element, Link


class SampleLineRow(BaseModel):
    line_no: int
    text: str


class FileScanRow(BaseModel):
    file_path: str
    pattern: str
    match_count: int
    duration_ms: int
    outcome: str  # 'clean' | 'flagged'
    # Detail-panel-only fields — not in `columns` above, so they never show
    # as table columns, only inside the popup.
    scan_meta: dict[str, Any]
    sample_lines: list[SampleLineRow]


class ScanProbeDetails(BaseModel):
    scan_note: str | None = probe_field(section='Scan', view='string', default=None)
    file_scans: list[FileScanRow] = probe_field(
        section='Scan',
        view='table',
        default_factory=list,
        columns={
            'file_path': Column(
                link_template='{artifact_store}/files/{value}',
                hint='Open {value} in the artifact store',
            ),
            'duration_ms': Column(format='duration_ms'),
            'outcome': Column(badge_colors={'clean': 'green', 'flagged': 'red', '*': 'gray'}),
        },
        detail=Detail(
            title='Scan: {row.pattern} in {row.file_path}',
            elements=[
                Element(field='pattern', view='string', label='Pattern'),
                Element(field='scan_meta', view='keyvalue', label='Scan metadata'),
                Element(field='sample_lines', view='table', label='Sample lines'),
                Element(
                    view='links',
                    links=[
                        Link(label='Open ticket', template='{ticket_tracker}/search?q={row.pattern}'),
                        Link(label='View artifact', template='{artifact_store}/files/{row.file_path}'),
                    ],
                ),
            ],
        ),
    )
```

`on_task_complete()` fills the two new fields alongside the ones from step 1
— invented-but-plausible values are fine here, since these fields exist to
exercise the popup, not to carry real business data:

```python
    async def on_task_complete(self, result):
        # non-empty lines of stdout, e.g. ['line 12: TODO', 'line 40: TODO']
        matches = [line for line in result.stdout.splitlines() if line.strip()]
        probe.append(
            'file_scans',
            FileScanRow(
                file_path=result.task.metadata['file_path'],
                pattern=result.task.metadata['pattern'],
                match_count=len(matches),
                duration_ms=round(result.duration_seconds * 1000),
                outcome='flagged' if matches else 'clean',
                scan_meta={'encoding': 'utf-8', 'scanner_version': '3.2.1'},
                sample_lines=[
                    SampleLineRow(line_no=i + 1, text=line[:80]) for i, line in enumerate(matches[:3])
                ],
            ),
        )
        ...
```

The links element needs a second named base — add it beside the one from
step 2:

```yaml
ui:
  link_bases:
    artifact_store: 'https://artifacts.internal.example.com'
    ticket_tracker: 'https://tickets.internal.example.com'
```

Clicking any row now opens a panel titled with that row's pattern and file,
showing the pattern as text, `scan_meta` as a flat key/value list,
`sample_lines` as a sub-table, and two links out — one into the ticket
tracker, one back into the artifact store.

---

## Step 4: A dashboard page over the pipeline's tasks

Everything so far lives inside the Message Probe — one replayed message at a
time. A [declared page](ui-pages.md) adds a standing nav entry instead,
reading data the framework already records:

```python
from drakkar.uipages import MetricsSource, Page, TasksSource, Widget


class ScanPipelineHandler(dk.BaseDrakkarHandler[...]):
    probe_details_model = ScanProbeDetails

    ui_pages = [
        Page(
            slug='scan-pipeline',
            title='Scan pipeline',
            widgets=[
                Widget(
                    title='Recent scan tasks',
                    view='table',
                    source=TasksSource(limit=100),
                    columns={
                        'task_id': Column(),
                        'status': Column(
                            badge_colors={'completed': 'green', 'failed': 'red', 'running': 'blue', '*': 'gray'}
                        ),
                    },
                ),
                Widget(
                    title='Files scanned total',
                    view='stat',
                    source=MetricsSource(metric='scan_pipeline_files_total'),
                    format='number',
                ),
            ],
        ),
    ]
```

No new YAML — a page's `columns` reuse `ui.link_bases` exactly the way
probe-details columns do, and this widget set doesn't happen to need a link.
`/p/scan-pipeline` now shows a live table of recent tasks next to a running
total of the `scan_pipeline_files_total` metric.

!!! note "Go workers: pages are Python-only today"
    `drakkar-go` has no equivalent of `ui_pages` yet — it answers
    `GET /api/v1/pages` with an empty list unconditionally, the same shape a
    Python worker returns when its handler declares no `ui_pages`. A fleet
    mixing this Python worker with Go workers simply shows no extra nav
    entry for the Go ones — never an error, never a broken page. See
    [Mixed fleets](ui-pages.md#mixed-fleets) for the full contract.

---

## Step 5: A custom cell renderer

Links, badges, and formats don't cover every presentation — here, a small
inline bar for `match_count` instead of a bare number. A **custom renderer**
is deployment-owned JavaScript the debug UI loads and calls per cell; see
[Custom cell renderers](ui-enrichment.md#custom-cell-renderers) for the
module contract and fallback rules.

```javascript
// scan-pipeline-renderers.js — deployment-owned, shipped beside the
// worker's YAML config. Each function is (value, row, cell) => HTMLElement.

const MATCH_GAUGE_MAX = 50;

function matchGauge(value, row, cell) {
  const count = typeof value === 'number' ? value : Number(value) || 0;
  const pct = Math.max(0, Math.min(1, count / MATCH_GAUGE_MAX)) * 100;

  const wrapper = document.createElement('div');
  wrapper.style.display = 'flex';
  wrapper.style.alignItems = 'center';
  wrapper.style.gap = '6px';

  const track = document.createElement('div');
  track.style.flex = '1 1 auto';
  track.style.height = '6px';
  track.style.borderRadius = '3px';
  track.style.background = 'rgba(127, 127, 127, 0.25)';

  const fill = document.createElement('div');
  fill.style.height = '100%';
  fill.style.width = pct + '%';
  fill.style.background = count > 0 ? '#3b82f6' : 'transparent';
  track.appendChild(fill);

  const label = document.createElement('span');
  label.textContent = String(count);
  wrapper.appendChild(track);
  wrapper.appendChild(label);
  return wrapper;
}

export default { matchGauge };
```

Point config at the file:

```yaml
ui:
  custom_renderers_path: '/etc/drakkar/scan-pipeline-renderers.js'
```

Then name it from a `Column`, the same way `link_template` or `badge_colors`
would be named — `renderer` is exclusive with those three, but `hint` still
composes:

```python
        columns={
            'file_path': Column(
                link_template='{artifact_store}/files/{value}',
                hint='Open {value} in the artifact store',
            ),
            'duration_ms': Column(format='duration_ms'),
            'outcome': Column(badge_colors={'clean': 'green', 'flagged': 'red', '*': 'gray'}),
            'match_count': Column(renderer='matchGauge', hint='{value} matches found'),
        },
```

`match_count` now renders as a small proportional bar instead of a plain
integer, and — since `Column(renderer=...)` is the same type a page widget's
`columns` uses — the `Recent scan tasks` widget from step 4 could point
`exit_code` or any other column at the same renderer with no further
plumbing.

---

## What you now have

| Step | Added | Reference |
|---|---|---|
| 1 | `probe_details_model`, `probe.set` / `probe.append` | [Probe User Details](probe-user-details.md) |
| 2 | `link_template`, `badge_colors`, `format`, `ui.link_bases` | [UI Enrichment](ui-enrichment.md) |
| 3 | `detail` popup (`string` / `keyvalue` / `table` / `links`) | [UI Enrichment: Detail panels](ui-enrichment.md#detail-panels) |
| 4 | A declared dashboard page with a table and a stat widget | [Declared UI Pages](ui-pages.md) |
| 5 | A custom cell renderer (`ui.custom_renderers_path`, `Column(renderer=...)`) | [UI Enrichment: Custom cell renderers](ui-enrichment.md#custom-cell-renderers) |

---

## See also

- [Probe User Details](probe-user-details.md) — the full `probe_field()` reference and the six view kinds
- [UI Enrichment](ui-enrichment.md) — links, badges, formats, hints, detail panels, and custom renderers in depth
- [Declared UI Pages](ui-pages.md) — sources, widget views, and the mixed-fleet contract
- [Configuration](configuration.md#ui-flight-recorder-ui) — `ui.link_bases` and `ui.custom_renderers_path`
