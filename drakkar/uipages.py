"""Declared UI pages (Phase 2): author models, validation, and wire schema.

A deployment may declare a handful of custom dashboard pages in config —
each a list of widgets reading from one of a fixed set of built-in sources
(events, annotations, tasks, metrics). ``build_pages`` validates the
declarations at startup (fail-fast, mirroring ``drakkar.probe``'s
probe-details validation) and produces the wire list the UI renders and
``drakkar-go`` mirrors.
"""

from __future__ import annotations

import re
from collections.abc import Collection
from typing import Annotated, Any, Literal, cast

from pydantic import BaseModel, Field

from drakkar.probe import (
    _TEMPLATE_TOKEN_RE,
    BADGE_COLOR_NAMES,
    FORMAT_KINDS,
    Column,
    ProbeDetailsColumn,
    ProbeDetailsConfigError,
    _prettify,
    _validate_template,
)

# Caps mirroring probe-details' fail-fast-at-startup posture: a config error
# here is a deploy-time mistake, never a runtime surprise.
MAX_PAGES = 20
MAX_WIDGETS_PER_PAGE = 12

_SLUG_RE = re.compile(r'^[a-z][a-z0-9-]*$')
_MAX_SLUG_LEN = 40


class UIPagesConfigError(ValueError):
    """A declared UI page violates the page/widget rules. Raised at startup."""


class EventsSource(BaseModel):
    """A widget source reading the flight-recorder event stream."""

    kind: Literal['events'] = 'events'
    event_types: list[Annotated[str, Field(min_length=1)]] = Field(min_length=1)
    limit: int = Field(default=200, ge=1, le=1000)


class AnnotationsSource(BaseModel):
    """A widget source reading probe annotations, optionally by kind prefix."""

    kind: Literal['annotations'] = 'annotations'
    kind_prefix: str = ''
    limit: int = Field(default=200, ge=1, le=1000)


class TasksSource(BaseModel):
    """A widget source reading recent task records."""

    kind: Literal['tasks'] = 'tasks'
    limit: int = Field(default=200, ge=1, le=1000)


class MetricsSource(BaseModel):
    """A widget source reading one named metric's current value."""

    kind: Literal['metrics'] = 'metrics'
    metric: str = Field(min_length=1)


Source = Annotated[EventsSource | AnnotationsSource | TasksSource | MetricsSource, Field(discriminator='kind')]


class Widget(BaseModel):
    """One panel on a declared page: a source plus a presentation."""

    title: str
    view: Literal['table', 'keyvalue', 'string', 'badge', 'stat']
    source: Source
    columns: list[str] | dict[str, Column] | None = None
    field: str | None = None
    badge_colors: dict[str, str] | None = None
    format: str | None = None


class Page(BaseModel):
    """One declared dashboard page: a slug, a title, and its widgets."""

    slug: str
    title: str
    widgets: list[Widget]


class UIPageWidget(BaseModel):
    """The wire form of a widget, mirrored by the UI and by drakkar-go."""

    title: str
    view: str
    source: dict[str, Any]
    columns: list[ProbeDetailsColumn] | None = None
    field: str | None = None
    badge_colors: dict[str, str] | None = None
    format: str | None = None


class UIPage(BaseModel):
    """The wire form of a declared page."""

    slug: str
    title: str
    widgets: list[UIPageWidget]


class _AnyRowFields:
    """Sentinel accepting every row-field name.

    Page rows are dynamic dicts (event rows, annotation payloads), so
    '{row.x}' membership cannot be checked at boot. _validate_template
    only does `name not in row_fields`; this object makes that check
    always pass while keeping the syntax rules (balanced braces, base
    name grammar) fully enforced.
    """

    def __contains__(self, item: object) -> bool:
        return True


_ANY_ROW_FIELDS = _AnyRowFields()


def _check_template(template: str, *, where: str) -> None:
    """Validate one column template, re-raising probe's error as UIPagesConfigError.

    Row refs are always allowed (unchecked, via ``_ANY_ROW_FIELDS``) since
    page rows are dynamic dicts; syntax rules still apply in full.
    """
    try:
        # _AnyRowFields only implements __contains__, which is all
        # _validate_template actually calls on row_fields; the Collection
        # type hint upstream is wider than the function needs.
        _validate_template(template, where=where, row_fields=cast('Collection[str]', _ANY_ROW_FIELDS))
    except ProbeDetailsConfigError as exc:
        raise UIPagesConfigError(str(exc)) from exc


def _build_wire_columns(
    columns: list[str] | dict[str, Column] | None, *, where: str
) -> list[ProbeDetailsColumn] | None:
    """Build wire columns exactly like probe-details' row-view columns.

    List form: default labels via ``_prettify``, no per-column options.
    Dict form: declared order, ``label or _prettify(name)``, options copied
    onto the wire column. Both forms reject empty/duplicate names and
    validate each column's palette and templates.
    """
    if columns is None:
        return None
    if not columns:
        raise UIPagesConfigError(f'{where}: columns must not be empty')
    if isinstance(columns, dict):
        columns_map = columns
    else:
        if len(dict.fromkeys(columns)) != len(columns):
            raise UIPagesConfigError(f'{where}: duplicate column name in columns')
        columns_map = {name: Column() for name in columns}

    wire_columns = [
        ProbeDetailsColumn(
            key=name,
            label=opts.label or _prettify(name),
            link_template=opts.link_template,
            badge_colors=opts.badge_colors,
            format=opts.format,
            hint=opts.hint,
        )
        for name, opts in columns_map.items()
    ]
    for col in wire_columns:
        for template in (col.link_template, col.hint):
            if template:
                _check_template(template, where=f'{where}.{col.key}')
        if col.badge_colors is not None and not col.badge_colors:
            raise UIPagesConfigError(f'{where}.{col.key}: badge_colors must not be empty')
        if col.badge_colors:
            for value_name, color in col.badge_colors.items():
                if color not in BADGE_COLOR_NAMES:
                    raise UIPagesConfigError(
                        f"{where}.{col.key}: unknown color '{color}' for badge value "
                        f"'{value_name}' (expected one of {BADGE_COLOR_NAMES})"
                    )
    return wire_columns


def _validate_widget(widget: Widget, *, where: str) -> UIPageWidget:
    """Validate one widget's view/source/field/columns pairing and build its wire form."""
    if not widget.title:
        raise UIPagesConfigError(f'{where}: title must not be empty')
    if widget.columns is not None and widget.view != 'table':
        raise UIPagesConfigError(f"{where}: columns is only valid with view='table'")
    if widget.view in ('string', 'badge') and not widget.field:
        raise UIPagesConfigError(f"{where}: view '{widget.view}' requires field")
    if widget.view in ('table', 'stat') and widget.field is not None:
        raise UIPagesConfigError(f"{where}: field is not valid with view '{widget.view}'")
    if widget.view == 'badge':
        if not widget.badge_colors:
            raise UIPagesConfigError(f"{where}: view 'badge' requires badge_colors={{value: color}}")
    elif widget.badge_colors is not None:
        raise UIPagesConfigError(f"{where}: badge_colors requires view='badge'")
    if widget.badge_colors:
        for value_name, color in widget.badge_colors.items():
            if color not in BADGE_COLOR_NAMES:
                raise UIPagesConfigError(
                    f"{where}: unknown color '{color}' for badge value '{value_name}' "
                    f'(expected one of {BADGE_COLOR_NAMES})'
                )
    if widget.format is not None:
        if widget.view != 'stat':
            raise UIPagesConfigError(f"{where}: format is only valid with view='stat'")
        if widget.format not in FORMAT_KINDS:
            raise UIPagesConfigError(f"{where}: unknown format '{widget.format}' (expected one of {FORMAT_KINDS})")
    if widget.view == 'stat' and not isinstance(widget.source, MetricsSource):
        raise UIPagesConfigError(f"{where}: view 'stat' requires a metrics source")
    if isinstance(widget.source, MetricsSource) and widget.view != 'stat':
        raise UIPagesConfigError(f"{where}: a metrics source requires view='stat'")

    wire_columns = _build_wire_columns(widget.columns, where=where)
    return UIPageWidget(
        title=widget.title,
        view=widget.view,
        source=widget.source.model_dump(),
        columns=wire_columns,
        field=widget.field,
        badge_colors=widget.badge_colors,
        format=widget.format,
    )


def _validate_page(page: Page) -> UIPage:
    """Validate one page's slug/title/widget-count and build its wire form."""
    where = f'pages[{page.slug}]'
    if not _SLUG_RE.fullmatch(page.slug) or len(page.slug) > _MAX_SLUG_LEN:
        raise UIPagesConfigError(f"{where}: slug must match '^[a-z][a-z0-9-]*$' and be at most {_MAX_SLUG_LEN} chars")
    if not page.title:
        raise UIPagesConfigError(f'{where}: title must not be empty')
    if not page.widgets:
        raise UIPagesConfigError(f'{where}: widgets must not be empty')
    if len(page.widgets) > MAX_WIDGETS_PER_PAGE:
        raise UIPagesConfigError(f'{where}: at most {MAX_WIDGETS_PER_PAGE} widgets are allowed per page')
    widgets = [_validate_widget(widget, where=f'{where}.widgets[{i}]') for i, widget in enumerate(page.widgets)]
    return UIPage(slug=page.slug, title=page.title, widgets=widgets)


def build_pages(pages: list[Page] | None) -> list[UIPage]:
    """Validate declarations and produce the wire list. Raises UIPagesConfigError."""
    if not pages:
        return []
    if len(pages) > MAX_PAGES:
        raise UIPagesConfigError(f'at most {MAX_PAGES} pages are allowed')
    seen_slugs: set[str] = set()
    wire_pages: list[UIPage] = []
    for page in pages:
        if page.slug in seen_slugs:
            raise UIPagesConfigError(f"duplicate page slug '{page.slug}'")
        seen_slugs.add(page.slug)
        wire_pages.append(_validate_page(page))
    return wire_pages


def pages_referenced_bases(pages: list[UIPage]) -> set[str]:
    """Collect {base} tokens from every column link_template/hint, for the startup warning."""
    bases: set[str] = set()

    def _scan(template: str | None) -> None:
        if not template:
            return
        for token in _TEMPLATE_TOKEN_RE.findall(template):
            if token != 'value' and not token.startswith('row.'):
                bases.add(token)

    for page in pages:
        for widget in page.widgets:
            for col in widget.columns or []:
                _scan(col.link_template)
                _scan(col.hint)
    return bases
