"""User-defined probe details: model annotations, layout, and the fill API.

A handler may register one Pydantic model (``probe_details_model``) whose
fields carry layout annotations from :func:`probe_field`. During a Message
Probe the runner binds a per-run state here; the module-level verbs
``set`` / ``append`` / ``update`` write into it from anywhere in handler
code and are near-zero-cost no-ops outside a probe — the logging-module
pattern, so business logic needs no ``if probing:`` guards.
"""

from __future__ import annotations

import builtins
import contextvars
import json
import re
import types
import typing
from collections.abc import Callable, Collection
from typing import Any, Literal, cast

from pydantic import BaseModel, ConfigDict, Field, ValidationError
from pydantic.fields import FieldInfo

ViewKind = Literal['string', 'keyvalue', 'dict', 'table', 'tables', 'tree', 'badge', 'custom']

_VIEW_KINDS: tuple[str, ...] = ('string', 'keyvalue', 'dict', 'table', 'tables', 'tree', 'badge', 'custom')
_ROW_VIEWS: tuple[str, ...] = ('table', 'tables', 'tree')

# Maximum number of grouping levels a 'tree' field may declare. Enforced at
# registration (startup), so a too-deep tree is a config error, never a
# runtime surprise.
TREE_MAX_DEPTH = 4
_METADATA_KEY = 'drakkar_probe'
_SCALARS = (str, int, float, bool)

# Default write caps per probe run; the live values come from
# ``ui.probe_details.*`` config and reach ``_DetailsState`` per instance.
MAX_WRITES = 10_000
MAX_TOTAL_BYTES = 5_000_000

# Named badge colors the UI owns CSS for; '*' is the fallback map key.
BADGE_COLOR_NAMES: tuple[str, ...] = ('green', 'red', 'yellow', 'blue', 'gray', 'purple')
# Client-side value formatting hints.
FORMAT_KINDS: tuple[str, ...] = ('duration_ms', 'bytes', 'timestamp', 'number')

_TEMPLATE_TOKEN_RE = re.compile(r'\{([A-Za-z_][A-Za-z0-9_.]*)\}')
_BASE_NAME_RE = re.compile(r'[a-z][a-z0-9_]*')
# Names in the deployment-provided renderers module's default export map.
_RENDERER_NAME_RE = re.compile(r'[a-zA-Z_][a-zA-Z0-9_]*')


class ProbeDetailsConfigError(ValueError):
    """A probe-details model violates the layout rules. Raised at startup."""


def _validate_template(template: str, *, where: str, row_fields: Collection[str] | None) -> None:
    """Validate one link/hint template at startup.

    Grammar: ``{value}``, ``{row.<field>}`` (only when ``row_fields`` is
    given), ``{<base>}`` for a named base. Base *membership* in config is
    deliberately not checked here — config varies per environment; the app
    startup emits a warning instead (see referenced_bases).
    """
    stripped = _TEMPLATE_TOKEN_RE.sub('', template)
    if '{' in stripped or '}' in stripped:
        raise ProbeDetailsConfigError(f'{where}: malformed template {template!r}')
    for token in _TEMPLATE_TOKEN_RE.findall(template):
        if token == 'value':
            continue
        if token.startswith('row.'):
            if row_fields is None:
                raise ProbeDetailsConfigError(
                    f"{where}: template {template!r} uses '{{row.*}}', which is only valid on table columns"
                )
            name = token[4:]
            if name not in row_fields:
                raise ProbeDetailsConfigError(
                    f"{where}: template {template!r} names '{name}', which is not a row-model field"
                )
            continue
        if not _BASE_NAME_RE.fullmatch(token):
            raise ProbeDetailsConfigError(
                f"{where}: template {template!r} has invalid base name '{{{token}}}' "
                '(bases are lower-case identifiers from ui.link_bases)'
            )


def _validate_renderer_name(renderer: str, *, where: str) -> None:
    """Check a renderer name against the deployment module's naming grammar."""
    if not _RENDERER_NAME_RE.fullmatch(renderer):
        raise ProbeDetailsConfigError(
            f'{where}: invalid renderer name {renderer!r} (expected to match {_RENDERER_NAME_RE.pattern!r})'
        )


def _validate_column_renderer(col: ProbeDetailsColumn, *, where: str) -> None:
    """Check a column's renderer name and its exclusivity with presentation options."""
    if col.renderer is None:
        return
    _validate_renderer_name(col.renderer, where=where)
    if col.link_template or col.badge_colors or col.format:
        raise ProbeDetailsConfigError(f'{where}: renderer is exclusive with link_template/badge_colors/format')


def probe_field(
    *,
    section: str,
    view: ViewKind,
    label: str | None = None,
    group_by: tuple[str, ...] | list[str] | None = None,
    link_template: str | None = None,
    badge_colors: dict[str, str] | None = None,
    format: str | None = None,
    hint: str | None = None,
    renderer: str | None = None,
    columns: list[str] | dict[str, Column] | None = None,
    detail: Detail | None = None,
    default: Any = ...,
    default_factory: Callable[[], Any] | None = None,
) -> Any:
    """Declare one probe-details field and its presentation.

    Thin wrapper over :func:`pydantic.Field` that stashes the layout
    metadata in ``json_schema_extra`` — one artifact, nothing to drift.

    ``group_by`` is required for (and exclusive to) ``view='tree'``: the
    ordered row-model field names the UI groups rows by, outermost level
    first, at most :data:`TREE_MAX_DEPTH` deep. Membership in the row model
    is checked later, in :func:`build_layout`, where the row type is known.
    """
    if view not in _VIEW_KINDS:
        raise ProbeDetailsConfigError(f"probe_field: unknown view '{view}' (expected one of {_VIEW_KINDS})")
    if not section:
        raise ProbeDetailsConfigError('probe_field: section must be a non-empty string')
    if view == 'tree':
        names = list(group_by or [])
        if not names:
            raise ProbeDetailsConfigError(
                "probe_field: view 'tree' requires group_by=(...) naming 1-4 row-model fields"
            )
        if len(names) > TREE_MAX_DEPTH:
            raise ProbeDetailsConfigError(f'probe_field: group_by allows at most {TREE_MAX_DEPTH} levels')
        if any(not isinstance(name, str) or not name for name in names):
            raise ProbeDetailsConfigError('probe_field: group_by entries must be non-empty strings')
        # dict.fromkeys, not set(): this module's `set` verb shadows the
        # builtin (see the note above the logging-like API below).
        if len(dict.fromkeys(names)) != len(names):
            raise ProbeDetailsConfigError('probe_field: group_by entries must be unique')
    elif group_by is not None:
        raise ProbeDetailsConfigError("probe_field: group_by is only valid with view='tree'")
    if view == 'badge':
        if not badge_colors:
            raise ProbeDetailsConfigError("probe_field: view 'badge' requires badge_colors={value: color}")
        if link_template is not None:
            raise ProbeDetailsConfigError("probe_field: link_template is not valid with view 'badge'")
    elif badge_colors is not None:
        raise ProbeDetailsConfigError("probe_field: badge_colors requires view 'badge' (or a Column option)")
    if badge_colors:
        for value_name, color in badge_colors.items():
            if color not in BADGE_COLOR_NAMES:
                raise ProbeDetailsConfigError(
                    f"probe_field: unknown color '{color}' for badge value '{value_name}' "
                    f'(expected one of {BADGE_COLOR_NAMES})'
                )
    if format is not None and format not in FORMAT_KINDS:
        raise ProbeDetailsConfigError(f"probe_field: unknown format '{format}' (expected one of {FORMAT_KINDS})")
    if format is not None and view not in ('string',):
        raise ProbeDetailsConfigError("probe_field: format is only valid with view 'string'")
    if link_template is not None and view not in ('string',):
        raise ProbeDetailsConfigError("probe_field: link_template is only valid with view 'string'")
    if link_template is not None:
        _validate_template(link_template, where='probe_field', row_fields=None)
    if view == 'custom':
        if not renderer:
            raise ProbeDetailsConfigError("probe_field: view 'custom' requires renderer='name'")
    elif renderer is not None:
        raise ProbeDetailsConfigError("probe_field: renderer is only valid with view='custom'")
    if renderer is not None:
        _validate_renderer_name(renderer, where='probe_field')
    if hint is not None and view in _ROW_VIEWS:
        raise ProbeDetailsConfigError('probe_field: hint on row-bearing views belongs on a Column')
    if columns is not None and view not in _ROW_VIEWS:
        raise ProbeDetailsConfigError("probe_field: columns is only valid with views 'table', 'tables', 'tree'")
    if detail is not None and view not in _ROW_VIEWS:
        raise ProbeDetailsConfigError("probe_field: detail is only valid with views 'table', 'tables', 'tree'")
    if link_template is not None and detail is not None:
        raise ProbeDetailsConfigError('probe_field: link_template and detail are mutually exclusive')
    columns_map: dict[str, Column] | None = None
    if columns is not None:
        # dict.fromkeys, not set(): this module's `set` verb shadows the
        # builtin (see the note above the logging-like API below).
        if not isinstance(columns, dict) and len(dict.fromkeys(columns)) != len(columns):
            raise ProbeDetailsConfigError('probe_field: duplicate column name in columns')
        columns_map = dict(columns) if isinstance(columns, dict) else {name: Column() for name in columns}
        if not columns_map:
            raise ProbeDetailsConfigError('probe_field: columns must not be empty')
    extra = {
        _METADATA_KEY: {
            'section': section,
            'view': view,
            'label': label,
            'group_by': list(group_by) if group_by else None,
            'link_template': link_template,
            'badge_colors': dict(badge_colors) if badge_colors else None,
            'format': format,
            'hint': hint,
            'renderer': renderer,
            'columns': columns_map,
            'detail': detail,
        }
    }
    if default_factory is not None:
        return Field(default_factory=default_factory, json_schema_extra=extra)
    return Field(default=default, json_schema_extra=extra)


# ---- wire models (rendered 1:1 by the UI) ----------------------------------


class ProbeDetailsColumn(BaseModel):
    """One table column, derived from a row model field."""

    key: str
    label: str
    link_template: str | None = None
    badge_colors: dict[str, str] | None = None
    format: str | None = None
    hint: str | None = None
    # Name in the deployment-provided renderers module (view='custom'
    # columns only); mutually exclusive with link_template/badge_colors/format.
    renderer: str | None = None


class ProbeDetailsEntry(BaseModel):
    """One field of the details model as the UI renders it."""

    key: str
    label: str
    view: ViewKind
    columns: list[ProbeDetailsColumn] | None = None
    # Ordered grouping keys for view='tree' (outermost first); None for
    # every other view. The named keys are a subset of ``columns``.
    group_by: list[str] | None = None
    link_template: str | None = None
    badge_colors: dict[str, str] | None = None
    format: str | None = None
    hint: str | None = None
    detail: Detail | None = None
    # Name in the deployment-provided renderers module; non-null only for
    # view='custom'.
    renderer: str | None = None


class ProbeDetailsSection(BaseModel):
    """A titled group of entries; order follows field declaration order."""

    title: str
    entries: list[ProbeDetailsEntry]


class ProbeDetailsLayout(BaseModel):
    """The full layout descriptor generated from the registered model."""

    sections: list[ProbeDetailsSection]


class ProbeDetailsWrite(BaseModel):
    """One successful verb call, stamped with its origin hook stage."""

    field: str
    op: Literal['set', 'append', 'update']
    origin_stage: str
    ms_since_start: float


class ProbeUserDetails(BaseModel):
    """The ``user_details`` object on the probe report."""

    model: str
    layout: ProbeDetailsLayout
    data: dict[str, Any]
    writes: list[ProbeDetailsWrite]


class Link(BaseModel):
    """One external link inside a detail panel's 'links' element."""

    # Author-facing model: an unknown kwarg (a typo, or Phase-1-shaped
    # config drifted from the current fields) is a config-authoring
    # mistake and must fail at boot, not degrade silently at render time.
    model_config = ConfigDict(extra='forbid')

    label: str
    template: str


class Element(BaseModel):
    """One block of a detail panel, rendered top to bottom."""

    model_config = ConfigDict(extra='forbid')

    view: Literal['string', 'keyvalue', 'table', 'links', 'custom']
    field: str | None = None
    label: str | None = None
    links: list[Link] | None = None
    # Name in the deployment-provided renderers module; required for and
    # exclusive to view='custom'.
    renderer: str | None = None


class Detail(BaseModel):
    """A declared right-panel layout opened by clicking a row."""

    model_config = ConfigDict(extra='forbid')

    title: str | None = None
    elements: list[Element]


class Column(BaseModel):
    """Per-column enrichment options for row-bearing probe views."""

    model_config = ConfigDict(extra='forbid')

    label: str | None = None
    link_template: str | None = None
    badge_colors: dict[str, str] | None = None
    format: Literal['duration_ms', 'bytes', 'timestamp', 'number'] | None = None
    hint: str | None = None
    # Name in the deployment-provided renderers module; exclusive with
    # link_template/badge_colors/format. Checked in build_layout, not here —
    # a pydantic validator would wrap ProbeDetailsConfigError in a
    # ValidationError, same reason badge_colors' color-name check lives
    # there too (see _validate_column_renderer).
    renderer: str | None = None


# ---- layout builder ---------------------------------------------------------

_layout_cache: dict[type[BaseModel], ProbeDetailsLayout] = {}


def _prettify(name: str) -> str:
    return name.replace('_', ' ').capitalize()


def _unwrap_optional(annotation: Any) -> Any:
    """Strip ``| None`` so 'str | None' validates like 'str'."""
    origin = typing.get_origin(annotation)
    if origin in (typing.Union, types.UnionType):
        args = [a for a in typing.get_args(annotation) if a is not type(None)]
        if len(args) == 1:
            return args[0]
    return annotation


def _probe_meta(model: type[BaseModel], name: str, info: FieldInfo) -> dict[str, Any]:
    extra = info.json_schema_extra
    if not isinstance(extra, dict) or _METADATA_KEY not in extra:
        raise ProbeDetailsConfigError(
            f'{model.__name__}.{name}: missing probe_field() annotation — every field of a '
            'probe details model must declare section and view'
        )
    extra_dict = cast(dict[str, Any], extra)
    return cast(dict[str, Any], extra_dict[_METADATA_KEY])


def _row_model_of(annotation: Any, view: str) -> type[BaseModel] | None:
    """Extract the row model from a validated table/tables annotation.

    ``table`` and ``tree`` fields are ``list[RowModel]``; ``tables`` fields
    are ``dict[str, list[RowModel]]`` (one sub-table per key). Returns None
    when the annotation does not have that shape — callers turn that into
    a config error with the model/field context.
    """
    ann = _unwrap_optional(annotation)
    if view == 'tables':
        args = typing.get_args(ann)
        if typing.get_origin(ann) is not dict or len(args) != 2 or args[0] is not str:
            return None
        ann = args[1]
    if typing.get_origin(ann) is not list or not typing.get_args(ann):
        return None
    row = typing.get_args(ann)[0]
    if isinstance(row, type) and issubclass(row, BaseModel):
        return row
    return None


def _validate_view(model: type[BaseModel], name: str, view: str, annotation: Any) -> type[BaseModel] | None:
    """Check the field type fits the declared view. Returns the row model for tables."""
    ann = _unwrap_optional(annotation)
    origin = typing.get_origin(ann)
    if view == 'table':
        row = _row_model_of(annotation, view)
        if row is None:
            raise ProbeDetailsConfigError(
                f"{model.__name__}.{name}: view 'table' requires list[RowModel] where RowModel is a BaseModel"
            )
        return row
    if view == 'tables':
        row = _row_model_of(annotation, view)
        if row is None:
            raise ProbeDetailsConfigError(
                f"{model.__name__}.{name}: view 'tables' requires dict[str, list[RowModel]] "
                'where RowModel is a BaseModel'
            )
        return row
    if view == 'tree':
        row = _row_model_of(annotation, view)
        if row is None:
            raise ProbeDetailsConfigError(
                f"{model.__name__}.{name}: view 'tree' requires list[RowModel] where RowModel is a BaseModel"
            )
        return row
    if view == 'keyvalue':
        args = typing.get_args(ann)
        ok = origin is dict and len(args) == 2 and args[0] is str and _unwrap_optional(args[1]) in _SCALARS
        if not ok:
            raise ProbeDetailsConfigError(
                f"{model.__name__}.{name}: view 'keyvalue' requires dict[str, <scalar>] (str/int/float/bool values)"
            )
        return None
    if view == 'dict':
        if not (ann is dict or origin is dict):
            raise ProbeDetailsConfigError(f"{model.__name__}.{name}: view 'dict' requires a dict field")
        return None
    if view == 'custom':
        # No type constraint: the renderer receives the raw JSON value,
        # whatever shape the field is.
        return None
    # view in ('string', 'badge')
    if ann not in _SCALARS:
        raise ProbeDetailsConfigError(
            f"{model.__name__}.{name}: view '{view}' requires a scalar field (str/int/float/bool)"
        )
    return None


def build_layout(model: type[BaseModel]) -> ProbeDetailsLayout:
    """Introspect ``model`` into the layout descriptor, validating every rule.

    Called at app startup (fail-fast) and cached — the descriptor is
    immutable per model class, so one build serves every probe.
    """
    cached = _layout_cache.get(model)
    if cached is not None:
        return cached
    sections: dict[str, list[ProbeDetailsEntry]] = {}
    for name, info in model.model_fields.items():
        meta = _probe_meta(model, name, info)
        view: ViewKind = meta['view']
        if info.is_required():
            raise ProbeDetailsConfigError(
                f'{model.__name__}.{name}: probe details fields must have a default — '
                'the framework constructs the empty instance itself'
            )
        row_model = _validate_view(model, name, view, info.annotation)
        columns = None
        if row_model is not None:
            columns = [ProbeDetailsColumn(key=rname, label=_prettify(rname)) for rname in row_model.model_fields]
        group_by: list[str] | None = meta.get('group_by')
        if view == 'tree' and row_model is not None and group_by:
            # probe_field already validated shape (non-empty, <=TREE_MAX_DEPTH,
            # unique); membership needs the row model, so it lives here.
            for key_name in group_by:
                if key_name not in row_model.model_fields:
                    raise ProbeDetailsConfigError(
                        f"{model.__name__}.{name}: group_by names '{key_name}', "
                        f'which is not a field of {row_model.__name__}'
                    )
        link_template = meta.get('link_template')
        badge_colors = meta.get('badge_colors')
        fmt = meta.get('format')
        hint = meta.get('hint')
        renderer = meta.get('renderer')
        columns_map: dict[str, Column] | None = meta.get('columns')
        detail: Detail | None = meta.get('detail')
        where = f'{model.__name__}.{name}'
        if row_model is not None:
            row_fields = row_model.model_fields
            if columns_map is not None:
                for col_name in columns_map:
                    if col_name not in row_fields:
                        raise ProbeDetailsConfigError(
                            f"{where}: columns names '{col_name}', which is not a field of {row_model.__name__}"
                        )
                columns = [
                    ProbeDetailsColumn(
                        key=col_name,
                        label=opts.label or _prettify(col_name),
                        link_template=opts.link_template,
                        badge_colors=opts.badge_colors,
                        format=opts.format,
                        hint=opts.hint,
                        renderer=opts.renderer,
                    )
                    for col_name, opts in columns_map.items()
                ]
                for col in columns:
                    for template in (col.link_template, col.hint):
                        if template:
                            _validate_template(template, where=f'{where}.{col.key}', row_fields=row_fields)
                    if col.badge_colors is not None and not col.badge_colors:
                        raise ProbeDetailsConfigError(f'{where}.{col.key}: badge_colors must not be empty')
                    if col.badge_colors:
                        for value_name, color in col.badge_colors.items():
                            if color not in BADGE_COLOR_NAMES:
                                raise ProbeDetailsConfigError(
                                    f"{where}.{col.key}: unknown color '{color}' for badge value "
                                    f"'{value_name}' (expected one of {BADGE_COLOR_NAMES})"
                                )
                    _validate_column_renderer(col, where=f'{where}.{col.key}')
            if detail is not None:
                if not detail.elements:
                    raise ProbeDetailsConfigError(f'{where}: detail requires at least one element')
                if detail.title:
                    _validate_template(detail.title, where=where, row_fields=row_fields)
                for i, element in enumerate(detail.elements):
                    el_where = f'{where}.detail[{i}]'
                    if element.view == 'links':
                        if element.field is not None or not element.links:
                            raise ProbeDetailsConfigError(
                                f"{el_where}: view 'links' requires links=[Link(...)] and no field"
                            )
                        if element.renderer is not None:
                            raise ProbeDetailsConfigError(f"{el_where}: renderer is only valid with view='custom'")
                        for link in element.links:
                            _validate_template(link.template, where=el_where, row_fields=row_fields)
                    else:
                        if element.field is None or element.links is not None:
                            raise ProbeDetailsConfigError(
                                f'{el_where}: element requires field= (links= is only for view=links)'
                            )
                        if element.field not in row_fields:
                            raise ProbeDetailsConfigError(
                                f"{el_where}: names '{element.field}', which is not a field of {row_model.__name__}"
                            )
                        if element.view == 'custom':
                            if not element.renderer:
                                raise ProbeDetailsConfigError(f"{el_where}: view 'custom' requires renderer='name'")
                            _validate_renderer_name(element.renderer, where=el_where)
                        elif element.renderer is not None:
                            raise ProbeDetailsConfigError(f"{el_where}: renderer is only valid with view='custom'")
        if link_template:
            _validate_template(link_template, where=where, row_fields=None)
        if hint and row_model is None:
            _validate_template(hint, where=where, row_fields=None)
        entry = ProbeDetailsEntry(
            key=name,
            label=meta['label'] or _prettify(name),
            view=view,
            columns=columns,
            group_by=group_by,
            link_template=link_template,
            badge_colors=badge_colors,
            format=fmt,
            hint=hint,
            detail=detail,
            renderer=renderer,
        )
        sections.setdefault(meta['section'], []).append(entry)
    layout = ProbeDetailsLayout(
        sections=[ProbeDetailsSection(title=title, entries=entries) for title, entries in sections.items()]
    )
    _layout_cache[model] = layout
    return layout


def referenced_bases(layout: ProbeDetailsLayout) -> builtins.set[str]:
    """Collect every named base a layout's templates reference.

    App startup compares this against ``ui.link_bases`` and warns about
    missing entries — a warning, not an error, because environments may
    deliberately omit a base (the UI then renders plain text).
    """
    # builtins.set, not set: this module's `set` verb shadows the builtin,
    # both in call position (set()) and in the set[str] type annotation.
    bases: builtins.set[str] = builtins.set()

    def _scan(template: str | None) -> None:
        if not template:
            return
        for token in _TEMPLATE_TOKEN_RE.findall(template):
            if token != 'value' and not token.startswith('row.'):
                bases.add(token)

    for section in layout.sections:
        for entry in section.entries:
            _scan(entry.link_template)
            _scan(entry.hint)
            for col in entry.columns or []:
                _scan(col.link_template)
                _scan(col.hint)
            if entry.detail:
                _scan(entry.detail.title)
                for element in entry.detail.elements:
                    for link in element.links or []:
                        _scan(link.template)
    return bases


# ---- per-probe state --------------------------------------------------------


class _DetailsState:
    """The live details instance plus the write log for ONE probe run.

    Constructed by the DebugRunner at probe start, bound to
    ``_active_state`` for the probe's duration, discarded with the run.
    ``stage`` / ``now_ms`` / ``on_error`` are callbacks so this core module
    never imports the uiserver layer (which owns the stage contextvar and
    the ProbeError model).
    """

    def __init__(
        self,
        *,
        model: type[BaseModel],
        layout: ProbeDetailsLayout,
        stage: Callable[[], str],
        now_ms: Callable[[], float],
        on_error: Callable[[str, str, str, str], None],
        max_writes: int = MAX_WRITES,
        max_total_bytes: int = MAX_TOTAL_BYTES,
    ) -> None:
        self.instance = model()
        self.layout = layout
        self.writes: list[ProbeDetailsWrite] = []
        self._stage = stage
        self._now_ms = now_ms
        self._on_error = on_error
        self._entries = {e.key: e for s in layout.sections for e in s.entries}
        # Row models are guaranteed present here: build_layout already
        # validated every table/tables annotation at startup.
        self._row_models = {
            name: row_model
            for name, info in model.model_fields.items()
            if self._entries[name].view in ('table', 'tables', 'tree')
            and (row_model := _row_model_of(info.annotation, self._entries[name].view)) is not None
        }
        self._max_writes = max_writes
        self._max_total_bytes = max_total_bytes
        self._bytes = 0
        self._capped = False

    # -- write admission ------------------------------------------------------

    def _admit(self, field: str, op: str) -> bool:
        """Cap check + existence check. Emits at most one cap error per probe.

        The two caps get distinct messages naming the configured limit and
        the config key: "write cap exceeded" alone sent operators raising
        the wrong knob (or misspelling its env var) with no way to tell.
        """
        if self._capped:
            return False
        if len(self.writes) >= self._max_writes:
            self._capped = True
            self._on_error(
                field,
                op,
                'ProbeDetailsError',
                f'max_writes cap exceeded ({self._max_writes} writes) — further writes are dropped; '
                'raise ui.probe_details.max_writes (env DK_UI__PROBE_DETAILS__MAX_WRITES)',
            )
            return False
        if self._bytes >= self._max_total_bytes:
            self._capped = True
            self._on_error(
                field,
                op,
                'ProbeDetailsError',
                f'max_total_bytes cap exceeded ({self._max_total_bytes} bytes) — further writes are dropped; '
                'raise ui.probe_details.max_total_bytes (env DK_UI__PROBE_DETAILS__MAX_TOTAL_BYTES)',
            )
            return False
        if field not in self._entries:
            self._on_error(field, op, 'ProbeDetailsError', f"unknown field '{field}' on {type(self.instance).__name__}")
            return False
        return True

    def _record(self, field: str, op: Literal['set', 'append', 'update'], value: Any) -> None:
        try:
            self._bytes += len(json.dumps(value, default=str))
        except (TypeError, ValueError):
            pass  # unserializable values get a placeholder at report time; size unknown
        self.writes.append(
            ProbeDetailsWrite(field=field, op=op, origin_stage=self._stage(), ms_since_start=self._now_ms())
        )

    # -- verbs (called via the module-level functions) -------------------------

    def set_fields(self, fields: dict[str, Any]) -> None:
        for name, value in fields.items():
            if not self._admit(name, 'set'):
                continue
            try:
                type(self.instance).__pydantic_validator__.validate_assignment(self.instance, name, value)
            except ValidationError as exc:
                self._on_error(name, 'set', 'ValidationError', str(exc))
                continue
            self._record(name, 'set', value)

    def append_row(self, field: str, row: Any, group: str | None = None) -> None:
        if not self._admit(field, 'append'):
            return
        entry = self._entries[field]
        if entry.view not in ('table', 'tables', 'tree'):
            self._on_error(
                field,
                'append',
                'ProbeDetailsError',
                f"append targets table/tables/tree; '{field}' is view '{entry.view}'",
            )
            return
        if entry.view == 'tables' and (not isinstance(group, str) or not group):
            # Non-str groups (an int id, say) must not reach the dict: two
            # keys like 123 and '123' would coerce to the same JSON key at
            # serialization time and silently merge their sub-tables.
            self._on_error(
                field,
                'append',
                'ProbeDetailsError',
                f"'{field}' is view 'tables' — append needs a non-empty string group "
                '(probe.append(field, row, group=...))',
            )
            return
        if entry.view != 'tables' and group is not None:
            # A tree row carries its grouping keys as ordinary row fields,
            # so `group` is meaningless there too.
            self._on_error(
                field,
                'append',
                'ProbeDetailsError',
                f"'{field}' is view '{entry.view}' — group targets grouped tables ('tables') only",
            )
            return
        row_model = self._row_models[field]
        try:
            validated = row if isinstance(row, row_model) else row_model.model_validate(row)
        except ValidationError as exc:
            self._on_error(field, 'append', 'ValidationError', str(exc))
            return
        # Handle None (nullable container) by treating as empty list/dict
        current = getattr(self.instance, field)
        if current is None:
            empty: list[Any] | dict[str, Any] = {} if entry.view == 'tables' else []
            type(self.instance).__pydantic_validator__.validate_assignment(self.instance, field, empty)
            current = getattr(self.instance, field)
        if entry.view == 'tables':
            # Group creation order is meaningful: to_user_details renders
            # the dict as an ordered [group, rows] pair array, so the UI
            # shows sub-tables in first-append order.
            current.setdefault(group, []).append(validated)
        else:
            current.append(validated)
        self._record(field, 'append', validated)

    def update_field(self, field: str, entries: dict[str, Any]) -> None:
        if not self._admit(field, 'update'):
            return
        entry = self._entries[field]
        if entry.view not in ('keyvalue', 'dict'):
            self._on_error(
                field, 'update', 'ProbeDetailsError', f"update targets keyvalue/dict; '{field}' is view '{entry.view}'"
            )
            return
        # Handle None (nullable container) by treating as empty dict
        current = getattr(self.instance, field)
        current_dict = current if current is not None else {}
        merged = {**current_dict, **entries}
        try:
            type(self.instance).__pydantic_validator__.validate_assignment(self.instance, field, merged)
        except ValidationError as exc:
            self._on_error(field, 'update', 'ValidationError', str(exc))
            return
        self._record(field, 'update', entries)

    # -- serialization ---------------------------------------------------------

    def to_user_details(self) -> ProbeUserDetails:
        """Serialize with per-field fallback: one bad value never loses the report."""
        data: dict[str, Any] = {}
        for name in type(self.instance).model_fields:
            value = getattr(self.instance, name)
            if self._entries[name].view == 'tables' and isinstance(value, dict):
                # Tables travel as an ordered [group, rows] pair array, not
                # as a JSON object: a JS client would enumerate integer-like
                # object keys ("0", "12") numerically first, breaking the
                # first-append order the pair array pins on every backend.
                value = [[group_name, rows] for group_name, rows in value.items()]
            try:
                data[name] = json.loads(json.dumps(value, default=_json_default))
            except (TypeError, ValueError) as exc:
                data[name] = f'<unserializable: {type(exc).__name__}>'
        return ProbeUserDetails(
            model=type(self.instance).__name__,
            layout=self.layout,
            data=data,
            writes=list(self.writes),
        )


def _json_default(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode='json')
    return str(value)


# ---- the logging-like singleton API -----------------------------------------
#
# NOTE: ``set`` shadows the builtin within this module — module code below
# these definitions must not call the builtin ``set()``.

_active_state: contextvars.ContextVar[_DetailsState | None] = contextvars.ContextVar(
    'drakkar_probe_details',
    default=None,
)


def set(**fields: Any) -> None:  # shadowing builtin by design - logging-like module API
    """Set scalar / whole-value fields on the probe details model. No-op outside a probe."""
    state = _active_state.get()
    if state is None:
        return
    state.set_fields(fields)


def append(field: str, row: Any, *, group: str | None = None) -> None:
    """Add one row to a table field. Accepts the row model or a dict. No-op outside a probe.

    For a ``tables`` field (``dict[str, list[RowModel]]``) ``group`` names
    the sub-table the row lands in, creating it on first use; for a plain
    ``table`` or a ``tree`` field it must stay ``None`` — a tree row
    carries its grouping keys as ordinary row fields.
    """
    state = _active_state.get()
    if state is None:
        return
    state.append_row(field, row, group)


def update(field: str, **entries: Any) -> None:
    """Merge keys into a keyvalue/dict field. No-op outside a probe."""
    state = _active_state.get()
    if state is None:
        return
    state.update_field(field, entries)
