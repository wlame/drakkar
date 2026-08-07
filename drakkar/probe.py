"""User-defined probe details: model annotations, layout, and the fill API.

A handler may register one Pydantic model (``probe_details_model``) whose
fields carry layout annotations from :func:`probe_field`. During a Message
Probe the runner binds a per-run state here; the module-level verbs
``set`` / ``append`` / ``update`` write into it from anywhere in handler
code and are near-zero-cost no-ops outside a probe — the logging-module
pattern, so business logic needs no ``if probing:`` guards.
"""

from __future__ import annotations

import contextvars
import json
import types
import typing
from collections.abc import Callable
from typing import Any, Literal, cast

from pydantic import BaseModel, Field, ValidationError
from pydantic.fields import FieldInfo

ViewKind = Literal['string', 'keyvalue', 'dict', 'table', 'tables', 'tree']

_VIEW_KINDS: tuple[str, ...] = ('string', 'keyvalue', 'dict', 'table', 'tables', 'tree')

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


class ProbeDetailsConfigError(ValueError):
    """A probe-details model violates the layout rules. Raised at startup."""


def probe_field(
    *,
    section: str,
    view: ViewKind,
    label: str | None = None,
    group_by: tuple[str, ...] | list[str] | None = None,
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
    extra = {
        _METADATA_KEY: {
            'section': section,
            'view': view,
            'label': label,
            'group_by': list(group_by) if group_by else None,
        }
    }
    if default_factory is not None:
        return Field(default_factory=default_factory, json_schema_extra=extra)
    return Field(default=default, json_schema_extra=extra)


# ---- wire models (mirrored 1:1 by drakkar-go and the UI) --------------------


class ProbeDetailsColumn(BaseModel):
    """One table column, derived from a row model field."""

    key: str
    label: str


class ProbeDetailsEntry(BaseModel):
    """One field of the details model as the UI renders it."""

    key: str
    label: str
    view: ViewKind
    columns: list[ProbeDetailsColumn] | None = None
    # Ordered grouping keys for view='tree' (outermost first); None for
    # every other view. The named keys are a subset of ``columns``.
    group_by: list[str] | None = None


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
    # view == 'string'
    if ann not in _SCALARS:
        raise ProbeDetailsConfigError(
            f"{model.__name__}.{name}: view 'string' requires a scalar field (str/int/float/bool)"
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
        entry = ProbeDetailsEntry(
            key=name, label=meta['label'] or _prettify(name), view=view, columns=columns, group_by=group_by
        )
        sections.setdefault(meta['section'], []).append(entry)
    layout = ProbeDetailsLayout(
        sections=[ProbeDetailsSection(title=title, entries=entries) for title, entries in sections.items()]
    )
    _layout_cache[model] = layout
    return layout


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
        """Cap check + existence check. Emits at most one cap error per probe."""
        if self._capped:
            return False
        if len(self.writes) >= self._max_writes or self._bytes >= self._max_total_bytes:
            self._capped = True
            self._on_error(field, op, 'ProbeDetailsError', 'write cap exceeded — further writes are dropped')
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
        if entry.view == 'tables' and not group:
            self._on_error(
                field,
                'append',
                'ProbeDetailsError',
                f"'{field}' is view 'tables' — append needs a non-empty group (probe.append(field, row, group=...))",
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
            # Group creation order is meaningful (dict preserves insertion
            # order end-to-end through JSON), so the UI shows sub-tables in
            # first-append order.
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
