"""User-defined probe details: model annotations, layout, and the fill API.

A handler may register one Pydantic model (``probe_details_model``) whose
fields carry layout annotations from :func:`probe_field`. During a Message
Probe the runner binds a per-run state here; the module-level verbs
``set`` / ``append`` / ``update`` write into it from anywhere in handler
code and are near-zero-cost no-ops outside a probe — the logging-module
pattern, so business logic needs no ``if probing:`` guards.
"""

from __future__ import annotations

import types
import typing
from collections.abc import Callable
from typing import Any, Literal, cast

from pydantic import BaseModel, Field
from pydantic.fields import FieldInfo

ViewKind = Literal['string', 'keyvalue', 'dict', 'table']

_VIEW_KINDS: tuple[str, ...] = ('string', 'keyvalue', 'dict', 'table')
_METADATA_KEY = 'drakkar_probe'
_SCALARS = (str, int, float, bool)

MAX_WRITES = 10_000
MAX_TOTAL_BYTES = 5_000_000


class ProbeDetailsConfigError(ValueError):
    """A probe-details model violates the layout rules. Raised at startup."""


def probe_field(
    *,
    section: str,
    view: ViewKind,
    label: str | None = None,
    default: Any = ...,
    default_factory: Callable[[], Any] | None = None,
) -> Any:
    """Declare one probe-details field and its presentation.

    Thin wrapper over :func:`pydantic.Field` that stashes the layout
    metadata in ``json_schema_extra`` — one artifact, nothing to drift.
    """
    if view not in _VIEW_KINDS:
        raise ProbeDetailsConfigError(f"probe_field: unknown view '{view}' (expected one of {_VIEW_KINDS})")
    if not section:
        raise ProbeDetailsConfigError('probe_field: section must be a non-empty string')
    extra = {_METADATA_KEY: {'section': section, 'view': view, 'label': label}}
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


def _validate_view(model: type[BaseModel], name: str, view: str, annotation: Any) -> type[BaseModel] | None:
    """Check the field type fits the declared view. Returns the row model for tables."""
    ann = _unwrap_optional(annotation)
    origin = typing.get_origin(ann)
    if view == 'table':
        row = typing.get_args(ann)[0] if origin is list and typing.get_args(ann) else None
        if row is None or not (isinstance(row, type) and issubclass(row, BaseModel)):
            raise ProbeDetailsConfigError(
                f"{model.__name__}.{name}: view 'table' requires list[RowModel] where RowModel is a BaseModel"
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
        entry = ProbeDetailsEntry(key=name, label=meta['label'] or _prettify(name), view=view, columns=columns)
        sections.setdefault(meta['section'], []).append(entry)
    layout = ProbeDetailsLayout(
        sections=[ProbeDetailsSection(title=title, entries=entries) for title, entries in sections.items()]
    )
    _layout_cache[model] = layout
    return layout
