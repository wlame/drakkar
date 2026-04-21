"""Handler-accessible key/value cache: models, scope enum, and JSON codec.

This module holds the building blocks used by the cache engine (added in
later tasks) and the handler-facing Cache API. Keeping the models and codec
isolated lets tests exercise the serialization contract without spinning up
SQLite, and makes the encode/decode semantics easy to reason about in
isolation — every `set` flows through `_encode`, every `get` flows through
`_decode`.

Scope model
-----------
`CacheScope` names the visibility contract a cache entry has in peer-sync:

- ``LOCAL``  — this worker only; never pulled by peers
- ``CLUSTER`` — workers with the same ``cluster_name``; pulled by same-cluster peers
- ``GLOBAL`` — visible to all peers regardless of cluster

The enum uses Python's ``StrEnum`` so members double as strings for SQL
parameter binding and JSON round-tripping with zero coercion.

Serialization (JSON-only in v1)
-------------------------------
We deliberately restrict cached values to JSON-serializable shapes:
primitives, lists, dicts, and Pydantic models (which serialize via
``model_dump_json``). The rationale is three-fold:

1. **Cross-worker portability.** Peer-sync moves entries across workers that
   may run different Python minor versions or different code revisions.
   JSON is the lowest-common-denominator format — a row written by
   worker-A on Python 3.13 will parse cleanly on worker-B running 3.14.

2. **Inspectability.** The debug UI shows cached values directly from the
   DB. JSON is human-readable; opaque binary blobs would force us to
   embed a decoder into the UI server per type.

3. **Safety.** Binary object serializers permit arbitrary code execution
   on load. Since peers can write to each other's DBs via sync, a
   compromised peer with such a codec would become an RCE vector. JSON
   parsing is bounded to data values — no code paths to execute.

For Pydantic-typed values, callers can pass ``as_type=SomeModel`` to
``Cache.get()`` (added in Task 9) and ``_decode`` will revive the value
through ``model_validate`` rather than returning a plain dict.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from pydantic import BaseModel

# ---- scope enum -------------------------------------------------------------


class CacheScope(StrEnum):
    """Visibility discriminator for a cache entry.

    The string values match the SQL schema's ``CHECK(scope IN (...))``
    constraint and the peer-sync scope-filter WHERE clauses. Keep the
    values in lockstep with ``cache_entries.scope`` column constraints.

    StrEnum inheritance means ``CacheScope.LOCAL`` is both a distinct
    enum member and an ordinary string equal to ``'local'`` — useful
    when binding SQL parameters or serializing to JSON.
    """

    LOCAL = 'local'
    CLUSTER = 'cluster'
    GLOBAL = 'global'


# ---- cache entry dataclass --------------------------------------------------


@dataclass(slots=True)
class CacheEntry:
    """In-memory representation of one cached row, mirroring the SQL schema.

    Fields:
        key: primary key, unique per worker DB.
        scope: visibility (see ``CacheScope``).
        value: serialized JSON text — opaque to the engine, typed by the
            caller via ``_encode`` / ``_decode``.
        size_bytes: UTF-8 byte length of ``value``. Caller populates this
            at construction time from ``_encode``'s second return value;
            we store it rather than recomputing so the Prometheus
            ``bytes_in_memory`` gauge can maintain a running sum without
            walking the dict on every scrape.
        created_at_ms: wall-clock ms when this key was first stored.
        updated_at_ms: wall-clock ms of the most recent write. Drives LWW
            conflict resolution during peer sync.
        expires_at_ms: wall-clock ms after which the entry is dead;
            ``None`` means "never expires".
        origin_worker_id: the worker that performed the write. Used as
            the lexicographic tiebreaker when two entries share
            ``updated_at_ms`` during LWW resolution.

    Using ``slots=True`` cuts the per-entry memory footprint since the
    in-memory dict can hold tens of thousands of entries; a regular
    dataclass would carry the full ``__dict__`` per instance.
    """

    key: str
    scope: CacheScope
    value: str
    size_bytes: int
    created_at_ms: int
    updated_at_ms: int
    expires_at_ms: int | None
    origin_worker_id: str


# ---- JSON codec -------------------------------------------------------------


def _encode(value: Any) -> tuple[str, int]:
    """Serialize a value to JSON text and report its UTF-8 byte length.

    The byte length is what SQLite will store and what Prometheus gauges
    count — we compute it once here so callers never have to re-encode
    just to learn the size.

    Pydantic models go through ``model_dump_json()`` rather than being
    handed to ``json.dumps`` — the latter would hit ``__dict__`` or fail
    outright, missing field aliases, ``Enum``/``datetime`` coercion, and
    the strict serialization mode the user declared on the model.

    All other inputs (primitives, lists, dicts) go through ``json.dumps``.
    Anything that isn't JSON-serializable raises ``TypeError`` — that's
    intentional: we want a clear error at ``set()`` time rather than a
    silent failure at read time on a peer that can't parse the value.

    Args:
        value: any JSON-serializable object, or a Pydantic ``BaseModel``.

    Returns:
        ``(json_text, byte_length)`` where ``byte_length`` is the number
        of UTF-8 bytes in ``json_text``.
    """
    # By the way: we use `isinstance` rather than duck-typing on
    # `model_dump_json` — a plain dict with a `model_dump_json` method
    # is not a Pydantic model and should take the json.dumps path.
    text = value.model_dump_json() if isinstance(value, BaseModel) else json.dumps(value)
    return text, len(text.encode('utf-8'))


def _decode[T: BaseModel](json_text: str, *, as_type: type[T] | None = None) -> Any:
    """Parse JSON text back to a Python value.

    Without ``as_type``, returns the raw ``json.loads`` result — primitives
    stay primitives, lists stay lists, dicts stay plain dicts. No magic
    revival: a dict-shaped payload will not be silently upcast to a
    Pydantic model, even if the caller "meant" one. Explicit is better
    than implicit.

    With ``as_type=SomeModel``, the parsed value is handed to
    ``SomeModel.model_validate(...)`` — Pydantic's standard entry point
    for external-data validation. Use this when the caller knows the
    value was stored from a specific Pydantic model and wants a typed
    instance back.

    Raises:
        json.JSONDecodeError: if ``json_text`` is not valid JSON (including
            an empty string). Surfacing the error rather than returning
            ``None`` makes data-corruption bugs visible immediately.
        pydantic.ValidationError: if ``as_type`` is provided and the
            parsed value doesn't match the model's declared shape.
    """
    parsed = json.loads(json_text)
    if as_type is not None:
        return as_type.model_validate(parsed)
    return parsed
