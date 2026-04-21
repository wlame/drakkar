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
import time
from collections import OrderedDict
from dataclasses import dataclass
from enum import Enum, StrEnum
from typing import Any

from pydantic import BaseModel

from drakkar import metrics

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


# ---- dirty-op tracking ------------------------------------------------------
#
# The Cache accumulates pending writes in a ``_dirty`` map keyed by cache
# key. Each entry records what the next flush should do: either UPSERT a
# value (``Op.SET`` with the current ``CacheEntry`` payload) or DELETE a
# row (``Op.DELETE``, no payload needed — the key is enough for SQL).
#
# Flush (added in Task 7) swaps the ``_dirty`` map atomically under the
# GIL:
#
#     snapshot, self._dirty = self._dirty, {}
#
# Any ``set`` or ``delete`` that lands **during** the flush writes into the
# fresh empty dict and gets picked up by the next cycle — no locking needed
# for in-memory coherence because Python dict ops are single-operation atomic
# under CPython's GIL. (If we ever run on a free-threaded interpreter, we'd
# wrap mutation in a lock; that's out of scope for v1.)


class Op(Enum):
    """What the flush worker should do with a dirty key.

    ``SET`` → upsert the attached ``CacheEntry`` into ``cache_entries``
    via the ``LWW_UPSERT_SQL`` (Task 7).

    ``DELETE`` → remove the row from ``cache_entries`` by key. Local-only:
    deletes do not propagate to peers (documented sharp edge — see plan
    "Delete is deliberately local-only").
    """

    SET = 'set'
    DELETE = 'delete'


@dataclass(slots=True)
class DirtyOp:
    """One pending mutation awaiting flush to the local SQLite DB.

    Fields:
        op: the operation kind — SET or DELETE.
        entry: the ``CacheEntry`` to upsert for SET ops; ``None`` for DELETE
            (the row removal only needs the key, which is the dict key of
            the dirty map itself).

    The entry reference is kept so flush can serialize the current value
    without another ``_memory`` lookup — necessary because the entry may
    have been LRU-evicted from ``_memory`` between the mutation and the
    flush. The DB is the source of truth, but the dirty map is the pipeline
    that gets writes there; losing a dirty op because the entry was evicted
    from memory would silently drop the write.
    """

    op: Op
    entry: CacheEntry | None


# ---- handler-facing cache ---------------------------------------------------


def _now_ms() -> int:
    """Wall-clock milliseconds since epoch.

    Used for ``created_at_ms`` / ``updated_at_ms`` / ``expires_at_ms``.
    Wall-clock is deliberate (not ``time.monotonic``): these timestamps
    participate in LWW resolution across workers, where each worker's
    monotonic clock has a different zero. Comparing wall-clock across
    workers assumes NTP-sync'd hosts — good enough for eventual consistency,
    and the usual baseline for LWW-based designs.
    """
    return int(time.time() * 1000)


class Cache:
    """In-memory key/value cache surfaced to handlers as ``self.cache``.

    This class covers the **synchronous**, memory-only operations of the
    cache contract: ``set``, ``peek``, ``delete``, ``__contains__``. All of
    these are GIL-safe dict manipulations; no I/O happens on these calls.

    The async DB-backed ``get()`` (with memory-miss → DB fallback) lands in
    Task 9 together with the reader connection. Flush to SQLite (Task 7)
    and peer sync (Tasks 11-13) read from the ``_dirty`` map this class
    populates.

    Why ``OrderedDict``?
    --------------------
    LRU eviction (when ``max_memory_entries`` is set) relies on access
    order. ``OrderedDict.move_to_end(key)`` is O(1) and the class preserves
    insertion order by contract — regular ``dict`` makes insertion order
    guarantees too, but lacks the ``move_to_end`` primitive, so we'd
    otherwise have to pop-and-reinsert. OrderedDict is the canonical fit.

    Eviction policy
    ---------------
    When ``max_memory_entries`` is set and the dict would grow past the
    cap, we pop the LRU key (the first item in iteration order). The
    evicted key's dirty-op is **preserved** — the DB is the source of
    truth, and any pending SET still needs to reach it via flush. Losing
    the dirty entry would silently drop the write. Evicted entries fall
    through to the DB on next read; the DB will re-warm ``_memory`` in
    ``get()`` (Task 9).
    """

    def __init__(
        self,
        *,
        origin_worker_id: str,
        max_memory_entries: int | None = None,
    ) -> None:
        """Construct a Cache for a single handler.

        Args:
            origin_worker_id: the stable identifier for this worker, stored
                on every written entry for LWW tiebreaks during peer sync.
            max_memory_entries: optional LRU cap. ``None`` (default) means
                unbounded — the dict grows at the caller's discretion.
        """
        # insertion-ordered; ``move_to_end`` bumps a key to MRU position
        self._memory: OrderedDict[str, CacheEntry] = OrderedDict()
        # pending mutations for the next flush (Task 7 consumes this)
        self._dirty: dict[str, DirtyOp] = {}
        self._origin_worker_id = origin_worker_id
        self._max_memory_entries = max_memory_entries

    # -- sync API -------------------------------------------------------------

    def set(
        self,
        key: str,
        value: Any,
        *,
        ttl: float | None = None,
        scope: CacheScope = CacheScope.LOCAL,
    ) -> None:
        """Store ``value`` under ``key`` in memory and mark it dirty for flush.

        Args:
            key: unique key within the worker's cache namespace.
            value: any JSON-serializable object, or a Pydantic model
                (serialized via ``model_dump_json``). See ``_encode``.
            ttl: optional time-to-live in **seconds**; ``None`` means
                "never expires". Internally we convert to a wall-clock
                ``expires_at_ms`` so downstream comparisons are trivial.
            scope: visibility for peer sync (default LOCAL — safe choice;
                LOCAL entries never leave this worker).

        Side effects:
            - Writes to ``_memory`` (creates or overwrites).
            - Writes to ``_dirty`` with an ``Op.SET`` referencing the entry.
            - Bumps the key to MRU position in the OrderedDict.
            - May trigger LRU eviction if ``max_memory_entries`` is set
              and the cap is now exceeded.
        """
        encoded, size_bytes = _encode(value)
        now_ms = _now_ms()
        expires_at_ms = now_ms + int(ttl * 1000) if ttl is not None else None

        # Preserve ``created_at_ms`` across overwrites — the column is
        # "when did this key first appear", not "when was this value
        # written". ``updated_at_ms`` is the one that changes every set.
        existing = self._memory.get(key)
        created_at_ms = existing.created_at_ms if existing is not None else now_ms

        entry = CacheEntry(
            key=key,
            scope=scope,
            value=encoded,
            size_bytes=size_bytes,
            created_at_ms=created_at_ms,
            updated_at_ms=now_ms,
            expires_at_ms=expires_at_ms,
            origin_worker_id=self._origin_worker_id,
        )

        # Place in memory and bump to MRU. If the key already existed,
        # OrderedDict's insertion replaces the value but keeps its slot —
        # we then explicitly move_to_end so overwrites also count as a
        # recency bump (consistent with typical LRU intuition).
        self._memory[key] = entry
        self._memory.move_to_end(key)

        # Track dirty op. A SET overrides any prior DELETE for the same
        # key — that's the expected last-write-wins at the dirty-map level,
        # matching the SQL LWW at the DB level.
        self._dirty[key] = DirtyOp(op=Op.SET, entry=entry)

        self._maybe_evict()

    def peek(self, key: str) -> Any | None:
        """Return the decoded value if present **and** unexpired, else None.

        Pure memory lookup — no DB access. If the entry is expired, we
        opportunistically evict it so stale data doesn't linger until the
        cleanup cycle runs. ``peek`` also bumps the key to MRU position
        (the call counts as an access for LRU bookkeeping).

        Returns ``None`` for missing or expired keys. To distinguish the
        two cases, use ``__contains__`` — but usually callers don't need
        to.
        """
        entry = self._memory.get(key)
        if entry is None:
            return None
        if self._is_expired(entry):
            # Opportunistic eviction. The dirty map is untouched: if there
            # was a pending SET, letting it flush and then be cleaned up
            # by the expiration cleanup cycle is cheaper than trying to
            # untangle it here. A pending DELETE stays as-is.
            self._memory.pop(key, None)
            return None
        # access → MRU bump
        self._memory.move_to_end(key)
        return _decode(entry.value)

    def delete(self, key: str) -> bool:
        """Remove ``key`` from memory and schedule a DB row deletion on next flush.

        Returns True if the key was present in memory, False otherwise.
        The dirty-op is always recorded (``Op.DELETE``) regardless of
        presence in memory — because the DB may still hold a row for this
        key that we can't see from memory alone.

        **Local-only**: this does NOT propagate to peers. A peer who synced
        our value before the delete still has its own copy and may re-push
        it back via LWW if its ``updated_at_ms`` is newer. See the "delete
        is deliberately local-only" note in the plan for the full edge
        discussion — use TTL for cross-worker invalidation.
        """
        present = key in self._memory
        self._memory.pop(key, None)
        # A DELETE op overwrites any prior SET; flush will see the final
        # state only. Entry is None since the row is going away.
        self._dirty[key] = DirtyOp(op=Op.DELETE, entry=None)
        return present

    def __contains__(self, key: str) -> bool:
        """Membership test including TTL check.

        Returns True if the key is in memory and unexpired; False otherwise.
        An expired entry is not a member. We do **not** bump LRU here —
        membership is a probe, not an access.
        """
        entry = self._memory.get(key)
        if entry is None:
            return False
        return not self._is_expired(entry)

    # -- internals ------------------------------------------------------------

    @staticmethod
    def _is_expired(entry: CacheEntry) -> bool:
        """True iff the entry has a TTL that has already elapsed."""
        if entry.expires_at_ms is None:
            return False
        return _now_ms() >= entry.expires_at_ms

    def _maybe_evict(self) -> None:
        """Drop the LRU key if the memory dict exceeds ``max_memory_entries``.

        Called after every ``set``. Evicts at most one entry per call —
        since we never bulk-insert without calls to ``set``, that matches
        the growth rate. The evicted key's dirty-op is preserved: the DB
        is the source of truth, and losing a pending SET before flush
        would silently drop the user's write.

        Note on ``popitem(last=False)``: OrderedDict's popitem pops the
        first inserted (i.e. least-recently-touched after our move_to_end
        calls), which is exactly LRU.
        """
        cap = self._max_memory_entries
        if cap is None:
            return
        while len(self._memory) > cap:
            # pop oldest by access order (move_to_end in set/peek keeps
            # recently-touched keys at the tail)
            self._memory.popitem(last=False)
            metrics.cache_evictions.inc()
