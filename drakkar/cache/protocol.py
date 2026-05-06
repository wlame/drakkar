"""Structural protocol describing the handler-facing cache surface.

Why this exists
---------------
The framework hands every handler a ``self.cache`` object. In production
that object is either :class:`drakkar.cache.memory.Cache` (real LRU + DB
fallback) or :class:`drakkar.cache.memory.NoOpCache` (silent stub used when
``cache.enabled=false``). User code calls a small set of methods on it —
``set``, ``peek``, ``delete``, ``__contains__``, ``get`` — and never touches
the engine or the SQLite layer directly.

A nominal Union (``Cache | NoOpCache``) describes those two concrete classes
exactly; this Protocol describes any object that "quacks like a cache". The
distinction matters in two places:

1. **Tests.** A test that wants a hand-rolled fake (no SQLite, no event
   loop wiring) can implement the five methods on a small class without
   subclassing ``Cache``. ``isinstance(fake, CacheLike)`` returns True
   thanks to ``runtime_checkable``.
2. **Alternate backends.** Future work may add a Redis-backed cache, a
   distributed cache, or a process-local mock. They satisfy the Protocol
   structurally; no base-class change to ``Cache`` is needed.

The Protocol intentionally describes only the **public** surface a handler
relies on. Engine-internal hooks like ``attach_reader_db``, ``swap_dirty``,
``restore_dirty``, ``_invalidate_memory_keys``, ``_expire_purge`` are not
part of the user contract — they couple memory layout to the SQLite
write-behind path and live on ``Cache`` only.

Design note (deviates from Phase 4 plan's design sketch)
--------------------------------------------------------
The plan's design sketch listed seven methods (``get``, ``put``,
``invalidate``, ``invalidate_keys``, ``copy_scope``, ``hit_rate_recent``,
``flush_all``). Those names do not match the real handler API. The real
API — verified against ``drakkar/cache/memory.py:Cache`` and ``NoOpCache``
— is ``set``, ``peek``, ``delete``, ``__contains__``, ``get``. The Protocol
mirrors what handlers actually call on ``self.cache``; the plan checkbox
text records the divergence.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    # ``CacheScope`` is referenced in the ``set`` signature default below.
    # Import only under TYPE_CHECKING to keep this protocol module light —
    # it is meant to be importable from type-hint sites without dragging in
    # the model layer at runtime.
    from pydantic import BaseModel

    from drakkar.cache.models import CacheScope


@runtime_checkable
class CacheLike(Protocol):
    """Structural interface for the handler-facing cache.

    Any class providing the five methods below — with compatible signatures
    — is treated as a cache by the framework's type checker and by
    ``isinstance(obj, CacheLike)`` (Protocol is ``runtime_checkable``).

    Both :class:`drakkar.cache.memory.Cache` and
    :class:`drakkar.cache.memory.NoOpCache` satisfy this Protocol without
    inheriting from it — that is the whole point of structural typing.

    Method semantics mirror :class:`Cache` exactly; consult that class for
    the canonical behaviour, TTL conventions, and metric side effects.
    """

    def set(
        self,
        key: str,
        value: Any,
        *,
        ttl: float | None = None,
        scope: CacheScope = ...,
    ) -> None:
        """Store ``value`` under ``key`` with optional TTL and scope.

        The ``...`` default for ``scope`` is the Protocol idiom for "the
        implementation supplies its own default" — concrete implementations
        use ``CacheScope.LOCAL``. Type checkers do not enforce default
        values across Protocol/implementation pairs, so the literal here
        is decorative.
        """
        ...

    def peek(self, key: str) -> Any | None:
        """Synchronous in-memory probe; return decoded value or None.

        Pure memory lookup with no DB fallback — TTL is honoured, but a
        miss is reported as ``None`` regardless of whether the key exists
        on disk. Use ``get`` when DB fallback is desired.
        """
        ...

    def delete(self, key: str) -> bool:
        """Remove ``key`` from memory and schedule a DB-row deletion on flush.

        Returns ``True`` iff the key was present in memory before the call
        (matches the dict-pop convention). The dirty-op is recorded
        regardless so a row that lives only on disk is still purged on the
        next flush.
        """
        ...

    def __contains__(self, key: str) -> bool:
        """Membership probe (TTL-aware), no LRU bookkeeping.

        ``key in cache`` returns ``True`` iff the entry is in memory and
        unexpired. Does not touch the DB; an entry that lives only on
        disk reports ``False`` until a ``get`` warms it into memory.
        """
        ...

    async def get[T: BaseModel](
        self,
        key: str,
        *,
        as_type: type[T] | None = None,
    ) -> Any | None:
        """Lookup with memory→DB fallback; ``None`` on miss.

        When ``as_type`` is supplied, the decoded JSON is revived through
        ``model_validate`` for typed return. Otherwise returns the raw
        ``json.loads`` result. The async signature is part of the contract
        — ``NoOpCache.get`` is also async so handler call sites do not
        change between enabled/disabled cache states.
        """
        ...
