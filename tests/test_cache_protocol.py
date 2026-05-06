"""Tests for the ``CacheLike`` structural Protocol.

These tests verify three contracts:

1. The Protocol is ``runtime_checkable`` — ``isinstance(obj, CacheLike)``
   returns True for any object implementing the five required methods.
2. Both production cache classes — :class:`Cache` (real) and
   :class:`NoOpCache` (stub) — satisfy the Protocol structurally without
   inheriting from it.
3. A handler subclass can be instantiated with a hand-rolled ``FakeCache``
   in place of the framework-supplied ``Cache``/``NoOpCache`` and its hooks
   work end-to-end against the fake.

The point of the Protocol is to make (3) easy: tests no longer need to
construct a real ``Cache`` (and the SQLite reader connection that backs
its ``get`` fallback) just to wire a handler under test. A small in-memory
fake passes ``isinstance(..., CacheLike)`` and is accepted by code that
declares ``self.cache: CacheLike``.
"""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import BaseModel

from drakkar.cache import Cache, CacheLike, CacheScope, NoOpCache
from drakkar.handler import BaseDrakkarHandler


class FakeCache:
    """Minimal in-memory fake satisfying :class:`CacheLike` structurally.

    No inheritance from ``Cache`` or ``CacheLike`` — the whole point of
    the Protocol is that structural conformance is enough. Mirrors the
    public surface of :class:`Cache` exactly:

    - ``set`` / ``peek`` / ``delete`` / ``__contains__`` are sync.
    - ``get`` is async (matches both ``Cache.get`` and ``NoOpCache.get``).
    - TTL is ignored — tests using this fake should not depend on TTL
      semantics. ``scope`` is accepted and stored verbatim.
    """

    def __init__(self) -> None:
        # Plain dict — no LRU, no DB fallback. Sufficient for verifying
        # that handlers wired against ``CacheLike`` actually call into
        # the fake (and that ``isinstance`` recognises it).
        self._storage: dict[str, Any] = {}

    def set(
        self,
        key: str,
        value: Any,
        *,
        ttl: float | None = None,
        scope: CacheScope = CacheScope.LOCAL,
    ) -> None:
        # ttl + scope are accepted for signature parity; this fake doesn't
        # model expiration or peer-sync. Tests that need those should use
        # the real ``Cache``.
        _ = ttl
        _ = scope
        self._storage[key] = value

    def peek(self, key: str) -> Any | None:
        return self._storage.get(key)

    def delete(self, key: str) -> bool:
        # ``True`` iff the key was present — mirrors the real Cache.delete
        # contract (and Python's dict-pop convention).
        return self._storage.pop(key, None) is not None

    def __contains__(self, key: str) -> bool:
        return key in self._storage

    async def get[T: BaseModel](
        self,
        key: str,
        *,
        as_type: type[T] | None = None,
    ) -> Any | None:
        # Async to match the real cache. ``as_type`` is honoured for typed
        # revival when the stored value is a dict — this keeps the fake
        # useful in tests that exercise typed get() paths.
        value = self._storage.get(key)
        if value is None:
            return None
        if as_type is not None and isinstance(value, dict):
            return as_type.model_validate(value)
        return value


# -- Protocol membership ------------------------------------------------------


def test_fake_cache_satisfies_protocol() -> None:
    """A hand-rolled fake with the five methods passes isinstance(CacheLike).

    This is the test-double use case the Protocol exists to enable:
    no inheritance, no SQLite, just ducktype the methods and the type
    system + isinstance both recognise it.
    """
    fake = FakeCache()
    assert isinstance(fake, CacheLike)


def test_real_cache_satisfies_protocol() -> None:
    """The production :class:`Cache` class structurally implements CacheLike.

    No base-class change to ``Cache`` is needed — the Protocol describes
    what ``Cache`` already provides.
    """
    real = Cache(origin_worker_id='test-worker', max_memory_entries=10)
    assert isinstance(real, CacheLike)


def test_noop_cache_satisfies_protocol() -> None:
    """The stub :class:`NoOpCache` also structurally satisfies CacheLike.

    Important because handlers may receive either flavour — the Protocol
    has to accept both for the typed ``self.cache: CacheLike`` declaration
    on :class:`BaseDrakkarHandler` to make sense.
    """
    stub = NoOpCache()
    assert isinstance(stub, CacheLike)


def test_object_missing_methods_fails_isinstance() -> None:
    """A class that doesn't implement the full surface fails isinstance.

    ``runtime_checkable`` Protocols check method NAMES at runtime (not
    signatures), so the negative case here is "no method at all". A class
    missing ``get`` and ``set`` should be rejected.
    """

    class Incomplete:
        def peek(self, key: str) -> Any | None:
            return None

    assert not isinstance(Incomplete(), CacheLike)


# -- Handler integration ------------------------------------------------------


class _ProtocolHandler(BaseDrakkarHandler):
    """Tiny handler used to verify a fake cache wires through cleanly.

    Implements the minimum (``arrange``) and uses ``self.cache`` in a
    pattern a real handler would — sync ``set`` / ``peek`` plus async
    ``get``. The framework's ``self.cache`` is typed ``CacheLike`` after
    Phase 4 Task 4, so a ``FakeCache`` assignment must be accepted by
    the type system AND work at runtime.
    """

    async def arrange(self, messages, pending):
        # Not exercised in this test — required by BaseDrakkarHandler.
        return []

    def store_and_peek(self, key: str, value: Any) -> Any | None:
        """Round-trip a value through the cache — sync path."""
        self.cache.set(key, value)
        return self.cache.peek(key)

    async def store_and_get(self, key: str, value: Any) -> Any | None:
        """Round-trip a value through the cache — async ``get`` path."""
        self.cache.set(key, value)
        return await self.cache.get(key)


def test_handler_can_be_wired_with_fake_cache() -> None:
    """A handler accepts a FakeCache for ``self.cache`` and round-trips data.

    Mirrors the real wiring: the framework reassigns ``handler.cache``
    in :meth:`AppLifecycle._async_run`. Tests that don't spin up the
    full app can skip that step and assign directly — proving the
    Protocol-based annotation does not require a concrete ``Cache``.
    """
    handler = _ProtocolHandler()
    fake = FakeCache()
    handler.cache = fake

    # Sync round-trip — the handler's set/peek calls land on the fake.
    assert handler.store_and_peek('k1', 42) == 42
    # Membership probe — backed by FakeCache.__contains__.
    assert 'k1' in handler.cache
    # Delete returns True (was present), then False (absent).
    assert handler.cache.delete('k1') is True
    assert handler.cache.delete('k1') is False


async def test_handler_async_get_through_fake_cache() -> None:
    """Async ``get`` on a FakeCache works through a handler.

    The framework guarantees ``Cache.get`` is awaitable; the Protocol
    encodes that, and ``FakeCache.get`` mirrors it. Without the Protocol,
    swapping in a synchronous fake would silently break handler code.
    """
    handler = _ProtocolHandler()
    handler.cache = FakeCache()

    result = await handler.store_and_get('async-key', {'x': 1})
    assert result == {'x': 1}


async def test_fake_cache_get_with_as_type_revives_pydantic_model() -> None:
    """The Protocol's ``get(as_type=...)`` parity is honoured by FakeCache.

    A handler that stores a dict and later asks for a typed return must
    get a model instance back — same contract as the real ``Cache.get``.
    Verifies the test double does not silently lose ``as_type`` semantics.
    """

    class Item(BaseModel):
        name: str
        count: int

    cache = FakeCache()
    cache.set('item', {'name': 'widget', 'count': 3})

    revived = await cache.get('item', as_type=Item)
    assert isinstance(revived, Item)
    assert revived.name == 'widget'
    assert revived.count == 3


# -- Method-set drift guard ---------------------------------------------------


@pytest.mark.parametrize(
    'method_name',
    ['set', 'peek', 'delete', '__contains__', 'get'],
    ids=['set', 'peek', 'delete', 'contains', 'get'],
)
def test_protocol_requires_each_method(method_name: str) -> None:
    """Each declared method exists on the real Cache — fails loudly if removed.

    If a future refactor renames or drops one of these on ``Cache``, this
    parametrised test will catch it before the Protocol drifts out of
    sync with the implementation.
    """
    real = Cache(origin_worker_id='drift-guard', max_memory_entries=1)
    assert hasattr(real, method_name)
