"""Tests for the ``CacheLike`` structural Protocol.

These tests pin the contract between the Protocol and the production
cache classes: both :class:`Cache` (real) and :class:`NoOpCache` (stub)
satisfy ``CacheLike`` structurally without inheriting from it, and every
method the Protocol declares exists on the real ``Cache`` — so a rename
or removal on either side fails loudly here before handlers break.
"""

from __future__ import annotations

import pytest

from drakkar.cache import Cache, CacheLike, NoOpCache

# -- Protocol membership ------------------------------------------------------


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
