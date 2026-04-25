"""Public façade for the handler-facing cache.

The implementation has been split across four sibling modules:

- :mod:`drakkar.cache_models`  — ``CacheScope``, ``CacheEntry``, JSON codec,
  ``Op``, ``DirtyOp``, ``_now_ms``.
- :mod:`drakkar.cache_memory`  — ``Cache`` and ``NoOpCache`` (handler-facing
  in-memory layer + DB fallback for ``get``).
- :mod:`drakkar.cache_sql`     — ``SCHEMA_CACHE_ENTRIES``, ``LWW_UPSERT_SQL``,
  ``PEER_CLUSTER_CACHE_TTL_SECONDS``.
- :mod:`drakkar.cache_engine`  — ``CacheEngine`` (SQLite write-behind, cleanup
  loop, peer-sync).

This module re-exports the public API so existing call sites and tests can
continue to use ``from drakkar.cache import ...``.

If you are reading this file looking for the implementation, follow the
imports below to the appropriate sibling module.
"""

from __future__ import annotations

# Re-exported for backward compatibility — tests historically reach through
# ``cache_module.aiosqlite`` to patch ``aiosqlite.connect`` on the shared
# module object. Keeping the import here means those patches still take
# effect against ``cache_engine`` (both modules reference the same aiosqlite
# module object).
import aiosqlite  # noqa: F401
import structlog

from drakkar.cache_engine import CacheEngine
from drakkar.cache_memory import Cache, NoOpCache
from drakkar.cache_models import (
    CacheEntry,
    CacheScope,
    DirtyOp,
    Op,
    _decode,
    _encode,
    _now_ms,
)
from drakkar.cache_sql import (
    LWW_UPSERT_SQL,
    PEER_CLUSTER_CACHE_TTL_SECONDS,
    SCHEMA_CACHE_ENTRIES,
)

# Module-level logger preserved here too so external code that did
# ``from drakkar.cache import logger`` (or referenced ``cache_module.logger``
# in test patches) keeps working. Note: this is a *separate* logger
# instance from the one used inside ``cache_engine`` — patching this one
# will not affect engine-side logging. Tests that need to spy on engine
# log calls should patch ``drakkar.cache_engine.logger`` directly.
logger = structlog.get_logger()

__all__ = [
    'LWW_UPSERT_SQL',
    'PEER_CLUSTER_CACHE_TTL_SECONDS',
    'SCHEMA_CACHE_ENTRIES',
    'Cache',
    'CacheEngine',
    'CacheEntry',
    'CacheScope',
    'DirtyOp',
    'NoOpCache',
    'Op',
    '_decode',
    '_encode',
    '_now_ms',
]
