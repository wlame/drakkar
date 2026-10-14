"""Request parsing for ``GET /api/v1/peer/cache/changes``.

Peers pull the ``cluster`` and ``global`` rows this worker originated,
page by page, after a ``(updated_at_ms, key)`` cursor. The parameters are
parsed here, as plain strings, so every implementation answers the same
400 message for the same input (``tests/fixtures/cache_changes.json``).
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass

# Scopes a peer may ask for, in the order a parsed query lists them.
# ``local`` never leaves the worker.
EXCHANGED_SCOPES: tuple[str, ...] = ('cluster', 'global')
DEFAULT_LIMIT = 500
MAX_LIMIT = 1000
# A card mark carries at most this many characters of its key: a reader
# skips a card over 4096 bytes, so one long cache key must not grow it
# without bound. A cut key sorts at or before the full key, so the gate
# still pulls when something is new, except for a later key that shares
# the whole cut prefix at the same millisecond.
MARK_KEY_MAX_CHARS = 128

SCOPES_ERROR = 'scopes must be cluster, global or both'
CURSOR_PAIR_ERROR = 'after_ms and after_key must be given together'
AFTER_MS_ERROR = 'after_ms must be an integer'
LIMIT_ERROR = 'limit must be an integer from 1 to 1000'

# Only an optional minus and ASCII digits: no '+', spaces or '_' (which
# int() would accept), so every implementation reads the same values.
_INTEGER = re.compile(r'-?[0-9]+', re.ASCII)
_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1
_INT64_MAX_CHARS = len(str(_INT64_MIN))

type Cursor = tuple[int, str]


@dataclass(frozen=True, slots=True)
class ChangesQuery:
    """A valid changes request: scopes, the cursor (or ``None`` from the start), the page size."""

    scopes: tuple[str, ...]
    after: Cursor | None
    limit: int


def parse_changes_query(params: Mapping[str, str]) -> ChangesQuery | str:
    """Return the query, or the 400 message of the first check that fails.

    ``params`` holds one value per name (the last one of a repeated
    parameter). The checks run in a fixed order: scopes, the cursor pair,
    ``after_ms``, ``limit``.
    """
    scopes = _parse_scopes(params.get('scopes'))
    if scopes is None:
        return SCOPES_ERROR
    has_after_ms, has_after_key = 'after_ms' in params, 'after_key' in params
    if has_after_ms != has_after_key:
        return CURSOR_PAIR_ERROR
    after: Cursor | None = None
    if has_after_ms:
        after_ms = _parse_int64(params['after_ms'])
        if after_ms is None:
            return AFTER_MS_ERROR
        after = (after_ms, params['after_key'])
    limit = DEFAULT_LIMIT
    if 'limit' in params:
        parsed_limit = _parse_int64(params['limit'])
        if parsed_limit is None or not 1 <= parsed_limit <= MAX_LIMIT:
            return LIMIT_ERROR
        limit = parsed_limit
    return ChangesQuery(scopes=scopes, after=after, limit=limit)


def _parse_scopes(raw: str | None) -> tuple[str, ...] | None:
    """The requested scopes in canonical order, or ``None`` when any item is not an exchanged scope."""
    if not raw:
        return None
    items = raw.split(',')
    if any(item not in EXCHANGED_SCOPES for item in items):
        return None
    return tuple(scope for scope in EXCHANGED_SCOPES if scope in items)


def _parse_int64(raw: str) -> int | None:
    """A signed 64-bit integer written as plain digits, else ``None``."""
    # More characters than any int64 has are out of range already, and
    # int() refuses strings past 4300 digits with a ValueError.
    if len(raw) > _INT64_MAX_CHARS or not _INTEGER.fullmatch(raw):
        return None
    value = int(raw)
    if not _INT64_MIN <= value <= _INT64_MAX:
        return None
    return value
