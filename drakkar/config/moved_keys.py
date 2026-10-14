"""Config keys that moved or were removed, and the load error that names them.

The nested config sections ignore unknown keys, so a leftover old key would
otherwise be dropped without a word and its value would silently stop
applying. The table below is checked against the raw config tree (YAML
merged with ``DK_*`` environment variables) before validation; any hit fails
the load with one message that lists every old key found, in table order.

The message bytes are a contract shared with every implementation of the
config format (``tests/fixtures/config_contract.json``).
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class MovedKey:
    """One old config key and the line the load error shows for it."""

    path: str
    line: str


_STATS_REMOVED = 'removed, database statistics are no longer cached in a shared file'

MOVED_CONFIG_KEYS: tuple[MovedKey, ...] = (
    MovedKey('ui.recorder.db_dir', 'ui.recorder.db_dir -> storage.dir'),
    MovedKey('ui.recorder.archive_enabled', 'ui.recorder.archive_enabled -> history.archive.enabled'),
    MovedKey('ui.recorder.archive_window_hours', 'ui.recorder.archive_window_hours -> history.archive.window_hours'),
    MovedKey(
        'ui.recorder.archive_retention_days',
        'ui.recorder.archive_retention_days -> history.archive.retention_days',
    ),
    MovedKey(
        'ui.workers_offline_after_seconds',
        'ui.workers_offline_after_seconds -> fleet.registry.offline_after_seconds',
    ),
    MovedKey('cache.peer_sync.enabled', 'cache.peer_sync.enabled -> fleet.cache_exchange.enabled'),
    MovedKey(
        'cache.peer_sync.interval_seconds',
        'cache.peer_sync.interval_seconds -> fleet.cache_exchange.interval_seconds',
    ),
    MovedKey('cache.peer_sync.batch_size', 'cache.peer_sync.batch_size -> fleet.cache_exchange.page_size'),
    MovedKey(
        'cache.peer_sync.timeout_seconds',
        'cache.peer_sync.timeout_seconds -> fleet.cache_exchange.timeout_seconds',
    ),
    MovedKey(
        'cache.peer_sync.cycle_deadline_seconds',
        'cache.peer_sync.cycle_deadline_seconds -> fleet.cache_exchange.cycle_deadline_seconds',
    ),
    MovedKey('cache.db_dir', 'cache.db_dir: removed, the cache file lives in storage.dir'),
    MovedKey(
        'ui.recorder.dbstats_warm_interval_seconds',
        f'ui.recorder.dbstats_warm_interval_seconds: {_STATS_REMOVED}',
    ),
    MovedKey('ui.recorder.dbstats_inline_scan_limit', f'ui.recorder.dbstats_inline_scan_limit: {_STATS_REMOVED}'),
)

MOVED_KEYS_HEADER = (
    'config uses keys that moved or were removed (YAML paths; as env vars they are '
    'DK_ plus the path with "__"; see docs/upgrading.md):'
)


def find_moved_keys(tree: Mapping[str, Any]) -> list[MovedKey]:
    """Return the old keys present in a raw config tree, in table order.

    A key counts as present when its whole path exists, whatever its value
    (``None`` and ``''`` included): the operator wrote it, so it must move.
    """
    return [key for key in MOVED_CONFIG_KEYS if _has_path(tree, key.path.split('.'))]


def _has_path(node: Any, parts: list[str]) -> bool:
    """Whether the nested mappings under ``node`` contain the key path ``parts``."""
    for part in parts:
        if not isinstance(node, Mapping) or part not in node:
            return False
        node = node[part]
    return True


def moved_keys_message(found: Sequence[MovedKey]) -> str:
    """The load error: the header, then one line per found key, indented by two spaces."""
    return '\n'.join([MOVED_KEYS_HEADER, *(f'  {key.line}' for key in found)])
