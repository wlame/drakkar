"""Shared helper for discovering peer worker DBs via live symlinks.

Drakkar workers expose their SQLite state through timestamped DB files
accompanied by a stable live-symlink:

    /shared/<worker>-<timestamp>.db          # the actual DB (rotated)
    /shared/<worker>-live.db        -> ...   # points at the current file
    /shared/<worker>-cache.db       -> ...   # cache DB, same convention

Peers read via the symlink so the target can rotate underneath them
without breaking lookups. We resolve through `os.path.realpath` once per
call so we open the actual file (aiosqlite URI connections with relative
paths or symlinks that later break are painful to debug).

Callers:

- `EventRecorder.discover_workers()` uses suffix `'-live.db'` and then
  reads the `worker_config` table from each resolved target.
- `CacheEngine._sync_once()` uses suffix `'-cache.db'` and pulls rows
  from each peer's `cache_entries` table with a scope-aware filter.
"""

from __future__ import annotations

import asyncio
import glob
import os
from collections.abc import AsyncIterator


def _scan_peer_dbs(db_dir: str, suffix: str, self_worker_name: str) -> list[tuple[str, str]]:
    """Synchronous filesystem scan behind :func:`discover_peer_dbs`.

    All the stat/glob/realpath calls live here so the async wrapper can
    run the whole scan in one worker thread — `db_dir` is typically a
    shared (often NFS) mount, where a single stalled stat call would
    otherwise block the event loop.
    """
    if not db_dir or not os.path.isdir(db_dir):
        return []

    results: list[tuple[str, str]] = []
    # glob returns symlinks themselves, not their targets, which is what we
    # want. Sorted so peers are visited in a deterministic order, which
    # keeps log order and first-match scans reproducible rather than
    # depending on how the filesystem happens to list the directory.
    pattern = os.path.join(db_dir, f'*{suffix}')
    for link_path in sorted(glob.glob(pattern)):
        # skip plain files — only true symlinks are peer live-links
        if not os.path.islink(link_path):
            continue

        # derive worker_name by stripping the suffix from the basename
        link_name = os.path.basename(link_path)
        worker_name = link_name.removesuffix(suffix)
        if worker_name == self_worker_name:
            continue

        # resolve symlink — survives recorder rotation (target moves under us)
        target = os.path.realpath(link_path)
        if not os.path.exists(target):
            # broken symlink: peer's DB file is gone. Silently skip; the
            # peer may come back later with a fresh file and new symlink.
            continue

        results.append((worker_name, target))
    return results


async def discover_peer_dbs(
    db_dir: str,
    suffix: str,
    self_worker_name: str,
) -> AsyncIterator[tuple[str, str]]:
    """Yield (worker_name, resolved_db_path) for each peer symlink in db_dir.

    Scans `db_dir/*<suffix>` for symlinks, skips our own entry and any
    broken symlinks, and yields pairs with the target path resolved via
    `os.path.realpath`. The whole filesystem scan runs in a worker thread
    (`asyncio.to_thread`) because `db_dir` is usually a shared NFS mount
    and every stat call there can stall; the async-generator surface lets
    callers keep their `async for` loops unchanged.

    Returns nothing if `db_dir` is empty, missing, or has no matching
    symlinks — callers can rely on the iterator never raising for the
    "cache disabled" / "first worker to start" cases.

    Args:
        db_dir: shared directory that holds worker DB files. Empty or
            missing directory yields nothing.
        suffix: symlink name suffix — e.g. ``'-live.db'`` for recorder
            discovery, ``'-cache.db'`` for cache peer sync.
        self_worker_name: our own worker name; its symlink is filtered
            out so we don't peer with ourselves.
    """
    for pair in await asyncio.to_thread(_scan_peer_dbs, db_dir, suffix, self_worker_name):
        yield pair
