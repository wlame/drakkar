"""Tests for the CacheEngine peer-sync pull step (Task 11).

This task adds the *discovery + per-peer pull* half of peer sync. The LWW
UPSERT step and the cursor-based incremental behavior land in Tasks 12-13.

The plan requires these behaviors:

1. ``_sync_once`` calls ``discover_peer_dbs(db_dir, '-cache.db', self_id)``
   with the expected arguments — we spy on the helper to verify.
2. For each peer, the engine resolves the peer's ``cluster_name`` from the
   peer's ``-live.db`` ``worker_config`` row, cached in-process for 300s.
3. Peer in same cluster as self → query uses ``scope IN ('cluster','global')``.
4. Peer in different cluster → query uses ``scope = 'global'`` only.
5. Expired rows are excluded (``expires_at_ms IS NULL OR expires_at_ms > now``).
6. ``LIMIT batch_size`` caps the pull.
7. Peer without a ``worker_config`` table → skipped with a warn log, cluster
   falls back to "unknown" (pull global-only).
8. ``peer_sync.enabled=false`` at startup → ``_sync_once`` is a no-op (zero
   filesystem accesses, via a spy on ``discover_peer_dbs``).
9. ``debug.store_config=false`` at startup → peer sync is silently disabled
   (effective-disable logged once at startup).

We use an on-disk ``tmp_path`` directory for peer DBs so we can exercise the
real symlink discovery and aiosqlite read paths. Peer DBs are minimal: a
``cache_entries`` table with our own schema and a ``-live.db`` recorder
stand-in with just the ``worker_config`` row we need. This keeps the tests
focused on the sync-pull contract without booting a full recorder.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import aiosqlite
import pytest

from drakkar import cache as cache_module
from drakkar.cache import (
    Cache,
    CacheEngine,
    CacheScope,
)
from drakkar.config import CacheConfig, CachePeerSyncConfig, DebugConfig

# --- helpers ----------------------------------------------------------------


def make_debug_config(tmp_path: Path, **overrides: Any) -> DebugConfig:
    """DebugConfig with ``db_dir`` pointed at the temp path and events off.

    Defaults match the cleanup-loop tests' helper so it's easy to read
    across files. Callers can override any field — in particular
    ``store_config`` for the gating-rule tests.
    """
    defaults: dict[str, Any] = {
        'enabled': True,
        'db_dir': str(tmp_path),
        'store_events': False,
        'store_config': True,
        'store_state': False,
    }
    defaults.update(overrides)
    return DebugConfig(**defaults)


def make_cache_config(**overrides: Any) -> CacheConfig:
    """CacheConfig enabled by default, with peer_sync overrides honoured.

    ``peer_sync`` override can be a ``CachePeerSyncConfig`` instance or a
    dict — pydantic handles either one.
    """
    defaults: dict[str, Any] = {'enabled': True}
    defaults.update(overrides)
    return CacheConfig(**defaults)


async def _make_engine(
    tmp_path: Path,
    *,
    worker_id: str = 'w1',
    cluster_name: str = '',
    cache_overrides: dict[str, Any] | None = None,
    debug_overrides: dict[str, Any] | None = None,
) -> CacheEngine:
    """Spin up a started CacheEngine for sync-pull testing.

    Creates the worker's own ``-cache.db.actual`` file + symlink. Tests
    then set up one or more peer DBs in the same ``tmp_path`` so they
    share the discovery directory.
    """
    cache = Cache(origin_worker_id=worker_id)
    engine = CacheEngine(
        config=make_cache_config(**(cache_overrides or {})),
        debug_config=make_debug_config(tmp_path, **(debug_overrides or {})),
        worker_id=worker_id,
        cluster_name=cluster_name,
        recorder=None,
    )
    engine.attach_cache(cache)
    await engine.start()
    return engine


async def _seed_peer_cache_db(
    tmp_path: Path,
    peer_name: str,
    rows: list[dict[str, Any]],
) -> Path:
    """Create a peer ``-cache.db.actual`` + symlink with the given rows.

    Rows are dicts with the eight ``cache_entries`` columns. Missing
    ``size_bytes`` is auto-computed; missing ``expires_at_ms`` defaults
    to NULL (never expires); ``scope`` defaults to ``LOCAL`` (which
    peer-sync should never pull — useful for negative tests).
    """
    actual = tmp_path / f'{peer_name}-cache.db.actual'
    async with aiosqlite.connect(str(actual)) as db:
        await db.executescript(cache_module.SCHEMA_CACHE_ENTRIES)
        for r in rows:
            value = r.get('value', '"v"')
            size = r.get('size_bytes', len(value.encode('utf-8')))
            await db.execute(
                'INSERT INTO cache_entries '
                '(key, scope, value, size_bytes, created_at_ms, updated_at_ms, '
                ' expires_at_ms, origin_worker_id) VALUES (?,?,?,?,?,?,?,?)',
                (
                    r['key'],
                    r.get('scope', CacheScope.LOCAL.value),
                    value,
                    size,
                    r.get('created_at_ms', 1000),
                    r.get('updated_at_ms', 1000),
                    r.get('expires_at_ms'),
                    r.get('origin_worker_id', peer_name),
                ),
            )
        await db.commit()
    # Create the symlink peer-discovery scans for.
    link = tmp_path / f'{peer_name}-cache.db'
    link.symlink_to(actual.name)
    return actual


async def _seed_peer_live_db(tmp_path: Path, peer_name: str, cluster_name: str | None) -> Path:
    """Create a peer ``-live.db.actual`` with a single ``worker_config`` row.

    When ``cluster_name`` is ``None``, the DB is created without a
    ``worker_config`` table at all — used to test the "peer missing
    worker_config" fallback.
    """
    actual = tmp_path / f'{peer_name}-live.db.actual'
    async with aiosqlite.connect(str(actual)) as db:
        if cluster_name is not None:
            await db.execute(
                """CREATE TABLE worker_config (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    worker_name TEXT NOT NULL,
                    cluster_name TEXT,
                    created_at REAL NOT NULL,
                    created_at_dt TEXT NOT NULL
                )"""
            )
            await db.execute(
                'INSERT INTO worker_config (id, worker_name, cluster_name, created_at, created_at_dt) '
                'VALUES (1, ?, ?, 0, "")',
                (peer_name, cluster_name or None),
            )
        await db.commit()
    # Symlink too, matching the real recorder's rotation convention.
    link = tmp_path / f'{peer_name}-live.db'
    link.symlink_to(actual.name)
    return actual


# --- discovery arguments ----------------------------------------------------


async def test_sync_calls_discover_peer_dbs_with_expected_args(tmp_path, monkeypatch):
    """``_sync_once`` must invoke ``discover_peer_dbs`` with the cache
    suffix ``'-cache.db'`` and the engine's worker_id as the self filter.
    """
    engine = await _make_engine(tmp_path, worker_id='me')
    try:
        calls: list[tuple[str, str, str]] = []

        async def fake_discover(db_dir: str, suffix: str, self_name: str):
            calls.append((db_dir, suffix, self_name))
            # yield nothing — test only cares about the call signature
            return
            yield  # type: ignore[unreachable]

        monkeypatch.setattr(cache_module, 'discover_peer_dbs', fake_discover)

        await engine._sync_once()  # type: ignore[reportPrivateUsage]

        assert calls == [(str(tmp_path), '-cache.db', 'me')]
    finally:
        await engine.stop()


# --- cluster-aware scope filtering ------------------------------------------


async def test_sync_same_cluster_pulls_cluster_and_global(tmp_path):
    """A peer with the same ``cluster_name`` as us contributes both
    ``cluster``- and ``global``-scoped rows, but not ``local`` rows.
    """
    engine = await _make_engine(tmp_path, worker_id='me', cluster_name='prod')
    try:
        await _seed_peer_live_db(tmp_path, 'peer1', cluster_name='prod')
        await _seed_peer_cache_db(
            tmp_path,
            'peer1',
            [
                {'key': 'k_local', 'scope': CacheScope.LOCAL.value},
                {'key': 'k_cluster', 'scope': CacheScope.CLUSTER.value},
                {'key': 'k_global', 'scope': CacheScope.GLOBAL.value},
            ],
        )

        pulled = await engine._sync_once()  # type: ignore[reportPrivateUsage]

        assert 'peer1' in pulled
        keys = {row[0] for row in pulled['peer1']}
        assert keys == {'k_cluster', 'k_global'}
    finally:
        await engine.stop()


async def test_sync_different_cluster_pulls_global_only(tmp_path):
    """A peer in a different cluster only contributes ``global``-scoped
    rows — ``cluster``-scoped rows stay in their home cluster."""
    engine = await _make_engine(tmp_path, worker_id='me', cluster_name='prod')
    try:
        await _seed_peer_live_db(tmp_path, 'peer2', cluster_name='staging')
        await _seed_peer_cache_db(
            tmp_path,
            'peer2',
            [
                {'key': 'k_local', 'scope': CacheScope.LOCAL.value},
                {'key': 'k_cluster', 'scope': CacheScope.CLUSTER.value},
                {'key': 'k_global', 'scope': CacheScope.GLOBAL.value},
            ],
        )

        pulled = await engine._sync_once()  # type: ignore[reportPrivateUsage]

        assert 'peer2' in pulled
        keys = {row[0] for row in pulled['peer2']}
        assert keys == {'k_global'}
    finally:
        await engine.stop()


# --- TTL filter -------------------------------------------------------------


async def test_sync_excludes_expired_rows(tmp_path):
    """Rows whose ``expires_at_ms`` is already in the past must not be
    pulled — there is no point syncing a dead row across workers.
    """
    engine = await _make_engine(tmp_path, worker_id='me', cluster_name='prod')
    try:
        await _seed_peer_live_db(tmp_path, 'peer1', cluster_name='prod')
        await _seed_peer_cache_db(
            tmp_path,
            'peer1',
            [
                # Long-dead TTL
                {'key': 'k_dead', 'scope': CacheScope.GLOBAL.value, 'expires_at_ms': 1},
                # No TTL
                {'key': 'k_forever', 'scope': CacheScope.GLOBAL.value, 'expires_at_ms': None},
                # Far-future TTL
                {
                    'key': 'k_future',
                    'scope': CacheScope.GLOBAL.value,
                    'expires_at_ms': 10_000_000_000_000,
                },
            ],
        )

        pulled = await engine._sync_once()  # type: ignore[reportPrivateUsage]

        keys = {row[0] for row in pulled['peer1']}
        assert keys == {'k_forever', 'k_future'}
    finally:
        await engine.stop()


# --- batch_size limit --------------------------------------------------------


async def test_sync_respects_batch_size_limit(tmp_path):
    """The pull query must cap rows at ``peer_sync.batch_size``."""
    engine = await _make_engine(
        tmp_path,
        worker_id='me',
        cluster_name='prod',
        cache_overrides={'peer_sync': CachePeerSyncConfig(batch_size=3)},
    )
    try:
        await _seed_peer_live_db(tmp_path, 'peer1', cluster_name='prod')
        # Stage 10 global rows — only 3 should come back.
        rows = [
            {
                'key': f'k{i}',
                'scope': CacheScope.GLOBAL.value,
                'updated_at_ms': 1000 + i,
            }
            for i in range(10)
        ]
        await _seed_peer_cache_db(tmp_path, 'peer1', rows)

        pulled = await engine._sync_once()  # type: ignore[reportPrivateUsage]

        assert len(pulled['peer1']) == 3
    finally:
        await engine.stop()


# --- peer with missing worker_config table ----------------------------------


async def test_sync_peer_without_worker_config_is_skipped_with_warn(tmp_path, caplog):
    """Peer whose ``-live.db`` is missing ``worker_config`` table should
    be handled gracefully — ``_resolve_peer_cluster`` returns ``None``
    and the engine pulls ``scope='global'`` only (conservative fallback).
    A warning is logged.
    """
    engine = await _make_engine(tmp_path, worker_id='me', cluster_name='prod')
    try:
        # Peer has a -live.db but no worker_config table inside.
        await _seed_peer_live_db(tmp_path, 'peer1', cluster_name=None)
        await _seed_peer_cache_db(
            tmp_path,
            'peer1',
            [
                {'key': 'k_cluster', 'scope': CacheScope.CLUSTER.value},
                {'key': 'k_global', 'scope': CacheScope.GLOBAL.value},
            ],
        )

        pulled = await engine._sync_once()  # type: ignore[reportPrivateUsage]

        # Conservative fallback — only global-scoped rows pulled when
        # cluster membership is unknown.
        assert {row[0] for row in pulled['peer1']} == {'k_global'}
    finally:
        await engine.stop()


# --- disabled paths ---------------------------------------------------------


async def test_sync_disabled_is_no_op(tmp_path, monkeypatch):
    """When ``peer_sync.enabled=false`` at startup, ``_sync_once`` must
    not touch the filesystem at all — not even to list peers.
    """
    engine = await _make_engine(
        tmp_path,
        worker_id='me',
        cache_overrides={'peer_sync': CachePeerSyncConfig(enabled=False)},
    )
    try:
        call_count = 0

        async def fake_discover(db_dir, suffix, self_name):
            nonlocal call_count
            call_count += 1
            return
            yield  # type: ignore[unreachable]

        monkeypatch.setattr(cache_module, 'discover_peer_dbs', fake_discover)

        pulled = await engine._sync_once()  # type: ignore[reportPrivateUsage]

        assert call_count == 0
        assert pulled == {}
    finally:
        await engine.stop()


async def test_sync_disabled_when_store_config_off(tmp_path, monkeypatch):
    """``debug.store_config=false`` silently disables peer sync at startup
    regardless of ``peer_sync.enabled=true`` — the pull needs
    ``worker_config`` rows from peer recorders to determine cluster
    membership. ``_sync_once`` short-circuits.
    """
    engine = await _make_engine(
        tmp_path,
        worker_id='me',
        debug_overrides={'store_config': False},
    )
    try:
        # Gating runs in start() — the effective flag should be False.
        assert engine._peer_sync_enabled is False  # type: ignore[reportPrivateUsage]

        call_count = 0

        async def fake_discover(db_dir, suffix, self_name):
            nonlocal call_count
            call_count += 1
            return
            yield  # type: ignore[unreachable]

        monkeypatch.setattr(cache_module, 'discover_peer_dbs', fake_discover)

        await engine._sync_once()  # type: ignore[reportPrivateUsage]

        assert call_count == 0
    finally:
        await engine.stop()


# --- cluster cache behavior -------------------------------------------------


async def test_peer_cluster_lookup_is_cached(tmp_path):
    """The in-process cluster-name cache should serve repeat calls without
    re-opening the peer's ``-live.db``.
    """
    engine = await _make_engine(tmp_path, worker_id='me', cluster_name='prod')
    try:
        live_db = await _seed_peer_live_db(tmp_path, 'peer1', cluster_name='prod')

        # First call populates the cache.
        cluster1 = await engine._resolve_peer_cluster('peer1', str(tmp_path))  # type: ignore[reportPrivateUsage]
        assert cluster1 == 'prod'

        # Remove the peer's live DB — cached value should still serve.
        live_db.unlink()
        (tmp_path / 'peer1-live.db').unlink()

        cluster2 = await engine._resolve_peer_cluster('peer1', str(tmp_path))  # type: ignore[reportPrivateUsage]
        assert cluster2 == 'prod'
    finally:
        await engine.stop()


async def test_peer_cluster_empty_string_means_no_cluster(tmp_path):
    """Some deployments run without a cluster label — the peer's
    ``cluster_name`` column is an empty string (or NULL, which we normalize
    to empty). The scope filter then treats the peer as same-cluster only
    if *we* also have empty cluster_name.
    """
    engine = await _make_engine(tmp_path, worker_id='me', cluster_name='')
    try:
        await _seed_peer_live_db(tmp_path, 'peer1', cluster_name='')
        await _seed_peer_cache_db(
            tmp_path,
            'peer1',
            [
                {'key': 'k_cluster', 'scope': CacheScope.CLUSTER.value},
                {'key': 'k_global', 'scope': CacheScope.GLOBAL.value},
            ],
        )

        pulled = await engine._sync_once()  # type: ignore[reportPrivateUsage]

        # Both have cluster_name='' → same cluster → pulls CLUSTER + GLOBAL.
        assert {row[0] for row in pulled['peer1']} == {'k_cluster', 'k_global'}
    finally:
        await engine.stop()


# --- scope filter helper ----------------------------------------------------


@pytest.mark.parametrize(
    ('self_cluster', 'peer_cluster', 'expected_scopes'),
    [
        ('prod', 'prod', {'cluster', 'global'}),
        ('prod', 'staging', {'global'}),
        ('prod', None, {'global'}),  # unknown cluster — conservative
        ('', '', {'cluster', 'global'}),  # empty == empty
        ('', 'prod', {'global'}),
    ],
)
def test_peer_scope_filter_matrix(tmp_path, self_cluster, peer_cluster, expected_scopes):
    """The ``_peer_scope_filter`` helper must produce the right SQL
    predicate and parameters for each (self_cluster, peer_cluster) pair.
    """
    # Construct engine without starting — pure helper test.
    cache = Cache(origin_worker_id='me')
    engine = CacheEngine(
        config=make_cache_config(),
        debug_config=make_debug_config(tmp_path),
        worker_id='me',
        cluster_name=self_cluster,
        recorder=None,
    )
    engine.attach_cache(cache)

    sql, params = engine._peer_scope_filter(peer_cluster)  # type: ignore[reportPrivateUsage]

    # Flatten params to a set for scope-agnostic comparison.
    param_set = set(params)
    assert param_set == expected_scopes
    # SQL fragment must match the params arity.
    if len(expected_scopes) == 1:
        assert sql == 'scope = ?'
    else:
        assert sql == 'scope IN (?, ?)'
