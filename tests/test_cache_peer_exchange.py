"""The cache exchange over HTTP, end to end on the client side.

Peers are cache DB files served in this process through the real changes
query (``tests.conftest.FakePeers``); the engine under test pulls them with
its real HTTP client over a mock transport.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import sqlite3
import stat
import time
from pathlib import Path
from typing import Any

import httpx
import pytest
from prometheus_client import REGISTRY
from structlog.testing import capture_logs

from drakkar.cache import Cache, CacheEngine, CacheScope
from drakkar.config import CacheConfig, CacheExchangeConfig
from tests.conftest import fake_peers, set_peer_cluster


async def _engine(tmp_path: Path, *, worker_id: str = 'me', cluster: str = 'prod', **exchange: Any) -> CacheEngine:
    settings: dict[str, Any] = {'interval_seconds': 3600, **exchange}
    engine = CacheEngine(
        config=CacheConfig(enabled=True, flush_interval_seconds=3600, cleanup_interval_seconds=3600),
        exchange=CacheExchangeConfig(**settings),
        storage_dir=str(tmp_path),
        worker_id=worker_id,
        cluster_name=cluster,
    )
    engine.attach_cache(Cache(origin_worker_id=worker_id))
    await engine.start()
    return engine


@pytest.fixture
async def engine(tmp_path):
    engine = await _engine(tmp_path, page_size=2)
    yield engine
    await engine.stop()


def _local_keys(engine: CacheEngine) -> list[str]:
    with contextlib.closing(sqlite3.connect(os.path.join(engine._storage_dir, 'me-cache.db'))) as db:
        return [row[0] for row in db.execute('SELECT key FROM cache_entries ORDER BY key')]


def _rows(*keys: str, scope: str = 'cluster', ms: int = 1000) -> list[dict]:
    return [{'key': key, 'scope': scope, 'updated_at_ms': ms} for key in keys]


def _sample(name: str, labels: dict[str, str]) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


# --- the private cache file -------------------------------------------------


async def test_the_cache_file_is_a_plain_private_file(engine, tmp_path):
    path = tmp_path / 'me-cache.db'

    assert stat.S_ISREG(os.lstat(path).st_mode)
    assert stat.S_IMODE(os.lstat(path).st_mode) == 0o600
    assert not (tmp_path / 'me-cache.db.actual').exists()


# --- what is pulled ---------------------------------------------------------


async def test_a_same_cluster_peer_is_pulled_page_by_page_in_one_cycle(engine):
    set_peer_cluster(engine, 'w-2', 'prod')
    fake_peers(engine).seed('w-2', _rows('a', 'b', 'c', 'd', 'e'))

    await engine._sync_once()

    assert _local_keys(engine) == ['a', 'b', 'c', 'd', 'e']
    assert engine._peer_cursors['w-2'] == (1000, 'e')
    assert [params['limit'] for _name, params in fake_peers(engine).requests] == ['2', '2', '2']


async def test_rows_sharing_one_timestamp_across_a_page_boundary_arrive_once(engine):
    set_peer_cluster(engine, 'w-2', 'prod')
    fake_peers(engine).seed('w-2', _rows('a', 'b', 'c'))
    peers = fake_peers(engine)

    await engine._sync_once()

    assert _local_keys(engine) == ['a', 'b', 'c']
    # The second page starts after (1000, 'b'): no row repeats, none is lost.
    assert peers.requests[1][1]['after_ms'] == '1000'
    assert peers.requests[1][1]['after_key'] == 'b'


async def test_a_peer_of_another_cluster_gives_only_its_global_rows(engine):
    set_peer_cluster(engine, 'w-2', 'other')
    fake_peers(engine).seed('w-2', _rows('c1') + _rows('g1', scope='global'))

    await engine._sync_once()

    assert _local_keys(engine) == ['g1']
    assert fake_peers(engine).requests[0][1]['scopes'] == 'global'


async def test_a_peer_the_registry_does_not_list_is_never_asked(engine):
    peers = fake_peers(engine)
    peers.seed('w-2', _rows('a'))

    await engine._sync_once()

    assert peers.requests == []


async def test_no_registry_means_no_pull(tmp_path):
    engine = await _engine(tmp_path)
    try:
        assert engine._peer_views is None
        assert await engine._sync_once() == {}
    finally:
        await engine.stop()


async def test_a_peer_with_nothing_new_is_not_asked_again(engine):
    set_peer_cluster(engine, 'w-2', 'prod')
    peers = fake_peers(engine)
    peers.seed('w-2', _rows('a'))
    await engine._sync_once()

    await engine._sync_once()

    assert len(peers.requests) == 1


async def test_the_cursor_moves_up_to_the_card_mark_after_a_complete_pull(engine):
    # The card's mark points at a row the peer no longer serves (deleted or
    # expired): one empty pull, then the cursor sits on the mark.
    set_peer_cluster(engine, 'w-2', 'prod')
    peers = fake_peers(engine)
    peers.seed('w-2', _rows('a'))
    peers.cards['w-2'] = {'features': {'cache': {'cluster': {'ms': 9000, 'key': 'gone'}}}}

    await engine._sync_once()
    await engine._sync_once()

    assert engine._peer_cursors['w-2'] == (9000, 'gone')
    assert len(peers.requests) == 1


async def test_new_rows_after_the_cursor_arrive_in_the_next_cycle(engine):
    set_peer_cluster(engine, 'w-2', 'prod')
    peers = fake_peers(engine)
    peers.seed('w-2', _rows('a'))
    await engine._sync_once()
    peers.seed('w-2', _rows('b', ms=2000))

    await engine._sync_once()

    assert _local_keys(engine) == ['a', 'b']
    assert peers.requests[-1][1]['after_key'] == 'a'


async def test_an_offline_or_starting_peer_is_skipped_and_an_unknown_one_pulled(engine):
    peers = fake_peers(engine)
    for name in ('w-off', 'w-starting', 'w-unknown'):
        set_peer_cluster(engine, name, 'prod')
        peers.seed(name, _rows(f'{name}-k'))
    peers.online['w-off'] = False
    peers.online['w-unknown'] = None
    peers.cards['w-starting'] = {'state': 'starting'}

    await engine._sync_once()

    assert [name for name, _params in peers.requests] == ['w-unknown']


# --- what is kept -----------------------------------------------------------


async def test_rows_outside_the_asked_scopes_or_of_another_origin_are_dropped(engine):
    set_peer_cluster(engine, 'w-2', 'prod')
    peers = fake_peers(engine)
    # A faulty peer: its own rows plus a relayed one that its query serves
    # because the stored origin lies.
    peers.seed('w-2', _rows('mine'))
    peers.cards['w-2'] = {}

    async def lying(request: httpx.Request) -> httpx.Response:
        rows = [
            {'key': 'mine', 'scope': 'cluster', 'value': '1', 'size_bytes': 1, 'created_at_ms': 1,
             'updated_at_ms': 1000, 'expires_at_ms': None, 'origin_worker_id': 'w-2'},
            {'key': 'secret', 'scope': 'local', 'value': '1', 'size_bytes': 1, 'created_at_ms': 1,
             'updated_at_ms': 1001, 'expires_at_ms': None, 'origin_worker_id': 'w-2'},
            {'key': 'relayed', 'scope': 'cluster', 'value': '1', 'size_bytes': 1, 'created_at_ms': 1,
             'updated_at_ms': 1002, 'expires_at_ms': None, 'origin_worker_id': 'w-3'},
        ]  # fmt: skip
        return httpx.Response(200, json={'rows': rows, 'more': False})

    engine._peer_transport = httpx.MockTransport(lying)
    errors_before = _sample('drakkar_cache_sync_errors_total', {'peer': 'w-2'})

    with capture_logs() as logs:
        await engine._sync_once()

    assert _local_keys(engine) == ['mine']
    assert _sample('drakkar_cache_sync_errors_total', {'peer': 'w-2'}) == errors_before + 2
    (rejected,) = [entry for entry in logs if entry['event'] == 'fleet_peer_rows_rejected']
    assert (rejected['peer'], rejected['rejected']) == ('w-2', {'scope': 1, 'origin': 1})


# --- failures ---------------------------------------------------------------


async def test_a_failing_peer_backs_off_and_resets_after_a_success(engine):
    set_peer_cluster(engine, 'w-2', 'prod')
    peers = fake_peers(engine)
    peers.seed('w-2', _rows('a'))
    peers.failures['w-2'] = 500

    with capture_logs() as logs:
        for _cycle in range(4):
            await engine._sync_once()
    # Failed at cycle 1 (skip 1), skipped at 2, failed at 3 (skip 2), skipped at 4.
    assert len(peers.requests) == 2
    assert [entry['event'] for entry in logs if entry['event'].startswith('fleet_peer')] == [
        'fleet_peer_request_failed',
        'fleet_peer_backoff',
        'fleet_peer_backoff',
    ]
    backoffs = [entry for entry in logs if entry['event'] == 'fleet_peer_backoff']
    assert [(entry['failures'], entry['skip_cycles']) for entry in backoffs] == [(1, 1), (2, 2)]
    assert 'w-2' not in engine._peer_cursors

    del peers.failures['w-2']
    for _cycle in range(3):
        await engine._sync_once()
    assert _local_keys(engine) == ['a']
    assert engine._peer_backoff.skip('w-2') is False


async def test_one_failing_peer_does_not_stop_the_others(engine):
    peers = fake_peers(engine)
    for name in ('w-2', 'w-3'):
        set_peer_cluster(engine, name, 'prod')
        peers.seed(name, _rows(f'{name}-k'))
    peers.failures['w-2'] = httpx.ConnectError('refused')

    await engine._sync_once()

    assert _local_keys(engine) == ['w-3-k']


async def test_the_cycle_ends_at_its_deadline_and_waiting_peers_do_not_back_off(tmp_path):
    engine = await _engine(tmp_path, max_concurrent_peers=2, cycle_deadline_seconds=0.3, interval_seconds=3600)
    try:
        peers = fake_peers(engine)
        set_peer_cluster(engine, 'a-fast', 'prod')
        peers.seed('a-fast', _rows('fast'))
        for index in range(10):
            name = f'slow-{index}'
            set_peer_cluster(engine, name, 'prod')
            peers.seed(name, _rows(f'{name}-k'))
            peers.delays[name] = 5.0
        started = time.monotonic()

        with pytest.raises(TimeoutError):
            await engine._sync_once()

        assert time.monotonic() - started < 2.0
        assert _local_keys(engine) == ['fast']
        assert engine._peer_backoff._failures == {}
        # Two slots: the fast peer, then slow ones one slot at a time.
        assert len(peers.requests) <= 3
    finally:
        await engine.stop()


async def test_a_hung_pull_never_holds_up_a_flush(tmp_path):
    engine = await _engine(tmp_path, cycle_deadline_seconds=30, interval_seconds=60)
    try:
        set_peer_cluster(engine, 'w-2', 'prod')
        peers = fake_peers(engine)
        peers.seed('w-2', _rows('a'))
        peers.delays['w-2'] = 60.0
        sync = asyncio.create_task(engine._sync_once())
        await asyncio.sleep(0.05)
        assert engine._cache is not None
        engine._cache.set('local-key', 1, scope=CacheScope.CLUSTER)

        await asyncio.wait_for(engine._flush_once(), timeout=2)

        assert 'local-key' in _local_keys(engine)
        sync.cancel()
        with pytest.raises(asyncio.CancelledError):
            await sync
    finally:
        await engine.stop()
