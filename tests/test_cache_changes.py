"""The serving side of the cache exchange: request parsing, the changes query, high-water marks.

``tests/fixtures/cache_changes.json`` pins the request validation (every
400 message and the order of the checks) and the pages a seeded cache
answers. Every implementation runs the same cases.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest

from drakkar.cache import LWW_UPSERT_SQL, Cache, CacheEngine, CacheScope
from drakkar.cache.changes import MARK_KEY_MAX_CHARS, ChangesQuery, parse_changes_query
from drakkar.cache.sql import SCHEMA_CACHE_ENTRIES, changes_query
from drakkar.config import CacheConfig, CacheExchangeConfig
from drakkar.fleet.card import MAX_CARD_BYTES, WorkerIdentity, build_card, serialize_card, validate_card

FIXTURE = json.loads((Path(__file__).parent / 'fixtures' / 'cache_changes.json').read_text(encoding='utf-8'))
PAGE = FIXTURE['page']


def _seed_tuple(row: dict) -> tuple:
    return (
        row['key'],
        row['scope'],
        row['value'],
        row['size_bytes'],
        row['created_at_ms'],
        row['updated_at_ms'],
        row['expires_at_ms'],
        row['origin_worker_id'],
    )


def _engine(tmp_path: Path, *, worker_id: str = 'w-a', exchange: bool = True) -> CacheEngine:
    engine = CacheEngine(
        config=CacheConfig(enabled=True, flush_interval_seconds=3600, cleanup_interval_seconds=3600),
        exchange=CacheExchangeConfig(enabled=exchange, interval_seconds=3600),
        storage_dir=str(tmp_path),
        worker_id=worker_id,
        cluster_name='search',
    )
    engine.attach_cache(Cache(origin_worker_id=worker_id))
    return engine


async def _seed(engine: CacheEngine, rows: list[dict]) -> None:
    assert engine._writer_db is not None
    await engine._writer_db.executemany(LWW_UPSERT_SQL, [_seed_tuple(row) for row in rows])
    await engine._writer_db.commit()


@pytest.fixture
async def seeded_engine(tmp_path):
    engine = _engine(tmp_path, worker_id=PAGE['worker'])
    await engine.start()
    await _seed(engine, PAGE['seed'])
    yield engine
    await engine.stop()


# --- request parsing --------------------------------------------------------


@pytest.mark.parametrize('case', FIXTURE['validation'], ids=[case['name'] for case in FIXTURE['validation']])
def test_parse_changes_query_matches_the_shared_fixture(case):
    parsed = parse_changes_query(case['params'])

    if case['status'] == 400:
        assert parsed == case['body']['error']
    else:
        expected = case['query']
        assert parsed == ChangesQuery(
            scopes=tuple(expected['scopes']),
            after=None if expected['after'] is None else (expected['after'][0], expected['after'][1]),
            limit=expected['limit'],
        )


# --- the changes query ------------------------------------------------------


@pytest.mark.parametrize('request_case', PAGE['requests'], ids=[case['name'] for case in PAGE['requests']])
async def test_changes_answers_the_fixture_pages(seeded_engine, request_case):
    query = parse_changes_query(request_case['params'])
    assert isinstance(query, ChangesQuery)

    answer = await seeded_engine.changes(query, now_ms=PAGE['now_ms'])

    assert answer == request_case['response']


def test_a_small_page_never_reads_the_whole_range(request):
    # One index search per scope, each stopping at the page size: a small
    # page over a large cache costs a small number of SQLite VM steps. A
    # single query over both scopes sorted every row after the cursor.
    db = sqlite3.connect(':memory:')
    request.addfinalizer(db.close)
    db.executescript(SCHEMA_CACHE_ENTRIES)
    db.executemany(
        'INSERT INTO cache_entries VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
        [(f'k{i:06d}', 'cluster' if i % 2 else 'global', 'v', 1, 1, 1000 + i, None, 'me') for i in range(20_000)],
    )
    sql, params = changes_query(origin='me', scopes=('cluster', 'global'), now_ms=0, after=None, limit=11)
    steps = 0

    def count() -> int:
        nonlocal steps
        steps += 1
        return 0

    db.set_progress_handler(count, 100)

    rows = db.execute(sql, params).fetchall()

    assert [row[0] for row in rows] == [f'k{i:06d}' for i in range(11)]
    assert steps < 100, f'{steps * 100} VM steps for an 11-row page'


async def test_changes_never_serves_rows_expired_on_the_server_clock(seeded_engine):
    query = ChangesQuery(scopes=('global',), after=None, limit=500)

    later = await seeded_engine.changes(query, now_ms=1760000001000)

    assert [row['key'] for row in later['rows']] == ['a3']


async def test_changes_is_none_once_the_engine_stopped(tmp_path):
    engine = _engine(tmp_path)
    await engine.start()
    await engine.stop()

    assert await engine.changes(ChangesQuery(scopes=('cluster',), after=None, limit=500), now_ms=0) is None


async def test_serves_changes_only_with_the_exchange_on_and_the_engine_running(tmp_path):
    off = _engine(tmp_path / 'off', exchange=False)
    on = _engine(tmp_path / 'on')
    await off.start()
    await on.start()
    try:
        assert off.serves_changes is False
        assert on.serves_changes is True
    finally:
        await off.stop()
        await on.stop()
    assert on.serves_changes is False


# --- high-water marks and the card's features -------------------------------


def _set(cache: Cache, key: str, scope: CacheScope, now_ms: int) -> None:
    cache.set(key, {'v': key}, scope=scope)
    # Pin updated_at_ms so the expected marks do not depend on the clock.
    entry = cache._dirty[key].entry
    assert entry is not None
    entry.updated_at_ms = now_ms


async def test_a_flush_raises_the_marks_of_own_cluster_and_global_rows(tmp_path):
    engine = _engine(tmp_path)
    await engine.start()
    try:
        cache = engine._cache
        assert cache is not None
        _set(cache, 'c-old', CacheScope.CLUSTER, 1000)
        _set(cache, 'c-new', CacheScope.CLUSTER, 2000)
        _set(cache, 'g', CacheScope.GLOBAL, 1500)
        _set(cache, 'l', CacheScope.LOCAL, 9000)

        await engine._flush_once()

        assert engine.features() == {
            'cache': {'cluster': {'ms': 2000, 'key': 'c-new'}, 'global': {'ms': 1500, 'key': 'g'}}
        }
    finally:
        await engine.stop()


async def test_a_long_key_is_cut_in_the_card_mark_so_the_card_stays_small(tmp_path):
    # Every reader skips a card over MAX_CARD_BYTES: one long cache key must
    # not remove the worker from the workers list and the exchange.
    engine = _engine(tmp_path)
    await engine.start()
    try:
        cache = engine._cache
        assert cache is not None
        _set(cache, 'k' * 3000 + 'ü', CacheScope.CLUSTER, 2000)
        _set(cache, 'g' * 3000, CacheScope.GLOBAL, 2000)
        await engine._flush_once()

        features = engine.features()
        identity = WorkerIdentity(
            worker='w-a', instance='1' * 16, cluster='search', backend='python', version='1.0.0',
            url='http://10.0.0.1:8080/', started_at=datetime(2026, 10, 14, tzinfo=UTC),
        )  # fmt: skip
        raw = serialize_card(build_card(identity, 'running', datetime(2026, 10, 14, tzinfo=UTC), features))
    finally:
        await engine.stop()

    assert features['cache']['cluster'] == {'ms': 2000, 'key': 'k' * MARK_KEY_MAX_CHARS}
    assert len(raw) <= MAX_CARD_BYTES
    assert validate_card(json.loads(raw)) == ''


async def test_rows_pulled_from_a_peer_never_raise_the_marks(tmp_path):
    engine = _engine(tmp_path)
    await engine.start()
    try:
        peer_row = {**PAGE['seed'][4], 'updated_at_ms': 99_999}
        await engine._apply_peer_rows(peer_name='w-b', rows=[_seed_tuple(peer_row)])

        assert engine.features() == {'cache': {}}
    finally:
        await engine.stop()


async def test_the_marks_are_read_from_the_file_at_start(tmp_path):
    engine = _engine(tmp_path, worker_id=PAGE['worker'])
    await engine.start()
    await _seed(engine, PAGE['seed'])
    await engine.stop()

    restarted = _engine(tmp_path, worker_id=PAGE['worker'])
    await restarted.start()
    try:
        assert restarted.features() == {
            'cache': {'cluster': {'ms': 4000, 'key': 'z1'}, 'global': {'ms': 2700, 'key': 'f1'}}
        }
    finally:
        await restarted.stop()


async def test_features_are_empty_when_the_exchange_is_off(tmp_path):
    engine = _engine(tmp_path, exchange=False)
    await engine.start()
    try:
        assert engine.features() == {}
    finally:
        await engine.stop()


def test_features_are_empty_before_the_engine_starts(tmp_path):
    assert _engine(tmp_path).features() == {}
