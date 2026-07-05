"""Round-trip tests against DB fixtures written by the GO backend.

``tests/fixtures/go-db/`` holds a recorder DB and a cache DB produced by
the real Go engines (``drakkar-go/internal/crossbackend/gen``, run via
``just gen-db-fixtures`` there). These tests prove the interop contract
behind supported mixed fleets: a Python worker discovers, traces, and
cache-syncs against files a Go worker wrote, byte-level quirks included.
The Go repo runs the mirror suite against Python-written fixtures.

Fixture content contract (see the generator's package comment):
worker ``go-fixture`` / cluster ``main``; recorder events ``committed``
@ partition 0 offset 1, ``periodic_run``, ``webapp_request_received``
(canonical started_at); cache keys ``fx:global`` / ``fx:cluster`` /
``fx:local`` / ``fx:expired``.
"""

from pathlib import Path
from shutil import copy

import pytest

from drakkar.cache import Cache, CacheEngine
from drakkar.config import CacheConfig
from drakkar.recorder import EventRecorder, open_reader
from drakkar.recorder.schema import EVENT_COLUMNS
from tests.conftest import make_ui_config

FIXTURES = Path(__file__).parent / 'fixtures' / 'go-db'
GO_WORKER = 'go-fixture'


@pytest.fixture
def shared_dir(tmp_path):
    """A db_dir seeded with the Go worker's files, symlinks included.

    Mirrors what a live Go worker leaves in a shared volume: the rotated
    recorder DB behind a ``-live.db`` symlink and the cache DB behind the
    ``-cache.db`` → ``.actual`` indirection.
    """
    recorder_db = tmp_path / f'{GO_WORKER}-2026-07-05__00_00_00.db'
    copy(FIXTURES / 'recorder.db', recorder_db)
    (tmp_path / f'{GO_WORKER}-live.db').symlink_to(recorder_db.name)

    cache_actual = tmp_path / f'{GO_WORKER}-cache.db.actual'
    copy(FIXTURES / 'cache.db', cache_actual)
    (tmp_path / f'{GO_WORKER}-cache.db').symlink_to(cache_actual.name)
    return tmp_path


async def test_discovery_reads_go_worker_config(shared_dir):
    recorder = EventRecorder(make_ui_config(db_dir=str(shared_dir)), worker_name='observer', cluster_name='main')
    await recorder.start()
    try:
        workers = await recorder.discover_workers()
    finally:
        await recorder.stop()

    assert len(workers) == 1
    peer = workers[0]
    assert peer['worker_name'] == GO_WORKER
    assert peer['cluster_name'] == 'main'
    assert peer['source_topic'] == 'fixture-topic'
    assert peer['consumer_group'] == 'fixture-group'


async def test_cross_trace_finds_events_in_go_written_db(shared_dir):
    recorder = EventRecorder(make_ui_config(db_dir=str(shared_dir)), worker_name='observer', cluster_name='main')
    await recorder.start()
    try:
        events = await recorder.cross_trace(0, 1)
    finally:
        await recorder.stop()

    assert events, 'expected the committed event from the Go fixture'
    assert {e['event'] for e in events} == {'committed'}
    assert all(e['worker_name'] == GO_WORKER for e in events)


async def test_go_event_rows_carry_the_pinned_column_shape(shared_dir):
    db = await open_reader(str(shared_dir / f'{GO_WORKER}-live.db'))
    try:
        async with db.execute('SELECT * FROM events ORDER BY id') as cur:
            columns = tuple(d[0] for d in cur.description)
            rows = await cur.fetchall()
    finally:
        await db.close()

    assert columns == EVENT_COLUMNS
    row = dict(zip(columns, rows[-1], strict=True))
    assert row['event'] == 'webapp_request_received'
    assert row['origin'] == 'http'
    assert row['client_name'] == 'fixture-client'
    assert row['request_id'] == 'fx-req-1'
    # Canonical cross-backend datetime, byte-identical to what a Python
    # worker records for the same instant.
    assert row['metadata'] == '{"started_at":"2026-07-05T12:00:00.250000Z"}'


async def test_cache_peer_sync_pulls_from_go_written_db(shared_dir):
    engine = CacheEngine(
        config=CacheConfig(enabled=True, db_dir=str(shared_dir)),
        ui_config=make_ui_config(db_dir=str(shared_dir)),
        worker_id='observer',
        cluster_name='main',
        recorder=None,
    )
    cache = Cache(origin_worker_id='observer')
    engine.attach_cache(cache)
    await engine.start()
    try:
        await engine._sync_once()

        # Same cluster (resolved from the Go worker's live.db) → cluster
        # and global scopes arrive; local NEVER syncs; expired rows are
        # filtered at pull time.
        assert await cache.get('fx:global') == {'v': 'global'}
        assert await cache.get('fx:cluster') == 'cluster-value'
        assert await cache.get('fx:local') is None
        assert await cache.get('fx:expired') is None
    finally:
        await engine.stop()
