"""Read classes and the source rule: which reads the memory store answers and which go to the live file."""

from __future__ import annotations

import asyncio
import json
import time
from unittest.mock import MagicMock

import pytest
from httpx import ASGITransport, AsyncClient
from structlog.testing import capture_logs

from drakkar.config import DrakkarConfig
from drakkar.providers.resilience import RateLimitedLog
from drakkar.recorder import EventRecorder
from drakkar.recorder import core as core_mod
from drakkar.recorder import memstore as memstore_mod
from drakkar.recorder import queries as queries_mod
from drakkar.recorder.memstore import MemoryCoverage
from drakkar.recorder.queries import ReadBound, ReadClass, pick_source, still_covered
from drakkar.uiserver import server as server_mod
from drakkar.uiserver.server import UIDeps, create_ui_app
from tests.conftest import make_ui_config

WORKER = 'w-1'
LARGE_WINDOW_SECONDS = 86_400
LARGE_MAX_ROWS = 1_000_000
ENDLESS_QUERY = 'WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) SELECT count(*) FROM c'
COUNT_QUERY = 'SELECT COUNT(*) FROM events'

# Coverage of a store that lost rows: ids 11..20 kept, the largest removed ts is 100.
EVICTED = MemoryCoverage(floor_id=11, last_id=20, evicted=True, evicted_max_ts=100.0)
WHOLE = MemoryCoverage(floor_id=1, last_id=20, evicted=False, evicted_max_ts=0.0)


@pytest.mark.parametrize(
    ('persistence', 'read_class', 'bound', 'coverage', 'expected'),
    [
        ('sqlite', ReadClass.WINDOW, ReadBound(since=50.0), WHOLE, 'memory'),
        ('sqlite', ReadClass.WINDOW, ReadBound(since=100.5), EVICTED, 'memory'),
        ('sqlite', ReadClass.WINDOW, ReadBound(since=100.0), EVICTED, 'file'),
        ('sqlite', ReadClass.WINDOW, ReadBound(), EVICTED, 'file'),
        ('sqlite', ReadClass.POLL, ReadBound(after_id=1), WHOLE, 'memory'),
        ('sqlite', ReadClass.POLL, ReadBound(after_id=10), EVICTED, 'memory'),
        ('sqlite', ReadClass.POLL, ReadBound(after_id=15), EVICTED, 'memory'),
        ('sqlite', ReadClass.POLL, ReadBound(after_id=9), EVICTED, 'file'),
        ('sqlite', ReadClass.NEWEST, ReadBound(limit=5), WHOLE, 'memory'),
        ('sqlite', ReadClass.NEWEST, ReadBound(limit=50), EVICTED, 'memory'),
        ('sqlite', ReadClass.KEYED, ReadBound(), WHOLE, 'file'),
        ('sqlite', ReadClass.WHOLE, ReadBound(), WHOLE, 'file'),
        ('none', ReadClass.WINDOW, ReadBound(since=1.0), EVICTED, 'memory'),
        ('none', ReadClass.POLL, ReadBound(after_id=1), EVICTED, 'memory'),
        ('none', ReadClass.NEWEST, ReadBound(limit=50), EVICTED, 'memory'),
        ('none', ReadClass.KEYED, ReadBound(), EVICTED, 'memory'),
        ('none', ReadClass.WHOLE, ReadBound(), EVICTED, 'memory'),
        ('none', ReadClass.WINDOW, ReadBound(since=1.0), None, 'file'),
        ('sqlite', ReadClass.WINDOW, ReadBound(since=1.0), None, 'file'),
    ],
)
def test_pick_source_table(persistence, read_class, bound, coverage, expected):
    assert pick_source(persistence, read_class, bound, coverage) == expected


@pytest.mark.parametrize(
    ('read_class', 'bound', 'coverage', 'rows_returned', 'expected'),
    [
        (ReadClass.NEWEST, ReadBound(limit=5), WHOLE, 2, True),
        (ReadClass.NEWEST, ReadBound(limit=5), EVICTED, 5, True),
        (ReadClass.NEWEST, ReadBound(limit=5), EVICTED, 4, False),
        (ReadClass.WINDOW, ReadBound(since=100.5), EVICTED, 0, True),
        (ReadClass.WINDOW, ReadBound(since=99.0), EVICTED, 10, False),
        (ReadClass.POLL, ReadBound(after_id=10), EVICTED, 0, True),
        (ReadClass.POLL, ReadBound(after_id=9), EVICTED, 10, False),
        (ReadClass.KEYED, ReadBound(), EVICTED, 10, False),
    ],
)
def test_still_covered_table(read_class, bound, coverage, rows_returned, expected):
    assert still_covered(read_class, bound, coverage, rows_returned) is expected


def _recorder(tmp_path, persistence: str, **memory) -> EventRecorder:
    config = make_ui_config(enabled=True, store_output=False, flush_interval_seconds=60)
    return EventRecorder(
        config,
        worker_name=WORKER,
        cluster_name='c',
        persistence=persistence,
        storage_dir=str(tmp_path / 'storage') if persistence == 'sqlite' else '',
        memory_window_seconds=memory.get('window_seconds', LARGE_WINDOW_SECONDS),
        memory_max_rows=memory.get('max_rows', LARGE_MAX_ROWS),
    )


@pytest.fixture
async def started(tmp_path):
    """Start recorders built in the test and stop them at the end."""
    recorders: list[EventRecorder] = []

    async def start(persistence: str, **memory) -> EventRecorder:
        (tmp_path / 'storage').mkdir(exist_ok=True)
        rec = _recorder(tmp_path, persistence, **memory)
        await rec.start()
        recorders.append(rec)
        return rec

    yield start
    for rec in recorders:
        await rec.stop()


def _drakkar_app() -> MagicMock:
    app = MagicMock()
    app._config = DrakkarConfig(sources={'kafka': {'enabled': True}})
    app._config.ui.release.enabled = False
    app._executor_pool = None
    app._consumer = None
    app._offload_pool = None
    app.processors = {}
    return app


def _client(rec: EventRecorder) -> AsyncClient:
    app = create_ui_app(rec.config, rec, _drakkar_app())
    return AsyncClient(transport=ASGITransport(app=app), base_url='http://test')


def _deps(rec: EventRecorder) -> UIDeps:
    return UIDeps(rec.config, rec, _drakkar_app())


def _consumed(offset: int, ts: float | None = None) -> dict:
    return {'ts': time.time() if ts is None else ts, 'event': 'consumed', 'partition': 0, 'offset': offset}


async def _file_count(rec: EventRecorder) -> int:
    async with rec.file_reader_db.execute(COUNT_QUERY) as cursor:
        return (await cursor.fetchone())[0]


async def test_newest_read_goes_to_the_file_when_evicted_and_memory_is_short(started):
    rec = await started('sqlite', max_rows=3)
    for offset in range(6):
        rec._record(_consumed(offset))
    await rec.flush()
    assert rec.memory_coverage().rows == 3
    query, params = queries_mod.events_query(limit=5)

    short = await _deps(rec).recorder_select(
        query, params, read_class=ReadClass.NEWEST, bound=ReadBound(limit=5), endpoint='e'
    )
    query, params = queries_mod.events_query(limit=2)
    full = await _deps(rec).recorder_select(
        query, params, read_class=ReadClass.NEWEST, bound=ReadBound(limit=2), endpoint='e'
    )

    assert (short.source, len(short.rows)) == ('file', 5)
    assert (full.source, len(full.rows)) == ('memory', 2)


@pytest.mark.parametrize(('persistence', 'source', 'rows'), [('sqlite', 'file', 4), ('none', 'memory', 1)])
async def test_window_read_rechecks_coverage_after_an_eviction_between_decision_and_select(
    started, persistence, source, rows
):
    rec = await started(persistence)
    now = time.time()
    for offset in range(4):
        rec._record(_consumed(offset, now - 10 + offset))
    await rec.flush()
    store = rec.memory_store
    real_read = store.read

    async def read_after_an_eviction(query, params=()):
        rec._memory_max_rows = 1
        await rec._stage_memory(0.0)
        return await real_read(query, params)

    store.read = read_after_an_eviction
    query, params = 'SELECT id FROM events WHERE ts >= ?', [now - 60]

    result = await _deps(rec).recorder_select(
        query, params, read_class=ReadClass.WINDOW, bound=ReadBound(since=now - 60), endpoint='e'
    )

    assert (result.source, len(result.rows)) == (source, rows)


async def test_file_read_runs_both_stages_first(started):
    rec = await started('sqlite')
    rec._record({'ts': time.time(), 'event': 'task_started', 'task_id': 't-1', 'partition': 0})
    query, params = queries_mod.task_state_query(['t-1'])

    result = await _deps(rec).recorder_select(
        query, params, read_class=ReadClass.KEYED, bound=ReadBound(), endpoint='e'
    )

    assert result.source == 'file'
    assert [row[0] for row in result.rows] == ['t-1']


async def test_memory_read_runs_only_the_memory_stage(started):
    rec = await started('sqlite')
    now = time.time()
    rec._record(_consumed(1, now))
    query, params = queries_mod.event_count_query(event_names=['consumed'], since=now - 60)

    result = await _deps(rec).recorder_select(
        query, params, read_class=ReadClass.WINDOW, bound=ReadBound(since=now - 60), endpoint='e'
    )

    assert (result.source, result.rows) == ('memory', [(1,)])
    assert await _file_count(rec) == 0


@pytest.mark.parametrize(('persistence', 'memory_stages'), [('sqlite', 0), ('none', 2)])
async def test_flush_free_reads_stay_flush_free(started, persistence, memory_stages):
    rec = await started(persistence)
    calls: list[str] = []
    real_stage_memory, real_stage_file = rec._stage_memory, rec._stage_file

    async def stage_memory(evict_min_interval):
        calls.append('memory')
        await real_stage_memory(evict_min_interval)

    async def stage_file():
        calls.append('file')
        await real_stage_file()

    rec._stage_memory = stage_memory
    rec._stage_file = stage_file
    rec._run_queued_stage_file = stage_file

    await rec.get_events()
    await rec.get_partition_summary()

    assert calls == ['memory'] * memory_stages


async def test_partitions_answers_its_degraded_shape_when_the_main_loop_does_not_answer(started, monkeypatch):
    rec = await started('none')
    monkeypatch.setattr(server_mod, 'MAIN_LOOP_DISPATCH_TIMEOUT_SECONDS', 0.05)

    async def stuck() -> list[dict]:
        await asyncio.sleep(60)
        return []

    rec.get_partition_summary = stuck

    async with _client(rec) as client:
        resp = await client.get('/api/v1/partitions')

    assert (resp.status_code, resp.json()) == (200, [])


@pytest.mark.parametrize(('max_rows', 'source'), [(LARGE_MAX_ROWS, 'memory'), (2, 'file')])
async def test_task_results_aux_lookup_uses_the_source_of_the_primary_query(started, monkeypatch, max_rows, source):
    rec = await started('sqlite', max_rows=max_rows)
    now = time.time()
    rec._record(_consumed(1, now))
    rec._record(_consumed(2, now))
    rec._record({'ts': now, 'event': 'task_started', 'task_id': 't-1', 'partition': 0, 'metadata': '{}'})
    rec._record({'ts': now, 'event': 'task_completed', 'task_id': 't-1', 'duration': 1.0, 'exit_code': 0})
    rec._record({'ts': now, 'event': 'task_complete', 'task_id': 't-1', 'partition': 0, 'metadata': '{}'})
    await rec.flush()
    aux_sources: list[str] = []
    real_select_on = UIDeps.recorder_select_on

    async def spy(self, aux_source, query, params, *, endpoint):
        aux_sources.append(aux_source)
        return await real_select_on(self, aux_source, query, params, endpoint=endpoint)

    monkeypatch.setattr(UIDeps, 'recorder_select_on', spy)

    async with _client(rec) as client:
        resp = await client.get('/api/v1/live/task-results')

    assert aux_sources == [source]
    assert [(row['task_id'], row['status']) for row in resp.json()] == [('t-1', 'completed')]


async def test_memory_read_timeout_answers_the_empty_shape_and_logs_once(started, monkeypatch):
    rec = await started('none')
    monkeypatch.setattr(memstore_mod, 'MEMORY_READ_LIMIT_SECONDS', 0.05)
    monkeypatch.setattr(queries_mod, '_memory_read_log', RateLimitedLog(time.monotonic))
    store = rec.memory_store
    real_read = store.read

    async def endless_read(query, params=()):
        return await real_read(ENDLESS_QUERY)

    store.read = endless_read

    with capture_logs() as logs:
        async with _client(rec) as client:
            first = await client.get('/api/v1/events')
            second = await client.get('/api/v1/events')

    assert (first.json(), second.json()) == ([], [])
    timed_out = [entry for entry in logs if entry['event'] == 'ui_read_memory_timed_out']
    assert len(timed_out) == 1
    assert timed_out[0]['endpoint'] == '/api/v1/events'
    assert timed_out[0]['limit_seconds'] == 0.05  # the limit the read ran under; 2.0 by default
    assert timed_out[0]['category'] == 'debug'


async def test_trace_live_part_reads_the_file_with_sqlite_and_shows_no_row_twice(started):
    rec = await started('sqlite')
    rec._record(_consumed(10))
    rec._record({'ts': time.time(), 'event': 'committed', 'partition': 0, 'offset': 10})

    events = await rec.cross_trace(0, 10)

    assert sorted(event['event'] for event in events) == ['committed', 'consumed']
    assert len({event['id'] for event in events}) == 2
    assert await _file_count(rec) == 2


async def test_label_keys_read_memory_with_sqlite(started):
    rec = await started('sqlite')
    rec._record({**_consumed(1), 'labels': json.dumps({'tenant': 'a'})})

    async with _client(rec) as client:
        resp = await client.get('/api/v1/debug/label-keys')

    assert resp.json() == ['tenant']
    assert await _file_count(rec) == 0


def _seed_every_event_kind(rec: EventRecorder) -> None:
    now = time.time()
    labels = json.dumps({'tenant': 'a'})
    timeline_event = json.dumps({'kind': 'timeline_event', 'type': 'marker', 'label': 'deploy'})
    for event in (
        {'event': 'consumed', 'partition': 0, 'offset': 10},
        {'event': 'task_started', 'task_id': 't-1', 'partition': 0, 'offset': 10, 'labels': labels},
        {'event': 'task_completed', 'task_id': 't-1', 'partition': 0, 'duration': 5.0, 'exit_code': 0},
        {'event': 'produced', 'partition': 0, 'offset': 10, 'output_topic': 'out'},
        {'event': 'task_complete', 'task_id': 't-1', 'partition': 0, 'metadata': '{}'},
        {'event': 'message_complete', 'partition': 0, 'offset': 10, 'metadata': '{}'},
        {'event': 'window_complete', 'partition': 0, 'metadata': '{}'},
        {'event': 'periodic_run', 'task_id': 'tick', 'duration': 0.1, 'metadata': '{}'},
        {'event': 'annotation', 'partition': 0, 'metadata': timeline_event},
    ):
        rec._record({'ts': now, **event})


# One row per live endpoint: the request and a check that the answer holds the seeded rows.
LIVE_ENDPOINTS = [
    ('GET', '/api/v1/events', None, lambda body: len(body) == 9),
    ('GET', '/api/v1/events?after_id=8', None, lambda body: [row['id'] for row in body] == [9]),
    ('GET', '/api/v1/recent-tasks', None, lambda body: [task['task_id'] for task in body['tasks']] == ['t-1']),
    ('GET', '/api/v1/timeline/events', None, lambda body: len(body['events']) == 1),
    ('POST', '/api/v1/live/arrange-tasks', {'task_ids': ['t-1']}, lambda body: body['t-1']['status'] == 'completed'),
    ('GET', '/api/v1/live/window-results', None, lambda body: len(body) == 1),
    ('GET', '/api/v1/live/task-results', None, lambda body: body[0]['status'] == 'completed'),
    ('GET', '/api/v1/live/message-results', None, lambda body: body[0]['end_to_end_duration'] is not None),
    ('POST', '/api/v1/live/sink-breakdown', {'partition': 0, 'offsets': [10]}, lambda body: body == {'out': 1}),
    ('GET', '/api/v1/debug/label-keys', None, lambda body: body == ['tenant']),
    ('GET', '/api/v1/debug/periodic', None, lambda body: [group['name'] for group in body] == ['tick']),
    ('GET', '/api/v1/partitions', None, lambda body: body[0]['consumed_count'] == 1),
    ('GET', '/api/v1/task/t-1', None, lambda body: body['completed'] is not None),
    ('GET', '/api/v1/debug/trace?partition=0&offset=10', None, lambda body: len(body) >= 2),
    ('GET', '/api/v1/debug/trace-by-label?key=tenant&value=a', None, lambda body: len(body) == 3),
]


@pytest.mark.parametrize(('method', 'path', 'payload', 'check'), LIVE_ENDPOINTS, ids=[row[1] for row in LIVE_ENDPOINTS])
async def test_none_worker_answers_every_live_endpoint_from_memory(started, method, path, payload, check):
    rec = await started('none')
    _seed_every_event_kind(rec)

    async with _client(rec) as client:
        resp = await client.request(method, path, json=payload)

    assert resp.status_code == 200
    assert check(resp.json()), resp.json()


async def test_default_timeline_reads_the_file_with_sqlite(started):
    rec = await started('sqlite', window_seconds=60)
    now = time.time()
    rec._record({'ts': now - 600, 'event': 'task_started', 'task_id': 'old', 'partition': 0})
    rec._record({'ts': now - 595, 'event': 'task_completed', 'task_id': 'old', 'duration': 5.0})
    rec._record({'ts': now, 'event': 'task_started', 'task_id': 'new', 'partition': 0})
    await rec.flush()
    assert rec.memory_coverage().evicted

    async with _client(rec) as client:
        resp = await client.get('/api/v1/recent-tasks?minutes=60')

    assert sorted(task['task_id'] for task in resp.json()['tasks']) == ['new', 'old']


async def test_reads_answer_their_empty_shape_when_the_memory_store_is_already_closed(started):
    rec = await started('none')
    rec._record({**_consumed(1), 'labels': json.dumps({'tenant': 'a'})})
    await rec.flush_memory()
    await rec.memory_store.close()

    async with _client(rec) as client:
        resp = await client.get('/api/v1/events')

    assert (resp.status_code, resp.json()) == (200, [])
    assert await rec.trace_by_label('tenant', 'a') == []


async def test_a_read_answers_its_empty_shape_when_the_memory_store_closes_under_it(started):
    rec = await started('none')
    rec._record(_consumed(1))
    store = rec.memory_store
    real_read = store.read

    async def read_while_the_recorder_stops(query, params=()):
        await store.close()
        return await real_read(query, params)

    store.read = read_while_the_recorder_stops

    async with _client(rec) as client:
        resp = await client.get('/api/v1/events')

    assert (resp.status_code, resp.json()) == (200, [])


async def test_a_runtime_error_while_the_memory_store_is_open_propagates(started):
    rec = await started('none')
    store = rec.memory_store

    async def broken_read(query, params=()):
        raise RuntimeError('a bug, not a stop')

    store.read = broken_read

    with pytest.raises(RuntimeError, match='a bug'):
        await queries_mod.read_memory(store, COUNT_QUERY, (), endpoint='e')


def _waiting_file_stages() -> list[asyncio.Task]:
    return [
        task
        for task in asyncio.all_tasks()
        if not task.done() and task.get_coro().__qualname__ == 'EventRecorder._run_queued_stage_file'
    ]


async def test_timed_out_file_reads_share_one_waiting_file_stage(started):
    rec = await started('sqlite')
    await rec._flush_lock.acquire()  # the disk hangs: a stage F holds the lock
    try:
        for offset in range(3):
            rec._record(_consumed(offset))
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(rec.flush(), timeout=0.05)
        assert len(_waiting_file_stages()) == 1
    finally:
        rec._flush_lock.release()
    await rec.flush()

    assert _waiting_file_stages() == []
    assert await _file_count(rec) == 3


async def test_a_flush_after_a_run_took_the_lock_starts_a_new_run_that_writes_its_row(started):
    rec = await started('sqlite')
    real_write = rec._write_file_chunk
    first_write_started, release_first_write = asyncio.Event(), asyncio.Event()

    async def write(db, batch):
        if not first_write_started.is_set():
            first_write_started.set()
            await release_first_write.wait()
        return await real_write(db, batch)

    rec._write_file_chunk = write
    rec._record(_consumed(1))
    first_flush = asyncio.ensure_future(rec.flush())
    await first_write_started.wait()  # the first run holds the lock and took row 1
    first_run = [
        task for task in asyncio.all_tasks() if task.get_coro().__qualname__.endswith('_run_queued_stage_file')
    ]

    rec._record(_consumed(2))
    second_flush = asyncio.ensure_future(rec.flush())
    await asyncio.sleep(0.01)
    assert rec._waiting_stage_file is not None
    assert rec._waiting_stage_file not in first_run
    release_first_write.set()
    await asyncio.gather(first_flush, second_flush)

    assert await _file_count(rec) == 2


def _file_stage_failure_logs(logs: list[dict]) -> list[dict]:
    return [entry for entry in logs if entry['event'] == 'recorder_flush_loop_iteration_failed']


async def _failing_file_stage(rec: EventRecorder) -> None:
    async def broken_stage() -> None:
        raise RuntimeError('disk gone')

    rec._stage_file_locked = broken_stage


async def test_a_file_stage_failure_that_no_flush_receives_is_logged(started):
    rec = await started('sqlite')
    await _failing_file_stage(rec)
    await rec._flush_lock.acquire()
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(rec.flush(), timeout=0.05)
    run = rec._waiting_stage_file

    with capture_logs() as logs:
        rec._flush_lock.release()
        await asyncio.wait([run])
        await asyncio.sleep(0)

    failures = _file_stage_failure_logs(logs)
    assert [(entry['stage'], entry['error']) for entry in failures] == [('file', 'disk gone')]


async def test_a_file_stage_failure_that_a_flush_receives_is_raised_not_logged(started):
    rec = await started('sqlite')
    await _failing_file_stage(rec)

    with capture_logs() as logs, pytest.raises(RuntimeError, match='disk gone'):
        await rec.flush()

    assert _file_stage_failure_logs(logs) == []


async def test_a_file_stage_queued_during_stop_does_not_write_after_the_seal_starts(tmp_path, monkeypatch):
    (tmp_path / 'storage').mkdir()
    rec = _recorder(tmp_path, 'sqlite')
    await rec.start()
    steps: list[str] = []
    real_write, real_sync_state = rec._write_file_chunk, rec._sync_state
    real_seal_connection = core_mod.seal_connection
    queued: list[asyncio.Future] = []

    async def write(db, batch):
        steps.append('write')
        await asyncio.sleep(0)
        return await real_write(db, batch)

    async def sync_state_with_a_read_arriving():
        rec._record(_consumed(2))
        queued.append(asyncio.ensure_future(rec.flush()))
        for _ in range(5):
            await asyncio.sleep(0)
        await real_sync_state()

    def seal_connection(*args, **kwargs):
        steps.append('seal')
        return real_seal_connection(*args, **kwargs)

    rec._write_file_chunk = write
    rec._sync_state = sync_state_with_a_read_arriving
    monkeypatch.setattr(core_mod, 'seal_connection', seal_connection)
    rec._record(_consumed(1))

    await rec.stop()
    await asyncio.gather(*queued)

    assert steps == ['write', 'seal']
