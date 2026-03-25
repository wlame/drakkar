"""Tests for Drakkar flight recorder."""

import os
from pathlib import Path

import pytest
from pydantic import BaseModel

from drakkar.config import DebugConfig
from drakkar.models import (
    ExecutorError,
    ExecutorResult,
    ExecutorTask,
    KafkaPayload,
    SourceMessage,
)
from drakkar.recorder import (
    SCHEMA_WORKER_CONFIG,
    EventRecorder,
    _format_dt,
    _list_db_files,
    _live_link_path,
    _make_db_path,
)


class _RecData(BaseModel):
    v: str = 'ok'


WORKER_NAME = 'test-worker'


def make_debug_config(tmp_path, **overrides) -> DebugConfig:
    defaults = {
        'enabled': True,
        'db_dir': str(tmp_path),
        'retention_hours': 24,
        'retention_max_events': 1000,
        'store_output': False,
        'flush_interval_seconds': 60,
    }
    defaults.update(overrides)
    return DebugConfig(**defaults)


def make_msg(partition=0, offset=0) -> SourceMessage:
    return SourceMessage(
        topic='t',
        partition=partition,
        offset=offset,
        value=b'{"x":1}',
        timestamp=1000,
    )


def make_task(task_id='t1', args=None, offsets=None) -> ExecutorTask:
    return ExecutorTask(
        task_id=task_id,
        args=args or ['--input', 'file.txt'],
        source_offsets=offsets or [0],
    )


def make_result(task_id='t1', task=None) -> ExecutorResult:
    t = task or make_task(task_id)
    return ExecutorResult(
        task_id=task_id,
        exit_code=0,
        stdout='line1\nline2\n',
        stderr='',
        duration_seconds=1.5,
        task=t,
    )


@pytest.fixture
async def recorder(tmp_path):
    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()
    yield rec
    await rec.stop()


# --- DB path generation ---


def test_make_db_path_includes_timestamp():
    path = _make_db_path('/tmp', 'worker-1')
    assert path.startswith('/tmp/worker-1-')
    assert path.endswith('.db')
    assert '__' in path  # YYYY-MM-DD__HH_MM_SS


def test_make_db_path_uses_worker_name():
    path = _make_db_path('/var/log', 'my-worker')
    assert path.startswith('/var/log/my-worker-')


def test_list_db_files_returns_sorted(tmp_path):
    for name in [
        'test-worker-2026-03-16__14_00_00.db',
        'test-worker-2026-03-15__10_00_00.db',
        'test-worker-2026-03-16__15_00_00.db',
    ]:
        (tmp_path / name).touch()
    files = _list_db_files(str(tmp_path), 'test-worker')
    assert len(files) == 3
    assert '10_00' in files[0]  # sorted oldest first


def test_list_db_files_excludes_live_symlink(tmp_path):
    (tmp_path / 'test-worker-2026-03-16__14_00_00.db').touch()
    live = tmp_path / 'test-worker-live.db'
    live.symlink_to('test-worker-2026-03-16__14_00_00.db')
    files = _list_db_files(str(tmp_path), 'test-worker')
    assert len(files) == 1
    assert 'live' not in files[0]


# --- Start/stop ---


async def test_start_creates_timestamped_db(tmp_path):
    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()
    assert rec._db is not None
    assert rec.db_path.startswith(str(tmp_path / f'{WORKER_NAME}-'))
    assert os.path.exists(rec.db_path)
    await rec.stop()
    assert rec._db is None


# --- Recording + flush ---


async def test_record_consumed_and_flush(recorder):
    recorder.record_consumed(make_msg(partition=3, offset=42))
    recorder.record_consumed(make_msg(partition=3, offset=43))
    await recorder._flush()

    events = await recorder.get_events(partition=3)
    assert len(events) == 2
    assert events[0]['event'] == 'consumed'
    assert events[0]['partition'] == 3


async def test_record_arranged(recorder):
    msgs = [make_msg(offset=10), make_msg(offset=11)]
    tasks = [make_task('t1', offsets=[10]), make_task('t2', offsets=[11])]
    recorder.record_arranged(partition=0, messages=msgs, tasks=tasks)
    await recorder._flush()

    events = await recorder.get_events(event_type='arranged')
    assert len(events) == 1
    import json

    meta = json.loads(events[0]['metadata'])
    assert meta['task_count'] == 2
    assert meta['offsets'] == [10, 11]


async def test_record_task_started(recorder):
    task = make_task('t1', args=['--pattern', 'error'])
    recorder.record_task_started(task, partition=5)
    await recorder._flush()

    events = await recorder.get_events(event_type='task_started')
    assert len(events) == 1
    assert events[0]['task_id'] == 't1'
    assert events[0]['partition'] == 5


async def test_record_task_completed_without_output(recorder):
    result = make_result('t1')
    recorder.record_task_completed(result, partition=2)
    await recorder._flush()

    events = await recorder.get_events(event_type='task_completed')
    assert len(events) == 1
    assert events[0]['exit_code'] == 0
    assert events[0]['duration'] == 1.5
    assert events[0]['stdout_size'] == len(b'line1\nline2\n')
    assert events[0]['stdout'] is None


async def test_record_task_completed_with_output(tmp_path):
    config = make_debug_config(tmp_path, store_output=True)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    result = make_result('t1')
    rec.record_task_completed(result, partition=0)
    await rec._flush()

    events = await rec.get_events(event_type='task_completed')
    assert events[0]['stdout'] == 'line1\nline2\n'
    await rec.stop()


async def test_record_task_failed(recorder):
    task = make_task('t1')
    error = ExecutorError(task=task, exit_code=1, stderr='bad input')
    recorder.record_task_failed(task, error, partition=0)
    await recorder._flush()

    events = await recorder.get_events(event_type='task_failed')
    assert len(events) == 1
    assert events[0]['exit_code'] == 1


async def test_record_produced(recorder):
    msg = KafkaPayload(key=b'k', data=_RecData())
    recorder.record_produced(msg, source_partition=3, source_offset=42)
    await recorder._flush()

    events = await recorder.get_events(event_type='produced')
    assert len(events) == 1
    assert events[0]['partition'] == 3
    assert events[0]['offset'] == 42


async def test_record_committed(recorder):
    recorder.record_committed(partition=5, offset=100)
    await recorder._flush()

    events = await recorder.get_events(event_type='committed')
    assert len(events) == 1
    assert events[0]['partition'] == 5
    assert events[0]['offset'] == 100


async def test_record_assigned_and_revoked(recorder):
    recorder.record_assigned([0, 1, 2])
    recorder.record_revoked([1])
    await recorder._flush()

    assigned = await recorder.get_events(event_type='assigned')
    assert len(assigned) == 3

    revoked = await recorder.get_events(event_type='revoked')
    assert len(revoked) == 1
    assert revoked[0]['partition'] == 1


# --- Queries ---


async def test_get_events_filtering(recorder):
    recorder.record_consumed(make_msg(partition=0, offset=0))
    recorder.record_consumed(make_msg(partition=1, offset=0))
    recorder.record_committed(partition=0, offset=1)
    await recorder._flush()

    all_events = await recorder.get_events()
    assert len(all_events) == 3

    p0_events = await recorder.get_events(partition=0)
    assert len(p0_events) == 2

    commits = await recorder.get_events(event_type='committed')
    assert len(commits) == 1


async def test_get_events_pagination(recorder):
    for i in range(20):
        recorder.record_consumed(make_msg(offset=i))
    await recorder._flush()

    page1 = await recorder.get_events(limit=5, offset=0)
    assert len(page1) == 5

    page2 = await recorder.get_events(limit=5, offset=5)
    assert len(page2) == 5
    assert page1[0]['id'] != page2[0]['id']


async def test_get_trace(recorder):
    recorder.record_consumed(make_msg(partition=3, offset=42))
    task = make_task('t-42', offsets=[42])
    recorder.record_task_started(task, partition=3)
    recorder.record_task_completed(make_result('t-42', task=task), partition=3)
    recorder.record_committed(partition=3, offset=43)
    await recorder._flush()

    trace = await recorder.get_trace(partition=3, msg_offset=42)
    assert len(trace) >= 2
    event_types = [e['event'] for e in trace]
    assert 'consumed' in event_types


async def test_get_partition_summary(recorder):
    recorder.record_consumed(make_msg(partition=0, offset=0))
    recorder.record_consumed(make_msg(partition=0, offset=1))
    recorder.record_consumed(make_msg(partition=1, offset=0))
    recorder.record_committed(partition=0, offset=2)
    task = make_task('t1', offsets=[0])
    recorder.record_task_completed(make_result('t1', task), partition=0)
    await recorder._flush()

    summary = await recorder.get_partition_summary()
    assert len(summary) == 2
    p0 = next(s for s in summary if s['partition'] == 0)
    assert p0['consumed_count'] == 2
    assert p0['completed_count'] == 1
    assert p0['last_committed_offset'] == 2


async def test_get_active_tasks(recorder):
    recorder.record_task_started(make_task('t1'), partition=0)
    recorder.record_task_started(make_task('t2'), partition=0)
    recorder.record_task_completed(make_result('t1'), partition=0)
    await recorder._flush()

    active = await recorder.get_active_tasks()
    assert len(active) == 1
    assert active[0]['task_id'] == 't2'


async def test_get_stats(recorder):
    recorder.record_consumed(make_msg(offset=0))
    recorder.record_consumed(make_msg(offset=1))
    task = make_task('t1')
    recorder.record_task_completed(make_result('t1', task), partition=0)
    recorder.record_committed(partition=0, offset=2)
    await recorder._flush()

    stats = await recorder.get_stats()
    assert stats['total_events'] == 4
    assert stats['consumed'] == 2
    assert stats['completed'] == 1
    assert stats['committed'] == 1


async def test_get_stats_empty_db(recorder):
    stats = await recorder.get_stats()
    assert stats['total_events'] == 0


async def test_get_events_no_db():
    config = DebugConfig(enabled=True, db_dir='')
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    events = await rec.get_events()
    assert events == []


# --- Rotation ---


async def test_rotate_creates_new_db_and_flushes(tmp_path):
    config = make_debug_config(tmp_path, retention_hours=24)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    rec.record_consumed(make_msg(offset=0))
    rec.record_consumed(make_msg(offset=1))
    # buffer has 2 events, not flushed yet

    await rec._rotate()

    assert rec._db is not None  # new DB connection open
    assert os.path.exists(rec.db_path)
    # buffer was cleared during rotation flush
    assert len(rec._buffer) == 0

    await rec.stop()


async def test_rotate_flushes_buffer_to_old_db(tmp_path):
    config = make_debug_config(tmp_path, retention_hours=24)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    rec.record_consumed(make_msg(offset=0))
    rec.record_consumed(make_msg(offset=1))
    # don't flush — buffer has 2 events

    old_path = rec.db_path
    await rec._rotate()

    # buffer was flushed to old DB before rotation
    import aiosqlite

    async with (
        aiosqlite.connect(old_path) as old_db,
        old_db.execute('SELECT COUNT(*) FROM events') as cursor,
    ):
        row = await cursor.fetchone()
        assert row[0] == 2

    await rec.stop()


async def test_rotate_deletes_old_files(tmp_path):
    config = make_debug_config(tmp_path, retention_hours=1)  # short retention

    # create an "old" DB file manually with ancient mtime
    old_file = tmp_path / f'{WORKER_NAME}-2025-01-01__00_00_00.db'
    old_file.write_text('')
    os.utime(old_file, (0, 0))

    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    await rec._rotate()

    assert not os.path.exists(old_file)  # deleted because retention_hours=0
    assert os.path.exists(rec.db_path)  # current file exists
    await rec.stop()


async def test_rotate_enforces_max_file_count(tmp_path):
    config = make_debug_config(
        tmp_path,
        retention_hours=999,
        retention_max_events=10_000,  # max_files = 10000/10000 = 1
    )
    # create several old DB files manually
    for i in range(5):
        p = tmp_path / f'{WORKER_NAME}-2026-03-{10 + i:02d}__00_00_00.db'
        p.write_text('')

    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()
    await rec._rotate()

    remaining = _list_db_files(str(tmp_path), WORKER_NAME)
    # should keep at most 1 old file + the new one
    assert len(remaining) <= 2
    await rec.stop()


# --- Pool stats in events ---


async def test_task_started_includes_pool_stats(recorder):
    task = make_task('t1')
    recorder.record_task_started(task, partition=0, pool_active=5, pool_waiting=12)
    await recorder._flush()

    queue = recorder.subscribe()
    # re-record to check WS event
    recorder.record_task_started(task, partition=0, pool_active=3, pool_waiting=7)
    event = queue.get_nowait()
    assert event['pool_active'] == 3
    assert event['pool_waiting'] == 7
    recorder.unsubscribe(queue)


async def test_task_completed_includes_pool_stats(recorder):
    result = make_result('t1')
    queue = recorder.subscribe()
    recorder.record_task_completed(result, partition=0, pool_active=4, pool_waiting=0)
    event = queue.get_nowait()
    assert event['pool_active'] == 4
    assert event['pool_waiting'] == 0
    recorder.unsubscribe(queue)


# --- WebSocket broadcast tests ---


async def test_subscribe_receives_events(tmp_path):
    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    queue = rec.subscribe()
    assert len(rec._ws_subscribers) == 1

    rec.record_consumed(make_msg(partition=5, offset=99))

    event = queue.get_nowait()
    assert event['event'] == 'consumed'
    assert event['partition'] == 5
    assert event['offset'] == 99

    rec.unsubscribe(queue)
    assert len(rec._ws_subscribers) == 0
    await rec.stop()


async def test_broadcast_to_multiple_subscribers(tmp_path):
    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    q1 = rec.subscribe()
    q2 = rec.subscribe()
    assert len(rec._ws_subscribers) == 2

    rec.record_committed(partition=3, offset=42)

    e1 = q1.get_nowait()
    e2 = q2.get_nowait()
    assert e1['event'] == 'committed'
    assert e2['event'] == 'committed'

    rec.unsubscribe(q1)
    rec.unsubscribe(q2)
    await rec.stop()


async def test_broadcast_drops_on_full_queue(tmp_path):
    """If subscriber queue is full, events are dropped without error."""
    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    import queue as queue_mod

    q = queue_mod.Queue(maxsize=2)
    rec._ws_subscribers.add(q)

    # fill the queue
    rec.record_consumed(make_msg(offset=0))
    rec.record_consumed(make_msg(offset=1))
    # this should be dropped, not raise
    rec.record_consumed(make_msg(offset=2))

    assert q.qsize() == 2  # only 2 fit

    rec._ws_subscribers.discard(q)
    await rec.stop()


async def test_no_broadcast_without_subscribers(tmp_path):
    """Recording works fine with no subscribers."""
    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    assert len(rec._ws_subscribers) == 0
    rec.record_consumed(make_msg(offset=0))  # should not raise
    await rec._flush()

    events = await rec.get_events()
    assert len(events) == 1
    await rec.stop()


# --- All event types persisted ---


async def test_all_event_types_persisted(recorder):
    """Every event type must survive buffer → flush → SQLite round-trip."""
    msg = make_msg(partition=1, offset=10)
    task = make_task('t-all', args=['--check'], offsets=[10])
    result = make_result('t-all', task=task)
    error = ExecutorError(task=task, exit_code=1, stderr='oops')
    out_msg = KafkaPayload(key=b'k', data=_RecData())

    recorder.record_consumed(msg)
    recorder.record_arranged(partition=1, messages=[msg], tasks=[task])
    recorder.record_task_started(task, partition=1, pool_active=2, pool_waiting=3, slot=0)
    recorder.record_task_completed(result, partition=1, pool_active=1, pool_waiting=0)
    recorder.record_task_failed(task, error, partition=1)
    recorder.record_collect_completed(task_id='t-all', partition=1, duration=0.05, output_message_count=2)
    recorder.record_produced(out_msg, source_partition=1, source_offset=10)
    recorder.record_committed(partition=1, offset=11)
    recorder.record_assigned([1, 2])
    recorder.record_revoked([2])
    recorder.record_sink_delivery(sink_type='kafka', sink_name='out', payload_count=1, duration=0.01)
    recorder.record_sink_error(sink_type='http', sink_name='wh', error='timeout', attempt=1)

    await recorder._flush()

    all_events = await recorder.get_events(limit=50)
    event_types = {e['event'] for e in all_events}

    expected = {
        'consumed',
        'arranged',
        'task_started',
        'task_completed',
        'task_failed',
        'collect_completed',
        'produced',
        'committed',
        'assigned',
        'revoked',
        'sink_delivered',
        'sink_error',
    }
    assert event_types == expected, f'missing: {expected - event_types}, extra: {event_types - expected}'


async def test_collect_completed_persisted(recorder):
    """collect_completed event stores task_id, duration, and output_message_count."""
    recorder.record_collect_completed(task_id='t-cc', partition=2, duration=0.123, output_message_count=5)
    await recorder._flush()

    events = await recorder.get_events(event_type='collect_completed')
    assert len(events) == 1
    e = events[0]
    assert e['task_id'] == 't-cc'
    assert e['partition'] == 2
    assert e['duration'] == 0.123

    import json

    meta = json.loads(e['metadata'])
    assert meta['output_message_count'] == 5


# --- Rotation smoothness ---


async def test_rotation_no_event_loss(tmp_path):
    """Events recorded before, during, and after rotation are all queryable."""
    config = make_debug_config(tmp_path, retention_hours=999, retention_max_events=100_000)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    # phase 1: record before rotation
    rec.record_consumed(make_msg(offset=0))
    rec.record_consumed(make_msg(offset=1))
    await rec._flush()

    # verify pre-rotation events
    pre_events = await rec.get_events()
    assert len(pre_events) == 2

    # phase 2: record into buffer (not flushed), then rotate
    rec.record_consumed(make_msg(offset=2))
    rec.record_consumed(make_msg(offset=3))
    old_path = rec.db_path

    import asyncio

    await asyncio.sleep(1.1)  # ensure different second-level timestamp
    await rec._rotate()  # flushes buffer to old DB, opens new DB

    # old DB should have all 4 events
    import aiosqlite

    async with (
        aiosqlite.connect(old_path) as old_db,
        old_db.execute('SELECT COUNT(*) FROM events') as cur,
    ):
        row = await cur.fetchone()
        assert row[0] == 4

    # phase 3: record after rotation → goes to new DB
    rec.record_consumed(make_msg(offset=4))
    rec.record_consumed(make_msg(offset=5))
    await rec._flush()

    new_events = await rec.get_events()
    assert len(new_events) == 2
    offsets = {e['offset'] for e in new_events}
    assert offsets == {4, 5}

    await rec.stop()


async def test_rotation_queries_work_on_new_db(tmp_path):
    """After rotation, all query methods work on the new DB."""
    config = make_debug_config(tmp_path, retention_hours=999, retention_max_events=100_000)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    await rec._rotate()

    # record fresh events in new DB
    msg = make_msg(partition=0, offset=100)
    task = make_task('t-rot', offsets=[100])
    rec.record_consumed(msg)
    rec.record_task_started(task, partition=0)
    rec.record_task_completed(make_result('t-rot', task=task), partition=0)
    rec.record_committed(partition=0, offset=101)
    await rec._flush()

    # all query methods should return data
    events = await rec.get_events()
    assert len(events) == 4

    summary = await rec.get_partition_summary()
    assert len(summary) == 1

    stats = await rec.get_stats()
    assert stats['total_events'] == 4

    active = await rec.get_active_tasks()
    assert len(active) == 0  # task was completed

    trace = await rec.get_trace(partition=0, msg_offset=100)
    assert len(trace) >= 1

    await rec.stop()


async def test_rotation_new_db_has_schema(tmp_path):
    """The new DB file after rotation has the full schema (tables + indexes)."""
    config = make_debug_config(tmp_path, retention_hours=999, retention_max_events=100_000)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    await rec._rotate()
    new_path = rec.db_path

    import aiosqlite

    async with aiosqlite.connect(new_path) as db:
        # verify events table exists
        async with db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='events'") as cur:
            tables = await cur.fetchall()
            assert len(tables) == 1

        # verify indexes exist
        async with db.execute("SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_events_%'") as cur:
            indexes = await cur.fetchall()
            assert len(indexes) == 5  # partition_offset, ts, dt, task_id, type

    await rec.stop()


async def test_multiple_rotations_keep_recent_files(tmp_path):
    """Multiple rotations keep only files within retention limits."""
    config = make_debug_config(
        tmp_path,
        retention_hours=1,
        retention_max_events=10_000,
    )
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    # create several "old" files with ancient mtime
    old_files = []
    for i in range(5):
        p = tmp_path / f'{WORKER_NAME}-2024-01-{10 + i:02d}__00_00_00.db'
        p.write_text('')
        os.utime(p, (0, 0))
        old_files.append(p)

    # rotate twice with different timestamps
    import asyncio

    await rec._rotate()
    await asyncio.sleep(1.1)
    await rec._rotate()
    path_after_second = rec.db_path

    # old files should be deleted (ancient mtime < retention cutoff)
    for p in old_files:
        assert not p.exists(), f'{p.name} should have been deleted'

    # current DB should exist
    assert os.path.exists(path_after_second)

    await rec.stop()


# --- Sink event recording ---


async def test_record_sink_delivery(recorder):
    recorder.record_sink_delivery(sink_type='kafka', sink_name='results', payload_count=5, duration=0.042)
    await recorder._flush()

    events = await recorder.get_events(event_type='sink_delivered')
    assert len(events) == 1

    import json

    meta = json.loads(events[0]['metadata'])
    assert meta['sink_type'] == 'kafka'
    assert meta['sink_name'] == 'results'
    assert meta['payload_count'] == 5
    assert meta['duration'] == 0.042


async def test_record_sink_error(recorder):
    recorder.record_sink_error(sink_type='postgres', sink_name='main-db', error='connection refused', attempt=2)
    await recorder._flush()

    events = await recorder.get_events(event_type='sink_error')
    assert len(events) == 1

    import json

    meta = json.loads(events[0]['metadata'])
    assert meta['sink_type'] == 'postgres'
    assert meta['sink_name'] == 'main-db'
    assert meta['error'] == 'connection refused'
    assert meta['attempt'] == 2


async def test_sink_events_in_all_event_types(recorder):
    """sink_delivered and sink_error survive the buffer → flush → SQLite round-trip."""
    recorder.record_sink_delivery(sink_type='kafka', sink_name='out', payload_count=1, duration=0.01)
    recorder.record_sink_error(sink_type='http', sink_name='webhook', error='timeout', attempt=1)
    await recorder._flush()

    all_events = await recorder.get_events(limit=50)
    event_types = {e['event'] for e in all_events}
    assert 'sink_delivered' in event_types
    assert 'sink_error' in event_types


# --- Granular schema creation flags ---


async def _table_exists(db, table_name: str) -> bool:
    async with db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        [table_name],
    ) as cur:
        return await cur.fetchone() is not None


async def test_schema_all_tables_created_by_default(tmp_path):
    """With default config all three tables are created."""
    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    assert await _table_exists(rec._db, 'events')
    assert await _table_exists(rec._db, 'worker_config')
    assert await _table_exists(rec._db, 'worker_state')
    await rec.stop()


async def test_schema_events_only(tmp_path):
    """store_config=False, store_state=False → only events table."""
    config = make_debug_config(tmp_path, store_config=False, store_state=False)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    assert await _table_exists(rec._db, 'events')
    assert not await _table_exists(rec._db, 'worker_config')
    assert not await _table_exists(rec._db, 'worker_state')
    await rec.stop()


async def test_schema_config_only(tmp_path):
    """store_events=False, store_state=False → only worker_config table."""
    config = make_debug_config(tmp_path, store_events=False, store_state=False)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    assert not await _table_exists(rec._db, 'events')
    assert await _table_exists(rec._db, 'worker_config')
    assert not await _table_exists(rec._db, 'worker_state')
    await rec.stop()


async def test_schema_state_only(tmp_path):
    """store_events=False, store_config=False → only worker_state table."""
    config = make_debug_config(tmp_path, store_events=False, store_config=False)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    assert not await _table_exists(rec._db, 'events')
    assert not await _table_exists(rec._db, 'worker_config')
    assert await _table_exists(rec._db, 'worker_state')
    await rec.stop()


async def test_schema_none_creates_db_but_no_tables(tmp_path):
    """All store flags False → DB file exists but has no application tables."""
    config = make_debug_config(tmp_path, store_events=False, store_config=False, store_state=False)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    assert rec._db is not None
    assert os.path.exists(rec.db_path)
    assert not await _table_exists(rec._db, 'events')
    assert not await _table_exists(rec._db, 'worker_config')
    assert not await _table_exists(rec._db, 'worker_state')
    await rec.stop()


async def test_schema_no_db_dir_means_no_db(tmp_path):
    """Empty db_dir → no DB opened at all, recorder runs memory-only."""
    config = make_debug_config(tmp_path, db_dir='')
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    assert rec._db is None
    assert rec.db_path == ''
    await rec.stop()


# --- write_config (worker_config table) ---


def _make_drakkar_config():
    """Create a minimal DrakkarConfig for testing write_config."""
    from drakkar.config import DrakkarConfig

    return DrakkarConfig(
        kafka={'brokers': 'kafka:9092', 'source_topic': 'test-topic', 'consumer_group': 'test-group'},
        executor={
            'binary_path': '/usr/bin/test',
            'max_workers': 4,
            'task_timeout_seconds': 60,
            'max_retries': 2,
            'window_size': 5,
        },
        sinks={'kafka': {'out': {'topic': 'results'}}},
    )


async def test_write_config_populates_single_row(tmp_path):
    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    drakkar_cfg = _make_drakkar_config()
    await rec.write_config(drakkar_cfg)

    import json

    async with rec._db.execute('SELECT * FROM worker_config WHERE id = 1') as cur:
        columns = [d[0] for d in cur.description]
        row = await cur.fetchone()
    assert row is not None
    data = dict(zip(columns, row, strict=False))
    assert data['worker_name'] == WORKER_NAME
    assert data['kafka_brokers'] == 'kafka:9092'
    assert data['source_topic'] == 'test-topic'
    assert data['consumer_group'] == 'test-group'
    assert data['binary_path'] == '/usr/bin/test'
    assert data['max_workers'] == 4
    sinks = json.loads(data['sinks_json'])
    assert 'kafka' in sinks
    await rec.stop()


async def test_write_config_stores_debug_url(tmp_path):
    config = make_debug_config(tmp_path, debug_url='http://localhost:8081/')
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    await rec.write_config(_make_drakkar_config())

    async with rec._db.execute('SELECT debug_url FROM worker_config WHERE id = 1') as cur:
        row = await cur.fetchone()
    assert row[0] == 'http://localhost:8081/'
    await rec.stop()


async def test_write_config_debug_url_null_when_empty(tmp_path):
    """Empty debug_url is stored as NULL."""
    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    await rec.write_config(_make_drakkar_config())

    async with rec._db.execute('SELECT debug_url FROM worker_config WHERE id = 1') as cur:
        row = await cur.fetchone()
    assert row[0] is None
    await rec.stop()


async def test_write_config_stores_cluster_name(tmp_path):
    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME, cluster_name='main cluster')
    await rec.start()

    await rec.write_config(_make_drakkar_config())

    async with rec._db.execute('SELECT cluster_name FROM worker_config WHERE id = 1') as cur:
        row = await cur.fetchone()
    assert row[0] == 'main cluster'
    await rec.stop()


async def test_write_config_cluster_name_null_when_empty(tmp_path):
    """Empty cluster_name is stored as NULL."""
    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    await rec.write_config(_make_drakkar_config())

    async with rec._db.execute('SELECT cluster_name FROM worker_config WHERE id = 1') as cur:
        row = await cur.fetchone()
    assert row[0] is None
    await rec.stop()


async def test_write_config_captures_env_vars(tmp_path, monkeypatch):
    monkeypatch.setenv('MY_VAR', 'hello')
    config = make_debug_config(tmp_path, expose_env_vars=['MY_VAR', 'MISSING_VAR'])
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    await rec.write_config(_make_drakkar_config())

    import json

    async with rec._db.execute('SELECT env_vars_json FROM worker_config WHERE id = 1') as cur:
        row = await cur.fetchone()
    env = json.loads(row[0])
    assert env['MY_VAR'] == 'hello'
    assert env['MISSING_VAR'] == ''
    await rec.stop()


async def test_write_config_idempotent(tmp_path):
    """Calling write_config twice replaces the row (INSERT OR REPLACE)."""
    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    await rec.write_config(_make_drakkar_config())
    await rec.write_config(_make_drakkar_config())

    async with rec._db.execute('SELECT COUNT(*) FROM worker_config') as cur:
        row = await cur.fetchone()
    assert row[0] == 1
    await rec.stop()


async def test_write_config_skipped_when_store_config_false(tmp_path):
    config = make_debug_config(tmp_path, store_config=False)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    await rec.write_config(_make_drakkar_config())
    # worker_config table doesn't exist, but no error
    assert not await _table_exists(rec._db, 'worker_config')
    await rec.stop()


# --- worker_state sync ---


async def test_sync_state_writes_row(tmp_path):
    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    rec.set_state_provider(
        lambda: {
            'uptime_seconds': 42.5,
            'assigned_partitions': [0, 1, 2],
            'partition_count': 3,
            'pool_active': 2,
            'pool_max': 8,
            'total_queued': 10,
            'paused': False,
        }
    )

    # increment some counters
    rec._counters['consumed'] = 100
    rec._counters['completed'] = 50

    await rec._sync_state()

    import json

    async with rec._db.execute('SELECT * FROM worker_state ORDER BY id DESC LIMIT 1') as cur:
        columns = [d[0] for d in cur.description]
        row = await cur.fetchone()
    assert row is not None
    data = dict(zip(columns, row, strict=False))
    assert data['uptime_seconds'] == 42.5
    assert json.loads(data['assigned_partitions']) == [0, 1, 2]
    assert data['partition_count'] == 3
    assert data['pool_active'] == 2
    assert data['consumed_count'] == 100
    assert data['completed_count'] == 50
    assert data['paused'] == 0
    assert data['updated_at_dt'] is not None
    await rec.stop()


async def test_sync_state_accumulates_rows(tmp_path):
    """Each sync appends a new row (time series)."""
    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()
    rec.set_state_provider(lambda: {'uptime_seconds': 1.0})

    await rec._sync_state()
    await rec._sync_state()
    await rec._sync_state()

    async with rec._db.execute('SELECT COUNT(*) FROM worker_state') as cur:
        row = await cur.fetchone()
    assert row[0] == 3
    await rec.stop()


async def test_sync_state_skipped_when_store_state_false(tmp_path):
    config = make_debug_config(tmp_path, store_state=False)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()
    rec.set_state_provider(lambda: {'uptime_seconds': 1.0})

    await rec._sync_state()  # should not raise
    assert not await _table_exists(rec._db, 'worker_state')
    await rec.stop()


async def test_sync_state_without_provider(tmp_path):
    """_sync_state works even without a state provider (uses empty dict)."""
    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    await rec._sync_state()

    async with rec._db.execute('SELECT uptime_seconds FROM worker_state ORDER BY id DESC LIMIT 1') as cur:
        row = await cur.fetchone()
    assert row is not None
    assert row[0] == 0  # default when no provider
    await rec.stop()


# --- In-memory counters ---


async def test_counters_increment_with_store_events_true(tmp_path):
    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    rec.record_consumed(make_msg(offset=0))
    rec.record_consumed(make_msg(offset=1))
    rec.record_task_completed(make_result('t1'), partition=0)
    rec.record_task_failed(make_task('t2'), ExecutorError(task=make_task('t2'), exit_code=1, stderr='err'), partition=0)
    rec.record_produced(KafkaPayload(key=b'k', data=_RecData()), source_partition=0)
    rec.record_committed(partition=0, offset=2)

    assert rec.counters == {
        'consumed': 2,
        'completed': 1,
        'failed': 1,
        'produced': 1,
        'committed': 1,
    }
    await rec.stop()


async def test_counters_increment_with_store_events_false(tmp_path):
    """Counters track regardless of store_events flag."""
    config = make_debug_config(tmp_path, store_events=False)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    rec.record_consumed(make_msg(offset=0))
    rec.record_consumed(make_msg(offset=1))
    rec.record_consumed(make_msg(offset=2))
    rec.record_task_completed(make_result('t1'), partition=0)
    rec.record_committed(partition=0, offset=3)

    assert rec.counters['consumed'] == 3
    assert rec.counters['completed'] == 1
    assert rec.counters['committed'] == 1
    await rec.stop()


async def test_counters_survive_flush(tmp_path):
    """Counters are in-memory, not reset by flush."""
    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    rec.record_consumed(make_msg(offset=0))
    await rec._flush()
    rec.record_consumed(make_msg(offset=1))
    await rec._flush()

    assert rec.counters['consumed'] == 2
    await rec.stop()


# --- store_events=False behavior ---


async def test_store_events_false_queries_return_empty(tmp_path):
    """All query methods return empty when store_events is disabled."""
    config = make_debug_config(tmp_path, store_events=False)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    rec.record_consumed(make_msg(offset=0))
    await rec._flush()

    assert await rec.get_events() == []
    assert await rec.get_trace(partition=0, msg_offset=0) == []
    assert await rec.get_partition_summary() == []
    assert await rec.get_active_tasks() == []
    stats = await rec.get_stats()
    assert stats == {'total_events': 0}
    await rec.stop()


async def test_store_events_false_no_events_table(tmp_path):
    config = make_debug_config(tmp_path, store_events=False)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    assert not await _table_exists(rec._db, 'events')
    await rec.stop()


async def test_store_events_false_flush_clears_buffer(tmp_path):
    """Buffer is cleared on flush even without events table."""
    config = make_debug_config(tmp_path, store_events=False)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    rec.record_consumed(make_msg(offset=0))
    rec.record_consumed(make_msg(offset=1))
    assert len(rec._buffer) == 2

    await rec._flush()
    assert len(rec._buffer) == 0
    await rec.stop()


async def test_store_events_false_ws_broadcast_still_works(tmp_path):
    """Events are broadcast to WS subscribers even without persistence."""
    config = make_debug_config(tmp_path, store_events=False)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    q = rec.subscribe()
    rec.record_consumed(make_msg(partition=7, offset=99))

    event = q.get_nowait()
    assert event['event'] == 'consumed'
    assert event['partition'] == 7
    rec.unsubscribe(q)
    await rec.stop()


async def test_store_events_false_no_flush_task(tmp_path):
    """Flush loop task is not started when store_events=False."""
    config = make_debug_config(tmp_path, store_events=False)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    assert rec._flush_task is None
    await rec.stop()


# --- Autodiscovery (discover_workers) ---


async def test_discover_workers_finds_other_worker(tmp_path):
    """discover_workers reads worker_config from another worker's live DB."""
    import aiosqlite

    # create a fake other worker's DB
    other_db_path = tmp_path / 'other-worker-2026-03-24__10_00_00.db'
    async with aiosqlite.connect(str(other_db_path)) as db:
        await db.executescript(SCHEMA_WORKER_CONFIG)
        await db.execute(
            """INSERT INTO worker_config
               (id, worker_name, ip_address, debug_port, debug_url, kafka_brokers, source_topic,
                consumer_group, binary_path, max_workers, task_timeout_seconds,
                max_retries, window_size, sinks_json, env_vars_json, created_at, created_at_dt)
               VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                'other-worker',
                '10.0.0.2',
                8080,
                'http://localhost:8080/',
                'kafka:9092',
                'test',
                'grp',
                '/bin/test',
                4,
                60,
                2,
                5,
                '{}',
                '{}',
                1000.0,
                _format_dt(1000.0),
            ],
        )
        await db.commit()

    # create live symlink for the other worker
    link = _live_link_path(str(tmp_path), 'other-worker')
    os.symlink(other_db_path.name, link)

    # start our recorder
    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    workers = await rec.discover_workers()
    assert len(workers) == 1
    assert workers[0]['worker_name'] == 'other-worker'
    assert workers[0]['ip_address'] == '10.0.0.2'
    assert workers[0]['debug_port'] == 8080
    assert workers[0]['debug_url'] == 'http://localhost:8080/'
    await rec.stop()


async def test_discover_workers_skips_own_symlink(tmp_path):
    """discover_workers does not include our own worker."""
    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()
    await rec.write_config(_make_drakkar_config())

    # our own live symlink exists
    assert os.path.islink(_live_link_path(str(tmp_path), WORKER_NAME))

    workers = await rec.discover_workers()
    assert len(workers) == 0
    await rec.stop()


async def test_discover_workers_skips_missing_config_table(tmp_path):
    """If another worker's DB has no worker_config table, it's skipped."""
    import aiosqlite

    # create a DB without worker_config
    other_db = tmp_path / 'no-config-worker-2026-03-24__10_00_00.db'
    async with aiosqlite.connect(str(other_db)) as db:
        await db.execute('CREATE TABLE IF NOT EXISTS dummy (id INTEGER)')
        await db.commit()

    link = _live_link_path(str(tmp_path), 'no-config-worker')
    os.symlink(other_db.name, link)

    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    workers = await rec.discover_workers()
    assert len(workers) == 0
    await rec.stop()


async def test_discover_workers_handles_broken_symlink(tmp_path):
    """Broken symlink (target deleted) is handled gracefully."""
    link = _live_link_path(str(tmp_path), 'ghost-worker')
    os.symlink('nonexistent-file.db', link)

    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    workers = await rec.discover_workers()
    assert len(workers) == 0
    await rec.stop()


async def test_discover_workers_empty_when_store_config_false(tmp_path):
    """discover_workers returns empty when store_config is disabled."""
    config = make_debug_config(tmp_path, store_config=False)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    workers = await rec.discover_workers()
    assert workers == []
    await rec.stop()


async def test_discover_workers_empty_when_no_db_dir():
    """discover_workers returns empty when db_dir is empty."""
    config = DebugConfig(enabled=True, db_dir='')
    rec = EventRecorder(config, worker_name=WORKER_NAME)

    workers = await rec.discover_workers()
    assert workers == []


async def test_discover_workers_ignores_non_symlink_files(tmp_path):
    """Regular files matching *-live.db are not treated as workers."""
    # create a regular file (not a symlink) that looks like a live link
    fake_link = _live_link_path(str(tmp_path), 'fake-worker')
    Path(fake_link).write_text('not a symlink')

    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    workers = await rec.discover_workers()
    assert len(workers) == 0
    await rec.stop()


# --- Rotation compatibility with new tables ---


async def test_rotation_recreates_all_tables(tmp_path):
    """After rotation, all configured tables exist in the new DB."""
    import asyncio

    config = make_debug_config(tmp_path, retention_hours=999, retention_max_events=100_000)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    await asyncio.sleep(1.1)
    await rec._rotate()

    assert await _table_exists(rec._db, 'events')
    assert await _table_exists(rec._db, 'worker_config')
    assert await _table_exists(rec._db, 'worker_state')
    await rec.stop()


async def test_rotation_auto_rewrites_config(tmp_path):
    """Rotation automatically re-writes worker_config to the new DB."""
    import asyncio

    config = make_debug_config(tmp_path, retention_hours=999, retention_max_events=100_000)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    await rec.write_config(_make_drakkar_config())
    old_path = rec.db_path

    await asyncio.sleep(1.1)
    await rec._rotate()
    new_path = rec.db_path
    assert old_path != new_path

    # config should be present without manual re-write
    async with rec._db.execute('SELECT worker_name FROM worker_config WHERE id = 1') as cur:
        row = await cur.fetchone()
    assert row is not None
    assert row[0] == WORKER_NAME
    await rec.stop()


async def test_rotation_state_sync_uses_new_db(tmp_path):
    """_sync_state after rotation writes to the new DB."""
    import asyncio

    config = make_debug_config(tmp_path, retention_hours=999, retention_max_events=100_000)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()
    rec.set_state_provider(lambda: {'uptime_seconds': 99.0})

    await asyncio.sleep(1.1)
    await rec._rotate()

    rec._counters['consumed'] = 42
    await rec._sync_state()

    async with rec._db.execute('SELECT consumed_count FROM worker_state ORDER BY id DESC LIMIT 1') as cur:
        row = await cur.fetchone()
    assert row[0] == 42
    await rec.stop()


async def test_rotation_respects_granular_flags(tmp_path):
    """Rotation with store_events=False still creates config/state tables."""
    import asyncio

    config = make_debug_config(
        tmp_path,
        store_events=False,
        store_config=True,
        store_state=True,
        retention_hours=999,
        retention_max_events=100_000,
    )
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    await asyncio.sleep(1.1)
    await rec._rotate()

    assert not await _table_exists(rec._db, 'events')
    assert await _table_exists(rec._db, 'worker_config')
    assert await _table_exists(rec._db, 'worker_state')
    await rec.stop()


# --- Stop flushes state one final time ---


async def test_stop_syncs_state_before_shutdown(tmp_path):
    """Graceful stop writes final state snapshot."""
    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()
    rec.set_state_provider(lambda: {'uptime_seconds': 77.0})
    rec._counters['consumed'] = 999

    db_path = rec.db_path
    await rec.stop()

    # verify by reopening the DB
    import aiosqlite

    async with (
        aiosqlite.connect(db_path) as db,
        db.execute('SELECT consumed_count, uptime_seconds FROM worker_state ORDER BY id DESC LIMIT 1') as cur,
    ):
        row = await cur.fetchone()
    assert row is not None
    assert row[0] == 999
    assert row[1] == 77.0


async def test_stop_removes_live_link(tmp_path):
    """Graceful stop removes the -live.db symlink."""
    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    link = _live_link_path(str(tmp_path), WORKER_NAME)
    assert os.path.islink(link)

    await rec.stop()
    assert not os.path.exists(link)
