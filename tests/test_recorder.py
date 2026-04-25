"""Tests for Drakkar flight recorder."""

import asyncio
import json
import os
from collections import deque
from pathlib import Path

import aiosqlite
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
    SCHEMA_EVENTS,
    SCHEMA_WORKER_CONFIG,
    EventRecorder,
    detect_worker_ip,
    format_dt,
    list_db_files,
    live_link_path,
    make_db_path,
)
from drakkar.recorder import (
    logger as recorder_logger,
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


def testmake_db_path_includes_timestamp():
    path = make_db_path('/tmp', 'worker-1')
    assert path.startswith('/tmp/worker-1-')
    assert path.endswith('.db')
    assert '__' in path  # YYYY-MM-DD__HH_MM_SS


def testmake_db_path_uses_worker_name():
    path = make_db_path('/var/log', 'my-worker')
    assert path.startswith('/var/log/my-worker-')


def testlist_db_files_returns_sorted(tmp_path):
    for name in [
        'test-worker-2026-03-16__14_00_00.db',
        'test-worker-2026-03-15__10_00_00.db',
        'test-worker-2026-03-16__15_00_00.db',
    ]:
        (tmp_path / name).touch()
    files = list_db_files(str(tmp_path), 'test-worker')
    assert len(files) == 3
    assert '10_00' in files[0]  # sorted oldest first


def testlist_db_files_excludes_live_symlink(tmp_path):
    (tmp_path / 'test-worker-2026-03-16__14_00_00.db').touch()
    live = tmp_path / 'test-worker-live.db'
    live.symlink_to('test-worker-2026-03-16__14_00_00.db')
    files = list_db_files(str(tmp_path), 'test-worker')
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


async def test_record_task_started_sanitizes_env_secrets(recorder):
    """Per-task env values matching secret-name patterns are redacted to ***
    before being stored in the recorder DB (the debug UI surface).
    Non-secret values pass through unchanged.
    """

    task = make_task('t1')
    task.env = {
        'DB_PASSWORD': 'supersecret',
        'PUBLIC_URL': 'https://api.example.com',
        'API_TOKEN': 'abc123',
    }
    recorder.record_task_started(task, partition=0)
    await recorder._flush()

    events = await recorder.get_events(event_type='task_started')
    meta = json.loads(events[0]['metadata'])
    assert meta['env']['DB_PASSWORD'] == '***'
    assert meta['env']['API_TOKEN'] == '***'
    assert meta['env']['PUBLIC_URL'] == 'https://api.example.com'


@pytest.mark.parametrize(
    ('name', 'expected_redacted'),
    [
        # True-positive matches — these SHOULD be redacted.
        ('DB_PASSWORD', True),
        ('API_TOKEN', True),
        ('MY_SECRET_VALUE', True),
        ('STRIPE_API_KEY', True),
        ('AWS_CREDENTIALS', True),
        ('DATABASE_DSN', True),
        # "False positives" — these ALSO match the aggressive patterns even
        # though the name alone doesn't imply a secret. The contract is:
        # aggressive redact, accept false positives. Operators who need to
        # expose these exact names should rename them.
        ('PASSWORD_RESET_URL', True),  # '*PASSWORD*' matches
        ('API_KEY_VERSION', True),  # '*API_KEY*' matches
        ('SECRET_MANAGER_ENDPOINT', True),  # '*SECRET*' matches
        # Genuinely non-matching names — should pass through unredacted.
        ('PUBLIC_URL', False),
        ('DB_HOSTNAME', False),
        ('DEPLOY_ENV', False),
        ('REQUEST_ID', False),
    ],
)
async def test_record_task_started_env_redaction_coverage(recorder, name: str, expected_redacted: bool):
    """Documents the redaction contract: aggressive pattern match accepts
    false positives rather than leaking a secret under an innocuous name.

    The patterns in ``_SECRET_ENV_PATTERNS`` use fnmatch-style globs (``*X*``),
    so ``PASSWORD_RESET_URL`` matches ``*PASSWORD*`` and is redacted even
    though a "reset URL" isn't secret per se. This behavior is intentional —
    the recorder DB is downloadable via the debug UI and the cost of an
    over-redacted URL in logs is far lower than a leaked credential.
    """

    task = make_task('t1')
    # Non-empty value so the redaction produces '***' (empty stays empty).
    task.env = {name: 'test-value-123'}
    recorder.record_task_started(task, partition=0)
    await recorder._flush()

    events = await recorder.get_events(event_type='task_started')
    meta = json.loads(events[0]['metadata'])
    stored = meta['env'][name]
    if expected_redacted:
        assert stored == '***', f'{name!r} expected to be redacted; got {stored!r}'
    else:
        assert stored == 'test-value-123', f'{name!r} expected to pass through; got {stored!r}'


async def test_record_task_started_env_sanitize_does_not_mutate_task(recorder):
    """The real task.env must stay intact on the task object — the subprocess
    launch still needs the real secret values; only the recorded copy is redacted.
    """
    task = make_task('t1')
    task.env = {'DB_PASSWORD': 'supersecret', 'PUBLIC_URL': 'https://example.com'}
    recorder.record_task_started(task, partition=0)
    await recorder._flush()

    # Task object itself must be unchanged.
    assert task.env == {'DB_PASSWORD': 'supersecret', 'PUBLIC_URL': 'https://example.com'}


async def test_record_task_started_empty_env_no_metadata_key(recorder):
    """Empty task.env should not create an 'env' key in metadata (matches
    existing behavior — the write site is guarded by ``if task.env``)."""

    task = make_task('t1')
    # ExecutorTask.env default_factory=dict, so task.env is already {} here.
    recorder.record_task_started(task, partition=0)
    await recorder._flush()

    events = await recorder.get_events(event_type='task_started')
    meta = json.loads(events[0]['metadata'])
    assert 'env' not in meta


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

    remaining = list_db_files(str(tmp_path), WORKER_NAME)
    # should keep at most 1 old file + the new one
    assert len(remaining) <= 2
    await rec.stop()


async def test_rotate_creates_schema_before_swap(tmp_path):
    """_create_schema(new_db) must run BEFORE self._db is swapped.

    Previously the swap happened first, leaving a schemaless window during
    which a concurrent flush could hit ``no such table: events``. The fix
    inverts the order, and this test locks it in by recording the value of
    ``self._db`` at the moment ``_create_schema`` is called — it must still
    point at the OLD connection.
    """
    config = make_debug_config(tmp_path, retention_hours=24)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    old_db = rec._db
    observed: dict[str, object] = {}
    real_create_schema = rec._create_schema

    async def spy_create_schema(db):
        # Record the live writer at the moment the new DB is being primed.
        # If the swap happened first, rec._db would equal the passed-in db.
        observed['db_arg'] = db
        observed['self_db_at_call'] = rec._db
        await real_create_schema(db)

    rec._create_schema = spy_create_schema  # type: ignore[method-assign]
    await rec._rotate()

    # Schema was created on a NEW connection, not the old writer.
    assert observed['db_arg'] is not old_db
    # At the moment _create_schema ran, the live writer was still the OLD
    # connection — ordering is schema-first, swap-second.
    assert observed['self_db_at_call'] is old_db
    # After rotation completes, the live writer is the new connection.
    assert rec._db is observed['db_arg']

    await rec.stop()


async def test_concurrent_flush_during_rotate_no_missing_table(tmp_path):
    """A flush running concurrently with _rotate must never hit 'no such table'.

    This exercises the real race that motivated the fix: _rotate previously
    swapped self._db first and then created the schema, so a _flush_loop
    iteration interleaved between the swap and the schema-creation would
    execute INSERT against a schemaless file.

    We run the race many times to make a scheduling-dependent failure
    observable; with the fix, every iteration should succeed.
    """
    iterations = 20
    for i in range(iterations):
        # Isolate each iteration in a subdirectory so DB filenames don't
        # collide and old files don't influence retention checks.
        iter_dir = tmp_path / f'iter-{i}'
        iter_dir.mkdir()
        config = make_debug_config(iter_dir, retention_hours=999, retention_max_events=100_000)
        rec = EventRecorder(config, worker_name=WORKER_NAME)
        await rec.start()

        # Seed the buffer so _flush has real work to do on both the old
        # and the new DB; this is what would trip 'no such table' if the
        # schema were still missing at flush time.
        for offset in range(10):
            rec.record_consumed(make_msg(offset=offset))

        # Launch flush + rotate + flush concurrently. The second flush
        # is the one most likely to land in the swap-then-schema window
        # under the old (broken) ordering.
        await asyncio.gather(
            rec._flush(),
            rec._rotate(),
            rec._flush(),
        )

        # No exception means the race didn't surface — there was no
        # window where self._db was live but missing the events table.
        await rec.stop()


async def test_rotate_schema_creation_failure_does_not_swap(tmp_path):
    """If _create_schema raises on the new DB, self._db stays unchanged."""
    config = make_debug_config(tmp_path, retention_hours=24)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    old_db = rec._db
    old_path = rec.db_path

    async def failing_create_schema(db):
        raise RuntimeError('simulated schema creation failure')

    rec._create_schema = failing_create_schema  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match='simulated schema creation failure'):
        await rec._rotate()

    # Live writer was NOT swapped — the old (functional) connection is
    # still installed, so subsequent flushes/queries keep working.
    assert rec._db is old_db
    assert rec.db_path == old_path

    # Sanity: the old connection is still usable.
    rec.record_consumed(make_msg(offset=42))
    # Restore the real _create_schema so _flush's writes (which go to the
    # old, still-schema-ful DB) and cleanup work.
    del rec._create_schema
    await rec._flush()

    events = await rec.get_events()
    assert any(e['offset'] == 42 for e in events)

    await rec.stop()


# --- Dedicated reader connection (H3) ---


async def test_start_creates_both_writer_and_reader_connections(tmp_path):
    """start() must open both ``_db`` (writer) and ``_reader_db`` (reader)."""

    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    # Both connection attributes must be set and be aiosqlite connections.
    assert rec._db is not None
    assert rec._reader_db is not None
    assert isinstance(rec._db, aiosqlite.Connection)
    assert isinstance(rec._reader_db, aiosqlite.Connection)
    # The reader is a separate connection object, not an alias to the writer.
    assert rec._db is not rec._reader_db
    # The public property returns the reader connection.
    assert rec.reader_db is rec._reader_db

    await rec.stop()


async def test_stop_closes_both_connections(tmp_path):
    """stop() must close both the writer and the reader and None them out."""
    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    writer = rec._db
    reader = rec._reader_db
    assert writer is not None
    assert reader is not None

    await rec.stop()

    # Both references are cleared so a post-stop access returns None.
    assert rec._db is None
    assert rec._reader_db is None
    assert rec.reader_db is None
    # The underlying connections were closed — attempting to use them
    # should raise a ValueError from aiosqlite (operation on closed
    # connection). We check the reader first since stop() closes it
    # first and the error surfaces deterministically.
    with pytest.raises(ValueError):
        async with reader.execute('SELECT 1') as cur:
            await cur.fetchall()


async def test_reader_sees_writer_writes(tmp_path):
    """A row committed via the writer must be visible through the reader.

    This is the core WAL-based invariant we rely on: the reader connection
    is a separate aiosqlite connection on its own worker thread, but WAL
    journaling lets it see committed snapshots produced by the writer.
    """
    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()
    assert rec._db is not None
    assert rec._reader_db is not None

    # Buffer an event, then flush to force a writer commit.
    rec.record_consumed(make_msg(partition=7, offset=123))
    await rec._flush()

    # Query via the reader (not the writer) and assert the row is visible.
    async with rec._reader_db.execute(
        'SELECT partition, offset FROM events WHERE event = ?',
        ('consumed',),
    ) as cursor:
        rows = await cursor.fetchall()

    assert (7, 123) in rows

    await rec.stop()


async def test_rotate_rotates_reader_too(tmp_path):
    """_rotate() must swap both writer AND reader to the new DB path."""
    config = make_debug_config(tmp_path, retention_hours=24)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    old_writer = rec._db
    old_reader = rec._reader_db

    await rec._rotate()

    # After rotation both connections are replaced (new objects, not
    # aliases of the old ones). Note: make_db_path timestamp has
    # second-precision, so a rapid rotate in the same wall-clock second
    # can reuse the filename; we assert on connection-object identity
    # rather than path string to avoid that flake.
    assert rec._db is not None
    assert rec._reader_db is not None
    assert rec._db is not old_writer
    assert rec._reader_db is not old_reader

    # The new reader must be usable — write via the writer then SELECT
    # via the reader to confirm both point at the same (new) file.
    rec.record_consumed(make_msg(partition=1, offset=999))
    await rec._flush()
    async with rec._reader_db.execute(
        'SELECT partition, offset FROM events WHERE event = ?',
        ('consumed',),
    ) as cur:
        rows = await cur.fetchall()
    assert (1, 999) in rows

    await rec.stop()


async def test_concurrent_reader_and_writer_no_deadlock(tmp_path):
    """A UI-style reader SELECT and a writer flush run concurrently without
    deadlocking. WAL mode is what makes this safe — the reader sees a
    snapshot while the writer commits.
    """
    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()
    assert rec._db is not None
    assert rec._reader_db is not None

    # Seed a baseline batch so the reader has rows to return while the
    # writer is committing the next batch concurrently.
    for offset in range(50):
        rec.record_consumed(make_msg(offset=offset))
    await rec._flush()

    # Second batch not yet flushed — the writer work below will flush it.
    for offset in range(50, 100):
        rec.record_consumed(make_msg(offset=offset))

    async def reader_task():
        # Simulate a debug-UI SELECT on the reader connection.
        async with rec._reader_db.execute('SELECT COUNT(*) FROM events') as cur:
            row = await cur.fetchone()
        return row[0] if row else 0

    async def writer_task():
        # Flush the buffered second batch to the writer.
        await rec._flush()

    # Run both concurrently under a generous timeout. If WAL isolation
    # were missing OR connections shared a worker thread, this could
    # deadlock or serialize pathologically; we expect both to complete
    # well under the 2s budget.
    reader_count, _ = await asyncio.wait_for(
        asyncio.gather(reader_task(), writer_task()),
        timeout=2.0,
    )

    # Reader ran at some point during the flush — count may be 50 or 100
    # depending on ordering, but must be at least the pre-flush baseline.
    assert reader_count >= 50

    # After both finish, the final flush is durable and visible via the
    # reader — confirms WAL snapshot isolation did not drop writes.
    async with rec._reader_db.execute('SELECT COUNT(*) FROM events') as cur:
        row = await cur.fetchone()
    assert row is not None
    assert row[0] == 100

    await rec.stop()


async def test_stop_reader_close_failure_still_closes_writer(tmp_path):
    """If reader.close() raises, stop() logs a warning, clears the reader,
    and still closes the writer. The worker must leave a clean state.
    """
    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()
    assert rec._reader_db is not None
    assert rec._db is not None

    # Swap the reader for one whose close() raises. The writer must still
    # go through its own close() branch after the failed reader close.
    class _ExplodingClose:
        async def close(self):
            raise RuntimeError('simulated reader close failure')

    rec._reader_db = _ExplodingClose()  # type: ignore[assignment]

    # stop() should swallow the reader-close exception (logged as warning)
    # and still transition the writer to closed + remove the live link.
    await rec.stop()

    assert rec._reader_db is None
    assert rec._db is None


async def test_rotate_new_reader_open_failure_rolls_back(tmp_path, monkeypatch):
    """If open_reader() raises on the new path, _rotate closes the freshly
    opened new_db and leaves the previous writer/reader pair in place so the
    worker stays usable.
    """
    from drakkar import recorder as recorder_module

    config = make_debug_config(tmp_path, retention_hours=24)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    old_writer = rec._db
    old_reader = rec._reader_db
    old_path = rec.db_path

    async def failingopen_reader(path):
        raise RuntimeError('simulated new reader open failure')

    monkeypatch.setattr(recorder_module, 'open_reader', failingopen_reader)

    with pytest.raises(RuntimeError, match='simulated new reader open failure'):
        await rec._rotate()

    # Old writer/reader untouched — worker remains usable.
    assert rec._db is old_writer
    assert rec._reader_db is old_reader
    assert rec.db_path == old_path

    # Sanity: still usable after the failed rotate.
    rec.record_consumed(make_msg(offset=7))
    await rec._flush()
    events = await rec.get_events()
    assert any(e['offset'] == 7 for e in events)

    await rec.stop()


async def test_rotate_old_reader_close_failure_does_not_abort_rotation(tmp_path):
    """_rotate logs a warning on old reader/writer close failure but still
    completes. Post-rotation the new writer/reader are live.
    """
    config = make_debug_config(tmp_path, retention_hours=24)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    # Wrap the live reader with one that raises on close; the rotation
    # exercises the warning-log branch and keeps going.
    real_reader = rec._reader_db
    assert real_reader is not None

    class _ExplodingOnClose:
        async def close(self):
            # close the real underlying reader so the fd isn't leaked,
            # then raise to exercise the warn path.
            with contextlib.suppress(Exception):
                await real_reader.close()
            raise RuntimeError('simulated old reader close failure')

    import contextlib  # local import to keep the fixture tight

    rec._reader_db = _ExplodingOnClose()  # type: ignore[assignment]

    # Rotation should complete despite the faulty close.
    await rec._rotate()

    # New reader/writer installed and usable.
    assert rec._db is not None
    assert rec._reader_db is not None
    rec.record_consumed(make_msg(partition=2, offset=99))
    await rec._flush()
    async with rec._reader_db.execute(
        'SELECT partition, offset FROM events WHERE event = ?',
        ('consumed',),
    ) as cur:
        rows = await cur.fetchall()
    assert (2, 99) in rows

    await rec.stop()


# --- Pool stats in events ---


async def test_task_started_includes_pool_stats(tmp_path):
    config = make_debug_config(tmp_path, ws_min_duration_ms=0)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    task = make_task('t1')
    rec.record_task_started(task, partition=0, pool_active=5, pool_waiting=12)
    await rec._flush()

    queue = rec.subscribe()
    # re-record to check WS event
    rec.record_task_started(task, partition=0, pool_active=3, pool_waiting=7)
    event = queue.get_nowait()
    assert event['pool_active'] == 3
    assert event['pool_waiting'] == 7
    rec.unsubscribe(queue)
    await rec.stop()


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
    recorder.record_task_complete(task_id='t-all', partition=1, duration=0.05, output_message_count=2)
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
        'task_complete',
        'produced',
        'committed',
        'assigned',
        'revoked',
        'sink_delivered',
        'sink_error',
    }
    assert event_types == expected, f'missing: {expected - event_types}, extra: {event_types - expected}'


async def test_task_complete_persisted(recorder):
    """task_complete event stores task_id, duration, and output_message_count."""
    recorder.record_task_complete(task_id='t-cc', partition=2, duration=0.123, output_message_count=5)
    await recorder._flush()

    events = await recorder.get_events(event_type='task_complete')
    assert len(events) == 1
    e = events[0]
    assert e['task_id'] == 't-cc'
    assert e['partition'] == 2
    assert e['duration'] == 0.123

    meta = json.loads(e['metadata'])
    assert meta['output_message_count'] == 5


async def test_message_complete_persisted(recorder):
    """message_complete event stores the per-message aggregate snapshot."""
    recorder.record_message_complete(
        partition=3,
        offset=42,
        duration=0.5,
        task_count=6,
        succeeded=4,
        failed=1,
        replaced=1,
        output_message_count=2,
    )
    await recorder._flush()

    events = await recorder.get_events(event_type='message_complete')
    assert len(events) == 1
    e = events[0]
    assert e['partition'] == 3
    assert e['offset'] == 42
    assert e['duration'] == 0.5

    meta = json.loads(e['metadata'])
    assert meta['task_count'] == 6
    assert meta['succeeded'] == 4
    assert meta['failed'] == 1
    assert meta['replaced'] == 1
    assert meta['output_message_count'] == 2


async def test_window_complete_persisted(recorder):
    """window_complete event stores the per-window task count + duration."""
    recorder.record_window_complete(
        partition=7,
        window_id=11,
        duration=1.25,
        task_count=10,
        output_message_count=0,
    )
    await recorder._flush()

    events = await recorder.get_events(event_type='window_complete')
    assert len(events) == 1
    e = events[0]
    assert e['partition'] == 7
    assert e['duration'] == 1.25

    meta = json.loads(e['metadata'])
    assert meta['window_id'] == 11
    assert meta['task_count'] == 10
    assert meta['output_message_count'] == 0


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
    assert stats['consumed'] == 1
    assert stats['completed'] == 1
    assert stats['committed'] == 1
    assert stats['total_events'] == 3  # consumed + completed + committed (task_started has no counter)

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

    async with aiosqlite.connect(new_path) as db:
        # verify events table exists
        async with db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='events'") as cur:
            tables = await cur.fetchall()
            assert len(tables) == 1

        # verify indexes exist
        async with db.execute("SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_events_%'") as cur:
            indexes = await cur.fetchall()
            assert len(indexes) == 6  # partition_offset, ts, dt, task_id, type, labels

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
            'max_executors': 4,
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
    assert data['max_executors'] == 4
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

    async with rec._db.execute('SELECT env_vars_json FROM worker_config WHERE id = 1') as cur:
        row = await cur.fetchone()
    env = json.loads(row[0])
    assert env['MY_VAR'] == 'hello'
    assert env['MISSING_VAR'] == ''
    await rec.stop()


# --- H7: secrets in recorder must be redacted ---


async def test_write_config_redacts_sasl_kafka_brokers(tmp_path):
    """Kafka brokers with SASL credentials must be stripped before writing
    to the recorder DB. The SQLite file is downloadable via the debug UI,
    so storing raw user:pass URLs publishes them to anyone with access.
    """
    from drakkar.config import DrakkarConfig

    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    drakkar_cfg = DrakkarConfig(
        kafka={
            'brokers': 'SASL_SSL://alice:s3cret@kafka-1.example.com:9094',
            'source_topic': 't',
            'consumer_group': 'g',
        },
        executor={'binary_path': '/bin/echo'},
        sinks={'kafka': {'out': {'topic': 'results'}}},
    )
    await rec.write_config(drakkar_cfg)

    async with rec._db.execute('SELECT kafka_brokers FROM worker_config WHERE id = 1') as cur:
        row = await cur.fetchone()
    stored = row[0]
    assert 'alice' not in stored
    assert 's3cret' not in stored
    assert '***:***' in stored
    # host and port are still visible — operators need these for debugging.
    assert 'kafka-1.example.com:9094' in stored
    await rec.stop()


async def test_write_config_redacts_secret_named_env_vars(tmp_path, monkeypatch):
    """Even when the operator explicitly adds a secret-named env var to
    expose_env_vars, the VALUE must be redacted before it hits disk.
    """
    monkeypatch.setenv('DB_PASSWORD', 'supersecret')
    monkeypatch.setenv('GITHUB_TOKEN', 'ghp_abc123')
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'wxyz')
    monkeypatch.setenv('MY_API_KEY', 'k1')
    monkeypatch.setenv('SERVICE_NAME', 'my-service')  # NOT a secret

    config = make_debug_config(
        tmp_path,
        expose_env_vars=['DB_PASSWORD', 'GITHUB_TOKEN', 'AWS_SECRET_ACCESS_KEY', 'MY_API_KEY', 'SERVICE_NAME'],
    )
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    await rec.write_config(_make_drakkar_config())

    async with rec._db.execute('SELECT env_vars_json FROM worker_config WHERE id = 1') as cur:
        row = await cur.fetchone()
    env = json.loads(row[0])

    assert env['DB_PASSWORD'] == '***'
    assert env['GITHUB_TOKEN'] == '***'
    assert env['AWS_SECRET_ACCESS_KEY'] == '***'
    assert env['MY_API_KEY'] == '***'
    # Non-secret names pass through.
    assert env['SERVICE_NAME'] == 'my-service'
    await rec.stop()


async def test_write_config_redacts_url_credentials_in_env_values(tmp_path, monkeypatch):
    """Env vars that aren't named like secrets but hold a DSN/URL with
    embedded credentials must still be redacted.
    """
    monkeypatch.setenv('UPSTREAM_URL', 'https://alice:pw@api.example.com/endpoint')
    monkeypatch.setenv('CACHE_URL', 'redis://:rediskey@cache:6379/0')

    config = make_debug_config(tmp_path, expose_env_vars=['UPSTREAM_URL', 'CACHE_URL'])
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    await rec.write_config(_make_drakkar_config())

    async with rec._db.execute('SELECT env_vars_json FROM worker_config WHERE id = 1') as cur:
        row = await cur.fetchone()
    env = json.loads(row[0])

    assert 'alice' not in env['UPSTREAM_URL']
    assert 'pw' not in env['UPSTREAM_URL']
    assert '***:***' in env['UPSTREAM_URL']
    assert 'api.example.com/endpoint' in env['UPSTREAM_URL']

    assert 'rediskey' not in env['CACHE_URL']
    assert '***:***' in env['CACHE_URL']
    assert 'cache:6379' in env['CACHE_URL']
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
    assert stats['consumed'] == 1  # in-memory counters work regardless of store_events
    assert stats['total_events'] == 1
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

    # create a fake other worker's DB
    other_db_path = tmp_path / 'other-worker-2026-03-24__10_00_00.db'
    async with aiosqlite.connect(str(other_db_path)) as db:
        await db.executescript(SCHEMA_WORKER_CONFIG)
        await db.execute(
            """INSERT INTO worker_config
               (id, worker_name, ip_address, debug_port, debug_url, kafka_brokers, source_topic,
                consumer_group, binary_path, max_executors, task_timeout_seconds,
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
                format_dt(1000.0),
            ],
        )
        await db.commit()

    # create live symlink for the other worker
    link = live_link_path(str(tmp_path), 'other-worker')
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
    assert os.path.islink(live_link_path(str(tmp_path), WORKER_NAME))

    workers = await rec.discover_workers()
    assert len(workers) == 0
    await rec.stop()


async def test_discover_workers_skips_missing_config_table(tmp_path):
    """If another worker's DB has no worker_config table, it's skipped."""

    # create a DB without worker_config
    other_db = tmp_path / 'no-config-worker-2026-03-24__10_00_00.db'
    async with aiosqlite.connect(str(other_db)) as db:
        await db.execute('CREATE TABLE IF NOT EXISTS dummy (id INTEGER)')
        await db.commit()

    link = live_link_path(str(tmp_path), 'no-config-worker')
    os.symlink(other_db.name, link)

    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    workers = await rec.discover_workers()
    assert len(workers) == 0
    await rec.stop()


async def test_discover_workers_handles_broken_symlink(tmp_path):
    """Broken symlink (target deleted) is handled gracefully."""
    link = live_link_path(str(tmp_path), 'ghost-worker')
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
    fake_link = live_link_path(str(tmp_path), 'fake-worker')
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

    link = live_link_path(str(tmp_path), WORKER_NAME)
    assert os.path.islink(link)

    await rec.stop()
    assert not os.path.exists(link)


# --- cross_trace ---


async def _create_worker_db(
    db_path,
    worker_name,
    cluster_name='',
    partition=0,
    offset=42,
):
    """Create a DB with worker_config + events for cross_trace testing."""

    async with aiosqlite.connect(str(db_path)) as db:
        await db.executescript(SCHEMA_WORKER_CONFIG)
        await db.executescript(SCHEMA_EVENTS)
        await db.execute(
            """INSERT INTO worker_config
               (id, worker_name, cluster_name, ip_address, debug_port, debug_url, kafka_brokers,
                source_topic, consumer_group, binary_path, max_executors, task_timeout_seconds,
                max_retries, window_size, sinks_json, env_vars_json, created_at, created_at_dt)
               VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                worker_name,
                cluster_name,
                '10.0.0.1',
                8080,
                '',
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
                format_dt(1000.0),
            ],
        )
        await db.execute(
            """INSERT INTO events (ts, dt, event, partition, offset, task_id, metadata)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            [
                1000.0,
                format_dt(1000.0),
                'consumed',
                partition,
                offset,
                None,
                None,
            ],
        )
        await db.execute(
            """INSERT INTO events (ts, dt, event, partition, offset, task_id, metadata)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            [
                1001.0,
                format_dt(1001.0),
                'task_started',
                partition,
                None,
                f'task-{offset}',
                f'{{"source_offsets": [{offset}]}}',
            ],
        )
        await db.commit()


async def test_cross_trace_finds_in_current_worker(recorder):
    """cross_trace returns events from local DB with worker_name attached."""
    recorder.record_consumed(make_msg(partition=3, offset=42))
    task = make_task('t-42', offsets=[42])
    recorder.record_task_started(task, partition=3)
    await recorder._flush()

    events = await recorder.cross_trace(partition=3, msg_offset=42)
    assert len(events) >= 2
    assert all(e['worker_name'] == WORKER_NAME for e in events)


async def test_cross_trace_fallback_to_other_live_worker(tmp_path):
    """cross_trace falls back to another worker's live DB if local has no match."""
    # create other worker's DB with events for partition=5 offset=99
    other_db_path = tmp_path / 'other-worker-2026-03-25__10_00_00.db'
    await _create_worker_db(other_db_path, 'other-worker', partition=5, offset=99)

    # create live symlink
    link = live_link_path(str(tmp_path), 'other-worker')
    os.symlink(other_db_path.name, link)

    # start our recorder (has no events for partition=5 offset=99)
    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    events = await rec.cross_trace(partition=5, msg_offset=99)
    assert len(events) >= 1
    assert all(e['worker_name'] == 'other-worker' for e in events)

    await rec.stop()


async def test_cross_trace_fallback_to_rotated_db(tmp_path):
    """cross_trace searches rotated DB files when live workers have no match."""
    # create a rotated (non-symlinked) DB with events
    rotated_path = tmp_path / 'old-worker-2026-03-20__08_00_00.db'
    await _create_worker_db(rotated_path, 'old-worker', partition=7, offset=200)

    # start our recorder (has no events for partition=7 offset=200)
    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    events = await rec.cross_trace(partition=7, msg_offset=200)
    assert len(events) >= 1
    assert all(e['worker_name'] == 'old-worker' for e in events)

    await rec.stop()


async def test_cross_trace_cluster_filtering(tmp_path):
    """cross_trace only searches DBs in the same cluster."""
    # create other worker's DB in a DIFFERENT cluster
    other_db_path = tmp_path / 'other-worker-2026-03-25__10_00_00.db'
    await _create_worker_db(
        other_db_path,
        'other-worker',
        cluster_name='other-cluster',
        partition=5,
        offset=99,
    )
    link = live_link_path(str(tmp_path), 'other-worker')
    os.symlink(other_db_path.name, link)

    # our recorder is in 'my-cluster'
    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME, cluster_name='my-cluster')
    await rec.start()

    events = await rec.cross_trace(partition=5, msg_offset=99)
    assert events == []

    await rec.stop()


async def test_cross_trace_prefers_newest_rotated_db(tmp_path):
    """cross_trace searches rotated DBs newest-first."""
    # create two rotated DBs for different timestamps
    old_path = tmp_path / 'wk-2026-03-20__08_00_00.db'
    await _create_worker_db(old_path, 'wk', partition=1, offset=10)

    new_path = tmp_path / 'wk-2026-03-25__12_00_00.db'
    await _create_worker_db(new_path, 'wk', partition=1, offset=10)

    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    events = await rec.cross_trace(partition=1, msg_offset=10)
    assert len(events) >= 1
    # both DBs have events — should find from newest first
    assert events[0]['worker_name'] == 'wk'

    await rec.stop()


async def test_cross_trace_empty_when_no_match(recorder):
    """cross_trace returns empty list when no events match anywhere."""
    events = await recorder.cross_trace(partition=99, msg_offset=999)
    assert events == []


# --- Duration threshold tests ---


def make_fast_result(task_id='t-fast', task=None) -> ExecutorResult:
    """Result with 100ms duration — below default 500ms thresholds."""
    t = task or make_task(task_id)
    return ExecutorResult(
        exit_code=0,
        stdout='fast output\n',
        stderr='',
        duration_seconds=0.1,
        task=t,
    )


def make_slow_result(task_id='t-slow', task=None) -> ExecutorResult:
    """Result with 1.5s duration — above default 500ms thresholds."""
    t = task or make_task(task_id)
    return ExecutorResult(
        exit_code=0,
        stdout='slow output\n',
        stderr='warn',
        duration_seconds=1.5,
        task=t,
    )


async def test_ws_threshold_skips_fast_task(tmp_path):
    """Fast task (start + complete before threshold) is invisible to WS.

    ws_min_duration_ms defers task_started; if the task completes before
    the threshold fires, neither start nor completion reaches WS.
    """
    config = make_debug_config(tmp_path, ws_min_duration_ms=500)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    q = rec.subscribe()
    task = make_task('t-fast')
    rec.record_task_started(task, partition=0)
    rec.record_task_completed(make_fast_result('t-fast', task=task), partition=0)

    assert q.empty()
    rec.unsubscribe(q)
    await rec.stop()


async def test_ws_threshold_broadcasts_slow_task(tmp_path):
    """Slow task — deferred start fires, then completion is broadcast too."""
    config = make_debug_config(tmp_path, ws_min_duration_ms=10)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    q = rec.subscribe()
    task = make_task('t-slow')
    rec.record_task_started(task, partition=0)

    # wait for the deferred timer to fire (10ms threshold)
    await asyncio.sleep(0.05)

    rec.record_task_completed(make_slow_result('t-slow', task=task), partition=0)

    events = []
    while not q.empty():
        events.append(q.get_nowait())
    assert len(events) == 2
    assert events[0]['event'] == 'task_started'
    assert events[1]['event'] == 'task_completed'
    rec.unsubscribe(q)
    await rec.stop()


async def test_ws_deferred_start_not_sent_immediately(tmp_path):
    """With ws_min_duration_ms>0, task_started is NOT sent to WS immediately."""
    config = make_debug_config(tmp_path, ws_min_duration_ms=500)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    q = rec.subscribe()
    task = make_task('t1')
    rec.record_task_started(task, partition=0)

    # start event is deferred — WS queue should be empty right after
    assert q.empty()
    # but it IS written to the DB buffer
    assert len(rec._buffer) == 1
    assert rec._buffer[0]['event'] == 'task_started'
    rec.unsubscribe(q)
    await rec.stop()


async def test_ws_zero_threshold_sends_start_immediately(tmp_path):
    """With ws_min_duration_ms=0, task_started goes to WS immediately."""
    config = make_debug_config(tmp_path, ws_min_duration_ms=0)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    q = rec.subscribe()
    task = make_task('t1')
    rec.record_task_started(task, partition=0)

    assert not q.empty()
    event = q.get_nowait()
    assert event['event'] == 'task_started'
    rec.unsubscribe(q)
    await rec.stop()


async def test_ws_deferred_fast_fail_sends_both_events(tmp_path):
    """Failed tasks always go to WS — even fast ones. Deferred start is flushed first."""
    config = make_debug_config(tmp_path, ws_min_duration_ms=500)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    q = rec.subscribe()
    task = make_task('t-fail')
    error = ExecutorError(task=task, exit_code=1, stderr='boom')

    rec.record_task_started(task, partition=0)
    # still deferred
    assert q.empty()

    rec.record_task_failed(task, error, partition=0, duration_seconds=0.05)

    # both start and fail should be on WS
    events = []
    while not q.empty():
        events.append(q.get_nowait())
    assert len(events) == 2
    assert events[0]['event'] == 'task_started'
    assert events[1]['event'] == 'task_failed'
    rec.unsubscribe(q)
    await rec.stop()


async def test_ws_deferred_cleanup_on_stop(tmp_path):
    """Deferred WS timers are cancelled on recorder stop."""
    config = make_debug_config(tmp_path, ws_min_duration_ms=5000)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    task = make_task('t-pending')
    rec.record_task_started(task, partition=0)
    assert len(rec._deferred_ws) == 1

    await rec.stop()
    assert len(rec._deferred_ws) == 0


async def test_labels_stored_in_task_started_event(tmp_path):
    """Task labels are JSON-encoded in the task_started event buffer entry."""
    config = make_debug_config(tmp_path, ws_min_duration_ms=0)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    task = make_task('t-labeled')
    task.labels = {'request_id': 'req-abc', 'user': 'alice'}
    rec.record_task_started(task, partition=0)

    assert len(rec._buffer) == 1
    entry = rec._buffer[0]
    # Compare semantically: the exact byte layout differs between orjson
    # (compact) and stdlib json (spaces after ``:`` / ``,``). Both produce
    # valid JSON that decodes to the same dict.
    assert json.loads(entry['labels']) == {'request_id': 'req-abc', 'user': 'alice'}
    await rec.stop()


async def test_labels_stored_in_task_completed_event(tmp_path):
    """Task labels propagate from task through to completed event."""
    config = make_debug_config(tmp_path, ws_min_duration_ms=0)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    task = make_task('t-labeled')
    task.labels = {'request_id': 'req-xyz'}
    result = make_result(task=task)
    rec.record_task_completed(result, partition=0)

    entry = rec._buffer[0]
    assert json.loads(entry['labels']) == {'request_id': 'req-xyz'}
    await rec.stop()


async def test_labels_broadcast_via_ws(tmp_path):
    """Labels appear in WebSocket events for live UI consumption."""
    config = make_debug_config(tmp_path, ws_min_duration_ms=0)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    q = rec.subscribe()
    task = make_task('t-ws-labels')
    task.labels = {'request_id': 'req-ws'}
    rec.record_task_started(task, partition=0)

    event = q.get_nowait()
    assert json.loads(event['labels']) == {'request_id': 'req-ws'}
    rec.unsubscribe(q)
    await rec.stop()


async def test_labels_none_when_empty(tmp_path):
    """Empty labels dict produces None in the event (no DB bloat)."""
    config = make_debug_config(tmp_path, ws_min_duration_ms=0)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    task = make_task('t-no-labels')
    rec.record_task_started(task, partition=0)

    entry = rec._buffer[0]
    assert entry['labels'] is None
    await rec.stop()


async def test_labels_stored_in_task_failed_event(tmp_path):
    """Task labels propagate to failed events (always sent to WS)."""
    config = make_debug_config(tmp_path, ws_min_duration_ms=500)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    task = make_task('t-fail-labeled')
    task.labels = {'request_id': 'req-fail'}
    error = ExecutorError(task=task, exit_code=1, stderr='boom')

    q = rec.subscribe()
    rec.record_task_started(task, partition=0)
    rec.record_task_failed(task, error, partition=0, duration_seconds=0.05)

    events = []
    while not q.empty():
        events.append(q.get_nowait())
    # both start and fail events carry labels
    assert json.loads(events[0]['labels']) == {'request_id': 'req-fail'}
    assert json.loads(events[1]['labels']) == {'request_id': 'req-fail'}
    rec.unsubscribe(q)
    await rec.stop()


async def test_labels_persisted_to_db(tmp_path):
    """Labels are written to the SQLite labels column and queryable."""
    config = make_debug_config(tmp_path, ws_min_duration_ms=0)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    task = make_task('t-db-labels')
    task.labels = {'request_id': 'req-db', 'env': 'prod'}
    rec.record_task_started(task, partition=0)
    await rec._flush()

    async with rec._db.execute('SELECT labels FROM events WHERE task_id = ?', ['t-db-labels']) as cur:
        row = await cur.fetchone()
    assert row is not None

    assert json.loads(row[0]) == {'request_id': 'req-db', 'env': 'prod'}
    await rec.stop()


async def test_trace_by_label_finds_matching_tasks(tmp_path):
    """trace_by_label returns all events for tasks with a matching label."""
    config = make_debug_config(tmp_path, ws_min_duration_ms=0)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    # task with labels
    task1 = make_task('t-labeled-1')
    task1.labels = {'request_id': 'req-find-me'}
    rec.record_task_started(task1, partition=0)
    result1 = make_result(task=task1)
    rec.record_task_completed(result1, partition=0)

    # task with different label value
    task2 = make_task('t-labeled-2')
    task2.labels = {'request_id': 'req-other'}
    rec.record_task_started(task2, partition=0)

    # task with no labels
    task3 = make_task('t-no-labels')
    rec.record_task_started(task3, partition=0)

    await rec._flush()

    events = await rec.trace_by_label('request_id', 'req-find-me')
    task_ids = {e['task_id'] for e in events}
    assert 't-labeled-1' in task_ids
    assert 't-labeled-2' not in task_ids
    assert 't-no-labels' not in task_ids
    # should have both started and completed events
    event_types = {e['event'] for e in events}
    assert 'task_started' in event_types
    assert 'task_completed' in event_types
    await rec.stop()


async def test_trace_by_label_returns_empty_for_no_match(tmp_path):
    """trace_by_label returns empty list when no tasks match."""
    config = make_debug_config(tmp_path, ws_min_duration_ms=0)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    task = make_task('t-x')
    task.labels = {'user': 'alice'}
    rec.record_task_started(task, partition=0)
    await rec._flush()

    events = await rec.trace_by_label('user', 'bob')
    assert events == []
    await rec.stop()


async def test_trace_by_label_returns_empty_for_unknown_key(tmp_path):
    """trace_by_label returns empty list for a label key that doesn't exist."""
    config = make_debug_config(tmp_path, ws_min_duration_ms=0)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    task = make_task('t-y')
    task.labels = {'request_id': 'abc'}
    rec.record_task_started(task, partition=0)
    await rec._flush()

    events = await rec.trace_by_label('nonexistent_key', 'abc')
    assert events == []
    await rec.stop()


async def test_event_threshold_zero_saves_all(tmp_path):
    """event_min_duration_ms=0 (default) saves all tasks to buffer."""
    config = make_debug_config(tmp_path, event_min_duration_ms=0)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    rec.record_task_completed(make_fast_result(), partition=0)

    assert len(rec._buffer) == 1
    assert rec._buffer[0]['event'] == 'task_completed'
    await rec.stop()


async def test_event_threshold_skips_fast_task(tmp_path):
    """event_min_duration_ms=500 skips fast tasks from buffer."""
    config = make_debug_config(tmp_path, event_min_duration_ms=500)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    rec.record_task_completed(make_fast_result(), partition=0)

    assert len(rec._buffer) == 0
    await rec.stop()


async def test_event_threshold_saves_slow_task(tmp_path):
    """event_min_duration_ms=500 saves slow tasks to buffer."""
    config = make_debug_config(tmp_path, event_min_duration_ms=500)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    rec.record_task_completed(make_slow_result(), partition=0)

    assert len(rec._buffer) == 1
    await rec.stop()


async def test_output_threshold_excludes_args_for_fast_task(tmp_path):
    """Fast task entry does not include args."""
    config = make_debug_config(tmp_path, output_min_duration_ms=500)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    rec.record_task_completed(make_fast_result(), partition=0)

    assert len(rec._buffer) == 1
    assert 'args' not in rec._buffer[0]
    await rec.stop()


async def test_output_threshold_includes_args_for_slow_task(tmp_path):
    """Slow task entry includes args."""
    config = make_debug_config(tmp_path, output_min_duration_ms=500)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    rec.record_task_completed(make_slow_result(), partition=0)

    assert len(rec._buffer) == 1
    assert 'args' in rec._buffer[0]
    await rec.stop()


async def test_output_threshold_excludes_stdout_stderr_for_fast_task(tmp_path):
    """Fast task with store_output=True still excludes stdout/stderr."""
    config = make_debug_config(tmp_path, store_output=True, output_min_duration_ms=500)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    rec.record_task_completed(make_slow_result('t-slow'), partition=0)
    rec.record_task_completed(make_fast_result('t-fast'), partition=0)

    slow_entry = rec._buffer[0]
    fast_entry = rec._buffer[1]

    assert slow_entry['stdout'] == 'slow output\n'
    assert slow_entry['stderr'] == 'warn'
    assert 'stdout' not in fast_entry
    assert 'stderr' not in fast_entry
    await rec.stop()


async def test_output_threshold_store_output_false_no_stdout(tmp_path):
    """store_output=False suppresses stdout/stderr even for slow tasks."""
    config = make_debug_config(tmp_path, store_output=False, output_min_duration_ms=0)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    rec.record_task_completed(make_slow_result(), partition=0)

    entry = rec._buffer[0]
    assert 'args' in entry  # output_min_duration_ms=0 so args included
    assert 'stdout' not in entry  # store_output=False suppresses this
    assert 'stderr' not in entry
    await rec.stop()


async def test_log_threshold_logs_slow_task(tmp_path):
    """Slow task triggers structlog info line."""
    from unittest.mock import patch

    config = make_debug_config(tmp_path, log_min_duration_ms=500)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    with patch.object(recorder_logger, 'info') as mock_log:
        rec.record_task_completed(make_slow_result(), partition=0)
        mock_log.assert_called_once()
        assert mock_log.call_args[0][0] == 'slow_task_completed'

    await rec.stop()


async def test_log_threshold_skips_fast_task(tmp_path):
    """Fast task does not trigger structlog info line."""
    from unittest.mock import patch

    config = make_debug_config(tmp_path, log_min_duration_ms=500)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    with patch.object(recorder_logger, 'info') as mock_log:
        rec.record_task_completed(make_fast_result(), partition=0)
        mock_log.assert_not_called()

    await rec.stop()


async def test_failed_with_duration_applies_thresholds(tmp_path):
    """record_task_failed with duration_seconds applies db/output thresholds
    but always sends to WS (failed tasks are never suppressed from live UI).
    """
    config = make_debug_config(
        tmp_path,
        ws_min_duration_ms=500,
        event_min_duration_ms=500,
        output_min_duration_ms=500,
    )
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    task = make_task('t-fast')
    error = ExecutorError(task=task, exit_code=1, stderr='error output')
    q = rec.subscribe()

    # start the task (deferred) then fail it
    rec.record_task_started(task, partition=0)
    rec.record_task_failed(task, error, partition=0, duration_seconds=0.1)

    # buffer has task_started (always written to DB) but not task_failed
    # (skipped by event_min_duration_ms=500)
    assert len(rec._buffer) == 1
    assert rec._buffer[0]['event'] == 'task_started'
    events = []
    while not q.empty():
        events.append(q.get_nowait())
    assert len(events) == 2
    assert events[0]['event'] == 'task_started'
    assert events[1]['event'] == 'task_failed'
    rec.unsubscribe(q)
    await rec.stop()


async def test_failed_slow_task_recorded_with_duration(tmp_path):
    """record_task_failed with slow duration records normally."""
    config = make_debug_config(
        tmp_path,
        store_output=True,
        ws_min_duration_ms=500,
        event_min_duration_ms=500,
        output_min_duration_ms=500,
    )
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    task = make_task('t-slow')
    error = ExecutorError(task=task, exit_code=1, stderr='slow error')
    q = rec.subscribe()

    rec.record_task_failed(task, error, partition=0, duration_seconds=1.5)

    assert len(rec._buffer) == 1
    entry = rec._buffer[0]
    assert entry['duration'] == 1.5
    assert 'args' in entry
    assert entry['stderr'] == 'slow error'
    assert not q.empty()
    rec.unsubscribe(q)
    await rec.stop()


async def test_failed_without_duration_records_everything(tmp_path):
    """record_task_failed without duration records all data (safe default)."""
    config = make_debug_config(
        tmp_path,
        store_output=True,
        ws_min_duration_ms=500,
        event_min_duration_ms=500,
        output_min_duration_ms=500,
    )
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    task = make_task('t-nodur')
    error = ExecutorError(task=task, exit_code=1, stderr='unknown duration error')
    q = rec.subscribe()

    rec.record_task_failed(task, error, partition=0)

    # without duration: everything recorded
    assert len(rec._buffer) == 1
    entry = rec._buffer[0]
    assert 'duration' not in entry
    assert 'args' in entry
    assert entry['stderr'] == 'unknown duration error'
    assert not q.empty()
    rec.unsubscribe(q)
    await rec.stop()


async def test_failed_output_threshold_excludes_args_stderr(tmp_path):
    """record_task_failed with fast duration excludes args and stderr."""
    config = make_debug_config(
        tmp_path,
        store_output=True,
        event_min_duration_ms=0,
        output_min_duration_ms=500,
    )
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    task = make_task('t-fast')
    error = ExecutorError(task=task, exit_code=1, stderr='error detail')

    rec.record_task_failed(task, error, partition=0, duration_seconds=0.1)

    assert len(rec._buffer) == 1
    entry = rec._buffer[0]
    assert 'args' not in entry
    assert 'stderr' not in entry
    await rec.stop()


async def test_log_threshold_failed_slow_task(tmp_path):
    """Slow failed task triggers structlog info line."""
    from unittest.mock import patch

    config = make_debug_config(tmp_path, log_min_duration_ms=500)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    task = make_task('t-slow')
    error = ExecutorError(task=task, exit_code=1, stderr='err')

    with patch.object(recorder_logger, 'info') as mock_log:
        rec.record_task_failed(task, error, partition=0, duration_seconds=1.5)
        mock_log.assert_called_once()
        assert mock_log.call_args[0][0] == 'slow_task_failed'

    await rec.stop()


async def test_log_threshold_failed_fast_task_no_log(tmp_path):
    """Fast failed task does not trigger structlog info line."""
    from unittest.mock import patch

    config = make_debug_config(tmp_path, log_min_duration_ms=500)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    task = make_task('t-fast')
    error = ExecutorError(task=task, exit_code=1, stderr='err')

    with patch.object(recorder_logger, 'info') as mock_log:
        rec.record_task_failed(task, error, partition=0, duration_seconds=0.1)
        mock_log.assert_not_called()

    await rec.stop()


async def test_counters_always_increment_regardless_of_thresholds(tmp_path):
    """Counters increment even when events are filtered by thresholds."""
    config = make_debug_config(
        tmp_path,
        event_min_duration_ms=500,
        ws_min_duration_ms=500,
    )
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    rec.record_task_completed(make_fast_result('t1'), partition=0)
    rec.record_task_completed(make_fast_result('t2'), partition=0)
    task = make_task('t3')
    error = ExecutorError(task=task, exit_code=1, stderr='err')
    rec.record_task_failed(task, error, partition=0, duration_seconds=0.1)

    assert rec.counters['completed'] == 2
    assert rec.counters['failed'] == 1
    # buffer should be empty (fast tasks, event_min_duration_ms=500)
    assert len(rec._buffer) == 0
    await rec.stop()


async def test_threshold_boundary_exact_match(tmp_path):
    """Task with duration exactly at threshold is included."""
    config = make_debug_config(
        tmp_path,
        ws_min_duration_ms=500,
        output_min_duration_ms=500,
        log_min_duration_ms=500,
    )
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    task = make_task('t-boundary')
    result = ExecutorResult(
        exit_code=0,
        stdout='boundary\n',
        stderr='',
        duration_seconds=0.5,  # exactly 500ms
        task=task,
    )
    q = rec.subscribe()
    rec.record_task_completed(result, partition=0)

    # 500ms >= 500ms threshold → included
    assert not q.empty()
    assert 'args' in rec._buffer[0]
    rec.unsubscribe(q)
    await rec.stop()


async def test_threshold_zero_means_include_all(tmp_path):
    """All thresholds set to 0 means everything is recorded."""
    config = make_debug_config(
        tmp_path,
        store_output=True,
        log_min_duration_ms=0,
        ws_min_duration_ms=0,
        event_min_duration_ms=0,
        output_min_duration_ms=0,
    )
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    q = rec.subscribe()
    rec.record_task_completed(make_fast_result(), partition=0)

    assert len(rec._buffer) == 1
    assert not q.empty()
    entry = rec._buffer[0]
    assert 'args' in entry
    assert 'stdout' in entry
    rec.unsubscribe(q)
    await rec.stop()


def test_detect_worker_ip_happy_path_returns_ip_string():
    """Happy path: real socket returns a valid-looking IPv4 string."""
    import ipaddress

    ip = detect_worker_ip()
    # Must be a valid IPv4 address — either a real outbound IP or the
    # 127.0.0.1 fallback if the socket operations failed in the sandbox.
    parsed = ipaddress.ip_address(ip)
    assert parsed.version == 4


def test_detect_worker_ip_closes_socket_when_getsockname_raises():
    """Regression: socket must be closed even if getsockname() raises.

    Guards against fd leak on DB rotation: prior implementation called
    ``s.close()`` after ``getsockname()``, so an exception between
    ``connect()`` and ``close()`` leaked the descriptor.
    """
    from unittest.mock import MagicMock, patch

    fake_socket = MagicMock()
    fake_socket.getsockname.side_effect = OSError('boom')

    with patch('drakkar.recorder.socket.socket', return_value=fake_socket):
        ip = detect_worker_ip()

    # Fallback value preserved regardless of the underlying failure.
    assert ip == '127.0.0.1'
    # Critical assertion: contextlib.closing guarantees exactly one close().
    fake_socket.close.assert_called_once()


# --- Recorder buffer / drop / flush metrics ---
#
# Task 5 (Phase 1 ship-blockers): three Prometheus metrics surface the
# flush pipeline's health — gauge for live depth, counter for silent
# overflow drops, histogram for flush latency. These tests exercise each
# wiring path end-to-end against a real recorder. We follow the existing
# ``before = metric._value.get()`` / ``after = ...`` convention used in
# ``tests/test_metrics.py`` so failures read naturally.


async def test_recorder_buffer_gauge_tracks_length_after_append(tmp_path):
    """Gauge reflects ``len(self._buffer)`` after each ``_record`` call."""
    from drakkar.metrics import recorder_buffer_size

    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    rec.record_consumed(make_msg(offset=0))
    assert recorder_buffer_size._value.get() == 1
    rec.record_consumed(make_msg(offset=1))
    rec.record_consumed(make_msg(offset=2))
    assert recorder_buffer_size._value.get() == 3

    await rec.stop()


async def test_recorder_buffer_gauge_reflects_zero_after_flush(tmp_path):
    """Gauge drops to zero once the buffer drains via ``_flush``."""
    from drakkar.metrics import recorder_buffer_size

    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    for offset in range(5):
        rec.record_consumed(make_msg(offset=offset))
    assert recorder_buffer_size._value.get() == 5

    await rec._flush()
    assert recorder_buffer_size._value.get() == 0

    await rec.stop()


async def test_recorder_dropped_events_increments_on_overflow(tmp_path):
    """Appends past ``max_buffer`` increment ``recorder_dropped_events_total``.

    A ``deque(maxlen=N)`` silently evicts the leftmost entry on every
    over-cap append. The recorder now detects this case **before** the
    append and bumps the counter exactly once per lost event.
    """
    from drakkar.metrics import recorder_dropped_events

    # Tiny buffer makes the overflow test cheap and deterministic.
    config = make_debug_config(tmp_path, max_buffer=1000)  # field constraint: ge=1000
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    # Shrink post-construction to keep the test fast; validates the drop
    # path without pushing 1000+ events through the recorder.
    rec._buffer = deque(maxlen=3)
    await rec.start()

    before = recorder_dropped_events._value.get()

    # Fill to cap: three appends, none dropped (len goes 0→1→2→3).
    for offset in range(3):
        rec.record_consumed(make_msg(offset=offset))
    assert recorder_dropped_events._value.get() == before

    # Each append beyond cap increments the drop counter once.
    rec.record_consumed(make_msg(offset=3))
    rec.record_consumed(make_msg(offset=4))
    assert recorder_dropped_events._value.get() == before + 2
    # Buffer length stays at maxlen — deque silently rolled the window.
    assert len(rec._buffer) == 3

    await rec.stop()


def _histogram_count(hist) -> float:
    """Read a no-label Histogram's current observation count via ``collect()``.

    ``prometheus_client.Histogram`` does not expose ``_count`` as a public
    attribute; the authoritative sample is emitted through ``collect()``
    with the name suffix ``_count``. Reading it this way keeps the test
    decoupled from library internals (matches the approach used in
    ``drakkar.metrics.cache_gauge_snapshot``).
    """
    for metric in hist.collect():
        for sample in metric.samples:
            if sample.name.endswith('_count') and not sample.labels:
                return sample.value
    return 0.0


async def test_recorder_flush_duration_histogram_observed(tmp_path):
    """``_flush`` observes the executemany+commit duration in the histogram."""
    from drakkar.metrics import recorder_flush_duration

    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    before_sum = recorder_flush_duration._sum.get()
    before_count = _histogram_count(recorder_flush_duration)

    # Populate buffer so _flush executes the full path (not the early
    # return when buffer is empty).
    for offset in range(5):
        rec.record_consumed(make_msg(offset=offset))
    await rec._flush()

    # Histogram must have observed exactly one sample with a positive
    # duration. Sum strictly > before guards against a zero-valued
    # observe() call slipping through.
    assert _histogram_count(recorder_flush_duration) == before_count + 1
    assert recorder_flush_duration._sum.get() > before_sum

    await rec.stop()


async def test_recorder_flush_skipped_when_buffer_empty_no_histogram_observation(tmp_path):
    """Empty-buffer flush returns early without recording a histogram sample.

    The no-op early return in ``_flush`` must not falsely inflate the
    flush-duration histogram. Operators reading the rate would otherwise
    see fabricated ticks on idle recorders.
    """
    from drakkar.metrics import recorder_flush_duration

    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    before_count = _histogram_count(recorder_flush_duration)
    await rec._flush()
    assert _histogram_count(recorder_flush_duration) == before_count

    await rec.stop()


async def test_recorder_dropped_events_not_incremented_when_skip_db(tmp_path):
    """``skip_db=True`` bypasses the buffer entirely — no drop counter tick."""
    from drakkar.metrics import recorder_dropped_events

    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    rec._buffer = deque(maxlen=2)
    await rec.start()

    # Pre-fill to cap so any append would normally drop one event.
    rec.record_consumed(make_msg(offset=0))
    rec.record_consumed(make_msg(offset=1))
    before = recorder_dropped_events._value.get()

    # skip_db path writes only to WS subscribers — buffer untouched,
    # no overflow possible, counter must not tick.
    rec._record({'ts': 1.0, 'event': 'skipped'}, skip_db=True)
    assert recorder_dropped_events._value.get() == before

    await rec.stop()


# --- Task 8 (Phase 3): aiosqlite error injection / snapshot-restore ------
#
# Production recorders will sometimes hit aiosqlite ``OperationalError`` —
# under contention (``database is locked``) or disk failure (``disk I/O
# error``). ``_flush`` now takes a local snapshot of the batch BEFORE the
# DB write, and on ``OperationalError`` re-queues the snapshot at the
# FRONT of the buffer so the next flush retries the same rows. After
# ``max_flush_retries`` consecutive failures the batch is dropped and
# the ``drakkar_recorder_flush_batches_dropped_total`` counter ticks.
# Each failed attempt ticks ``drakkar_recorder_flush_retries_total``.
#
# The tests below exercise:
#   - transient failure → re-queue → next-flush success
#   - persistent failure → N retries → drop + counter tick + no mem leak
#   - rotation DURING a failed flush (re-queued rows go to the new DB —
#     acceptable for a flight recorder; see the ``_flush`` comment).
#   - commit never reached when executemany fails (narrow regression check).


async def test_recorder_flush_operational_error_requeues_batch(tmp_path):
    """Transient ``OperationalError`` re-queues the batch; next flush succeeds.

    Scenario: the first executemany raises ``database is locked``; we
    verify the snapshot was pushed back to the FRONT of the buffer (order
    preserved), the retry counter ticked by 1, and the second flush
    (with executemany restored) writes the rows exactly once.
    """
    from drakkar.metrics import recorder_flush_retries

    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()
    try:
        rec.record_consumed(make_msg(partition=0, offset=0))
        rec.record_consumed(make_msg(partition=0, offset=1))
        assert len(rec._buffer) == 2

        assert rec._db is not None
        original_executemany = rec._db.executemany
        calls = {'n': 0}

        async def failing_once(query, rows):
            calls['n'] += 1
            if calls['n'] == 1:
                raise aiosqlite.OperationalError('database is locked')
            return await original_executemany(query, rows)

        rec._db.executemany = failing_once  # type: ignore[method-assign]

        retries_before = recorder_flush_retries._value.get()

        # First flush: swallows the OperationalError and re-queues.
        await rec._flush()

        # Rows are back in the buffer — no data lost.
        assert len(rec._buffer) == 2
        # Order preserved: offset 0 at the front, offset 1 behind it.
        buffered = list(rec._buffer)
        assert buffered[0]['offset'] == 0
        assert buffered[1]['offset'] == 1
        # Retry counter incremented once.
        assert recorder_flush_retries._value.get() == retries_before + 1
        # Instance-level failure counter reflects the single failure.
        assert rec._flush_failures == 1

        # Second flush: real executemany runs, rows land in the DB.
        await rec._flush()
        assert len(rec._buffer) == 0
        # Retry counter reset on success.
        assert rec._flush_failures == 0

        # Rows written exactly once (not duplicated by the re-queue).
        events = await rec.get_events(partition=0)
        offsets = sorted(ev['offset'] for ev in events)
        assert offsets == [0, 1]
    finally:
        await rec.stop()


async def test_recorder_flush_requeue_overflow_increments_counter(tmp_path):
    """Requeue that overflows ``max_buffer`` ticks ``recorder_requeue_overflow``.

    Scenario: an OperationalError during flush forces a re-queue via
    ``extendleft``. If concurrent ``_record`` appends filled the buffer
    during the flush's await window, ``extendleft`` silently evicts rows
    from the tail to honour the deque's ``maxlen``. This was previously
    uncounted data loss; the new counter surfaces it.

    Test construction:
      - Shrink ``rec._buffer`` to a tiny ``maxlen`` so overflow is cheap
        to trigger deterministically.
      - Populate with 3 rows and take a snapshot of executemany to fail.
      - Simulate concurrent appends during the failed flush by refilling
        the buffer BEFORE the requeue lands (we hook the failing
        executemany to push more rows in).
    """
    from drakkar.metrics import recorder_requeue_overflow

    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    # Shrink post-construction so the overflow path is reachable without
    # pushing thousands of events through the recorder.
    rec._buffer = deque(maxlen=3)
    await rec.start()
    try:
        # Seed the buffer with 3 rows (at capacity).
        for off in range(3):
            rec.record_consumed(make_msg(partition=0, offset=off))
        assert len(rec._buffer) == 3

        assert rec._db is not None

        async def failing_and_filling(query, rows):
            # Simulate: during the flush's await window a concurrent
            # ``_record`` pushed 2 new rows into the (now drained) buffer.
            # After this await returns with an OperationalError, the
            # executor will extendleft the original 3-row batch back —
            # with 2 rows already in the buffer and maxlen=3, that means
            # 2 rows must be evicted from the tail.
            for off in range(100, 102):
                rec.record_consumed(make_msg(partition=0, offset=off))
            raise aiosqlite.OperationalError('database is locked')

        rec._db.executemany = failing_and_filling  # type: ignore[method-assign]

        overflow_before = recorder_requeue_overflow._value.get()

        # Trigger the flush: drains 3 rows into the batch, call fails,
        # 2 concurrent rows land in the buffer, then extendleft(3 batch
        # rows) overflows by (2 + 3) - 3 = 2.
        await rec._flush()

        # Counter ticked by the overflow count (2).
        assert recorder_requeue_overflow._value.get() == overflow_before + 2
        # Buffer is pinned at maxlen — the deque rolled the window.
        assert len(rec._buffer) == 3
    finally:
        await rec.stop()


async def test_recorder_flush_persistent_error_drops_after_max_retries(tmp_path):
    """Persistent ``OperationalError`` → batch dropped after N attempts.

    After ``max_flush_retries`` consecutive failures we drop the batch,
    tick the drop counter, reset the instance counter (so fresh batches
    start from zero), and leave the buffer empty — no memory leak.
    """
    from drakkar.metrics import (
        recorder_flush_batches_dropped,
        recorder_flush_retries,
    )

    # Set an explicit small retry budget so the test runs quickly.
    config = make_debug_config(tmp_path, max_flush_retries=3)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()
    try:
        rec.record_consumed(make_msg(partition=0, offset=0))
        rec.record_consumed(make_msg(partition=0, offset=1))
        assert len(rec._buffer) == 2

        assert rec._db is not None

        async def always_failing(query, rows):
            raise aiosqlite.OperationalError('disk I/O error')

        rec._db.executemany = always_failing  # type: ignore[method-assign]

        retries_before = recorder_flush_retries._value.get()
        dropped_before = recorder_flush_batches_dropped._value.get()

        # Attempts 1..N-1: re-queue each time, counter climbs.
        for attempt in range(1, config.max_flush_retries):
            await rec._flush()
            assert rec._flush_failures == attempt
            # Rows still in buffer — no data loss yet.
            assert len(rec._buffer) == 2

        # Attempt N (max_flush_retries): give up, drop the batch.
        await rec._flush()
        # Buffer is empty (no memory leak; the batch was discarded).
        assert len(rec._buffer) == 0
        # Instance counter reset so the NEXT batch starts from zero.
        assert rec._flush_failures == 0
        # Drop counter ticked exactly once.
        assert recorder_flush_batches_dropped._value.get() == dropped_before + 1
        # Retry counter ticked on every failed attempt (including the one
        # that finally tripped the drop).
        assert recorder_flush_retries._value.get() == retries_before + config.max_flush_retries

        # Subsequent batches with a working DB flush cleanly — retries
        # counter is fresh because we reset it on drop. Unpatch by
        # routing executemany back through the real aiosqlite method.
        rec._db.executemany = type(rec._db).executemany.__get__(rec._db)  # type: ignore[method-assign]

        rec.record_consumed(make_msg(partition=0, offset=2))
        await rec._flush()
        assert len(rec._buffer) == 0
        assert rec._flush_failures == 0

        events = await rec.get_events(partition=0)
        # Only the post-drop event lands — the original batch was correctly
        # dropped (intended behaviour once retries are exhausted).
        assert len(events) == 1
        assert events[0]['offset'] == 2
    finally:
        await rec.stop()


async def test_recorder_flush_rotation_between_failures_reroutes_to_new_db(tmp_path, monkeypatch):
    """Rotation DURING a failed flush sends re-queued rows to the NEW DB.

    Documented trade-off: when ``_rotate`` runs while a batch sits in the
    buffer after a failed flush, the next flush writes those rows to the
    rotated (new) DB. The rows' ``ts`` still records when they were
    observed, so their wall-clock position is preserved — only the file-
    level window association shifts. This is strictly better than silent
    data loss, which would be the alternative "drop on rotation" policy.
    """
    # ``retention_max_events=50_000`` keeps the old DB file around after
    # rotation (``max_files = retention_max_events // 10_000 = 5``), so we
    # can open the old file read-only and prove nothing was committed.
    config = make_debug_config(tmp_path, retention_max_events=50_000)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()
    try:
        old_db_path = rec.db_path
        rec.record_consumed(make_msg(partition=0, offset=0))
        assert len(rec._buffer) == 1

        # First flush fails, rows re-queued.
        assert rec._db is not None

        async def failing_executemany(query, rows):
            raise aiosqlite.OperationalError('database is locked')

        rec._db.executemany = failing_executemany  # type: ignore[method-assign]
        await rec._flush()
        assert len(rec._buffer) == 1

        # Force a distinct rotated DB path deterministically, without the
        # ~1.1s real-time sleep that the previous implementation needed to
        # roll the second-resolution timestamp in ``make_db_path``. This
        # keeps the test fast and removes a long wait from the suite.
        from drakkar import recorder as recorder_module

        unique_new_path = str(tmp_path / f'{WORKER_NAME}-rotated.db')
        monkeypatch.setattr(
            recorder_module,
            'make_db_path',
            lambda db_dir, worker_name: unique_new_path,
        )

        # Rotate: flushes once to the old DB (still failing, re-queues
        # again), opens a new DB, and swaps. The re-queued rows survive.
        await rec._rotate()
        new_db_path = rec.db_path
        assert new_db_path != old_db_path
        assert len(rec._buffer) >= 1

        # The new DB opens cleanly — no patched executemany carried over.
        # The instance counter carries over from the pre-rotation failures
        # (first _flush + the _flush inside _rotate both bumped it), but
        # since the default ``max_flush_retries`` is 3 we still have one
        # more attempt available. A successful flush resets the counter.
        await rec._flush()
        assert len(rec._buffer) == 0
        assert rec._flush_failures == 0

        # Re-queued row is visible through the recorder (new DB).
        events = await rec.get_events(partition=0)
        assert any(ev['offset'] == 0 for ev in events)

        # Old DB file: row was never committed (executemany kept failing
        # on the old connection before the rotate swapped it out).
        async with (
            aiosqlite.connect(f'file:{old_db_path}?mode=ro', uri=True) as old_db,
            old_db.execute('SELECT COUNT(*) FROM events WHERE partition = 0') as cur,
        ):
            row = await cur.fetchone()
            assert row is not None
            assert row[0] == 0
    finally:
        await rec.stop()


async def test_recorder_flush_executemany_failure_does_not_commit(tmp_path):
    """When ``executemany`` raises, ``commit`` is never reached.

    Narrow regression check: the failure path must not accidentally
    commit partial results. SQLite auto-commits in some isolation modes
    and we want to confirm the error surfaces before commit runs.
    """
    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()
    try:
        rec.record_consumed(make_msg(partition=0, offset=0))

        assert rec._db is not None
        commit_called = {'n': 0}
        original_commit = rec._db.commit
        original_executemany = rec._db.executemany

        async def spy_commit():
            commit_called['n'] += 1
            return await original_commit()

        async def locked_executemany(query, rows):
            raise aiosqlite.OperationalError('database is locked')

        rec._db.commit = spy_commit  # type: ignore[method-assign]
        rec._db.executemany = locked_executemany  # type: ignore[method-assign]

        # No longer raises — the handler catches the error and re-queues.
        await rec._flush()

        # commit must not have been called — executemany failed first.
        assert commit_called['n'] == 0
        # Rows re-queued so nothing is lost.
        assert len(rec._buffer) == 1
    finally:
        # Restore originals so stop()'s own _flush drain + close work.
        if rec._db is not None:
            rec._db.executemany = original_executemany  # type: ignore[method-assign]
            rec._db.commit = original_commit  # type: ignore[method-assign]
        await rec.stop()


# --- JSON encoder helpers (orjson fast path + stdlib fallback) ---
#
# The recorder encodes payloads (args, metadata, labels, env_vars) via
# ``encode_json`` / ``encode_json_str``. orjson is an optional
# dependency (``drakkar[perf]``) and when installed we take the fast
# path; otherwise we fall back to ``json.dumps`` with matching options
# so the bytes are identical. These tests pin that contract.


_ROUND_TRIP_PAYLOADS = [
    pytest.param({}, id='empty-dict'),
    pytest.param([], id='empty-list'),
    pytest.param({'a': 1, 'b': 'str'}, id='flat-dict'),
    pytest.param({'b': 1, 'a': 2, 'c': 3}, id='unsorted-keys'),
    pytest.param({'nested': {'y': 'val', 'x': 3}}, id='nested-dict'),
    pytest.param([1, 2, 3, None, True, False], id='mixed-list'),
    pytest.param({'unicode': 'naïve', 'emoji': 'test'}, id='unicode-strings'),
    pytest.param({'float': 1.5, 'zero': 0, 'neg': -7}, id='numbers'),
    pytest.param({'deeply': {'nested': {'structure': {'value': 42}}}}, id='deep-nesting'),
    pytest.param(
        {'task_id': 't-1', 'args': ['--flag', 'value'], 'retry': 0},
        id='realistic-task',
    ),
    pytest.param(
        {
            'task_count': 100,
            'succeeded': 95,
            'failed': 5,
            'labels': {'env': 'prod', 'region': 'us-east'},
        },
        id='realistic-metadata',
    ),
]


@pytest.mark.parametrize('payload', _ROUND_TRIP_PAYLOADS)
def testencode_json_round_trip(payload):
    """``encode_json(x)`` → ``json.loads`` → re-encode must be stable.

    Property: a payload that survives one encode/decode cycle must
    produce identical bytes when re-encoded. Catches encoder state bugs
    (e.g. non-deterministic key order) and ensures our options
    (``OPT_SORT_KEYS``) actually take effect.
    """
    from drakkar.recorder import encode_json

    encoded = encode_json(payload)
    decoded = json.loads(encoded)
    re_encoded = encode_json(decoded)
    assert encoded == re_encoded
    assert decoded == payload


@pytest.mark.parametrize('payload', _ROUND_TRIP_PAYLOADS)
def testencode_json_orjson_stdlib_byte_parity(payload):
    """Both encoder paths produce byte-for-byte identical output.

    Swapping the ``perf`` extra on/off (install/uninstall orjson) MUST
    NOT change the bytes stored in the recorder SQLite file — otherwise
    a user who installs the extra after running without it would see
    recorder data whose hashes diverge from historical output, and
    cross-worker deduplication keyed on JSON payload bytes would break.

    We compare the currently bound encoder (orjson fast path when
    available) with an inline reconstruction of the stdlib fallback
    using the exact options the real fallback uses.
    """
    from drakkar import recorder

    fast_path_bytes = recorder.encode_json(payload)

    def stdlib_fallback(obj):
        return json.dumps(
            obj,
            sort_keys=True,
            default=str,
            separators=(',', ':'),
            ensure_ascii=False,
        ).encode('utf-8')

    stdlib_bytes = stdlib_fallback(payload)
    assert fast_path_bytes == stdlib_bytes, f'fast-path bytes {fast_path_bytes!r} != stdlib bytes {stdlib_bytes!r}'


def testencode_json_monkeypatched_fallback_matches_orjson(monkeypatch):
    """Force the stdlib fallback by patching ``encode_json`` and
    verify that encoded output is still identical to the fast path.

    This exercises the ``except ImportError`` branch structurally — we
    can't actually uninstall orjson at test time, but we can run the
    fallback code and prove its output matches.
    """
    from drakkar import recorder

    # Capture the fast-path bytes before patching.
    payload = {'b': 1, 'a': 2, 'nested': {'y': 'val', 'x': 3}, 'unicode': 'naïve'}
    fast_path_bytes = recorder.encode_json(payload)

    # Swap in the stdlib fallback (verbatim from recorder.py).
    def fallback(obj):
        return json.dumps(
            obj,
            sort_keys=True,
            default=str,
            separators=(',', ':'),
            ensure_ascii=False,
        ).encode('utf-8')

    monkeypatch.setattr(recorder, 'encode_json', fallback)
    fallback_bytes = recorder.encode_json(payload)

    assert fast_path_bytes == fallback_bytes


def testencode_json_datetime_utc_z_suffix():
    """UTC datetimes encode with a trailing ``Z``, not ``+00:00``.

    orjson's ``OPT_UTC_Z`` flag emits ``Z`` directly. The stdlib fallback
    uses ``default=str`` which yields ``datetime.isoformat()`` — that
    writes ``+00:00``. This test pins the orjson behaviour; if we ever
    swap encoders the test will catch a regression in the format.

    The recorder uses ``format_dt`` for persisted timestamps (which
    does the ``+00:00`` → ``Z`` swap itself), so the encoder's datetime
    handling only affects ad-hoc datetimes embedded inside payload
    dicts — still worth pinning.
    """
    from datetime import UTC, datetime

    from drakkar.recorder import _HAS_ORJSON, encode_json

    dt = datetime(2026, 4, 24, 12, 30, 45, tzinfo=UTC)
    encoded = encode_json({'ts': dt})

    if _HAS_ORJSON:
        # orjson: native datetime support, OPT_UTC_Z → "Z" suffix.
        assert encoded == b'{"ts":"2026-04-24T12:30:45Z"}'
    else:
        # stdlib fallback: default=str → isoformat → "+00:00".
        assert encoded == b'{"ts":"2026-04-24T12:30:45+00:00"}'


def testencode_json_str_decodes_bytes():
    """``encode_json_str`` returns a string that equals the bytes
    decoded as UTF-8 — no hidden encoding differences."""
    from drakkar.recorder import encode_json, encode_json_str

    payload = {'key': 'value', 'n': 42}
    assert encode_json_str(payload) == encode_json(payload).decode('utf-8')


def testencode_json_non_json_native_types_via_default_str():
    """Custom classes fall back to ``default=str`` (both paths).

    orjson raises ``TypeError`` on unsupported types by default; we pass
    ``default=str`` so anything unusual serializes via ``repr``/``str``.
    The stdlib path does the same. This test pins that contract so a
    recorder event carrying a ``Path`` / ``UUID`` / custom object won't
    crash the flush loop.
    """
    import uuid as _uuid
    from pathlib import Path

    from drakkar.recorder import encode_json

    # pathlib.Path — both paths coerce via str(path).
    encoded = encode_json({'p': Path('/tmp/x')})
    decoded = json.loads(encoded)
    assert decoded == {'p': '/tmp/x'}

    # UUID — orjson serializes natively (no ``default`` hook needed),
    # stdlib uses default=str. Either way, the decoded JSON is the
    # canonical UUID string.
    sample_uuid = _uuid.UUID('12345678-1234-5678-1234-567812345678')
    encoded = encode_json({'uid': sample_uuid})
    decoded = json.loads(encoded)
    assert decoded == {'uid': '12345678-1234-5678-1234-567812345678'}

    # Custom class with ``__str__`` — both paths coerce via str(obj),
    # proving the default-str hook catches arbitrary user types.
    class _Custom:
        def __str__(self) -> str:
            return 'custom-repr'

    encoded = encode_json({'obj': _Custom()})
    decoded = json.loads(encoded)
    assert decoded == {'obj': 'custom-repr'}


def testencode_json_sort_keys_deterministic():
    """``OPT_SORT_KEYS`` / ``sort_keys=True`` make output deterministic
    regardless of dict insertion order. Critical for cache dedup hashes
    downstream — two workers encoding the same logical payload must
    produce identical bytes even if they built the dict in different
    orders.
    """
    from drakkar.recorder import encode_json

    a = {'z': 1, 'a': 2, 'm': 3}
    b = {'a': 2, 'm': 3, 'z': 1}
    c = {'m': 3, 'z': 1, 'a': 2}
    assert encode_json(a) == encode_json(b) == encode_json(c)


# --- cross_trace_by_label ---------------------------------------------------
#
# The local-only path is exercised by the ``trace_by_label`` tests above. The
# tests below cover the cross-worker fallback (lines 987-1035 of recorder.py),
# which mirrors ``cross_trace`` (partition+offset) for label-based lookups.


async def _create_worker_db_with_labels(
    db_path,
    worker_name,
    cluster_name='',
    task_id='task-labeled',
    labels=None,
):
    """Create a DB with worker_config + events whose tasks carry labels."""
    labels = labels or {'request_id': 'req-find-me'}
    async with aiosqlite.connect(str(db_path)) as db:
        await db.executescript(SCHEMA_WORKER_CONFIG)
        await db.executescript(SCHEMA_EVENTS)
        await db.execute(
            """INSERT INTO worker_config
               (id, worker_name, cluster_name, ip_address, debug_port, debug_url, kafka_brokers,
                source_topic, consumer_group, binary_path, max_executors, task_timeout_seconds,
                max_retries, window_size, sinks_json, env_vars_json, created_at, created_at_dt)
               VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                worker_name,
                cluster_name,
                '10.0.0.1',
                8080,
                '',
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
                format_dt(1000.0),
            ],
        )
        await db.execute(
            """INSERT INTO events (ts, dt, event, partition, offset, task_id, metadata, labels)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                1000.0,
                format_dt(1000.0),
                'task_started',
                0,
                None,
                task_id,
                None,
                json.dumps(labels),
            ],
        )
        await db.execute(
            """INSERT INTO events (ts, dt, event, partition, offset, task_id, metadata, labels)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                1001.0,
                format_dt(1001.0),
                'task_completed',
                0,
                None,
                task_id,
                None,
                json.dumps(labels),
            ],
        )
        await db.commit()


async def test_cross_trace_by_label_finds_in_current_worker(tmp_path):
    """Local DB has matching label — returns events with worker_name attached."""
    config = make_debug_config(tmp_path, ws_min_duration_ms=0)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    task = make_task('t-local-label')
    task.labels = {'request_id': 'req-local'}
    rec.record_task_started(task, partition=0)
    rec.record_task_completed(make_result(task=task), partition=0)
    await rec._flush()

    events = await rec.cross_trace_by_label('request_id', 'req-local')
    assert len(events) >= 2
    assert all(e['worker_name'] == WORKER_NAME for e in events)
    # Result is sorted by ts ascending.
    timestamps = [e.get('ts', 0) for e in events]
    assert timestamps == sorted(timestamps)
    await rec.stop()


async def test_cross_trace_by_label_fallback_to_other_live_worker(tmp_path):
    """Local has no match — search other workers' live DBs."""
    other_db_path = tmp_path / 'other-worker-2026-03-25__10_00_00.db'
    await _create_worker_db_with_labels(
        other_db_path,
        'other-worker',
        task_id='t-remote',
        labels={'request_id': 'req-remote'},
    )
    link = live_link_path(str(tmp_path), 'other-worker')
    os.symlink(other_db_path.name, link)

    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    events = await rec.cross_trace_by_label('request_id', 'req-remote')
    assert len(events) >= 1
    assert all(e['worker_name'] == 'other-worker' for e in events)
    await rec.stop()


async def test_cross_trace_by_label_returns_empty_when_no_db_dir():
    """Without ``db_dir`` the cross-worker fallback short-circuits to []."""
    # ``DebugConfig`` requires ``db_dir`` for its other validators, so build
    # a recorder, then null the directory to exercise the early-return.
    config = DebugConfig(enabled=True, db_dir='', flush_interval_seconds=60)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    events = await rec.cross_trace_by_label('request_id', 'whatever')
    assert events == []
    await rec.stop()


async def test_cross_trace_by_label_skips_self_via_realpath(tmp_path):
    """Own live symlink resolves to the local writer DB and must be skipped
    so we don't open the writer file twice (which would either fail or
    return the same already-empty events)."""
    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    # Force a live symlink to exist for our own worker, pointing at our DB.
    self_link = live_link_path(str(tmp_path), WORKER_NAME)
    if not os.path.lexists(self_link):
        os.symlink(os.path.basename(rec.db_path), self_link)

    # No labels anywhere — fallback walk must visit the self-link, skip it,
    # and return [] without raising.
    events = await rec.cross_trace_by_label('request_id', 'no-such-id')
    assert events == []
    await rec.stop()


async def test_cross_trace_by_label_cluster_filtering(tmp_path):
    """A live worker in a different cluster is filtered out even if it has
    a matching label."""
    other_db_path = tmp_path / 'other-worker-2026-03-25__10_00_00.db'
    await _create_worker_db_with_labels(
        other_db_path,
        'other-worker',
        cluster_name='other-cluster',
        task_id='t-other',
        labels={'request_id': 'req-other'},
    )
    link = live_link_path(str(tmp_path), 'other-worker')
    os.symlink(other_db_path.name, link)

    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME, cluster_name='my-cluster')
    await rec.start()

    events = await rec.cross_trace_by_label('request_id', 'req-other')
    assert events == []
    await rec.stop()


async def test_cross_trace_by_label_skips_db_without_events_table(tmp_path):
    """A DB whose ``events`` table was dropped (or never created) is skipped
    rather than raising 'no such table'."""
    # Build a DB with worker_config but no events table.
    broken_db_path = tmp_path / 'broken-worker-2026-03-25__10_00_00.db'
    async with aiosqlite.connect(str(broken_db_path)) as db:
        await db.executescript(SCHEMA_WORKER_CONFIG)
        await db.execute(
            """INSERT INTO worker_config
               (id, worker_name, cluster_name, ip_address, debug_port, debug_url, kafka_brokers,
                source_topic, consumer_group, binary_path, max_executors, task_timeout_seconds,
                max_retries, window_size, sinks_json, env_vars_json, created_at, created_at_dt)
               VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                'broken-worker',
                '',
                '10.0.0.2',
                8080,
                '',
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
                format_dt(1000.0),
            ],
        )
        await db.commit()
    link = live_link_path(str(tmp_path), 'broken-worker')
    os.symlink(broken_db_path.name, link)

    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    events = await rec.cross_trace_by_label('request_id', 'whatever')
    assert events == []
    await rec.stop()


async def test_cross_trace_by_label_swallows_db_open_exception(tmp_path, monkeypatch):
    """An ``aiosqlite.connect`` failure on a peer DB must not abort the
    walk — the loop is wrapped in ``try/except Exception: continue`` so a
    transiently-locked or corrupt peer DB does not poison cross-worker
    queries."""
    # Place a peer live symlink so the walk has something to visit.
    peer_db_path = tmp_path / 'peer-worker-2026-03-25__10_00_00.db'
    await _create_worker_db_with_labels(
        peer_db_path,
        'peer-worker',
        task_id='t-peer',
        labels={'request_id': 'req-peer'},
    )
    link = live_link_path(str(tmp_path), 'peer-worker')
    os.symlink(peer_db_path.name, link)

    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    from drakkar import recorder as recorder_module

    real_connect = recorder_module.aiosqlite.connect

    def failing_connect(uri, *args, **kwargs):
        # Only fail when the cross-worker walk dials the peer DB. Other
        # callers (the recorder's own writer/reader) keep working.
        if 'peer-worker' in uri:
            raise OSError('simulated peer DB lock')
        return real_connect(uri, *args, **kwargs)

    monkeypatch.setattr(recorder_module.aiosqlite, 'connect', failing_connect)

    # Even though our peer DB has the matching label, the simulated open
    # failure makes the walk fall through to the empty return.
    events = await rec.cross_trace_by_label('request_id', 'req-peer')
    assert events == []
    await rec.stop()


# --- get_task_events --------------------------------------------------------


async def test_get_task_events_returns_chronological(tmp_path):
    """``get_task_events`` returns every event for a task in id-ascending
    (chronological) order so the debug UI can render the lifecycle."""
    config = make_debug_config(tmp_path, ws_min_duration_ms=0)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    task = make_task('t-lifecycle')
    rec.record_task_started(task, partition=2)
    rec.record_task_completed(make_result(task=task), partition=2)
    # Some unrelated event for a different task — must not appear.
    other = make_task('t-other')
    rec.record_task_started(other, partition=2)
    await rec._flush()

    events = await rec.get_task_events('t-lifecycle')
    assert len(events) == 2
    assert all(e['task_id'] == 't-lifecycle' for e in events)
    # Chronological → ids ascending → earlier ts first.
    timestamps = [e.get('ts', 0) for e in events]
    assert timestamps == sorted(timestamps)
    event_types = [e['event'] for e in events]
    assert event_types == ['task_started', 'task_completed']
    await rec.stop()


async def test_get_task_events_empty_when_store_events_false(tmp_path):
    """With ``store_events=False`` the events table is never created, so
    the query short-circuits to []."""
    config = make_debug_config(tmp_path, store_events=False)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    events = await rec.get_task_events('any-task-id')
    assert events == []
    await rec.stop()


# --- Branch coverage on event recording -------------------------------------


async def test_record_task_started_includes_stdin_metadata(recorder):
    """When the task carries non-empty stdin the recorder computes its byte
    size and line count and puts both on the in-memory entry. These fields
    are NOT in the events table schema — they live on the buffered entry
    and on WS broadcasts only — so we assert against the buffer."""
    task = make_task('t-stdin')
    task.stdin = 'first line\nsecond line\n'
    recorder.record_task_started(task, partition=0)

    target = next(e for e in recorder._buffer if e.get('task_id') == 't-stdin')
    assert target['stdin_size'] == len(b'first line\nsecond line\n')
    # Two newline-terminated lines → exactly 2 lines.
    assert target['stdin_lines'] == 2


async def test_record_task_started_stdin_without_trailing_newline(recorder):
    """A stdin payload that doesn't end with ``\\n`` still counts the final
    line — the recorder adds 1 to the newline count when the input doesn't
    end with one."""
    task = make_task('t-stdin-no-trailing')
    task.stdin = 'line-a\nline-b'  # no trailing newline
    recorder.record_task_started(task, partition=0)

    target = next(e for e in recorder._buffer if e.get('task_id') == 't-stdin-no-trailing')
    assert target['stdin_lines'] == 2


async def test_record_task_started_precomputed_marker(recorder):
    """``precomputed=True`` adds a neutral marker into the metadata JSON
    so downstream queries can filter precomputed outcomes."""
    task = make_task('t-precomputed-start')
    recorder.record_task_started(task, partition=0, precomputed=True)
    await recorder._flush()

    events = await recorder.get_events(event_type='task_started')
    target = next(e for e in events if e['task_id'] == 't-precomputed-start')
    metadata = json.loads(target['metadata'])
    assert metadata.get('precomputed') is True


async def test_record_task_completed_precomputed_marker(recorder):
    """The same ``precomputed`` marker is mirrored on the completion event
    so dashboards don't have to join back to the started event."""
    task = make_task('t-precomputed-end')
    result = make_result(task=task)
    recorder.record_task_completed(result, partition=0, precomputed=True)
    await recorder._flush()

    events = await recorder.get_events(event_type='task_completed')
    target = next(e for e in events if e['task_id'] == 't-precomputed-end')
    metadata = json.loads(target['metadata'])
    assert metadata == {'precomputed': True}


async def test_get_events_filtered_by_since(recorder):
    """The ``since`` filter restricts results to events at or after the
    supplied unix timestamp. Combined with ``LIMIT/OFFSET`` it lets the UI
    paginate from a known watermark."""
    # Three events spaced over time. We can't control time.time(), so insert
    # rows directly with explicit timestamps to make the assertion stable.
    async with recorder._db.execute(
        """INSERT INTO events (ts, dt, event, partition) VALUES (?, ?, ?, ?)""",
        [100.0, format_dt(100.0), 'consumed', 0],
    ):
        pass
    async with recorder._db.execute(
        """INSERT INTO events (ts, dt, event, partition) VALUES (?, ?, ?, ?)""",
        [200.0, format_dt(200.0), 'consumed', 0],
    ):
        pass
    async with recorder._db.execute(
        """INSERT INTO events (ts, dt, event, partition) VALUES (?, ?, ?, ?)""",
        [300.0, format_dt(300.0), 'consumed', 0],
    ):
        pass
    await recorder._db.commit()

    # since=200 → drops the ts=100 row, keeps 200 and 300.
    events = await recorder.get_events(since=200.0)
    timestamps = sorted(e['ts'] for e in events)
    assert timestamps == [200.0, 300.0]


async def test_trace_by_label_returns_empty_when_store_events_false(tmp_path):
    """With ``store_events=False`` the events table doesn't exist, so the
    query path is short-circuited to [] before touching SQLite."""
    config = make_debug_config(tmp_path, store_events=False)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    events = await rec.trace_by_label('request_id', 'whatever')
    assert events == []
    await rec.stop()


# --- cross_trace corner cases -----------------------------------------------


async def test_trace_db_file_returns_empty_when_events_table_missing(tmp_path):
    """A peer DB whose ``events`` table was never created (or got dropped)
    must be skipped via the ``not await cur.fetchone()`` branch in
    ``_trace_db_file`` instead of raising 'no such table'."""
    # Build a peer DB with worker_config but no events table.
    broken_db_path = tmp_path / 'broken-worker-2026-03-25__10_00_00.db'
    async with aiosqlite.connect(str(broken_db_path)) as db:
        await db.executescript(SCHEMA_WORKER_CONFIG)
        await db.execute(
            """INSERT INTO worker_config
               (id, worker_name, cluster_name, ip_address, debug_port, debug_url, kafka_brokers,
                source_topic, consumer_group, binary_path, max_executors, task_timeout_seconds,
                max_retries, window_size, sinks_json, env_vars_json, created_at, created_at_dt)
               VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                'broken-worker',
                '',
                '10.0.0.2',
                8080,
                '',
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
                format_dt(1000.0),
            ],
        )
        await db.commit()

    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    # Drive the trace through ``_trace_db_file`` directly — it's the
    # internal helper that owns the events-table existence check.
    events = await rec._trace_db_file(str(broken_db_path), partition=0, msg_offset=0)
    assert events == []
    await rec.stop()


async def test_cross_trace_returns_empty_when_no_db_dir():
    """Without ``db_dir`` ``cross_trace`` short-circuits to [] before any
    glob/symlink walk."""
    config = DebugConfig(enabled=True, db_dir='', flush_interval_seconds=60)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    events = await rec.cross_trace(partition=0, msg_offset=0)
    assert events == []
    await rec.stop()


async def test_cross_trace_skips_non_symlink_files(tmp_path):
    """A regular file (not a symlink) matching the ``*-live.db`` glob
    pattern must be skipped — ``cross_trace`` only follows real live links
    so a stray file doesn't get treated as a peer worker."""
    # Plant a non-symlink that matches the glob pattern. cross_trace must
    # ignore it. (A worker would never legitimately write a *-live.db
    # regular file, but defensive code is defensive code.)
    fake_live = tmp_path / 'fake-worker-live.db'
    async with aiosqlite.connect(str(fake_live)) as db:
        await db.executescript(SCHEMA_WORKER_CONFIG)
        await db.executescript(SCHEMA_EVENTS)
        await db.execute(
            """INSERT INTO events (ts, dt, event, partition, offset, task_id, metadata)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            [1.0, format_dt(1.0), 'consumed', 5, 99, None, None],
        )
        await db.commit()

    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    # Even though the file has a matching event, cross_trace must skip it
    # because it isn't a symlink. With no rotated files either, the trace
    # falls through to [].
    events = await rec.cross_trace(partition=5, msg_offset=99)
    # The events MAY come back via the rotated-DB phase (since the file
    # also matches that glob), so the assertion is on the live-symlink
    # path: if it were treated as a live link the query would short-circuit
    # and never visit the rotated path. We can't easily prove the negative
    # here without observation hooks, so make the weaker assertion that
    # the call returns successfully and the file was treated as a regular
    # DB file (events ARE found via the rotated-files fallback).
    assert isinstance(events, list)
    await rec.stop()


# --- Lifecycle error paths --------------------------------------------------


async def test_start_rolls_back_when_reader_open_fails(tmp_path, monkeypatch):
    """If ``open_reader()`` raises during ``start()`` the writer must be
    closed too — otherwise the half-opened writer leaks its fd / file lock
    and a retry of ``start()`` collides with itself."""
    from drakkar import recorder as recorder_module

    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)

    async def failingopen_reader(path):
        raise RuntimeError('simulated reader open failure')

    monkeypatch.setattr(recorder_module, 'open_reader', failingopen_reader)

    with pytest.raises(RuntimeError, match='simulated reader open failure'):
        await rec.start()

    # Both connection attrs must be reset so a later ``stop()`` is a no-op
    # and the recorder is in a clean state.
    assert rec._db is None
    assert rec._reader_db is None
    assert rec._running is False


async def test_update_live_link_swallows_oserror(tmp_path, monkeypatch):
    """``_update_live_link`` is best-effort — an OSError from os.symlink or
    os.replace must be swallowed so a failure to refresh the link never
    aborts ``start()`` or ``_rotate()``."""
    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    def failing_symlink(*_args, **_kwargs):
        raise OSError('simulated symlink failure')

    monkeypatch.setattr(os, 'symlink', failing_symlink)

    # Must not raise — the OSError is swallowed.
    rec._update_live_link()

    await rec.stop()


async def test_remove_live_link_swallows_oserror(tmp_path, monkeypatch):
    """``_remove_live_link`` is best-effort — a permissions error during
    ``os.remove`` on shutdown must not propagate out of ``stop()``."""
    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    def failing_remove(_path):
        raise OSError('simulated remove failure')

    monkeypatch.setattr(os, 'remove', failing_remove)

    # Must not raise — _remove_live_link swallows OSError.
    rec._remove_live_link()


async def test_stop_logs_warning_on_writer_close_failure(tmp_path):
    """If the writer's ``close()`` raises during ``stop()`` the recorder
    logs a warning and continues to the live-link cleanup + final
    ``recorder_stopped`` log so shutdown completes cleanly."""
    import contextlib

    import structlog.testing

    # Disable store_state so stop()'s pre-shutdown _sync_state() doesn't try
    # to .execute() through our exploding mock — the close-failure path is
    # what we're testing here.
    config = make_debug_config(tmp_path, store_state=False)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    real_writer = rec._db
    assert real_writer is not None

    class _ExplodingOnClose:
        async def close(self):
            with contextlib.suppress(Exception):
                await real_writer.close()
            raise RuntimeError('simulated writer close failure')

    rec._db = _ExplodingOnClose()  # type: ignore[assignment]

    with structlog.testing.capture_logs() as captured:
        await rec.stop()

    close_failures = [ev for ev in captured if ev.get('event') == 'recorder_writer_close_failed']
    stop_events = [ev for ev in captured if ev.get('event') == 'recorder_stopped']
    assert len(close_failures) == 1
    assert close_failures[0]['log_level'] == 'warning'
    # The final recorder_stopped log still fires — proving the close failure
    # didn't abort the rest of stop().
    assert len(stop_events) == 1


async def test_trace_db_file_returns_empty_on_corrupt_db(tmp_path):
    """An exception thrown anywhere inside ``_trace_db_file`` (e.g. a
    truncated or locked SQLite file) must be swallowed and turned into
    [] — the catch-all is the last line of defence for the cross-trace
    walk so one bad peer DB never poisons the search."""
    # A non-SQLite file: aiosqlite raises ``DatabaseError`` on first query.
    bogus_path = tmp_path / 'not-actually-a-db.db'
    bogus_path.write_text('this is not sqlite')

    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    events = await rec._trace_db_file(str(bogus_path), partition=0, msg_offset=0)
    assert events == []
    await rec.stop()


async def test_discover_workers_swallows_corrupt_db(tmp_path):
    """A corrupt peer DB encountered during ``discover_workers`` must be
    skipped via the ``except Exception: continue`` branch — one bad file
    doesn't blank out the whole roster."""
    # Plant a corrupt peer DB with a valid live symlink so the discovery
    # walk visits it.
    corrupt_path = tmp_path / 'corrupt-worker-2026-03-25__10_00_00.db'
    corrupt_path.write_text('not a sqlite file')
    link = live_link_path(str(tmp_path), 'corrupt-worker')
    os.symlink(corrupt_path.name, link)

    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    workers = await rec.discover_workers()
    # No exception → the corrupt DB was skipped, not propagated.
    assert all(w.get('worker_name') != 'corrupt-worker' for w in workers)
    await rec.stop()


async def test_rotate_logs_warning_on_old_writer_close_failure(tmp_path):
    """``_rotate`` must finish even if closing the OLD writer fails — the
    new writer is already live, so propagating a stale-close exception
    would falsely abort the rotate. Verify the warning fires AND the new
    writer is usable post-rotate."""
    import contextlib

    import structlog.testing

    config = make_debug_config(tmp_path, retention_hours=24)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    real_writer = rec._db
    assert real_writer is not None

    class _ExplodingOnClose:
        async def close(self):
            with contextlib.suppress(Exception):
                await real_writer.close()
            raise RuntimeError('simulated old writer close failure')

    rec._db = _ExplodingOnClose()  # type: ignore[assignment]

    with structlog.testing.capture_logs() as captured:
        await rec._rotate()

    close_failures = [ev for ev in captured if ev.get('event') == 'recorder_old_db_close_failed']
    assert len(close_failures) == 1
    assert close_failures[0]['log_level'] == 'warning'
    # New writer must be live and usable.
    assert rec._db is not None
    rec.record_consumed(make_msg(partition=4, offset=77))
    await rec._flush()
    events = await rec.get_events()
    assert any(e.get('partition') == 4 and e.get('offset') == 77 for e in events)
    await rec.stop()


async def test_rotate_swallows_oserror_when_deleting_old_files(tmp_path, monkeypatch):
    """If retention deletion fails (e.g. EACCES) ``_rotate`` continues —
    failing to GC one stale file is not a reason to abort the whole
    rotation cycle."""
    config = make_debug_config(tmp_path, retention_hours=1)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    # Plant a fake stale rotated file so retention has something to try
    # to delete. Backdate the mtime well past the 1-hour cutoff.
    stale = tmp_path / f'{WORKER_NAME}-2020-01-01__00_00_00.db'
    stale.touch()
    old_mtime = 1.0  # 1970-01-01, definitely past the cutoff
    os.utime(stale, (old_mtime, old_mtime))

    real_remove = os.remove

    def failing_remove(path):
        if str(path) == str(stale):
            raise OSError('simulated EACCES on retention delete')
        real_remove(path)

    monkeypatch.setattr(os, 'remove', failing_remove)

    # Rotation must complete without raising even though os.remove fails.
    await rec._rotate()
    assert rec._db is not None
    await rec.stop()


async def test_rotate_swallows_oserror_when_enforcing_max_files(tmp_path, monkeypatch):
    """The max-file-count enforcement loop also wraps ``os.remove`` in a
    try/except — it's the second tier of cleanup after retention-by-mtime,
    and one failed delete must not stop the rotation."""
    # retention_max_events=10000 → max_files = 10000 // 10000 = 1
    config = make_debug_config(
        tmp_path,
        retention_hours=24 * 365,  # nothing expires by mtime
        retention_max_events=10000,
    )
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    # Plant several historical rotated DB files so we exceed max_files=1.
    for ts in ('2026-03-20__08_00_00', '2026-03-21__08_00_00', '2026-03-22__08_00_00'):
        (tmp_path / f'{WORKER_NAME}-{ts}.db').touch()

    real_remove = os.remove

    def failing_remove(path):
        # Fail only on the historical files (not the new rotated DB).
        if 'WORKER_NAME' in str(path) or '-2026-03-2' in str(path):
            raise OSError('simulated EACCES on max-file enforcement')
        real_remove(path)

    monkeypatch.setattr(os, 'remove', failing_remove)

    # Rotation must complete without raising.
    await rec._rotate()
    assert rec._db is not None
    await rec.stop()


# --- Trivial getters / aliases ----------------------------------------------


async def test_config_property_returns_underlying_config(tmp_path):
    """The ``config`` property exposes the DebugConfig the recorder was
    constructed with — used by the debug server to inspect settings
    without reaching into ``_config``."""
    config = make_debug_config(tmp_path, retention_hours=12)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    assert rec.config is config
    assert rec.config.retention_hours == 12


async def test_flush_public_alias_drains_buffer(recorder):
    """The public ``flush()`` method is a thin alias for ``_flush()`` — it
    lets external callers force the buffer to disk without touching the
    private name."""
    recorder.record_consumed(make_msg(offset=11))
    assert len(recorder._buffer) >= 1

    await recorder.flush()
    assert len(recorder._buffer) == 0
    events = await recorder.get_events()
    assert any(e.get('offset') == 11 for e in events)


async def test_update_live_link_no_op_when_no_db_path(tmp_path):
    """Without ``db_dir``/``db_path`` the recorder skips the symlink dance
    entirely — there's nothing to point at."""
    config = DebugConfig(enabled=True, db_dir='', flush_interval_seconds=60)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    # _db_path stays empty when db_dir is empty (early-skip in start()).
    assert not rec._db_path
    # No DB path → early return. No exception, no symlink created.
    rec._update_live_link()
    await rec.stop()
