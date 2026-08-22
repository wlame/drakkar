"""Tests for recorder flush interruption safety and shutdown resilience.

Covers the paths where a flush is interrupted (cancellation, non-operational
SQLite errors) and where ``stop()`` must finish teardown despite failures in
the final flush or state sync.
"""

import asyncio
import os
import sqlite3
from collections import deque

import pytest

from drakkar.recorder import EventRecorder, live_link_path
from drakkar.recorder.core import _LIVE_RECORDERS
from tests.test_recorder import (
    WORKER_NAME,
    _create_worker_db_with_labels,
    make_debug_config,
    make_msg,
)


async def test_flush_cancelled_mid_write_requeues_batch(tmp_path):
    """Cancelling a flush mid-executemany re-queues the popped batch.

    The batch is snapshot-popped before the DB write; a CancelledError at the
    write await (e.g. a caller's ``wait_for`` timing out, or ``stop()``
    cancelling the flush task) must put it back at the FRONT of the buffer in
    order, so a later flush persists it — not silently drop it.
    """
    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()
    entered = asyncio.Event()
    release = asyncio.Event()
    try:
        rec.record_consumed(make_msg(partition=0, offset=0))
        rec.record_consumed(make_msg(partition=0, offset=1))
        assert len(rec._buffer) == 2

        assert rec._db is not None
        original_executemany = rec._db.executemany

        async def blocking_executemany(query, rows):
            # Park the flush at a known await point so the test can cancel
            # it deterministically mid-write.
            entered.set()
            await release.wait()
            return await original_executemany(query, rows)

        rec._db.executemany = blocking_executemany  # type: ignore[method-assign]

        flush_task = asyncio.create_task(rec._flush())
        await entered.wait()
        flush_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await flush_task

        # The batch is back in the buffer, FIFO order preserved.
        assert [e['offset'] for e in rec._buffer] == [0, 1]

        # A later flush (write restored) persists the events exactly once.
        rec._db.executemany = original_executemany  # type: ignore[method-assign]
        await rec._flush()
        assert len(rec._buffer) == 0
        events = await rec.get_events(partition=0)
        assert sorted(ev['offset'] for ev in events) == [0, 1]
    finally:
        release.set()
        await rec.stop()


async def test_flush_cancelled_requeue_overflow_ticks_counter(tmp_path):
    """The cancellation re-queue path shares the overflow accounting.

    If concurrent appends filled the bounded buffer while the flush was
    parked at the write await, the re-queue evicts rows from the tail —
    that loss must tick ``recorder_requeue_overflow`` exactly as the
    error-retry re-queue does.
    """
    from drakkar.metrics import recorder_requeue_overflow

    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    # Shrink post-construction so overflow is cheap to trigger.
    rec._buffer = deque(maxlen=3)
    await rec.start()
    entered = asyncio.Event()
    release = asyncio.Event()
    try:
        for off in range(3):
            rec.record_consumed(make_msg(partition=0, offset=off))
        assert len(rec._buffer) == 3

        assert rec._db is not None

        async def blocking_executemany(query, rows):
            entered.set()
            await release.wait()

        rec._db.executemany = blocking_executemany  # type: ignore[method-assign]

        overflow_before = recorder_requeue_overflow._value.get()

        flush_task = asyncio.create_task(rec._flush())
        await entered.wait()
        # Concurrent appends land while the 3-row batch is popped out.
        rec.record_consumed(make_msg(partition=0, offset=100))
        rec.record_consumed(make_msg(partition=0, offset=101))
        flush_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await flush_task

        # Re-queue of 3 rows into a maxlen-3 deque holding 2 → 2 evicted.
        assert recorder_requeue_overflow._value.get() == overflow_before + 2
        assert len(rec._buffer) == 3
    finally:
        release.set()
        await rec.stop()


@pytest.mark.parametrize(
    'exc_type',
    [sqlite3.IntegrityError, sqlite3.ProgrammingError, sqlite3.DatabaseError],
)
async def test_flush_non_operational_sqlite_error_requeues_batch(tmp_path, exc_type):
    """Every ``sqlite3.Error`` subtype routes through the retry machinery.

    Previously only ``OperationalError`` was caught; an IntegrityError /
    ProgrammingError / DatabaseError escaped the handler and bypassed the
    re-queue and retry accounting entirely.
    """
    from drakkar.metrics import recorder_flush_retries

    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()
    try:
        rec.record_consumed(make_msg(partition=0, offset=0))
        rec.record_consumed(make_msg(partition=0, offset=1))

        assert rec._db is not None
        original_executemany = rec._db.executemany
        calls = {'n': 0}

        async def failing_once(query, rows):
            calls['n'] += 1
            if calls['n'] == 1:
                raise exc_type('simulated failure')
            return await original_executemany(query, rows)

        rec._db.executemany = failing_once  # type: ignore[method-assign]

        retries_before = recorder_flush_retries._value.get()

        # First flush: handled, re-queued, retry counter ticked.
        await rec._flush()
        assert [e['offset'] for e in rec._buffer] == [0, 1]
        assert rec._flush_failures == 1
        assert recorder_flush_retries._value.get() == retries_before + 1

        # Second flush succeeds and resets the counter.
        await rec._flush()
        assert len(rec._buffer) == 0
        assert rec._flush_failures == 0
        events = await rec.get_events(partition=0)
        assert sorted(ev['offset'] for ev in events) == [0, 1]
    finally:
        await rec.stop()


async def test_stop_completes_teardown_when_final_flush_fails(tmp_path):
    """A failing final flush must not skip the closes and live-set cleanup."""
    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()
    assert rec in _LIVE_RECORDERS

    async def exploding_flush():
        raise RuntimeError('simulated flush failure')

    rec._flush = exploding_flush  # type: ignore[method-assign]

    await rec.stop()

    # Teardown ran to completion despite the flush error.
    assert rec._db is None
    assert rec._reader_db is None
    assert rec not in _LIVE_RECORDERS
    assert not os.path.lexists(live_link_path(str(tmp_path), WORKER_NAME))


async def test_stop_completes_teardown_when_state_sync_fails(tmp_path):
    """A failing final state sync must not skip the closes either."""
    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()

    async def exploding_sync():
        raise RuntimeError('simulated sync failure')

    rec._sync_state = exploding_sync  # type: ignore[method-assign]

    await rec.stop()

    assert rec._db is None
    assert rec._reader_db is None
    assert rec not in _LIVE_RECORDERS


async def test_cross_trace_by_label_respects_scan_budget(tmp_path, monkeypatch):
    """A zero-file scan budget stops the peer sweep before any DB is opened,
    even when a peer holds a matching label."""
    from drakkar.recorder import core as recorder_core

    peer_db_path = tmp_path / 'other-worker-2026-03-25__10_00_00.db'
    await _create_worker_db_with_labels(
        peer_db_path,
        'other-worker',
        task_id='t-remote',
        labels={'request_id': 'req-remote'},
    )
    os.symlink(peer_db_path.name, live_link_path(str(tmp_path), 'other-worker'))

    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()
    try:
        monkeypatch.setattr(recorder_core, 'CROSS_TRACE_MAX_FILES', 0)
        events = await rec.cross_trace_by_label('request_id', 'req-remote')
        assert events == []
    finally:
        await rec.stop()
