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
from drakkar.recorder import core as core_mod
from drakkar.recorder.core import _LIVE_RECORDERS
from tests.conftest import wait_for
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


# --- Chunked flush -----------------------------------------------------


async def test_flush_writes_in_bounded_chunks(tmp_path, monkeypatch):
    """The whole buffer used to become row tuples in one comprehension, so a
    flush holding tens of thousands of events stalled the loop for tens of
    milliseconds. Chunking bounds that work and hands the loop back on each
    chunk's write.
    """
    monkeypatch.setattr(core_mod, 'FLUSH_CHUNK_ROWS', 100)
    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()
    try:
        for offset in range(250):
            rec.record_committed(partition=0, offset=offset)

        assert rec._db is not None
        original = rec._db.executemany
        chunk_sizes: list[int] = []

        async def counting_executemany(query, rows):
            rows = list(rows)
            chunk_sizes.append(len(rows))
            return await original(query, rows)

        rec._db.executemany = counting_executemany  # type: ignore[method-assign]

        await rec._flush()

        assert chunk_sizes == [100, 100, 50]
        assert len(rec._buffer) == 0
        events = await rec.get_events(partition=0, limit=1000)
        assert len(events) == 250
    finally:
        await rec.stop()


async def test_flush_ignores_events_appended_while_it_runs(tmp_path, monkeypatch):
    """The chunk loop works off the depth it saw on entry, so a producer
    faster than the writer cannot hold one flush open indefinitely.
    """
    monkeypatch.setattr(core_mod, 'FLUSH_CHUNK_ROWS', 2)
    config = make_debug_config(tmp_path)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()
    try:
        for offset in range(4):
            rec.record_committed(partition=0, offset=offset)

        assert rec._db is not None
        original = rec._db.executemany

        async def appending_executemany(query, rows):
            # A concurrent record() landing mid-flush, every chunk.
            rec.record_committed(partition=1, offset=99)
            return await original(query, rows)

        rec._db.executemany = appending_executemany  # type: ignore[method-assign]

        await rec._flush()

        # Two chunks ran, each appending one event; both wait for the next tick.
        assert len(rec._buffer) == 2
    finally:
        await rec.stop()


async def test_flush_failure_drops_only_one_chunk(tmp_path, monkeypatch):
    """The chunk is the unit of loss. Before chunking, a database that stayed
    unavailable for ``max_flush_retries`` ticks cost the entire buffer at
    once — on a busy worker that is every event held.
    """
    from drakkar.metrics import recorder_flush_batches_dropped

    monkeypatch.setattr(core_mod, 'FLUSH_CHUNK_ROWS', 10)
    config = make_debug_config(tmp_path, max_flush_retries=2)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()
    try:
        for offset in range(30):
            rec.record_committed(partition=0, offset=offset)

        assert rec._db is not None

        async def always_failing(query, rows):
            raise sqlite3.OperationalError('disk I/O error')

        rec._db.executemany = always_failing  # type: ignore[method-assign]
        dropped_before = recorder_flush_batches_dropped._value.get()

        # First failure: the chunk goes back, nothing is lost yet.
        await rec._flush()
        assert len(rec._buffer) == 30
        assert recorder_flush_batches_dropped._value.get() == dropped_before

        # Second failure hits the cap: exactly one chunk is dropped.
        await rec._flush()
        assert len(rec._buffer) == 20
        assert recorder_flush_batches_dropped._value.get() == dropped_before + 1
        # The oldest events went; the rest kept their order.
        assert [event['offset'] for event in rec._buffer][:3] == [10, 11, 12]
    finally:
        await rec.stop()


async def test_flush_loop_wakes_early_when_the_buffer_is_half_full(tmp_path):
    """A burst must not sit in the buffer until the next interval — at a high
    event rate the buffer would reach ``max_buffer`` and start evicting.
    """
    config = make_debug_config(tmp_path, flush_interval_seconds=600, max_buffer=1000)
    rec = EventRecorder(config, worker_name=WORKER_NAME)
    await rec.start()
    try:
        for offset in range(600):
            rec.record_committed(partition=0, offset=offset)

        # Nothing has been written yet: the interval is ten minutes away.
        await wait_for(lambda: len(rec._buffer) == 0, timeout=5.0, interval=0.05)
        events = await rec.get_events(partition=0, limit=1000)
        assert len(events) == 600
    finally:
        await rec.stop()
