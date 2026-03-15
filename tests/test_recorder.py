"""Tests for Drakkar flight recorder."""

import asyncio
import time

import pytest

from drakkar.config import DebugConfig
from drakkar.models import (
    ExecutorError,
    ExecutorResult,
    ExecutorTask,
    OutputMessage,
    SourceMessage,
)
from drakkar.recorder import EventRecorder


def make_debug_config(tmp_path, **overrides) -> DebugConfig:
    defaults = {
        'enabled': True,
        'db_path': str(tmp_path / "test-debug.db"),
        'retention_hours': 24,
        'retention_max_events': 1000,
        'store_output': False,
        'flush_interval_seconds': 60,
    }
    defaults.update(overrides)
    return DebugConfig(**defaults)


def make_msg(partition=0, offset=0) -> SourceMessage:
    return SourceMessage(
        topic="t", partition=partition, offset=offset,
        value=b'{"x":1}', timestamp=1000,
    )


def make_task(task_id="t1", args=None, offsets=None) -> ExecutorTask:
    return ExecutorTask(
        task_id=task_id,
        args=args or ["--input", "file.txt"],
        source_offsets=offsets or [0],
    )


def make_result(task_id="t1", task=None) -> ExecutorResult:
    t = task or make_task(task_id)
    return ExecutorResult(
        task_id=task_id, exit_code=0,
        stdout="line1\nline2\n", stderr="",
        duration_seconds=1.5, task=t,
    )


@pytest.fixture
async def recorder(tmp_path):
    config = make_debug_config(tmp_path)
    rec = EventRecorder(config)
    await rec.start()
    yield rec
    await rec.stop()


async def test_start_creates_db(tmp_path):
    config = make_debug_config(tmp_path)
    rec = EventRecorder(config)
    await rec.start()
    assert rec._db is not None
    await rec.stop()
    assert rec._db is None


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
    tasks = [make_task("t1", offsets=[10]), make_task("t2", offsets=[11])]
    recorder.record_arranged(partition=0, messages=msgs, tasks=tasks)
    await recorder._flush()

    events = await recorder.get_events(event_type='arranged')
    assert len(events) == 1
    import json
    meta = json.loads(events[0]['metadata'])
    assert meta['task_count'] == 2
    assert meta['offsets'] == [10, 11]


async def test_record_task_started(recorder):
    task = make_task("t1", args=["--pattern", "error"])
    recorder.record_task_started(task, partition=5)
    await recorder._flush()

    events = await recorder.get_events(event_type='task_started')
    assert len(events) == 1
    assert events[0]['task_id'] == "t1"
    assert events[0]['partition'] == 5
    import json
    assert json.loads(events[0]['args']) == ["--pattern", "error"]


async def test_record_task_completed_without_output(recorder):
    result = make_result("t1")
    recorder.record_task_completed(result, partition=2)
    await recorder._flush()

    events = await recorder.get_events(event_type='task_completed')
    assert len(events) == 1
    assert events[0]['exit_code'] == 0
    assert events[0]['duration'] == 1.5
    assert events[0]['stdout_size'] == len("line1\nline2\n".encode())
    assert events[0]['stdout'] is None  # store_output=False


async def test_record_task_completed_with_output(tmp_path):
    config = make_debug_config(tmp_path, store_output=True)
    rec = EventRecorder(config)
    await rec.start()

    result = make_result("t1")
    rec.record_task_completed(result, partition=0)
    await rec._flush()

    events = await rec.get_events(event_type='task_completed')
    assert events[0]['stdout'] == "line1\nline2\n"
    await rec.stop()


async def test_record_task_failed(recorder):
    task = make_task("t1")
    error = ExecutorError(task=task, exit_code=1, stderr="bad input")
    recorder.record_task_failed(task, error, partition=0)
    await recorder._flush()

    events = await recorder.get_events(event_type='task_failed')
    assert len(events) == 1
    assert events[0]['exit_code'] == 1


async def test_record_produced(recorder):
    msg = OutputMessage(key=b"k", value=b"v")
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
    task = make_task("t-42", offsets=[42])
    recorder.record_task_started(task, partition=3)
    recorder.record_task_completed(make_result("t-42", task=task), partition=3)
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
    task = make_task("t1", offsets=[0])
    recorder.record_task_completed(make_result("t1", task), partition=0)
    await recorder._flush()

    summary = await recorder.get_partition_summary()
    assert len(summary) == 2
    p0 = next(s for s in summary if s['partition'] == 0)
    assert p0['consumed_count'] == 2
    assert p0['completed_count'] == 1
    assert p0['last_committed_offset'] == 2


async def test_get_active_tasks(recorder):
    recorder.record_task_started(make_task("t1"), partition=0)
    recorder.record_task_started(make_task("t2"), partition=0)
    recorder.record_task_completed(make_result("t1"), partition=0)
    await recorder._flush()

    active = await recorder.get_active_tasks()
    assert len(active) == 1
    assert active[0]['task_id'] == "t2"


async def test_get_stats(recorder):
    recorder.record_consumed(make_msg(offset=0))
    recorder.record_consumed(make_msg(offset=1))
    task = make_task("t1")
    recorder.record_task_completed(make_result("t1", task), partition=0)
    recorder.record_committed(partition=0, offset=2)
    await recorder._flush()

    stats = await recorder.get_stats()
    assert stats['total_events'] == 4
    assert stats['consumed'] == 2
    assert stats['completed'] == 1
    assert stats['committed'] == 1


async def test_retention_by_time(tmp_path):
    config = make_debug_config(tmp_path, retention_hours=0)  # expire everything
    rec = EventRecorder(config)
    await rec.start()

    rec.record_consumed(make_msg(offset=0))
    await rec._flush()

    events_before = await rec.get_events()
    assert len(events_before) == 1

    await rec._cleanup()

    events_after = await rec.get_events()
    assert len(events_after) == 0
    await rec.stop()


async def test_retention_by_count(tmp_path):
    config = make_debug_config(tmp_path, retention_max_events=5)
    rec = EventRecorder(config)
    await rec.start()

    for i in range(10):
        rec.record_consumed(make_msg(offset=i))
    await rec._flush()

    await rec._cleanup()

    events = await rec.get_events(limit=20)
    assert len(events) == 5
    await rec.stop()


async def test_get_stats_empty_db(recorder):
    stats = await recorder.get_stats()
    assert stats['total_events'] == 0


async def test_get_events_no_db():
    config = DebugConfig(enabled=True, db_path="/tmp/nonexistent.db")
    rec = EventRecorder(config)
    # don't start — db is None
    events = await rec.get_events()
    assert events == []
