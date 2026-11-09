"""Unit tests for the recorder's write side.

:class:`EventWriter` builds rows and hands them to an abstract ``_record``.
That makes it testable with no database, no flush loop and no rotation —
the harness below collects rows in a list and asserts on their shape.
The persistence half is covered by ``tests/test_recorder.py``.
"""

from __future__ import annotations

import json

import pytest

from drakkar.config import RecorderConfig
from drakkar.models import ExecutorError, ExecutorResult, ExecutorTask, SourceMessage
from drakkar.recorder.writer import EventWriter


class CollectingWriter(EventWriter):
    """An EventWriter whose sink is a list — the whole contract a subclass owes."""

    def __init__(self, config: RecorderConfig) -> None:
        super().__init__(config, ws_min_duration_ms=500, log_min_duration_ms=500)
        self.rows: list[tuple[dict, bool, bool]] = []

    def _record(self, event: dict, *, skip_ws: bool = False, skip_db: bool = False) -> None:
        event['dt'] = 'stub-dt'
        self.rows.append((event, skip_ws, skip_db))

    @property
    def last(self) -> dict:
        return self.rows[-1][0]

    def last_metadata(self) -> dict:
        return json.loads(self.last['metadata'])


@pytest.fixture
def writer():
    return CollectingWriter(RecorderConfig())


def make_task(task_id='t1', **kwargs) -> ExecutorTask:
    kwargs.setdefault('source_offsets', [1])
    return ExecutorTask(task_id=task_id, **kwargs)


def make_message(partition=0, offset=10) -> SourceMessage:
    return SourceMessage(topic='orders', partition=partition, offset=offset, value=b'{}', timestamp=0)


class TestWriterIsIndependentOfStorage:
    def test_a_writer_needs_no_database(self, writer):
        """The point of the split: rows can be built with nothing open."""
        writer.record_committed(partition=0, offset=5)

        assert writer.last['event'] == 'committed'

    def test_record_is_abstract_on_the_base_class(self):
        base = EventWriter(RecorderConfig(), ws_min_duration_ms=500, log_min_duration_ms=500)

        with pytest.raises(NotImplementedError):
            base._record({'ts': 0.0, 'event': 'x'})


class TestCounters:
    def test_counters_track_the_lifecycle_events(self, writer):
        writer.record_consumed(make_message())
        writer.record_committed(partition=0, offset=10)

        assert writer.counters['consumed'] == 1
        assert writer.counters['committed'] == 1
        assert writer.counters['failed'] == 0

    def test_counters_are_a_copy_not_the_live_dict(self, writer):
        snapshot = writer.counters
        writer.record_consumed(make_message())

        assert snapshot['consumed'] == 0


class TestRowShapes:
    def test_consumed_row_carries_partition_and_offset(self, writer):
        writer.record_consumed(make_message(partition=3, offset=99))

        row = writer.last
        assert row['event'] == 'consumed'
        assert row['partition'] == 3
        assert row['offset'] == 99

    def test_sink_delivery_metadata_is_json(self, writer):
        writer.record_sink_delivery(
            sink_type='postgres',
            sink_name='order_events_writer',
            payload_count=7,
            duration=0.123456,
        )

        meta = writer.last_metadata()
        assert meta == {
            'sink_type': 'postgres',
            'sink_name': 'order_events_writer',
            'payload_count': 7,
            'duration': 0.1235,
        }

    def test_partition_stalled_names_the_partition(self, writer):
        writer.record_partition_stalled(partition=4)

        assert writer.last['event'] == 'partition_stalled'
        assert writer.last['partition'] == 4

    def test_assigned_and_revoked_emit_one_row_per_partition(self, writer):
        writer.record_assigned([0, 1])
        writer.record_revoked([0, 1])

        assert [(row['event'], row['partition']) for row, _, _ in writer.rows] == [
            ('assigned', 0),
            ('assigned', 1),
            ('revoked', 0),
            ('revoked', 1),
        ]

    def test_record_periodic_run_with_slot_adds_scope_and_slot(self, writer):
        writer.record_periodic_run('refresh_rates', duration=1.5, status='ok', slot=1_800_000_030)

        assert writer.last['event'] == 'periodic_run'
        assert writer.last['metadata'] == '{"scope":"cluster","slot":1800000030,"status":"ok"}'

    def test_record_periodic_run_without_slot_keeps_its_bytes(self, writer):
        writer.record_periodic_run('refresh_rates', duration=1.5, status='ok')

        assert writer.last['metadata'] == '{"status":"ok"}'


class TestDeferredStartEvents:
    """Deferral arms a timer on the running loop, so these are async."""

    async def test_start_is_held_back_when_a_threshold_is_set(self, writer):
        writer._ws_min_duration_ms = 100

        writer.record_task_started(make_task(), partition=0)

        _row, skip_ws, _skip_db = writer.rows[-1]
        assert skip_ws is True, 'the start event goes to the DB but not (yet) to the live stream'
        assert 't1' in writer.fanout.deferred

    def test_start_goes_straight_out_without_a_threshold(self, writer):
        writer._ws_min_duration_ms = 0

        writer.record_task_started(make_task(), partition=0)

        _row, skip_ws, _skip_db = writer.rows[-1]
        assert skip_ws is False
        assert not writer.fanout.deferred

    async def test_a_fast_task_suppresses_both_start_and_completion(self, writer):
        writer._ws_min_duration_ms = 100
        task = make_task()
        writer.record_task_started(task, partition=0)

        writer.record_task_completed(
            ExecutorResult(task=task, stdout='', stderr='', exit_code=0, duration_seconds=0.001),
            partition=0,
        )

        _row, skip_ws, _skip_db = writer.rows[-1]
        assert skip_ws is True, 'a task nobody could see start must not appear finishing'
        assert not writer.fanout.deferred

    async def test_a_failure_releases_the_held_start_first(self, writer):
        writer._ws_min_duration_ms = 100
        sub = writer.subscribe()
        task = make_task()
        writer.record_task_started(task, partition=0)

        writer.record_task_failed(
            task,
            ExecutorError(task=task, exception='boom'),
            partition=0,
            duration_seconds=0.001,
        )

        # A failure always reaches the live stream, so the start it was
        # holding must be released first — the UI needs the whole sequence.
        # (The failure row itself reaches the stream through ``_record``,
        # which this harness collects instead of broadcasting.)
        assert [event['event'] for event in _drain(sub)] == ['task_started']
        _row, skip_ws, _skip_db = writer.rows[-1]
        assert writer.last['event'] == 'task_failed'
        assert skip_ws is False, 'failures are never suppressed by ws_min_duration_ms'


def _drain(sub) -> list[dict]:
    out = []
    while not sub.empty():
        out.append(sub.get_nowait())
    return out


@pytest.mark.parametrize('kind', ['nonzero_exit', 'timeout', 'launch_failure', 'internal'])
def test_a_failed_task_row_stores_the_failure_kind(writer, kind):
    task = make_task()

    writer.record_task_failed(task, ExecutorError(task=task, kind=kind, exit_code=None), partition=0)

    assert writer.last_metadata()['kind'] == kind


def test_a_failed_task_row_stores_the_source_offsets(writer):
    task = make_task(source_offsets=[12, 10, 11])

    writer.record_task_failed(task, ExecutorError(task=task), partition=0)

    assert writer.last_metadata()['source_offsets'] == [12, 10, 11]
    assert 'offset' not in writer.last


def test_a_failed_http_task_row_has_no_source_offsets(writer):
    task = make_task(source_offsets=[0])

    writer.record_task_failed(task, ExecutorError(task=task), partition=-1, origin='http')

    assert 'source_offsets' not in writer.last_metadata()
