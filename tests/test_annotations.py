"""Tests for handler annotations."""

import json

import pytest
from structlog.testing import capture_logs

from drakkar import metrics
from drakkar.annotations import (
    MAX_DROP_LOGS_PER_CALL,
    REASON_BUDGET_EXHAUSTED,
    REASON_NO_CONTEXT,
    REASON_OVERSIZE,
    REASON_UNSERIALIZABLE,
    Annotator,
    NoOpAnnotator,
)
from drakkar.executor import ExecutorPool
from drakkar.handler import BaseDrakkarHandler
from drakkar.hookctx import bind_hook_context, clear_hook_context, current_hook_context
from drakkar.models import ExecutorTask, SourceMessage
from drakkar.partition import PartitionProcessor
from drakkar.recorder import EventRecorder
from tests.conftest import make_ui_config, wait_for


class FakeRecorder:
    """Captures ``record_annotation`` calls without touching SQLite."""

    def __init__(self) -> None:
        self.calls: list[dict] = []

    def record_annotation(
        self,
        *,
        kind: str,
        partition: int,
        metadata_json: str,
        offset: int | None = None,
        task_id: str | None = None,
        labels: dict[str, str] | None = None,
    ) -> None:
        self.calls.append(
            {
                'kind': kind,
                'partition': partition,
                'metadata': json.loads(metadata_json),
                'offset': offset,
                'task_id': task_id,
                'labels': labels,
            }
        )


@pytest.fixture
def recorder() -> FakeRecorder:
    return FakeRecorder()


@pytest.fixture
def annotator(recorder: FakeRecorder) -> Annotator:
    return Annotator(recorder)


@pytest.fixture
def in_arrange_hook():
    """Bind a window-scoped hook context for the duration of a test."""
    token = bind_hook_context(hook='arrange', partition=4, window_id=7, offsets=(90, 91))
    yield
    clear_hook_context(token)


def dropped_count(reason: str) -> float:
    return metrics.annotations_dropped.labels(reason=reason)._value.get()  # type: ignore[attr-defined]


def recorded_count() -> float:
    return metrics.annotations_recorded._value.get()  # type: ignore[attr-defined]


# --- accepted path ---------------------------------------------------------


def test_emit_with_message_target_anchors_on_offset(
    annotator: Annotator, recorder: FakeRecorder, source_message: SourceMessage, in_arrange_hook
):
    annotator.emit(source_message, 'input_selection', {'candidates': ['a', 'b']})

    assert len(recorder.calls) == 1
    call = recorder.calls[0]
    assert call['offset'] == source_message.offset
    assert call['task_id'] is None
    assert call['partition'] == 4
    assert call['metadata']['scope'] == 'message'
    assert call['metadata']['kind'] == 'input_selection'
    assert call['metadata']['data'] == {'candidates': ['a', 'b']}


def test_emit_with_task_target_anchors_on_task_id(
    annotator: Annotator, recorder: FakeRecorder, executor_task: ExecutorTask, in_arrange_hook
):
    annotator.emit(executor_task, 'arg_derivation', {'template': 'x'})

    call = recorder.calls[0]
    assert call['offset'] is None
    assert call['task_id'] == executor_task.task_id
    assert call['metadata']['scope'] == 'task'


def test_emit_with_no_target_is_window_scoped_and_carries_offsets(
    annotator: Annotator, recorder: FakeRecorder, in_arrange_hook
):
    annotator.emit(None, 'window_summary', {'dropped': 12})

    call = recorder.calls[0]
    assert call['offset'] is None
    assert call['task_id'] is None
    assert call['metadata']['scope'] == 'window'
    # Window rows have no anchor column, so the trace query matches on these.
    assert call['metadata']['offsets'] == [90, 91]
    assert call['metadata']['window_id'] == 7


def test_emit_records_hook_name_and_labels(annotator: Annotator, recorder: FakeRecorder, in_arrange_hook):
    annotator.emit(None, 'k', {'a': 1}, labels={'request_id': 'abc'})

    call = recorder.calls[0]
    assert call['metadata']['hook'] == 'arrange'
    assert call['labels'] == {'request_id': 'abc'}


def test_emit_without_data_records_empty_payload(annotator: Annotator, recorder: FakeRecorder, in_arrange_hook):
    annotator.emit(None, 'marker')

    assert recorder.calls[0]['metadata']['data'] == {}


def test_emit_accumulates_accepted_bytes_on_the_context(annotator: Annotator, in_arrange_hook):
    annotator.emit(None, 'k', {'a': 1})
    ctx = current_hook_context()

    assert ctx is not None
    assert ctx.accepted_bytes > 0
    assert ctx.drops == 0


def test_emit_increments_recorded_counter(annotator: Annotator, in_arrange_hook):
    before = recorded_count()
    annotator.emit(None, 'k', {'a': 1})

    assert recorded_count() == before + 1


# --- disabled / no-op paths ------------------------------------------------


def test_disabled_annotator_records_nothing(recorder: FakeRecorder, in_arrange_hook):
    annotator = Annotator(recorder, enabled=False)
    before = dropped_count(REASON_OVERSIZE)

    annotator.emit(None, 'k', {'a': 1})

    assert recorder.calls == []
    # Disabled is a configuration state, not a drop — nothing is counted.
    assert dropped_count(REASON_OVERSIZE) == before


def test_noop_annotator_accepts_every_call_shape(source_message: SourceMessage):
    annotator = NoOpAnnotator()

    annotator.emit(source_message, 'k', {'a': 1}, labels={'x': 'y'})
    annotator.emit(None, 'k')


# --- drop paths ------------------------------------------------------------


def test_emit_outside_hook_context_drops_and_logs_once(annotator: Annotator, recorder: FakeRecorder):
    before = dropped_count(REASON_NO_CONTEXT)

    with capture_logs() as cap:
        annotator.emit(None, 'k', {'a': 1})
        annotator.emit(None, 'k', {'a': 2})

    assert recorder.calls == []
    assert dropped_count(REASON_NO_CONTEXT) == before + 2
    # A static code bug: logged loudly once, counted thereafter.
    dropped_logs = [e for e in cap if e['event'] == 'annotation_dropped']
    assert len(dropped_logs) == 1
    assert dropped_logs[0]['reason'] == REASON_NO_CONTEXT


def test_oversize_payload_is_dropped_whole_not_truncated(recorder: FakeRecorder, in_arrange_hook):
    annotator = Annotator(recorder, max_bytes=200)
    before = dropped_count(REASON_OVERSIZE)

    annotator.emit(None, 'k', {'blob': 'x' * 5000})

    assert recorder.calls == []
    assert dropped_count(REASON_OVERSIZE) == before + 1


def test_oversize_drop_does_not_consume_the_per_call_byte_budget(recorder: FakeRecorder, in_arrange_hook):
    # The reason the two budgets are independent: a rejected record must
    # never cost a well-formed one its place.
    annotator = Annotator(recorder, max_bytes=200, max_bytes_per_call=1000)

    for _ in range(20):
        annotator.emit(None, 'k', {'blob': 'x' * 5000})
    annotator.emit(None, 'k', {'small': 1})

    assert len(recorder.calls) == 1
    assert recorder.calls[0]['metadata']['data'] == {'small': 1}


def test_per_call_budget_exhaustion_drops_further_annotations(recorder: FakeRecorder, in_arrange_hook):
    annotator = Annotator(recorder, max_bytes=0, max_bytes_per_call=300)
    before = dropped_count(REASON_BUDGET_EXHAUSTED)

    for i in range(20):
        annotator.emit(None, 'k', {'i': i, 'pad': 'y' * 50})

    assert 0 < len(recorder.calls) < 20
    assert dropped_count(REASON_BUDGET_EXHAUSTED) > before


def test_zero_limits_disable_the_budgets(recorder: FakeRecorder, in_arrange_hook):
    annotator = Annotator(recorder, max_bytes=0, max_bytes_per_call=0)

    annotator.emit(None, 'k', {'blob': 'x' * 100_000})

    assert len(recorder.calls) == 1


def test_unserializable_payload_is_dropped(recorder: FakeRecorder, in_arrange_hook):
    annotator = Annotator(recorder)
    circular: dict = {}
    circular['self'] = circular
    before = dropped_count(REASON_UNSERIALIZABLE)

    annotator.emit(None, 'k', circular)

    assert recorder.calls == []
    assert dropped_count(REASON_UNSERIALIZABLE) == before + 1


def test_emit_never_raises_into_handler_code(annotator: Annotator, in_arrange_hook):
    circular: dict = {}
    circular['self'] = circular

    # No pytest.raises — the point is that nothing escapes.
    annotator.emit(None, 'k', circular)


# --- log budget ------------------------------------------------------------


def test_drop_logging_suppresses_after_budget_but_metric_keeps_counting(recorder: FakeRecorder, in_arrange_hook):
    annotator = Annotator(recorder, max_bytes=50)
    attempts = MAX_DROP_LOGS_PER_CALL + 7
    before = dropped_count(REASON_OVERSIZE)

    with capture_logs() as cap:
        for _ in range(attempts):
            annotator.emit(None, 'k', {'blob': 'x' * 500})

    dropped_logs = [e for e in cap if e['event'] == 'annotation_dropped']
    suppressed = [e for e in cap if e['event'] == 'annotation_drops_suppressed']
    assert len(dropped_logs) == MAX_DROP_LOGS_PER_CALL
    assert len(suppressed) == 1
    assert dropped_count(REASON_OVERSIZE) == before + attempts


def test_drop_log_payload_is_capped_and_marked(recorder: FakeRecorder, in_arrange_hook):
    annotator = Annotator(recorder, max_bytes=100, log_max_bytes=64)

    with capture_logs() as cap:
        annotator.emit(None, 'k', {'blob': 'x' * 5000})

    entry = next(e for e in cap if e['event'] == 'annotation_dropped')
    assert entry['data_truncated'] is True
    assert len(entry['data'].encode('utf-8')) <= 64
    # The full size stays visible even though the copy was cut.
    assert entry['size_bytes'] > 5000


def test_drop_log_payload_uncapped_when_limit_is_zero(recorder: FakeRecorder, in_arrange_hook):
    annotator = Annotator(recorder, max_bytes=100, log_max_bytes=0)

    with capture_logs() as cap:
        annotator.emit(None, 'k', {'blob': 'x' * 500})

    entry = next(e for e in cap if e['event'] == 'annotation_dropped')
    assert entry['data_truncated'] is False
    assert 'x' * 500 in entry['data']


def test_drop_counter_is_per_invocation(recorder: FakeRecorder):
    # A fresh hook invocation gets a fresh log budget, so a noisy window
    # does not silence the next one.
    annotator = Annotator(recorder, max_bytes=50)

    for _ in range(2):
        token = bind_hook_context(hook='arrange', partition=1, window_id=1)
        try:
            with capture_logs() as cap:
                for _ in range(MAX_DROP_LOGS_PER_CALL + 3):
                    annotator.emit(None, 'k', {'blob': 'x' * 500})
            assert len([e for e in cap if e['event'] == 'annotation_dropped']) == MAX_DROP_LOGS_PER_CALL
        finally:
            clear_hook_context(token)


def test_drop_log_names_the_target_not_the_hook_anchor(
    recorder: FakeRecorder, source_message: SourceMessage, in_arrange_hook
):
    # The arrange context has no offset of its own; the dropped record's
    # message is what the operator needs to identify.
    annotator = Annotator(recorder, max_bytes=100)

    with capture_logs() as cap:
        annotator.emit(source_message, 'k', {'blob': 'x' * 5000})

    entry = next(e for e in cap if e['event'] == 'annotation_dropped')
    assert entry['offset'] == source_message.offset
    assert entry['hook'] == 'arrange'


def test_drop_log_names_the_target_task(recorder: FakeRecorder, executor_task: ExecutorTask, in_arrange_hook):
    annotator = Annotator(recorder, max_bytes=100)

    with capture_logs() as cap:
        annotator.emit(executor_task, 'k', {'blob': 'x' * 5000})

    entry = next(e for e in cap if e['event'] == 'annotation_dropped')
    assert entry['task_id'] == executor_task.task_id


def test_message_scope_does_not_carry_the_windows_offsets(
    annotator: Annotator, recorder: FakeRecorder, source_message: SourceMessage, in_arrange_hook
):
    # ``offsets`` is how the trace query reaches WINDOW rows, which have no
    # anchor column. A message row carrying its window's offsets would match
    # every sibling message's trace as well.
    annotator.emit(source_message, 'k', {'a': 1})

    assert recorder.calls[0]['metadata']['offsets'] == []


def test_task_scope_does_not_carry_the_windows_offsets(
    annotator: Annotator, recorder: FakeRecorder, executor_task: ExecutorTask, in_arrange_hook
):
    annotator.emit(executor_task, 'k', {'a': 1})

    assert recorder.calls[0]['metadata']['offsets'] == []


# --- end-to-end: real annotator -> real processor -> real recorder DB ---


class AnnotatingPipelineHandler(BaseDrakkarHandler):
    """Annotates at all three scopes from the hooks a real window drives."""

    async def arrange(self, messages, pending):
        self.annotate(None, 'window_note', {'messages': len(messages)})
        tasks = []
        for msg in messages:
            self.annotate(msg, 'message_note', {'offset': msg.offset})
            task = ExecutorTask(task_id=f'task-{msg.offset}', args=['hi'], source_offsets=[msg.offset])
            self.annotate(task, 'task_note', {'task_id': task.task_id})
            tasks.append(task)
        return tasks


async def test_annotations_reach_the_recorder_db_through_the_real_pipeline(tmp_path):
    """The wiring path no unit test covers: Annotator -> PartitionProcessor ->
    EventRecorder -> SQLite -> get_trace.

    Everything below is the real component, not a fake: a real recorder writing
    a real DB file, the real partition processor binding the real hook context,
    and the real annotator applying the real budgets.
    """
    recorder = EventRecorder(make_ui_config(db_dir=str(tmp_path)), worker_name='e2e', cluster_name='main')
    await recorder.start()
    try:
        handler = AnnotatingPipelineHandler()
        handler._annotator = Annotator(recorder)
        proc = PartitionProcessor(
            partition_id=0,
            handler=handler,
            executor_pool=ExecutorPool(binary_path='/bin/echo', max_executors=2, task_timeout_seconds=10),
            window_size=10,
            recorder=recorder,
        )
        proc.enqueue(
            SourceMessage(topic='t', partition=0, offset=7, value=b'{}', timestamp=1000),
        )
        proc.start()
        await wait_for(lambda: not proc.offset_tracker.has_pending() and proc.inflight_count == 0)
        await proc.stop()

        trace = await recorder.get_trace(partition=0, msg_offset=7)
    finally:
        await recorder.stop()

    annotations = [e for e in trace if e['event'] == 'annotation']
    by_kind = {json.loads(e['metadata'])['kind']: e for e in annotations}

    # All three scopes reach the message's trace, each by its own route:
    # the window row through metadata.offsets, the message row through its
    # offset column, the task row through the task_started join.
    assert set(by_kind) == {'window_note', 'message_note', 'task_note'}
    assert by_kind['message_note']['offset'] == 7
    assert by_kind['task_note']['task_id'] == 'task-7'
    assert by_kind['window_note']['offset'] is None
    assert json.loads(by_kind['window_note']['metadata'])['offsets'] == [7]
