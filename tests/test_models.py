"""Tests for Drakkar data models."""

import pytest
from pydantic import ValidationError

from drakkar.models import (
    CollectResult,
    DBRow,
    ErrorAction,
    ExecutorError,
    ExecutorResult,
    ExecutorTask,
    OutputMessage,
    PendingContext,
    SourceMessage,
)


def test_source_message_required_fields():
    msg = SourceMessage(topic='t', partition=0, offset=0, value=b'data', timestamp=0)
    assert msg.topic == 't'
    assert msg.key is None


def test_source_message_with_all_fields():
    msg = SourceMessage(
        topic='t',
        partition=3,
        offset=100,
        key=b'k',
        value=b'v',
        timestamp=1700000000,
    )
    assert msg.key == b'k'
    assert msg.partition == 3


def test_source_message_missing_required_field():
    with pytest.raises(ValidationError):
        SourceMessage(topic='t', partition=0, offset=0, timestamp=0)  # type: ignore[call-arg]


def test_source_message_serialization(source_message: SourceMessage):
    data = source_message.model_dump()
    assert data['topic'] == 'test-topic'
    assert data['offset'] == 42
    roundtrip = SourceMessage.model_validate(data)
    assert roundtrip == source_message


def test_executor_task_defaults():
    task = ExecutorTask(task_id='t1', args=['--help'], source_offsets=[0])
    assert task.metadata == {}


def test_executor_task_with_metadata(executor_task: ExecutorTask):
    assert executor_task.metadata == {'source': 'test'}
    assert executor_task.source_offsets == [42]


def test_executor_task_multiple_source_offsets():
    task = ExecutorTask(task_id='t1', args=[], source_offsets=[10, 11, 12])
    assert len(task.source_offsets) == 3


def test_executor_result_fields(executor_result: ExecutorResult):
    assert executor_result.exit_code == 0
    assert executor_result.duration_seconds == 1.5
    assert executor_result.task.task_id == 'task-001'


def test_executor_result_serialization(executor_result: ExecutorResult):
    data = executor_result.model_dump()
    roundtrip = ExecutorResult.model_validate(data)
    assert roundtrip.task.task_id == executor_result.task.task_id


def test_executor_error_defaults():
    task = ExecutorTask(task_id='t', args=[], source_offsets=[0])
    err = ExecutorError(task=task)
    assert err.exit_code is None
    assert err.stderr == ''
    assert err.exception is None


def test_executor_error_with_details(executor_error: ExecutorError):
    assert executor_error.exit_code == 1
    assert 'file not found' in executor_error.stderr


def test_pending_context_empty():
    ctx = PendingContext()
    assert ctx.pending_tasks == []
    assert ctx.pending_task_ids == set()


def test_pending_context_with_tasks(pending_context: PendingContext):
    assert len(pending_context.pending_tasks) == 1
    assert 'task-001' in pending_context.pending_task_ids


def test_output_message_defaults():
    msg = OutputMessage(value=b'data')
    assert msg.key is None


def test_output_message_with_key():
    msg = OutputMessage(key=b'k', value=b'v')
    assert msg.key == b'k'


def test_db_row():
    row = DBRow(table='results', data={'id': 1, 'val': 'x'})
    assert row.table == 'results'
    assert row.data['id'] == 1


def test_collect_result_empty():
    result = CollectResult()
    assert result.output_messages == []
    assert result.db_rows == []


def test_collect_result_with_data(collect_result: CollectResult):
    assert len(collect_result.output_messages) == 1
    assert len(collect_result.db_rows) == 1
    assert collect_result.db_rows[0].table == 'results'


def test_error_action_values():
    assert ErrorAction.RETRY == 'retry'
    assert ErrorAction.SKIP == 'skip'


def test_error_action_is_str():
    assert isinstance(ErrorAction.RETRY, str)
