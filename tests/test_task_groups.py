"""Tests for the task group model of the streaming arrange hook."""

import pytest

import drakkar as dk
from drakkar.groups import ArrangeStreamError, collect_group_labels, drop_invalid_notes, new_task_group
from drakkar.models import ExecutorError, ExecutorResult, ExecutorTask, MessageGroup, SourceMessage, TaskGroup


@pytest.fixture
def msg() -> SourceMessage:
    return SourceMessage(topic='t', partition=0, offset=7, value=b'x', timestamp=0)


def task(labels: dict[str, str] | None = None, task_id: str = 't') -> ExecutorTask:
    return ExecutorTask(task_id=task_id, source_offsets=[7], labels=labels or {})


def g(msg: SourceMessage, *, index: int = 0, succeeded: int = 0, failed: int = 0, replaced: int = 0) -> TaskGroup:
    return TaskGroup(
        index=index,
        source_message=msg,
        task_count=succeeded + failed + replaced,
        succeeded=succeeded,
        failed=failed,
        replaced=replaced,
    )


def test_collect_group_labels_keeps_order_and_equal_values():
    tasks = [task(labels={'size': '10', 'dir': 'a'}), task(labels={'size': '10'}), task(labels={})]
    assert collect_group_labels(tasks, ('size', 'dir', 'none')) == {'size': ['10', '10'], 'dir': ['a'], 'none': []}


def test_collect_group_labels_with_no_names_is_empty():
    assert collect_group_labels([task(labels={'size': '10'})], ()) == {}


def test_new_task_group_sets_the_group_on_each_task_and_holds_no_task(msg):
    tasks = [task(labels={'dir': 'a'}, task_id='a'), task(labels={'dir': 'b'}, task_id='b')]
    group = new_task_group(index=3, source_message=msg, tasks=tasks, label_names=('dir',), started_at=1.0)
    assert group.index == 3 and group.task_count == len(tasks)
    assert group.source_message is msg
    assert group.labels == {'dir': ['a', 'b']}
    assert group.started_at == 1.0
    assert (group.succeeded, group.failed, group.replaced, group.finished_at) == (0, 0, 0, 0.0)
    assert all(t.group is group for t in tasks)
    assert not {'tasks', 'results', 'errors'} & set(TaskGroup.model_fields)


def test_task_group_is_not_serialized_with_the_task(msg):
    tasks = [task()]
    new_task_group(index=0, source_message=msg, tasks=tasks, label_names=(), started_at=0.0)
    assert 'group' not in tasks[0].model_dump()
    assert 'group' not in tasks[0].model_dump_json()
    assert 'TaskGroup' not in repr(tasks[0])


def test_task_has_no_group_by_default():
    assert task().group is None


def test_drop_invalid_notes_removes_values_that_are_not_scalars(msg):
    group = g(msg)
    group.notes.update({'rows': 3, 'ok': True, 'name': 'x', 'ratio': 0.5, 'obj': object(), 'list': [1], 'none': None})
    assert drop_invalid_notes(group) == ['list', 'none', 'obj']
    assert set(group.notes) == {'rows', 'ok', 'name', 'ratio'}


def test_drop_invalid_notes_removes_subclasses_of_scalars(msg):
    class TaggedText(str):
        pass

    group = g(msg)
    group.notes.update({'tagged': TaggedText('x'), 'plain': 'x'})
    assert drop_invalid_notes(group) == ['tagged']
    assert set(group.notes) == {'plain'}


@pytest.mark.parametrize('value', [float('nan'), float('inf'), float('-inf')])
def test_drop_invalid_notes_removes_a_float_that_is_not_finite(msg, value):
    group = g(msg)
    group.notes.update({'broken': value, 'ratio': 0.5, 'zero': 0.0, 'count': 10**400})
    assert drop_invalid_notes(group) == ['broken']
    assert group.notes == {'ratio': 0.5, 'zero': 0.0, 'count': 10**400}


def test_drop_invalid_notes_with_valid_notes_returns_nothing(msg):
    group = g(msg)
    group.notes['rows'] = 1
    assert drop_invalid_notes(group) == []
    assert group.notes == {'rows': 1}


def test_message_group_counters_come_from_the_groups_in_stream_mode(msg):
    mg = MessageGroup(
        source_message=msg,
        started_at=0.0,
        groups=[g(msg, succeeded=2, failed=1, replaced=1), g(msg, index=1, succeeded=3)],
    )
    assert (mg.succeeded, mg.failed, mg.replaced, mg.total) == (5, 1, 1, 7)
    assert mg.any_failed and not mg.is_empty
    assert not mg.all_succeeded


def test_message_group_counters_are_unchanged_without_groups(msg):
    original = task(task_id='original')
    replacement = task(task_id='replacement')
    other = task(task_id='other')
    mg = MessageGroup(
        source_message=msg,
        started_at=0.0,
        tasks=[original, replacement, other],
        results=[ExecutorResult(task=replacement, exit_code=0, stdout='', stderr='', duration_seconds=0.1)],
        errors=[ExecutorError(task=other, exit_code=1)],
    )
    assert mg.groups == []
    assert (mg.succeeded, mg.failed, mg.replaced, mg.total) == (1, 1, 1, 3)


def test_arrange_stream_error_is_an_exception():
    assert issubclass(ArrangeStreamError, Exception)


def test_task_group_is_exported():
    assert dk.TaskGroup is TaskGroup
