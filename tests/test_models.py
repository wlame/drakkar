"""Tests for Drakkar data models."""

from pydantic import BaseModel

from drakkar.models import (
    CollectResult,
    ExecutorError,
    ExecutorResult,
    ExecutorTask,
    FilePayload,
    HttpPayload,
    KafkaPayload,
    MessageGroup,
    MongoPayload,
    PostgresPayload,
    PrecomputedResult,
    RedisPayload,
    SourceMessage,
    make_task_id,
)

# --- Helper model for payload data field ---


class SampleData(BaseModel):
    request_id: str = 'abc'
    value: int = 42


# --- CollectResult ---


def test_collect_result_empty():
    result = CollectResult()
    assert not result.has_outputs
    assert result.used_sink_types == set()


def test_collect_result_single_kafka():
    result = CollectResult(kafka=[KafkaPayload(data=SampleData())])
    assert result.has_outputs
    assert result.used_sink_types == {'kafka'}


def test_collect_result_single_postgres():
    result = CollectResult(postgres=[PostgresPayload(table='t', data=SampleData())])
    assert result.has_outputs
    assert result.used_sink_types == {'postgres'}


def test_collect_result_single_mongo():
    result = CollectResult(mongo=[MongoPayload(collection='c', data=SampleData())])
    assert result.used_sink_types == {'mongo'}


def test_collect_result_single_http():
    result = CollectResult(http=[HttpPayload(data=SampleData())])
    assert result.used_sink_types == {'http'}


def test_collect_result_single_redis():
    result = CollectResult(redis=[RedisPayload(key='k', data=SampleData())])
    assert result.used_sink_types == {'redis'}


def test_collect_result_single_filesystem():
    result = CollectResult(files=[FilePayload(path='/tmp/out.jsonl', data=SampleData())])
    assert result.used_sink_types == {'filesystem'}


def test_collect_result_multiple_sinks():
    result = CollectResult(
        kafka=[KafkaPayload(data=SampleData())],
        postgres=[PostgresPayload(table='t', data=SampleData())],
        redis=[RedisPayload(key='k', data=SampleData())],
    )
    assert result.has_outputs
    assert result.used_sink_types == {'kafka', 'postgres', 'redis'}


def test_collect_result_all_sinks():
    result = CollectResult(
        kafka=[KafkaPayload(data=SampleData())],
        postgres=[PostgresPayload(table='t', data=SampleData())],
        mongo=[MongoPayload(collection='c', data=SampleData())],
        http=[HttpPayload(data=SampleData())],
        redis=[RedisPayload(key='k', data=SampleData())],
        files=[FilePayload(path='/tmp/f', data=SampleData())],
    )
    assert result.used_sink_types == {'kafka', 'postgres', 'mongo', 'http', 'redis', 'filesystem'}


def test_collect_result_multiple_payloads_same_type():
    result = CollectResult(
        kafka=[
            KafkaPayload(sink='topic-a', data=SampleData(value=1)),
            KafkaPayload(sink='topic-b', data=SampleData(value=2)),
        ],
    )
    assert len(result.kafka) == 2
    assert result.used_sink_types == {'kafka'}


# --- make_task_id ---


def test_make_task_id_no_collisions_under_burst():
    """make_task_id should produce unique IDs even under rapid generation."""
    ids = [make_task_id('t') for _ in range(10000)]
    assert len(set(ids)) == 10000


def test_make_task_id_is_time_sortable():
    """IDs generated later should sort after earlier ones."""
    import time

    id1 = make_task_id('t')
    time.sleep(0.001)
    id2 = make_task_id('t')
    assert id1 < id2


def test_make_task_id_prefix():
    assert make_task_id('rg').startswith('rg-')


# --- MessageGroup properties ---


def _msg(offset: int = 0) -> SourceMessage:
    return SourceMessage(topic='t', partition=0, offset=offset, value=b'v', timestamp=0)


def _task(task_id: str, offsets: list[int] | None = None) -> ExecutorTask:
    return ExecutorTask(task_id=task_id, args=[], source_offsets=offsets or [0])


def _result(task: ExecutorTask) -> ExecutorResult:
    return ExecutorResult(exit_code=0, stdout='', stderr='', duration_seconds=0.1, task=task)


def _error(task: ExecutorTask) -> ExecutorError:
    return ExecutorError(task=task, exit_code=1, stderr='nope')


def test_message_group_all_succeeded_counts():
    t1 = _task('t1')
    group = MessageGroup(
        source_message=_msg(), tasks=[t1], results=[_result(t1)], errors=[], started_at=1.0, finished_at=1.5
    )
    assert group.succeeded == 1
    assert group.failed == 0
    assert group.total == 1
    assert group.all_succeeded
    assert not group.any_failed
    assert group.replaced == 0
    assert group.duration_seconds == 0.5


def test_message_group_partial_failure():
    t1, t2 = _task('t1'), _task('t2')
    group = MessageGroup(
        source_message=_msg(),
        tasks=[t1, t2],
        results=[_result(t1)],
        errors=[_error(t2)],
        started_at=1.0,
        finished_at=2.0,
    )
    assert group.succeeded == 1
    assert group.failed == 1
    assert group.total == 2
    assert group.any_failed
    assert not group.all_succeeded


def test_message_group_replaced_count_inferred():
    """replaced = total - (succeeded + failed). History preserves replaced."""
    t_orig, t_repl = _task('orig'), _task('repl')
    group = MessageGroup(
        source_message=_msg(),
        tasks=[t_orig, t_repl],  # original + replacement in history
        results=[_result(t_repl)],  # only replacement terminally succeeded
        errors=[],
        started_at=0,
        finished_at=0,
    )
    assert group.succeeded == 1
    assert group.failed == 0
    assert group.replaced == 1  # the original


def test_message_group_empty_is_not_all_succeeded():
    """A message whose arrange() produced zero tasks is NOT 'all_succeeded'."""
    group = MessageGroup(source_message=_msg(), tasks=[], results=[], errors=[], started_at=0, finished_at=0)
    assert group.is_empty
    assert group.total == 0
    assert not group.all_succeeded
    assert not group.any_failed


def test_message_group_duration_never_negative():
    """If finished_at < started_at somehow, duration reports 0."""
    group = MessageGroup(source_message=_msg(), started_at=10.0, finished_at=5.0)
    assert group.duration_seconds == 0.0


# --- PrecomputedResult + ExecutorTask.precomputed ---


def test_executor_task_args_defaults_to_empty_list():
    """args no longer required — defaults to [] so precomputed tasks don't
    need to specify args the subprocess would never see.
    """
    t = ExecutorTask(task_id='t-noargs', source_offsets=[0])
    assert t.args == []


def test_executor_task_with_precomputed_result():
    pr = PrecomputedResult(stdout='cached payload', exit_code=0)
    t = ExecutorTask(task_id='t-pc', source_offsets=[42], precomputed=pr)
    assert t.precomputed is pr
    assert t.args == []  # unused when precomputed
    assert t.precomputed.stdout == 'cached payload'
    assert make_task_id('task').startswith('task-')
