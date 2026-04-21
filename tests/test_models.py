"""Tests for Drakkar data models."""

from pydantic import BaseModel

from drakkar.models import (
    CollectResult,
    DeliveryError,
    ExecutorError,
    ExecutorResult,
    ExecutorTask,
    FilePayload,
    HttpPayload,
    KafkaPayload,
    MessageGroup,
    MongoPayload,
    PendingContext,
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


# --- SourceMessage ---


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


# --- ExecutorTask ---


def test_executor_task_defaults():
    task = ExecutorTask(task_id='t1', args=['--help'], source_offsets=[0])
    assert task.metadata == {}


def test_executor_task_with_metadata(executor_task: ExecutorTask):
    assert executor_task.metadata == {'source': 'test'}
    assert executor_task.source_offsets == [42]


def test_executor_task_multiple_source_offsets():
    task = ExecutorTask(task_id='t1', args=[], source_offsets=[10, 11, 12])
    assert len(task.source_offsets) == 3


def test_executor_task_labels_default_empty():
    task = ExecutorTask(task_id='t1', args=[], source_offsets=[0])
    assert task.labels == {}


def test_executor_task_labels():
    task = ExecutorTask(
        task_id='t1',
        args=[],
        source_offsets=[0],
        labels={'request_id': 'abc-123', 'user': 'alice'},
    )
    assert task.labels == {'request_id': 'abc-123', 'user': 'alice'}


# --- ExecutorResult ---


def test_executor_result_fields(executor_result: ExecutorResult):
    assert executor_result.exit_code == 0
    assert executor_result.duration_seconds == 1.5
    assert executor_result.task.task_id == 'task-001'


# --- ExecutorError ---


def test_executor_error_defaults():
    task = ExecutorTask(task_id='t', args=[], source_offsets=[0])
    err = ExecutorError(task=task)
    assert err.exit_code is None
    assert err.stderr == ''
    assert err.exception is None


def test_executor_error_with_details(executor_error: ExecutorError):
    assert executor_error.exit_code == 1
    assert 'file not found' in executor_error.stderr


# --- PendingContext ---


def test_pending_context_empty():
    ctx = PendingContext()
    assert ctx.pending_tasks == []
    assert ctx.pending_task_ids == set()


def test_pending_context_with_tasks(pending_context: PendingContext):
    assert len(pending_context.pending_tasks) == 1
    assert 'task-001' in pending_context.pending_task_ids


# --- Sink Payloads ---


def test_kafka_payload_defaults():
    p = KafkaPayload(data=SampleData())
    assert p.sink == ''
    assert p.key is None
    assert p.data.request_id == 'abc'


def test_kafka_payload_with_key_and_sink():
    p = KafkaPayload(sink='results', key=b'my-key', data=SampleData(value=99))
    assert p.sink == 'results'
    assert p.key == b'my-key'
    assert p.data.value == 99


def test_postgres_payload():
    p = PostgresPayload(table='results', data=SampleData())
    assert p.table == 'results'
    assert p.data.model_dump() == {'request_id': 'abc', 'value': 42}


def test_postgres_payload_with_sink():
    p = PostgresPayload(sink='analytics-db', table='events', data=SampleData())
    assert p.sink == 'analytics-db'


def test_mongo_payload():
    p = MongoPayload(collection='results', data=SampleData())
    assert p.collection == 'results'
    assert p.data.model_dump() == {'request_id': 'abc', 'value': 42}


def test_http_payload():
    p = HttpPayload(data=SampleData())
    assert p.sink == ''
    body = p.data.model_dump_json()
    assert '"request_id":"abc"' in body


def test_redis_payload_without_ttl():
    p = RedisPayload(key='result:abc', data=SampleData())
    assert p.key == 'result:abc'
    assert p.ttl is None


def test_redis_payload_with_ttl():
    p = RedisPayload(key='cache:abc', data=SampleData(), ttl=3600)
    assert p.ttl == 3600


def test_file_payload():
    p = FilePayload(path='/data/output.jsonl', data=SampleData())
    assert p.path == '/data/output.jsonl'
    line = p.data.model_dump_json()
    assert '"value":42' in line


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


# --- DeliveryError ---


def test_delivery_error_basic():
    err = DeliveryError(
        sink_name='results',
        sink_type='kafka',
        error='BrokerNotAvailableError',
    )
    assert err.sink_name == 'results'
    assert err.sink_type == 'kafka'
    assert err.payloads == []


def test_delivery_error_with_payloads():
    payloads = [SampleData(value=1), SampleData(value=2)]
    err = DeliveryError(
        sink_name='main-db',
        sink_type='postgres',
        error='connection refused',
        payloads=payloads,
    )
    assert len(err.payloads) == 2


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


def test_executor_task_parent_task_id_default_is_none():
    t = _task('t1')
    assert t.parent_task_id is None


# --- PrecomputedResult + ExecutorTask.precomputed ---


def test_precomputed_result_defaults():
    pr = PrecomputedResult()
    assert pr.stdout == ''
    assert pr.stderr == ''
    assert pr.exit_code == 0
    assert pr.duration_seconds == 0.0


def test_precomputed_result_with_values():
    pr = PrecomputedResult(stdout='hello', stderr='warn', exit_code=2, duration_seconds=0.05)
    assert pr.stdout == 'hello'
    assert pr.stderr == 'warn'
    assert pr.exit_code == 2
    assert pr.duration_seconds == 0.05


def test_executor_task_precomputed_default_none():
    t = _task('t-default')
    assert t.precomputed is None


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
