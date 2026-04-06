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
    MongoPayload,
    PendingContext,
    PostgresPayload,
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
    assert make_task_id('task').startswith('task-')
