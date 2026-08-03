"""Tests for Drakkar data models."""

import pytest
from pydantic import BaseModel, ValidationError

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
    PostgresOp,
    PostgresPayload,
    PrecomputedResult,
    RedisPayload,
    SourceMessage,
    TaskOrigin,
    make_stable_task_id,
    make_task_id,
)

# --- Helper model for payload data field ---


class SampleData(BaseModel):
    request_id: str = 'abc'
    value: int = 42


class SampleKey(BaseModel):
    """Predicate model for the Postgres payload's ``where`` field."""

    id: int = 1


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


# --- make_stable_task_id ---


def test_make_stable_task_id_is_deterministic():
    """Same prefix+parts → the same id, always (that determinism IS the feature)."""
    a = make_stable_task_id('rg', 'pattern', '/var/log/app.log')
    b = make_stable_task_id('rg', 'pattern', '/var/log/app.log')
    assert a == b


def test_make_stable_task_id_shape():
    """{prefix}-{16 lowercase hex}; no ':' (retry composite keys split on ':r')."""
    sid = make_stable_task_id('rg', 'x')
    assert sid.startswith('rg-')
    suffix = sid.removeprefix('rg-')
    assert len(suffix) == 16
    assert all(c in '0123456789abcdef' for c in suffix)
    assert ':' not in sid


def test_make_stable_task_id_part_joining_is_injective():
    """('a','bc') and ('ab','c') concatenate identically — the length prefix
    must keep them distinct."""
    assert make_stable_task_id('x', 'a', 'bc') != make_stable_task_id('x', 'ab', 'c')


def test_make_stable_task_id_requires_parts():
    """Zero parts would produce one constant id and dedupe EVERYTHING."""
    with pytest.raises(ValueError, match='at least one part'):
        make_stable_task_id('rg')


def test_make_stable_task_id_cross_backend_golden():
    """Byte-parity pin — the Go suite pins this SAME literal (mixed fleets
    must see one convention)."""
    assert make_stable_task_id('t', 'alpha', 'beta') == 't-5d11bfa62398519b'


# --- MessageGroup properties ---


def _msg(offset: int = 0) -> SourceMessage:
    return SourceMessage(topic='t', partition=0, offset=offset, value=b'v', timestamp=0)


def _task(task_id: str, offsets: list[int] | None = None) -> ExecutorTask:
    return ExecutorTask(task_id=task_id, args=[], source_offsets=offsets or [0])


def _result(task: ExecutorTask) -> ExecutorResult:
    return ExecutorResult(exit_code=0, stdout='', stderr='', duration_seconds=0.1, task=task)


def _error(task: ExecutorTask) -> ExecutorError:
    return ExecutorError(task=task, exit_code=1, stderr='nope')


# --- ExecutorResult truncation flags ---


def test_executor_result_truncation_flags_default_false(executor_task):
    result = ExecutorResult(
        exit_code=0,
        stdout='',
        stderr='',
        duration_seconds=0.1,
        task=executor_task,
    )
    assert result.stdout_truncated is False
    assert result.stderr_truncated is False


def test_executor_result_truncation_flags_settable(executor_task):
    result = ExecutorResult(
        exit_code=0,
        stdout='partial',
        stderr='',
        duration_seconds=0.1,
        task=executor_task,
        stdout_truncated=True,
        stderr_truncated=True,
    )
    assert result.stdout_truncated is True
    assert result.stderr_truncated is True


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


# --- TaskOrigin / origin / client_name / request_id ---


def test_executor_task_origin_default_kafka():
    """The historical Kafka path is the default — operators don't have to opt in."""
    t = ExecutorTask(task_id='t-default', source_offsets=[0])
    assert t.origin == 'kafka'
    assert t.client_name is None
    assert t.request_id is None


def test_executor_task_origin_http_propagates_through_model_dump():
    """Webapp tasks carry origin/client/request through serialization round-trips."""
    t = ExecutorTask(
        task_id='t-http',
        source_offsets=[0],
        origin='http',
        client_name='tenant-A',
        request_id='req_20260506T184231_0042',
    )
    dumped = t.model_dump()
    assert dumped['origin'] == 'http'
    assert dumped['client_name'] == 'tenant-A'
    assert dumped['request_id'] == 'req_20260506T184231_0042'

    restored = ExecutorTask.model_validate(dumped)
    assert restored.origin == 'http'
    assert restored.client_name == 'tenant-A'
    assert restored.request_id == 'req_20260506T184231_0042'


def test_executor_task_kafka_origin_round_trip_uses_defaults():
    """Round-tripping a Kafka task without explicit origin keeps the defaults."""
    t = ExecutorTask(task_id='t-k', source_offsets=[1])
    restored = ExecutorTask.model_validate(t.model_dump())
    assert restored.origin == 'kafka'
    assert restored.client_name is None
    assert restored.request_id is None


def test_message_group_origin_defaults_kafka():
    """MessageGroup defaults match ExecutorTask defaults — Kafka path needs no changes."""
    group = MessageGroup(source_message=_msg(), started_at=0.0, finished_at=0.0)
    assert group.origin == 'kafka'
    assert group.client_name is None
    assert group.request_id is None


def test_message_group_origin_http_round_trip():
    """Webapp-origin MessageGroups serialise their tagging fields end-to-end."""
    group = MessageGroup(
        source_message=_msg(),
        tasks=[],
        results=[],
        errors=[],
        started_at=1.0,
        finished_at=2.0,
        origin='http',
        client_name='tenant-A',
        request_id='req_20260506T184231_0042',
    )
    dumped = group.model_dump()
    assert dumped['origin'] == 'http'
    assert dumped['client_name'] == 'tenant-A'
    assert dumped['request_id'] == 'req_20260506T184231_0042'

    restored = MessageGroup.model_validate(dumped)
    assert restored.origin == 'http'
    assert restored.client_name == 'tenant-A'
    assert restored.request_id == 'req_20260506T184231_0042'


def test_executor_task_origin_rejects_unknown_value():
    """``TaskOrigin`` is a closed Literal — bogus values must fail validation."""
    import pytest
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        ExecutorTask(task_id='t-bad', source_offsets=[0], origin='websocket')  # type: ignore[arg-type]


def test_task_origin_alias_exposes_the_two_known_values():
    """``TaskOrigin`` is a public type alias importable from drakkar.models."""
    from typing import get_args

    assert set(get_args(TaskOrigin)) == {'kafka', 'http'}


# --- PostgresPayload operations ---


def test_postgres_payload_defaults_to_insert():
    payload = PostgresPayload(table='results', data=SampleData())
    assert payload.op is PostgresOp.INSERT


def test_postgres_payload_op_serializes_as_a_plain_string():
    """DLQ JSON byte-stability: op must serialize as its value, not 'PostgresOp.UPDATE'."""
    payload = PostgresPayload(op=PostgresOp.UPDATE, table='jobs', data=SampleData(), where=SampleKey())
    assert '"op":"update"' in payload.model_dump_json().replace(' ', '')


def test_postgres_payload_update_requires_where():
    """An absent predicate would render UPDATE with no WHERE — every row."""
    with pytest.raises(ValidationError, match="requires 'where'"):
        PostgresPayload(op=PostgresOp.UPDATE, table='jobs', data=SampleData())


def test_postgres_payload_upsert_requires_non_empty_conflict():
    with pytest.raises(ValidationError, match="requires 'conflict'"):
        PostgresPayload(op=PostgresOp.UPSERT, table='t', data=SampleData(), conflict=[])


def test_postgres_payload_statement_requires_a_name():
    with pytest.raises(ValidationError, match="requires 'statement'"):
        PostgresPayload(op=PostgresOp.STATEMENT)


@pytest.mark.parametrize(
    ('op', 'kwargs', 'unused'),
    [
        ('insert', {'table': 't', 'where': SampleKey()}, 'where'),
        ('insert', {'table': 't', 'conflict': ['id']}, 'conflict'),
        ('insert', {'table': 't', 'statement': 'claim'}, 'statement'),
        ('update', {'table': 't', 'where': SampleKey(), 'conflict': ['id']}, 'conflict'),
        ('upsert', {'table': 't', 'conflict': ['id'], 'where': SampleKey()}, 'where'),
        ('statement', {'statement': 'claim', 'table': 't'}, 'table'),
        ('statement', {'statement': 'claim', 'conflict': ['id']}, 'conflict'),
    ],
)
def test_postgres_payload_rejects_fields_the_op_does_not_use(op, kwargs, unused):
    """A mis-set field must be a loud error, not silently ignored.

    This is what recovers the main weakness of one class with optional
    fields — without it, PostgresPayload(op='insert', where=key) would
    quietly drop the predicate.
    """
    kwargs.setdefault('data', SampleData())
    if op == 'statement':
        kwargs.pop('data', None)
    with pytest.raises(ValidationError, match=f"does not use '{unused}'"):
        PostgresPayload(op=op, **kwargs)


def test_postgres_payload_update_columns_must_not_overlap_conflict():
    with pytest.raises(ValidationError, match='overlaps conflict'):
        PostgresPayload(
            op=PostgresOp.UPSERT,
            table='t',
            data=SampleData(),
            conflict=['id'],
            update_columns=['id'],
        )


def test_postgres_payload_valid_forms_construct():
    assert PostgresPayload(table='t', data=SampleData()).op is PostgresOp.INSERT
    assert PostgresPayload(op=PostgresOp.UPDATE, table='t', data=SampleData(), where=SampleKey()).where is not None
    assert PostgresPayload(op=PostgresOp.UPSERT, table='t', data=SampleData(), conflict=['id']).conflict == ['id']
    assert PostgresPayload(op=PostgresOp.STATEMENT, statement='claim').params is None
    assert PostgresPayload(op=PostgresOp.STATEMENT, statement='claim', params=SampleKey()).statement == 'claim'


def test_postgres_payload_upsert_accepts_disjoint_update_columns():
    """update_columns naming non-conflict columns is the ordinary upsert shape."""
    payload = PostgresPayload(
        op=PostgresOp.UPSERT,
        table='sessions',
        data=SampleData(),
        conflict=['request_id'],
        update_columns=['value'],
    )
    assert payload.update_columns == ['value']


# --- payload data survives DLQ serialization ---


class _DLQRow(BaseModel):
    """A concrete payload body — the thing that must reach the DLQ intact."""

    request_id: str = 'req-42'
    amount: int = 999


_DLQ_ROW_JSON = {'request_id': 'req-42', 'amount': 999}


@pytest.mark.parametrize(
    ('label', 'payload'),
    [
        ('kafka', KafkaPayload(data=_DLQRow())),
        ('postgres', PostgresPayload(table='t', data=_DLQRow())),
        ('mongo', MongoPayload(collection='c', data=_DLQRow())),
        ('http', HttpPayload(data=_DLQRow())),
        ('redis', RedisPayload(key='k', data=_DLQRow())),
        ('file', FilePayload(path='/tmp/out.jsonl', data=_DLQRow())),
    ],
)
def test_payload_data_survives_model_dump_json(label, payload):
    """A payload's body must serialize as its ACTUAL fields.

    ``data`` is declared as ``BaseModel``, and pydantic serializes against the
    declared type by default — which has no fields, so the body would come out
    as ``{}``. The DLQ serializes payloads exactly this way, so without
    duck-typed serialization every dead-lettered record loses the data it
    exists to preserve, silently and with no warning.
    """
    import json

    assert json.loads(payload.model_dump_json())['data'] == _DLQ_ROW_JSON, label


def test_postgres_payload_where_and_params_survive_serialization():
    """The update predicate and statement params are bodies too."""
    import json

    from drakkar.models import PostgresOp

    updated = json.loads(
        PostgresPayload(op=PostgresOp.UPDATE, table='t', data=_DLQRow(), where=_DLQRow()).model_dump_json()
    )
    assert updated['where'] == _DLQ_ROW_JSON

    stmt = json.loads(PostgresPayload(op=PostgresOp.STATEMENT, statement='s', params=_DLQRow()).model_dump_json())
    assert stmt['params'] == _DLQ_ROW_JSON


def test_dlq_message_preserves_payload_data():
    """End to end through the real DLQ path, not just the payload model."""
    import json

    from drakkar.models import DeliveryError
    from drakkar.sinks.dlq import DLQMessage

    error = DeliveryError(
        sink_name='main',
        sink_type='postgres',
        error='boom',
        payloads=[PostgresPayload(table='results', data=_DLQRow())],
    )
    entry = json.loads(DLQMessage(error, partition_id=0).serialize())
    assert json.loads(entry['original_payloads'][0])['data'] == _DLQ_ROW_JSON
