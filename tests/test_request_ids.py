"""Tests for the request id of every message: the hook, its fallback, the log scopes and the task stamp."""

import pytest
import structlog
from structlog.contextvars import merge_contextvars
from structlog.testing import capture_logs

from drakkar.executor import ExecutorPool
from drakkar.handler import BaseDrakkarHandler
from drakkar.models import ExecutorTask, SourceMessage
from drakkar.partition import PartitionProcessor
from drakkar.request_ids import (
    MAX_FIELD_VALUES,
    RequestIdResolver,
    joined_request_ids,
    request_id_field,
)
from drakkar.sinks.manager import MessageContext, _message_record
from tests.conftest import wait_for


def _message(offset: int = 0, key: bytes | None = None) -> SourceMessage:
    return SourceMessage(topic='t', partition=0, offset=offset, key=key, value=b'{}', timestamp=1000)


class _PlainHandler(BaseDrakkarHandler):
    async def arrange(self, messages, pending):  # pragma: no cover - unused
        return []


class _KeyHandler(_PlainHandler):
    """Uses the message key as the request id."""

    def request_id(self, msg):
        return msg.key.decode()


# --- the field and the joined text ---


@pytest.mark.parametrize(
    ('values', 'field'),
    [
        ([None, None], None),
        (['a'], 'a'),
        (['a', None, 'a'], 'a'),
        (['b', 'a', 'b', 'c'], ['b', 'a', 'c']),
        ([], None),
    ],
)
def test_request_id_field_is_text_for_one_and_a_list_for_several(values, field):
    assert request_id_field(values) == field


def test_request_id_field_holds_at_most_the_cap():
    assert request_id_field(str(n) for n in range(MAX_FIELD_VALUES + 5)) == [str(n) for n in range(MAX_FIELD_VALUES)]


def test_joined_request_ids_joins_with_a_comma():
    assert joined_request_ids(['b', 'a', 'b']) == 'b,a'
    assert joined_request_ids([None]) is None


# --- the resolver ---


def test_default_hook_gives_a_generated_id_and_stores_it_on_the_message():
    msg = _message()
    request_id = RequestIdResolver(_PlainHandler()).assign(msg)
    assert request_id.startswith('req_')
    assert msg.request_id == request_id


def test_hook_answer_is_the_request_id():
    assert RequestIdResolver(_KeyHandler()).resolve(_message(key=b'order-7')) == 'order-7'


@pytest.mark.parametrize(
    ('answer', 'error'),
    [
        (RuntimeError('boom'), 'RuntimeError: boom'),
        (42, 'TypeError: request_id returned int, want str'),
        ('has space', "ValueError: request_id must not contain whitespace: 'has space'"),
        ('', 'ValueError: request_id must not be empty'),
        ('x' * 65, 'ValueError: request_id too long'),
    ],
)
def test_failing_hook_gives_a_generated_id_and_one_warning_per_interval(answer, error):
    class _Hook(_PlainHandler):
        def request_id(self, msg):
            if isinstance(answer, Exception):
                raise answer
            return answer

    resolver = RequestIdResolver(_Hook())
    with capture_logs() as cap:
        assert resolver.resolve(_message()).startswith('req_')
        assert resolver.resolve(_message()).startswith('req_')
    warnings = [line for line in cap if line['event'] == 'request_id_hook_failed']
    assert len(warnings) == 1
    assert warnings[0]['error'].startswith(error)
    assert warnings[0]['category'] == 'handler'
    assert warnings[0]['suppressed'] == 0


def test_stamp_task_sets_the_joined_ids_and_the_label_and_keeps_handler_values():
    plain = ExecutorTask(task_id='a', args=[], source_offsets=[0, 1])
    RequestIdResolver.stamp_task(plain, ['r0', 'r1'])
    assert (plain.request_id, plain.labels) == ('r0,r1', {'request_id': 'r0,r1'})

    own = ExecutorTask(task_id='b', args=[], source_offsets=[0], request_id='mine', labels={'request_id': 'label'})
    RequestIdResolver.stamp_task(own, ['r0'])
    assert (own.request_id, own.labels) == ('mine', {'request_id': 'label'})


def test_scope_binds_the_field_and_puts_the_outer_value_back():
    resolver = RequestIdResolver(_PlainHandler())
    with capture_logs(processors=[merge_contextvars]) as cap:
        with resolver.scope(['a', 'b']):
            structlog.get_logger().info('outer')
            with resolver.scope([None]):
                structlog.get_logger().info('inner_without_an_id')
            with resolver.scope(['c']):
                structlog.get_logger().info('inner')
            structlog.get_logger().info('outer_again')
        structlog.get_logger().info('outside')
    fields = {line['event']: line.get('request_id') for line in cap}
    assert fields == {
        'outer': ['a', 'b'],
        'inner_without_an_id': None,
        'inner': 'c',
        'outer_again': ['a', 'b'],
        'outside': None,
    }


# --- the scopes of the Kafka pipeline ---


class _ScopeHandler(_KeyHandler):
    """Logs one line in every hook, so a test can read the request id each scope carries."""

    def __init__(self) -> None:
        self.windows_done = 0
        self.started: dict[str, tuple[str | None, dict[str, str]]] = {}

    async def arrange(self, messages, pending):
        structlog.get_logger().info('in_arrange')
        tasks = [ExecutorTask(task_id=f'one-{m.offset}', args=['x'], source_offsets=[m.offset]) for m in messages]
        tasks.append(ExecutorTask(task_id='both', args=['x'], source_offsets=[m.offset for m in messages]))
        return tasks

    async def on_task_complete(self, result):
        task = result.task
        self.started[task.task_id] = (task.request_id, dict(task.labels))
        structlog.get_logger().info('in_task', task=task.task_id)

    async def on_message_complete(self, group):
        structlog.get_logger().info('in_message', at=group.source_message.offset, seen=group.source_message.request_id)

    async def on_window_complete(self, results, source_messages):
        structlog.get_logger().info('in_window')
        self.windows_done += 1


def _processor(handler: BaseDrakkarHandler, **kwargs) -> PartitionProcessor:
    pool = ExecutorPool(binary_path='/bin/echo', max_executors=4, task_timeout_seconds=10)
    return PartitionProcessor(partition_id=0, handler=handler, executor_pool=pool, window_size=10, **kwargs)


async def test_every_scope_carries_the_request_ids_of_its_messages():
    handler = _ScopeHandler()
    processor = _processor(handler)
    with capture_logs(processors=[merge_contextvars]) as cap:
        await processor._process_window([_message(0, key=b'k0'), _message(1, key=b'k1')])
        await wait_for(lambda: handler.windows_done == 1)
        await wait_for(lambda: not processor._message_trackers)

    def field_of(event: str, **match) -> list:
        return [line.get('request_id') for line in cap if line['event'] == event and match.items() <= line.items()]

    assert field_of('in_arrange') == [['k0', 'k1']]
    assert field_of('in_task', task='one-0') == ['k0']
    assert field_of('in_task', task='both') == [['k0', 'k1']]
    assert field_of('in_message', at=0, seen='k0') == ['k0']
    assert field_of('in_message', at=1, seen='k1') == ['k1']
    assert field_of('in_window') == [['k0', 'k1']]
    assert handler.started['one-1'] == ('k1', {'request_id': 'k1'})
    assert handler.started['both'] == ('k0,k1', {'request_id': 'k0,k1'})


async def test_parse_failure_line_carries_the_request_id_of_its_message():
    class _StrictHandler(_ScopeHandler):
        def deserialize_message(self, msg):
            msg.parse_error = 'not JSON'

    processor = _processor(_StrictHandler(), on_parse_error='dlq')
    with capture_logs(processors=[merge_contextvars]) as cap:
        await processor._process_window([_message(0, key=b'bad-one')])
    # With no DLQ sink, the failure path logs that the payload was dropped.
    assert [line['event'] for line in cap] == ['dlq_failure_payloads_dropped']
    assert cap[0]['request_id'] == 'bad-one'


# --- the DLQ record ---


def test_a_kafka_dlq_record_carries_the_request_id_of_its_own_message():
    first, second = _message(0, key=b'k0'), _message(1, key=b'k1')
    first.request_id, second.request_id = 'r0', 'r1'
    record = _message_record(second, is_request=False, stage='sink', error='boom', attempts=1, delivered_sinks=set())
    assert (record.request_id, record.source_offset, record.key) == ('r1', 1, b'k1')


def test_message_context_joins_its_request_ids_and_marks_only_a_request():
    first, second = _message(0), _message(1)
    first.request_id, second.request_id = 'r0', 'r1'
    kafka = MessageContext(source_messages=[first, second], states=[object(), object()])  # type: ignore[list-item]
    assert (kafka.request_id, kafka.is_request) == ('r0,r1', False)
    request = MessageContext.for_request(body=b'{}', request_id='req-9', received_ms=1)
    assert (request.request_id, request.is_request, request.source_messages[0].request_id) == ('req-9', True, 'req-9')
