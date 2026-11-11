"""Tests for the message identity: the rules of tests/fixtures/message_identity.json and its log scopes."""

import base64
import json
from pathlib import Path

import pytest
import structlog
from structlog.contextvars import merge_contextvars
from structlog.testing import capture_logs

from drakkar.config import MessageIdentityConfig
from drakkar.executor import ExecutorPool
from drakkar.handler import BaseDrakkarHandler
from drakkar.message_identity import (
    MAX_IDENTITY_CHARS,
    RESERVED_NAMES,
    extract_identity,
    identity_field,
    identity_label,
    name_error,
    parse_source,
)
from drakkar.message_identity_runtime import MessageIdentityResolver
from drakkar.models import ExecutorTask, SourceMessage
from drakkar.partition import PartitionProcessor
from tests.conftest import wait_for

FIXTURE = json.loads((Path(__file__).parent / 'fixtures' / 'message_identity.json').read_text(encoding='utf-8'))


def _bytes(value: str | dict | None) -> bytes | None:
    """Decode the fixture's byte notation: a string is UTF-8 text, {"base64": ...} raw bytes."""
    if value is None:
        return None
    if isinstance(value, dict):
        return base64.b64decode(value['base64'])
    return value.encode('utf-8')


def test_fixture_pins_the_constants():
    assert FIXTURE['max_identity_chars'] == MAX_IDENTITY_CHARS
    assert FIXTURE['reserved_names'] == sorted(RESERVED_NAMES)


@pytest.mark.parametrize('case', FIXTURE['name_cases'], ids=lambda case: case['name'][:20] or 'empty')
def test_name_rule_matches_the_fixture(case):
    assert name_error(case['name']) == case['message']


@pytest.mark.parametrize('case', FIXTURE['source_cases'], ids=lambda case: case['source'] or 'empty')
def test_source_rule_matches_the_fixture(case):
    if case['message'] is not None:
        with pytest.raises(ValueError) as excinfo:
            parse_source(case['source'])
        assert str(excinfo.value) == case['message']
        return
    source = parse_source(case['source'])
    assert (source.kind, source.header, list(source.path)) == (case['kind'], case['header'], case['path'])


@pytest.mark.parametrize('case', FIXTURE['extract_cases'], ids=lambda case: case['name'])
def test_extract_identity_matches_the_fixture(case):
    headers = [(name, _bytes(value)) for name, value in case['headers']]
    value = _bytes(case['value'])
    assert value is not None
    got = extract_identity(parse_source(case['source']), key=_bytes(case['key']), value=value, headers=headers)
    assert got == case['identity']


@pytest.mark.parametrize('case', FIXTURE['field_cases'], ids=lambda case: case['name'])
def test_field_and_label_match_the_fixture(case):
    assert identity_field(case['values'], case['max_values']) == case['field']
    assert identity_label(case['values'], case['max_values']) == case['label']


def test_a_value_nested_too_deep_has_no_identity_and_raises_nothing():
    deep = b'[' * 100_000
    assert extract_identity(parse_source('value:id'), key=None, value=deep, headers=[]) is None


# --- the resolver and the handler hook ---


def _message(offset: int = 0, key: bytes | None = None, value: bytes = b'{}') -> SourceMessage:
    return SourceMessage(topic='t', partition=0, offset=offset, key=key, value=value, timestamp=1000)


class _PlainHandler(BaseDrakkarHandler):
    async def arrange(self, messages, pending):  # pragma: no cover - unused
        return []


def _resolver(handler: BaseDrakkarHandler | None = None, **config) -> MessageIdentityResolver:
    return MessageIdentityResolver(
        MessageIdentityConfig(enabled=True, **config),
        handler or _PlainHandler(),
    )


def test_resolver_reads_the_configured_source():
    assert _resolver(source='key').of_message(_message(key=b'order-1')) == 'order-1'


def test_hook_can_change_the_configured_value():
    class _Hook(_PlainHandler):
        def message_identity(self, msg, configured):
            return f'{configured}@{msg.offset}'

    assert _resolver(_Hook()).of_message(_message(offset=3, key=b'k')) == 'k@3'


def test_hook_that_returns_an_empty_text_means_no_identity():
    class _Hook(_PlainHandler):
        def message_identity(self, msg, configured):
            return ''

    assert _resolver(_Hook()).of_message(_message(key=b'k')) is None


@pytest.mark.parametrize(
    ('answer', 'error'),
    [
        (RuntimeError('boom'), 'RuntimeError: boom'),
        (42, 'message_identity returned int, want str or None'),
    ],
)
def test_failing_hook_gives_no_identity_and_one_warning_per_interval(answer, error):
    class _Hook(_PlainHandler):
        def message_identity(self, msg, configured):
            if isinstance(answer, Exception):
                raise answer
            return answer

    resolver = _resolver(_Hook())
    with capture_logs() as cap:
        assert resolver.of_message(_message(key=b'k')) is None
        assert resolver.of_message(_message(key=b'k')) is None
    warnings = [line for line in cap if line['event'] == 'message_identity_hook_failed']
    assert len(warnings) == 1
    assert warnings[0]['error'] == error
    assert warnings[0]['category'] == 'handler'
    assert warnings[0]['suppressed'] == 0


def test_http_request_reads_headers_and_the_raw_body_and_has_no_key():
    msg = _message(key=b'client-name', value=b'{"serialized": true}')
    by_header = _resolver(source='header:X-Request-Id')
    assert by_header.of_http_request(msg, {'x-request-id': 'r-9'}, b'{}') == 'r-9'
    by_value = _resolver(source='value:order.id')
    assert by_value.of_http_request(msg, {}, b'{"order": {"id": "o-1"}}') == 'o-1'
    assert _resolver(source='key').of_http_request(msg, {}, b'{}') is None


def test_scope_binds_the_field_and_puts_the_outer_value_back():
    resolver = _resolver(name='correlation_id')
    with capture_logs(processors=[merge_contextvars]) as cap:
        with resolver.scope(['a', 'b']):
            structlog.get_logger().info('outer')
            with resolver.scope([None]):
                structlog.get_logger().info('inner_without_identity')
            with resolver.scope(['c']):
                structlog.get_logger().info('inner')
            structlog.get_logger().info('outer_again')
        structlog.get_logger().info('outside')
    fields = {line['event']: line.get('correlation_id') for line in cap}
    assert fields == {
        'outer': ['a', 'b'],
        'inner_without_identity': None,
        'inner': 'c',
        'outer_again': ['a', 'b'],
        'outside': None,
    }


# --- the scopes of the Kafka pipeline ---


class _ScopeHandler(BaseDrakkarHandler):
    """Logs one line in every hook, so a test can read the identity each scope carries."""

    def __init__(self) -> None:
        self.windows_done = 0

    async def arrange(self, messages, pending):
        structlog.get_logger().info('in_arrange')
        tasks = [ExecutorTask(task_id=f'one-{m.offset}', args=['x'], source_offsets=[m.offset]) for m in messages]
        tasks.append(ExecutorTask(task_id='both', args=['x'], source_offsets=[m.offset for m in messages]))
        tasks.append(
            ExecutorTask(task_id='own-label', args=['x'], source_offsets=[messages[0].offset], labels={'req': 'mine'})
        )
        return tasks

    async def on_task_complete(self, result):
        structlog.get_logger().info('in_task', task=result.task.task_id)

    async def on_message_complete(self, group):
        structlog.get_logger().info('in_message', at=group.source_message.offset)

    async def on_window_complete(self, results, source_messages):
        structlog.get_logger().info('in_window')
        self.windows_done += 1


async def test_every_scope_carries_the_identity_of_its_messages():
    handler = _ScopeHandler()
    pool = ExecutorPool(binary_path='/bin/echo', max_executors=4, task_timeout_seconds=10)
    resolver = MessageIdentityResolver(MessageIdentityConfig(enabled=True, name='req', source='key'), handler)
    processor = PartitionProcessor(
        partition_id=0, handler=handler, executor_pool=pool, window_size=10, message_identity=resolver
    )
    with capture_logs(processors=[merge_contextvars]) as cap:
        await processor._process_window([_message(0, key=b'k0'), _message(1, key=b'k1')])
        await wait_for(lambda: handler.windows_done == 1)
        await wait_for(lambda: not processor._message_trackers)

    def field_of(event: str, **match) -> list:
        return [line.get('req') for line in cap if line['event'] == event and match.items() <= line.items()]

    assert field_of('in_arrange') == [['k0', 'k1']]
    assert field_of('in_task', task='one-0') == ['k0']
    assert field_of('in_task', task='one-1') == ['k1']
    assert field_of('in_task', task='both') == [['k0', 'k1']]
    assert field_of('in_message', at=0) == ['k0']
    assert field_of('in_message', at=1) == ['k1']
    assert field_of('in_window') == [['k0', 'k1']]


async def test_tasks_get_the_identity_as_a_label_and_a_handler_label_wins():
    handler = _ScopeHandler()
    pool = ExecutorPool(binary_path='/bin/echo', max_executors=4, task_timeout_seconds=10)
    resolver = MessageIdentityResolver(MessageIdentityConfig(enabled=True, name='req', source='key'), handler)
    processor = PartitionProcessor(
        partition_id=0, handler=handler, executor_pool=pool, window_size=10, message_identity=resolver
    )
    started: dict[str, dict[str, str]] = {}
    original_execute = pool.execute

    async def recording_execute(task, *args, **kwargs):
        started[task.task_id] = dict(task.labels)
        return await original_execute(task, *args, **kwargs)

    pool.execute = recording_execute  # type: ignore[method-assign]
    await processor._process_window([_message(0, key=b'k0'), _message(1, key=None)])
    await wait_for(lambda: handler.windows_done == 1)
    assert started['one-0'] == {'req': 'k0'}
    assert started['one-1'] == {}
    assert started['both'] == {'req': 'k0'}
    assert started['own-label'] == {'req': 'mine'}


async def test_parse_failure_line_carries_the_identity_of_its_message():
    class _StrictHandler(_ScopeHandler):
        def deserialize_message(self, msg):
            msg.parse_error = 'not JSON'

    handler = _StrictHandler()
    pool = ExecutorPool(binary_path='/bin/echo', max_executors=4, task_timeout_seconds=10)
    resolver = MessageIdentityResolver(MessageIdentityConfig(enabled=True, name='req', source='key'), handler)
    processor = PartitionProcessor(
        partition_id=0,
        handler=handler,
        executor_pool=pool,
        window_size=10,
        on_parse_error='dlq',
        message_identity=resolver,
    )
    with capture_logs(processors=[merge_contextvars]) as cap:
        await processor._process_window([_message(0, key=b'bad-one')])
    # With no DLQ sink, the failure path logs that the payload was dropped.
    assert [line['event'] for line in cap] == ['dlq_failure_payloads_dropped']
    assert cap[0]['req'] == 'bad-one'
