"""The DLQ record: the original source message plus ``dk-`` headers.

``tests/fixtures/dlq_contract.json`` pins the header names and their order,
the cut rule of ``dk-error`` and four worked records. The record is a
contract: tooling reads the headers by name and replays the key and value
as they are.
"""

import base64
import json
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import BaseModel

from drakkar.config import KafkaConfig, KafkaSourceConfig
from drakkar.consumer import KafkaConsumer
from drakkar.executor import ExecutorPool
from drakkar.handler import BaseDrakkarHandler
from drakkar.models import SourceMessage
from drakkar.partition import PartitionProcessor
from drakkar.sinks import dlq as dlq_module
from drakkar.sinks.dlq import DLQEntry, read_dlq_entries
from drakkar.sinks.dlq_record import (
    DLQ_HEADER_ORDER,
    DLQ_RECORD_VERSION,
    DLQRecord,
    cut_error,
    render_headers,
)
from tests.conftest import wait_for

FIXTURE = json.loads((Path(__file__).parent / 'fixtures' / 'dlq_contract.json').read_text(encoding='utf-8'))


def _fixture_bytes(value: str | dict | None) -> bytes | None:
    """Decode the fixture's byte notation: a string is UTF-8, ``{"base64": ...}`` is raw bytes."""
    if value is None:
        return None
    if isinstance(value, dict):
        return base64.b64decode(value['base64'])
    return value.encode('utf-8')


def _fixture_headers(pairs: list) -> list[tuple[str, bytes | None]]:
    return [(name, _fixture_bytes(value)) for name, value in pairs]


def _record_from_fixture_input(facts: dict) -> DLQRecord:
    return DLQRecord(
        key=_fixture_bytes(facts['key']),
        value=_fixture_bytes(facts['value']),
        source_headers=_fixture_headers(facts['source_headers']),
        stage=facts['stage'],
        error=facts['error'],
        sink=facts['sink'],
        task_id=facts['task_id'],
        attempts=facts['attempts'],
        delivered_sinks=facts['delivered_sinks'],
        source_topic=facts['source_topic'],
        source_partition=facts['source_partition'],
        source_offset=facts['source_offset'],
        source_timestamp_ms=facts['source_timestamp_ms'],
        request_id=facts['request_id'],
        worker=facts['worker'],
        instance=facts['instance'],
        failed_at=datetime.fromisoformat(facts['failed_at']),
    )


def test_header_order_matches_the_fixture():
    assert list(DLQ_HEADER_ORDER) == FIXTURE['header_order']
    assert DLQ_HEADER_ORDER[0] == 'dk-record-version'
    assert FIXTURE['record_version'] == DLQ_RECORD_VERSION


@pytest.mark.parametrize('case', FIXTURE['cases'], ids=lambda case: case['name'])
def test_fixture_cases_render_the_expected_headers(case):
    record = _record_from_fixture_input(case['input'])

    assert render_headers(record) == _fixture_headers(case['expected_headers'])
    assert case['expected_value_equal_to_input'] is True
    assert record.value == _fixture_bytes(case['input']['value'])
    assert record.key == _fixture_bytes(case['input']['key'])


def test_record_keeps_bytes_and_drops_old_dk_headers():
    """A replayed message that fails again: its bytes stay, its old facts go."""
    value = b'\xff\xfe\x00not utf-8 \xc3\x28'
    record = DLQRecord(
        key=b'\x80key',
        value=value,
        source_headers=[
            ('dk-stage', b'sink'),
            ('x-trace', b'\xfe\x01'),
            ('dk-error', b'old failure'),
            ('dk-attempts', b'3'),
        ],
        stage='parse',
        error='bad input',
        source_timestamp_ms=5,
    )

    headers = render_headers(record)

    assert headers == [
        ('x-trace', b'\xfe\x01'),
        ('dk-record-version', b'1'),
        ('dk-stage', b'parse'),
        ('dk-error', b'bad input'),
        ('dk-source-timestamp', b'5'),
    ]
    assert record.value == value
    assert record.key == b'\x80key'

    dropped = FIXTURE['dropped_source_headers']
    replayed = DLQRecord(
        key=None,
        value=b'v',
        source_headers=_fixture_headers(dropped['source_headers']),
        stage='sink',
        error='e',
    )
    source_part = [pair for pair in render_headers(replayed) if not pair[0].startswith('dk-')]
    assert source_part == _fixture_headers(dropped['kept'])


@pytest.mark.parametrize('case', FIXTURE['error_cut_cases'], ids=lambda case: case['name'])
def test_error_cut_cases(case):
    cut = cut_error(case['input'])

    assert cut == case['expected']
    assert len(cut.encode('utf-8')) == case['expected_bytes']
    assert len(cut.encode('utf-8')) <= FIXTURE['error_cut_bytes']


def test_error_with_a_lone_surrogate_is_still_written():
    """Text decoded with surrogateescape has no UTF-8 form; the record must not be lost over it."""
    assert cut_error('exit 1: \udcff tail') == 'exit 1: ? tail'


def test_absent_values_write_no_header():
    bare = DLQRecord(key=None, value=b'v', stage='sink', error='boom')
    assert render_headers(bare) == [
        ('dk-record-version', b'1'),
        ('dk-stage', b'sink'),
        ('dk-error', b'boom'),
    ]

    # Zero is a value, not an absent one.
    zeros = DLQRecord(
        key=None,
        value=b'v',
        stage='sink',
        error='boom',
        attempts=0,
        source_partition=0,
        source_offset=0,
        source_timestamp_ms=0,
    )
    assert render_headers(zeros) == [
        ('dk-record-version', b'1'),
        ('dk-stage', b'sink'),
        ('dk-error', b'boom'),
        ('dk-attempts', b'0'),
        ('dk-source-partition', b'0'),
        ('dk-source-offset', b'0'),
        ('dk-source-timestamp', b'0'),
    ]


class _TypedInput(BaseModel):
    x: int


class _TypedOutput(BaseModel):
    v: str = ''


class _TypedHandler(BaseDrakkarHandler[_TypedInput, _TypedOutput]):
    async def arrange(self, messages, pending):
        return []


async def test_parse_failure_writes_the_record_not_json():
    sent: list[tuple[DLQRecord, int]] = []
    committed: list[int] = []

    async def dlq_send(record: DLQRecord, partition_id: int) -> bool:
        sent.append((record, partition_id))
        return True

    async def on_commit(partition_id: int, offset: int) -> None:
        committed.append(offset)

    processor = PartitionProcessor(
        partition_id=4,
        handler=_TypedHandler(),
        executor_pool=ExecutorPool(binary_path='/bin/echo', max_executors=1, task_timeout_seconds=10),
        window_size=10,
        on_commit=on_commit,
        on_parse_error='dlq',
        dlq_send=dlq_send,
    )
    raw = b'\xff not json'
    processor.enqueue(
        SourceMessage(
            topic='orders',
            partition=4,
            offset=9,
            key=b'order-9',
            value=raw,
            timestamp=1793000000123,
            headers=[('traceparent', b'00-ab-01'), ('dk-stage', b'sink')],
        )
    )
    processor.start()
    await wait_for(lambda: 10 in committed)
    await processor.stop()

    assert len(sent) == 1
    record, partition_id = sent[0]
    assert partition_id == 4
    assert record.key == b'order-9'
    assert record.value == raw
    headers = render_headers(record)
    names = [name for name, _ in headers]
    assert names[0] == 'traceparent'
    assert names.count('dk-stage') == 1
    assert dict(headers)['dk-stage'] == b'parse'
    assert dict(headers)['dk-source-topic'] == b'orders'
    assert dict(headers)['dk-source-partition'] == b'4'
    assert dict(headers)['dk-source-offset'] == b'9'
    assert dict(headers)['dk-source-timestamp'] == b'1793000000123'
    assert 'dk-attempts' not in names
    assert 'dk-sink' not in names
    assert dict(headers)['dk-error']


def _kafka_message(*, headers):
    msg = MagicMock()
    msg.error.return_value = None
    msg.topic.return_value = 'orders'
    msg.partition.return_value = 2
    msg.offset.return_value = 40
    msg.key.return_value = b'k'
    msg.value.return_value = b'v'
    msg.timestamp.return_value = (1, 1000)
    msg.headers.return_value = headers
    return msg


@patch('drakkar.consumer.AIOConsumer')
async def test_consumer_keeps_message_headers(mock_cls):
    inner = AsyncMock()
    inner.consume.return_value = [
        _kafka_message(headers=[('traceparent', b'00-ab-01'), ('empty', None), ('x-bin', b'\xff')]),
        _kafka_message(headers=None),
    ]
    mock_cls.return_value = inner
    consumer = KafkaConsumer(
        connection=KafkaConfig(brokers='localhost:9092'),
        source=KafkaSourceConfig(enabled=True, topic='orders', consumer_group='g'),
    )

    messages = await consumer.poll_batch(max_messages=10, timeout=0.1)

    assert messages[0].headers == [('traceparent', b'00-ab-01'), ('empty', None), ('x-bin', b'\xff')]
    assert messages[1].headers == []


async def test_read_dlq_entries_returns_key_value_headers():
    stored = [
        _kafka_message(headers=[('x-trace', b'1'), ('dk-stage', b'parse')]),
        _kafka_message(headers=None),
    ]
    stored[1].key.return_value = None
    stored[1].value.return_value = b'\xff raw'
    stored[1].offset.return_value = 41
    consumer = AsyncMock()
    consumer.consume.side_effect = [stored, []]

    with patch.object(dlq_module, 'AIOConsumer', return_value=consumer):
        entries = [
            entry
            async for entry in read_dlq_entries(
                topic='orders_dlq', brokers='localhost:9092', idle_polls_before_stop=1, poll_timeout=0.01
            )
        ]

    assert entries == [
        DLQEntry(key=b'k', value=b'v', headers=[('x-trace', b'1'), ('dk-stage', b'parse')], partition=2, offset=40),
        DLQEntry(key=None, value=b'\xff raw', headers=[], partition=2, offset=41),
    ]
