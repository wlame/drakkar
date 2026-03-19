"""Tests for Drakkar Kafka consumer wrapper."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from drakkar.config import KafkaConfig
from drakkar.consumer import KafkaConsumer


@pytest.fixture
def kafka_config() -> KafkaConfig:
    return KafkaConfig(
        brokers='localhost:9092',
        source_topic='test-source',
        target_topic='test-target',
        consumer_group='test-group',
        max_poll_records=10,
    )


def make_mock_message(
    topic='test-source', partition=0, offset=0, key=b'k', value=b'v', timestamp=1000
):
    msg = MagicMock()
    msg.error.return_value = None
    msg.topic.return_value = topic
    msg.partition.return_value = partition
    msg.offset.return_value = offset
    msg.key.return_value = key
    msg.value.return_value = value
    msg.timestamp.return_value = (1, timestamp)
    return msg


def make_error_message(error_code):
    msg = MagicMock()
    err = MagicMock()
    err.code.return_value = error_code
    msg.error.return_value = err
    return msg


@patch('drakkar.consumer.AIOConsumer')
def test_consumer_creation(mock_cls, kafka_config):
    _consumer = KafkaConsumer(kafka_config)
    mock_cls.assert_called_once()
    call_args = mock_cls.call_args[0][0]
    assert call_args['bootstrap.servers'] == 'localhost:9092'
    assert call_args['group.id'] == 'test-group'
    assert call_args['enable.auto.commit'] is False
    assert call_args['partition.assignment.strategy'] == 'cooperative-sticky'


@patch('drakkar.consumer.AIOConsumer')
async def test_consumer_subscribe(mock_cls, kafka_config):
    mock_inner = AsyncMock()
    mock_cls.return_value = mock_inner

    consumer = KafkaConsumer(kafka_config)
    await consumer.subscribe()

    mock_inner.subscribe.assert_called_once()
    call_args = mock_inner.subscribe.call_args
    assert call_args[0][0] == ['test-source']


@patch('drakkar.consumer.AIOConsumer')
async def test_poll_batch_returns_messages(mock_cls, kafka_config):
    mock_inner = AsyncMock()
    mock_inner.consume.return_value = [
        make_mock_message(partition=0, offset=10, value=b'msg1'),
        make_mock_message(partition=1, offset=20, value=b'msg2'),
    ]
    mock_cls.return_value = mock_inner

    consumer = KafkaConsumer(kafka_config)
    messages = await consumer.poll_batch(max_messages=10, timeout=0.1)

    assert len(messages) == 2
    assert messages[0].partition == 0
    assert messages[0].offset == 10
    assert messages[0].value == b'msg1'
    assert messages[1].partition == 1


@patch('drakkar.consumer.AIOConsumer')
async def test_poll_batch_empty(mock_cls, kafka_config):
    mock_inner = AsyncMock()
    mock_inner.consume.return_value = []
    mock_cls.return_value = mock_inner

    consumer = KafkaConsumer(kafka_config)
    messages = await consumer.poll_batch(timeout=0.1)
    assert messages == []


@patch('drakkar.consumer.AIOConsumer')
async def test_poll_batch_skips_partition_eof(mock_cls, kafka_config):
    from confluent_kafka import KafkaError

    mock_inner = AsyncMock()
    mock_inner.consume.return_value = [
        make_error_message(KafkaError._PARTITION_EOF),
        make_mock_message(partition=0, offset=5, value=b'valid'),
    ]
    mock_cls.return_value = mock_inner

    consumer = KafkaConsumer(kafka_config)
    messages = await consumer.poll_batch(timeout=0.1)
    assert len(messages) == 1
    assert messages[0].value == b'valid'


@patch('drakkar.consumer.AIOConsumer')
async def test_poll_batch_logs_errors(mock_cls, kafka_config):
    from confluent_kafka import KafkaError

    mock_inner = AsyncMock()
    mock_inner.consume.return_value = [
        make_error_message(KafkaError._ALL_BROKERS_DOWN),
    ]
    mock_cls.return_value = mock_inner

    consumer = KafkaConsumer(kafka_config)
    messages = await consumer.poll_batch(timeout=0.1)
    assert messages == []


@patch('drakkar.consumer.AIOConsumer')
async def test_commit_offsets(mock_cls, kafka_config):
    mock_inner = AsyncMock()
    mock_cls.return_value = mock_inner

    consumer = KafkaConsumer(kafka_config)
    await consumer.commit({0: 100, 1: 200})

    mock_inner.commit.assert_called_once()
    call_kwargs = mock_inner.commit.call_args[1]
    assert len(call_kwargs['offsets']) == 2
    assert call_kwargs['asynchronous'] is False


@patch('drakkar.consumer.AIOConsumer')
async def test_on_assign_callback(mock_cls, kafka_config):
    mock_inner = AsyncMock()
    mock_cls.return_value = mock_inner

    assigned = []
    consumer = KafkaConsumer(kafka_config, on_assign=lambda parts: assigned.extend(parts))
    await consumer.subscribe()

    # get the async callback registered with subscribe
    call_kwargs = mock_inner.subscribe.call_args[1]
    assign_cb = call_kwargs['on_assign']
    from confluent_kafka import TopicPartition

    await assign_cb(mock_inner, [TopicPartition('test-source', 0), TopicPartition('test-source', 1)])
    assert assigned == [0, 1]


@patch('drakkar.consumer.AIOConsumer')
async def test_on_revoke_callback(mock_cls, kafka_config):
    mock_inner = AsyncMock()
    mock_cls.return_value = mock_inner

    revoked = []
    consumer = KafkaConsumer(kafka_config, on_revoke=lambda parts: revoked.extend(parts))
    await consumer.subscribe()

    call_kwargs = mock_inner.subscribe.call_args[1]
    revoke_cb = call_kwargs['on_revoke']
    from confluent_kafka import TopicPartition

    await revoke_cb(mock_inner, [TopicPartition('test-source', 3)])
    assert revoked == [3]


@patch('drakkar.consumer.AIOConsumer')
async def test_pause_partitions(mock_cls, kafka_config):
    mock_inner = AsyncMock()
    mock_cls.return_value = mock_inner

    consumer = KafkaConsumer(kafka_config)
    await consumer.pause([0, 3])

    mock_inner.pause.assert_called_once()
    tps = mock_inner.pause.call_args[0][0]
    assert len(tps) == 2
    assert tps[0].partition == 0
    assert tps[1].partition == 3


@patch('drakkar.consumer.AIOConsumer')
async def test_resume_partitions(mock_cls, kafka_config):
    mock_inner = AsyncMock()
    mock_cls.return_value = mock_inner

    consumer = KafkaConsumer(kafka_config)
    await consumer.resume([1, 5])

    mock_inner.resume.assert_called_once()
    tps = mock_inner.resume.call_args[0][0]
    assert len(tps) == 2


@patch('drakkar.consumer.AIOConsumer')
async def test_get_total_lag(mock_cls, kafka_config):
    from confluent_kafka import TopicPartition

    mock_inner = AsyncMock()
    mock_cls.return_value = mock_inner

    # committed returns TopicPartitions with offsets
    tp0 = TopicPartition(kafka_config.source_topic, 0, 90)
    tp1 = TopicPartition(kafka_config.source_topic, 1, 80)
    mock_inner.committed.return_value = [tp0, tp1]

    # watermarks: (low, high)
    async def fake_watermarks(tp):
        if tp.partition == 0:
            return (0, 100)  # lag = 100 - 90 = 10
        return (0, 95)  # lag = 95 - 80 = 15

    mock_inner.get_watermark_offsets.side_effect = fake_watermarks

    consumer = KafkaConsumer(kafka_config)
    total = await consumer.get_total_lag([0, 1])
    assert total == 25  # 10 + 15


@patch('drakkar.consumer.AIOConsumer')
async def test_close(mock_cls, kafka_config):
    mock_inner = AsyncMock()
    mock_cls.return_value = mock_inner

    consumer = KafkaConsumer(kafka_config)
    await consumer.close()
    mock_inner.close.assert_called_once()
