"""Tests for Drakkar Kafka producer wrapper."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from drakkar.config import KafkaConfig
from drakkar.models import OutputMessage
from drakkar.producer import KafkaProducer


@pytest.fixture
def kafka_config() -> KafkaConfig:
    return KafkaConfig(
        brokers='localhost:9092',
        target_topic='test-output',
    )


def _make_delivered_msg():
    msg = MagicMock()
    msg.topic.return_value = 'test-output'
    msg.partition.return_value = 0
    msg.offset.return_value = 42
    return msg


@patch('drakkar.producer.AIOProducer')
async def test_produce_single_message(mock_aio_cls, kafka_config):
    mock_inner = AsyncMock()
    delivered = _make_delivered_msg()
    # produce() returns a coroutine that resolves to a future
    delivery_future: asyncio.Future = asyncio.get_event_loop().create_future()
    delivery_future.set_result(delivered)
    mock_inner.produce.return_value = delivery_future
    mock_inner.flush.return_value = None
    mock_aio_cls.return_value = mock_inner

    producer = KafkaProducer(kafka_config)
    msg = OutputMessage(key=b'k1', value=b'{"result": true}')
    await producer.produce(msg)

    mock_inner.produce.assert_called_once()
    call_kwargs = mock_inner.produce.call_args[1]
    assert call_kwargs['topic'] == 'test-output'
    assert call_kwargs['key'] == b'k1'
    assert call_kwargs['value'] == b'{"result": true}'


@patch('drakkar.producer.AIOProducer')
async def test_produce_delivery_error(mock_aio_cls, kafka_config):
    mock_inner = AsyncMock()
    delivery_future: asyncio.Future = asyncio.get_event_loop().create_future()
    delivery_future.set_exception(Exception('delivery failed'))
    mock_inner.produce.return_value = delivery_future
    mock_inner.flush.return_value = None
    mock_aio_cls.return_value = mock_inner

    producer = KafkaProducer(kafka_config)
    msg = OutputMessage(value=b'test')
    with pytest.raises(Exception, match='delivery failed'):
        await producer.produce(msg)


@patch('drakkar.producer.AIOProducer')
async def test_produce_batch(mock_aio_cls, kafka_config):
    mock_inner = AsyncMock()

    def make_future(*args, **kwargs):
        f: asyncio.Future = asyncio.get_event_loop().create_future()
        f.set_result(_make_delivered_msg())
        return f

    mock_inner.produce.side_effect = make_future
    mock_inner.flush.return_value = None
    mock_aio_cls.return_value = mock_inner

    producer = KafkaProducer(kafka_config)
    messages = [
        OutputMessage(key=b'k1', value=b'v1'),
        OutputMessage(key=b'k2', value=b'v2'),
        OutputMessage(value=b'v3'),
    ]
    await producer.produce_batch(messages)
    assert mock_inner.produce.call_count == 3
    mock_inner.flush.assert_called_once()


@patch('drakkar.producer.AIOProducer')
async def test_flush(mock_aio_cls, kafka_config):
    mock_inner = AsyncMock()
    mock_aio_cls.return_value = mock_inner

    producer = KafkaProducer(kafka_config)
    await producer.flush(timeout=5.0)
    mock_inner.flush.assert_called_once_with(5.0)


@patch('drakkar.producer.AIOProducer')
async def test_close(mock_aio_cls, kafka_config):
    mock_inner = AsyncMock()
    mock_aio_cls.return_value = mock_inner

    producer = KafkaProducer(kafka_config)
    await producer.close()
    mock_inner.close.assert_called_once()


@patch('drakkar.producer.AIOProducer')
def test_producer_creation(mock_aio_cls, kafka_config):
    KafkaProducer(kafka_config)
    mock_aio_cls.assert_called_once_with(
        {
            'bootstrap.servers': 'localhost:9092',
        }
    )
