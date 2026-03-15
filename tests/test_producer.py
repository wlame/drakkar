"""Tests for Drakkar Kafka producer wrapper."""

from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import KafkaException

from drakkar.config import KafkaConfig
from drakkar.models import OutputMessage
from drakkar.producer import KafkaProducer


@pytest.fixture
def kafka_config() -> KafkaConfig:
    return KafkaConfig(
        brokers="localhost:9092",
        target_topic="test-output",
    )


@patch("drakkar.producer.Producer")
async def test_produce_single_message(mock_producer_cls, kafka_config):
    mock_inner = MagicMock()

    def fake_produce(topic, key, value, callback):
        callback(None, MagicMock())  # success

    mock_inner.produce.side_effect = fake_produce
    mock_inner.poll.return_value = 0
    mock_producer_cls.return_value = mock_inner

    producer = KafkaProducer(kafka_config)
    msg = OutputMessage(key=b"k1", value=b'{"result": true}')
    await producer.produce(msg)

    mock_inner.produce.assert_called_once()
    call_kwargs = mock_inner.produce.call_args[1]
    assert call_kwargs["topic"] == "test-output"
    assert call_kwargs["key"] == b"k1"
    assert call_kwargs["value"] == b'{"result": true}'


@patch("drakkar.producer.Producer")
async def test_produce_delivery_error(mock_producer_cls, kafka_config):
    mock_inner = MagicMock()
    mock_err = MagicMock()
    mock_err.str.return_value = "delivery failed"

    def fake_produce(topic, key, value, callback):
        callback(mock_err, None)  # error

    mock_inner.produce.side_effect = fake_produce
    mock_inner.poll.return_value = 0
    mock_producer_cls.return_value = mock_inner

    producer = KafkaProducer(kafka_config)
    msg = OutputMessage(value=b"test")
    with pytest.raises(KafkaException):
        await producer.produce(msg)


@patch("drakkar.producer.Producer")
async def test_produce_batch(mock_producer_cls, kafka_config):
    mock_inner = MagicMock()

    def fake_produce(topic, key, value, callback):
        callback(None, MagicMock())

    mock_inner.produce.side_effect = fake_produce
    mock_inner.poll.return_value = 0
    mock_producer_cls.return_value = mock_inner

    producer = KafkaProducer(kafka_config)
    messages = [
        OutputMessage(key=b"k1", value=b"v1"),
        OutputMessage(key=b"k2", value=b"v2"),
        OutputMessage(value=b"v3"),
    ]
    await producer.produce_batch(messages)
    assert mock_inner.produce.call_count == 3


@patch("drakkar.producer.Producer")
async def test_flush(mock_producer_cls, kafka_config):
    mock_inner = MagicMock()
    mock_producer_cls.return_value = mock_inner

    producer = KafkaProducer(kafka_config)
    await producer.flush(timeout=5.0)
    mock_inner.flush.assert_called_once_with(5.0)


@patch("drakkar.producer.Producer")
def test_close(mock_producer_cls, kafka_config):
    mock_inner = MagicMock()
    mock_producer_cls.return_value = mock_inner

    producer = KafkaProducer(kafka_config)
    producer.close()
    mock_inner.flush.assert_called_once_with(timeout=30.0)


@patch("drakkar.producer.Producer")
def test_producer_creation(mock_producer_cls, kafka_config):
    KafkaProducer(kafka_config)
    mock_producer_cls.assert_called_once_with({
        "bootstrap.servers": "localhost:9092",
    })
