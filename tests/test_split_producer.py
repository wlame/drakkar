"""The split producer: delivery futures, error classes, a full client queue, order and the loop staying free."""

import asyncio

import pytest
from confluent_kafka import KafkaError, KafkaException

from drakkar.config.connections import KafkaConnectionConfig
from drakkar.split.producer import SplitProduceError, SplitProducer, is_final_error
from drakkar.split.records import WireRecord
from tests.split_helpers import FakeLog, FakeProducerClient

TOPIC = 'items_split_parts'


def record(index: int = 0, partition: int = 0) -> WireRecord:
    return WireRecord(key=b'k', value=str(index).encode(), headers=[('h', b'v')], partition=partition)


@pytest.fixture
def log() -> FakeLog:
    return FakeLog()


@pytest.fixture
def client(log) -> FakeProducerClient:
    return FakeProducerClient(log)


@pytest.fixture
async def producer(client):
    connection = KafkaConnectionConfig(brokers='broker:9092', client_config={'client.id': 'x'})

    def factory(config: dict) -> FakeProducerClient:
        client.config = config
        return client

    split_producer = SplitProducer(connection, producer_factory=factory)
    await split_producer.start()
    yield split_producer
    await split_producer.close(1.0)


async def test_publish_returns_after_the_delivery_report(producer, client, log):
    client.hold_deliveries = True
    task = asyncio.create_task(producer.publish(TOPIC, record(1, partition=3)))
    await asyncio.sleep(0.05)
    assert not task.done()
    client.release_deliveries()
    await asyncio.wait_for(task, 2)
    [stored] = log.records(TOPIC, 3)
    assert (stored.key, stored.value, stored.headers) == (b'k', b'1', [('h', b'v')])


async def test_client_config_has_the_connection_and_the_delivery_settings(producer, client):
    assert client.config['bootstrap.servers'] == 'broker:9092'
    assert client.config['client.id'] == 'x'
    assert (client.config['acks'], client.config['enable.idempotence'], client.config['linger.ms']) == ('all', True, 5)


@pytest.mark.parametrize(
    ('code', 'expected_final'),
    [
        (KafkaError.MSG_SIZE_TOO_LARGE, True),
        (KafkaError.UNKNOWN_TOPIC_OR_PART, True),
        (KafkaError.TOPIC_AUTHORIZATION_FAILED, True),
        (KafkaError._MSG_TIMED_OUT, False),
        (KafkaError._TRANSPORT, False),
    ],
)
async def test_failed_delivery_raises_with_the_right_final_flag(producer, client, code, expected_final):
    client.fail_next_deliveries(KafkaError(code))
    with pytest.raises(SplitProduceError) as raised:
        await producer.publish(TOPIC, record())
    assert raised.value.final is expected_final


def test_retriable_error_is_never_final_and_fatal_error_always_is():
    assert not is_final_error(KafkaError(KafkaError.MSG_SIZE_TOO_LARGE, retriable=True))
    assert is_final_error(KafkaError(KafkaError._TRANSPORT, fatal=True))


async def test_error_the_client_refuses_at_once_raises_from_enqueue(producer, client):
    client.fail_next('produce', KafkaException(KafkaError(KafkaError.MSG_SIZE_TOO_LARGE)))
    with pytest.raises(SplitProduceError) as raised:
        await producer.enqueue(TOPIC, record())
    assert raised.value.final


async def test_exception_without_an_error_object_is_not_final(producer, client):
    client.fail_next('produce', KafkaException('odd'))
    with pytest.raises(SplitProduceError) as raised:
        await producer.enqueue(TOPIC, record())
    assert not raised.value.final


async def test_delivery_report_without_a_message_or_error_fails(producer):
    callback = producer._delivery_callback(
        asyncio.get_running_loop(), future := asyncio.get_running_loop().create_future()
    )
    callback(None, None)
    with pytest.raises(SplitProduceError, match='no delivery report'):
        await asyncio.wait_for(future, 1)


async def test_delivery_report_that_arrives_for_a_cancelled_future_does_not_raise(producer):
    loop = asyncio.get_running_loop()
    future = loop.create_future()
    callback = producer._delivery_callback(loop, future)
    future.cancel()
    callback(None, None)
    await asyncio.sleep(0.01)
    assert future.cancelled()


async def test_buffer_full_makes_enqueue_wait_and_try_again(log):
    client = FakeProducerClient(log, queue_capacity=1)
    producer = SplitProducer(KafkaConnectionConfig(), producer_factory=lambda config: client)
    await producer.start()
    client.hold_deliveries = True
    first = await producer.enqueue(TOPIC, record(1))
    second = asyncio.create_task(producer.enqueue(TOPIC, record(2)))
    await asyncio.sleep(0.05)
    assert not second.done()
    client.release_deliveries()
    await asyncio.wait_for(first, 2)
    await asyncio.wait_for(await asyncio.wait_for(second, 2), 2)
    assert [r.value for r in log.records(TOPIC, 0)] == [b'1', b'2']
    await producer.close(1.0)


async def test_enqueue_before_start_is_refused():
    with pytest.raises(RuntimeError, match='not started'):
        await SplitProducer(KafkaConnectionConfig()).enqueue(TOPIC, record())


async def test_five_thousand_records_keep_their_order_and_the_loop_stays_free(producer, log):
    ticks = 0
    stop = False

    async def ticker() -> None:
        nonlocal ticks
        while not stop:
            ticks += 1
            await asyncio.sleep(0.001)

    ticking = asyncio.create_task(ticker())
    futures = []
    for index in range(5000):
        futures.append(await producer.enqueue(TOPIC, record(index)))
    await asyncio.wait_for(asyncio.gather(*futures), 20)
    stop = True
    await ticking
    assert [int(r.value) for r in log.records(TOPIC, 0)] == list(range(5000))
    assert ticks > 10


async def test_close_flushes_what_is_queued_and_ends_the_thread(log):
    client = FakeProducerClient(log)
    producer = SplitProducer(KafkaConnectionConfig(), producer_factory=lambda config: client)
    await producer.start()
    thread = producer._thread
    client.hold_deliveries = True
    future = await producer.enqueue(TOPIC, record(1))
    client.release_deliveries()
    await producer.close(1.0)
    await asyncio.wait_for(future, 1)
    assert not thread.is_alive()
    assert len(log.records(TOPIC, 0)) == 1


async def test_close_with_records_the_flush_could_not_deliver_still_returns(log):
    client = FakeProducerClient(log)
    producer = SplitProducer(KafkaConnectionConfig(), producer_factory=lambda config: client)
    await producer.start()
    client.hold_deliveries = True
    await producer.enqueue(TOPIC, record(1))
    await producer.close(0.1)
    assert log.records(TOPIC, 0) == []


async def test_close_without_start_does_nothing():
    await SplitProducer(KafkaConnectionConfig()).close(1.0)


async def test_a_failed_poll_is_logged_and_the_thread_goes_on(log):
    client = FakeProducerClient(log)
    client.fail_next('poll', RuntimeError('boom'))
    producer = SplitProducer(KafkaConnectionConfig(), producer_factory=lambda config: client)
    await producer.start()
    await producer.publish(TOPIC, record(1))
    await producer.close(1.0)
    assert len(log.records(TOPIC, 0)) == 1
