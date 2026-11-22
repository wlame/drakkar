"""The split producer: delivery futures, error classes, a full client queue, order and the loop staying free."""

import asyncio
import threading
from unittest.mock import MagicMock

import pytest
from confluent_kafka import KafkaError, KafkaException

from drakkar.config.connections import KafkaConnectionConfig
from drakkar.split.producer import NO_OFFSET, SplitProduceError, SplitProducer, is_final_error
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


async def test_five_thousand_records_keep_their_order_and_the_loop_stays_free(producer, client, log):
    # The delivery reports are held back, as a slow broker holds them: enqueueing does not wait for them.
    client.hold_deliveries = True
    turned = asyncio.Event()

    async def take_turns() -> None:
        # Ends only when the event loop gives turns while every record waits for its report.
        for _ in range(3):
            await asyncio.sleep(0)
        turned.set()

    futures = [await producer.enqueue(TOPIC, record(index)) for index in range(5000)]
    turning = asyncio.create_task(take_turns())
    await asyncio.wait_for(turned.wait(), 5)
    assert not any(future.done() for future in futures)

    client.release_deliveries()
    await asyncio.wait_for(asyncio.gather(*futures), 20)
    await turning
    assert [int(r.value) for r in log.records(TOPIC, 0)] == list(range(5000))


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


async def finished_thread(func, *args):
    """Run ``func`` on a thread and return with no turn of the loop, as ``asyncio.to_thread`` may.

    When the thread ends before the loop registers its callback, the awaited future is already done
    and ``await`` does not give the loop a turn; the reports the thread handed to the loop still wait.
    """
    outcome: list = []
    thread = threading.Thread(target=lambda: outcome.append(func(*args)))
    thread.start()
    thread.join()
    return outcome[0]


async def test_close_settles_a_record_the_flush_delivered_when_the_flush_returns_without_a_loop_turn(log, monkeypatch):
    client = FakeProducerClient(log)
    producer = SplitProducer(KafkaConnectionConfig(), producer_factory=lambda config: client)
    await producer.start()
    client.hold_deliveries = True
    future = await producer.enqueue(TOPIC, record(1))
    client.release_deliveries()
    monkeypatch.setattr(asyncio, 'to_thread', finished_thread)
    await producer.close(1.0)
    assert future.done()
    assert future.result() == 0
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


async def test_start_twice_keeps_one_poll_thread(log):
    producer = SplitProducer(KafkaConnectionConfig(), producer_factory=lambda config: FakeProducerClient(log))
    await producer.start()
    thread = producer._thread
    await producer.start()
    assert producer._thread is thread
    await producer.close(1.0)


async def test_close_fails_the_futures_the_flush_could_not_deliver(log):
    client = FakeProducerClient(log)
    producer = SplitProducer(KafkaConnectionConfig(), producer_factory=lambda config: client)
    await producer.start()
    client.hold_deliveries = True
    future = await producer.enqueue(TOPIC, record(1))
    await producer.close(0.1)
    assert future.done()
    with pytest.raises(SplitProduceError) as raised:
        future.result()
    assert not raised.value.final


async def test_future_the_client_never_reports_is_failed_by_close(log):
    class Forgetful(FakeProducerClient):
        def purge(self, *args, **kwargs):
            self._pending.clear()  # drops records without any report

    client = Forgetful(log)
    producer = SplitProducer(KafkaConnectionConfig(), producer_factory=lambda config: client)
    await producer.start()
    client.hold_deliveries = True
    future = await producer.enqueue(TOPIC, record(1))
    await producer.close(0.1)
    with pytest.raises(SplitProduceError, match='closed'):
        future.result()


async def test_enqueue_after_close_is_refused(log):
    producer = SplitProducer(KafkaConnectionConfig(), producer_factory=lambda config: FakeProducerClient(log))
    await producer.start()
    await producer.close(1.0)
    with pytest.raises(RuntimeError, match='not started'):
        await producer.enqueue(TOPIC, record())


async def test_waiter_on_a_full_queue_gives_up_when_the_producer_closes(log):
    client = FakeProducerClient(log, queue_capacity=1)
    producer = SplitProducer(KafkaConnectionConfig(), producer_factory=lambda config: client)
    await producer.start()
    client.hold_deliveries = True
    await producer.enqueue(TOPIC, record(1))
    waiter = asyncio.create_task(producer.enqueue(TOPIC, record(2)))
    await asyncio.sleep(0.05)
    await producer.close(0.1)
    with pytest.raises(SplitProduceError) as raised:
        await asyncio.wait_for(waiter, 2)
    assert not raised.value.final


async def test_only_the_first_of_two_waiters_tries_the_full_queue(log):
    client = FakeProducerClient(log, queue_capacity=1)
    producer = SplitProducer(KafkaConnectionConfig(), producer_factory=lambda config: client)
    await producer.start()
    client.hold_deliveries = True
    await producer.enqueue(TOPIC, record(1))
    second = asyncio.create_task(producer.enqueue(TOPIC, record(2)))
    third = asyncio.create_task(producer.enqueue(TOPIC, record(3)))
    await asyncio.sleep(0.05)
    assert set(client.rejected) == {b'2'}
    client.release_deliveries()
    await asyncio.wait_for(await asyncio.wait_for(second, 2), 2)
    await asyncio.wait_for(await asyncio.wait_for(third, 2), 2)
    assert [r.value for r in log.records(TOPIC, 0)] == [b'1', b'2', b'3']
    await producer.close(1.0)


async def test_repeated_poll_failures_are_logged_once_in_the_interval(log, monkeypatch):
    from drakkar.split import producer as producer_module

    lines = []
    monkeypatch.setattr(producer_module.logger, 'error', lambda event, **fields: lines.append(fields))
    client = FakeProducerClient(log)
    client.fail_next('poll', RuntimeError('boom'), times=3)
    producer = SplitProducer(KafkaConnectionConfig(), producer_factory=lambda config: client)
    await producer.start()
    await producer.publish(TOPIC, record(1))
    await producer.close(1.0)
    assert len(lines) == 1


async def test_enqueue_nowait_gives_the_delivery_future_while_the_queue_has_room(producer, log):
    future = producer.enqueue_nowait(TOPIC, record(7, partition=2))
    assert future is not None
    await asyncio.wait_for(future, 2)
    assert [stored.value for stored in log.records(TOPIC, 2)] == [b'7']


async def test_enqueue_nowait_gives_none_for_a_full_queue_and_behind_a_waiting_caller(log):
    client = FakeProducerClient(log, queue_capacity=1)
    producer = SplitProducer(KafkaConnectionConfig(), producer_factory=lambda config: client)
    await producer.start()
    client.hold_deliveries = True
    first = producer.enqueue_nowait(TOPIC, record(1))
    assert first is not None
    # The queue is full: nothing is given to the client, and the caller is told so at once.
    assert producer.enqueue_nowait(TOPIC, record(2)) is None
    waiter = asyncio.create_task(producer.enqueue(TOPIC, record(2)))
    await asyncio.sleep(0.02)
    client.queue_capacity = 5
    # Room again, but another caller waits for it: records keep their order.
    assert producer.enqueue_nowait(TOPIC, record(3)) is None
    client.release_deliveries()
    await asyncio.wait_for(await asyncio.wait_for(waiter, 2), 2)
    third = producer.enqueue_nowait(TOPIC, record(3))
    assert third is not None
    await asyncio.wait_for(third, 2)
    assert [stored.value for stored in log.records(TOPIC, 0)] == [b'1', b'2', b'3']
    await producer.close(1.0)


async def test_enqueue_nowait_raises_for_a_record_the_client_refuses_and_before_start(producer, client):
    client.fail_next('produce', KafkaException(KafkaError(KafkaError.MSG_SIZE_TOO_LARGE)))
    with pytest.raises(SplitProduceError) as raised:
        producer.enqueue_nowait(TOPIC, record())
    assert raised.value.final
    with pytest.raises(RuntimeError, match='not started'):
        SplitProducer(KafkaConnectionConfig()).enqueue_nowait(TOPIC, record())


async def test_a_delivered_record_gives_its_offset_in_the_partition(producer, log):
    assert await producer.publish(TOPIC, record(0, partition=3)) == 0
    assert await producer.publish(TOPIC, record(1, partition=3)) == 1
    # Another partition counts for itself.
    assert await producer.publish(TOPIC, record(2, partition=4)) == 0
    queued = await producer.enqueue(TOPIC, record(3, partition=3))
    at_once = producer.enqueue_nowait(TOPIC, record(4, partition=3))
    assert at_once is not None
    assert (await queued, await at_once) == (2, 3)
    assert [stored.offset for stored in log.records(TOPIC, 3)] == [0, 1, 2, 3]


async def test_a_delivery_report_that_names_no_offset_gives_the_mark_for_no_offset(producer):
    loop = asyncio.get_running_loop()
    future: asyncio.Future[int] = loop.create_future()
    message = MagicMock()
    message.error.return_value = None
    message.offset.return_value = None

    producer._delivery_callback(loop, future)(None, message)

    assert await asyncio.wait_for(future, 2) == NO_OFFSET
