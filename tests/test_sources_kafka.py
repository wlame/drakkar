"""KafkaSource: lifecycle contract on top of the moved poll-loop code."""

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock

from structlog.testing import capture_logs

from drakkar.config import DrakkarConfig
from drakkar.consume_pause import ConsumePauseController
from drakkar.executor import ExecutorPool
from drakkar.models import SourceMessage
from drakkar.partition import PartitionProcessor
from drakkar.sources.base import SourceContext
from drakkar.sources.kafka import KafkaSource
from tests.conftest import kafka_connections


def _config() -> DrakkarConfig:
    return DrakkarConfig(
        connections=kafka_connections(),
        ui={'release': {'enabled': False}},
        executor={'binary_path': '/usr/bin/echo', 'drain_timeout': '1s'},
        sources={'kafka': {'enabled': True, 'startup_align': {'enabled': False}}},
        sinks={'kafka': {'out': {'topic': 't'}}},
    )


def _context(config: DrakkarConfig) -> SourceContext:
    pool = MagicMock()
    pool.active_count = 0
    return SourceContext(
        config=config,
        handler=MagicMock(),
        executor_pool=pool,
        sink_manager=MagicMock(),
        dlq_sink=None,
        recorder=None,
        worker_id='w',
        cluster_name='',
        on_collect=AsyncMock(),
        is_worker_ready=lambda: True,
    )


def _source_with_fake_consumer(monkeypatch) -> tuple[KafkaSource, MagicMock]:
    config = _config()
    fake = MagicMock()
    fake.subscribe = AsyncMock()
    fake.poll_batch = AsyncMock(return_value=[])
    fake.close = AsyncMock()
    fake.commit = AsyncMock()
    fake.pause = AsyncMock()
    fake.resume = AsyncMock()
    monkeypatch.setattr('drakkar.sources.kafka.KafkaConsumer', lambda **kwargs: fake)
    source = KafkaSource(consume_pause=MagicMock(spec=ConsumePauseController, active=False))
    source.bind(_context(config))
    return source, fake


async def test_start_subscribes_and_is_not_ready_before_first_poll(monkeypatch):
    source, fake = _source_with_fake_consumer(monkeypatch)
    await source.start()
    fake.subscribe.assert_awaited_once()
    assert source.is_ready is False


async def test_run_flips_ready_after_first_poll_and_exits_on_signal_stop(monkeypatch):
    source, _ = _source_with_fake_consumer(monkeypatch)
    await source.start()
    task = asyncio.create_task(source.run())
    for _ in range(50):
        if source.is_ready:
            break
        await asyncio.sleep(0.01)
    assert source.is_ready is True
    source.signal_stop()
    await asyncio.wait_for(task, timeout=2)


async def test_run_with_stop_during_poll_does_not_enqueue_the_polled_messages(monkeypatch):
    source, fake = _source_with_fake_consumer(monkeypatch)
    await source.start()
    processor = MagicMock(queue_size=0, inflight_count=0)
    source.processors[0] = processor
    source.processors[3] = MagicMock(queue_size=0, inflight_count=0)
    messages = [MagicMock(partition=3, offset=9), MagicMock(partition=0, offset=7), MagicMock(partition=3, offset=10)]

    async def poll_then_stop(*_args, **_kwargs):
        source.signal_stop()
        return messages

    fake.poll_batch = AsyncMock(side_effect=poll_then_stop)
    with capture_logs() as logs:
        await asyncio.wait_for(source.run(), timeout=2)

    processor.enqueue.assert_not_called()
    source.processors[3].enqueue.assert_not_called()
    dropped = [entry for entry in logs if entry['event'] == 'messages_polled_at_stop_dropped']
    assert dropped == [
        {
            'event': 'messages_polled_at_stop_dropped',
            'log_level': 'info',
            'category': 'kafka',
            'count': 3,
            'partitions': [0, 3],
        }
    ]


async def test_run_with_stop_during_an_empty_poll_logs_no_drop(monkeypatch):
    source, fake = _source_with_fake_consumer(monkeypatch)
    await source.start()

    async def poll_then_stop(*_args, **_kwargs):
        source.signal_stop()
        return []

    fake.poll_batch = AsyncMock(side_effect=poll_then_stop)
    with capture_logs() as logs:
        await asyncio.wait_for(source.run(), timeout=2)

    assert not [entry for entry in logs if entry['event'] == 'messages_polled_at_stop_dropped']


async def test_drain_returns_when_a_message_reaches_a_partition_whose_loop_has_ended(monkeypatch):
    source, _ = _source_with_fake_consumer(monkeypatch)
    await source.start()
    processor = PartitionProcessor(
        partition_id=0,
        handler=MagicMock(),
        executor_pool=ExecutorPool(binary_path='/bin/echo', max_executors=1, task_timeout_seconds=10),
        window_size=10,
    )
    source.processors[0] = processor
    processor.start()
    source.signal_stop()
    for _ in range(200):
        if processor._task is not None and processor._task.done():
            break
        await asyncio.sleep(0.01)
    processor.enqueue(SourceMessage(topic='t', partition=0, offset=5, value=b'{}', timestamp=1000))

    started = time.monotonic()
    drained = await source.drain(deadline=started + 1)

    assert drained is True
    assert time.monotonic() - started < 0.5


async def test_stop_closes_consumer(monkeypatch):
    source, fake = _source_with_fake_consumer(monkeypatch)
    await source.start()
    source.signal_stop()
    assert await source.drain(deadline=time.monotonic() + 1) is True
    await source.stop(deadline=time.monotonic() + 1)
    fake.close.assert_awaited_once()


async def test_snapshot_reports_partitions_and_pause_state(monkeypatch):
    source, _ = _source_with_fake_consumer(monkeypatch)
    assert source.snapshot() == {
        'assigned_partitions': [],
        'partition_count': 0,
        'paused': False,
        'total_queued': 0,
    }


async def test_name_is_kafka(monkeypatch):
    source, _ = _source_with_fake_consumer(monkeypatch)
    assert source.name == 'kafka'


async def test_start_logs_the_wall_clock_alignment_window(monkeypatch):
    """The alignment log events must name the exact wall-clock boundary a
    fleet converges on, and report how long this worker actually waited."""
    from structlog.testing import capture_logs

    source, _ = _source_with_fake_consumer(monkeypatch)
    source._context().config.sources.kafka.startup_align.enabled = True
    monkeypatch.setattr('drakkar.sources.kafka.wait_for_aligned_startup', AsyncMock(return_value=0.25))

    with capture_logs() as cap:
        await source.start()

    waiting = next(entry for entry in cap if entry['event'] == 'startup_align_waiting')
    done = next(entry for entry in cap if entry['event'] == 'startup_align_done')
    # The published target must actually sit on an interval boundary —
    # that is the entire point of the alignment sleep.
    interval = source._context().config.sources.kafka.startup_align.interval
    assert waiting['target_wall_unix'] % interval == 0
    assert done['slept_seconds'] == 0.25


async def test_lag_refresh_sets_gauge_for_assigned_partitions(monkeypatch):
    from drakkar.metrics import consumer_lag

    source, fake = _source_with_fake_consumer(monkeypatch)
    fake.cached_broker_lag = AsyncMock(return_value={7: 42})
    await source.start()
    source.processors[7] = MagicMock()

    await source._refresh_lag_once()

    assert consumer_lag.labels(partition='7')._value.get() == 42
    args, kwargs = fake.cached_broker_lag.await_args
    assert args == ([7], frozenset())
    assert kwargs['may_query']() is True
    consumer_lag.remove('7')
    await source.stop(time.monotonic() + 1)


async def test_revoke_removes_the_lag_label(monkeypatch):
    from drakkar.metrics import consumer_lag

    source, _ = _source_with_fake_consumer(monkeypatch)
    consumer_lag.labels(partition='8').set(5)
    processor = MagicMock()
    processor.stop = AsyncMock()
    source.processors[8] = processor

    await source.on_revoke([8])

    assert '8' not in {sample.labels['partition'] for sample in consumer_lag.collect()[0].samples}


async def test_lag_refresh_failure_is_logged_once_and_not_raised(monkeypatch):
    source, fake = _source_with_fake_consumer(monkeypatch)
    fake.cached_broker_lag = AsyncMock(side_effect=RuntimeError('boom'))
    await source.start()

    with capture_logs() as logs:
        await source._refresh_lag_once()
        await source._refresh_lag_once()

    failures = [entry for entry in logs if entry['event'] == 'consumer_lag_refresh_failed']
    assert len(failures) == 1
    assert failures[0]['error'] == 'boom'
    await source.stop(time.monotonic() + 1)


def _bare_consumer(committed: dict[int, int]):
    """A KafkaConsumer with no client behind it, its own commits set to ``committed``."""
    from drakkar.consumer import LAG_REFRESH_MAX_QUERIES_IN_FLIGHT, KafkaConsumer

    consumer = KafkaConsumer.__new__(KafkaConsumer)
    consumer._source = MagicMock(topic='t')
    consumer._offsets_committed_here = dict(committed)
    consumer._lag_query_slots = asyncio.Semaphore(LAG_REFRESH_MAX_QUERIES_IN_FLIGHT)
    consumer._last_lag_queried = None
    return consumer


async def test_paused_partition_lag_uses_a_real_watermark_query(monkeypatch):
    consumer = _bare_consumer({0: 10, 1: 10})
    calls = []

    async def watermarks(tp, **kwargs):
        calls.append((tp.partition, kwargs))
        return (0, 100) if kwargs.get('cached') else (0, 500)

    consumer._consumer = MagicMock(get_watermark_offsets=watermarks)

    lag = await consumer.cached_broker_lag([0, 1], frozenset({1}))

    assert lag == {0: 90, 1: 490}
    assert (0, {'cached': True}) in calls
    assert any(pid == 1 and 'timeout' in kw and 'cached' not in kw for pid, kw in calls)


async def test_failed_real_query_skips_only_that_paused_partition():
    consumer = _bare_consumer({0: 10, 1: 10})

    async def watermarks(tp, **kwargs):
        if tp.partition == 1:
            raise RuntimeError('timed out')
        return (0, 100)

    consumer._consumer = MagicMock(get_watermark_offsets=watermarks)

    assert await consumer.cached_broker_lag([0, 1], frozenset({1})) == {0: 90}


async def test_paused_partitions_are_all_assigned_ones_under_backpressure(monkeypatch):
    source, _ = _source_with_fake_consumer(monkeypatch)
    source.processors.update({1: MagicMock(), 2: MagicMock()})
    source.stalled_partitions.add(2)

    assert source._paused_partitions() == frozenset({2})
    source.paused = True
    assert source._paused_partitions() == frozenset({1, 2})


def _real_query_recorder(consumer, delay: float = 0.0):
    """Answer every real query with high watermark 500; return the queried partitions and the peak in flight."""
    seen: dict = {'queried': [], 'in_flight': 0, 'peak': 0}

    async def watermarks(tp, **kwargs):
        if kwargs.get('cached'):
            return (0, 100)
        seen['queried'].append(tp.partition)
        seen['in_flight'] += 1
        seen['peak'] = max(seen['peak'], seen['in_flight'])
        await asyncio.sleep(delay)
        seen['in_flight'] -= 1
        return (0, 500)

    consumer._consumer = MagicMock(get_watermark_offsets=watermarks)
    return seen


async def test_lag_refresh_queries_at_most_eight_paused_partitions_rotating_across_rounds():
    consumer = _bare_consumer({pid: 0 for pid in range(10)})
    seen = _real_query_recorder(consumer)
    paused = frozenset(range(10))

    first = await consumer.cached_broker_lag(list(range(10)), paused)
    second = await consumer.cached_broker_lag(list(range(10)), paused)

    assert sorted(first) == list(range(8))
    assert sorted(second) == [0, 1, 2, 3, 4, 5, 8, 9]
    assert seen['queried'] == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5]


async def test_lag_refresh_keeps_at_most_two_real_queries_in_flight():
    consumer = _bare_consumer({pid: 0 for pid in range(8)})
    seen = _real_query_recorder(consumer, delay=0.01)

    lag = await consumer.cached_broker_lag(list(range(8)), frozenset(range(8)))

    assert len(lag) == 8
    assert seen['peak'] == 2


async def test_lag_refresh_sends_nothing_once_may_query_answers_false():
    consumer = _bare_consumer({0: 10, 1: 10})
    seen = _real_query_recorder(consumer)

    lag = await consumer.cached_broker_lag([0, 1], frozenset({1}), may_query=lambda: False)

    assert lag == {}
    assert seen['queried'] == []


async def test_lag_refresh_query_failure_goes_to_the_callback_and_is_not_a_consumer_error():
    from drakkar.metrics import consumer_errors

    consumer = _bare_consumer({1: 10})

    async def watermarks(tp, **kwargs):
        raise RuntimeError('timed out')

    consumer._consumer = MagicMock(get_watermark_offsets=watermarks)
    failures = []
    errors_before = consumer_errors._value.get()

    with capture_logs() as logs:
        lag = await consumer.cached_broker_lag(
            [1], frozenset({1}), on_query_failure=lambda pid, exc: failures.append((pid, str(exc)))
        )

    assert lag == {}
    assert failures == [(1, 'timed out')]
    assert consumer_errors._value.get() == errors_before
    assert not [entry for entry in logs if entry['event'] == 'lag_query_failed']


async def test_lag_refresh_does_nothing_while_a_revoke_drains(monkeypatch):
    source, fake = _source_with_fake_consumer(monkeypatch)
    fake.cached_broker_lag = AsyncMock(return_value={})
    await source.start()
    source.processors[3] = MagicMock()
    release = asyncio.Event()
    revoked = MagicMock()

    async def slow_stop(processor):
        await release.wait()

    monkeypatch.setattr(source, '_stop_processor', slow_stop)
    source.processors[4] = revoked
    revoke = asyncio.create_task(source.on_revoke([4]))
    await asyncio.sleep(0)

    await source._refresh_lag_once()
    assert source._lag_refresh_allowed() is False
    fake.cached_broker_lag.assert_not_awaited()

    release.set()
    await revoke
    await source._refresh_lag_once()
    fake.cached_broker_lag.assert_awaited_once()
    await source.stop(time.monotonic() + 1)


async def test_lag_refresh_does_nothing_after_a_stop_request(monkeypatch):
    source, fake = _source_with_fake_consumer(monkeypatch)
    fake.cached_broker_lag = AsyncMock(return_value={})
    await source.start()

    source.signal_stop()
    await source._refresh_lag_once()

    fake.cached_broker_lag.assert_not_awaited()
    await source.stop(time.monotonic() + 1)


async def test_lag_refresh_query_failures_share_one_rate_limited_line(monkeypatch):
    source, fake = _source_with_fake_consumer(monkeypatch)

    async def failing_refresh(partition_ids, paused_ids, *, may_query, on_query_failure):
        on_query_failure(1, RuntimeError('slow broker'))
        on_query_failure(2, RuntimeError('slow broker'))
        return {}

    fake.cached_broker_lag = failing_refresh
    await source.start()

    with capture_logs() as logs:
        await source._refresh_lag_once()
        await source._refresh_lag_once()

    failures = [entry for entry in logs if entry['event'] == 'consumer_lag_refresh_failed']
    assert len(failures) == 1
    assert failures[0]['partition'] == 1
    assert failures[0]['error'] == 'slow broker'
    await source.stop(time.monotonic() + 1)
