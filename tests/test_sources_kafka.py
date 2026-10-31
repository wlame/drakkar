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
    source, fake = _source_with_fake_consumer(monkeypatch)
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
