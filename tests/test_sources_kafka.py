"""KafkaSource: lifecycle contract on top of the moved poll-loop code."""

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock

from drakkar.config import DrakkarConfig
from drakkar.consume_pause import ConsumePauseController
from drakkar.sources.base import SourceContext
from drakkar.sources.kafka import KafkaSource


def _config() -> DrakkarConfig:
    return DrakkarConfig(
        ui={'release': {'enabled': False}},
        executor={'binary_path': '/usr/bin/echo', 'drain_timeout_seconds': 1},
        sources={'kafka': {'enabled': True, 'startup_align_enabled': False}},
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
        throughput=None,
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
    source._context().config.sources.kafka.startup_align_enabled = True
    monkeypatch.setattr('drakkar.sources.kafka.wait_for_aligned_startup', AsyncMock(return_value=0.25))

    with capture_logs() as cap:
        await source.start()

    waiting = next(entry for entry in cap if entry['event'] == 'startup_align_waiting')
    done = next(entry for entry in cap if entry['event'] == 'startup_align_done')
    # The published target must actually sit on an interval boundary —
    # that is the entire point of the alignment sleep.
    interval = source._context().config.sources.kafka.startup_align_interval_seconds
    assert waiting['target_wall_unix'] % interval == 0
    assert done['slept_seconds'] == 0.25
