"""Tests for shutdown-phase Prometheus metrics.

Covers the three drain-phase metrics emitted by ``AppLifecycle._shutdown``
(see ``drakkar/metrics.py``):

  - ``drakkar_uncommitted_offsets_at_stop`` (Gauge)
  - ``drakkar_inflight_at_stop`` (Gauge)
  - ``drakkar_drain_timeout_hit_total`` (Counter)

Pattern mirrors ``tests/test_metrics.py``: read prometheus_client values
via ``Gauge._value.get()`` / ``Counter._value.get()``, snapshot the counter
before the test and assert the delta. Process-wide registry — tests don't
share state because each one targets a distinct metric or a delta read.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from drakkar.app import DrakkarApp
from drakkar.config import (
    DrakkarConfig,
    ExecutorConfig,
    KafkaConfig,
    KafkaSinkConfig,
    LoggingConfig,
    MetricsConfig,
    PostgresSinkConfig,
    SinksConfig,
)
from drakkar.executor import ExecutorPool
from drakkar.handler import BaseDrakkarHandler
from drakkar.metrics import (
    drain_timeout_hit,
    inflight_at_stop,
    uncommitted_offsets_at_stop,
)
from drakkar.models import ExecutorTask
from tests.sink_mocks import setup_app_sinks as _setup_app_sinks


class _StubHandler(BaseDrakkarHandler):
    """Minimal handler — only ``arrange`` is required by ``BaseDrakkarHandler``.

    The shutdown path never calls ``arrange`` (no messages are flowing),
    so the body can be a trivial passthrough that returns nothing.
    """

    async def arrange(self, messages, pending):
        return [
            ExecutorTask(
                task_id=f't-{msg.offset}',
                args=['noop'],
                source_offsets=[msg.offset],
            )
            for msg in messages
        ]


@pytest.fixture
def shutdown_config() -> DrakkarConfig:
    """A minimal config sufficient to construct ``DrakkarApp``.

    Mirrors the ``test_config`` fixture in ``tests/test_app.py`` but lives
    here to keep the test file self-contained. Sinks are required (the
    construction path validates that at least one sink is configured),
    even though we replace them with mocks before exercising shutdown.
    """
    return DrakkarConfig(
        kafka=KafkaConfig(
            brokers='localhost:9092',
            source_topic='test-in',
        ),
        executor=ExecutorConfig(
            binary_path='/bin/echo',
            max_executors=2,
            task_timeout_seconds=10,
            window_size=5,
        ),
        sinks=SinksConfig(
            kafka={'results': KafkaSinkConfig(topic='test-out')},
            postgres={'main': PostgresSinkConfig(dsn='postgresql://localhost/test')},
        ),
        metrics=MetricsConfig(enabled=False),
        logging=LoggingConfig(level='WARNING', format='console'),
    )


# --- Gauge snapshots at shutdown ---


async def test_shutdown_sets_uncommitted_offsets_gauge(shutdown_config):
    """``_shutdown`` snapshots the per-partition pending-offset count to
    ``drakkar_uncommitted_offsets_at_stop`` BEFORE drain begins.

    Sums ``OffsetTracker.pending_count`` across every assigned partition.
    """
    app = DrakkarApp(handler=_StubHandler(), config=shutdown_config)
    app._executor_pool = ExecutorPool(
        binary_path='/bin/echo',
        max_executors=2,
        task_timeout_seconds=10,
    )
    app._consumer = AsyncMock()
    _setup_app_sinks(app)
    app._dlq_sink = AsyncMock()

    # Spin up two processors and stage three uncommitted offsets across
    # them. The drain loop will see them as pending and bail out — but we
    # only care that the snapshot was taken correctly.
    app._lifecycle._on_assign([0, 1])
    app.processors[0]._offset_tracker.register(10)
    app.processors[0]._offset_tracker.register(11)
    app.processors[1]._offset_tracker.register(20)

    # Tight drain timeout so the test does not hang waiting on offsets
    # that will never be marked complete.
    shutdown_config.executor.drain_timeout_seconds = 0.05

    await app._lifecycle._shutdown()

    # 2 pending in partition 0 + 1 pending in partition 1 = 3.
    assert uncommitted_offsets_at_stop._value.get() == 3


async def test_shutdown_sets_uncommitted_offsets_to_zero_on_clean_state(shutdown_config):
    """The gauge must be set EVEN when the snapshot is zero.

    Otherwise a clean shutdown right after a noisy one would leave the
    stale value visible in scrape output, which is misleading. Test by
    seeding the gauge to a sentinel and confirming ``_shutdown`` resets it.
    """
    # Seed a stale value so we can detect the explicit set-to-zero.
    uncommitted_offsets_at_stop.set(99)

    app = DrakkarApp(handler=_StubHandler(), config=shutdown_config)
    app._executor_pool = ExecutorPool(
        binary_path='/bin/echo',
        max_executors=2,
        task_timeout_seconds=10,
    )
    app._consumer = AsyncMock()
    _setup_app_sinks(app)
    app._dlq_sink = AsyncMock()

    # No processors → no pending offsets → snapshot must be 0.
    await app._lifecycle._shutdown()

    assert uncommitted_offsets_at_stop._value.get() == 0


async def test_shutdown_sets_inflight_gauge(shutdown_config):
    """``_shutdown`` snapshots ``ExecutorPool.active_count`` to
    ``drakkar_inflight_at_stop`` BEFORE drain begins.
    """
    app = DrakkarApp(handler=_StubHandler(), config=shutdown_config)
    app._executor_pool = ExecutorPool(
        binary_path='/bin/echo',
        max_executors=2,
        task_timeout_seconds=10,
    )
    # Force a known active_count without spawning real subprocesses.
    # The pool exposes ``active_count`` as a read-only property over
    # ``_active_count``, so set the underlying field directly.
    app._executor_pool._active_count = 4
    app._consumer = AsyncMock()
    _setup_app_sinks(app)
    app._dlq_sink = AsyncMock()

    shutdown_config.executor.drain_timeout_seconds = 0.05

    await app._lifecycle._shutdown()

    assert inflight_at_stop._value.get() == 4


async def test_shutdown_inflight_zero_when_pool_missing(shutdown_config):
    """When ``_shutdown`` runs before startup wired up the executor pool
    (e.g. shutdown raised mid-boot), the gauge must still be set — to 0.
    """
    inflight_at_stop.set(77)

    app = DrakkarApp(handler=_StubHandler(), config=shutdown_config)
    # Deliberately leave _executor_pool as None.
    app._consumer = AsyncMock()
    _setup_app_sinks(app)
    app._dlq_sink = AsyncMock()

    await app._lifecycle._shutdown()

    assert inflight_at_stop._value.get() == 0


# --- Drain-timeout counter ---


async def test_drain_timeout_increments_counter(shutdown_config):
    """When ``_drain_all_processors`` exceeds its timeout, the
    ``drakkar_drain_timeout_hit_total`` counter increments by exactly 1.

    Stage a partition with a registered-but-never-completed offset so the
    drain loop spins indefinitely; pair with a tight timeout so the test
    finishes quickly.
    """
    before = drain_timeout_hit._value.get()

    shutdown_config.executor.drain_timeout_seconds = 0.05

    app = DrakkarApp(handler=_StubHandler(), config=shutdown_config)
    app._executor_pool = ExecutorPool(
        binary_path='/bin/echo',
        max_executors=2,
        task_timeout_seconds=10,
    )
    app._consumer = AsyncMock()
    _setup_app_sinks(app)
    app._dlq_sink = AsyncMock()

    app._lifecycle._on_assign([0])
    # Pending offset that never completes — drain hangs until timeout.
    app.processors[0]._offset_tracker.register(42)

    await app._lifecycle._shutdown()

    after = drain_timeout_hit._value.get()
    assert after - before == 1


async def test_drain_no_timeout_does_not_increment_counter(shutdown_config):
    """A clean drain (no pending work) must NOT bump the counter.

    Guards against accidentally moving the increment outside the timeout
    branch in a future refactor.
    """
    before = drain_timeout_hit._value.get()

    app = DrakkarApp(handler=_StubHandler(), config=shutdown_config)
    app._executor_pool = ExecutorPool(
        binary_path='/bin/echo',
        max_executors=2,
        task_timeout_seconds=10,
    )
    app._consumer = AsyncMock()
    _setup_app_sinks(app)
    app._dlq_sink = AsyncMock()

    # Assign a partition but leave its offset tracker empty so drain
    # returns immediately.
    app._lifecycle._on_assign([0])

    await app._lifecycle._shutdown()

    after = drain_timeout_hit._value.get()
    assert after == before


# --- Teardown is bounded and concurrent ---


async def test_shutdown_stops_processors_concurrently(shutdown_config):
    """Sequential stops cost one stop timeout per partition. The pod's grace
    period then expires mid-teardown — after the watchdog was marked clean, so
    the SIGKILL is recorded as a clean exit and the sink/DLQ/recorder/consumer
    closes never run.
    """
    shutdown_config.executor.drain_timeout_seconds = 0.05

    app = DrakkarApp(handler=_StubHandler(), config=shutdown_config)
    app._executor_pool = ExecutorPool(
        binary_path='/bin/echo',
        max_executors=2,
        task_timeout_seconds=10,
    )
    app._consumer = AsyncMock()
    _setup_app_sinks(app)
    app._dlq_sink = AsyncMock()

    app._lifecycle._on_assign([0, 1, 2])

    concurrent = 0
    peak = 0

    async def slow_stop(timeout: float = 10.0) -> None:
        nonlocal concurrent, peak
        concurrent += 1
        peak = max(peak, concurrent)
        await asyncio.sleep(0.05)
        concurrent -= 1

    for processor in app.processors.values():
        processor.stop = slow_stop

    await app._lifecycle._shutdown()

    assert peak == 3, f'processors stopped {peak} at a time, expected all 3 together'


async def test_shutdown_drain_timeout_cancels_the_zombie_tasks(shutdown_config):
    """A zombie that is only suppressed keeps its executor slot and its
    subprocess for up to ``task_timeout_seconds`` and keeps its processing
    loop from exiting, so every ``processor.stop()`` in the teardown waits out
    its full timeout inside the pod's grace period.
    """
    shutdown_config.executor.drain_timeout_seconds = 0.05

    app = DrakkarApp(handler=_StubHandler(), config=shutdown_config)
    app._executor_pool = ExecutorPool(
        binary_path='/bin/echo',
        max_executors=2,
        task_timeout_seconds=10,
    )
    app._consumer = AsyncMock()
    _setup_app_sinks(app)
    app._dlq_sink = AsyncMock()

    app._lifecycle._on_assign([0, 1])
    for processor in app.processors.values():
        # Pending offset that never completes — the drain hangs until timeout.
        processor._offset_tracker.register(42)

    cancelled: list[int] = []
    for pid, processor in app.processors.items():

        async def _record(_pid: int = pid) -> int:
            cancelled.append(_pid)
            return 0

        processor.cancel_active_tasks = _record

    await app._lifecycle._shutdown()

    # ``stop()`` sweeps again on its way out, so each partition appears more
    # than once; what matters is that no zombie was left running.
    assert set(cancelled) == {0, 1}


async def test_revoke_teardown_stays_within_the_drain_budget(shutdown_config):
    """librdkafka's rebalance thread blocks on ``_stop_processor``, so the
    drain, the final commit RPC and the processor stop all have to come out of
    one ``drain_timeout_seconds`` budget. A step taking its own budget lets the
    callback overrun ``max.poll.interval.ms`` and the member is evicted, which
    triggers the next rebalance.
    """
    shutdown_config.executor.drain_timeout_seconds = 0.2

    app = DrakkarApp(handler=_StubHandler(), config=shutdown_config)
    app._executor_pool = ExecutorPool(
        binary_path='/bin/echo',
        max_executors=2,
        task_timeout_seconds=10,
    )
    app._consumer = AsyncMock()
    _setup_app_sinks(app)
    app._dlq_sink = AsyncMock()

    app._lifecycle._on_assign([0])
    processor = app.processors[0]
    # Pending offset that never completes — the drain hangs until timeout.
    processor._offset_tracker.register(42)

    loop = asyncio.get_running_loop()
    start = loop.time()
    await app._lifecycle._stop_processor(processor)
    elapsed = loop.time() - start

    # Drain budget plus the step floor, with slack for scheduling. Before the
    # fix this was drain_timeout + a hardcoded 10s stop.
    assert elapsed < 3.0, f'revoke teardown took {elapsed:.1f}s on a 0.2s drain budget'


async def test_stop_processor_bounds_the_final_commit(shutdown_config):
    """The post-drain commit is a synchronous librdkafka call dispatched to the
    consumer's thread pool. Unbounded, a coordinator that stopped answering
    holds the rebalance callback open for as long as it stays silent.
    """
    shutdown_config.executor.drain_timeout_seconds = 0.2

    app = DrakkarApp(handler=_StubHandler(), config=shutdown_config)
    app._executor_pool = ExecutorPool(
        binary_path='/bin/echo',
        max_executors=2,
        task_timeout_seconds=10,
    )
    _setup_app_sinks(app)
    app._dlq_sink = AsyncMock()

    app._lifecycle._on_assign([0])
    processor = app.processors[0]
    processor._offset_tracker.register(7)
    processor._offset_tracker.complete(7)  # drains cleanly, so a commit is due

    async def never_answers(_offsets):
        await asyncio.sleep(3600)

    app._consumer = AsyncMock()
    app._consumer.commit = never_answers

    loop = asyncio.get_running_loop()
    start = loop.time()
    await app._lifecycle._stop_processor(processor)
    elapsed = loop.time() - start

    assert elapsed < 3.0, f'the final commit was unbounded: {elapsed:.1f}s'
    # The commit never confirmed, so the watermark must stay uncommitted.
    assert processor.offset_tracker.committable() == 8
