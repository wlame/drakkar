"""Tests for handler.offload(): the InlineOffloader stub and the OffloadPool.

Covers the contract promised by ``BaseDrakkarHandler.offload``:

- the function runs OFF the event-loop thread and its return value /
  exception propagate to the awaiting side unchanged;
- the hook context (``drakkar.hookctx``) is copied into the pool thread,
  so ``self.annotate(...)`` keeps anchoring correctly;
- the pool is bounded (``offload.max_threads``) with FIFO queueing, and
  its running/queued counters + Prometheus gauges stay coherent through
  completion, failure, and cancellation;
- one ``offload`` flight-recorder event is emitted per call, anchored
  like an annotation (hook context) or unanchored outside hooks.

All tests are isolated: the recorder is a hand-rolled fake capturing
``record_offload`` kwargs, no DB or app is constructed.
"""

from __future__ import annotations

import asyncio
import threading

import pytest

from drakkar import metrics
from drakkar.config import OffloadConfig
from drakkar.handler import BaseDrakkarHandler
from drakkar.hookctx import bind_hook_context, clear_hook_context, current_hook_context
from drakkar.offload import InlineOffloader, OffloaderLike, OffloadPool, resolve_max_threads


class FakeRecorder:
    """Captures record_offload calls; satisfies the recorder surface the pool uses."""

    def __init__(self) -> None:
        self.offload_events: list[dict] = []

    def record_offload(self, **kwargs) -> None:
        self.offload_events.append(kwargs)


def _gauge(metric) -> float:
    return metric._value.get()  # type: ignore[attr-defined]


@pytest.fixture
def pool():
    """A 2-thread pool with a fake recorder; shut down after the test."""
    recorder = FakeRecorder()
    p = OffloadPool(OffloadConfig(max_threads=2), recorder=recorder)
    p.recorder = recorder  # test-side back-reference for assertions
    yield p
    p.shutdown()


# --- InlineOffloader (the unit-test default stub) ----------------------------


def test_handler_default_offloader_is_inline_stub():
    """A bare handler subclass gets a working offload() without any app."""

    class H(BaseDrakkarHandler):
        pass

    assert isinstance(H()._offloader, InlineOffloader)
    assert isinstance(H()._offloader, OffloaderLike)


def test_inline_offloader_runs_off_the_loop_thread():
    async def scenario():
        return (threading.get_ident(), await InlineOffloader().run(threading.get_ident))

    loop_thread, worker_thread = asyncio.run(scenario())
    # The whole point: the function must not run on the loop's thread.
    assert worker_thread != loop_thread


def test_inline_offloader_forwards_args_kwargs_and_result():
    async def scenario():
        return await InlineOffloader().run(divmod, 17, 5)

    assert asyncio.run(scenario()) == (3, 2)


def test_inline_offloader_propagates_exceptions():
    def boom():
        raise ValueError('crunch failed')

    async def scenario():
        await InlineOffloader().run(boom)

    with pytest.raises(ValueError, match='crunch failed'):
        asyncio.run(scenario())


def test_handler_offload_delegates_to_installed_offloader():
    """The framework swaps _offloader per instance; offload() must use it."""

    class RecordingOffloader:
        def __init__(self) -> None:
            self.calls: list[tuple] = []

        async def run(self, fn, /, *args, **kwargs):
            self.calls.append((fn, args, kwargs))
            return 'sentinel'

    class H(BaseDrakkarHandler):
        pass

    handler = H()
    handler._offloader = RecordingOffloader()

    async def scenario():
        return await handler.offload(len, [1, 2, 3])

    assert asyncio.run(scenario()) == 'sentinel'
    assert handler._offloader.calls == [(len, ([1, 2, 3],), {})]


# --- OffloadPool: execution semantics ----------------------------------------


def test_pool_runs_function_on_named_pool_thread(pool):
    async def scenario():
        return await pool.run(lambda: threading.current_thread().name)

    assert asyncio.run(scenario()).startswith('drakkar-offload')


def test_pool_returns_result_and_forwards_kwargs(pool):
    def compute(base, *, exponent):
        return base**exponent

    async def scenario():
        return await pool.run(compute, 2, exponent=8)

    assert asyncio.run(scenario()) == 256


def test_pool_propagates_exception_and_records_error_event(pool):
    def boom():
        raise RuntimeError('bad plan')

    async def scenario():
        await pool.run(boom)

    with pytest.raises(RuntimeError, match='bad plan'):
        asyncio.run(scenario())

    (event,) = pool.recorder.offload_events
    assert event['status'] == 'error'
    assert 'bad plan' in event['error']
    assert event['function'].endswith('boom')


def test_pool_copies_hook_context_into_the_thread(pool):
    """current_hook_context() inside the offloaded fn sees the caller's hook.

    This is what keeps self.annotate(...) working from offloaded code.
    """

    async def scenario():
        token = bind_hook_context(hook='arrange', partition=3, window_id=9, offsets=(5, 6))
        try:
            return await pool.run(current_hook_context)
        finally:
            clear_hook_context(token)

    ctx = asyncio.run(scenario())
    assert ctx is not None
    assert (ctx.hook, ctx.partition, ctx.window_id, ctx.offsets) == ('arrange', 3, 9, (5, 6))


# --- OffloadPool: recorder events --------------------------------------------


def test_offload_event_anchored_by_hook_context(pool):
    async def scenario():
        token = bind_hook_context(hook='arrange', partition=3, window_id=9, offsets=(5, 6))
        try:
            await pool.run(sum, (1, 2, 3))
        finally:
            clear_hook_context(token)

    asyncio.run(scenario())
    (event,) = pool.recorder.offload_events
    assert event['hook'] == 'arrange'
    assert event['partition'] == 3
    assert event['window_id'] == 9
    assert event['offsets'] == (5, 6)
    assert event['status'] == 'ok'
    assert event['duration'] >= 0
    assert event['queued'] >= 0


def test_offload_event_outside_hooks_is_unanchored(pool):
    """offload() from on_ready / a periodic method still records a row."""

    async def scenario():
        await pool.run(sum, (1, 2))

    asyncio.run(scenario())
    (event,) = pool.recorder.offload_events
    assert event['hook'] == 'none'
    assert event['partition'] is None
    assert 'offset' not in event or event.get('offset') is None


def test_pool_without_recorder_still_runs():
    pool = OffloadPool(OffloadConfig(max_threads=1), recorder=None)
    try:

        async def scenario():
            return await pool.run(sum, (4, 5))

        assert asyncio.run(scenario()) == 9
    finally:
        pool.shutdown()


# --- OffloadPool: bounded concurrency + counters -----------------------------


def test_pool_bounds_concurrency_and_reports_queue():
    """With max_threads=1, a second offload queues; snapshot shows it."""
    pool = OffloadPool(OffloadConfig(max_threads=1), recorder=None)
    release = threading.Event()
    started = threading.Event()

    def blocker():
        started.set()
        release.wait(timeout=5)
        return 'first'

    async def scenario():
        first = asyncio.ensure_future(pool.run(blocker))
        # Wait until the blocker actually occupies the single thread.
        await asyncio.to_thread(started.wait, 5)
        second = asyncio.ensure_future(pool.run(lambda: 'second'))
        # Give the second call a moment to land in the executor queue.
        for _ in range(50):
            if pool.snapshot() == {'running': 1, 'queued': 1, 'max_threads': 1}:
                break
            await asyncio.sleep(0.01)
        snap_during = pool.snapshot()
        release.set()
        results = await asyncio.gather(first, second)
        return snap_during, results

    try:
        snap_during, results = asyncio.run(scenario())
        assert snap_during == {'running': 1, 'queued': 1, 'max_threads': 1}
        assert results == ['first', 'second']
        assert pool.snapshot() == {'running': 0, 'queued': 0, 'max_threads': 1}
        assert _gauge(metrics.offload_running) == 0
        assert _gauge(metrics.offload_queued) == 0
    finally:
        pool.shutdown()


def test_cancelling_a_queued_offload_pays_back_the_counter():
    """Cancel the queued (never-started) call: counters return to baseline."""
    pool = OffloadPool(OffloadConfig(max_threads=1), recorder=None)
    release = threading.Event()
    started = threading.Event()

    def blocker():
        started.set()
        release.wait(timeout=5)
        return 'first'

    async def scenario():
        first = asyncio.ensure_future(pool.run(blocker))
        await asyncio.to_thread(started.wait, 5)
        second = asyncio.ensure_future(pool.run(lambda: 'second'))
        for _ in range(50):
            if pool.snapshot()['queued'] == 1:
                break
            await asyncio.sleep(0.01)
        second.cancel()
        with pytest.raises(asyncio.CancelledError):
            await second
        release.set()
        assert await first == 'first'

    try:
        asyncio.run(scenario())
        assert pool.snapshot() == {'running': 0, 'queued': 0, 'max_threads': 1}
    finally:
        pool.shutdown()


def test_offload_after_shutdown_raises_and_leaks_no_counter():
    pool = OffloadPool(OffloadConfig(max_threads=1), recorder=None)
    pool.shutdown()

    async def scenario():
        await pool.run(sum, (1, 2))

    with pytest.raises(RuntimeError):
        asyncio.run(scenario())
    assert pool.snapshot() == {'running': 0, 'queued': 0, 'max_threads': 1}


class TestResolveMaxThreads:
    @pytest.mark.parametrize(
        ('pool_max', 'expected'),
        [
            (1, 2),  # floor of 2
            (4, 2),
            (8, 2),  # the motivating examples: ceil(n/4), min 2
            (9, 3),
            (12, 3),
            (13, 4),
            (16, 4),
            (40, 10),
            (0, 2),  # no executor pool known — floor
        ],
    )
    def test_auto_scales_with_the_executor_pool(self, pool_max, expected):
        assert resolve_max_threads(0, pool_max) == expected

    def test_explicit_value_wins_untouched(self):
        assert resolve_max_threads(7, 40) == 7
        assert resolve_max_threads(1, 40) == 1

    def test_pool_uses_the_resolved_auto_size(self):
        pool = OffloadPool(OffloadConfig(), recorder=None, executor_pool_max=13)
        try:
            assert pool.max_threads == 4
        finally:
            pool.shutdown()

    def test_default_config_without_executor_hint_floors_at_two(self):
        pool = OffloadPool(OffloadConfig(), recorder=None)
        try:
            assert pool.max_threads == 2
        finally:
            pool.shutdown()


async def test_lifecycle_wires_auto_sized_pool_from_the_executor_config():
    """The wiring path resolves max_threads=0 against executor.max_executors
    — pinned end to end because the executor field name is easy to get
    wrong (the similarly named pool_max belongs to the Postgres sink)."""
    from types import SimpleNamespace

    from drakkar.config import DrakkarConfig
    from drakkar.lifecycle import AppLifecycle

    config = DrakkarConfig()
    config.executor.max_executors = 13
    handler = SimpleNamespace(_offloader=None)
    app = SimpleNamespace(_config=config, _recorder=None, _handler=handler, _offload_pool=None)
    lifecycle = AppLifecycle.__new__(AppLifecycle)
    lifecycle._app = app

    lifecycle._wire_offload_pool()

    try:
        assert app._offload_pool.max_threads == 4  # ceil(13 / 4)
        assert handler._offloader is app._offload_pool
    finally:
        app._offload_pool.shutdown()
