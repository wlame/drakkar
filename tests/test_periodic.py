"""Tests for periodic task decorator, discovery, and scheduling."""

import asyncio

import pytest

from drakkar.handler import BaseDrakkarHandler
from drakkar.periodic import (
    PERIODIC_ATTR,
    PeriodicMeta,
    discover_periodic_tasks,
    periodic,
    run_periodic_task,
)


# --- Decorator tests ---


def test_periodic_sets_metadata():
    @periodic(seconds=10)
    async def my_task(self):
        pass

    meta = getattr(my_task, PERIODIC_ATTR)
    assert isinstance(meta, PeriodicMeta)
    assert meta.seconds == 10
    assert meta.on_error == 'continue'


def test_periodic_custom_on_error():
    @periodic(seconds=5, on_error='stop')
    async def my_task(self):
        pass

    meta = getattr(my_task, PERIODIC_ATTR)
    assert meta.on_error == 'stop'


def test_periodic_rejects_non_positive_seconds():
    with pytest.raises(ValueError, match='positive'):

        @periodic(seconds=0)
        async def my_task(self):
            pass


def test_periodic_rejects_negative_seconds():
    with pytest.raises(ValueError, match='positive'):

        @periodic(seconds=-5)
        async def my_task(self):
            pass


def test_periodic_rejects_sync_function():
    with pytest.raises(TypeError, match='async'):

        @periodic(seconds=10)
        def not_async(self):
            pass


def test_periodic_preserves_function_identity():
    async def my_task(self):
        pass

    decorated = periodic(seconds=10)(my_task)
    assert decorated is my_task


# --- Discovery tests ---


def test_discover_periodic_tasks_on_handler():
    class MyHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return []

        @periodic(seconds=30)
        async def refresh_cache(self):
            pass

        @periodic(seconds=60, on_error='stop')
        async def health_check(self):
            pass

    handler = MyHandler()
    tasks = discover_periodic_tasks(handler)
    names = {name for name, _, _ in tasks}
    assert names == {'refresh_cache', 'health_check'}

    for name, method, meta in tasks:
        if name == 'refresh_cache':
            assert meta.seconds == 30
            assert meta.on_error == 'continue'
        elif name == 'health_check':
            assert meta.seconds == 60
            assert meta.on_error == 'stop'


def test_discover_periodic_tasks_empty_handler():
    class PlainHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return []

    handler = PlainHandler()
    tasks = discover_periodic_tasks(handler)
    assert tasks == []


def test_discover_returns_bound_methods():
    class MyHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return []

        @periodic(seconds=10)
        async def tick(self):
            pass

    handler = MyHandler()
    tasks = discover_periodic_tasks(handler)
    assert len(tasks) == 1
    _, method, _ = tasks[0]
    # bound method — calling it requires no extra `self`
    assert hasattr(method, '__self__')
    assert method.__self__ is handler


# --- Scheduler loop tests ---


async def test_run_periodic_task_executes_on_interval():
    call_count = 0

    async def task():
        nonlocal call_count
        call_count += 1

    t = asyncio.create_task(
        run_periodic_task(
            name='test_task',
            coro_fn=task,
            seconds=0.05,
            on_error='continue',
        )
    )

    await asyncio.sleep(0.18)
    t.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t

    assert call_count >= 2


async def test_run_periodic_task_no_overlap():
    """Long-running task should not overlap — effective cycle = sleep + execution."""
    timestamps: list[float] = []

    async def slow_task():
        timestamps.append(asyncio.get_event_loop().time())
        await asyncio.sleep(0.1)

    t = asyncio.create_task(
        run_periodic_task(
            name='slow_task',
            coro_fn=slow_task,
            seconds=0.05,
            on_error='continue',
        )
    )

    await asyncio.sleep(0.4)
    t.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t

    # effective cycle is ~0.15s (0.05 sleep + 0.1 execution)
    # in 0.4s we expect ~2-3 runs, not 8 (which would be 0.4/0.05)
    assert len(timestamps) <= 4


async def test_run_periodic_task_on_error_continue():
    call_count = 0

    async def failing_task():
        nonlocal call_count
        call_count += 1
        raise RuntimeError("boom")

    t = asyncio.create_task(
        run_periodic_task(
            name='failing_continue',
            coro_fn=failing_task,
            seconds=0.05,
            on_error='continue',
        )
    )

    await asyncio.sleep(0.18)
    t.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t

    # task kept running despite errors
    assert call_count >= 2


async def test_run_periodic_task_on_error_stop():
    call_count = 0

    async def failing_task():
        nonlocal call_count
        call_count += 1
        raise RuntimeError("fatal")

    t = asyncio.create_task(
        run_periodic_task(
            name='failing_stop',
            coro_fn=failing_task,
            seconds=0.05,
            on_error='stop',
        )
    )

    await asyncio.sleep(0.2)
    # task should have stopped after first error, not cancelled
    assert t.done()
    assert call_count == 1


async def test_run_periodic_task_cancellation():
    call_count = 0

    async def task():
        nonlocal call_count
        call_count += 1

    t = asyncio.create_task(
        run_periodic_task(
            name='cancel_me',
            coro_fn=task,
            seconds=0.05,
            on_error='continue',
        )
    )

    await asyncio.sleep(0.08)
    t.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t


async def test_run_periodic_task_sleep_first():
    """First execution should happen after one interval, not immediately."""
    call_count = 0

    async def task():
        nonlocal call_count
        call_count += 1

    t = asyncio.create_task(
        run_periodic_task(
            name='sleep_first',
            coro_fn=task,
            seconds=0.2,
            on_error='continue',
        )
    )

    # check immediately — should not have run yet
    await asyncio.sleep(0.05)
    assert call_count == 0

    t.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t


async def test_run_periodic_task_cancellation_during_sleep():
    """Cancelling during sleep should propagate cleanly."""
    executed = False

    async def task():
        nonlocal executed
        executed = True

    t = asyncio.create_task(
        run_periodic_task(
            name='cancel_during_sleep',
            coro_fn=task,
            seconds=10.0,
            on_error='continue',
        )
    )

    await asyncio.sleep(0.05)
    t.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t

    assert not executed


async def test_run_periodic_task_cancellation_during_execution():
    """Cancelling during task execution should propagate CancelledError."""
    started = asyncio.Event()

    async def blocking_task():
        started.set()
        await asyncio.sleep(100)

    t = asyncio.create_task(
        run_periodic_task(
            name='cancel_during_exec',
            coro_fn=blocking_task,
            seconds=0.01,
            on_error='continue',
        )
    )

    await started.wait()
    t.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t


# --- Integration with handler class ---


async def test_handler_periodic_methods_are_callable():
    """Periodic methods on a handler instance work as bound methods."""
    results: list[str] = []

    class MyHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return []

        @periodic(seconds=0.05)
        async def tick(self):
            results.append('tick')

    handler = MyHandler()
    tasks = discover_periodic_tasks(handler)
    assert len(tasks) == 1

    name, method, meta = tasks[0]
    t = asyncio.create_task(
        run_periodic_task(
            name=name,
            coro_fn=method,
            seconds=meta.seconds,
            on_error=meta.on_error,
        )
    )

    await asyncio.sleep(0.13)
    t.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t

    assert len(results) >= 2
    assert all(r == 'tick' for r in results)


async def test_handler_periodic_accesses_self_state():
    """Periodic tasks can access handler instance state set during on_ready."""

    class StatefulHandler(BaseDrakkarHandler):
        def __init__(self):
            self.counter = 0

        async def arrange(self, messages, pending):
            return []

        @periodic(seconds=0.05)
        async def increment(self):
            self.counter += 1

    handler = StatefulHandler()
    tasks = discover_periodic_tasks(handler)
    name, method, meta = tasks[0]

    t = asyncio.create_task(
        run_periodic_task(
            name=name,
            coro_fn=method,
            seconds=meta.seconds,
            on_error=meta.on_error,
        )
    )

    await asyncio.sleep(0.18)
    t.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t

    assert handler.counter >= 2


async def test_multiple_periodic_tasks_run_independently():
    """Multiple periodic tasks with different intervals run concurrently."""
    fast_count = 0
    slow_count = 0

    class MultiHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return []

        @periodic(seconds=0.05)
        async def fast_task(self):
            nonlocal fast_count
            fast_count += 1

        @periodic(seconds=0.15)
        async def slow_task(self):
            nonlocal slow_count
            slow_count += 1

    handler = MultiHandler()
    tasks_meta = discover_periodic_tasks(handler)
    running = []

    for name, method, meta in tasks_meta:
        t = asyncio.create_task(
            run_periodic_task(
                name=name,
                coro_fn=method,
                seconds=meta.seconds,
                on_error=meta.on_error,
            )
        )
        running.append(t)

    await asyncio.sleep(0.35)

    for t in running:
        t.cancel()
    await asyncio.gather(*running, return_exceptions=True)

    assert fast_count > slow_count
    assert fast_count >= 4
    assert slow_count >= 1
