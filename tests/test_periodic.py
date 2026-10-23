"""Tests for periodic task decorator, discovery, and scheduling."""

import asyncio
import math
import time
import warnings
from datetime import UTC, datetime

import pytest
import structlog
from structlog.testing import capture_logs

from drakkar.cronexpr import CRON_FIELDS_MESSAGE
from drakkar.handler import BaseDrakkarHandler
from drakkar.periodic import (
    PERIODIC_ATTR,
    RULE_CLUSTER_INTERVAL,
    RULE_LEASE_BOUNDS,
    RULE_LEASE_SCOPE,
    RULE_WORKER_INTERVAL,
    SYSTEM_CLOCK,
    PeriodicMeta,
    PeriodicRuleError,
    check_periodic_rules,
    discover_periodic_tasks,
    periodic,
    run_periodic_task,
    run_task_once,
)
from tests.conftest import make_ui_config, wait_for

# --- Decorator tests ---


def test_periodic_sets_metadata():
    @periodic(interval=10)
    async def my_task(self):
        pass

    meta = getattr(my_task, PERIODIC_ATTR)
    assert isinstance(meta, PeriodicMeta)
    assert meta.interval == 10
    assert meta.on_error == 'continue'


def test_periodic_custom_on_error():
    @periodic(interval=5, on_error='stop')
    async def my_task(self):
        pass

    meta = getattr(my_task, PERIODIC_ATTR)
    assert meta.on_error == 'stop'


def test_periodic_positional_interval_keeps_working():
    @periodic(10)
    async def my_task(self):
        pass

    meta = getattr(my_task, PERIODIC_ATTR)
    assert meta == PeriodicMeta(interval=10, cron=None, scope='worker', lease_seconds=None, on_error='continue')


def test_periodic_cluster_cron_metadata():
    @periodic(cron='0 3 * * *', scope='cluster', lease_seconds=7200, on_error='stop')
    async def nightly(self):
        pass

    meta = getattr(nightly, PERIODIC_ATTR)
    assert meta == PeriodicMeta(interval=None, cron='0 3 * * *', scope='cluster', lease_seconds=7200, on_error='stop')


def test_periodic_seconds_keyword_is_gone():
    with pytest.raises(TypeError):
        periodic(seconds=10)  # type: ignore[call-arg]


def test_zero_interval_is_refused_as_not_positive():
    with pytest.raises(PeriodicRuleError) as caught:

        @periodic(0)
        async def my_task(self):
            pass

    assert str(caught.value) == "periodic task 'my_task': " + RULE_WORKER_INTERVAL


@pytest.mark.parametrize(
    ('scope', 'rule'),
    [('cluster', RULE_LEASE_BOUNDS), ('worker', RULE_LEASE_SCOPE)],
)
def test_zero_lease_is_refused(scope, rule):
    with pytest.raises(PeriodicRuleError) as caught:

        @periodic(60, scope=scope, lease_seconds=0)
        async def my_task(self):
            pass

    assert str(caught.value) == "periodic task 'my_task': " + rule


def test_periodic_rule_error_is_a_value_error():
    with pytest.raises(ValueError):

        @periodic(-5)
        async def my_task(self):
            pass


def test_periodic_raises_the_rule_error_only_when_it_decorates():
    """The rules need the method name, so periodic(...) itself accepts a bad declaration."""
    decorate = periodic(-5)

    async def my_task(self):
        pass

    with pytest.raises(PeriodicRuleError):
        decorate(my_task)


def test_periodic_empty_cron_counts_as_given_and_is_refused():
    with pytest.raises(PeriodicRuleError) as caught:

        @periodic(cron='')
        async def my_task(self):
            pass

    assert str(caught.value) == "periodic task 'my_task': " + CRON_FIELDS_MESSAGE.format(expr='')


def test_periodic_cron_that_is_not_text_is_refused():
    with pytest.raises(PeriodicRuleError) as caught:

        @periodic(cron=5)  # type: ignore[arg-type]
        async def my_task(self):
            pass

    assert str(caught.value) == "periodic task 'my_task': " + CRON_FIELDS_MESSAGE.format(expr=5)


def test_check_periodic_rules_accepts_a_whole_number_float_cluster_interval():
    rule = check_periodic_rules(
        name='refresh', interval=30.0, cron=None, scope='cluster', lease_seconds=None, now_epoch=0
    )

    assert rule is None


@pytest.mark.parametrize(
    ('scope', 'interval', 'rule'),
    [
        ('worker', True, RULE_WORKER_INTERVAL),
        ('worker', '10', RULE_WORKER_INTERVAL),
        ('cluster', '60', RULE_CLUSTER_INTERVAL),
    ],
)
def test_check_periodic_rules_refuses_an_interval_that_is_not_a_number(scope, interval, rule):
    found = check_periodic_rules(
        name='refresh', interval=interval, cron=None, scope=scope, lease_seconds=None, now_epoch=0
    )

    assert found == rule


def test_system_clock_reads_the_wall_clock():
    before = time.time()

    assert before <= SYSTEM_CLOCK.time() <= time.time()


async def test_run_periodic_task_needs_exactly_one_schedule():
    async def task():
        pass

    with pytest.raises(ValueError, match='exactly one of interval and cron'):
        await run_periodic_task('t', task, on_error='continue')
    with pytest.raises(ValueError, match='exactly one of interval and cron'):
        await run_periodic_task('t', task, interval=1, cron='* * * * *', on_error='continue')


def test_periodic_rejects_sync_function():
    with pytest.raises(TypeError, match='async'):

        @periodic(interval=10)
        def not_async(self):
            pass


def test_periodic_decorator_emits_no_deprecation_warning():
    """Guard for Python 3.14+: asyncio.iscoroutinefunction is deprecated
    (removal slated for 3.16); the decorator must not trip it. On 3.13 no
    warning exists, so this passes trivially — the guard protects the
    newer CI lanes."""
    with warnings.catch_warnings():
        warnings.simplefilter('error', DeprecationWarning)

        @periodic(interval=10)
        async def my_task(self):
            pass


def test_periodic_preserves_function_identity():
    async def my_task(self):
        pass

    decorated = periodic(interval=10)(my_task)
    assert decorated is my_task


# --- Discovery tests ---


def test_discover_periodic_tasks_on_handler():
    class MyHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return []

        @periodic(interval=30)
        async def refresh_cache(self):
            pass

        @periodic(interval=60, on_error='stop')
        async def health_check(self):
            pass

    handler = MyHandler()
    tasks = discover_periodic_tasks(handler)
    names = {name for name, _, _ in tasks}
    assert names == {'refresh_cache', 'health_check'}

    for name, _method, meta in tasks:
        if name == 'refresh_cache':
            assert meta.interval == 30
            assert meta.on_error == 'continue'
        elif name == 'health_check':
            assert meta.interval == 60
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

        @periodic(interval=10)
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
#
# Tests that count invocations wait on the count itself (wait_for) instead of
# sleeping a fixed wall-clock window: a stalled shared CI runner can delay
# even the first 50ms tick past any fixed window, but a condition wait stays
# correct on slow machines and fast on quick ones.


async def _cancel_periodic(t: asyncio.Task) -> None:
    """Cancel a run_periodic_task loop and wait for it to end."""
    t.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t


async def test_run_periodic_task_executes_on_interval():
    call_count = 0

    async def task():
        nonlocal call_count
        call_count += 1

    t = asyncio.create_task(
        run_periodic_task(
            name='test_task',
            coro_fn=task,
            interval=0.05,
            on_error='continue',
        )
    )

    await wait_for(lambda: call_count >= 2)
    await _cancel_periodic(t)

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
            interval=0.05,
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
        raise RuntimeError('boom')

    t = asyncio.create_task(
        run_periodic_task(
            name='failing_continue',
            coro_fn=failing_task,
            interval=0.05,
            on_error='continue',
        )
    )

    # a second call happening at all proves the loop survived the first error
    await wait_for(lambda: call_count >= 2)
    await _cancel_periodic(t)

    assert call_count >= 2


async def test_run_periodic_task_on_error_stop():
    call_count = 0

    async def failing_task():
        nonlocal call_count
        call_count += 1
        raise RuntimeError('fatal')

    t = asyncio.create_task(
        run_periodic_task(
            name='failing_stop',
            coro_fn=failing_task,
            interval=0.05,
            on_error='stop',
        )
    )

    # poll until the task self-terminates (avoids flaky wall-clock sleep)
    await wait_for(lambda: t.done())
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
            interval=0.05,
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
            interval=0.2,
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
            interval=10.0,
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
            interval=0.01,
            on_error='continue',
        )
    )

    await started.wait()
    t.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t


# --- Scheduler loop with a fake clock ---


class FakeClock:
    """A clock that advances only when the loop sleeps; no test waits for real time.

    ``early`` lists how many seconds each of the first sleeps wakes before it
    was asked to, like ``asyncio.sleep`` rounding or a clock that runs behind.
    """

    def __init__(self, now: float, early: tuple[float, ...] = ()) -> None:
        self.now = now
        self.early = list(early)
        self.sleeps: list[float] = []

    def time(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds - (self.early.pop(0) if self.early else 0)
        await asyncio.sleep(0)


def at(text: str) -> float:
    """Epoch seconds of an ISO 8601 UTC time."""
    return datetime.fromisoformat(text).replace(tzinfo=UTC).timestamp()


async def _collect_starts(clock: FakeClock, runs: int, run_seconds: float, **schedule) -> list[float]:
    """Run the loop until ``runs`` runs have started; return the clock time of each start."""
    starts: list[float] = []

    async def body():
        starts.append(clock.time())
        if len(starts) == runs:
            raise asyncio.CancelledError
        await clock.sleep(run_seconds)

    with pytest.raises(asyncio.CancelledError):
        await run_periodic_task('fake_clock_task', body, on_error='continue', clock=clock, **schedule)
    return starts


async def test_worker_interval_task_sleeps_interval_after_the_run_ended():
    clock = FakeClock(now=1000.0)

    starts = await _collect_starts(clock, runs=3, run_seconds=3, interval=10)

    assert starts == [1010.0, 1023.0, 1036.0]


async def test_worker_cron_task_runs_at_fire_times_and_skips_those_passed_during_a_run():
    clock = FakeClock(now=at('2026-11-01T00:01:30.700'))

    # every five minutes; each run lasts 7 minutes, so the next fire time passes during it
    starts = await _collect_starts(clock, runs=3, run_seconds=420, cron='*/5 * * * *')

    assert starts == [at('2026-11-01T00:05:00'), at('2026-11-01T00:15:00'), at('2026-11-01T00:25:00')]


async def test_worker_cron_task_wakes_early_and_sleeps_again():
    clock = FakeClock(now=at('2026-11-01T00:00:10'), early=(0.4, 0.05))

    starts = await _collect_starts(clock, runs=1, run_seconds=0, cron='* * * * *')

    assert starts == [at('2026-11-01T00:01:00')]
    assert len(clock.sleeps) == 3
    assert clock.sleeps[0] == 50
    assert math.isclose(clock.sleeps[1], 0.4, abs_tol=1e-6)
    assert math.isclose(clock.sleeps[2], 0.05, abs_tol=1e-6)


async def test_worker_cron_task_never_runs_a_fire_time_twice_when_the_clock_steps_back():
    clock = FakeClock(now=at('2026-11-01T00:00:50'))
    starts: list[float] = []

    async def body():
        starts.append(clock.time())
        if len(starts) == 2:
            raise asyncio.CancelledError
        clock.now -= 5  # the clock steps back during the run

    with pytest.raises(asyncio.CancelledError):
        await run_periodic_task('step_back', body, cron='* * * * *', on_error='continue', clock=clock)

    assert starts == [at('2026-11-01T00:01:00'), at('2026-11-01T00:02:00')]


class RecorderThatFailsOnOk:
    """A recorder whose ok row raises, to check that a failing count is a failed run."""

    def __init__(self) -> None:
        self.statuses: list[str] = []

    def record_periodic_run(self, name, duration, status, error='', system=False, *, slot=None):
        self.statuses.append(status)
        if status == 'ok':
            raise RuntimeError('recorder is full')


@pytest.mark.parametrize(
    ('on_error', 'expected_statuses'), [('continue', ['ok', 'error'] * 2), ('stop', ['ok', 'error'])]
)
async def test_recorder_error_on_the_ok_path_is_a_failed_run(on_error, expected_statuses):
    from drakkar.metrics import periodic_task_runs

    clock = FakeClock(now=1000.0)
    recorder = RecorderThatFailsOnOk()
    errors = periodic_task_runs.labels(name='recorder_fails', status='error')
    errors_before = errors._value.get()
    runs = 0

    async def body():
        nonlocal runs
        runs += 1
        if runs == 3:
            raise asyncio.CancelledError

    loop = run_periodic_task(
        'recorder_fails',
        body,
        interval=10,
        on_error=on_error,
        recorder=recorder,
        clock=clock,  # type: ignore[arg-type]
    )
    if on_error == 'stop':
        await loop
    else:
        with pytest.raises(asyncio.CancelledError):
            await loop

    assert recorder.statuses == expected_statuses
    assert errors._value.get() == errors_before + len(expected_statuses) // 2


async def test_run_task_once_with_a_failing_ok_row_counts_the_run_once_as_error():
    from drakkar.metrics import periodic_task_duration, periodic_task_runs

    name = 'counted_once'
    ok = periodic_task_runs.labels(name=name, status='ok')
    errors = periodic_task_runs.labels(name=name, status='error')
    ok_before, errors_before = ok._value.get(), errors._value.get()
    durations_before = _histogram_count(periodic_task_duration, name)

    async def body():
        return None

    must_stop = await run_task_once(
        name,
        body,
        recorder=RecorderThatFailsOnOk(),  # type: ignore[arg-type]
        system=False,
        on_error='continue',
        log=structlog.get_logger(),
    )

    assert must_stop is False
    assert (ok._value.get(), errors._value.get()) == (ok_before, errors_before + 1)
    assert _histogram_count(periodic_task_duration, name) == durations_before + 1


def _histogram_count(histogram, name: str) -> float:
    """How many samples the histogram holds for the task ``name``."""
    for metric in histogram.collect():
        for sample in metric.samples:
            if sample.name.endswith('_count') and sample.labels.get('name') == name:
                return sample.value
    return 0.0


@pytest.mark.parametrize(
    ('schedule', 'expected'),
    [({'interval': 10}, {'interval_seconds': 10}), ({'cron': '*/5 * * * *'}, {'cron': '*/5 * * * *'})],
)
async def test_periodic_task_started_carries_scope_and_schedule(schedule, expected):
    clock = FakeClock(now=1000.0)

    async def body():
        raise asyncio.CancelledError

    with capture_logs() as logs, pytest.raises(asyncio.CancelledError):
        await run_periodic_task('started_line', body, on_error='continue', clock=clock, **schedule)

    started = [entry for entry in logs if entry['event'] == 'periodic_task_started']
    assert started == [
        {
            'event': 'periodic_task_started',
            'log_level': 'info',
            'category': 'periodic',
            'scope': 'worker',
            'periodic_task': 'started_line',
            **expected,
        }
    ]


# --- Integration with handler class ---


async def test_handler_periodic_methods_are_callable():
    """Periodic methods on a handler instance work as bound methods."""
    results: list[str] = []

    class MyHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return []

        @periodic(interval=0.05)
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
            interval=meta.interval,
            on_error=meta.on_error,
        )
    )

    await wait_for(lambda: len(results) >= 2)
    await _cancel_periodic(t)

    assert len(results) >= 2
    assert all(r == 'tick' for r in results)


async def test_handler_periodic_accesses_self_state():
    """Periodic tasks can access handler instance state set during on_ready."""

    class StatefulHandler(BaseDrakkarHandler):
        def __init__(self):
            self.counter = 0

        async def arrange(self, messages, pending):
            return []

        @periodic(interval=0.05)
        async def increment(self):
            self.counter += 1

    handler = StatefulHandler()
    tasks = discover_periodic_tasks(handler)
    name, method, meta = tasks[0]

    t = asyncio.create_task(
        run_periodic_task(
            name=name,
            coro_fn=method,
            interval=meta.interval,
            on_error=meta.on_error,
        )
    )

    await wait_for(lambda: handler.counter >= 2)
    await _cancel_periodic(t)

    assert handler.counter >= 2


async def test_multiple_periodic_tasks_run_independently():
    """Multiple periodic tasks with different intervals run concurrently."""
    fast_count = 0
    slow_count = 0

    class MultiHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return []

        @periodic(interval=0.05)
        async def fast_task(self):
            nonlocal fast_count
            fast_count += 1

        @periodic(interval=0.15)
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
                interval=meta.interval,
                on_error=meta.on_error,
            )
        )
        running.append(t)

    await wait_for(lambda: fast_count >= 4 and slow_count >= 1)

    for t in running:
        t.cancel()
    await asyncio.gather(*running, return_exceptions=True)

    assert fast_count > slow_count
    assert fast_count >= 4
    assert slow_count >= 1


def test_discover_periodic_tasks_skips_broken_descriptor():
    """Discovery handles getattr raising an exception (e.g. broken descriptors)."""

    class BrokenDescriptor:
        def __get__(self, obj, objtype=None):
            raise RuntimeError('descriptor broken')

    class HandlerWithBroken(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return []

        broken = BrokenDescriptor()

        @periodic(interval=10)
        async def good_task(self):
            pass

    handler = HandlerWithBroken()
    tasks = discover_periodic_tasks(handler)
    names = {name for name, _, _ in tasks}
    assert 'good_task' in names
    assert 'broken' not in names


# --- Metrics and recorder instrumentation ---


async def test_periodic_run_increments_metrics():
    """Successful periodic run increments ok counter and observes duration."""
    from drakkar.metrics import periodic_task_duration, periodic_task_runs

    call_count = 0

    async def fast_task():
        nonlocal call_count
        call_count += 1

    ok_before = periodic_task_runs.labels(name='test_metric_task', status='ok')._value.get()
    dur_before = periodic_task_duration.labels(name='test_metric_task')._sum.get()

    task = asyncio.create_task(
        run_periodic_task(name='test_metric_task', coro_fn=fast_task, interval=0.05, on_error='continue')
    )
    await wait_for(lambda: call_count >= 2)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert call_count >= 2
    ok_after = periodic_task_runs.labels(name='test_metric_task', status='ok')._value.get()
    assert ok_after >= ok_before + 2
    dur_after = periodic_task_duration.labels(name='test_metric_task')._sum.get()
    assert dur_after > dur_before


async def test_periodic_run_error_increments_error_metric():
    """Failed periodic run increments error counter."""
    from drakkar.metrics import periodic_task_runs

    async def failing_task():
        raise ValueError('boom')

    err_before = periodic_task_runs.labels(name='test_err_metric', status='error')._value.get()

    task = asyncio.create_task(
        run_periodic_task(name='test_err_metric', coro_fn=failing_task, interval=0.05, on_error='continue')
    )
    await wait_for(
        lambda: periodic_task_runs.labels(name='test_err_metric', status='error')._value.get() >= err_before + 1
    )
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    err_after = periodic_task_runs.labels(name='test_err_metric', status='error')._value.get()
    assert err_after >= err_before + 1


async def test_periodic_run_records_to_recorder(tmp_path):
    """Periodic run records events to the flight recorder."""
    from drakkar.recorder import EventRecorder

    config = make_ui_config(enabled=True)
    rec = EventRecorder(config, worker_name='test', storage_dir=str(tmp_path))
    await rec.start()

    call_count = 0

    async def tracked_task():
        nonlocal call_count
        call_count += 1

    task = asyncio.create_task(
        run_periodic_task(name='test_recorded', coro_fn=tracked_task, interval=0.05, on_error='continue', recorder=rec)
    )
    await wait_for(lambda: call_count >= 2)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert call_count >= 2
    # check buffer has periodic_run events
    periodic_events = [e for e in rec._buffer if e['event'] == 'periodic_run']
    assert len(periodic_events) >= 2
    assert periodic_events[0]['task_id'] == 'test_recorded'
    assert periodic_events[0]['exit_code'] == 0
    assert periodic_events[0]['duration'] >= 0
    await rec.stop()


async def test_periodic_run_records_error_to_recorder(tmp_path):
    """Periodic run records error events with error message."""
    import json

    from drakkar.recorder import EventRecorder

    config = make_ui_config(enabled=True)
    rec = EventRecorder(config, worker_name='test', storage_dir=str(tmp_path))
    await rec.start()

    async def failing_task():
        raise RuntimeError('test failure')

    task = asyncio.create_task(
        run_periodic_task(
            name='test_err_recorded', coro_fn=failing_task, interval=0.05, on_error='continue', recorder=rec
        )
    )
    await wait_for(lambda: any(e['event'] == 'periodic_run' for e in rec._buffer))
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    periodic_events = [e for e in rec._buffer if e['event'] == 'periodic_run']
    assert len(periodic_events) >= 1
    err_event = periodic_events[0]
    assert err_event['exit_code'] == 1
    meta = json.loads(err_event['metadata'])
    assert meta['status'] == 'error'
    assert 'test failure' in meta['error']
    await rec.stop()


# --- System flag propagation (framework-internal periodic tasks) ---


async def test_run_periodic_task_system_flag_propagates_to_recorder(tmp_path):
    """run_periodic_task(system=True) stores `system: true` in event metadata."""
    import json

    from drakkar.recorder import EventRecorder

    config = make_ui_config(enabled=True)
    rec = EventRecorder(config, worker_name='test', storage_dir=str(tmp_path))
    await rec.start()

    async def task():
        pass

    t = asyncio.create_task(
        run_periodic_task(
            name='cache.flush',
            coro_fn=task,
            interval=0.05,
            on_error='continue',
            recorder=rec,
            system=True,
        )
    )
    await wait_for(lambda: any(e['event'] == 'periodic_run' for e in rec._buffer))
    t.cancel()
    try:
        await t
    except asyncio.CancelledError:
        pass

    periodic_events = [e for e in rec._buffer if e['event'] == 'periodic_run']
    assert len(periodic_events) >= 1
    ev = periodic_events[0]
    meta = json.loads(ev['metadata'])
    assert meta.get('system') is True
    await rec.stop()


async def test_run_periodic_task_system_flag_default_omitted(tmp_path):
    """run_periodic_task without system=True (default False) omits the key from metadata.

    Default behaviour must leave metadata byte-identical to pre-flag rows so existing
    event consumers keep working.
    """
    import json

    from drakkar.recorder import EventRecorder

    config = make_ui_config(enabled=True)
    rec = EventRecorder(config, worker_name='test', storage_dir=str(tmp_path))
    await rec.start()

    async def task():
        pass

    t = asyncio.create_task(
        run_periodic_task(
            name='user.task',
            coro_fn=task,
            interval=0.05,
            on_error='continue',
            recorder=rec,
        )
    )
    await wait_for(lambda: any(e['event'] == 'periodic_run' for e in rec._buffer))
    t.cancel()
    try:
        await t
    except asyncio.CancelledError:
        pass

    periodic_events = [e for e in rec._buffer if e['event'] == 'periodic_run']
    assert len(periodic_events) >= 1
    ev = periodic_events[0]
    meta = json.loads(ev['metadata'])
    assert 'system' not in meta
    await rec.stop()


def test_recorder_record_periodic_run_system_true_stores_in_metadata(tmp_path):
    """EventRecorder.record_periodic_run(system=True) embeds `system: true` in metadata JSON."""
    import json

    from drakkar.recorder import EventRecorder

    config = make_ui_config(enabled=True)
    rec = EventRecorder(config, worker_name='test', storage_dir=str(tmp_path))
    rec.record_periodic_run(name='cache.flush', duration=0.01, status='ok', system=True)

    periodic_events = [e for e in rec._buffer if e['event'] == 'periodic_run']
    assert len(periodic_events) == 1
    meta = json.loads(periodic_events[0]['metadata'])
    assert meta['system'] is True
    assert meta['status'] == 'ok'


def test_recorder_record_periodic_run_system_false_omits_key(tmp_path):
    """EventRecorder.record_periodic_run with default system=False omits the key entirely.

    Keeping the metadata row byte-identical to pre-flag rows — no schema drift.
    """
    import json

    from drakkar.recorder import EventRecorder

    config = make_ui_config(enabled=True)
    rec = EventRecorder(config, worker_name='test', storage_dir=str(tmp_path))
    rec.record_periodic_run(name='user.task', duration=0.01, status='ok')

    periodic_events = [e for e in rec._buffer if e['event'] == 'periodic_run']
    assert len(periodic_events) == 1
    meta = json.loads(periodic_events[0]['metadata'])
    assert 'system' not in meta
    assert meta['status'] == 'ok'


def test_recorder_record_periodic_run_system_true_with_error(tmp_path):
    """system=True combines cleanly with the error case."""
    import json

    from drakkar.recorder import EventRecorder

    config = make_ui_config(enabled=True)
    rec = EventRecorder(config, worker_name='test', storage_dir=str(tmp_path))
    rec.record_periodic_run(
        name='cache.sync',
        duration=0.05,
        status='error',
        error='boom',
        system=True,
    )

    periodic_events = [e for e in rec._buffer if e['event'] == 'periodic_run']
    assert len(periodic_events) == 1
    meta = json.loads(periodic_events[0]['metadata'])
    assert meta['system'] is True
    assert meta['status'] == 'error'
    assert meta['error'] == 'boom'
