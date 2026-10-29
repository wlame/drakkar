"""The resilient caller: retries, timeouts, the breaker, deadlines, metrics and failure-log throttling.

The caller reads one injected clock for everything it times (breaker cooldown,
deadline, failure-log interval, duration) and awaits an injected ``sleep`` for
the backoff, so these tests use a fake clock and a recording sleep. Only the
per-attempt timeout runs on real time, with a tiny limit where a test needs it.
"""

import asyncio

import pytest
from prometheus_client import REGISTRY
from structlog.testing import capture_logs

from drakkar.config.resilience import ResilienceConfig
from drakkar.providers.resilience import CallResult, RateLimitedLog, ResilientCaller

FEATURE = 'registry'
OP = 'publish'


class FakeClock:
    """A monotonic clock a test moves by hand."""

    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class ScriptedCall:
    """A ``make_call``: each call plays the next step, the last step repeats.

    A step that is an exception is raised; any other step is returned.
    """

    def __init__(self, *steps: object) -> None:
        self._steps = steps
        self.calls = 0

    async def __call__(self) -> object:
        step = self._steps[min(self.calls, len(self._steps) - 1)]
        self.calls += 1
        if isinstance(step, BaseException):
            raise step
        return step


class BlockingLogger:
    """A logger whose failure line for one op waits until the test lets it go."""

    def __init__(self, blocked_op: str) -> None:
        self._blocked_op = blocked_op
        self.reached = asyncio.Event()
        self.release = asyncio.Event()

    async def awarning(self, event: str, **fields: object) -> None:
        if event == 'provider_call_failed' and fields['op'] == self._blocked_op:
            self.reached.set()
            await self.release.wait()

    async def ainfo(self, event: str, **fields: object) -> None:
        return None


def _config(**overrides) -> ResilienceConfig:
    values = {
        'timeout': '1s',
        'max_retries': 2,
        'backoff': '200ms',
        'max_backoff': '300ms',
        'breaker_failures': 2,
        'breaker_cooldown': '10s',
    }
    return ResilienceConfig(**{**values, **overrides})


def _sample(name: str, provider: str, **labels: str) -> float:
    value = REGISTRY.get_sample_value(name, {'feature': FEATURE, 'provider': provider, **labels})
    return 0.0 if value is None else value


def _calls(provider: str, op: str, outcome: str) -> float:
    return _sample('drakkar_provider_calls_total', provider, op=op, outcome=outcome)


def _breaker_gauge(provider: str) -> float:
    return _sample('drakkar_provider_breaker_open', provider)


@pytest.fixture
def provider(request) -> str:
    """A provider label no other test uses, because prometheus metrics are process-global."""
    return request.node.name


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def slept() -> list[float]:
    return []


@pytest.fixture
def make_caller(clock, slept, provider):
    def build(**config_overrides) -> ResilientCaller:
        async def record_sleep(delay: float) -> None:
            slept.append(delay)
            clock.advance(delay)

        return ResilientCaller(
            _config(**config_overrides),
            feature=FEATURE,
            provider=provider,
            now_monotonic=clock,
            sleep=record_sleep,
        )

    return build


@pytest.fixture
def wait_for_timeouts(monkeypatch) -> list[float]:
    """Record the timeout of every ``asyncio.wait_for`` call, then run it for real."""
    seen: list[float] = []
    real_wait_for = asyncio.wait_for

    async def recording_wait_for(awaitable, timeout):
        seen.append(timeout)
        return await real_wait_for(awaitable, timeout)

    monkeypatch.setattr(asyncio, 'wait_for', recording_wait_for)
    return seen


async def _open_breaker(caller: ResilientCaller) -> None:
    """Fail two operations, which is the threshold of the test config."""
    failing = ScriptedCall(ValueError('bad'))
    await caller.call(OP, failing)
    await caller.call(OP, failing)
    assert caller.breaker_open


# --- retries ---


async def test_a_transient_error_is_retried_with_doubling_capped_backoff(make_caller, slept):
    call = ScriptedCall(ConnectionError('refused'), ConnectionError('refused'), 'v')

    result = await make_caller().call(OP, call)

    assert result == CallResult(ok=True, value='v', outcome='ok', attempts=3)
    assert slept == [0.2, 0.3]


@pytest.mark.parametrize(
    ('overrides', 'expected'),
    [
        ({}, 3 * 1.0 + 0.2 + 0.3),
        ({'max_retries': 0}, 1.0),
        ({'max_retries': 3, 'backoff': 0, 'max_backoff': 0}, 4 * 1.0),
    ],
    ids=['capped-backoff', 'no-retries', 'no-backoff'],
)
def test_the_operation_bound_is_every_attempt_limit_plus_every_backoff(make_caller, overrides, expected):
    assert make_caller(**overrides).operation_bound_seconds == pytest.approx(expected)


async def test_a_non_transient_error_is_not_retried_and_counts_one_failure(make_caller, slept):
    caller = make_caller()
    call = ScriptedCall(ValueError('bad'))

    result = await caller.call(OP, call)

    assert result == CallResult(ok=False, value=None, outcome='error', attempts=1)
    assert call.calls == 1
    assert slept == []
    assert not caller.breaker_open

    await caller.call(OP, call)

    assert caller.breaker_open


async def test_a_timeout_on_the_last_attempt_is_outcome_timeout(make_caller, slept):
    async def never_returns():
        await asyncio.Event().wait()

    result = await make_caller(timeout='10ms').call(OP, never_returns)

    assert result.outcome == 'timeout'
    assert result.attempts == 3
    assert slept == [0.2, 0.3]


async def test_the_outcome_follows_the_last_attempt(make_caller):
    call = ScriptedCall(TimeoutError(), TimeoutError(), ConnectionError('refused'))

    result = await make_caller().call(OP, call)

    assert (result.ok, result.outcome, result.attempts) == (False, 'error', 3)


async def test_attempts_one_makes_exactly_one_call(make_caller, slept):
    call = ScriptedCall(ConnectionError('refused'))

    result = await make_caller().call(OP, call, attempts=1)

    assert call.calls == 1
    assert result.attempts == 1
    assert slept == []


@pytest.mark.parametrize(
    ('error', 'expected_attempts'),
    [(ConnectionError('refused'), 3), (ValueError('bad'), 1)],
    ids=['transient', 'non-transient'],
)
async def test_a_make_call_that_raises_before_returning_an_awaitable_fails_like_an_attempt(
    make_caller, error, expected_attempts
):
    calls = []

    def make_call():
        calls.append(1)
        raise error

    result = await make_caller().call(OP, make_call)

    assert (result.ok, result.outcome, result.attempts) == (False, 'error', expected_attempts)
    assert len(calls) == expected_attempts


async def test_a_huge_retry_count_does_not_overflow_the_backoff(make_caller, slept):
    call = ScriptedCall(ConnectionError('refused'))

    result = await make_caller(max_retries=1100, breaker_failures=100).call(OP, call)

    assert result.attempts == 1101
    assert max(slept) == 0.3


@pytest.mark.parametrize('phase', ['attempt', 'backoff'])
async def test_a_cancelled_operation_is_not_counted_and_not_held_against_the_breaker(clock, provider, phase):
    in_phase = asyncio.Event()

    async def never_returns():
        in_phase.set()
        await asyncio.Event().wait()

    async def sleep_forever(delay: float) -> None:
        in_phase.set()
        await asyncio.Event().wait()

    make_call = never_returns if phase == 'attempt' else ScriptedCall(ConnectionError('refused'))
    caller = ResilientCaller(_config(), feature=FEATURE, provider=provider, now_monotonic=clock, sleep=sleep_forever)
    operation = asyncio.create_task(caller.call(OP, make_call))
    await in_phase.wait()

    operation.cancel()
    with pytest.raises(asyncio.CancelledError):
        await operation
    await caller.call(OP, ScriptedCall(ValueError('bad')))

    # Threshold 2: had the cancelled operation counted as a failure, this one would open the breaker.
    assert not caller.breaker_open
    assert [_calls(provider, OP, outcome) for outcome in ('ok', 'error', 'timeout')] == [0.0, 1.0, 0.0]
    assert _sample('drakkar_provider_call_duration_seconds_count', provider, op=OP) == 1.0


# --- breaker ---


async def test_an_open_breaker_skips_without_calling_until_cooldown_then_probes(make_caller, clock, provider):
    caller = make_caller()
    await _open_breaker(caller)
    assert _breaker_gauge(provider) == 1.0
    healthy = ScriptedCall('v')

    skipped = await caller.call(OP, healthy)

    assert skipped == CallResult(ok=False, value=None, outcome='skipped_open', attempts=0)
    assert healthy.calls == 0
    assert _calls(provider, OP, 'skipped_open') == 1.0

    clock.advance(9.0)
    assert (await caller.call(OP, healthy)).outcome == 'skipped_open'
    assert healthy.calls == 0

    clock.advance(1.0)
    probe = await caller.call(OP, healthy)

    assert probe.ok
    assert healthy.calls == 1
    assert not caller.breaker_open
    assert _breaker_gauge(provider) == 0.0


async def test_a_failed_probe_reopens_the_breaker_with_a_fresh_cooldown(make_caller, clock):
    caller = make_caller()
    await _open_breaker(caller)
    clock.advance(10.0)
    failing = ScriptedCall(ValueError('bad'))
    healthy = ScriptedCall('v')

    probe = await caller.call(OP, failing)

    assert (probe.outcome, probe.attempts) == ('error', 1)
    assert caller.breaker_open

    clock.advance(9.0)
    assert (await caller.call(OP, healthy)).outcome == 'skipped_open'

    clock.advance(1.0)
    assert (await caller.call(OP, healthy)).ok
    assert not caller.breaker_open


async def test_only_one_probe_runs_while_the_first_is_in_flight(make_caller, clock):
    caller = make_caller()
    await _open_breaker(caller)
    clock.advance(10.0)
    release = asyncio.Event()

    async def slow_probe():
        await release.wait()
        return 'v'

    probe = asyncio.create_task(caller.call(OP, slow_probe))
    await asyncio.sleep(0)

    other = await caller.call(OP, ScriptedCall('v'))
    release.set()

    assert other.outcome == 'skipped_open'
    assert (await probe).ok


async def test_cancelling_a_probe_frees_the_probe_slot(make_caller, clock):
    caller = make_caller()
    await _open_breaker(caller)
    clock.advance(10.0)

    async def never_returns():
        await asyncio.Event().wait()

    probe = asyncio.create_task(caller.call(OP, never_returns))
    await asyncio.sleep(0)
    probe.cancel()
    with pytest.raises(asyncio.CancelledError):
        await probe

    assert (await caller.call(OP, ScriptedCall('v'))).ok


async def test_a_late_failure_of_an_earlier_operation_does_not_extend_the_cooldown(make_caller, clock):
    caller = make_caller()
    release = asyncio.Event()

    async def fails_late():
        await release.wait()
        raise ValueError('bad')

    late = asyncio.create_task(caller.call(OP, fails_late))
    await asyncio.sleep(0)
    await _open_breaker(caller)
    clock.advance(5.0)
    release.set()
    await late
    clock.advance(5.0)

    assert (await caller.call(OP, ScriptedCall('v'))).ok


async def test_a_late_success_of_an_earlier_operation_does_not_close_an_open_breaker(make_caller, provider):
    caller = make_caller()
    release = asyncio.Event()

    async def succeeds_late():
        await release.wait()
        return 'v'

    late = asyncio.create_task(caller.call(OP, succeeds_late))
    await asyncio.sleep(0)
    await _open_breaker(caller)

    with capture_logs() as logs:
        release.set()
        assert (await late).ok

    assert caller.breaker_open
    assert _breaker_gauge(provider) == 1.0
    assert [entry for entry in logs if entry['event'] == 'provider_breaker_closed'] == []


async def test_the_breaker_gauge_is_set_before_the_failure_is_logged(clock, provider):
    logger = BlockingLogger(blocked_op='list')
    caller = ResilientCaller(_config(), feature=FEATURE, provider=provider, logger=logger, now_monotonic=clock)
    await caller.call(OP, ScriptedCall(ValueError('bad')))

    opening = asyncio.create_task(caller.call('list', ScriptedCall(ValueError('bad'))))
    await logger.reached.wait()

    assert caller.breaker_open
    assert _breaker_gauge(provider) == 1.0
    opening.cancel()
    with pytest.raises(asyncio.CancelledError):
        await opening


async def test_the_breaker_counts_operations_not_attempts(make_caller):
    caller = make_caller()
    exhausted = ScriptedCall(ConnectionError('refused'))

    await caller.call(OP, exhausted)

    # Three failed attempts are one failed operation, and the threshold is 2.
    assert not caller.breaker_open
    await caller.call(OP, exhausted)
    assert caller.breaker_open


async def test_a_retry_that_succeeds_resets_the_failure_count(make_caller):
    caller = make_caller()

    await caller.call(OP, ScriptedCall(ValueError('bad')))
    recovered = await caller.call(OP, ScriptedCall(ConnectionError('refused'), 'v'))
    await caller.call(OP, ScriptedCall(ValueError('bad')))

    assert (recovered.ok, recovered.attempts) == (True, 2)
    assert not caller.breaker_open


async def test_a_success_between_failures_resets_the_failure_count(make_caller):
    caller = make_caller()

    await caller.call(OP, ScriptedCall(ValueError('bad')))
    await caller.call(OP, ScriptedCall('v'))
    await caller.call(OP, ScriptedCall(ValueError('bad')))

    assert not caller.breaker_open


async def test_breaker_transitions_are_logged_without_throttling_and_a_failed_probe_is_no_transition(
    make_caller, clock, provider
):
    caller = make_caller()

    with capture_logs() as logs:
        await _open_breaker(caller)
        clock.advance(10.0)
        await caller.call(OP, ScriptedCall(ValueError('bad')))
        gauge_after_failed_probe = _breaker_gauge(provider)
        clock.advance(10.0)
        await caller.call(OP, ScriptedCall('v'))

    events = [entry['event'] for entry in logs if entry['event'].startswith('provider_breaker')]
    assert events == ['provider_breaker_opened', 'provider_breaker_closed']
    assert gauge_after_failed_probe == 1.0


async def test_the_rate_limited_log_holds_back_lines_per_key_and_counts_them(clock):
    lines: list[tuple[str, dict]] = []

    async def log(event: str, **fields: object) -> None:
        lines.append((event, fields))

    rate_limited = RateLimitedLog(clock)

    await rate_limited.emit('publish', log, 'event_a', field=1)
    await rate_limited.emit('publish', log, 'event_a', field=2)
    await rate_limited.emit('list', log, 'event_a', field=3)
    clock.advance(59.0)
    await rate_limited.emit('publish', log, 'event_a', field=4)
    clock.advance(1.0)
    await rate_limited.emit('publish', log, 'event_a', field=5)
    await rate_limited.emit('publish', log, 'event_a', field=6)
    clock.advance(60.0)
    await rate_limited.emit('publish', log, 'event_a', field=7)

    assert lines == [
        ('event_a', {'field': 1, 'suppressed': 0}),
        ('event_a', {'field': 3, 'suppressed': 0}),
        ('event_a', {'field': 5, 'suppressed': 2}),
        ('event_a', {'field': 7, 'suppressed': 1}),
    ]


# --- deadlines ---


async def test_a_deadline_caps_the_attempt_timeout_and_stops_retries(make_caller, clock, slept, wait_for_timeouts):
    async def spend_the_deadline_then_fail():
        clock.advance(0.5)
        raise ConnectionError('refused')

    result = await make_caller().call(OP, spend_the_deadline_then_fail, deadline_seconds=0.5)

    assert wait_for_timeouts == [0.5]
    assert (result.outcome, result.attempts) == ('error', 1)
    assert slept == []


async def test_a_deadline_shortens_a_later_attempt_and_skips_a_backoff_that_would_overrun_it(
    make_caller, clock, slept, wait_for_timeouts
):
    async def spend_half_a_second_then_fail():
        clock.advance(0.5)
        raise ConnectionError('refused')

    result = await make_caller().call(OP, spend_half_a_second_then_fail, deadline_seconds=1.25)

    # Attempt 1 keeps the full 1.0 s. After it and the 0.2 s backoff 0.55 s are left,
    # so attempt 2 is cut to that. The 0.3 s backoff after it no longer fits.
    assert wait_for_timeouts == pytest.approx([1.0, 0.55])
    assert slept == [0.2]
    assert result.attempts == 2


async def test_a_spent_deadline_makes_no_attempt_and_is_not_a_failure(make_caller, provider):
    caller = make_caller()
    call = ScriptedCall('v')

    with capture_logs() as logs:
        for _ in range(3):
            result = await caller.call(OP, call, deadline_seconds=0.0)

    assert result == CallResult(ok=False, value=None, outcome='timeout', attempts=0)
    assert call.calls == 0
    assert not caller.breaker_open
    assert logs == []
    assert _calls(provider, OP, 'timeout') == 3.0
    assert _sample('drakkar_provider_call_duration_seconds_count', provider, op=OP) == 0.0


async def test_a_backoff_that_overruns_the_deadline_ends_the_operation_without_another_call(clock, provider):
    async def overrunning_sleep(delay: float) -> None:
        clock.advance(delay + 5.0)

    caller = ResilientCaller(
        _config(), feature=FEATURE, provider=provider, now_monotonic=clock, sleep=overrunning_sleep
    )
    call = ScriptedCall(ConnectionError('refused'))

    result = await caller.call(OP, call, deadline_seconds=1.0)

    assert call.calls == 1
    assert (result.outcome, result.attempts) == ('error', 1)


async def test_a_spent_deadline_is_checked_before_the_breaker(make_caller):
    caller = make_caller()
    await _open_breaker(caller)

    result = await caller.call(OP, ScriptedCall('v'), deadline_seconds=0.0)

    assert result == CallResult(ok=False, value=None, outcome='timeout', attempts=0)


# --- metrics ---


async def test_an_operation_counts_one_call_and_one_duration_however_many_attempts(make_caller, provider):
    call = ScriptedCall(ConnectionError('refused'), ConnectionError('refused'), 'v')

    await make_caller().call(OP, call)

    assert _calls(provider, OP, 'ok') == 1.0
    assert _sample('drakkar_provider_call_duration_seconds_count', provider, op=OP) == 1.0
    # The fake clock only moves during the two backoffs (0.2 s + 0.3 s): the duration spans the retries.
    assert _sample('drakkar_provider_call_duration_seconds_sum', provider, op=OP) == pytest.approx(0.5)


async def test_a_skipped_operation_is_counted_but_adds_no_duration_sample(make_caller, provider):
    caller = make_caller()
    await _open_breaker(caller)
    samples_before = _sample('drakkar_provider_call_duration_seconds_count', provider, op=OP)

    skipped = await caller.call(OP, ScriptedCall('v'))

    assert skipped.outcome == 'skipped_open'
    assert samples_before == 2.0
    assert _sample('drakkar_provider_call_duration_seconds_count', provider, op=OP) == samples_before
    assert _calls(provider, OP, 'skipped_open') == 1.0


def test_skip_counts_the_outcome_without_a_call(make_caller, provider):
    make_caller().skip('publish', 'not_started')

    assert _calls(provider, 'publish', 'not_started') == 1.0
    assert _sample('drakkar_provider_call_duration_seconds_count', provider, op='publish') == 0.0


async def test_unknown_outcomes_and_zero_attempts_are_rejected(make_caller):
    caller = make_caller()

    with pytest.raises(ValueError, match='outcome'):
        caller.skip(OP, 'exploded')
    with pytest.raises(ValueError, match='attempts'):
        await caller.call(OP, ScriptedCall('v'), attempts=0)


# --- failure log ---


@pytest.mark.parametrize(
    ('failure', 'expected_text'),
    [
        (TimeoutError(), 'timed out'),
        (ValueError(), 'ValueError'),
        (ExceptionGroup('group', [ValueError('first'), TimeoutError()]), 'first; timed out'),
    ],
    ids=['timeout', 'empty-message', 'exception-group'],
)
async def test_the_failure_log_describes_errors_that_have_no_text_of_their_own(make_caller, failure, expected_text):
    with capture_logs() as logs:
        await make_caller(max_retries=0).call(OP, ScriptedCall(failure))

    assert [entry['error'] for entry in logs if entry['event'] == 'provider_call_failed'] == [expected_text]


@pytest.mark.parametrize(
    'failure',
    [
        ValueError('could not connect to postgresql://u:p@h/db?password=x'),
        ExceptionGroup('group', [ValueError('could not connect to postgresql://u:p@h/db?password=x')]),
    ],
    ids=['plain', 'exception-group'],
)
async def test_the_failure_log_removes_credentials_from_the_error_text(make_caller, failure):
    with capture_logs() as logs:
        await make_caller(max_retries=0).call(OP, ScriptedCall(failure))

    (error,) = [entry['error'] for entry in logs if entry['event'] == 'provider_call_failed']
    assert error == 'could not connect to postgresql://***:***@h/db?password=***'
    assert 'u:p@' not in error
    assert 'password=x' not in error


async def test_failures_log_once_a_minute_per_operation_with_the_suppressed_count(make_caller, clock, provider):
    caller = make_caller(breaker_failures=100)
    failing = ScriptedCall(ValueError('bad'))

    with capture_logs() as first_window:
        for _ in range(3):
            await caller.call(OP, failing)
        await caller.call('list', failing)
    clock.advance(60.0)
    with capture_logs() as second_window:
        await caller.call(OP, failing)

    failures = [entry for entry in first_window if entry['event'] == 'provider_call_failed']
    assert [(entry['op'], entry['suppressed']) for entry in failures] == [(OP, 0), ('list', 0)]
    assert failures[0]['feature'] == FEATURE
    assert failures[0]['category'] == 'fleet'
    assert failures[0]['provider'] == provider
    assert failures[0]['error'] == 'bad'
    assert failures[0]['attempts'] == 1
    assert [(entry['event'], entry['suppressed']) for entry in second_window] == [('provider_call_failed', 2)]


async def test_log_lines_carry_the_category_of_the_feature(clock, provider):
    config = _config(breaker_failures=1, breaker_cooldown='10s')
    caller = ResilientCaller(config, feature=FEATURE, provider=provider, category='recorder', now_monotonic=clock)

    with capture_logs() as logs:
        await caller.call(OP, ScriptedCall(ValueError('bad')))
        clock.advance(10.0)
        await caller.call(OP, ScriptedCall('v'))

    assert [(entry['event'], entry['category']) for entry in logs] == [
        ('provider_call_failed', 'recorder'),
        ('provider_breaker_opened', 'recorder'),
        ('provider_breaker_closed', 'recorder'),
    ]
