"""Run one provider operation under a time limit, with bounded retries and a circuit breaker.

A feature (for example the worker registry) calls its provider
through a :class:`ResilientCaller`. An *operation* is one logical call of the
feature. Inside it the provider call is attempted up to ``1 + retries`` times;
each attempt has its own time limit, and only transient errors are retried.

The breaker counts failed *operations*, not attempts. After
``failure_threshold`` failures in a row it opens and operations are skipped
without a call until ``cooldown_seconds`` pass. Then one operation runs as a
probe: success closes the breaker, failure opens it again with a fresh
cooldown.

Every timing decision except the per-attempt limit reads the injected
``now_monotonic``; the attempt limit is ``asyncio.wait_for`` on real time.
"""

import asyncio
import enum
import math
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import structlog

from drakkar.config.resilience import BreakerConfig, ResilienceConfig
from drakkar.metrics import provider_breaker_open, provider_call_duration, provider_calls

OUTCOME_OK = 'ok'
OUTCOME_ERROR = 'error'
OUTCOME_TIMEOUT = 'timeout'
OUTCOME_SKIPPED_OPEN = 'skipped_open'
OUTCOME_NOT_STARTED = 'not_started'
OUTCOMES = (OUTCOME_OK, OUTCOME_ERROR, OUTCOME_TIMEOUT, OUTCOME_SKIPPED_OPEN, OUTCOME_NOT_STARTED)

# A failing provider logs one line per operation per this many seconds; the
# line carries how many failures it stood for.
FAILURE_LOG_INTERVAL_SECONDS = 60.0


class TransientError(Exception):
    """A provider error worth a retry that is not already an OS or timeout error."""


# The backoff doubles per retry; past this exponent the cap has long won, and a
# larger power of two could overflow the float multiplication for a huge ``retries``.
MAX_BACKOFF_EXPONENT = 30

# ``ConnectionError`` is an ``OSError`` and ``asyncio.TimeoutError`` is
# ``TimeoutError``, so these three cover the standard connection and timeout
# errors without a provider having to wrap them.
TRANSIENT_ERRORS: tuple[type[Exception], ...] = (OSError, TimeoutError, TransientError)


@dataclass(frozen=True)
class CallResult:
    """What one operation came to: ``value`` is the provider's answer when ``ok``, else ``None``."""

    ok: bool
    value: Any
    outcome: str
    attempts: int


def describe_error(exc: BaseException) -> str:
    """A log-friendly error text; a timeout's own text is empty, a task group's names only itself."""
    if isinstance(exc, BaseExceptionGroup):
        return '; '.join(describe_error(inner) for inner in exc.exceptions)
    if isinstance(exc, TimeoutError):
        return 'timed out'
    return str(exc) or type(exc).__name__


class _Admission(enum.Enum):
    """What the breaker decides for an operation that is about to start."""

    CALL = enum.auto()
    PROBE = enum.auto()
    REFUSED = enum.auto()


class _Breaker:
    """Closed until ``failure_threshold`` failed operations in a row; then open until a probe succeeds."""

    def __init__(self, config: BreakerConfig) -> None:
        self._config = config
        self._consecutive_failures = 0
        self._opened_at: float | None = None
        self._probe_in_flight = False

    @property
    def is_open(self) -> bool:
        """True from the moment it opens until a success closes it, including the wait for the probe."""
        return self._opened_at is not None

    def admit(self, now: float) -> _Admission:
        """Decide whether an operation may run; after the cooldown the first one becomes the probe."""
        if self._opened_at is None:
            return _Admission.CALL
        if self._probe_in_flight or now - self._opened_at < self._config.cooldown_seconds:
            return _Admission.REFUSED
        self._probe_in_flight = True
        return _Admission.PROBE

    def end_probe(self) -> None:
        """Free the probe slot, whatever became of the probe (a cancelled probe records no result)."""
        self._probe_in_flight = False

    def record_success(self, admission: _Admission) -> bool:
        """Close the breaker and reset the count; True when this closed an open breaker."""
        if self._opened_at is not None and admission is not _Admission.PROBE:
            # Only the probe decides an open breaker: an operation that started before it
            # opened and succeeded late says nothing about the provider now.
            return False
        was_open = self._opened_at is not None
        self._consecutive_failures = 0
        self._opened_at = None
        return was_open

    def record_failure(self, now: float, admission: _Admission) -> bool:
        """Count a failed operation; True when it opened the breaker, or reopened it after a failed probe."""
        if self._opened_at is not None:
            # Only the probe decides an open breaker: a late failure of an operation that
            # started before it opened must not push the cooldown further out.
            is_probe = admission is _Admission.PROBE
            if is_probe:
                self._opened_at = now
            return is_probe
        self._consecutive_failures += 1
        if self._consecutive_failures < self._config.failure_threshold:
            return False
        self._opened_at = now
        return True


class ResilientCaller:
    """Calls one provider for one feature; see the module docstring for the rules.

    Log lines carry ``category`` (the log category of the owning feature).
    """

    def __init__(
        self,
        config: ResilienceConfig,
        *,
        feature: str,
        provider: str,
        category: str = 'fleet',
        logger: Any = None,
        now_monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._config = config
        self._feature = feature
        self._provider = provider
        self._category = category
        self._logger = logger if logger is not None else structlog.get_logger()
        self._now_monotonic = now_monotonic
        self._sleep = sleep
        self._breaker = _Breaker(config.breaker)
        self._last_failure_log: dict[str, float] = {}
        self._suppressed_failures: dict[str, int] = {}
        self._breaker_gauge = provider_breaker_open.labels(feature=feature, provider=provider)
        self._breaker_gauge.set(0)

    @property
    def breaker_open(self) -> bool:
        """True while the breaker is open, until a probe operation succeeds."""
        return self._breaker.is_open

    async def call(
        self,
        op: str,
        make_call: Callable[[], Awaitable[Any]],
        *,
        attempts: int | None = None,
        deadline_seconds: float | None = None,
    ) -> CallResult:
        """Run one operation and return what it came to; a provider error never escapes.

        ``make_call`` takes no arguments and returns a fresh awaitable. It is
        invoked once per attempt. ``attempts`` replaces the ``1 + retries`` of
        the config. ``deadline_seconds`` bounds the whole operation: every
        attempt's time limit is cut to the time left, and no retry starts, nor
        waits for its backoff, when that would pass the deadline. A deadline
        that is already spent makes no attempt: outcome ``timeout`` with zero
        attempts, no breaker failure and no log line. It is checked before the
        breaker, so a spent deadline on an open breaker is ``timeout``, not
        ``skipped_open``.

        An operation that made no attempt (a spent deadline, an open breaker)
        is counted but has no duration to observe.

        Cancelling the task cancels the attempt, and the cancellation reaches
        the caller. A cancelled operation is neither counted nor held against
        the breaker.
        """
        max_attempts = self._config.retries + 1 if attempts is None else attempts
        if max_attempts < 1:
            raise ValueError(f'attempts must be >= 1, got {max_attempts}')
        started = self._now_monotonic()
        if deadline_seconds is not None and deadline_seconds <= 0:
            return self._count_without_attempt(op, OUTCOME_TIMEOUT)
        admission = self._breaker.admit(started)
        if admission is _Admission.REFUSED:
            return self._count_without_attempt(op, OUTCOME_SKIPPED_OPEN)
        deadline_at = None if deadline_seconds is None else started + deadline_seconds
        try:
            result, error = await self._run_attempts(make_call, max_attempts, deadline_at)
        finally:
            if admission is _Admission.PROBE:
                self._breaker.end_probe()
        self._observe_duration(op, started)
        self._count(op, result.outcome)
        if error is None:
            await self._record_success(admission)
        else:
            await self._record_failure(op, result, error, admission)
        return result

    def skip(self, op: str, outcome: str) -> None:
        """Count an operation the caller did not make (for example ``not_started``); no call, no duration."""
        if outcome not in OUTCOMES:
            raise ValueError(f'outcome must be one of {", ".join(OUTCOMES)}, got {outcome!r}')
        self._count(op, outcome)

    # --- attempts ---

    async def _run_attempts(
        self,
        make_call: Callable[[], Awaitable[Any]],
        max_attempts: int,
        deadline_at: float | None,
    ) -> tuple[CallResult, Exception | None]:
        """Attempt until success, a non-transient error, or no attempt or time is left; return the error if any."""
        attempt = 0
        while True:
            attempt += 1
            timeout = min(self._config.timeout_seconds, self._time_left(deadline_at))
            try:
                value = await asyncio.wait_for(make_call(), timeout=timeout)
            except TRANSIENT_ERRORS as exc:
                error = exc
                outcome = OUTCOME_TIMEOUT if isinstance(exc, TimeoutError) else OUTCOME_ERROR
            except Exception as exc:
                return CallResult(ok=False, value=None, outcome=OUTCOME_ERROR, attempts=attempt), exc
            else:
                return CallResult(ok=True, value=value, outcome=OUTCOME_OK, attempts=attempt), None
            failed = CallResult(ok=False, value=None, outcome=outcome, attempts=attempt)
            delay = self._backoff_delay(retry_index=attempt - 1)
            if attempt >= max_attempts or self._time_left(deadline_at) <= delay:
                return failed, error
            await self._sleep(delay)
            # A sleep can overrun. An attempt cut to no time would still start the call.
            if self._time_left(deadline_at) <= 0:
                return failed, error

    def _backoff_delay(self, *, retry_index: int) -> float:
        """The wait before retry ``retry_index`` (0-based): doubling from the base, capped."""
        doubled = self._config.backoff_seconds * 2 ** min(retry_index, MAX_BACKOFF_EXPONENT)
        return min(doubled, self._config.backoff_max_seconds)

    def _time_left(self, deadline_at: float | None) -> float:
        if deadline_at is None:
            return math.inf
        return deadline_at - self._now_monotonic()

    # --- metrics, breaker, logs ---

    def _count(self, op: str, outcome: str) -> None:
        provider_calls.labels(feature=self._feature, provider=self._provider, op=op, outcome=outcome).inc()

    def _observe_duration(self, op: str, started: float) -> None:
        """One sample per operation that made an attempt, whatever the number of attempts."""
        provider_call_duration.labels(feature=self._feature, provider=self._provider, op=op).observe(
            self._now_monotonic() - started
        )

    def _count_without_attempt(self, op: str, outcome: str) -> CallResult:
        """Count an operation that made no attempt. A zero sample would pull the duration percentiles down."""
        self._count(op, outcome)
        return CallResult(ok=False, value=None, outcome=outcome, attempts=0)

    async def _record_success(self, admission: _Admission) -> None:
        if not self._breaker.record_success(admission):
            return
        self._breaker_gauge.set(0)
        await self._logger.ainfo(
            'provider_breaker_closed',
            category=self._category,
            feature=self._feature,
            provider=self._provider,
        )

    async def _record_failure(self, op: str, result: CallResult, error: Exception, admission: _Admission) -> None:
        opened = self._breaker.record_failure(self._now_monotonic(), admission)
        if opened:
            # Before the first await: a cancelled log write must not leave an open breaker at 0.
            self._breaker_gauge.set(1)
        await self._log_failure(op, result, error)
        if not opened:
            return
        await self._logger.awarning(
            'provider_breaker_opened',
            category=self._category,
            feature=self._feature,
            provider=self._provider,
            failure_threshold=self._config.breaker.failure_threshold,
            cooldown_seconds=self._config.breaker.cooldown_seconds,
        )

    async def _log_failure(self, op: str, result: CallResult, error: Exception) -> None:
        """Log a failed operation, at most once per interval per operation, with how many it stands for."""
        now = self._now_monotonic()
        last_logged = self._last_failure_log.get(op)
        if last_logged is not None and now - last_logged < FAILURE_LOG_INTERVAL_SECONDS:
            self._suppressed_failures[op] = self._suppressed_failures.get(op, 0) + 1
            return
        self._last_failure_log[op] = now
        await self._logger.awarning(
            'provider_call_failed',
            category=self._category,
            feature=self._feature,
            provider=self._provider,
            op=op,
            error=describe_error(error),
            attempts=result.attempts,
            suppressed=self._suppressed_failures.pop(op, 0),
        )
