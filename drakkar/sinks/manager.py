"""Sink manager — orchestrates routing, delivery, and error handling.

The SinkManager holds all configured sink instances, validates that
CollectResult payloads target existing sinks, and delivers payloads
with error handling via the on_delivery_error handler hook.
"""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import structlog
from pydantic import BaseModel

from drakkar.config import CircuitBreakerConfig
from drakkar.metrics import sink_deliveries_skipped, sink_delivery_retries
from drakkar.models import CollectResult, DeliveryAction, DeliveryError
from drakkar.sinks.base import BaseSink
from drakkar.utils import redact_url

if TYPE_CHECKING:
    from drakkar.recorder import EventRecorder
    from drakkar.sinks.dlq import DLQSink

logger = structlog.get_logger()

DeliveryErrorCallback = Callable[[DeliveryError], Awaitable[DeliveryAction]]

# Sentinel ``DeliveryError.error`` and ``SinkStats.last_error`` value used when
# a delivery is short-circuited because the sink's circuit breaker is open.
# Kept as a module-level constant so tests, the debug UI, and the delivery
# code all agree on the exact wording.
CIRCUIT_OPEN_ERROR = 'circuit open'

# Mapping from CollectResult field name to sink_type string
_FIELD_TO_SINK_TYPE: dict[str, str] = {
    'kafka': 'kafka',
    'postgres': 'postgres',
    'mongo': 'mongo',
    'http': 'http',
    'redis': 'redis',
    'files': 'filesystem',
}


@dataclass
class SinkStats:
    """Per-sink delivery statistics tracked in memory."""

    delivered_count: int = 0
    delivered_payloads: int = 0
    error_count: int = 0
    retry_count: int = 0
    last_delivery_ts: float | None = None
    last_delivery_duration: float | None = None
    last_error: str | None = None
    last_error_ts: float | None = None


class SinkNotConfiguredError(Exception):
    """Raised when a CollectResult references a sink type or name that isn't configured."""


class AmbiguousSinkError(Exception):
    """Raised when a payload has empty sink name but multiple sinks of that type exist."""


class SinkManager:
    """Manages all sink instances and routes payloads from CollectResult.

    Responsibilities:
        - Register and hold sink instances keyed by (type, name)
        - Connect/close all sinks during worker lifecycle
        - Validate that CollectResult only targets configured sinks
        - Route payloads to the correct sink instance
        - Handle delivery errors via the on_delivery_error callback
        - Track per-sink delivery stats for the debug UI
    """

    def __init__(
        self,
        circuit_breaker_config: CircuitBreakerConfig | None = None,
        recorder: EventRecorder | None = None,
        dlq_sink: DLQSink | None = None,
    ) -> None:
        self._sinks: dict[tuple[str, str], BaseSink[Any]] = {}
        self._by_type: dict[str, list[BaseSink[Any]]] = defaultdict(list)
        self._stats: dict[tuple[str, str], SinkStats] = {}
        # The circuit breaker config is pushed onto each sink via register().
        # None => default CircuitBreakerConfig (5 failures / 30s cooldown).
        # Callers that build a SinkManager without a config (tests, standalone
        # usage) get the default behavior automatically.
        self._circuit_breaker_config: CircuitBreakerConfig = circuit_breaker_config or CircuitBreakerConfig()
        # Recorder + DLQ sink are owned by the app but accessed on every
        # delivery. Holding references here keeps the ``deliver_all`` hot
        # path signature minimal and removes the per-call plumbing from
        # ``drakkar/app.py``. Both are ``None``-tolerant: tests (and
        # pre-debug-enabled paths) can construct a SinkManager without
        # them and the delivery code handles the absence via
        # ``if self._recorder is not None`` / ``if self._dlq_sink is not None``
        # guards inside ``_deliver_to_sink``.
        self._recorder: EventRecorder | None = recorder
        self._dlq_sink: DLQSink | None = dlq_sink

    def attach_runtime(
        self,
        recorder: EventRecorder | None,
        dlq_sink: DLQSink | None,
    ) -> None:
        """Inject recorder + DLQ sink after construction.

        The ``DrakkarApp`` constructs the ``SinkManager`` in its own
        ``__init__`` (before the recorder and DLQ sink exist) but the
        SinkManager needs references to both during delivery. This setter
        lets ``_async_run`` wire them up once they've been built, without
        rebuilding the manager. Both arguments are assigned directly — pass
        ``None`` to clear a reference (e.g., when debug is disabled and no
        recorder was ever constructed).
        """
        self._recorder = recorder
        self._dlq_sink = dlq_sink

    @property
    def sinks(self) -> dict[tuple[str, str], BaseSink[Any]]:
        """All registered sinks keyed by (sink_type, name)."""
        return dict(self._sinks)

    @property
    def sink_count(self) -> int:
        """Total number of registered sinks."""
        return len(self._sinks)

    def register(self, sink: BaseSink[Any]) -> None:
        """Register a sink instance.

        Raises ValueError if a sink with the same (type, name) already exists.
        Also installs the manager's circuit breaker config on the sink so the
        breaker uses operator-configured thresholds instead of the default.
        """
        key = (sink.sink_type, sink.name)
        if key in self._sinks:
            raise ValueError(f'Duplicate sink: type={sink.sink_type!r}, name={sink.name!r}')
        self._sinks[key] = sink
        self._by_type[sink.sink_type].append(sink)
        self._stats[key] = SinkStats()
        # Route circuit-breaker config through the public setter so the sink
        # keeps a single documented API for thresholds — the manager never
        # reaches into BaseSink's private attributes.
        sink.configure_circuit_breaker(self._circuit_breaker_config)

    def get_sink_info(self) -> list[dict]:
        """Return list of all configured sinks with their type, name, and optional UI URL."""
        return [
            {'sink_type': sink_type, 'name': name, 'ui_url': sink.ui_url}
            for (sink_type, name), sink in self._sinks.items()
        ]

    def get_all_stats(self) -> dict[tuple[str, str], SinkStats]:
        """Return stats for all sinks, keyed by (sink_type, name)."""
        return dict(self._stats)

    async def connect_all(self) -> None:
        """Connect all registered sinks in parallel, cleaning up on partial failure.

        Uses asyncio.gather to overlap connect latencies — wall-clock time
        becomes ~max(connect_latency) instead of sum. Cold-start saving
        when multiple sinks are configured (each sink's connect can do
        network I/O, schema probes, etc.).

        Failure semantics: uses ``return_exceptions=True`` so every connect
        runs to completion (success or failure). If ANY sink's connect
        raised, we close the ones that succeeded — otherwise they would
        leak open connections for the rest of the process lifetime — then
        re-raise the FIRST connect exception we saw (preserving iteration
        order). Cleanup failures during the close pass are logged at
        warning level but never mask the original connect exception: the
        operator needs to see why startup failed, not why cleanup did.

        Empty sink list: asyncio.gather() with no args returns an empty tuple.
        """

        async def _connect_one(sink: BaseSink[Any]) -> None:
            await sink.connect()
            await logger.ainfo(
                'sink_connected',
                category='sink',
                sink_type=sink.sink_type,
                sink_name=sink.name,
            )

        sinks = list(self._sinks.values())
        # ``return_exceptions=True`` lets every connect finish (success or
        # raise) instead of short-circuiting on the first raise. This is the
        # prerequisite for cleaning up partially-connected sinks — we need
        # to know which ones made it through so we can close them.
        results = await asyncio.gather(
            *[_connect_one(sink) for sink in sinks],
            return_exceptions=True,
        )

        first_exception: BaseException | None = None
        successful_sinks: list[BaseSink[Any]] = []
        # ``strict=True`` guards against the impossible case where gather
        # returned a different-length list than the inputs — cheap defensive
        # check that catches framework regressions loudly.
        for sink, result in zip(sinks, results, strict=True):
            if isinstance(result, BaseException):
                # Capture the first exception (by iteration order) as the
                # one we re-raise — later exceptions are still reflected in
                # per-sink state but the operator sees a single canonical
                # cause.
                if first_exception is None:
                    first_exception = result
            else:
                successful_sinks.append(sink)

        if first_exception is None:
            return

        # Partial failure: close the sinks that connected successfully so
        # their connections don't leak. Use ``return_exceptions=True`` on
        # the cleanup gather so a broken close() on one sink can't abort
        # the cleanup of the others — every successful sink gets a shot at
        # releasing its resources.
        cleanup_results = await asyncio.gather(
            *[sink.close() for sink in successful_sinks],
            return_exceptions=True,
        )
        for sink, cleanup_result in zip(successful_sinks, cleanup_results, strict=True):
            if isinstance(cleanup_result, BaseException):
                # Log cleanup failure but never mask the original connect
                # error — operators need the startup-failure signal, not
                # the cleanup-failure signal, to diagnose the outage.
                await logger.awarning(
                    'sink_cleanup_after_connect_failure_error',
                    category='sink',
                    sink_type=sink.sink_type,
                    sink_name=sink.name,
                    error=str(cleanup_result),
                )

        raise first_exception

    async def close_all(self) -> None:
        """Close all registered sinks. Logs errors but doesn't raise."""
        for sink in self._sinks.values():
            try:
                await sink.close()
            except Exception as e:
                await logger.awarning(
                    'sink_close_error',
                    category='sink',
                    sink_type=sink.sink_type,
                    sink_name=sink.name,
                    error=str(e),
                )

    def resolve_sink(self, sink_type: str, sink_name: str) -> BaseSink[Any]:
        """Resolve a sink instance by type and name.

        If sink_name is empty and exactly one sink of that type exists,
        returns that sink (convenient default). Otherwise:
        - Empty name + multiple sinks → AmbiguousSinkError
        - Explicit name not found → SinkNotConfiguredError
        """
        if not sink_name:
            candidates = self._by_type.get(sink_type, [])
            if len(candidates) == 1:
                return candidates[0]
            if len(candidates) == 0:
                raise SinkNotConfiguredError(
                    f'No {sink_type!r} sink configured, but the handler returned {sink_type} payloads'
                )
            names = [s.name for s in candidates]
            raise AmbiguousSinkError(
                f'{len(candidates)} {sink_type!r} sinks configured ({names}), '
                f'but payload has empty sink name — specify which one'
            )
        key = (sink_type, sink_name)
        if key not in self._sinks:
            raise SinkNotConfiguredError(
                f'Sink {sink_type!r}/{sink_name!r} not configured, but the handler returned a payload targeting it'
            )
        return self._sinks[key]

    def validate_collect(self, result: CollectResult) -> None:
        """Validate that every payload in the result targets a configured sink.

        Iterates all populated fields, resolves each payload's sink,
        and raises SinkNotConfiguredError or AmbiguousSinkError on first problem.
        Called before delivery so the worker crashes fast on misconfiguration.
        """
        for field_name, sink_type in _FIELD_TO_SINK_TYPE.items():
            payloads = getattr(result, field_name)
            if not payloads:
                continue
            for payload in payloads:
                self.resolve_sink(sink_type, payload.sink)

    async def deliver_all(
        self,
        result: CollectResult,
        on_delivery_error: DeliveryErrorCallback,
        partition_id: int,
        max_retries: int = 3,
    ) -> None:
        """Route and deliver all payloads in a CollectResult to their sinks.

        Groups payloads by (sink_type, resolved_sink_name), then delivers
        each group concurrently via asyncio.gather. Total wall-clock time
        becomes ~max(sink_latency) instead of the sum. On delivery failure,
        calls on_delivery_error and handles the returned action
        (DLQ, RETRY, SKIP) PER SINK — each sink retries independently.

        Recorder + DLQ sink are read from instance state
        (``self._recorder`` / ``self._dlq_sink``) — both are set via
        ``__init__`` kwargs or the later ``attach_runtime`` setter. Holding
        them as instance state keeps the hot path signature minimal and
        avoids threading the same two objects through every delivery call.

        Args:
            result: The CollectResult from on_task_complete(),
                on_message_complete(), or on_window_complete().
            on_delivery_error: Handler callback for delivery failures.
            partition_id: Source partition (for DLQ metadata).
            max_retries: Max delivery retry attempts before falling through to DLQ.
        """
        groups: dict[tuple[str, str], list[BaseModel]] = defaultdict(list)

        for field_name, sink_type in _FIELD_TO_SINK_TYPE.items():
            payloads = getattr(result, field_name)
            if not payloads:
                continue
            for payload in payloads:
                sink = self.resolve_sink(sink_type, payload.sink)
                groups[(sink.sink_type, sink.name)].append(payload)

        if not groups:
            return

        # Dispatch per-sink delivery coroutines concurrently. Each coroutine
        # owns its own retry/DLQ/stats logic, so concurrent execution is safe
        # — stats are scoped per (sink_type, name), no cross-sink contention.
        # Any exception that leaks out of ``_deliver_to_sink`` is a bug and
        # should propagate loudly; we prefer a crash over a silently-lost
        # batch, so we DO NOT pass ``return_exceptions=True`` here.
        coros = [
            self._deliver_to_sink(
                sink_type=sink_type,
                sink_name=sink_name,
                payloads=groups[(sink_type, sink_name)],
                on_delivery_error=on_delivery_error,
                max_retries=max_retries,
                partition_id=partition_id,
            )
            for (sink_type, sink_name) in groups
        ]
        await asyncio.gather(*coros)

    async def _deliver_to_sink(
        self,
        sink_type: str,
        sink_name: str,
        payloads: list[BaseModel],
        on_delivery_error: DeliveryErrorCallback,
        max_retries: int,
        partition_id: int,
    ) -> None:
        """Deliver a single sink's payload group with retry + DLQ + circuit-breaker semantics.

        Extracted so sink groups can run under asyncio.gather; the retry loop,
        per-sink stats updates, and DLQ routing all run inside one coroutine
        so concurrent gather does not interleave a single sink's retries.

        Circuit breaker:
            Before the first delivery attempt we check the sink's breaker via
            ``should_skip_delivery``. When the breaker is open (and cooldown
            hasn't elapsed), we skip the sink entirely and route the payloads
            directly to the DLQ — no retry loop, no connection burn. The
            breaker's own state machine handles the cooldown-to-half-open
            transition on subsequent invocations.

            On terminal outcomes (success or retries-exhausted) we call the
            matching ``record_*`` method so the breaker can accumulate
            consecutive failures and trip when the threshold is hit.

        Circuit-open DLQ routing is intentionally separate from the normal
        delivery-error path: when a ``dlq_sink`` is provided we call
        ``dlq_sink.send`` directly and DO NOT invoke ``on_delivery_error``.
        The rationale is that a tripped breaker is an infrastructure signal
        outside the handler's SKIP/RETRY/DLQ contract — SKIP would drop
        data while the downstream is recovering, RETRY would hammer it, and
        DLQ via the handler could double-route if the handler itself ships
        DLQ-action results back to the same DLQ sink. We still invoke the
        handler for observability when no DLQ is wired (pre-breaker
        deployments). The trade-off is a dual-path DLQ plumbing that the
        app.py ``_on_delivery_error`` wrapper does NOT observe for circuit
        skips — deliberate, documented here so the inconsistency is
        explicit rather than accidental.
        """
        sink = self._sinks[(sink_type, sink_name)]
        stats = self._stats[(sink_type, sink_name)]

        # Circuit breaker gate. When the circuit is open and still cooling
        # down, bypass the retry loop entirely and route the payloads to the
        # DLQ. The breaker check itself is cheap (a time.monotonic + integer
        # compare) so running it before every batch is fine. We surface the
        # skip as an error_count tick + sentinel last_error so the debug UI
        # and stats endpoints can explain why the sink received nothing.
        if sink.should_skip_delivery():
            stats.error_count += 1
            stats.last_error = CIRCUIT_OPEN_ERROR
            stats.last_error_ts = time.time()
            if self._recorder is not None:
                self._recorder.record_sink_error(
                    sink_type=sink_type,
                    sink_name=sink_name,
                    error=CIRCUIT_OPEN_ERROR,
                    attempt=0,
                )
            error = DeliveryError(
                sink_name=sink_name,
                sink_type=sink_type,
                error=CIRCUIT_OPEN_ERROR,
                payloads=payloads,
            )
            # Force DLQ routing when we have a DLQ sink: the breaker has
            # already decided the sink is unhealthy, so there's nothing for
            # the handler's SKIP/RETRY/DLQ choice to usefully change.
            # We do NOT invoke ``on_delivery_error`` on this path — the
            # handler's job is to classify delivery failures against a
            # live sink; a tripped breaker is an infrastructure signal
            # outside that contract. Calling the handler here would also
            # risk double-DLQ (typical app handlers route DLQ-action
            # results back to the same DLQ sink), so we keep the path
            # direct.
            if self._dlq_sink is not None:
                await self._dlq_sink.send(error, partition_id=partition_id)
            else:
                # Legacy path: no DLQ sink wired. Fall back to handler-driven
                # routing and hope the handler's DLQ plumbing lives upstream.
                await on_delivery_error(error)
            await logger.awarning(
                'sink_delivery_circuit_open',
                category='sink',
                sink_type=sink_type,
                sink_name=sink_name,
                payload_count=len(payloads),
            )
            return

        # Track whether THIS invocation claimed the half-open probe slot.
        # ``should_skip_delivery`` above returned False, so if the sink is
        # now in ``half_open`` state with ``probe_inflight=True``, it was
        # this caller that claimed the probe slot (either by promoting
        # ``open → half_open`` after cooldown or by being first to enter
        # half_open with no probe in flight). We MUST release that slot
        # before returning, via either:
        #   - ``record_success`` / ``record_failure`` (terminal outcomes
        #     that also drive circuit-state transitions), or
        #   - ``release_probe_inflight`` (neutral release for SKIP and
        #     handler-raising paths that do neither of the above).
        # Without the try/finally below, a SKIP action or a raise inside
        # ``on_delivery_error`` would leak the probe flag and permanently
        # wedge the circuit in ``half_open + inflight=True``.
        probe_claimed = sink.circuit_state == 'half_open' and sink.probe_inflight
        try:
            attempt = 0
            while True:
                try:
                    start = time.monotonic()
                    await sink.deliver(payloads)
                    duration = time.monotonic() - start
                    stats.delivered_count += 1
                    stats.delivered_payloads += len(payloads)
                    stats.last_delivery_ts = time.time()
                    stats.last_delivery_duration = round(duration, 4)
                    # Terminal success — let the breaker close if it was probing
                    # (half_open), or simply reset the consecutive-failure count
                    # when the circuit was already closed.
                    sink.record_success()
                    if self._recorder is not None:
                        self._recorder.record_sink_delivery(
                            sink_type=sink_type,
                            sink_name=sink_name,
                            payload_count=len(payloads),
                            duration=duration,
                        )
                    return
                except Exception as e:
                    attempt += 1
                    stats.error_count += 1
                    # Sink exceptions can carry secrets (DSNs with passwords),
                    # so every string that lands in last_error / recorder /
                    # DeliveryError goes through ``redact_url`` first. Parallel
                    # delivery amplifies the blast radius of any leak, so
                    # redact at the source rather than hoping downstream logs
                    # filter it.
                    safe_error = redact_url(str(e))
                    stats.last_error = safe_error
                    stats.last_error_ts = time.time()
                    if self._recorder is not None:
                        self._recorder.record_sink_error(
                            sink_type=sink_type,
                            sink_name=sink_name,
                            error=safe_error,
                            attempt=attempt,
                        )
                    error = DeliveryError(
                        sink_name=sink_name,
                        sink_type=sink_type,
                        error=safe_error,
                        payloads=payloads,
                    )
                    action = await on_delivery_error(error)

                    if action == DeliveryAction.RETRY and attempt < max_retries:
                        stats.retry_count += 1
                        sink_delivery_retries.labels(sink_type=sink_type, sink_name=sink_name).inc()
                        await logger.awarning(
                            'sink_delivery_retry',
                            category='sink',
                            sink_type=sink_type,
                            sink_name=sink_name,
                            attempt=attempt,
                        )
                        continue
                    elif action == DeliveryAction.SKIP:
                        # SKIP is operator intent (handler returned SKIP) — treat
                        # as "this delivery is not a true failure" from the breaker's
                        # perspective, so we do NOT record a failure. The circuit
                        # should only trip on infrastructure failure, not on user
                        # code deciding to drop a batch. The try/finally around
                        # this loop releases the probe slot if we were the probe,
                        # so SKIP cannot wedge a half-open circuit.
                        sink_deliveries_skipped.labels(sink_type=sink_type, sink_name=sink_name).inc()
                        await logger.awarning(
                            'sink_delivery_skipped',
                            category='sink',
                            sink_type=sink_type,
                            sink_name=sink_name,
                            payload_count=len(payloads),
                        )
                        return
                    else:
                        # DLQ or RETRY exhausted — DLQ handling is done by the caller (app.py)
                        # since it needs access to the DLQ sink which lives outside the manager.
                        # This is a terminal failure for the sink — tell the breaker
                        # so consecutive failures can accumulate toward the trip threshold.
                        sink.record_failure()
                        await logger.awarning(
                            'sink_delivery_failed_to_dlq',
                            category='sink',
                            sink_type=sink_type,
                            sink_name=sink_name,
                            payload_count=len(payloads),
                            attempts=attempt,
                        )
                        return
        finally:
            # Probe-slot leak guard. ``record_success`` / ``record_failure``
            # clear ``probe_inflight`` on terminal outcomes, so this is a
            # no-op for the normal paths. It only fires when we exit via
            # SKIP, an uncaught exception from ``on_delivery_error``, or any
            # other path that skips both recorder helpers — without it those
            # paths would leave ``probe_inflight=True`` and every future
            # ``should_skip_delivery`` would return True, wedging the
            # circuit in half_open forever.
            if probe_claimed and sink.probe_inflight:
                sink.release_probe_inflight()
