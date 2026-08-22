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
from drakkar.metrics import dlq_dropped_payloads, sink_deliveries_skipped, sink_delivery_retries
from drakkar.models import CollectResult, DeliveryAction, DeliveryError, SinkDeliveryFailedError
from drakkar.sinks.base import BaseSink
from drakkar.sinks.registry import SinkRegistry
from drakkar.utils import redact_url

# Exception types that we treat as "transient" and retry for an idempotent
# sink before surfacing through ``on_delivery_error``. Kept narrow on
# purpose — over-classifying (e.g., treating every ``Exception`` as
# transient) would paper over real bugs and delay DLQ routing for
# genuinely broken downstreams. asyncio-native timeouts and OS-level
# connection errors cover the common "flaky network" case; sink-library
# specific errors (aiokafka, asyncpg, etc.) usually subclass
# ``ConnectionError`` or ``TimeoutError`` in practice, or raise a
# library-specific exception that the sink implementation can remap
# before re-raising. Users who need a broader classification can
# subclass ``BaseSink`` and catch + re-raise as a ``ConnectionError``
# inside their own ``deliver()`` method.
_TRANSIENT_ERRORS: tuple[type[BaseException], ...] = (
    ConnectionError,
    TimeoutError,
    asyncio.TimeoutError,
)

# Number of attempts made by the idempotent fast-retry before the error
# surfaces through ``on_delivery_error``. This is intentionally small —
# the idempotent retry is a quick best-effort to smooth out a single
# blipped packet; if the downstream is truly down, we want the circuit
# breaker + DLQ logic to see the outcome fast rather than burn budget.
_IDEMPOTENT_MAX_ATTEMPTS = 3

# Exponential backoff base for the idempotent fast-retry. At attempt i
# (0-indexed among retries only, not counting the first attempt) we
# sleep ``_IDEMPOTENT_BACKOFF_BASE * 2 ** i`` seconds — 50ms, 100ms,
# 200ms for the default 3 attempts. Small enough to be invisible under
# normal operation; large enough to let the downstream recover from a
# momentary blip before we hammer it again.
_IDEMPOTENT_BACKOFF_BASE = 0.05

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

# Mapping from CollectResult field name to sink_type string. Each entry
# is a built-in sink whose payload model lives in ``drakkar.models``.
# Plugin-registered sinks are dispatched separately via
# ``CollectResult.custom`` (see ``_dispatch_custom_payloads``) so the
# framework does not have to know the plugin's sink_type ahead of time.
_FIELD_TO_SINK_TYPE: dict[str, str] = {
    'kafka': 'kafka',
    'postgres': 'postgres',
    'mongo': 'mongo',
    'http': 'http',
    'redis': 'redis',
    'files': 'filesystem',
}

# Sink types Drakkar ships with. Any sink registered with a ``sink_type``
# NOT in this set is considered a plugin sink — payloads in
# ``CollectResult.custom`` route to those by instance name. We keep this
# in sync manually with ``_FIELD_TO_SINK_TYPE.values()`` rather than
# deriving it dynamically: the static set documents the framework
# contract for plugin authors and any future built-in addition is a
# breaking change that needs explicit acknowledgement here.
_BUILTIN_SINK_TYPES: frozenset[str] = frozenset({'kafka', 'postgres', 'mongo', 'http', 'redis', 'filesystem'})


@dataclass
class SinkStats:
    """Per-sink delivery statistics tracked in memory.

    ``last_delivery_duration`` measures the wall-clock of the FULL
    ``_deliver_to_sink`` attempt — for an idempotent sink that retries
    internally on a transient error, this includes the failed attempts
    and their backoff sleeps, not just the successful call. The figure
    is therefore an upper-bound estimate of per-batch sink cost rather
    than the precise "successful attempt" latency. Prometheus
    ``drakkar_sink_delivery_duration_seconds`` is a better source for
    latency percentiles across attempts.
    """

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
        # Strategy for circuit-open deliveries whose DLQ write fails.
        # Overridden via attach_runtime from DLQConfig.on_send_failure.
        self._dlq_on_send_failure: str = 'drop'

        # Run plugin discovery once at construction so any third-party
        # sinks installed via ``[project.entry-points."drakkar.sinks"]``
        # are visible to ``resolve_sink_class``. ``discover()`` is
        # idempotent — repeated SinkManager construction (in tests, in
        # restart loops) does not re-walk the entry-point table.
        SinkRegistry.discover()

    def resolve_sink_class(self, type_name: str) -> type[BaseSink[Any]] | None:
        """Look up a sink class by its registered type name.

        Returns ``None`` when the name is not registered so the caller
        can decide between hard failure (config explicitly named an
        unknown sink type) and silent skip (optional plugin not
        installed). Used by :class:`drakkar.app.DrakkarApp` when it
        materialises sink instances from config.
        """
        return SinkRegistry.get(type_name)

    def attach_runtime(
        self,
        recorder: EventRecorder | None,
        dlq_sink: DLQSink | None,
        dlq_on_send_failure: str = 'drop',
    ) -> None:
        """Inject recorder + DLQ sink after construction.

        ``DrakkarApp`` constructs the ``SinkManager`` in its own
        ``__init__`` (before the recorder and DLQ sink exist) but the
        SinkManager needs references to both during delivery. This setter
        is the named one-time wiring step that ``AppLifecycle._async_run``
        calls once both objects have been built — mirrors the precedent
        set by :meth:`BaseSink.mark_connected` / :meth:`mark_disconnected`
        for one-time state transitions on a public collaborator.

        Both arguments are assigned directly — pass ``None`` to clear a
        reference (e.g., when debug is disabled and no recorder was ever
        constructed).

        ``dlq_on_send_failure`` mirrors ``DLQConfig.on_send_failure`` and
        governs the circuit-open DLQ path: 'drop' logs + counts and lets
        the offset commit; 'stall' raises ``SinkDeliveryFailedError`` so
        the partition pipeline stalls the affected offsets.
        """
        self._recorder = recorder
        self._dlq_sink = dlq_sink
        self._dlq_on_send_failure = dlq_on_send_failure

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
            # Mark the sink as connected only AFTER ``connect()`` returns
            # successfully. If the subclass raises we want the flag to stay
            # False so ``all_connected()`` (used by ``/readyz``) reports the
            # sink as down — otherwise a post-raise readiness check could
            # falsely succeed.
            sink.mark_connected()
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
            # Flip the sink back to "not connected" regardless of whether
            # ``close()`` raised — we already logged the original connect
            # failure, and leaving the flag True on a cleanup-failed sink
            # would mislead ``/readyz`` into reporting it as healthy.
            sink.mark_disconnected()
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
            finally:
                # Always flip the connection flag even if ``close()`` raised
                # — after shutdown nothing else reads from the sink, and a
                # raised close() means the connection is in an undefined
                # state which is strictly not "connected".
                sink.mark_disconnected()

    def all_connected(self) -> bool:
        """Return True when every registered sink is connected.

        Consulted by the ``/readyz`` endpoint (``drakkar/uiserver_server.py``)
        to report whether the worker can serve traffic. An empty manager
        (no sinks registered) returns True — startup validation in
        ``DrakkarApp`` already rejects that config, so the invariant is
        upheld at a higher layer.
        """
        return all(sink.is_connected for sink in self._sinks.values())

    def disconnected_sink_names(self) -> list[str]:
        """Return ``"<type>:<name>"`` identifiers for sinks that are NOT connected.

        Used by the ``/readyz`` endpoint to populate the ``reasons`` array in
        its 503 response body so operators can see at a glance which sink is
        keeping the worker from reporting ready.
        """
        return [f'{sink.sink_type}:{sink.name}' for sink in self._sinks.values() if not sink.is_connected]

    def resolve_custom_sink(self, sink_name: str) -> BaseSink[Any]:
        """Resolve a plugin-registered sink instance by instance name.

        Plugin sinks are addressed by name in ``CollectResult.custom`` —
        the framework does not require the user to also pass the
        plugin's ``sink_type`` because the instance name is unique
        across plugin sinks (the validator below rejects duplicates so
        this contract holds).

        Args:
            sink_name: The non-empty instance name from
                ``CustomPayload.sink``. Must match a registered plugin
                sink instance.

        Raises:
            SinkNotConfiguredError: No plugin sink instance has that
                name (the operator forgot to declare it under
                ``sinks.custom`` or there's a typo in the handler).
            AmbiguousSinkError: Two or more plugin sink instances
                across different ``sink_type`` values share the same
                name — operators must rename one to disambiguate.
        """
        if not sink_name:
            # Custom payloads must always carry an explicit instance
            # name — the empty-string convenience used by built-in
            # payloads only works when the framework knows the
            # ``sink_type`` ahead of time. Plugin sinks have no shared
            # type for that fallback to lean on.
            raise SinkNotConfiguredError(
                'CustomPayload.sink is empty — plugin sink payloads must name '
                'the configured sink instance explicitly (one of: '
                f'{[name for (st, name), _ in self._sinks.items() if st not in _BUILTIN_SINK_TYPES]!r}).'
            )

        candidates = [
            (sink_type, sink)
            for (sink_type, name), sink in self._sinks.items()
            if name == sink_name and sink_type not in _BUILTIN_SINK_TYPES
        ]
        if not candidates:
            plugin_names = sorted({name for (st, name), _ in self._sinks.items() if st not in _BUILTIN_SINK_TYPES})
            raise SinkNotConfiguredError(
                f'No plugin sink instance named {sink_name!r} configured under sinks.custom — '
                f'known plugin sink instances: {plugin_names!r}'
            )
        if len(candidates) > 1:
            types = sorted({sink_type for sink_type, _ in candidates})
            raise AmbiguousSinkError(
                f'Plugin sink instance name {sink_name!r} is shared across multiple sink types '
                f'({types}). Rename one of them so the routing is unambiguous.'
            )
        return candidates[0][1]

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

        Iterates all populated fields (built-in + ``custom``), resolves
        each payload's sink, and raises ``SinkNotConfiguredError`` or
        ``AmbiguousSinkError`` on the first problem. Called before
        delivery so the worker crashes fast on misconfiguration.

        Plugin sink payloads in ``result.custom`` are resolved by
        instance name via :meth:`resolve_custom_sink` — the framework
        does not require the handler to also pass the sink type because
        plugin instance names are unique across plugin types (see the
        ambiguity check in ``resolve_custom_sink``).
        """
        for field_name, sink_type in _FIELD_TO_SINK_TYPE.items():
            payloads = getattr(result, field_name)
            if not payloads:
                continue
            for payload in payloads:
                self.resolve_sink(sink_type, payload.sink)
        for payload in result.custom:
            self.resolve_custom_sink(payload.sink)

    async def deliver_all(
        self,
        result: CollectResult,
        on_delivery_error: DeliveryErrorCallback,
        partition_id: int,
        max_retries: int = 3,
    ) -> None:
        """Route and deliver all payloads in a CollectResult to their sinks.

        Groups payloads by (sink_type, resolved_sink_name) for both
        built-in payload fields (``kafka``, ``postgres``, …) AND the
        plugin-sink ``custom`` field, then delivers each group
        concurrently via asyncio.gather. Total wall-clock time becomes
        ~max(sink_latency) instead of the sum. On delivery failure,
        calls on_delivery_error and handles the returned action
        (DLQ, RETRY, SKIP) PER SINK — each sink retries independently.

        Recorder + DLQ sink are read from instance state
        (``self._recorder`` / ``self._dlq_sink``) — both are set via
        ``__init__`` kwargs or the later :meth:`attach_runtime` setter.
        Holding them as instance state keeps the hot path signature
        minimal and avoids threading the same two objects through every
        delivery call.

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

        # Plugin sink payloads. Resolution is by instance name across the
        # non-built-in sink set; the validate_collect step above has
        # already rejected unknown / ambiguous names so resolve_custom_sink
        # is guaranteed to succeed at this point.
        for payload in result.custom:
            sink = self.resolve_custom_sink(payload.sink)
            groups[(sink.sink_type, sink.name)].append(payload)

        if not groups:
            return

        # Dispatch per-sink delivery coroutines concurrently. Each coroutine
        # owns its own retry/DLQ/stats logic, so concurrent execution is safe
        # — stats are scoped per (sink_type, name), no cross-sink contention.
        # ``return_exceptions=True`` lets every sibling delivery settle — a
        # bare gather would abandon in-flight groups on the first raise and
        # swallow any later failures entirely. After all groups finish we
        # re-raise the FIRST exception (iteration order), mirroring
        # ``connect_all``: an exception leaking out of ``_deliver_to_sink``
        # still propagates loudly, it just no longer cancels its siblings.
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
        results = await asyncio.gather(*coros, return_exceptions=True)
        for result_or_exc in results:
            if isinstance(result_or_exc, BaseException):
                raise result_or_exc

    async def _deliver_with_transient_retry(
        self,
        sink: BaseSink[Any],
        sink_type: str,
        sink_name: str,
        payloads: list[BaseModel],
    ) -> None:
        """Run ``sink.deliver(payloads)`` with a bounded fast-retry on transient errors.

        The retry budget is consulted ONLY when
        ``sink.batch_idempotent(payloads)`` is True: duplicate delivery is
        safe (broker-dedup, write-replace, etc.) so a transient error
        (network blip, connection reset) is worth retrying before we burn
        the user-facing ``on_delivery_error`` budget and advance the
        circuit breaker. Non-idempotent batches get a single attempt — a
        retry could double-submit, and the caller's error handler is the
        right place to decide SKIP / DLQ / RETRY.

        The decision is per BATCH rather than per sink because a sink whose
        payloads carry an operation discriminator can be idempotent for one
        delivery and not the next. ``BaseSink.batch_idempotent`` defaults to
        the class-level ``idempotent`` flag, so sinks that do not override
        it are unaffected.

        Non-transient exceptions (``ValueError``, ``RuntimeError``, sink
        configuration errors, etc.) are NOT retried even when the sink
        is idempotent — they indicate a bug in the payload or the sink
        itself, and a quick re-attempt won't change the outcome.

        The retries happen INSIDE a single ``_deliver_to_sink`` attempt
        from the circuit breaker's perspective: whether we made 1 or N
        underlying attempts, the breaker sees exactly one ``record_*``
        call for this delivery. That preserves the circuit-breaker
        invariant that a single batch with in-call retries counts as ONE
        consecutive failure.
        """
        # Non-idempotent fast path: a single shot, exactly as before.
        # We intentionally do NOT wrap in try/except here so the
        # exception propagates unchanged to the caller's handling logic.
        if not sink.batch_idempotent(payloads):
            await sink.deliver(payloads)
            return

        # Idempotent path: transient errors get a small number of quick
        # retries with exponential backoff. Non-transient errors propagate
        # on the first raise. Successful delivery returns immediately.
        for attempt_idx in range(_IDEMPOTENT_MAX_ATTEMPTS):
            try:
                await sink.deliver(payloads)
                return
            except _TRANSIENT_ERRORS as exc:
                # The last attempt has nothing to retry into — just
                # re-raise so the caller's error handler sees the
                # transient error exactly as it would on a non-idempotent
                # sink.
                if attempt_idx == _IDEMPOTENT_MAX_ATTEMPTS - 1:
                    raise
                # Count each in-call retry on the public counter so
                # operators see the retry activity separately from the
                # handler-driven RETRY action. ``stats.retry_count`` is
                # NOT bumped here because that counter reflects handler
                # decisions — the in-call retry is an internal fast-path
                # and should not alter the operator-visible "how often
                # did your handler return RETRY" signal.
                sink_delivery_retries.labels(sink_type=sink_type, sink_name=sink_name).inc()
                backoff = _IDEMPOTENT_BACKOFF_BASE * (2**attempt_idx)
                await logger.adebug(
                    'sink_idempotent_transient_retry',
                    category='sink',
                    sink_type=sink_type,
                    sink_name=sink_name,
                    attempt=attempt_idx + 1,
                    backoff=backoff,
                    error=redact_url(str(exc)),
                )
                await asyncio.sleep(backoff)

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
                if not await self._dlq_sink.send(error, partition_id=partition_id):
                    # The breaker is open AND the DLQ write failed — the
                    # payloads have nowhere safe to go. Apply the
                    # dlq.on_send_failure strategy.
                    if self._dlq_on_send_failure == 'stall':
                        raise SinkDeliveryFailedError(
                            sink_name=sink_name,
                            sink_type=sink_type,
                            reason='circuit open and DLQ send failed',
                        )
                    dlq_dropped_payloads.labels(partition=str(partition_id)).inc()
                    await logger.acritical(
                        'dlq_failure_payloads_dropped',
                        category='sink',
                        sink_name=sink_name,
                        sink_type=sink_type,
                        partition=partition_id,
                        payload_count=len(payloads),
                        reason='circuit open and DLQ send failed',
                        action='ALERT: payloads lost (dlq.on_send_failure=drop) — '
                        'set dlq.on_send_failure=stall to prefer replay over loss',
                    )
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
                    # Idempotent sinks get a small fast-retry on transient
                    # errors before the failure surfaces through
                    # ``on_delivery_error``. Non-idempotent sinks take a
                    # single shot (current behavior preserved).
                    # Both paths count as ONE delivery attempt from the
                    # circuit breaker's perspective — the transient-retry
                    # loop is purely internal to this ``deliver`` call.
                    await self._deliver_with_transient_retry(
                        sink=sink,
                        sink_type=sink_type,
                        sink_name=sink_name,
                        payloads=payloads,
                    )
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
                    try:
                        action = await on_delivery_error(error)
                    except Exception:
                        # The hook raising (handler bug, or a stall-mode
                        # SinkDeliveryFailedError from the DLQ fallback) is
                        # a terminal failure for this delivery — tell the
                        # breaker before propagating so consecutive
                        # failures still accumulate toward the trip
                        # threshold instead of leaving it blind.
                        sink.record_failure()
                        raise

                    if action == DeliveryAction.RETRY and attempt < max_retries:
                        stats.retry_count += 1
                        sink_delivery_retries.labels(sink_type=sink_type, sink_name=sink_name).inc()
                        logger.warning(
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
                        logger.warning(
                            'sink_delivery_skipped',
                            category='sink',
                            sink_type=sink_type,
                            sink_name=sink_name,
                            payload_count=len(payloads),
                        )
                        return
                    elif action == DeliveryAction.DLQ:
                        # The DLQ write already happened inside the
                        # on_delivery_error callback (app.py's wrapper handles
                        # the DLQ action, including dlq.on_send_failure).
                        # Terminal failure for the sink — tell the breaker so
                        # consecutive failures accumulate toward the trip
                        # threshold.
                        sink.record_failure()
                        logger.warning(
                            'sink_delivery_failed_to_dlq',
                            category='sink',
                            sink_type=sink_type,
                            sink_name=sink_name,
                            payload_count=len(payloads),
                            attempts=attempt,
                        )
                        return
                    else:
                        # RETRY with the budget exhausted. The handler never
                        # returned DLQ, so the app-level wrapper (which DLQs
                        # only on a DLQ action) never shipped these payloads
                        # — route them to the DLQ here instead of dropping
                        # them silently (docs/sinks.md promises this
                        # fallthrough).
                        sink.record_failure()
                        await self._dlq_exhausted_retry(error=error, partition_id=partition_id, attempts=attempt)
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

    async def _dlq_exhausted_retry(
        self,
        error: DeliveryError,
        partition_id: int,
        attempts: int,
    ) -> None:
        """Ship a retries-exhausted batch to the DLQ, honoring ``dlq.on_send_failure``.

        The handler kept answering RETRY, so the app-level wrapper — which
        DLQs only when the handler returns the DLQ action — never shipped
        these payloads. Without this fallthrough the batch would vanish
        once the retry budget ran out.
        """
        if self._dlq_sink is not None and await self._dlq_sink.send(
            error, partition_id=partition_id, attempt_count=attempts
        ):
            logger.warning(
                'sink_delivery_retries_exhausted_to_dlq',
                category='sink',
                sink_type=error.sink_type,
                sink_name=error.sink_name,
                payload_count=len(error.payloads),
                attempts=attempts,
            )
            return
        # No DLQ sink wired, or the DLQ write failed — the payloads have
        # nowhere safe to go. Apply the dlq.on_send_failure strategy, the
        # same way the circuit-open path above does.
        if self._dlq_on_send_failure == 'stall':
            raise SinkDeliveryFailedError(
                sink_name=error.sink_name,
                sink_type=error.sink_type,
                reason='delivery retries exhausted and DLQ unavailable',
            )
        dlq_dropped_payloads.labels(partition=str(partition_id)).inc()
        await logger.acritical(
            'dlq_failure_payloads_dropped',
            category='sink',
            sink_name=error.sink_name,
            sink_type=error.sink_type,
            partition=partition_id,
            payload_count=len(error.payloads),
            reason='delivery retries exhausted and DLQ unavailable',
            action='ALERT: payloads lost (dlq.on_send_failure=drop) — '
            'set dlq.on_send_failure=stall to prefer replay over loss',
        )
