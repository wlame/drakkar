"""Sink manager — orchestrates routing, delivery, and error handling.

The SinkManager holds all configured sink instances, validates that
CollectResult payloads target existing sinks, and delivers one result's
payload groups by each sink's declared policy: its retry budget, its
``on_failure`` and its ``depends_on``. It is also the one place that writes
the DLQ record of a sink failure (the circuit-open, hook and spent-budget
paths all end there) and of a task that ``on_error`` sent to the DLQ.
"""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from collections.abc import Awaitable, Callable, Iterator, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Literal, Protocol

import structlog
from pydantic import BaseModel

from drakkar.config import SinkPolicy
from drakkar.config.sinks import DEFAULT_SINK_POLICY
from drakkar.metrics import (
    dlq_dropped_payloads,
    dlq_unconfigured_drops,
    sink_deliveries_not_started,
    sink_deliveries_skipped,
    sink_delivery_retries,
)
from drakkar.models import (
    CollectResult,
    DeliveryAction,
    DeliveryError,
    ExecutorError,
    ExecutorTask,
    SinkDeliveryFailedError,
    SinkNotStartedReason,
    SourceMessage,
)
from drakkar.providers.resilience import RateLimitedLog
from drakkar.sinks.base import BaseSink
from drakkar.sinks.dlq_record import DLQRecord, cut_error
from drakkar.utils import redact_url

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
# is a sink whose payload model lives in ``drakkar.models``.
_FIELD_TO_SINK_TYPE: dict[str, str] = {
    'kafka': 'kafka',
    'postgres': 'postgres',
    'mongo': 'mongo',
    'http': 'http',
    'redis': 'redis',
    'files': 'filesystem',
}

# A sink instance, keyed by its sink type and its name.
SinkKey = tuple[str, str]

# A caller's check before one group starts (see ``SinkManager.deliver_all``).
GroupGate = Callable[[SinkKey], Awaitable[None]]


def sink_ref(key: SinkKey) -> str:
    """The ``<type>/<name>`` reference of a sink, as ``depends_on`` and the DLQ record write it."""
    return f'{key[0]}/{key[1]}'


class DeliveryOutcome(StrEnum):
    """How the delivery of one result to one sink ended."""

    DELIVERED = 'delivered'
    # The payloads were dropped: the hook answered SKIP or the declared policy is skip.
    SKIPPED = 'skipped'
    # The source message is on the DLQ (this delivery wrote its record, or an earlier one did).
    DLQ = 'dlq'
    # The delivery failed and no record was written (a window result, or the DLQ write failed).
    FAILED = 'failed'
    NOT_STARTED = 'not_started'


# Outcomes of a dependency that let its dependents start: a sink the handler
# skipped is not "undelivered".
_SATISFIED = frozenset({DeliveryOutcome.DELIVERED, DeliveryOutcome.SKIPPED})


@dataclass
class MessageDLQState:
    """The DLQ state of one source message, shared by every result of that message.

    ``dlq_written`` ends the message: no later delivery of it starts.
    ``delivered_sinks`` collects the ``<type>/<name>`` of every sink that
    delivered a result of it, for the record's ``dk-delivered-sinks``.
    ``dlq_lost`` marks a message whose record could not be written under
    ``dlq.on_send_failure: drop``: the loss is counted once, so a later
    failure of the message writes, counts and logs nothing new.
    ``lock`` guards the checks and the sets of both flags together with the
    record write, so one message gets one record or one counted loss.
    """

    dlq_written: bool = False
    dlq_lost: bool = False
    delivered_sinks: set[str] = field(default_factory=set)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


@dataclass(frozen=True)
class MessageContext:
    """The source messages a task or message result belongs to, each with its DLQ state.

    A task with several ``source_offsets`` has several messages.

    The window form (``window``) has no source message: an
    ``on_window_complete`` result is delivered after its messages completed
    their offsets, so it cannot send one to the DLQ. Only this explicit form
    makes ``on_failure: dlq`` act as ``skip``.

    The HTTP form (``for_request``) holds one message whose value is the raw
    request body; ``request_id`` marks it, and its record carries no key,
    no source headers and no topic, partition or offset.
    """

    source_messages: Sequence[SourceMessage]
    states: Sequence[MessageDLQState]
    request_id: str | None = None
    is_window: bool = False

    def __post_init__(self) -> None:
        if len(self.source_messages) != len(self.states):
            raise ValueError('MessageContext needs one DLQ state per source message')
        if not self.source_messages and not self.is_window:
            # An empty context would make a dlq outcome write nothing and
            # look handled; only the explicit window form may have no message.
            raise ValueError(
                'MessageContext needs at least one source message; use MessageContext.window() for a window result'
            )

    @classmethod
    def window(cls) -> MessageContext:
        """The context of an ``on_window_complete`` result: no source message, so no DLQ record."""
        return cls(source_messages=(), states=(), is_window=True)

    @classmethod
    def for_request(cls, *, body: bytes, request_id: str, received_ms: int) -> MessageContext:
        """The context of one HTTP request: its raw body, its id and the time it was received in ms."""
        message = SourceMessage(topic='', partition=-1, offset=-1, value=body, timestamp=received_ms)
        return cls(source_messages=[message], states=[MessageDLQState()], request_id=request_id)

    @property
    def is_on_dlq(self) -> bool:
        """True once a source message of this result is on the DLQ: no delivery of the result starts."""
        return any(state.dlq_written for state in self.states)

    def pairs(self) -> Iterator[tuple[SourceMessage, MessageDLQState]]:
        """Each source message with its DLQ state."""
        return zip(self.source_messages, self.states, strict=True)


class CollectCallback(Protocol):
    """Deliver a CollectResult to the sinks.

    ``partition_id`` is -1 for an HTTP request. ``message_context`` names
    the source messages of a task or message result and their DLQ state;
    ``MessageContext.window()`` marks a window result, which has no source
    message. ``None`` means no context (fail closed): a ``dlq`` outcome then
    follows ``dlq.on_send_failure`` (see ``SinkManager.deliver_all``).
    """

    def __call__(
        self,
        result: CollectResult,
        partition_id: int,
        message_context: MessageContext | None = None,
    ) -> Awaitable[None]: ...


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
        recorder: EventRecorder | None = None,
        dlq_sink: DLQSink | None = None,
    ) -> None:
        self._sinks: dict[tuple[str, str], BaseSink[Any]] = {}
        self._by_type: dict[str, list[BaseSink[Any]]] = defaultdict(list)
        self._stats: dict[tuple[str, str], SinkStats] = {}
        # The resolved policy of each sink and its dependencies as keys,
        # fixed when the sink is registered so a delivery reads two dicts
        # instead of walking the config section.
        self._policies: dict[SinkKey, SinkPolicy] = {}
        self._dependencies: dict[SinkKey, tuple[SinkKey, ...]] = {}
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
        # True when the DLQ is on but has no destination (no topic, no Kafka
        # source): a missing DLQ sink is then a counted drop, never a stall.
        self._dlq_unconfigured: bool = False
        self._dlq_drop_warned: bool = False
        # One warning line per sink and event per minute for the failure
        # paths that can fire on every result of a failing sink.
        self._rate_limited_log = RateLimitedLog(time.monotonic)

    def attach_runtime(
        self,
        recorder: EventRecorder | None,
        dlq_sink: DLQSink | None,
        dlq_on_send_failure: str = 'drop',
        dlq_unconfigured: bool = False,
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

        ``dlq_unconfigured`` mirrors ``DrakkarConfig.dlq_unconfigured``: with
        a DLQ that has no destination a record that has nowhere to go is a
        counted drop (``drakkar_dlq_unconfigured_drops_total``), whatever
        ``dlq_on_send_failure`` says. A DLQ switched off with ``dlq.enabled``
        is not this case: with no sink, ``dlq_on_send_failure`` decides.
        """
        self._recorder = recorder
        self._dlq_sink = dlq_sink
        self._dlq_on_send_failure = dlq_on_send_failure
        self._dlq_unconfigured = dlq_unconfigured

    @property
    def sinks(self) -> dict[tuple[str, str], BaseSink[Any]]:
        """All registered sinks keyed by (sink_type, name)."""
        return dict(self._sinks)

    @property
    def sink_count(self) -> int:
        """Total number of registered sinks."""
        return len(self._sinks)

    def register(self, sink: BaseSink[Any], policy: SinkPolicy = DEFAULT_SINK_POLICY) -> None:
        """Register a sink instance with its resolved policy.

        Raises ValueError if a sink with the same (type, name) already exists.
        Also installs the policy's circuit breaker settings and delivery timeout on the sink.
        """
        key = (sink.sink_type, sink.name)
        if key in self._sinks:
            raise ValueError(f'Duplicate sink: type={sink.sink_type!r}, name={sink.name!r}')
        self._sinks[key] = sink
        self._by_type[sink.sink_type].append(sink)
        self._stats[key] = SinkStats()
        self._policies[key] = policy
        dependencies: list[SinkKey] = []
        for ref in policy.depends_on:
            # The config load already checked that every reference names a configured sink.
            dependency_type, _, dependency_name = ref.partition('/')
            dependencies.append((dependency_type, dependency_name))
        self._dependencies[key] = tuple(dependencies)
        # Route circuit-breaker config through the public setter so the sink
        # keeps a single documented API for thresholds — the manager never
        # reaches into BaseSink's private attributes.
        sink.configure_circuit_breaker(failures=policy.breaker_failures, cooldown_seconds=policy.breaker_cooldown)
        sink.configure_delivery_timeout(policy.delivery_timeout)

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
        for key, sink in self._sinks.items():
            timeout = self._policies[key].delivery_timeout
            try:
                # Bounded for the same reason delivery is: a close() against
                # an unresponsive server would otherwise hold shutdown open
                # past the drain budget, and every sink after it too.
                await asyncio.wait_for(sink.close(), timeout=timeout)
            except TimeoutError:
                await logger.awarning(
                    'sink_close_timeout',
                    category='sink',
                    sink_type=sink.sink_type,
                    sink_name=sink.name,
                    timeout_seconds=timeout,
                )
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

        Iterates all populated fields, resolves
        each payload's sink, and raises ``SinkNotConfiguredError`` or
        ``AmbiguousSinkError`` on the first problem. Called before
        delivery so the worker crashes fast on misconfiguration.
        """
        for field_name, sink_type in _FIELD_TO_SINK_TYPE.items():
            payloads = getattr(result, field_name)
            if not payloads:
                continue
            for payload in payloads:
                self.resolve_sink(sink_type, payload.sink)

    def _group_payloads(self, result: CollectResult) -> dict[SinkKey, list[BaseModel]]:
        """The result's payloads grouped by sink, each group in payload order."""
        groups: dict[SinkKey, list[BaseModel]] = defaultdict(list)
        for field_name, sink_type in _FIELD_TO_SINK_TYPE.items():
            payloads = getattr(result, field_name)
            if not payloads:
                continue
            for payload in payloads:
                sink = self.resolve_sink(sink_type, payload.sink)
                groups[(sink.sink_type, sink.name)].append(payload)
        return groups

    async def deliver_all(
        self,
        result: CollectResult,
        on_delivery_error: DeliveryErrorCallback,
        partition_id: int,
        *,
        message_context: MessageContext | None = None,
        before_group_start: GroupGate | None = None,
    ) -> dict[SinkKey, DeliveryOutcome]:
        """Deliver every payload group of one result by its sink's declared policy and dependencies.

        Groups payloads by (sink_type, resolved_sink_name). A group whose
        sink has no dependency in this result starts at once, in parallel
        with the others; a group with dependencies starts when each of them
        settled with success (delivered or skipped). A dependency with no
        payloads in this result counts as satisfied. A dependency that ended
        the message on the DLQ stops its dependents with reason
        ``message_dlq``; one that failed without a record stops them with
        ``dependency_failed``.

        Args:
            result: The CollectResult from on_task_complete(),
                on_message_complete(), or on_window_complete().
            on_delivery_error: Handler callback for delivery failures.
            partition_id: Source partition (-1 for an HTTP request).
            message_context: The source messages of a task or message result
                and their DLQ state, or ``MessageContext.window()`` for a
                window result, where ``on_failure: dlq`` acts as ``skip``.
                ``None`` means no context: a ``dlq`` outcome then has no
                record to write and follows ``dlq.on_send_failure``, so a
                caller that forgets the context loses nothing silently.
            before_group_start: Awaited once per group with its key, after
                the group's dependencies settled and before it starts or is
                counted as not started. What it raises ends that group with
                nothing counted for it; the other groups still settle, and
                the first raise comes out of this call. The HTTP source
                checks its request timeout here, between groups.

        Returns:
            The outcome of every group, keyed by (sink_type, sink_name).

        Raises:
            SinkDeliveryFailedError: a record could not be written and
                ``dlq.on_send_failure`` is ``stall``. Every other group still
                settles first.
        """
        groups = self._group_payloads(result)
        if not groups:
            return {}

        if message_context is not None and message_context.is_on_dlq:
            # An earlier result of this message wrote its DLQ record: the
            # message is done, so none of its deliveries start.
            for key in groups:
                self._note_not_started(key, SinkNotStartedReason.MESSAGE_DLQ, partition_id, message_context)
            return dict.fromkeys(groups, DeliveryOutcome.NOT_STARTED)

        # One settle signal per group of this result. A dependent awaits the
        # signals of its dependencies; the config load refuses a cycle, so
        # the awaits always end.
        loop = asyncio.get_running_loop()
        settled: dict[SinkKey, asyncio.Future[DeliveryOutcome]] = {key: loop.create_future() for key in groups}

        async def settle(key: SinkKey) -> None:
            outcome = DeliveryOutcome.FAILED
            try:
                outcome = await self._deliver_group(
                    key,
                    payloads=groups[key],
                    settled=settled,
                    on_delivery_error=on_delivery_error,
                    partition_id=partition_id,
                    message_context=message_context,
                    before_group_start=before_group_start,
                )
            finally:
                # Set on every exit, a raise and a cancellation included:
                # a dependent waiting on this group must never wait for ever.
                # Waiters shield the signal, so nothing else can settle it.
                settled[key].set_result(outcome)

        # ``return_exceptions=True`` lets every group settle — a bare gather
        # would abandon in-flight groups on the first raise. After all groups
        # finish we re-raise the FIRST exception (iteration order), mirroring
        # ``connect_all``.
        results = await asyncio.gather(*(settle(key) for key in groups), return_exceptions=True)
        for result_or_exc in results:
            if isinstance(result_or_exc, BaseException):
                raise result_or_exc
        return {key: future.result() for key, future in settled.items()}

    async def _deliver_group(
        self,
        key: SinkKey,
        *,
        payloads: list[BaseModel],
        settled: dict[SinkKey, asyncio.Future[DeliveryOutcome]],
        on_delivery_error: DeliveryErrorCallback,
        partition_id: int,
        message_context: MessageContext | None,
        before_group_start: GroupGate | None,
    ) -> DeliveryOutcome:
        """Wait for the group's dependencies in this result, then deliver it, or say why it did not start."""
        dependency_failed = False
        for dependency in self._dependencies.get(key, ()):
            dependency_settled = settled.get(dependency)
            if dependency_settled is None:
                # No payloads for the dependency in this result: satisfied.
                continue
            # Shielded: a waiter that is cancelled must not cancel the shared
            # signal that the other dependents of the same sink await.
            outcome = await asyncio.shield(dependency_settled)
            if outcome not in _SATISFIED:
                dependency_failed = True
                break
        if before_group_start is not None:
            # Before the not-started note too: a gate that stops the call
            # (a dependency it stopped settles as failed) must not leave a
            # dependent counted as failed by its dependency.
            await before_group_start(key)
        if dependency_failed:
            # A dependency that wrote the record, or did not start because the
            # message is on the DLQ, ends the message; anything else (a window
            # failure, a lost record) is a failed dependency. The message's
            # state answers both, so a chain passes its reason down unchanged.
            reason = (
                SinkNotStartedReason.MESSAGE_DLQ
                if message_context is not None and message_context.is_on_dlq
                else SinkNotStartedReason.DEPENDENCY_FAILED
            )
            self._note_not_started(key, reason, partition_id, message_context)
            return DeliveryOutcome.NOT_STARTED
        if message_context is not None and message_context.is_on_dlq:
            # Another group of this result wrote the record while this one
            # waited for its dependencies.
            self._note_not_started(key, SinkNotStartedReason.MESSAGE_DLQ, partition_id, message_context)
            return DeliveryOutcome.NOT_STARTED
        return await self._deliver_to_sink(
            key,
            payloads=payloads,
            on_delivery_error=on_delivery_error,
            partition_id=partition_id,
            message_context=message_context,
        )

    def _note_not_started(
        self,
        key: SinkKey,
        reason: SinkNotStartedReason,
        partition_id: int,
        message_context: MessageContext | None,
    ) -> None:
        """Count, log and record one group that did not start.

        The row of an HTTP request carries its request id, as its
        ``message_dlq`` row does.
        """
        sink_deliveries_not_started.labels(sink_type=key[0], sink_name=key[1], reason=reason.value).inc()
        # Sync, not ``await logger.adebug``: the async variants pay for a
        # thread-pool hop before the level filter, and a message on the DLQ
        # makes this fire for every group of every later result.
        logger.debug('sink_not_started', category='sink', sink=sink_ref(key), reason=reason.value)
        if self._recorder is not None:
            self._recorder.record_sink_not_started(
                sink=sink_ref(key),
                reason=reason.value,
                partition=partition_id,
                request_id=message_context.request_id if message_context is not None else None,
            )

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
                # Sync, not ``await logger.adebug``: structlog's async
                # variants copy the context and hop through the default
                # thread pool BEFORE the level filter runs, so the cost is
                # paid even with debug logging off. This fires per transient
                # failure — exactly when the worker is already struggling.
                logger.debug(
                    'sink_idempotent_transient_retry',
                    category='sink',
                    sink_type=sink_type,
                    sink_name=sink_name,
                    attempt=attempt_idx + 1,
                    backoff=backoff,
                    error=redact_url(str(exc)),
                )
                await asyncio.sleep(backoff)

    async def _attempt_delivery(self, sink: BaseSink[Any], key: SinkKey, payloads: list[BaseModel]) -> None:
        """One hook-level attempt: ``sink.deliver`` with the quick transient retry, bounded by the delivery timeout.

        One budget for the whole attempt, transient retries included, so a
        partition's wait on a wedged sink is predictable rather than a
        multiple of the retry count. Without it a sink whose server stopped
        answering on a still-open TCP connection blocks its partition
        indefinitely: the breaker only counts a failure when a call returns,
        so /readyz stays green while the worker does nothing. A timeout
        cancels the sink coroutine:
        ``CancelledError`` is a ``BaseException``, so a sink's own ``except
        Exception`` fallback (the Postgres per-row retry) does not fire on it
        and the "failed batch wrote nothing" premise is preserved.
        """
        sink_type, sink_name = key
        timeout = self._policies[key].delivery_timeout
        try:
            await asyncio.wait_for(
                self._deliver_with_transient_retry(
                    sink=sink,
                    sink_type=sink_type,
                    sink_name=sink_name,
                    payloads=payloads,
                ),
                timeout=timeout,
            )
        except TimeoutError as exc:
            # ``str(TimeoutError())`` is the empty string, and this error is
            # what reaches the handler, the DLQ record and the log — say what
            # actually happened. Same class, so it stays classified transient.
            raise TimeoutError(
                f'{sink_type} sink {sink_name!r} did not finish delivering {len(payloads)} payload(s) within {timeout}s'
            ) from exc

    async def _deliver_to_sink(
        self,
        key: SinkKey,
        *,
        payloads: list[BaseModel],
        on_delivery_error: DeliveryErrorCallback,
        partition_id: int,
        message_context: MessageContext | None,
    ) -> DeliveryOutcome:
        """Deliver one sink's payload group with its retry budget, its declared policy and the circuit breaker.

        Retry budget:
            The hook is asked after every failed hook-level attempt, with the
            attempt (from 1) and the retries left of the sink's
            ``max_retries``. ``RETRY`` spends one; ``RETRY`` with none left
            is the declared ``on_failure``, logged
            ``sink_retry_budget_spent``. A hook that raises is logged
            ``delivery_error_hook_failed`` and the declared policy applies.

            The quick transient retry of an idempotent batch
            (``_deliver_with_transient_retry``) stays inside one hook-level
            attempt and outside this budget: it smooths a blipped packet
            before the handler is bothered, so it neither calls the hook nor
            counts as an attempt, and the breaker sees one failure for it.

        Circuit breaker:
            When the breaker is open the declared ``on_failure`` decides
            without the hook — an open breaker is an infrastructure signal,
            not a per-message decision: ``skip`` drops and counts, ``dlq``
            writes the record with ``dk-attempts: 0``. On terminal outcomes
            (success, or a failure that is not a skip) the matching
            ``record_*`` method lets the breaker count consecutive failures.
        """
        sink = self._sinks[key]
        stats = self._stats[key]
        policy = self._policies[key]
        declared = DeliveryAction(policy.on_failure)
        sink_type, sink_name = key

        # Circuit breaker gate. The breaker check itself is cheap (a
        # time.monotonic + integer compare) so running it before every batch
        # is fine. We surface the skip as an error_count tick + sentinel
        # last_error so the debug UI and stats endpoints can explain why the
        # sink received nothing.
        if sink.should_skip_delivery():
            stats.error_count += 1
            stats.last_error = CIRCUIT_OPEN_ERROR
            stats.last_error_ts = time.time()
            logger.warning(
                'sink_delivery_circuit_open',
                category='sink',
                sink_type=sink_type,
                sink_name=sink_name,
                payload_count=len(payloads),
                on_failure=policy.on_failure,
            )
            return await self._end_failed_delivery(
                key,
                action=declared,
                error_text=CIRCUIT_OPEN_ERROR,
                attempts=0,
                payload_count=len(payloads),
                partition_id=partition_id,
                message_context=message_context,
            )

        # Track whether THIS invocation claimed the half-open probe slot.
        # ``should_skip_delivery`` above returned False, so if the sink is
        # now in ``half_open`` state with ``probe_inflight=True``, it was
        # this caller that claimed the probe slot. We MUST release that slot
        # before returning, via either ``record_success`` /
        # ``record_failure`` (terminal outcomes that also drive
        # circuit-state transitions), or ``release_probe_inflight`` (neutral
        # release for SKIP, which does neither). Without the try/finally
        # below, a SKIP or a raise on the DLQ path would leak the probe flag
        # and permanently wedge the circuit in ``half_open + inflight=True``.
        probe_claimed = sink.circuit_state == 'half_open' and sink.probe_inflight
        try:
            attempt = 0
            while True:
                start = time.monotonic()
                try:
                    await self._attempt_delivery(sink, key, payloads)
                except Exception as e:
                    attempt += 1
                    stats.error_count += 1
                    # Sink exceptions can carry secrets (DSNs with passwords),
                    # so every string that lands in last_error / recorder /
                    # DeliveryError goes through ``redact_url`` first.
                    safe_error = redact_url(str(e))
                    stats.last_error = safe_error
                    stats.last_error_ts = time.time()
                    retries_left = policy.max_retries + 1 - attempt
                    error = DeliveryError(
                        sink_name=sink_name,
                        sink_type=sink_type,
                        error=safe_error,
                        payloads=payloads,
                        attempt=attempt,
                        retries_left=retries_left,
                        declared_policy=declared,
                    )
                    action = await self._ask_hook(on_delivery_error, error, key)
                    if action == DeliveryAction.RETRY and retries_left > 0:
                        # The retry re-delivers the WHOLE original group, not
                        # the unapplied tail. Partial application before the
                        # raise is normal — the HTTP sink POSTs sequentially,
                        # the Kafka sink has acks for some messages, Postgres
                        # and Mongo commit earlier adjacent runs in autocommit
                        # — so payloads that already landed land again. That
                        # is deliberate: the framework is at-least-once end to
                        # end (a revoke or a crash replays the whole window),
                        # so a RETRY that tried to be exactly-once would
                        # promise something the layer above cannot keep. See
                        # docs/sinks.md. The log says it explicitly so an
                        # operator choosing RETRY for a non-idempotent sink can
                        # see the cost.
                        self._record_sink_error(key, safe_error, attempt=attempt, action=DeliveryAction.RETRY.value)
                        stats.retry_count += 1
                        sink_delivery_retries.labels(sink_type=sink_type, sink_name=sink_name).inc()
                        logger.warning(
                            'sink_delivery_retry',
                            category='sink',
                            sink_type=sink_type,
                            sink_name=sink_name,
                            attempt=attempt,
                            retries_left=retries_left,
                            payload_count=len(payloads),
                            note='whole group re-delivered; payloads already applied are applied again',
                        )
                        continue
                    if action == DeliveryAction.RETRY:
                        await self._rate_limited_log.emit(
                            f'sink_retry_budget_spent:{sink_ref(key)}',
                            logger.awarning,
                            'sink_retry_budget_spent',
                            category='sink',
                            sink=sink_ref(key),
                            attempts=attempt,
                            max_retries=policy.max_retries,
                            on_failure=policy.on_failure,
                        )
                        action = declared
                    if action != DeliveryAction.SKIP:
                        # A terminal failure — tell the breaker so consecutive
                        # failures accumulate toward the trip threshold. SKIP
                        # is operator intent, not an infrastructure failure,
                        # so it does not count; the finally below releases
                        # the probe slot instead.
                        sink.record_failure()
                    return await self._end_failed_delivery(
                        key,
                        action=action,
                        error_text=safe_error,
                        attempts=attempt,
                        payload_count=len(payloads),
                        partition_id=partition_id,
                        message_context=message_context,
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
                if message_context is not None:
                    for state in message_context.states:
                        state.delivered_sinks.add(sink_ref(key))
                return DeliveryOutcome.DELIVERED
        finally:
            # Probe-slot leak guard. ``record_success`` / ``record_failure``
            # clear ``probe_inflight`` on terminal outcomes, so this is a
            # no-op for the normal paths. It only fires when we exit via SKIP
            # or a raise that skipped both recorder helpers.
            if probe_claimed and sink.probe_inflight:
                sink.release_probe_inflight()

    async def _ask_hook(
        self, on_delivery_error: DeliveryErrorCallback, error: DeliveryError, key: SinkKey
    ) -> DeliveryAction:
        """The hook's answer for one failed attempt; the declared policy when the hook raises."""
        try:
            return await on_delivery_error(error)
        except Exception as exc:
            await logger.aerror(
                'delivery_error_hook_failed',
                category='sink',
                sink=sink_ref(key),
                attempt=error.attempt,
                on_failure=error.declared_policy.value,
                error=redact_url(str(exc)),
                exc_info=exc,
            )
            return error.declared_policy

    def _record_sink_error(self, key: SinkKey, error: str, *, attempt: int, action: str) -> None:
        if self._recorder is not None:
            self._recorder.record_sink_error(
                sink_type=key[0],
                sink_name=key[1],
                error=error,
                attempt=attempt,
                action=action,
            )

    async def _end_failed_delivery(
        self,
        key: SinkKey,
        *,
        action: DeliveryAction,
        error_text: str,
        attempts: int,
        payload_count: int,
        partition_id: int,
        message_context: MessageContext | None,
    ) -> DeliveryOutcome:
        """End a failed delivery by ``action`` and record ``sink_error`` with what really came of it.

        The recorded action is the outcome, not the request: ``dlq`` only
        when a record exists, ``skip`` for a skip and for a window result
        (where ``dlq`` acts as ``skip``), and ``failed`` when no record could
        be written (a lost write, no context, a stall).
        """
        outcome = DeliveryOutcome.FAILED
        try:
            outcome = await self._end_delivery(
                key,
                action=action,
                error_text=error_text,
                attempts=attempts,
                payload_count=payload_count,
                partition_id=partition_id,
                message_context=message_context,
            )
            return outcome
        finally:
            self._record_sink_error(
                key, error_text, attempt=attempts, action=_recorded_action(outcome, message_context)
            )

    async def _end_delivery(
        self,
        key: SinkKey,
        *,
        action: DeliveryAction,
        error_text: str,
        attempts: int,
        payload_count: int,
        partition_id: int,
        message_context: MessageContext | None,
    ) -> DeliveryOutcome:
        """Apply the final ``SKIP`` or ``DLQ`` of a failed delivery."""
        sink_type, sink_name = key
        if action == DeliveryAction.SKIP:
            sink_deliveries_skipped.labels(sink_type=sink_type, sink_name=sink_name).inc()
            await self._rate_limited_log.emit(
                f'sink_delivery_skipped:{sink_ref(key)}',
                logger.awarning,
                'sink_delivery_skipped',
                category='sink',
                sink_type=sink_type,
                sink_name=sink_name,
                payload_count=payload_count,
                attempts=attempts,
                error=error_text,
            )
            return DeliveryOutcome.SKIPPED
        if message_context is not None and message_context.is_window:
            # A window result is delivered after its messages completed their
            # offsets: there is no source message to send to the DLQ. The
            # payloads are dropped as a skip would drop them, but the outcome
            # is a failure, so the sinks that depend on this one do not start.
            sink_deliveries_skipped.labels(sink_type=sink_type, sink_name=sink_name).inc()
            await self._rate_limited_log.emit(
                f'window_delivery_not_dlq:{sink_ref(key)}',
                logger.awarning,
                'window_delivery_not_dlq',
                category='sink',
                sink=sink_ref(key),
                payload_count=payload_count,
                error=error_text,
                note='a window result has no source message to send to the DLQ',
            )
            return DeliveryOutcome.FAILED
        return await self._send_message_to_dlq(
            key,
            error_text=error_text,
            attempts=attempts,
            payload_count=payload_count,
            partition_id=partition_id,
            message_context=message_context,
        )

    async def _send_message_to_dlq(
        self,
        key: SinkKey,
        *,
        error_text: str,
        attempts: int,
        payload_count: int,
        partition_id: int,
        message_context: MessageContext | None,
    ) -> DeliveryOutcome:
        """Write one DLQ record per source message of the result, unless the message already has one.

        The only place a sink failure writes the DLQ. Raises
        ``SinkDeliveryFailedError`` when a record could not be written and
        ``dlq.on_send_failure`` is ``stall``.
        """
        if self._dlq_sink is None and self._dlq_unconfigured:
            # The DLQ has no destination by configuration, it is not broken:
            # stalling a partition over it would wedge a worker that never
            # asked for a DLQ. A live Kafka source always gives the DLQ a
            # topic, so this guards the HTTP-only worker.
            await self.drop_unconfigured_dlq_send(partition_id, payload_count)
            return DeliveryOutcome.FAILED
        if message_context is None:
            # The caller named no source message, so there is nothing to
            # write. Failing closed: the strategy for a lost record applies,
            # never a silent skip.
            self._lose_messages(key, message_count=1, partition_id=partition_id, reason='no message context')
            return DeliveryOutcome.FAILED

        written, lost, already_written = await self._write_records(
            message_context,
            partition_id=partition_id,
            build=lambda message, state: _message_record(
                message,
                request_id=message_context.request_id,
                stage='sink',
                error=error_text,
                attempts=attempts,
                delivered_sinks=state.delivered_sinks,
                sink=sink_ref(key),
            ),
        )
        if lost:
            self._lose_messages(key, message_count=lost, partition_id=partition_id, reason=self._lost_reason())
        if written:
            return DeliveryOutcome.DLQ
        if already_written and not lost:
            # A message of the result was already on the DLQ: a group that
            # was in flight when an earlier failure wrote the record.
            logger.info(
                'message_already_dlq',
                category='sink',
                stage='sink',
                sink=sink_ref(key),
                partition=partition_id,
            )
            return DeliveryOutcome.DLQ
        # No record now and none before: the loss was counted, now or earlier.
        return DeliveryOutcome.FAILED

    async def send_parse_dlq(self, record: DLQRecord, partition_id: int) -> bool:
        """Write the ``parse`` record of a message whose value could not be parsed.

        Returns whether the write was confirmed; the caller applies
        ``dlq.on_send_failure`` when it was not. A written record is logged
        and recorded as ``message_dlq``, as the records of the other two
        stages are.
        """
        if self._dlq_sink is None or not await self._dlq_sink.send(record, partition_id=partition_id):
            return False
        self._note_message_dlq(record)
        return True

    async def send_execute_dlq(
        self,
        message_context: MessageContext | None,
        *,
        task: ExecutorTask,
        error: ExecutorError,
        attempts: int,
        partition_id: int,
    ) -> None:
        """Write the ``execute`` record of each source message of a task that ``on_error`` sent to the DLQ.

        ``attempts`` is the number of runs the task had. A message already on
        the DLQ gets no second record; that is only logged. Raises
        ``SinkDeliveryFailedError`` when a record could not be written and
        ``dlq.on_send_failure`` is ``stall``.
        """
        # Not a sink: the label of the lost-record line and of the stall error.
        subject = ('executor', task.task_id)
        if self._dlq_sink is None and self._dlq_unconfigured:
            message_count = len(message_context.source_messages) if message_context is not None else 1
            await self.drop_unconfigured_dlq_send(partition_id, message_count)
            return
        if message_context is None:
            self._lose_messages(subject, message_count=1, partition_id=partition_id, reason='no message context')
            return
        error_text = execute_error_text(error)
        written, lost, already_written = await self._write_records(
            message_context,
            partition_id=partition_id,
            build=lambda message, state: _message_record(
                message,
                request_id=message_context.request_id,
                stage='execute',
                error=error_text,
                attempts=attempts,
                delivered_sinks=state.delivered_sinks,
                task_id=task.task_id,
            ),
        )
        if lost:
            self._lose_messages(subject, message_count=lost, partition_id=partition_id, reason=self._lost_reason())
        if already_written and not written and not lost:
            # A sink failure or an earlier task of the message wrote its record.
            logger.info(
                'message_already_dlq',
                category='sink',
                stage='execute',
                task_id=task.task_id,
                partition=partition_id,
            )

    async def _write_records(
        self,
        message_context: MessageContext,
        *,
        partition_id: int,
        build: Callable[[SourceMessage, MessageDLQState], DLQRecord],
    ) -> tuple[int, int, int]:
        """Write the record ``build`` makes for each source message not yet on the DLQ or lost.

        Returns how many records were written, how many could not be, and
        how many messages were already on the DLQ.
        """
        written = 0
        lost = 0
        already_written = 0
        for message, state in message_context.pairs():
            # The flag is read and set under the message's own lock, with the
            # write in between: two results of one message (or two groups of
            # one result) can fail at the same moment, and only the first to
            # take the lock writes the record; the other then sees
            # ``dlq_written`` and writes none. A write that fails leaves the
            # flag clear, so the next failure of the message tries again.
            async with state.lock:
                if state.dlq_written:
                    already_written += 1
                    continue
                if state.dlq_lost:
                    # Its loss is already counted and logged.
                    continue
                record = build(message, state)
                if self._dlq_sink is not None and await self._dlq_sink.send(record, partition_id=partition_id):
                    state.dlq_written = True
                    written += 1
                    self._note_message_dlq(record)
                else:
                    lost += 1
                    # Under stall the offset stays pending and the message
                    # comes back; under drop it is gone, and counted once.
                    state.dlq_lost = self._dlq_on_send_failure == 'drop'
        return written, lost, already_written

    def _lost_reason(self) -> str:
        """Why a record could not be written."""
        return 'DLQ send failed' if self._dlq_sink is not None else 'no DLQ sink is configured'

    def _note_message_dlq(self, record: DLQRecord) -> None:
        """Log and record one source message written to the DLQ."""
        error = cut_error(record.error)
        logger.warning(
            'message_dlq',
            category='sink',
            stage=record.stage,
            sink=record.sink,
            error=error,
            source_topic=record.source_topic,
            partition=record.source_partition,
            offset=record.source_offset,
            request_id=record.request_id,
            delivered_sinks=record.delivered_sinks,
        )
        if self._recorder is not None:
            self._recorder.record_message_dlq(
                partition=record.source_partition,
                offset=record.source_offset,
                request_id=record.request_id,
                stage=record.stage,
                sink=record.sink,
                error=error,
                delivered_sinks=record.delivered_sinks,
                attempts=record.attempts,
            )

    def _lose_messages(self, key: SinkKey, *, message_count: int, partition_id: int, reason: str) -> None:
        """Apply ``dlq.on_send_failure`` to source messages whose record could not be written."""
        sink_type, sink_name = key
        if self._dlq_on_send_failure == 'stall':
            raise SinkDeliveryFailedError(sink_name=sink_name, sink_type=sink_type, reason=reason)
        dlq_dropped_payloads.labels(partition=str(partition_id)).inc(message_count)
        logger.critical(
            'dlq_failure_payloads_dropped',
            category='sink',
            sink_name=sink_name,
            sink_type=sink_type,
            partition=partition_id,
            message_count=message_count,
            reason=reason,
            action='ALERT: source messages lost (dlq.on_send_failure=drop) — '
            'set dlq.on_send_failure=stall to prefer replay over loss',
        )

    async def drop_unconfigured_dlq_send(self, partition_id: int, payload_count: int) -> None:
        """Count and log a DLQ send that has nowhere to go because the DLQ has no destination.

        Only the first drop warns: a worker running without a DLQ on purpose
        would otherwise repeat the warning for every failed delivery, and the
        counter already carries the volume.
        """
        dlq_unconfigured_drops.inc(payload_count)
        log = logger.adebug if self._dlq_drop_warned else logger.awarning
        self._dlq_drop_warned = True
        await log(
            'dlq_send_dropped_unconfigured',
            category='sink',
            partition=partition_id,
            payload_count=payload_count,
            hint='set dlq.topic to enable the DLQ on a worker without the Kafka source',
        )


def _recorded_action(outcome: DeliveryOutcome, message_context: MessageContext | None) -> str:
    """The ``action`` of a terminal ``sink_error`` row for ``outcome``."""
    if outcome == DeliveryOutcome.SKIPPED:
        return 'skip'
    if outcome == DeliveryOutcome.DLQ:
        return 'dlq'
    if message_context is not None and message_context.is_window:
        return 'skip'
    return 'failed'


def execute_error_text(error: ExecutorError) -> str:
    """The ``dk-error`` text of a failed task: its exit code, when it has one, then stderr or the exception."""
    detail = error.stderr or error.exception or error.kind
    if error.exit_code is None:
        return detail
    return f'exit code {error.exit_code}: {detail}'


def _message_record(
    message: SourceMessage,
    *,
    request_id: str | None,
    stage: Literal['execute', 'sink'],
    error: str,
    attempts: int,
    delivered_sinks: set[str],
    sink: str | None = None,
    task_id: str | None = None,
) -> DLQRecord:
    """The record of one source message; an HTTP request (``request_id`` set) has no key or source."""
    is_request = request_id is not None
    return DLQRecord(
        key=None if is_request else message.key,
        value=message.value,
        source_headers=[] if is_request else list(message.headers),
        stage=stage,
        error=error,
        sink=sink,
        task_id=task_id,
        attempts=attempts,
        delivered_sinks=sorted(delivered_sinks),
        source_topic=None if is_request else message.topic,
        source_partition=None if is_request else message.partition,
        source_offset=None if is_request else message.offset,
        source_timestamp_ms=message.timestamp,
        request_id=request_id,
    )
