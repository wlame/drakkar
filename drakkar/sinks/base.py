"""Base sink interface for Drakkar output destinations.

All sink implementations (Kafka, Postgres, MongoDB, HTTP, Redis, Filesystem)
extend BaseSink and implement connect(), deliver(), and close().
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from typing import Generic, Literal, TypeVar

from pydantic import BaseModel

from drakkar.config import CircuitBreakerConfig
from drakkar.metrics import sink_circuit_open, sink_circuit_trips

PayloadT = TypeVar('PayloadT', bound=BaseModel)

# Discrete circuit breaker states. `closed` is normal operation,
# `open` is a tripped circuit skipping all deliveries during cooldown,
# `half_open` is a single probe delivery in flight after cooldown
# elapsed — success closes the circuit, failure reopens with a
# renewed cooldown.
CircuitState = Literal['closed', 'open', 'half_open']


class BaseSink(ABC, Generic[PayloadT]):
    """Abstract base class for all sink implementations.

    A sink receives payloads from the framework after on_task_complete(),
    on_message_complete(), or on_window_complete() and delivers them to
    an external system.

    Lifecycle:
        1. __init__(name, config) — store configuration
        2. connect() — establish connections (called once at startup)
        3. deliver(payloads) — called for each batch of payloads (many times)
        4. close() — release resources (called once at shutdown)

    Circuit breaker:
        The ``record_success`` / ``record_failure`` / ``should_skip_delivery``
        / ``release_probe_inflight`` methods and the ``circuit_state``
        property implement a per-sink circuit breaker. The SinkManager drives
        the state transitions — a sink subclass never calls these directly.
        The breaker config is wired in by the SinkManager via
        ``configure_circuit_breaker`` when it registers the sink, or falls
        back to the default ``CircuitBreakerConfig()`` when no manager is
        involved (tests, standalone sink usage).
    """

    sink_type: str = ''
    """Identifier for this sink type (e.g., 'kafka', 'postgres', 'mongo')."""

    def __init__(self, name: str, ui_url: str = '') -> None:
        self._name = name
        self._ui_url = ui_url

        # Circuit breaker state. Starts closed (0.0 on the gauge) — the
        # per-sink gauge is initialized eagerly so a freshly-registered sink
        # appears in Prometheus scrape output as "closed" rather than absent.
        # SinkManager calls ``configure_circuit_breaker`` with operator-
        # configured thresholds when the manager registers the sink.
        self._consecutive_failures: int = 0
        self._circuit_state: CircuitState = 'closed'
        self._circuit_opened_at: float | None = None
        # Tracks whether a half-open probe delivery is already in flight.
        # With parallel deliver_all (asyncio.gather across partitions) two
        # concurrent invocations against the same sink could BOTH observe
        # ``half_open`` and BOTH let the probe through, slamming a recovering
        # downstream with many simultaneous requests. This flag forces a
        # single probe: the first caller marks it, subsequent callers see
        # ``half_open`` + inflight and skip.
        self._probe_inflight: bool = False
        self._circuit_config: CircuitBreakerConfig = CircuitBreakerConfig()
        sink_circuit_open.labels(sink_type=self.sink_type, sink_name=self._name).set(0.0)

    @property
    def circuit_state(self) -> CircuitState:
        """Read-only view of the current circuit-breaker state.

        Exposes ``closed`` / ``open`` / ``half_open`` to callers that need to
        reason about the breaker (SinkManager's probe-slot tracking, tests
        that assert transitions). Mutation goes through the ``record_*`` /
        ``should_skip_delivery`` / ``release_probe_inflight`` methods.
        """
        return self._circuit_state

    @property
    def probe_inflight(self) -> bool:
        """Read-only view of the half-open probe-inflight flag.

        Used by SinkManager's ``try/finally`` in ``_deliver_to_sink`` to
        detect whether THIS invocation claimed the probe slot (so it knows
        whether a neutral release is needed on non-terminal exits).
        """
        return self._probe_inflight

    def configure_circuit_breaker(self, config: CircuitBreakerConfig) -> None:
        """Replace the breaker's thresholds — called by SinkManager.register.

        Kept as an explicit method rather than a direct attribute assignment
        so every call site routes through a named public API. Sinks
        registered outside a manager keep the default ``CircuitBreakerConfig``
        wired in ``__init__``.
        """
        self._circuit_config = config

    @property
    def name(self) -> str:
        """The user-defined instance name from config (e.g., 'results', 'main-db')."""
        return self._name

    @property
    def ui_url(self) -> str:
        """Optional URL to a web UI for this sink's backing service."""
        return self._ui_url

    def record_failure(self) -> None:
        """Update circuit breaker state after a failed delivery attempt.

        Called by SinkManager after the retry budget on a sink is exhausted
        — a single failed retry does NOT count as a "consecutive failure"
        here; the manager only reports the terminal outcome of a delivery.

        Transitions:
          closed  + N+1th failure (where N+1 == threshold) → open (trip)
          half_open + failure                              → open (reopen with renewed cooldown)
          open       + failure — caller shouldn't reach here (we'd have skipped)
        """
        self._consecutive_failures += 1
        # Clear the probe-inflight flag on every terminal outcome —
        # ``should_skip_delivery`` can hand out the next probe once the
        # current one has finished (success or failure).
        self._probe_inflight = False

        if self._circuit_state == 'half_open':
            # Probe failed — snap back open with a fresh cooldown window.
            # The trip counter ticks again so operators can see flapping
            # circuits as a rate, not a single event.
            self._circuit_state = 'open'
            self._circuit_opened_at = time.monotonic()
            sink_circuit_open.labels(sink_type=self.sink_type, sink_name=self._name).set(1.0)
            sink_circuit_trips.labels(sink_type=self.sink_type, sink_name=self._name).inc()
            return

        if self._circuit_state == 'closed' and self._consecutive_failures >= self._circuit_config.failure_threshold:
            # Threshold hit — trip open. Cooldown timer starts now; the next
            # delivery attempt after cooldown elapses becomes a half-open
            # probe (see should_skip_delivery).
            self._circuit_state = 'open'
            self._circuit_opened_at = time.monotonic()
            sink_circuit_open.labels(sink_type=self.sink_type, sink_name=self._name).set(1.0)
            sink_circuit_trips.labels(sink_type=self.sink_type, sink_name=self._name).inc()

    def record_success(self) -> None:
        """Update circuit breaker state after a successful delivery.

        Transitions:
          closed    + success → closed (reset consecutive failures)
          half_open + success → closed (close the circuit, reset state)
          open      + success — caller shouldn't reach here (we'd have skipped)
        """
        self._consecutive_failures = 0
        # Clear probe-inflight on every terminal success so subsequent
        # ``should_skip_delivery`` checks promote cleanly to half_open on
        # the next cooldown elapse.
        self._probe_inflight = False
        if self._circuit_state == 'half_open':
            self._circuit_state = 'closed'
            self._circuit_opened_at = None
            sink_circuit_open.labels(sink_type=self.sink_type, sink_name=self._name).set(0.0)

    def release_probe_inflight(self) -> None:
        """Release the half-open probe slot without touching circuit state.

        The SinkManager normally clears ``_probe_inflight`` implicitly via
        ``record_success`` / ``record_failure`` on a terminal outcome.
        Some delivery paths end without either call — notably the
        ``DeliveryAction.SKIP`` branch (operator intent to drop the batch,
        NOT an infrastructure-health signal) and any rare case where
        ``on_delivery_error`` itself raises. Without a neutral release
        helper, those paths would leak the probe flag and leave the circuit
        stuck in half_open with ``_probe_inflight=True`` forever — every
        subsequent ``should_skip_delivery`` call would see the stale flag
        and skip, permanently wedging the sink.

        Idempotent by design: a stray call when no probe was in flight is
        harmless (it's already False). The caller is the delivery path's
        ``try/finally`` in ``SinkManager._deliver_to_sink``, which tracks
        whether this particular invocation claimed the slot.
        """
        self._probe_inflight = False

    def should_skip_delivery(self) -> bool:
        """Return True when the SinkManager should bypass `deliver()` entirely.

        - closed:    never skip.
        - open:      skip unless cooldown has elapsed. When it has, transition
                     to half_open (gauge → 0.5), claim the probe slot, and
                     allow this single delivery through as the probe.
        - half_open: a probe may already be in flight. If it is, skip (the
                     caller should route this batch to DLQ rather than issue
                     a concurrent probe). Otherwise claim the probe slot and
                     let this delivery through.

        Without the ``_probe_inflight`` gate, parallel delivery could slam a
        recovering downstream with many simultaneous probe requests, defeating
        the "single probe" intent of the half-open state. ``should_skip_delivery``
        is purely synchronous — asyncio cooperative scheduling only yields at
        ``await`` boundaries, so claim-the-slot and read-the-flag cannot
        interleave between callers.

        Callers that claim the probe slot here MUST eventually release it
        via ``record_success`` / ``record_failure`` (terminal outcomes)
        or ``release_probe_inflight`` (non-terminal exits like SKIP or a
        handler that itself raises). SinkManager's ``_deliver_to_sink``
        uses a ``try/finally`` to guarantee this.
        """
        if self._circuit_state == 'closed':
            return False

        if self._circuit_state == 'half_open':
            if self._probe_inflight:
                return True
            self._probe_inflight = True
            return False

        # state == 'open'
        # Using ``assert`` here would be stripped under ``python -O``, which
        # would leave the next line raising a confusing ``TypeError``
        # (``None - float``). Match the explicit-guard style used elsewhere
        # in the codebase (e.g. drakkar/cache.py) so the error message is
        # actionable under every interpreter mode.
        if self._circuit_opened_at is None:
            raise RuntimeError('circuit state is "open" but _circuit_opened_at is None — internal invariant broken')
        elapsed = time.monotonic() - self._circuit_opened_at
        if elapsed >= self._circuit_config.cooldown_seconds:
            # Cooldown elapsed — promote to half_open and claim the probe
            # slot in a single non-awaiting step.
            self._circuit_state = 'half_open'
            self._probe_inflight = True
            sink_circuit_open.labels(sink_type=self.sink_type, sink_name=self._name).set(0.5)
            return False

        return True

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to the external system.

        Called once during worker startup. Should raise on failure
        so the worker crashes fast with a clear error.
        """

    @abstractmethod
    async def deliver(self, payloads: list[PayloadT]) -> None:
        """Deliver a batch of payloads to the external system.

        Called by SinkManager for each group of payloads routed to this sink.
        Must raise on failure — the framework handles retries and DLQ.
        """

    @abstractmethod
    async def close(self) -> None:
        """Release connections and resources.

        Called once during worker shutdown. Should not raise — log
        errors instead so shutdown proceeds cleanly.
        """

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}(type={self.sink_type!r}, name={self._name!r})'
