"""Base sink interface for Drakkar output destinations.

All sink implementations (Kafka, Postgres, MongoDB, HTTP, Redis, Filesystem)
extend BaseSink and implement connect(), deliver(), and close().
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Generic, Literal, TypeVar

from pydantic import BaseModel

from drakkar.metrics import sink_circuit_open, sink_circuit_trips

if TYPE_CHECKING:
    from drakkar.config import CircuitBreakerConfig

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
        The `_record_*` / `_should_skip_delivery` helpers implement a per-sink
        circuit breaker. The SinkManager drives the state transitions — a sink
        subclass never calls these directly. The breaker config is wired in by
        `set_circuit_config` (called from SinkManager.register at startup) or
        falls back to the default `CircuitBreakerConfig()` when no manager is
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
        # set_circuit_config overrides the default with operator-configured
        # thresholds when the manager registers the sink.
        self._consecutive_failures: int = 0
        self._circuit_state: CircuitState = 'closed'
        self._circuit_opened_at: float | None = None
        # Lazy import to avoid a cycle: config.py itself may import sink
        # classes in other contexts (e.g. test fixtures).
        from drakkar.config import CircuitBreakerConfig

        self._circuit_config: CircuitBreakerConfig = CircuitBreakerConfig()
        sink_circuit_open.labels(sink_type=self.sink_type, sink_name=self._name).set(0.0)

    @property
    def name(self) -> str:
        """The user-defined instance name from config (e.g., 'results', 'main-db')."""
        return self._name

    @property
    def ui_url(self) -> str:
        """Optional URL to a web UI for this sink's backing service."""
        return self._ui_url

    @property
    def circuit_state(self) -> CircuitState:
        """Current circuit breaker state — exposed for tests and the debug UI."""
        return self._circuit_state

    def set_circuit_config(self, config: CircuitBreakerConfig) -> None:
        """Install the circuit breaker config from the SinkManager.

        Called by SinkManager.register at startup. Separated from __init__
        so sinks can be constructed independently (in tests, in user code)
        without threading the config through every sink constructor.
        """
        self._circuit_config = config

    def _record_failure(self) -> None:
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
            # probe (see _should_skip_delivery).
            self._circuit_state = 'open'
            self._circuit_opened_at = time.monotonic()
            sink_circuit_open.labels(sink_type=self.sink_type, sink_name=self._name).set(1.0)
            sink_circuit_trips.labels(sink_type=self.sink_type, sink_name=self._name).inc()

    def _record_success(self) -> None:
        """Update circuit breaker state after a successful delivery.

        Transitions:
          closed    + success → closed (reset consecutive failures)
          half_open + success → closed (close the circuit, reset state)
          open      + success — caller shouldn't reach here (we'd have skipped)
        """
        self._consecutive_failures = 0
        if self._circuit_state == 'half_open':
            self._circuit_state = 'closed'
            self._circuit_opened_at = None
            sink_circuit_open.labels(sink_type=self.sink_type, sink_name=self._name).set(0.0)

    def _should_skip_delivery(self) -> bool:
        """Return True when the SinkManager should bypass `deliver()` entirely.

        - closed:    never skip.
        - open:      skip unless cooldown has elapsed. When it has, transition
                     to half_open (gauge → 0.5) and allow the next delivery
                     through as a probe.
        - half_open: a probe is already in flight (the one we just allowed
                     through); don't skip, let the caller complete it. The
                     result (success or failure) will drive the next
                     transition via _record_success / _record_failure.
        """
        if self._circuit_state == 'closed':
            return False

        if self._circuit_state == 'half_open':
            # Probe in flight — don't skip, wait for the outcome.
            return False

        # state == 'open'
        assert self._circuit_opened_at is not None
        elapsed = time.monotonic() - self._circuit_opened_at
        if elapsed >= self._circuit_config.cooldown_seconds:
            # Cooldown elapsed — promote to half_open. The next delivery
            # (this one) is the probe; success closes, failure reopens.
            self._circuit_state = 'half_open'
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
