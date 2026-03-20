"""Base sink interface for Drakkar output destinations.

All sink implementations (Kafka, Postgres, MongoDB, HTTP, Redis, Filesystem)
extend BaseSink and implement connect(), deliver(), and close().
"""

from abc import ABC, abstractmethod

from pydantic import BaseModel


class BaseSink(ABC):
    """Abstract base class for all sink implementations.

    A sink receives payloads from the framework after collect() or
    on_window_complete() and delivers them to an external system.

    Lifecycle:
        1. __init__(name, config) — store configuration
        2. connect() — establish connections (called once at startup)
        3. deliver(payloads) — called for each batch of payloads (many times)
        4. close() — release resources (called once at shutdown)
    """

    sink_type: str = ''
    """Identifier for this sink type (e.g., 'kafka', 'postgres', 'mongo')."""

    def __init__(self, name: str) -> None:
        self._name = name

    @property
    def name(self) -> str:
        """The user-defined instance name from config (e.g., 'results', 'main-db')."""
        return self._name

    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to the external system.

        Called once during worker startup. Should raise on failure
        so the worker crashes fast with a clear error.
        """

    @abstractmethod
    async def deliver(self, payloads: list[BaseModel]) -> None:
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
