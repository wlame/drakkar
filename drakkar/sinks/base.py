"""Base sink interface for Drakkar output destinations.

All sink implementations (Kafka, Postgres, MongoDB, HTTP, Redis, Filesystem)
extend BaseSink and implement connect(), deliver(), and close().
"""

from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from pydantic import BaseModel

PayloadT = TypeVar('PayloadT', bound=BaseModel)


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
    """

    sink_type: str = ''
    """Identifier for this sink type (e.g., 'kafka', 'postgres', 'mongo')."""

    def __init__(self, name: str, ui_url: str = '') -> None:
        self._name = name
        self._ui_url = ui_url

    @property
    def name(self) -> str:
        """The user-defined instance name from config (e.g., 'results', 'main-db')."""
        return self._name

    @property
    def ui_url(self) -> str:
        """Optional URL to a web UI for this sink's backing service."""
        return self._ui_url

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
