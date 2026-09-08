"""The input-source contract shared by the built-in Kafka and HTTP sources.

A source owns one way of feeding work into the pipeline. The lifecycle
starts every enabled source, runs them until a stop is requested, drains
them against one deadline, and stops them. Everything a source needs from
the worker arrives through :class:`SourceContext`, never the app object.

Internal in this release: neither class is exported from ``drakkar`` and
custom sources are not a public API yet (see docs/sources.md).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from drakkar.config import DrakkarConfig
    from drakkar.executor import ExecutorPool
    from drakkar.handler import BaseDrakkarHandler
    from drakkar.models import CollectResult
    from drakkar.recorder import EventRecorder
    from drakkar.sinks.dlq import DLQSink
    from drakkar.sinks.manager import SinkManager


@dataclass
class SourceContext:
    """What a source may use. Built by the lifecycle after sinks connect."""

    config: DrakkarConfig
    handler: BaseDrakkarHandler
    executor_pool: ExecutorPool
    sink_manager: SinkManager
    dlq_sink: DLQSink | None
    recorder: EventRecorder | None
    throughput: Any
    worker_id: str
    cluster_name: str
    # Deliver a CollectResult to the sinks for the given partition (-1 for HTTP).
    on_collect: Callable[[CollectResult, int], Awaitable[None]]
    # Composed worker readiness (all sources ready, not stopping); the HTTP gate reads it.
    is_worker_ready: Callable[[], bool]


class Source(Protocol):
    """One input source. See the module docstring for the lifecycle contract."""

    name: str

    def bind(self, ctx: SourceContext) -> None:
        """Receive the context. Called once, before ``start``."""
        ...

    async def start(self) -> None:
        """Acquire resources and begin accepting input. Raising is fatal for the worker."""
        ...

    async def run(self) -> None:
        """Block until ``signal_stop``; raise on a fatal error."""
        ...

    @property
    def is_ready(self) -> bool:
        """Whether this source is serving input."""
        ...

    def signal_stop(self) -> None:
        """Stop accepting new input; in-flight work continues."""
        ...

    async def drain(self, deadline: float) -> bool:
        """Wait for in-flight work until ``deadline`` (monotonic). True when everything finished."""
        ...

    async def stop(self, deadline: float) -> None:
        """Release resources, bounded by ``deadline``."""
        ...

    def snapshot(self) -> dict[str, Any]:
        """Source state for worker_state rows and the UI."""
        ...
