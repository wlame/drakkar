"""The registry provider interface.

A provider stores worker cards and lists them back with their age, and with
its own liveness verdict when its service keeps one. It knows nothing about
loops, timeouts, retries or validation: the registry service owns those.
Adding a provider never touches the features that use the registry.

A provider is a class with the five async methods of :class:`RegistryProvider`
plus a factory ``(options) -> provider``; ``BUILTIN_PROVIDERS`` maps each
provider name to its factory.
"""

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from drakkar.providers.connections import Connections


@dataclass(frozen=True)
class PeerCard:
    """One listed card, its age on the provider's own clock, and the provider's liveness verdict.

    ``age_seconds`` is ``None`` when the provider cannot tell (a fixed peer
    list, or a file folder before this worker's first write gave it a
    server time sample).

    ``online`` is ``None`` unless the provider's service decides liveness
    itself (for example a card that exists only while its owner's session
    lives). When it is set, the registry service uses it instead of judging
    ``age_seconds`` against ``autodiscover.offline_after``.
    """

    card: dict[str, Any]
    age_seconds: float | None
    online: bool | None = None


# The return type of ``RegistryProvider.list``. A module-level alias, because
# inside the class body the name ``list`` is the method, not the builtin.
PeerCardList = list[PeerCard]


@dataclass(frozen=True)
class RegistryContext:
    """What a provider learns about the worker that runs it, at ``start``.

    ``options`` is the provider's own settings mapping: the same mapping its
    factory received.

    ``connections`` resolves the worker's named shared connections
    (``connections.<kind>.<name>``); a provider that needs none ignores it.
    It is ``None`` when the registry runs outside a worker, as in a test.
    """

    worker: str
    instance: str
    cluster: str
    logger: Any
    options: Mapping[str, Any]
    connections: Connections | None = None


class RegistryProvider(Protocol):
    """Storage of worker cards. Every method may raise; the service counts and logs the failure.

    A provider is single-use: the service never calls ``start`` after
    ``stop``. The built-in ``filesystem`` provider raises on such a call, so
    a misuse shows at once instead of leaving a provider with no thread.
    """

    async def start(self, ctx: RegistryContext) -> None:
        """Prepare the provider, before any other method.

        A ``start`` that raises or times out is called again until one
        succeeds: at the next heartbeat, at once when the worker's state
        changes, and once per breaker cooldown while the breaker is open. A
        failed ``start`` must therefore leave nothing behind that a second
        call would trip over.
        """
        ...

    async def publish(self, card: dict[str, Any]) -> None:
        """Store ``card``, replacing any card stored under the same worker name."""
        ...

    async def list(self) -> PeerCardList:
        """Return every card the provider can read, the caller's own included."""
        ...

    async def withdraw(self, worker: str, instance: str) -> None:
        """Delete the card of ``worker``, but only while it still holds ``instance``."""
        ...

    async def stop(self) -> None:
        """Release what ``start`` took. Called at most once, last.

        It is not called when no ``start`` succeeded, while the breaker is
        open, or when the shutdown time is spent. A ``start`` that the
        worker's shutdown cancels mid-attempt counts as not succeeded, so
        whatever it had half-opened is never released here. Open clients
        lazily on first use, or make sure a missing ``stop`` leaks nothing
        that outlives the process.
        """
        ...


RegistryFactory = Callable[[Mapping[str, Any]], RegistryProvider]
