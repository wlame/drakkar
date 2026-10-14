"""The registry provider interface.

A provider stores worker cards and lists them back with their age. It knows
nothing about loops, timeouts, liveness or validation: the registry service
owns those. Adding a provider never touches the features that use the
registry.

A third-party provider is a class with the five async methods of
:class:`RegistryProvider` plus a factory ``(options) -> provider`` that the
package exposes as an entry point of the ``drakkar.registry_providers``
group. ``drakkar.fleet.testkit`` holds the checks a provider should pass.
"""

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True)
class PeerCard:
    """One listed card and its age on the provider's own clock.

    ``age_seconds`` is ``None`` when the provider cannot tell (a fixed peer
    list, or a file folder before this worker's first write gave it a
    server time sample).
    """

    card: dict[str, Any]
    age_seconds: float | None


# The return type of ``RegistryProvider.list``. A module-level alias, because
# inside the class body the name ``list`` is the method, not the builtin.
PeerCardList = list[PeerCard]


@dataclass(frozen=True)
class RegistryContext:
    """What a provider learns about the worker that runs it, at ``start``.

    ``options`` is the provider's own settings mapping: the same mapping its
    factory received (for a plugin, ``fleet.registry.plugin_options``).
    """

    worker: str
    instance: str
    cluster: str
    logger: Any
    options: Mapping[str, Any]


class RegistryProvider(Protocol):
    """Storage of worker cards. Every method may raise; the service counts and logs the failure."""

    async def start(self, ctx: RegistryContext) -> None:
        """Prepare the provider. Called once, before any other method."""
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
        """Release what ``start`` took. Called once, last."""
        ...


RegistryFactory = Callable[[Mapping[str, Any]], RegistryProvider]
