"""In-process registry providers for the fleet tests.

``MemoryRegistry`` stores cards in a dict shared by every provider built on
the same :class:`MemoryStore`, with ages on a clock the test moves. It is a
reference implementation of the provider interface and runs through the
whole test kit, so the kit itself is tested against a provider that is not
a built-in.
"""

from typing import Any

from drakkar.fleet.card import sanitize_worker_name
from drakkar.fleet.provider import PeerCard, PeerCardList, RegistryContext


class MemoryStore:
    """Cards keyed by sanitized worker name, each with the clock time it was written."""

    def __init__(self) -> None:
        self.now = 1_000.0
        self.cards: dict[str, tuple[dict[str, Any], float]] = {}

    def advance(self, seconds: float) -> None:
        """Move the store clock forward; every stored card gets older."""
        self.now += seconds


class MemoryRegistry:
    """A provider whose storage is a :class:`MemoryStore`."""

    def __init__(self, store: MemoryStore, prune_after_seconds: float = 86_400.0) -> None:
        self._store = store
        self._prune_after = prune_after_seconds
        self.started_with: RegistryContext | None = None
        self.stopped = False

    async def start(self, ctx: RegistryContext) -> None:
        self.started_with = ctx

    async def publish(self, card: dict[str, Any]) -> None:
        self._store.cards[sanitize_worker_name(card['worker'])] = (dict(card), self._store.now)

    async def list(self) -> PeerCardList:
        listed: PeerCardList = []
        for name, (card, written_at) in sorted(self._store.cards.items()):
            age = self._store.now - written_at
            if age > self._prune_after:
                del self._store.cards[name]
                continue
            listed.append(PeerCard(card=dict(card), age_seconds=age))
        return listed

    async def withdraw(self, worker: str, instance: str) -> None:
        name = sanitize_worker_name(worker)
        stored = self._store.cards.get(name)
        if stored is not None and stored[0].get('instance') == instance:
            del self._store.cards[name]

    async def stop(self) -> None:
        self.stopped = True
