"""In-process registry and election providers for the fleet tests.

``MemoryRegistry`` stores cards in a dict shared by every provider built on
the same :class:`MemoryStore`, with ages on a clock the test moves. It is a
reference implementation of the provider interface and runs through the
whole test kit, so the kit itself is tested against a provider that is not
a built-in.

``MemoryElection`` does the same for the election: leases in a dict shared
through an :class:`ElectionStore`, expiring on a clock the test moves. It
runs through the election test kit.
"""

from collections import Counter
from typing import Any

from drakkar.fleet.card import sanitize_worker_name
from drakkar.fleet.provider import PeerCard, PeerCardList, RegistryContext
from drakkar.recorder.election import ElectionContext
from drakkar.recorder.lease import LeaseHolder


class MemoryStore:
    """Cards keyed by sanitized worker name, each with the clock time it was written."""

    def __init__(self) -> None:
        self.now = 1_000.0
        self.cards: dict[str, tuple[dict[str, Any], float]] = {}

    def advance(self, seconds: float) -> None:
        """Move the store clock forward; every stored card gets older."""
        self.now += seconds


class MemoryRegistry:
    """A provider whose storage is a :class:`MemoryStore`.

    With ``offline_after_seconds`` it also reports its own liveness verdict,
    from the store clock, the way a service that tracks liveness itself would;
    without it ``online`` stays ``None``.
    """

    def __init__(
        self,
        store: MemoryStore,
        prune_after_seconds: float = 86_400.0,
        offline_after_seconds: float | None = None,
    ) -> None:
        self._store = store
        self._prune_after = prune_after_seconds
        self._offline_after = offline_after_seconds
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
            online = None if self._offline_after is None else age <= self._offline_after
            listed.append(PeerCard(card=dict(card), age_seconds=age, online=online))
        return listed

    def card_key(self, worker: str) -> str:
        return f'memory/workers/{sanitize_worker_name(worker)}'

    async def withdraw(self, worker: str, instance: str) -> None:
        name = sanitize_worker_name(worker)
        stored = self._store.cards.get(name)
        if stored is not None and stored[0].get('instance') == instance:
            del self._store.cards[name]

    async def stop(self) -> None:
        self.stopped = True


class ElectionStore:
    """Leases keyed by scope: the holding token, when the lease ends on the store clock, and its length."""

    def __init__(self) -> None:
        self.now = 1_000.0
        self.leases: dict[str, tuple[str, float, float]] = {}
        # Scope -> (worker, instance) of the provider that last acquired it.
        self.owners: dict[str, tuple[str, str]] = {}

    def advance(self, seconds: float) -> None:
        """Move the store clock forward; every stored lease gets closer to its end."""
        self.now += seconds

    def holder(self, scope: str) -> str | None:
        """The token holding ``scope`` now, or ``None`` when it is free or its lease ran out."""
        lease = self.leases.get(scope)
        if lease is None or lease[1] <= self.now:
            return None
        return lease[0]


class MemoryElection:
    """An election provider whose leases live in an :class:`ElectionStore`; ``calls`` counts every method call.

    Like every provider, it refuses each call after :meth:`stop` (the call is
    still counted).
    """

    def __init__(self, store: ElectionStore) -> None:
        self._store = store
        self.calls: Counter[str] = Counter()
        self.started_with: ElectionContext | None = None
        self.stopped = False

    def _refuse_after_stop(self) -> None:
        if self.stopped:
            raise RuntimeError('provider stopped')

    async def start(self, ctx: ElectionContext) -> None:
        self.calls['start'] += 1
        self._refuse_after_stop()
        self.started_with = ctx

    async def acquire(self, scope: str, token: str, lease_seconds: float) -> bool:
        self.calls['acquire'] += 1
        self._refuse_after_stop()
        holder = self._store.holder(scope)
        if holder is not None and holder != token:
            return False
        self._store.leases[scope] = (token, self._store.now + lease_seconds, lease_seconds)
        if self.started_with is not None:
            self._store.owners[scope] = (self.started_with.worker, self.started_with.instance)
        return True

    async def refresh(self, scope: str, token: str) -> bool:
        self.calls['refresh'] += 1
        self._refuse_after_stop()
        if self._store.holder(scope) != token:
            return False
        lease_seconds = self._store.leases[scope][2]
        self._store.leases[scope] = (token, self._store.now + lease_seconds, lease_seconds)
        return True

    async def is_held(self, scope: str, token: str) -> bool:
        self.calls['is_held'] += 1
        self._refuse_after_stop()
        return self._store.holder(scope) == token

    async def release(self, scope: str, token: str) -> None:
        self.calls['release'] += 1
        self._refuse_after_stop()
        if self._store.holder(scope) == token:
            del self._store.leases[scope]

    async def holder(self, scope: str) -> LeaseHolder | None:
        self.calls['holder'] += 1
        self._refuse_after_stop()
        if self._store.holder(scope) is None:
            return None
        _, ends_at, lease_seconds = self._store.leases[scope]
        worker, instance = self._store.owners.get(scope, (None, None))
        return LeaseHolder(
            worker=worker,
            instance=instance,
            acquired_at=None,
            age_seconds=lease_seconds - (ends_at - self._store.now),
            expires_in_seconds=ends_at - self._store.now,
        )

    def lease_key(self, scope: str) -> str:
        return f'memory/task-locks/{scope}'

    async def stop(self) -> None:
        self.calls['stop'] += 1
        self.stopped = True
