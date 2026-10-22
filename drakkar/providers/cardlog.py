"""The log of undecodable cards that the connection registries share.

A registry lists its cards on every heartbeat of every worker. A card that
does not decode stays in the listing until its writer replaces it or it is
pruned, so logging it on every listing would repeat one line for as long as it
stays. :class:`InvalidCardLog` logs it once per worker name instead.
"""

from collections.abc import Iterable
from typing import Any


class InvalidCardLog:
    """Logs an undecodable card once per worker name while that name stays listed.

    The set of reported names is cut down to the names of each listing with
    :meth:`cut_to`, so it never outgrows the registry, and a worker that was
    pruned and came back with a bad card is logged again.
    """

    def __init__(self) -> None:
        self._reported: set[str] = set()

    def cut_to(self, listed: Iterable[str]) -> None:
        """Forget the names that are not in this listing."""
        self._reported &= set(listed)

    async def report(self, logger: Any, worker: str, reason: str) -> None:
        """Log ``fleet_card_invalid`` for ``worker`` once, and again only after its name has left the listing."""
        if worker in self._reported:
            return
        self._reported.add(worker)
        await logger.awarning('fleet_card_invalid', category='fleet', worker=worker, reason=reason)
