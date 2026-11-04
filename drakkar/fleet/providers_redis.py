"""The ``redis`` registry provider: worker cards as keys of a Redis database, listed through an index."""

import re
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, NamedTuple

import structlog

from drakkar.config.connections import REDIS_PREFIX_RE
from drakkar.fleet.card import decode_card, sanitize_worker_name, serialize_card
from drakkar.fleet.provider import PeerCard, PeerCardList, RegistryContext
from drakkar.providers.cardlog import InvalidCardLog
from drakkar.providers.redis import (
    DEFAULT_STATE_PREFIX,
    SCRIPTS,
    close_client,
    fragment,
    key,
    milliseconds,
    open_client,
    resolve_client,
    run,
)
from drakkar.providers.thread import PROVIDER_STOPPED

_MICROSECONDS_PER_SECOND = 1_000_000
# The list reply starts with the server time (seconds, microseconds); each live card adds three entries.
_LIST_HEADER_LENGTH = 2
_LIST_ENTRY_LENGTH = 3


class _ListEntry(NamedTuple):
    """One live card of a list reply: the index member, the server time of its last publish, the card bytes."""

    member: str
    published_at: float
    card: bytes


def _parse_list_reply(reply: list[Any]) -> tuple[float, list[_ListEntry]]:
    """Split the reply of the ``list`` script into the server's time now and the live cards.

    The reply is ``[seconds, microseconds, (member, score, card)*]``. Any other
    length cannot come from the pinned script, so it raises ``ValueError``.
    Members are decoded leniently: a member that a foreign writer put into the
    index is only used for a log line, and the script has read its card by the
    raw bytes already.
    """
    if len(reply) < _LIST_HEADER_LENGTH or (len(reply) - _LIST_HEADER_LENGTH) % _LIST_ENTRY_LENGTH:
        raise ValueError(f'redis registry: the list reply has an unexpected length ({len(reply)})')
    now = int(reply[0]) + int(reply[1]) / _MICROSECONDS_PER_SECOND
    entries = [
        _ListEntry(
            member=reply[i].decode('utf-8', errors='replace'), published_at=float(reply[i + 1]), card=reply[i + 2]
        )
        for i in range(_LIST_HEADER_LENGTH, len(reply), _LIST_ENTRY_LENGTH)
    ]
    return now, entries


class RedisRegistry:
    """Cards as string keys, one per worker name, found through an index of the workers, ages on the server's clock.

    ``publish`` runs one script: it stores the card's canonical bytes under
    ``<prefix>fleet:workers:<worker>`` with a time to live of
    ``prune_after_seconds``, and adds the worker to the sorted set
    ``<prefix>fleet:workers`` with the server's ``TIME`` as its score. ``list``
    runs one script too: it reads the index, fetches each card, and removes the
    members whose card expired. A card that nobody renews is deleted by Redis
    when its time to live ends and leaves the index at the next ``list``.

    An age is the server's ``TIME`` now minus the stored server ``TIME`` of the
    publish, so no host's local clock enters it, and the ``prune_after_seconds``
    of a reader never changes the age of a card another worker wrote. ``online``
    stays ``None``: the registry service judges the age.

    The client is the one of ``connections.redis.<connection>``, shared with
    every other feature that names the same connection. ``stop`` leaves it open:
    the worker closes the shared connections after every provider has stopped.
    ``client`` presets a client and skips the connections; ``state_prefix`` applies
    only to such a preset client, since otherwise it comes from the connection
    entry. It is put into the keys as it is, so it must match the characters
    that the entry allows.
    """

    def __init__(
        self,
        *,
        connection: str,
        prune_after_seconds: float,
        open_client: Callable[[Any], Awaitable[Any]] = open_client,
        close_client: Callable[[Any], Awaitable[None]] = close_client,
        client: Any | None = None,
        state_prefix: str = DEFAULT_STATE_PREFIX,
    ) -> None:
        if not re.fullmatch(REDIS_PREFIX_RE, state_prefix):
            raise ValueError(f"state_prefix '{state_prefix}' may only contain letters, digits, '_', ':', '.' and '-'")
        self._connection = connection
        self._prune_after = float(prune_after_seconds)
        self._open_client = open_client
        self._close_client = close_client
        self._preset_client = client
        self._preset_prefix = state_prefix
        # The client and the state prefix that go with it, set once a start succeeded.
        self._ready: tuple[Any, str] | None = None
        self._invalid_cards = InvalidCardLog()
        self._logger: Any = structlog.get_logger()
        self._stopped = False

    async def start(self, ctx: RegistryContext) -> None:
        """Take the client and check that the server answers; a failed start leaves nothing to undo."""
        if self._stopped:
            raise RuntimeError(PROVIDER_STOPPED)
        self._logger = ctx.logger
        client, prefix = await resolve_client(
            connections=ctx.connections,
            connection=self._connection,
            preset_client=self._preset_client,
            preset_prefix=self._preset_prefix,
            open_client=self._open_client,
            close_client=self._close_client,
            who='registry',
        )
        await run(client, 'PING')
        self._ready = (client, prefix)

    async def publish(self, card: dict[str, Any]) -> None:
        """Store ``card`` as its canonical bytes under its worker name and stamp the index with the server's ``TIME``."""
        client, prefix = self._started()
        member = sanitize_worker_name(card['worker'])
        await run(
            client,
            'EVAL',
            SCRIPTS['publish'],
            '2',
            key('registry_card', prefix=prefix, worker=member),
            key('registry_index', prefix=prefix),
            serialize_card(card).decode('utf-8'),
            milliseconds(self._prune_after),
            member,
        )

    async def list(self) -> PeerCardList:
        """Return every live card with its age; the index entries of expired cards are removed."""
        client, prefix = self._started()
        reply = await run(
            client,
            'EVAL',
            SCRIPTS['list'],
            '1',
            key('registry_index', prefix=prefix),
            key('registry_card', prefix=prefix, worker=''),
        )
        now, entries = _parse_list_reply(reply)
        self._invalid_cards.cut_to(entry.member for entry in entries)
        listed: PeerCardList = []
        for entry in entries:
            card, reason = decode_card(entry.card)
            if card is None:
                await self._invalid_cards.report(self._logger, entry.member, reason)
                continue
            # max(0, ...): the server's clock may step back between a write and a read.
            listed.append(PeerCard(card=card, age_seconds=max(0.0, now - entry.published_at), online=None))
        return listed

    async def withdraw(self, worker: str, instance: str) -> None:
        """Delete the card of ``worker`` and its index entry while the card still holds ``instance``."""
        client, prefix = self._started()
        member = sanitize_worker_name(worker)
        await run(
            client,
            'EVAL',
            SCRIPTS['withdraw'],
            '2',
            key('registry_card', prefix=prefix, worker=member),
            key('registry_index', prefix=prefix),
            fragment('instance', instance),
            member,
        )

    def card_key(self, worker: str) -> str:
        """The key of the card of ``worker``: ``<prefix>fleet:workers:<sanitized worker>``."""
        _, prefix = self._started()
        return key('registry_card', prefix=prefix, worker=sanitize_worker_name(worker))

    async def stop(self) -> None:
        """Refuse every later call; the shared client stays open for the worker to close."""
        self._stopped = True

    def _started(self) -> tuple[Any, str]:
        """The client and the state prefix; raises after :meth:`stop` and before a successful :meth:`start`."""
        if self._stopped:
            raise RuntimeError(PROVIDER_STOPPED)
        if self._ready is None:
            raise RuntimeError('RedisRegistry used before start()')
        return self._ready


def redis_registry_factory(options: Mapping[str, Any]) -> RedisRegistry:
    """The ``redis`` provider from ``{'connection': <name>, 'prune_after_seconds': <seconds>}``."""
    return RedisRegistry(connection=options['connection'], prune_after_seconds=options['prune_after_seconds'])
