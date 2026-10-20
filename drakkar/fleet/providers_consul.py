"""The ``consul`` registry provider: worker cards as keys of a Consul agent's KV store, held by the worker's session."""

from collections.abc import Awaitable, Callable, Mapping
from typing import Any

import structlog

from drakkar.fleet.card import decode_card, sanitize_worker_name, serialize_card
from drakkar.fleet.provider import PeerCard, PeerCardList, RegistryContext
from drakkar.providers.cardlog import InvalidCardLog
from drakkar.providers.consul import (
    CARD_HELD_MESSAGE,
    DEFAULT_KEY_PREFIX,
    KV_PATH_HEAD,
    ConsulClient,
    ConsulError,
    acquire_key,
    check_key_prefix,
    check_leader,
    close_client,
    create_session,
    destroy_session,
    holder_of,
    is_own_session_name,
    open_client,
    path,
    read_key,
    read_prefix,
    registry_session_name,
    renew_session,
    resolve_client,
    session_name,
)
from drakkar.providers.thread import PROVIDER_STOPPED


def _card_member(entry: dict[str, Any], card_key_prefix: str) -> str | None:
    """The worker name a KV entry of the listing stands for, or ``None`` when its key cannot be a card key.

    A card key is ``card_key_prefix`` followed by one sanitized worker name,
    which is never empty and has no ``/``. So the prefix itself (a folder key,
    such as the one a Consul UI creates), a nested key and a key outside the
    prefix are not cards.
    """
    key: str = entry['Key']
    if not key.startswith(card_key_prefix):
        return None
    member = key[len(card_key_prefix) :]
    if not member or '/' in member:
        return None
    return member


class ConsulRegistry:
    """Cards as KV keys, one per worker name, each held by a session of its worker; liveness from the session.

    ``publish`` writes the card's canonical bytes to
    ``<prefix>/fleet/workers/<worker>`` with ``?acquire=<session>``, so the key
    belongs to the worker's session. The session is created at the first
    publish and renewed at each later one. Its time to live is half of
    ``offline_after_seconds`` (at least 10 s), because the agent ends a session
    only about twice its TTL after the last renew: so Consul ends the session of
    a worker that stopped publishing by the offline limit, and with
    ``Behavior: delete`` the card goes with it. A renew that the agent answers
    ``404`` (the session ended while the worker could not reach it, or the
    agent lost its state) is followed by a new session.

    ``list`` reads every key under the card prefix. A card whose key a session
    holds (its ``Session`` is a non-empty string) is ``online``; a key without
    a session was written by something other than a provider, and is listed
    offline. A key that cannot be a card key (the prefix itself, or a nested
    key) is skipped without a log line. A card has no age: the session's TTL is
    the liveness clock, kept by Consul, so no host's clock enters the verdict
    and the provider reads no clock of its own.

    A worker restarted under its old name finds its card key held by the
    session of its predecessor, which may live on for up to the offline
    limit. When the holder's session name is the one a worker of the same
    sanitized name gives its sessions, the provider destroys that session and
    takes the key at once. A session of any other name is left alone, and the
    publish fails with a final error.

    ``withdraw`` destroys the session, which deletes the card. ``stop`` sends
    nothing. The client is the one of ``connections.consul.<connection>``,
    shared with every other feature that names the same connection; ``stop``
    leaves it open, for the worker to close after every provider has stopped.
    ``client`` presets a client and skips the connections; ``key_prefix``
    applies only to such a preset client, since otherwise it comes from the
    connection entry. It goes into the paths as it is, so it must match the
    characters that the entry allows.
    """

    def __init__(
        self,
        *,
        connection: str,
        offline_after_seconds: float,
        open_client: Callable[[Any], Awaitable[ConsulClient]] = open_client,
        close_client: Callable[[ConsulClient], Awaitable[None]] = close_client,
        client: ConsulClient | None = None,
        key_prefix: str = DEFAULT_KEY_PREFIX,
    ) -> None:
        check_key_prefix(key_prefix)
        self._connection = connection
        self._offline_after = float(offline_after_seconds)
        self._open_client = open_client
        self._close_client = close_client
        self._preset_client = client
        self._preset_prefix = key_prefix
        # The client and the key prefix that go with it, set once a start succeeded.
        self._ready: tuple[ConsulClient, str] | None = None
        # The id of this worker's session. Each change follows the answer that causes it with no await in
        # between: a new id is kept before anything else is awaited, so a publish cancelled later still
        # leaves it to the withdraw, and the withdraw takes it out before it sends the destroy.
        self._session: str | None = None
        self._invalid_cards = InvalidCardLog()
        self._logger: Any = structlog.get_logger()
        self._stopped = False

    async def start(self, ctx: RegistryContext) -> None:
        """Take the client and check that the agent names a leader; a failed start leaves nothing to undo."""
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
        await check_leader(client)
        self._ready = (client, prefix)

    async def publish(self, card: dict[str, Any]) -> None:
        """Renew (or create) the worker's session and write ``card`` to its key held by that session.

        When another session holds the key, :meth:`_clear_the_card_key` either
        clears the way, and the key is acquired once more, or raises. A key that
        is still held after that raises ``ConsulError(CARD_HELD_MESSAGE)``.
        """
        client, prefix = self._started()
        member = sanitize_worker_name(card['worker'])
        key_path = path('registry_card', prefix=prefix, worker=member)
        body = serialize_card(card).decode('utf-8')
        session = await self._live_session(client, registry_session_name(member, card['instance']))
        if await acquire_key(client, key_path, session, body):
            return
        await self._clear_the_card_key(client, key_path, member)
        if not await acquire_key(client, key_path, session, body):
            raise ConsulError(CARD_HELD_MESSAGE.format(worker=member))

    async def list(self) -> PeerCardList:
        """Return every card under the card prefix; online while a session holds its key, never with an age."""
        client, prefix = self._started()
        list_path = path('registry_list', prefix=prefix)
        card_key_prefix = list_path.removeprefix(KV_PATH_HEAD)
        entries = await read_prefix(client, list_path)
        members = [(member, entry) for entry in entries if (member := _card_member(entry, card_key_prefix)) is not None]
        self._invalid_cards.cut_to(member for member, _ in members)
        listed: PeerCardList = []
        for member, entry in members:
            card, reason = decode_card(entry['Value'])
            if card is None:
                await self._invalid_cards.report(self._logger, member, reason)
                continue
            listed.append(PeerCard(card=card, age_seconds=None, online=holder_of(entry) is not None))
        return listed

    async def withdraw(self, worker: str, instance: str) -> None:
        """Destroy this worker's session, which deletes its card; without a session nothing is sent.

        The session is this provider's own, so ``worker`` and ``instance`` are
        not needed: when a twin under the same name has taken the card over,
        the session no longer holds it, and destroying it changes nothing.

        A withdraw is single-shot: the registry service gives it one attempt,
        and the id is forgotten before the destroy is sent. When the destroy
        fails, the session (and the card with it) ends by its TTL.
        """
        client, _ = self._started()
        session, self._session = self._session, None
        if session is not None:
            await destroy_session(client, session)

    async def stop(self) -> None:
        """Refuse every later call; the session is left to ``withdraw`` and the shared client to the worker."""
        self._stopped = True

    async def _live_session(self, client: ConsulClient, name: str) -> str:
        """Return the id of this worker's session after a renew, or of a new session named ``name``."""
        session = self._session
        if session is not None:
            if await renew_session(client, session):
                return session
            # The agent no longer knows the session. Forgotten before the create, so a create that fails is
            # not followed by another renew of the ended session; unless a withdraw or another publish
            # replaced it during the renew, which this must not undo.
            if self._session == session:
                self._session = None
        session = await create_session(client, name, self._offline_after)
        self._session = session
        return session

    async def _clear_the_card_key(self, client: ConsulClient, key_path: str, member: str) -> None:
        """Make way for one more acquire of the card key of ``member``, or raise when another worker holds it.

        Returns at once when the key, its session or that session's name is
        gone: the holder left between the acquire and the read. When the
        holder's session name is one of ``member`` (a predecessor or a twin
        under the same name), that session is destroyed, which deletes the
        key. Any other holder raises ``ConsulError(CARD_HELD_MESSAGE)``.
        """
        entry = await read_key(client, key_path)
        holder = None if entry is None else holder_of(entry)
        if holder is None:
            return
        holder_name = await session_name(client, holder)
        if holder_name is None:
            return
        if not is_own_session_name(holder_name, member):
            raise ConsulError(CARD_HELD_MESSAGE.format(worker=member))
        await destroy_session(client, holder)

    def _started(self) -> tuple[ConsulClient, str]:
        """The client and the key prefix; raises after :meth:`stop` and before a successful :meth:`start`."""
        if self._stopped:
            raise RuntimeError(PROVIDER_STOPPED)
        if self._ready is None:
            raise RuntimeError('ConsulRegistry used before start()')
        return self._ready


def consul_registry_factory(options: Mapping[str, Any]) -> ConsulRegistry:
    """The ``consul`` provider from ``{'connection': <name>, 'offline_after_seconds': <seconds>}``."""
    return ConsulRegistry(connection=options['connection'], offline_after_seconds=options['offline_after_seconds'])
