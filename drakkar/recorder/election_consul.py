"""The ``consul`` election provider: archive leases as keys of a Consul agent's KV store, each held by a session."""

import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from drakkar.providers.consul import (
    CONSUL_PROVIDER,
    DEFAULT_KEY_PREFIX,
    REFRESH_BEFORE_ACQUIRE_MESSAGE,
    ConsulClient,
    acquire_key,
    check_key_prefix,
    check_leader,
    close_client,
    create_session,
    destroy_session,
    election_session_name,
    holder_of,
    open_client,
    path,
    read_key,
    renew_session,
    resolve_client,
)
from drakkar.providers.thread import PROVIDER_STOPPED
from drakkar.recorder.history import serialize_json
from drakkar.timefmt import format_epoch_micro

if TYPE_CHECKING:
    # Only for the annotations: drakkar.recorder.election imports this module to register the provider.
    from drakkar.recorder.election import ElectionContext

REFRESH_BEFORE_ACQUIRE = REFRESH_BEFORE_ACQUIRE_MESSAGE


@dataclass(frozen=True)
class _Lease:
    """What the provider remembers of one scope and token: the session that holds the lock key, and the body it wrote."""

    session: str
    body: str


class ConsulElection:
    """Leases as keys ``<prefix>/archive-locks/<scope>``, one key per scope, each held by one session per scope and token.

    ``acquire`` creates a session named after the scope and the token and
    writes the lock body to the scope's key with ``?acquire=<session>``, which
    the agent grants only while no other session holds the key. The lease is
    the session's time to live on the server: half the lease length
    (``history.archive.lock_stale_seconds``, at least 10 s), because the agent
    ends a session only about twice its TTL after the last renew, so Consul
    ends a dead holder's lease by that limit. ``refresh`` renews the session.
    No host's clock enters the judgement; the lock body's ``acquired_at``
    (sorted compact JSON with ``acquired_at``, ``instance``, ``provider``,
    ``token`` and ``worker``) is informational and comes from the acquiring
    host's clock.

    Every session has ``Behavior: delete``, so a session that ends takes the
    key it holds with it. An expired lease is therefore a deleted key: a taker
    finds a free scope and there is no previous holder to read, so this
    provider logs no ``recorder_archive_lock_taken_over`` line. ``release``
    destroys the session, which deletes the key only while that session holds
    it: a holder that was taken over cannot free the new holder's lease.

    The provider remembers, per scope and token, the session and the lock body.
    The entry is recorded as soon as the session exists, before the acquire
    request, so a retry of an acquire whose answer was lost, and a release
    after an unclear acquire, find the session. A create whose answer is lost
    leaves a session that nothing tracks; it holds no key and ends within
    twice its TTL. An entry is forgotten only after the destroy of its session
    succeeded, so a retried release really retries. An acquire by the token
    that already has an entry renews its session and writes the remembered body
    again, which keeps the holder's ``acquired_at``; when its session ended, a
    new one is created. An acquire that the agent refuses destroys its session
    at once (it holds nothing, and would otherwise live for up to twice its
    TTL) and forgets the entry. A ``refresh`` without an entry raises
    ``RuntimeError``: the archive pass always refreshes on the provider that
    acquired. ``is_held`` without an entry answers False without a request.

    The client is the one of ``connections.consul.<connection>``, shared with
    every other feature that names the same connection. ``stop`` sends nothing
    and leaves the client open: leases are released by their holders, and the
    worker closes the shared connections after every provider has stopped.
    ``client`` presets a client and skips the connections; ``key_prefix``
    applies only to such a preset client, since otherwise it comes from the
    connection entry. It goes into the paths as it is, so it must match the
    characters that the entry allows.
    """

    def __init__(
        self,
        *,
        connection: str,
        open_client: Callable[[Any], Awaitable[ConsulClient]] = open_client,
        close_client: Callable[[ConsulClient], Awaitable[None]] = close_client,
        client: ConsulClient | None = None,
        key_prefix: str = DEFAULT_KEY_PREFIX,
    ) -> None:
        check_key_prefix(key_prefix)
        self._connection = connection
        self._open_client = open_client
        self._close_client = close_client
        self._preset_client = client
        self._preset_prefix = key_prefix
        # The client and the key prefix that go with it, set once a start succeeded.
        self._ready: tuple[ConsulClient, str] | None = None
        # The worker and instance written into the lock bodies this provider sets.
        self._worker = ''
        self._instance = ''
        # (scope, token) -> the session and the lock body. Each change follows the answer that causes it with
        # no await in between, so a call cancelled later still leaves the session to a retry or a release.
        self._leases: dict[tuple[str, str], _Lease] = {}
        self._stopped = False

    async def start(self, ctx: 'ElectionContext') -> None:
        """Take the client and check that the agent names a leader; a failed start leaves nothing to undo."""
        if self._stopped:
            raise RuntimeError(PROVIDER_STOPPED)
        self._worker = ctx.worker
        self._instance = ctx.instance
        client, prefix = await resolve_client(
            connections=ctx.connections,
            connection=self._connection,
            preset_client=self._preset_client,
            preset_prefix=self._preset_prefix,
            open_client=self._open_client,
            close_client=self._close_client,
            who='election',
        )
        await check_leader(client)
        self._ready = (client, prefix)

    async def acquire(self, scope: str, token: str, lease_seconds: float) -> bool:
        """Take ``scope`` for ``token``; True for a free scope, an expired lease, or the token that holds it.

        ``lease_seconds`` sets the TTL of a new session only: the session of a
        token that already has an entry keeps the TTL it was created with.
        """
        client, prefix = self._started()
        lease = self._leases.get((scope, token))
        if lease is not None and not await renew_session(client, lease.session):
            # The session ended (its TTL ran out, or the agent lost it), and its key went with it.
            self._forget(scope, token, lease)
            lease = None
        if lease is None:
            session = await create_session(client, election_session_name(scope, token), lease_seconds)
            lease = _Lease(session=session, body=self._lock_body(token))
            # Recorded before the acquire request, so a retry or a release after an unclear answer finds it.
            self._leases[(scope, token)] = lease
        if await acquire_key(client, path('election_lock', prefix=prefix, scope=scope), lease.session, lease.body):
            return True
        # Another session holds the key. Destroyed before the entry is forgotten, so a destroy that fails
        # leaves the session to the release that follows an unclear acquire.
        await destroy_session(client, lease.session)
        self._forget(scope, token, lease)
        return False

    async def refresh(self, scope: str, token: str) -> bool:
        """Renew the session of ``token`` on ``scope``; False, and the entry forgotten, once the agent no longer knows it."""
        client, _ = self._started()
        lease = self._leases.get((scope, token))
        if lease is None:
            raise RuntimeError(REFRESH_BEFORE_ACQUIRE)
        if await renew_session(client, lease.session):
            return True
        self._forget(scope, token, lease)
        return False

    async def is_held(self, scope: str, token: str) -> bool:
        """True while the session of ``token`` holds the key of ``scope``; False without a request when there is none."""
        client, prefix = self._started()
        lease = self._leases.get((scope, token))
        if lease is None:
            return False
        entry = await read_key(client, path('election_lock', prefix=prefix, scope=scope))
        return entry is not None and holder_of(entry) == lease.session

    async def release(self, scope: str, token: str) -> None:
        """Destroy the session of ``token`` on ``scope``, which deletes the key only while it holds it.

        The entry is forgotten only after the destroy succeeded, so a release
        that the caller retries after a failed destroy sends the destroy
        again; destroying a session that is already gone is no error.
        Without an entry nothing is sent.
        """
        client, _ = self._started()
        lease = self._leases.get((scope, token))
        if lease is None:
            return
        await destroy_session(client, lease.session)
        self._forget(scope, token, lease)

    async def stop(self) -> None:
        """Refuse every later call; leases are released by their holders, the shared client by the worker."""
        self._stopped = True

    def _forget(self, scope: str, token: str, lease: _Lease) -> None:
        """Drop the entry of ``scope`` and ``token``, unless a call that overlapped this one replaced or dropped it."""
        if self._leases.get((scope, token)) is lease:
            del self._leases[(scope, token)]

    def _lock_body(self, token: str) -> str:
        """The body of a new lock key: the lock encoding of this worker's claim, as text."""
        return serialize_json(
            {
                'acquired_at': format_epoch_micro(time.time()),
                'instance': self._instance,
                'provider': CONSUL_PROVIDER,
                'token': token,
                'worker': self._worker,
            }
        ).decode('utf-8')

    def _started(self) -> tuple[ConsulClient, str]:
        """The client and the key prefix; raises after :meth:`stop` and before a successful :meth:`start`."""
        if self._stopped:
            raise RuntimeError(PROVIDER_STOPPED)
        if self._ready is None:
            raise RuntimeError('ConsulElection used before start()')
        return self._ready


def consul_election_factory(options: Mapping[str, Any]) -> ConsulElection:
    """The ``consul`` provider from ``{'connection': <name>}``."""
    return ConsulElection(connection=options['connection'])
