"""The ``redis`` election provider: task leases as keys of a Redis database."""

import re
import time
from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING, Any

from drakkar.config.connections import REDIS_PREFIX_RE
from drakkar.providers.redis import (
    DEFAULT_STATE_PREFIX,
    REDIS_PROVIDER,
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
from drakkar.recorder.history import serialize_json
from drakkar.recorder.lease import LeaseHolder, holder_from_body
from drakkar.timefmt import format_epoch_micro

if TYPE_CHECKING:
    # Only for the annotations: drakkar.recorder.election imports this module to register the provider.
    from drakkar.recorder.election import ElectionContext

REFRESH_BEFORE_ACQUIRE = 'redis election: refresh of a scope this provider has not acquired'
MILLISECONDS_PER_SECOND = 1000


class RedisElection:
    """Leases as keys ``<prefix>task-locks:<scope>``, one key per scope, the lease being the key's time to live.

    The scope (``<cluster>/<task>``) goes into the key as an opaque string.

    The server's clock judges every lease: a lease is alive while its key
    exists, and Redis deletes the key when its time to live ends. No host's
    local clock enters the judgement. An expired lease is therefore a deleted
    key, so a taker finds a free scope and there is no previous holder to read:
    this provider logs no ``periodic_lock_taken_over`` line.

    Every write names the token and the server checks it. Each write is one
    script that reads the key's body and acts only while the body holds the
    caller's ``"token":"<token>"`` text, so a holder that was taken over can
    neither extend nor free the new holder's lease. ``acquire`` sets the body
    together with the time to live when the key is absent, and for the token
    that already holds the key it moves the time to live and keeps the body.
    The body is the lock encoding (sorted compact JSON with ``acquired_at``,
    ``instance``, ``provider``, ``token`` and ``worker``); ``acquired_at`` is
    informational and comes from the acquiring host's clock.

    ``refresh`` takes no lease length, so the provider remembers, per scope,
    the lease of its last successful ``acquire`` and forgets it at ``release``.
    A ``refresh`` of a scope without a remembered lease raises
    ``RuntimeError``: a lease is always refreshed on the provider that
    acquired it.

    The client is the one of ``connections.redis.<connection>``, shared with
    every other feature that names the same connection. ``stop`` leaves it
    open: the worker closes the shared connections after every provider has
    stopped. ``client`` presets a client and skips the connections;
    ``state_prefix`` applies only to such a preset client, since otherwise it
    comes from the connection entry. It is put into the keys as it is, so it
    must match the characters that the entry allows.
    """

    def __init__(
        self,
        *,
        connection: str,
        open_client: Callable[[Any], Awaitable[Any]] = open_client,
        close_client: Callable[[Any], Awaitable[None]] = close_client,
        client: Any | None = None,
        state_prefix: str = DEFAULT_STATE_PREFIX,
    ) -> None:
        if not re.fullmatch(REDIS_PREFIX_RE, state_prefix):
            raise ValueError(f"state_prefix '{state_prefix}' may only contain letters, digits, '_', ':', '.' and '-'")
        self._connection = connection
        self._open_client = open_client
        self._close_client = close_client
        self._preset_client = client
        self._preset_prefix = state_prefix
        # The client and the state prefix that go with it, set once a start succeeded.
        self._ready: tuple[Any, str] | None = None
        # The worker and instance written into the lock bodies this provider sets.
        self._worker = ''
        self._instance = ''
        # Scope -> lease in milliseconds, from the last successful acquire; refresh needs it.
        self._lease_milliseconds: dict[str, str] = {}
        self._stopped = False

    async def start(self, ctx: 'ElectionContext') -> None:
        """Take the client and check that the server answers; a failed start leaves nothing to undo."""
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
        await run(client, 'PING')
        self._ready = (client, prefix)

    async def acquire(self, scope: str, token: str, lease_seconds: float) -> bool:
        """Take ``scope`` for ``token``; True for a free scope, an expired lease, or the token that holds it."""
        client, prefix = self._started()
        lease = milliseconds(lease_seconds)
        taken = await run(
            client,
            'EVAL',
            SCRIPTS['acquire'],
            '1',
            key('election_lock', prefix=prefix, scope=scope),
            self._lock_body(token),
            fragment('token', token),
            lease,
        )
        if taken != 1:
            return False
        self._lease_milliseconds[scope] = lease
        return True

    async def refresh(self, scope: str, token: str) -> bool:
        """Move the lease's time to live to the remembered lease length while ``token`` holds the key."""
        client, prefix = self._started()
        lease = self._lease_milliseconds.get(scope)
        if lease is None:
            raise RuntimeError(REFRESH_BEFORE_ACQUIRE)
        refreshed = await run(
            client,
            'EVAL',
            SCRIPTS['refresh'],
            '1',
            key('election_lock', prefix=prefix, scope=scope),
            fragment('token', token),
            lease,
        )
        return refreshed == 1

    async def is_held(self, scope: str, token: str) -> bool:
        """True while the body of the key of ``scope`` holds ``token``; a body that is not a lock holds nothing."""
        client, prefix = self._started()
        body = await run(client, 'GET', key('election_lock', prefix=prefix, scope=scope))
        # The reply is bytes, and the script searches bytes too, so a body that is not UTF-8 cannot raise here.
        return body is not None and fragment('token', token).encode('utf-8') in body

    async def release(self, scope: str, token: str) -> None:
        """Delete the key of ``scope`` while it holds ``token``; forget the remembered lease whatever the reply."""
        client, prefix = self._started()
        self._lease_milliseconds.pop(scope, None)
        await run(
            client,
            'EVAL',
            SCRIPTS['release'],
            '1',
            key('election_lock', prefix=prefix, scope=scope),
            fragment('token', token),
        )

    async def holder(self, scope: str) -> LeaseHolder | None:
        """The lock body of ``scope`` and the time left of its key: ``GET`` (as ``is_held``), then ``PTTL``.

        The two commands are not one read: when the key ends between them,
        the holder is reported with no time left known.
        """
        client, _ = self._started()
        lock_key = self.lease_key(scope)
        body = await run(client, 'GET', lock_key)
        if body is None:
            return None
        ttl = await run(client, 'PTTL', lock_key)
        expires_in = ttl / MILLISECONDS_PER_SECOND if isinstance(ttl, int) and ttl >= 0 else None
        return holder_from_body(body, expires_in_seconds=expires_in)

    def lease_key(self, scope: str) -> str:
        """The key of the lease of ``scope``: ``<prefix>task-locks:<scope>``."""
        _, prefix = self._started()
        return key('election_lock', prefix=prefix, scope=scope)

    async def stop(self) -> None:
        """Refuse every later call; leases are released by their holders, the shared client by the worker."""
        self._stopped = True

    def _lock_body(self, token: str) -> str:
        """The body of a new lock key: the lock encoding of this worker's claim, as text."""
        return serialize_json(
            {
                'acquired_at': format_epoch_micro(time.time()),
                'instance': self._instance,
                'provider': REDIS_PROVIDER,
                'token': token,
                'worker': self._worker,
            }
        ).decode('utf-8')

    def _started(self) -> tuple[Any, str]:
        """The client and the state prefix; raises after :meth:`stop` and before a successful :meth:`start`."""
        if self._stopped:
            raise RuntimeError(PROVIDER_STOPPED)
        if self._ready is None:
            raise RuntimeError('RedisElection used before start()')
        return self._ready


def redis_election_factory(options: Mapping[str, Any]) -> RedisElection:
    """The ``redis`` provider from ``{'connection': <name>}``."""
    return RedisElection(connection=options['connection'])
