"""The ``postgres`` election provider: task leases as rows of a Postgres table."""

import re
from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING, Any, cast

import structlog

from drakkar.config.connections import SCHEMA_RE, PostgresConnectionConfig
from drakkar.providers.postgres import (
    DEFAULT_SCHEMA,
    POSTGRES_PROVIDER,
    TASK_LOCKS_TABLE,
    borrow,
    borrow_transaction,
    close_pool,
    ensure_tables,
    open_pool,
    statements_for,
)
from drakkar.providers.thread import PROVIDER_STOPPED

if TYPE_CHECKING:
    # Only for the annotations: drakkar.recorder.election imports this module to register the provider.
    from drakkar.recorder.election import ElectionContext

NO_CONNECTIONS_MESSAGE = 'the postgres election needs the worker connections'


class PostgresElection:
    """Leases as rows of ``<schema>.task_locks``, one row per scope, judged on the server's clock.

    The scope (``<cluster>/<task>``) is the row's key, used as an opaque string.

    A lease is alive while its row's ``refreshed_at`` is younger than
    ``lease_seconds`` on the database server's ``now()``; no host's local
    clock enters the judgement. Every write names the token: ``refresh`` and
    ``release`` touch the row only while it holds that token, so a holder
    that was taken over cannot extend or free the new holder's lease.

    ``acquire`` is one upsert whose ``DO UPDATE ... WHERE`` lets the write
    through only for the token that already holds the row or over a lease
    that ran out; the takeover happens in that ``WHERE``, atomically. The
    row read right before it, in the same transaction, names the previous
    holder for the takeover log line.

    The pool is the one of ``connections.postgres.<connection>``, shared with
    every other feature that names the same connection. ``stop`` leaves it
    open: the worker closes the shared connections after every provider has
    stopped. ``pool`` presets a pool and skips the connections; ``schema``
    and ``manage_schema`` apply only to such a preset pool, since otherwise
    they come from the connection entry. ``schema`` must be a lower-case
    identifier, as the connection entry's is, because it is put into the
    statement texts unquoted.
    """

    def __init__(
        self,
        *,
        connection: str,
        open_pool: Callable[[Any], Awaitable[Any]] = open_pool,
        close_pool: Callable[[Any], Awaitable[None]] = close_pool,
        pool: Any | None = None,
        schema: str = DEFAULT_SCHEMA,
        manage_schema: bool = True,
    ) -> None:
        if not re.fullmatch(SCHEMA_RE, schema):
            raise ValueError(f"schema '{schema}' must be a lower-case identifier ([a-z_][a-z0-9_]*)")
        self._connection = connection
        self._open_pool = open_pool
        self._close_pool = close_pool
        self._preset_pool = pool
        self._preset_schema = schema
        self._preset_manage_schema = manage_schema
        # The pool and the statements rendered for its schema, set once a start succeeded.
        self._ready: tuple[Any, dict[str, str]] | None = None
        # The worker and instance written into the lock rows this provider takes.
        self._worker = ''
        self._instance = ''
        self._logger: Any = structlog.get_logger()
        self._stopped = False

    async def start(self, ctx: 'ElectionContext') -> None:
        """Take the pool and make the lock table available; a failed start leaves nothing to undo."""
        if self._stopped:
            raise RuntimeError(PROVIDER_STOPPED)
        self._logger = ctx.logger
        self._worker = ctx.worker
        self._instance = ctx.instance
        pool, schema, manage_schema = await self._resolve_pool(ctx)
        await ensure_tables(
            pool,
            schema=schema,
            manage_schema=manage_schema,
            connection=self._connection,
            tables=[TASK_LOCKS_TABLE],
        )
        self._ready = (pool, statements_for(schema))

    async def acquire(self, scope: str, token: str, lease_seconds: float) -> bool:
        """Take ``scope`` for ``token``; True for a free scope, an expired lease, or the token that holds it."""
        pool, statements = self._started()
        async with borrow_transaction(pool) as conn:
            # FOR UPDATE holds the row until the upsert below decides, so the
            # holder it names is the one the upsert replaced.
            previous = await conn.fetchrow(statements['acquire_select'], scope)
            taken = await conn.fetchrow(
                statements['acquire'], scope, token, self._worker, self._instance, float(lease_seconds)
            )
        if taken is None:
            return False
        if previous is not None and previous[0] != token:
            await self._logger.awarning(
                'periodic_lock_taken_over',
                category='periodic',
                provider=POSTGRES_PROVIDER,
                scope=scope,
                previous_worker=previous[1],
                previous_instance=previous[2],
                age_seconds=round(float(previous[3]), 3),
            )
        return True

    async def refresh(self, scope: str, token: str) -> bool:
        """Move the lease's ``refreshed_at`` to the server's ``now()`` while ``token`` holds it."""
        pool, statements = self._started()
        async with borrow(pool) as conn:
            tag = await conn.execute(statements['refresh'], scope, token)
        return _rows_affected(tag) == 1

    async def is_held(self, scope: str, token: str) -> bool:
        """True while the row of ``scope`` holds ``token``."""
        pool, statements = self._started()
        async with borrow(pool) as conn:
            row = await conn.fetchrow(statements['is_held'], scope, token)
        return row is not None

    async def release(self, scope: str, token: str) -> None:
        """Delete the row of ``scope`` while it holds ``token``."""
        pool, statements = self._started()
        async with borrow(pool) as conn:
            await conn.execute(statements['release'], scope, token)

    async def stop(self) -> None:
        """Refuse every later call; leases are released by their holders, the shared pool by the worker."""
        self._stopped = True

    async def _resolve_pool(self, ctx: 'ElectionContext') -> tuple[Any, str, bool]:
        """The pool with its schema and ``manage_schema``: the preset ones, or those of the connection entry."""
        if self._preset_pool is not None:
            return self._preset_pool, self._preset_schema, self._preset_manage_schema
        if ctx.connections is None:
            raise RuntimeError(NO_CONNECTIONS_MESSAGE)
        settings = cast(PostgresConnectionConfig, ctx.connections.settings(POSTGRES_PROVIDER, self._connection))
        pool = await ctx.connections.acquire(POSTGRES_PROVIDER, self._connection, self._open_pool, self._close_pool)
        return pool, settings.schema_name, settings.manage_schema

    def _started(self) -> tuple[Any, dict[str, str]]:
        """The pool and statements; raises after :meth:`stop` and before a successful :meth:`start`."""
        if self._stopped:
            raise RuntimeError(PROVIDER_STOPPED)
        if self._ready is None:
            raise RuntimeError('PostgresElection used before start()')
        return self._ready


def _rows_affected(tag: str) -> int:
    """The row count at the end of a command tag such as ``'UPDATE 1'``."""
    return int(tag.rsplit(' ', 1)[-1])


def postgres_election_factory(options: Mapping[str, Any]) -> PostgresElection:
    """The ``postgres`` provider from ``{'connection': <name>}``."""
    return PostgresElection(connection=options['connection'])
