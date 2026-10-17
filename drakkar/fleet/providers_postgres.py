"""The ``postgres`` registry provider: worker cards as rows of a Postgres table."""

import re
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, cast

import structlog

from drakkar.config.connections import SCHEMA_RE, PostgresConnectionConfig
from drakkar.fleet.card import decode_card, serialize_card
from drakkar.fleet.provider import PeerCard, PeerCardList, RegistryContext
from drakkar.providers.postgres import (
    FLEET_WORKERS_TABLE,
    POSTGRES_PROVIDER,
    borrow,
    close_pool,
    ensure_tables,
    open_pool,
    statements_for,
)
from drakkar.providers.thread import PROVIDER_STOPPED

NO_CONNECTIONS_MESSAGE = 'the postgres registry needs the worker connections'
# The schema of a connection entry that names none, so a preset pool gets the same default.
DEFAULT_SCHEMA: str = PostgresConnectionConfig.model_fields['schema_name'].default


class PostgresRegistry:
    """Cards as rows of ``<schema>.fleet_workers``, one per worker name, ages on the server's clock.

    ``publish`` upserts the card's canonical bytes with ``heartbeat_at =
    now()``; ``list`` first deletes the rows older than
    ``prune_after_seconds``, then reads every card with its age, both judged
    on the database server's ``now()``. No host's local clock enters an age,
    so ``online`` stays ``None`` and the registry service judges the age.

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
        prune_after_seconds: float,
        open_pool: Callable[[Any], Awaitable[Any]] = open_pool,
        close_pool: Callable[[Any], Awaitable[None]] = close_pool,
        pool: Any | None = None,
        schema: str = DEFAULT_SCHEMA,
        manage_schema: bool = True,
    ) -> None:
        if not re.fullmatch(SCHEMA_RE, schema):
            raise ValueError(f"schema '{schema}' must be a lower-case identifier ([a-z_][a-z0-9_]*)")
        self._connection = connection
        self._prune_after = float(prune_after_seconds)
        self._open_pool = open_pool
        self._close_pool = close_pool
        self._preset_pool = pool
        self._preset_schema = schema
        self._preset_manage_schema = manage_schema
        # The pool and the statements rendered for its schema, set once a start succeeded.
        self._ready: tuple[Any, dict[str, str]] | None = None
        # Workers whose card was logged as undecodable. Cut down to the
        # workers of each listing, so it never outgrows the table, and a
        # worker pruned and back with a bad card is logged again.
        self._reported_workers: set[str] = set()
        self._logger: Any = structlog.get_logger()
        self._stopped = False

    async def start(self, ctx: RegistryContext) -> None:
        """Take the pool and make the table available; a failed start leaves nothing to undo."""
        if self._stopped:
            raise RuntimeError(PROVIDER_STOPPED)
        self._logger = ctx.logger
        pool, schema, manage_schema = await self._resolve_pool(ctx)
        await ensure_tables(
            pool,
            schema=schema,
            manage_schema=manage_schema,
            connection=self._connection,
            tables=[FLEET_WORKERS_TABLE],
        )
        self._ready = (pool, statements_for(schema))

    async def publish(self, card: dict[str, Any]) -> None:
        """Upsert ``card`` as its canonical bytes under its worker name, stamped with the server's ``now()``."""
        pool, statements = self._started()
        text = serialize_card(card).decode('utf-8')
        async with borrow(pool) as conn:
            await conn.execute(statements['publish'], card['worker'], card['instance'], card['cluster'], text)

    async def list(self) -> PeerCardList:
        """Delete the cards older than the prune age, then return every other card with its age."""
        pool, statements = self._started()
        async with borrow(pool) as conn:
            await conn.execute(statements['list_prune'], self._prune_after)
            rows = await conn.fetch(statements['list_select'])
        self._reported_workers &= {row[0] for row in rows}
        listed: PeerCardList = []
        for row in rows:
            worker, text, age = row[0], row[1], row[2]
            card, reason = decode_card(text.encode('utf-8'))
            if card is None:
                await self._report_invalid_card(worker, reason)
                continue
            # max(0, ...): the server's clock may step back between a write and a read.
            listed.append(PeerCard(card=card, age_seconds=max(0.0, float(age)), online=None))
        return listed

    async def withdraw(self, worker: str, instance: str) -> None:
        """Delete the card of ``worker`` while it still holds ``instance``."""
        pool, statements = self._started()
        async with borrow(pool) as conn:
            await conn.execute(statements['withdraw'], worker, instance)

    async def stop(self) -> None:
        """Refuse every later call; the shared pool stays open for the worker to close."""
        self._stopped = True

    async def _resolve_pool(self, ctx: RegistryContext) -> tuple[Any, str, bool]:
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
            raise RuntimeError('PostgresRegistry used before start()')
        return self._ready

    async def _report_invalid_card(self, worker: str, reason: str) -> None:
        """Log a card that does not decode, once per worker name while that name stays listed."""
        if worker in self._reported_workers:
            return
        self._reported_workers.add(worker)
        await self._logger.awarning('fleet_card_invalid', category='fleet', worker=worker, reason=reason)


def postgres_registry_factory(options: Mapping[str, Any]) -> PostgresRegistry:
    """The ``postgres`` provider from ``{'connection': <name>, 'prune_after_seconds': <seconds>}``."""
    return PostgresRegistry(connection=options['connection'], prune_after_seconds=options['prune_after_seconds'])
