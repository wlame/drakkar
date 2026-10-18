"""Postgres statement corpus, rendering and pool helpers shared by the fleet providers.

The worker registry and the archive election both keep their state in Postgres
tables. This module holds what the two have in common: the DDL and every
statement text, the rule that fills in the connection's schema, the pool that
the shared ``connections`` section opens, and the mapping of Postgres errors
to the provider caller's retry rule.

The texts are pinned by ``tests/fixtures/fleet_providers.json`` so the emitted
bytes are identical for any implementation. ``{schema}`` in a text stands for
the schema of the connection; it is validated as a lower-case identifier when
the config loads, so a plain replace is safe.
"""

from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from typing import Any

import asyncpg

from drakkar.config.connections import PostgresConnectionConfig
from drakkar.providers.resilience import TransientError

POSTGRES_PROVIDER = 'postgres'
SCHEMA_PLACEHOLDER = '{schema}'
FLEET_WORKERS_TABLE = 'fleet_workers'
ARCHIVE_LOCKS_TABLE = 'archive_locks'
# The schema of a connection entry that names none, so a preset pool gets the same default.
DEFAULT_SCHEMA: str = PostgresConnectionConfig.model_fields['schema_name'].default

# Run in this order: the schema first, then each table with its indexes.
DDL_TEMPLATES: tuple[str, ...] = (
    'CREATE SCHEMA IF NOT EXISTS {schema}',
    'CREATE TABLE IF NOT EXISTS {schema}.fleet_workers (\n'
    '    worker        text        PRIMARY KEY,\n'
    '    instance      text        NOT NULL,\n'
    '    cluster       text        NOT NULL,\n'
    '    card          text        NOT NULL,\n'
    '    heartbeat_at  timestamptz NOT NULL DEFAULT now()\n'
    ')',
    'CREATE INDEX IF NOT EXISTS fleet_workers_cluster_idx ON {schema}.fleet_workers (cluster)',
    'CREATE INDEX IF NOT EXISTS fleet_workers_heartbeat_idx ON {schema}.fleet_workers (heartbeat_at)',
    'CREATE TABLE IF NOT EXISTS {schema}.archive_locks (\n'
    '    cluster       text        PRIMARY KEY,\n'
    '    token         text        NOT NULL,\n'
    '    worker        text        NOT NULL,\n'
    '    instance      text        NOT NULL,\n'
    '    acquired_at   timestamptz NOT NULL DEFAULT now(),\n'
    '    refreshed_at  timestamptz NOT NULL DEFAULT now()\n'
    ')',
)

STATEMENT_TEMPLATES: dict[str, str] = {
    'check_table': 'SELECT to_regclass($1) IS NOT NULL',
    'ddl_lock': 'SELECT pg_advisory_xact_lock(hashtext($1))',
    'publish': (
        'INSERT INTO {schema}.fleet_workers (worker, instance, cluster, card, heartbeat_at) '
        'VALUES ($1, $2, $3, $4, now()) '
        'ON CONFLICT (worker) DO UPDATE SET instance = EXCLUDED.instance, cluster = EXCLUDED.cluster, '
        'card = EXCLUDED.card, heartbeat_at = now()'
    ),
    'list_prune': 'DELETE FROM {schema}.fleet_workers WHERE heartbeat_at < now() - make_interval(secs => $1)',
    'list_select': (
        'SELECT worker, card, EXTRACT(EPOCH FROM now() - heartbeat_at)::float8 '
        'FROM {schema}.fleet_workers ORDER BY worker'
    ),
    'withdraw': 'DELETE FROM {schema}.fleet_workers WHERE worker = $1 AND instance = $2',
    'acquire_select': (
        'SELECT token, worker, instance, EXTRACT(EPOCH FROM now() - refreshed_at)::float8 '
        'FROM {schema}.archive_locks WHERE cluster = $1 FOR UPDATE'
    ),
    'acquire': (
        'INSERT INTO {schema}.archive_locks AS held (cluster, token, worker, instance, acquired_at, refreshed_at) '
        'VALUES ($1, $2, $3, $4, now(), now()) '
        'ON CONFLICT (cluster) DO UPDATE SET token = EXCLUDED.token, worker = EXCLUDED.worker, '
        'instance = EXCLUDED.instance, '
        'acquired_at = CASE WHEN held.token = EXCLUDED.token THEN held.acquired_at ELSE now() END, '
        'refreshed_at = now() '
        'WHERE held.token = EXCLUDED.token OR held.refreshed_at < now() - make_interval(secs => $5) '
        'RETURNING token'
    ),
    'refresh': 'UPDATE {schema}.archive_locks SET refreshed_at = now() WHERE cluster = $1 AND token = $2',
    'is_held': 'SELECT 1 FROM {schema}.archive_locks WHERE cluster = $1 AND token = $2',
    'release': 'DELETE FROM {schema}.archive_locks WHERE cluster = $1 AND token = $2',
}

# SQLSTATE classes a retry can cure: connection exceptions (08), insufficient
# resources (53), operator intervention such as a restart (57), and the two
# codes a transaction asks to be run again with (serialization failure,
# deadlock). Matched as a prefix, so a two-character entry covers a class.
TRANSIENT_SQLSTATE_PREFIXES = ('08', '53', '57', '40001', '40P01')

MISSING_TABLE_MESSAGE = (
    'table {schema}.{table} does not exist and connections.postgres.{connection}.manage_schema is false; '
    'create it with: drakkar-schema postgres --schema={schema}'
)


def render(template: str, schema: str) -> str:
    """Return ``template`` with every ``{schema}`` replaced by ``schema``."""
    return template.replace(SCHEMA_PLACEHOLDER, schema)


def statements_for(schema: str) -> dict[str, str]:
    """Return every statement text rendered for ``schema``, by statement name."""
    return {name: render(template, schema) for name, template in STATEMENT_TEMPLATES.items()}


def ddl_for(schema: str) -> list[str]:
    """Return the DDL statements rendered for ``schema``, in the order they must run."""
    return [render(template, schema) for template in DDL_TEMPLATES]


def render_schema_sql(schema: str) -> str:
    """Return the DDL as a script: each statement ends with ``;`` and a newline, one empty line between."""
    return '\n'.join(f'{statement};\n' for statement in ddl_for(schema))


def missing_table_message(schema: str, table: str, connection: str) -> str:
    """Return the error text for a table that is absent while the schema is not managed."""
    return MISSING_TABLE_MESSAGE.format(schema=schema, table=table, connection=connection)


def is_transient_sqlstate(sqlstate: str | None) -> bool:
    """Tell whether a SQLSTATE belongs to a class that a retry can cure."""
    return sqlstate is not None and sqlstate.startswith(TRANSIENT_SQLSTATE_PREFIXES)


def classify(exc: Exception) -> Exception:
    """Mark a transient Postgres error for the provider caller's retry rule.

    A ``PostgresError`` with a transient SQLSTATE becomes a ``TransientError``
    with the same text and the original as ``__cause__``. Anything else is
    returned unchanged.
    """
    # asyncpg sets ``sqlstate`` on each error class, but its type stubs do not declare it.
    if isinstance(exc, asyncpg.PostgresError) and is_transient_sqlstate(getattr(exc, 'sqlstate', None)):
        transient = TransientError(str(exc))
        transient.__cause__ = exc
        return transient
    return exc


async def open_pool(settings: PostgresConnectionConfig) -> asyncpg.Pool:
    """Open the pool for a connection entry.

    No ``command_timeout`` is set: the provider caller puts its own time limit
    on every attempt.
    """
    return await asyncpg.create_pool(dsn=settings.dsn, min_size=settings.pool_min, max_size=settings.pool_max)


async def close_pool(pool: asyncpg.Pool) -> None:
    """Close the pool and wait for its connections to end."""
    await pool.close()


@asynccontextmanager
async def borrow(pool: Any) -> AsyncIterator[Any]:
    """Borrow a connection from ``pool``; a transient Postgres error leaves as ``TransientError``.

    ``pool`` is typed ``Any`` so a test can hand in a fake that has ``acquire()``.
    """
    try:
        async with pool.acquire() as conn:
            yield conn
    except asyncpg.PostgresError as exc:
        classified = classify(exc)
        if classified is exc:
            raise
        raise classified from exc


async def ensure_tables(
    pool: Any,
    *,
    schema: str,
    manage_schema: bool,
    connection: str,
    tables: Sequence[str],
) -> None:
    """Make the tables a feature needs available.

    With ``manage_schema`` the DDL runs in one transaction, one statement at a
    time, after a transaction-scoped advisory lock on the schema name. The
    lock makes workers that start together take turns: concurrent
    ``CREATE ... IF NOT EXISTS`` statements can otherwise fail with a
    duplicate-object error that no retry cures. Without ``manage_schema`` each
    table is only checked, and the first missing one raises ``RuntimeError``
    naming the schema command that creates it. ``tables`` is ignored when the
    schema is managed (every table of the corpus is created) and only drives
    the checks otherwise.
    """
    async with borrow(pool) as conn:
        if not manage_schema:
            check_sql = render(STATEMENT_TEMPLATES['check_table'], schema)
            for table in tables:
                if not await conn.fetchval(check_sql, f'{schema}.{table}'):
                    raise RuntimeError(missing_table_message(schema, table, connection))
            return
        async with conn.transaction():
            await conn.execute(STATEMENT_TEMPLATES['ddl_lock'], schema)
            for statement in ddl_for(schema):
                await conn.execute(statement)
