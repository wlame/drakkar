"""Postgres statement corpus, rendering and pool helpers shared by the fleet providers.

The worker registry and the election both keep their state in Postgres
tables. This module holds what the two have in common: the DDL and every
statement text, the rule that fills in the connection's schema, the pool that
the shared ``connections`` section opens, and the mapping of Postgres errors
to the provider caller's retry rule.

The texts are pinned by ``tests/fixtures/fleet_providers.json`` so the emitted
bytes are identical for any implementation. ``{schema}`` in a text stands for
the schema of the connection; it is validated as a lower-case identifier when
the config loads, so a plain replace is safe.

The recorder events table of the ``postgres`` recorder mode has its own part of
the corpus below the providers' part: the start DDL, the hourly partitions,
the upkeep and search statements, the COPY columns and the names.
"""

import asyncio
import re
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager, suppress
from datetime import UTC, datetime
from typing import Any

import asyncpg

from drakkar.config.connections import PostgresConnectionConfig
from drakkar.providers.resilience import TransientError

POSTGRES_PROVIDER = 'postgres'
SCHEMA_PLACEHOLDER = '{schema}'
FLEET_WORKERS_TABLE = 'fleet_workers'
TASK_LOCKS_TABLE = 'task_locks'
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
    'CREATE TABLE IF NOT EXISTS {schema}.task_locks (\n'
    '    scope         text        PRIMARY KEY,\n'
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
        'FROM {schema}.task_locks WHERE scope = $1 FOR UPDATE'
    ),
    'acquire': (
        'INSERT INTO {schema}.task_locks AS held (scope, token, worker, instance, acquired_at, refreshed_at) '
        'VALUES ($1, $2, $3, $4, now(), now()) '
        'ON CONFLICT (scope) DO UPDATE SET token = EXCLUDED.token, worker = EXCLUDED.worker, '
        'instance = EXCLUDED.instance, '
        'acquired_at = CASE WHEN held.token = EXCLUDED.token THEN held.acquired_at ELSE now() END, '
        'refreshed_at = now() '
        'WHERE held.token = EXCLUDED.token OR held.refreshed_at < now() - make_interval(secs => $5) '
        'RETURNING token'
    ),
    'refresh': 'UPDATE {schema}.task_locks SET refreshed_at = now() WHERE scope = $1 AND token = $2',
    'is_held': 'SELECT 1 FROM {schema}.task_locks WHERE scope = $1 AND token = $2',
    'release': 'DELETE FROM {schema}.task_locks WHERE scope = $1 AND token = $2',
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


RECORDER_TABLE = 'recorder_events'
RECORDER_FEATURE = 'recorder'
RECORDER_PLACEHOLDERS: tuple[str, ...] = ('{schema}', '{table}', '{partition}', '{from}', '{to}', '{retention_hours}')

# Run in this order when the table is missing and the schema is managed. The
# GIN index has no pending list (fastupdate = off): with one, the pending-list
# flush lands inside some COPYs and makes the slow tail of the writes longer.
RECORDER_DDL_TEMPLATES: tuple[str, ...] = (
    'CREATE SCHEMA IF NOT EXISTS {schema}',
    'CREATE TABLE IF NOT EXISTS {schema}.{table} (\n'
    '    cluster        text             NOT NULL,\n'
    '    worker         text             NOT NULL,\n'
    '    instance       text             NOT NULL,\n'
    '    id             bigint           NOT NULL,\n'
    '    ts             double precision NOT NULL,\n'
    '    dt             text             NOT NULL,\n'
    '    event          text             NOT NULL,\n'
    '    "partition"    integer,\n'
    '    "offset"       bigint,\n'
    '    task_id        text,\n'
    '    args           text,\n'
    '    stdout_size    bigint,\n'
    '    stdout         text,\n'
    '    stderr         text,\n'
    '    exit_code      integer,\n'
    '    duration       double precision,\n'
    '    output_topic   text,\n'
    '    metadata       text,\n'
    '    pid            integer,\n'
    '    labels         text,\n'
    '    origin         text             NOT NULL,\n'
    '    client_name    text,\n'
    '    request_id     text,\n'
    '    trace_offsets  bigint[],\n'
    '    PRIMARY KEY (instance, id, ts)\n'
    ') PARTITION BY RANGE (ts)',
    "COMMENT ON TABLE {schema}.{table} IS 'retention_hours={retention_hours}'",
    'CREATE INDEX IF NOT EXISTS {table}_partition_offset_idx ON {schema}.{table} ("partition", "offset")',
    'CREATE INDEX IF NOT EXISTS {table}_task_id_idx ON {schema}.{table} (task_id)',
    'CREATE INDEX IF NOT EXISTS {table}_trace_offsets_idx ON {schema}.{table} USING gin (trace_offsets) '
    'WITH (fastupdate = off) WHERE trace_offsets IS NOT NULL',
)

# One partition per UTC hour; {from} and {to} are whole epoch seconds.
PARTITION_DDL_TEMPLATES: dict[str, str] = {
    'create': 'CREATE TABLE IF NOT EXISTS {schema}.{partition} PARTITION OF {schema}.{table} FOR VALUES FROM ({from}) TO ({to})',
    'drop': 'DROP TABLE IF EXISTS {schema}.{partition}',
}

# The recorder's own statements. ``check_table`` and ``ddl_lock`` are shared
# with the providers and live in STATEMENT_TEMPLATES.
_TRACE_COLUMNS = (
    'e.instance, e.worker, e.id, e.ts, e.dt, e.event, e."partition", e."offset", e.task_id, e.args, '
    'e.stdout_size, e.stdout, e.stderr, e.exit_code, e.duration, e.output_topic, e.metadata, e.pid, e.labels, '
    'e.origin, e.client_name, e.request_id'
)
RECORDER_STATEMENT_TEMPLATES: dict[str, str] = {
    # SET takes no bind parameter; set_config with true is local to the transaction.
    'set_local': 'SELECT set_config($1, $2, true)',
    'try_lock': 'SELECT pg_try_advisory_xact_lock(hashtext($1))',
    'server_now': 'SELECT extract(epoch FROM now())::float8',
    'retention_read': "SELECT obj_description(to_regclass($1), 'pg_class')",
    'partitions_list': (
        'SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid '
        'WHERE i.inhparent = to_regclass($1) ORDER BY c.relname'
    ),
    # $1 is the schema and $2 the partition names; a name with no table is not answered.
    'partition_sizes': (
        'SELECT c.relname, pg_total_relation_size(c.oid) FROM pg_class c '
        'JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relname = ANY($2::text[])'
    ),
    # Three branches, each served by its own index, joined by UNION ALL; the
    # key-only DISTINCT ON removes a row two branches found without hashing the
    # output texts. No join back to the table: with many partitions that join
    # turns into a hash join that reads every partition.
    'trace': (
        f'WITH hits AS ( SELECT {_TRACE_COLUMNS} FROM {{schema}}.{{table}} e '
        'WHERE e.cluster = $1 AND e."partition" = $2 AND e."offset" = $3::bigint '
        f'UNION ALL SELECT {_TRACE_COLUMNS} FROM {{schema}}.{{table}} e '
        'WHERE e.cluster = $1 AND e."partition" = $2 AND e.task_id IN ( SELECT s.task_id FROM {schema}.{table} s '
        'WHERE s.cluster = $1 AND s."partition" = $2 AND s.event = \'task_started\' '
        'AND s.trace_offsets @> ARRAY[$3::bigint]) '
        f'UNION ALL SELECT {_TRACE_COLUMNS} FROM {{schema}}.{{table}} e '
        "WHERE e.cluster = $1 AND e.\"partition\" = $2 AND e.event IN ('annotation', 'offload') "
        'AND e.trace_offsets @> ARRAY[$3::bigint]) '
        'SELECT d.id, d.ts, d.dt, d.event, d."partition", d."offset", d.task_id, d.args, d.stdout_size, d.stdout, '
        'd.stderr, d.exit_code, d.duration, d.output_topic, d.metadata, d.pid, d.labels, d.origin, d.client_name, '
        'd.request_id, d.worker AS worker_name '
        'FROM (SELECT DISTINCT ON (h.instance, h.id, h.ts) h.* FROM hits h ORDER BY h.instance, h.id, h.ts) d '
        'ORDER BY d.ts, d.worker, d.instance, d.id'
    ),
}

# The writer identity, the 20 recorder event columns in their pinned order,
# then the trace index column. Written out rather than derived, so this module
# stays free of the recorder package.
COPY_COLUMNS: tuple[str, ...] = (
    'cluster',
    'worker',
    'instance',
    'id',
    'ts',
    'dt',
    'event',
    'partition',
    'offset',
    'task_id',
    'args',
    'stdout_size',
    'stdout',
    'stderr',
    'exit_code',
    'duration',
    'output_topic',
    'metadata',
    'pid',
    'labels',
    'origin',
    'client_name',
    'request_id',
    'trace_offsets',
)

PARTITIONS_AHEAD_HOURS = 3
PARTITION_MAINTENANCE_SECONDS = 300
LOCK_TIMEOUT_MS = 3000
POSTGRES_CHUNK_MAX_BYTES = 4 * 1024 * 1024
POSTGRES_CHUNK_FINAL_FAILURES = 2
SQLSTATE_UNIQUE_VIOLATION = '23505'
SQLSTATE_CHECK_VIOLATION = '23514'
SQLSTATE_LOCK_NOT_AVAILABLE = '55P03'
SQLSTATE_UNDEFINED_TABLE = '42P01'
# SQLSTATE classes that the rows of a recorder COPY cause: data exceptions (22)
# and integrity constraint violations (23). Only these count toward dropping a
# chunk; a COPY that fails with any other server error keeps its chunk, since
# the same rows can land once the server is fixed. Matched as a prefix.
RECORDER_FINAL_SQLSTATE_CLASSES = ('22', '23')
SECONDS_PER_HOUR = 3600
RETENTION_HOURS_MIN = 1
RETENTION_HOURS_MAX = 168

# Values the recorder passes to set_local; statement_timeout gets the search's
# time limit in whole milliseconds.
SETTING_JIT = 'jit'
SETTING_LOCK_TIMEOUT = 'lock_timeout'
SETTING_SYNCHRONOUS_COMMIT = 'synchronous_commit'
STATEMENT_TIMEOUT_SETTING = 'statement_timeout'
SET_LOCAL_VALUES: dict[str, str] = {
    SETTING_JIT: 'off',
    SETTING_LOCK_TIMEOUT: str(LOCK_TIMEOUT_MS),
    SETTING_SYNCHRONOUS_COMMIT: 'off',
}
# The statement_timeout of a COPY transaction: no server limit, so a limit
# set for the role or the database never cuts a COPY; the attempt time limit
# of the provider caller bounds it instead.
COPY_STATEMENT_TIMEOUT = '0'

RECORDER_MISSING_TABLE_MESSAGE = (
    'table {schema}.{table} does not exist and connections.postgres.{connection}.manage_schema is false; '
    'create it with: {command}'
)
RECORDER_MISSING_TABLE_COMMAND = (
    'drakkar-schema postgres --schema={schema} --recorder --retention-hours={retention_hours}'
)
RECORDER_MISSING_TABLE_PREFIX_FLAG = ' --table-prefix={prefix}'

_PARTITION_DIGITS = 10
_RETENTION_COMMENT_RE = re.compile(r'retention_hours=([0-9]+)')


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


def recorder_table_name(prefix: str) -> str:
    """Return the events table name: ``recorder_events``, or ``<prefix>_recorder_events``."""
    return f'{prefix}_{RECORDER_TABLE}' if prefix else RECORDER_TABLE


def partition_name(table: str, hour: int) -> str:
    """Return the name of the partition of epoch hour ``hour``: ``{table}_pYYYYMMDDHH`` in UTC."""
    moment = datetime.fromtimestamp(hour * SECONDS_PER_HOUR, UTC)
    # Formatted by hand: strftime does not pad a year below 1000 on every platform.
    return f'{table}_p{moment.year:04d}{moment.month:02d}{moment.day:02d}{moment.hour:02d}'


def partition_bounds(hour: int) -> tuple[int, int]:
    """Return the ``ts`` range of the partition of epoch hour ``hour``, in whole epoch seconds."""
    return hour * SECONDS_PER_HOUR, (hour + 1) * SECONDS_PER_HOUR


def parse_partition_hour(table: str, name: str) -> int | None:
    """Return the epoch hour of a partition name of ``table``, or ``None`` when the name is not one.

    The name must be ``{table}_p`` and ten digits that form a real UTC hour,
    and it must be exactly the name :func:`partition_name` gives for that hour.
    """
    match = re.fullmatch(re.escape(table) + rf'_p([0-9]{{{_PARTITION_DIGITS}}})', name)
    if match is None:
        return None
    digits = match.group(1)
    try:
        moment = datetime(int(digits[:4]), int(digits[4:6]), int(digits[6:8]), int(digits[8:]), tzinfo=UTC)
    except ValueError:
        return None
    hour = int(moment.timestamp()) // SECONDS_PER_HOUR
    return hour if partition_name(table, hour) == name else None


def parse_retention_comment(text: str | None) -> int | None:
    """Return the retention stored in a table comment, or ``None`` when the comment holds no valid one.

    Valid is exactly ``retention_hours=<digits>`` with a value from 1 to 168.
    """
    if text is None:
        return None
    match = _RETENTION_COMMENT_RE.fullmatch(text)
    if match is None:
        return None
    hours = int(match.group(1))
    return hours if RETENTION_HOURS_MIN <= hours <= RETENTION_HOURS_MAX else None


def render_recorder(
    template: str,
    *,
    schema: str,
    table: str,
    retention_hours: int = 0,
    partition: str = '',
    from_: int = 0,
    to: int = 0,
) -> str:
    """Return a recorder template with its six placeholders replaced.

    Every value is a checked lower-case identifier or a whole number, so a
    plain replace is safe.
    """
    values = {
        '{schema}': schema,
        '{table}': table,
        '{partition}': partition,
        '{from}': str(from_),
        '{to}': str(to),
        '{retention_hours}': str(retention_hours),
    }
    for placeholder, value in values.items():
        template = template.replace(placeholder, value)
    return template


def recorder_ddl_for(schema: str, table: str, retention_hours: int) -> list[str]:
    """Return the recorder start DDL rendered for one table, in the order it must run."""
    return [
        render_recorder(template, schema=schema, table=table, retention_hours=retention_hours)
        for template in RECORDER_DDL_TEMPLATES
    ]


def render_recorder_schema_sql(schema: str, table: str, retention_hours: int) -> str:
    """Return the recorder DDL as a script, in the format of :func:`render_schema_sql`."""
    return '\n'.join(f'{statement};\n' for statement in recorder_ddl_for(schema, table, retention_hours))


def recorder_missing_table_message(schema: str, table: str, connection: str, retention_hours: int, prefix: str) -> str:
    """Return the error text for a missing recorder table while the schema is not managed.

    The command in the text gains ``--table-prefix`` only for a non-empty prefix.
    """
    command = RECORDER_MISSING_TABLE_COMMAND.format(schema=schema, retention_hours=retention_hours)
    if prefix:
        command += RECORDER_MISSING_TABLE_PREFIX_FLAG.format(prefix=prefix)
    return RECORDER_MISSING_TABLE_MESSAGE.format(schema=schema, table=table, connection=connection, command=command)


def is_transient_sqlstate(sqlstate: str | None) -> bool:
    """Tell whether a SQLSTATE belongs to a class that a retry can cure."""
    return sqlstate is not None and sqlstate.startswith(TRANSIENT_SQLSTATE_PREFIXES)


def is_recorder_final_sqlstate(sqlstate: str | None) -> bool:
    """Tell whether a SQLSTATE of a recorder COPY is caused by its rows (a data or integrity error)."""
    return sqlstate is not None and sqlstate.startswith(RECORDER_FINAL_SQLSTATE_CLASSES)


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

    A cancelled block (the caller's time limit) closes the connection's socket
    before the pool takes it back. The pool's release otherwise waits, shielded
    from cancellation, for the server to confirm the cancel of the running
    query; a server that does not answer would hold the call past its limit.
    The server is not asked to cancel the statement, so it runs to its end
    there; the statements of the providers are small, so that is harmless.
    ``pool`` is typed ``Any`` so a test can hand in a fake that has ``acquire()``.
    """
    try:
        async with pool.acquire() as conn:
            try:
                yield conn
            except asyncio.CancelledError:
                # The pool proxy raises InterfaceError once the connection is
                # detached; the cancel must still leave as it came.
                with suppress(asyncpg.InterfaceError):
                    conn.terminate()
                raise
    except asyncpg.PostgresError as exc:
        classified = classify(exc)
        if classified is exc:
            raise
        raise classified from exc


@asynccontextmanager
async def borrow_transaction(pool: Any) -> AsyncIterator[Any]:
    """Borrow a connection as :func:`borrow` does and run the block in one transaction.

    Commits after the block and rolls back on an error. A rollback that fails
    (the connection is gone) does not hide the error of the block, which
    leaves as it came. A cancelled block sends no ``ROLLBACK``: that would
    wait for the server, and :func:`borrow` closes the connection instead,
    which ends the transaction on the server.
    """
    async with borrow(pool) as conn:
        transaction = conn.transaction()
        await transaction.start()
        try:
            yield conn
        except asyncio.CancelledError:
            raise
        except BaseException:
            # The block's error is the one to report; a connection that broke
            # in the block also breaks the rollback, and the server ends the
            # transaction when the connection closes.
            with suppress(Exception):
                await transaction.rollback()
            raise
        await transaction.commit()


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
    if not manage_schema:
        check_sql = render(STATEMENT_TEMPLATES['check_table'], schema)
        async with borrow(pool) as conn:
            for table in tables:
                if not await conn.fetchval(check_sql, f'{schema}.{table}'):
                    raise RuntimeError(missing_table_message(schema, table, connection))
        return
    async with borrow_transaction(pool) as conn:
        await conn.execute(STATEMENT_TEMPLATES['ddl_lock'], schema)
        for statement in ddl_for(schema):
            await conn.execute(statement)


async def ensure_recorder_table(
    pool: Any,
    *,
    schema: str,
    table: str,
    retention_hours: int,
    manage_schema: bool,
    connection: str,
    prefix: str,
) -> None:
    """Make the recorder events table available; never touches the providers' tables.

    A table that exists costs one read-only check and no lock. A missing table
    raises ``RuntimeError`` naming the schema command when the schema is not
    managed. Otherwise one transaction takes the schema's advisory lock, bounds
    its lock waits, checks again (another worker may have created the table
    while this one waited) and runs the recorder DDL only if it is still
    missing. Partitions are not created here.
    """
    check_sql = STATEMENT_TEMPLATES['check_table']
    qualified = f'{schema}.{table}'
    async with borrow(pool) as conn:
        if await conn.fetchval(check_sql, qualified):
            return
    if not manage_schema:
        raise RuntimeError(recorder_missing_table_message(schema, table, connection, retention_hours, prefix))
    async with borrow_transaction(pool) as conn:
        await conn.execute(STATEMENT_TEMPLATES['ddl_lock'], schema)
        await conn.execute(
            RECORDER_STATEMENT_TEMPLATES['set_local'], SETTING_LOCK_TIMEOUT, SET_LOCAL_VALUES[SETTING_LOCK_TIMEOUT]
        )
        if await conn.fetchval(check_sql, qualified):
            return
        for statement in recorder_ddl_for(schema, table, retention_hours):
            await conn.execute(statement)
