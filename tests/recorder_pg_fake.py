"""A recording fake of the recorder's write pool, for the stage P tests.

``RecorderFakePostgres.open_pool`` stands in for
:func:`drakkar.providers.postgres.open_pool`: it records the connection
settings it was given and hands back a pool whose connections answer the
recorder's statements. Every statement, transaction step, COPY, terminate
and close is appended to ``calls`` in order, so a test sees what the writer
sent and in which order. A COPY lands in ``copied`` only when its transaction
commits.

A test makes the next COPYs fail with ``fail_copies`` (one error per COPY,
in order; an error the client raises while it encodes a record is raised the
same way), makes every COPY hang with ``hang_copies``, makes the pool close
hang with ``hang_close``, and makes the pool open fail with ``open_error``. The fake is driven from the stage P thread's
loop and read from the test's loop; each access is one plain list or
attribute operation.

The fake also answers the partition upkeep: ``try_lock`` answers
``try_lock_answer``, ``server_now`` answers ``server_now`` (the worker's wall
clock when it is ``None``), ``retention_read`` answers ``retention_comment``,
and ``partitions_list`` lists ``partitions``. A partition create or drop
changes ``partitions`` when its transaction commits; ``fail_partition_ddl``
makes the next ones raise. With ``require_partition`` a COPY of a record
whose ``ts`` hour has no partition raises the check violation a real server
raises.

``connect`` stands in for :func:`asyncpg.connect` for the history search: it
records the DSN and hands back one connection. The trace statement answers
``trace_rows``; ``connect_error`` and ``trace_error`` make the connect or the
trace raise, and ``hang_connect`` and ``hang_trace`` make them wait until they
are cancelled. ``connections`` lists every connection handed out.
"""

import asyncio
import re
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import asyncpg

from drakkar.config.connections import PostgresConnectionConfig
from drakkar.providers.postgres import (
    RECORDER_STATEMENT_TEMPLATES,
    SECONDS_PER_HOUR,
    STATEMENT_TEMPLATES,
    partition_name,
)

HANG_SECONDS = 3600
# The partition DDL shapes of PARTITION_DDL_TEMPLATES; group 1 is the partition name.
CREATE_PARTITION_RE = re.compile(r'CREATE TABLE IF NOT EXISTS [a-z0-9_]+\.([a-z0-9_]+) PARTITION OF ')
DROP_PARTITION_RE = re.compile(r'DROP TABLE IF EXISTS [a-z0-9_]+\.([a-z0-9_]+)$')
# How the rendered trace statement starts, whatever its schema and table.
TRACE_PREFIX = RECORDER_STATEMENT_TEMPLATES['trace'].split('{', 1)[0]
# The ts column of a COPY record: after cluster, worker, instance and id.
RECORD_TS_INDEX = 4


class RecorderFakePostgres:
    """The server side of the fake: what exists, what landed and what was sent."""

    def __init__(self, *, table_exists: bool = True) -> None:
        self.table_exists = table_exists
        self.calls: list[tuple[Any, ...]] = []
        self.copied: list[tuple] = []
        self.opened: list[PostgresConnectionConfig] = []
        self.open_error: BaseException | None = None
        self.copy_failures: list[BaseException] = []
        self.hang = False
        self.hang_close = False
        self.copies_started = 0
        self.try_lock_answer = True
        self.server_now: float | None = None
        self.retention_comment: str | None = 'retention_hours=6'
        self.partitions: set[str] = set()
        self.partition_ddl_failures: list[BaseException] = []
        self.require_partition = False
        self.connections: list[FakeConnection] = []
        self.connect_error: BaseException | None = None
        self.hang_connect = False
        self.trace_rows: list[dict[str, Any]] = []
        self.trace_error: BaseException | None = None
        self.hang_trace = False

    async def open_pool(self, settings: PostgresConnectionConfig) -> 'FakePool':
        """Record the settings and open a pool, or raise ``open_error``."""
        self.opened.append(settings)
        if self.open_error is not None:
            raise self.open_error
        return FakePool(self)

    async def connect(self, *, dsn: str) -> 'FakeConnection':
        """Record the DSN and open one connection, or raise ``connect_error``."""
        self.calls.append(('connect', dsn))
        if self.hang_connect:
            await asyncio.sleep(HANG_SECONDS)
        if self.connect_error is not None:
            raise self.connect_error
        conn = FakeConnection(self)
        self.connections.append(conn)
        return conn

    def fail_copies(self, *errors: BaseException) -> None:
        """Make the next COPYs raise these errors, one per COPY."""
        self.copy_failures.extend(errors)

    def fail_partition_ddl(self, *errors: BaseException) -> None:
        """Make the next partition creates or drops raise these errors, one per statement."""
        self.partition_ddl_failures.extend(errors)

    def statements(self, name: str) -> list[tuple[Any, ...]]:
        """The calls that sent the recorder statement ``name``, in order."""
        sql = RECORDER_STATEMENT_TEMPLATES[name]
        return [call for call in self.calls if len(call) > 1 and call[1] == sql]

    def partition_ddl(self) -> list[str]:
        """The partition creates and drops sent, committed or not, in order."""
        return [
            call[1]
            for call in self.calls
            if call[0] == 'execute' and (CREATE_PARTITION_RE.match(call[1]) or DROP_PARTITION_RE.match(call[1]))
        ]

    def hang_copies(self) -> None:
        """Make every COPY from now on wait until it is cancelled."""
        self.hang = True

    def copy_calls(self) -> list[tuple[Any, ...]]:
        """The COPYs sent, landed or not, in order."""
        return [call for call in self.calls if call[0] == 'copy']

    def copied_ids(self) -> list[int]:
        """The event ids of the rows that landed; the id is the fourth COPY column."""
        return [record[3] for record in self.copied]


class FakePool:
    """The slice of an asyncpg pool the writer uses: ``acquire()``, ``close()`` and ``terminate()``."""

    def __init__(self, server: RecorderFakePostgres) -> None:
        self._server = server

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator['FakeConnection']:
        yield FakeConnection(self._server)

    async def close(self) -> None:
        self._server.calls.append(('close',))
        if self._server.hang_close:
            await asyncio.sleep(HANG_SECONDS)

    def terminate(self) -> None:
        self._server.calls.append(('pool_terminate',))


class FakeConnection:
    """Answers ``check_table``, records every other statement, and runs COPYs inside a fake transaction."""

    def __init__(self, server: RecorderFakePostgres) -> None:
        self._server = server
        self._pending: list[tuple] = []
        # Partition changes of the open transaction: (name, True) for a create, (name, False) for a drop.
        self._pending_partitions: list[tuple[str, bool]] = []
        self.closed = False
        self.terminated = False

    async def execute(self, sql: str, *args: Any) -> str:
        server = self._server
        server.calls.append(('execute', sql, args))
        created = CREATE_PARTITION_RE.match(sql)
        dropped = DROP_PARTITION_RE.match(sql)
        if created or dropped:
            if server.partition_ddl_failures:
                raise server.partition_ddl_failures.pop(0)
            match = created or dropped
            assert match is not None
            self._pending_partitions.append((match.group(1), created is not None))
        return 'SELECT 1'

    async def fetchval(self, sql: str, *args: Any) -> Any:
        server = self._server
        server.calls.append(('fetchval', sql, args))
        if sql == STATEMENT_TEMPLATES['check_table']:
            return server.table_exists
        if sql == RECORDER_STATEMENT_TEMPLATES['try_lock']:
            return server.try_lock_answer
        if sql == RECORDER_STATEMENT_TEMPLATES['server_now']:
            return server.server_now if server.server_now is not None else time.time()
        if sql == RECORDER_STATEMENT_TEMPLATES['retention_read']:
            return server.retention_comment
        raise AssertionError(f'statement not known to the fake: {sql}')

    async def fetch(self, sql: str, *args: Any) -> list[tuple[str]]:
        server = self._server
        server.calls.append(('fetch', sql, args))
        if sql == RECORDER_STATEMENT_TEMPLATES['partitions_list']:
            return [(name,) for name in sorted(server.partitions)]
        if sql.startswith(TRACE_PREFIX):
            if server.hang_trace:
                await asyncio.sleep(HANG_SECONDS)
            if server.trace_error is not None:
                raise server.trace_error
            return [dict(row) for row in server.trace_rows]
        raise AssertionError(f'statement not known to the fake: {sql}')

    async def copy_records_to_table(
        self, table_name: str, *, records: list[tuple], columns: Any = None, schema_name: str | None = None
    ) -> str:
        server = self._server
        server.copies_started += 1
        server.calls.append(('copy', schema_name, table_name, tuple(columns), list(records)))
        if server.hang:
            await asyncio.sleep(HANG_SECONDS)
        if server.copy_failures:
            raise server.copy_failures.pop(0)
        if server.require_partition:
            for record in records:
                hour = int(record[RECORD_TS_INDEX] // SECONDS_PER_HOUR)
                if partition_name(table_name, hour) not in server.partitions:
                    raise asyncpg.exceptions.CheckViolationError('no partition of relation found for row')
        self._pending = list(records)
        return f'COPY {len(records)}'

    def transaction(self) -> 'FakeTransaction':
        return FakeTransaction(self)

    def terminate(self) -> None:
        self._server.calls.append(('terminate',))
        self.terminated = True

    async def close(self) -> None:
        self._server.calls.append(('conn_close',))
        self.closed = True

    def is_closed(self) -> bool:
        return self.closed or self.terminated

    def commit_pending(self) -> None:
        server = self._server
        server.copied.extend(self._pending)
        self._pending = []
        for name, created in self._pending_partitions:
            if created:
                server.partitions.add(name)
            else:
                server.partitions.discard(name)
        self._pending_partitions = []

    def roll_back_pending(self) -> None:
        self._pending = []
        self._pending_partitions = []


class FakeTransaction:
    """``start``, ``commit`` and ``rollback`` of one transaction; a commit lands the COPY rows."""

    def __init__(self, conn: FakeConnection) -> None:
        self._conn = conn

    async def start(self) -> None:
        self._conn._server.calls.append(('BEGIN',))

    async def commit(self) -> None:
        self._conn._server.calls.append(('COMMIT',))
        self._conn.commit_pending()

    async def rollback(self) -> None:
        self._conn._server.calls.append(('ROLLBACK',))
        self._conn.roll_back_pending()
