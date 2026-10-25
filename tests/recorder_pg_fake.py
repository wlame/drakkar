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
"""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from drakkar.config.connections import PostgresConnectionConfig
from drakkar.providers.postgres import STATEMENT_TEMPLATES

HANG_SECONDS = 3600


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

    async def open_pool(self, settings: PostgresConnectionConfig) -> 'FakePool':
        """Record the settings and open a pool, or raise ``open_error``."""
        self.opened.append(settings)
        if self.open_error is not None:
            raise self.open_error
        return FakePool(self)

    def fail_copies(self, *errors: BaseException) -> None:
        """Make the next COPYs raise these errors, one per COPY."""
        self.copy_failures.extend(errors)

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

    async def execute(self, sql: str, *args: Any) -> str:
        self._server.calls.append(('execute', sql, args))
        return 'SELECT 1'

    async def fetchval(self, sql: str, *args: Any) -> Any:
        self._server.calls.append(('fetchval', sql, args))
        if sql == STATEMENT_TEMPLATES['check_table']:
            return self._server.table_exists
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
        self._pending = list(records)
        return f'COPY {len(records)}'

    def transaction(self) -> 'FakeTransaction':
        return FakeTransaction(self)

    def terminate(self) -> None:
        self._server.calls.append(('terminate',))

    def commit_pending(self) -> None:
        self._server.copied.extend(self._pending)
        self._pending = []


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
        self._conn._pending = []
