"""An in-memory Postgres for the fleet provider tests.

``FakePostgres`` keeps the two tables of the fleet providers, ``fleet_workers``
and ``archive_locks``, on a clock the test moves. It answers only the exact
statement texts of the shared corpus (``drakkar.providers.postgres``) rendered
for its schema, and models what each one does, so a test both pins the bytes
a provider sends and watches the provider behave. A text the corpus does not
hold fails the test at once.

The pool API is the slice of asyncpg's the providers use: ``acquire()`` is an
async context manager that yields a connection with ``execute``, ``fetch``,
``fetchrow``, ``fetchval`` and ``transaction()``.
"""

from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from typing import Any

import asyncpg

from drakkar.providers.postgres import ARCHIVE_LOCKS_TABLE, FLEET_WORKERS_TABLE, ddl_for, statements_for

# What a statement comes to: the rows it returns and its command tag.
Result = tuple[list[tuple[Any, ...]], str]
Handler = Callable[[tuple[Any, ...]], Result]


@dataclass(frozen=True)
class WorkerRow:
    """One row of ``fleet_workers``; ``heartbeat_at`` is on the fake clock."""

    instance: str
    cluster: str
    card: str
    heartbeat_at: float


@dataclass(frozen=True)
class LockRow:
    """One row of ``archive_locks``; both times are on the fake clock."""

    token: str
    worker: str
    instance: str
    acquired_at: float
    refreshed_at: float


class FakePostgres:
    """In-memory fleet_workers and archive_locks behind the exact statement texts of the fixture, on a fake clock.

    ``fail_next`` is raised by the next statement and then cleared;
    ``fail_every`` is raised by every statement while it is set. Both let a
    test inject an asyncpg error. ``calls`` records every statement, in
    order, with ``('BEGIN', ())``, ``('COMMIT', ())`` and ``('ROLLBACK', ())``
    around a transaction.

    ``tables`` holds the names of the tables that exist (both, or none with
    ``tables_exist=False``); a test may set it to one of them. ``check_table``
    answers per table and each ``CREATE TABLE`` adds its own.
    """

    def __init__(self, schema: str = 'drakkar', *, tables_exist: bool = True) -> None:
        self.schema = schema
        self.tables: set[str] = {FLEET_WORKERS_TABLE, ARCHIVE_LOCKS_TABLE} if tables_exist else set()
        # Set by FakeConnection.transaction while its block runs.
        self.in_transaction = False
        self.open_transaction: FakeTransaction | None = None
        self.now = 1_000.0
        self.calls: list[tuple[str, tuple[Any, ...]]] = []
        self.workers: dict[str, WorkerRow] = {}
        self.locks: dict[str, LockRow] = {}
        self.fail_next: Exception | None = None
        self.fail_every: Exception | None = None
        statements = statements_for(schema)
        ddl = ddl_for(schema)
        self._handlers: dict[str, Handler] = {
            statements['check_table']: self._check_table,
            statements['ddl_lock']: self._ddl_lock,
            ddl[0]: lambda _args: ([], 'CREATE SCHEMA'),
            ddl[1]: self._create_table(FLEET_WORKERS_TABLE),
            ddl[2]: lambda _args: ([], 'CREATE INDEX'),
            ddl[3]: lambda _args: ([], 'CREATE INDEX'),
            ddl[4]: self._create_table(ARCHIVE_LOCKS_TABLE),
            statements['publish']: self._on_tables(self._publish, FLEET_WORKERS_TABLE),
            statements['list_prune']: self._on_tables(self._list_prune, FLEET_WORKERS_TABLE),
            statements['list_select']: self._on_tables(self._list_select, FLEET_WORKERS_TABLE),
            statements['withdraw']: self._on_tables(self._withdraw, FLEET_WORKERS_TABLE),
            statements['acquire_select']: self._on_tables(self._acquire_select, ARCHIVE_LOCKS_TABLE),
            statements['acquire']: self._on_tables(self._acquire, ARCHIVE_LOCKS_TABLE),
            statements['refresh']: self._on_tables(self._refresh, ARCHIVE_LOCKS_TABLE),
            statements['is_held']: self._on_tables(self._is_held, ARCHIVE_LOCKS_TABLE),
            statements['release']: self._on_tables(self._release, ARCHIVE_LOCKS_TABLE),
        }

    def advance(self, seconds: float) -> None:
        """Move the server clock forward; every row gets older."""
        self.now += seconds

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator['FakeConnection']:
        """Borrow a connection, as ``asyncpg.Pool.acquire`` does."""
        yield FakeConnection(self)

    def run(self, sql: str, args: tuple[Any, ...]) -> Result:
        """Record one statement, raise an injected failure, else answer it."""
        self.calls.append((sql, args))
        failure = self.fail_next or self.fail_every
        self.fail_next = None
        if failure is not None:
            raise failure
        handler = self._handlers.get(sql)
        if handler is None:
            raise AssertionError(f'statement not in the fixture: {sql}')
        return handler(args)

    def snapshot(self) -> tuple[dict[str, WorkerRow], dict[str, LockRow], set[str]]:
        """The table contents, for a transaction to put back on rollback (rows are immutable)."""
        return dict(self.workers), dict(self.locks), set(self.tables)

    def restore(self, saved: tuple[dict[str, WorkerRow], dict[str, LockRow], set[str]]) -> None:
        """Put back the table contents a :meth:`snapshot` took."""
        self.workers, self.locks, self.tables = dict(saved[0]), dict(saved[1]), set(saved[2])

    # --- the statements ---

    def _on_tables(self, handler: Handler, table: str) -> Handler:
        """A statement on ``table`` fails as Postgres does while that table does not exist."""

        def guarded(args: tuple[Any, ...]) -> Result:
            if table not in self.tables:
                raise asyncpg.exceptions.UndefinedTableError(f'relation "{self.schema}.{table}" does not exist')
            return handler(args)

        return guarded

    def _check_table(self, args: tuple[Any, ...]) -> Result:
        existing = {f'{self.schema}.{table}' for table in self.tables}
        return [(args[0] in existing,)], 'SELECT 1'

    def _ddl_lock(self, _args: tuple[Any, ...]) -> Result:
        # pg_advisory_xact_lock holds only until the transaction ends; outside one it protects nothing.
        if not self.in_transaction:
            raise AssertionError('ddl_lock ran outside a transaction')
        return [(None,)], 'SELECT 1'

    def _create_table(self, table: str) -> Handler:
        def create(_args: tuple[Any, ...]) -> Result:
            self.tables.add(table)
            return [], 'CREATE TABLE'

        return create

    def _publish(self, args: tuple[Any, ...]) -> Result:
        worker, instance, cluster, card = args
        self.workers[worker] = WorkerRow(instance=instance, cluster=cluster, card=card, heartbeat_at=self.now)
        return [], 'INSERT 0 1'

    def _list_prune(self, args: tuple[Any, ...]) -> Result:
        # heartbeat_at < now() - prune_after
        (prune_after,) = args
        stale = [worker for worker, row in self.workers.items() if row.heartbeat_at < self.now - prune_after]
        for worker in stale:
            del self.workers[worker]
        return [], f'DELETE {len(stale)}'

    def _list_select(self, _args: tuple[Any, ...]) -> Result:
        rows = [(worker, row.card, self.now - row.heartbeat_at) for worker, row in sorted(self.workers.items())]
        return rows, f'SELECT {len(rows)}'

    def _withdraw(self, args: tuple[Any, ...]) -> Result:
        worker, instance = args
        row = self.workers.get(worker)
        if row is None or row.instance != instance:
            return [], 'DELETE 0'
        del self.workers[worker]
        return [], 'DELETE 1'

    def _acquire_select(self, args: tuple[Any, ...]) -> Result:
        (scope,) = args
        row = self.locks.get(scope)
        if row is None:
            return [], 'SELECT 0'
        return [(row.token, row.worker, row.instance, self.now - row.refreshed_at)], 'SELECT 1'

    def _acquire(self, args: tuple[Any, ...]) -> Result:
        scope, token, worker, instance, lease_seconds = args
        held = self.locks.get(scope)
        if held is None:
            self.locks[scope] = LockRow(token, worker, instance, acquired_at=self.now, refreshed_at=self.now)
            return [(token,)], 'INSERT 0 1'
        # DO UPDATE ... WHERE held.token = EXCLUDED.token OR held.refreshed_at < now() - lease
        if held.token != token and not held.refreshed_at < self.now - lease_seconds:
            return [], 'INSERT 0 0'
        acquired_at = held.acquired_at if held.token == token else self.now
        self.locks[scope] = LockRow(token, worker, instance, acquired_at=acquired_at, refreshed_at=self.now)
        return [(token,)], 'INSERT 0 1'

    def _refresh(self, args: tuple[Any, ...]) -> Result:
        scope, token = args
        held = self.locks.get(scope)
        if held is None or held.token != token:
            return [], 'UPDATE 0'
        self.locks[scope] = replace(held, refreshed_at=self.now)
        return [], 'UPDATE 1'

    def _is_held(self, args: tuple[Any, ...]) -> Result:
        scope, token = args
        held = self.locks.get(scope)
        if held is None or held.token != token:
            return [], 'SELECT 0'
        return [(1,)], 'SELECT 1'

    def _release(self, args: tuple[Any, ...]) -> Result:
        scope, token = args
        held = self.locks.get(scope)
        if held is None or held.token != token:
            return [], 'DELETE 0'
        del self.locks[scope]
        return [], 'DELETE 1'


class FakeConnection:
    """One borrowed connection of a :class:`FakePostgres`; each method answers through its dispatch table."""

    def __init__(self, server: FakePostgres) -> None:
        self._server = server

    async def execute(self, sql: str, *args: Any) -> str:
        """The command tag, such as ``'UPDATE 1'``."""
        return self._server.run(sql, args)[1]

    async def fetch(self, sql: str, *args: Any) -> list[tuple[Any, ...]]:
        """Every row; each reads by position as an asyncpg ``Record`` does."""
        return self._server.run(sql, args)[0]

    async def fetchrow(self, sql: str, *args: Any) -> tuple[Any, ...] | None:
        """The first row, or ``None``."""
        rows = self._server.run(sql, args)[0]
        return rows[0] if rows else None

    async def fetchval(self, sql: str, *args: Any) -> Any:
        """The first column of the first row, or ``None``."""
        rows = self._server.run(sql, args)[0]
        return rows[0][0] if rows else None

    def transaction(self) -> 'FakeTransaction':
        """A transaction to start, as ``asyncpg.Connection.transaction`` returns."""
        return FakeTransaction(self._server)

    def terminate(self) -> None:
        """Close the connection; the server rolls back a transaction left open on it."""
        self._server.calls.append(('TERMINATE', ()))
        if self._server.open_transaction is not None:
            self._server.open_transaction.discard()


class FakeTransaction:
    """One transaction of a :class:`FakeConnection`; a rollback puts the tables back."""

    def __init__(self, server: FakePostgres) -> None:
        self._server = server
        self._saved: Any = None

    async def start(self) -> None:
        self._server.calls.append(('BEGIN', ()))
        self._saved = self._server.snapshot()
        self._server.in_transaction = True
        self._server.open_transaction = self

    async def commit(self) -> None:
        self._server.calls.append(('COMMIT', ()))
        self._close()

    async def rollback(self) -> None:
        self._server.calls.append(('ROLLBACK', ()))
        self.discard()

    def discard(self) -> None:
        """Put the tables back and close the transaction; the server's side of a rollback."""
        self._server.restore(self._saved)
        self._close()

    def _close(self) -> None:
        self._server.in_transaction = False
        self._server.open_transaction = None
