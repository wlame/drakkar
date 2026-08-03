"""PostgreSQL sink — inserts rows into database tables.

Wraps asyncpg connection pool. Each PostgresPayload's data field is
serialized via model_dump() to get a column-name → value mapping,
then inserted into the specified table.
"""

import time
from dataclasses import dataclass
from typing import Protocol

import asyncpg
import structlog

from drakkar.config import PostgresSinkConfig
from drakkar.metrics import sink_deliver_duration, sink_deliver_errors, sink_payloads_delivered
from drakkar.models import PostgresPayload
from drakkar.pgsql import MAX_INSERT_PARAMS, quote_ident, render_insert
from drakkar.sinks.base import BaseSink

logger = structlog.get_logger()


@dataclass(frozen=True)
class _PgRow:
    """One payload reduced to its INSERT building blocks, identifiers quoted."""

    quoted_table: str
    quoted_columns: list[str]
    values: list[object]

    @property
    def group_key(self) -> tuple[str, tuple[str, ...]]:
        """Rows sharing this key can go in one multi-row INSERT."""
        return (self.quoted_table, tuple(self.quoted_columns))


class _HasGroupKey(Protocol):
    """Anything groupable into runs — see ``_group_into_runs``."""

    @property
    def group_key(self) -> tuple: ...


def _group_into_runs[UnitT: _HasGroupKey](units: list[UnitT]) -> list[list[UnitT]]:
    """Bucket units into consecutive runs sharing a ``group_key``.

    A unit merges only with its immediate neighbours, which guarantees
    execution order equals payload order. Global bucketing would be a
    slightly better batcher but reorders: payloads ``A(shape1), B(shape2),
    C(shape1)`` would execute as A, C, B, deferring B past C. That is
    harmless for INSERT — and is what this sink used to do — but for UPDATE
    it silently loses a write when two payloads target the same row.
    Restricting to runs also avoids reordering across ops, which mixing the
    two rules would reintroduce.

    Handlers overwhelmingly emit uniform payload lists, so runs are long in
    practice and the batching cost is small.
    """
    runs: list[list[UnitT]] = []
    for unit in units:
        if runs and runs[-1][0].group_key == unit.group_key:
            runs[-1].append(unit)
        else:
            runs.append([unit])
    return runs


def _build_multi_insert(rows: list[_PgRow]) -> tuple[str, list[object]]:
    """Build one INSERT covering every row (all share a table + column set)."""
    query = render_insert(rows[0].quoted_table, rows[0].quoted_columns, len(rows))
    values: list[object] = []
    for row in rows:
        values.extend(row.values)
    return query, values


class PostgresSink(BaseSink[PostgresPayload]):
    """Inserts rows into PostgreSQL tables.

    Each PostgresPayload is serialized:
        - table = payload.table (validated against SQL injection)
        - columns/values = payload.data.model_dump() dict

    Exposes the asyncpg pool via the `pool` property so users can
    access it in on_ready() for migrations or lookups.
    """

    sink_type = 'postgres'

    # Plain ``INSERT`` is NOT idempotent — a retry after a partial batch
    # or timeout can insert duplicate rows. We keep the safe default of
    # ``False`` so transient DB errors route to ``on_delivery_error``
    # instead of being auto-retried. Users whose schema has a unique
    # constraint + ``ON CONFLICT DO NOTHING`` (or ``ON CONFLICT DO
    # UPDATE`` for upsert semantics) can subclass and flip this to
    # ``True`` to opt into automatic transient-error retry.
    idempotent = False

    def __init__(self, name: str, config: PostgresSinkConfig) -> None:
        super().__init__(name, ui_url=config.ui_url)
        self._config = config
        self._pool: asyncpg.Pool | None = None

    @property
    def pool(self) -> asyncpg.Pool | None:
        """The asyncpg connection pool, available after connect().

        Useful for direct DB access in user hooks like on_ready().
        """
        return self._pool

    async def connect(self) -> None:
        """Create the asyncpg connection pool."""
        self._pool = await asyncpg.create_pool(
            dsn=self._config.dsn,
            min_size=self._config.pool_min,
            max_size=self._config.pool_max,
        )
        await logger.ainfo(
            'postgres_sink_connected',
            category='sink',
            sink_name=self._name,
            host=self._config.dsn.split('@')[-1],
        )

    async def deliver(self, payloads: list[PostgresPayload]) -> None:
        """Insert every payload into its target table.

        Each payload's data is serialized via ``model_dump()`` to a
        column-name → value mapping; the table and every column identifier
        are validated against SQL injection.

        Consecutive rows sharing a ``(table, column-set)`` are sent as ONE
        multi-row ``INSERT`` instead of one round-trip per payload. Grouping
        stops at the first differently-shaped neighbour, so what reaches the
        database is always in payload order. Failure granularity is
        preserved: a batch that fails is retried row-by-row so the error an
        operator sees names the offending row, exactly as the per-payload
        loop did. See the Go backend's ``internal/sinks/postgres.go`` — the
        two must stay observably identical (divergence #18 in its migration
        notes).
        """
        if not payloads or not self._pool:
            return

        start = time.monotonic()
        labels = {'sink_type': self.sink_type, 'sink_name': self._name}
        try:
            rows, bad_index, build_error = self._build_rows(payloads)
            async with self._pool.acquire() as conn:
                if build_error is not None:
                    # The per-payload loop executed every row BEFORE the
                    # invalid payload, then raised. Reproduce those side
                    # effects (an exec failure on the way takes precedence,
                    # exactly as the sequential loop would have hit first).
                    for row in rows[:bad_index]:
                        await self._exec_single(conn, row)
                    raise build_error
                for group in _group_into_runs(rows):
                    await self._deliver_group(conn, group)

            sink_payloads_delivered.labels(**labels).inc(len(payloads))
            sink_deliver_duration.labels(**labels).observe(time.monotonic() - start)
        except Exception:
            sink_deliver_errors.labels(**labels).inc()
            raise

    def _build_rows(self, payloads: list[PostgresPayload]) -> tuple[list[_PgRow], int, Exception | None]:
        """Validate and convert every payload up front.

        On the first bad payload returns the rows built so far, the failing
        index, and the error — the caller replays the legacy partial side
        effects before raising it.
        """
        rows: list[_PgRow] = []
        for i, payload in enumerate(payloads):
            try:
                if payload.data is None:
                    raise ValueError(f'PostgresPayload(op={payload.op.value!r}) requires {"data"!r}')
                data = payload.data.model_dump()
                rows.append(
                    _PgRow(
                        quoted_table=quote_ident(payload.table),
                        quoted_columns=[quote_ident(c) for c in data],
                        values=list(data.values()),
                    )
                )
            except Exception as e:
                return rows, i, e
        return rows, len(rows), None

    async def _deliver_group(self, conn: asyncpg.Connection, group: list[_PgRow]) -> None:
        """Insert one (table, column-set) group, chunked to the parameter cap."""
        # A payload whose data serializes to an empty mapping has zero
        # columns; dividing by zero would raise here instead of surfacing
        # the graceful "INSERT INTO t () ..." SQL error the per-payload
        # loop produced. Route those through the single-row path.
        columns = len(group[0].quoted_columns)
        rows_per_statement = max(MAX_INSERT_PARAMS // columns, 1) if columns else 1
        for start in range(0, len(group), rows_per_statement):
            chunk = group[start : start + rows_per_statement]
            if len(chunk) == 1:
                await self._exec_single(conn, chunk[0])
                continue
            query, values = _build_multi_insert(chunk)
            try:
                await conn.execute(query, *values)
            except Exception:
                # Batch failed — fall back to per-row delivery so the error
                # names the offending row (and to ride out a
                # statement-level transient).
                for row in chunk:
                    await self._exec_single(conn, row)

    @staticmethod
    async def _exec_single(conn: asyncpg.Connection, row: _PgRow) -> None:
        """Insert one row — the shape the pre-batching loop produced."""
        query, values = _build_multi_insert([row])
        await conn.execute(query, *values)

    async def close(self) -> None:
        """Close the connection pool."""
        if self._pool:
            try:
                await self._pool.close()
            except Exception as e:
                await logger.awarning(
                    'postgres_sink_close_error',
                    category='sink',
                    sink_name=self._name,
                    error=str(e),
                )
            self._pool = None
