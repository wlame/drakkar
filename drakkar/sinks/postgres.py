"""PostgreSQL sink — writes rows to database tables.

Wraps an asyncpg connection pool. A PostgresPayload's ``op`` selects what
the sink builds: an INSERT, an UPDATE, or an upsert, each from the
payload's own models serialized via ``model_dump()``. SQL construction
itself lives in ``drakkar.sinks.pgsql``, which has no I/O and no drakkar
imports, so every emitted statement is testable without a database.
"""

import time
from dataclasses import dataclass
from typing import Protocol

import asyncpg
import structlog
from pydantic import BaseModel

from drakkar.config import PostgresSinkConfig
from drakkar.metrics import (
    sink_batch_fallbacks,
    sink_deliver_duration,
    sink_deliver_errors,
    sink_payloads_delivered,
)
from drakkar.models import PostgresOp, PostgresPayload
from drakkar.sinks.base import BaseSink
from drakkar.sinks.pgsql import (
    MAX_INSERT_PARAMS,
    compile_named_statement,
    quote_ident,
    render_insert,
    render_update,
    render_upsert,
)
from drakkar.utils import redact_url

logger = structlog.get_logger()


@dataclass(frozen=True)
class _RowUnit:
    """An insert or upsert — contributes one VALUES tuple to a shared statement.

    These batch as ONE multi-row statement, so the SQL depends on how many
    units end up in the run and is rendered per chunk rather than stored.
    """

    op: PostgresOp
    quoted_table: str
    quoted_columns: tuple[str, ...]
    quoted_conflict: tuple[str, ...]
    quoted_update_columns: tuple[str, ...]
    values: list[object]

    @property
    def group_key(self) -> tuple:
        """Units sharing this key can go in one multi-row statement."""
        return (
            self.op,
            self.quoted_table,
            self.quoted_columns,
            self.quoted_conflict,
            self.quoted_update_columns,
        )

    def render(self, row_count: int) -> str:
        """Build the statement covering ``row_count`` units from this group."""
        columns = list(self.quoted_columns)
        if self.op is PostgresOp.INSERT:
            return render_insert(self.quoted_table, columns, row_count)
        return render_upsert(
            self.quoted_table,
            columns,
            row_count,
            list(self.quoted_conflict),
            list(self.quoted_update_columns),
        )


@dataclass(frozen=True)
class _StmtUnit:
    """An update or named statement — one execution of a fixed SQL string.

    The SQL does not vary with the number of units, so a run of these is sent
    with ``executemany``: one prepared statement, N argument tuples. The
    rendered SQL therefore IS the group key — it already encodes the table,
    the SET columns, and which predicate columns are IS NULL.
    """

    op: PostgresOp
    sql: str
    values: list[object]
    statement_name: str = ''

    @property
    def group_key(self) -> tuple:
        return (self.op, self.sql)


def _dump_required(model: BaseModel | None, field: str, op: PostgresOp) -> dict:
    """Serialize a payload sub-model to a non-empty column→value mapping.

    The non-empty check is the second of two guards on ``where``: the model
    validator rejects ``where=None`` at construction, and this rejects a model
    that *dumps* empty. Both are needed because a model mutated after the
    payload was constructed would slip past the validator, and an empty
    predicate renders an UPDATE with no WHERE — rewriting every row.
    """
    if model is None:
        raise ValueError(f'PostgresPayload(op={op.value!r}) requires {field!r}')
    dumped = model.model_dump()
    if not dumped:
        raise ValueError(f'PostgresPayload(op={op.value!r}) {field!r} serialized to an empty mapping')
    return dumped


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


class PostgresSink(BaseSink[PostgresPayload]):
    """Writes rows to PostgreSQL tables.

    Each PostgresPayload is serialized:
        - table = payload.table (validated against SQL injection)
        - columns/values = payload.data.model_dump() dict
        - predicate = payload.where.model_dump() dict, for an update

    Exposes the asyncpg pool via the `pool` property so users can
    access it in on_ready() for migrations or lookups.
    """

    sink_type = 'postgres'

    # Retry-safety is a property of the BATCH here, not of the sink, so the
    # real decision lives in ``batch_idempotent`` below. This flag stays
    # ``False`` as the conservative fallback for any code path that still
    # reads it directly.
    idempotent = False

    def batch_idempotent(self, payloads: list[PostgresPayload]) -> bool:
        """Retry-safe only when every payload converges on re-delivery.

        ``UPDATE`` with a literal ``SET`` against a fixed predicate, and
        ``INSERT ... ON CONFLICT DO UPDATE``, both reach the same state when
        applied twice. A plain ``INSERT`` duplicates rows. A named statement's
        SQL is opaque to the framework — ``attempts = attempts + 1`` is not
        idempotent and we cannot tell — so both veto the batch.

        Marking individual statements idempotent in configuration is a natural
        extension, deliberately left out for now.
        """
        return all(p.op in (PostgresOp.UPDATE, PostgresOp.UPSERT) for p in payloads)

    def __init__(self, name: str, config: PostgresSinkConfig) -> None:
        super().__init__(name, ui_url=config.ui_url)
        self._config = config
        self._pool: asyncpg.Pool | None = None
        # Operator-authored statements, compiled from :name to positional $n
        # once at connect(). Never compiled on the delivery path.
        self._statements: dict[str, tuple[str, list[str]]] = {}

    @property
    def pool(self) -> asyncpg.Pool | None:
        """The asyncpg connection pool, available after connect().

        Useful for direct DB access in user hooks like on_ready().
        """
        return self._pool

    async def connect(self) -> None:
        """Compile configured statements, then create the asyncpg pool.

        Compiling first means a malformed statement fails without leaving a
        pool behind. Config validation already rejects one, so this is the
        sink's own guard against a config built in code rather than YAML.
        """
        self._statements = {name: compile_named_statement(sql) for name, sql in self._config.statements.items()}
        self._pool = await asyncpg.create_pool(
            dsn=self._config.dsn,
            min_size=self._config.pool_min,
            max_size=self._config.pool_max,
            # Same budget the manager gives one delivery, so a wedged server
            # produces asyncpg's own timeout — which names the query — a
            # moment before the manager's outer wait_for would cancel it.
            command_timeout=self._delivery_timeout_seconds,
        )
        await logger.ainfo(
            'postgres_sink_connected',
            category='sink',
            sink_name=self._name,
            # The whole DSN goes through ``redact_url`` rather than a
            # split('@') — a DSN without an authority part (e.g.
            # ``postgresql://host/db?password=x``) would otherwise log
            # the secret verbatim.
            host=redact_url(self._config.dsn),
            statements=len(self._statements),
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
        if not payloads:
            return
        # ``deliver`` must raise on failure (BaseSink contract) — silently
        # returning here would let the offset commit past lost payloads.
        if self._pool is None:
            raise RuntimeError(f'PostgresSink {self._name!r} is not connected — call connect() before deliver()')

        start = time.monotonic()
        labels = {'sink_type': self.sink_type, 'sink_name': self._name}
        try:
            units, bad_index, build_error = self._build_units(payloads)
            async with self._pool.acquire() as conn:
                if build_error is not None:
                    # The per-payload loop executed every unit BEFORE the
                    # invalid payload, then raised. Reproduce those side
                    # effects (an exec failure on the way takes precedence,
                    # exactly as the sequential loop would have hit first).
                    for unit in units[:bad_index]:
                        await self._exec_single(conn, unit)
                    raise build_error
                for run in _group_into_runs(units):
                    await self._deliver_run(conn, run)

            sink_payloads_delivered.labels(**labels).inc(len(payloads))
            sink_deliver_duration.labels(**labels).observe(time.monotonic() - start)
        except Exception as e:
            sink_deliver_errors.labels(**labels).inc()
            await logger.aerror(
                'postgres_sink_deliver_failed',
                category='sink',
                sink_name=self._name,
                error=str(e),
                error_type=type(e).__name__,
                ops=sorted({p.op.value for p in payloads}),
                # Names only — statement TEXT would be high-cardinality and
                # could leak row data into logs.
                statements=sorted({p.statement for p in payloads if p.op is PostgresOp.STATEMENT}),
            )
            raise

    def _build_units(self, payloads: list[PostgresPayload]) -> tuple[list[_RowUnit | _StmtUnit], int, Exception | None]:
        """Validate and convert every payload up front.

        On the first bad payload returns the units built so far, the failing
        index, and the error — the caller replays the partial side effects
        before raising it.
        """
        units: list[_RowUnit | _StmtUnit] = []
        for i, payload in enumerate(payloads):
            try:
                units.append(self._build_unit(payload))
            except Exception as e:
                return units, i, e
        return units, len(units), None

    def _build_unit(self, payload: PostgresPayload) -> _RowUnit | _StmtUnit:
        """Reduce one payload to its execution inputs, identifiers quoted."""
        if payload.op is PostgresOp.STATEMENT:
            return self._build_statement_unit(payload)
        data = _dump_required(payload.data, 'data', payload.op)
        # Sorted, not left in the model's declaration order: the Go backend
        # decodes payload data into a map, which has no order to preserve, so
        # sorting is the only rule both backends can honour unconditionally.
        # Values are re-read by column so they stay aligned with the sort.
        columns = sorted(data)
        quoted_table = quote_ident(payload.table)

        if payload.op is PostgresOp.UPDATE:
            where = _dump_required(payload.where, 'where', payload.op)
            eq_columns = sorted(c for c, v in where.items() if v is not None)
            null_columns = sorted(c for c, v in where.items() if v is None)
            sql = render_update(
                quoted_table,
                [quote_ident(c) for c in columns],
                [quote_ident(c) for c in eq_columns],
                [quote_ident(c) for c in null_columns],
            )
            return _StmtUnit(
                op=payload.op,
                sql=sql,
                values=[*(data[c] for c in columns), *(where[c] for c in eq_columns)],
            )

        return _RowUnit(
            op=payload.op,
            quoted_table=quoted_table,
            quoted_columns=tuple(quote_ident(c) for c in columns),
            quoted_conflict=tuple(quote_ident(c) for c in payload.conflict),
            quoted_update_columns=tuple(quote_ident(c) for c in self._resolve_update_columns(payload, data)),
            values=[data[c] for c in columns],
        )

    def _build_statement_unit(self, payload: PostgresPayload) -> _StmtUnit:
        """Bind a payload's params to an operator-authored statement.

        Both a missing and an unexpected key are errors: a silently-ignored
        key is almost always a typo in the payload model or the config.
        """
        compiled = self._statements.get(payload.statement)
        if compiled is None:
            known = ', '.join(sorted(self._statements)) or '<none configured>'
            raise ValueError(
                f'Unknown postgres statement {payload.statement!r} on sink {self._name!r}; configured: {known}'
            )
        sql, names = compiled
        supplied = payload.params.model_dump() if payload.params is not None else {}
        missing = sorted(n for n in names if n not in supplied)
        unexpected = sorted(n for n in supplied if n not in names)
        if missing or unexpected:
            raise ValueError(
                f'Statement {payload.statement!r} params mismatch — missing: {missing}, unexpected: {unexpected}'
            )
        return _StmtUnit(
            op=payload.op,
            sql=sql,
            values=[supplied[n] for n in names],
            statement_name=payload.statement,
        )

    @staticmethod
    def _resolve_update_columns(payload: PostgresPayload, data: dict) -> list[str]:
        """Which columns an upsert overwrites on conflict (empty ⇒ DO NOTHING)."""
        if payload.op is not PostgresOp.UPSERT:
            return []
        if payload.update_columns is None:
            # Sorted for the same reason the data columns are — see _build_unit.
            # An explicit list is the operator's own order and is preserved.
            conflict = set(payload.conflict)
            return [c for c in sorted(data) if c not in conflict]
        unknown = sorted(c for c in payload.update_columns if c not in data)
        if unknown:
            raise ValueError(f'update_columns not present in data: {unknown}')
        return list(payload.update_columns)

    async def _deliver_run(self, conn: asyncpg.Connection, run: list[_RowUnit | _StmtUnit]) -> None:
        """Execute one run of same-shaped units."""
        if len(run) == 1:
            await self._exec_single(conn, run[0])
            return
        first = run[0]
        if isinstance(first, _StmtUnit):
            try:
                await conn.executemany(first.sql, [u.values for u in run])
            except Exception:
                # ``executemany`` is atomic (asyncpg >= 0.22): either every
                # execution succeeded or none did. Nothing was written, so
                # re-running one at a time cannot double-write — unlike the
                # multi-row INSERT fallback below. Retry per unit so the
                # surfaced error names the offending payload.
                for unit in run:
                    await self._exec_single(conn, unit)
            return
        await self._deliver_row_group(conn, [u for u in run if isinstance(u, _RowUnit)])

    async def _deliver_row_group(self, conn: asyncpg.Connection, group: list[_RowUnit]) -> None:
        """Send one VALUES-family run, chunked to the bind-parameter cap."""
        columns = len(group[0].quoted_columns)
        rows_per_statement = max(MAX_INSERT_PARAMS // columns, 1)
        for start in range(0, len(group), rows_per_statement):
            chunk = group[start : start + rows_per_statement]
            if len(chunk) == 1:
                await self._exec_single(conn, chunk[0])
                continue
            query = chunk[0].render(len(chunk))
            values = [v for unit in chunk for v in unit.values]
            try:
                await conn.execute(query, *values)
            except Exception as exc:
                # Batch failed — fall back to per-row delivery so the error
                # names the offending row (and to ride out a
                # statement-level transient). Safe only because a multi-row
                # INSERT is atomic: the failed batch wrote nothing.
                #
                # Report it. The fallback keeps delivery correct but costs
                # one round trip per payload, and it used to be silent — a
                # run that degraded from one statement to hundreds looked
                # exactly like a healthy one.
                sink_batch_fallbacks.labels(sink_type=self.sink_type, sink_name=self._name).inc()
                await logger.awarning(
                    'sink_batch_fallback_per_row',
                    category='sink',
                    sink_type=self.sink_type,
                    sink_name=self._name,
                    rows=len(chunk),
                    # Sink errors can carry a DSN with a password — same
                    # redaction the manager applies before an error reaches
                    # stats, the recorder or a DeliveryError.
                    error=redact_url(str(exc)),
                    error_type=type(exc).__name__,
                )
                for unit in chunk:
                    await self._exec_single(conn, unit)

    @staticmethod
    async def _exec_single(conn: asyncpg.Connection, unit: _RowUnit | _StmtUnit) -> None:
        """Execute one unit as a single statement."""
        if isinstance(unit, _StmtUnit):
            await conn.execute(unit.sql, *unit.values)
            return
        await conn.execute(unit.render(1), *unit.values)

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
