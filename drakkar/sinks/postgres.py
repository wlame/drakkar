"""PostgreSQL sink — inserts rows into database tables.

Wraps asyncpg connection pool. Each PostgresPayload's data field is
serialized via model_dump() to get a column-name → value mapping,
then inserted into the specified table.
"""

import re
import time

import asyncpg
import structlog

from drakkar.config import PostgresSinkConfig
from drakkar.metrics import sink_deliver_duration, sink_deliver_errors, sink_payloads_delivered
from drakkar.models import PostgresPayload
from drakkar.sinks.base import BaseSink

logger = structlog.get_logger()

_IDENT_RE = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')


def _quote_ident(name: str) -> str:
    """Quote a SQL identifier to prevent injection.

    Only allows simple alphanumeric+underscore identifiers.
    Raises ValueError for anything suspicious.
    """
    if not _IDENT_RE.match(name):
        raise ValueError(f'Invalid SQL identifier: {name!r}')
    return f'"{name}"'


class PostgresSink(BaseSink):
    """Inserts rows into PostgreSQL tables.

    Each PostgresPayload is serialized:
        - table = payload.table (validated against SQL injection)
        - columns/values = payload.data.model_dump() dict

    Exposes the asyncpg pool via the `pool` property so users can
    access it in on_ready() for migrations or lookups.
    """

    sink_type = 'postgres'

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

    async def deliver(self, payloads: list[PostgresPayload]) -> None:  # type: ignore[override]
        """Insert all payloads into their respective tables.

        Each payload's data is serialized via model_dump() and inserted
        as a row. Table and column names are validated against SQL injection.
        """
        if not payloads or not self._pool:
            return

        start = time.monotonic()
        labels = {'sink_type': self.sink_type, 'sink_name': self._name}
        try:
            async with self._pool.acquire() as conn:
                for payload in payloads:
                    data = payload.data.model_dump()
                    columns = list(data.keys())
                    table = _quote_ident(payload.table)
                    col_names = ', '.join(_quote_ident(c) for c in columns)
                    placeholders = ', '.join(f'${i + 1}' for i in range(len(columns)))
                    query = f'INSERT INTO {table} ({col_names}) VALUES ({placeholders})'
                    values = list(data.values())
                    await conn.execute(query, *values)

            sink_payloads_delivered.labels(**labels).inc(len(payloads))
            sink_deliver_duration.labels(**labels).observe(time.monotonic() - start)
        except Exception:
            sink_deliver_errors.labels(**labels).inc()
            raise

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
