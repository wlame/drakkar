"""PostgreSQL database writer for Drakkar framework."""

import time

import asyncpg
import structlog

from drakkar.config import PostgresConfig
from drakkar.metrics import db_errors, db_rows_written, db_write_duration
from drakkar.models import DBRow

logger = structlog.get_logger()


class DBWriter:
    """Manages asyncpg connection pool and writes result rows to PostgreSQL."""

    def __init__(self, config: PostgresConfig):
        self._config = config
        self._pool: asyncpg.Pool | None = None

    @property
    def pool(self) -> asyncpg.Pool | None:
        """Expose pool for direct use in user hooks if needed."""
        return self._pool

    async def connect(self) -> None:
        """Initialize the connection pool."""
        self._pool = await asyncpg.create_pool(
            dsn=self._config.dsn,
            min_size=self._config.pool_min,
            max_size=self._config.pool_max,
        )
        logger.info("db_connected", dsn=self._config.dsn.split("@")[-1])

    async def write(self, rows: list[DBRow]) -> None:
        """Write rows to their respective tables.

        Each DBRow specifies a table name and a dict of column->value.
        Uses INSERT with column names derived from the data dict keys.
        """
        if not rows or not self._pool:
            return

        start = time.monotonic()
        try:
            async with self._pool.acquire() as conn:
                for row in rows:
                    columns = list(row.data.keys())
                    placeholders = ", ".join(f"${i + 1}" for i in range(len(columns)))
                    col_names = ", ".join(columns)
                    query = f"INSERT INTO {row.table} ({col_names}) VALUES ({placeholders})"
                    values = list(row.data.values())
                    await conn.execute(query, *values)

            db_rows_written.inc(len(rows))
            db_write_duration.observe(time.monotonic() - start)
            logger.debug("db_rows_written", count=len(rows))
        except Exception:
            db_errors.inc()
            raise

    async def close(self) -> None:
        """Close the connection pool."""
        if self._pool:
            await self._pool.close()
            self._pool = None
