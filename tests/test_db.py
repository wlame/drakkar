"""Tests for Drakkar database writer."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from drakkar.config import PostgresConfig
from drakkar.db import DBWriter
from drakkar.models import DBRow


@pytest.fixture
def pg_config() -> PostgresConfig:
    return PostgresConfig(
        dsn="postgresql://user:pass@localhost:5432/testdb",
        pool_min=1,
        pool_max=5,
    )


@pytest.fixture
def mock_pool():
    pool = MagicMock()
    conn = AsyncMock()
    ctx = AsyncMock()
    ctx.__aenter__ = AsyncMock(return_value=conn)
    ctx.__aexit__ = AsyncMock(return_value=False)
    pool.acquire.return_value = ctx
    pool.close = AsyncMock()
    return pool, conn


async def test_db_writer_connect(pg_config):
    with patch("drakkar.db.asyncpg.create_pool", new_callable=AsyncMock) as mock_create:
        mock_create.return_value = AsyncMock()
        writer = DBWriter(pg_config)
        await writer.connect()
        mock_create.assert_called_once_with(
            dsn="postgresql://user:pass@localhost:5432/testdb",
            min_size=1,
            max_size=5,
        )
        assert writer.pool is not None


async def test_db_writer_write_rows(pg_config, mock_pool):
    pool, conn = mock_pool

    writer = DBWriter(pg_config)
    writer._pool = pool

    rows = [
        DBRow(table="results", data={"id": 1, "status": "done"}),
        DBRow(table="results", data={"id": 2, "status": "failed"}),
    ]
    await writer.write(rows)
    assert conn.execute.call_count == 2

    first_call = conn.execute.call_args_list[0]
    query = first_call[0][0]
    assert "INSERT INTO results" in query
    assert "id" in query
    assert "status" in query


async def test_db_writer_write_empty_rows(pg_config):
    writer = DBWriter(pg_config)
    writer._pool = AsyncMock()
    await writer.write([])
    writer._pool.acquire.assert_not_called()


async def test_db_writer_write_no_pool(pg_config):
    writer = DBWriter(pg_config)
    rows = [DBRow(table="t", data={"x": 1})]
    await writer.write(rows)  # should not raise


async def test_db_writer_close(pg_config):
    writer = DBWriter(pg_config)
    mock_pool = AsyncMock()
    writer._pool = mock_pool
    await writer.close()
    mock_pool.close.assert_called_once()
    assert writer.pool is None


async def test_db_writer_close_no_pool(pg_config):
    writer = DBWriter(pg_config)
    await writer.close()  # should not raise


async def test_db_writer_pool_property(pg_config):
    writer = DBWriter(pg_config)
    assert writer.pool is None


async def test_db_writer_write_error_reraises(pg_config):
    """DB write errors are re-raised after incrementing metrics (metric check in test_metrics.py)."""
    pool = MagicMock()
    conn = AsyncMock()
    conn.execute.side_effect = Exception("connection lost")
    ctx = AsyncMock()
    ctx.__aenter__ = AsyncMock(return_value=conn)
    ctx.__aexit__ = AsyncMock(return_value=False)
    pool.acquire.return_value = ctx

    writer = DBWriter(pg_config)
    writer._pool = pool

    with pytest.raises(Exception, match="connection lost"):
        await writer.write([DBRow(table="t", data={"x": 1})])
