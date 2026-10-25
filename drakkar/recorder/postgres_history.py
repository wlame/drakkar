"""The history search of the ``postgres`` recorder mode: one trace over the events table.

``GET /api/v1/debug/trace?history=1`` on a worker whose recorder keeps its
history in Postgres reads the rows of the worker's own cluster from the
events table, never the sealed files. The search runs on the UI server's
loop with a connection of its own (no pool), makes one attempt, and is
bounded as a whole by ``ui.trace.history_max_seconds``: the client side by
``asyncio.timeout``, the server side by ``statement_timeout``.
"""

from __future__ import annotations

import asyncio
import math
from collections.abc import Awaitable, Callable
from typing import Any, Literal

import asyncpg

from drakkar.providers.postgres import (
    RECORDER_STATEMENT_TEMPLATES,
    SET_LOCAL_VALUES,
    SETTING_JIT,
    STATEMENT_TIMEOUT_SETTING,
    render_recorder,
)
from drakkar.providers.resilience import describe_error
from drakkar.recorder.postgres import RecorderPostgresSettings, replace_nul

MILLISECONDS_PER_SECOND = 1000
# The smallest statement_timeout sent: 0 would mean no limit on the server.
MIN_STATEMENT_TIMEOUT_MS = 1
# The longest a search may take; a larger or non-finite
# ``ui.trace.history_max_seconds`` uses this limit.
MAX_SEARCH_SECONDS = 3600.0

HistoryFailureReason = Literal['timeout', 'error']
Connect = Callable[..., Awaitable[Any]]


class PostgresHistoryError(Exception):
    """A history search that failed (``reason`` ``'error'``) or ran out of time (``'timeout'``).

    ``error`` is a log-friendly text without credentials.
    """

    def __init__(self, reason: HistoryFailureReason, error: str) -> None:
        super().__init__(f'{reason}: {error}')
        self.reason: HistoryFailureReason = reason
        self.error = error


def search_limit_seconds(max_seconds: float) -> float:
    """The time limit of a search: ``max_seconds``, or :data:`MAX_SEARCH_SECONDS` when it is larger or not finite."""
    if not math.isfinite(max_seconds) or max_seconds > MAX_SEARCH_SECONDS:
        return MAX_SEARCH_SECONDS
    return max_seconds


def statement_timeout_ms(max_seconds: float) -> str:
    """The server-side time limit of a search, in whole milliseconds (rounded down, at least 1), as text.

    The limit is capped first (:func:`search_limit_seconds`).
    """
    limit = search_limit_seconds(max_seconds)
    return str(max(MIN_STATEMENT_TIMEOUT_MS, math.floor(limit * MILLISECONDS_PER_SECOND)))


async def search_postgres_history(
    settings: RecorderPostgresSettings,
    cluster: str,
    partition: int,
    offset: int,
    *,
    max_seconds: float,
    connect: Connect | None = None,
) -> list[dict]:
    """Trace ``(partition, offset)`` through the events table rows of ``cluster``; rows in time order.

    Each row holds the 20 event columns plus ``worker_name``. One
    connection is opened for the search and closed after it. The
    transaction sets ``statement_timeout`` (the same limit in whole
    milliseconds) and turns JIT off before the trace statement.

    ``connect`` defaults to :func:`asyncpg.connect`, looked up at call time.
    Raises :class:`PostgresHistoryError` when the search fails or does not
    end within ``max_seconds``, connect included.
    """
    connect = connect if connect is not None else asyncpg.connect
    statement = render_recorder(RECORDER_STATEMENT_TEMPLATES['trace'], schema=settings.schema, table=settings.table)
    set_local = RECORDER_STATEMENT_TEMPLATES['set_local']
    # The writer stores the cluster name with every NUL replaced; the search
    # must ask for the same text.
    stored_cluster = replace_nul(cluster)
    deadline = asyncio.get_running_loop().time() + search_limit_seconds(max_seconds)
    conn: Any = None
    try:
        async with asyncio.timeout_at(deadline):
            conn = await connect(dsn=settings.connection.dsn)
            # Started and committed by hand: on a failure or a time-out nothing
            # more is sent (a ROLLBACK would wait for the server); closing the
            # socket below ends the transaction on the server.
            transaction = conn.transaction()
            await transaction.start()
            await conn.execute(set_local, STATEMENT_TIMEOUT_SETTING, statement_timeout_ms(max_seconds))
            await conn.execute(set_local, SETTING_JIT, SET_LOCAL_VALUES[SETTING_JIT])
            rows = await conn.fetch(statement, stored_cluster, partition, offset)
            await transaction.commit()
        await _close_after_search(conn, deadline)
    except TimeoutError as exc:
        raise PostgresHistoryError('timeout', describe_error(exc)) from exc
    except Exception as exc:
        raise PostgresHistoryError('error', describe_error(exc)) from exc
    finally:
        # Never waits: a connection left open by a failure, a time-out, a
        # cancel or a close that did not end is closed at once.
        if conn is not None and not conn.is_closed():
            conn.terminate()
    return [dict(row) for row in rows]


async def _close_after_search(conn: Any, deadline: float) -> None:
    """Close the connection of a search that already has its rows, within the search's time limit.

    The rows are committed and complete, so a close that fails or runs out
    of time does not fail the search: the caller terminates the connection,
    which ends it on the server anyway. The close error is dropped on
    purpose; a cancel still leaves as a cancel.
    """
    try:
        async with asyncio.timeout_at(deadline):
            await conn.close()
    except Exception:
        # The caller terminates the connection right after; nothing is lost.
        return
