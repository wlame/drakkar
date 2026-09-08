"""Uvicorn server whose event loop reports unhandled errors through structlog.

The UI server and the webapp each run uvicorn on a daemon thread with its
own event loop. An exception that no coroutine retrieves on such a loop —
a WebSocket library's close path failing after its client went silent, an
un-awaited task that raised — reaches asyncio's default exception handler,
which prints a bare traceback to stderr through the stdlib ``logging``
module. In a worker whose output is otherwise structured JSON that reads
like a crash. :class:`LoopLoggingServer` installs a handler on its loop
that logs those errors as structured events instead.
"""

from __future__ import annotations

import asyncio
import functools
import socket
from typing import Any

import structlog
import uvicorn
from websockets.exceptions import ConnectionClosed

logger = structlog.get_logger()

# A peer that vanished mid-connection is routine for a long-lived dashboard
# socket (a sleeping laptop, a throttled background tab), not a fault.
CLIENT_DISCONNECT_ERRORS: tuple[type[BaseException], ...] = (ConnectionError, ConnectionClosed)

DEFAULT_LOOP_ERROR_MESSAGE = 'Unhandled exception in event loop'


def log_loop_exception(loop: asyncio.AbstractEventLoop, context: dict[str, Any], *, server_name: str) -> None:
    """Asyncio exception handler that logs through structlog.

    Client disconnects are logged at info level without a traceback;
    everything else is logged at error level with the traceback attached.
    """
    exc = context.get('exception')
    message = context.get('message') or DEFAULT_LOOP_ERROR_MESSAGE
    if isinstance(exc, CLIENT_DISCONNECT_ERRORS):
        logger.info(
            'server_client_disconnected',
            category='server',
            server=server_name,
            error_type=type(exc).__name__,
            error=str(exc),
        )
        return
    logger.error(
        'server_loop_unhandled_exception',
        category='server',
        server=server_name,
        detail=message,
        error_type=type(exc).__name__ if exc is not None else None,
        exc_info=exc,
    )


class LoopLoggingServer(uvicorn.Server):
    """``uvicorn.Server`` that routes its loop's unhandled errors to structlog.

    The handler is installed at the top of ``serve`` because uvicorn creates
    the loop inside ``run``; ``serve`` is the first code on that loop.
    """

    def __init__(self, config: uvicorn.Config, *, server_name: str) -> None:
        super().__init__(config)
        self._server_name = server_name

    async def serve(self, sockets: list[socket.socket] | None = None) -> None:
        asyncio.get_running_loop().set_exception_handler(
            functools.partial(log_loop_exception, server_name=self._server_name)
        )
        await super().serve(sockets=sockets)
