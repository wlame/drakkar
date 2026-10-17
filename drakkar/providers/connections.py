"""Shared connections to the services a feature talks to, opened once per name.

A feature names a connection (``connections.<kind>.<name>`` in the config) and
asks the resolver for it. The first ``acquire`` of a name opens the client; every
later one, from this feature or another, gets the same client. The resolver does
not know what a client is: the caller passes the function that opens it and the
function that closes it, and :meth:`Connections.close_all` runs the closers at
shutdown.
"""

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import structlog
from pydantic import BaseModel

from drakkar.config.connections import ConnectionsConfig

logger = structlog.get_logger()


class UnknownConnectionError(ValueError):
    """A feature named a connection that the ``connections`` config does not declare."""


@dataclass(frozen=True)
class _OpenConnection:
    """A client that is open, with the function that closes it."""

    client: Any
    closer: Callable[[Any], Awaitable[None]]


class Connections:
    """The declared connections and the clients opened from them so far."""

    def __init__(self, config: ConnectionsConfig) -> None:
        # Built from the model's fields, so a new service type needs no change here.
        self._declared: dict[str, dict[str, BaseModel]] = {
            kind: getattr(config, kind) for kind in ConnectionsConfig.model_fields
        }
        # A dict keeps the order of opening, which close_all reverses.
        self._open: dict[tuple[str, str], _OpenConnection] = {}
        self._open_locks: dict[tuple[str, str], asyncio.Lock] = {}

    def settings(self, kind: str, name: str) -> BaseModel:
        """The declared settings of ``connections.<kind>.<name>``; raises :class:`UnknownConnectionError` if there are none."""
        declared = self._declared.get(kind, {}).get(name)
        if declared is None:
            raise UnknownConnectionError(f'connections.{kind}.{name} is not defined')
        return declared

    async def acquire[T](
        self,
        kind: str,
        name: str,
        opener: Callable[[BaseModel], Awaitable[T]],
        closer: Callable[[T], Awaitable[None]],
    ) -> T:
        """The client of ``connections.<kind>.<name>``, opened by ``opener`` on the first call only.

        Concurrent first calls wait for one open and share its client. An open that
        raises is not remembered: the error goes to the caller, and the next call
        opens again. ``closer`` is kept for :meth:`close_all`; the one passed with
        the first successful open is the one used.
        """
        settings = self.settings(kind, name)
        key = (kind, name)
        async with self._open_locks.setdefault(key, asyncio.Lock()):
            already_open = self._open.get(key)
            if already_open is not None:
                return already_open.client
            client = await opener(settings)
            self._open[key] = _OpenConnection(client=client, closer=closer)
            return client

    async def close_all(self) -> None:
        """Close every open client, the last opened first. A closer that raises is logged, never raised.

        Each client is dropped before its closer runs, so a second call closes nothing again.
        """
        while self._open:
            (kind, name), opened = self._open.popitem()
            try:
                await opened.closer(opened.client)
            except Exception as exc:
                await logger.awarning(
                    'connection_close_failed',
                    category='fleet',
                    kind=kind,
                    name=name,
                    error=str(exc) or type(exc).__name__,
                )
