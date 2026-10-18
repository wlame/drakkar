"""In-process Redis for the fleet provider tests.

``fakeredis`` runs the real command set and the real Lua scripts, so the
scripts of the shared corpus (``drakkar.providers.redis``) are tested as they
are sent. This module adds what a provider test needs around it:

* ``FakeClock`` moves the wall clock that ``fakeredis`` reads for TTLs, ``TIME``
  and the ``TIME`` call inside a script.
* ``fake_client`` opens a client on a server that the test owns.
* ``RecordingClient`` wraps a client, records the exact arguments of every
  command and injects failures and hangs.
"""

import asyncio
import time
from typing import Any

import fakeredis
import pytest

# 2027-01-15 08:00:00 UTC: a whole second, so ``TIME`` answers a micros part of ``0``.
DEFAULT_START_EPOCH = 1_800_000_000.0


class FakeClock:
    """The wall clock ``fakeredis`` reads, for one test.

    It replaces ``time.time`` until the test ends and stands still until the
    test moves it. The monotonic clock is not touched, so the provider caller
    (which times its attempts on ``time.monotonic``) runs on real time.
    """

    def __init__(self, monkeypatch: pytest.MonkeyPatch, start: float = DEFAULT_START_EPOCH) -> None:
        self._epoch = start
        monkeypatch.setattr(time, 'time', self._read)

    def _read(self) -> float:
        return self._epoch

    def set(self, epoch: float) -> None:
        """Move the clock to ``epoch`` (seconds since the Unix epoch)."""
        self._epoch = epoch

    def advance(self, seconds: float) -> None:
        """Move the clock forward by ``seconds``."""
        self._epoch += seconds


def fake_client(server: fakeredis.FakeServer) -> fakeredis.FakeAsyncRedis:
    """Open a client on ``server``.

    Each call returns a new client, so two providers of one test keep their
    own connections and share the data through the server. Replies are bytes,
    as the clients of ``drakkar.providers.redis.open_client`` give them.

    The protocol is set to 2 on purpose. A real server runs every script in
    RESP2 whatever the client speaks, and a client that sends no ``HELLO`` is
    in RESP2; ``fakeredis`` instead takes an unset protocol for RESP3 inside a
    script, where ``ZRANGE ... WITHSCORES`` answers pairs instead of a flat list.
    """
    return fakeredis.FakeAsyncRedis(server=server, protocol=2)


class RecordingClient:
    """Forwards ``execute_command`` to a client and records each argv as a tuple of ``str``.

    Every command that reaches the wrapper is recorded in ``calls``, also one
    that fails or hangs. An argument that is not a ``str`` raises ``TypeError``
    instead of being converted: a provider must send strings, so the recorded
    argv equals the pinned one exactly.

    ``fail_next`` is raised by the next command instead of sending it, then
    cleared. ``fail_every`` is raised by every command until it is set back to
    ``None``. ``hang`` makes the next command wait forever (until its caller
    cancels it) and is then cleared.
    """

    def __init__(self, client: Any) -> None:
        self._client = client
        self.calls: list[tuple[str, ...]] = []
        self.fail_next: Exception | None = None
        self.fail_every: Exception | None = None
        self.hang = False

    async def execute_command(self, *argv: Any) -> Any:
        """Record ``argv``, raise an injected failure or hang, else send it to the wrapped client."""
        not_text = [argument for argument in argv if not isinstance(argument, str)]
        if not_text:
            raise TypeError(f'redis arguments must be str, got {not_text!r} in {argv!r}')
        self.calls.append(tuple(argv))
        if self.fail_next is not None:
            failure, self.fail_next = self.fail_next, None
            raise failure
        if self.fail_every is not None:
            raise self.fail_every
        if self.hang:
            self.hang = False
            await asyncio.Event().wait()
        return await self._client.execute_command(*argv)

    async def aclose(self) -> None:
        """Close the wrapped client."""
        await self._client.aclose()
