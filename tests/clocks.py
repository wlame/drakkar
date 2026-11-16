"""A virtual clock for tests of code that sleeps: time moves only when the event loop is idle."""

import asyncio
import heapq
import itertools
import math


class VirtualClock:
    """A clock whose time moves only to the next sleeper's wake time, once the event loop has nothing else to run.

    ``drift`` lists, for each of the first sleeps, how many seconds it wakes
    before it was asked to (negative: after), like ``asyncio.sleep`` rounding
    or a clock that runs behind. The clock does not move past ``limit``;
    when the next wake time is past it, ``parked`` is set and the sleepers
    stay asleep.
    """

    def __init__(self, now: float, drift: tuple[float, ...] = ()) -> None:
        self.now = now
        self.drift = list(drift)
        self.sleeps: list[float] = []
        self.limit = math.inf
        self.parked = asyncio.Event()
        self._sleepers: list[tuple[float, int, asyncio.Future[None]]] = []
        self._order = itertools.count()
        self._check_scheduled = False

    def time(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        wake = self.now + seconds - (self.drift.pop(0) if self.drift else 0)
        future = asyncio.get_running_loop().create_future()
        heapq.heappush(self._sleepers, (wake, next(self._order), future))
        self._schedule_check()
        await future

    def _schedule_check(self) -> None:
        if not self._check_scheduled:
            self._check_scheduled = True
            asyncio.get_running_loop().call_soon(self._wake_next_when_idle)

    def _wake_next_when_idle(self) -> None:
        loop = asyncio.get_running_loop()
        # asyncio has no public "idle" signal: the ready queue is empty only
        # when every task waits, here on a sleeper's future.
        if loop._ready:  # type: ignore[attr-defined]
            loop.call_soon(self._wake_next_when_idle)
            return
        self._check_scheduled = False
        while self._sleepers and self._sleepers[0][2].done():
            heapq.heappop(self._sleepers)  # a sleeper whose task was cancelled
        if not self._sleepers or self._sleepers[0][0] > self.limit:
            self.parked.set()
            return
        wake = self._sleepers[0][0]
        self.now = max(self.now, wake)
        # Every sleeper due at this time wakes at once, so loops due
        # in the same second really run side by side.
        while self._sleepers and self._sleepers[0][0] == wake:
            future = heapq.heappop(self._sleepers)[2]
            if not future.done():
                future.set_result(None)
        self._schedule_check()
