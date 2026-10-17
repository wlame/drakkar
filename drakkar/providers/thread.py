"""A daemon thread that runs one provider's blocking calls, one at a time.

Providers that work on files (the ``filesystem`` registry and the
``filesystem`` election) run every blocking call on a :class:`ProviderThread`
of their own instead of the shared ``asyncio.to_thread`` pool.
"""

import asyncio
import concurrent.futures
import queue
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class _ThreadJob:
    """One call for the provider thread, its result, and how to free the slot afterwards."""

    call: Callable[[], Any]
    future: concurrent.futures.Future
    free_slot: Callable[[], None]


def _call_soon(loop: asyncio.AbstractEventLoop, callback: Callable[[], Any]) -> None:
    """Schedule ``callback`` on ``loop`` from another thread; a closed loop (process exit) is ignored."""
    try:
        loop.call_soon_threadsafe(callback)
    except RuntimeError:
        pass


class ProviderThread:
    """One daemon thread that runs a provider's blocking calls, in order.

    Not the shared ``asyncio.to_thread`` pool: a call stuck on a dead network
    mount would hold a pool thread for as long as it hangs, and enough of them
    would starve every other ``to_thread`` user in the worker. Here a stuck
    call holds only this thread, so the provider's next call times out instead.
    The thread is a daemon because the pool threads are joined at interpreter
    exit, and a hung call would then keep the process from exiting.
    """

    def __init__(self, name: str) -> None:
        self._jobs: queue.SimpleQueue[_ThreadJob | None] = queue.SimpleQueue()
        # One call at a time: a caller takes the slot before it queues its
        # call, and the thread frees the slot when the call ends. While a
        # call hangs, later callers wait for the slot and give up with their
        # timeout without queuing anything, so nothing piles up in memory.
        self._slot = asyncio.Semaphore(1)
        self.thread = threading.Thread(target=self._serve, name=name, daemon=True)
        self.thread.start()

    def _serve(self) -> None:
        while (job := self._jobs.get()) is not None:
            try:
                # False when the caller gave up (timeout) before the call
                # started: skip it rather than run a stale call.
                if job.future.set_running_or_notify_cancel():
                    try:
                        job.future.set_result(job.call())
                    except Exception as exc:
                        job.future.set_exception(exc)
            finally:
                job.free_slot()

    async def run(self, call: Callable[[], Any]) -> Any:
        """Run ``call`` on the thread and wait for its result.

        Waiting for the slot can be cancelled with nothing queued; a call
        whose caller gave up after queuing it is skipped by the thread.
        """
        loop = asyncio.get_running_loop()
        await self._slot.acquire()
        future: concurrent.futures.Future = concurrent.futures.Future()
        self._jobs.put(_ThreadJob(call=call, future=future, free_slot=lambda: _call_soon(loop, self._slot.release)))
        return await asyncio.wrap_future(future)

    def stop(self) -> None:
        """Let the thread exit after the calls already queued."""
        self._jobs.put(None)
