"""The worker's budget of pending tasks and held bytes.

One :class:`PendingBudget` per worker, shared by every partition loop and the
HTTP source. It counts two things:

- **pending tasks**: tasks created by ``arrange`` or ``on_error`` and not yet
  settled — waiting for an executor slot, running, or in ``on_task_complete``
  and its delivery;
- **pending bytes**: the stdin of those tasks, plus the stdout and stderr of
  results kept for ``on_message_complete`` and ``on_window_complete``.

A partition loop takes a new window only while both counts are below their
limits (``executor.max_pending_tasks``, ``executor.max_pending_bytes``). The
limits are soft: a loop that has room takes at least one message, and the
framework sees a message's tasks only after ``arrange`` returned them, so one
window's fan-out can pass a limit. The HTTP source charges its tasks too (they
hold the same memory) but never waits here: a budget full of Kafka work must
not refuse HTTP requests.

Every operation is a few integer updates and two gauge writes, cheap enough
to run once per task at the target rate. Sizes come from ``sys.getsizeof``,
which is O(1) and is the real memory of a string.
"""

from __future__ import annotations

import asyncio
import sys
from collections import deque
from typing import Any

from drakkar.metrics import pending_bytes, pending_tasks

# The ``limit`` label values of drakkar_window_admission_wait_seconds_total.
TASKS_LIMIT = 'tasks'
BYTES_LIMIT = 'bytes'


def text_bytes(text: str | None) -> int:
    """Memory held by ``text``; 0 for no text."""
    return 0 if text is None else sys.getsizeof(text)


def output_bytes(outcome: Any) -> int:
    """Memory held by the stdout and stderr of an ``ExecutorResult`` or ``ExecutorError``."""
    return text_bytes(getattr(outcome, 'stdout', None)) + text_bytes(getattr(outcome, 'stderr', None))


class PendingBudget:
    """Counts pending tasks and held bytes against two limits, and queues loops that wait for room.

    Waiters are futures in a FIFO list. A release that leaves room wakes the
    oldest waiter that is still waiting; a loop that took a window calls
    :meth:`wake_next` so that room left after its window goes to the next
    waiter rather than staying idle.
    """

    def __init__(self, *, max_tasks: int, max_bytes: int) -> None:
        self.max_tasks = max_tasks
        self.max_bytes = max_bytes
        self._tasks = 0
        self._bytes = 0
        self._waiters: deque[asyncio.Future[bool]] = deque()
        self._publish()

    @property
    def tasks(self) -> int:
        """Pending tasks now."""
        return self._tasks

    @property
    def bytes(self) -> int:
        """Pending bytes now."""
        return self._bytes

    @property
    def free_tasks(self) -> int:
        """Tasks that fit under the task limit now, never below 0."""
        return max(0, self.max_tasks - self._tasks)

    @property
    def free_bytes(self) -> int:
        """Bytes that fit under the byte limit now, never below 0."""
        return max(0, self.max_bytes - self._bytes)

    def full_limit(self) -> str | None:
        """The limit that is reached, or ``None`` when there is room.

        The task limit is named first when both are reached: one label per
        wait keeps the wait counter a plain sum of seconds.
        """
        if self._tasks >= self.max_tasks:
            return TASKS_LIMIT
        if self._bytes >= self.max_bytes:
            return BYTES_LIMIT
        return None

    def charge(self, tasks: int, held_bytes: int) -> None:
        """Count ``tasks`` more pending tasks and ``held_bytes`` more held bytes."""
        self._tasks += tasks
        self._bytes += held_bytes
        self._publish()

    def release(self, tasks: int, held_bytes: int) -> None:
        """Count ``tasks`` fewer pending tasks and ``held_bytes`` fewer held bytes; wake a waiter when room opens."""
        self._tasks -= tasks
        self._bytes -= held_bytes
        self._publish()
        if self._waiters:
            self.wake_next()

    def add_waiter(self) -> asyncio.Future[bool]:
        """Queue a waiter at the end of the FIFO list.

        The future resolves ``True`` when room opens for it. Its owner may
        resolve it ``False`` to leave (a stop), or cancel it when it no longer
        waits; either way the budget skips it.
        """
        waiter: asyncio.Future[bool] = asyncio.get_running_loop().create_future()
        self._waiters.append(waiter)
        return waiter

    def wake_next(self) -> None:
        """Wake the oldest waiter that still waits, when there is room."""
        if self.full_limit() is not None:
            return
        while self._waiters:
            waiter = self._waiters.popleft()
            if not waiter.done():
                waiter.set_result(True)
                return

    def _publish(self) -> None:
        pending_tasks.set(self._tasks)
        pending_bytes.set(self._bytes)


class BudgetAccount:
    """One holder's share of a :class:`PendingBudget`: a partition processor, or one HTTP request.

    Every charge and release goes through the account, so the holder can give
    back what it still holds in one call (:meth:`close`) when it stops. A task
    cancelled before its coroutine ever ran settles nothing, and a cancelled
    window never finishes; without the account their counts would stay in the
    budget for the life of the worker.
    """

    def __init__(self, budget: PendingBudget) -> None:
        self.budget = budget
        self._tasks = 0
        self._bytes = 0

    def charge(self, tasks: int, held_bytes: int) -> None:
        """Charge the budget and remember the share."""
        self._tasks += tasks
        self._bytes += held_bytes
        self.budget.charge(tasks, held_bytes)

    def release(self, tasks: int, held_bytes: int) -> None:
        """Release part of the share."""
        self._tasks -= tasks
        self._bytes -= held_bytes
        self.budget.release(tasks, held_bytes)

    def close(self) -> None:
        """Release everything the account still holds. Idempotent."""
        tasks, held_bytes = self._tasks, self._bytes
        if tasks or held_bytes:
            self.release(tasks, held_bytes)
