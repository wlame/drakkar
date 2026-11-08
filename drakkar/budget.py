"""The worker's budget of pending tasks and held bytes.

One :class:`PendingBudget` per worker, shared by every partition loop and the
HTTP source. It counts two things:

- **pending tasks**: tasks created by ``arrange`` or ``on_error`` and not yet
  settled — waiting for an executor slot, running, or in ``on_task_complete``
  and its delivery;
- **pending bytes**: the stdin of those tasks, plus the stdout and stderr of
  their results while ``on_task_complete``, ``on_error`` and the sink delivery
  run, and of results kept for ``on_message_complete`` and
  ``on_window_complete``.

A partition loop takes a new window only while both counts are below their
limits (``executor.max_pending_tasks``, ``executor.max_pending_bytes``). When
it takes one, it **reserves** the window's estimated tasks and bytes at once,
before it awaits anything: the framework sees a window's tasks only after
``arrange`` returns them, and without the reservation every loop that checked
the budget in the meantime would pass on the same room. The fan-out turns the
reservation into real charges, and what is left is given back when the window
has fanned out. A partition takes one message per window for its first
few windows (with tasks or without), and a message never reserves less than
``limit / window_size``, so a full window reserves the whole cap. With a
steady message shape the limits are passed by about one message for the whole
worker; after the warm-up a sudden jump in tasks per message can pass them by
that jump, because the fan-out is known only after ``arrange`` returns.

The estimate comes from moving averages of tasks and bytes per message, which
rise fast and fall slowly, fed only by windows that created tasks. Before any
such window of the worker was measured there is no estimate, so only one
unmeasured window (of one message) is admitted at a time.

The HTTP source charges its tasks too (they hold the same memory) but never
waits here: a budget full of Kafka work must not refuse HTTP requests.

Every operation is a few integer updates and two gauge writes, cheap enough to
run once per task at the target rate. Sizes come from ``sys.getsizeof``, which
is O(1) and is the real memory of a string.
"""

from __future__ import annotations

import asyncio
import math
import sys
from collections import deque
from dataclasses import dataclass
from typing import Any

from drakkar.metrics import pending_bytes, pending_tasks

# The ``limit`` label values of drakkar_window_admission_wait_seconds_total.
TASKS_LIMIT = 'tasks'
BYTES_LIMIT = 'bytes'

# Weights of the newest sample in the moving averages per message. The average
# rises fast and falls slowly: an estimate that is too high costs only
# throughput (a window waits a little longer), one that is too low lets windows
# pass the limits. 0.5 upward takes half of a larger sample at once; 0.05
# downward needs about 14 smaller windows to lose half of the gap.
SHAPE_RISE_WEIGHT = 0.5
SHAPE_FALL_WEIGHT = 0.05


def text_bytes(text: str | None) -> int:
    """Memory held by ``text``; 0 for no text."""
    return 0 if text is None else sys.getsizeof(text)


def output_bytes(outcome: Any) -> int:
    """Memory held by the stdout and stderr of an ``ExecutorResult`` or ``ExecutorError``."""
    return text_bytes(getattr(outcome, 'stdout', None)) + text_bytes(getattr(outcome, 'stderr', None))


def _moving_average(current: float | None, sample: float) -> float:
    """Fold ``sample`` into ``current``, fast upward and slowly downward; the first sample is the average."""
    if current is None:
        return sample
    weight = SHAPE_RISE_WEIGHT if sample > current else SHAPE_FALL_WEIGHT
    return (1.0 - weight) * current + weight * sample


@dataclass
class MessageShape:
    """Moving averages of what one message costs: tasks, stdin bytes and output bytes.

    Output is averaged in three parts: the output a task holds while its own
    hooks and delivery run, and the output kept by a window and by a message
    tracker. A window and a tracker keep their results apart, and a result
    kept by both counts twice. A result kept by a window counts only as kept.
    """

    tasks: float | None = None
    stdin_bytes: float = 0.0
    output_bytes: float | None = None
    window_kept_bytes: float | None = None
    message_kept_bytes: float | None = None

    @property
    def measured(self) -> bool:
        """Whether a window that created tasks was measured, so there is an estimate.

        A window with no tasks is never noted: counted as zeros its messages
        would drive the estimate to 0, and a zero estimate reserves no room.
        """
        return self.tasks is not None

    @property
    def held_bytes(self) -> float:
        """Estimated pending bytes of one message: stdin, output in its tasks' hooks and delivery, and kept output."""
        return (
            self.stdin_bytes
            + (self.output_bytes or 0.0)
            + (self.window_kept_bytes or 0.0)
            + (self.message_kept_bytes or 0.0)
        )

    def note_window(self, message_count: int, task_count: int, stdin_bytes: int) -> None:
        """Fold the fan-out of one window that created tasks into the averages."""
        previous_stdin = self.stdin_bytes if self.measured else None
        self.tasks = _moving_average(self.tasks, task_count / message_count)
        self.stdin_bytes = _moving_average(previous_stdin, stdin_bytes / message_count)

    def note_window_output(self, message_count: int, output_bytes: int) -> None:
        """Fold the output a window's tasks held in their hooks and delivery into the averages."""
        self.output_bytes = _moving_average(self.output_bytes, output_bytes / message_count)

    def note_window_kept(self, message_count: int, kept_bytes: int) -> None:
        """Fold the output a window kept for on_window_complete into the averages."""
        self.window_kept_bytes = _moving_average(self.window_kept_bytes, kept_bytes / message_count)

    def note_message_kept(self, kept_bytes: int) -> None:
        """Fold the output a message tracker kept for on_message_complete into the averages."""
        self.message_kept_bytes = _moving_average(self.message_kept_bytes, kept_bytes)


class PendingBudget:
    """Counts pending tasks and held bytes against two limits, and queues loops that wait for room.

    ``tasks`` and ``bytes`` are what is charged now (the gauges show them).
    Reservations count against the limits too (``full_limit``, ``free_tasks``,
    ``free_bytes``), so a window that is being arranged takes its room before
    its tasks exist.

    Waiters are futures in a FIFO list. A release that leaves room wakes the
    oldest waiter that is still waiting; a woken loop checks the room again
    before it takes a window, and queues again at the front if the room is
    gone.
    """

    def __init__(self, *, max_tasks: int, max_bytes: int) -> None:
        self.max_tasks = max_tasks
        self.max_bytes = max_bytes
        self._tasks = 0
        self._bytes = 0
        self._reserved_tasks = 0
        self._reserved_bytes = 0
        # The worker-wide averages: a partition's first window is sized by them.
        self.shape = MessageShape()
        self._unmeasured_window_open = False
        # The limit that last blocked a loop: the label of a wait behind
        # other waiters while there is room.
        self.last_reached_limit = TASKS_LIMIT
        self._waiters: deque[asyncio.Future[bool]] = deque()
        self._publish()

    @property
    def tasks(self) -> int:
        """Pending tasks charged now."""
        return self._tasks

    @property
    def bytes(self) -> int:
        """Pending bytes charged now."""
        return self._bytes

    @property
    def reserved_tasks(self) -> int:
        """Tasks reserved for windows that are being arranged."""
        return self._reserved_tasks

    @property
    def reserved_bytes(self) -> int:
        """Bytes reserved for windows that are being arranged."""
        return self._reserved_bytes

    @property
    def free_tasks(self) -> int:
        """Tasks that fit under the task limit now, reservations included, never below 0."""
        return max(0, self.max_tasks - self._tasks - self._reserved_tasks)

    @property
    def free_bytes(self) -> int:
        """Bytes that fit under the byte limit now, reservations included, never below 0."""
        return max(0, self.max_bytes - self._bytes - self._reserved_bytes)

    def full_limit(self) -> str | None:
        """The limit that is reached, reservations included, or ``None`` when there is room.

        The task limit is named first when both are reached: one label per
        wait keeps the wait counter a plain sum of seconds.
        """
        if self._tasks + self._reserved_tasks >= self.max_tasks:
            self.last_reached_limit = TASKS_LIMIT
            return TASKS_LIMIT
        if self._bytes + self._reserved_bytes >= self.max_bytes:
            self.last_reached_limit = BYTES_LIMIT
            return BYTES_LIMIT
        return None

    @property
    def unmeasured_window_open(self) -> bool:
        """Whether a window with no estimate is being arranged; a second one waits."""
        return self._unmeasured_window_open

    def open_unmeasured_window(self) -> None:
        """Mark the one window with no estimate as taken."""
        self._unmeasured_window_open = True

    def close_unmeasured_window(self) -> None:
        """The window with no estimate has fanned out; the next one may go."""
        self._unmeasured_window_open = False
        if self._waiters:
            self.wake_next()

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

    def reserve(self, tasks: int, held_bytes: int) -> None:
        """Hold room for a window whose tasks do not exist yet."""
        self._reserved_tasks += tasks
        self._reserved_bytes += held_bytes

    def unreserve(self, tasks: int, held_bytes: int) -> None:
        """Give back reserved room; wake a waiter when room opens."""
        self._reserved_tasks -= tasks
        self._reserved_bytes -= held_bytes
        if self._waiters:
            self.wake_next()

    def convert(self, reserved_tasks: int, reserved_bytes: int, tasks: int, held_bytes: int) -> None:
        """Turn part of a reservation into charges: the room taken never shrinks in between, so nobody is woken."""
        self._reserved_tasks -= reserved_tasks
        self._reserved_bytes -= reserved_bytes
        self.charge(tasks, held_bytes)

    def add_waiter(self, *, front: bool = False) -> asyncio.Future[bool]:
        """Queue a waiter, at the end of the FIFO list or, for a loop that lost its room, at the front.

        The future resolves ``True`` when room opens for it. Its owner may
        resolve it ``False`` to leave (a stop), or cancel it; either way the
        budget skips it.
        """
        waiter: asyncio.Future[bool] = asyncio.get_running_loop().create_future()
        if front:
            self._waiters.appendleft(waiter)
        else:
            self._waiters.append(waiter)
        return waiter

    def has_waiters(self) -> bool:
        """Whether a loop still waits in the FIFO list."""
        while self._waiters and self._waiters[0].done():
            self._waiters.popleft()
        return bool(self._waiters)

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

    Every charge, release and reservation goes through the account, so the
    holder can give back what it still holds in one call (:meth:`close`) when
    it stops. A task cancelled before its coroutine ever ran settles nothing,
    and a cancelled window never finishes; without the account their counts
    would stay in the budget for the life of the worker.
    """

    def __init__(self, budget: PendingBudget) -> None:
        self.budget = budget
        self._tasks = 0
        self._bytes = 0
        self._reserved_tasks = 0
        self._reserved_bytes = 0

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

    def reserve(self, estimated_tasks: float, estimated_bytes: float) -> None:
        """Reserve the estimate of a window, rounded up."""
        tasks = math.ceil(estimated_tasks)
        held_bytes = math.ceil(estimated_bytes)
        self._reserved_tasks += tasks
        self._reserved_bytes += held_bytes
        self.budget.reserve(tasks, held_bytes)

    def charge_reserved(self, tasks: int, held_bytes: int) -> None:
        """Charge tasks of a reserved window: the reservation shrinks by as much, never below 0."""
        from_tasks = min(self._reserved_tasks, tasks)
        from_bytes = min(self._reserved_bytes, held_bytes)
        self._reserved_tasks -= from_tasks
        self._reserved_bytes -= from_bytes
        self._tasks += tasks
        self._bytes += held_bytes
        self.budget.convert(from_tasks, from_bytes, tasks, held_bytes)

    def end_reservation(self) -> None:
        """Give back what is left of the reservation once the window has fanned out (or failed)."""
        tasks, held_bytes = self._reserved_tasks, self._reserved_bytes
        if tasks or held_bytes:
            self._reserved_tasks = 0
            self._reserved_bytes = 0
            self.budget.unreserve(tasks, held_bytes)

    def close(self) -> None:
        """Release everything the account still holds, reservations included. Idempotent."""
        self.end_reservation()
        tasks, held_bytes = self._tasks, self._bytes
        if tasks or held_bytes:
            self.release(tasks, held_bytes)
