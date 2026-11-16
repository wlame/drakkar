"""Task groups of the streaming arrange hook: creation, label collection, note checks, pulls and the close."""

from __future__ import annotations

import asyncio
import contextvars
import inspect
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine, Sequence
from dataclasses import dataclass
from typing import Any

from drakkar.models import ExecutorTask, SourceMessage, TaskGroup

# How long closing a stream may take: cancelling the pull in progress, then
# ``aclose()`` of the generator. A generator that ignores both is left behind
# after this bound, so it cannot hold the worker's stop or a probe for longer.
STREAM_CLOSE_TIMEOUT = 5.0

# Exact types, not isinstance: a subclass of str or int can carry attributes,
# and so a reference to a task or a result, which a note must never hold.
_NOTE_VALUE_TYPES = frozenset({str, int, float, bool})


class ArrangeStreamError(Exception):
    """A value that ``arrange_stream`` produced breaks the rules of the hook."""


def open_arrange_stream(handler: Any, message: SourceMessage) -> AsyncIterator[list[ExecutorTask]]:
    """Call ``handler.arrange_stream(message)`` and return its async iterator.

    Raises:
        ArrangeStreamError: when the hook returns anything that is not an
            async iterator.
    """
    result = handler.arrange_stream(message)
    if not hasattr(result, '__aiter__'):
        if inspect.iscoroutine(result):
            # An ``async def`` without a yield: close it so it does not warn
            # that it was never awaited.
            result.close()
        raise ArrangeStreamError(f'arrange_stream must be an async generator, got {type(result).__name__}')
    return result.__aiter__()


def check_stream_item(item: object, message: SourceMessage) -> list[ExecutorTask]:
    """Return ``item`` as the task list of one group, or raise ``ArrangeStreamError``.

    The item must be a list of tasks, each covering only ``message``, and no
    task may belong to a group already: the same task object in two groups
    (or twice in one list) would count its outcome twice.
    """
    if not isinstance(item, list):
        raise ArrangeStreamError(f'arrange_stream must yield a list of ExecutorTask, got {type(item).__name__}')
    expected_offsets = [message.offset]
    seen: set[int] = set()
    for task in item:
        if not isinstance(task, ExecutorTask):
            raise ArrangeStreamError(f'arrange_stream must yield a list of ExecutorTask, got {type(task).__name__}')
        if task.source_offsets != expected_offsets:
            raise ArrangeStreamError(
                f'arrange_stream yielded task {task.task_id} with source_offsets {task.source_offsets}; '
                f'a stream task covers only its message, offset {message.offset}'
            )
        if task.group is not None or id(task) in seen:
            raise ArrangeStreamError(f'arrange_stream yielded task {task.task_id} twice; a task belongs to one group')
        seen.add(id(task))
    return item


def collect_group_labels(tasks: list[ExecutorTask], names: Sequence[str]) -> dict[str, list[str]]:
    """Return, for each declared label name, the values of that label on the tasks.

    Values keep the order of the task list, and equal values stay: one entry
    for each task that has the label. A name that no task has gives ``[]``.
    """
    labels: dict[str, list[str]] = {name: [] for name in names}
    if not labels:
        return labels
    for task in tasks:
        for name, values in labels.items():
            value = task.labels.get(name)
            if value is not None:
                values.append(value)
    return labels


def new_task_group(
    *,
    index: int,
    source_message: SourceMessage,
    tasks: list[ExecutorTask],
    label_names: Sequence[str],
    started_at: float,
) -> TaskGroup:
    """Create the group of one yielded task list and set ``task.group`` on each task.

    The group keeps the count and the declared labels of the tasks, never the
    tasks themselves.
    """
    group = TaskGroup(
        index=index,
        source_message=source_message,
        task_count=len(tasks),
        labels=collect_group_labels(tasks, label_names),
        started_at=started_at,
    )
    for task in tasks:
        task.group = group
    return group


class _StreamEnd:
    """The marker a pull returns when the generator has no group left."""


STREAM_END = _StreamEnd()


@dataclass(frozen=True)
class StreamCloseFailure:
    """Why a stream could not be closed cleanly: the error it raised, or ``None`` when the close timed out."""

    error: BaseException | None

    @property
    def timed_out(self) -> bool:
        """Whether the close passed its time bound."""
        return self.error is None


def retrieve_task_outcome(task: asyncio.Task) -> None:
    """Read the outcome of a task nobody awaits, so asyncio does not log an unread exception."""
    if not task.cancelled():
        task.exception()


async def next_stream_item(iterator: AsyncIterator[list[ExecutorTask]]) -> Any:
    """The next item of ``iterator``, or ``STREAM_END`` when it has none; a coroutine, so it can run as a task."""
    try:
        return await anext(iterator)
    except StopAsyncIteration:
        return STREAM_END


async def await_close(aclose: Callable[[], Awaitable[Any]]) -> None:
    """Await ``aclose()`` inside a coroutine, so it can run as a task."""
    await aclose()


async def close_stream(
    iterator: AsyncIterator[list[ExecutorTask]],
    *,
    pull: asyncio.Task | None,
    context: contextvars.Context | None,
    timeout: float,
    run_close: Callable[[Callable[[], Awaitable[Any]]], Coroutine[Any, Any, None]] = await_close,
) -> StreamCloseFailure | None:
    """Cancel the pull in progress, then ``aclose()`` the generator in ``context``, within ``timeout`` seconds.

    Returns ``None`` after a clean close, else the failure; never raises
    one. Each wait uses ``asyncio.wait``, which does not cancel what it
    waits for: a generator that catches the cancellation and keeps awaiting
    cannot hold the close past its time bound, and is left behind. A cancel
    of the caller during a wait cancels what it waits for and is passed on.
    ``run_close`` awaits ``aclose()``; a caller passes its own to run the
    close inside a scope of its own.
    """
    deadline = time.monotonic() + timeout
    if pull is not None:
        pull.cancel()
        if not await _wait_until(pull, deadline):
            return StreamCloseFailure(error=None)
        pull_error = None if pull.cancelled() else pull.exception()
        if pull_error is not None:
            # A generator that raised when its pull was cancelled.
            return StreamCloseFailure(error=pull_error)
    aclose = getattr(iterator, 'aclose', None)
    if aclose is None:
        return None
    closing = asyncio.create_task(run_close(aclose), context=context)
    closing.add_done_callback(retrieve_task_outcome)
    if not await _wait_until(closing, deadline):
        return StreamCloseFailure(error=None)
    error = None if closing.cancelled() else closing.exception()
    return None if error is None else StreamCloseFailure(error=error)


async def _wait_until(task: asyncio.Task, deadline: float) -> bool:
    """Wait for ``task`` until ``deadline``; true when it ended. A task still running then is cancelled.

    A cancel of the caller while it waits cancels ``task`` too and is passed
    on to the caller.
    """
    try:
        await asyncio.wait({task}, timeout=max(0.0, deadline - time.monotonic()))
    except asyncio.CancelledError:
        task.cancel()
        raise
    if task.done():
        return True
    task.cancel()
    return False


def drop_invalid_notes(group: TaskGroup) -> list[str]:
    """Remove the notes whose value is not a str, int, float or bool; return their keys, sorted."""
    invalid = sorted(key for key, value in group.notes.items() if type(value) not in _NOTE_VALUE_TYPES)
    for key in invalid:
        del group.notes[key]
    return invalid
