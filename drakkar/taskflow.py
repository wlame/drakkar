"""The task-outcome rules that every runner must agree on.

Two pieces of the framework run the same handler contract over an
executor task:

- :class:`drakkar.partition.PartitionProcessor` — production;
- :class:`drakkar.uiserver.runner.DebugRunner` — the message probe, whose
  entire purpose is to answer *"what would production do with this
  message?"*.

(``drakkar.webapp.runner.WebRunner``, the third runner, deliberately runs
no ``on_error`` / retry / replacement machinery at all — an HTTP request
lives for one round-trip — so none of these rules apply to it.)

Production's and the probe's *orchestration* differ and should: production
owns asyncio tasks, message trackers and an offset watermark, while the
probe builds a report and recurses. Their *policy* must not.
A divergence between production and the probe is not a bug the operator
can see — it is the probe quietly reporting an outcome production would
never produce.

So the policy lives here, as data and small pure functions, and each
runner keeps its own bookkeeping around it.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from drakkar.hookctx import bind_hook_context, clear_hook_context
from drakkar.models import ErrorAction, ExecutorError, ExecutorResult, ExecutorTask

if TYPE_CHECKING:
    from drakkar.handler import DrakkarHandler


class TaskOutcome(StrEnum):
    """What happens to a task after ``on_error`` answered.

    ``REPLACE``
        The handler returned a list. The original task is not a terminal
        failure of its group — it is *replaced*, and the replacements
        report their own outcomes later.
    ``RETRY``
        Re-execute the same task; the retry inherits the original's slot
        in whatever the runner is tracking.
    ``FAIL``
        Terminal failure. Either the handler said so (SKIP, or anything
        it returned that is not RETRY or a list), or it asked for a retry
        the budget can no longer pay for.
    """

    REPLACE = 'replace'
    RETRY = 'retry'
    FAIL = 'fail'


@dataclass(frozen=True)
class ErrorDecision:
    """The outcome of one ``on_error`` answer, plus what the runner needs to act."""

    outcome: TaskOutcome
    replacements: tuple[ExecutorTask, ...] = ()
    #: True only when the handler asked to RETRY and the budget was spent —
    #: this is what production logs as ``max_retries_exceeded``. A plain
    #: SKIP is a terminal failure but not an exhausted one.
    retries_exhausted: bool = False


def decide_after_on_error(action: Any, *, retry_count: int, max_retries: int) -> ErrorDecision:
    """Map an ``on_error`` return value to what happens to the task.

    ``action`` is whatever the user's handler returned, deliberately typed
    loosely: ``ErrorAction`` is a ``StrEnum``, so a handler may return the
    raw string ``'retry'``, and a handler that returns something
    unrecognised must degrade to a terminal failure rather than raise.

    ``retry_count`` is how many retries this task has already had (0 on the
    first failure), and ``max_retries`` is the total budget — so
    ``max_retries=3`` allows up to four executor invocations in the worst
    case.
    """
    if isinstance(action, list):
        return ErrorDecision(outcome=TaskOutcome.REPLACE, replacements=tuple(action))
    if action == ErrorAction.RETRY:
        if retry_count < max_retries:
            return ErrorDecision(outcome=TaskOutcome.RETRY)
        return ErrorDecision(outcome=TaskOutcome.FAIL, retries_exhausted=True)
    return ErrorDecision(outcome=TaskOutcome.FAIL)


def link_replacements(parent: ExecutorTask, replacements: list[ExecutorTask]) -> list[ExecutorTask]:
    """Point each replacement back at the task it replaces, unless already set.

    Lets ``on_message_complete`` walk the replacement chain. A handler that
    set ``parent_task_id`` itself meant it, so it is never overwritten.
    Returns the same list for call-site chaining.
    """
    for replacement in replacements:
        if replacement.parent_task_id is None:
            replacement.parent_task_id = parent.task_id
    return replacements


def synthesize_internal_failure(task: ExecutorTask, exc: BaseException) -> tuple[ExecutorResult, ExecutorError]:
    """The result/error pair that stands in for a task killed by a raising hook.

    An exception out of ``on_task_complete`` (or anything else unexpected
    around a task) has no subprocess outcome behind it, but the group still
    needs one terminal result and one terminal error — otherwise a window
    never completes and its offsets never commit. ``kind='internal'`` is
    what tells a handler this failure was synthesized by the framework.
    """
    return (
        ExecutorResult(
            exit_code=-1,
            stdout='',
            stderr=str(exc),
            duration_seconds=0,
            task=task,
        ),
        ExecutorError(
            task=task,
            kind='internal',
            exception=str(exc),
            stderr=str(exc),
        ),
    )


@dataclass(frozen=True)
class HookCall:
    """The result of one user-hook invocation: an answer or an exception."""

    action: Any = None
    exception: Exception | None = None
    duration_seconds: float = 0.0

    @property
    def failed(self) -> bool:
        """Whether the hook raised instead of answering."""
        return self.exception is not None


async def call_on_error(
    handler: DrakkarHandler,
    task: ExecutorTask,
    error: ExecutorError,
    *,
    partition: int,
    window_id: int | None = None,
) -> HookCall:
    """Invoke ``handler.on_error`` under a bound hook context, capturing a raise.

    The exception is **returned, not raised**. Every caller invokes this
    from inside an ``except`` clause handling the task's own failure, where
    a raise would skip the caller's settle step — in production that leaves
    the offset pending forever and freezes the partition's commit
    watermark, because ``committable()`` stops at the first incomplete
    offset. What to *do* about a broken hook stays with the caller:
    production degrades to a terminal failure and counts the hook error,
    the probe reports it to the operator.

    The context is released in a ``finally`` so a raising hook cannot leak
    ``task_id`` into every later record this coroutine emits.
    """
    token = bind_hook_context(hook='on_error', partition=partition, window_id=window_id, task_id=task.task_id)
    started = time.monotonic()
    try:
        action = await handler.on_error(task, error)
    except Exception as exc:
        return HookCall(exception=exc, duration_seconds=time.monotonic() - started)
    finally:
        clear_hook_context(token)
    return HookCall(action=action, duration_seconds=time.monotonic() - started)
