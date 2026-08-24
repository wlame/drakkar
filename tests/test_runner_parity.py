"""The probe must answer "what would production do" — so prove it does.

`DebugRunner` exists to tell an operator what the production pipeline
would make of a message. These tests run one handler through **both**
`PartitionProcessor` (production) and `DebugRunner` (the probe) and
compare what the handler saw: how many attempts, how many ``on_error``
calls, what lineage the replacements got.

Both runners now take those rules from `drakkar/taskflow.py`, so a
divergence means someone re-inlined the policy in one of them.

Everything is hermetic: tasks are ``precomputed``, so no subprocess ever
spawns and the outcomes are exact.
"""

from __future__ import annotations

import pytest

from drakkar.config import DrakkarConfig, ExecutorConfig
from drakkar.executor import ExecutorPool
from drakkar.handler import BaseDrakkarHandler
from drakkar.models import (
    ErrorAction,
    ExecutorError,
    ExecutorTask,
    PrecomputedResult,
    SourceMessage,
)
from drakkar.partition import PartitionProcessor
from drakkar.uiserver.runner import DebugRunner
from drakkar.uiserver.runner_models import ProbeInput
from tests.conftest import wait_for

BOGUS_BINARY = '/nonexistent/binary/should-never-run'


def failing_task(task_id: str, offset: int = 0) -> ExecutorTask:
    """A task whose precomputed outcome is a non-zero exit — no subprocess."""
    return ExecutorTask(
        task_id=task_id,
        source_offsets=[offset],
        precomputed=PrecomputedResult(stdout='', stderr='boom', exit_code=1, duration_seconds=0.001),
    )


def succeeding_task(task_id: str, offset: int = 0) -> ExecutorTask:
    return ExecutorTask(
        task_id=task_id,
        source_offsets=[offset],
        precomputed=PrecomputedResult(stdout='ok', stderr='', exit_code=0, duration_seconds=0.001),
    )


def make_pool() -> ExecutorPool:
    return ExecutorPool(binary_path=BOGUS_BINARY, max_executors=2, task_timeout_seconds=5)


def make_message(offset: int = 0) -> SourceMessage:
    return SourceMessage(topic='orders', partition=0, offset=offset, value=b'{"x": 1}', timestamp=1000)


class RecordingHandler(BaseDrakkarHandler):
    """Arranges one failing task and records every hook call it receives."""

    def __init__(self, on_error_action) -> None:
        self._on_error_action = on_error_action
        self.attempts: list[str] = []
        self.on_error_calls: list[str] = []
        self.completed: list[str] = []

    async def arrange(self, messages, pending):
        return [failing_task('t-1', messages[0].offset)]

    async def on_task_complete(self, result):
        self.completed.append(result.task.task_id)
        return None

    async def on_error(self, task, error: ExecutorError):
        self.attempts.append(task.task_id)
        self.on_error_calls.append(task.task_id)
        action = self._on_error_action
        return action(self) if callable(action) else action


async def run_in_production(handler, *, max_retries: int) -> None:
    """Drive one message through PartitionProcessor and wait for its commit."""
    committed: list[int] = []

    async def on_commit(partition_id, offset):
        committed.append(offset)

    processor = PartitionProcessor(
        partition_id=0,
        handler=handler,
        executor_pool=make_pool(),
        window_size=1,
        max_retries=max_retries,
        on_commit=on_commit,
    )
    processor.enqueue(make_message())
    processor.start()
    try:
        await wait_for(lambda: bool(committed), timeout=10)
    finally:
        await processor.stop()


async def run_in_probe(handler, *, max_retries: int):
    """Drive the same message through the debug probe."""
    runner = DebugRunner(
        handler=handler,
        executor_pool=make_pool(),
        app_config=DrakkarConfig(
            executor=ExecutorConfig(
                binary_path=BOGUS_BINARY,
                task_timeout_seconds=5,
                max_retries=max_retries,
            )
        ),
    )
    return await runner.run(ProbeInput(value='{"x": 1}', offset=0))


@pytest.mark.parametrize('max_retries', [0, 1, 3])
async def test_retry_budget_is_spent_identically_in_both_runners(max_retries):
    """A handler that never stops asking for RETRY must be cut off after the
    same number of attempts in the probe as in production — otherwise the
    probe reports a task that production would have retried more (or less)."""
    production = RecordingHandler(ErrorAction.RETRY)
    probe = RecordingHandler(ErrorAction.RETRY)

    await run_in_production(production, max_retries=max_retries)
    await run_in_probe(probe, max_retries=max_retries)

    # One on_error per failed attempt: the original plus every retry.
    assert len(production.on_error_calls) == max_retries + 1
    assert len(probe.on_error_calls) == len(production.on_error_calls)


async def test_skip_ends_the_task_after_one_attempt_in_both_runners():
    production = RecordingHandler(ErrorAction.SKIP)
    probe = RecordingHandler(ErrorAction.SKIP)

    await run_in_production(production, max_retries=3)
    await run_in_probe(probe, max_retries=3)

    assert len(production.on_error_calls) == 1
    assert len(probe.on_error_calls) == 1


async def test_an_unrecognised_action_is_terminal_in_both_runners():
    """A handler returning something the framework does not know must not
    retry and must not wedge — it is a terminal failure on both paths."""
    production = RecordingHandler('not-a-real-action')
    probe = RecordingHandler('not-a-real-action')

    await run_in_production(production, max_retries=3)
    await run_in_probe(probe, max_retries=3)

    assert len(production.on_error_calls) == 1
    assert len(probe.on_error_calls) == 1


class ReplacingHandler(RecordingHandler):
    """on_error replaces the failing task with two tasks that succeed."""

    def __init__(self) -> None:
        super().__init__(None)
        self.replacements: list[ExecutorTask] = []

    async def on_error(self, task, error):
        self.on_error_calls.append(task.task_id)
        self.replacements = [succeeding_task('r-1'), succeeding_task('r-2', 0)]
        # One replacement names its own parent; the framework must respect
        # that and only auto-link the other.
        self.replacements[1].parent_task_id = 'chosen-by-handler'
        return self.replacements


async def test_replacement_lineage_is_linked_the_same_way_in_both_runners():
    production = ReplacingHandler()
    probe = ReplacingHandler()

    await run_in_production(production, max_retries=3)
    await run_in_probe(probe, max_retries=3)

    for handler in (production, probe):
        assert handler.replacements[0].parent_task_id == 't-1', 'an unset parent links to the failing task'
        assert handler.replacements[1].parent_task_id == 'chosen-by-handler', 'an explicit parent is kept'
        assert sorted(handler.completed) == ['r-1', 'r-2'], 'both replacements ran to completion'
