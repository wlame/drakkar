"""Tests for Drakkar handler protocol and base handler."""

import pytest

from drakkar.handler import BaseDrakkarHandler
from drakkar.models import (
    ErrorAction,
    ExecutorError,
    ExecutorResult,
    ExecutorTask,
    PendingContext,
    SourceMessage,
)


@pytest.fixture
def handler() -> BaseDrakkarHandler:
    return BaseDrakkarHandler()


async def test_base_handler_arrange_raises(
    handler: BaseDrakkarHandler,
    source_message: SourceMessage,
):
    with pytest.raises(NotImplementedError, match="arrange"):
        await handler.arrange([source_message], PendingContext())


async def test_base_handler_collect_returns_none(
    handler: BaseDrakkarHandler,
    executor_result: ExecutorResult,
):
    result = await handler.collect(executor_result)
    assert result is None


async def test_base_handler_on_window_complete_returns_none(
    handler: BaseDrakkarHandler,
    executor_result: ExecutorResult,
    source_message: SourceMessage,
):
    result = await handler.on_window_complete([executor_result], [source_message])
    assert result is None


async def test_base_handler_on_error_returns_skip(
    handler: BaseDrakkarHandler,
    executor_task: ExecutorTask,
    executor_error: ExecutorError,
):
    action = await handler.on_error(executor_task, executor_error)
    assert action == ErrorAction.SKIP


async def test_base_handler_on_assign_is_noop(handler: BaseDrakkarHandler):
    await handler.on_assign([0, 1, 2])


async def test_base_handler_on_revoke_is_noop(handler: BaseDrakkarHandler):
    await handler.on_revoke([0, 1, 2])


async def test_custom_handler_overrides():
    class MyHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return [
                ExecutorTask(
                    task_id="custom-1",
                    args=["--test"],
                    source_offsets=[m.offset for m in messages],
                )
            ]

    handler = MyHandler()
    msg = SourceMessage(
        topic="t", partition=0, offset=5, value=b"x", timestamp=0
    )
    tasks = await handler.arrange([msg], PendingContext())
    assert len(tasks) == 1
    assert tasks[0].task_id == "custom-1"
    assert tasks[0].source_offsets == [5]
