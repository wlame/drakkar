"""Shared helpers for the tests of stream handlers (``arrange_stream``).

Tasks carry precomputed results, so the executor pool never spawns a
subprocess and every test stays hermetic and fast.
"""

import asyncio
from collections.abc import Callable
from typing import Any

from pydantic import BaseModel

from drakkar.executor import ExecutorPool
from drakkar.handler import BaseDrakkarHandler
from drakkar.models import (
    CollectResult,
    ExecutorError,
    ExecutorResult,
    ExecutorTask,
    KafkaPayload,
    MessageGroup,
    PrecomputedResult,
    SourceMessage,
    TaskGroup,
)
from drakkar.partition import PartitionProcessor
from tests.conftest import wait_for


class Out(BaseModel):
    """A sink payload that names where it came from."""

    v: str


def make_msg(offset: int, partition: int = 0) -> SourceMessage:
    """A parsable source message at ``offset``."""
    return SourceMessage(topic='t', partition=partition, offset=offset, value=b'{}', timestamp=0)


def stream_task(
    task_id: str,
    offset: int,
    *,
    exit_code: int = 0,
    labels: dict[str, str] | None = None,
    stdin: str | None = 'in',
) -> ExecutorTask:
    """A task of the message at ``offset`` with a precomputed outcome."""
    return ExecutorTask(
        task_id=task_id,
        source_offsets=[offset],
        stdin=stdin,
        labels=labels or {},
        precomputed=PrecomputedResult(stdout=f'out-{task_id}', exit_code=exit_code),
    )


def make_pool() -> ExecutorPool:
    """A pool that never spawns: every task in these tests is precomputed."""
    return ExecutorPool(binary_path='/bin/echo', max_executors=4, task_timeout_seconds=10)


def stream_processor(handler: BaseDrakkarHandler, **kwargs: Any) -> PartitionProcessor:
    """A processor with no budget unless the caller passes one."""
    return PartitionProcessor(partition_id=0, handler=handler, executor_pool=make_pool(), window_size=10, **kwargs)


async def run_until_settled(proc: PartitionProcessor) -> None:
    """Start the loop, wait until the queue, the offsets and the tasks are all done, then stop."""
    proc.start()
    await wait_for(lambda: proc.queue_size == 0 and not proc.offset_tracker.has_pending() and proc.inflight_count == 0)
    await proc.stop()


class RecordingStreamHandler(BaseDrakkarHandler):
    """Yields one group per entry of ``sizes`` for every message and records each hook call.

    ``log`` holds the hook calls in order: ``pull:<offset>:<index>`` before
    each yield, ``task:<task_id>``, ``group:<offset>:<index>`` and
    ``message:<offset>``. The group hook is defined by the subclass
    ``WithGroupHook`` only, so this class reads as not implementing it.
    """

    def __init__(self, sizes: list[int] | None = None, *, exit_code: Callable[[str], int] | None = None) -> None:
        super().__init__()
        self.sizes = sizes if sizes is not None else [1]
        self.exit_code = exit_code or (lambda task_id: 0)
        self.log: list[str] = []
        self.message_groups: list[MessageGroup] = []
        self.task_groups: list[TaskGroup | None] = []

    def make_group(self, message: SourceMessage, index: int, size: int) -> list[ExecutorTask]:
        """The tasks of group ``index`` of ``message``."""
        tasks = []
        for position in range(size):
            task_id = f'm{message.offset}-g{index}-t{position}'
            tasks.append(stream_task(task_id, message.offset, exit_code=self.exit_code(task_id)))
        return tasks

    async def arrange_stream(self, message: SourceMessage):
        for index, size in enumerate(self.sizes):
            self.log.append(f'pull:{message.offset}:{index}')
            yield self.make_group(message, index, size)

    async def on_task_complete(self, result: ExecutorResult) -> CollectResult | None:
        self.log.append(f'task:{result.task.task_id}')
        self.task_groups.append(result.task.group)
        return None

    async def on_message_complete(self, group: MessageGroup) -> CollectResult | None:
        self.log.append(f'message:{group.source_message.offset}')
        self.message_groups.append(group)
        return None


class WithGroupHook(RecordingStreamHandler):
    """A recording stream handler that also overrides ``on_group_complete``."""

    def __init__(self, sizes: list[int] | None = None, **kwargs: Any) -> None:
        super().__init__(sizes, **kwargs)
        self.group_calls: list[tuple[TaskGroup, list[ExecutorResult], list[ExecutorError]]] = []

    async def on_group_complete(
        self, group: TaskGroup, results: list[ExecutorResult], errors: list[ExecutorError]
    ) -> CollectResult | None:
        self.log.append(f'group:{group.source_message.offset}:{group.index}')
        self.group_calls.append((group, list(results), list(errors)))
        return CollectResult(kafka=[KafkaPayload(data=Out(v=f'group-{group.index}'))])


class RecordingCollect:
    """An ``on_collect`` double: records where each delivered result came from and answers ``outcomes``."""

    def __init__(self, log: list[str], outcomes: dict | None = None) -> None:
        self.log = log
        self.outcomes = outcomes or {}

    async def __call__(self, result: CollectResult, partition_id: int, message_context: Any = None) -> dict:
        for payload in result.kafka:
            self.log.append(f'deliver:{payload.data.v}')
        await asyncio.sleep(0)
        return dict(self.outcomes)
