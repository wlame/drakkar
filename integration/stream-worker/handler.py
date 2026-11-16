"""Stream handler: one message turns into many small groups of tasks.

A request names how many groups to make and how many tasks each group has.
``arrange_stream()`` yields the groups one at a time, so the worker holds at
most about ``executor.max_pending_tasks`` tasks in memory, however large the
request is. Every task carries its group's name as the label ``group``.

Two kinds of record go to the ``stream_records`` Kafka sink, keyed by the
request id:

  - one ``group`` record per group, from ``on_group_complete()``, which also
    keeps the group's number of succeeded tasks in ``group.notes['done']``;
  - one ``final`` record per message, from ``on_message_complete()``, with
    the sum of ``notes['done']`` over the groups.

Each record carries ``run_id``, a random value of this worker process, so a
reader can tell the records of a run from those of the process before a
restart (the message then runs again from its first group).
"""

import os
import secrets
from typing import Literal

from pydantic import BaseModel, Field

import drakkar as dk

# A random value of this process: the records of one run share it.
RUN_ID = secrets.token_hex(4)
GROUP_LABEL = 'group'
SINK_NAME = 'stream_records'


class StreamRequest(BaseModel):
    """Input: how many groups of how many tasks one message makes."""

    request_id: str
    groups: int = Field(ge=1, le=10000)
    tasks_per_group: int = Field(ge=1, le=1000)
    task_seconds: float = Field(default=0.3, ge=0, le=10)


class StreamRecord(BaseModel):
    """Output: one record per finished group, and one per finished message."""

    kind: Literal['group', 'final']
    request_id: str
    run_id: str
    worker_id: str
    # Group records only.
    group_index: int | None = None
    group_name: str | None = None
    task_count: int | None = None
    succeeded: int | None = None
    failed: int | None = None
    # True when every task of the group carried the group's name as its label.
    labels_match: bool | None = None
    # Final records only.
    group_count: int | None = None
    done_total: int | None = None


def group_name(index: int) -> str:
    """The name of the group with the given index, the value of its tasks' label."""
    return f'g{index:03d}'


class StreamHandler(dk.BaseDrakkarHandler[StreamRequest, StreamRecord]):
    """Yield the groups of a request, report each group and each message."""

    group_labels = (GROUP_LABEL,)

    def message_label(self, msg: dk.SourceMessage) -> str:
        if msg.payload:
            return f'{msg.partition}:{msg.offset} [{msg.payload.request_id}]'
        return f'{msg.partition}:{msg.offset}'

    async def arrange_stream(self, message: dk.SourceMessage):
        request: StreamRequest | None = message.payload
        if request is None:
            return
        for index in range(request.groups):
            name = group_name(index)
            yield [
                dk.ExecutorTask(
                    task_id=dk.make_task_id('stream'),
                    args=[str(request.task_seconds), name],
                    source_offsets=[message.offset],
                    labels={GROUP_LABEL: name},
                    metadata={'request_id': request.request_id},
                )
                for _ in range(request.tasks_per_group)
            ]

    async def on_group_complete(
        self,
        group: dk.TaskGroup,
        results: list[dk.ExecutorResult],
        errors: list[dk.ExecutorError],
    ) -> dk.CollectResult | None:
        request: StreamRequest = group.source_message.payload
        name = group_name(group.index)
        group.notes['done'] = group.succeeded
        values = group.labels.get(GROUP_LABEL, [])
        record = StreamRecord(
            kind='group',
            request_id=request.request_id,
            run_id=RUN_ID,
            worker_id=os.environ.get('WORKER_ID', 'unknown'),
            group_index=group.index,
            group_name=name,
            task_count=group.task_count,
            succeeded=group.succeeded,
            failed=group.failed,
            labels_match=len(values) == group.task_count and all(value == name for value in values),
        )
        return self._to_sink(record)

    async def on_message_complete(self, group: dk.MessageGroup) -> dk.CollectResult | None:
        request: StreamRequest | None = group.source_message.payload
        if request is None:
            return None
        record = StreamRecord(
            kind='final',
            request_id=request.request_id,
            run_id=RUN_ID,
            worker_id=os.environ.get('WORKER_ID', 'unknown'),
            group_count=len(group.groups),
            done_total=sum(int(task_group.notes.get('done', 0)) for task_group in group.groups),
        )
        return self._to_sink(record)

    @staticmethod
    def _to_sink(record: StreamRecord) -> dk.CollectResult:
        """One Kafka payload of the record, keyed by its request id."""
        return dk.CollectResult(
            kafka=[
                dk.KafkaPayload(
                    data=record,
                    key=record.request_id.encode(),
                    sink=SINK_NAME,
                ),
            ],
        )
