"""Split handler: one large request is cut into parts that run on every worker.

A request carries a list of items. A request with more than ``LOCAL_LIMIT``
items is split into parts of ``PART_SIZE`` items; a smaller request runs on
the worker that owns its partition (a local message). Each item is one task
of ``split-task``, which sleeps ``sleep_ms`` and then exits by its ``mode``:
``ok`` exits 0, ``fail`` exits 1, ``hang`` sleeps ``HANG_SECONDS`` (longer
than the job timeout the harness gives the workers).

Two Kafka sinks:

  - ``split_items`` (a part sink): one record per finished item, from
    ``on_task_complete``, keyed by the request id;
  - ``split_summaries`` (the aggregate sink): one record per request, from
    ``aggregate``, keyed by the request id, with the part counts of the job
    and the items counted from ``job.results()``.

``split_pause_ms`` makes the split wait between two parts, so a scenario can
stop the owner while the split runs. ``fail_policy`` decides what a failed
item does: ``dlq`` sends the message to the DLQ (``ErrorAction.DLQ``), any
other value skips the task. The ``request_id`` hook gives the payload's
``request_id`` to every log line of a job, so a scenario finds the owner of a
request in the logs (``split_job_opened``); a request that comes over HTTP
takes its id from the ``x-request-id`` header when it has one.

Requests also arrive over HTTP (``POST /jobs``): such a request is a split job
whose sinks are off, and ``http_job_result`` gives the client the items
counted from ``job.results()`` and the number of parts.
"""

import asyncio
import os
from typing import Literal

from pydantic import BaseModel, Field

import drakkar as dk

PART_SIZE = 5
LOCAL_LIMIT = 20
# How long an item of mode `hang` sleeps: longer than the job timeout the
# scenario gives the workers (90 s), shorter than executor.task_timeout.
HANG_SECONDS = 150
ITEMS_SINK = 'split_items'
SUMMARIES_SINK = 'split_summaries'
WORKER_ID = os.environ.get('WORKER_ID', 'unknown')
# The HTTP header that names the id of a job that comes over HTTP.
REQUEST_ID_HEADER = 'x-request-id'


class Item(BaseModel):
    """One unit of work: one task."""

    id: str
    sleep_ms: int = Field(default=100, ge=0, le=60000)
    mode: Literal['ok', 'fail', 'hang'] = 'ok'


class SplitRequest(BaseModel):
    """Input: a request of items. A part has the same shape with a slice of the items."""

    request_id: str
    items: list[Item]
    split_pause_ms: int = Field(default=0, ge=0, le=60000)
    fail_policy: Literal['dlq', 'skip'] = 'skip'


class ItemRecord(BaseModel):
    """Output of the part sink: one finished item."""

    request_id: str
    item: str
    exit_code: int
    worker_id: str


class PartCounts(BaseModel):
    """The data of one part result: how many items the part had."""

    items: int


class RequestSummary(BaseModel):
    """Output of the aggregate sink: one record per request."""

    request_id: str
    job: str
    attempt: int
    local: bool
    parts: int | None
    received_parts: int
    failed_parts: int
    items: int
    tasks_succeeded: int
    worker_id: str


def task_args(item: Item) -> list[str]:
    """The arguments of split-task for one item: seconds to sleep, then the mode."""
    seconds = HANG_SECONDS if item.mode == 'hang' else item.sleep_ms / 1000
    return [f'{seconds:.3f}', item.mode]


class SplitHandler(dk.BaseDrakkarHandler[SplitRequest, ItemRecord]):
    """Split large requests, run one task per item, sum the parts up at the end."""

    part_data_model = PartCounts

    def message_label(self, msg: dk.SourceMessage) -> str:
        if msg.payload:
            return f'{msg.partition}:{msg.offset} [{msg.payload.request_id}]'
        return f'{msg.partition}:{msg.offset}'

    def request_id(self, msg: dk.SourceMessage) -> str:
        """The ``x-request-id`` header when there is one, else the request id of the payload.

        So log lines of a job name the request, and an HTTP client can choose
        the id of its job.
        """
        for name, value in msg.headers:
            if name.lower() == REQUEST_ID_HEADER and value:
                return value.decode('utf-8')
        if msg.payload is not None:
            return msg.payload.request_id
        return super().request_id(msg)

    async def split(self, message: dk.SourceMessage):
        request: SplitRequest | None = message.payload
        if request is None or len(request.items) <= LOCAL_LIMIT:
            return
        for start in range(0, len(request.items), PART_SIZE):
            if start and request.split_pause_ms:
                await asyncio.sleep(request.split_pause_ms / 1000)
            yield request.model_copy(update={'items': request.items[start : start + PART_SIZE]})

    async def arrange(self, messages: list[dk.SourceMessage], pending: dk.PendingContext) -> list[dk.ExecutorTask]:
        return [
            dk.ExecutorTask(
                task_id=dk.make_task_id('item'),
                args=task_args(item),
                metadata={
                    'request_id': msg.payload.request_id,
                    'item': item.id,
                    'mode': item.mode,
                    'fail_policy': msg.payload.fail_policy,
                },
                source_offsets=[msg.offset],
            )
            for msg in messages
            if msg.payload is not None
            for item in msg.payload.items
        ]

    async def on_error(self, task: dk.ExecutorTask, error: dk.ExecutorError) -> dk.ErrorAction:
        if task.metadata.get('mode') == 'fail' and task.metadata.get('fail_policy') == 'dlq':
            return dk.ErrorAction.DLQ
        return dk.ErrorAction.SKIP

    async def on_task_complete(self, result: dk.ExecutorResult) -> dk.CollectResult | None:
        meta = result.task.metadata
        record = ItemRecord(
            request_id=meta['request_id'],
            item=meta['item'],
            exit_code=result.exit_code,
            worker_id=WORKER_ID,
        )
        return dk.CollectResult(
            kafka=[dk.KafkaPayload(sink=ITEMS_SINK, data=record, key=record.request_id.encode())],
        )

    async def part_data(self, group: dk.MessageGroup) -> PartCounts | None:
        request: SplitRequest | None = group.source_message.payload
        if request is None:
            return None
        return PartCounts(items=len(request.items))

    async def aggregate(self, job: dk.SplitJob) -> dk.CollectResult | dk.JobAction | None:
        default = await super().aggregate(job)
        if default == dk.JobAction.DLQ:
            return default
        items = 0
        async for result in job.results():
            if isinstance(result.data, PartCounts):
                items += result.data.items
        request: SplitRequest = job.source_message.payload
        summary = RequestSummary(
            request_id=request.request_id,
            job=job.job_id,
            attempt=job.attempt,
            local=job.local,
            parts=job.total_parts,
            received_parts=job.received_parts,
            failed_parts=job.failed_parts,
            items=items,
            tasks_succeeded=job.tasks_succeeded,
            worker_id=WORKER_ID,
        )
        return dk.CollectResult(
            kafka=[dk.KafkaPayload(sink=SUMMARIES_SINK, data=summary, key=request.request_id.encode())],
        )

    async def http_job_result(self, job: dk.SplitJob) -> dict:
        """The result an HTTP client reads: the items counted from the part results, and the parts."""
        items = 0
        async for result in job.results():
            if isinstance(result.data, PartCounts):
                items += result.data.items
        if job.local:
            return {'items': items}
        return {'items': items, 'parts': job.total_parts}
