"""A worker that owns one source partition, for the tests of the owner side of a split.

``World`` is what outlives a worker: the topics (a ``FakeLog``) and the clock.
``OwnerWorker`` is one worker over it, built from real objects: a ``SplitRuntime``
with its job tracker and its producer over the fake clients, a real partition
processor with its owner, and a real sink manager. A test that restarts the
owner builds a second ``OwnerWorker`` on the same world.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from typing import Any

from pydantic import BaseModel

from drakkar.budget import PendingBudget
from drakkar.config import DrakkarConfig
from drakkar.config.sinks import DEFAULT_SINK_POLICY
from drakkar.executor import ExecutorPool
from drakkar.models import CollectResult, ErrorAction, KafkaPayload, PostgresPayload, SourceMessage, SplitPart
from drakkar.partition import PartitionProcessor
from drakkar.sinks.manager import SinkManager
from drakkar.split.models import JobAction, PartResult, SplitJob
from drakkar.split.owner import SplitOwner
from drakkar.split.partmode import PartMode, no_report, part_collect
from drakkar.split.records import (
    JobRef,
    LogEntry,
    closed_body,
    log_record,
    open_body,
    parse_log_record,
    parse_part_headers,
    result_body,
)
from drakkar.split.runtime import SplitRuntime
from drakkar.split.table import JobTable
from tests.split_helpers import FakeClock, FakeConsumerClient, FakeLog, FakeMessage, FakeProducerClient, FakeRecord
from tests.test_message_dlq import CountingSink, FanOutHandler, ScriptedPool
from tests.test_sink_delivery_policy import CollectingRecorder, RecordingDLQ

GROUP = 'workers'
SOURCE_TOPIC = 'requests'
PARTS_TOPIC = 'requests_workers_split_parts'
RESULTS_TOPIC = 'requests_workers_split_results'
RESULTS_GROUP = 'workers_split_results'
PARTITION = 3
PARTS_PARTITIONS = 8
WORKER = 'worker-a'
AGGREGATE_SINK = ('kafka', 'summaries')
PART_SINK = ('postgres', 'main')
REQUEST_ID = 'req-of-the-original'
# The answer of ``SplittingHandler.aggregate`` that stands for the default of the framework.
DEFAULT_ANSWER = 'default'


class Item(BaseModel):
    n: int = 0


def source_message(offset: int = 500, value: bytes = b'{"n": 5}') -> SourceMessage:
    """A message of the source topic, with a source header and a ``dk-`` header a part must not carry."""
    return SourceMessage(
        topic=SOURCE_TOPIC,
        partition=PARTITION,
        offset=offset,
        key=b'request-key',
        value=value,
        timestamp=1_793_000_000_456,
        headers=[('traceparent', b'00-ab-01'), ('dk-stage', b'old')],
    )


def summary(n: int = 1) -> CollectResult:
    """A result for the aggregate sink."""
    return CollectResult(kafka=[KafkaPayload(sink=AGGREGATE_SINK[1], data=Item(n=n))])


def part_sink_result() -> CollectResult:
    """A result for the part sink."""
    return CollectResult(postgres=[PostgresPayload(table='t', data=Item())])


def owner_config(*, on_send_failure: str = 'drop', on_parse_error: str = 'skip', **split: Any) -> DrakkarConfig:
    """A config with the split feature on; ``split`` overrides keys of its section."""
    values: dict[str, Any] = {
        'connections': {'kafka': {'main': {}}},
        'sources': {
            'kafka': {
                'enabled': True,
                'topic': SOURCE_TOPIC,
                'consumer_group': GROUP,
                'on_parse_error': on_parse_error,
                'startup_align': {'enabled': False},
            }
        },
        'split': {'enabled': True, **split},
        'dlq': {'on_send_failure': on_send_failure},
        'storage': {'dir': ''},
        'recorder': {'enabled': False},
        'api': {'enabled': False},
        'ui': {'enabled': False, 'release': {'enabled': False}},
        'metrics': {'enabled': False},
        'runtime_health': {'enabled': False},
        'executor': {'binary_path': '/usr/bin/echo', 'drain_timeout': '1s'},
        'sinks': {
            'kafka': {AGGREGATE_SINK[1]: {'topic': 'summaries'}},
        },
    }
    return DrakkarConfig.model_validate(values)


class ManualSleep:
    """An injectable sleep: every call waits until the test fires its duration."""

    def __init__(self) -> None:
        self.waiters: list[tuple[float, asyncio.Future[None]]] = []
        self.calls: list[float] = []

    async def __call__(self, seconds: float) -> None:
        future = asyncio.get_running_loop().create_future()
        self.calls.append(seconds)
        self.waiters.append((seconds, future))
        await future

    def waiting(self, seconds: float, count: int = 1) -> bool:
        return sum(1 for waited, future in self.waiters if waited == seconds and not future.done()) >= count

    async def fire(self, seconds: float, count: int = 1) -> None:
        """Wait for ``count`` sleepers of ``seconds`` and wake every sleeper of that duration."""
        await wait_until(lambda: self.waiting(seconds, count))
        woken = [future for waited, future in self.waiters if waited == seconds]
        self.waiters = [(waited, future) for waited, future in self.waiters if waited != seconds]
        for future in woken:
            if not future.done():
                future.set_result(None)
        await asyncio.sleep(0.01)


async def wait_until(condition: Callable[[], bool], timeout: float = 3.0) -> None:
    """Poll ``condition`` every millisecond; fail the test when it does not hold in time."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not condition():
        if loop.time() > deadline:
            raise AssertionError('condition not met in time')
        await asyncio.sleep(0.001)


class World:
    """The topics and the clock that every worker of a test shares."""

    def __init__(self) -> None:
        self.clock = FakeClock()
        self.log = FakeLog(self.clock)

    def seconds(self) -> float:
        """The clock in epoch seconds, as the tracker and the owner read it."""
        return self.clock() / 1000

    def entries(self) -> list[LogEntry]:
        """The records of the results partition of ``PARTITION``, parsed, in log order."""
        parsed = [
            parse_log_record(record.headers, record.value) for record in self.log.records(RESULTS_TOPIC, PARTITION)
        ]
        return [entry for entry in parsed if entry is not None]

    def kinds(self) -> list[str]:
        """The kinds of the records of the results partition, in log order."""
        return [entry.kind for entry in self.entries()]

    def closed(self) -> list[dict[str, Any]]:
        """The bodies of the ``closed`` records, in log order."""
        return [entry.body for entry in self.entries() if entry.kind == 'closed']

    def part_records(self) -> list[tuple[int, FakeRecord, SplitPart]]:
        """Every record of the parts topic with its partition and its split headers, by attempt and part number."""
        found = []
        for partition in range(PARTS_PARTITIONS):
            for record in self.log.records(PARTS_TOPIC, partition):
                part = parse_part_headers(record.headers)
                assert part is not None
                found.append((partition, record, part))
        return sorted(found, key=lambda item: (item[2].source_offset, item[2].attempt, item[2].index))

    def parts(self, *, attempt: int | None = None, offset: int | None = None) -> list[SplitPart]:
        """The parts that were published, optionally of one attempt or one source offset."""
        return [
            part
            for _, _, part in self.part_records()
            if (attempt is None or part.attempt == attempt) and (offset is None or part.source_offset == offset)
        ]

    def put(self, ref: JobRef, kind: Any, body: dict[str, Any]) -> int:
        """Append one record to the results partition, as any worker could."""
        record = log_record(ref, kind, body)
        return self.log.append(RESULTS_TOPIC, PARTITION, key=record.key, value=record.value, headers=record.headers)

    def ref(self, offset: int, attempt_id: str, attempt: int = 1) -> JobRef:
        return JobRef(group=GROUP, partition=PARTITION, offset=offset, attempt=attempt, attempt_id=attempt_id)

    def put_open(self, offset: int, attempt_id: str, *, attempt: int = 1, instance: str = 'f' * 16) -> int:
        body = open_body(worker='worker-gone', instance=instance, request_id=REQUEST_ID)
        return self.put(self.ref(offset, attempt_id, attempt), 'open', body)

    def put_closed(self, offset: int, attempt_id: str, outcome: Any = 'aggregated') -> int:
        body = closed_body(outcome=outcome, received=1, parts=1, duration_seconds=1.0)
        return self.put(self.ref(offset, attempt_id), 'closed', body)

    def report(self, part: SplitPart, **fields: Any) -> None:
        """Write the result of one part to the results log, as the worker that ran it does."""
        ref = JobRef(
            group=part.group,
            partition=part.source_partition,
            offset=part.source_offset,
            attempt=part.attempt,
            attempt_id=part.attempt_id,
        )
        result = PartResult(**{'index': part.index, 'worker': 'worker-b', 'outcome': 'ok', **fields})
        self.put(ref, 'result', result_body(result))

    def report_all(self, parts: list[SplitPart] | None = None, **fields: Any) -> None:
        """Write an ``ok`` result for each of ``parts``; every published part when none are given."""
        for part in self.parts() if parts is None else parts:
            self.report(part, **fields)


class SplittingHandler(FanOutHandler):
    """Splits by a plan: the items of each source offset; an offset with no plan is a local message.

    An item that is an exception is raised at its place in the generator,
    and an ``asyncio.Event`` is awaited there. ``answer`` is what
    ``aggregate`` returns: a value, an exception to raise, or a callable
    that gets the job. Without one the default ``aggregate`` of the
    framework answers.
    """

    def __init__(self, plan: dict[int, list[Any]] | None = None, *, answer: Any = DEFAULT_ANSWER) -> None:
        super().__init__(['t-a'])
        self.plan = plan or {}
        self.answer = answer
        self.split_calls: list[int] = []
        self.pulls = 0
        self.closed_splits = 0
        self.jobs: list[SplitJob] = []
        self.timeouts: list[SplitJob] = []
        self.assigned: list[list[int]] = []
        self.revoked: list[list[int]] = []

    async def split(self, message: SourceMessage) -> AsyncIterator[Any]:
        self.split_calls.append(message.offset)
        try:
            for item in self.plan.get(message.offset, []):
                self.pulls += 1
                if isinstance(item, BaseException):
                    raise item
                if isinstance(item, asyncio.Event):
                    await item.wait()
                    continue
                yield item
        finally:
            self.closed_splits += 1

    async def aggregate(self, job: SplitJob) -> CollectResult | JobAction | None:
        self.jobs.append(job)
        answer = self.answer
        if answer is DEFAULT_ANSWER:
            return await super().aggregate(job)
        if callable(answer):
            answer = answer(job)
            if asyncio.iscoroutine(answer):
                answer = await answer
        if isinstance(answer, BaseException):
            raise answer
        return answer

    async def on_job_timeout(self, job: SplitJob) -> CollectResult | JobAction | None:
        self.timeouts.append(job)
        return await super().on_job_timeout(job)


def items(count: int) -> list[Any]:
    """``count`` parts for a split plan."""
    return [Item(n=index) for index in range(count)]


class OwnerWorker:
    """One worker that owns ``PARTITION`` of the source topic."""

    def __init__(
        self,
        world: World,
        handler: Any,
        *,
        instance: str = 'a' * 16,
        config: DrakkarConfig | None = None,
        pool: ExecutorPool | None = None,
        dlq_ok: bool = True,
        aggregate_sink_fails: bool = False,
        aggregate_on_failure: str = 'dlq',
        window_size: int = 10,
        budget: PendingBudget | None = None,
        client: FakeProducerClient | None = None,
        executor_on_failure: str = 'skip',
    ) -> None:
        self.world = world
        self.handler = handler
        # The app tells the handler what its default ``on_error`` answers.
        handler._executor_on_failure = ErrorAction(executor_on_failure)
        self.config = config or owner_config()
        self.sleep = ManualSleep()
        self.client = client if client is not None else FakeProducerClient(world.log)
        self.tracker_consumers: list[FakeConsumerClient] = []
        self.dlq = RecordingDLQ(ok=dlq_ok)
        self.recorder = CollectingRecorder()
        self.commits: list[int] = []
        self.stalls: list[int] = []
        self.aggregate_sink = CountingSink(AGGREGATE_SINK[1], AGGREGATE_SINK[0], fail=aggregate_sink_fails)
        self.part_sink = CountingSink(PART_SINK[1], PART_SINK[0])
        self.manager = SinkManager()
        no_retries = DEFAULT_SINK_POLICY.model_copy(update={'max_retries': 0})
        self.manager.register(
            self.aggregate_sink,
            policy=no_retries.model_copy(update={'on_failure': aggregate_on_failure}),
        )
        self.manager.register(self.part_sink, policy=no_retries)
        on_send_failure = self.config.dlq.on_send_failure
        self.manager.attach_runtime(
            recorder=self.recorder,  # ty: ignore[invalid-argument-type]
            dlq_sink=self.dlq,  # ty: ignore[invalid-argument-type]
            dlq_on_send_failure=on_send_failure,
        )
        self.manager.set_split_stages(frozenset({AGGREGATE_SINK}))
        self.runtime = SplitRuntime(
            self.config,
            worker=WORKER,
            instance=instance,
            producer_factory=lambda client_config: self.client,
            metadata=self._topic_counts,
            retention=self._no_retention,
            consumer_factory=self._tracker_consumer,
            results_reader=self._results_reader,
            clock=world.seconds,
            sleep=self.sleep,
        )
        part_mode = PartMode(
            worker=WORKER,
            max_part_data=self.config.split.max_part_data,
            part_data_model=getattr(type(handler), 'part_data_model', None),
            report=no_report,
            source_group=GROUP,
        )
        self.processor = PartitionProcessor(
            partition_id=PARTITION,
            handler=handler,
            executor_pool=pool if pool is not None else ScriptedPool(),
            window_size=window_size,
            max_retries=3,
            on_collect=part_collect(self.manager, self.on_collect),
            on_commit=self._on_commit,
            on_stall=self._on_stall,
            recorder=self.recorder,  # ty: ignore[invalid-argument-type]
            on_parse_error=self.config.sources.kafka.on_parse_error,
            dlq_send=self.manager.send_parse_dlq,
            on_dlq_failure=on_send_failure,
            execute_dlq=self.manager.send_execute_dlq,
            executor_on_failure=executor_on_failure,
            part_mode=part_mode,
            budget=budget,
        )
        self.owner: SplitOwner | None = None
        self.table: JobTable | None = None

    async def _topic_counts(self) -> dict[str, int]:
        return {SOURCE_TOPIC: 4, PARTS_TOPIC: PARTS_PARTITIONS, RESULTS_TOPIC: 4}

    async def _no_retention(self, topic: str) -> int | None:
        return None

    def _tracker_consumer(self, client_config: dict) -> FakeConsumerClient:
        consumer = FakeConsumerClient(self.world.log, group_id=RESULTS_GROUP)
        consumer.wait_for_records = True
        self.tracker_consumers.append(consumer)
        return consumer

    def _results_reader(self) -> FakeConsumerClient:
        return FakeConsumerClient(self.world.log, group_id=RESULTS_GROUP, auto_offset_reset='error')

    async def on_collect(self, result: CollectResult, partition_id: int, message_context: Any = None) -> Any:
        return await self.manager.deliver_all(
            result,
            on_delivery_error=self.handler.on_delivery_error,
            partition_id=partition_id,
            message_context=message_context,
        )

    async def _on_commit(self, partition_id: int, offset: int) -> None:
        self.commits.append(offset)
        self.world.log.commit(GROUP, SOURCE_TOPIC, partition_id, offset)
        self.runtime.note_source_commit(partition_id, offset)

    async def _on_stall(self, partition_id: int) -> None:
        self.stalls.append(partition_id)

    async def start(self, *messages: SourceMessage) -> OwnerWorker:
        """Start the runtime, restore the partition as an assignment does, then start the processor."""
        await self.restore()
        return self.run(*messages)

    async def restore(self) -> None:
        """Start the runtime and restore the job table of the partition; no owner exists yet."""
        await self.runtime.start()
        committed = self.world.log.committed(GROUP, SOURCE_TOPIC, PARTITION)
        self.table = await self.runtime.follow(PARTITION, committed_source_offset=committed)

    def run(self, *messages: SourceMessage) -> OwnerWorker:
        """Build the owner on the restored table and start the processor."""
        assert self.table is not None
        self.owner = self.runtime.owner_for(
            PARTITION,
            table=self.table,
            host=self.processor,
            handler=self.handler,
            sink_manager=self.manager,
            on_collect=self.on_collect,
            on_dlq_failure=self.config.dlq.on_send_failure,
        )
        self.processor.attach_split_owner(self.owner)
        for message in messages:
            self.processor.enqueue(message)
        self.processor.start()
        return self

    async def caught_up(self) -> None:
        """Wait until the job tracker has read every record the results partition holds now."""
        end = self.world.log.watermarks(RESULTS_TOPIC, PARTITION)[1]
        followed = self.runtime.tracker._followed
        await wait_until(lambda: PARTITION in followed and followed[PARTITION].next_offset >= end)

    def give(self, *messages: SourceMessage) -> None:
        for message in messages:
            self.processor.enqueue(message)

    @property
    def pending(self) -> int:
        return self.processor.offset_tracker.pending_count

    @property
    def committed(self) -> int | None:
        """The last offset this worker committed for the source partition."""
        return self.commits[-1] if self.commits else None

    def dlq_records(self) -> list[Any]:
        return [record for record, _ in self.dlq.sent]

    def hold_tracker_reads(self) -> asyncio.Event:
        """Make the job tracker read nothing from the results log until the returned event is set.

        A tracker reads a record some time after its acknowledgement; with
        this a test decides how long.
        """
        [consumer] = self.tracker_consumers
        released = asyncio.Event()
        collect = consumer._collect
        consumer._collect = lambda num_messages: collect(num_messages) if released.is_set() else []
        return released

    def hide_offsets_of(self, *kinds: str) -> None:
        """Make the delivery report of every record of ``kinds`` of the results log name no offset.

        The record is written as usual; only its report differs.
        """
        deliver = self.client._deliver

        def deliver_without_offset(item: Any) -> None:
            entry = parse_log_record(item.headers, item.value) if item.topic == RESULTS_TOPIC else None
            if entry is None or entry.kind not in kinds or item.on_delivery is None:
                deliver(item)
                return
            report, item.on_delivery = item.on_delivery, None
            deliver(item)
            report(None, FakeMessage(topic=item.topic, partition=item.partition))

        self.client._deliver = deliver_without_offset  # ty: ignore[invalid-assignment]

    async def stop(self) -> None:
        """A clean stop: the processor first, then the runtime."""
        await self.processor.stop()
        await self.runtime.stop(deadline=0.0)

    async def crash(self) -> None:
        """End the worker with no clean stop: nothing is cancelled in the log and nothing is committed."""
        self.processor.suppress_deliveries()
        loop_task = self.processor._task
        tasks = [task for task in (loop_task, *self.processor._active_tasks) if task is not None]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await self.runtime.stop(deadline=0.0)
