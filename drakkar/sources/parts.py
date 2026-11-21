"""The parts source: the Kafka source code over the parts topic of the split feature.

Every worker with the feature on reads the parts topic in a consumer group of
its own. Its partition processors run in part mode: each message is a part of
a split job, and its result is reported to the results log before its offset
completes.
"""

from __future__ import annotations

import asyncio
import functools
from typing import TYPE_CHECKING, Any

from drakkar.sources.kafka import GroupSource, KafkaSource, LeaveOutcome, SourceGauges
from drakkar.split.partmode import PartMode, ResultWriter, part_collect
from drakkar.split.records import PARTS_PARTITION_BASE

if TYPE_CHECKING:
    from drakkar.split.runtime import SplitRuntime


class NoSourceGauges(SourceGauges):
    """The worker-level metrics describe the source topic; the parts source leaves them alone."""

    def assigned(self, count: int) -> None:
        """Not written: the gauge counts partitions of the source topic."""

    def backpressure(self, active: bool) -> None:
        """Not written: the gauge shows the pause of the source topic."""

    def queued(self, total: int) -> None:
        """Not written: the gauge counts messages of the source topic."""

    def idle(self, seconds: float) -> None:
        """Not written: the counter measures the source topic."""

    def idle_waste(self, slot_seconds: float) -> None:
        """Not written: the Kafka source already counts the free slots of the shared pool."""


class PartsSource(KafkaSource):
    """The source of the parts topic; it exists only on a worker with the split feature on.

    It shares the executor pool, the pending budget, the sink manager and the
    drain of the worker with the Kafka source. What it shows about partition
    ``n`` of the parts topic carries the number ``PARTS_PARTITION_BASE + n``,
    because a partition of the parts topic has the same numbers as a
    partition of the source topic. The handler's ``on_assign`` and
    ``on_revoke`` hooks are not called: they tell about the source topic.
    """

    name = 'parts'
    partition_label_base = PARTS_PARTITION_BASE
    _gauges = NoSourceGauges()

    def _runtime(self) -> SplitRuntime:
        """The split runtime of the worker. The lifecycle builds it before it binds the sources."""
        runtime = self._context().split
        if runtime is None:
            raise RuntimeError('the parts source needs the split runtime of the worker')
        return runtime

    async def start(self) -> None:
        """Construct the consumer and subscribe; the fleet was aligned when the Kafka source started."""
        self._runtime()
        await self._subscribe()

    def _consumer_settings(self) -> dict[str, Any]:
        """The parts topic and group on the split connection, with the poll settings of ``sources.kafka``."""
        runtime = self._runtime()
        source = self._context().config.sources.kafka
        return {
            'connection': runtime.connection,
            'source': source.model_copy(update={'topic': runtime.parts_topic, 'consumer_group': runtime.parts_group}),
            'partition_label_base': self.partition_label_base,
        }

    def _processor_settings(self, partition_id: int) -> dict[str, Any]:
        """The processor of a partition of the parts topic: part mode, and the shown partition number."""
        ctx = self._context()
        runtime = self._runtime()
        writer = ResultWriter(
            runtime.producer,
            results_topic=runtime.results_topic,
            stopping=functools.partial(self._partition_is_stopping, partition_id),
        )
        part_mode = PartMode(
            worker=runtime.worker,
            max_part_data=ctx.config.split.max_part_data,
            part_data_model=getattr(type(ctx.handler), 'part_data_model', None),
            report=writer.write,
            source_group=runtime.source_group,
        )
        return {
            **super()._processor_settings(partition_id),
            'on_collect': part_collect(ctx.sink_manager, ctx.on_collect),
            'part_mode': part_mode,
            'partition_label': self._shown(partition_id),
        }

    def _partition_is_stopping(self, partition_id: int) -> bool:
        """Whether a result of this partition gives up its retries: the worker stops, or the partition is given up."""
        processor = self.processors.get(partition_id)
        return self._stop_requested or processor is None or processor.is_stopping

    def _notify_handler_of_assign(self, partition_ids: list[int]) -> None:
        """Not called for the parts topic: the hook tells about the source topic."""

    def _notify_handler_of_revoke(self, partition_ids: list[int]) -> None:
        """Not called for the parts topic: the hook tells about the source topic."""

    def _pausable_partitions(self) -> list[int]:
        """Assigned partitions minus the stalled and the dead ones, which no pause or resume touches."""
        return [
            partition_id
            for partition_id, processor in self.processors.items()
            if partition_id not in self.stalled_partitions and not processor.is_dead
        ]

    async def pause_for_operator(self) -> list[int]:
        """Pause the fetch for an operator's debug pause; returns the shown numbers of the paused partitions."""
        if self.consumer is None:
            return []
        partition_ids = self._pausable_partitions()
        if partition_ids:
            await self.consumer.pause(partition_ids)
        return [self._shown(partition_id) for partition_id in partition_ids]

    async def resume_after_operator(self) -> None:
        """End the operator's pause; partitions that backpressure holds stay paused until its loop resumes them."""
        if self.consumer is None or self.paused:
            return
        partition_ids = self._pausable_partitions()
        if partition_ids:
            await self.consumer.resume(partition_ids)


# The outcome of a leave of several groups: the first of these that any of them gave.
_LEAVE_OUTCOME_ORDER = (LeaveOutcome.STOPPING, LeaveOutcome.TIMED_OUT, LeaveOutcome.LEFT)


class SourcePair:
    """The group memberships of the Kafka source and the parts source, left and joined as one.

    A detach leaves both consumer groups and a join enters both. A failure of
    either one is a failure of the whole step, so the caller handles the pair
    as it handles one source.
    """

    def __init__(self, source: GroupSource, parts: GroupSource) -> None:
        self.sources: tuple[GroupSource, GroupSource] = (source, parts)

    @property
    def is_ready(self) -> bool:
        """Whether both sources have polled once."""
        return all(source.is_ready for source in self.sources)

    @property
    def is_group_member(self) -> bool:
        """Whether both consumers are known to be members of their groups."""
        return all(source.is_group_member for source in self.sources)

    def _poll_waits_end(self) -> bool:
        """Whether a source will not poll any more, so a leave or a join of the pair cannot complete."""
        return any(source._poll_waits_end() for source in self.sources)

    async def leave_group(self) -> LeaveOutcome:
        """Leave both groups at the same time.

        Returns the outcome of the pair: ``STOPPING`` when a source stops,
        else ``TIMED_OUT`` when a leave timed out, else ``LEFT``.

        Raises:
            Exception: the first error of a leave, after both leaves ended.
        """
        outcomes = await self._both(source.leave_group() for source in self.sources)
        return next(outcome for outcome in _LEAVE_OUTCOME_ORDER if outcome in outcomes)

    async def join_group(self) -> bool:
        """Join every group the worker is not known to be in; true when it is a member of both afterwards.

        Raises:
            Exception: the first error of a join, after both joins ended.
        """
        outside = [source for source in self.sources if not source.is_group_member]
        return all(await self._both(source.join_group() for source in outside))

    @staticmethod
    async def _both(calls: Any) -> list[Any]:
        """Await the calls together; raise the first error only after every call has ended."""
        results = await asyncio.gather(*calls, return_exceptions=True)
        for result in results:
            if isinstance(result, BaseException):
                raise result
        return results
