"""Public models of the split feature: parts, part results and the job view given to ``aggregate``.

``SplitPart`` is defined in ``drakkar.models``, because ``SourceMessage`` carries it and
that module may not import this package; it is re-exported here.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, Field

from drakkar.models import SourceMessage, SplitPart

PartOutcome = Literal['ok', 'failed']
TimedOutKind = Literal['', 'job', 'idle']


class JobAction(StrEnum):
    """What ``aggregate`` or ``on_job_timeout`` can answer instead of a ``CollectResult``."""

    DONE = 'done'
    DLQ = 'dlq'


class SinkCounts(BaseModel):
    """Delivery counts of one part sink for one part or one job."""

    delivered: int = 0
    skipped: int = 0
    failed: int = 0


class PartDLQ(BaseModel):
    """The DLQ facts of a part that ended in a ``dlq`` outcome."""

    stage: Literal['execute', 'sink']
    sink: str | None = None
    task_id: str | None = None
    error: str
    attempts: int | None = None


class PartResult(BaseModel):
    """What a worker reports for one finished part."""

    index: int = Field(description='The part number inside its job, from 0.')
    worker: str = Field(description='The name of the worker that ran the part.')
    outcome: PartOutcome = Field(description='failed when the part did not run to a normal end.')
    tasks_succeeded: int = 0
    tasks_failed: int = 0
    tasks_replaced: int = 0
    deliveries: dict[str, SinkCounts] = Field(default_factory=dict, description='Counts per part sink <type>/<name>.')
    dlq: PartDLQ | None = None
    error: str | None = Field(default=None, description='The text of a parse failure or of an arrange error.')
    data: Any = Field(default=None, description='The answer of part_data: a model or a dict, None when absent.')
    data_error: str | None = Field(default=None, description='Why the data was dropped, when it was.')
    duration_seconds: float = 0.0
    finished_at: float = Field(default=0.0, description='Epoch seconds when the part finished.')


class SplitResultsUnavailable(Exception):
    """Reading the results log failed; the hook may be called again later."""


class SplitResultsExpired(Exception):
    """The records of the job are gone from the results log."""


class SplitJob:
    """The view of a job that ``aggregate`` and ``on_job_timeout`` receive."""

    def __init__(
        self,
        *,
        source_message: SourceMessage,
        job_id: str,
        attempt: int,
        local: bool,
        total_parts: int | None,
        received_parts: int,
        failed_parts: int,
        dlq_parts: int,
        tasks_succeeded: int,
        tasks_failed: int,
        tasks_replaced: int,
        deliveries: dict[str, SinkCounts],
        timed_out: TimedOutKind,
        opened_at: float,
        duration_seconds: float,
        missing: Callable[[], list[int]],
        results_source: Callable[[], AsyncIterator[PartResult]],
    ) -> None:
        self.source_message = source_message
        self.job_id = job_id
        self.attempt = attempt
        self.local = local
        self.total_parts = total_parts
        self.received_parts = received_parts
        self.failed_parts = failed_parts
        self.dlq_parts = dlq_parts
        self.tasks_succeeded = tasks_succeeded
        self.tasks_failed = tasks_failed
        self.tasks_replaced = tasks_replaced
        self.deliveries = deliveries
        self.timed_out = timed_out
        self.opened_at = opened_at
        self.duration_seconds = duration_seconds
        self._missing = missing
        self._results_source = results_source

    def missing_parts(self) -> list[int]:
        """The part numbers that have no result yet.

        Meant for a timeout handler: it checks every part number in one go,
        so its time grows with the number of parts (about 3 seconds for 16
        million parts), and the event loop waits that long.
        """
        return self._missing()

    def results(self) -> AsyncIterator[PartResult]:
        """An async iterator of the part results, read back from the results log."""
        return self._results_source()


__all__ = [
    'JobAction',
    'PartDLQ',
    'PartResult',
    'SinkCounts',
    'SplitJob',
    'SplitPart',
    'SplitResultsExpired',
    'SplitResultsUnavailable',
]
