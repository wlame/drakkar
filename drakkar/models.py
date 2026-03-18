"""Data models for Drakkar framework."""

import os
import time
from enum import StrEnum
from typing import Any, TypeVar

from pydantic import BaseModel, Field


def make_task_id(prefix: str = 't') -> str:
    """Generate a short, time-sortable, unique task ID.

    Format: {prefix}-{timestamp_hex}-{random_hex}
    Example: t-68561a3f1b2c-a7c2f1e3  (28 chars with default prefix)

    Time-sortable: lexicographic order matches creation order.
    Unique: nanosecond timestamp + 32-bit random suffix.
    """
    ts = time.time_ns()
    rnd = int.from_bytes(os.urandom(4))
    return f'{prefix}-{ts:016x}-{rnd:08x}'


InputT = TypeVar('InputT', bound=BaseModel)
OutputT = TypeVar('OutputT', bound=BaseModel)


class SourceMessage(BaseModel):
    """A message consumed from the Kafka source topic."""

    topic: str
    partition: int
    offset: int
    key: bytes | None = None
    value: bytes
    timestamp: int
    payload: Any = None


class ExecutorTask(BaseModel):
    """A task to be executed by the subprocess executor pool."""

    task_id: str
    args: list[str]
    metadata: dict = Field(default_factory=dict)
    source_offsets: list[int]


class ExecutorResult(BaseModel):
    """Result of a completed executor task."""

    task_id: str
    exit_code: int
    stdout: str
    stderr: str
    duration_seconds: float
    task: ExecutorTask
    pid: int | None = None


class ExecutorError(BaseModel):
    """Error information when an executor task fails."""

    task: ExecutorTask
    exit_code: int | None = None
    stderr: str = ''
    exception: str | None = None
    pid: int | None = None


class PendingContext(BaseModel):
    """Context about currently in-flight tasks for a partition."""

    pending_tasks: list[ExecutorTask] = Field(default_factory=list)
    pending_task_ids: set[str] = Field(default_factory=set)


class OutputMessage(BaseModel):
    """A message to produce to the Kafka target topic."""

    key: bytes | None = None
    value: bytes

    @classmethod
    def from_model(cls, model: BaseModel, key: bytes | None = None) -> 'OutputMessage':
        """Create an OutputMessage by JSON-serializing a Pydantic model."""
        return cls(key=key, value=model.model_dump_json().encode())


class DBRow(BaseModel):
    """A row to write to PostgreSQL."""

    table: str
    data: dict


class CollectResult(BaseModel):
    """Result of collect/on_window_complete hook — output messages and DB rows."""

    output_messages: list[OutputMessage] = Field(default_factory=list)
    db_rows: list[DBRow] = Field(default_factory=list)


class ErrorAction(StrEnum):
    """Actions the error hook can return."""

    RETRY = 'retry'
    SKIP = 'skip'
