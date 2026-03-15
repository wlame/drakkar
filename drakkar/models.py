"""Data models for Drakkar framework."""

from enum import StrEnum

from pydantic import BaseModel, Field


class SourceMessage(BaseModel):
    """A message consumed from the Kafka source topic."""

    topic: str
    partition: int
    offset: int
    key: bytes | None = None
    value: bytes
    timestamp: int


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
    stderr: str = ""
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

    RETRY = "retry"
    SKIP = "skip"
