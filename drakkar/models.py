"""Data models for Drakkar framework.

Defines source messages, executor tasks/results, sink payloads,
and the CollectResult that routes data to configured sinks.
"""

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


# --- Sink payload models ---
# Each payload type represents data destined for a specific sink type.
# The `sink` field names which configured sink instance receives the payload:
#   - empty string = use the default (only valid when exactly one sink of that type exists)
#   - explicit name = route to that specific sink instance
# The `data` field is always a Pydantic BaseModel; the framework serializes it
# appropriately for each sink type (JSON for Kafka/HTTP/Redis/File, dict for Postgres/Mongo).


class KafkaPayload(BaseModel):
    """Payload for a Kafka sink — produces a message to a Kafka topic.

    The framework serializes `data` via `model_dump_json().encode()` as the
    Kafka message value. The `key` field is passed through as-is.
    """

    sink: str = ''
    key: bytes | None = None
    data: BaseModel


class PostgresPayload(BaseModel):
    """Payload for a PostgreSQL sink — inserts a row into a table.

    The framework serializes `data` via `model_dump()` to get a column-name
    to value mapping, then executes an INSERT statement.
    """

    sink: str = ''
    table: str
    data: BaseModel


class MongoPayload(BaseModel):
    """Payload for a MongoDB sink — inserts a document into a collection.

    The framework serializes `data` via `model_dump()` to get a dict
    suitable for MongoDB document insertion.
    """

    sink: str = ''
    collection: str
    data: BaseModel


class HttpPayload(BaseModel):
    """Payload for an HTTP sink — sends a POST request to a configured endpoint.

    The framework serializes `data` via `model_dump_json()` as the request body
    with Content-Type: application/json.
    """

    sink: str = ''
    data: BaseModel


class RedisPayload(BaseModel):
    """Payload for a Redis sink — sets a key-value pair.

    The framework serializes `data` via `model_dump_json()` as the string value.
    The full Redis key is `{config.key_prefix}{key}`. Optional TTL in seconds.
    """

    sink: str = ''
    key: str
    data: BaseModel
    ttl: int | None = None


class FilePayload(BaseModel):
    """Payload for a filesystem sink — appends a JSON line to a file.

    The framework serializes `data` via `model_dump_json()` and appends it
    as a newline-terminated line (JSONL format). Creates the file if it
    doesn't exist. Raises an error if the parent directory is missing.
    """

    sink: str = ''
    path: str
    data: BaseModel


class CollectResult(BaseModel):
    """Result returned by collect() and on_window_complete() hooks.

    Each field holds payloads destined for a specific sink type.
    The framework routes each payload to the matching configured sink,
    serializes the `data` field appropriately, and delivers it.

    Offset commits happen only after all sinks confirm delivery
    (or delivery errors are handled via on_delivery_error).

    Example::

        class MyHandler(BaseDrakkarHandler):
            async def collect(self, result):
                output = MyOutput(request_id="abc", answer="42")
                return CollectResult(
                    kafka=[KafkaPayload(data=output, key=b"abc")],
                    postgres=[PostgresPayload(table="results", data=output)],
                )
    """

    kafka: list[KafkaPayload] = Field(default_factory=list)
    postgres: list[PostgresPayload] = Field(default_factory=list)
    mongo: list[MongoPayload] = Field(default_factory=list)
    http: list[HttpPayload] = Field(default_factory=list)
    redis: list[RedisPayload] = Field(default_factory=list)
    files: list[FilePayload] = Field(default_factory=list)

    @property
    def has_outputs(self) -> bool:
        """True if any sink field contains at least one payload."""
        return bool(
            self.kafka or self.postgres or self.mongo or self.http or self.redis or self.files
        )

    @property
    def used_sink_types(self) -> set[str]:
        """Return the set of sink type names that have payloads.

        Useful for validation — the framework checks that every returned
        sink type has a corresponding configured sink.
        """
        types: set[str] = set()
        if self.kafka:
            types.add('kafka')
        if self.postgres:
            types.add('postgres')
        if self.mongo:
            types.add('mongo')
        if self.http:
            types.add('http')
        if self.redis:
            types.add('redis')
        if self.files:
            types.add('filesystem')
        return types


class ErrorAction(StrEnum):
    """Actions the on_error hook can return for processing failures."""

    RETRY = 'retry'
    SKIP = 'skip'


class DeliveryAction(StrEnum):
    """Actions the on_delivery_error hook can return for sink delivery failures.

    DLQ: Write the failed payloads to the dead letter queue (default).
    RETRY: Retry delivery (up to max_retries from config).
    SKIP: Drop the payloads and continue processing.
    """

    DLQ = 'dlq'
    RETRY = 'retry'
    SKIP = 'skip'


class DeliveryError(BaseModel):
    """Error context passed to the on_delivery_error handler hook.

    Contains all information about which sink failed, what error occurred,
    and the payloads that could not be delivered.
    """

    sink_name: str
    sink_type: str
    error: str
    payloads: list[BaseModel] = Field(default_factory=list)
