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

    topic: str = Field(description='Kafka topic name the message was consumed from.')
    partition: int = Field(description='Kafka partition number.')
    offset: int = Field(description='Kafka offset of the message within the partition.')
    key: bytes | None = Field(default=None, description='Optional message key bytes from Kafka.')
    value: bytes = Field(description='Raw message value bytes from Kafka.')
    timestamp: int = Field(description='Message timestamp in milliseconds (Kafka-provided).')
    payload: Any = Field(
        default=None,
        description='Parsed payload object. Set by the handler arrange() method.',
    )


class ExecutorTask(BaseModel):
    """A task to be executed by the subprocess executor pool."""

    task_id: str = Field(description='Unique identifier for this task. See make_task_id().')
    args: list[str] = Field(
        description='Command-line arguments appended to the binary path when launching the process.'
    )
    metadata: dict = Field(
        default_factory=dict,
        description='Arbitrary key-value data carried through the pipeline. Accessible in on_task_complete().',
    )
    source_offsets: list[int] = Field(
        description=(
            'Kafka offsets of the source messages that produced this task. '
            'Used for offset watermark tracking — offsets are committed only after all sinks confirm delivery.'
        )
    )
    labels: dict[str, str] = Field(
        default_factory=dict,
        description=(
            'User-defined key-value labels displayed in the debug UI alongside task details. '
            'Use for tracing fields like request_id, user_id, or any domain-specific identifiers '
            'that help correlate tasks with source messages. Shown on the live timeline, '
            'task detail page, and debug trace view.'
        ),
    )
    env: dict[str, str] = Field(
        default_factory=dict,
        description=(
            'Per-task environment variables passed to the subprocess. '
            'Merged on top of executor.env from config — task values override config values '
            'on key conflict. Both are merged on top of the parent process environment.'
        ),
    )
    binary_path: str | None = Field(
        default=None,
        description=(
            'Optional override for the executor binary path from config. '
            'When set, this binary is used instead of executor.binary_path from the YAML/env config. '
            'If neither config nor task provides a binary_path, execution fails with a clear error.'
        ),
    )
    stdin: str | None = Field(
        default=None,
        description=(
            'Optional string written to the process stdin immediately after launch. '
            'Equivalent to redirecting a file with < in a shell. '
            'When None, the process stdin is not connected.'
        ),
    )
    parent_task_id: str | None = Field(
        default=None,
        description=(
            'task_id of the task this one was created to REPLACE. Set by the '
            'framework automatically when on_error returns a replacement list '
            '(unless the handler already set it explicitly). None for tasks '
            'produced directly by arrange(). Useful in on_message_complete '
            'to walk the replacement chain: task.parent_task_id -> original '
            'failure -> its parent -> ... up to the arrange()-produced root.'
        ),
    )


class ExecutorResult(BaseModel):
    """Result of a completed executor task."""

    exit_code: int = Field(description='Process exit code. 0 = success; any other value raises ExecutorTaskError.')
    stdout: str = Field(description='Captured stdout from the process, decoded as UTF-8 (errors replaced).')
    stderr: str = Field(description='Captured stderr from the process, decoded as UTF-8 (errors replaced).')
    duration_seconds: float = Field(
        description='Wall-clock time from process start to completion, rounded to 3 decimal places.'
    )
    task: ExecutorTask = Field(
        description='The originating ExecutorTask, available for context in on_task_complete() and on_error().'
    )
    pid: int | None = Field(
        default=None,
        description='OS process ID of the subprocess. None if the process never started.',
    )


class ExecutorError(BaseModel):
    """Error information when an executor task fails."""

    task: ExecutorTask = Field(description='The task that failed.')
    exit_code: int | None = Field(
        default=None,
        description='Process exit code. None if the process failed to start or timed out.',
    )
    stderr: str = Field(
        default='',
        description='Stderr output from the process, or a short error description.',
    )
    exception: str | None = Field(
        default=None,
        description=(
            'Exception message if the process failed to launch or timed out. None for normal non-zero exit failures.'
        ),
    )
    pid: int | None = Field(
        default=None,
        description='OS process ID of the subprocess. None if the process never started.',
    )


class PendingContext(BaseModel):
    """Context about currently in-flight tasks for a partition."""

    pending_tasks: list[ExecutorTask] = Field(
        default_factory=list,
        description='Tasks currently in-flight for this partition.',
    )
    pending_task_ids: set[str] = Field(
        default_factory=set,
        description='Set of in-flight task IDs. Used for O(1) membership checks.',
    )


class MessageGroup(BaseModel):
    """All tasks and outcomes derived from a single source message.

    Passed to ``BaseDrakkarHandler.on_message_complete`` after every task
    scheduled from one SourceMessage has reached a terminal state.

    Terminal means: succeeded, SKIP'd via ``on_error``, or exhausted
    its retry budget. When ``on_error`` returns a replacement list,
    the original task is considered "replaced" (not terminal) and the
    replacements take its place in the lifecycle — the group only
    completes when the replacement chain itself terminates.

    Note on membership:
      - ``tasks`` is the FULL scheduled history for this message,
        including tasks that were later replaced via on_error list-return.
        Kept for debuggability — a user may want to inspect what was
        attempted even if the final outcome came from a replacement.
      - ``results`` and ``errors`` count only TERMINAL outcomes. A task
        replaced via on_error contributes to neither (its successors do).
        So ``len(tasks)`` may exceed ``len(results) + len(errors)``; the
        difference is the count of replaced tasks.

    See also: ``SourceGroup`` (a future, not-yet-implemented extension
    for aggregating across multiple source messages).
    """

    source_message: SourceMessage = Field(description='The originating Kafka message.')
    tasks: list[ExecutorTask] = Field(
        default_factory=list,
        description=(
            'Every task that was scheduled for this message, including '
            'tasks later replaced via on_error list-return. Full history.'
        ),
    )
    results: list[ExecutorResult] = Field(
        default_factory=list,
        description='Terminal successes, in completion order.',
    )
    errors: list[ExecutorError] = Field(
        default_factory=list,
        description=(
            'Terminal failures (SKIP or retries exhausted). Does not '
            'include originals that were replaced via on_error — those '
            'are not considered terminal failures of the group.'
        ),
    )
    started_at: float = Field(
        description='Monotonic timestamp when arrange() produced the first task of this group.',
    )
    finished_at: float = Field(
        default=0.0,
        description='Monotonic timestamp when the last task reached a terminal state.',
    )

    @property
    def succeeded(self) -> int:
        """Number of tasks in this group that terminally succeeded."""
        return len(self.results)

    @property
    def failed(self) -> int:
        """Number of tasks in this group that terminally failed."""
        return len(self.errors)

    @property
    def total(self) -> int:
        """Total tasks ever scheduled for this group (includes replaced)."""
        return len(self.tasks)

    @property
    def replaced(self) -> int:
        """Tasks that were replaced via on_error — the delta between scheduled and terminal."""
        return max(0, self.total - self.succeeded - self.failed)

    @property
    def all_succeeded(self) -> bool:
        """True if at least one task existed and every terminal outcome was a success."""
        return self.failed == 0 and self.succeeded > 0

    @property
    def any_failed(self) -> bool:
        """True if any task ended in a terminal failure state."""
        return self.failed > 0

    @property
    def is_empty(self) -> bool:
        """True if arrange() produced no tasks for this message."""
        return self.total == 0

    @property
    def duration_seconds(self) -> float:
        """Wall-clock duration from first task scheduled to last terminal outcome."""
        return max(0.0, self.finished_at - self.started_at)


# --- Sink payload models ---
# Each payload type represents data destined for a specific sink type.
# The `sink` field names which configured sink instance receives the payload:
#   - empty string = use the default (only valid when exactly one sink of that type exists)
#   - explicit name = route to that specific sink instance
# The `data` field is always a Pydantic BaseModel; the framework serializes it
# appropriately for each sink type (JSON for Kafka/HTTP/Redis/File, dict for Postgres/Mongo).

_SINK_FIELD = Field(
    default='',
    description=(
        'Name of the configured sink instance to route this payload to. '
        'Empty string selects the default, which is only valid when exactly one '
        'sink of this type is configured. An unknown name causes a startup error.'
    ),
)


class KafkaPayload(BaseModel):
    """Payload for a Kafka sink — produces a message to a Kafka topic.

    The framework serializes `data` via `model_dump_json().encode()` as the
    Kafka message value. The `key` field is passed through as-is.
    """

    sink: str = _SINK_FIELD
    key: bytes | None = Field(
        default=None,
        description='Optional Kafka message key. Passed through as-is to the Kafka producer.',
    )
    data: BaseModel = Field(
        description='Payload model. Serialized via model_dump_json().encode() as the Kafka message value.'
    )


class PostgresPayload(BaseModel):
    """Payload for a PostgreSQL sink — inserts a row into a table.

    The framework serializes `data` via `model_dump()` to get a column-name
    to value mapping, then executes an INSERT statement.
    """

    sink: str = _SINK_FIELD
    table: str = Field(description='Target table name for the INSERT statement.')
    data: BaseModel = Field(description='Payload model. Serialized via model_dump() to a column→value dict for INSERT.')


class MongoPayload(BaseModel):
    """Payload for a MongoDB sink — inserts a document into a collection.

    The framework serializes `data` via `model_dump()` to get a dict
    suitable for MongoDB document insertion.
    """

    sink: str = _SINK_FIELD
    collection: str = Field(description='Target MongoDB collection name.')
    data: BaseModel = Field(description='Payload model. Serialized via model_dump() to a dict for document insertion.')


class HttpPayload(BaseModel):
    """Payload for an HTTP sink — sends a POST request to a configured endpoint.

    The framework serializes `data` via `model_dump_json()` as the request body
    with Content-Type: application/json.
    """

    sink: str = _SINK_FIELD
    data: BaseModel = Field(description='Payload model. Serialized via model_dump_json() as the JSON request body.')


class RedisPayload(BaseModel):
    """Payload for a Redis sink — sets a key-value pair.

    The framework serializes `data` via `model_dump_json()` as the string value.
    The full Redis key is `{config.key_prefix}{key}`. Optional TTL in seconds.
    """

    sink: str = _SINK_FIELD
    key: str = Field(description='Redis key suffix. The full Redis key is {config.key_prefix}{key}.')
    data: BaseModel = Field(description='Payload model. Serialized via model_dump_json() as the Redis string value.')
    ttl: int | None = Field(
        default=None,
        description='Optional expiry time in seconds. The key does not expire when None.',
    )


class FilePayload(BaseModel):
    """Payload for a filesystem sink — appends a JSON line to a file.

    The framework serializes `data` via `model_dump_json()` and appends it
    as a newline-terminated line (JSONL format). Creates the file if it
    doesn't exist. Raises an error if the parent directory is missing.
    """

    sink: str = _SINK_FIELD
    path: str = Field(description="File path relative to the sink's configured base_path.")
    data: BaseModel = Field(description='Payload model. Appended as a JSON line (model_dump_json() + newline).')


class CollectResult(BaseModel):
    """Result returned by on_task_complete(), on_message_complete(), and
    on_window_complete() hooks.

    Each field holds payloads destined for a specific sink type.
    The framework routes each payload to the matching configured sink,
    serializes the `data` field appropriately, and delivers it.

    Offset commits happen only after all sinks confirm delivery
    (or delivery errors are handled via on_delivery_error).

    Example::

        class MyHandler(BaseDrakkarHandler):
            async def on_task_complete(self, result):
                output = MyOutput(request_id="abc", answer="42")
                return CollectResult(
                    kafka=[KafkaPayload(data=output, key=b"abc")],
                    postgres=[PostgresPayload(table="results", data=output)],
                )
    """

    kafka: list[KafkaPayload] = Field(
        default_factory=list,
        description='Payloads routed to configured Kafka sinks.',
    )
    postgres: list[PostgresPayload] = Field(
        default_factory=list,
        description='Payloads routed to configured PostgreSQL sinks.',
    )
    mongo: list[MongoPayload] = Field(
        default_factory=list,
        description='Payloads routed to configured MongoDB sinks.',
    )
    http: list[HttpPayload] = Field(
        default_factory=list,
        description='Payloads routed to configured HTTP sinks.',
    )
    redis: list[RedisPayload] = Field(
        default_factory=list,
        description='Payloads routed to configured Redis sinks.',
    )
    files: list[FilePayload] = Field(
        default_factory=list,
        description='Payloads routed to configured filesystem sinks.',
    )

    @property
    def has_outputs(self) -> bool:
        """True if any sink field contains at least one payload."""
        return bool(self.kafka or self.postgres or self.mongo or self.http or self.redis or self.files)

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

    sink_name: str = Field(description='Configured name of the sink that failed (from sinks config).')
    sink_type: str = Field(description='Type of the sink that failed (e.g. "kafka", "postgres", "http").')
    error: str = Field(description='Human-readable error message from the failed delivery attempt.')
    payloads: list[BaseModel] = Field(
        default_factory=list,
        description='The payloads that could not be delivered to this sink.',
    )
