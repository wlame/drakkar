"""Data models for Drakkar framework.

Defines source messages, executor tasks/results, sink payloads,
and the CollectResult that routes data to configured sinks.
"""

import hashlib
import os
import time
from enum import StrEnum
from typing import Any, Literal, TypeVar

from pydantic import BaseModel, Field

# Marks where a task / message-group originated. ``'kafka'`` is the historical
# source path (consumer poll → arrange → executor). ``'http'`` is the webapp
# pipeline path (synchronous POST → arrange_http_request → executor). Stored on
# ``ExecutorTask`` and ``MessageGroup`` so the priority gate, debug UI, and
# recorder can distinguish the two without re-deriving it from upstream state.
TaskOrigin = Literal['kafka', 'http']


def make_task_id(prefix: str = 't') -> str:
    """Generate a short, time-sortable, unique task ID.

    Format: {prefix}-{timestamp_hex}-{random_hex}
    Example: t-68561a3f1b2c-a7c2f1e3  (28 chars with default prefix)

    Time-sortable: lexicographic order matches creation order.
    Unique: nanosecond timestamp + 32-bit random suffix.

    NOT suitable for PendingContext dedup — a fresh random ID can never
    match an in-flight one. Use make_stable_task_id for that.
    """
    ts = time.time_ns()
    rnd = int.from_bytes(os.urandom(4))
    return f'{prefix}-{ts:016x}-{rnd:08x}'


def make_stable_task_id(prefix: str, *parts: str) -> str:
    """Derive a deterministic task ID from the work's content.

    Format: {prefix}-{16 hex chars of sha256}
    Example: make_stable_task_id('t', 'alpha', 'beta') -> 't-5d11bfa62398519b'

    Same prefix+parts always produce the same ID — across retries,
    restarts, workers, and backends (the Go MakeStableTaskID emits
    byte-identical output). That determinism is what makes
    ``PendingContext`` deduplication work: a redelivered message
    arranges to the same ID, which IS in ``pending_task_ids``.
    ``make_task_id`` is random and can never match — do not use it
    for dedup.

    Each part is length-prefixed (8-byte big-endian, over the part's
    UTF-8 bytes) before hashing so part boundaries matter: ('a', 'bc')
    and ('ab', 'c') yield different IDs.

    Raises ValueError when no parts are given — a partless ID would be
    one constant value and silently dedupe every task into one.
    """
    if not parts:
        raise ValueError(
            'make_stable_task_id requires at least one part; a partless id '
            'would be constant and dedupe every task into one'
        )
    digest = hashlib.sha256()
    for part in parts:
        encoded = part.encode('utf-8')
        digest.update(len(encoded).to_bytes(8, 'big'))
        digest.update(encoded)
    return f'{prefix}-{digest.hexdigest()[:16]}'


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
    parse_error: str | None = Field(
        default=None,
        description=(
            'Set by deserialize_message() when the raw value could not be '
            'parsed into the input model. None means parsing succeeded (or '
            'no input_model is configured). The framework applies the '
            'kafka.on_parse_error policy to messages with a non-None value.'
        ),
    )


class SinkDeliveryFailedError(Exception):
    """Raised when a sink delivery could not be confirmed AND the DLQ fallback
    also failed (or no DLQ sink is available).

    This is the framework's signal that a payload has nowhere safe to go.
    The partition pipeline reacts by NOT completing the affected offsets —
    the watermark stalls and the messages are redelivered after a restart
    or rebalance (at-least-once: prefer replay over silent loss).
    """

    def __init__(self, sink_name: str, sink_type: str, reason: str) -> None:
        self.sink_name = sink_name
        self.sink_type = sink_type
        self.reason = reason
        super().__init__(f'delivery to {sink_type}/{sink_name} failed and DLQ fallback failed: {reason}')


class MessageParseError(Exception):
    """Raised under ``kafka.on_parse_error: raise`` when a source message
    fails input_model deserialization.

    Propagates out of the partition processor's window loop, stopping the
    partition with an error log and leaving the offset uncommitted — the
    message is redelivered after restart. Fail-fast for deployments where
    a parse failure means the schema contract is broken.
    """

    def __init__(self, partition: int, offset: int, error: str) -> None:
        self.partition = partition
        self.offset = offset
        self.error = error
        super().__init__(f'message {partition}:{offset} failed deserialization: {error}')


class ParseFailurePayload(BaseModel):
    """DLQ payload wrapper for a source message that failed deserialization.

    Produced by the framework when ``kafka.on_parse_error: dlq`` is set.
    Carries the raw value (decoded with replacement characters) plus
    enough metadata to locate the original message in Kafka.
    """

    topic: str = Field(description='Source topic of the unparseable message.')
    partition: int = Field(description='Source partition of the unparseable message.')
    offset: int = Field(description='Source offset of the unparseable message.')
    raw_value: str = Field(description='Raw message value, UTF-8 decoded with errors replaced.')
    parse_error: str = Field(description='The deserialization error message.')


class PrecomputedResult(BaseModel):
    """A subprocess outcome supplied by the handler instead of being produced
    by running the binary.

    When a handler already knows what a task would output — whether from a
    cache, a lookup table, deterministic logic, or any other source — it
    can attach a ``PrecomputedResult`` to the ``ExecutorTask`` returned
    from ``arrange()``. The framework skips the subprocess entirely and
    synthesises an ``ExecutorResult`` from these values.

    The framework is agnostic to WHY the subprocess was skipped; this
    type just carries the outcome. Observability marks the resulting
    events with ``precomputed=true`` so operators can tell them apart
    from real executions, but the label does not imply any specific
    source (cache, lookup, etc.).
    """

    stdout: str = Field(
        default='',
        description='Process stdout that the framework would have captured.',
    )
    stderr: str = Field(
        default='',
        description='Process stderr that the framework would have captured.',
    )
    exit_code: int = Field(
        default=0,
        description=(
            'Exit code. Non-zero triggers the same on_error flow as a real '
            'subprocess failure — the handler can RETRY, SKIP, or return '
            'replacement tasks just as it would for a subprocess failure.'
        ),
    )
    duration_seconds: float = Field(
        default=0.0,
        description=(
            'Apparent duration of the "execution". Defaults to 0 for an '
            'instantaneous result; set explicitly if you want the recorder / '
            'UI to show a non-zero duration (e.g. to reflect the cache '
            'lookup time).'
        ),
    )


class ExecutorTask(BaseModel):
    """A task to be executed by the subprocess executor pool."""

    task_id: str = Field(description='Unique identifier for this task. See make_task_id().')
    args: list[str] = Field(
        default_factory=list,
        description=(
            'Command-line arguments appended to the binary path when launching '
            'the process. May be empty, especially when ``precomputed`` is set '
            'and no subprocess will run.'
        ),
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
    precomputed: PrecomputedResult | None = Field(
        default=None,
        description=(
            'If set, the framework does NOT run a subprocess for this task. '
            'It synthesises an ExecutorResult from the precomputed values and '
            'feeds it straight through on_task_complete / on_message_complete. '
            'Use for any short-circuit that avoids the subprocess: cache hits, '
            'lookup-table answers, deterministic shortcuts. The framework is '
            'agnostic to the reason — only observability marks the outcome as '
            '``precomputed=true`` in events and increments a dedicated counter.'
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
    origin: TaskOrigin = Field(
        default='kafka',
        description=(
            'Where this task originated. ``"kafka"`` for the historical '
            'consumer→arrange path, ``"http"`` for tasks produced by '
            '``arrange_http_request`` on the webapp pipeline. Read by the '
            'priority gate (``handler.task_priority``), the debug UI '
            '(rendering color/labels), and the recorder.'
        ),
    )
    client_name: str | None = Field(
        default=None,
        description=(
            'Name of the webapp client that triggered this task. Populated '
            'only for ``origin == "http"`` tasks; ``None`` for Kafka-origin '
            'tasks. Matches a ``WebClientConfig.name`` from the webapp '
            'config block.'
        ),
    )
    request_id: str | None = Field(
        default=None,
        description=(
            'Framework-assigned request id for the originating webapp '
            'request. Populated only for ``origin == "http"`` tasks; '
            '``None`` for Kafka-origin tasks. See ``make_request_id``.'
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
    stdout_truncated: bool = Field(
        default=False,
        description=(
            'True when stdout retention stopped at executor.max_stdout_bytes '
            'and the remainder was discarded. Always False when the cap is '
            'unlimited (0) and for handler-supplied precomputed results.'
        ),
    )
    stderr_truncated: bool = Field(
        default=False,
        description=(
            'True when stderr retention stopped at executor.max_stderr_bytes '
            'and the remainder was discarded. Always False when the cap is '
            'unlimited (0) and for handler-supplied precomputed results.'
        ),
    )


class ExecutorError(BaseModel):
    """Error information when an executor task fails."""

    task: ExecutorTask = Field(description='The task that failed.')
    kind: Literal['nonzero_exit', 'timeout', 'launch_failure', 'internal'] = Field(
        default='nonzero_exit',
        description=(
            'Structured failure classification — discriminate failures by this '
            "field, never by parsing ``exception`` text. Values: 'nonzero_exit' "
            '(the subprocess or precomputed result finished with a non-zero exit '
            "code; the default), 'timeout' (exceeded executor.task_timeout_seconds), "
            "'launch_failure' (the process could not be started: missing binary or "
            "spawn error), 'internal' (synthesized by the framework with no "
            'subprocess outcome behind it — e.g. a hook raised or the pool '
            'violated its contract).'
        ),
    )
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
    origin: TaskOrigin = Field(
        default='kafka',
        description=(
            'Origin of the source message that produced this group. '
            '``"kafka"`` for consumer-driven groups, ``"http"`` for groups '
            'synthesised by the webapp runner. Mirrors the same field on '
            'each contained ``ExecutorTask``.'
        ),
    )
    client_name: str | None = Field(
        default=None,
        description=(
            'Webapp client name that triggered this group. ``None`` for '
            'Kafka-origin groups; populated only for ``origin == "http"``.'
        ),
    )
    request_id: str | None = Field(
        default=None,
        description=(
            'Framework-assigned request id for the originating webapp '
            'request. ``None`` for Kafka-origin groups; populated only for '
            '``origin == "http"``.'
        ),
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


class CustomPayload(BaseModel):
    """Payload for a plugin-registered sink — passes ``data`` to the sink's ``deliver()`` method.

    Plugin sinks (registered via ``[project.entry-points."drakkar.sinks"]``
    and configured under ``sinks.custom.<type>.<instance>``) define their
    own payload semantics — Drakkar carries the payload through unchanged
    and the sink's ``deliver()`` method consumes ``data`` directly. The
    ``data`` field is typed as ``BaseModel`` (the same constraint applied
    to every other payload type) so plugin authors can define a Pydantic
    model that captures whatever shape their downstream consumes.

    Plugin authors are free to subclass this and add typed fields if
    they prefer — the framework only inspects ``sink`` (instance name)
    when routing, then hands the whole payload to the sink. Subclasses
    therefore work transparently.
    """

    sink: str = Field(
        description=(
            'Name of the configured plugin sink instance to route this payload to. '
            'Must match a key under ``sinks.custom.<type>.<instance>`` in config.'
        ),
    )
    data: BaseModel = Field(description="Payload model. Passed to the plugin sink's ``deliver()`` as-is.")


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
    custom: list[CustomPayload] = Field(
        default_factory=list,
        description=(
            "Payloads routed to plugin-registered sinks. Each payload's "
            '``sink`` field must match a configured plugin sink instance '
            'name under ``sinks.custom.<type>.<instance>``.'
        ),
    )

    @property
    def has_outputs(self) -> bool:
        """True if any sink field contains at least one payload."""
        return bool(self.kafka or self.postgres or self.mongo or self.http or self.redis or self.files or self.custom)

    @property
    def used_sink_types(self) -> set[str]:
        """Return the set of sink type names that have payloads.

        Useful for validation — the framework checks that every returned
        sink type has a corresponding configured sink. ``custom`` entries
        contribute the resolved sink type of their target instance, but
        we cannot resolve those without the SinkManager — for ``custom``
        the property reports the placeholder ``'custom'`` and full
        resolution happens in :meth:`SinkManager.validate_collect`.
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
        if self.custom:
            types.add('custom')
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
