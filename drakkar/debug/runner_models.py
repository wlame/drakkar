"""Pydantic models + the ``_probe_stage`` contextvar shared by the Message Probe.

The runner orchestrates a probe through several hook stages — ``deserialize``,
``message_label``, ``arrange``, ``task_complete``, ``message_complete``,
``window_complete``. Each stage tags the cache calls it makes so the operator
can attribute every cache touch to the hook that triggered it. The
contextvar lives here so both the cache proxy and the runtime can import a
single shared symbol without circular dependencies.

These models are the on-wire JSON shape of the report returned by the
``/api/debug/probe`` endpoint, mirrored 1:1 in the Message Probe UI panels.
"""

from __future__ import annotations

import contextlib
import contextvars
from collections.abc import Iterator
from typing import Any, Literal

from pydantic import BaseModel, Field

from drakkar.models import CollectResult

# ---- probe-stage contextvar ------------------------------------------------
#
# The runner sets this before each hook call ('arrange',
# 'task_complete:<id>', etc.) so the DebugCacheProxy can tag every cache
# call with the hook that made it. Reset to 'unknown' after each hook.
#
# Using a ContextVar (rather than a plain attribute on the proxy) keeps
# the proxy thread/task-agnostic: concurrent probes on the same handler
# would each see their own stage tag. We still serialize probes with a
# lock in DebugRunner because swapping ``handler.cache`` is inherently
# process-wide, but the contextvar is the right choice for the stage
# tag itself.

_probe_stage: contextvars.ContextVar[str] = contextvars.ContextVar(
    'drakkar_probe_stage',
    default='unknown',
)


@contextlib.contextmanager
def _stage(name: str) -> Iterator[None]:
    """Temporarily set the ``_probe_stage`` contextvar to ``name``.

    Hook helpers wrap their body in ``with _stage('arrange'):`` so the
    contextvar reflects the current stage for any ``DebugCacheProxy``
    call the hook makes. The ``finally`` block always resets the
    contextvar via the returned token — even if the hook raises — so a
    later cache call outside the hook sees the previous stage rather
    than a leaked "arrange" tag.
    """
    token = _probe_stage.set(name)
    try:
        yield
    finally:
        _probe_stage.reset(token)


# ---- pydantic models for the report ----------------------------------------


class ProbeInput(BaseModel):
    """Body of the incoming probe request, and the echo shown in section A of the UI.

    ``value`` is received as a UTF-8 string — the runner encodes it to
    bytes when constructing the synthetic ``SourceMessage``. We keep it
    as a string on the wire because the /debug tab posts JSON, and
    base64-encoding the payload just to flip back immediately is noise.

    Length caps defend against authenticated-DoS from posting huge
    payloads through the debug endpoint — ~10MB per value / 64KB per
    topic/key is more than enough for any realistic production message.
    """

    value: str = Field(
        max_length=10_000_000,
        description='Raw message value as text. Encoded to UTF-8 bytes before the synthetic SourceMessage is built.',
    )
    key: str | None = Field(
        default=None,
        max_length=65_536,
        description='Optional Kafka message key. Encoded to UTF-8 bytes when set.',
    )
    partition: int = Field(default=0, ge=0, description='Synthetic partition id for the probe.')
    offset: int = Field(default=0, ge=0, description='Synthetic offset for the probe.')
    topic: str = Field(
        default='',
        max_length=65_536,
        description='Synthetic topic name. The endpoint substitutes the configured source topic when this is empty.',
    )
    timestamp: int | None = Field(
        default=None,
        description='Kafka-style milliseconds timestamp. Defaults to time.time() * 1000 in the runner.',
    )
    use_cache: bool = Field(
        default=False,
        description='When true, cache reads forward to the live cache; writes are ALWAYS suppressed.',
    )


class ProbeCacheCall(BaseModel):
    """One call made to the DebugCacheProxy during the probe."""

    op: Literal['get', 'set', 'peek', 'delete', 'contains'] = Field(description='Which proxy method was called.')
    key: str = Field(description='Cache key involved.')
    scope: str | None = Field(
        default=None,
        description=(
            'CacheScope name for the call (LOCAL / CLUSTER / GLOBAL). '
            'None for ops where scope is not meaningful (get/peek/delete/contains do not take a scope).'
        ),
    )
    outcome: Literal['hit', 'miss', 'suppressed'] = Field(
        description='hit / miss for reads; suppressed for writes (set/delete), since writes never reach the live cache.'
    )
    value_preview: str | None = Field(
        default=None,
        description='First ~120 chars of the value being set/returned. Newlines collapsed to spaces.',
    )
    origin_stage: str = Field(
        description='The _probe_stage value at the time of the call (e.g. "arrange", "task_complete:t-abc").'
    )
    ms_since_start: float = Field(description='Elapsed milliseconds since the runner started this probe.')


class ProbeStageResult(BaseModel):
    """Generic container for the outcome of a single hook stage.

    Used for ``arrange``, ``on_message_complete``, ``on_window_complete``.
    ``error`` carries a one-line summary of any exception; the full
    traceback goes into the top-level ``errors`` list on ``DebugReport``
    so the UI can render all failures in one panel.
    """

    duration_seconds: float | None = Field(
        default=None, description='Wall-clock duration of the hook invocation, in seconds.'
    )
    collect_result: CollectResult | None = Field(
        default=None, description='CollectResult returned by the hook, if any.'
    )
    error: str | None = Field(
        default=None, description='One-line summary of any raised exception. Full traceback goes in DebugReport.errors.'
    )


class ProbeTaskEntry(BaseModel):
    """One row of the Tasks table in section C of the Message Probe UI.

    ``status`` values:
      - ``done``      — subprocess (or precomputed fast path) succeeded.
      - ``failed``    — the task terminally failed after on_error.
      - ``replaced``  — on_error returned a replacement list, original
                        task is no longer the terminal outcome.
    """

    task_id: str = Field(description='Unique task identifier (ExecutorTask.task_id).')
    parent_task_id: str | None = Field(
        default=None, description='Optional parent task id (set when on_error returns replacements).'
    )
    labels: dict[str, str] = Field(default_factory=dict, description='User-defined labels carried on the ExecutorTask.')
    source_offsets: list[int] = Field(default_factory=list, description='Source Kafka offsets that produced this task.')
    precomputed: bool = Field(
        default=False, description='True if the task was served via ExecutorTask.precomputed (no subprocess ran).'
    )
    status: Literal['done', 'failed', 'replaced'] = Field(
        description='Terminal status of this task within the probe run.'
    )
    exit_code: int | None = Field(
        default=None, description='Process exit code, or None if the task never produced one.'
    )
    duration_seconds: float | None = Field(
        default=None, description='Wall-clock duration of the subprocess / precomputed fast path.'
    )
    stdin: str = Field(default='', description='Raw stdin text written to the subprocess (from ExecutorTask.stdin).')
    stdout: str = Field(default='', description='Captured stdout. May be very large (multi-megabyte).')
    stderr: str = Field(default='', description='Captured stderr. May be very large.')
    subprocess_exception: str | None = Field(
        default=None,
        description='Exception message when the subprocess failed to launch or timed out.',
    )
    on_task_complete_duration: float | None = Field(
        default=None,
        description='Wall-clock duration of the on_task_complete hook for this task.',
    )
    on_task_complete_result: CollectResult | None = Field(
        default=None,
        description='CollectResult returned by on_task_complete for this task, if any.',
    )
    on_task_complete_error: str | None = Field(
        default=None,
        description='One-line summary if on_task_complete raised; traceback goes in DebugReport.errors.',
    )
    retry_of: str | None = Field(
        default=None, description='task_id of the task this row retries (set when on_error returned RETRY).'
    )
    replacement_for: str | None = Field(
        default=None, description='task_id of the task this row replaces (set for items in on_error replacement lists).'
    )


class ProbeError(BaseModel):
    """One exception captured during the probe, aggregated in the Errors panel (section I)."""

    stage: str = Field(
        description='Stage identifier where the exception was raised (e.g. "arrange", "on_task_complete:t-abc").'
    )
    exception_class: str = Field(description='Fully qualified exception class name.')
    message: str = Field(description='str(exception).')
    traceback: str = Field(description='traceback.format_exc() output captured at the point of failure.')
    occurred_at_ms: float = Field(description='Milliseconds since the probe started.')


class PlannedSinkRecord(BaseModel):
    """One record that WOULD have been written to a sink if this were a real run.

    The runner never actually writes to a sink during a probe. This record
    is built from the ``CollectResult`` payload field for the corresponding
    sink type. ``extras`` carries type-specific metadata (e.g. Kafka key,
    Postgres sink instance name, etc.) without bloating the core schema.
    """

    sink_type: Literal['kafka', 'postgres', 'mongo', 'http', 'redis', 'files'] = Field(
        description='Which CollectResult field the payload came from.'
    )
    destination: str = Field(
        description=(
            'Type-specific destination — topic for kafka, table for postgres, collection for mongo, '
            'sink instance name for http, key for redis, path for files.'
        )
    )
    origin_stage: str = Field(
        description=(
            'Which stage produced this sink record (e.g. "task_complete:t-abc", "message_complete", "window_complete").'
        )
    )
    payload: Any = Field(description='The serialized payload data (from BaseModel.model_dump()).')
    extras: dict[str, Any] = Field(
        default_factory=dict, description='Type-specific metadata: kafka key, sink instance name, ttl, etc.'
    )


class DebugReport(BaseModel):
    """Full report returned by the probe endpoint.

    ``truncated=True`` signals that the runner did not complete every
    stage — either the wall-clock timeout fired or an upstream hook
    raised before the downstream stages could run. The UI renders a
    warning banner when this is set.
    """

    input: ProbeInput = Field(description='Echo of the probe request, for the Input & Deserialization card.')
    deserialize_error: ProbeError | None = Field(
        default=None,
        description='Captured exception from handler.deserialize_message, if any.',
    )
    parsed_payload: Any | None = Field(
        default=None,
        description='Parsed message payload. Typically a dict (Pydantic model dump) or None if no input_model is set.',
    )
    message_label: str | None = Field(
        default=None, description='Output of handler.message_label for the synthetic SourceMessage.'
    )
    arrange: ProbeStageResult = Field(description='Outcome of the arrange() hook.')
    tasks: list[ProbeTaskEntry] = Field(
        default_factory=list, description='One entry per ExecutorTask run through the probe, in order.'
    )
    on_message_complete: ProbeStageResult | None = Field(
        default=None, description='Outcome of the on_message_complete hook, if it ran.'
    )
    on_window_complete: ProbeStageResult | None = Field(
        default=None, description='Outcome of the on_window_complete hook, if it ran.'
    )
    planned_sink_payloads: list[PlannedSinkRecord] = Field(
        default_factory=list,
        description='Flattened list of every payload that would have been written to a sink.',
    )
    cache_calls: list[ProbeCacheCall] = Field(
        default_factory=list, description='Full log of every cache call made during the probe.'
    )
    cache_summary: dict[str, int] = Field(
        default_factory=dict,
        description='Counts keyed by calls / hits / misses / writes_suppressed. Derived from cache_calls.',
    )
    timing: dict[str, float] = Field(
        default_factory=dict,
        description=(
            'Per-stage wall-clock durations in seconds: '
            'total_wallclock, arrange, on_message_complete, on_window_complete.'
        ),
    )
    errors: list[ProbeError] = Field(
        default_factory=list, description='Every exception captured during the probe, with full traceback.'
    )
    truncated: bool = Field(
        default=False, description='True when the probe did not reach all stages (timeout or upstream failure).'
    )
