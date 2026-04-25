"""Standalone helpers used by the Message Probe runner.

Each function is pure (no module-level state, no contextvar reads) so it
can be exercised in isolation without spinning up a runner. The runner
imports these by name; tests historically import them directly from
:mod:`drakkar.debug_runner` and that path is preserved via re-export.
"""

from __future__ import annotations

import time
from typing import Any

from pydantic import BaseModel

from drakkar.debug_runner_models import ProbeCacheCall, ProbeInput, ProbeTaskEntry
from drakkar.executor import ExecutorTaskError
from drakkar.models import ExecutorTask, SourceMessage


def _make_value_preview(value: Any) -> str | None:
    """Return a short, single-line preview of ``value`` for the cache-call log.

    Truncates to ~240 chars and collapses newlines to spaces so the UI
    can render the cell on one line. Returns ``None`` for ``None``
    inputs so the UI cell stays empty (rather than displaying the
    literal string ``"None"``).
    """
    if value is None:
        return None
    # repr() handles bytes, numbers, dicts, Pydantic models; str() would
    # silently lose bytes and re-raise on exotic types. We trust repr()
    # not to raise — Python's stdlib objects always have a working
    # repr, and user-provided values that would raise here would have
    # already blown up earlier in the pipeline.
    text = repr(value)
    text = text.replace('\n', ' ').replace('\r', ' ')
    if len(text) > 240:
        text = text[:237] + '...'
    return text


def _build_source_message(probe_input: ProbeInput) -> SourceMessage:
    """Build a synthetic ``SourceMessage`` from the probe input.

    - Encodes ``value`` and ``key`` as UTF-8 bytes (the on-wire Kafka shape).
    - Defaults ``timestamp`` to the current wall-clock time in ms when
      the input did not supply one (Kafka-style epoch ms).
    """
    timestamp = probe_input.timestamp
    if timestamp is None:
        timestamp = int(time.time() * 1000)
    return SourceMessage(
        topic=probe_input.topic,
        partition=probe_input.partition,
        offset=probe_input.offset,
        key=probe_input.key.encode('utf-8') if probe_input.key is not None else None,
        value=probe_input.value.encode('utf-8'),
        timestamp=timestamp,
    )


def _serialize_payload(payload: Any) -> Any:
    """Serialize ``msg.payload`` into a JSON-safe shape for the report.

    ``deserialize_message`` typically sets ``msg.payload`` to a Pydantic
    BaseModel instance. Pydantic models round-trip cleanly through
    ``model_dump(mode='json')``; everything else (None, dict, primitive)
    passes through unchanged so user handlers that store plain dicts
    still work.
    """
    if payload is None:
        return None
    if isinstance(payload, BaseModel):
        return payload.model_dump(mode='json')
    return payload


def _summarize_cache_calls(calls: list[ProbeCacheCall]) -> dict[str, int]:
    """Compute the ``cache_summary`` counts used in the Cache calls header.

    Keys: ``calls`` (total), ``hits`` (read+hit), ``misses`` (read+miss),
    ``writes_suppressed`` (set/delete calls).
    """
    hits = sum(1 for c in calls if c.outcome == 'hit')
    misses = sum(1 for c in calls if c.outcome == 'miss')
    writes_suppressed = sum(1 for c in calls if c.outcome == 'suppressed')
    return {
        'calls': len(calls),
        'hits': hits,
        'misses': misses,
        'writes_suppressed': writes_suppressed,
    }


def _one_line_summary(exc: BaseException) -> str:
    """Short one-line summary of an exception for the stage's ``error`` field.

    The full traceback is always captured separately on the top-level
    ``DebugReport.errors`` list; this short form is what the UI shows
    inline on the corresponding stage card (arrange, message_complete,
    etc.) so the operator sees at-a-glance what went wrong.
    """
    message = str(exc) or '<no message>'
    # Collapse any embedded newlines so the cell stays on one line.
    message = message.replace('\n', ' ').replace('\r', ' ')
    return f'{type(exc).__name__}: {message}'


def _failed_task_entry(
    *,
    task: ExecutorTask,
    error: ExecutorTaskError,
    retry_of: str | None = None,
    replacement_for: str | None = None,
) -> ProbeTaskEntry:
    """Build a ``ProbeTaskEntry`` for a subprocess-level task failure.

    Used when ``ExecutorPool.execute`` raises ``ExecutorTaskError`` —
    the task never produced a successful ``ExecutorResult``, so we
    synthesise an entry from the ``ExecutorError`` attached to the
    exception. The entry starts with ``status='failed'``; callers may
    later flip it to ``'replaced'`` if on_error returns a replacement
    list (see ``DebugRunner._handle_task_failure``).

    ``retry_of`` / ``replacement_for`` thread through on_error retries
    and replacement cascades so the UI can show the lineage of each
    entry (see Tasks section C in the probe tab).
    """
    err = error.error
    result = error.result
    # Prefer the exception text when it's set (timeouts, launch failures);
    # otherwise fall back to stderr for non-zero-exit-code failures.
    subprocess_exception = err.exception or (err.stderr or None)
    return ProbeTaskEntry(
        task_id=task.task_id,
        parent_task_id=task.parent_task_id,
        labels=dict(task.labels),
        source_offsets=list(task.source_offsets),
        precomputed=task.precomputed is not None,
        status='failed',
        exit_code=err.exit_code,
        duration_seconds=result.duration_seconds,
        stdin=task.stdin or '',
        stdout=result.stdout,
        stderr=result.stderr,
        subprocess_exception=subprocess_exception,
        retry_of=retry_of,
        replacement_for=replacement_for,
    )
