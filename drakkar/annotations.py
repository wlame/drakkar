"""Handler-emitted diagnostic records attached to pipeline entities.

An *annotation* is structured data a handler chooses to expose to whoever
is reading the debug UI later — the inputs a hook considered, the flag that
explains why a task was built the way it was, a rejected alternative. It is
information that is not worth persisting in a sink and not worth logging on
every run, but is exactly what someone needs when they open one message and
ask "why did this happen".

Storage
-------
Annotations are rows in the flight recorder's existing ``events`` table under
the ``annotation`` event value, anchored by the same ``partition`` /
``offset`` / ``task_id`` columns every other event uses. That choice buys
three things for free: rotation and retention already expire them, the
existing trace queries already return them alongside the events they explain,
and no schema column is added, so the pinned cross-backend row shape is
untouched.

Scope comes from the target passed to :meth:`Annotator.emit`:

======================  ===========  ==========  ===========
target                  ``offset``   ``task_id``  scope
======================  ===========  ==========  ===========
``SourceMessage``       set          ``NULL``     one message
``ExecutorTask``        ``NULL``     set          one task
``None``                ``NULL``     ``NULL``     whole window
======================  ===========  ==========  ===========

Best-effort by design
---------------------
Annotations must never affect processing. Every failure path drops the record
and returns; :meth:`Annotator.emit` does not raise into user code, because a
debug-only feature that can fail a production message is a worse bug than the
one it was added to diagnose.

Payloads are **never truncated to fit**. A truncated structured document still
parses and still looks complete, so it misleads the person reading it far more
effectively than a missing record does. Oversize payloads are dropped whole and
counted. The single exception is the copy written to the warning log, which is
already a lossy human-facing artifact.

Two independent budgets guard two different resources, each per hook
invocation (see :mod:`drakkar.hookctx` for why the unit is an invocation):

* ``max_bytes_per_call`` bounds **accepted** bytes, protecting the recorder
  DB. Without it a handler annotating every message of a wide window can
  flood the events table with low-value rows, destroying the debug value
  of the whole database.
* :data:`MAX_DROP_LOGS_PER_CALL` bounds **rejected** records' log lines,
  protecting the log pipeline. It deliberately counts drops only, never
  accepted records, so one bad payload can never cost a well-formed one its
  place.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

import structlog

from drakkar import metrics
from drakkar.hookctx import HookContext, current_hook_context
from drakkar.models import ExecutorTask, SourceMessage
from drakkar.recorder.helpers import encode_json

if TYPE_CHECKING:
    from collections.abc import Mapping

logger = structlog.get_logger()

# Warning lines emitted per hook invocation before the annotator goes quiet.
# Deliberately a constant rather than config: it exists to stop one runaway
# hook from flooding the log pipeline, and an operator who needs the full
# picture reads ``drakkar_recorder_annotations_dropped_total``, which never
# suppresses. Making it tunable would invite raising it to a value that
# reintroduces the flood this guards against.
MAX_DROP_LOGS_PER_CALL = 5

# Drop reasons. Mirrored in the Go backend and in the metric's label values —
# keep the two in lockstep.
REASON_OVERSIZE = 'oversize'
REASON_BUDGET_EXHAUSTED = 'budget_exhausted'
REASON_NO_CONTEXT = 'no_context'
REASON_UNSERIALIZABLE = 'unserializable'


@runtime_checkable
class AnnotatorLike(Protocol):
    """Structural interface for the object backing ``handler.annotate``.

    Mirrors :class:`~drakkar.cache.protocol.CacheLike`: both :class:`Annotator`
    and :class:`NoOpAnnotator` satisfy it without inheriting from it, and a
    test can substitute a recording fake without constructing either.
    """

    def emit(
        self,
        target: SourceMessage | ExecutorTask | None,
        kind: str,
        data: Mapping[str, Any] | None = None,
        *,
        labels: dict[str, str] | None = None,
    ) -> None:
        """Record one annotation, or drop it and account for the drop."""
        ...


class AnnotationRecorder(Protocol):
    """The single recorder method the annotator needs.

    Declared structurally so this module stays a leaf: it does not import
    :class:`~drakkar.recorder.core.EventRecorder`, and tests can pass a
    trivial fake without constructing a recorder or touching SQLite.
    """

    def record_annotation(
        self,
        *,
        kind: str,
        partition: int,
        metadata_json: str,
        offset: int | None = None,
        task_id: str | None = None,
        labels: dict[str, str] | None = None,
    ) -> None: ...


class Annotator:
    """Applies the budgets and drop policy, then hands rows to the recorder."""

    def __init__(
        self,
        recorder: AnnotationRecorder,
        *,
        enabled: bool = True,
        max_bytes: int = 16_384,
        max_bytes_per_call: int = 262_144,
        log_max_bytes: int = 2048,
    ) -> None:
        """Configure the annotator.

        Args:
            recorder: Sink for accepted rows.
            enabled: When False every call returns immediately, recording
                nothing and counting nothing. Mirrors ``annotations_enabled``.
            max_bytes: Largest single encoded annotation accepted. ``0``
                disables the limit.
            max_bytes_per_call: Total encoded bytes accepted per hook
                invocation. ``0`` disables the limit.
            log_max_bytes: Cap on the payload copy written to the warning log
                when a record is dropped. ``0`` disables the cap.
        """
        self._recorder = recorder
        self._enabled = enabled
        self._max_bytes = max_bytes
        self._max_bytes_per_call = max_bytes_per_call
        self._log_max_bytes = log_max_bytes
        # ``no_context`` means the handler called annotate() outside any
        # framework-invoked hook — a static code bug, not a runtime condition.
        # It is logged loudly once per process and then left to the metric,
        # since there is no per-invocation budget to bound it (there is no
        # invocation) and a periodic task making the same mistake would
        # otherwise log forever.
        self._no_context_logged = False

    def emit(
        self,
        target: SourceMessage | ExecutorTask | None,
        kind: str,
        data: Mapping[str, Any] | None = None,
        *,
        labels: dict[str, str] | None = None,
    ) -> None:
        """Record one annotation, or drop it and account for the drop.

        Never raises. See the module docstring for scope and budget rules.
        """
        if not self._enabled:
            return

        ctx = current_hook_context()
        if ctx is None:
            self._drop_without_context(kind, data)
            return

        offset, task_id = _resolve_anchor(target)
        scope = _scope_name(target)
        envelope = {
            'kind': kind,
            'scope': scope,
            'hook': ctx.hook,
            'window_id': ctx.window_id,
            # ONLY window scope carries the offsets. Those rows have neither an
            # ``offset`` nor a ``task_id``, so the trace query finds them by
            # matching this array — and that is exactly why the other scopes
            # must leave it empty. A message-scoped row carrying its window's
            # offsets would match every SIBLING message's trace too, putting
            # one message's diagnostics on another's timeline.
            'offsets': list(ctx.offsets) if scope == 'window' else [],
            'data': dict(data) if data is not None else {},
        }

        try:
            encoded = encode_json(envelope)
        except (TypeError, ValueError, RecursionError) as exc:
            # encode_json's default hook str()s anything it cannot represent,
            # so this is reached only by genuinely pathological input such as
            # a self-referential structure.
            self._drop(ctx, REASON_UNSERIALIZABLE, kind, data, offset, task_id, size=0, error=str(exc))
            return

        size = len(encoded)
        reason = self._check_limits(ctx, size)
        if reason is not None:
            self._drop(ctx, reason, kind, data, offset, task_id, size=size)
            return

        self._recorder.record_annotation(
            kind=kind,
            partition=ctx.partition,
            metadata_json=encoded.decode('utf-8'),
            offset=offset,
            task_id=task_id,
            labels=labels or None,
        )
        ctx.accepted_bytes += size
        metrics.annotations_recorded.inc()

    def _check_limits(self, ctx: HookContext, size: int) -> str | None:
        """Return a drop reason, or None when the annotation may be recorded.

        ``ctx.drops`` is deliberately not consulted: rejected records must
        never influence whether the next one is admitted.
        """
        if self._max_bytes and size > self._max_bytes:
            return REASON_OVERSIZE
        if self._max_bytes_per_call and ctx.accepted_bytes + size > self._max_bytes_per_call:
            return REASON_BUDGET_EXHAUSTED
        return None

    def _drop(
        self,
        ctx: HookContext,
        reason: str,
        kind: str,
        data: Mapping[str, Any] | None,
        offset: int | None,
        task_id: str | None,
        *,
        size: int,
        error: str | None = None,
    ) -> None:
        """Account for a dropped annotation and log it within the budget.

        ``offset`` / ``task_id`` are the TARGET's anchors, not the running
        hook's. They differ whenever a hook annotates something other than the
        entity it is anchored to — a message-scoped annotation emitted from
        ``arrange`` is the common case — and the target's identity is what
        makes the log line actionable.
        """
        metrics.annotations_dropped.labels(reason=reason).inc()
        ctx.drops += 1
        if ctx.drops > MAX_DROP_LOGS_PER_CALL:
            return

        payload, truncated = self._log_payload(data)
        logger.warning(
            'annotation_dropped',
            category='annotations',
            reason=reason,
            kind=kind,
            hook=ctx.hook,
            partition=ctx.partition,
            window_id=ctx.window_id,
            offset=offset if offset is not None else ctx.offset,
            task_id=task_id if task_id is not None else ctx.task_id,
            size_bytes=size,
            data=payload,
            data_truncated=truncated,
            error=error,
        )
        if ctx.drops == MAX_DROP_LOGS_PER_CALL:
            logger.warning(
                'annotation_drops_suppressed',
                category='annotations',
                hook=ctx.hook,
                partition=ctx.partition,
                window_id=ctx.window_id,
                logged=MAX_DROP_LOGS_PER_CALL,
                detail=(
                    'further annotation drops in this hook invocation will not be logged; '
                    'see drakkar_recorder_annotations_dropped_total'
                ),
            )

    def _drop_without_context(self, kind: str, data: Mapping[str, Any] | None) -> None:
        """Account for an annotate() call made outside any hook."""
        metrics.annotations_dropped.labels(reason=REASON_NO_CONTEXT).inc()
        if self._no_context_logged:
            return
        self._no_context_logged = True

        payload, truncated = self._log_payload(data)
        logger.warning(
            'annotation_dropped',
            category='annotations',
            reason=REASON_NO_CONTEXT,
            kind=kind,
            data=payload,
            data_truncated=truncated,
            detail=(
                'annotate() was called outside a framework-invoked hook, so the record has no '
                'pipeline entity to attach to; this is logged once per process and counted thereafter'
            ),
        )

    def _log_payload(self, data: Mapping[str, Any] | None) -> tuple[str, bool]:
        """Render the payload for the warning log, capped to a readable size.

        Returns ``(text, truncated)``. This is the one place truncation is
        allowed: a log line is already lossy and human-facing, unlike the
        structured record, and an uncapped copy would push the cost of a
        dropped row onto the log pipeline, which usually bills per byte.
        """
        try:
            text = encode_json(data if data is not None else {}).decode('utf-8')
        except (TypeError, ValueError, RecursionError):
            # The payload is why we are here; refusing to describe it at all
            # would leave the operator with nothing to act on.
            text = repr(data)

        if self._log_max_bytes and len(text.encode('utf-8')) > self._log_max_bytes:
            return text.encode('utf-8')[: self._log_max_bytes].decode('utf-8', errors='replace'), True
        return text, False


class NoOpAnnotator:
    """Stateless stand-in used when no recorder is wired.

    Shared as a class-level default on ``BaseDrakkarHandler`` exactly as
    ``NoOpCache`` is, so ``self.annotate(...)`` is always callable — in unit
    tests, with ``ui.recorder.store_events=false``, and before the framework
    has finished starting up.
    """

    def emit(
        self,
        target: SourceMessage | ExecutorTask | None,
        kind: str,
        data: Mapping[str, Any] | None = None,
        *,
        labels: dict[str, str] | None = None,
    ) -> None:
        """Discard the annotation."""


def _resolve_anchor(target: SourceMessage | ExecutorTask | None) -> tuple[int | None, str | None]:
    """Map a target to its ``(offset, task_id)`` anchor columns."""
    if isinstance(target, SourceMessage):
        return target.offset, None
    if isinstance(target, ExecutorTask):
        return None, target.task_id
    return None, None


def _scope_name(target: SourceMessage | ExecutorTask | None) -> str:
    """Name the scope so the UI can label a row without inspecting columns."""
    if isinstance(target, SourceMessage):
        return 'message'
    if isinstance(target, ExecutorTask):
        return 'task'
    return 'window'
