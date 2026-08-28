"""Handler-emitted markers, ranges, and flag pins drawn on the live timeline.

A *timeline event* is an instance of a type declared in
``ui.timeline.events`` (see :class:`~drakkar.config.TimelineEventType`) — a
deploy marker, an incident range, a flag pin — that a handler emits from
inside a hook to annotate the live Debug UI timeline with domain events as
they happen. Structurally it rides the same machinery as
:mod:`drakkar.annotations`: :class:`TimelineEventEmitter` builds a payload
and hands it to :meth:`~drakkar.annotations.Annotator.emit` under the fixed
kind ``'timeline_event'``, so window scope, byte caps, no-context handling,
and the accepted-counter all come from the annotator unchanged. This module
adds only the layer specific to timeline events: validating an instance
against its declared type before it ever reaches the annotator.

Best-effort by design, same as annotations
-------------------------------------------
:meth:`TimelineEventEmitter.emit` never raises into user code. A handler
calling ``self.timeline_event(...)`` with a typo'd type name or a
malformed range should never fail the message it was emitted from — a
debug-only visualization is a worse bug to crash the pipeline over than the
one it was added to diagnose. Every validation failure is a drop: it
increments ``drakkar_recorder_annotations_dropped_total`` (the same counter
:mod:`drakkar.annotations` uses, since it is one budget shared by every
kind of dropped diagnostic record) and logs one ``timeline_event_dropped``
error, then returns.

Disabled types are the one exception to "loud": ``enabled: false`` on a
declared type is deliberate operator config, not a mistake, so emitting it
is a silent no-op — no metric, no log. Unknown types and malformed
emissions stay loud because those are genuinely config or code errors an
operator needs to notice.
"""

from __future__ import annotations

import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

import structlog

from drakkar import metrics
from drakkar.hookctx import current_hook_context

if TYPE_CHECKING:
    from drakkar.annotations import AnnotatorLike
    from drakkar.config import TimelineEventType

logger = structlog.get_logger()

TIMELINE_EVENT_KIND = 'timeline_event'
REASON_UNKNOWN_TYPE = 'unknown_type'
REASON_BAD_SHAPE = 'bad_shape'

# Click behaviors that correlate the event with a set of tasks, so they
# need a match — either given explicitly or auto-filled from the current
# window.
_MATCH_ACTIONS = frozenset({'highlight', 'filter'})


@dataclass(slots=True, frozen=True)
class TimelineMatch:
    """Which tasks a highlight/filter event correlates with; exactly one field set."""

    window_id: int | None = None
    offsets: tuple[tuple[int, int], ...] | None = None
    label: tuple[str, str] | None = None

    def field_count(self) -> int:
        """Count how many of the three correlation fields are set."""
        return sum(v is not None for v in (self.window_id, self.offsets, self.label))

    def wire(self) -> dict[str, Any]:
        """Render the one set field in its wire shape."""
        if self.window_id is not None:
            return {'window_id': self.window_id}
        if self.offsets is not None:
            return {'offsets': [list(pair) for pair in self.offsets]}
        assert self.label is not None  # callers guarantee exactly one field is set
        return {'label': list(self.label)}


class TimelineEventEmitter:
    """Validates instances against the declared types, then rides the annotator."""

    def __init__(self, annotator: AnnotatorLike, types: Mapping[str, TimelineEventType]) -> None:
        """Configure the emitter.

        Args:
            annotator: Where accepted events are handed off — the same
                ``Annotator`` instance backing ``handler.annotate``.
            types: Declared timeline event types, keyed by name. An emitted
                ``type_name`` not present here is dropped as unknown.
        """
        self._annotator = annotator
        self._types = dict(types)

    def emit(
        self,
        type_name: str,
        text: str = '',
        *,
        ts: datetime | None = None,
        end_ts: datetime | None = None,
        values: Mapping[str, Any] | None = None,
        match: TimelineMatch | None = None,
    ) -> None:
        """Record one timeline event instance, or drop it and account for the drop.

        Never raises. See the module docstring for the drop policy.

        Args:
            type_name: Name of a type declared in ``ui.timeline.events``.
            text: Instance text substituted into the type's label/link
                templates.
            ts: Event start time. Defaults to now.
            end_ts: Event end time. Required for ``kind=range`` types,
                rejected for every other kind, and must not precede ``ts``.
            values: Extra instance data available to link templates.
            match: Which tasks this event correlates with, for
                ``action=highlight``/``action=filter`` types. Auto-filled
                from the current window when omitted.
        """
        decl = self._types.get(type_name)
        if decl is None:
            self._drop(REASON_UNKNOWN_TYPE, type_name)
            return
        if not decl.enabled:
            return  # deliberate config, not an error: no metric, no log

        ts_s = ts.timestamp() if isinstance(ts, datetime) else time.time()
        end_s = end_ts.timestamp() if isinstance(end_ts, datetime) else None

        if decl.kind == 'range' and end_s is None:
            self._drop(REASON_BAD_SHAPE, type_name, detail='kind=range requires end_ts')
            return
        if decl.kind != 'range' and end_s is not None:
            self._drop(REASON_BAD_SHAPE, type_name, detail=f'end_ts given for kind={decl.kind!r}, not range')
            return
        if end_s is not None and end_s < ts_s:
            self._drop(REASON_BAD_SHAPE, type_name, detail='end_ts precedes ts')
            return

        if match is not None and match.field_count() != 1:
            self._drop(REASON_BAD_SHAPE, type_name, detail='match must set exactly one of window_id/offsets/label')
            return

        if match is None and decl.action in _MATCH_ACTIONS:
            ctx = current_hook_context()
            if ctx is not None and ctx.window_id is not None:
                match = TimelineMatch(window_id=ctx.window_id)
            else:
                self._drop(
                    REASON_BAD_SHAPE,
                    type_name,
                    detail=f'action={decl.action!r} requires a match and none could be auto-filled',
                )
                return

        payload = {
            'type': type_name,
            'ts_ms': int(ts_s * 1000),
            'end_ts_ms': int(end_s * 1000) if end_s is not None else None,
            'text': text,
            'values': dict(values) if values else {},
            'match': match.wire() if match is not None else None,
        }
        self._annotator.emit(None, TIMELINE_EVENT_KIND, payload)

    def _drop(self, reason: str, type_name: str, *, detail: str = '') -> None:
        """Account for a dropped event instance and log it.

        Unlike :meth:`drakkar.annotations.Annotator._drop`, this is not
        rate-limited: unknown types and shape errors are rare config
        mistakes, not a hot path a runaway hook could flood.
        """
        metrics.annotations_dropped.labels(reason=reason).inc()
        logger.error('timeline_event_dropped', reason=reason, type=type_name, detail=detail or None)


class NoOpTimelineEventEmitter:
    """Installed by default; real wiring happens in lifecycle when the recorder exists."""

    def emit(
        self,
        type_name: str,
        text: str = '',
        *,
        ts: datetime | None = None,
        end_ts: datetime | None = None,
        values: Mapping[str, Any] | None = None,
        match: TimelineMatch | None = None,
    ) -> None:
        """Discard the event."""
        return
