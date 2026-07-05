"""Webapp data models — request context, response report, sink summary.

These models flow through the synchronous-HTTP pipeline. They are split
out from :mod:`drakkar.webapp.server` so user code can type-hint client
wrappers (``-> WebReport``) and tests can construct synthetic contexts
without spinning up the FastAPI server.

The split mirrors the project's broader convention: small Pydantic /
dataclass containers for cross-cutting types live near the feature that
owns them, with the user-facing handful re-exported from
``drakkar/__init__.py``.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field, PlainSerializer

from drakkar.timefmt import format_rfc3339_micro

# JSON dumps of report timestamps use the canonical cross-backend format
# (fixed six-digit microseconds, ``Z`` suffix) so a Python worker's
# response body is byte-identical to the Go backend's for the same
# instant. ``when_used='json'`` keeps plain ``model_dump()`` returning
# real datetimes for in-process consumers.
UTCMicroDatetime = Annotated[
    datetime,
    PlainSerializer(format_rfc3339_micro, return_type=str, when_used='json'),
]

# ---------------------------------------------------------------------------
# WebRequestContext — the per-request envelope passed from T2 (webapp loop)
# to T1 (main loop) via ``dispatch_to_loop``.
#
# Implemented as a ``dataclass`` rather than a Pydantic model because the
# ``cancelled: asyncio.Event`` field is loop-bound: it MUST be created on
# the loop that will await it. Pydantic v2 forbids arbitrary types in its
# BaseModel by default, and even with ``arbitrary_types_allowed=True`` an
# ``asyncio.Event`` carried through ``.model_validate(...)`` would not bind
# to the right loop. A plain dataclass keeps the field assignment trivial
# and the loop-binding contract explicit (the runner sets it as the first
# line of ``run()`` so the Event is created on T1).
# ---------------------------------------------------------------------------


@dataclass
class WebRequestContext:
    """Per-request envelope for the synchronous-HTTP pipeline.

    Constructed on T2 (the webapp event loop) at request entry, then
    passed across to T1 (the main pipeline loop) via
    :func:`drakkar.concurrency.dispatch_to_loop`. Carries the parsed
    request body, the resolved client identity, the framework request id,
    request headers, and a cancellation event (lazily created on T1 so
    the Event binds to the correct loop).

    Attributes
    ----------
    request_id:
        Framework or user-supplied request id (validated to ASCII, ≤64
        chars, no whitespace). Stamped on tasks, recorder rows, and the
        response body.
    client_name:
        The matched ``WebClientConfig.name`` from auth — propagated onto
        ``ExecutorTask`` / ``MessageGroup`` and used for metric labels.
    request:
        The parsed ``HttpRequestT`` Pydantic model (body of the POST).
        Typed as ``Any`` here because the concrete type is user-defined
        per-handler; the runner narrows it before invoking user hooks.
    started_at:
        UTC timestamp of when the request was accepted on T2 — used for
        ``WebReport.duration_ms`` and the response timeline.
    headers:
        Request headers as received by FastAPI. ``Mapping[str, str]``
        because handler hooks (``http_request_id``) inspect them but
        should never mutate them.
    cancelled:
        Set by T2 when its outer ``wait_for`` times out, signalling T1
        to skip side effects (sinks, ``on_http_request_complete``).
        ``None`` until ``WebappRunner.run()`` populates it on T1 — the
        Event MUST be created on the loop that will await ``is_set()``.
    """

    request_id: str
    client_name: str
    request: Any
    started_at: datetime
    headers: Mapping[str, str]
    cancelled: asyncio.Event | None = None


# ---------------------------------------------------------------------------
# Response building blocks. Pydantic models so they round-trip cleanly
# through ``model_dump()`` / ``model_validate()`` for tests, and so the
# JSON schema is self-describing for downstream client-wrapper authors.
# ---------------------------------------------------------------------------


class TaskReport(BaseModel):
    """Compact per-task record included in the response body.

    Excludes stdout/stderr deliberately — operators read those via the
    recorder/debug UI, never via the webapp response body (size + PII
    blast radius). See plan section "Response shape" for the rationale.
    """

    task_id: str
    exit_code: int | None = None
    duration_ms: float = 0.0
    retries: int = 0


class TaskSummary(BaseModel):
    """Aggregate counts for the per-request task fan-out."""

    total: int = 0
    success: int = 0
    failed: int = 0


class CacheStats(BaseModel):
    """Cache hit/miss counts captured during the request lifetime."""

    hits: int = 0
    misses: int = 0


class StageTiming(BaseModel):
    """Single entry in the response timeline.

    The runner populates one of these per pipeline stage so operators
    can see where time was spent (arrange / execute / sinks /
    on_http_request_complete) without correlating timestamps across
    log lines.
    """

    stage: str
    duration_ms: float


class SinkResult(BaseModel):
    """Per-sink delivery outcome (one entry per sink type touched)."""

    attempted: int = 0
    delivered: int = 0
    dlq: int = 0
    errors: list[str] = Field(default_factory=list)


class SinkDeliverySummary(BaseModel):
    """Aggregate sink delivery results for one request.

    Keyed by sink type (``kafka``, ``postgres``, ``mongo`` …). Emitted
    only on the ``sinks_enabled=True`` path; the response body carries
    ``sinks: null`` when sinks are disabled (the default mode).

    Modelled as ``dict[str, SinkResult]`` rather than a fixed-field model
    because the set of active sinks is config-driven and we want the
    response to mirror exactly what was attempted, not advertise empty
    slots for every supported sink type.
    """

    by_type: dict[str, SinkResult] = Field(default_factory=dict)


WebReportStatus = Literal[
    'ok',
    'error',
    'timeout',
    'rate_limited',
    'auth_failed',
    'shutdown',
    'not_ready',
]


class WebReport(BaseModel):
    """Assembled JSON response for a synchronous-HTTP request.

    Mirrors the shape documented in the plan's "Response shape" section.
    The user's ``HttpResponseT`` lives under ``result``; everything else
    is framework-built (timing, task list, cache stats, sink summary,
    timeline). NO subprocess stdout/stderr is ever included in the
    response body.
    """

    request_id: str
    client: str
    started_at: UTCMicroDatetime
    finished_at: UTCMicroDatetime
    duration_ms: float
    status: WebReportStatus
    result: Any | None = None
    tasks: list[TaskReport] = Field(default_factory=list)
    task_summary: TaskSummary = Field(default_factory=TaskSummary)
    cache: CacheStats = Field(default_factory=CacheStats)
    sinks: SinkDeliverySummary | None = None
    timeline: list[StageTiming] = Field(default_factory=list)
    error: str | None = None
