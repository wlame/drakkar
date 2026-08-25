"""Consume-pause JSON API: ``/api/v1/debug/consume-pause`` (contract v1.14).

Routes:
  * ``GET  /api/v1/debug/consume-pause``   — current state (always 200; carries
    ``enabled`` so the Live page can hide the control without a probe dance).
  * ``POST /api/v1/debug/consume-pause``   — pause consuming for N seconds.
  * ``POST /api/v1/debug/consume-resume``  — resume now (idempotent).

The mechanics — why this never rebalances, and how it coordinates with
backpressure and stall pauses — live in ``drakkar.consume_pause``. This
module only maps the controller onto HTTP.

Gating: ``require_auth`` like every API route, plus ``ui.consume_pause.enabled``
on the two mutating routes (403 naming the config key — the probe/merge
pattern). Unlike probe/merge this flag defaults to **false**: pausing stops
message intake, so the deployment must opt in deliberately.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import structlog
from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from drakkar.concurrency import dispatch_to_loop
from drakkar.consume_pause import MAX_PAUSE_SECONDS, ConsumerNotReadyError

if TYPE_CHECKING:
    from drakkar.uiserver.server import UIDeps

logger = structlog.get_logger()


class ConsumePauseRequest(BaseModel):
    """POST body for /api/v1/debug/consume-pause.

    Module-scope on purpose: FastAPI resolves body models by reference, and
    a model nested inside the factory would defeat schema generation.
    """

    duration_seconds: int = Field(ge=1, le=MAX_PAUSE_SECONDS)


def _disabled_response() -> JSONResponse:
    """403 when the operator has not opted in — same shape and reasoning as
    routes_debug._disabled_response (policy, not absence, refuses)."""
    return JSONResponse(
        {
            'error': 'The consume-pause endpoints are disabled by configuration '
            '(set ui.consume_pause.enabled=true to enable)'
        },
        status_code=403,
    )


def create_consume_pause_router(deps: UIDeps) -> APIRouter:
    """Build the router that owns the consume-pause endpoints."""
    router = APIRouter(dependencies=[Depends(deps.require_auth)])
    drakkar_app = deps.drakkar_app

    def _enabled() -> bool:
        return bool(drakkar_app.config.ui.consume_pause.enabled)

    @router.get('/api/v1/debug/consume-pause')
    async def consume_pause_state():
        """Current pause state. Served even when the feature is disabled —
        ``enabled: false`` in the body IS the signal the UI hides on, so
        the Live page needs no failure-probe to know."""
        return drakkar_app.consume_pause.state()

    @router.post('/api/v1/debug/consume-pause')
    async def consume_pause_start(body: ConsumePauseRequest):
        """Pause consuming for ``duration_seconds`` (replaces an active pause)."""
        if not _enabled():
            return _disabled_response()
        try:
            # The controller mutates loop-bound state and calls the consumer;
            # it must run on the app's main loop, not the UI server's.
            return await dispatch_to_loop(
                drakkar_app.consume_pause.pause(body.duration_seconds),
                drakkar_app.main_loop,
            )
        except ConsumerNotReadyError as exc:
            return JSONResponse({'detail': str(exc)}, status_code=503)

    @router.post('/api/v1/debug/consume-resume')
    async def consume_pause_resume():
        """Resume consuming now. Idempotent — resuming while not paused
        answers 200 with the (inactive) state."""
        if not _enabled():
            return _disabled_response()
        return await dispatch_to_loop(drakkar_app.consume_pause.resume(), drakkar_app.main_loop)

    return router
