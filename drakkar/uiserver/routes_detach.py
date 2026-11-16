"""Detach JSON API: the operator's view and control of the worker's consumer group membership.

Routes:
  * ``GET  /api/v1/debug/detach`` — the detach state (always 200; ``enabled``
    in the body is the signal the UI hides its control on).
  * ``POST /api/v1/debug/detach`` — leave the consumer group for a bounded time.
  * ``POST /api/v1/debug/attach`` — join the consumer group again (idempotent).

A detach waits for the revoke drain, which can take ``executor.drain_timeout``
and more, so the two POST routes only start the transition on the main loop
and answer 202 with the state at once. The outcome shows later in
``last_request`` of the state. The mechanics live in ``drakkar.detach``.

Gating: ``require_auth`` like every UI route, plus ``ui.detach_enabled`` on the
two POST routes (403 naming the key). The switch is off by default: a detach
takes the worker out of its consumer group.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from drakkar.concurrency import dispatch_to_loop
from drakkar.detach import ACTION_ATTACH, ACTION_DETACH, REASON_MAX_CHARS, check_duration

if TYPE_CHECKING:
    from drakkar.uiserver.server import UIDeps

ACCEPTED = 202

DISABLED_DETAIL = (
    'The detach and attach routes are disabled by configuration: set ui.detach_enabled to true '
    '(it needs the Kafka source and sources.kafka.detach.enabled)'
)


class DetachRequest(BaseModel):
    """POST body of ``/api/v1/debug/detach``.

    Module scope on purpose: FastAPI resolves body models by reference, and a
    model nested inside the factory would defeat schema generation. The range
    of ``duration_seconds`` depends on the config, so the route checks it.
    """

    duration_seconds: float
    reason: str = ''


def _disabled_response() -> JSONResponse:
    """403 when the operator has not turned the routes on."""
    return JSONResponse({'detail': DISABLED_DETAIL}, status_code=403)


def create_detach_router(deps: UIDeps) -> APIRouter:
    """Build the router that owns the detach state and the operator detach and attach routes."""
    router = APIRouter(dependencies=[Depends(deps.require_auth)])
    drakkar_app = deps.drakkar_app

    def _routes_enabled() -> bool:
        # The load refuses ui.detach_enabled with the feature off; a worker with no
        # Kafka source has no group to leave, so it refuses the routes too.
        config = drakkar_app.config
        kafka = config.sources.kafka
        return bool(config.ui.detach_enabled and kafka.enabled and kafka.detach.enabled)

    async def _start(action: str, *, duration: float | None = None, reason: str = '') -> dict[str, Any]:
        """Start the transition and read the state; runs on the main loop, which owns the controller."""
        controller = drakkar_app.detach
        controller.request(action=action, duration=duration, reason=reason)
        return controller.wire_state()

    @router.get('/api/v1/debug/detach')
    async def detach_state():
        """The detach state; served also when the feature is off (``enabled: false``)."""
        return drakkar_app.detach.wire_state()

    @router.post('/api/v1/debug/detach')
    async def detach_start(body: DetachRequest):
        """Start an operator detach for ``duration_seconds`` and answer 202 at once."""
        if not _routes_enabled():
            return _disabled_response()
        try:
            check_duration(body.duration_seconds, drakkar_app.config.sources.kafka.detach)
        except ValueError as exc:
            return JSONResponse({'detail': str(exc)}, status_code=422)
        state = await dispatch_to_loop(
            _start(ACTION_DETACH, duration=body.duration_seconds, reason=body.reason[:REASON_MAX_CHARS]),
            drakkar_app.main_loop,
        )
        return JSONResponse(state, status_code=ACCEPTED)

    @router.post('/api/v1/debug/attach')
    async def attach_start():
        """Start an operator attach and answer 202 at once; on an attached worker it changes nothing."""
        if not _routes_enabled():
            return _disabled_response()
        state = await dispatch_to_loop(_start(ACTION_ATTACH), drakkar_app.main_loop)
        return JSONResponse(state, status_code=ACCEPTED)

    return router
