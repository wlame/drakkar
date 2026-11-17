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

import asyncio
import inspect
import threading
from collections.abc import Coroutine
from typing import TYPE_CHECKING, Any

import structlog
from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from drakkar.concurrency import dispatch_to_loop
from drakkar.detach import ACTION_ATTACH, ACTION_DETACH, REASON_MAX_CHARS, check_duration
from drakkar.uiserver import server as server_module

if TYPE_CHECKING:
    from drakkar.uiserver.server import UIDeps

logger = structlog.get_logger()

ACCEPTED = 202

DISABLED_DETAIL = (
    'The detach and attach routes are disabled by configuration: set ui.detach_enabled to true '
    '(it needs the Kafka source and sources.kafka.detach.enabled)'
)

NO_ANSWER_DETAIL = 'The worker did not answer in time; try again'


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


def _duration_error(message: str) -> JSONResponse:
    """422 in the validation envelope of the API, for a duration that the config range refuses."""
    error = {'loc': ['body', 'duration_seconds'], 'msg': message, 'type': 'value_error'}
    return JSONResponse({'detail': [error]}, status_code=422)


async def _dispatch_start(
    coro: Coroutine[Any, Any, dict[str, Any]], main_loop: Any, abandoned: threading.Event
) -> JSONResponse:
    """Run ``coro`` on the main loop and answer 202 with its state, or 503 when the loop does not answer.

    The wait is bounded like every UI read: a wedged main loop gives a 503,
    not a request that hangs. A timeout does not remove ``coro`` from the
    main loop's queue: the loop runs it whenever it wakes. So ``abandoned``
    is set before the 503 goes out, and ``coro`` checks it before it starts
    anything; a request answered 503 is not started later. A request whose
    step began on the main loop before ``abandoned`` was set still runs. That
    window is the limit itself plus one turn of this server loop: the time
    from the timeout to the ``except`` block below, which sets the flag.
    A closed loop (a stop) raises ``RuntimeError`` before ``coro`` is
    scheduled; it is closed then, so no warning about a coroutine that was
    never awaited is raised.
    """
    try:
        state = await asyncio.wait_for(
            dispatch_to_loop(coro, main_loop),
            # Read at call time, so a test can shorten it.
            timeout=server_module.MAIN_LOOP_DISPATCH_TIMEOUT_SECONDS,
        )
    except (TimeoutError, RuntimeError) as exc:
        abandoned.set()
        if inspect.getcoroutinestate(coro) == inspect.CORO_CREATED and isinstance(exc, RuntimeError):
            coro.close()
        await logger.awarning(
            'ui_detach_request_unanswered',
            category='debug',
            error_type=type(exc).__name__,
            error=str(exc),
        )
        return JSONResponse({'detail': NO_ANSWER_DETAIL}, status_code=503)
    return JSONResponse(state, status_code=ACCEPTED)


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

    async def _start(
        action: str, *, abandoned: threading.Event, duration: float | None = None, reason: str = ''
    ) -> dict[str, Any]:
        """Start the transition and read the state; runs on the main loop, which owns the controller.

        No ``await`` comes before ``request()``: the check of ``abandoned`` and
        the start run in one step of the main loop.
        """
        controller = drakkar_app.detach
        if not abandoned.is_set():
            controller.request(action=action, duration=duration, reason=reason)
        return controller.wire_state()

    async def _dispatch(action: str, *, duration: float | None = None, reason: str = '') -> JSONResponse:
        """Start ``action`` on the main loop with a flag of its own that a 503 sets."""
        abandoned = threading.Event()
        return await _dispatch_start(
            _start(action, abandoned=abandoned, duration=duration, reason=reason),
            drakkar_app.main_loop,
            abandoned,
        )

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
            return _duration_error(str(exc))
        return await _dispatch(ACTION_DETACH, duration=body.duration_seconds, reason=body.reason[:REASON_MAX_CHARS])

    @router.post('/api/v1/debug/attach')
    async def attach_start():
        """Start an operator attach and answer 202 at once; on an attached worker it changes nothing."""
        if not _routes_enabled():
            return _disabled_response()
        return await _dispatch(ACTION_ATTACH)

    return router
