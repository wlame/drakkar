"""Runtime health endpoints: loop-lag snapshot + on-demand task census.

Routes:
  * ``/api/v1/runtime/health``   — current state, lag history window,
                                   recent stalls. Answers from monitor
                                   memory without touching the main event
                                   loop, so it works DURING a stall.
  * ``/api/debug/runtime/units`` — census of live concurrency units
                                   (asyncio tasks here) grouped by
                                   coroutine + suspension point. Runs on
                                   the main loop; a dispatch timeout maps
                                   to 503 — which is itself a signal that
                                   the loop is not serving coroutines.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from drakkar.concurrency import dispatch_to_loop
from drakkar.runtimehealth import UNIT_LABEL, task_census

if TYPE_CHECKING:
    from drakkar.uiserver.server import UIDeps

# Wall-clock budget for the census dispatch. Generous relative to any
# healthy loop iteration; when it expires the loop is effectively stalled
# and the endpoint's 503 says so explicitly.
CENSUS_TIMEOUT_SECONDS: float = 5.0


def create_runtime_router(deps: UIDeps) -> APIRouter:
    """Build the runtime-health router. Pure factory, no side effects."""
    router = APIRouter()

    @router.get('/api/v1/runtime/health')
    async def api_runtime_health():
        """Current runtime health snapshot + in-memory lag history."""
        monitor = deps.drakkar_app._runtime_health
        if monitor is None:
            return JSONResponse(
                {'enabled': False, 'reason': 'runtime_health.enabled is false'},
                status_code=404,
            )
        return JSONResponse({'enabled': True, **monitor.snapshot()})

    @router.get('/api/debug/runtime/units')
    async def api_debug_runtime_units():
        """Census of live concurrency units, grouped by suspension point."""

        async def _census_on_loop() -> list[dict]:
            return task_census()

        try:
            rows = await asyncio.wait_for(
                dispatch_to_loop(_census_on_loop(), deps.drakkar_app.main_loop),
                timeout=CENSUS_TIMEOUT_SECONDS,
            )
        except TimeoutError:
            return JSONResponse(
                {
                    'error': 'census dispatch timed out — the runtime is not serving coroutines (stalled?)',
                    'timeout_seconds': CENSUS_TIMEOUT_SECONDS,
                },
                status_code=503,
            )
        return JSONResponse({'unit_label': UNIT_LABEL, 'total': sum(r['count'] for r in rows), 'units': rows})

    return router
