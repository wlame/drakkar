"""Split jobs JSON API: ``GET /api/v1/debug/split``.

The route lists the split jobs of this worker, read-only and from the
worker's memory. The whole answer is built on the main loop in one bounded
dispatch, because the job tables belong to that loop; when the loop does not
answer in time the route answers 503. It answers 200 also with the feature
off, where the body has ``enabled: false`` and empty lists. It follows
``require_auth`` like every UI route, and answers cross-origin reads like the
other fan-out paths.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse

from drakkar.metrics import split_result_write_failures
from drakkar.split.view import build_split_state

if TYPE_CHECKING:
    from drakkar.uiserver.server import UIDeps

DEFAULT_JOB_LIMIT = 100
MAX_JOB_LIMIT = 500
MAIN_LOOP_SILENT_DETAIL = 'the main loop did not answer in time'
MS_PER_SECOND = 1000

# Distinguishes "the dispatch timed out" from any answer the view can give.
_NO_ANSWER: Any = object()


def create_split_router(deps: UIDeps) -> APIRouter:
    """Build the router that serves the split jobs of this worker."""
    router = APIRouter(dependencies=[Depends(deps.require_auth)])
    recorder = deps.recorder

    async def build_on_main_loop(limit: int) -> dict[str, Any]:
        """Build the answer; runs on the main loop, which owns the job tables."""
        app = deps.drakkar_app
        config = app._config
        runtime = app._split
        kafka = config.sources.kafka
        return build_split_state(
            runtime,
            config.split if runtime is not None else None,
            worker=recorder.worker_name,
            instance=recorder.instance,
            source_topic=kafka.topic if kafka.enabled else None,
            consumer_group=config.resolved_consumer_group or None,
            now_ms=int(time.time() * MS_PER_SECOND),
            now_s=time.monotonic(),
            limit=limit,
            result_write_failures=int(split_result_write_failures._value.get()),
        )

    @router.get('/api/v1/debug/split')
    async def split_state(limit: int = Query(DEFAULT_JOB_LIMIT, ge=1, le=MAX_JOB_LIMIT)) -> Any:
        """The split jobs of this worker, at most ``limit`` of them."""
        state = await deps.dispatch_bounded(build_on_main_loop(limit), default=_NO_ANSWER)
        if state is _NO_ANSWER:
            return JSONResponse({'detail': MAIN_LOOP_SILENT_DETAIL}, status_code=503)
        return state

    return router
