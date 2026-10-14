"""Worker-to-worker routes: ``/api/v1/peer/*``.

Workers call these on each other and send no token, so the router carries
no ``require_auth``. They reveal only what a peer needs: the ``cluster``
and ``global`` cache rows this worker originated, and only while it
serves the cache exchange.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from drakkar.cache.changes import parse_changes_query
from drakkar.cache.models import _now_ms

if TYPE_CHECKING:
    from drakkar.uiserver.server import UIDeps

EXCHANGE_OFF_ERROR = 'The cache exchange is off on this worker'
NO_ANSWER_ERROR = 'The cache did not answer in time'

# Default of dispatch_bounded when the read times out or fails: told apart
# from the engine's own ``None`` ("I do not serve"), which is a 404.
_NO_ANSWER = object()


def create_peer_router(deps: UIDeps) -> APIRouter:
    """The public router of the peer routes."""
    router = APIRouter()

    @router.get('/api/v1/peer/cache/changes')
    async def api_peer_cache_changes(request: Request):
        """One page of this worker's own ``cluster``/``global`` cache rows after a cursor.

        The request is checked first (400 with the first failing check's
        message), then whether this worker serves (404). The read runs on
        the main loop, where the cache's reader connection belongs, bounded
        like every other UI read of live state (503 when it does not
        answer).
        """
        # dict() of the query parameters keeps the last value of a repeated name.
        query = parse_changes_query(dict(request.query_params))
        if isinstance(query, str):
            return JSONResponse({'error': query}, status_code=400)
        engine = deps.drakkar_app.cache_engine
        if engine is None or not engine.serves_changes:
            return JSONResponse({'error': EXCHANGE_OFF_ERROR}, status_code=404)
        answer = await deps.dispatch_bounded(engine.changes(query, now_ms=_now_ms()), default=_NO_ANSWER)
        if answer is _NO_ANSWER:
            return JSONResponse({'error': NO_ANSWER_ERROR}, status_code=503)
        if answer is None:
            # The engine stopped between the check and the read.
            return JSONResponse({'error': EXCHANGE_OFF_ERROR}, status_code=404)
        return JSONResponse(answer)

    return router
