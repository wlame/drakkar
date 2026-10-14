"""Cross-origin reads for the browser fan-out.

The page served by one worker reads the trace, the label trace and the
databases list from every worker of its cluster. Those reads are
cross-origin, so these three paths answer CORS: with the origin rule of the
WebSocket (``origin_allowed`` and ``ui.allowed_ws_origins``, one decision
table for both), and with a preflight that needs no token, because a
browser sends none with it. No other path sends CORS headers.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

from drakkar.uiserver.server_helpers import origin_allowed

if TYPE_CHECKING:
    from starlette.middleware.base import RequestResponseEndpoint

    from drakkar.config import UIConfig

FANOUT_PATHS = frozenset(
    {
        '/api/v1/debug/trace',
        '/api/v1/debug/trace-by-label',
        '/api/v1/debug/databases',
    }
)
PREFLIGHT_MAX_AGE_SECONDS = 600
ORIGIN_REFUSED_ERROR = 'Origin not allowed'


def _allow_headers(origin: str) -> dict[str, str]:
    return {'Access-Control-Allow-Origin': origin, 'Vary': 'Origin'}


def _preflight_headers(origin: str) -> dict[str, str]:
    return {
        **_allow_headers(origin),
        'Access-Control-Allow-Methods': 'GET',
        'Access-Control-Allow-Headers': 'Authorization',
        'Access-Control-Max-Age': str(PREFLIGHT_MAX_AGE_SECONDS),
    }


def install_fanout_cors(app: FastAPI, config: UIConfig) -> None:
    """Answer CORS on the fan-out paths: preflights before auth, headers on allowed GETs."""

    @app.middleware('http')
    async def _fanout_cors(request: Request, call_next: RequestResponseEndpoint) -> Response:
        origin = request.headers.get('origin')
        # No Origin: not a browser's cross-origin read; nothing to add.
        if request.url.path not in FANOUT_PATHS or origin is None:
            return await call_next(request)
        allowed = origin_allowed(origin, request.headers.get('host', ''), config)
        if request.method == 'OPTIONS':
            if not allowed:
                return JSONResponse({'error': ORIGIN_REFUSED_ERROR}, status_code=403)
            return Response(status_code=204, headers=_preflight_headers(origin))
        response = await call_next(request)
        # A refused origin gets no CORS header: the browser then hides the
        # answer from the page. Auth still ran as usual inside call_next.
        if allowed:
            response.headers.update(_allow_headers(origin))
        return response
