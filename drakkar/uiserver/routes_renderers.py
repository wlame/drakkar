"""Serves the deployment-provided custom cell renderers module.

The module at ``ui.custom_renderers_path`` is deployment-owned JavaScript,
trusted at the same level as the rest of the backend config — it runs
same-origin in the operator UI, unsandboxed. This router only serves its
bytes; it never inspects or executes them.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import TYPE_CHECKING

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse, Response

if TYPE_CHECKING:
    from drakkar.uiserver.server import UIDeps


def create_renderers_router(deps: UIDeps) -> APIRouter:
    """Build the router serving ``GET /api/v1/ui/renderers.js``.

    Always registered, even when the feature is off — the route-parity
    test requires the served route table to match the spec's declared
    paths regardless of configuration. When ``ui.custom_renderers_path``
    is unset, the handler 404s with a reason. When set, the file is read
    ONCE here (mirroring the spec-json caching in ``routes_openapi.py``)
    and its content-hash ETag is computed up front, so every request after
    the first is served from memory with no filesystem access.
    """
    router = APIRouter(dependencies=[Depends(deps.require_auth)])
    configured_path = deps.config.custom_renderers_path
    content: bytes | None = None
    etag: str | None = None
    if configured_path:
        content = Path(configured_path).read_bytes()
        etag = f'"{hashlib.sha256(content).hexdigest()}"'

    @router.get('/api/v1/ui/renderers.js')
    async def api_renderers_js(request: Request) -> Response:
        """The configured custom-renderers module, or 404 when unset."""
        if content is None or etag is None:
            return JSONResponse(
                {'enabled': False, 'reason': 'no custom renderers module is configured'},
                status_code=404,
            )
        if request.headers.get('if-none-match') == etag:
            return Response(status_code=304)
        return Response(content=content, media_type='text/javascript', headers={'ETag': etag})

    return router
