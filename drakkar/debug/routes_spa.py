"""SPA catch-all router serving a resolved drakkar-ui bundle.

When ``ui.enabled`` and a bundle resolved, the SPA owns every non-API path:
bundle files are served as-is, and any unknown path returns ``index.html``
with a 200 so the UI's client-side router (not the server) owns navigation
(History-API fallback). The router is registered LAST on the app, so the
probes, ``/ws``, every ``/api*`` JSON route, and ``/debug/download`` keep
precedence (Starlette matches routes in registration order).

Auth-gating matches the HTML pages: every request goes through
``deps.require_auth`` (a no-op when ``auth_token`` is empty).
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse

if TYPE_CHECKING:
    from drakkar.debug.server import DebugDeps


def create_spa_router(deps: DebugDeps, ui_root: Path) -> APIRouter:
    """Build the catch-all router that serves the UI bundle at ``ui_root``."""
    router = APIRouter(dependencies=[Depends(deps.require_auth)])
    root = ui_root.resolve()

    @router.get('/{full_path:path}')
    async def spa(full_path: str) -> FileResponse:
        """Serve a bundle file, falling back to ``index.html`` (200) for unknown paths.

        The requested path is resolved against the bundle root and must stay
        inside it — traversal attempts and directory hits fall through to the
        SPA shell rather than exposing anything outside the bundle.
        """
        candidate = (root / full_path).resolve()
        if candidate != root and candidate.is_relative_to(root) and candidate.is_file():
            return FileResponse(candidate)
        index = root / 'index.html'
        if not index.is_file():
            # No bundle at all — resolution should have prevented this.
            raise HTTPException(status_code=404, detail='UI bundle has no index.html')
        return FileResponse(index, media_type='text/html')

    return router
