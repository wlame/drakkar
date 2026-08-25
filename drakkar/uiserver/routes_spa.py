"""SPA catch-all router serving a resolved drakkar-ui bundle.

The SPA owns every non-API path: bundle files are served as-is, and any
unknown path returns ``index.html`` with a 200 so the UI's client-side
router (not the server) owns navigation (History-API fallback). The router
is registered LAST on the app, so the probes, ``/ws`` and every
``/api/v1/*`` route keep precedence (Starlette matches routes in
registration order).

When no bundle resolved — offline first start, ``ui.release.enabled: false``,
or a repo that cannot be reached — the same catch-all answers **503** with
what to do about it. There is deliberately no built-in HTML fallback: the
UI is one versioned artifact shared by both backends, and a second
server-rendered copy meant every feature had to be built twice. Losing the
pages costs nothing operationally — the JSON API under ``/api/v1``, the
Kubernetes probes and the Prometheus exporter are all unaffected.

Auth-gating matches the API: every request goes through
``deps.require_auth`` (a no-op when ``auth_token`` is empty).
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse, JSONResponse

if TYPE_CHECKING:
    from drakkar.uiserver.server import UIDeps

# What an operator can do about a missing bundle, in the order they should
# try it. Sent verbatim in the 503 body so the fix does not require finding
# the docs first.
NO_BUNDLE_REMEDIES = (
    'check this worker can reach the release source (ui.release.repo, default wlame/drakkar-ui)',
    'or stage a bundle into the shared cache with: drakkar-ui fetch <version>',
    'or point ui.release.repo at an internal mirror that publishes the same release assets',
    'or set ui.release.enabled: false to run API-only on purpose',
)


def create_spa_router(deps: UIDeps, ui_root: Path | None) -> APIRouter:
    """Build the catch-all router that serves the UI bundle at ``ui_root``.

    ``ui_root`` of ``None`` means no bundle was resolved; the router then
    reports that on every page request instead of serving anything.
    """
    router = APIRouter(dependencies=[Depends(deps.require_auth)])

    if ui_root is None:

        @router.get('/{full_path:path}')
        async def no_bundle(full_path: str) -> JSONResponse:
            """Report the missing bundle, with the ways to supply one.

            503 rather than 404: the path is not wrong, the UI is not
            available *yet*. A load balancer reading this must not conclude
            the worker is broken — it is processing messages normally, and
            ``/readyz`` says so.
            """
            return JSONResponse(
                {
                    'error': 'UI bundle not available',
                    'detail': (
                        'This worker has no drakkar-ui bundle to serve. Its JSON API '
                        '(/api/v1/...), health probes and event WebSocket are unaffected.'
                    ),
                    'remedies': list(NO_BUNDLE_REMEDIES),
                },
                status_code=503,
            )

        return router

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
