"""Static mount for the operator-supplied docs site (contract v1.22).

``ui.docs.site_dir`` names a directory of prebuilt static HTML — an mkdocs
``site/``, a Sphinx ``_build/html/``, anything — and this router serves it
verbatim under ``GET /docs/``. It is a static mount, not part of the JSON
API: the path is absent from ``openapi.yaml`` and from the route-parity pin.

Serving rules, all normative:

- requests resolve inside the configured directory; an escape is a 404.
  Resolution follows symlinks, so a link pointing outside ``site_dir`` is an
  escape too: a site whose ``assets/`` is a symlink to a shared directory
  elsewhere on the host answers 404 rather than serving through it;
- a directory request serves its ``index.html``;
- a missing file is a 404 — deliberately NOT the SPA shell, unlike
  :mod:`drakkar.uiserver.routes_spa`, because a wrong docs URL must read as
  wrong rather than silently render the operator UI;
- unconfigured, or a directory that is not there, answers a 404 carrying a
  JSON hint naming ``ui.docs.site_dir``;
- the mount is registered unconditionally, so ``/docs/`` never falls through
  to the SPA catch-all whatever the configuration says;
- auth matches the SPA pages: open with no ``ui.auth_token``, token-gated
  otherwise.

``site_dir`` is resolved once here, at router-build time (the same read-once
precedent as :mod:`drakkar.uiserver.routes_renderers`). A directory created
after the worker started is picked up on the next restart, not before.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, Response

if TYPE_CHECKING:
    from drakkar.uiserver.server import UIDeps

DOCS_HINT_DETAIL = 'Set ui.docs.site_dir to a prebuilt static site directory to serve docs at /docs/.'


def create_docs_router(deps: UIDeps) -> APIRouter:
    """Build the router serving the operator docs site under ``/docs/``."""
    router = APIRouter(dependencies=[Depends(deps.require_auth)])
    site_dir = deps.config.docs.site_dir
    root = Path(site_dir).resolve() if site_dir else None

    def _unavailable() -> JSONResponse:
        """The 404 an operator sees when the feature is off or the site is gone."""
        return JSONResponse(
            {'error': 'Operator docs site not available', 'detail': DOCS_HINT_DETAIL},
            status_code=404,
        )

    @router.get('/docs')
    async def docs_root_redirect() -> Response:
        """Send the bare path to the mount root so relative links resolve."""
        return RedirectResponse(url='/docs/', status_code=307)

    @router.get('/docs/{full_path:path}')
    async def docs_site(full_path: str) -> Response:
        """Serve one file from the docs site, or 404 with a reason."""
        # Re-checked per request, not captured: an operator can delete or
        # rebuild the site under a running worker, and the hint is a better
        # answer than a stack trace from FileResponse.
        if root is None or not root.is_dir():
            return _unavailable()
        try:
            candidate = (root / full_path).resolve()
            if candidate.is_dir():
                # Directory request (including '/docs/' itself, where full_path
                # is empty). Resolved again so a symlinked index cannot escape.
                candidate = (candidate / 'index.html').resolve()
        except (ValueError, OSError) as exc:
            # A path the OS refuses to even look at — an embedded NUL byte,
            # an over-long segment. It names nothing inside the site, so it
            # is a 404 like any other miss; unguarded it escaped as a 500.
            raise HTTPException(status_code=404, detail='Not found in the docs site') from exc
        # Containment: the resolved path must stay under the site root. The
        # is_file() check also rules out the root directory itself.
        if candidate.is_relative_to(root) and candidate.is_file():
            return FileResponse(candidate)
        raise HTTPException(status_code=404, detail='Not found in the docs site')

    return router
