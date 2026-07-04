"""OpenAPI serving: the vendored contract spec and a self-hosted Swagger page.

The spec (``openapi.yaml`` next to this module) is the byte-identical
vendored copy of ``drakkar-ui/docs/openapi-v1.yaml`` — the canonical,
hand-maintained description of the ``/api/v1`` surface both backends must
serve. It is exposed two ways:

- ``GET /api/v1/openapi.json`` — the document converted to JSON once at
  router build time (same auth as every API route);
- ``GET /docs`` — a minimal Swagger UI shell over that document, using the
  vendored ``swagger-ui-dist`` assets under ``swagger/`` so the page works
  fully offline (no CDN), token-gated exactly like the other UI pages.

``tests/test_openapi_parity.py`` pins the served route table to the spec's
``paths`` — the mechanism that keeps this backend drop-in identical to the
Go one.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import quote

import yaml
from fastapi import APIRouter, Depends, Request
from fastapi.responses import FileResponse, HTMLResponse, Response

if TYPE_CHECKING:
    from drakkar.uiserver.server import UIDeps

_HERE = Path(__file__).parent
SPEC_PATH = _HERE / 'openapi.yaml'
SWAGGER_DIR = _HERE / 'swagger'

# The page shell is tiny and static except for the optional ?token=
# propagation: plain <script>/<link> tags cannot send an Authorization
# header, so when the page itself was opened with ?token=... the asset and
# spec URLs carry the same query parameter (mirroring how downloads and the
# WebSocket already pass the token).
_DOCS_HTML = """<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>Drakkar API</title>
<link rel="stylesheet" href="/docs/swagger-ui.css{token_qs}">
</head>
<body>
<div id="swagger-ui"></div>
<script src="/docs/swagger-ui-bundle.js{token_qs}"></script>
<script>
SwaggerUIBundle({{
  url: '/api/v1/openapi.json{token_qs}',
  dom_id: '#swagger-ui',
  deepLinking: true,
  tryItOutEnabled: true,
}});
</script>
</body>
</html>"""


def load_spec_json() -> bytes:
    """The vendored YAML spec rendered as compact JSON bytes."""
    doc = yaml.safe_load(SPEC_PATH.read_text())
    return json.dumps(doc, separators=(',', ':')).encode()


def create_openapi_router(deps: UIDeps) -> APIRouter:
    """Build the router serving the spec and the Swagger UI page."""
    router = APIRouter(dependencies=[Depends(deps.require_auth)])
    spec_json = load_spec_json()

    @router.get('/api/v1/openapi.json')
    async def api_openapi() -> Response:
        """The OpenAPI 3.1 document for this backend's surface."""
        return Response(content=spec_json, media_type='application/json')

    @router.get('/docs')
    async def docs_page(request: Request) -> HTMLResponse:
        """Self-hosted Swagger UI over the vendored spec."""
        token = request.query_params.get('token', '')
        # Strict percent-encoding (safe='') neutralizes every character
        # that could break out of the URL attribute or the inline JS
        # string — without it the reflected token would be an XSS vector
        # on deployments that run with auth disabled.
        token_qs = f'?token={quote(token, safe="")}' if token else ''
        return HTMLResponse(_DOCS_HTML.format(token_qs=token_qs))

    @router.get('/docs/swagger-ui-bundle.js')
    async def docs_js() -> FileResponse:
        """Vendored swagger-ui-dist bundle (no CDN — offline-safe)."""
        return FileResponse(SWAGGER_DIR / 'swagger-ui-bundle.js', media_type='text/javascript')

    @router.get('/docs/swagger-ui.css')
    async def docs_css() -> FileResponse:
        """Vendored swagger-ui-dist stylesheet."""
        return FileResponse(SWAGGER_DIR / 'swagger-ui.css', media_type='text/css')

    return router
