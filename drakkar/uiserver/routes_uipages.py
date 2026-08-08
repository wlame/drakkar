"""Declared UI pages: ``GET /api/v1/pages``.

Serves the handler's validated page declarations (see
:mod:`drakkar.uipages`) verbatim — an empty list when the handler declares
none. v1-only, no legacy unprefixed alias, same as ``routes_config_reference``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import APIRouter, Depends

if TYPE_CHECKING:
    from drakkar.uiserver.server import UIDeps


def create_uipages_router(deps: UIDeps) -> APIRouter:
    """Build the router owning ``GET /api/v1/pages`` (v1-only, no legacy alias)."""
    router = APIRouter(dependencies=[Depends(deps.require_auth)])

    @router.get('/api/v1/pages')
    async def api_pages() -> list[dict]:
        """Deployment-declared dashboard pages, in declaration order."""
        return [p.model_dump() for p in deps.drakkar_app.ui_pages]

    return router
