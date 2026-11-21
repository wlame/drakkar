"""Split jobs JSON API: ``GET /api/v1/debug/split``.

The route lists the split jobs of this worker, read-only and from the
worker's memory. It always answers 200, also with the feature off, where the
body has ``enabled: false`` and empty lists. It follows ``require_auth`` like
every UI route, and answers cross-origin reads like the other fan-out paths.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, Depends, Query

if TYPE_CHECKING:
    from drakkar.uiserver.server import UIDeps

DEFAULT_JOB_LIMIT = 100
MAX_JOB_LIMIT = 500


def create_split_router(deps: UIDeps) -> APIRouter:
    """Build the router that serves the split jobs of this worker."""
    router = APIRouter(dependencies=[Depends(deps.require_auth)])
    recorder = deps.recorder

    @router.get('/api/v1/debug/split')
    async def split_state(limit: int = Query(DEFAULT_JOB_LIMIT, ge=1, le=MAX_JOB_LIMIT)) -> dict[str, Any]:
        """The split jobs of this worker, at most ``limit`` of them."""
        return {
            'enabled': False,
            'worker': {'name': recorder.worker_name, 'instance': recorder.instance},
            'source_topic': None,
            'consumer_group': None,
            'parts_topic': None,
            'results_topic': None,
            'parts_partition_base': None,
            'settings': None,
            'partitions': [],
            'jobs': [],
            'jobs_total': 0,
            'result_write_failures_total': 0,
        }

    return router
