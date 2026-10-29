"""The Kubernetes probes: ``/healthz`` and ``/readyz``.

Routes:
  * ``/healthz`` — liveness probe (public).
  * ``/readyz``  — readiness probe (public).

Every worker with an API serves them, UI on or off. They read only the worker
itself, so they need neither the recorder nor the ``ui`` section.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import APIRouter
from fastapi.responses import JSONResponse

if TYPE_CHECKING:
    from drakkar.app import DrakkarApp


def create_health_router(drakkar_app: DrakkarApp) -> APIRouter:
    """Build the public router of the two probes.

    The probes skip the auth dependency: Kubernetes probes have no facility
    for bearer tokens, and the routes expose only liveness and readiness
    signals, nothing that leaks message content, partition state or operator
    credentials. They do no I/O and read no recorder, so the kubelet's
    sub-second budget holds under load.

    ``/healthz`` is 200 while the process runs and the server loop responds;
    Kubernetes restarts the pod on failure. ``/readyz`` is 200 only when
    ``DrakkarApp.is_ready`` is true and every registered sink reports
    ``is_connected``, otherwise 503 with a ``reasons`` list; Kubernetes takes
    the pod out of the service endpoints on failure, without a restart.
    """
    router = APIRouter()

    @router.get('/healthz')
    async def healthz() -> JSONResponse:
        """Liveness probe — returns 200 as long as the event loop is alive."""
        return JSONResponse({'status': 'ok'})

    @router.get('/readyz')
    async def readyz() -> JSONResponse:
        """Readiness probe — 200 iff the worker is ready AND all sinks are connected."""
        reasons: list[str] = []
        if not drakkar_app.is_ready:
            reasons.append('not_started')
        # ``sink_manager`` is always constructed in ``DrakkarApp.__init__``
        # and ``all_connected`` returns True for an empty manager, so we
        # can consult it directly. Startup validation rejects a zero-sink
        # config before the server is mounted.
        mgr = drakkar_app.sink_manager
        if not mgr.all_connected():
            for sink_id in mgr.disconnected_sink_names():
                reasons.append(f'sink_{sink_id}_not_connected')
        # A partition whose loop gave up is stalled for the life of this
        # process: its queue is never drained and its offsets are never
        # committed. Failing readiness is what gets the pod replaced and
        # the partition reassigned to a worker that can serve it — without
        # this the worker looks healthy while silently processing nothing.
        for pid, processor in sorted(drakkar_app.processors.items()):
            if processor.is_dead:
                reasons.append(f'partition_{pid}_processor_died')
        if reasons:
            return JSONResponse({'status': 'not_ready', 'reasons': reasons}, status_code=503)
        return JSONResponse({'status': 'ready'})

    return router
