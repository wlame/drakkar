"""The HTTP ingress (webapp server) as an input source."""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Any

import structlog

from drakkar.metrics import drain_timeout_hit
from drakkar.webapp import WebApp

if TYPE_CHECKING:
    from drakkar.app import DrakkarApp
    from drakkar.sources.base import SourceContext

logger = structlog.get_logger()

# Generous for a clean uvicorn bind on a free port; tests use the same value.
BIND_TIMEOUT_SECONDS = 5.0

# How often ``drain`` re-reads the webapp's in-flight request counter.
DRAIN_POLL_SECONDS = 0.05


class HttpSource:
    """Owns the webapp server thread. Ready as soon as the socket is bound."""

    name = 'http'

    def __init__(self, app: DrakkarApp) -> None:
        # The app is held only to hand it to ``WebApp``, whose request path
        # reads live worker state through it. Everything this source reads
        # itself comes from the context, which is bound after ``on_startup``
        # so a config the hook replaced is the one that takes effect.
        self._app = app
        self.webapp: Any = None
        self._stopped = asyncio.Event()
        self._ready = False
        self._ctx: SourceContext | None = None

    def bind(self, ctx: SourceContext) -> None:
        """Receive the worker context. Called once, before ``start``."""
        self._ctx = ctx

    def _context(self) -> SourceContext:
        """The bound context. A method reaching here before ``bind`` is a wiring bug."""
        assert self._ctx is not None
        return self._ctx

    async def start(self) -> None:
        ctx = self._context()
        webapp = WebApp(self._app, ctx.config.sources.http)
        webapp.start_in_thread()
        try:
            # ``wait_until_ready`` blocks; keep the main loop responsive.
            await asyncio.to_thread(webapp.wait_until_ready, timeout=BIND_TIMEOUT_SECONDS)
        except Exception:
            # The bind error is what the operator needs; a cleanup that
            # raises on top of it must not replace it.
            try:
                await asyncio.to_thread(webapp.stop, drain_timeout=1.0)
            except Exception as exc:
                await logger.awarning(
                    'webapp_cleanup_failed',
                    category='webapp',
                    worker_id=ctx.worker_id,
                    error=str(exc),
                    exc_info=True,
                )
            raise
        self.webapp = webapp
        self._ready = True

    async def run(self) -> None:
        await self._stopped.wait()

    @property
    def is_ready(self) -> bool:
        return self._ready

    def signal_stop(self) -> None:
        self._ready = False
        if self.webapp is not None:
            self.webapp.shutdown_event.set()
        self._stopped.set()

    async def drain(self, deadline: float) -> bool:
        """Wait for in-flight requests to finish, bounded by ``deadline``.

        The gate set in ``signal_stop`` already refuses new requests, so the
        counter only falls. Returning before it reaches zero would let the
        shutdown close the sinks under a request still delivering to them.
        """
        if self.webapp is None:
            return True
        log = logger.bind(worker_id=self._context().worker_id)
        while self.webapp.inflight_count > 0:
            if time.monotonic() >= deadline:
                # Same counter the Kafka source ticks: one alert covers
                # every source that is killed mid-flight.
                drain_timeout_hit.inc()
                await log.awarning(
                    'webapp_drain_timeout',
                    category='webapp',
                    inflight_requests=self.webapp.inflight_count,
                )
                return False
            await asyncio.sleep(DRAIN_POLL_SECONDS)
        return True

    async def stop(self, deadline: float) -> None:
        if self.webapp is None:
            return
        budget = max(deadline - time.monotonic(), 0.5)
        log = logger.bind(worker_id=self._context().worker_id)
        try:
            await asyncio.to_thread(self.webapp.stop, drain_timeout=budget)
        except Exception as exc:
            await log.awarning('webapp_stop_failed', category='webapp', error=str(exc), exc_info=True)
        self.webapp = None

    def snapshot(self) -> dict[str, Any]:
        inflight = self.webapp.inflight_count if self.webapp is not None else 0
        return {'inflight_requests': inflight}
