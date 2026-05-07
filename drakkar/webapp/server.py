"""Webapp HTTP server — FastAPI app factory + uvicorn-on-thread bootstrap.

The webapp runs on a dedicated daemon thread with its own asyncio event
loop. This mirrors the ``DebugServer`` pattern (see
:mod:`drakkar.debug.server`): keeping the FastAPI request handlers off
the main pipeline loop means a slow client or contended HTTP socket
never stalls Kafka polling, the executor pool, or sink flushes.

Lifecycle
---------

* ``WebApp.__init__`` builds the FastAPI app and validates the resolved
  handler exposes concrete ``HttpRequestT`` / ``HttpResponseT`` models
  (raising :class:`ConfigurationError` on a misconfigured deployment).
* ``WebApp.start_in_thread()`` spawns the daemon thread that runs
  ``uvicorn.Server.run()``. The thread is daemon so an unrelated
  startup exception in the worker doesn't strand the process holding a
  bound port.
* ``WebApp.wait_until_ready(timeout)`` blocks the caller until the
  FastAPI startup hook captures a reference to the inner loop AND
  uvicorn flips ``server.started=True``. Used by ``AppLifecycle`` to
  fail fast if the bind fails.
* ``WebApp.stop(drain_timeout)`` signals uvicorn to exit and joins the
  thread within ``drain_timeout`` — beyond that we log and return
  rather than block worker shutdown indefinitely.

Per-request 503 gates
---------------------

Two readiness checks are evaluated at the top of the request handler,
ahead of the runner stub introduced in Task 6:

* ``not self._app.is_ready`` → 503 with ``status='not_ready'`` and the
  Kafka-routing hint. Covers the window between the webapp thread
  starting and the main loop's first poll completing.
* ``self.shutdown_event.is_set()`` → 503 with ``status='shutdown'``.
  ``AppLifecycle._shutdown`` sets this BEFORE the drain phase begins so
  new requests are rejected immediately while in-flight ones complete.

Both responses include a ``hint`` field telling the client to route the
workload through the Kafka source topic for higher throughput and
worker-restart resilience (the documented Kafka-fallback pattern).
"""

from __future__ import annotations

import asyncio
import threading
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

import structlog
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from drakkar.config import WebAppConfig
from drakkar.utils import make_request_id

if TYPE_CHECKING:
    from drakkar.app import DrakkarApp

logger = structlog.get_logger()


# Hint included in every 429 / 503 response body. Documented in the plan
# under "HTTP status codes" as the Kafka-fallback pattern: clients
# rate-limited or hitting an unavailable webapp should publish to the
# source topic for higher throughput and restart-resilient delivery.
KAFKA_FALLBACK_HINT = (
    'route this workload through the Kafka source topic for higher throughput and worker-restart resilience'
)


class ConfigurationError(RuntimeError):
    """Raised at webapp startup when required handler types are missing.

    Webapp users opt in by declaring concrete Pydantic models in the
    ``HttpRequestT`` / ``HttpResponseT`` slots of
    :class:`drakkar.handler.BaseDrakkarHandler`. When ``webapp.enabled=True``
    but a slot is left at the PEP 696 ``None`` default, the framework
    fails fast at startup rather than producing a confusing per-request
    error later.
    """


class WebApp:
    """Owns the webapp FastAPI server and its uvicorn-on-thread lifecycle.

    Internal-use class — not part of the public API. Reachable as
    ``from drakkar.webapp import WebApp`` for advanced integration but
    intentionally absent from ``drakkar.__all__``.
    """

    def __init__(self, drakkar_app: DrakkarApp, config: WebAppConfig) -> None:
        # Back-references — kept as private fields so the internal
        # nature of this object is obvious at every read site.
        self._app = drakkar_app
        self._config = config

        # Thread + uvicorn state. Populated in ``start_in_thread``; held
        # here so ``stop`` can signal the running server and join the
        # thread without re-discovering them.
        self._thread: threading.Thread | None = None
        self._uvicorn_server: uvicorn.Server | None = None
        # The webapp's inner asyncio loop, captured by the FastAPI lifespan
        # hook running on T2. ``None`` until the thread enters the lifespan
        # startup phase. Future tasks (Task 6) dispatch back to T1 from this
        # loop using ``dispatch_to_loop(coro, target_loop=self._app.main_loop)``.
        self._loop: asyncio.AbstractEventLoop | None = None
        # Signalled by the FastAPI lifespan startup hook — ``wait_until_ready``
        # blocks on this until the inner loop is captured.
        self._loop_ready: threading.Event = threading.Event()
        # Drain coordinator — ``AppLifecycle._shutdown`` flips this to make
        # new requests get an immediate 503 while the worker drains
        # in-flight work. The route handler checks ``is_set()`` ahead of
        # any dispatch.
        self.shutdown_event: threading.Event = threading.Event()

        # Auth + rate-limit dependencies are wired in Task 5. Kept as
        # placeholders here so the route handler can reference them
        # without a fragile attribute-presence check.
        self._authenticate: Any = None
        self._rate_limit: Any = None

        # Validate at construction time that the handler exposes concrete
        # HTTP request/response models. We do this BEFORE building the
        # FastAPI app so a misconfigured deployment fails at startup with
        # a clear message, not at first-request time with a confusing
        # 500 about a missing override.
        self._validate_handler_types()

        # Build the FastAPI app eagerly — uvicorn binds it inside the
        # daemon thread but the route registration must already be in
        # place when the thread starts (otherwise ``wait_until_ready``
        # could observe a server with no routes).
        self._fastapi_app: FastAPI = self._build_app()

    # ------------------------------------------------------------------
    # Public lifecycle API. Called from ``AppLifecycle._async_run``.
    # ------------------------------------------------------------------

    def start_in_thread(self) -> None:
        """Spawn the daemon thread that runs ``uvicorn.Server.run()``.

        Mirrors :class:`drakkar.debug.server.DebugServer.start` — uvicorn
        constructs its own event loop inside the thread. We capture that
        loop via the FastAPI lifespan startup hook (see ``_build_app``),
        which fires inside the same loop and stores a reference on
        ``self._loop`` before signalling ``self._loop_ready``.
        """
        # ``log_config=None`` defers to structlog (the project-wide
        # logging stack) rather than uvicorn's default formatter; we set
        # ``log_level='warning'`` so uvicorn only surfaces real errors,
        # matching the DebugServer pattern.
        uvi_config = uvicorn.Config(
            app=self._fastapi_app,
            host=self._config.host,
            port=self._config.port,
            log_config=None,
            log_level='warning',
        )
        self._uvicorn_server = uvicorn.Server(uvi_config)
        self._thread = threading.Thread(
            target=self._uvicorn_server.run,
            name='drakkar-webapp',
            daemon=True,
        )
        self._thread.start()

        # Startup log — no ``await`` because ``start_in_thread`` is a
        # plain method (callable from sync context). The async lifecycle
        # uses the sync logger via ``logger.info`` here; it's a one-shot
        # bootstrap message.
        unauth = all(c.token == '' for c in self._config.clients)
        logger.info(
            'webapp_started',
            category='webapp',
            host=self._config.host,
            port=self._config.port,
            path=self._config.path,
            clients_count=len(self._config.clients),
            sinks_enabled=self._config.sinks_enabled,
        )
        if unauth:
            # Documented in the plan: emit a distinct warning when every
            # configured client has an empty token (anonymous-only). Lets
            # operators alert on a private-network deployment that should
            # have had a token configured.
            logger.warning(
                'webapp_unauthenticated_warning',
                category='webapp',
                reason=(
                    'all webapp clients have empty tokens — the endpoint '
                    'is unauthenticated; configure non-empty tokens for '
                    'production deployments'
                ),
            )

    def wait_until_ready(self, timeout: float = 5.0) -> None:
        """Block until the inner loop is captured AND uvicorn is serving.

        Two-stage wait:

        1. ``_loop_ready`` is set inside the FastAPI lifespan startup
           hook — the inner loop is now captured on ``self._loop`` and
           the request handlers can safely dispatch to T1.
        2. ``uvicorn_server.started`` flips True once the socket is
           bound and accept() is running. We poll this short list of
           booleans rather than spin up another Event because uvicorn
           does not surface a "started" signal we can hook.
        """
        if not self._loop_ready.wait(timeout=timeout):
            raise TimeoutError(
                f'webapp inner loop was not captured within {timeout}s — '
                f'check the daemon thread for an early uvicorn failure'
            )
        # Best-effort poll for ``started`` — uvicorn sets the attribute
        # synchronously inside its run loop, so a tiny sleep gives it
        # time to flip after the lifespan hook returns. This loop has a
        # bounded budget (``timeout``) so a wedged uvicorn surfaces as a
        # ``TimeoutError`` rather than hanging the test/worker.
        deadline = timeout
        step = 0.05
        elapsed = 0.0
        while elapsed < deadline:
            if self._uvicorn_server is not None and getattr(self._uvicorn_server, 'started', False):
                return
            threading.Event().wait(step)  # sleep without an extra import
            elapsed += step
        # If we reach here uvicorn never flipped ``started`` — surface
        # the same TimeoutError shape so callers can handle both stages
        # uniformly.
        raise TimeoutError(f'webapp uvicorn server did not enter the started state within {timeout}s')

    def stop(self, drain_timeout: float) -> None:
        """Signal uvicorn to exit and join the worker thread.

        ``should_exit = True`` makes uvicorn's run loop break out of
        ``serve()`` after the next tick. We then ``thread.join`` with
        ``drain_timeout`` so a stuck request handler can't block worker
        shutdown forever — past the timeout we log a warning and
        return, letting the rest of the teardown proceed.
        """
        if self._uvicorn_server is not None:
            self._uvicorn_server.should_exit = True
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=drain_timeout)
            if self._thread.is_alive():
                # Daemon thread will be killed when the process exits; we
                # log so operators can correlate the warning with whatever
                # in-flight handler refused to release.
                logger.warning(
                    'webapp_stop_thread_join_timeout',
                    category='webapp',
                    drain_timeout=drain_timeout,
                )
        logger.info('webapp_stopped', category='webapp')

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _validate_handler_types(self) -> None:
        """Fail fast if the handler is missing webapp HTTP types.

        Reads ``http_request_model`` / ``http_response_model`` off the
        handler subclass — populated by ``BaseDrakkarHandler.__init_subclass__``
        when the user declares concrete Pydantic models in the 3rd / 4th
        Generic slots. ``None`` means "this slot was left at the PEP 696
        default", which is a configuration error when ``webapp.enabled=True``.
        """
        handler = self._app._handler
        request_model = getattr(handler, 'http_request_model', None)
        response_model = getattr(handler, 'http_response_model', None)
        if request_model is None or response_model is None:
            cls_name = type(handler).__name__
            raise ConfigurationError(
                f'webapp.enabled=true but {cls_name} did not declare '
                f'HttpRequestT/HttpResponseT — extend '
                f'BaseDrakkarHandler[InputT, OutputT, HttpRequestT, '
                f'HttpResponseT] with concrete Pydantic models in slots '
                f'3 and 4. See docs/webapp.md for an example.'
            )

    def _build_app(self) -> FastAPI:
        """Construct the FastAPI app and register the single POST route.

        Uses the ``lifespan`` async context manager (FastAPI's modern
        startup/shutdown API) to capture the inner asyncio loop inside
        the running webapp thread. Same pattern the project uses
        elsewhere for thread-bound loop discovery.
        """

        @asynccontextmanager
        async def lifespan(_app: FastAPI):
            # Capture the loop running in the webapp thread (T2). The
            # main pipeline loop (T1) is on a different thread; cross-
            # thread dispatch is handled by ``dispatch_to_loop``.
            self._loop = asyncio.get_running_loop()
            self._loop_ready.set()
            try:
                yield
            finally:
                # Nothing to release here — uvicorn owns the loop and
                # tears it down after this generator exits. Kept as a
                # try/finally so future per-request cleanup hooks (Task
                # 6) drop in without restructuring this block.
                pass

        app = FastAPI(
            title='Drakkar Webapp',
            docs_url=None,
            redoc_url=None,
            lifespan=lifespan,
        )

        # Single POST route. The handler is a method on ``self`` so it
        # can reach ``self._app.is_ready`` and ``self.shutdown_event``
        # without closing over the whole ``WebApp`` from a free function.
        # Task 6 replaces the 501 stub with a real runner dispatch.
        path = self._config.path

        @app.post(path)
        async def process(request: Request) -> Any:
            # The ``Request`` type-hint tells FastAPI "give me the raw
            # ASGI request, do not parse the body". Body parsing into
            # ``HttpRequestT`` happens inside the runner introduced in
            # Task 6 — we deliberately skip it here so the readiness
            # gates fire on a body-shape-agnostic path. The parameter
            # is unused in this stub but kept so the route signature
            # already matches the Task 5 / Task 6 wiring.
            #
            # Ahead of any dispatch we evaluate the readiness gates.
            # Both branches return the same shape (status / error /
            # request_id / hint) so client code can switch on the
            # status field without per-branch parsing.
            request_id = make_request_id('req')
            if self.shutdown_event.is_set():
                return JSONResponse(
                    status_code=503,
                    content={
                        'status': 'shutdown',
                        'error': 'webapp is shutting down; request rejected',
                        'request_id': request_id,
                        'hint': KAFKA_FALLBACK_HINT,
                    },
                )
            if not self._app.is_ready:
                return JSONResponse(
                    status_code=503,
                    content={
                        'status': 'not_ready',
                        'error': 'webapp is starting; main pipeline is not yet ready',
                        'request_id': request_id,
                        'hint': KAFKA_FALLBACK_HINT,
                    },
                )
            # Stub — Task 6 wires the real runner here. We return 501
            # (Not Implemented) so a misrouted production request fails
            # loudly rather than silently 200-ing with no result.
            return JSONResponse(
                status_code=501,
                content={
                    'status': 'not_implemented',
                    'error': 'webapp runner not yet wired',
                    'request_id': request_id,
                },
            )

        return app
