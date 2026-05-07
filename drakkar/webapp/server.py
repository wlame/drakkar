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

Three readiness checks are evaluated at the top of the request handler,
ahead of the runner dispatch:

* ``not self._app.is_ready`` → 503 with ``status='not_ready'`` and the
  Kafka-routing hint. Covers the window between the webapp thread
  starting and the main loop's first poll completing.
* ``self.shutdown_event.is_set()`` → 503 with ``status='shutdown'``.
  ``AppLifecycle._shutdown`` sets this BEFORE the drain phase begins so
  new requests are rejected immediately while in-flight ones complete.
* T2-side concurrency cap (``WebAppConfig.max_concurrent``): if the
  semaphore cannot be acquired immediately, return 503 ``status='capacity'``
  with the Kafka-routing hint. The acquire uses a tiny non-blocking
  poll (``wait_for(..., timeout)``) so a fully-loaded webapp sheds load
  rather than queuing, and clients learn quickly to switch over to the
  Kafka source path.

All 503/504 responses include a ``hint`` field telling the client to
route the workload through the Kafka source topic for higher throughput
and worker-restart resilience (the documented Kafka-fallback pattern).

Timeout + cancellation
----------------------

Each dispatch is wrapped in ``asyncio.wait_for(...,
timeout=request_timeout_seconds)``. On ``TimeoutError``:

1. signal cancellation to T1 via
   ``main_loop.call_soon_threadsafe(ctx.cancelled.set)`` so the runner
   short-circuits its post-execute side effects (sinks +
   ``on_http_request_complete``);
2. ``asyncio.wait_for`` already cancels the wrapped task when its
   budget expires, which propagates through ``asyncio.wrap_future`` to
   cancel the underlying ``concurrent.futures.Future`` (so the runner's
   currently-awaiting line raises ``CancelledError``);
3. return a flat 504 body with ``status='timeout'``, ``error``,
   ``request_id`` and ``duration_ms``.

The runner creates ``ctx.cancelled`` on its first line (T1's loop) so
the Event binds to the right loop. If T2 hits the ``wait_for`` budget
before the runner has a chance to allocate the Event, the
``call_soon_threadsafe`` callback handles the ``None`` case
gracefully.
"""

from __future__ import annotations

import asyncio
import threading
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import structlog
import uvicorn
from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from drakkar.concurrency import dispatch_to_loop
from drakkar.config import WebAppConfig
from drakkar.utils import make_request_id, validate_request_id
from drakkar.webapp.dependencies import (
    WebappError,
    make_authenticate,
    make_rate_limit,
)
from drakkar.webapp.models import WebRequestContext
from drakkar.webapp.runner import WebappHandlerError, WebappRunner

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

# Tiny acquire-probe timeout for the T2-side concurrency semaphore. The
# value is small enough that a fully-loaded webapp sheds load almost
# immediately (returning 503 ``capacity``) but large enough to absorb a
# scheduling jitter on a barely-loaded pool. It is deliberately NOT a
# user-tunable knob — operators tune ``max_concurrent`` instead, and
# horizontal-scale-out / Kafka-fallback covers everything else.
_SEMAPHORE_ACQUIRE_PROBE_SECONDS = 0.001


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

        # Auth + rate-limit dependencies (Task 5). Built once at
        # construction time so the route handler can refer to them via
        # ``Depends(self._authenticate)`` / ``Depends(self._rate_limit)``.
        # The factories close over ``config`` and over per-client deque
        # state respectively — a fresh ``WebApp`` instance gets its own
        # rate-limit counters, which keeps tests independent.
        self._authenticate = make_authenticate(config)
        self._rate_limit = make_rate_limit(config)

        # Validate at construction time that the handler exposes concrete
        # HTTP request/response models. We do this BEFORE building the
        # FastAPI app so a misconfigured deployment fails at startup with
        # a clear message, not at first-request time with a confusing
        # 500 about a missing override.
        self._validate_handler_types()

        # Webapp runner — Task 6a. One instance per ``WebApp`` so the
        # synthetic ``SourceMessage.offset`` counter is monotone for the
        # process lifetime. The runner runs on the main loop (T1); the
        # FastAPI route handler dispatches to it via ``dispatch_to_loop``.
        self._runner = WebappRunner(drakkar_app, config)
        # Cache the resolved HttpRequestT model on ``self`` so the route
        # handler can validate the POST body without re-walking the
        # handler's class attributes per request. ``_validate_handler_types``
        # already proved this is non-None before the runner was built.
        self._http_request_type: Any = drakkar_app._handler.http_request_model

        # T2-side concurrency cap. The semaphore must be allocated on
        # the loop that awaits it (the webapp loop, T2). Constructing
        # the semaphore here works because Python 3.10+ removed the
        # implicit-loop binding from ``asyncio.Semaphore``: a Semaphore
        # constructed without a running loop binds lazily to whichever
        # loop first awaits ``acquire``. The bound is set once at
        # construction; FastAPI's webapp loop is the only loop that
        # ever touches it (the route handler is the sole consumer).
        self._semaphore: asyncio.Semaphore = asyncio.Semaphore(config.max_concurrent)

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

    def _signal_cancel(self, ctx: WebRequestContext) -> None:
        """Schedule ``ctx.cancelled.set()`` on the main pipeline loop (T1).

        Called from T2 (the webapp loop) when ``asyncio.wait_for``
        trips its budget. ``ctx.cancelled`` is an :class:`asyncio.Event`
        bound to T1's loop (the runner allocates it on its first line);
        touching it from T2 directly would risk inconsistent behaviour
        on contended event-loop policies. ``call_soon_threadsafe`` is
        the documented cross-thread way to interact with loop-bound
        objects.

        The race we tolerate gracefully: if T2's ``wait_for`` budget
        fires before the runner has a chance to allocate the Event,
        ``ctx.cancelled`` is still ``None``. The scheduled callback
        re-checks the field and returns silently in that case — the
        ``concurrent.futures.Future`` cancellation propagated by
        ``asyncio.wrap_future`` is sufficient to short-circuit the
        runner before it gets to its post-execute gate.
        """

        def _set_cancelled() -> None:
            if ctx.cancelled is not None:
                ctx.cancelled.set()

        # ``main_loop`` is the loop captured by ``DrakkarApp`` at
        # startup. In production this is always a real event loop;
        # tests that exercise the route handler in-process may pass a
        # ``MagicMock`` (no ``call_soon_threadsafe``), in which case we
        # fall back to a direct set — same observable behaviour for the
        # runner's ``is_set()`` check, and the test stays loop-agnostic.
        main_loop = getattr(self._app, 'main_loop', None)
        if main_loop is not None and hasattr(main_loop, 'call_soon_threadsafe'):
            try:
                main_loop.call_soon_threadsafe(_set_cancelled)
                return
            except RuntimeError:
                # Loop already closed (worker is in late-shutdown). The
                # runner is no longer running; nothing to cancel. Fall
                # through to the direct-set path so tests still observe
                # the flag.
                pass
        if ctx.cancelled is not None:
            ctx.cancelled.set()

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

        # Single exception handler for the webapp's typed errors. By
        # registering against the ``WebappError`` base class, FastAPI's
        # exception-dispatch machinery routes both subclasses
        # (``WebappAuthError``, ``WebappRateLimitError``) through this
        # handler. The handler emits ``JSONResponse`` directly with the
        # carried ``body_dict`` — bypassing the default ``HTTPException``
        # envelope (``{'detail': ...}``) so the response shape stays flat
        # as documented in the plan.
        @app.exception_handler(WebappError)
        async def _webapp_error_handler(_request: Request, exc: WebappError) -> JSONResponse:
            # ``content=exc.body_dict`` writes the dict verbatim; pydantic
            # is not involved because the body has already been built by
            # the dependency raising the exception.
            return JSONResponse(
                status_code=exc.status_code,
                content=exc.body_dict,
                headers=exc.headers or {},
            )

        # Single POST route. The handler is a method on ``self`` so it
        # can reach ``self._app.is_ready`` and ``self.shutdown_event``
        # without closing over the whole ``WebApp`` from a free function.
        # Task 6 replaces the 501 stub with a real runner dispatch.
        path = self._config.path

        # Capture the dependency callables in local names so the
        # ``Depends(...)`` defaults below close over plain functions
        # instead of bound-method-on-self (which FastAPI cannot
        # introspect cleanly when the surrounding ``self`` isn't a
        # frozen dataclass). Same intent as Pydantic's
        # ``model_construct`` callsites elsewhere in the codebase.
        authenticate_dep = self._authenticate
        rate_limit_dep = self._rate_limit

        # The rate-limit dep receives the matched client from the auth
        # dep. Because ``_rate_limit`` is itself a closure with
        # ``async def _rate_limit(client: WebClientConfig)`` in a
        # ``from __future__ import annotations`` module, FastAPI cannot
        # auto-resolve the ``client`` parameter as a sub-dependency on
        # ``authenticate_dep`` from the function signature alone. We
        # bridge them with a tiny outer dep that pulls the matched
        # client from ``request.state`` (where the auth dep stashes it)
        # and passes it explicitly into the rate-limit closure. Same
        # observable behaviour, simpler dependency graph for FastAPI.
        async def _enforce_rate_limit(request: Request) -> None:
            client = getattr(request.state, 'client_name', None)
            # ``client_name`` is the matched client's name; we look it
            # up in the config to get the rpm-bearing object. The
            # config's ``clients`` list is short (operator-configured),
            # so a linear scan is fine.
            matched = next(
                (c for c in self._config.clients if c.name == client),
                None,
            )
            if matched is None:
                # Defensive — auth must have run first; if request.state
                # has no client_name something is wired wrong upstream.
                # Treat as auth failure rather than silently admitting.
                raise WebappError(status_code=401, body_dict={'error': 'unauthorized'})
            await rate_limit_dep(matched)

        # The route signature uses the legacy ``Depends`` default-value
        # form because the closure-bound dep callables are local
        # variables at this scope — ``from __future__ import annotations``
        # defers annotation evaluation, and FastAPI's ``Annotated[...]``
        # resolver cannot find local names when it later evaluates the
        # strings. The default-value form sidesteps that.
        #
        # We type ``client`` as ``Any`` (not ``WebClientConfig``) for
        # the same reason — newer FastAPI versions classify a Pydantic-
        # typed parameter with a default value as a body field, which
        # auto-generates a 422 if the body is missing. The runner can
        # narrow back to ``WebClientConfig`` from the matched config.
        @app.post(path)
        async def process(
            request: Request,
            # idiom is ``Depends(...)`` in argument defaults; B008 only
            # applies to plain function calls in defaults, not to FastAPI
            # marker objects.
            client: Any = Depends(authenticate_dep),  # noqa: B008
            _: Any = Depends(_enforce_rate_limit),  # noqa: B008
        ) -> Any:
            # The ``Request`` type-hint tells FastAPI "give me the raw
            # ASGI request, do not parse the body". Body parsing into
            # ``HttpRequestT`` happens inside the runner introduced in
            # Task 6 — we deliberately skip it here so the readiness
            # gates fire on a body-shape-agnostic path.
            #
            # The auth and rate-limit dependencies have already run by
            # the time we get here: a 401 or 429 short-circuits via
            # ``WebappError`` and the registered exception handler.
            # ``client`` is the matched ``WebClientConfig``; ``_`` is
            # the rate-limit dependency's None return (kept in the
            # signature so FastAPI knows to invoke it).
            #
            # Readiness gates fire AFTER auth+rate-limit so an
            # unauthenticated burst still returns 401 (not 503) — that
            # keeps audit trails accurate during deploys.
            # Pre-allocate a request_id used for the gate responses
            # below. If the request makes it past the gates the runner
            # asks the handler for the real request_id (which may be
            # derived from headers — typical for tracing scenarios).
            gate_request_id = make_request_id('req')
            if self.shutdown_event.is_set():
                return JSONResponse(
                    status_code=503,
                    content={
                        'status': 'shutdown',
                        'error': 'webapp is shutting down; request rejected',
                        'request_id': gate_request_id,
                        'hint': KAFKA_FALLBACK_HINT,
                    },
                )
            if not self._app.is_ready:
                return JSONResponse(
                    status_code=503,
                    content={
                        'status': 'not_ready',
                        'error': 'webapp is starting; main pipeline is not yet ready',
                        'request_id': gate_request_id,
                        'hint': KAFKA_FALLBACK_HINT,
                    },
                )

            # Parse the body manually so we control the 422 envelope
            # shape. Declaring the parameter as the typed model would
            # let FastAPI auto-422 with its own envelope; we want a flat
            # body matching the documented webapp shape.
            body_bytes = await request.body()
            try:
                parsed = self._http_request_type.model_validate_json(body_bytes)
            except ValidationError as exc:
                # ``include_input=False`` keeps the offending raw bytes
                # OUT of the response body (the default ``input`` field
                # carries the raw value, which can be ``bytes`` and isn't
                # JSON-encodable; even when encodable, echoing it back
                # leaks request payload to anyone who could already see
                # the response). ``include_url=False`` strips the
                # documentation URLs Pydantic adds — operators read
                # docs, the API caller does not need them.
                return JSONResponse(
                    status_code=422,
                    content={
                        'error': 'invalid_request',
                        'request_id': gate_request_id,
                        'details': exc.errors(include_input=False, include_url=False),
                    },
                )
            except ValueError as exc:
                # ``model_validate_json`` surfaces malformed JSON as a
                # plain ``ValueError`` — same flat envelope, different
                # ``details`` shape (Pydantic gives us a single string).
                return JSONResponse(
                    status_code=422,
                    content={
                        'error': 'invalid_request',
                        'request_id': gate_request_id,
                        'details': str(exc),
                    },
                )

            # Resolve the framework request_id via the handler hook so
            # users can promote an upstream tracing header (e.g.,
            # ``X-Request-ID``) into the framework id. The hook's return
            # value goes through ``validate_request_id`` so a buggy
            # override fails loudly rather than producing log labels
            # with whitespace / non-ASCII.
            try:
                request_id = self._app._handler.http_request_id(parsed, dict(request.headers))
                validate_request_id(request_id)
            except Exception as exc:
                logger.warning(
                    'webapp_request_id_resolution_failed',
                    category='webapp',
                    error_type=type(exc).__name__,
                    error=str(exc),
                )
                return JSONResponse(
                    status_code=500,
                    content={
                        'status': 'error',
                        'request_id': gate_request_id,
                        'error': 'internal error',
                    },
                )

            ctx = WebRequestContext(
                request_id=request_id,
                client_name=client.name,
                request=parsed,
                started_at=datetime.now(UTC),
                headers=dict(request.headers),
            )

            # T2-side concurrency cap. ``acquire`` with a tiny non-zero
            # timeout (``_SEMAPHORE_ACQUIRE_PROBE_SECONDS``) probes the
            # semaphore: if a slot is free we get it instantly, otherwise
            # ``TimeoutError`` falls through and we shed the request as
            # 503 ``capacity`` rather than queue. This matches the
            # documented "fail fast → tell the client to use Kafka"
            # contract for over-cap requests.
            try:
                await asyncio.wait_for(
                    self._semaphore.acquire(),
                    timeout=_SEMAPHORE_ACQUIRE_PROBE_SECONDS,
                )
            except TimeoutError:
                logger.warning(
                    'webapp_request_over_capacity',
                    category='webapp',
                    request_id=ctx.request_id,
                    client=ctx.client_name,
                    max_concurrent=self._config.max_concurrent,
                )
                return JSONResponse(
                    status_code=503,
                    content={
                        'status': 'capacity',
                        'error': 'webapp is over capacity; request rejected',
                        'request_id': ctx.request_id,
                        'max_concurrent': self._config.max_concurrent,
                        'hint': KAFKA_FALLBACK_HINT,
                    },
                )

            # Dispatch the runner coroutine to the main pipeline loop.
            # The loop captured by ``DrakkarApp`` is where the executor
            # pool's gate, the recorder writer, and any aiosqlite
            # connections were constructed — running the runner on T2
            # would trip the "loop binding" RuntimeError documented in
            # ``concurrency.py``. ``dispatch_to_loop`` handles the
            # cross-thread hop and exception forwarding.
            #
            # Wrapped in ``asyncio.wait_for`` so a slow handler cannot
            # block the webapp loop indefinitely. On timeout we signal
            # the runner via ``ctx.cancelled.set`` (scheduled on T1's
            # loop so the Event binds correctly) and return a flat 504.
            try:
                try:
                    report = await asyncio.wait_for(
                        dispatch_to_loop(
                            self._runner.run(ctx),
                            target_loop=self._app.main_loop,
                        ),
                        timeout=self._config.request_timeout_seconds,
                    )
                except TimeoutError:
                    # Schedule the cancellation flag flip on T1's loop.
                    # ``call_soon_threadsafe`` is the documented way to
                    # touch loop-bound objects (here, the ``asyncio.Event``
                    # the runner created on T1). The callback handles the
                    # "runner hasn't started yet" race by checking
                    # ``ctx.cancelled is not None``.
                    self._signal_cancel(ctx)
                    duration_ms = (datetime.now(UTC) - ctx.started_at).total_seconds() * 1000.0
                    logger.warning(
                        'webapp_request_timeout',
                        category='webapp',
                        request_id=ctx.request_id,
                        client=ctx.client_name,
                        timeout_seconds=self._config.request_timeout_seconds,
                        duration_ms=duration_ms,
                    )
                    return JSONResponse(
                        status_code=504,
                        content={
                            'status': 'timeout',
                            'error': (f'request exceeded {self._config.request_timeout_seconds} seconds'),
                            'request_id': ctx.request_id,
                            'duration_ms': duration_ms,
                            'hint': KAFKA_FALLBACK_HINT,
                        },
                    )
                except asyncio.CancelledError:
                    # The runner raised CancelledError after the post-
                    # execute / pre-on_http_request_complete gate — this
                    # path is taken when ``ctx.cancelled`` was already
                    # set (e.g., the route handler is being torn down by
                    # the webapp's own shutdown). Surface it as a 504
                    # for consistency with the timeout path: from the
                    # client's perspective a cancelled request looks the
                    # same as one that timed out.
                    duration_ms = (datetime.now(UTC) - ctx.started_at).total_seconds() * 1000.0
                    return JSONResponse(
                        status_code=504,
                        content={
                            'status': 'timeout',
                            'error': 'request was cancelled before completion',
                            'request_id': ctx.request_id,
                            'duration_ms': duration_ms,
                            'hint': KAFKA_FALLBACK_HINT,
                        },
                    )
                except WebappHandlerError as exc:
                    # User-hook failure on the pipeline side. Already logged
                    # with the full traceback inside the runner; here we just
                    # surface a flat 500 body. NEVER include the traceback
                    # in the response.
                    logger.warning(
                        'webapp_request_handler_error',
                        category='webapp',
                        request_id=ctx.request_id,
                        client=ctx.client_name,
                        where=exc.where,
                        error_type=type(exc.original_exc).__name__,
                    )
                    return JSONResponse(
                        status_code=500,
                        content={
                            'status': 'error',
                            'request_id': ctx.request_id,
                            'error': 'internal error',
                        },
                    )
                return JSONResponse(
                    status_code=200,
                    content=report.model_dump(mode='json'),
                )
            finally:
                # Always release the semaphore — even on timeout / error.
                # Without this an unhandled exception path would leak a
                # permit and slowly starve the pool.
                self._semaphore.release()

        return app
