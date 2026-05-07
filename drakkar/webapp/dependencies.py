"""Webapp authentication and rate-limit dependencies.

The webapp exposes one route. FastAPI dependency injection (``Depends``)
is the idiomatic way to slot per-request work in front of that route:

* dependencies are plain async functions, easy to unit-test by calling
  them with a synthetic ``Request`` (no ASGI machinery required);
* the ``BaseHTTPMiddleware`` caveats around response buffering and
  exception-handler dispatch don't apply;
* error bodies stay flat (``{"error": "unauthorized"}``) because we
  raise typed exceptions instead of ``HTTPException`` (which would
  wrap the body in ``{"detail": ...}``).

Error-body emission strategy
----------------------------

Both auth and rate-limit failures need flat JSON bodies. Rather than
fight FastAPI's ``HTTPException`` envelope, we define a base class
:class:`WebappError` carrying ``status_code``, ``body_dict``, and
optional ``headers``. ``WebApp._build_app`` registers a single
exception handler for this base class, and FastAPI's
exception-dispatch machinery routes both subclasses
(:class:`WebappAuthError`, :class:`WebappRateLimitError`) through it.

Rate-limit window
-----------------

Per-client deques live in a closure over the ``make_rate_limit``
factory. Each deque holds the timestamps of recent admissions; on
every call we pop entries older than 60s and admit when ``len(deque)
< rpm``. Over-cap requests raise :class:`WebappRateLimitError` with a
``Retry-After`` header derived from the oldest timestamp.

Constant-time comparison
------------------------

Token matching uses :func:`hmac.compare_digest` so a malicious caller
cannot use response timing to discover which token prefixes are valid.
"""

from __future__ import annotations

import hmac
import time
from collections import deque
from collections.abc import Awaitable, Callable
from typing import Any

import structlog
from fastapi import Request

from drakkar.config import WebAppConfig, WebClientConfig
from drakkar.webapp.utils import compute_retry_after, redact_token

logger = structlog.get_logger()


# Hint included in 429 response bodies. Documented in the plan under
# "HTTP status codes" — clients hitting the rpm cap should publish to
# the source topic for higher throughput and restart-resilient delivery.
KAFKA_FALLBACK_HINT = (
    'route this workload through the Kafka source topic for higher throughput and worker-restart resilience'
)


class WebappError(Exception):
    """Base class for webapp-specific HTTP errors with flat JSON bodies.

    Subclasses are raised from FastAPI dependencies and handled by a
    single ``@app.exception_handler(WebappError)`` registered in
    ``WebApp._build_app``. The handler emits a ``JSONResponse`` with
    the carried ``status_code``, ``body_dict``, and ``headers``,
    bypassing the default ``HTTPException`` envelope ``{'detail': ...}``.

    Attributes
    ----------
    status_code:
        HTTP status code to emit (e.g., 401, 429).
    body_dict:
        Flat dict serialised as the JSON body verbatim.
    headers:
        Optional response headers (e.g., ``{'Retry-After': '5'}``).
    """

    def __init__(
        self,
        status_code: int,
        body_dict: dict[str, Any],
        headers: dict[str, str] | None = None,
    ) -> None:
        # ``Exception.__init__`` accepts an arbitrary tuple as ``args``;
        # we store the structured fields explicitly so the handler can
        # read them without parsing ``args``.
        super().__init__(body_dict.get('error', 'webapp_error'))
        self.status_code = status_code
        self.body_dict = body_dict
        self.headers = headers


class WebappAuthError(WebappError):
    """Raised when the Authorization header doesn't match a configured client.

    Always emits 401 with ``{'error': 'unauthorized'}``. Subclassing
    :class:`WebappError` keeps the dispatch path uniform — the single
    registered exception handler catches the base class and serialises
    the carried ``body_dict`` directly.
    """


class WebappRateLimitError(WebappError):
    """Raised when a client exceeds its per-minute request cap.

    Emits 429 with the documented body shape (client name, rpm cap,
    retry-after seconds, Kafka-fallback hint) and a ``Retry-After``
    header so well-behaved clients back off automatically.
    """


def _extract_bearer_token(request: Request) -> tuple[str, bool]:
    """Pull the bearer token from the ``Authorization`` header.

    Returns ``(token, header_present)``:

    * ``("", False)`` when no Authorization header is set — anonymous
      attempt; auth will succeed if a configured client has empty
      token.
    * ``("", True)`` when the header is present but doesn't use the
      ``Bearer`` scheme (e.g., ``Basic ...``) — auth must fail because
      we cannot extract a token to compare against.
    * ``("<token>", True)`` for a well-formed ``Bearer <token>`` —
      auth proceeds with the extracted token.

    Returning the ``header_present`` flag lets the caller distinguish
    "no auth attempted" (empty-token client wins on anonymous match)
    from "wrong scheme" (always 401, even if anonymous is configured).
    """
    header = request.headers.get('authorization') or request.headers.get('Authorization')
    if header is None:
        return '', False
    parts = header.strip().split(None, 1)
    if len(parts) != 2 or parts[0].lower() != 'bearer':
        # Header present but malformed or non-Bearer — caller must
        # 401 even if there is an anonymous slot configured.
        return '', True
    return parts[1].strip(), True


def make_authenticate(
    config: WebAppConfig,
) -> Callable[[Request], Awaitable[WebClientConfig]]:
    """Build the auth dependency callable closed over ``config``.

    The returned async function is a FastAPI dependency: it inspects
    the request's ``Authorization`` header, walks the configured
    clients with :func:`hmac.compare_digest`, and either returns the
    matched :class:`WebClientConfig` or raises :class:`WebappAuthError`.

    On success we also stash ``client.name`` on ``request.state`` so
    downstream code (logging middleware, the runner) can read the
    matched-client identity without re-doing the lookup.

    Why a factory? FastAPI passes dependency callables a request
    object only — we cannot pass ``config`` as a positional arg. The
    factory closes over it once at startup, which also matches how we
    build the rate-limiter (see ``make_rate_limit`` for symmetry).
    """

    async def _authenticate(request: Request) -> WebClientConfig:
        # Stash a request-start monotonic timestamp on ``request.state``
        # so the route's outcome-observation helper can record a duration
        # alongside the outcome counter. The auth dep is the earliest
        # per-request entry point we own — putting it here means even
        # auth-failed responses get a duration observation.
        request.state.webapp_start_monotonic = time.monotonic()
        token, header_present = _extract_bearer_token(request)

        # Walk every configured client and pick the one whose token
        # matches in constant time. The empty-token slot only matches
        # when no Authorization header was present — sending
        # ``Authorization: Bearer`` (empty token) does NOT auth as
        # anonymous because that's almost certainly a misconfigured
        # client trying to use a real tenant slot.
        for client in config.clients:
            if client.token == '':
                if not header_present:
                    request.state.client_name = client.name
                    return client
                continue
            # ``hmac.compare_digest`` returns False for differing-length
            # inputs but never short-circuits, so a malicious caller
            # cannot tell which prefix matched from response timing.
            if hmac.compare_digest(client.token, token):
                request.state.client_name = client.name
                return client

        # No match. Log the redacted token so operators can correlate
        # auth failures with specific clients in audit trails without
        # ever persisting the raw secret.
        await logger.ainfo(
            'webapp_request_auth_failed',
            category='webapp',
            token_prefix=redact_token(token),
            header_present=header_present,
        )
        raise WebappAuthError(
            status_code=401,
            body_dict={'error': 'unauthorized'},
        )

    return _authenticate


def make_rate_limit(
    config: WebAppConfig,
) -> Callable[[WebClientConfig], Awaitable[None]]:
    """Build the rate-limit dependency callable closed over ``config``.

    The returned async function is a FastAPI dependency: it consumes
    the matched :class:`WebClientConfig` (resolved by the auth
    dependency) and either records a fresh admission or raises
    :class:`WebappRateLimitError`.

    Internal state lives in a closure-scoped ``dict[str, deque[float]]``
    keyed by client name. Each deque is a sliding window of admission
    timestamps; on every call we trim entries older than 60s and admit
    when ``len(deque) < client.rpm``. The dict is local to the closure
    so each ``WebApp`` instance gets its own counter set — important
    for tests that spin up multiple webapps in the same process.
    """

    # Per-client sliding-window state. ``setdefault`` lazily creates
    # the deque on first hit so configured-but-idle clients don't
    # consume memory.
    windows: dict[str, deque[float]] = {}

    async def _rate_limit(client: WebClientConfig) -> None:
        now = time.monotonic()
        window = windows.setdefault(client.name, deque())

        # Pop expired timestamps. A while-loop is cheaper than building
        # a fresh deque because the deque is at most ``rpm`` elements
        # long and only entries past the leftmost edge can possibly be
        # expired (timestamps are appended chronologically).
        cutoff = now - 60.0
        while window and window[0] <= cutoff:
            window.popleft()

        if len(window) >= client.rpm:
            retry_after = compute_retry_after(window, client.rpm, now)
            await logger.ainfo(
                'webapp_request_rate_limited',
                category='webapp',
                client=client.name,
                rpm_limit=client.rpm,
                retry_after_seconds=retry_after,
            )
            # ``Retry-After`` is integer seconds per RFC 7231; we round
            # up to be safe (a client backing off for the exact float
            # would race the window edge). ``int(x) + 1`` rounds up
            # for any positive ``x`` and stays well-defined at 0.0.
            retry_after_header = str(int(retry_after) + 1)
            raise WebappRateLimitError(
                status_code=429,
                body_dict={
                    'error': 'rate_limited',
                    'client': client.name,
                    'rpm_limit': client.rpm,
                    'retry_after_seconds': retry_after,
                    'hint': KAFKA_FALLBACK_HINT,
                },
                headers={'Retry-After': retry_after_header},
            )

        window.append(now)

    return _rate_limit
