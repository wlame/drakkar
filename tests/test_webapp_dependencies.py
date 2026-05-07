"""Tests for :mod:`drakkar.webapp.dependencies` and :mod:`drakkar.webapp.utils`.

Covers three layers:

* Pure helpers (``compute_retry_after``, ``redact_token``) — small,
  synchronous, table-driven tests.
* Dependency callables (``make_authenticate``, ``make_rate_limit``)
  invoked directly with a synthetic ``Request`` so the auth/rate-limit
  logic is exercised without a TestClient round-trip.
* End-to-end via FastAPI's ``TestClient`` against a fully-built
  :class:`drakkar.webapp.WebApp` to verify the route signature wires
  the dependencies and that the registered exception handler emits
  flat JSON bodies (not the default ``{'detail': ...}`` envelope).
"""

from __future__ import annotations

import time
from collections import deque
from typing import Any
from unittest.mock import MagicMock

import pytest
from fastapi import Request
from fastapi.testclient import TestClient
from pydantic import BaseModel

from drakkar.config import WebAppConfig, WebClientConfig
from drakkar.handler import BaseDrakkarHandler
from drakkar.webapp import WebApp
from drakkar.webapp.dependencies import (
    KAFKA_FALLBACK_HINT,
    WebappAuthError,
    WebappRateLimitError,
    make_authenticate,
    make_rate_limit,
)
from drakkar.webapp.utils import compute_retry_after, redact_token

# ---------------------------------------------------------------------------
# Test handlers + WebApp builder
# ---------------------------------------------------------------------------


class _Input(BaseModel):
    """Stand-in Kafka-input model — required slot for BaseDrakkarHandler."""

    a: int = 0


class _Output(BaseModel):
    """Stand-in Kafka-output model."""

    b: int = 0


class _HttpReq(BaseModel):
    """Stand-in HTTP request model — fills the 3rd Generic slot."""

    pattern: str = ''


class _HttpResp(BaseModel):
    """Stand-in HTTP response model — fills the 4th Generic slot."""

    matches: int = 0


class _WebHandler(BaseDrakkarHandler[_Input, _Output, _HttpReq, _HttpResp]):
    """Handler with all four Generic slots populated."""

    async def arrange(self, messages, pending):
        return []


def _make_app_stub(*, is_ready: bool = True) -> Any:
    """Build a minimal ``DrakkarApp`` stand-in for unit tests.

    The webapp only reaches into ``app._handler`` (for type validation)
    and ``app.is_ready`` (for the 503 gate). A ``MagicMock`` with those
    attributes set is enough to drive the dependency wiring.
    """
    app = MagicMock()
    app._handler = _WebHandler()
    app.is_ready = is_ready
    return app


def _make_request(headers: dict[str, str] | None = None) -> Request:
    """Build a synthetic Starlette ``Request`` for direct dep invocation.

    We construct just enough of an ASGI scope for ``Request.headers``
    and ``Request.state`` to behave normally — no body, no transport,
    no ASGI app. Lets us call dependencies as plain async functions.
    """
    raw_headers = []
    if headers:
        for key, value in headers.items():
            raw_headers.append((key.lower().encode(), value.encode()))
    scope: dict[str, Any] = {
        'type': 'http',
        'method': 'POST',
        'path': '/process',
        'headers': raw_headers,
        # ``request.state`` reads from this dict; FastAPI populates it
        # automatically inside the real ASGI flow.
        'state': {},
    }
    return Request(scope)


# ---------------------------------------------------------------------------
# Pure helpers — compute_retry_after, redact_token
# ---------------------------------------------------------------------------


def test_compute_retry_after_under_cap_returns_zero():
    """Below the rpm cap, the helper returns 0.0 (no wait needed)."""
    timestamps: deque[float] = deque([100.0, 101.0])
    assert compute_retry_after(timestamps, rpm=4, now=102.0) == 0.0


def test_compute_retry_after_at_cap_returns_seconds_until_window_edge():
    """At the cap, retry-after is ``oldest + 60 - now``."""
    # Oldest at 100.0, now at 110.0, window is 60s → next admission at
    # 100.0 + 60 = 160.0; from now=110.0 that is 50s away.
    timestamps: deque[float] = deque([100.0, 105.0, 108.0, 110.0])
    assert compute_retry_after(timestamps, rpm=4, now=110.0) == pytest.approx(50.0)


def test_compute_retry_after_clamps_to_zero_when_oldest_already_expired():
    """Edge race: caller didn't pop expired entries → result clamps at 0."""
    # Oldest at 30.0, now at 200.0 → 30 + 60 - 200 = -110, clamps to 0.
    timestamps: deque[float] = deque([30.0, 31.0, 32.0, 33.0])
    assert compute_retry_after(timestamps, rpm=4, now=200.0) == 0.0


def test_redact_token_normal_token_keeps_first_four_chars():
    """A real-shape token surfaces only the first 4 chars + ellipsis."""
    assert redact_token('tenant-A-secret-XYZ123') == 'tena...'


def test_redact_token_empty_token_returns_sentinel():
    """Empty token (anonymous attempt) → ``<empty>`` sentinel."""
    assert redact_token('') == '<empty>'


def test_redact_token_short_token_returns_sentinel_not_partial():
    """Tokens shorter than the prefix length → sentinel (never the value).

    This is the load-bearing safety property: redact_token never
    surfaces a full token, even by accident on a malformed input.
    """
    assert redact_token('abc') == '<empty>'
    assert redact_token('a') == '<empty>'


def test_redact_token_never_returns_full_token_for_realistic_inputs():
    """Defence-in-depth: scan a range of inputs, ensure none round-trip raw."""
    samples = [
        'sk-abcdef0123456789',
        'tenant-A',  # 8 chars
        'secret',  # 6 chars
        'AAAA',  # exactly 4 chars — boundary case
        'AAAAA',  # 5 chars — just above prefix
    ]
    for tok in samples:
        out = redact_token(tok)
        assert out != tok, f'redact_token leaked the full token for {tok!r}'


# ---------------------------------------------------------------------------
# make_authenticate — direct invocation
# ---------------------------------------------------------------------------


@pytest.fixture
def multi_client_config() -> WebAppConfig:
    """Three clients: anonymous (empty token), tenant-A, tenant-B."""
    return WebAppConfig(
        enabled=True,
        clients=[
            WebClientConfig(name='anonymous', token='', rpm=4),
            WebClientConfig(name='tenant-A', token='token-a-secret', rpm=10),
            WebClientConfig(name='tenant-B', token='token-b-secret', rpm=20),
        ],
    )


@pytest.mark.asyncio
async def test_authenticate_anonymous_matches_request_with_no_auth_header(
    multi_client_config: WebAppConfig,
):
    """No Authorization header + empty-token client configured → match anonymous."""
    auth = make_authenticate(multi_client_config)
    request = _make_request(headers=None)

    client = await auth(request)

    assert client.name == 'anonymous'
    assert client.token == ''
    # The auth dep stashes the matched client name on request.state for
    # downstream code (logging, runner) to read without re-doing the match.
    assert request.state.client_name == 'anonymous'


@pytest.mark.asyncio
async def test_authenticate_valid_bearer_token_matches_named_client(
    multi_client_config: WebAppConfig,
):
    """``Authorization: Bearer <known>`` → returns matching WebClientConfig."""
    auth = make_authenticate(multi_client_config)
    request = _make_request(headers={'Authorization': 'Bearer token-a-secret'})

    client = await auth(request)

    assert client.name == 'tenant-A'
    assert request.state.client_name == 'tenant-A'


@pytest.mark.asyncio
async def test_authenticate_unknown_bearer_token_raises_webapp_auth_error(
    multi_client_config: WebAppConfig,
):
    """Unknown token → 401 with flat ``{'error': 'unauthorized'}`` body."""
    auth = make_authenticate(multi_client_config)
    request = _make_request(headers={'Authorization': 'Bearer not-a-valid-token'})

    with pytest.raises(WebappAuthError) as exc_info:
        await auth(request)

    err = exc_info.value
    assert err.status_code == 401
    assert err.body_dict == {'error': 'unauthorized'}
    # No headers required for auth failures (rate-limit is the one that
    # ships ``Retry-After``).
    assert err.headers is None


@pytest.mark.asyncio
async def test_authenticate_non_bearer_scheme_raises_webapp_auth_error(
    multi_client_config: WebAppConfig,
):
    """``Authorization: Basic ...`` → 401 even when anonymous slot exists.

    A malformed scheme means the caller intended to authenticate but
    used the wrong format. Falling through to anonymous would be
    surprising and would silently grant access — fail loudly instead.
    """
    auth = make_authenticate(multi_client_config)
    request = _make_request(headers={'Authorization': 'Basic dXNlcjpwYXNz'})

    with pytest.raises(WebappAuthError):
        await auth(request)


@pytest.mark.asyncio
async def test_authenticate_missing_header_with_no_anonymous_client_raises():
    """No Authorization header AND no empty-token client → 401."""
    config = WebAppConfig(
        enabled=True,
        clients=[
            WebClientConfig(name='tenant-A', token='token-a-secret', rpm=10),
        ],
    )
    auth = make_authenticate(config)
    request = _make_request(headers=None)

    with pytest.raises(WebappAuthError):
        await auth(request)


# ---------------------------------------------------------------------------
# make_rate_limit — direct invocation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rate_limit_under_cap_admits_without_raising(multi_client_config: WebAppConfig):
    """Under the rpm cap → dependency returns None and admits the request."""
    rate_limit = make_rate_limit(multi_client_config)
    client = WebClientConfig(name='tenant-A', token='t', rpm=3)

    # Three calls fit under rpm=3 (< condition, the 4th would block).
    for _ in range(3):
        result = await rate_limit(client)
        assert result is None


@pytest.mark.asyncio
async def test_rate_limit_at_cap_raises_with_full_documented_body():
    """Over-cap → 429 body matches the documented shape exactly."""
    config = WebAppConfig(
        enabled=True,
        clients=[WebClientConfig(name='tenant-A', token='t', rpm=2)],
    )
    rate_limit = make_rate_limit(config)
    client = config.clients[0]

    # Burn the cap.
    await rate_limit(client)
    await rate_limit(client)

    with pytest.raises(WebappRateLimitError) as exc_info:
        await rate_limit(client)

    err = exc_info.value
    assert err.status_code == 429
    body = err.body_dict
    assert body['error'] == 'rate_limited'
    assert body['client'] == 'tenant-A'
    assert body['rpm_limit'] == 2
    assert isinstance(body['retry_after_seconds'], float)
    assert body['retry_after_seconds'] >= 0.0
    assert body['hint'] == KAFKA_FALLBACK_HINT
    # Retry-After header present and value is integer-stringified seconds.
    assert err.headers is not None
    retry_after_header = err.headers.get('Retry-After')
    assert retry_after_header is not None
    # Round-up plus 1 — the value is always at least 1 (header values
    # below 1 second don't help clients back off effectively).
    assert int(retry_after_header) >= 1


@pytest.mark.asyncio
async def test_rate_limit_trims_expired_timestamps():
    """Old timestamps fall off the rolling window; new admissions allowed.

    We can't easily wait 60s in a unit test, so we monkey-patch
    ``time.monotonic`` to fake the passage of time. The rate-limit
    closure reads the clock once per call, so swapping it is enough.
    """
    config = WebAppConfig(
        enabled=True,
        clients=[WebClientConfig(name='tenant-A', token='t', rpm=2)],
    )
    rate_limit = make_rate_limit(config)
    client = config.clients[0]

    fake_now = {'t': 1000.0}

    real_monotonic = time.monotonic

    def fake_monotonic() -> float:
        return fake_now['t']

    time.monotonic = fake_monotonic  # type: ignore[assignment]
    try:
        # Burn the cap at t=1000.
        await rate_limit(client)
        await rate_limit(client)
        # Now at-cap: a third call at the same moment must raise.
        with pytest.raises(WebappRateLimitError):
            await rate_limit(client)

        # Fast-forward past the 60-second window — both timestamps are
        # now expired and a fresh call should admit cleanly.
        fake_now['t'] = 1100.0
        result = await rate_limit(client)
        assert result is None
    finally:
        time.monotonic = real_monotonic  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# End-to-end via TestClient — verifies route wiring + flat error bodies
# ---------------------------------------------------------------------------


def _make_webapp(rpm: int = 4) -> WebApp:
    """Build a fully-wired ``WebApp`` for TestClient round-trips.

    Uses an in-process app stub (no thread, no real ``DrakkarApp``) —
    the dependency wiring and exception handler do not require the
    main pipeline to be running.
    """
    config = WebAppConfig(
        enabled=True,
        host='127.0.0.1',
        port=0,  # we never call start_in_thread — TestClient bypasses uvicorn
        path='/process',
        clients=[
            WebClientConfig(name='anonymous', token='', rpm=rpm),
            WebClientConfig(name='tenant-A', token='token-a-secret', rpm=rpm),
        ],
    )
    return WebApp(_make_app_stub(is_ready=True), config)


def test_post_with_no_auth_returns_401_flat_body():
    """E2E: missing Auth + no anonymous match → 401 with flat ``error`` body.

    Flatness is the load-bearing assertion: FastAPI's default
    ``HTTPException`` handling would wrap the body in
    ``{'detail': 'unauthorized'}``. Our custom exception handler
    emits the documented flat shape instead.
    """
    config = WebAppConfig(
        enabled=True,
        host='127.0.0.1',
        port=0,
        path='/process',
        clients=[
            # Only a named client — no anonymous slot.
            WebClientConfig(name='tenant-A', token='token-a-secret', rpm=4),
        ],
    )
    webapp = WebApp(_make_app_stub(is_ready=True), config)
    client = TestClient(webapp._fastapi_app)

    response = client.post('/process', json={})

    assert response.status_code == 401
    body = response.json()
    # Flat shape — NOT ``{'detail': ...}``.
    assert body == {'error': 'unauthorized'}
    assert 'detail' not in body


def test_post_with_valid_token_reaches_route_stub():
    """E2E: valid Bearer token passes auth+rate-limit → reaches 501 stub.

    501 is the Task 4 stub — Task 6 replaces it with the real runner.
    Reaching 501 (rather than 401/429) proves the dependencies admitted
    the request.
    """
    webapp = _make_webapp(rpm=4)
    client = TestClient(webapp._fastapi_app)

    response = client.post(
        '/process',
        json={},
        headers={'Authorization': 'Bearer token-a-secret'},
    )

    assert response.status_code == 501
    body = response.json()
    assert body['status'] == 'not_implemented'
    assert body['client'] == 'tenant-A'


def test_post_over_rpm_cap_returns_429_with_documented_shape():
    """E2E: exceeding rpm → 429 with full documented body + Retry-After header."""
    webapp = _make_webapp(rpm=2)
    client = TestClient(webapp._fastapi_app)

    # Burn the cap as anonymous — first two requests admit (501 stub),
    # third must rate-limit.
    for _ in range(2):
        response = client.post('/process', json={})
        assert response.status_code == 501

    response = client.post('/process', json={})
    assert response.status_code == 429
    body = response.json()
    # Exact documented shape.
    assert body['error'] == 'rate_limited'
    assert body['client'] == 'anonymous'
    assert body['rpm_limit'] == 2
    assert isinstance(body['retry_after_seconds'], float)
    assert body['retry_after_seconds'] >= 0.0
    assert body['hint'] == KAFKA_FALLBACK_HINT
    # Header present.
    assert 'retry-after' in {k.lower() for k in response.headers}


def test_post_anonymous_without_auth_admitted_under_cap():
    """E2E: empty-token client matches no-Authorization request and admits."""
    webapp = _make_webapp(rpm=4)
    client = TestClient(webapp._fastapi_app)

    response = client.post('/process', json={})

    assert response.status_code == 501
    body = response.json()
    assert body['client'] == 'anonymous'
