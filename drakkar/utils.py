"""Shared utility functions for Drakkar framework."""

import asyncio
import datetime as _dt
import itertools
import math
import re
import threading
import time
from collections.abc import Awaitable, Callable

# Query parameters whose value is secret material. Matched by substring so
# variants like ``sslpassword`` or ``api_token`` are caught too — over-redacting
# an odd parameter name in a log line is harmless, leaking a secret is not.
_SENSITIVE_QUERY_PARAM = re.compile(r'(?i)([?&][^=&#]*(?:password|passwd|pwd|secret|token|key)[^=&#]*=)[^&#]*')


def redact_url(url: str) -> str:
    """Redact credentials from URIs.

    Replaces ``user:pass@`` with ``***:***@`` and blanks the value of any
    password-like query parameter (``?password=...``, ``&sslkey=...``) —
    DSNs such as ``postgresql://host/db?password=x`` carry the secret in
    the query string instead of the authority part.
    """
    redacted = re.sub(r'://[^@/]+@', '://***:***@', url)
    return _SENSITIVE_QUERY_PARAM.sub(r'\1***', redacted)


# Module-level monotone counter for ``make_request_id``. Locked so the
# generator stays correct under threaded use (the webapp event loop runs in a
# separate thread from the main pipeline loop). ``itertools.count`` is itself
# atomic in CPython for single ``next()`` calls, but we keep the lock to make
# the contract portable and the timestamp-snapshot + counter-increment atomic
# as a pair.
_REQUEST_ID_COUNTER = itertools.count(start=1)
_REQUEST_ID_LOCK = threading.Lock()
_REQUEST_ID_MAX_LEN = 64


def make_request_id(prefix: str = 'req') -> str:
    """Generate a webapp request ID.

    Format: ``<prefix>_<UTC-timestamp>_<monotone-counter>``
    Example: ``req_20260506T184231_0042``

    The timestamp uses UTC compact ISO-8601 form (``YYYYMMDDTHHMMSS``) so IDs
    sort lexicographically by creation time within the same monotone-counter
    epoch. The monotone counter is module-level — consecutive calls always
    produce different IDs, even within the same second, even across threads.

    Mirrors the role of :func:`drakkar.models.make_task_id` but uses a
    human-readable timestamp (operators read these IDs in logs and HTTP
    responses; task IDs are framework-internal and use a hex format for
    compactness).
    """
    with _REQUEST_ID_LOCK:
        # ``utcnow()`` is deprecated in 3.13; use the timezone-aware form and
        # format manually to keep the compact ``YYYYMMDDTHHMMSS`` shape.
        now = _dt.datetime.now(_dt.UTC)
        ts = now.strftime('%Y%m%dT%H%M%S')
        seq = next(_REQUEST_ID_COUNTER)
    return f'{prefix}_{ts}_{seq:04d}'


def validate_request_id(rid: str) -> None:
    """Validate a webapp request ID supplied by user code.

    Enforces three invariants the framework relies on for safe inclusion in
    log labels, HTTP response bodies, and recorder rows:

    * length ``<= 64`` characters
    * ASCII-only (no non-ASCII characters such as ``é``)
    * no whitespace (no spaces, tabs, newlines, or other ``str.isspace``
      characters)

    Raises :class:`ValueError` with a message that names the offending input
    so operators can quickly find the bad handler override.

    Used internally by the webapp framework when validating request IDs
    returned from the optional ``http_request_id`` handler hook. Not part of
    the public API surface.
    """
    if len(rid) > _REQUEST_ID_MAX_LEN:
        raise ValueError(f'request_id too long (max {_REQUEST_ID_MAX_LEN} chars, got {len(rid)}): {rid!r}')
    if not rid.isascii():
        raise ValueError(f'request_id must be ASCII-only: {rid!r}')
    if any(ch.isspace() for ch in rid):
        raise ValueError(f'request_id must not contain whitespace: {rid!r}')


async def wait_for_aligned_startup(
    min_wait_seconds: float,
    align_interval_seconds: int,
    *,
    _clock: Callable[[], float] | None = None,
    _sleep: Callable[[float], Awaitable[None]] | None = None,
) -> float:
    """Sleep until a wall-clock boundary so a fleet of workers aligns on one moment.

    Sequence:
      1. Sleep ``min_wait_seconds`` — buffer for slow init (DB connects,
         schema migrations, cache warm-up, etc.) before we're ready to
         subscribe to Kafka.
      2. Sleep until the next Unix-epoch second that is a multiple of
         ``align_interval_seconds``. For the default interval of 10,
         that's :00/:10/:20/:30/:40/:50 in wall-clock (timezone offsets
         are always whole minutes so second-of-minute alignment is
         identical in UTC and local time).

    Rationale: during a rolling deploy, workers come up one at a time.
    Each fresh subscribe triggers a Kafka consumer-group rebalance that
    stalls consumption on all other workers. Converging on a shared
    boundary collapses N rebalances into 1.

    Returns the total seconds actually slept.

    ``_clock`` and ``_sleep`` are injection hooks for tests; production
    callers should not set them. ``_clock()`` must return current unix
    seconds (like ``time.time()``); ``_sleep(seconds)`` must be an async
    sleep (like ``asyncio.sleep``).
    """
    # time.time and asyncio.sleep are the real-world defaults; tests
    # inject fakes to avoid sitting through 10-second waits.
    clock = _clock if _clock is not None else time.time
    sleep = _sleep if _sleep is not None else asyncio.sleep

    start = clock()
    if min_wait_seconds > 0:
        await sleep(min_wait_seconds)

    # ceil(now / interval) * interval — the earliest boundary that is
    # >= now. If we land EXACTLY on a boundary, stay there (zero extra
    # wait) so an on-time worker doesn't skip a whole interval.
    now = clock()
    target = math.ceil(now / align_interval_seconds) * align_interval_seconds
    remaining = target - now
    if remaining > 0:
        await sleep(remaining)
    return clock() - start
