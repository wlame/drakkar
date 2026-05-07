"""Webapp utility helpers — small, pure functions used by dependencies.

Split out from :mod:`drakkar.webapp.dependencies` so they can be unit-
tested without importing FastAPI machinery. The functions here are
intentionally synchronous and side-effect-free; logic that touches a
``Request`` or schedules logs lives in ``dependencies.py``.
"""

from __future__ import annotations

from collections import deque

# How many leading characters of a token we keep in log lines. Picked to
# be enough to disambiguate per-tenant tokens at a glance while still
# being short enough that an attacker observing logs cannot reconstruct
# the secret.
_TOKEN_REDACT_PREFIX_LEN = 4

# Width of the rolling rate-limit window in seconds. The webapp config
# expresses caps as "requests per minute"; we apply the cap over a
# 60-second sliding window so a burst at second 0 gets its quota back at
# second 60 rather than at the top of the next minute.
_RATE_LIMIT_WINDOW_SECONDS = 60.0


def compute_retry_after(timestamps: deque[float], rpm: int, now: float) -> float:
    """Return seconds until the rate-limit window admits another request.

    The deque holds the timestamps of recent admissions for one client,
    sorted oldest-first. When the deque length is below ``rpm`` the
    client has spare capacity and the caller should not be raising
    ``WebappRateLimitError``; we return 0.0 for that case so the helper
    is safe to call unconditionally.

    When the deque is at or above the cap, the next admission can only
    happen after the oldest timestamp falls off the rolling 60-second
    window. The deadline is ``oldest + 60s``; from ``now`` that is
    ``oldest + 60 - now``. ``max(0.0, ...)`` clamps the result for the
    edge case where the oldest timestamp is already past the window
    (the caller should pop it before calling, but we tolerate races
    rather than raising).

    Parameters
    ----------
    timestamps:
        Deque of recent admission timestamps for a single client. The
        helper does not mutate it.
    rpm:
        Per-client requests-per-minute cap.
    now:
        Current monotonic-ish timestamp (seconds, same clock the deque
        was populated with).

    Returns
    -------
    float
        Seconds the client must wait before the next admission. Always
        non-negative; 0.0 when the client is under cap.
    """
    if len(timestamps) < rpm:
        return 0.0
    # Deque entries are appended chronologically, so the leftmost is
    # the oldest. ``timestamps[0]`` is O(1) on ``collections.deque``.
    oldest = timestamps[0]
    return max(0.0, oldest + _RATE_LIMIT_WINDOW_SECONDS - now)


def redact_token(token: str) -> str:
    """Return a log-safe rendering of a bearer token.

    Never log the full token — operators tail webapp logs at varying
    privilege levels and a leaked token grants the same access as the
    real one. We surface only the leading ``_TOKEN_REDACT_PREFIX_LEN``
    characters plus an ellipsis so per-tenant audits can still spot
    which client was hit while keeping the secret unrecoverable.

    Parameters
    ----------
    token:
        Raw bearer token string (or empty string when no Authorization
        header was supplied).

    Returns
    -------
    str
        ``"<empty>"`` for an empty / missing / too-short token,
        otherwise ``"<first-4>..."``.
    """
    if not token or len(token) < _TOKEN_REDACT_PREFIX_LEN:
        # Tokens shorter than the prefix length are either misconfigured
        # or empty; in either case the redaction would expose the entire
        # value, so fall back to the sentinel. This preserves the
        # invariant "redact_token NEVER returns the full token".
        return '<empty>'
    return f'{token[:_TOKEN_REDACT_PREFIX_LEN]}...'
