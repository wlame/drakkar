"""Format helpers + WebSocket origin validation + handler-hook detection.

Pure functions used by the debug server's route handlers. Splitting them
out keeps the FastAPI factory in :mod:`drakkar.debug_server` focused on
route definitions and lifecycle, while the leaf logic that turns a
timestamp into ``HH:MM:SS`` or decides whether a WebSocket Origin matches
the configured allowlist lives here in isolation (and is straightforward
to test without spinning up a FastAPI app).

The request-body Pydantic models (``_ArrangeTaskLookupRequest``,
``_SinkBreakdownRequest``, ``_ProbeRequest``) deliberately stay inside
:mod:`drakkar.debug_server` — FastAPI's "single Pydantic param = request
body" heuristic only fires when the model is defined in the same module
as the endpoint function. An imported model is treated as a query
parameter and surfaces as a 422 error at runtime.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from urllib.parse import urlparse

from drakkar.config import DebugConfig

# Default ports are implicit in browsers' Origin header but may appear
# explicitly in the Host header (and vice-versa). We strip both sides to
# the same canonical form before comparing so a legitimate same-origin
# request never trips the check on a ``:80``/``:443`` mismatch.
_DEFAULT_PORTS = {'http': 80, 'https': 443, 'ws': 80, 'wss': 443}


def _hook_flags(handler: object) -> dict[str, bool]:
    """Detect which completion hooks the user's handler overrides.

    The Live view uses this to render only the tabs that actually have
    semantic meaning for this handler — an empty "Window Results" tab on
    a handler that never implements ``on_window_complete`` is just noise.

    We compare bound-method identity against the base class's no-op
    implementation. A subclass that overrides the hook has a different
    function object reachable via ``type(handler).on_*``; one that
    inherits the default shares the same object. This works for:

      * standard subclasses of BaseDrakkarHandler (common case)
      * handlers that implement DrakkarHandler directly without
        subclassing — their on_* methods aren't the base class's, so
        they also register as "implemented"

    Handlers that use composition or decorators that wrap the method
    will still register as "implemented" — we err on the side of showing
    the tab rather than hiding it.
    """
    # Import here to avoid a circular import; handler imports debug_server
    # only via app.py's wiring, but the other direction is live.
    from drakkar.handler import BaseDrakkarHandler

    cls = type(handler)
    # ``getattr`` on the class (not the instance) so we compare unbound
    # function objects — bound methods would wrap with a different id
    # per-instance and break the identity check. A handler that somehow
    # lacks one of these attributes (shouldn't happen given Protocol
    # conformance) is treated as "not implemented".
    return {
        'task_complete': getattr(cls, 'on_task_complete', None) is not BaseDrakkarHandler.on_task_complete,
        'message_complete': getattr(cls, 'on_message_complete', None) is not BaseDrakkarHandler.on_message_complete,
        'window_complete': getattr(cls, 'on_window_complete', None) is not BaseDrakkarHandler.on_window_complete,
    }


def _normalize_hostport(scheme: str, host: str, port: int | None) -> tuple[str, int | None]:
    """Lowercase host and drop default ports for a given scheme.

    Returns ``(host_lower, effective_port)`` where effective_port is ``None``
    when the supplied port is the scheme default (e.g. 80 for http, 443 for
    https). An explicit non-default port is preserved so the caller can
    compare port-qualified origins correctly.
    """
    host_lower = host.lower()
    default = _DEFAULT_PORTS.get(scheme.lower())
    if port is None or (default is not None and port == default):
        return host_lower, None
    return host_lower, port


def _parse_host_header(host_header: str) -> tuple[str, int | None]:
    """Split a Host header into ``(host, port)`` with IPv6-bracket support.

    RFC 7230 allows ``Host: [::1]:8080`` for IPv6 addresses. Using
    ``urlparse`` on a raw Host header doesn't work (no scheme); we parse
    manually here. Non-numeric ports fall back to ``None`` — the caller
    will then compare ports as ``None`` which is the most lenient choice.
    """
    host_header = host_header.strip()
    if not host_header:
        return '', None
    # IPv6 literal: ``[::1]`` or ``[::1]:8080``
    if host_header.startswith('['):
        end = host_header.find(']')
        if end == -1:
            # malformed — treat the whole thing as a host
            return host_header.lower(), None
        host = host_header[1:end]
        rest = host_header[end + 1 :]
        if rest.startswith(':'):
            try:
                return host.lower(), int(rest[1:])
            except ValueError:
                return host.lower(), None
        return host.lower(), None
    # IPv4 or hostname: ``example.com`` or ``example.com:8080``
    if ':' in host_header:
        host, _, port_str = host_header.rpartition(':')
        try:
            return host.lower(), int(port_str)
        except ValueError:
            return host_header.lower(), None
    return host_header.lower(), None


def _origin_allowed(origin: str | None, host_header: str, config: DebugConfig) -> bool:
    """Return True when a WebSocket handshake origin is allowed.

    Decision table (only consulted when ``auth_token`` is set — caller
    enforces that guard):

    1. ``origin is None``  → accept. Non-browser clients (curl, websocat,
       Python websockets lib) typically omit ``Origin``; they already
       authenticated via token. We err on the side of letting tooling
       connect rather than pretending a missing Origin is hostile.
    2. ``origin is not None`` + ``allowed_ws_origins`` non-empty → accept
       only if ``origin`` is in the allowlist. Case-insensitive compare
       on the parsed scheme://host:port form so trivial casing differences
       don't reject legitimate browsers.
    3. ``origin is not None`` + ``allowed_ws_origins`` empty → same-origin
       fallback. Parse scheme/host/port from ``origin`` and compare to
       the Host header after normalizing case and default ports.

    Returns False on any malformed origin — the WS endpoint will close
    with 4403 in that case.
    """
    if origin is None:
        # Case 1: absent Origin. Non-browser client — token already checked.
        return True

    # Case 2: explicit allowlist. Normalize both sides (case + default
    # ports) so operators don't have to match casing letter-for-letter.
    if config.allowed_ws_origins:
        try:
            parsed_origin = urlparse(origin)
        except ValueError:
            return False
        origin_scheme = parsed_origin.scheme.lower()
        origin_host_norm, origin_port_norm = _normalize_hostport(
            origin_scheme,
            parsed_origin.hostname or '',
            parsed_origin.port,
        )
        for allowed in config.allowed_ws_origins:
            try:
                parsed_allowed = urlparse(allowed)
            except ValueError:
                continue
            allowed_scheme = parsed_allowed.scheme.lower()
            allowed_host_norm, allowed_port_norm = _normalize_hostport(
                allowed_scheme,
                parsed_allowed.hostname or '',
                parsed_allowed.port,
            )
            if (
                origin_scheme == allowed_scheme
                and origin_host_norm == allowed_host_norm
                and origin_port_norm == allowed_port_norm
            ):
                return True
        return False

    # Case 3: same-origin fallback. Compare host/port pairs; scheme is
    # skipped because browsers sometimes upgrade ws→wss through a proxy
    # and strict scheme matching would reject legitimate flows.
    try:
        parsed_origin = urlparse(origin)
    except ValueError:
        return False
    origin_host_norm, origin_port_norm = _normalize_hostport(
        parsed_origin.scheme.lower(),
        parsed_origin.hostname or '',
        parsed_origin.port,
    )
    host_host, host_port = _parse_host_header(host_header)
    # Compute the effective port for the Host header. We don't know the
    # scheme (Host header carries no scheme), so treat a missing port as
    # "default for scheme" based on the Origin's scheme — this matches
    # the common browser behavior of stripping default ports from Origin
    # while leaving them implicit in Host.
    _, host_port_norm = _normalize_hostport(
        parsed_origin.scheme.lower() or 'http',
        host_host,
        host_port,
    )
    if not origin_host_norm or not host_host:
        return False
    return origin_host_norm == host_host and origin_port_norm == host_port_norm


def _format_ts(ts: float | None) -> str:
    """Format a Unix timestamp as ``HH:MM:SS`` (UTC). Empty string for None."""
    if ts is None:
        return ''
    return datetime.fromtimestamp(ts, tz=UTC).strftime('%H:%M:%S')


def _format_ts_ms(ts: float | None) -> str:
    """Format a Unix timestamp as ``HH:MM:SS.mmm`` (UTC). Empty string for None."""
    if ts is None:
        return ''
    return datetime.fromtimestamp(ts, tz=UTC).strftime('%H:%M:%S.%f')[:-3]


def _format_ts_full(ts: float | None) -> str:
    """Format a Unix timestamp as ``YYYY-MM-DD HH:MM:SS.mmm`` (UTC). Empty for None."""
    if ts is None:
        return ''
    return datetime.fromtimestamp(ts, tz=UTC).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]


def _format_uptime(seconds: float) -> str:
    """Format uptime as a human-readable string, scaling to the largest unit."""
    s = int(seconds)
    if s < 60:
        return f'{s}s'
    if s < 3600:
        return f'{s // 60}m {s % 60}s'
    if s < 86400:
        h = s // 3600
        m = (s % 3600) // 60
        return f'{h}h {m}m'
    if s < 86400 * 30:
        d = s // 86400
        h = (s % 86400) // 3600
        return f'{d}d {h}h'
    if s < 86400 * 365:
        d = s // 86400
        mo = d // 30
        d_rem = d % 30
        return f'{mo}mo {d_rem}d'
    d = s // 86400
    y = d // 365
    d_rem = d % 365
    return f'{y}y {d_rem // 30}mo'


def _worker_group(name: str) -> str:
    """Derive a group key by stripping trailing numbers and separator.

    ``worker-1``  → ``worker``
    ``worker-vip-2`` → ``worker-vip``
    ``slow-worker-05`` → ``slow-worker``
    ``worker15`` → ``worker``
    """
    return re.sub(r'[-_]?\d+$', '', name) or name
