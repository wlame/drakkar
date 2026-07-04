"""Startup-time security warnings for ``DrakkarApp``.

A small leaf module so the security-policy text and the structured
warning shape can be exercised without importing the full app.
"""

from __future__ import annotations

import structlog

from drakkar.config import DrakkarConfig

logger = structlog.get_logger()


def warn_if_ui_unauthenticated(config: DrakkarConfig) -> None:
    """Emit a startup warning when the UI is enabled without an ``auth_token``.

    Auth is opt-in by design: the UI is read-only (no endpoint stops a
    worker, replays Kafka messages, mutates sinks, or fakes pipeline data),
    and Drakkar is intended to run inside a private contour (VPC, internal
    cluster network, operator-only ingress). Combined, that makes
    "unauthenticated by default" a reasonable starting point — operators
    who need a token can opt in by setting ``ui.auth_token``.

    The warning fires once at startup whenever ``ui.enabled`` is True and
    ``ui.auth_token`` is empty (the field validator on ``UIConfig``
    already strips whitespace, so this is a plain emptiness check). It is
    informational only — the worker continues starting normally so a missing
    token never blocks deployment in environments where auth genuinely is
    not required.

    To enable token-based auth, either set ``ui.auth_token`` in your
    YAML config or export ``DK_UI__AUTH_TOKEN=<32+ char value>``.
    Once set, the WebSocket live-event stream additionally validates the
    ``Origin`` header (against ``ui.allowed_ws_origins`` when configured,
    otherwise against the request's ``Host`` header).
    """
    if not config.ui.enabled:
        return
    if config.ui.auth_token != '':
        return

    logger.warning(
        'ui_unauthenticated',
        category='lifecycle',
        host=config.ui.host,
        port=config.ui.port,
        message=(
            f'The operator UI bound to {config.ui.host}:{config.ui.port} is running '
            'without auth_token — every endpoint (including the database download, '
            'merge, and message-probe routes) is reachable to anyone who can reach '
            'the port. The UI is read-only by design (cannot stop workers, replay '
            'messages, or modify state) and intended for private-network deployments. '
            'To enable bearer-token auth, set ui.auth_token in your YAML config '
            'or export DK_UI__AUTH_TOKEN=<32+ char random value>; the '
            'WebSocket stream then also validates Origin against '
            'ui.allowed_ws_origins (or the request Host header).'
        ),
    )
