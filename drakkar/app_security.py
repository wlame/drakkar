"""Startup-time security warnings for ``DrakkarApp``.

A small leaf module so the security-policy text and the structured
warning shape can be exercised without importing the full app.
"""

from __future__ import annotations

from typing import NamedTuple

import structlog

from drakkar.config import DrakkarConfig

logger = structlog.get_logger()


class SideEffectingEndpoint(NamedTuple):
    """A UI endpoint that is not read-only, and the switch that closes it."""

    route: str
    field: str  # attribute on UIConfig
    effect: str  # what it does, phrased to follow the route name

    @property
    def config_key(self) -> str:
        return f'ui.{self.field}'


# The UI endpoints that are NOT read-only. Kept as data so the warning
# text, the docs, and the route gates all enumerate the same set — adding
# a side-effecting endpoint means adding a row here, not editing prose in
# three places and forgetting the fourth. That is exactly how the old
# "the UI is read-only" claim drifted out of true.
SIDE_EFFECTING_ENDPOINTS: tuple[SideEffectingEndpoint, ...] = (
    SideEffectingEndpoint(
        route='POST /api/debug/probe',
        field='probe_enabled',
        effect=(
            'runs caller-supplied bytes through the live handler and competes '
            'with production traffic for executor slots'
        ),
    ),
    SideEffectingEndpoint(
        route='POST /api/debug/merge',
        field='merge_enabled',
        effect='writes a new merged-<ts>.db into ui.recorder.db_dir that nothing reclaims',
    ),
)


def _exposure_clause(config: DrakkarConfig) -> str:
    """Describe the side-effecting endpoints that are currently reachable.

    Only endpoints actually left enabled are named, so an operator who has
    already closed them is not warned about exposure they do not have.
    """
    live = [e for e in SIDE_EFFECTING_ENDPOINTS if getattr(config.ui, e.field)]
    if not live:
        return (
            'Every endpoint still served is read-only (queries, downloads, and the '
            'live event stream); the side-effecting endpoints are disabled by config.'
        )
    effects = '; '.join(f'{e.route} {e.effect}' for e in live)
    keys = ', '.join(f'{e.config_key}=false' for e in live)
    return (
        f'Most endpoints are read-only, but not all: {effects}. Set {keys} to close them independently of auth_token.'
    )


def warn_if_ui_unauthenticated(config: DrakkarConfig) -> None:
    """Emit a startup warning when the UI is enabled without an ``auth_token``.

    Auth is opt-in by design: Drakkar is intended to run inside a private
    contour (VPC, internal cluster network, operator-only ingress), and no
    endpoint stops a worker, replays Kafka messages, mutates sinks, or
    commits offsets. That makes "unauthenticated by default" a reasonable
    starting point — operators who need a token opt in via ``ui.auth_token``.

    What it does **not** make the UI is read-only. Two endpoints have real
    side effects (see :data:`SIDE_EFFECTING_ENDPOINTS`), so the warning
    names whichever of them is still enabled rather than claiming, as it
    used to, that no endpoint can affect the worker.

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
            f'the port. {_exposure_clause(config)} Drakkar is intended for '
            'private-network deployments. To enable bearer-token auth, set '
            'ui.auth_token in your YAML config or export '
            'DK_UI__AUTH_TOKEN=<32+ char random value>; the '
            'WebSocket stream then also validates Origin against '
            'ui.allowed_ws_origins (or the request Host header).'
        ),
    )
