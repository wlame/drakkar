"""The ``api`` section: the worker's one HTTP server."""

from pydantic import Field

from drakkar.config.base import ConfigModel


class APIConfig(ConfigModel):
    """The HTTP server of the worker: its address, and the switch that turns it off.

    The server always answers ``/healthz``, ``/readyz`` and the peer route.
    The ``ui`` section adds every other route to the same listener.
    """

    enabled: bool = Field(
        default=True,
        description=(
            'Run the HTTP server: /healthz, /readyz and the peer route. Off means no listener at all, so nothing '
            'answers the Kubernetes probes, and ui.enabled and autodiscover.enabled must both be false.'
        ),
    )
    host: str = Field(
        default='127.0.0.1',
        description='Bind address for the HTTP server. Use 0.0.0.0 to expose on all interfaces.',
    )
    port: int = Field(default=8080, ge=1, le=65535, description='Port for the HTTP server.')
    public_url: str = Field(
        default='',
        description=(
            "Externally reachable URL of this worker's HTTP server. Peers and browsers reach the worker through it, "
            'and the registry card carries it. Empty derives http://<outbound ip>:<port>/.'
        ),
    )
