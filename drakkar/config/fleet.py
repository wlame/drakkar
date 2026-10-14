"""How workers find each other and exchange cache rows.

``fleet.registry`` selects the provider of worker cards (who is in the
fleet, where each worker answers, whether it is online). ``fleet.cache_exchange``
tunes how a worker pulls ``cluster`` and ``global`` cache rows from its
peers. Neither is needed for the pipeline itself: sources, executor, sinks
and offset commits work the same with both switched off.
"""

from typing import Any

from pydantic import BaseModel, Field, model_validator


class StaticPeerConfig(BaseModel):
    """One peer of the ``static`` registry provider."""

    name: str = Field(min_length=1, description='Worker name of the peer.')
    cluster: str = Field(default='', description='Cluster name of the peer; empty for none.')
    url: str = Field(min_length=1, description='Base URL of the peer UI server, for example http://10.0.3.17:8080/.')


class StaticRegistryConfig(BaseModel):
    """Settings of the ``static`` registry provider."""

    peers: list[StaticPeerConfig] = Field(
        default_factory=list,
        description='Fixed list of peers. Their liveness is unknown, so the workers list shows them as neither online nor offline.',
    )


class FilesystemRegistryConfig(BaseModel):
    """Settings of the ``filesystem`` registry provider."""

    dir: str = Field(
        default='',
        description=(
            'Folder for the worker cards. Empty means <shared.dir>/fleet (or <storage.dir>/fleet '
            'when shared.dir is empty). A folder set here that cannot be created or written fails startup.'
        ),
    )


class FleetRegistryConfig(BaseModel):
    """The registry of worker cards."""

    provider: str = Field(
        default='filesystem',
        min_length=1,
        description=(
            'Registry provider: filesystem (cards in a shared folder), static (a fixed peer list), '
            'none (no peers), or the name of a provider plugin. Needs ui.enabled.'
        ),
    )
    heartbeat_seconds: int = Field(default=10, ge=1, description='Seconds between card publishes.')
    refresh_seconds: int = Field(default=15, ge=1, description='Seconds between reads of the peer cards.')
    offline_after_seconds: int = Field(
        default=30,
        ge=1,
        description=(
            'A peer whose card is older than this many seconds (on the provider clock) is offline. '
            'Must be >= 2 * heartbeat_seconds.'
        ),
    )
    prune_after_seconds: int = Field(
        default=86_400,
        ge=1,
        description='Cards older than this many seconds are deleted. Must be >= offline_after_seconds.',
    )
    timeout_seconds: float = Field(default=5.0, gt=0, description='Time limit for one registry operation.')
    filesystem: FilesystemRegistryConfig = Field(default_factory=FilesystemRegistryConfig)
    static: StaticRegistryConfig = Field(default_factory=StaticRegistryConfig)
    plugin_options: dict[str, Any] = Field(
        default_factory=dict,
        description='Options passed unchanged to a provider plugin, which validates them itself.',
    )

    @model_validator(mode='after')
    def _validate_timing(self) -> 'FleetRegistryConfig':
        """Offline needs two missed heartbeats; pruning must not delete an online card."""
        if self.offline_after_seconds < 2 * self.heartbeat_seconds:
            raise ValueError(
                f'fleet.registry.offline_after_seconds ({self.offline_after_seconds}) must be >= '
                f'2 * fleet.registry.heartbeat_seconds ({self.heartbeat_seconds})'
            )
        if self.prune_after_seconds < self.offline_after_seconds:
            raise ValueError(
                f'fleet.registry.prune_after_seconds ({self.prune_after_seconds}) must be >= '
                f'fleet.registry.offline_after_seconds ({self.offline_after_seconds})'
            )
        return self


class CacheExchangeConfig(BaseModel):
    """How this worker pulls ``cluster`` and ``global`` cache rows from its peers."""

    enabled: bool = Field(
        default=True,
        description=(
            'Pull cluster and global cache rows from peers. Off, flush and cleanup still run but '
            'nothing propagates across workers.'
        ),
    )
    interval_seconds: float = Field(default=30.0, gt=0, description='Seconds between exchange cycles.')
    page_size: int = Field(default=500, ge=1, le=1000, description='Maximum rows in one page pulled from a peer.')
    timeout_seconds: float = Field(default=5.0, gt=0, description='Time limit for one request to a peer.')
    # ``None`` means 0.9 x interval_seconds, so one cycle always ends before
    # the next one is scheduled. The 0.1 s floor stops a value so small that
    # no cycle could ever finish.
    cycle_deadline_seconds: float | None = Field(
        default=None,
        ge=0.1,
        description='Hard time limit for one exchange cycle. Empty means 0.9 x interval_seconds.',
    )
    max_concurrent_peers: int = Field(default=4, ge=1, description='Peers pulled from in parallel.')

    @model_validator(mode='after')
    def _validate_deadline_vs_interval(self) -> 'CacheExchangeConfig':
        """An explicit deadline must fire before the next cycle is scheduled."""
        if self.cycle_deadline_seconds is not None and self.cycle_deadline_seconds >= self.interval_seconds:
            raise ValueError(
                f'fleet.cache_exchange.cycle_deadline_seconds ({self.cycle_deadline_seconds:g}) must be '
                f'strictly less than fleet.cache_exchange.interval_seconds ({self.interval_seconds:g}) — '
                'otherwise the deadline can never fire before the next cycle schedules.'
            )
        return self


class FleetConfig(BaseModel):
    """Worker registry and cache exchange."""

    registry: FleetRegistryConfig = Field(default_factory=FleetRegistryConfig)
    cache_exchange: CacheExchangeConfig = Field(default_factory=CacheExchangeConfig)
