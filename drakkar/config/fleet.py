"""How workers find each other and exchange cache rows.

``fleet.registry`` selects the provider of worker cards (who is in the
fleet, where each worker answers, whether it is online). ``fleet.cache_exchange``
tunes how a worker pulls ``cluster`` and ``global`` cache rows from its
peers. Neither is needed for the pipeline itself: sources, executor, sinks
and offset commits work the same with both switched off.
"""

from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator

from drakkar.config.resilience import ResilienceConfig, validate_resilience

# Providers that reach a shared service through a named entry of ``connections``.
# The tuple is the set of keys of ``ConnectionsConfig``; a plugin provider
# manages its own connections.
CONNECTION_PROVIDERS = ('consul', 'redis', 'postgres')

# The smallest session TTL Consul accepts. The floor applies to every
# provider, so a config written for one provider stays valid for the others.
MIN_OFFLINE_AFTER_SECONDS = 10


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


class ConsulRegistryConfig(BaseModel):
    """Settings of the ``consul`` registry provider."""

    mode: Literal['publish', 'catalog'] = Field(
        default='publish',
        description=(
            'publish: every worker stores its own card in Consul under a session and reads the cards of '
            'its peers. catalog: workers are listed from the Consul service catalog and this worker '
            'publishes nothing.'
        ),
    )
    service_name: str = Field(
        default='drakkar',
        min_length=1,
        description='Name of the Consul service that lists the workers. Used only when mode is catalog.',
    )


class RedisRegistryConfig(BaseModel):
    """Settings of the ``redis`` registry provider; none yet.

    The key prefix comes from the Redis connection named by ``fleet.registry.connection``.
    """


class PostgresRegistryConfig(BaseModel):
    """Settings of the ``postgres`` registry provider; none yet.

    The schema comes from the Postgres connection named by ``fleet.registry.connection``.
    """


class FleetRegistryConfig(BaseModel):
    """The registry of worker cards."""

    provider: str = Field(
        default='filesystem',
        min_length=1,
        description=(
            'Registry provider: filesystem (cards in a shared folder), static (a fixed peer list), '
            'none (no peers), postgres (cards in a table of the database named by '
            'fleet.registry.connection), redis (cards in keys of the Redis server named by '
            'fleet.registry.connection), or the name of a provider plugin. The name consul is '
            'reserved for a provider of a later release; this release does not ship it. '
            'Needs ui.enabled.'
        ),
    )
    connection: str = Field(
        default='main',
        min_length=1,
        description=(
            'Name of the entry under connections.<provider> that the postgres and redis providers use, '
            'and that the consul provider will use. The other providers ignore it.'
        ),
    )
    heartbeat_seconds: int = Field(default=10, ge=1, description='Seconds between card publishes.')
    refresh_seconds: int = Field(default=15, ge=1, description='Seconds between reads of the peer cards.')
    offline_after_seconds: int = Field(
        default=60,
        ge=1,
        description=(
            'A peer whose card is older than this many seconds (on the provider clock) is offline. '
            f'Must be >= 2 * heartbeat_seconds and >= {MIN_OFFLINE_AFTER_SECONDS}.'
        ),
    )
    prune_after_seconds: int = Field(
        default=86_400,
        ge=1,
        description='Cards older than this many seconds are deleted. Must be >= offline_after_seconds.',
    )
    resilience: ResilienceConfig = Field(
        default_factory=ResilienceConfig,
        description='Time limit, retries and circuit breaker for the calls to the registry provider.',
    )
    filesystem: FilesystemRegistryConfig = Field(default_factory=FilesystemRegistryConfig)
    static: StaticRegistryConfig = Field(default_factory=StaticRegistryConfig)
    consul: ConsulRegistryConfig = Field(
        default_factory=ConsulRegistryConfig,
        description='Settings of the consul provider.',
    )
    redis: RedisRegistryConfig = Field(
        default_factory=RedisRegistryConfig,
        description='Settings of the redis provider; none yet. The key prefix comes from the connection.',
    )
    postgres: PostgresRegistryConfig = Field(
        default_factory=PostgresRegistryConfig,
        description='Settings of the postgres provider; none yet. The schema and the pool come from the connection.',
    )
    plugin_options: dict[str, Any] = Field(
        default_factory=dict,
        description='Options passed unchanged to a provider plugin, which validates them itself.',
    )

    @model_validator(mode='after')
    def _validate_timing(self) -> 'FleetRegistryConfig':
        """Offline needs two missed heartbeats and the shared floor; pruning must not delete an online card."""
        if self.offline_after_seconds < MIN_OFFLINE_AFTER_SECONDS:
            raise ValueError(
                f'fleet.registry.offline_after_seconds ({self.offline_after_seconds}) '
                f'must be >= {MIN_OFFLINE_AFTER_SECONDS}'
            )
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

    @model_validator(mode='after')
    def _validate_resilience(self) -> 'FleetRegistryConfig':
        """Check the rules of the resilience block that span fields, naming it by its YAML path."""
        validate_resilience('fleet.registry.resilience', self.resilience)
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
