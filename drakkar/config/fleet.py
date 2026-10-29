"""How workers find each other and exchange cache rows.

``fleet.registry`` selects the provider of worker cards (who is in the
fleet, where each worker answers, whether it is online). ``fleet.cache_exchange``
tunes how a worker pulls ``cluster`` and ``global`` cache rows from its
peers. Neither is needed for the pipeline itself: sources, executor, sinks
and offset commits work the same with both switched off.
"""

from pydantic import Field, model_validator

from drakkar.config.base import ConfigModel
from drakkar.config.resilience import ResilienceConfig, validate_resilience

# Providers that reach a shared service through a named entry of ``connections``.
# The tuple is the set of keys of ``ConnectionsConfig``.
CONNECTION_PROVIDERS = ('consul', 'redis', 'postgres')

# The floor of offline_after_seconds for every provider, not for one: a config written for one provider
# stays valid for the others. The consul registry's session TTL is derived from the setting (half the
# limit, at least 10 s), but the floor is what keeps the setting portable.
MIN_OFFLINE_AFTER_SECONDS = 10

# The consul registry sets the TTL of a card's session to half of offline_after_seconds, and Consul
# accepts a TTL of 24 hours at most. One day is the documented limit of the setting.
CONSUL_MAX_OFFLINE_AFTER_SECONDS = 86_400


class FleetRegistryConfig(ConfigModel):
    """The registry of worker cards."""

    provider: str = Field(
        default='filesystem',
        min_length=1,
        description=(
            'Registry provider: filesystem (cards in <storage.shared_dir>/fleet, or <storage.dir>/fleet when '
            'storage.shared_dir is empty), postgres (cards in a table of the database named by '
            'fleet.registry.connection), redis (cards in keys of the Redis server named by '
            'fleet.registry.connection), consul (cards in keys of the Consul agent named by '
            'fleet.registry.connection, each held by a session of its worker). Needs ui.enabled.'
        ),
    )
    connection: str = Field(
        default='main',
        min_length=1,
        description=(
            'Name of the entry under connections.<provider> that the consul, postgres and redis '
            'providers use. The other providers ignore it.'
        ),
    )
    heartbeat_seconds: int = Field(default=10, ge=1, description='Seconds between card publishes.')
    refresh_seconds: int = Field(default=15, ge=1, description='Seconds between reads of the peer cards.')
    offline_after_seconds: int = Field(
        default=60,
        ge=1,
        description=(
            'A peer whose card is older than this many seconds (on the provider clock) is offline. '
            f'Must be >= 2 * heartbeat_seconds and >= {MIN_OFFLINE_AFTER_SECONDS}. '
            f'With the consul provider it must also be <= {CONSUL_MAX_OFFLINE_AFTER_SECONDS}.'
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
    def _validate_consul(self) -> 'FleetRegistryConfig':
        """The consul registry's card session lives at most a day."""
        if self.provider != 'consul':
            return self
        if self.offline_after_seconds > CONSUL_MAX_OFFLINE_AFTER_SECONDS:
            raise ValueError(
                f'fleet.registry.offline_after_seconds ({self.offline_after_seconds}) must be <= '
                f'{CONSUL_MAX_OFFLINE_AFTER_SECONDS} when fleet.registry.provider is consul'
            )
        return self

    @model_validator(mode='after')
    def _validate_resilience(self) -> 'FleetRegistryConfig':
        """Check the rules of the resilience block that span fields, naming it by its YAML path."""
        validate_resilience('fleet.registry.resilience', self.resilience)
        return self


class CacheExchangeConfig(ConfigModel):
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


class FleetConfig(ConfigModel):
    """Worker registry and cache exchange."""

    registry: FleetRegistryConfig = Field(default_factory=FleetRegistryConfig)
    cache_exchange: CacheExchangeConfig = Field(default_factory=CacheExchangeConfig)
