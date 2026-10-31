"""How workers find each other and elect the one that runs a cluster task.

One provider serves both jobs: the registry of worker cards (who is in the
fleet, where each worker answers, whether it is online) and the election of
cluster periodic tasks. The feature is optional: sources, executor, sinks and
offset commits work the same without it.
"""

from pydantic import Field, model_validator

from drakkar.config.base import ConfigModel
from drakkar.config.resilience import ResilienceConfig, validate_resilience
from drakkar.config.units import WholeSeconds, seconds_text

# The name of the provider that keeps its data in ``storage.shared_dir``. Every
# reader of the name takes it from here.
FILESYSTEM_PROVIDER = 'filesystem'

# The providers, sorted: the order the two messages print them in.
AUTODISCOVER_PROVIDERS = ('consul', FILESYSTEM_PROVIDER, 'postgres', 'redis')

# Providers that reach a shared service through a named entry of ``connections``.
# Each is a key of ``ConnectionsConfig``.
CONNECTION_PROVIDERS = ('consul', 'redis', 'postgres')

# The floor of ``offline_after`` for every provider, not for one: a config written for one provider
# stays valid for the others. The consul registry's session TTL is derived from the setting (half the
# limit, at least 10 s), but the floor is what keeps the setting portable.
MIN_OFFLINE_AFTER_SECONDS = 10

# The consul registry sets the TTL of a card's session to half of ``offline_after``, and Consul
# accepts a TTL of 24 hours at most. One day is the documented limit of the setting.
CONSUL_MAX_OFFLINE_AFTER_SECONDS = 86_400


class AutodiscoverConfig(ConfigModel):
    """The one provider behind the worker registry and the election of cluster tasks."""

    enabled: bool = Field(
        default=False,
        description=(
            'Publish this worker in the registry, read its peers and take part in the election of cluster '
            'tasks. Off, the workers list shows only this worker, the cache exchange pulls nothing, and a '
            'declared cluster task fails startup.'
        ),
    )
    provider: str = Field(
        default='',
        description=(
            'The provider of both the registry and the election, required when enabled: filesystem '
            '(cards in <storage.shared_dir>/fleet/workers, locks in <storage.shared_dir>/tasks/<cluster>; '
            'needs storage.shared_dir), postgres (tables of the database named by autodiscover.connection), '
            'redis (keys of the Redis server named by autodiscover.connection), consul (keys of the Consul '
            'agent named by autodiscover.connection, each held by a session). Every worker of a cluster must '
            'use the same provider.'
        ),
    )
    connection: str = Field(
        default='main',
        min_length=1,
        description=(
            'Name of the entry under connections.<provider> that the consul, postgres and redis '
            'providers use. The filesystem provider ignores it.'
        ),
    )
    heartbeat: WholeSeconds = Field(default=10, description='Time between card publishes.')
    refresh: WholeSeconds = Field(default=15, description='Time between reads of the peer cards.')
    offline_after: WholeSeconds = Field(
        default=60,
        description=(
            'A peer whose card is older than this (on the provider clock) is offline. '
            f'Must be at least 2 x heartbeat and at least {seconds_text(MIN_OFFLINE_AFTER_SECONDS)}. '
            f'With the consul provider it must also be at most {seconds_text(CONSUL_MAX_OFFLINE_AFTER_SECONDS)}.'
        ),
    )
    prune_after: WholeSeconds = Field(
        default=86_400,
        description='Cards older than this are deleted. Must be at least offline_after.',
    )
    resilience: ResilienceConfig = Field(
        default_factory=ResilienceConfig,
        description=(
            'Time limit, retries and circuit breaker for the calls to the provider. The registry and the '
            'election each build their own caller from this block, so each has its own breaker.'
        ),
    )

    def check_provider(self) -> None:
        """Refuse a provider name outside the four, and an enabled feature with none.

        A public method because a handler can change these keys in ``on_startup``,
        and the startup then gives the message of the load.
        """
        if self.provider and self.provider not in AUTODISCOVER_PROVIDERS:
            raise ValueError(
                f"autodiscover.provider must be one of {', '.join(AUTODISCOVER_PROVIDERS)}, got '{self.provider}'"
            )
        if self.enabled and not self.provider:
            raise ValueError(
                'autodiscover.enabled is true but autodiscover.provider is empty; '
                f'choose one of {", ".join(AUTODISCOVER_PROVIDERS)}'
            )

    @model_validator(mode='after')
    def _validate_provider(self) -> 'AutodiscoverConfig':
        """The provider name, ahead of the checks that depend on it."""
        self.check_provider()
        return self

    @model_validator(mode='after')
    def _validate_timing(self) -> 'AutodiscoverConfig':
        """Offline needs two missed heartbeats and the shared floor; pruning must not delete an online card."""
        offline_text = f'autodiscover.offline_after ({seconds_text(self.offline_after)})'
        if self.offline_after < MIN_OFFLINE_AFTER_SECONDS:
            raise ValueError(f'{offline_text} must be >= {seconds_text(MIN_OFFLINE_AFTER_SECONDS)}')
        if self.offline_after < 2 * self.heartbeat:
            raise ValueError(f'{offline_text} must be >= 2 * autodiscover.heartbeat ({seconds_text(self.heartbeat)})')
        if self.prune_after < self.offline_after:
            raise ValueError(f'autodiscover.prune_after ({seconds_text(self.prune_after)}) must be >= {offline_text}')
        return self

    @model_validator(mode='after')
    def _validate_consul(self) -> 'AutodiscoverConfig':
        """The consul registry's card session lives at most a day."""
        if self.provider == 'consul' and self.offline_after > CONSUL_MAX_OFFLINE_AFTER_SECONDS:
            raise ValueError(
                f'autodiscover.offline_after ({seconds_text(self.offline_after)}) must be <= '
                f'{seconds_text(CONSUL_MAX_OFFLINE_AFTER_SECONDS)} when autodiscover.provider is consul'
            )
        return self

    @model_validator(mode='after')
    def _validate_resilience(self) -> 'AutodiscoverConfig':
        """Check the rules of the resilience block that span fields, naming it by its YAML path."""
        validate_resilience('autodiscover.resilience', self.resilience)
        return self
