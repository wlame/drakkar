"""Timeout, retry and circuit-breaker settings for calls to an external provider.

One model serves every provider-backed feature, so an operator tunes them
all the same way. The breaker has the same two fields as the sinks' circuit
breaker, but it counts failed operations and is described for providers, so
it is a model of its own.
"""

from pydantic import Field

from drakkar.config.base import ConfigModel


class BreakerConfig(ConfigModel):
    """Circuit breaker of one provider: when it opens and how long it stays open."""

    failure_threshold: int = Field(
        default=5,
        ge=1,
        description=(
            'Consecutive failed operations before the breaker opens; while open, operations are skipped without a call.'
        ),
    )
    cooldown_seconds: float = Field(
        default=30.0,
        ge=0.0,
        description=(
            'Time the breaker stays open before one probe operation is allowed through; success closes it, '
            'failure reopens it with a fresh cooldown.'
        ),
    )


class ResilienceConfig(ConfigModel):
    """How one provider is called: attempt time limit, retries with backoff, circuit breaker."""

    timeout_seconds: float = Field(
        default=5.0,
        gt=0,
        description='Time limit for one attempt of a provider operation. Retries get their own limit each.',
    )
    retries: int = Field(
        default=2,
        ge=0,
        description=(
            'Extra attempts after the first one, made only for a transient error (a timeout, a connection '
            'or other OS error). Any other error ends the operation at once. 0 = a single attempt.'
        ),
    )
    backoff_seconds: float = Field(
        default=0.2,
        ge=0.0,
        description='Wait before the first retry. The wait doubles for each further retry, up to backoff_max_seconds.',
    )
    backoff_max_seconds: float = Field(
        default=5.0,
        ge=0.0,
        description='Longest wait between two attempts. Must be >= backoff_seconds.',
    )
    breaker: BreakerConfig = Field(
        default_factory=BreakerConfig,
        description=(
            'Circuit breaker of the provider. It counts failed operations, not attempts: after '
            'failure_threshold failed operations in a row the provider is skipped without a call for '
            'cooldown_seconds, then one operation probes it.'
        ),
    )


def validate_resilience(path: str, cfg: ResilienceConfig) -> None:
    """Check the rules that span fields; ``path`` is the config path of the block, for the message.

    This is a function and not a model validator because the message must name
    the block where the operator wrote it, and one model is embedded in several
    places of the config.
    """
    if cfg.backoff_max_seconds < cfg.backoff_seconds:
        raise ValueError(
            f'{path}.backoff_max_seconds ({cfg.backoff_max_seconds:g}) must be >= '
            f'{path}.backoff_seconds ({cfg.backoff_seconds:g})'
        )
