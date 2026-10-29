"""Timeout, retry and circuit-breaker settings for calls to an external provider.

One model serves every provider-backed feature, so an operator tunes them
all the same way. The breaker counts failed operations, not attempts.
"""

from pydantic import Field

from drakkar.config.base import ConfigModel
from drakkar.config.units import Duration, format_duration

_MILLISECONDS_PER_SECOND = 1000


class ResilienceConfig(ConfigModel):
    """How one provider is called: attempt time limit, retries with backoff, circuit breaker."""

    timeout: Duration = Field(
        default=5.0,
        gt=0,
        description='Time limit for one attempt of a provider operation. Retries get their own limit each.',
    )
    max_retries: int = Field(
        default=2,
        ge=0,
        description=(
            'Extra attempts after the first one, made only for a transient error (a timeout, a connection '
            'or other OS error). Any other error ends the operation at once. 0 = a single attempt.'
        ),
    )
    backoff: Duration = Field(
        default=0.2,
        ge=0.0,
        description='Wait before the first retry. The wait doubles for each further retry, up to max_backoff.',
    )
    max_backoff: Duration = Field(
        default=5.0,
        ge=0.0,
        description='Longest wait between two attempts. Must be >= backoff.',
    )
    breaker_failures: int = Field(
        default=5,
        ge=1,
        description=(
            'Consecutive failed operations before the breaker opens; while open, operations are skipped without a call.'
        ),
    )
    breaker_cooldown: Duration = Field(
        default=30.0,
        ge=0.0,
        description=(
            'Time the breaker stays open before one probe operation is allowed through; success closes it, '
            'failure reopens it with a fresh cooldown.'
        ),
    )


def _duration_text(seconds: float) -> str:
    return format_duration(round(seconds * _MILLISECONDS_PER_SECOND))


def validate_resilience(path: str, cfg: ResilienceConfig) -> None:
    """Check the rules that span fields; ``path`` is the config path of the block, for the message.

    This is a function and not a model validator because the message must name
    the block where the operator wrote it, and one model is embedded in several
    places of the config.
    """
    if cfg.max_backoff < cfg.backoff:
        raise ValueError(
            f'{path}.max_backoff ({_duration_text(cfg.max_backoff)}) must be >= '
            f'{path}.backoff ({_duration_text(cfg.backoff)})'
        )
