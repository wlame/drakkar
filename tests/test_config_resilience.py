"""The resilience block: defaults and the cross-field check."""

import pytest
from pydantic import ValidationError

from drakkar.config.resilience import BreakerConfig, ResilienceConfig, validate_resilience


def test_defaults_match_the_spec():
    cfg = ResilienceConfig()

    assert (cfg.timeout_seconds, cfg.retries, cfg.backoff_seconds, cfg.backoff_max_seconds) == (5.0, 2, 0.2, 5.0)
    assert isinstance(cfg.breaker, BreakerConfig)
    assert (cfg.breaker.failure_threshold, cfg.breaker.cooldown_seconds) == (5, 30.0)


def test_backoff_max_below_base_names_both_paths():
    cfg = ResilienceConfig(backoff_max_seconds=0.1)

    with pytest.raises(
        ValueError,
        match=r'x\.resilience\.backoff_max_seconds \(0\.1\) must be >= x\.resilience\.backoff_seconds \(0\.2\)',
    ):
        validate_resilience('x.resilience', cfg)


def test_backoff_max_equal_to_base_is_accepted():
    validate_resilience('x.resilience', ResilienceConfig(backoff_seconds=0.5, backoff_max_seconds=0.5))


@pytest.mark.parametrize(
    'overrides', [{'failure_threshold': 0}, {'cooldown_seconds': -1.0}], ids=['threshold', 'cooldown']
)
def test_breaker_rejects_values_out_of_range(overrides):
    with pytest.raises(ValidationError):
        BreakerConfig(**overrides)
