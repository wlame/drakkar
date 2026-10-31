"""The resilience block: defaults, units and the cross-field check."""

import pytest
from pydantic import ValidationError

from drakkar.config.resilience import ResilienceConfig, validate_resilience


def test_defaults_match_the_spec():
    cfg = ResilienceConfig()

    assert (cfg.timeout, cfg.max_retries, cfg.backoff, cfg.max_backoff) == (5.0, 2, 0.2, 5.0)
    assert (cfg.breaker_failures, cfg.breaker_cooldown) == (5, 30.0)


def test_max_backoff_below_backoff_names_both_paths():
    cfg = ResilienceConfig(max_backoff='100ms')

    with pytest.raises(
        ValueError,
        match=r'x\.resilience\.max_backoff \(100ms\) must be >= x\.resilience\.backoff \(200ms\)',
    ):
        validate_resilience('x.resilience', cfg)


def test_max_backoff_equal_to_backoff_is_accepted():
    validate_resilience('x.resilience', ResilienceConfig(backoff='500ms', max_backoff='500ms'))


@pytest.mark.parametrize(
    'overrides',
    [{'breaker_failures': 0}, {'breaker_cooldown': '-1s'}, {'timeout': 5}, {'max_retries': -1}],
    ids=['failures', 'cooldown', 'timeout_no_unit', 'retries'],
)
def test_resilience_rejects_values_out_of_range(overrides):
    with pytest.raises(ValidationError):
        ResilienceConfig(**overrides)
