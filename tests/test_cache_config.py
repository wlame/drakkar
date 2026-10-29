"""Tests for CacheConfig and the cache exchange settings (cache.exchange).

Validates defaults, environment-variable overrides (nested delimiter), field
constraints, and confirms the v1 scope of the config — no preload fields, no
exposed peer-resolution cache TTL.
"""

import pytest
from pydantic import ValidationError

from drakkar.config import (
    CacheConfig,
    CacheExchangeConfig,
    DrakkarConfig,
    ExecutorConfig,
)
from tests.conftest import kafka_connections

# --- CacheExchangeConfig bounds ---


def test_cache_exchange_config_rejects_invalid():
    # interval_seconds must be > 0 (gt=0)
    with pytest.raises(ValidationError):
        CacheExchangeConfig(interval_seconds=0)
    with pytest.raises(ValidationError):
        CacheExchangeConfig(interval_seconds=-1.0)
    # page_size must be in 1..1000
    with pytest.raises(ValidationError):
        CacheExchangeConfig(page_size=0)
    with pytest.raises(ValidationError):
        CacheExchangeConfig(page_size=1001)
    # timeout_seconds must be > 0 (gt=0)
    with pytest.raises(ValidationError):
        CacheExchangeConfig(timeout_seconds=0)


def test_cache_exchange_deadline_below_interval_is_accepted():
    cfg = CacheExchangeConfig(interval_seconds=10, cycle_deadline_seconds=9.5)
    assert cfg.cycle_deadline_seconds == 9.5


# --- CacheConfig defaults ---


def test_cache_config_defaults():
    cfg = CacheConfig()
    assert cfg.enabled is False
    assert cfg.flush_interval_seconds == 3.0
    assert cfg.cleanup_interval_seconds == 60.0
    # Default cap prevents unbounded growth under write-heavy workloads;
    # operators can opt into unbounded behavior by setting None explicitly.
    assert cfg.max_memory_entries == 10_000


def test_cache_config_rejects_invalid_intervals():
    # flush_interval_seconds must be > 0 (gt=0)
    with pytest.raises(ValidationError):
        CacheConfig(flush_interval_seconds=0)
    with pytest.raises(ValidationError):
        CacheConfig(flush_interval_seconds=-1.0)
    # cleanup_interval_seconds must be > 0 (gt=0)
    with pytest.raises(ValidationError):
        CacheConfig(cleanup_interval_seconds=0)


def test_cache_config_rejects_invalid_max_memory_entries():
    # max_memory_entries must be >= 1 when set (ge=1); None is allowed
    with pytest.raises(ValidationError):
        CacheConfig(max_memory_entries=0)
    with pytest.raises(ValidationError):
        CacheConfig(max_memory_entries=-1)


def test_cache_config_max_memory_entries_none_is_valid():
    # Explicit opt-in to unbounded cache is still a valid choice — the
    # engine logs a warning at startup so operators see the decision.
    cfg = CacheConfig(max_memory_entries=None)
    assert cfg.max_memory_entries is None


# --- v1 scope enforcement — cut fields must NOT exist ---


def test_cache_config_has_no_preload_fields():
    """Guard against future-creep: preload_on_start / preload_limit were cut
    from v1 per the plan. Ensure they can't sneak back in without updating
    this test intentionally.
    """
    fields = set(CacheConfig.model_fields.keys())
    assert 'preload_on_start' not in fields
    assert 'preload_limit' not in fields


def test_cache_exchange_config_has_no_resolution_cache_knob():
    """peer_resolution_cache_seconds is intentionally hardcoded (300s) in v1.
    YAGNI — expose later only if operators actually need it.
    """
    fields = set(CacheExchangeConfig.model_fields.keys())
    assert 'peer_resolution_cache_seconds' not in fields


# --- DrakkarConfig integration — cache field present and defaults applied ---


def test_drakkar_config_has_cache_field():
    cfg = DrakkarConfig(
        connections=kafka_connections(),
        sources={'kafka': {'enabled': True}},
        executor=ExecutorConfig(binary_path='/bin/true'),
    )
    assert isinstance(cfg.cache, CacheConfig)
    # default: disabled
    assert cfg.cache.enabled is False


# --- Environment variable overrides (DK_CACHE__* via pydantic-settings) ---


def test_cache_config_env_override_enabled(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv('DK_EXECUTOR__BINARY_PATH', '/bin/true')
    monkeypatch.setenv('DK_CACHE__ENABLED', 'true')
    cfg = DrakkarConfig(connections=kafka_connections(), sources={'kafka': {'enabled': True}})
    assert cfg.cache.enabled is True


def test_cache_exchange_env_override_nested_interval(monkeypatch: pytest.MonkeyPatch):
    """DK_CACHE__EXCHANGE__INTERVAL_SECONDS=60 should set the nested
    cache.exchange.interval_seconds field. Exercises the two-level
    nested delimiter.
    """
    monkeypatch.setenv('DK_EXECUTOR__BINARY_PATH', '/bin/true')
    monkeypatch.setenv('DK_CACHE__ENABLED', 'true')
    monkeypatch.setenv('DK_CACHE__EXCHANGE__INTERVAL_SECONDS', '60')
    cfg = DrakkarConfig(connections=kafka_connections(), sources={'kafka': {'enabled': True}})
    assert cfg.cache.enabled is True
    assert cfg.cache.exchange.interval_seconds == 60.0


def test_cache_config_env_override_max_memory_entries(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv('DK_EXECUTOR__BINARY_PATH', '/bin/true')
    monkeypatch.setenv('DK_CACHE__MAX_MEMORY_ENTRIES', '2000')
    cfg = DrakkarConfig(connections=kafka_connections(), sources={'kafka': {'enabled': True}})
    assert cfg.cache.max_memory_entries == 2000
