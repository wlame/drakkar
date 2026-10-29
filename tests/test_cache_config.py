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
    # interval must be > 0 (gt=0)
    with pytest.raises(ValidationError):
        CacheExchangeConfig(interval='0')
    with pytest.raises(ValidationError):
        CacheExchangeConfig(interval=-1.0)
    # page_size must be in 1..1000
    with pytest.raises(ValidationError):
        CacheExchangeConfig(page_size=0)
    with pytest.raises(ValidationError):
        CacheExchangeConfig(page_size=1001)
    # timeout must be > 0 (gt=0)
    with pytest.raises(ValidationError):
        CacheExchangeConfig(timeout='0')


def test_cache_exchange_deadline_below_interval_is_accepted():
    cfg = CacheExchangeConfig(interval='10s', cycle_deadline='9s500ms')
    assert cfg.cycle_deadline == 9.5


# --- CacheConfig defaults ---


def test_cache_config_defaults():
    cfg = CacheConfig()
    assert cfg.enabled is False
    assert cfg.flush_interval == 3.0
    assert cfg.cleanup_interval == 60.0
    # Default cap prevents unbounded growth under write-heavy workloads;
    # operators can opt into unbounded behavior by setting 0.
    assert cfg.max_memory_entries == 10_000


def test_cache_config_rejects_invalid_intervals():
    # flush_interval must be > 0 (gt=0)
    with pytest.raises(ValidationError):
        CacheConfig(flush_interval='0')
    with pytest.raises(ValidationError):
        CacheConfig(flush_interval=-1.0)
    # cleanup_interval must be > 0 (gt=0)
    with pytest.raises(ValidationError):
        CacheConfig(cleanup_interval='0')


def test_cache_config_rejects_negative_or_null_max_memory_entries():
    with pytest.raises(ValidationError):
        CacheConfig(max_memory_entries=-1)
    with pytest.raises(ValidationError):
        CacheConfig(max_memory_entries=None)


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
    """DK_CACHE__EXCHANGE__INTERVAL=1m should set the nested
    cache.exchange.interval field. Exercises the two-level
    nested delimiter.
    """
    monkeypatch.setenv('DK_EXECUTOR__BINARY_PATH', '/bin/true')
    monkeypatch.setenv('DK_CACHE__ENABLED', 'true')
    monkeypatch.setenv('DK_CACHE__EXCHANGE__INTERVAL', '1m')
    cfg = DrakkarConfig(connections=kafka_connections(), sources={'kafka': {'enabled': True}})
    assert cfg.cache.enabled is True
    assert cfg.cache.exchange.interval == 60.0


def test_cache_config_env_override_max_memory_entries(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv('DK_EXECUTOR__BINARY_PATH', '/bin/true')
    monkeypatch.setenv('DK_CACHE__MAX_MEMORY_ENTRIES', '2000')
    cfg = DrakkarConfig(connections=kafka_connections(), sources={'kafka': {'enabled': True}})
    assert cfg.cache.max_memory_entries == 2000
