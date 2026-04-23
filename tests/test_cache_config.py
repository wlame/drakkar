"""Tests for CacheConfig and CachePeerSyncConfig in drakkar/config.py.

Validates defaults, environment-variable overrides (nested delimiter), field
constraints, and confirms the v1 scope of the config — no preload fields, no
exposed peer-resolution cache TTL.
"""

import pytest
from pydantic import ValidationError

from drakkar.config import (
    CacheConfig,
    CachePeerSyncConfig,
    DrakkarConfig,
    ExecutorConfig,
)

# --- CachePeerSyncConfig defaults ---


def test_cache_peer_sync_config_defaults():
    cfg = CachePeerSyncConfig()
    assert cfg.enabled is True
    assert cfg.interval_seconds == 30.0
    assert cfg.batch_size == 500
    assert cfg.timeout_seconds == 5.0


def test_cache_peer_sync_config_custom_values():
    cfg = CachePeerSyncConfig(
        enabled=False,
        interval_seconds=120.0,
        batch_size=1000,
        timeout_seconds=10.0,
    )
    assert cfg.enabled is False
    assert cfg.interval_seconds == 120.0
    assert cfg.batch_size == 1000
    assert cfg.timeout_seconds == 10.0


def test_cache_peer_sync_config_rejects_invalid():
    # interval_seconds must be > 0 (gt=0)
    with pytest.raises(ValidationError):
        CachePeerSyncConfig(interval_seconds=0)
    with pytest.raises(ValidationError):
        CachePeerSyncConfig(interval_seconds=-1.0)
    # batch_size must be >= 1 (ge=1)
    with pytest.raises(ValidationError):
        CachePeerSyncConfig(batch_size=0)
    with pytest.raises(ValidationError):
        CachePeerSyncConfig(batch_size=-1)
    # timeout_seconds must be > 0 (gt=0)
    with pytest.raises(ValidationError):
        CachePeerSyncConfig(timeout_seconds=0)


# --- CacheConfig defaults ---


def test_cache_config_defaults():
    cfg = CacheConfig()
    assert cfg.enabled is False
    # db_dir empty — resolution happens at engine init, not config load
    assert cfg.db_dir == ''
    assert cfg.flush_interval_seconds == 3.0
    assert cfg.cleanup_interval_seconds == 60.0
    # Default cap prevents unbounded growth under write-heavy workloads;
    # operators can opt into unbounded behavior by setting None explicitly.
    assert cfg.max_memory_entries == 10_000
    # peer_sync defaults nested in
    assert isinstance(cfg.peer_sync, CachePeerSyncConfig)
    assert cfg.peer_sync.enabled is True
    assert cfg.peer_sync.interval_seconds == 30.0


def test_cache_config_custom_values():
    cfg = CacheConfig(
        enabled=True,
        db_dir='/var/lib/drakkar/cache',
        flush_interval_seconds=1.5,
        cleanup_interval_seconds=30.0,
        max_memory_entries=1000,
        peer_sync=CachePeerSyncConfig(enabled=False),
    )
    assert cfg.enabled is True
    assert cfg.db_dir == '/var/lib/drakkar/cache'
    assert cfg.flush_interval_seconds == 1.5
    assert cfg.cleanup_interval_seconds == 30.0
    assert cfg.max_memory_entries == 1000
    assert cfg.peer_sync.enabled is False


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


def test_cache_config_max_memory_entries_explicit_value_is_honored():
    # Explicit smaller cap wins over the default.
    cfg = CacheConfig(max_memory_entries=500)
    assert cfg.max_memory_entries == 500


def test_cache_config_db_dir_empty_stays_empty():
    """Config should not resolve db_dir on load — that happens at engine init
    using debug.db_dir as fallback. Keeps config layer pure and testable.
    """
    cfg = CacheConfig(db_dir='')
    assert cfg.db_dir == ''


# --- v1 scope enforcement — cut fields must NOT exist ---


def test_cache_config_has_no_preload_fields():
    """Guard against future-creep: preload_on_start / preload_limit were cut
    from v1 per the plan. Ensure they can't sneak back in without updating
    this test intentionally.
    """
    fields = set(CacheConfig.model_fields.keys())
    assert 'preload_on_start' not in fields
    assert 'preload_limit' not in fields


def test_cache_peer_sync_config_has_no_resolution_cache_knob():
    """peer_resolution_cache_seconds is intentionally hardcoded (300s) in v1.
    YAGNI — expose later only if operators actually need it.
    """
    fields = set(CachePeerSyncConfig.model_fields.keys())
    assert 'peer_resolution_cache_seconds' not in fields


# --- DrakkarConfig integration — cache field present and defaults applied ---


def test_drakkar_config_has_cache_field():
    cfg = DrakkarConfig(executor=ExecutorConfig(binary_path='/bin/true'))
    assert isinstance(cfg.cache, CacheConfig)
    # default: disabled
    assert cfg.cache.enabled is False


def test_drakkar_config_cache_custom():
    cfg = DrakkarConfig(
        executor=ExecutorConfig(binary_path='/bin/true'),
        cache=CacheConfig(enabled=True, flush_interval_seconds=0.5),
    )
    assert cfg.cache.enabled is True
    assert cfg.cache.flush_interval_seconds == 0.5


# --- Environment variable overrides (DRAKKAR_CACHE__* via pydantic-settings) ---


def test_cache_config_env_override_enabled(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv('DRAKKAR_EXECUTOR__BINARY_PATH', '/bin/true')
    monkeypatch.setenv('DRAKKAR_CACHE__ENABLED', 'true')
    cfg = DrakkarConfig()
    assert cfg.cache.enabled is True


def test_cache_config_env_override_nested_peer_sync(monkeypatch: pytest.MonkeyPatch):
    """DRAKKAR_CACHE__PEER_SYNC__INTERVAL_SECONDS=60 should set the nested
    peer_sync.interval_seconds field. Exercises the two-level nested delimiter.
    """
    monkeypatch.setenv('DRAKKAR_EXECUTOR__BINARY_PATH', '/bin/true')
    monkeypatch.setenv('DRAKKAR_CACHE__ENABLED', 'true')
    monkeypatch.setenv('DRAKKAR_CACHE__PEER_SYNC__INTERVAL_SECONDS', '60')
    cfg = DrakkarConfig()
    assert cfg.cache.enabled is True
    assert cfg.cache.peer_sync.interval_seconds == 60.0


def test_cache_config_env_override_db_dir(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv('DRAKKAR_EXECUTOR__BINARY_PATH', '/bin/true')
    monkeypatch.setenv('DRAKKAR_CACHE__DB_DIR', '/srv/cache')
    cfg = DrakkarConfig()
    assert cfg.cache.db_dir == '/srv/cache'


def test_cache_config_env_override_max_memory_entries(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv('DRAKKAR_EXECUTOR__BINARY_PATH', '/bin/true')
    monkeypatch.setenv('DRAKKAR_CACHE__MAX_MEMORY_ENTRIES', '2000')
    cfg = DrakkarConfig()
    assert cfg.cache.max_memory_entries == 2000
