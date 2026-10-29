"""Tests for the merged first-class ``ui.*`` config section.

Covers the two-tier shape (server / ``ui.release``)
and nested ``DK_UI__*`` env overrides.
"""

import pytest
from pydantic import ValidationError

from drakkar.config import (
    DrakkarConfig,
    RecorderConfig,
    UIConfig,
    UIReleaseConfig,
)
from tests.conftest import kafka_connections


def test_ui_defaults():
    ui = UIConfig()
    assert ui.enabled is True
    assert ui.host == '127.0.0.1'
    assert ui.port == 8080
    assert ui.auth_token == ''
    assert ui.allowed_ws_origins == []
    assert ui.public_url == ''
    assert ui.max_rows == 5000
    assert ui.ws_min_duration_ms == 500
    assert ui.log_min_duration_ms == 500
    assert ui.prometheus_rate_interval == '5m'
    assert ui.custom_links == []


def test_recorder_defaults():
    rec = RecorderConfig()
    assert rec.capture.events is True
    assert rec.capture.config is True
    assert rec.sqlite.state_snapshots is True
    assert rec.sqlite.state_interval == 10
    assert rec.sqlite.rotation_interval == 1
    assert rec.capture.output is True
    assert rec.flush_interval == 5
    assert rec.max_buffer_rows == 50_000
    assert rec.sqlite.max_flush_retries == 3
    assert rec.capture.min_event_duration == 0
    assert rec.capture.min_output_duration == 0.5


def test_recorder_rotation_interval_rejects_below_one_hour():
    with pytest.raises(ValidationError):
        RecorderConfig(sqlite={'rotation_interval': 0})


def test_annotation_defaults():
    annotations = RecorderConfig().annotations
    assert annotations.enabled is True
    assert annotations.max_size == 16_384
    assert annotations.max_size_per_call == 262_144
    assert annotations.max_log_size == 2048


@pytest.mark.parametrize('field', ['max_size', 'max_size_per_call', 'max_log_size'])
def test_annotation_size_caps_accept_zero_as_unlimited(field: str):
    assert getattr(RecorderConfig(annotations={field: 0}).annotations, field) == 0


@pytest.mark.parametrize('field', ['max_size', 'max_size_per_call', 'max_log_size'])
def test_annotation_size_caps_reject_negative(field: str):
    with pytest.raises(ValidationError):
        RecorderConfig(annotations={field: '-1B'})


def test_annotation_env_override(monkeypatch):
    monkeypatch.setenv('DK_RECORDER__ANNOTATIONS__ENABLED', 'false')
    monkeypatch.setenv('DK_RECORDER__ANNOTATIONS__MAX_SIZE', '4KiB')
    monkeypatch.setenv('DK_SOURCES__KAFKA__ENABLED', 'true')
    cfg = DrakkarConfig(connections=kafka_connections(), sources={'kafka': {'enabled': True}})
    assert cfg.recorder.annotations.enabled is False
    assert cfg.recorder.annotations.max_size == 4096


def test_release_defaults():
    rel = UIReleaseConfig()
    assert rel.enabled is True
    assert rel.repo == 'wlame/drakkar-ui'
    assert rel.pinned_version == ''
    assert rel.cache_dir == ''
    assert rel.check_update is True


def test_release_repo_slug_validated():
    with pytest.raises(ValidationError, match='owner/name'):
        UIReleaseConfig(repo='not-a-slug')


def test_auth_token_stripped_on_load():
    assert UIConfig(auth_token='  secret  ').auth_token == 'secret'


def test_env_nested_override(monkeypatch):
    monkeypatch.setenv('DK_RECORDER__FLUSH_INTERVAL', '7s')
    monkeypatch.setenv('DK_UI__RELEASE__CHECK_UPDATE', 'false')
    monkeypatch.setenv('DK_UI__PORT', '9001')
    monkeypatch.setenv('DK_SOURCES__KAFKA__ENABLED', 'true')
    cfg = DrakkarConfig(connections=kafka_connections(), sources={'kafka': {'enabled': True}})
    assert cfg.recorder.flush_interval == 7
    assert cfg.ui.release.check_update is False
    assert cfg.ui.port == 9001


def test_unknown_top_level_section_fails_with_the_unknown_keys_message():
    with pytest.raises(ValidationError, match='config has unknown keys'):
        DrakkarConfig(bogus_section={'port': 9999})


def test_config_summary_uses_ui_token():
    cfg = DrakkarConfig(connections=kafka_connections(), sources={'kafka': {'enabled': True}})
    summary = cfg.config_summary(worker_id='w1')
    assert ' ui=on:8080 ' in summary
    assert 'debug=' not in summary


def test_config_summary_ui_off():
    cfg = DrakkarConfig(connections=kafka_connections(), ui={'enabled': False}, sources={'kafka': {'enabled': True}})
    assert ' ui=off ' in cfg.config_summary(worker_id='w1')


def test_config_summary_storage_and_fleet_tokens_follow_ui():
    cfg = DrakkarConfig(connections=kafka_connections(), sources={'kafka': {'enabled': True}})
    summary = cfg.config_summary(worker_id='w1')
    assert ' ui=on:8080 storage=/tmp/drakkar fleet=off cache=off ' in summary


def test_probe_and_merge_default_to_enabled():
    """The gates are opt-in — an existing deployment keeps serving both endpoints."""
    cfg = UIConfig()
    assert cfg.probe_enabled is True
    assert cfg.merge_enabled is True


def test_probe_and_merge_env_overrides(monkeypatch):
    """Reachable via ``DK_UI__*`` so a deployment can close them without a config file."""
    monkeypatch.setenv('DK_UI__PROBE_ENABLED', 'false')
    monkeypatch.setenv('DK_UI__MERGE_ENABLED', 'false')
    monkeypatch.setenv('DK_SOURCES__KAFKA__ENABLED', 'true')
    cfg = DrakkarConfig(connections=kafka_connections(), sources={'kafka': {'enabled': True}})
    assert cfg.ui.probe_enabled is False
    assert cfg.ui.merge_enabled is False
