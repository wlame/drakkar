"""Tests for the merged first-class ``ui.*`` config section.

Covers the three-tier shape (server / ``ui.recorder`` / ``ui.release``)
and nested ``DK_UI__*`` env overrides.
"""

import pytest
from pydantic import ValidationError

from drakkar.config import DrakkarConfig, UIConfig, UIRecorderConfig, UIReleaseConfig


def test_ui_defaults_match_old_debug_defaults():
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


def test_recorder_defaults_match_old_debug_defaults():
    rec = UIRecorderConfig()
    assert rec.db_dir == '/tmp'
    assert rec.store_events is True
    assert rec.store_config is True
    assert rec.store_state is True
    assert rec.state_sync_interval_seconds == 10
    assert rec.rotation_interval_hours == 1
    assert rec.archive_enabled is True
    assert rec.archive_window_hours == 24
    assert rec.archive_retention_days == 0
    assert rec.store_output is True
    assert rec.flush_interval_seconds == 5
    assert rec.max_buffer == 50_000
    assert rec.max_flush_retries == 3
    assert rec.event_min_duration_ms == 0
    assert rec.output_min_duration_ms == 500


def test_recorder_rotation_interval_hours_rejects_below_one():
    with pytest.raises(ValidationError):
        UIRecorderConfig(rotation_interval_hours=0)


def test_recorder_archive_window_hours_rejects_below_one():
    with pytest.raises(ValidationError):
        UIRecorderConfig(archive_window_hours=0)


def test_recorder_archive_retention_days_accepts_zero():
    assert UIRecorderConfig(archive_retention_days=0).archive_retention_days == 0


def test_recorder_archive_retention_days_rejects_negative():
    with pytest.raises(ValidationError):
        UIRecorderConfig(archive_retention_days=-1)


def test_recorder_archive_retention_shorter_than_two_windows_is_fatal():
    with pytest.raises(ValidationError, match=r'archive_retention_days \(1\).*archive_window_hours \(24\)'):
        UIRecorderConfig(archive_retention_days=1, archive_window_hours=24)


def test_recorder_archive_retention_of_two_windows_is_accepted():
    rec = UIRecorderConfig(archive_retention_days=2, archive_window_hours=24)
    assert rec.archive_retention_days == 2


def test_recorder_archive_retention_zero_is_accepted_for_any_window():
    assert UIRecorderConfig(archive_retention_days=0, archive_window_hours=168).archive_retention_days == 0


def test_recorder_archive_window_shorter_than_rotation_is_fatal():
    with pytest.raises(ValidationError, match=r'archive_window_hours \(1\).*rotation_interval_hours \(2\)'):
        UIRecorderConfig(archive_window_hours=1, rotation_interval_hours=2)


def test_retired_recorder_keys_are_ignored_extras():
    """The pre-1.0 migration guards are gone: the removed keys are plain unknown keys now."""
    rec = UIRecorderConfig(rotation_interval_minutes=60, retention_hours=24, retention_max_events=100_000)
    assert rec.rotation_interval_hours == 1
    assert not hasattr(rec, 'retention_hours')


def test_annotation_defaults():
    rec = UIRecorderConfig()
    assert rec.annotations_enabled is True
    assert rec.annotation_max_bytes == 16_384
    assert rec.annotation_max_bytes_per_call == 262_144
    assert rec.annotation_log_max_bytes == 2048


@pytest.mark.parametrize(
    'field',
    ['annotation_max_bytes', 'annotation_max_bytes_per_call', 'annotation_log_max_bytes'],
)
def test_annotation_byte_caps_accept_zero_as_unlimited(field: str):
    assert getattr(UIRecorderConfig(**{field: 0}), field) == 0


@pytest.mark.parametrize(
    'field',
    ['annotation_max_bytes', 'annotation_max_bytes_per_call', 'annotation_log_max_bytes'],
)
def test_annotation_byte_caps_reject_negative(field: str):
    with pytest.raises(ValidationError):
        UIRecorderConfig(**{field: -1})


def test_annotation_env_override(monkeypatch):
    monkeypatch.setenv('DK_UI__RECORDER__ANNOTATIONS_ENABLED', 'false')
    monkeypatch.setenv('DK_UI__RECORDER__ANNOTATION_MAX_BYTES', '4096')
    cfg = DrakkarConfig()
    assert cfg.ui.recorder.annotations_enabled is False
    assert cfg.ui.recorder.annotation_max_bytes == 4096


def test_release_defaults_match_old_flat_ui_defaults():
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
    monkeypatch.setenv('DK_UI__RECORDER__DB_DIR', '/data')
    monkeypatch.setenv('DK_UI__RELEASE__CHECK_UPDATE', 'false')
    monkeypatch.setenv('DK_UI__PORT', '9001')
    cfg = DrakkarConfig()
    assert cfg.ui.recorder.db_dir == '/data'
    assert cfg.ui.release.check_update is False
    assert cfg.ui.port == 9001


def test_unknown_top_level_section_is_still_fatal():
    """The bespoke debug.* guard is gone, but the root model forbids unknown sections."""
    with pytest.raises(ValidationError, match='Extra inputs are not permitted'):
        DrakkarConfig(debug={'port': 9999})


def test_retired_flat_ui_keys_are_ignored_extras():
    cfg = DrakkarConfig(ui={'release_repo': 'a/b', 'check_update': False})
    assert cfg.ui.release.repo == 'wlame/drakkar-ui'
    assert cfg.ui.release.check_update is True


def test_config_summary_uses_ui_token():
    cfg = DrakkarConfig()
    summary = cfg.config_summary(worker_id='w1')
    assert ' ui=on:8080 ' in summary
    assert 'debug=' not in summary


def test_config_summary_ui_off():
    cfg = DrakkarConfig(ui={'enabled': False})
    assert ' ui=off ' in cfg.config_summary(worker_id='w1')


def test_config_summary_webapp_token_follows_ui():
    cfg = DrakkarConfig()
    summary = cfg.config_summary(worker_id='w1')
    assert ' ui=on:8080 webapp=off ' in summary


def test_probe_and_merge_default_to_enabled():
    """The gates are opt-in — an existing deployment keeps serving both endpoints."""
    cfg = UIConfig()
    assert cfg.probe_enabled is True
    assert cfg.merge_enabled is True


def test_probe_and_merge_env_overrides(monkeypatch):
    """Reachable via ``DK_UI__*`` so a deployment can close them without a config file."""
    monkeypatch.setenv('DK_UI__PROBE_ENABLED', 'false')
    monkeypatch.setenv('DK_UI__MERGE_ENABLED', 'false')
    cfg = DrakkarConfig()
    assert cfg.ui.probe_enabled is False
    assert cfg.ui.merge_enabled is False
