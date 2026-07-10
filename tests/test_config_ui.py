"""Tests for the merged first-class ``ui.*`` config section.

Covers the three-tier shape (server / ``ui.recorder`` / ``ui.release``),
nested ``DK_UI__*`` env overrides, and the hard-break migration guards that
reject the retired ``debug.*`` section and the old flat ``ui.*`` fetch keys
with errors that name the new home of every offending key.
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
    assert rec.rotation_interval_minutes == 60
    assert rec.retention_hours == 24
    assert rec.retention_max_events == 100_000
    assert rec.store_output is True
    assert rec.flush_interval_seconds == 5
    assert rec.max_buffer == 50_000
    assert rec.max_flush_retries == 3
    assert rec.event_min_duration_ms == 0
    assert rec.output_min_duration_ms == 500


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


def test_old_debug_section_is_fatal_with_mapping():
    with pytest.raises(ValidationError, match=r'debug\.port.*ui\.port'):
        DrakkarConfig(debug={'port': 9999})


def test_old_debug_section_lists_only_present_keys():
    with pytest.raises(ValidationError) as excinfo:
        DrakkarConfig(debug={'db_dir': '/x', 'max_ui_rows': 10})
    message = str(excinfo.value)
    assert 'debug.db_dir -> ui.recorder.db_dir' in message
    assert 'debug.max_ui_rows -> ui.max_rows' in message
    assert 'debug.port' not in message


def test_old_dk_debug_env_is_fatal(monkeypatch):
    monkeypatch.setenv('DK_DEBUG__PORT', '9999')
    with pytest.raises(ValidationError, match='DK_DEBUG__PORT'):
        DrakkarConfig()


def test_old_flat_ui_keys_are_fatal():
    with pytest.raises(ValidationError, match=r'ui\.release_repo -> ui\.release\.repo'):
        DrakkarConfig(ui={'release_repo': 'a/b'})


def test_old_flat_ui_check_update_is_fatal():
    with pytest.raises(ValidationError, match=r'ui\.check_update -> ui\.release\.check_update'):
        DrakkarConfig(ui={'check_update': False})


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
