"""Tests for the user-defined application config loader (drakkar/appconfig.py).

Fully isolated: config comes from tmp_path YAML files, in-memory dicts,
and monkeypatched environment variables — no app, no network, no database.
"""

from pathlib import Path

import pytest
from pydantic import BaseModel, Field, SecretStr

from drakkar import load_app_config


class ScoringConfig(BaseModel):
    """Nested section of the test app model."""

    url: str = 'http://localhost:9000/score'
    timeout_seconds: int = 5


class DemoAppConfig(BaseModel):
    """A representative user model: scalars, a secret, and a nested model."""

    priority_threshold: int = 10
    notify_enabled: bool = False
    api_key: SecretStr = SecretStr('')
    scoring: ScoringConfig = Field(default_factory=ScoringConfig)


class RequiredFieldConfig(BaseModel):
    """A model with a required field, for missing-value failure tests."""

    scoring_url: str


# --- Precedence: defaults / YAML / env ---


def test_load_app_config_defaults_only_returns_model_defaults():
    cfg = load_app_config(DemoAppConfig, yaml_data={})
    assert cfg == DemoAppConfig()


def test_load_app_config_yaml_only_overrides_defaults():
    yaml_data = {'app': {'priority_threshold': 42, 'scoring': {'url': 'http://scoring-service:9000'}}}
    cfg = load_app_config(DemoAppConfig, yaml_data=yaml_data)
    assert cfg.priority_threshold == 42
    assert cfg.scoring.url == 'http://scoring-service:9000'
    # Untouched fields keep their model defaults.
    assert cfg.notify_enabled is False
    assert cfg.scoring.timeout_seconds == 5


def test_load_app_config_env_only_overrides_defaults(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv('MYAPP_PRIORITY_THRESHOLD', '77')
    cfg = load_app_config(DemoAppConfig, yaml_data={}, env_prefix='MYAPP_')
    assert cfg.priority_threshold == 77


def test_load_app_config_env_wins_over_yaml(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv('MYAPP_PRIORITY_THRESHOLD', '99')
    yaml_data = {'app': {'priority_threshold': 42, 'notify_enabled': True}}
    cfg = load_app_config(DemoAppConfig, yaml_data=yaml_data, env_prefix='MYAPP_')
    # Env beats YAML for the overridden field; YAML still beats defaults elsewhere.
    assert cfg.priority_threshold == 99
    assert cfg.notify_enabled is True


def test_load_app_config_nested_field_via_double_underscore(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv('MYAPP_SCORING__URL', 'http://env-scoring:9000')
    yaml_data = {'app': {'scoring': {'url': 'http://yaml-scoring:9000', 'timeout_seconds': 30}}}
    cfg = load_app_config(DemoAppConfig, yaml_data=yaml_data, env_prefix='MYAPP_')
    # The env var replaces one nested leaf; its YAML sibling survives the merge.
    assert cfg.scoring.url == 'http://env-scoring:9000'
    assert cfg.scoring.timeout_seconds == 30


# --- Type coercion and secrets ---


def test_load_app_config_coerces_env_strings_to_declared_types(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv('MYAPP_PRIORITY_THRESHOLD', '20')
    monkeypatch.setenv('MYAPP_NOTIFY_ENABLED', 'true')
    monkeypatch.setenv('MYAPP_SCORING__TIMEOUT_SECONDS', '15')
    cfg = load_app_config(DemoAppConfig, yaml_data={}, env_prefix='MYAPP_')
    assert cfg.priority_threshold == 20
    assert cfg.notify_enabled is True
    assert cfg.scoring.timeout_seconds == 15


def test_load_app_config_secretstr_field_wraps_env_value(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv('MYAPP_API_KEY', 'topsecret')
    cfg = load_app_config(DemoAppConfig, yaml_data={}, env_prefix='MYAPP_')
    assert isinstance(cfg.api_key, SecretStr)
    assert cfg.api_key.get_secret_value() == 'topsecret'
    assert 'topsecret' not in repr(cfg)


# --- Guard rails ---


def test_load_app_config_rejects_dk_prefix():
    with pytest.raises(ValueError, match="collides with the framework's DK_ namespace"):
        load_app_config(DemoAppConfig, yaml_data={}, env_prefix='DK_')


def test_load_app_config_rejects_dk_derived_prefix():
    """Any DK_-rooted prefix collides — DK_MYAPP__X would also be parsed as
    a framework override — so the guard covers the whole namespace."""
    with pytest.raises(ValueError, match="collides with the framework's DK_ namespace"):
        load_app_config(DemoAppConfig, yaml_data={}, env_prefix='DK_MYAPP_')


def test_load_app_config_missing_section_returns_defaults():
    cfg = load_app_config(DemoAppConfig, yaml_data={'kafka': {'brokers': 'k:9092'}})
    assert cfg == DemoAppConfig()


def test_load_app_config_non_mapping_section_raises_clear_error():
    with pytest.raises(ValueError, match="Config section 'app' must be a mapping for model DemoAppConfig"):
        load_app_config(DemoAppConfig, yaml_data={'app': ['not', 'a', 'mapping']})


def test_load_app_config_invalid_value_names_section_and_model(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv('MYAPP_PRIORITY_THRESHOLD', 'not-a-number')
    with pytest.raises(ValueError, match="Invalid config in section 'app' for app config model DemoAppConfig"):
        load_app_config(DemoAppConfig, yaml_data={}, env_prefix='MYAPP_')


def test_load_app_config_missing_required_field_raises_with_field_name():
    with pytest.raises(ValueError, match='scoring_url'):
        load_app_config(RequiredFieldConfig, yaml_data={})


# --- File and path resolution (mirrors load_config) ---


def test_load_app_config_reads_named_section_from_yaml_file(tmp_path: Path):
    config_path = tmp_path / 'drakkar.yaml'
    config_path.write_text('app:\n  priority_threshold: 33\n')
    cfg = load_app_config(DemoAppConfig, config_path)
    assert cfg.priority_threshold == 33


def test_load_app_config_custom_section_name(tmp_path: Path):
    config_path = tmp_path / 'drakkar.yaml'
    config_path.write_text('ranking_app:\n  priority_threshold: 5\n')
    cfg = load_app_config(DemoAppConfig, config_path, section='ranking_app')
    assert cfg.priority_threshold == 5


def test_load_app_config_resolves_path_from_dk_config_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    config_path = tmp_path / 'drakkar.yaml'
    config_path.write_text('app:\n  notify_enabled: true\n')
    monkeypatch.setenv('DK_CONFIG', str(config_path))
    cfg = load_app_config(DemoAppConfig)
    assert cfg.notify_enabled is True


def test_load_app_config_missing_file_raises():
    with pytest.raises(FileNotFoundError, match='Config file not found'):
        load_app_config(DemoAppConfig, '/nonexistent/drakkar.yaml')


def test_load_app_config_no_path_no_env_var_uses_env_only(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv('DK_CONFIG', raising=False)
    monkeypatch.setenv('MYAPP_PRIORITY_THRESHOLD', '8')
    cfg = load_app_config(DemoAppConfig, env_prefix='MYAPP_')
    assert cfg.priority_threshold == 8


def test_load_app_config_yaml_data_takes_precedence_over_file_resolution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """With yaml_data given, the file path machinery is bypassed entirely."""
    config_path = tmp_path / 'drakkar.yaml'
    config_path.write_text('app:\n  priority_threshold: 1\n')
    monkeypatch.setenv('DK_CONFIG', str(config_path))
    cfg = load_app_config(DemoAppConfig, yaml_data={'app': {'priority_threshold': 2}})
    assert cfg.priority_threshold == 2


def test_load_app_config_prefix_config_var_is_a_field_not_a_path(monkeypatch: pytest.MonkeyPatch):
    """MYAPP_CONFIG must reach a user field named ``config`` — only the
    framework's DK_CONFIG is a file-path convention."""

    class WithConfigField(BaseModel):
        config: str = 'default'

    monkeypatch.setenv('MYAPP_CONFIG', 'from-env')
    cfg = load_app_config(WithConfigField, yaml_data={}, env_prefix='MYAPP_')
    assert cfg.config == 'from-env'
