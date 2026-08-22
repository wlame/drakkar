"""Tests for the app-config wiring in ``DrakkarApp`` (docs/app-config.md).

Covers the startup seam: a handler declaring ``app_config_model`` gets a
validated ``self.app_config`` at app construction (i.e. before the
lifecycle ever calls ``on_startup``), a validation failure is
startup-fatal, and a non-empty ``app:`` section with no declared model
warns instead of vanishing silently.

Fully isolated: in-memory ``DrakkarConfig`` objects and monkeypatched env
vars only — no Kafka, no sinks connected, no event loop beyond the hooks
invoked directly.
"""

import pytest
from pydantic import BaseModel, Field, SecretStr
from structlog.testing import capture_logs

from drakkar import BaseDrakkarHandler, DrakkarApp, DrakkarConfig
from drakkar.config import ExecutorConfig, KafkaConfig, LoggingConfig, MetricsConfig


class DemoAppConfig(BaseModel):
    """Representative user app config: two scalars and a secret."""

    priority_threshold: int = 10
    scoring_url: str = 'http://localhost:9000/score'
    api_key: SecretStr = SecretStr('')


class DeclaringHandler(BaseDrakkarHandler):
    """Handler opting into the app config via the class attributes."""

    app_config_model = DemoAppConfig
    app_env_prefix = 'MYAPP_'

    def __init__(self) -> None:
        self.app_config_seen_in_startup: DemoAppConfig | None = None

    async def arrange(self, messages, pending):
        return []

    async def on_startup(self, config: DrakkarConfig) -> DrakkarConfig:
        # Captured so the test can assert the instance was already loaded
        # by the time the framework invokes this hook.
        assert isinstance(self.app_config, DemoAppConfig)
        self.app_config_seen_in_startup = self.app_config
        return config


class UndeclaredHandler(BaseDrakkarHandler):
    """Handler that never declares an app config model."""

    async def arrange(self, messages, pending):
        return []


def make_config(**overrides) -> DrakkarConfig:
    defaults = {
        'kafka': KafkaConfig(brokers='localhost:9092', source_topic='test-in'),
        'executor': ExecutorConfig(binary_path='/bin/echo'),
        'metrics': MetricsConfig(enabled=False),
        'logging': LoggingConfig(level='WARNING', format='console'),
    }
    defaults.update(overrides)
    return DrakkarConfig(**defaults)


# --- Loading path ---


def test_declared_model_yields_app_config_at_construction():
    handler = DeclaringHandler()
    DrakkarApp(handler=handler, config=make_config(app={'priority_threshold': 42}))
    assert isinstance(handler.app_config, DemoAppConfig)
    assert handler.app_config.priority_threshold == 42
    # Untouched fields keep model defaults.
    assert handler.app_config.scoring_url == 'http://localhost:9000/score'


async def test_app_config_is_visible_inside_on_startup_hook():
    """The instance must exist before ``on_startup`` runs — the hook itself
    asserts it (see DeclaringHandler) and records what it saw."""
    handler = DeclaringHandler()
    app = DrakkarApp(handler=handler, config=make_config(app={'priority_threshold': 7}))
    # Mirror the lifecycle's exact call (lifecycle._async_run) without the
    # full Kafka startup machinery.
    app._config = await app._handler.on_startup(app._config)
    assert handler.app_config_seen_in_startup is not None
    assert handler.app_config_seen_in_startup.priority_threshold == 7


def test_handler_env_prefix_overrides_yaml_section(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv('MYAPP_PRIORITY_THRESHOLD', '99')
    monkeypatch.setenv('MYAPP_API_KEY', 'sekrit')
    handler = DeclaringHandler()
    DrakkarApp(handler=handler, config=make_config(app={'priority_threshold': 42}))
    assert handler.app_config is not None
    assert handler.app_config.priority_threshold == 99
    assert handler.app_config.api_key.get_secret_value() == 'sekrit'


def test_loaded_log_line_reports_field_count_never_values():
    with capture_logs() as cap:
        DrakkarApp(handler=DeclaringHandler(), config=make_config(app={'scoring_url': 'http://scoring-svc:9000'}))
    loaded = [entry for entry in cap if entry['event'] == 'app_config_loaded']
    assert len(loaded) == 1
    assert loaded[0]['model'] == 'DemoAppConfig'
    assert loaded[0]['field_count'] == 3
    assert 'http://scoring-svc:9000' not in str(loaded[0])


# --- Failure and warning paths ---


def test_invalid_app_section_fails_startup_naming_the_model():
    with pytest.raises(ValueError, match='DemoAppConfig') as excinfo:
        DrakkarApp(handler=DeclaringHandler(), config=make_config(app={'priority_threshold': 'not-a-number'}))
    assert 'priority_threshold' in str(excinfo.value)


def test_invalid_env_override_fails_startup(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv('MYAPP_PRIORITY_THRESHOLD', 'lots')
    with pytest.raises(ValueError, match='DemoAppConfig'):
        DrakkarApp(handler=DeclaringHandler(), config=make_config())


def test_undeclared_model_with_nonempty_section_warns():
    with capture_logs() as cap:
        handler = UndeclaredHandler()
        DrakkarApp(handler=handler, config=make_config(app={'priority_threshold': 42}))
    assert handler.app_config is None
    warnings = [entry for entry in cap if entry['event'] == 'app_config_ignored']
    assert len(warnings) == 1
    assert warnings[0]['log_level'] == 'warning'
    assert warnings[0]['keys'] == ['priority_threshold']


def test_undeclared_model_with_empty_section_is_silent():
    with capture_logs() as cap:
        handler = UndeclaredHandler()
        DrakkarApp(handler=handler, config=make_config())
    assert handler.app_config is None
    assert not [entry for entry in cap if entry['event'] == 'app_config_ignored']


def test_default_handler_surface_is_inert():
    """Base attributes: no model, APP_ prefix, property returns None."""
    handler = UndeclaredHandler()
    assert UndeclaredHandler.app_config_model is None
    assert handler.app_env_prefix == 'APP_'
    assert handler.app_config is None


class RequiredFieldAppConfig(BaseModel):
    scoring_url: str = Field(description='No default — must come from YAML or env.')


class RequiringHandler(BaseDrakkarHandler):
    app_config_model = RequiredFieldAppConfig

    async def arrange(self, messages, pending):
        return []


def test_missing_required_field_fails_startup_with_field_name():
    with pytest.raises(ValueError, match='scoring_url'):
        DrakkarApp(handler=RequiringHandler(), config=make_config())


def test_default_app_prefix_is_used_when_handler_does_not_override(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv('APP_SCORING_URL', 'http://scoring-from-env:9000')
    handler = RequiringHandler()
    DrakkarApp(handler=handler, config=make_config())
    assert handler.app_config is not None
    assert handler.app_config.scoring_url == 'http://scoring-from-env:9000'
