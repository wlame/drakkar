"""The `on_startup` config-replacement contract.

`on_startup` receives the loaded config and returns a possibly modified
one. Most of the config is read *after* the hook, so a change lands. A few
settings are consumed while `DrakkarApp` is constructed — before the hook
can run at all — and a change to those is silently ignored.

`drakkar/app.py` names those settings in one table, and the lifecycle warns
when the hook changed one of them. These tests pin both halves.
"""

from __future__ import annotations

from structlog.testing import capture_logs

from drakkar import BaseDrakkarHandler, DrakkarApp, DrakkarConfig
from drakkar.app import (
    SETTINGS_CONSUMED_BEFORE_ON_STARTUP,
    changed_consumed_settings,
    snapshot_consumed_settings,
)
from drakkar.config import ExecutorConfig, KafkaConfig, LoggingConfig, MetricsConfig


class NoopHandler(BaseDrakkarHandler):
    async def arrange(self, messages, pending):
        return []


def make_config(**overrides) -> DrakkarConfig:
    defaults = {
        'kafka': KafkaConfig(brokers='localhost:9092', source_topic='test-in'),
        'executor': ExecutorConfig(binary_path='/bin/echo'),
        'metrics': MetricsConfig(enabled=False),
        'logging': LoggingConfig(level='WARNING', format='console'),
        'ui': {'release': {'enabled': False}},
    }
    defaults.update(overrides)
    return DrakkarConfig(**defaults)


class TestTheTableItself:
    def test_every_listed_path_resolves_on_a_real_config(self):
        """A typo in the table would silently stop guarding that setting."""
        config = make_config()

        snapshot = snapshot_consumed_settings(config)

        assert set(snapshot) == {setting.path for setting in SETTINGS_CONSUMED_BEFORE_ON_STARTUP}

    def test_every_entry_explains_why_it_is_consumed_early(self):
        for setting in SETTINGS_CONSUMED_BEFORE_ON_STARTUP:
            assert setting.reason, f'{setting.path} needs a reason an operator can act on'


class TestChangeDetection:
    def test_an_unchanged_config_reports_nothing(self):
        config = make_config()
        before = snapshot_consumed_settings(config)

        assert changed_consumed_settings(before, config) == []

    def test_a_changed_consumed_section_is_reported(self):
        config = make_config()
        before = snapshot_consumed_settings(config)

        config.sinks.circuit_breaker.failure_threshold += 1

        assert changed_consumed_settings(before, config) == ['sinks.circuit_breaker']

    def test_a_change_the_framework_honours_is_not_reported(self):
        """`executor` is read long after the hook — changing it is the
        documented, working use of on_startup."""
        config = make_config()
        before = snapshot_consumed_settings(config)

        config.executor.max_executors = 99

        assert changed_consumed_settings(before, config) == []

    def test_replacing_the_whole_config_object_is_still_compared_by_value(self):
        """A handler may build a fresh DrakkarConfig instead of mutating."""
        config = make_config()
        before = snapshot_consumed_settings(config)

        replacement = make_config(app={'anything': 1})

        assert changed_consumed_settings(before, replacement) == ['app']

    def test_several_changed_settings_are_all_reported_in_table_order(self):
        config = make_config()
        before = snapshot_consumed_settings(config)

        config.sinks.delivery_timeout_seconds = 99.0
        config.cluster_name = 'renamed-in-on_startup'

        # Table order, not alphabetical — deterministic so the warning text
        # is stable and the Go backend can emit the same list.
        assert changed_consumed_settings(before, config) == [
            'sinks.delivery_timeout_seconds',
            'cluster_name',
        ]


class TestLifecycleWarning:
    """The warning is emitted by the lifecycle, right after the hook returns."""

    async def test_changing_a_consumed_setting_warns(self):
        class TunesTheBreaker(NoopHandler):
            async def on_startup(self, config: DrakkarConfig) -> DrakkarConfig:
                config.sinks.circuit_breaker.failure_threshold = 42
                return config

        app = DrakkarApp(handler=TunesTheBreaker(), config=make_config())

        with capture_logs() as cap:
            await app._lifecycle._run_on_startup()

        (warning,) = [entry for entry in cap if entry['event'] == 'on_startup_config_change_ignored']
        assert warning['settings'] == ['sinks.circuit_breaker']
        assert warning['log_level'] == 'warning'
        # The operator needs to know what consumed it, not just that the
        # change was dropped.
        assert warning['reasons'] == [
            'sinks.circuit_breaker — the sink manager is constructed with it in DrakkarApp.__init__'
        ]
        assert 'config file or environment' in warning['hint']

    async def test_a_quiet_hook_warns_about_nothing(self):
        app = DrakkarApp(handler=NoopHandler(), config=make_config())

        with capture_logs() as cap:
            await app._lifecycle._run_on_startup()

        assert not [entry for entry in cap if entry['event'] == 'on_startup_config_change_ignored']

    async def test_the_returned_config_still_replaces_the_apps_config(self):
        """Warning about the ignored parts must not stop the honoured ones."""

        class TunesTheExecutor(NoopHandler):
            async def on_startup(self, config: DrakkarConfig) -> DrakkarConfig:
                config.executor.max_executors = 17
                return config

        app = DrakkarApp(handler=TunesTheExecutor(), config=make_config())

        await app._lifecycle._run_on_startup()

        assert app._config.executor.max_executors == 17
