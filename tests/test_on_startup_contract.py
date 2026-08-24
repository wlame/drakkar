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

        config.cluster_name = 'renamed-in-on_startup'

        assert changed_consumed_settings(before, config) == ['cluster_name']

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

        config.worker_name_env = 'OTHER_WORKER'
        config.app = {'priority_threshold': 1}

        # Table order, not alphabetical — deterministic so the warning text
        # is stable and the Go backend emits the same list.
        assert changed_consumed_settings(before, config) == ['app', 'worker_name_env']


class TestLifecycleWarning:
    """The warning is emitted by the lifecycle, right after the hook returns."""

    async def test_changing_a_consumed_setting_warns(self):
        class RenamesTheCluster(NoopHandler):
            async def on_startup(self, config: DrakkarConfig) -> DrakkarConfig:
                config.cluster_name = 'renamed-in-on_startup'
                return config

        app = DrakkarApp(handler=RenamesTheCluster(), config=make_config())

        with capture_logs() as cap:
            await app._lifecycle._run_on_startup()

        (warning,) = [entry for entry in cap if entry['event'] == 'on_startup_config_change_ignored']
        assert warning['settings'] == ['cluster_name']
        assert warning['log_level'] == 'warning'
        # The operator needs to know what consumed it, not just that the
        # change was dropped.
        assert warning['reasons'] == ['cluster_name — the worker resolves its cluster name in DrakkarApp.__init__']
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


class TestSinkSettingsAreHonoured:
    """`sinks.circuit_breaker` and `sinks.delivery_timeout_seconds` are read
    when the sinks are built, which happens after the hook — so tuning them
    in `on_startup` works, and matches the Go backend.
    """

    def test_the_manager_reads_the_live_config_not_a_snapshot(self):
        config = make_config()
        app = DrakkarApp(handler=NoopHandler(), config=config)

        config.sinks.circuit_breaker.failure_threshold = 42
        config.sinks.delivery_timeout_seconds = 7.5

        assert app._sink_manager._circuit_breaker_config.failure_threshold == 42
        assert app._sink_manager._delivery_timeout_seconds == 7.5

    def test_a_sink_registered_after_the_hook_gets_the_new_values(self):
        from tests.test_sink_manager import FakeSink

        config = make_config()
        app = DrakkarApp(handler=NoopHandler(), config=config)
        config.sinks.circuit_breaker.failure_threshold = 42
        config.sinks.delivery_timeout_seconds = 7.5

        sink = FakeSink('after-the-hook')
        app._sink_manager.register(sink)

        assert sink._circuit_config.failure_threshold == 42
        assert sink._delivery_timeout_seconds == 7.5

    async def test_tuning_the_breaker_in_on_startup_no_longer_warns(self):
        """It is honoured now, so warning about it would be wrong."""

        class TunesTheBreaker(NoopHandler):
            async def on_startup(self, config: DrakkarConfig) -> DrakkarConfig:
                config.sinks.circuit_breaker.failure_threshold = 42
                config.sinks.delivery_timeout_seconds = 7.5
                return config

        app = DrakkarApp(handler=TunesTheBreaker(), config=make_config())

        with capture_logs() as cap:
            await app._lifecycle._run_on_startup()

        assert not [entry for entry in cap if entry['event'] == 'on_startup_config_change_ignored']
        assert app._sink_manager._circuit_breaker_config.failure_threshold == 42

    async def test_a_replaced_config_object_is_followed_too(self):
        """A handler may return a fresh DrakkarConfig rather than mutating."""

        class ReplacesTheConfig(NoopHandler):
            async def on_startup(self, config: DrakkarConfig) -> DrakkarConfig:
                return make_config(sinks={'delivery_timeout_seconds': 3.0})

        app = DrakkarApp(handler=ReplacesTheConfig(), config=make_config())

        await app._lifecycle._run_on_startup()

        assert app._sink_manager._delivery_timeout_seconds == 3.0

    def test_the_table_no_longer_claims_the_sink_settings_are_consumed_early(self):
        """Both backends must list the same settings — Go builds its sinks
        after the hook too."""
        paths = {setting.path for setting in SETTINGS_CONSUMED_BEFORE_ON_STARTUP}

        assert 'sinks.circuit_breaker' not in paths
        assert 'sinks.delivery_timeout_seconds' not in paths
