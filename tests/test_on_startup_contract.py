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
    reasons_for_settings,
    snapshot_consumed_settings,
)
from drakkar.config import ExecutorConfig, KafkaSinkConfig, LoggingConfig, MetricsConfig
from drakkar.models import ErrorAction, ExecutorError, ExecutorTask
from tests.conftest import kafka_connections


class NoopHandler(BaseDrakkarHandler):
    async def arrange(self, messages, pending):
        return []


def make_config(**overrides) -> DrakkarConfig:
    defaults = {
        'connections': kafka_connections(brokers='localhost:9092'),
        'sources': {'kafka': {'enabled': True, 'topic': 'test-in', 'startup_align': {'enabled': False}}},
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

    def test_the_source_toggles_are_listed(self):
        """Sources are built in ``__init__``, so flipping one in the hook does nothing."""
        paths = {setting.path for setting in SETTINGS_CONSUMED_BEFORE_ON_STARTUP}

        assert 'sources.kafka.enabled' in paths
        assert 'sources.http.enabled' in paths

    def test_a_changed_storage_dir_is_reported(self):
        """The own-files folder is created before the hook runs, so moving it there does nothing."""
        config = make_config()
        before = snapshot_consumed_settings(config)

        config.storage.dir = '/var/lib/elsewhere'

        assert changed_consumed_settings(before, config) == ['storage.dir']

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

        config.cluster.name = 'renamed-in-on_startup'

        assert changed_consumed_settings(before, config) == ['cluster.name']

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

        config.worker.name_env = 'OTHER_WORKER'
        config.app = {'priority_threshold': 1}

        # Table order, not alphabetical — deterministic so the warning text
        # is stable.
        assert changed_consumed_settings(before, config) == ['app', 'worker.name_env']


class TestLifecycleWarning:
    """The warning is emitted by the lifecycle, right after the hook returns."""

    async def test_changing_a_consumed_setting_warns(self):
        class RenamesTheCluster(NoopHandler):
            async def on_startup(self, config: DrakkarConfig) -> DrakkarConfig:
                config.cluster.name = 'renamed-in-on_startup'
                return config

        app = DrakkarApp(handler=RenamesTheCluster(), config=make_config())

        with capture_logs() as cap:
            await app._lifecycle._run_on_startup()

        (warning,) = [entry for entry in cap if entry['event'] == 'on_startup_config_change_ignored']
        assert warning['settings'] == ['cluster.name']
        assert warning['log_level'] == 'warning'
        # The operator needs to know what consumed it, not just that the
        # change was dropped.
        assert warning['reasons'] == ['cluster.name — the worker resolves its cluster name in DrakkarApp.__init__']
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

    async def test_executor_on_failure_set_in_the_hook_is_what_on_error_answers(self):
        class SendsFailuresToTheDLQ(NoopHandler):
            async def on_startup(self, config: DrakkarConfig) -> DrakkarConfig:
                config.executor.on_failure = 'dlq'
                return config

        handler = SendsFailuresToTheDLQ()
        app = DrakkarApp(handler=handler, config=make_config())
        task = ExecutorTask(task_id='t-1', args=['x'], source_offsets=[0])

        await app._lifecycle._run_on_startup()

        assert await handler.on_error(task, ExecutorError(task=task)) is ErrorAction.DLQ


def _config_with_a_sink(**settings) -> DrakkarConfig:
    return make_config(sinks={'kafka': {'results': {'topic': 'out', **settings}}})


def _results_sink(app: DrakkarApp):
    return app._sink_manager.sinks[('kafka', 'results')]


class TestSinkPoliciesAreResolvedAtConstruction:
    """Each sink's policy is resolved in `DrakkarApp.__init__`, so a change of a policy key in `on_startup` is ignored.

    The warning names the key, as for every other early-consumed setting.
    """

    async def _ignored_settings(self, app: DrakkarApp) -> list[str]:
        with capture_logs() as cap:
            await app._lifecycle._run_on_startup()
        warnings = [entry for entry in cap if entry['event'] == 'on_startup_config_change_ignored']
        return warnings[0]['settings'] if warnings else []

    async def test_a_changed_default_is_reported_and_does_not_reach_the_sink(self):
        class TunesTheDefaults(NoopHandler):
            async def on_startup(self, config: DrakkarConfig) -> DrakkarConfig:
                config.sinks.defaults.delivery_timeout = 7.5
                config.sinks.defaults.breaker_failures = 42
                return config

        app = DrakkarApp(handler=TunesTheDefaults(), config=_config_with_a_sink())

        assert await self._ignored_settings(app) == ['sinks.defaults']

        app._build_sinks()
        assert _results_sink(app).delivery_timeout_seconds == 30.0
        assert _results_sink(app)._breaker_failures == 5

    async def test_a_changed_instance_key_is_reported_and_does_not_reach_the_sink(self):
        class TunesTheSink(NoopHandler):
            async def on_startup(self, config: DrakkarConfig) -> DrakkarConfig:
                config.sinks.kafka['results'].delivery_timeout = 7.5
                config.sinks.kafka['results'].depends_on.append('kafka/results')
                return config

        app = DrakkarApp(handler=TunesTheSink(), config=_config_with_a_sink())

        assert await self._ignored_settings(app) == [
            'sinks.kafka.results.delivery_timeout',
            'sinks.kafka.results.depends_on',
        ]

        app._build_sinks()
        assert _results_sink(app).delivery_timeout_seconds == 30.0
        assert app._sink_manager._policies[('kafka', 'results')].depends_on == ()

    async def test_a_replaced_config_object_keeps_the_resolved_policy_too(self):
        class ReplacesTheConfig(NoopHandler):
            async def on_startup(self, config: DrakkarConfig) -> DrakkarConfig:
                return _config_with_a_sink(delivery_timeout='3s')

        app = DrakkarApp(handler=ReplacesTheConfig(), config=_config_with_a_sink())

        assert await self._ignored_settings(app) == ['sinks.kafka.results.delivery_timeout']

        app._build_sinks()
        assert _results_sink(app).delivery_timeout_seconds == 30.0

    async def test_a_sink_added_in_the_hook_is_no_ignored_change_and_gets_its_policy(self):
        class AddsASink(NoopHandler):
            async def on_startup(self, config: DrakkarConfig) -> DrakkarConfig:
                config.sinks.kafka['added'] = KafkaSinkConfig(topic='more', delivery_timeout='5s')
                return config

        app = DrakkarApp(handler=AddsASink(), config=_config_with_a_sink())

        assert await self._ignored_settings(app) == []

        app._build_sinks()
        assert app._sink_manager.sinks[('kafka', 'added')].delivery_timeout_seconds == 5.0

    async def test_a_sink_setting_outside_the_policy_lands(self):
        class TunesTheFlush(NoopHandler):
            async def on_startup(self, config: DrakkarConfig) -> DrakkarConfig:
                config.sinks.kafka['results'].flush_timeout = 7.5
                return config

        app = DrakkarApp(handler=TunesTheFlush(), config=_config_with_a_sink())

        assert await self._ignored_settings(app) == []

        app._build_sinks()
        assert _results_sink(app)._config.flush_timeout == 7.5

    def test_the_reason_names_the_policy_for_an_instance_key(self):
        assert reasons_for_settings(['sinks.kafka.results.max_retries']) == [
            "sinks.kafka.results.max_retries — each sink's policy is resolved in DrakkarApp.__init__"
        ]


class TestTheOwnFolderIsFixedBeforeTheHook:
    """The owner lock is taken on the own folder before `on_startup` runs.

    A `storage.dir` changed in the hook is reported as ignored, and every own
    file still goes to the locked folder.
    """

    async def test_a_changed_storage_dir_does_not_move_the_own_folder(self, tmp_path):
        class MovesTheStorage(NoopHandler):
            async def on_startup(self, config: DrakkarConfig) -> DrakkarConfig:
                config.storage.dir = str(tmp_path / 'elsewhere')
                return config

        app = DrakkarApp(
            handler=MovesTheStorage(),
            config=make_config(storage={'dir': str(tmp_path / 'store')}),
            worker_id='w-1',
        )
        await app._lifecycle._prepare_storage()
        locked_folder = app.own_storage_dir
        try:
            with capture_logs() as cap:
                await app._lifecycle._run_on_startup()

            assert app.own_storage_dir == locked_folder
            assert locked_folder.startswith(str(tmp_path / 'store'))
            ignored = [entry for entry in cap if entry['event'] == 'on_startup_config_change_ignored']
            assert ignored[0]['settings'] == ['storage.dir']
        finally:
            await app._lifecycle._release_owner_lock()
