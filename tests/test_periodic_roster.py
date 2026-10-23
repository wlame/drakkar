"""The periodic roster: discovery, the ``periodic.tasks`` overrides, the startup checks and the definition hash."""

import asyncio
import time
from unittest.mock import MagicMock

import pytest
from structlog.testing import capture_logs

from drakkar.app import DrakkarApp
from drakkar.cluster_tasks import ClusterTask, SlotContext
from drakkar.config import DrakkarConfig
from drakkar.cronexpr import CronNeverFiresError
from drakkar.lifecycle import user_cluster_task
from drakkar.periodic import PeriodicMeta, periodic
from drakkar.periodic_roster import PeriodicConfigError, RosterTask, archive_definition, resolve_periodic_roster
from drakkar.schedule import ARCHIVE_TASK_NAME, ClusterTaskDefinition, cluster_tasks_hash
from tests.test_sources_validation import KafkaOnlyHandler

NOW_EPOCH = int(time.time())


class RatesHandler:
    """Two cluster tasks and one worker task, as a handler declares them."""

    @periodic(cron='*/10 * * * *', scope='cluster')
    async def refresh_rates(self) -> None:
        return None

    @periodic(60, scope='cluster', lease_seconds=90)
    async def rebuild_index(self) -> None:
        return None

    @periodic(5)
    async def sweep(self) -> None:
        return None


class RatesKafkaHandler(KafkaOnlyHandler):
    @periodic(60, scope='cluster')
    async def rebuild_index(self) -> None:
        return None


def _config(*, archive: bool = False, tasks: dict | None = None, **values) -> DrakkarConfig:
    """A config whose archive runs only when ``archive`` is true."""
    return DrakkarConfig(
        sources={'kafka': {'enabled': True}},
        ui={'enabled': archive, 'release': {'enabled': False}},
        periodic={'tasks': tasks or {}},
        **values,
    )


def _roster(tasks: dict | None = None, *, archive: bool = False, handler: object | None = None):
    return resolve_periodic_roster(
        handler if handler is not None else RatesHandler(), _config(archive=archive, tasks=tasks), now_epoch=NOW_EPOCH
    )


def test_roster_splits_worker_and_cluster_tasks_and_fills_the_default_lease():
    roster = _roster()

    assert [task.name for task in roster.worker_tasks] == ['sweep']
    assert [task.name for task in roster.cluster_tasks] == ['rebuild_index', 'refresh_rates']
    refresh = roster.cluster_tasks[1].meta
    assert (refresh.interval, refresh.cron, refresh.lease_seconds) == (None, '*/10 * * * *', 60)
    assert roster.definitions == (
        ClusterTaskDefinition(name='rebuild_index', interval=60, cron=None, lease_seconds=90),
        ClusterTaskDefinition(name='refresh_rates', interval=None, cron='*/10 * * * *', lease_seconds=60),
    )
    assert roster.cluster_hash == cluster_tasks_hash(roster.definitions)
    assert roster.cluster_task_count == 2


def test_roster_override_replaces_cron_with_interval():
    roster = _roster({'refresh_rates': {'interval': 600}})

    (definition,) = [d for d in roster.definitions if d.name == 'refresh_rates']
    assert definition == ClusterTaskDefinition(name='refresh_rates', interval=600, cron=None, lease_seconds=60)
    # Read from YAML as a float; the definition hash needs a JSON integer.
    assert type(definition.interval) is int
    meta = roster.cluster_tasks[1].meta
    assert (meta.interval, meta.cron) == (600, None)


def test_roster_override_replaces_interval_with_cron_and_the_lease():
    roster = _roster({'rebuild_index': {'cron': '0 * * * *', 'lease_seconds': 120}})

    assert roster.definitions[0] == ClusterTaskDefinition(
        name='rebuild_index', interval=None, cron='0 * * * *', lease_seconds=120
    )


def test_roster_whole_number_floats_become_ints():
    class FloatHandler:
        @periodic(30.0, scope='cluster', lease_seconds=40.0)  # type: ignore[arg-type]
        async def compact(self) -> None:
            return None

    roster = _roster(handler=FloatHandler())

    (definition,) = roster.definitions
    assert (definition.interval, definition.lease_seconds) == (30, 40)
    assert (type(definition.interval), type(definition.lease_seconds)) == (int, int)
    assert roster.cluster_hash == cluster_tasks_hash(
        [ClusterTaskDefinition(name='compact', interval=30, cron=None, lease_seconds=40)]
    )


def test_roster_override_enabled_false_drops_the_task_from_the_hash():
    roster = _roster({'refresh_rates': {'enabled': False}, 'sweep': {'enabled': False}})

    assert [task.name for task in roster.cluster_tasks] == ['rebuild_index']
    assert roster.worker_tasks == ()
    assert roster.cluster_hash == cluster_tasks_hash(
        [ClusterTaskDefinition(name='rebuild_index', interval=60, cron=None, lease_seconds=90)]
    )


def test_roster_disabled_override_is_still_checked():
    with pytest.raises(PeriodicConfigError) as raised:
        _roster({'sweep': {'enabled': False, 'lease_seconds': 60}})

    assert str(raised.value) == 'periodic.tasks.sweep: lease_seconds is only for cluster tasks'


def test_roster_override_for_an_undeclared_task_fails_startup():
    with pytest.raises(PeriodicConfigError) as raised:
        _roster({'zz_unknown': {}, 'ghost': {}})

    assert str(raised.value) == 'periodic.tasks.ghost: the handler declares no periodic task of that name'


def test_roster_override_interval_5_on_a_cluster_task_fails_startup():
    with pytest.raises(PeriodicConfigError) as raised:
        _roster({'refresh_rates': {'interval': 5}})

    assert str(raised.value) == (
        "periodic.tasks.refresh_rates: a cluster task's interval must be a whole number of seconds and at least 30"
    )


def test_roster_override_fractional_interval_on_a_cluster_task_fails_startup():
    with pytest.raises(PeriodicConfigError, match="cluster task's interval must be a whole number"):
        _roster({'rebuild_index': {'interval': 45.5}})


def test_roster_override_lease_on_a_worker_task_fails_startup():
    with pytest.raises(PeriodicConfigError) as raised:
        _roster({'sweep': {'lease_seconds': 60}})

    assert str(raised.value) == 'periodic.tasks.sweep: lease_seconds is only for cluster tasks'


def test_roster_overrides_are_checked_in_sorted_name_order():
    with pytest.raises(PeriodicConfigError) as raised:
        _roster({'sweep': {'lease_seconds': 60}, 'refresh_rates': {'interval': 5}, 'zz_unknown': {}})

    assert str(raised.value).startswith('periodic.tasks.refresh_rates: ')


def test_roster_worker_task_interval_override_keeps_a_fraction():
    roster = _roster({'sweep': {'interval': 0.5}})

    assert roster.worker_tasks[0].meta.interval == 0.5


def test_roster_hash_includes_history_archive_only_when_it_runs():
    without_archive = _roster()
    with_archive = _roster(archive=True)

    assert not without_archive.runs_archive
    assert ARCHIVE_TASK_NAME not in without_archive.cluster_task_names
    assert with_archive.runs_archive
    assert with_archive.cluster_task_names == ['rebuild_index', 'refresh_rates', ARCHIVE_TASK_NAME]
    assert with_archive.definitions[-1] == ClusterTaskDefinition(
        name=ARCHIVE_TASK_NAME, interval=3600, cron=None, lease_seconds=60
    )
    assert with_archive.cluster_hash == cluster_tasks_hash(without_archive.definitions + with_archive.definitions[-1:])
    assert with_archive.cluster_hash != without_archive.cluster_hash


def test_roster_with_no_cluster_task_hashes_the_empty_list():
    roster = resolve_periodic_roster(object(), _config(), now_epoch=NOW_EPOCH)

    assert roster.definitions == ()
    assert roster.cluster_hash == '4f53cda18c2baa0c'
    assert roster.cluster_task_count == 0


def test_archive_definition_follows_the_archive_schedule():
    config = _config(archive=True, history={'archive': {'cron': '15 * * * *', 'lease_seconds': 300}})

    assert archive_definition(config) == ClusterTaskDefinition(
        name=ARCHIVE_TASK_NAME, interval=None, cron='15 * * * *', lease_seconds=300
    )


def _kafka_app(tmp_path, tasks: dict | None = None) -> DrakkarApp:
    config = DrakkarConfig(
        sources={'kafka': {'enabled': True}},
        storage={'dir': ''},
        ui={'enabled': False, 'release': {'enabled': False}},
        periodic={'tasks': tasks or {}},
    )
    return DrakkarApp(handler=RatesKafkaHandler(), config=config, worker_id='w-1')


def test_roster_filesystem_without_shared_folder_fails_for_an_enabled_cluster_task(tmp_path):
    app = _kafka_app(tmp_path)

    with pytest.raises(ValueError) as raised:
        app._lifecycle._build_periodic_roster()

    assert str(raised.value) == (
        "periodic task 'rebuild_index' has scope cluster, but the filesystem election needs a shared folder "
        '(shared.dir or storage.dir)'
    )


def test_roster_filesystem_without_shared_folder_starts_when_the_cluster_task_is_disabled(tmp_path):
    app = _kafka_app(tmp_path, tasks={'rebuild_index': {'enabled': False}})

    app._lifecycle._build_periodic_roster()

    assert app._cluster_election is None
    assert app._periodic_roster is not None and app._periodic_roster.cluster_task_count == 0


def test_roster_build_sets_the_app_hash_and_an_election_for_every_cluster_task(tmp_path):
    app = DrakkarApp(
        handler=RatesKafkaHandler(),
        config=DrakkarConfig(
            sources={'kafka': {'enabled': True}},
            storage={'dir': str(tmp_path)},
            ui={'enabled': False, 'release': {'enabled': False}},
        ),
        worker_id='w-1',
    )

    app._lifecycle._build_periodic_roster()

    roster = app._periodic_roster
    assert roster is not None
    assert app.cluster_tasks_hash == roster.cluster_hash
    assert app._cluster_election is not None
    assert app._cluster_election.provider_name == 'filesystem'


async def test_user_cluster_task_runs_the_bound_method_with_the_roster_settings():
    calls: list[str] = []

    class Handler:
        @periodic(cron='*/10 * * * *', scope='cluster', on_error='stop')
        async def refresh_rates(self) -> None:
            calls.append('refresh_rates')

    (roster_task,) = _roster(handler=Handler()).cluster_tasks

    cluster_task = user_cluster_task(roster_task)
    await cluster_task.run(SlotContext(slot=0, lease=MagicMock()))

    assert calls == ['refresh_rates']
    assert (cluster_task.name, cluster_task.interval, cluster_task.cron) == ('refresh_rates', None, '*/10 * * * *')
    assert (cluster_task.lease_seconds, cluster_task.on_error, cluster_task.system) == (60, 'stop', False)


def test_a_cluster_task_without_an_election_gets_no_loop(tmp_path):
    app = _kafka_app(tmp_path, tasks={'rebuild_index': {'enabled': False}})
    (roster_task,) = _roster(handler=RatesKafkaHandler()).cluster_tasks

    app._lifecycle._start_cluster_task(user_cluster_task(roster_task))

    assert app._periodic_tasks == []


class _NeverFiringTask(ClusterTask):
    """A cluster task whose slot math raises, as a cron with no fire time left would."""

    def next_slot(self, now: float) -> int:
        raise CronNeverFiresError('the expression has no fire time left')


async def _settle(task: asyncio.Task) -> None:
    """Wait for a loop task to end without raising its exception here."""
    await asyncio.wait({task})
    # The done callbacks run on the next turn of the loop.
    await asyncio.sleep(0)


async def test_cluster_loop_that_raises_outside_a_run_logs_periodic_loop_ended(tmp_path):
    app = DrakkarApp(
        handler=RatesKafkaHandler(),
        config=DrakkarConfig(
            sources={'kafka': {'enabled': True}},
            storage={'dir': str(tmp_path)},
            ui={'enabled': False, 'release': {'enabled': False}},
        ),
        worker_id='w-1',
    )
    app._lifecycle._build_periodic_roster()

    async def body(_ctx: SlotContext) -> None:
        return None

    task = _NeverFiringTask(name='broken', run=body, interval=30, cron=None, lease_seconds=60, on_error='continue')
    with capture_logs() as captured:
        app._lifecycle._start_cluster_task(task)
        await _settle(app._periodic_tasks[-1])

    [ended] = [entry for entry in captured if entry['event'] == 'periodic_loop_ended']
    assert (ended['log_level'], ended['category'], ended['name']) == ('error', 'periodic', 'broken')
    assert ended['error'] == 'the expression has no fire time left'


async def test_worker_loop_that_raises_outside_a_run_logs_periodic_loop_ended(tmp_path):
    app = _kafka_app(tmp_path)

    async def method() -> None:
        return None

    # Neither schedule: the loop raises before its first run.
    meta = PeriodicMeta(interval=None, cron=None, scope='worker', lease_seconds=None, on_error='continue')
    with capture_logs() as captured:
        app._lifecycle._start_worker_task(RosterTask(name='unscheduled', method=method, meta=meta))
        await _settle(app._periodic_tasks[-1])

    [ended] = [entry for entry in captured if entry['event'] == 'periodic_loop_ended']
    assert ended['name'] == 'unscheduled'
    assert 'exactly one of interval and cron' in ended['error']


async def test_cancelled_periodic_loop_logs_nothing(tmp_path):
    app = _kafka_app(tmp_path)

    async def method() -> None:
        return None

    meta = PeriodicMeta(interval=3600, cron=None, scope='worker', lease_seconds=None, on_error='continue')
    with capture_logs() as captured:
        app._lifecycle._start_worker_task(RosterTask(name='hourly', method=method, meta=meta))
        loop_task = app._periodic_tasks[-1]
        await asyncio.sleep(0)
        loop_task.cancel()
        await _settle(loop_task)

    assert [entry for entry in captured if entry['event'] == 'periodic_loop_ended'] == []


@pytest.mark.parametrize(
    ('archive', 'storage', 'started'),
    [(True, True, True), (False, True, False), (True, False, False)],
    ids=['archiving-on', 'archiving-disabled', 'memory-only'],
)
def test_archive_loop_starts_only_when_the_roster_runs_the_archive(tmp_path, archive, storage, started):
    app = DrakkarApp(
        handler=KafkaOnlyHandler(),
        config=DrakkarConfig(
            sources={'kafka': {'enabled': True}},
            storage={'dir': str(tmp_path) if storage else ''},
            ui={'enabled': True, 'release': {'enabled': False}},
            history={'archive': {'enabled': archive}},
        ),
        worker_id='w-1',
    )
    app._lifecycle._build_periodic_roster()
    app._recorder = MagicMock()
    started_tasks: list[ClusterTask] = []
    app._lifecycle._start_cluster_task = started_tasks.append

    app._lifecycle._start_archive_task()

    assert app._periodic_roster is not None and app._periodic_roster.runs_archive is started
    assert started_tasks == ([app._recorder.archive_cluster_task.return_value] if started else [])
