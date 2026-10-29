"""The recorder section: its switch, its provider, its folders and the units of its keys."""

import pytest
import yaml
from pydantic import ValidationError

from drakkar.config import DrakkarConfig, RecorderArchiveConfig, RecorderConfig, load_config
from drakkar.config.root import SQLITE_WITHOUT_DIR_MESSAGE
from tests.conftest import kafka_connections

BASE = {
    'connections': {'kafka': {'main': {}}},
    'sources': {'kafka': {'enabled': True}},
    'ui': {'release': {'enabled': False}},
}


def _config(storage_dir: str = '/data', **recorder) -> DrakkarConfig:
    return DrakkarConfig(
        connections=kafka_connections(),
        sources={'kafka': {'enabled': True}},
        storage={'dir': storage_dir},
        ui={'release': {'enabled': False}},
        recorder=recorder,
    )


def _clear_dk_env(monkeypatch: pytest.MonkeyPatch) -> None:
    import os

    for name in list(os.environ):
        if name.startswith('DK_'):
            monkeypatch.delenv(name)


def test_recorder_memory_defaults():
    recorder = _config().recorder

    assert recorder.enabled is True
    assert recorder.provider == 'sqlite'
    assert recorder.memory.window == 120
    assert recorder.memory.rows_per_executor == 3000
    assert recorder.memory.max_size == 256 * 1024 * 1024


def test_recorder_defaults_print_in_canonical_units():
    dumped = RecorderConfig().model_dump()

    assert dumped['flush_interval'] == '5s'
    assert dumped['memory'] == {'window': '2m', 'rows_per_executor': 3000, 'max_size': '256MiB'}
    assert dumped['capture']['max_stdin'] == '64KiB'
    assert dumped['capture']['min_event_duration'] == '0'
    assert dumped['capture']['min_output_duration'] == '500ms'
    assert dumped['annotations'] == {
        'enabled': True,
        'max_size': '16KiB',
        'max_size_per_call': '256KiB',
        'max_log_size': '2KiB',
    }
    assert dumped['sqlite'] == {
        'rotation_interval': '1h',
        'max_flush_retries': 3,
        'state_snapshots': True,
        'state_interval': '10s',
    }
    assert dumped['search'] == {'max_files': 256, 'timeout': '10s'}
    assert dumped['postgres']['resilience'] == {
        'timeout': '5s',
        'max_retries': 2,
        'backoff': '200ms',
        'max_backoff': '5s',
        'breaker_failures': 5,
        'breaker_cooldown': '30s',
    }


@pytest.mark.parametrize(
    'recorder,storage_dir,expected',
    [
        ({}, '/data', 'sqlite'),
        ({'provider': 'sqlite'}, '/data', 'sqlite'),
        ({'enabled': False}, '/data', 'none'),
        ({'enabled': False}, '', 'none'),
        ({'enabled': False, 'provider': 'postgres'}, '/data', 'none'),
    ],
)
def test_resolved_persistence_follows_enabled_and_provider(recorder, storage_dir, expected):
    assert _config(storage_dir, **recorder).resolved_persistence == expected


@pytest.mark.parametrize('ui_enabled', [True, False], ids=['ui_on', 'ui_off'])
def test_sqlite_with_empty_storage_dir_fails_with_the_message(ui_enabled):
    with pytest.raises(ValidationError) as excinfo:
        DrakkarConfig(
            connections=kafka_connections(),
            sources={'kafka': {'enabled': True}},
            storage={'dir': ''},
            ui={'enabled': ui_enabled, 'release': {'enabled': False}},
        )

    assert (
        'recorder.provider is sqlite but storage.dir is empty; set storage.dir to a folder, '
        'or set recorder.enabled to false'
    ) in str(excinfo.value)


def test_recorder_off_ignores_empty_storage_dir():
    assert _config('', enabled=False).resolved_persistence == 'none'


def test_recorder_off_ignores_an_undeclared_postgres_connection():
    assert _config(enabled=False, provider='postgres').recorder.provider == 'postgres'


@pytest.mark.parametrize('value', ['files', '', 'none', 'NONE'])
def test_provider_refuses_unknown_values(value):
    with pytest.raises(ValidationError) as excinfo:
        _config(provider=value)

    assert 'recorder.provider' in str(excinfo.value)


def test_resolved_persistence_follows_environment_overrides(monkeypatch, tmp_path):
    path = tmp_path / 'drakkar.yaml'
    path.write_text(yaml.safe_dump({**BASE, 'storage': {'dir': '/x'}}), encoding='utf-8')
    _clear_dk_env(monkeypatch)
    assert load_config(path).resolved_persistence == 'sqlite'

    monkeypatch.setenv('DK_RECORDER__ENABLED', 'false')
    assert load_config(path).resolved_persistence == 'none'

    monkeypatch.delenv('DK_RECORDER__ENABLED')
    monkeypatch.setenv('DK_STORAGE__DIR', '')
    with pytest.raises(ValidationError) as excinfo:
        load_config(path)
    assert SQLITE_WITHOUT_DIR_MESSAGE in str(excinfo.value)


def test_memory_window_by_env_with_a_unit(monkeypatch, tmp_path):
    path = tmp_path / 'drakkar.yaml'
    path.write_text(yaml.safe_dump({**BASE, 'storage': {'dir': '/x'}}), encoding='utf-8')
    _clear_dk_env(monkeypatch)
    monkeypatch.setenv('DK_RECORDER__MEMORY__WINDOW', '2m')

    assert load_config(path).recorder.memory.window == 120


def test_summary_has_no_recorder_token_with_the_recorder_off_and_no_folder():
    line = _config('', enabled=False).config_summary(worker_id='w-1', cluster_name='c')

    assert 'storage=mem fleet=' in line
    assert 'recorder=' not in line


def test_summary_names_none_when_a_folder_is_set():
    line = _config('/data', enabled=False).config_summary(worker_id='w-1', cluster_name='c')

    assert ' storage=/data recorder=none fleet=' in line


def test_summary_shows_a_shared_dir_that_differs():
    config = DrakkarConfig(
        connections=kafka_connections(),
        sources={'kafka': {'enabled': True}},
        storage={'dir': '/data', 'shared_dir': '/mnt/fleet'},
    )

    assert ' storage=/data shared=/mnt/fleet fleet=' in config.config_summary(worker_id='w-1', cluster_name='c')


POSTGRES_DSN = 'postgresql://drakkar:secret@db:5432/drakkar'
POSTGRES_CONNECTIONS = {'postgres': {'main': {'dsn': POSTGRES_DSN}}}


def _postgres_config(storage_dir: str = '', ui_enabled: bool = True, **postgres) -> DrakkarConfig:
    return DrakkarConfig(
        sources={'kafka': {'enabled': True}},
        storage={'dir': storage_dir},
        connections={**kafka_connections(), **POSTGRES_CONNECTIONS},
        ui={'enabled': ui_enabled, 'release': {'enabled': False}},
        recorder={'provider': 'postgres', 'postgres': postgres},
    )


def test_recorder_postgres_defaults():
    postgres = RecorderConfig().postgres

    assert postgres.connection == 'main'
    assert postgres.table_prefix == ''
    assert postgres.initial_retention == 24
    assert postgres.store_completed_output is False
    assert postgres.max_output == 64 * 1024
    assert postgres.max_size == 0
    assert postgres.batch_rows == 5000
    assert postgres.max_queue == 128 * 1024 * 1024
    assert postgres.resilience.timeout == 5.0


def test_postgres_table_settings_load_with_units():
    postgres = _postgres_config(
        initial_retention='2d',
        max_output='1MiB',
        max_size='64MiB',
        max_queue='1GiB',
        resilience={
            'timeout': '1500ms',
            'max_retries': 1,
            'backoff': '1s',
            'max_backoff': '1m',
            'breaker_cooldown': '2m',
        },
    ).recorder.postgres

    assert postgres.initial_retention == 48
    assert postgres.max_output == 1024 * 1024
    assert postgres.max_size == 64 * 1024 * 1024
    assert postgres.max_queue == 1024 * 1024 * 1024
    assert postgres.resilience.timeout == 1.5
    assert postgres.resilience.max_retries == 1
    assert postgres.resilience.backoff == 1.0
    assert postgres.resilience.max_backoff == 60.0
    assert postgres.resilience.breaker_cooldown == 120.0


@pytest.mark.parametrize(
    'key,value,expected',
    [
        ('initial_retention', '90m', "must be a whole number of hours, got '90m'"),
        ('initial_retention', 24, "needs a unit, for example 30s; got '24'"),
        ('initial_retention', '169h', 'less than or equal to 168'),
        ('max_queue', 128, "needs a unit, for example 64KiB; got '128'"),
        ('max_queue', '15MiB', 'greater than or equal to 16777216'),
    ],
)
def test_postgres_table_settings_refuse_bad_values(key, value, expected):
    with pytest.raises(ValidationError) as excinfo:
        _postgres_config(**{key: value})

    assert expected in str(excinfo.value)


@pytest.mark.parametrize('storage_dir', ['', '/data'])
def test_provider_postgres_loads_with_or_without_storage_dir(storage_dir):
    assert _postgres_config(storage_dir).resolved_persistence == 'postgres'


def test_postgres_connection_is_checked_even_with_ui_disabled():
    with pytest.raises(ValidationError) as excinfo:
        DrakkarConfig(
            connections=kafka_connections(),
            sources={'kafka': {'enabled': True}},
            ui={'enabled': False},
            recorder={'provider': 'postgres'},
        )

    assert "recorder.postgres.connection 'main' is not defined under connections.postgres" in str(excinfo.value)


def test_postgres_connection_key_names_another_entry():
    config = DrakkarConfig(
        sources={'kafka': {'enabled': True}},
        connections={**kafka_connections(), 'postgres': {'history': {'dsn': POSTGRES_DSN}}},
        ui={'release': {'enabled': False}},
        recorder={'provider': 'postgres', 'postgres': {'connection': 'history'}},
    )

    assert config.recorder.postgres.connection == 'history'


@pytest.mark.parametrize(
    'prefix,ok',
    [
        ('', True),
        ('a', True),
        ('a_1', True),
        ('a' * 24, True),
        ('_a', False),
        ('1a', False),
        ('A', False),
        ('a-b', False),
        ('a' * 25, False),
        ('a b', False),
        ('a\n', False),
    ],
)
def test_table_prefix_rule(prefix, ok):
    if ok:
        assert _postgres_config(table_prefix=prefix).recorder.postgres.table_prefix == prefix
        return
    with pytest.raises(ValidationError) as excinfo:
        _postgres_config(table_prefix=prefix)

    assert f"recorder.postgres.table_prefix '{prefix}' must be empty, or a lower-case letter" in str(excinfo.value)


def test_postgres_keys_by_environment(monkeypatch, tmp_path):
    path = tmp_path / 'drakkar.yaml'
    path.write_text(
        yaml.safe_dump({**BASE, 'connections': {**kafka_connections(), **POSTGRES_CONNECTIONS}}), encoding='utf-8'
    )
    _clear_dk_env(monkeypatch)
    monkeypatch.setenv('DK_RECORDER__PROVIDER', 'postgres')
    monkeypatch.setenv('DK_RECORDER__POSTGRES__TABLE_PREFIX', 'heavy')
    monkeypatch.setenv('DK_RECORDER__POSTGRES__RESILIENCE__MAX_RETRIES', '0')

    postgres = load_config(path).recorder.postgres

    assert postgres.table_prefix == 'heavy'
    assert postgres.resilience.max_retries == 0


@pytest.mark.parametrize(
    'storage_dir,expected',
    [('', ' storage=mem recorder=postgres fleet='), ('/data', ' storage=/data recorder=postgres fleet=')],
)
def test_summary_has_recorder_postgres_token_with_and_without_storage_dir(storage_dir, expected):
    line = _postgres_config(storage_dir).config_summary(worker_id='w-1', cluster_name='search')

    assert expected in line


@pytest.mark.parametrize(
    'sqlite,archive,expected',
    [
        (
            {'rotation_interval': '2h'},
            {'window': '1h'},
            'recorder.archive.window (1h) must be >= recorder.sqlite.rotation_interval (2h)',
        ),
        (
            {'rotation_interval': '2h'},
            {'due_after': '1h'},
            'recorder.archive.due_after (1h) must be >= recorder.sqlite.rotation_interval (2h)',
        ),
        (
            {},
            {'retention': '2d'},
            'recorder.archive.retention (2d) must be 0 (keep archives for ever) or cover the due delay plus one '
            'window: it must be >= recorder.archive.due_after (2d) + recorder.archive.window (1d).',
        ),
    ],
    ids=['window', 'due_after', 'retention'],
)
def test_archive_window_checks_name_the_recorder_paths(sqlite, archive, expected):
    with pytest.raises(ValidationError) as excinfo:
        _config(sqlite=sqlite, archive=archive)

    assert expected in str(excinfo.value)


@pytest.mark.parametrize(
    'archive,expected',
    [
        ({'interval': '30s', 'cron': '0 * * * *'}, 'recorder.archive: give at most one of interval and cron'),
        ({'interval': '29s'}, 'recorder.archive: interval must be a whole number of seconds and at least 30'),
        ({'lease': '39s'}, 'recorder.archive: lease must be a whole number of seconds from 40 to 86400'),
        ({'lease': '25h'}, 'recorder.archive: lease must be a whole number of seconds from 40 to 86400'),
        ({'sweep_after': '59s'}, 'recorder.archive: sweep_after must be at least 60 seconds'),
    ],
)
def test_archive_schedule_checks_name_the_recorder_prefix(archive, expected):
    with pytest.raises(ValidationError) as excinfo:
        RecorderArchiveConfig(**archive)

    assert expected in str(excinfo.value)


def test_archive_loads_whole_units():
    archive = RecorderArchiveConfig(
        window='12h',
        due_after='1d',
        retention='7d',
        merged_retention='1d',
        interval='10m',
        lease='2m',
        sweep_after='1h',
    )

    assert (archive.window, archive.due_after, archive.retention, archive.merged_retention) == (12, 24, 7, 1)
    assert (archive.interval, archive.lease, archive.sweep_after) == (600, 120, 3600)
    assert archive.effective_interval == 600
