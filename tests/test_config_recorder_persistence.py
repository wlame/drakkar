"""Recorder persistence key, its derive rule and the memory store bounds."""

import pytest
import yaml
from pydantic import ValidationError

from drakkar.config import DrakkarConfig, load_config
from drakkar.config.root import PERSISTENCE_SQLITE_WITHOUT_DIR_MESSAGE

BASE = {'sources': {'kafka': {'enabled': True}}, 'ui': {'release': {'enabled': False}}}


def _config(storage_dir: str = '/data', **recorder) -> DrakkarConfig:
    return DrakkarConfig(
        sources={'kafka': {'enabled': True}},
        storage={'dir': storage_dir},
        ui={'release': {'enabled': False}, 'recorder': recorder},
    )


def _clear_dk_env(monkeypatch: pytest.MonkeyPatch) -> None:
    import os

    for name in list(os.environ):
        if name.startswith('DK_'):
            monkeypatch.delenv(name)


def test_recorder_memory_defaults():
    recorder = DrakkarConfig(sources={'kafka': {'enabled': True}}).ui.recorder

    assert recorder.persistence is None
    assert recorder.memory.window_seconds == 120
    assert recorder.memory.rows_per_executor == 3000
    assert recorder.memory.max_mib == 256


@pytest.mark.parametrize(
    'persistence,storage_dir,expected',
    [
        (None, '/data', 'sqlite'),
        (None, '', 'none'),
        ('none', '/data', 'none'),
        ('none', '', 'none'),
        ('sqlite', '/data', 'sqlite'),
    ],
)
def test_resolved_persistence_derive_table(persistence, storage_dir, expected):
    recorder = {} if persistence is None else {'persistence': persistence}

    assert _config(storage_dir, **recorder).resolved_persistence == expected


def test_persistence_sqlite_without_storage_dir_fails_even_with_ui_disabled():
    with pytest.raises(ValidationError) as excinfo:
        DrakkarConfig(
            sources={'kafka': {'enabled': True}},
            storage={'dir': ''},
            ui={'enabled': False, 'recorder': {'persistence': 'sqlite'}},
        )

    assert PERSISTENCE_SQLITE_WITHOUT_DIR_MESSAGE in str(excinfo.value)


@pytest.mark.parametrize('value', ['files', '', 'NONE'])
def test_persistence_refuses_unknown_values(value):
    with pytest.raises(ValidationError) as excinfo:
        _config(persistence=value)

    assert 'ui.recorder.persistence' in str(excinfo.value)


def test_resolved_persistence_follows_environment_overrides(monkeypatch, tmp_path):
    path = tmp_path / 'drakkar.yaml'
    path.write_text(yaml.safe_dump({**BASE, 'storage': {'dir': '/x'}}), encoding='utf-8')
    _clear_dk_env(monkeypatch)
    assert load_config(path).resolved_persistence == 'sqlite'

    monkeypatch.setenv('DK_STORAGE__DIR', '')
    assert load_config(path).resolved_persistence == 'none'

    monkeypatch.setenv('DK_UI__RECORDER__PERSISTENCE', 'sqlite')
    with pytest.raises(ValidationError) as excinfo:
        load_config(path)
    assert PERSISTENCE_SQLITE_WITHOUT_DIR_MESSAGE in str(excinfo.value)

    _clear_dk_env(monkeypatch)
    monkeypatch.setenv('DK_UI__RECORDER__MEMORY__WINDOW_SECONDS', '60')
    assert load_config(path).ui.recorder.memory.window_seconds == 60


def test_summary_has_no_recorder_token_for_derived_none():
    line = _config('').config_summary(worker_id='w-1', cluster_name='c')

    assert 'storage=mem fleet=' in line
    assert 'recorder=' not in line


def test_summary_names_none_when_a_folder_is_set():
    line = _config('/data', persistence='none').config_summary(worker_id='w-1', cluster_name='c')

    assert ' storage=/data recorder=none fleet=' in line


POSTGRES_DSN = 'postgresql://drakkar:secret@db:5432/drakkar'
POSTGRES_CONNECTIONS = {'postgres': {'main': {'dsn': POSTGRES_DSN}}}


def _postgres_config(storage_dir: str = '', ui_enabled: bool = True, **postgres) -> DrakkarConfig:
    return DrakkarConfig(
        sources={'kafka': {'enabled': True}},
        storage={'dir': storage_dir},
        connections=POSTGRES_CONNECTIONS,
        ui={
            'enabled': ui_enabled,
            'release': {'enabled': False},
            'recorder': {'persistence': 'postgres', 'postgres': postgres},
        },
    )


def test_recorder_postgres_defaults():
    postgres = DrakkarConfig(sources={'kafka': {'enabled': True}}).ui.recorder.postgres

    assert postgres.connection == 'main'
    assert postgres.table_prefix == ''
    assert postgres.retention_hours == 6
    assert postgres.store_output is False
    assert postgres.batch_rows == 5000
    assert postgres.queue_max_mib == 128
    assert postgres.resilience.timeout_seconds == 5.0


@pytest.mark.parametrize('storage_dir', ['', '/data'])
def test_resolved_persistence_postgres_is_never_derived_and_loads_without_storage_dir(storage_dir):
    assert _postgres_config(storage_dir).resolved_persistence == 'postgres'
    # Without the key a storage.dir still derives sqlite or none, never postgres.
    derived = _config(storage_dir, **{}).resolved_persistence
    assert derived == ('sqlite' if storage_dir else 'none')


def test_postgres_connection_is_checked_even_with_ui_disabled():
    with pytest.raises(ValidationError) as excinfo:
        DrakkarConfig(
            sources={'kafka': {'enabled': True}},
            ui={'enabled': False, 'recorder': {'persistence': 'postgres'}},
        )

    assert "ui.recorder.postgres.connection 'main' is not defined under connections.postgres" in str(excinfo.value)


def test_postgres_connection_key_names_another_entry():
    config = DrakkarConfig(
        sources={'kafka': {'enabled': True}},
        connections={'postgres': {'history': {'dsn': POSTGRES_DSN}}},
        ui={
            'release': {'enabled': False},
            'recorder': {'persistence': 'postgres', 'postgres': {'connection': 'history'}},
        },
    )

    assert config.ui.recorder.postgres.connection == 'history'


def test_postgres_keys_are_not_checked_for_a_connection_in_other_modes():
    DrakkarConfig(
        sources={'kafka': {'enabled': True}},
        ui={'release': {'enabled': False}, 'recorder': {'persistence': 'none'}},
    )


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
        assert _postgres_config(table_prefix=prefix).ui.recorder.postgres.table_prefix == prefix
        return
    with pytest.raises(ValidationError) as excinfo:
        _postgres_config(table_prefix=prefix)

    assert f"ui.recorder.postgres.table_prefix '{prefix}' must be empty, or a lower-case letter" in str(excinfo.value)


def test_postgres_keys_by_environment(monkeypatch, tmp_path):
    path = tmp_path / 'drakkar.yaml'
    path.write_text(yaml.safe_dump({**BASE, 'connections': POSTGRES_CONNECTIONS}), encoding='utf-8')
    _clear_dk_env(monkeypatch)
    monkeypatch.setenv('DK_UI__RECORDER__PERSISTENCE', 'postgres')
    monkeypatch.setenv('DK_UI__RECORDER__POSTGRES__TABLE_PREFIX', 'heavy')
    monkeypatch.setenv('DK_UI__RECORDER__POSTGRES__RESILIENCE__RETRIES', '0')

    postgres = load_config(path).ui.recorder.postgres

    assert postgres.table_prefix == 'heavy'
    assert postgres.resilience.retries == 0


@pytest.mark.parametrize(
    'storage_dir,expected',
    [('', ' storage=mem recorder=postgres fleet='), ('/data', ' storage=/data recorder=postgres fleet=')],
)
def test_summary_has_recorder_postgres_token_with_and_without_storage_dir(storage_dir, expected):
    line = _postgres_config(storage_dir).config_summary(worker_id='w-1', cluster_name='search')

    assert expected in line
