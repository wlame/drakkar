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


@pytest.mark.parametrize('value', ['postgres', '', 'NONE'])
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
