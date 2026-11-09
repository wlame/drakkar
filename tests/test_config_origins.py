"""Where each config value came from: the loader records the keys of the YAML document and of DK_ variables."""

import os
from pathlib import Path

import pytest

from drakkar.config import load_config_with_origins
from drakkar.config.origins import ConfigOrigins, leaf_paths

YAML_TEXT = """
connections:
  kafka:
    main: {}
sources:
  kafka:
    enabled: true
    topic: input-events
executor:
  max_executors: 6
  env:
    SERVICE_URL: http://service
sinks:
  kafka:
    results_out:
      topic: results
ui:
  release:
    enabled: false
"""


@pytest.fixture
def config_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    for name in [name for name in os.environ if name.startswith('DK_')]:
        monkeypatch.delenv(name)
    path = tmp_path / 'drakkar.yaml'
    path.write_text(YAML_TEXT)
    return path


def test_leaf_paths_lists_scalars_and_lists_and_skips_empty_maps():
    tree = {'a': {'b': 1, 'c': [1, 2], 'empty': {}}, 'd': 'x'}

    assert leaf_paths(tree) == {('a', 'b'), ('a', 'c'), ('d',)}


def test_leaf_paths_of_a_value_that_is_not_a_map_is_empty():
    assert leaf_paths(None) == frozenset()


@pytest.mark.parametrize(
    ('path', 'expected'),
    [
        (('executor', 'max_executors'), 'env'),
        (('executor', 'task_timeout'), 'yaml'),
        (('executor', 'env'), 'yaml'),
        (('executor', 'window_size'), 'default'),
        (('sinks', 'kafka', 'results_out', 'topic'), 'default'),
    ],
)
def test_origin_of_prefers_env_then_yaml_then_default(path, expected):
    origins = ConfigOrigins(
        yaml_paths=frozenset({('executor', 'task_timeout'), ('executor', 'env', 'SERVICE_URL')}),
        env_paths=frozenset({('executor', 'max_executors')}),
    )

    assert origins.origin_of(path) == expected


def test_load_config_with_origins_records_yaml_and_env_keys(config_file, monkeypatch):
    monkeypatch.setenv('DK_EXECUTOR__MAX_EXECUTORS', '8')
    monkeypatch.setenv('DK_SINKS__KAFKA__RESULTS_OUT__FLUSH_TIMEOUT', '10s')

    config, origins = load_config_with_origins(config_file)

    assert config.executor.max_executors == 8
    assert origins.origin_of(('executor', 'max_executors')) == 'env'
    assert origins.origin_of(('sources', 'kafka', 'topic')) == 'yaml'
    assert origins.origin_of(('executor', 'env')) == 'yaml'
    assert origins.origin_of(('sinks', 'kafka', 'results_out', 'flush_timeout')) == 'env'
    assert origins.origin_of(('sinks', 'kafka', 'results_out', 'topic')) == 'yaml'
    assert origins.origin_of(('connections', 'kafka', 'main', 'brokers')) == 'default'
