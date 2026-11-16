"""The ``sources.kafka.detach`` block and ``ui.detach_enabled``: defaults, ranges, env and the load checks."""

import os

import pytest
import yaml

from drakkar.app import DrakkarApp
from drakkar.config import DrakkarConfig, load_config
from drakkar.config.root import DETACH_WITHOUT_AUTODISCOVER_MESSAGE, UI_DETACH_WITHOUT_FEATURE_MESSAGE
from drakkar.config.sources import KafkaSourceConfig
from tests.conftest import kafka_connections
from tests.test_sources_validation import KafkaOnlyHandler

FILESYSTEM_AUTODISCOVER = {'enabled': True, 'provider': 'filesystem'}


def _load(tmp_path, monkeypatch, document: dict):
    """Load ``document`` through ``load_config`` with no ``DK_*`` variable of the host set."""
    for name in list(os.environ):
        if name.startswith('DK_'):
            monkeypatch.delenv(name)
    path = tmp_path / 'drakkar.yaml'
    path.write_text(yaml.safe_dump(document), encoding='utf-8')
    return load_config(path)


def _kafka_document(detach: dict, **sections) -> dict:
    """A config with the Kafka source on, the given detach block and the bundle fetch off."""
    return {
        'connections': kafka_connections(),
        'sources': {'kafka': {'enabled': True, 'detach': detach}},
        'ui': {'release': {'enabled': False}, **sections.pop('ui', {})},
        **sections,
    }


def test_detach_defaults():
    detach = KafkaSourceConfig().detach

    assert (
        detach.enabled,
        detach.max_detached,
        detach.min_attached,
        detach.default_duration,
        detach.max_duration,
        detach.cooldown,
        detach.lease,
    ) == (False, 1, 1, 3600, 86400, 300, 60)


def test_detach_enabled_without_autodiscover_fails_the_load(tmp_path, monkeypatch):
    with pytest.raises(ValueError) as raised:
        _load(tmp_path, monkeypatch, _kafka_document({'enabled': True}))

    assert DETACH_WITHOUT_AUTODISCOVER_MESSAGE in str(raised.value)


def test_detach_enabled_with_autodiscover_loads(tmp_path, monkeypatch):
    config = _load(
        tmp_path,
        monkeypatch,
        _kafka_document(
            {'enabled': True},
            autodiscover=FILESYSTEM_AUTODISCOVER,
            storage={'shared_dir': str(tmp_path / 'shared')},
        ),
    )

    assert config.sources.kafka.detach.enabled is True


def test_detach_enabled_with_the_kafka_source_off_loads(tmp_path, monkeypatch):
    config = _load(
        tmp_path,
        monkeypatch,
        {
            'sources': {'kafka': {'detach': {'enabled': True}}, 'http': {'enabled': True}},
            'ui': {'release': {'enabled': False}},
        },
    )

    assert config.sources.kafka.detach.enabled is True


def test_default_duration_above_max_duration_fails(tmp_path, monkeypatch):
    with pytest.raises(ValueError) as raised:
        _load(tmp_path, monkeypatch, _kafka_document({'default_duration': '2h', 'max_duration': '1h'}))

    assert 'sources.kafka.detach.default_duration (2h) must be <= sources.kafka.detach.max_duration (1h)' in str(
        raised.value
    )


def test_ui_detach_without_the_feature_fails(tmp_path, monkeypatch):
    with pytest.raises(ValueError) as raised:
        _load(tmp_path, monkeypatch, _kafka_document({'enabled': False}, ui={'detach_enabled': True}))

    assert UI_DETACH_WITHOUT_FEATURE_MESSAGE in str(raised.value)


def test_ui_detach_with_the_feature_loads(tmp_path, monkeypatch):
    config = _load(
        tmp_path,
        monkeypatch,
        _kafka_document(
            {'enabled': True},
            ui={'detach_enabled': True},
            autodiscover=FILESYSTEM_AUTODISCOVER,
            storage={'shared_dir': str(tmp_path / 'shared')},
        ),
    )

    assert config.ui.detach_enabled is True


@pytest.mark.parametrize(
    ('detach', 'expected'),
    [
        ({'lease': '39s'}, 'sources.kafka.detach.lease must be from 40s to 1d, got 39s'),
        ({'max_duration': '8d'}, 'sources.kafka.detach.max_duration must be from 10s to 7d, got 8d'),
        ({'default_duration': '5s'}, 'sources.kafka.detach.default_duration must be from 10s to 7d, got 5s'),
        ({'cooldown': '25h'}, 'sources.kafka.detach.cooldown must be at most 1d, got 1d1h'),
        ({'max_detached': 0}, 'sources.kafka.detach.max_detached'),
        ({'max_detached': 101}, 'sources.kafka.detach.max_detached'),
        ({'min_attached': 0}, 'sources.kafka.detach.min_attached'),
        ({'min_attached': 1001}, 'sources.kafka.detach.min_attached'),
    ],
)
def test_a_detach_value_out_of_its_range_fails_the_load(detach, expected, tmp_path, monkeypatch):
    with pytest.raises(ValueError) as raised:
        _load(tmp_path, monkeypatch, _kafka_document(detach))

    assert expected in str(raised.value)


def test_env_override_reaches_the_block(tmp_path, monkeypatch):
    path = tmp_path / 'drakkar.yaml'
    path.write_text(yaml.safe_dump(_kafka_document({})), encoding='utf-8')
    for name in list(os.environ):
        if name.startswith('DK_'):
            monkeypatch.delenv(name)
    monkeypatch.setenv('DK_SOURCES__KAFKA__DETACH__MAX_DETACHED', '3')

    assert load_config(path).sources.kafka.detach.max_detached == 3


def _app() -> DrakkarApp:
    config = DrakkarConfig(
        connections=kafka_connections(),
        sources={'kafka': {'enabled': True}},
        ui={'release': {'enabled': False}},
        recorder={'enabled': False},
    )
    return DrakkarApp(handler=KafkaOnlyHandler(), config=config, worker_id='w-1')


def test_check_detach_runs_again_after_on_startup():
    app = _app()
    app._config.sources.kafka.detach.enabled = True

    with pytest.raises(ValueError, match=DETACH_WITHOUT_AUTODISCOVER_MESSAGE):
        app._lifecycle._check_config_after_startup()


def test_a_hook_that_turns_the_ui_switch_on_alone_gets_the_load_message():
    app = _app()
    app._config.ui.detach_enabled = True

    with pytest.raises(ValueError, match=UI_DETACH_WITHOUT_FEATURE_MESSAGE):
        app._lifecycle._check_config_after_startup()
