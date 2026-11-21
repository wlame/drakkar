"""The ``split`` section: defaults, ranges, env, resolved names, the load checks and the summary token."""

import os

import pytest
import yaml

from drakkar.app import DrakkarApp
from drakkar.config import DrakkarConfig, SplitConfig, load_config
from drakkar.config.root import SPLIT_WITHOUT_KAFKA_SOURCE_MESSAGE
from tests.conftest import kafka_connections
from tests.test_sources_validation import KafkaOnlyHandler

STAGE_MESSAGE = (
    "sinks.kafka.final.depends_on: '{dependency}' is in the other split stage; "
    'a sink depends only on sinks of its own stage'
)


def _document(split: dict | None = None, **sections) -> dict:
    """A config with the Kafka source on, the bundle fetch off and the given ``split`` block."""
    document = {
        'connections': kafka_connections(),
        'sources': {'kafka': {'enabled': True}},
        'ui': {'release': {'enabled': False}},
        'split': split or {},
    }
    document.update(sections)
    return document


def _load(tmp_path, monkeypatch, document: dict, env: dict | None = None):
    """Load ``document`` through ``load_config`` with only the given ``DK_*`` variables set."""
    for name in list(os.environ):
        if name.startswith('DK_'):
            monkeypatch.delenv(name)
    for name, value in (env or {}).items():
        monkeypatch.setenv(name, value)
    path = tmp_path / 'drakkar.yaml'
    path.write_text(yaml.safe_dump(document), encoding='utf-8')
    return load_config(path)


def _fails_with(tmp_path, monkeypatch, document: dict) -> str:
    with pytest.raises(ValueError) as raised:
        _load(tmp_path, monkeypatch, document)
    return str(raised.value)


def _sinks(**dependencies) -> dict:
    """Kafka sinks ``first``, ``second`` and ``final``; ``dependencies`` maps a name to its ``depends_on``."""
    return {
        'kafka': {
            name: {'topic': f'{name}-topic', 'depends_on': dependencies.get(name, [])}
            for name in ('first', 'second', 'final')
        }
    }


def test_split_defaults():
    split = SplitConfig()

    assert (
        split.enabled,
        split.connection,
        split.parts_topic,
        split.results_topic,
        split.parts_group,
        split.results_group,
        split.aggregate_sinks,
        split.job_timeout,
        split.idle_timeout,
        split.max_attempts,
        split.max_part_data,
    ) == (False, 'main', '', '', '', '', [], 86400, 0, 3, 65536)


def test_enabled_split_without_the_kafka_source_fails(tmp_path, monkeypatch):
    document = _document({'enabled': True}, sources={'kafka': {'enabled': False}, 'http': {'enabled': True}})

    assert SPLIT_WITHOUT_KAFKA_SOURCE_MESSAGE in _fails_with(tmp_path, monkeypatch, document)


def test_split_connection_that_is_not_declared_fails(tmp_path, monkeypatch):
    message = _fails_with(tmp_path, monkeypatch, _document({'enabled': True, 'connection': 'internal'}))

    assert "split.connection 'internal' is not defined under connections.kafka" in message


def test_disabled_split_ignores_an_undeclared_connection(tmp_path, monkeypatch):
    config = _load(tmp_path, monkeypatch, _document({'connection': 'internal'}))

    assert config.split.connection == 'internal'
    assert config.split_connection is None


def test_enabled_split_gives_its_connection(tmp_path, monkeypatch):
    config = _load(tmp_path, monkeypatch, _document({'enabled': True}))

    assert config.split_connection is config.connections.kafka['main']


def test_aggregate_sink_that_is_not_configured_fails(tmp_path, monkeypatch):
    document = _document({'enabled': True, 'aggregate_sinks': ['kafka/missing']})

    message = _fails_with(tmp_path, monkeypatch, document)

    assert "split.aggregate_sinks: 'kafka/missing' is not a configured sink; write '<type>/<name>'" in message


def test_disabled_split_does_not_look_an_aggregate_sink_up(tmp_path, monkeypatch):
    config = _load(tmp_path, monkeypatch, _document({'aggregate_sinks': ['kafka/missing']}))

    assert config.split.aggregate_sinks == ['kafka/missing']


@pytest.mark.parametrize('entry', ['missing', 'kafka/', '/final', 'kafka/a/b', ''])
@pytest.mark.parametrize('enabled', [True, False])
def test_aggregate_sink_entry_of_a_wrong_form_fails_also_with_the_feature_off(entry, enabled, tmp_path, monkeypatch):
    document = _document({'enabled': enabled, 'aggregate_sinks': [entry]}, sinks=_sinks())

    message = _fails_with(tmp_path, monkeypatch, document)

    assert f"split.aggregate_sinks: '{entry}' is not a configured sink; write '<type>/<name>'" in message


def test_aggregate_sinks_come_back_as_type_and_name_pairs(tmp_path, monkeypatch):
    config = _load(
        tmp_path,
        monkeypatch,
        _document({'aggregate_sinks': ['kafka/final']}, sinks=_sinks()),
    )

    assert config.split_aggregate_sinks == frozenset({('kafka', 'final')})


@pytest.mark.parametrize(
    ('split', 'expected'),
    [
        ({'job_timeout': '1h', 'idle_timeout': '1h'}, 'split.idle_timeout (1h) must be < split.job_timeout (1h)'),
        ({'job_timeout': '1h', 'idle_timeout': '2h'}, 'split.idle_timeout (2h) must be < split.job_timeout (1h)'),
        (
            {'job_timeout': 0, 'idle_timeout': 0},
            'split.job_timeout and split.idle_timeout cannot both be 0: a job with a lost part would never end',
        ),
    ],
)
def test_timeouts_out_of_order_fail_the_load(split, expected, tmp_path, monkeypatch):
    assert expected in _fails_with(tmp_path, monkeypatch, _document(split))


def test_job_timeout_zero_with_an_idle_timeout_loads(tmp_path, monkeypatch):
    config = _load(tmp_path, monkeypatch, _document({'job_timeout': 0, 'idle_timeout': '10m'}))

    assert (config.split.job_timeout, config.split.idle_timeout) == (0, 600)


@pytest.mark.parametrize(
    ('split', 'expected'),
    [
        ({'job_timeout': '30s'}, 'split.job_timeout must be 0 or from 1m to 30d, got 30s'),
        ({'job_timeout': '31d'}, 'split.job_timeout must be 0 or from 1m to 30d, got 31d'),
        ({'idle_timeout': '30s'}, 'split.idle_timeout must be 0 or from 1m to 30d, got 30s'),
        ({'max_part_data': '512B'}, 'split.max_part_data must be from 1KiB to 512KiB, got 512B'),
        ({'max_part_data': '513KiB'}, 'split.max_part_data must be from 1KiB to 512KiB, got 513KiB'),
        ({'max_attempts': 0}, 'split.max_attempts'),
        ({'max_attempts': 11}, 'split.max_attempts'),
    ],
)
def test_a_value_out_of_its_range_fails_the_load(split, expected, tmp_path, monkeypatch):
    assert expected in _fails_with(tmp_path, monkeypatch, _document(split))


@pytest.mark.parametrize(
    ('split', 'dlq', 'first', 'second', 'topic'),
    [
        (
            {'enabled': True, 'parts_topic': 'input-events'},
            {},
            'sources.kafka.topic',
            'split.parts_topic',
            'input-events',
        ),
        (
            {'enabled': True, 'results_topic': 'input-events'},
            {},
            'sources.kafka.topic',
            'split.results_topic',
            'input-events',
        ),
        (
            {'enabled': True, 'parts_topic': 'same', 'results_topic': 'same'},
            {},
            'split.parts_topic',
            'split.results_topic',
            'same',
        ),
        ({'enabled': True, 'parts_topic': 'late'}, {'topic': 'late'}, 'split.parts_topic', 'dlq.topic', 'late'),
        ({'enabled': True}, {'topic': 'input-events'}, 'sources.kafka.topic', 'dlq.topic', 'input-events'),
    ],
)
def test_the_four_topics_must_differ(split, dlq, first, second, topic, tmp_path, monkeypatch):
    message = _fails_with(tmp_path, monkeypatch, _document(split, dlq=dlq))

    assert f"split: {first} and {second} name the same topic '{topic}'" in message


@pytest.mark.parametrize(
    ('split', 'dlq'),
    [
        ({}, {'topic': 'input-events'}),
        ({'parts_topic': 'input-events'}, {}),
        ({'parts_topic': 'same', 'results_topic': 'same'}, {}),
    ],
)
def test_topic_check_does_not_run_with_the_feature_off(split, dlq, tmp_path, monkeypatch):
    config = _load(tmp_path, monkeypatch, _document(split, dlq=dlq))

    assert config.split.enabled is False


def test_http_only_worker_with_a_leftover_kafka_topic_equal_to_its_dlq_topic_loads(tmp_path, monkeypatch):
    document = _document(dlq={'topic': 'events'})
    document['sources'] = {'kafka': {'enabled': False, 'topic': 'events'}, 'http': {'enabled': True}}

    assert _load(tmp_path, monkeypatch, document).dlq.topic == 'events'


@pytest.mark.parametrize('max_attempts', [0, 11])
def test_max_attempts_out_of_range_names_the_key_and_the_bound(max_attempts, tmp_path, monkeypatch):
    message = _fails_with(tmp_path, monkeypatch, _document({'max_attempts': max_attempts}))

    bound = 'greater than or equal to 1' if max_attempts == 0 else 'less than or equal to 10'
    assert 'split.max_attempts' in message and bound in message


def test_resolved_names_with_empty_keys():
    config = DrakkarConfig(
        connections=kafka_connections(),
        sources={'kafka': {'enabled': True, 'topic': 'requests', 'consumer_group': 'workers'}},
        ui={'release': {'enabled': False}},
    )

    assert (
        config.resolved_split_parts_topic,
        config.resolved_split_results_topic,
        config.resolved_split_parts_group,
        config.resolved_split_results_group,
    ) == (
        'requests_workers_split_parts',
        'requests_workers_split_results',
        'workers_split_parts',
        'workers_split_results',
    )


def test_resolved_names_with_set_keys():
    config = DrakkarConfig(
        connections=kafka_connections(),
        sources={'kafka': {'enabled': True}},
        split={'parts_topic': 'p', 'results_topic': 'r', 'parts_group': 'pg', 'results_group': 'rg'},
        ui={'release': {'enabled': False}},
    )

    assert (
        config.resolved_split_parts_topic,
        config.resolved_split_results_topic,
        config.resolved_split_parts_group,
        config.resolved_split_results_group,
    ) == ('p', 'r', 'pg', 'rg')


def test_split_stage_dependency_across_stages_fails(tmp_path, monkeypatch):
    document = _document({'enabled': True, 'aggregate_sinks': ['kafka/final']}, sinks=_sinks(final=['kafka/first']))

    assert STAGE_MESSAGE.format(dependency='kafka/first') in _fails_with(tmp_path, monkeypatch, document)


def test_split_stage_dependency_of_a_part_sink_on_an_aggregate_sink_fails(tmp_path, monkeypatch):
    document = _document({'enabled': True, 'aggregate_sinks': ['kafka/final']}, sinks=_sinks(first=['kafka/final']))

    message = _fails_with(tmp_path, monkeypatch, document)

    assert "sinks.kafka.first.depends_on: 'kafka/final' is in the other split stage" in message


def test_split_stage_dependency_inside_one_stage_loads(tmp_path, monkeypatch):
    document = _document(
        {'enabled': True, 'aggregate_sinks': ['kafka/final', 'kafka/second']},
        sinks=_sinks(final=['kafka/second'], first=[]),
    )

    assert _load(tmp_path, monkeypatch, document).split.enabled is True


def test_stage_rule_does_not_run_with_the_feature_off(tmp_path, monkeypatch):
    document = _document({'aggregate_sinks': ['kafka/final']}, sinks=_sinks(final=['kafka/first']))

    assert _load(tmp_path, monkeypatch, document).split.enabled is False


def test_env_override_reaches_the_section(tmp_path, monkeypatch):
    config = _load(tmp_path, monkeypatch, _document(), env={'DK_SPLIT__JOB_TIMEOUT': '2h'})

    assert config.split.job_timeout == 7200


def test_unknown_key_under_split_fails_with_the_unknown_keys_message(tmp_path, monkeypatch):
    message = _fails_with(tmp_path, monkeypatch, _document({'job_timout': '1h'}))

    assert "split.job_timout (did you mean 'job_timeout'?)" in message


@pytest.mark.parametrize(('enabled', 'token'), [(True, ' split=on'), (False, ' split=off')])
def test_summary_has_the_split_token_after_the_sources(enabled, token):
    config = DrakkarConfig(
        connections=kafka_connections(),
        sources={'kafka': {'enabled': True}},
        split={'enabled': enabled},
        ui={'release': {'enabled': False}},
    )

    line = config.config_summary('w-1', 'search')

    assert '100poll]' + token + ' exec=' in line


def _app() -> DrakkarApp:
    config = DrakkarConfig(
        connections=kafka_connections(),
        sources={'kafka': {'enabled': True}},
        ui={'release': {'enabled': False}},
        recorder={'enabled': False},
    )
    return DrakkarApp(handler=KafkaOnlyHandler(), config=config, worker_id='w-1')


def test_check_split_runs_again_after_on_startup():
    app = _app()
    app._config.split.enabled = True
    app._config.sources.kafka.enabled = False
    app._config.sources.http.enabled = True

    with pytest.raises(ValueError, match=SPLIT_WITHOUT_KAFKA_SOURCE_MESSAGE):
        app._lifecycle._check_config_after_startup()


class SplitHandler(KafkaOnlyHandler):
    async def split(self, message):
        yield b''


def test_a_worker_that_reads_parts_is_checked_as_one_when_on_startup_turns_the_key_off():
    config = DrakkarConfig(
        connections=kafka_connections(),
        sources={'kafka': {'enabled': True}},
        split={'enabled': True},
        ui={'release': {'enabled': False}},
        recorder={'enabled': False},
    )
    app = DrakkarApp(handler=SplitHandler(), config=config, worker_id='w-1')
    assert app.parts_source is not None
    # What ``on_startup`` did: the change of ``split.enabled`` is ignored, the other one is not.
    app._config.split.enabled = False
    app._config.split.connection = 'gone'

    with pytest.raises(ValueError) as raised:
        app._lifecycle._check_config_after_startup()

    assert "split.connection 'gone' is not defined under connections.kafka" in str(raised.value)


def test_stage_rule_leaves_a_dependency_that_is_no_sink_to_the_sink_check(tmp_path, monkeypatch):
    document = _document({'enabled': True, 'aggregate_sinks': ['kafka/final']}, sinks=_sinks(final=['kafka/ghost']))

    message = _fails_with(tmp_path, monkeypatch, document)

    assert "sinks.kafka.final.depends_on: 'kafka/ghost' is not a configured sink; write '<type>/<name>'" in message
    assert 'split stage' not in message


def test_stage_rule_itself_skips_a_dependency_that_is_no_sink():
    config = DrakkarConfig(
        connections=kafka_connections(),
        sources={'kafka': {'enabled': True}},
        split={'enabled': True, 'aggregate_sinks': ['kafka/final']},
        sinks=_sinks(),
        ui={'release': {'enabled': False}},
    )
    # What a hook can do after the load: name a sink that does not exist.
    config.sinks.kafka['final'].depends_on = ['kafka/ghost']

    config._check_split_stages()
