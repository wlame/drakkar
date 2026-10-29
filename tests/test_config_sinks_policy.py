"""The failure policy, retry budget and dependencies declared on every sink instance.

The shared cases live in ``tests/fixtures/config_contract.json``; these tests
cover the rest: every sink type, the check order and the graph accessors.
"""

import pytest
from pydantic import ValidationError

from drakkar.config import (
    DrakkarConfig,
    ExecutorConfig,
    FileSinkConfig,
    HttpSinkConfig,
    KafkaSinkConfig,
    MongoSinkConfig,
    PostgresSinkConfig,
    RedisSinkConfig,
    SinkPolicy,
    SinksConfig,
)
from tests.test_config_contract import FIXTURE


def _config(**sections) -> DrakkarConfig:
    """A loadable config: one enabled source, bundle fetching off, plus ``sections``."""
    sections.setdefault('sources', {'kafka': {'enabled': True}})
    ui = sections.setdefault('ui', {})
    ui.setdefault('release', {'enabled': False})
    return DrakkarConfig(**sections)


def _kafka_sinks(**instances: dict) -> dict:
    """``sinks`` with Kafka sink instances named by the keyword, each with a topic."""
    return {'kafka': {name: {'topic': 'out', **settings} for name, settings in instances.items()}}


def _load_error(**sections) -> str:
    with pytest.raises(ValidationError) as excinfo:
        _config(**sections)
    return str(excinfo.value)


@pytest.mark.parametrize(
    'instance',
    [
        KafkaSinkConfig(topic='out'),
        PostgresSinkConfig(dsn='postgresql://db.example/drakkar'),
        MongoSinkConfig(uri='mongodb://db.example', database='drakkar'),
        HttpSinkConfig(url='https://api.example.com/results'),
        RedisSinkConfig(),
        FileSinkConfig(base_path='/data/out'),
    ],
    ids=lambda instance: type(instance).__name__,
)
def test_defaults_dlq_two_retries_no_dependencies(instance):
    assert instance.on_failure == 'dlq'
    assert instance.max_retries == 2
    assert instance.depends_on == []


def test_policy_of_reads_the_declared_policy():
    config = _config(sinks=_kafka_sinks(results={'on_failure': 'skip', 'max_retries': 5}, audit={}))

    assert config.sinks.policy_of('kafka', 'results') == SinkPolicy(on_failure='skip', max_retries=5, depends_on=())
    assert config.sinks.policy_of('kafka', 'audit') == SinkPolicy(on_failure='dlq', max_retries=2, depends_on=())


def test_on_failure_refuses_other_values():
    text = _load_error(sinks=_kafka_sinks(results={'on_failure': 'stall'}))

    assert "sinks.kafka.results.on_failure must be dlq or skip, got 'stall'" in text


@pytest.mark.parametrize('value', [0, 10, 3.0, '4'])
def test_max_retries_bounds_and_whole_number_values_that_load(value):
    config = _config(sinks=_kafka_sinks(results={'max_retries': value}))

    assert config.sinks.kafka['results'].max_retries == int(value)


@pytest.mark.parametrize('value', [-1, 11, 11.0, '11'])
def test_max_retries_bounds_and_whole_number_values_out_of_range(value):
    text = _load_error(sinks=_kafka_sinks(results={'max_retries': value}))

    assert 'sinks.kafka.results.max_retries must be a whole number from 0 to 10' in text


def test_max_retries_bounds_and_whole_number_fraction_keeps_the_integer_rule():
    text = _load_error(sinks=_kafka_sinks(results={'max_retries': 2.5}))

    assert 'sinks.kafka.results.max_retries' in text
    assert 'fractional part' in text


def test_depends_on_unknown_reference_message():
    text = _load_error(sinks=_kafka_sinks(results={'depends_on': ['postgres/main-db']}))

    assert "sinks.kafka.results.depends_on: 'postgres/main-db' is not a configured sink; write '<type>/<name>'" in text


def test_depends_on_self_message():
    text = _load_error(sinks=_kafka_sinks(results={'depends_on': ['kafka/results']}))

    assert 'sinks.kafka.results.depends_on: a sink cannot depend on itself' in text


def test_depends_on_cycle_message_starts_at_smallest_reference():
    # The search starts at kafka/a and meets the cycle at kafka/c; the
    # message still starts at the cycle's smallest member, kafka/b.
    sinks = _kafka_sinks(
        a={'depends_on': ['kafka/c']},
        c={'depends_on': ['kafka/b']},
        b={'depends_on': ['kafka/c']},
    )

    text = _load_error(sinks=sinks)

    assert 'sinks: depends_on forms a cycle: kafka/b -> kafka/c -> kafka/b' in text


def test_depends_on_checks_instances_in_sorted_reference_order():
    sinks = _kafka_sinks(
        zeta={'depends_on': ['kafka/zeta']},
        alpha={'depends_on': ['kafka/missing']},
    )

    text = _load_error(sinks=sinks)

    assert "sinks.kafka.alpha.depends_on: 'kafka/missing' is not a configured sink" in text
    assert 'zeta' not in text


def test_instance_checks_come_before_references():
    sinks = _kafka_sinks(alpha={'depends_on': ['kafka/missing']}, zeta={'on_failure': 'stall'})

    text = _load_error(sinks=sinks)

    assert "sinks.kafka.zeta.on_failure must be dlq or skip, got 'stall'" in text
    assert 'missing' not in text


def test_dependency_graph_and_instance_path():
    sinks = {
        'kafka': {'results': {'topic': 'out', 'depends_on': ['postgres/main-db', 'http/webhook']}},
        'postgres': {'main-db': {'dsn': 'postgresql://db.example/drakkar'}},
        'http': {'webhook': {'url': 'https://hooks.example', 'depends_on': ['postgres/main-db']}},
    }

    config = _config(sinks=sinks)

    assert config.sinks.dependency_graph() == {
        'http/webhook': ['postgres/main-db'],
        'kafka/results': ['postgres/main-db', 'http/webhook'],
        'postgres/main-db': [],
    }
    assert config.sinks.instance_path('kafka/results') == 'sinks.kafka.results'
    assert config.sinks.instance_path('http/webhook') == 'sinks.http.webhook'


def test_executor_on_failure_values():
    assert ExecutorConfig().on_failure == 'skip'
    assert _config(executor={'on_failure': 'dlq'}).executor.on_failure == 'dlq'

    text = _load_error(executor={'on_failure': 'retry'})

    assert "executor.on_failure must be skip or dlq, got 'retry'" in text


NEW_VALIDATION_CASES = [
    'sink_on_failure_unknown',
    'sink_max_retries_above_ten',
    'sink_max_retries_fraction',
    'sink_depends_on_unknown',
    'sink_depends_on_self',
    'sink_depends_on_cycle',
    'executor_on_failure_unknown',
]
NEW_LOAD_VALUE_CASES = [
    'sink_on_failure_default_dlq',
    'sink_max_retries_default_two',
    'sink_depends_on_default_empty',
    'http_sink_max_retries_default_two',
    'sink_max_retries_whole_float',
    'sink_depends_on_loads',
    'executor_on_failure_default_skip',
]


def test_config_contract_fixture_has_the_sink_policy_cases():
    # The shared runner in test_config_contract.py loads every case; this
    # only keeps the sink policy cases from leaving the fixture.
    validation = {case['name'] for case in FIXTURE['validation']}
    load_values = {case['name'] for case in FIXTURE['load_values']}

    assert set(NEW_VALIDATION_CASES) <= validation
    assert set(NEW_LOAD_VALUE_CASES) <= load_values


def test_sinks_declares_only_the_built_in_sink_types():
    assert 'custom' not in SinksConfig.model_fields
