"""The range check of values with a unit: message forms, the table, and map entries."""

import pytest
from pydantic import ValidationError

from drakkar.config import DrakkarConfig, unit_bounds
from drakkar.config.unit_bounds import UNIT_BOUNDS, UnitBound, bound_message, check_unit_bounds
from drakkar.config.units import format_size, seconds_text
from tests.conftest import kafka_connections


def _config(**sections: object) -> DrakkarConfig:
    return DrakkarConfig(
        **{
            'sources': {'kafka': {'enabled': True}},
            'connections': kafka_connections(),
            'ui': {'release': {'enabled': False}},
            **sections,
        }
    )


@pytest.mark.parametrize(
    ('bound', 'value', 'expected'),
    [
        (UnitBound('a.b', 10, 60), 5, 'a.b must be from 10s to 1m, got 5s'),
        (UnitBound('a.b', 10, 60), 61, 'a.b must be from 10s to 1m, got 1m1s'),
        (UnitBound('a.b', 10, None), 9.5, 'a.b must be at least 10s, got 9s500ms'),
        (UnitBound('a.b', None, 60), 90, 'a.b must be at most 1m, got 1m30s'),
    ],
    ids=['from_to_below', 'from_to_above', 'at_least', 'at_most'],
)
def test_bound_message_prints_bounds_and_value_as_canonical_units(bound, value, expected):
    assert bound_message('a.b', bound, value, seconds_text) == expected


@pytest.mark.parametrize('value', [10, 30, 60])
def test_bound_message_is_none_inside_the_range_and_on_its_edges(value):
    assert bound_message('a.b', UnitBound('a.b', 10, 60), value, seconds_text) is None


def test_bound_message_prints_sizes_with_the_size_writer():
    bound = UnitBound('a.b', 1024, None)

    assert bound_message('a.b', bound, 1000, format_size) == 'a.b must be at least 1KiB, got 1KB'


def test_every_table_path_names_a_value_with_a_unit_of_a_full_config():
    # One instance of every sink type, so each path under a sink map resolves.
    config = _config(
        sinks={
            'kafka': {'k': {'topic': 't'}},
            'postgres': {'p': {}},
            'mongo': {'m': {'database': 'd'}},
            'redis': {'r': {}},
            'http': {'h': {'url': 'https://example.com/r'}},
            'filesystem': {'f': {'base_path': '/tmp'}},
        },
        connections={
            **kafka_connections(),
            'postgres': {'main': {'dsn': 'postgresql://u@h/d'}},
            'mongo': {'main': {'uri': 'mongodb://h'}},
            'redis': {'main': {}},
        },
    )

    # A config inside every range passes; a path that names no unit field raises TypeError.
    check_unit_bounds(config)
    assert len({bound.path for bound in UNIT_BOUNDS}) == len(UNIT_BOUNDS)


def test_a_sink_map_reports_the_first_instance_by_sorted_name():
    with pytest.raises(ValidationError, match=r'sinks\.kafka\.alpha\.flush_timeout must be at least 1ms, got 0'):
        _config(
            sinks={
                'kafka': {
                    'zulu': {'topic': 'z', 'flush_timeout': 0},
                    'alpha': {'topic': 'a', 'flush_timeout': 0},
                },
            },
        )


def test_a_sink_policy_key_not_given_has_no_bound_of_its_own():
    config = _config(sinks={'kafka': {'results': {'topic': 'out'}}})

    assert config.sinks.kafka['results'].delivery_timeout is None


def test_the_range_check_comes_before_the_cross_field_checks():
    with pytest.raises(ValidationError) as excinfo:
        DrakkarConfig(ui={'release': {'enabled': False}}, recorder={'memory': {'window': '5s'}})

    assert 'recorder.memory.window must be from 10s to 1d, got 5s' in str(excinfo.value)
    assert 'no input source enabled' not in str(excinfo.value)


def test_a_table_path_that_names_no_unit_field_fails_loudly(monkeypatch):
    monkeypatch.setattr(unit_bounds, 'UNIT_BOUNDS', (UnitBound('executor.max_executors', 1, None),))

    with pytest.raises(TypeError, match=r'ExecutorConfig\.max_executors is not a field with a unit'):
        check_unit_bounds(_config())
