"""The strict key walker: unknown keys, suggestions, env map names."""

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import BaseModel, Field, ValidationError

from drakkar.config import ConfigModel, DrakkarConfig, MetricsConfig, SinksConfig, deep_merge, load_config
from drakkar.config.strict import (
    UNKNOWN_KEYS_HEADER,
    UnknownKey,
    decode_env_json,
    find_unknown_keys,
    known_leaf_paths,
    resolve_env_map_names,
    unknown_keys_message,
)
from drakkar.configmeta import build_config_metadata

CONTRACT = json.loads((Path(__file__).parent / 'fixtures' / 'config_contract.json').read_text(encoding='utf-8'))
LEAF_VALUE = 'leaf'


def _tree_for(path: str) -> dict[str, Any]:
    """The smallest tree holding one metadata path; a ``*`` segment becomes an operator name."""
    tree: Any = LEAF_VALUE
    for segment in reversed(path.split('.')):
        tree = {'instance' if segment == '*' else segment: tree}
    return tree


def _metadata_paths() -> set[str]:
    return {entry.path for group in build_config_metadata().groups for entry in group.entries}


def _write_config(tmp_path: Path, tree: Mapping[str, Any]) -> Path:
    path = tmp_path / 'drakkar.yaml'
    path.write_text(yaml.safe_dump(deep_merge(CONTRACT['base'], dict(tree))), encoding='utf-8')
    return path


def _case_tree(case: Mapping[str, Any]) -> Mapping[str, Any]:
    """The tree of one ``unknown_keys`` case, written as an object or as YAML text."""
    return yaml.safe_load(case['yaml_text']) if 'yaml_text' in case else case['yaml']


@pytest.mark.parametrize('case', CONTRACT['unknown_keys'], ids=[case['name'] for case in CONTRACT['unknown_keys']])
def test_unknown_keys_message_matches_the_fixture(case):
    found = find_unknown_keys(deep_merge(CONTRACT['base'], _case_tree(case)), DrakkarConfig)

    assert unknown_keys_message(found) == case['message']


@pytest.mark.parametrize('case', CONTRACT['unknown_keys'], ids=[case['name'] for case in CONTRACT['unknown_keys']])
def test_unknown_keys_fail_the_load_before_any_field_check(case, tmp_path):
    # An invalid value next to the unknown key must not hide the message.
    tree = deep_merge(_case_tree(case), {'metrics': {'port': 'not-a-port'}})

    with pytest.raises(ValueError) as excinfo:
        load_config(_write_config(tmp_path, tree))

    assert str(excinfo.value) == case['message']


@pytest.mark.parametrize(
    'variable, key',
    [('DK__ENV_FILE', '_env_file'), ('DK__SECRETS_DIR', '_secrets_dir'), ('DK__ENV_PREFIX', '_env_prefix')],
)
def test_a_variable_that_names_a_settings_argument_is_an_unknown_key(variable, key, tmp_path, monkeypatch):
    monkeypatch.setenv(variable, 'x')

    with pytest.raises(ValueError) as excinfo:
        load_config(_write_config(tmp_path, {}))

    assert str(excinfo.value) == f'{UNKNOWN_KEYS_HEADER}\n  {key}'


def test_walker_accepts_exactly_the_metadata_paths():
    walker_paths = {path.split('[', maxsplit=1)[0] for path in known_leaf_paths(DrakkarConfig)}

    assert walker_paths - {'app'} == _metadata_paths()
    assert 'app' in walker_paths


@pytest.mark.parametrize('path', sorted(_metadata_paths()))
def test_walker_reports_nothing_for_a_metadata_path(path):
    assert find_unknown_keys(_tree_for(path), DrakkarConfig) == []


def test_walker_leaves_go_below_the_lists_of_models():
    paths = known_leaf_paths(DrakkarConfig)

    assert 'ui.timeline.events[].color' in paths
    assert 'ui.timeline.color_rules[].when[].field' in paths
    assert 'sources.http.clients[].rpm' in paths
    assert 'sinks.mongo.*.statements.*.op' in paths
    assert 'connections.postgres.*.schema' in paths
    assert 'connections.postgres.*.schema_name' not in paths


@pytest.mark.parametrize(
    'tree',
    [
        {'app': {'anything': {'at': ['any', {'depth': 1}]}}},
        {'executor': {'env': {'ANY_VARIABLE': 'x'}}},
        {'sinks': {'http': {'x': {'headers': {'X-Any-Header': 'x'}}}}},
        {'sinks': {'mongo': {'m': {'statements': {'s': {'update': {'$set': {'a': 1}}}}}}}},
        {'sinks': {'mongo': {'m': {'statements': {'s': {'update': [{'$set': {'a': 1}}]}}}}}},
        {'ui': {'custom_links': [{'any': 'key'}]}},
    ],
)
def test_free_maps_are_open(tree):
    assert find_unknown_keys(tree, DrakkarConfig) == []


def test_a_map_name_is_not_reported_and_the_level_below_it_is():
    tree = {'sinks': {'kafka': {'any-name_1': {'topic': 't', 'bogus': 1}}}}

    assert find_unknown_keys(tree, DrakkarConfig) == [UnknownKey(path='sinks.kafka.any-name_1.bogus', suggestion=None)]


@pytest.mark.parametrize(
    'tree',
    [
        {'metrics': 'not a mapping'},
        {'sinks': {'kafka': None}},
        {'sinks': {'kafka': {'x': 'not a mapping'}}},
        {'ui': {'timeline': {'events': 'not a list'}}},
        {'ui': {'timeline': {'events': ['not a mapping']}}},
    ],
)
def test_a_value_of_the_wrong_shape_is_left_to_the_field_check(tree):
    assert find_unknown_keys(tree, DrakkarConfig) == []


def test_a_key_that_is_no_string_is_reported_as_text():
    tree = {'metrics': {7: 'a', True: 'b', None: 'c'}}

    paths = [key.path for key in find_unknown_keys(tree, DrakkarConfig)]

    assert paths == ['metrics.7', 'metrics.null', 'metrics.true']


def test_a_top_level_key_that_is_no_string_fails_the_load_naming_it(tmp_path):
    path = tmp_path / 'drakkar.yaml'
    path.write_text('1: x\nsources: {kafka: {enabled: true}}\n', encoding='utf-8')

    with pytest.raises(ValueError) as excinfo:
        load_config(path)

    assert str(excinfo.value) == f'{UNKNOWN_KEYS_HEADER}\n  1'


def test_a_single_mapping_at_a_list_position_is_walked():
    tree = {'ui': {'timeline': {'color_rules': [{'when': {'bogus': 1}}]}}}

    assert [key.path for key in find_unknown_keys(tree, DrakkarConfig)] == ['ui.timeline.color_rules[0].when.bogus']


def test_a_line_whose_key_has_children_lists_that_key_only():
    tree = {'ui': {'recordr': {'a': 1, 'b': {'c': 2}}}}

    assert [key.path for key in find_unknown_keys(tree, DrakkarConfig)] == ['ui.recordr']


def test_the_message_is_the_header_then_one_indented_line_per_key():
    found = [UnknownKey(path='a.b', suggestion='c'), UnknownKey(path='d', suggestion=None)]

    assert unknown_keys_message(found) == f"{UNKNOWN_KEYS_HEADER}\n  a.b (did you mean 'c'?)\n  d"


class _Inner(ConfigModel):
    leaf: int = 0


class _Outer(ConfigModel):
    optional_block: _Inner | None = None
    block_or_text: str | _Inner = ''
    bare_map: dict = Field(default_factory=dict)
    nested_maps: dict[str, dict[str, _Inner]] = Field(default_factory=dict)
    aliased: int = Field(default=0, alias='written_key')


def test_an_optional_model_is_followed():
    assert [key.path for key in find_unknown_keys({'optional_block': {'bogus': 1}}, _Outer)] == ['optional_block.bogus']
    assert find_unknown_keys({'optional_block': None}, _Outer) == []


def test_a_union_with_a_model_branch_follows_the_model_only_for_a_mapping():
    assert [key.path for key in find_unknown_keys({'block_or_text': {'bogus': 1}}, _Outer)] == ['block_or_text.bogus']
    assert find_unknown_keys({'block_or_text': 'text'}, _Outer) == []


def test_a_bare_dict_is_open():
    assert find_unknown_keys({'bare_map': {'any': 1}}, _Outer) == []


def test_a_map_of_maps_is_checked_below_both_names():
    assert [key.path for key in find_unknown_keys({'nested_maps': {'a': {'b': {'bogus': 1}}}}, _Outer)] == [
        'nested_maps.a.b.bogus'
    ]


def test_an_aliased_field_answers_to_its_yaml_key_only():
    found = find_unknown_keys({'written_key': 1, 'aliased': 2}, _Outer)

    assert [key.path for key in found] == ['aliased']


def test_known_leaf_paths_name_the_yaml_key_and_the_shapes():
    assert set(known_leaf_paths(_Outer)) == {
        'optional_block.leaf',
        'block_or_text.leaf',
        'bare_map',
        'nested_maps.*.*.leaf',
        'written_key',
    }


def test_a_section_built_in_code_is_not_walked():
    config = DrakkarConfig(**CONTRACT['base'], metrics=MetricsConfig(port=9191))

    assert config.metrics.port == 9191


def test_every_config_model_refuses_an_unknown_keyword():
    def models(model: type[BaseModel], seen: set[type[BaseModel]]) -> set[type[BaseModel]]:
        seen.add(model)
        for info in model.model_fields.values():
            for inner in _models_in(info.annotation):
                if inner not in seen:
                    models(inner, seen)
        return seen

    found = models(DrakkarConfig, set())

    assert len(found) > 30
    assert [model.__name__ for model in found if model.model_config.get('extra') != 'forbid'] == []


def _models_in(annotation: Any) -> list[type[BaseModel]]:
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return [annotation]
    return [model for arg in getattr(annotation, '__args__', ()) for model in _models_in(arg)]


# --- env ---------------------------------------------------------------------


def test_env_typo_is_reported(tmp_path, monkeypatch):
    monkeypatch.setenv('DK_EXECUTOR__MAX_EXECUTER', '8')

    with pytest.raises(ValueError) as excinfo:
        load_config(_write_config(tmp_path, {}))

    assert "executor.max_executer (did you mean 'max_executors'?)" in str(excinfo.value)


def test_env_typo_is_reported_with_no_config_file(monkeypatch):
    monkeypatch.setenv('DK_SOURCES__KAFKA__ENABLED', 'true')
    monkeypatch.setenv('DK_CONNECTIONS__KAFKA__MAIN__BROKERS', 'localhost:9092')
    monkeypatch.setenv('DK_EXECUTOR__MAX_EXECUTER', '8')

    with pytest.raises(ValueError, match=r'executor\.max_executer'):
        load_config(None)


def test_env_typo_is_reported_for_a_config_built_in_code(monkeypatch):
    monkeypatch.setenv('DK_EXECUTOR__MAX_EXECUTER', '8')

    with pytest.raises(ValidationError, match=r'executor\.max_executer'):
        DrakkarConfig(**CONTRACT['base'])


def test_an_unknown_top_level_env_key_is_reported(tmp_path, monkeypatch):
    monkeypatch.setenv('DK_BOGUS', '1')

    with pytest.raises(ValueError, match='has unknown keys') as excinfo:
        load_config(_write_config(tmp_path, {}))

    assert '\n  bogus' in str(excinfo.value)


@pytest.mark.parametrize(
    'yaml_name, segment',
    [('main-db', 'MAIN_DB'), ('MainDB', 'MAINDB'), ('main_db', 'MAIN_DB'), ('a-b_c', 'A_B_C')],
)
def test_env_segment_reaches_the_existing_sink_name(yaml_name, segment, tmp_path, monkeypatch):
    tree = {'connections': {'postgres': {'main': {'dsn': 'postgresql://h/db'}}}, 'sinks': {'postgres': {yaml_name: {}}}}
    monkeypatch.setenv(f'DK_SINKS__POSTGRES__{segment}__POOL_MAX', '3')

    config = load_config(_write_config(tmp_path, tree))

    assert list(config.sinks.postgres) == [yaml_name]
    assert config.sinks.postgres[yaml_name].pool_max == 3


def test_env_segment_with_no_matching_name_keeps_the_segment(tmp_path, monkeypatch):
    tree = {'connections': {'postgres': {'main': {'dsn': 'postgresql://h/db'}}}, 'sinks': {'postgres': {'main-db': {}}}}
    monkeypatch.setenv('DK_SINKS__POSTGRES__OTHER_DB__POOL_MAX', '7')

    config = load_config(_write_config(tmp_path, tree))

    assert sorted(config.sinks.postgres) == ['main-db', 'other_db']
    assert config.sinks.postgres['main-db'].pool_max == 10
    assert config.sinks.postgres['other_db'].pool_max == 7


def test_env_segment_matching_two_names_fails_the_load(tmp_path, monkeypatch):
    tree = {'sinks': {'postgres': {'main-db': {}, 'main_db': {}}}}
    monkeypatch.setenv('DK_SINKS__POSTGRES__MAIN_DB__POOL_MAX', '3')

    with pytest.raises(ValueError) as excinfo:
        load_config(_write_config(tmp_path, tree))

    assert str(excinfo.value) == (
        "the environment name 'main_db' under sinks.postgres matches more than one configured name: main-db, main_db"
    )


def test_two_segments_that_reach_one_name_both_land(tmp_path, monkeypatch):
    tree = {'connections': {'postgres': {'main': {'dsn': 'postgresql://h/db'}}}, 'sinks': {'postgres': {'main-db': {}}}}
    monkeypatch.setenv('DK_SINKS__POSTGRES__MAIN_DB__POOL_MAX', '7')
    monkeypatch.setenv('DK_SINKS__POSTGRES__MAIN-DB__POOL_MIN', '3')

    config = load_config(_write_config(tmp_path, tree))

    assert list(config.sinks.postgres) == ['main-db']
    assert config.sinks.postgres['main-db'].pool_max == 7
    assert config.sinks.postgres['main-db'].pool_min == 3


def test_two_segments_that_set_one_key_have_one_result():
    env_tree = {'postgres': {'main_db': {'pool_max': 'underscore'}, 'main-db': {'pool_max': 'hyphen'}}}
    yaml_tree = {'postgres': {'main-db': {}}}

    resolved = resolve_env_map_names(env_tree, yaml_tree, SinksConfig)

    assert resolved == {'postgres': {'main-db': {'pool_max': 'underscore'}}}


def test_env_segment_reaches_a_nested_map_name():
    env_tree = {'sinks': {'mongo': {'m_1': {'statements': {'on_done': {'op': 'update_one'}}}}}}
    yaml_tree = {'sinks': {'mongo': {'m-1': {'statements': {'on-done': {'op': 'insert_one'}}}}}}

    resolved = resolve_env_map_names(env_tree, yaml_tree, DrakkarConfig)

    assert resolved == {'sinks': {'mongo': {'m-1': {'statements': {'on-done': {'op': 'update_one'}}}}}}


def test_env_segment_of_a_free_map_stays_as_written():
    env_tree = {'executor': {'env': {'x_y': '1'}}, 'app': {'a_b': '1'}}
    yaml_tree = {'executor': {'env': {'X-Y': '0'}}, 'app': {'a-b': '0'}}

    assert resolve_env_map_names(env_tree, yaml_tree, DrakkarConfig) == env_tree


def test_env_resolution_goes_through_list_elements():
    class Element(ConfigModel):
        named: dict[str, _Inner] = Field(default_factory=dict)

    class Root(ConfigModel):
        items: list[Element] = Field(default_factory=list)

    env_tree = {'items': [{'named': {'a_b': {'leaf': '1'}}}, {'named': {'c_d': {'leaf': '2'}}}]}
    yaml_tree = {'items': [{'named': {'a-b': {'leaf': 0}}}]}

    assert resolve_env_map_names(env_tree, yaml_tree, Root) == {
        'items': [{'named': {'a-b': {'leaf': '1'}}}, {'named': {'c_d': {'leaf': '2'}}}]
    }


@pytest.mark.parametrize(
    'env_tree, yaml_tree',
    [
        ({'sinks': 'text'}, {}),
        ({'bogus': {'a_b': 1}}, {}),
        ({'sinks': {'postgres': {'a_b': {'pool_max': '1'}}}}, {'sinks': 'text'}),
        ({'sinks': {'postgres': {'a_b': {'pool_max': '1'}}}}, {'sinks': {'postgres': 'text'}}),
        ({'sinks': {'postgres': {'a_b': {'pool_max': '1'}}}}, {'sinks': {'postgres': {1: {}}}}),
    ],
)
def test_env_resolution_leaves_every_other_shape_alone(env_tree, yaml_tree):
    assert resolve_env_map_names(env_tree, yaml_tree, DrakkarConfig) == env_tree


def test_the_loader_applies_each_env_value_once_with_no_second_source(tmp_path, monkeypatch):
    # The settings env source would add the literal 'main_db' next to 'main-db'.
    tree = {'sinks': {'kafka': {'main-db': {'topic': 't'}}}}
    monkeypatch.setenv('DK_SINKS__KAFKA__MAIN_DB__TOPIC', 'from-env')

    config = load_config(_write_config(tmp_path, tree))

    assert {name: sink.topic for name, sink in config.sinks.kafka.items()} == {'main-db': 'from-env'}


# --- JSON text in env values ---------------------------------------------------


@pytest.mark.parametrize(
    'variable, value, read, expected',
    [
        ('DK_RECORDER__CAPTURE__ENV_VARS', '["A","B"]', lambda c: c.recorder.capture.env_vars, ['A', 'B']),
        ('DK_EXECUTOR__ENV', '{"FOO":"bar"}', lambda c: c.executor.env, {'FOO': 'bar'}),
        ('DK_SINKS__POSTGRES__MAIN_DB__DEPENDS_ON', '[]', lambda c: c.sinks.postgres['main-db'].depends_on, []),
        ('DK_UI__CUSTOM_LINKS', '[{"name":"a","url":"b"}]', lambda c: c.ui.custom_links, [{'name': 'a', 'url': 'b'}]),
    ],
)
def test_a_json_text_for_a_list_or_map_field_loads(variable, value, read, expected, tmp_path, monkeypatch):
    tree = {'connections': {'postgres': {'main': {'dsn': 'postgresql://h/db'}}}, 'sinks': {'postgres': {'main-db': {}}}}
    monkeypatch.setenv(variable, value)

    assert read(load_config(_write_config(tmp_path, tree))) == expected


def test_a_json_text_replaces_the_whole_yaml_value(tmp_path, monkeypatch):
    tree = {'recorder': {'capture': {'env_vars': ['X', 'Y', 'Z']}}, 'executor': {'env': {'KEEP': 'no'}}}
    monkeypatch.setenv('DK_RECORDER__CAPTURE__ENV_VARS', '["A"]')
    monkeypatch.setenv('DK_EXECUTOR__ENV', '{"FOO":"bar"}')

    config = load_config(_write_config(tmp_path, tree))

    assert config.recorder.capture.env_vars == ['A']
    assert config.executor.env == {'FOO': 'bar'}


def test_a_json_text_for_a_map_of_models_loads(tmp_path, monkeypatch):
    monkeypatch.setenv('DK_CONNECTIONS__POSTGRES', '{"pg":{"dsn":"postgresql://h/db"}}')

    config = load_config(_write_config(tmp_path, {}))

    assert config.connections.postgres['pg'].dsn == 'postgresql://h/db'


@pytest.mark.parametrize(
    'value',
    ['[not json', '{"FOO":', 'plain', '5', '"text"', 'null'],
)
def test_a_text_that_is_no_json_list_or_object_stays_text_for_the_field_check(value, tmp_path, monkeypatch):
    monkeypatch.setenv('DK_EXECUTOR__ENV', value)

    with pytest.raises(ValidationError, match=r'executor\.env'):
        load_config(_write_config(tmp_path, {}))


def test_a_json_text_for_a_section_is_not_decoded(tmp_path, monkeypatch):
    monkeypatch.setenv('DK_METRICS', '{"port":9191}')

    with pytest.raises(ValidationError, match='metrics'):
        load_config(_write_config(tmp_path, {}))


def test_a_scalar_field_keeps_a_text_that_looks_like_json():
    assert decode_env_json({'worker_name_env': '[1]'}, {'worker_name_env': '[1]'}, DrakkarConfig) == {
        'worker_name_env': '[1]'
    }


def test_the_keys_inside_a_json_text_are_checked(tmp_path, monkeypatch):
    monkeypatch.setenv('DK_SOURCES__HTTP__CLIENTS', '[{"bogus":1}]')

    with pytest.raises(ValueError, match=r'sources\.http\.clients\[0\]\.bogus'):
        load_config(_write_config(tmp_path, {}))


def test_json_decoding_goes_through_lists_and_maps_and_leaves_other_values():
    class Element(ConfigModel):
        names: list[str] = Field(default_factory=list)
        count: int = 0

    class Root(ConfigModel):
        items: list[Element] = Field(default_factory=list)
        named: dict[str, Element] = Field(default_factory=dict)
        optional_names: list[str] | None = None
        section: Element = Field(default_factory=Element)
        bogus_shape: dict[str, str] = Field(default_factory=dict)

    env_tree = {
        'items': [{'names': '["a"]', 'count': '1'}],
        'named': {'n': {'names': '["b"]'}},
        'optional_names': '["c"]',
        'section': 'text',
        'bogus_shape': ['x'],
        'unknown': '["d"]',
    }
    merged = {**env_tree, 'items': [env_tree['items'][0], {'count': 9}], 'named': {'n': {'names': '["b"]'}, 'm': {}}}

    assert decode_env_json(merged, env_tree, Root) == {
        'items': [{'names': ['a'], 'count': '1'}, {'count': 9}],
        'named': {'n': {'names': ['b']}, 'm': {}},
        'optional_names': ['c'],
        'section': 'text',
        'bogus_shape': ['x'],
        'unknown': '["d"]',
    }


def test_a_json_text_in_dk_app_is_refused(tmp_path, monkeypatch):
    monkeypatch.setenv('DK_APP', '{"a":1}')

    with pytest.raises(ValidationError, match=r'rename: DK_APP'):
        load_config(_write_config(tmp_path, {}))
