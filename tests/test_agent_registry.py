"""The agent knowledge registry: schema, keys, references, completeness and rendering."""

from __future__ import annotations

import importlib
import json
import pkgutil
from pathlib import Path

import jsonschema
import pytest
import yaml
from prometheus_client.registry import REGISTRY

import drakkar
from drakkar.agent import params
from drakkar.agent import registry as reg

REPO_ROOT = Path(__file__).parent.parent
FIXTURES = Path(__file__).parent / 'fixtures'
CONTRACT = json.loads((FIXTURES / 'agent_contract.json').read_text(encoding='utf-8'))
SCHEMA = json.loads((REPO_ROOT / 'drakkar' / 'agentdocs' / 'schema.json').read_text(encoding='utf-8'))

REQUIRED_CONCEPTS = {
    'pipeline',
    'window',
    'arrange',
    'task',
    'offsets',
    'at-least-once',
    'pending-budget',
    'backpressure',
    'sinks-and-delivery',
    'dlq',
    'own-folder',
    'worker-name',
    'recorder',
    'memory-store',
    'sealed-history',
    'archive',
    'autodiscover',
    'cluster-tasks',
    'cache-exchange',
    'runtime-health',
    'http-source',
    'databases-view',
    'agent-surface',
}
REQUIRED_RECIPES = {
    'lag-growing',
    'worker-memory-high',
    'tasks-slow',
    'tasks-failing',
    'dlq-filling',
    'sink-failing',
    'worker-stalled',
    'partition-dead',
    'recorder-dropping',
    'peer-missing',
    'cluster-task-not-running',
    'disk-filling',
}
REQUIRED_RULES = {
    'worker-stalled',
    'loop-lag-high',
    'budget-saturated',
    'pool-saturated',
    'partition-dead',
    'partition-paused-by-debug',
    'sink-breaker-open',
    'lag-growing',
    'recorder-dropping',
    'dlq-writing',
    'peers-offline',
    'cluster-tasks-mismatch',
    'agent-unauthenticated',
}
RULE_SEVERITIES = {
    'worker-stalled': 'critical',
    'loop-lag-high': 'warning',
    'budget-saturated': 'warning',
    'pool-saturated': 'info',
    'partition-dead': 'critical',
    'partition-paused-by-debug': 'info',
    'sink-breaker-open': 'critical',
    'lag-growing': 'warning',
    'recorder-dropping': 'warning',
    'dlq-writing': 'info',
    'peers-offline': 'warning',
    'cluster-tasks-mismatch': 'warning',
    'agent-unauthenticated': 'info',
}


def _tiny_files() -> dict[str, bytes]:
    files = {name: text.encode('utf-8') for name, text in CONTRACT['tiny_registry']['files'].items()}
    files[reg.SCHEMA_FILE] = (REPO_ROOT / 'drakkar' / 'agentdocs' / 'schema.json').read_bytes()
    return files


def _tiny_registry() -> reg.Registry:
    return reg.build_registry(files=_tiny_files(), config_paths=CONTRACT['tiny_registry']['config_paths'])


def _keys(part: str) -> list[str]:
    return [entry['key'] for entry in reg.load_registry().parts[part]]


def _framework_metric_names() -> set[str]:
    """Every metric family of the framework, under its exposed name (counters with ``_total``)."""
    for module in pkgutil.walk_packages(drakkar.__path__, 'drakkar.'):
        importlib.import_module(module.name)
    exposed_suffix = {'counter': '_total', 'info': '_info'}
    return {
        family.name + exposed_suffix.get(family.type, '')
        for family in REGISTRY.collect()
        if family.name.startswith('drakkar_') and not family.name.startswith('drakkar_test_')
    }


# --- the shipped registry: completeness ------------------------------------


def test_shipped_registry_loads_and_every_reference_resolves():
    registry = reg.load_registry()
    assert len(registry.version) == reg.REGISTRY_VERSION_LENGTH
    assert registry.version == reg.registry_version(reg.read_registry_files())


def test_shipped_registry_validates_against_the_schema_with_a_reference_checker():
    files = reg.read_registry_files()
    document = {part: yaml.safe_load(files[name]) for part, name in reg.PART_FILES}
    jsonschema.Draft202012Validator(SCHEMA).validate(document)


def test_every_framework_metric_has_an_entry_and_every_entry_names_one():
    registered = _framework_metric_names()
    entries = set(_keys('metrics'))
    assert sorted(registered - entries) == []
    assert sorted(entries - registered) == []


def test_every_recorder_event_has_an_entry():
    vocabulary = json.loads((FIXTURES / 'event_vocabulary.json').read_text(encoding='utf-8'))['events']
    entries = {key for key in _keys('events') if not key.startswith('log:')}
    assert sorted(set(vocabulary) - entries) == []
    assert sorted(entries - set(vocabulary)) == []


def test_every_openapi_route_has_an_entry_and_every_entry_is_a_route():
    spec = yaml.safe_load((REPO_ROOT / 'drakkar' / 'uiserver' / 'openapi.yaml').read_text(encoding='utf-8'))
    methods = {'get', 'post', 'put', 'patch', 'delete'}
    routes = {f'{method.upper()} {path}' for path, ops in spec['paths'].items() for method in ops if method in methods}
    entries = set(_keys('routes'))
    assert sorted(routes - entries) == []
    assert sorted(entries - routes) == []


def test_every_config_entry_names_a_metadata_path():
    registry = reg.load_registry()
    unknown = [key for key in _keys('config') if not reg.config_key_matches(key, registry.config_paths)]
    assert unknown == []


def test_required_concepts_recipes_and_rules_are_present():
    assert sorted(REQUIRED_CONCEPTS - set(_keys('concepts'))) == []
    assert sorted(REQUIRED_RECIPES - set(_keys('recipes'))) == []
    assert set(_keys('rules')) == REQUIRED_RULES


def test_rule_severities_follow_the_rule_table():
    severities = {entry['key']: entry['severity'] for entry in reg.load_registry().parts['rules']}
    assert severities == RULE_SEVERITIES


def test_shipped_text_is_within_the_size_limits():
    registry = reg.load_registry()
    for part, entries in registry.parts.items():
        for entry in entries:
            assert len(entry['summary']) <= 200, f'{part} {entry["key"]}'
            assert len(entry.get('body', '')) <= 2000, f'{part} {entry["key"]}'


@pytest.mark.parametrize('word', ['golang', ' Go ', 'Go backend', 'Python', 'phase', 'backlog'])
def test_registry_text_names_no_backend_and_no_process_word(word):
    for name, data in reg.read_registry_files().items():
        assert word not in data.decode('utf-8'), f'{name} contains {word!r}'


# --- schema checker ---------------------------------------------------------

_BAD_DOCUMENTS = {
    'summary_too_long': ('concepts', {'key': 'a', 'summary': 'x' * 201, 'body': 'b'}),
    'body_too_long': ('concepts', {'key': 'a', 'summary': 's', 'body': 'b' * 2001}),
    'unknown_field': ('concepts', {'key': 'a', 'summary': 's', 'body': 'b', 'extra': 'x'}),
    'missing_body': ('concepts', {'key': 'a', 'summary': 's'}),
    'bad_key': ('concepts', {'key': 'A b', 'summary': 's', 'body': 'b'}),
    'summary_two_lines': ('concepts', {'key': 'a', 'summary': 's\nt', 'body': 'b'}),
    'empty_related': ('concepts', {'key': 'a', 'summary': 's', 'body': 'b', 'related': []}),
    'bad_reference': ('concepts', {'key': 'a', 'summary': 's', 'body': 'b', 'related': ['widget:x']}),
    'boolean_summary': ('concepts', {'key': 'a', 'summary': True, 'body': 'b'}),
    'bad_cost': ('routes', {'key': 'GET /x', 'summary': 's', 'use': 'u', 'cost': 'free'}),
    'bad_severity': (
        'rules',
        {
            'key': 'r',
            'summary': 's',
            'severity': 'high',
            'title': 't',
            'explain': 'concept:a',
            'suggestion': 'x',
            'inputs': ['a'],
        },
    ),
    'fields_not_text': ('events', {'key': 'e', 'summary': 's', 'when': 'w', 'fields': {'a': 1}}),
    'fields_empty': ('events', {'key': 'e', 'summary': 's', 'when': 'w', 'fields': {}}),
    'promql_without_query': (
        'metrics',
        {'key': 'drakkar_x', 'summary': 's', 'meaning': 'm', 'healthy': 'h', 'promql': [{'title': 't'}]},
    ),
    'steps_not_a_list': ('recipes', {'key': 'r', 'summary': 's', 'question': 'q', 'steps': 'x', 'causes': []}),
}


def _document_with(part: str, entry: dict) -> dict:
    document = {name: [] for name, _ in reg.PART_FILES}
    document[part] = [entry]
    return document


@pytest.mark.parametrize('name', sorted(_BAD_DOCUMENTS))
def test_schema_checker_refuses_what_the_reference_checker_refuses(name):
    part, entry = _BAD_DOCUMENTS[name]
    document = _document_with(part, entry)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.Draft202012Validator(SCHEMA).validate(document)
    assert reg.schema_problems(instance=document, schema=SCHEMA, root=SCHEMA)


def test_schema_checker_refuses_a_summary_that_ends_with_a_line_break():
    document = _document_with('concepts', {'key': 'a', 'summary': 's\n', 'body': 'b'})
    problems = reg.schema_problems(instance=document, schema=SCHEMA, root=SCHEMA)
    assert "$.concepts[0].summary: does not match '^[^\\n]+$'" in problems


def test_schema_checker_refuses_an_unsupported_keyword():
    with pytest.raises(ValueError, match="schema keyword 'oneOf' is not supported"):
        reg.schema_problems(instance=1, schema={'oneOf': []}, root={})


def test_schema_checker_reports_a_missing_part_and_a_wrong_type():
    problems = reg.schema_problems(instance={'concepts': 'x'}, schema=SCHEMA, root=SCHEMA)
    assert '$.concepts: must be of type array' in problems
    assert '$: missing field rules' in problems


# --- build checks -------------------------------------------------------------


def test_tiny_registry_builds():
    registry = _tiny_registry()
    assert registry.find('concept:pipeline')[0] == 'concepts'
    assert registry.find('event:log:agent_notes_failed')[0] == 'events'
    assert registry.find('concept:nope') is None


def _broken(file_name: str, text: str) -> reg.RegistryError:
    files = _tiny_files()
    files[file_name] = text.encode('utf-8')
    with pytest.raises(reg.RegistryError) as raised:
        reg.build_registry(files=files, config_paths=CONTRACT['tiny_registry']['config_paths'])
    return raised.value


def test_build_refuses_a_repeated_key():
    text = CONTRACT['tiny_registry']['files']['routes.yaml'] * 2
    assert _broken('routes.yaml', text).problems == ['routes: key GET /healthz is repeated']


def test_build_refuses_a_reference_that_does_not_resolve():
    text = CONTRACT['tiny_registry']['files']['config.yaml'].replace('concept:pipeline', 'concept:nope')
    assert _broken('config.yaml', text).problems == [
        'config: executor.max_executors references concept:nope, which does not resolve'
    ]


def test_build_refuses_a_config_key_with_no_metadata_path():
    text = CONTRACT['tiny_registry']['files']['config.yaml'].replace(
        'key: executor.max_executors', 'key: executor.no_such_key'
    )
    assert _broken('config.yaml', text).problems == ['config: key executor.no_such_key names no config path']


def test_build_refuses_a_schema_problem_and_unreadable_yaml():
    assert _broken('routes.yaml', '- key: GET /x\n  summary: s\n').problems == [
        '$.routes[0]: missing field use',
        '$.routes[0]: missing field cost',
    ]
    assert _broken('routes.yaml', '- [').problems[0].startswith('routes.yaml: cannot be read:')


def test_an_empty_file_is_an_empty_list():
    problems = _broken('routes.yaml', '').problems
    assert problems == ['$.routes: needs at least 1 items']


@pytest.mark.parametrize(
    ('key', 'matches'),
    [
        ('executor.max_executors', True),
        ('sinks.kafka.*.topic', True),
        ('sinks.kafka.*', True),
        ('sinks.*', True),
        ('executor.*', True),
        ('executor', False),
        ('sinks.kafka.results.topic', False),
        ('executor.max', False),
        ('executor.max*', False),
        ('nothing.*', False),
    ],
)
def test_config_key_matches_metadata_paths_with_star_rules(key, matches):
    paths = set(CONTRACT['tiny_registry']['config_paths'])
    assert reg.config_key_matches(key, paths) is matches


def test_config_reference_resolves_against_metadata_not_only_entries():
    registry = _tiny_registry()
    assert registry.resolves('config:executor.max_pending_tasks')
    assert registry.resolves('config:sinks.kafka.*')
    assert not registry.resolves('config:executor.unknown')


# --- canonical bytes and rendering against the fixture -----------------------


def test_registry_files_order_matches_the_fixture():
    assert list(reg.CANONICAL_FILE_ORDER) == CONTRACT['registry_files']


def test_canonical_bytes_and_version_match_the_fixture():
    example = CONTRACT['canonical_example']
    files = {name: text.encode('utf-8') for name, text in example['files'].items()}
    assert reg.canonical_bytes(files).decode('utf-8') == example['canonical_bytes']
    assert reg.registry_version(files) == example['registry_version']


@pytest.mark.parametrize('case', CONTRACT['manual_cases'], ids=lambda case: case['name'])
def test_manual_rendering_matches_the_fixture(case):
    registry = _tiny_registry()
    selection = reg.select_manual(
        registry,
        parts=params.parse_parts(case['parts'], params.MANUAL_PARTS),
        detail=case['detail'],
        topic=case['topic'],
        app_entries=case['app_entries'],
    )
    assert reg.selection_json(selection, case['promql_filters']) == case['json']
    assert reg.selection_markdown(selection, case['promql_filters']) == case['markdown']


@pytest.mark.parametrize('topic', CONTRACT['topic_not_found'])
def test_unknown_topic_raises(topic):
    with pytest.raises(reg.TopicNotFoundError):
        reg.select_manual(_tiny_registry(), topic=topic)


def test_full_manual_of_the_shipped_registry_renders_every_part():
    selection = reg.select_manual(reg.load_registry(), detail='full')
    markdown = reg.selection_markdown(selection, {'worker': 'job="w"', 'cluster': 'cluster="c"'})
    assert '{{worker}}' not in markdown
    for part in params.MANUAL_PARTS:
        assert f'\n# {part}\n' in '\n' + markdown


def test_markdown_rejects_an_unknown_style():
    with pytest.raises(ValueError, match='unknown field style'):
        reg._md_field('table', 'X', [])


def test_approx_tokens_rounds_up_utf8_bytes():
    assert reg.approx_tokens('') == 0
    assert reg.approx_tokens('abcde') == 2
    assert reg.approx_tokens('ü') == 1


# --- parameters ---------------------------------------------------------------


def test_messages_and_parts_match_the_fixture():
    assert CONTRACT['messages'] == {
        'agent_off': params.AGENT_OFF_MESSAGE,
        'token_required': params.TOKEN_REQUIRED_MESSAGE,
        'unknown_part': params.UNKNOWN_PART_MESSAGE,
        'detail': params.DETAIL_MESSAGE,
        'format': params.FORMAT_MESSAGE,
        'no_such_topic': params.NO_SUCH_TOPIC_MESSAGE,
        'rule_timed_out': params.RULE_TIMED_OUT_MESSAGE,
        'no_recorder_store': params.NO_RECORDER_STORE_NOTE,
    }
    assert CONTRACT['parts'] == {
        'manual': list(params.MANUAL_PARTS),
        'config': list(params.CONFIG_PARTS),
        'state': list(params.STATE_PARTS),
    }


def test_config_parts_are_the_root_sections_in_order():
    from drakkar.config import DrakkarConfig

    assert list(params.CONFIG_PARTS) == list(DrakkarConfig.model_fields)


def _outcome(parse, *args) -> dict:
    try:
        return {'value': parse(*args)}
    except params.ParameterError as error:
        return {'error': str(error)}


@pytest.mark.parametrize('case', CONTRACT['parts_cases'], ids=lambda case: f'{case["route"]}-{case["parts"]}')
def test_parts_parameter_matches_the_fixture(case):
    known = {'manual': params.MANUAL_PARTS, 'state': params.STATE_PARTS}[case['route']]
    expected = {key: case[key] for key in ('value', 'error') if key in case}
    assert _outcome(params.parse_parts, case['parts'], known) == expected


@pytest.mark.parametrize('case', CONTRACT['detail_cases'], ids=lambda case: str(case['detail']))
def test_detail_parameter_matches_the_fixture(case):
    expected = {key: case[key] for key in ('value', 'error') if key in case}
    assert _outcome(params.parse_detail, case['detail']) == expected


@pytest.mark.parametrize('case', CONTRACT['format_cases'], ids=lambda case: str(case['format']))
def test_format_parameter_matches_the_fixture(case):
    expected = {key: case[key] for key in ('value', 'error') if key in case}
    assert _outcome(params.parse_format, case['format']) == expected
