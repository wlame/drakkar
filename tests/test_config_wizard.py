"""The config wizard's decision table (``docs/data/config-wizard.json``).

The docs page reads the table to print the fleet part of a config. This module
holds a Python port of the page logic (rule matching, merge, YAML serializer,
URL rules) and proves with it that every answer the page accepts prints a
config that loads, that no secret goes into the YAML, and that the golden
examples and the URL rules stay as written. The page script runs the same
examples and URL cases (``just docs-wizard-check``).
"""

import itertools
import json
import os
import re
import urllib.parse
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

import pytest
import yaml
from pydantic import BaseModel, ConfigDict, ValidationError

from drakkar.config import DrakkarConfig, load_config
from drakkar.configmeta import build_config_metadata, dump_by_yaml_key

REPO = Path(__file__).resolve().parent.parent
DOCS = REPO / 'docs'
TABLE_PATH = DOCS / 'data' / 'config-wizard.json'
PAGE_PATH = DOCS / 'calculator.md'
URL_CASES_PATH = Path(__file__).parent / 'fixtures' / 'config_wizard_url_cases.json'

KNOWN_REGISTRY_PROVIDERS = {'filesystem', 'static', 'none', 'postgres', 'redis', 'consul'}
KNOWN_ELECTION_PROVIDERS = {'filesystem', 'postgres', 'redis', 'consul'}
STEPS = ('workload', 'hosts', 'services', 'choose', 'result')
ENV_PLACEHOLDERS = {
    'DK_CONNECTIONS__POSTGRES__MAIN__DSN': 'postgresql://u:p@placeholder/db',
    'DK_CONNECTIONS__REDIS__MAIN__URL': 'redis://placeholder:6379/0',
    'DK_CONNECTIONS__CONSUL__MAIN__TOKEN': 'placeholder-token',
}
TEST_ONLY_YAML = {'ui': {'release': {'enabled': False}}}


# ---------------------------------------------------------------- table shape


class Strict(BaseModel):
    """A table object: unknown keys are an error, so a typo cannot pass unnoticed."""

    model_config = ConfigDict(extra='forbid')


Match = str | list[str]


class NumberInput(Strict):
    label: str
    help: str
    default: int
    min: int
    max: int


class ChoiceValue(Strict):
    value: str
    label: str
    help: str


class Choice(Strict):
    label: str
    help: str
    values: list[ChoiceValue]
    default: str | list[str]


class Feature(Strict):
    label: str
    help: str


class Provider(Strict):
    label: str
    needs: list[str]
    fragment: dict[str, Any]
    explain: str
    docs: str


class EnvVar(Strict):
    name: str
    path: str
    explain: str


class Connection(Strict):
    fragment: dict[str, Any]
    explain: str
    docs: str
    env: list[EnvVar]


class SharedEntry(Strict):
    fragment: dict[str, Any]
    explain: str = ''
    docs: str = ''


class RuleWhen(Strict):
    workers: Literal['one', 'many'] | None = None
    hosts: Match | None = None
    services_include: Match | None = None
    services_exclude: Match | None = None


class NoteWhen(RuleWhen):
    registry: Match | None = None
    election: Match | None = None


class Rule(Strict):
    when: RuleWhen
    registry: list[str]
    election: list[str]
    shared: Literal['none', 'folder']


class Note(Strict):
    id: str
    when: NoteWhen
    text: str
    docs: str
    fragment: dict[str, Any]


class Example(Strict):
    name: str
    query: str
    yaml: str
    env: list[str]


class Table(Strict):
    schema_version: Literal[1]
    inputs: dict[str, NumberInput]
    choices: dict[Literal['hosts', 'services'], Choice]
    features: dict[Literal['registry', 'election'], Feature]
    providers: dict[Literal['registry', 'election'], dict[str, Provider]]
    connections: dict[Literal['postgres', 'redis', 'consul'], Connection]
    shared: dict[Literal['none', 'folder'], SharedEntry]
    rules: list[Rule]
    notes: list[Note]
    section_order: list[str]
    base: dict[str, Any]
    examples: list[Example]


def load_table() -> dict[str, Any]:
    """The raw table, as the page reads it."""
    return json.loads(TABLE_PATH.read_text(encoding='utf-8'))


TABLE = load_table()


# ------------------------------------------------------- port of the page logic


@dataclass(frozen=True)
class Answers:
    """Every answer of the wizard; ``reg`` and ``elect`` are ``None`` for the recommended provider."""

    numbers: dict[str, int]
    hosts: str
    services: frozenset[str]
    reg: str | None = None
    elect: str | None = None
    step: str = 'workload'


@dataclass
class Resolution:
    """What the answers resolve to: the providers, the printed fleet part and the secrets to set."""

    registry_allowed: list[str]
    election_allowed: list[str]
    registry: str
    election: str
    notes: list[dict[str, Any]]
    fleet: dict[str, Any]
    env: list[str] = field(default_factory=list)


def default_answers(table: dict[str, Any]) -> Answers:
    """The answers of an empty link."""
    return Answers(
        numbers={name: spec['default'] for name, spec in table['inputs'].items()},
        hosts=table['choices']['hosts']['default'],
        services=frozenset(table['choices']['services']['default']),
    )


def effective(answers: Answers) -> Answers:
    """Reset the answers of skipped questions: one worker asks no hosts, one host asks no services."""
    hosts = answers.hosts if answers.numbers['workers'] >= 2 else 'one'
    services = answers.services if hosts == 'many' else frozenset()
    return Answers(answers.numbers, hosts, services, answers.reg, answers.elect, answers.step)


def as_list(value: Match) -> list[str]:
    """A ``when`` value is one string or a list of strings (any of)."""
    return [value] if isinstance(value, str) else value


def when_matches(when: dict[str, Match], answers: Answers, resolved: dict[str, str]) -> bool:
    """Whether every key of ``when`` matches; a missing key matches anything."""
    for key, expected in when.items():
        options = as_list(expected)
        if key == 'workers':
            actual = 'one' if answers.numbers['workers'] == 1 else 'many'
            matched = actual in options
        elif key == 'hosts':
            matched = answers.hosts in options
        elif key == 'services_include':
            matched = any(option in answers.services for option in options)
        elif key == 'services_exclude':
            matched = not any(option in answers.services for option in options)
        else:
            matched = resolved[key] in options
        if not matched:
            return False
    return True


def matching_rule(table: dict[str, Any], answers: Answers) -> dict[str, Any]:
    """The first rule whose ``when`` matches; the last rule matches anything."""
    return next(rule for rule in table['rules'] if when_matches(rule['when'], answers, {}))


def allowed_providers(table: dict[str, Any], feature: str, preferences: list[str], services: frozenset[str]) -> list:
    """The providers of a preference list whose services are all checked, in preference order."""
    providers = table['providers'][feature]
    return [name for name in preferences if set(providers[name]['needs']) <= services]


def merge_fragment(target: dict[str, Any], fragment: dict[str, Any], path: str = '') -> None:
    """Deep-merge ``fragment`` into ``target``; a leaf set twice is an error. ``{}`` is a leaf."""
    for key, value in fragment.items():
        here = f'{path}.{key}' if path else key
        is_branch = isinstance(value, dict) and bool(value)
        if key not in target:
            target[key] = {} if is_branch else value
            if is_branch:
                merge_fragment(target[key], value, here)
            continue
        existing = target[key]
        if not is_branch or not (isinstance(existing, dict) and existing):
            raise ValueError(f'two fragments set {here}')
        merge_fragment(existing, value, here)


def ordered(document: dict[str, Any], section_order: list[str]) -> dict[str, Any]:
    """The document with its top-level keys in ``section_order``."""
    unknown = [key for key in document if key not in section_order]
    if unknown:
        raise ValueError(f'top-level keys not in section_order: {unknown}')
    return {key: document[key] for key in section_order if key in document}


def resolve(table: dict[str, Any], raw_answers: Answers) -> Resolution:
    """Resolve the answers to providers, the fleet part of the YAML and the env names."""
    answers = effective(raw_answers)
    rule = matching_rule(table, answers)
    registry_allowed = allowed_providers(table, 'registry', rule['registry'], answers.services)
    election_allowed = allowed_providers(table, 'election', rule['election'], answers.services)
    registry = answers.reg if answers.reg in registry_allowed else registry_allowed[0]
    election = answers.elect if answers.elect in election_allowed else election_allowed[0]
    resolved = {'registry': registry, 'election': election}
    notes = [note for note in table['notes'] if when_matches(note['when'], answers, resolved)]

    needed = set(table['providers']['registry'][registry]['needs']) | set(
        table['providers']['election'][election]['needs']
    )
    kinds = [kind for kind in table['connections'] if kind in needed]

    fleet: dict[str, Any] = {}
    merge_fragment(fleet, table['shared'][rule['shared']]['fragment'])
    for kind in kinds:
        merge_fragment(fleet, table['connections'][kind]['fragment'])
    merge_fragment(fleet, table['providers']['registry'][registry]['fragment'])
    merge_fragment(fleet, table['providers']['election'][election]['fragment'])
    for note in notes:
        merge_fragment(fleet, note['fragment'])
    env = [entry['name'] for kind in kinds for entry in table['connections'][kind]['env']]
    return Resolution(registry_allowed, election_allowed, registry, election, notes, fleet, env)


def document_for(table: dict[str, Any], resolution: Resolution) -> dict[str, Any]:
    """The printed document without the calculator values: ``base`` plus the fleet part."""
    document: dict[str, Any] = {}
    merge_fragment(document, table['base'])
    merge_fragment(document, resolution.fleet)
    return ordered(document, table['section_order'])


# A string is printed bare when it starts with a letter, '_' or '/', holds only these
# characters and does not end with ':' (a plain scalar that ends with ':' starts a mapping).
# The first-character rule also keeps every number-like text quoted.
BARE_STRING = re.compile(r'[A-Za-z_/][A-Za-z0-9_./:-]*')
RESERVED_WORDS = {'true', 'false', 'yes', 'no', 'on', 'off', 'null', '~'}


def yaml_scalar(value: Any) -> str:
    """One scalar as the page prints it."""
    if isinstance(value, bool):
        return 'true' if value else 'false'
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str):
        if BARE_STRING.fullmatch(value) and not value.endswith(':') and value.lower() not in RESERVED_WORDS:
            return value
        return json.dumps(value, ensure_ascii=False)
    raise TypeError(f'no YAML form for {value!r}')


def serialize_yaml(document: dict[str, Any]) -> str:
    """The page's YAML serializer: two-space indent, ``{}`` for an empty mapping, one trailing newline."""
    lines: list[str] = []

    def emit(mapping: dict[str, Any], depth: int) -> None:
        pad = '  ' * depth
        for key, value in mapping.items():
            if isinstance(value, dict) and value:
                lines.append(f'{pad}{yaml_scalar(key)}:')
                emit(value, depth + 1)
            elif isinstance(value, dict):
                lines.append(f'{pad}{yaml_scalar(key)}: {{}}')
            else:
                lines.append(f'{pad}{yaml_scalar(key)}: {yaml_scalar(value)}')

    emit(document, 0)
    return '\n'.join(lines) + '\n'


INTEGER_TEXT = re.compile(r'[0-9]{1,7}')


def shown_steps(table: dict[str, Any], raw_answers: Answers) -> list[str]:
    """The steps the answers show, in order."""
    answers = effective(raw_answers)
    resolution = resolve(table, answers)
    shown = {
        'workload': True,
        'hosts': answers.numbers['workers'] >= 2,
        'services': answers.hosts == 'many',
        'choose': len(resolution.registry_allowed) > 1 or len(resolution.election_allowed) > 1,
        'result': True,
    }
    return [step for step in STEPS if shown[step]]


def parse_query(table: dict[str, Any], query: str) -> tuple[Answers, list[str]]:
    """Read the answers from a query string; return them with the keys whose values were refused."""
    params: dict[str, str] = {}
    for key, value in urllib.parse.parse_qsl(query, keep_blank_values=True):
        params.setdefault(key, value)
    defaults = default_answers(table)
    if params.get('v', '1') != '1':
        return defaults, ['v']
    invalid: set[str] = set()

    numbers = dict(defaults.numbers)
    for name, spec in table['inputs'].items():
        raw = params.get(name)
        if raw is None:
            continue
        if INTEGER_TEXT.fullmatch(raw) and spec['min'] <= int(raw) <= spec['max']:
            numbers[name] = int(raw)
        else:
            invalid.add(name)

    hosts = defaults.hosts
    if numbers['workers'] >= 2 and 'hosts' in params:
        if params['hosts'] in [item['value'] for item in table['choices']['hosts']['values']]:
            hosts = params['hosts']
        else:
            invalid.add('hosts')

    services = defaults.services
    service_values = [item['value'] for item in table['choices']['services']['values']]
    if hosts == 'many' and 'svc' in params:
        items = params['svc'].split(',') if params['svc'] else []
        if all(item in service_values for item in items):
            services = frozenset(items)
        else:
            invalid.add('svc')

    answers = Answers(numbers, hosts, services)
    resolution = resolve(table, answers)
    picks: dict[str, str | None] = {}
    for key, allowed in (('reg', resolution.registry_allowed), ('elect', resolution.election_allowed)):
        picks[key] = None
        raw = params.get(key)
        if len(allowed) < 2 or raw is None:
            continue
        if raw in allowed:
            picks[key] = None if raw == allowed[0] else raw
        else:
            invalid.add(key)

    step = 'workload'
    raw_step = params.get('step')
    if raw_step is not None:
        if raw_step in STEPS:
            shown = shown_steps(table, answers)
            step = next(candidate for candidate in STEPS[STEPS.index(raw_step) :] if candidate in shown)
        else:
            invalid.add('step')

    order = query_key_order(table)
    return Answers(numbers, hosts, services, picks['reg'], picks['elect'], step), [k for k in order if k in invalid]


def query_key_order(table: dict[str, Any]) -> list[str]:
    """The keys of the query, in the order the canonical query writes them."""
    return ['v', 'step', *table['inputs'], 'hosts', 'svc', 'reg', 'elect']


def canonical_query(table: dict[str, Any], raw_answers: Answers) -> str:
    """The canonical query: ``v=1``, then every answer that differs from its default and is not skipped."""
    answers = effective(raw_answers)
    defaults = default_answers(table)
    parts = ['v=1']
    if answers.step != 'workload':
        parts.append(f'step={answers.step}')
    for name in table['inputs']:
        if answers.numbers[name] != defaults.numbers[name]:
            parts.append(f'{name}={answers.numbers[name]}')
    if answers.hosts != defaults.hosts:
        parts.append(f'hosts={answers.hosts}')
    if answers.services:
        service_values = [item['value'] for item in table['choices']['services']['values']]
        parts.append('svc=' + ','.join(value for value in service_values if value in answers.services))
    resolution = resolve(table, answers)
    if answers.reg is not None and answers.reg != resolution.registry_allowed[0]:
        parts.append(f'reg={answers.reg}')
    if answers.elect is not None and answers.elect != resolution.election_allowed[0]:
        parts.append(f'elect={answers.elect}')
    return '&'.join(parts)


# -------------------------------------------------------------- combinations


def all_combinations(table: dict[str, Any]) -> dict[str, Answers]:
    """Every distinct answer set the page accepts, keyed by a readable id."""
    defaults = default_answers(table)
    service_values = [item['value'] for item in table['choices']['services']['values']]
    combinations: dict[str, Answers] = {}
    for workers, hosts in itertools.product((1, 2), ('one', 'many')):
        for size in range(len(service_values) + 1):
            for subset in itertools.combinations(service_values, size):
                numbers = {**defaults.numbers, 'workers': workers}
                answers = effective(Answers(numbers, hosts, frozenset(subset)))
                resolution = resolve(table, answers)
                for reg, elect in itertools.product(resolution.registry_allowed, resolution.election_allowed):
                    picked = Answers(answers.numbers, answers.hosts, answers.services, reg, elect)
                    combinations.setdefault(combination_id(picked), picked)
    return combinations


def combination_id(answers: Answers) -> str:
    """An id such as ``w2-many-folder+redis-reg=redis-elect=filesystem``."""
    if answers.numbers['workers'] == 1:
        head = 'w1'
    elif answers.hosts == 'one':
        head = 'w2-one'
    else:
        head = 'w2-many-' + ('+'.join(sorted(answers.services)) or 'none')
    return f'{head}-reg={answers.reg}-elect={answers.elect}'


COMBINATIONS = all_combinations(TABLE)


@pytest.fixture
def clean_environment(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    """No ``DK_*`` variable set; the returned patcher sets the ones a test needs."""
    for name in list(os.environ):
        if name.startswith('DK_'):
            monkeypatch.delenv(name)
    return monkeypatch


def write_and_load(path: Path, document: dict[str, Any]) -> DrakkarConfig:
    """Write the document with the test-only keys added and load it like an operator's file."""
    data = yaml.safe_load(serialize_yaml(document))
    merge_fragment(data, TEST_ONLY_YAML)
    path.write_text(yaml.safe_dump(data, sort_keys=False), encoding='utf-8')
    return load_config(path)


def value_at(config: DrakkarConfig, path: str) -> Any:
    """The value of a dotted YAML path in the loaded config."""
    node: Any = dump_by_yaml_key(config, mode='json')
    for segment in path.split('.'):
        node = node[segment]
    return node


def leaf_paths(fragment: dict[str, Any], prefix: str = '') -> list[tuple[str, Any]]:
    """Every leaf of a fragment with its dotted path; ``{}`` is a leaf."""
    leaves: list[tuple[str, Any]] = []
    for key, value in fragment.items():
        path = f'{prefix}.{key}' if prefix else key
        if isinstance(value, dict) and value:
            leaves.extend(leaf_paths(value, path))
        else:
            leaves.append((path, value))
    return leaves


# ---------------------------------------------------------------------- tests


def test_table_loads_into_the_shape_models():
    Table.model_validate(TABLE)


def test_every_rule_names_known_providers_and_ends_with_one_that_needs_nothing():
    for rule in TABLE['rules']:
        for feature in ('registry', 'election'):
            providers = TABLE['providers'][feature]
            assert rule[feature], f'empty {feature} list in {rule}'
            assert all(name in providers for name in rule[feature]), rule
            assert providers[rule[feature][-1]]['needs'] == [], rule


def test_the_last_rule_matches_anything():
    assert TABLE['rules'][-1]['when'] == {}


def test_provider_names_exist_in_the_config_model():
    assert set(TABLE['providers']['registry']) <= KNOWN_REGISTRY_PROVIDERS
    assert set(TABLE['providers']['election']) <= KNOWN_ELECTION_PROVIDERS


def test_every_when_value_and_every_need_is_a_known_value():
    hosts = {item['value'] for item in TABLE['choices']['hosts']['values']}
    services = {item['value'] for item in TABLE['choices']['services']['values']}
    known = {
        'workers': {'one', 'many'},
        'hosts': hosts,
        'services_include': services,
        'services_exclude': services,
        'registry': set(TABLE['providers']['registry']),
        'election': set(TABLE['providers']['election']),
    }
    for row in [*TABLE['rules'], *TABLE['notes']]:
        for key, value in row['when'].items():
            assert set(as_list(value)) <= known[key], (key, value)
    for feature in ('registry', 'election'):
        for name, provider in TABLE['providers'][feature].items():
            assert set(provider['needs']) <= services, name
            assert set(provider['needs']) <= set(TABLE['connections']) | {'folder'}, name


def test_section_order_covers_every_top_level_key():
    fragments = [TABLE['base'], *(entry['fragment'] for entry in TABLE['shared'].values())]
    fragments += [entry['fragment'] for entry in TABLE['connections'].values()]
    fragments += [provider['fragment'] for feature in TABLE['providers'].values() for provider in feature.values()]
    fragments += [note['fragment'] for note in TABLE['notes']]
    keys = {key for fragment in fragments for key in fragment}
    assert keys <= set(TABLE['section_order'])
    assert len(TABLE['section_order']) == len(set(TABLE['section_order']))


def heading_anchors(markdown: str) -> set[str]:
    """The anchors of the headings of one page: explicit ``{#id}`` ones and the slugs mkdocs makes."""
    anchors: set[str] = set()
    for line in markdown.splitlines():
        match = re.match(r'#{1,6} (.*)$', line)
        if not match:
            continue
        title = match.group(1)
        explicit = re.search(r'\{#([^}]+)\}\s*$', title)
        if explicit:
            anchors.add(explicit.group(1))
            title = title[: explicit.start()]
        slug = re.sub(r'[^\w\s-]', '', title.replace('`', '').strip().lower())
        anchors.add(re.sub(r'[\s]+', '-', slug))
    return anchors


def test_every_docs_link_points_at_an_existing_heading():
    links = [provider['docs'] for feature in TABLE['providers'].values() for provider in feature.values()]
    links += [entry['docs'] for entry in TABLE['connections'].values()]
    links += [entry['docs'] for entry in TABLE['shared'].values() if entry.get('docs')]
    links += [note['docs'] for note in TABLE['notes']]
    for link in links:
        page, _, anchor = link.partition('#')
        path = DOCS / page
        assert path.is_file(), link
        assert anchor in heading_anchors(path.read_text(encoding='utf-8')), link


def test_combinations_cover_every_provider():
    registries = {answers.reg for answers in COMBINATIONS.values()}
    elections = {answers.elect for answers in COMBINATIONS.values()}
    assert registries == set(TABLE['providers']['registry'])
    assert elections == set(TABLE['providers']['election'])


@pytest.mark.parametrize('answers', list(COMBINATIONS.values()), ids=list(COMBINATIONS))
def test_every_combination_prints_a_config_that_loads(answers, tmp_path, clean_environment):
    resolution = resolve(TABLE, answers)
    document = document_for(TABLE, resolution)
    assert yaml.safe_load(serialize_yaml(document)) == document
    for name in resolution.env:
        clean_environment.setenv(name, ENV_PLACEHOLDERS[name])

    config = write_and_load(tmp_path / 'drakkar.yaml', document)

    for path, value in leaf_paths(resolution.fleet):
        assert value_at(config, path) == value, path
    env_paths = {
        entry['name']: entry['path'] for connection in TABLE['connections'].values() for entry in connection['env']
    }
    for name in resolution.env:
        assert value_at(config, env_paths[name]) == ENV_PLACEHOLDERS[name], name
    assert config.fleet.registry.provider == answers.reg
    assert config.periodic.election.provider == answers.elect
    summary = config.config_summary()
    assert re.search(r' fleet=(\S+)', summary).group(1) == answers.reg
    election_token = re.search(r' election=(\S+)', summary)
    if answers.elect == 'filesystem':
        assert election_token is None, summary
    else:
        assert election_token.group(1) == answers.elect


def test_fragment_leaves_are_never_secret_and_every_secret_has_an_env_entry():
    metadata = {entry.path: entry for group in build_config_metadata().groups for entry in group.entries}

    def metadata_path(path: str) -> str:
        parts = path.split('.')
        if parts[0] == 'connections':
            parts[2] = '*'
        return '.'.join(parts)

    fragments = [entry['fragment'] for entry in TABLE['shared'].values()]
    fragments += [entry['fragment'] for entry in TABLE['connections'].values()]
    fragments += [provider['fragment'] for feature in TABLE['providers'].values() for provider in feature.values()]
    fragments += [note['fragment'] for note in TABLE['notes']]
    for fragment in fragments:
        for path, _value in leaf_paths(fragment):
            entry = metadata.get(metadata_path(path))
            assert entry is not None, f'{path} is not a config field'
            assert not entry.secret, f'{path} is secret and must come from the environment'

    for kind, connection in TABLE['connections'].items():
        env_paths = {entry['path'] for entry in connection['env']}
        secret_keys = [
            path.rsplit('.', 1)[1]
            for path, entry in metadata.items()
            if path.startswith(f'connections.{kind}.*.') and entry.secret
        ]
        assert secret_keys, kind
        assert env_paths == {f'connections.{kind}.main.{key}' for key in secret_keys}, kind
        for entry in connection['env']:
            assert entry['name'] == 'DK_' + '__'.join(entry['path'].split('.')).upper()


def test_a_postgres_combination_without_its_dsn_fails_the_load(tmp_path, clean_environment):
    answers = next(a for a in COMBINATIONS.values() if a.reg == 'postgres' and a.elect == 'postgres')
    document = document_for(TABLE, resolve(TABLE, answers))

    with pytest.raises(ValidationError, match='dsn'):
        write_and_load(tmp_path / 'drakkar.yaml', document)


def test_merging_the_same_leaf_twice_is_an_error():
    target: dict[str, Any] = {}
    merge_fragment(target, {'fleet': {'registry': {'provider': 'none'}}})

    with pytest.raises(ValueError, match=r'^two fragments set fleet\.registry\.provider$'):
        merge_fragment(target, {'fleet': {'registry': {'provider': 'redis'}}})
    with pytest.raises(ValueError, match=r'^two fragments set fleet\.registry$'):
        merge_fragment(target, {'fleet': {'registry': {}}})


@pytest.mark.parametrize(
    ('value', 'printed'),
    [
        ('drakkar', 'drakkar'),
        ('/mnt/drakkar-shared', '/mnt/drakkar-shared'),
        ('http://127.0.0.1:8500', 'http://127.0.0.1:8500'),
        ('drakkar:', '"drakkar:"'),
        ('yes', '"yes"'),
        ('Null', '"Null"'),
        ('8500', '"8500"'),
        ('.inf', '".inf"'),
        ('-1', '"-1"'),
        ('', '""'),
        ('a b', '"a b"'),
        (True, 'true'),
        (0, '0'),
    ],
)
def test_yaml_scalar_quotes_what_yaml_would_read_as_another_type(value, printed):
    assert yaml_scalar(value) == printed
    assert yaml.safe_load(f'key: {printed}\n') == {'key': value}


URL_CASES = json.loads(URL_CASES_PATH.read_text(encoding='utf-8'))['cases']


@pytest.mark.parametrize('case', URL_CASES, ids=[case['name'] for case in URL_CASES])
def test_url_rules_give_the_canonical_query(case):
    answers, invalid = parse_query(TABLE, case['query'])

    assert canonical_query(TABLE, answers) == case['canonical']
    assert invalid == case['invalid']
    assert parse_query(TABLE, case['canonical']) == (answers, [])
