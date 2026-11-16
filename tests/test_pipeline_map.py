"""The pipeline map of the docs (``docs/data/pipeline-map.json``).

The page ``docs/pipeline-map.md`` reads the data file to show, for each way
to define the input of a worker, the hooks that run and what happens when a
step fails. These tests tie the data to the code: every hook it names is a
handler attribute, every config key a metadata path, every policy value one
the config accepts (and every accepted value has an outcome). The page also
carries the reference tables built from the data, so it reads without
JavaScript; run this module as a script to write them again::

    uv run python tests/test_pipeline_map.py
"""

import json
import re
import types
import typing
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field

from drakkar.config import DrakkarConfig
from drakkar.handler import BaseDrakkarHandler

REPO = Path(__file__).resolve().parent.parent
DOCS = REPO / 'docs'
DATA_PATH = DOCS / 'data' / 'pipeline-map.json'
PAGE_PATH = DOCS / 'pipeline-map.md'
SCRIPT_PATH = DOCS / 'javascripts' / 'pipeline-map.js'
METADATA_PATH = REPO / 'drakkar' / 'uiserver' / 'config-metadata.json'
METRICS_PATH = REPO / 'drakkar' / 'metrics.py'
MKDOCS_PATH = REPO / 'mkdocs.yml'

OffsetWord = Literal['commits', 'stalls', 'replays', 'pending']
OFFSET_WORDS = set(typing.get_args(OffsetWord))
FORM_IDS = ('kafka-list', 'kafka-stream', 'http')
KAFKA_FORMS = {'kafka-list', 'kafka-stream'}
STEP_IDS = ('parse', 'arrange', 'task-run', 'completion-hook', 'sink-delivery', 'dlq-write', 'stop')
POLICY_KEYS = (
    'sources.kafka.on_parse_error',
    'executor.on_failure',
    'sinks.defaults.on_failure',
    'dlq.on_send_failure',
)
TABLES_START = '<!-- pipeline-map-tables:start -->'
TABLES_END = '<!-- pipeline-map-tables:end -->'
METRIC_NAME = re.compile(r'\bdrakkar_[a-z0-9_]+')


# ---------------------------------------------------------------- data shape


class Strict(BaseModel):
    """A data object: unknown keys are an error, so a typo cannot pass unnoticed."""

    model_config = ConfigDict(extra='forbid')


class Hook(Strict):
    name: str
    step: str
    runs: str
    receives: str
    kept_until_it_runs: str


class Form(Strict):
    id: str
    title: str
    short: str
    summary: str
    arrange_hook: str
    unit: Literal['window', 'group', 'request']
    start: str
    end: str
    hooks: list[Hook]
    notes: list[str]


class DecidedBy(Strict):
    kind: Literal['config', 'hook']
    ref: str
    forms: list[str] | None = None


class Policy(Strict):
    key: str
    value: str


class Outcome(Strict):
    when: str
    result: str
    record: str
    forms: list[str] | None = None
    forms_result: dict[str, str] = Field(default_factory=dict)
    offset: OffsetWord | None = None
    response: str | None = None
    policy: Policy | None = None


class Step(Strict):
    id: str
    title: str
    summary: str
    forms: list[str]
    titles_by_form: dict[str, str] = Field(default_factory=dict)
    decided_by: list[DecidedBy]
    outcomes: list[Outcome]


class PipelineMap(Strict):
    schema_version: Literal[1]
    offset_words: dict[str, str]
    forms: list[Form]
    steps: list[Step]


def load_map() -> PipelineMap:
    """The data file, checked against the shape models."""
    return PipelineMap.model_validate(json.loads(DATA_PATH.read_text(encoding='utf-8')))


def outcome_forms(step: Step, outcome: Outcome) -> list[str]:
    """The forms an outcome applies to: its own list, else every form of its step."""
    return outcome.forms if outcome.forms is not None else step.forms


def metadata_paths() -> set[str]:
    """Every leaf path of the config metadata, with ``*`` for map names."""
    metadata = json.loads(METADATA_PATH.read_text(encoding='utf-8'))
    return {field['path'] for group in metadata['groups'] for field in group['entries']}


def unwrap(annotation: Any) -> Any:
    """The annotation without ``Optional`` and ``Annotated`` around it."""
    while True:
        origin = typing.get_origin(annotation)
        if origin is typing.Annotated:
            annotation = typing.get_args(annotation)[0]
        elif origin in (typing.Union, types.UnionType):
            members = [arg for arg in typing.get_args(annotation) if arg is not type(None)]
            if len(members) != 1:
                return annotation
            annotation = members[0]
        else:
            return annotation


def accepted_values(path: str) -> set[str]:
    """The values the config model accepts for a ``Literal`` key at a dotted YAML path."""
    model: Any = DrakkarConfig
    annotation: Any = None
    for segment in path.split('.'):
        annotation = unwrap(model.model_fields[segment].annotation)
        model = annotation
    assert typing.get_origin(annotation) is Literal, path
    return set(typing.get_args(annotation))


def every_text(pipeline_map: PipelineMap) -> list[str]:
    """Every text of the data that the page shows."""
    texts: list[str] = list(pipeline_map.offset_words.values())
    for form in pipeline_map.forms:
        texts += [form.title, form.short, form.summary, form.start, form.end, *form.notes]
        for hook in form.hooks:
            texts += [hook.runs, hook.receives, hook.kept_until_it_runs]
    for step in pipeline_map.steps:
        texts += [step.title, step.summary, *step.titles_by_form.values()]
        for outcome in step.outcomes:
            texts += [outcome.when, outcome.result, outcome.record, *outcome.forms_result.values()]
            texts += [text for text in (outcome.response,) if text]
    return texts


# ------------------------------------------------------------ static tables


def render_tables(pipeline_map: PipelineMap) -> str:
    """The Markdown reference tables of the page: hooks per form, then the outcomes of each step."""
    forms = {form.id: form for form in pipeline_map.forms}
    lines = [TABLES_START, '', '### Hooks of each input form', '']
    for form in pipeline_map.forms:
        lines += [f'#### {form.title}', '', form.summary, '']
        lines += ['| Hook | Runs | Receives | Kept in memory until it runs |', '|---|---|---|---|']
        for hook in form.hooks:
            lines.append(f'| `{hook.name}` | {hook.runs} | {hook.receives} | {hook.kept_until_it_runs} |')
        lines.append('')
        lines += [f'- {note}' for note in form.notes]
        lines.append('')
    lines += ['### What can fail at each step', '']
    lines += ['| Offset word | Meaning |', '|---|---|']
    lines += [f'| **{word}** | {meaning} |' for word, meaning in pipeline_map.offset_words.items()]
    lines.append('')
    for step in pipeline_map.steps:
        lines += [f'#### {step.title}', '', step.summary, '']
        decided = ', '.join(
            f'`{entry.ref}`' + ('' if entry.forms is None else f' ({", ".join(forms[f].short for f in entry.forms)})')
            for entry in step.decided_by
        )
        lines += [f'Decided by: {decided}.', '']
        lines += ['| When | Form | What happens | Offset or response | Record |', '|---|---|---|---|---|']
        for outcome in step.outcomes:
            for form_ids, result, end in outcome_rows(step, outcome):
                names = ', '.join(forms[form_id].short for form_id in form_ids)
                lines.append(f'| {outcome.when} | {names} | {result} | {end} | {outcome.record} |')
        lines.append('')
    lines.append(TABLES_END)
    return '\n'.join(lines)


def outcome_rows(step: Step, outcome: Outcome) -> list[tuple[list[str], str, str]]:
    """The table rows of one outcome: forms that share a result and an end share a row."""
    rows: list[tuple[list[str], str, str]] = []
    for form_id in outcome_forms(step, outcome):
        result = outcome.forms_result.get(form_id, outcome.result)
        end = (outcome.response or '') if form_id == 'http' else f'**{outcome.offset}**'
        for form_ids, row_result, row_end in rows:
            if (row_result, row_end) == (result, end):
                form_ids.append(form_id)
                break
        else:
            rows.append(([form_id], result, end))
    return rows


def page_tables() -> str:
    """The block of reference tables as the page holds it now."""
    page = PAGE_PATH.read_text(encoding='utf-8')
    start = page.index(TABLES_START)
    end = page.index(TABLES_END) + len(TABLES_END)
    return page[start:end]


# ---------------------------------------------------------------------- tests


def test_pipeline_map_loads_into_the_shape_models():
    assert load_map().schema_version == 1


def test_pipeline_map_has_the_three_forms_and_the_seven_steps():
    pipeline_map = load_map()
    assert tuple(form.id for form in pipeline_map.forms) == FORM_IDS
    assert tuple(step.id for step in pipeline_map.steps) == STEP_IDS
    assert {form.id: form.arrange_hook for form in pipeline_map.forms} == {
        'kafka-list': 'arrange',
        'kafka-stream': 'arrange_stream',
        'http': 'arrange_http_request',
    }


def test_pipeline_map_hooks_are_handler_attributes():
    pipeline_map = load_map()
    names = [form.arrange_hook for form in pipeline_map.forms]
    names += [hook.name for form in pipeline_map.forms for hook in form.hooks]
    names += [entry.ref for step in pipeline_map.steps for entry in step.decided_by if entry.kind == 'hook']
    missing = sorted({name for name in names if not callable(getattr(BaseDrakkarHandler, name, None))})
    assert missing == []


def test_pipeline_map_config_keys_are_metadata_paths():
    paths = metadata_paths()
    pipeline_map = load_map()
    refs = [entry.ref for step in pipeline_map.steps for entry in step.decided_by if entry.kind == 'config']
    refs += [o.policy.key for step in pipeline_map.steps for o in step.outcomes if o.policy is not None]
    assert sorted(set(refs) - paths) == []


def test_pipeline_map_policy_values_are_the_values_the_config_accepts():
    pipeline_map = load_map()
    named: dict[str, set[str]] = {key: set() for key in POLICY_KEYS}
    for step in pipeline_map.steps:
        for outcome in step.outcomes:
            if outcome.policy is not None:
                assert outcome.policy.key in named, outcome.policy.key
                named[outcome.policy.key].add(outcome.policy.value)
    for key in POLICY_KEYS:
        assert named[key] == accepted_values(key), key


def test_pipeline_map_every_policy_key_is_named_by_its_step():
    pipeline_map = load_map()
    for step in pipeline_map.steps:
        refs = {entry.ref for entry in step.decided_by}
        for outcome in step.outcomes:
            if outcome.policy is not None:
                assert outcome.policy.key in refs, (step.id, outcome.policy.key)


def test_pipeline_map_kafka_outcomes_have_an_offset_and_http_outcomes_a_response():
    pipeline_map = load_map()
    for step in pipeline_map.steps:
        for outcome in step.outcomes:
            forms = set(outcome_forms(step, outcome))
            if forms & KAFKA_FORMS:
                assert outcome.offset in OFFSET_WORDS, (step.id, outcome.when)
            else:
                assert outcome.offset is None, (step.id, outcome.when)
            assert (outcome.response is not None) == ('http' in forms), (step.id, outcome.when)


def test_pipeline_map_offset_words_are_the_four_words():
    assert set(load_map().offset_words) == OFFSET_WORDS


def test_pipeline_map_form_lists_name_forms_of_their_step():
    pipeline_map = load_map()
    for step in pipeline_map.steps:
        assert set(step.forms) <= set(FORM_IDS), step.id
        assert set(step.titles_by_form) <= set(step.forms), step.id
        for entry in step.decided_by:
            assert set(entry.forms or []) <= set(step.forms), (step.id, entry.ref)
        for outcome in step.outcomes:
            forms = set(outcome_forms(step, outcome))
            assert forms and forms <= set(step.forms), (step.id, outcome.when)
            assert set(outcome.forms_result) <= forms, (step.id, outcome.when)


def test_pipeline_map_every_step_has_an_outcome_for_each_of_its_forms():
    pipeline_map = load_map()
    for step in pipeline_map.steps:
        covered = {form for outcome in step.outcomes for form in outcome_forms(step, outcome)}
        assert covered == set(step.forms), step.id


def test_pipeline_map_hooks_run_at_a_step_of_their_form():
    pipeline_map = load_map()
    steps = {step.id: step for step in pipeline_map.steps}
    for form in pipeline_map.forms:
        assert form.arrange_hook in {hook.name for hook in form.hooks}, form.id
        for hook in form.hooks:
            assert hook.step in steps, (form.id, hook.name)
            assert form.id in steps[hook.step].forms, (form.id, hook.name)


def test_pipeline_map_texts_name_existing_metrics_and_hold_no_pipe():
    metrics_source = METRICS_PATH.read_text(encoding='utf-8')
    for text in every_text(load_map()):
        # A pipe would break a row of the static tables.
        assert '|' not in text, text
        for name in METRIC_NAME.findall(text):
            base = name.removesuffix('_total')
            assert f"'{base}" in metrics_source, name


def test_pipeline_map_page_holds_the_static_tables():
    assert page_tables() == render_tables(load_map())


def test_pipeline_map_page_has_a_static_diagram_and_the_map_container():
    page = PAGE_PATH.read_text(encoding='utf-8')
    assert '```mermaid' in page
    assert 'id="pipeline-map-app"' in page
    for step in load_map().steps:
        assert f'#### {step.title}' in page, step.id


def test_pipeline_map_script_is_loaded_by_the_site():
    mkdocs = yaml.safe_load(
        '\n'.join(line for line in MKDOCS_PATH.read_text(encoding='utf-8').splitlines() if '!!python' not in line)
    )
    assert 'javascripts/pipeline-map.js' in mkdocs['extra_javascript']
    assert SCRIPT_PATH.is_file()
    nav_pages = [next(iter(entry.values())) for entry in mkdocs['nav']]
    assert nav_pages.index('pipeline-map.md') == nav_pages.index('handler.md') + 1


if __name__ == '__main__':
    page = PAGE_PATH.read_text(encoding='utf-8')
    start = page.index(TABLES_START)
    end = page.index(TABLES_END) + len(TABLES_END)
    PAGE_PATH.write_text(page[:start] + render_tables(load_map()) + page[end:], encoding='utf-8')
