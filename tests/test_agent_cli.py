"""The offline agent command: pinned output bytes, usage errors and the generated reference page."""

from __future__ import annotations

import io
import json
import os
import runpy
import subprocess
import sys
import tomllib
from pathlib import Path
from typing import Any

import pytest
import structlog

from drakkar.agent import cli, reference
from drakkar.agent import registry as reg

REPO_ROOT = Path(__file__).parent.parent
FIXTURES = Path(__file__).parent / 'fixtures'
CONTRACT = json.loads((FIXTURES / 'agent_contract.json').read_text(encoding='utf-8'))
CLI = CONTRACT['cli_cases']
REFERENCE_PAGE = REPO_ROOT / 'docs' / 'agent-reference.md'
CONFIG_PLACEHOLDER = '{config}'

# The smallest config the strict loader accepts: one input source must be on.
MINIMAL_YAML = 'sources:\n  http:\n    enabled: true\n'


def _tiny_registry() -> reg.Registry:
    files = {name: text.encode('utf-8') for name, text in CONTRACT['tiny_registry']['files'].items()}
    files[reg.SCHEMA_FILE] = (REPO_ROOT / 'drakkar' / 'agentdocs' / 'schema.json').read_bytes()
    return reg.build_registry(files=files, config_metadata=CONTRACT['tiny_registry']['config_metadata'])


@pytest.fixture
def clean_env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    """No ``DK_`` variable of the test process reaches the loader."""
    for name in list(os.environ):
        if name.startswith('DK_'):
            monkeypatch.delenv(name)
    return monkeypatch


def _run(argv: list[str], registry: reg.Registry | None = None) -> tuple[int, str, str]:
    stdout = io.StringIO()
    stderr = io.StringIO()
    code = cli.main(argv, stdout=stdout, stderr=stderr, registry=registry)
    return code, stdout.getvalue(), stderr.getvalue()


def _case_argv(case: dict[str, Any], tmp_path: Path) -> list[str]:
    config_path = tmp_path / 'drakkar.yaml'
    if 'config_yaml' in case:
        config_path.write_text(case['config_yaml'], encoding='utf-8')
    return [arg.replace(CONFIG_PLACEHOLDER, str(config_path)) for arg in case['argv']]


@pytest.mark.parametrize('case', CLI['cases'], ids=lambda case: case['name'])
def test_cli_cases(case: dict[str, Any], tmp_path: Path, clean_env: pytest.MonkeyPatch):
    for name, value in case.get('env', {}).items():
        clean_env.setenv(name, value)
    code, out, err = _run(_case_argv(case, tmp_path), registry=_tiny_registry())
    assert code == case['exit_code']
    assert out == case['stdout']
    if code == CLI['usage_exit_code']:
        assert out == ''
        assert err.startswith(f'{CLI["program"]}: ')


def test_cli_fixture_exit_codes_are_the_module_constants():
    assert CLI['program'] == cli.PROGRAM
    assert CLI['usage_exit_code'] == cli.EXIT_USAGE
    assert {case['exit_code'] for case in CLI['cases']} == {cli.EXIT_OK, cli.EXIT_USAGE}


def test_help_prints_the_usage_text_to_stdout():
    for argv in (['--help'], ['-h'], ['manual', '--detail=nope', '--help']):
        assert _run(argv) == (cli.EXIT_OK, cli.USAGE_TEXT, '')


def test_a_usage_error_writes_the_reason_and_the_usage_text_to_stderr():
    code, out, err = _run(['manual', '--detail'])
    assert (code, out) == (cli.EXIT_USAGE, '')
    assert err == f"drakkar-agent: unknown argument '--detail'\n\n{cli.USAGE_TEXT}"


def test_a_load_error_writes_only_the_reason(tmp_path: Path, clean_env: pytest.MonkeyPatch):
    config_path = tmp_path / 'drakkar.yaml'
    config_path.write_text('nope: 1\n' + MINIMAL_YAML, encoding='utf-8')
    code, out, err = _run(['config', f'--config={config_path}'])
    assert (code, out) == (cli.EXIT_USAGE, '')
    assert err.startswith('drakkar-agent: config has unknown keys')
    assert cli.USAGE_TEXT not in err


def test_manual_prints_the_route_text_of_the_shipped_registry():
    registry = reg.load_registry()
    selection = reg.select_manual(registry, parts=['concepts', 'rules'], detail='full')
    assert _run(['manual', '--parts=rules,concepts', '--detail=full']) == (
        cli.EXIT_OK,
        reg.dumps(reg.selection_json(selection)) + '\n',
        '',
    )
    assert _run(['manual', '--parts=rules,concepts', '--detail=full', '--format=markdown']) == (
        cli.EXIT_OK,
        reg.selection_markdown(selection),
        '',
    )


def test_manual_keeps_the_promql_placeholders():
    code, out, _ = _run(['manual', '--parts=metrics', '--detail=full'])
    assert code == cli.EXIT_OK
    assert '{{worker}}' in out


def test_config_masks_secrets_and_shows_advice_with_full_detail(tmp_path: Path, clean_env: pytest.MonkeyPatch):
    config_path = tmp_path / 'drakkar.yaml'
    config_path.write_text(
        MINIMAL_YAML
        + 'agent:\n  enabled: true\n  auth_token: the-agent-secret\napp:\n  ledger_dsn: postgres://u:p@h/db\n',
        encoding='utf-8',
    )
    clean_env.setenv('DK_EXECUTOR__MAX_EXECUTORS', '7')
    code, out, _ = _run(['config', f'--config={config_path}', '--detail=full'])
    assert code == cli.EXIT_OK
    assert out.endswith('\n')
    entries = {entry['path']: entry for entry in json.loads(out)['entries']}
    assert entries['agent.auth_token']['value'] == '***'
    assert entries['agent.auth_token']['origin'] == 'yaml'
    assert entries['app.ledger_dsn']['value'] == '***'
    assert (entries['executor.max_executors']['value'], entries['executor.max_executors']['origin']) == ('7', 'env')
    assert 'advice' in entries['executor.max_executors']
    assert {entry['origin'] for entry in entries.values()} <= {'default', 'yaml', 'env'}
    assert 'the-agent-secret' not in out
    assert 'u:p@' not in out


def test_config_with_a_missing_file_is_a_load_error(tmp_path: Path, clean_env: pytest.MonkeyPatch):
    code, out, err = _run(['config', f'--config={tmp_path / "missing.yaml"}'])
    assert (code, out) == (cli.EXIT_USAGE, '')
    assert 'missing.yaml' in err


def test_the_committed_reference_page_is_the_generated_page():
    assert REFERENCE_PAGE.read_text(encoding='utf-8') == reference.reference_markdown(reg.load_registry())


def test_reference_prints_the_page():
    assert _run(['reference']) == (cli.EXIT_OK, reference.reference_markdown(reg.load_registry()), '')


@pytest.mark.parametrize(
    ('text', 'escaped'),
    [
        ('plain text', 'plain text'),
        ('a <b> & c', 'a &lt;b&gt; &amp; c'),
        ('keep `a<b>&c` and <x>', 'keep `a<b>&c` and &lt;x&gt;'),
        ('`<one>` <two> `<three>`', '`<one>` &lt;two&gt; `<three>`'),
        ('an odd ` <tail>', 'an odd ` <tail>'),
    ],
)
def test_reference_escapes_html_outside_code_spans(text: str, escaped: str):
    assert reference.escape_outside_code(text) == escaped


def test_the_reference_page_holds_every_entry_but_no_handler_notes():
    page = reference.reference_markdown(reg.load_registry())
    registry = reg.load_registry()
    for part, entries in registry.parts.items():
        for entry in entries:
            assert f'### `{reg.reference_of(part, entry["key"])}`' in page
    assert 'app:' not in page.split('\n## ', 1)[0]


def test_the_console_script_names_the_cli():
    pyproject = tomllib.loads((REPO_ROOT / 'pyproject.toml').read_text(encoding='utf-8'))
    assert pyproject['project']['scripts']['drakkar-agent'] == 'drakkar.agent.cli:run'


def test_the_package_runs_as_a_module(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
    monkeypatch.setattr(sys, 'argv', ['drakkar.agent', 'manual', '--parts=rules'])
    saved = structlog.get_config()
    try:
        with pytest.raises(SystemExit) as stopped:
            runpy.run_module('drakkar.agent', run_name='__main__')
    finally:
        # The entry sends log lines to stderr; the other tests keep their logging.
        structlog.configure(**saved)
    assert stopped.value.code == cli.EXIT_OK
    assert json.loads(capsys.readouterr().out)['parts']['rules']


def test_a_subprocess_keeps_log_lines_off_stdout(tmp_path: Path):
    config_path = tmp_path / 'drakkar.yaml'
    # A memory cache with no cap logs a warning while the config loads.
    config_path.write_text(MINIMAL_YAML + 'cache:\n  enabled: true\n  max_memory_entries: 0\n', encoding='utf-8')
    env = {name: value for name, value in os.environ.items() if not name.startswith('DK_')}
    done = subprocess.run(
        [sys.executable, '-m', 'drakkar.agent', 'config', f'--config={config_path}', '--parts=cache'],
        capture_output=True,
        text=True,
        env=env,
        check=False,
        timeout=60,
    )
    assert done.returncode == cli.EXIT_OK, done.stderr
    assert json.loads(done.stdout)['entries']
    assert 'cache_max_memory_entries_unbounded' in done.stderr


def test_an_empty_part_of_the_reference_page_says_so():
    parts = {part: () for part, _ in reference.REFERENCE_PARTS}
    page = reference.reference_markdown(reg.Registry(parts=parts, config_paths=frozenset(), version='0' * 16))
    assert page.endswith('## Rules\n\nNo entries.\n')
    assert page.count('No entries.') == len(reference.REFERENCE_PARTS)
