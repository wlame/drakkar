"""Tests for the ``drakkar-schema`` command: pinned DDL output and the usage errors."""

import io
import json
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

from drakkar.schema import main

FIXTURES = Path(__file__).parent / 'fixtures'
FIXTURE = json.loads((FIXTURES / 'fleet_providers.json').read_text())
PYPROJECT = Path(__file__).parent.parent / 'pyproject.toml'


@pytest.mark.parametrize('case', FIXTURE['schema_command'], ids=lambda case: case['args'][1])
def test_main_prints_the_pinned_ddl(case):
    stdout, stderr = io.StringIO(), io.StringIO()

    code = main(case['args'], stdout, stderr)

    assert code == 0
    assert stdout.getvalue() == case['stdout']
    assert stderr.getvalue() == ''


@pytest.mark.parametrize(
    'case', FIXTURE['schema_command_usage_errors'], ids=lambda case: ' '.join(case['args']) or 'no-args'
)
def test_main_rejects_bad_usage_with_nothing_on_stdout(case):
    stdout, stderr = io.StringIO(), io.StringIO()

    code = main(case['args'], stdout, stderr)

    assert code == case['exit'] == 2
    assert stdout.getvalue() == ''
    assert 'Usage:' in stderr.getvalue()


@pytest.mark.parametrize('flag', ['--help', '-h'])
def test_main_help_prints_usage_to_stdout(flag):
    stdout, stderr = io.StringIO(), io.StringIO()

    code = main(['postgres', flag], stdout, stderr)

    assert code == 0
    assert 'Usage:' in stdout.getvalue()
    assert stderr.getvalue() == ''


def test_main_with_an_extra_argument_is_a_usage_error():
    stdout, stderr = io.StringIO(), io.StringIO()

    code = main(['postgres', '--schema=drakkar', 'extra'], stdout, stderr)

    assert code == 2
    assert stdout.getvalue() == ''
    assert 'extra' in stderr.getvalue()


def test_main_reads_argv_when_none_is_given(monkeypatch):
    monkeypatch.setattr(sys, 'argv', ['drakkar-schema', 'postgres', '--schema=drakkar'])
    stdout = io.StringIO()

    code = main(stdout=stdout)

    assert code == 0
    assert stdout.getvalue() == FIXTURE['schema_command'][0]['stdout']


def test_module_runs_as_a_script():
    result = subprocess.run(
        [sys.executable, '-m', 'drakkar.schema', 'postgres', '--schema=drakkar'],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert result.stdout == FIXTURE['schema_command'][0]['stdout']


def test_console_script_is_declared():
    scripts = tomllib.loads(PYPROJECT.read_text())['project']['scripts']

    assert scripts['drakkar-schema'] == 'drakkar.schema:main'
