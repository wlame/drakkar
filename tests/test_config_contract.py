"""Config contract cases from ``tests/fixtures/config_contract.json``.

The fixture pins three things byte for byte for any implementation of the
config format: the load error for moved or removed keys, the summary
one-liner, and the cross-field validation messages. Every case goes
through the public loader exactly as an operator's file would: its ``yaml``
is merged over the fixture's ``base`` and written to a real YAML file, and
its ``env`` entries are set as ``DK_*`` variables.
"""

import json
import os
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from drakkar.config import DrakkarConfig, _deep_merge, load_config

FIXTURE = json.loads((Path(__file__).parent / 'fixtures' / 'config_contract.json').read_text(encoding='utf-8'))


def _case_ids(cases: list[dict]) -> list[str]:
    return [case['name'] for case in cases]


def _load(case: dict, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> DrakkarConfig:
    """Load one fixture case through ``load_config`` with only its own ``DK_*`` variables set."""
    for name in list(os.environ):
        if name.startswith('DK_'):
            monkeypatch.delenv(name)
    for name, value in case.get('env', {}).items():
        monkeypatch.setenv(name, value)
    if case.get('env_only'):
        return load_config(None)
    path = tmp_path / 'drakkar.yaml'
    path.write_text(yaml.safe_dump(_deep_merge(FIXTURE['base'], case['yaml'])), encoding='utf-8')
    return load_config(path)


def test_fixture_case_names_are_unique():
    for section in ('moved_keys', 'summary', 'validation'):
        names = _case_ids(FIXTURE[section])
        assert len(names) == len(set(names)), section


@pytest.mark.parametrize('case', FIXTURE['moved_keys'], ids=_case_ids(FIXTURE['moved_keys']))
def test_moved_key_case_fails_the_load_with_the_contract_message(case, tmp_path, monkeypatch):
    with pytest.raises(ValidationError) as excinfo:
        _load(case, tmp_path, monkeypatch)

    assert case['message'] in str(excinfo.value)


@pytest.mark.parametrize('case', FIXTURE['summary'], ids=_case_ids(FIXTURE['summary']))
def test_summary_case_matches_the_contract_line(case, tmp_path, monkeypatch):
    config = _load(case, tmp_path, monkeypatch)

    assert config.config_summary(worker_id=case['worker'], cluster_name=case['cluster']) == case['line']


@pytest.mark.parametrize('case', FIXTURE['validation'], ids=_case_ids(FIXTURE['validation']))
def test_validation_case_fails_the_load_naming_the_rule(case, tmp_path, monkeypatch):
    with pytest.raises(ValidationError) as excinfo:
        _load(case, tmp_path, monkeypatch)

    assert (case.get('message') or case['path']) in str(excinfo.value)
