"""The ``periodic`` config section and the schedule keys of ``history.archive``.

The shared cases live in ``tests/fixtures/config_contract.json``; these tests
cover what the fixture cannot pin: the order of several bad override entries,
the environment form of an override, and the effective archive schedule.
"""

import json
import os
import re
from pathlib import Path

import pytest
from pydantic import ValidationError

from drakkar.config import DrakkarConfig, HistoryArchiveConfig, PeriodicTaskOverride, load_config

MESSAGES = json.loads((Path(__file__).parent / 'fixtures' / 'periodic_contract.json').read_text(encoding='utf-8'))[
    'messages'
]


def _config(**sections) -> DrakkarConfig:
    """A loadable config: one enabled source, bundle fetching off, plus ``sections``."""
    sections.setdefault('sources', {'kafka': {'enabled': True}})
    ui = sections.setdefault('ui', {})
    ui.setdefault('release', {'enabled': False})
    return DrakkarConfig(**sections)


def _override_message(name: str, rule_id: str) -> str:
    return MESSAGES['prefix_override'].format(name=name) + MESSAGES[rule_id]


def test_override_entries_are_checked_in_sorted_name_order():
    bad = {'interval': 60, 'cron': '* * * * *'}

    with pytest.raises(ValidationError) as excinfo:
        _config(periodic={'tasks': {'b_task': bad, 'a_task': bad}})

    text = str(excinfo.value)
    assert _override_message('a_task', 'rule_at_most_one') in text
    assert 'b_task' not in text


def test_override_null_entries_are_named_in_sorted_order():
    with pytest.raises(ValidationError) as excinfo:
        _config(periodic={'tasks': {'b_task': None, 'a_task': None}})

    text = str(excinfo.value)
    assert MESSAGES['override_null'].replace('{name}', 'a_task') in text
    assert 'b_task' not in text


def test_override_by_env_sets_a_cron(monkeypatch):
    for name in list(os.environ):
        if name.startswith('DK_'):
            monkeypatch.delenv(name)
    monkeypatch.setenv('DK_SOURCES__KAFKA__ENABLED', 'true')
    monkeypatch.setenv('DK_UI__RELEASE__ENABLED', 'false')
    monkeypatch.setenv('DK_PERIODIC__TASKS__REFRESH_RATES__CRON', '*/10 * * * *')

    config = load_config(None)

    assert config.periodic.tasks == {'refresh_rates': PeriodicTaskOverride(cron='*/10 * * * *')}


def test_override_enabled_false_loads():
    config = _config(periodic={'tasks': {'refresh_rates': {'enabled': False}}})

    override = config.periodic.tasks['refresh_rates']
    assert override.enabled is False
    assert (override.interval, override.cron, override.lease_seconds) == (None, None, None)


def test_override_empty_entry_keeps_every_declared_setting():
    config = _config(periodic={'tasks': {'refresh_rates': {}}})

    assert config.periodic.tasks['refresh_rates'] == PeriodicTaskOverride()


def test_override_with_a_trailing_newline_in_its_cron_fails_the_load():
    with pytest.raises(ValidationError) as excinfo:
        _config(periodic={'tasks': {'refresh_rates': {'cron': '* * * * *\n'}}})

    assert MESSAGES['prefix_override'].format(name='refresh_rates') in str(excinfo.value)


def test_archive_effective_interval_defaults_to_3600_and_is_none_with_cron():
    assert HistoryArchiveConfig().effective_interval == 3600
    assert HistoryArchiveConfig(interval=600).effective_interval == 600
    assert HistoryArchiveConfig(cron='0 * * * *').effective_interval is None


def test_archive_interval_with_a_fraction_fails_the_load():
    with pytest.raises(ValidationError) as excinfo:
        _config(history={'archive': {'interval': 30.5}})

    assert re.search(r'history\.archive\.interval|archive\.interval', str(excinfo.value))


def test_archive_empty_cron_is_checked_as_a_given_cron():
    with pytest.raises(ValidationError) as excinfo:
        HistoryArchiveConfig(cron='')

    assert MESSAGES['prefix_archive'] + MESSAGES['cron_fields'].format(expr='') in str(excinfo.value)


def test_archive_checks_the_schedule_before_the_retention():
    with pytest.raises(ValidationError) as excinfo:
        HistoryArchiveConfig(interval=10, retention_days=1)

    text = str(excinfo.value)
    assert MESSAGES['prefix_archive'] + MESSAGES['rule_archive_interval'] in text
    assert 'keep archives forever' not in text
