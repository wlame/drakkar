"""The ``periodic`` config section and the schedule keys of ``history.archive``.

The shared cases live in ``tests/fixtures/config_contract.json``; these tests
cover what the fixture cannot pin: the fields of the section and the
effective archive schedule.
"""

import json
import re
from pathlib import Path

import pytest
from pydantic import ValidationError

from drakkar.config import DrakkarConfig, HistoryArchiveConfig, PeriodicConfig

MESSAGES = json.loads((Path(__file__).parent / 'fixtures' / 'periodic_contract.json').read_text(encoding='utf-8'))[
    'messages'
]


def _config(**sections) -> DrakkarConfig:
    """A loadable config: one enabled source, bundle fetching off, plus ``sections``."""
    sections.setdefault('sources', {'kafka': {'enabled': True}})
    ui = sections.setdefault('ui', {})
    ui.setdefault('release', {'enabled': False})
    return DrakkarConfig(**sections)


def test_periodic_section_holds_only_the_election():
    assert set(PeriodicConfig.model_fields) == {'election'}


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
