"""The shared schedule contract: cron verdicts and fire times, slots, scopes, hashes and messages.

Every case comes from ``tests/fixtures/periodic_contract.json``, which every
implementation reads, so a library upgrade that changes a fire time fails here.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest

from drakkar import cronexpr, schedule
from drakkar.cronexpr import check_cron_syntax, next_cron_fire, validate_cron
from drakkar.schedule import (
    ClusterTaskDefinition,
    canonical_definitions,
    cluster_tasks_hash,
    next_cron_slot,
    next_interval_slot,
    parse_task_scope,
    task_scope,
)

FIXTURE = json.loads((Path(__file__).parent / 'fixtures' / 'periodic_contract.json').read_text(encoding='utf-8'))
# Refused cases carry no start; any moment serves for their single fire-time lookup.
REFUSED_CASE_NOW = '2026-10-31T00:00:00Z'

# The message constants this module set defines, by fixture ID.
MESSAGE_CONSTANTS = {
    'cron_fields': cronexpr.CRON_FIELDS_MESSAGE,
    'cron_day_rule': cronexpr.CRON_DAY_RULE_MESSAGE,
    'cron_element': cronexpr.CRON_ELEMENT_MESSAGE,
    'cron_value': cronexpr.CRON_VALUE_MESSAGE,
    'cron_range': cronexpr.CRON_RANGE_MESSAGE,
    'cron_step': cronexpr.CRON_STEP_MESSAGE,
    'cron_never': cronexpr.CRON_NEVER_MESSAGE,
    'scope_invalid': schedule.SCOPE_INVALID,
}


def epoch(text: str) -> int:
    """Epoch seconds of an RFC 3339 UTC fixture time."""
    return int(datetime.fromisoformat(text).timestamp())


def _cron_id(case: dict) -> str:
    return f'{case["expr"]!r}@{case.get("start", "-")}'


@pytest.mark.parametrize('case', FIXTURE['cron_cases'], ids=[_cron_id(c) for c in FIXTURE['cron_cases']])
def test_cron_case_verdict_message_and_next_times(case):
    expr = case['expr']
    now = epoch(case.get('start', REFUSED_CASE_NOW))
    never = cronexpr.CRON_NEVER_MESSAGE.format(expr=expr)

    assert validate_cron(expr, now_epoch=now) == case['message']
    assert check_cron_syntax(expr) == (None if case['message'] in (None, never) else case['message'])
    if not case['accepted']:
        return
    fire = now
    for expected in case['next']:
        fire = next_cron_fire(expr, fire)
        assert fire == epoch(expected)


@pytest.mark.parametrize('case', FIXTURE['interval_slots'])
def test_interval_slot_case(case):
    assert next_interval_slot(epoch(case['now']), case['interval']) == epoch(case['next'])


@pytest.mark.parametrize('case', FIXTURE['cron_slots'])
def test_cron_slot_case(case):
    assert next_cron_slot(epoch(case['now']), case['cron']) == epoch(case['next'])


@pytest.mark.parametrize('case', FIXTURE['scopes'])
def test_scope_case_builds_the_scope(case):
    assert task_scope(case['cluster'], case['task']) == case['scope']


@pytest.mark.parametrize('case', FIXTURE['scope_checks'], ids=[repr(c['scope']) for c in FIXTURE['scope_checks']])
def test_scope_check_case(case):
    if case['valid']:
        assert parse_task_scope(case['scope']) == (case['cluster'], case['task'])
        return
    with pytest.raises(ValueError) as caught:
        parse_task_scope(case['scope'])
    assert str(caught.value) == FIXTURE['messages']['scope_invalid'].format(scope=case['scope'])


@pytest.mark.parametrize('case', FIXTURE['hashes'], ids=[c['name'] for c in FIXTURE['hashes']])
def test_hash_case_bytes_and_hash(case):
    definitions = [
        ClusterTaskDefinition(
            name=d['name'], interval=d.get('interval'), cron=d.get('cron'), lease_seconds=d['lease_seconds']
        )
        for d in case['definitions']
    ]

    assert canonical_definitions(definitions) == case['bytes'].encode('utf-8')
    assert cluster_tasks_hash(definitions) == case['hash']


def test_message_constants_equal_the_fixture():
    for message_id, constant in MESSAGE_CONSTANTS.items():
        assert constant == FIXTURE['messages'][message_id], message_id


def test_constants_equal_the_fixture():
    defined = {
        'acquire_window_seconds': schedule.ACQUIRE_WINDOW_SECONDS,
        'min_hold_seconds': schedule.MIN_HOLD_SECONDS,
        'default_lease_seconds': schedule.DEFAULT_LEASE_SECONDS,
        'min_lease_seconds': schedule.MIN_LEASE_SECONDS,
        'max_lease_seconds': schedule.MAX_LEASE_SECONDS,
        'min_cluster_interval_seconds': schedule.MIN_CLUSTER_INTERVAL_SECONDS,
        'archive_task_name': schedule.ARCHIVE_TASK_NAME,
        'system_task_names': list(schedule.SYSTEM_TASK_NAMES),
        'task_name_pattern': f'^{schedule.TASK_NAME_PATTERN}$',
        'task_scope_pattern': f'^{schedule.TASK_SCOPE_PATTERN}$',
    }

    assert defined == FIXTURE['constants']
