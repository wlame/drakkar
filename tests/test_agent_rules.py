"""The diagnose rules: the contract cases, the baselines, failures and the answer order."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from drakkar.agent.registry import dumps, load_registry
from drakkar.agent.rules import (
    FRAMEWORK_RULES,
    SEVERITY_ORDER,
    Baselines,
    DiagnoseResult,
    RuleInput,
    RuleSettings,
    diagnose_answer,
    run_framework_rules,
)

CONTRACT = json.loads((Path(__file__).parent / 'fixtures' / 'agent_contract.json').read_text(encoding='utf-8'))
CASES = CONTRACT['diagnose_cases']['cases']
REGISTRY = load_registry()
RULE_ENTRIES = REGISTRY.parts['rules']
RULE_KEYS = [entry['key'] for entry in RULE_ENTRIES]


def _input(raw: dict[str, Any], now: float) -> RuleInput:
    return RuleInput(
        state=raw['state'],
        metrics=raw['metrics'],
        settings=RuleSettings(**raw['settings']),
        now=now,
    )


@pytest.mark.parametrize('case', CASES, ids=[case['name'] for case in CASES])
def test_diagnose_cases(case):
    baselines = Baselines()

    for number, step in enumerate(case['steps']):
        result = run_framework_rules(_input(step['input'], step['now']), rule_entries=RULE_ENTRIES, baselines=baselines)
        answer = diagnose_answer(result)

        found = [{'rule': f['rule'], 'severity': f['severity'], 'evidence': f['evidence']} for f in answer['findings']]
        assert dumps(found) == dumps(step['findings']), f'step {number}'
        assert answer['checked'] == RULE_KEYS
        assert answer['failed'] == []


def test_every_rule_entry_has_an_implementation_and_back():
    assert list(FRAMEWORK_RULES) == RULE_KEYS


def test_a_finding_carries_the_text_of_its_registry_entry():
    entry = next(e for e in RULE_ENTRIES if e['key'] == 'worker-stalled')
    rule_input = RuleInput(
        state={'health': {'state': 'stalled', 'heartbeat_age_ms': 3000}},
        metrics={},
        settings=RuleSettings(warn_lag_ms=100, agent_token_set=True),
        now=0,
    )

    (finding,) = diagnose_answer(run_framework_rules(rule_input, rule_entries=RULE_ENTRIES, baselines=Baselines()))[
        'findings'
    ]

    assert list(finding) == ['rule', 'severity', 'title', 'evidence', 'explain', 'suggestion']
    assert finding == {
        'rule': 'worker-stalled',
        'severity': entry['severity'],
        'title': entry['title'],
        'evidence': {'state': 'stalled', 'heartbeat_age_ms': 3000},
        'explain': entry['explain'],
        'suggestion': entry['suggestion'],
    }


def test_a_raising_rule_is_listed_in_failed_and_the_others_still_run(monkeypatch):
    def broken(rule_input: RuleInput, baselines: Baselines) -> dict[str, Any] | None:
        raise ZeroDivisionError('division by zero')

    monkeypatch.setitem(FRAMEWORK_RULES, 'budget-saturated', broken)
    rule_input = RuleInput(
        state={'health': {'state': 'stalled', 'heartbeat_age_ms': 3000}},
        metrics={},
        settings=RuleSettings(warn_lag_ms=100, agent_token_set=True),
        now=0,
    )

    answer = diagnose_answer(run_framework_rules(rule_input, rule_entries=RULE_ENTRIES, baselines=Baselines()))

    assert answer['failed'] == [{'rule': 'budget-saturated', 'error': 'division by zero'}]
    assert answer['checked'] == RULE_KEYS
    assert [f['rule'] for f in answer['findings']] == ['worker-stalled']


def test_an_error_with_no_text_is_named_by_its_type(monkeypatch):
    def broken(rule_input: RuleInput, baselines: Baselines) -> dict[str, Any] | None:
        raise KeyError

    monkeypatch.setitem(FRAMEWORK_RULES, 'pool-saturated', broken)
    rule_input = RuleInput(state={}, metrics={}, settings=RuleSettings(warn_lag_ms=100, agent_token_set=True), now=0)

    answer = diagnose_answer(run_framework_rules(rule_input, rule_entries=RULE_ENTRIES, baselines=Baselines()))

    assert answer['failed'] == [{'rule': 'pool-saturated', 'error': 'KeyError'}]


def test_a_registry_rule_with_no_implementation_is_listed_in_failed():
    entries = (*RULE_ENTRIES, {**RULE_ENTRIES[0], 'key': 'rule-with-no-code'})
    rule_input = RuleInput(state={}, metrics={}, settings=RuleSettings(warn_lag_ms=100, agent_token_set=True), now=0)

    answer = diagnose_answer(run_framework_rules(rule_input, rule_entries=entries, baselines=Baselines()))

    assert answer['checked'][-1] == 'rule-with-no-code'
    assert answer['failed'] == [{'rule': 'rule-with-no-code', 'error': 'the rule has no implementation'}]


def test_results_merge_and_the_answer_sorts_findings_by_severity_then_rule():
    first = DiagnoseResult(
        findings=[{'rule': 'b', 'severity': 'info'}, {'rule': 'z', 'severity': 'critical'}],
        checked=['b', 'z'],
        failed=[],
    )
    second = DiagnoseResult(
        findings=[{'rule': 'app:a', 'severity': 'warning'}, {'rule': 'a', 'severity': 'info'}],
        checked=['app:a', 'app:slow'],
        failed=[{'rule': 'app:slow', 'error': 'timed out after 1 s'}],
    )

    answer = diagnose_answer(first.merged(second))

    assert [f['rule'] for f in answer['findings']] == ['z', 'app:a', 'a', 'b']
    assert answer['checked'] == ['b', 'z', 'app:a', 'app:slow']
    assert answer['failed'] == [{'rule': 'app:slow', 'error': 'timed out after 1 s'}]
    assert list(answer) == ['findings', 'checked', 'failed']


def test_severity_order_follows_the_contract():
    assert SEVERITY_ORDER == ('critical', 'warning', 'info')


def test_every_registry_rule_severity_is_known():
    assert {entry['severity'] for entry in RULE_ENTRIES} <= set(SEVERITY_ORDER)


def test_the_lag_baseline_ignores_a_growing_lag_within_60_seconds():
    baselines = Baselines()
    settings = RuleSettings(warn_lag_ms=100, agent_token_set=True)

    def lag_finding(lag: int, now: float) -> list[str]:
        rule_input = RuleInput(state={'partitions': [{'lag': lag}]}, metrics={}, settings=settings, now=now)
        answer = diagnose_answer(run_framework_rules(rule_input, rule_entries=RULE_ENTRIES, baselines=baselines))
        return [f['rule'] for f in answer['findings']]

    assert lag_finding(1, now=0) == []
    assert lag_finding(1000, now=59.9) == []
    assert lag_finding(2, now=60) == ['lag-growing']


@pytest.mark.parametrize(
    ('left', 'timeout', 'fires'),
    [(8639, 86400, True), (8640, 86400, False), (299, 600, True), (300, 600, False), (-1, 600, True)],
)
def test_split_job_near_timeout_threshold_is_the_larger_of_ten_percent_and_300_seconds(left, timeout, fires):
    from drakkar.agent.rules import split_job_near_timeout

    state = {'split': {'nearest_timeout_seconds_left': left, 'nearest_timeout_seconds': timeout}}
    rule_input = RuleInput(state=state, metrics={}, settings=RuleSettings(100, True), now=0)

    found = split_job_near_timeout(rule_input, Baselines())

    assert (found is not None) is fires


def test_split_job_near_timeout_is_silent_with_no_timeout():
    from drakkar.agent.rules import split_job_near_timeout

    state = {'split': {'nearest_timeout_seconds_left': None, 'nearest_timeout_seconds': None}}

    assert split_job_near_timeout(RuleInput(state, {}, RuleSettings(100, True), 0), Baselines()) is None


@pytest.mark.parametrize(('seconds', 'fires'), [(120, False), (120.5, True)])
def test_split_partition_restoring_fires_only_over_120_seconds(seconds, fires):
    from drakkar.agent.rules import split_partition_restoring

    state = {'split': {'restoring_partitions': [{'partition': 4, 'seconds': seconds}]}}

    found = split_partition_restoring(RuleInput(state, {}, RuleSettings(100, True), 0), Baselines())

    assert found == ({'partitions': [{'partition': 4, 'seconds': seconds}]} if fires else None)
