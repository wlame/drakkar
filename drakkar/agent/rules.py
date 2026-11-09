"""The diagnose rules of the agent routes.

A rule reads one :class:`RuleInput` (the parts of ``/api/v1/agent/state``,
metric values by exposed name, and two settings) and returns the evidence of a
finding, or ``None``. The condition of each rule is code here; its title,
severity, explanation and suggestion are the entry of ``rules.yaml`` with the
same key. ``tests/fixtures/agent_contract.json`` (``diagnose_cases``) pins the
conditions and the evidence.

A part, a field or a metric that the input does not hold makes its rule find
nothing; a missing metric reads as 0. Evidence holds counts, names, partition
numbers and settings, never payload text.

Three rules compare with the previous call and keep a baseline per worker
process (:class:`Baselines`): ``lag-growing`` keeps its baseline until a call
comes at least 60 seconds after it, ``recorder-dropping`` and ``dlq-writing``
replace theirs on every call. The first call only sets them.

Handler rules run after these, through the route; :meth:`DiagnoseResult.merged`
joins their result to the result of the framework rules.
"""

from __future__ import annotations

import threading
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from drakkar.agent.metrics_view import json_number

SEVERITY_ORDER = ('critical', 'warning', 'info')
LAG_BASELINE_MIN_SECONDS = 60.0
NO_IMPLEMENTATION_ERROR = 'the rule has no implementation'

DROPPED_EVENTS_METRIC = 'drakkar_recorder_dropped_events_total'
DROPPED_POSTGRES_ROWS_METRIC = 'drakkar_recorder_postgres_dropped_rows_total'
DLQ_MESSAGES_METRIC = 'drakkar_sink_dlq_messages_total'
# The metrics the rules read; the route collects exactly these.
RULE_METRICS = (DROPPED_EVENTS_METRIC, DROPPED_POSTGRES_ROWS_METRIC, DLQ_MESSAGES_METRIC)

STALLED_STATE = 'stalled'
DEBUG_PAUSE = 'debug'


@dataclass(frozen=True)
class RuleSettings:
    """The settings the rules read: ``runtime_health.warn_lag`` in ms, and whether ``agent.auth_token`` is set."""

    warn_lag_ms: float
    agent_token_set: bool


@dataclass(frozen=True)
class RuleInput:
    """One diagnose call: the state parts, the metric values by exposed name, the settings and the monotonic time."""

    state: Mapping[str, Any]
    metrics: Mapping[str, float]
    settings: RuleSettings
    now: float


@dataclass
class Baselines:
    """The values of the previous diagnose calls that three rules compare with, one set per worker process.

    ``lock`` serializes the calls that read and replace them.
    """

    lock: threading.Lock = field(default_factory=threading.Lock)
    lag: tuple[float, float] | None = None
    counters: dict[str, tuple[float, ...]] = field(default_factory=dict)


@dataclass(frozen=True)
class DiagnoseResult:
    """The findings, the rules that ran and the rules that failed, before the answer sorts the findings."""

    findings: list[dict[str, Any]]
    checked: list[str]
    failed: list[dict[str, str]]

    def merged(self, other: DiagnoseResult) -> DiagnoseResult:
        """This result followed by ``other``: the handler's rules come after the framework's."""
        return DiagnoseResult(
            findings=[*self.findings, *other.findings],
            checked=[*self.checked, *other.checked],
            failed=[*self.failed, *other.failed],
        )


Rule = Callable[[RuleInput, Baselines], dict[str, Any] | None]


def _part(rule_input: RuleInput, name: str) -> Mapping[str, Any]:
    part = rule_input.state.get(name)
    return part if isinstance(part, Mapping) else {}


def _rows(rule_input: RuleInput, name: str) -> list[Mapping[str, Any]]:
    rows = rule_input.state.get(name)
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, Mapping)]


def _numbers(part: Mapping[str, Any], *names: str) -> list[float] | None:
    """The values of ``names`` in ``part`` when every one is a number, else ``None``."""
    numbers: list[float] = []
    for name in names:
        value = part.get(name)
        if isinstance(value, bool) or not isinstance(value, int | float):
            return None
        numbers.append(value)
    return numbers


def _metric(rule_input: RuleInput, name: str) -> float:
    value = rule_input.metrics.get(name, 0)
    return value if isinstance(value, int | float) else 0


# --- the rules -----------------------------------------------------------


def worker_stalled(rule_input: RuleInput, baselines: Baselines) -> dict[str, Any] | None:
    """``health.state`` is ``stalled``."""
    health = _part(rule_input, 'health')
    if health.get('state') != STALLED_STATE:
        return None
    return {'state': STALLED_STATE, 'heartbeat_age_ms': health.get('heartbeat_age_ms')}


def loop_lag_high(rule_input: RuleInput, baselines: Baselines) -> dict[str, Any] | None:
    """The window peak lag reached ``runtime_health.warn_lag`` and the loop is not stalled."""
    health = _part(rule_input, 'health')
    numbers = _numbers(health, 'window_peak_lag_ms')
    if numbers is None or health.get('state') == STALLED_STATE:
        return None
    (peak,) = numbers
    warn = rule_input.settings.warn_lag_ms
    if peak < warn:
        return None
    return {'window_peak_lag_ms': json_number(peak), 'warn_lag_ms': json_number(warn)}


def budget_saturated(rule_input: RuleInput, baselines: Baselines) -> dict[str, Any] | None:
    """The pending tasks or the pending bytes reached their limit."""
    budget = _part(rule_input, 'budget')
    numbers = _numbers(budget, 'tasks', 'max_tasks', 'bytes', 'max_bytes')
    if numbers is None:
        return None
    tasks, max_tasks, held, max_bytes = numbers
    if tasks < max_tasks and held < max_bytes:
        return None
    return {'tasks': tasks, 'max_tasks': max_tasks, 'bytes': held, 'max_bytes': max_bytes}


def pool_saturated(rule_input: RuleInput, baselines: Baselines) -> dict[str, Any] | None:
    """Every executor slot is busy and tasks wait for a slot."""
    budget = _part(rule_input, 'budget')
    numbers = _numbers(budget, 'pool_active', 'pool_max', 'pool_waiting')
    if numbers is None:
        return None
    active, maximum, waiting = numbers
    if active != maximum or waiting <= 0:
        return None
    return {'pool_active': active, 'pool_max': maximum, 'pool_waiting': waiting}


def partition_dead(rule_input: RuleInput, baselines: Baselines) -> dict[str, Any] | None:
    """A partition loop is dead."""
    dead = sorted(row['partition'] for row in _rows(rule_input, 'partitions') if row.get('dead') is True)
    return {'partitions': dead} if dead else None


def partition_paused_by_debug(rule_input: RuleInput, baselines: Baselines) -> dict[str, Any] | None:
    """An operator paused consuming."""
    paused = sorted(
        row['partition']
        for row in _rows(rule_input, 'partitions')
        if isinstance(row.get('paused_by'), list) and DEBUG_PAUSE in row['paused_by']
    )
    return {'partitions': paused} if paused else None


def sink_breaker_open(rule_input: RuleInput, baselines: Baselines) -> dict[str, Any] | None:
    """The circuit breaker of a sink is open."""
    sinks = sorted(
        f'{row["type"]}/{row["name"]}' for row in _rows(rule_input, 'sinks') if row.get('breaker_open') is True
    )
    return {'sinks': sinks} if sinks else None


def _lag_sum(rule_input: RuleInput) -> int | None:
    """The sum of the known partition lags; ``None`` when no partition has a known lag."""
    lags = [
        row['lag']
        for row in _rows(rule_input, 'partitions')
        if isinstance(row.get('lag'), int) and not isinstance(row.get('lag'), bool)
    ]
    return sum(lags) if lags else None


def lag_growing(rule_input: RuleInput, baselines: Baselines) -> dict[str, Any] | None:
    """The lag sum is above 0 and above the baseline of a call at least 60 seconds earlier.

    A call within 60 seconds of the baseline compares nothing and keeps it; an
    unknown lag sum changes nothing.
    """
    lag = _lag_sum(rule_input)
    if lag is None:
        return None
    now = rule_input.now
    previous = baselines.lag
    if previous is None:
        baselines.lag = (lag, now)
        return None
    previous_lag, previous_at = previous
    elapsed = now - previous_at
    if elapsed < LAG_BASELINE_MIN_SECONDS:
        return None
    baselines.lag = (lag, now)
    if lag <= 0 or lag <= previous_lag:
        return None
    return {
        'lag': lag,
        'previous_lag': json_number(previous_lag),
        'seconds_since_previous': json_number(round(elapsed, 3)),
    }


def _grown(rule_input: RuleInput, baselines: Baselines, key: str, names: Sequence[str]) -> list[float] | None:
    """How much each metric of ``names`` grew since the previous call, or ``None`` on the first call.

    The baseline of ``key`` is replaced on every call.
    """
    values = tuple(_metric(rule_input, name) for name in names)
    previous = baselines.counters.get(key)
    baselines.counters[key] = values
    if previous is None:
        return None
    return [max(0, value - before) for value, before in zip(values, previous, strict=True)]


def recorder_dropping(rule_input: RuleInput, baselines: Baselines) -> dict[str, Any] | None:
    """The recorder dropped events or Postgres rows since the previous call."""
    grown = _grown(rule_input, baselines, 'recorder-dropping', (DROPPED_EVENTS_METRIC, DROPPED_POSTGRES_ROWS_METRIC))
    if grown is None or not any(grown):
        return None
    events, rows = grown
    return {'dropped_events': json_number(events), 'dropped_postgres_rows': json_number(rows)}


def dlq_writing(rule_input: RuleInput, baselines: Baselines) -> dict[str, Any] | None:
    """The worker wrote DLQ records since the previous call."""
    grown = _grown(rule_input, baselines, 'dlq-writing', (DLQ_MESSAGES_METRIC,))
    if grown is None or not any(grown):
        return None
    (records,) = grown
    return {'dlq_records': json_number(records)}


def peers_offline(rule_input: RuleInput, baselines: Baselines) -> dict[str, Any] | None:
    """Some peers count as offline."""
    autodiscover = _part(rule_input, 'autodiscover')
    numbers = _numbers(autodiscover, 'peers_offline')
    if numbers is None or numbers[0] <= 0:
        return None
    return {'peers_offline': numbers[0], 'peers_online': autodiscover.get('peers_online')}


def cluster_tasks_mismatch(rule_input: RuleInput, baselines: Baselines) -> dict[str, Any] | None:
    """Online peers of this cluster declare other cluster tasks."""
    if _part(rule_input, 'autodiscover').get('cluster_tasks_match') is not False:
        return None
    return {'cluster_tasks_match': False}


def agent_unauthenticated(rule_input: RuleInput, baselines: Baselines) -> dict[str, Any] | None:
    """``agent.auth_token`` is empty."""
    if rule_input.settings.agent_token_set:
        return None
    return {'auth_token_set': False}


# The implementations by rule key, in the order of rules.yaml; a test keeps
# the two lists equal.
FRAMEWORK_RULES: dict[str, Rule] = {
    'worker-stalled': worker_stalled,
    'loop-lag-high': loop_lag_high,
    'budget-saturated': budget_saturated,
    'pool-saturated': pool_saturated,
    'partition-dead': partition_dead,
    'partition-paused-by-debug': partition_paused_by_debug,
    'sink-breaker-open': sink_breaker_open,
    'lag-growing': lag_growing,
    'recorder-dropping': recorder_dropping,
    'dlq-writing': dlq_writing,
    'peers-offline': peers_offline,
    'cluster-tasks-mismatch': cluster_tasks_mismatch,
    'agent-unauthenticated': agent_unauthenticated,
}


def finding_of(entry: Mapping[str, Any], evidence: dict[str, Any], *, rule: str | None = None) -> dict[str, Any]:
    """A finding in the contract field order, its text from the rule's registry entry."""
    return {
        'rule': rule if rule is not None else entry['key'],
        'severity': entry['severity'],
        'title': entry['title'],
        'evidence': evidence,
        'explain': entry['explain'],
        'suggestion': entry['suggestion'],
    }


def error_text(exc: BaseException) -> str:
    """The text of a failed rule: the exception message, or its type name when the message is empty."""
    return str(exc) or type(exc).__name__


def run_framework_rules(
    rule_input: RuleInput,
    *,
    rule_entries: Sequence[Mapping[str, Any]],
    baselines: Baselines,
) -> DiagnoseResult:
    """Run every rule of ``rule_entries`` (the registry order) on one input.

    A rule that raises, or a registry rule with no implementation, is listed in
    ``failed`` and the others still run. Calls are serialized on the baselines.
    """
    findings: list[dict[str, Any]] = []
    checked: list[str] = []
    failed: list[dict[str, str]] = []
    with baselines.lock:
        for entry in rule_entries:
            key = entry['key']
            checked.append(key)
            rule = FRAMEWORK_RULES.get(key)
            if rule is None:
                failed.append({'rule': key, 'error': NO_IMPLEMENTATION_ERROR})
                continue
            try:
                evidence = rule(rule_input, baselines)
            except Exception as exc:
                failed.append({'rule': key, 'error': error_text(exc)})
                continue
            if evidence is not None:
                findings.append(finding_of(entry, evidence))
    return DiagnoseResult(findings=findings, checked=checked, failed=failed)


def diagnose_answer(result: DiagnoseResult) -> dict[str, Any]:
    """The diagnose answer: findings by severity (critical, warning, info) then rule; ``checked`` and ``failed`` as run."""
    rank = {severity: number for number, severity in enumerate(SEVERITY_ORDER)}
    findings = sorted(
        result.findings,
        key=lambda finding: (rank.get(finding['severity'], len(rank)), finding['rule'].encode('utf-8')),
    )
    return {'findings': findings, 'checked': list(result.checked), 'failed': list(result.failed)}
