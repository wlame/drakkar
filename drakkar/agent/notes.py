"""The handler's own agent notes: business concepts, recipes, metric notes, ``app`` key meanings and rules.

A handler returns an :class:`AgentNotes` from ``agent_notes()``. The worker calls
the hook once after ``on_startup`` and checks the notes with
:func:`prepare_notes`: every reference must name a framework entry, a config
path or another note (``app:<kind>:<key>``), a key is used once per kind, and a
metric note must name a metric the handler declares. A note that breaks a rule
is dropped and its reasons are logged; the others stay.

The notes reach the agent routes as :class:`PreparedNotes`: the entries of the
manual part ``app``, the meanings of the ``app`` config keys, the notes of the
handler metrics and the rules. :class:`HandlerRules` runs the rules for the
diagnose route. Each rule runs on a thread of its own with a limit of
:data:`RULE_TIMEOUT_SECONDS`; the route stops waiting for a rule that passes it
and lists it as failed. A call that finds a run of a rule still within its
limit waits for that run; a run past its limit is not started again and fails
at once, so a rule that never returns holds one thread, not one per call.
"""

from __future__ import annotations

import asyncio
import math
import threading
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import TYPE_CHECKING, Annotated, Any, Literal

import structlog
from pydantic import BaseModel, ConfigDict, Field, SkipValidation

from drakkar.agent.metrics_view import MetricNotes, exposed_name
from drakkar.agent.params import RULE_TIMED_OUT_MESSAGE
from drakkar.agent.registry import APP_REFERENCE_PREFIX, Registry, dumps
from drakkar.agent.rules import DiagnoseResult, RuleInput, error_text, finding_of

if TYPE_CHECKING:
    from prometheus_client.metrics import MetricWrapperBase

logger = structlog.get_logger()

RULE_TIMEOUT_SECONDS = 1.0
# The largest evidence a handler rule may give, as compact UTF-8 JSON.
EVIDENCE_MAX_BYTES = 4096
EVIDENCE_TOO_LARGE_MESSAGE = 'the evidence is larger than 4096 bytes'
APP_CONFIG_PREFIX = 'app.'
AGENT_NOTES_ASYNC_MESSAGE = 'agent_notes must be a plain method, not async def'
SUMMARY_MAX_LENGTH = 200
SUMMARY_CUT_MARK = '...'

# The same limits as the entries of the framework registry.
NoteKey = Annotated[str, Field(pattern=r'^[a-z0-9][a-z0-9_-]*$', max_length=64)]
Summary = Annotated[str, Field(min_length=1, max_length=SUMMARY_MAX_LENGTH, pattern=r'^[^\n]+$')]
Line = Annotated[str, Field(min_length=1, max_length=2000, pattern=r'^[^\n]+$')]
Body = Annotated[str, Field(min_length=1, max_length=2000, pattern=r'[^ \n\t]$')]
Reference = Annotated[str, Field(min_length=3, max_length=300, pattern=r'^[^ \n][^\n]*$')]
Severity = Literal['info', 'warning', 'critical']

# The order of the note kinds in the manual part ``app``: the order of the framework parts.
APP_KIND_ORDER = ('concept', 'metric', 'recipe', 'rule')


class _NoteModel(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True)


class AgentTopic(_NoteModel):
    """A business concept: what it is and how it relates to the worker. Its reference is ``app:concept:<key>``."""

    key: NoteKey
    summary: Summary
    body: Body | None = None
    related: list[Reference] = Field(default_factory=list)


class AgentRecipeStep(_NoteModel):
    """One step of a recipe: what to read and why."""

    do: Line
    why: Line


class AgentRecipeCause(_NoteModel):
    """One likely cause of a recipe's question: how to check it and how to fix it."""

    cause: Line
    check: Line
    fix: Line


class AgentRecipe(_NoteModel):
    """An answer to a question an operator asks about the business. Its reference is ``app:recipe:<key>``."""

    key: NoteKey
    summary: Summary
    question: Line
    steps: list[AgentRecipeStep] = Field(min_length=1)
    causes: list[AgentRecipeCause] = Field(default_factory=list)
    related: list[Reference] = Field(default_factory=list)


class AgentPromql(_NoteModel):
    """A PromQL query with a title; ``{{worker}}`` and ``{{cluster}}`` are filled when it is served."""

    title: Line
    query: Line


class AgentMetricNote(_NoteModel):
    """What a value of one of the handler's metrics tells an operator."""

    meaning: Line
    healthy: Line | None = None
    promql: list[AgentPromql] = Field(default_factory=list)


class AgentFinding(_NoteModel):
    """What a handler rule found: evidence as numbers, names and ids (JSON values), never payload text."""

    evidence: dict[str, Any] = Field(default_factory=dict)


class AgentSnapshot(BaseModel):
    """The read-only input of a handler rule: the parts of ``/api/v1/agent/state`` and metric values.

    ``state`` holds every state part by name (a part that could not be read is
    ``None``); maps are read-only and lists are tuples. ``metrics`` holds the
    metric values the framework rules read, by exposed name. One snapshot is
    shared by every rule of a diagnose call.
    """

    model_config = ConfigDict(frozen=True)

    state: SkipValidation[Mapping[str, Any]]
    metrics: SkipValidation[Mapping[str, float]]

    def part(self, name: str) -> Any:
        """The state part ``name``, or ``None`` when it is absent or was not read."""
        return self.state.get(name)


RuleCheck = Callable[[AgentSnapshot], AgentFinding | None]


class AgentRule(_NoteModel):
    """A business diagnose rule; ``check`` returns a finding or ``None``.

    ``check`` runs on a thread of its own, with a limit of 1 second; it must be a
    fast, pure function of the snapshot. In the diagnose answer the rule is named
    ``app:<key>``; its ``explain`` is the reference given, else its own manual
    entry ``app:rule:<key>``.
    """

    key: NoteKey
    severity: Severity
    title: Line
    suggestion: Line
    check: RuleCheck
    summary: Summary | None = None
    explain: Reference | None = None


class AgentNotes(_NoteModel):
    """The handler's notes for the agent routes; every field may be left empty.

    ``metrics`` is keyed by the name of a metric the handler declares (the name
    it is exposed with, or its family name). ``app_config`` is keyed by a path
    below the ``app`` section (``billing.api_url``; ``app.billing.api_url`` is
    the same path).
    """

    concepts: list[AgentTopic] = Field(default_factory=list)
    recipes: list[AgentRecipe] = Field(default_factory=list)
    metrics: dict[str, AgentMetricNote] = Field(default_factory=dict)
    app_config: dict[str, Line] = Field(default_factory=dict)
    rules: list[AgentRule] = Field(default_factory=list)


@dataclass(frozen=True)
class HandlerMetric:
    """One metric the handler declares: the name it is exposed with and its help text."""

    exposed: str
    help: str


@dataclass(frozen=True)
class PreparedNotes:
    """The checked notes, in the shapes the agent routes read."""

    entries: tuple[dict[str, Any], ...] = ()
    config_meanings: Mapping[str, str] = field(default_factory=dict)
    metric_notes: MetricNotes = field(default_factory=MetricNotes)
    rules: tuple[AgentRule, ...] = ()


def handler_metrics_from(rows: Iterable[tuple[str, str, str]]) -> dict[str, HandlerMetric]:
    """The handler's metrics by every name a note may use, from ``(family name, type, help)`` rows.

    A metric answers to the name it is exposed with and to its family name.
    """
    found: dict[str, HandlerMetric] = {}
    for family, metric_type, help_text in rows:
        exposed = exposed_name(family, metric_type)
        described = HandlerMetric(exposed=exposed, help=help_text)
        found[exposed] = described
        found.setdefault(family, described)
    return found


def handler_metrics_of(metrics: Iterable[MetricWrapperBase]) -> dict[str, HandlerMetric]:
    """The handler's metrics by every name a note may use, from the metric objects it declares."""
    return handler_metrics_from((metric._name, metric._type or '', metric._documentation) for metric in metrics)


def _cut_summary(text: str) -> str:
    line = text.strip().splitlines()[0] if text.strip() else ''
    if len(line) <= SUMMARY_MAX_LENGTH:
        return line
    return line[: SUMMARY_MAX_LENGTH - len(SUMMARY_CUT_MARK)] + SUMMARY_CUT_MARK


def app_config_path(path: str) -> str:
    """The full config path of an ``app`` key path: ``billing.url`` and ``app.billing.url`` give ``app.billing.url``."""
    return path if path.startswith(APP_CONFIG_PREFIX) else APP_CONFIG_PREFIX + path


@dataclass
class _Candidate:
    """One note on its way to the manual: its reference, entry, references and the source note."""

    kind: str
    ref: str
    entry: dict[str, Any]
    references: list[str]
    note: Any


def _concept_entry(topic: AgentTopic) -> dict[str, Any]:
    entry: dict[str, Any] = {'key': f'{APP_REFERENCE_PREFIX}concept:{topic.key}', 'summary': topic.summary}
    if topic.body is not None:
        entry['body'] = topic.body
    if topic.related:
        entry['related'] = list(topic.related)
    return entry


def _recipe_entry(recipe: AgentRecipe) -> dict[str, Any]:
    entry: dict[str, Any] = {
        'key': f'{APP_REFERENCE_PREFIX}recipe:{recipe.key}',
        'summary': recipe.summary,
        'question': recipe.question,
        'steps': [step.model_dump() for step in recipe.steps],
    }
    if recipe.causes:
        entry['causes'] = [cause.model_dump() for cause in recipe.causes]
    if recipe.related:
        entry['related'] = list(recipe.related)
    return entry


def _metric_entry(metric: HandlerMetric, note: AgentMetricNote) -> dict[str, Any]:
    entry: dict[str, Any] = {
        'key': f'{APP_REFERENCE_PREFIX}metric:{metric.exposed}',
        'summary': _cut_summary(metric.help) or _cut_summary(note.meaning),
        'meaning': note.meaning,
    }
    if note.healthy is not None:
        entry['healthy'] = note.healthy
    if note.promql:
        entry['promql'] = [row.model_dump() for row in note.promql]
    return entry


def _rule_entry(rule: AgentRule) -> dict[str, Any]:
    entry: dict[str, Any] = {
        'key': f'{APP_REFERENCE_PREFIX}rule:{rule.key}',
        'summary': rule.summary if rule.summary is not None else _cut_summary(rule.title),
        'severity': rule.severity,
        'title': rule.title,
    }
    if rule.explain is not None:
        entry['explain'] = rule.explain
    entry['suggestion'] = rule.suggestion
    return entry


def _candidates(
    notes: AgentNotes,
    handler_metrics: Mapping[str, HandlerMetric],
    reasons: list[str],
) -> list[_Candidate]:
    """Every note as a manual entry; a repeated key or an unknown metric is a reason and no entry."""
    candidates: list[_Candidate] = []
    seen: set[str] = set()

    def add(kind: str, key: str, entry: dict[str, Any], references: list[str], note: Any) -> None:
        ref = entry['key']
        if ref in seen:
            reasons.append(f"{kind} '{key}': the key is used twice")
            return
        seen.add(ref)
        candidates.append(_Candidate(kind=kind, ref=ref, entry=entry, references=references, note=note))

    for topic in notes.concepts:
        add('concept', topic.key, _concept_entry(topic), list(topic.related), topic)
    for name, metric_note in notes.metrics.items():
        metric = handler_metrics.get(name)
        if metric is None:
            reasons.append(f"metric '{name}': the handler declares no such metric")
            continue
        add('metric', metric.exposed, _metric_entry(metric, metric_note), [], (metric, metric_note))
    for recipe in notes.recipes:
        add('recipe', recipe.key, _recipe_entry(recipe), list(recipe.related), recipe)
    for rule in notes.rules:
        references = [rule.explain] if rule.explain is not None else []
        add('rule', rule.key, _rule_entry(rule), references, rule)
    return candidates


def _framework_reference_resolves(registry: Registry, ref: str) -> bool:
    if registry.resolves(ref):
        return True
    kind, _, key = ref.partition(':')
    return kind == 'config' and registry.config_metadata_entry(key) is not None


def _keep_resolving(candidates: list[_Candidate], registry: Registry, reasons: list[str]) -> list[_Candidate]:
    """Drop every note with a reference that names nothing, until the kept notes name only kept notes."""
    kept = list(candidates)
    while True:
        own = {candidate.ref for candidate in kept}
        broken: list[_Candidate] = []
        for candidate in kept:
            bad = [
                ref
                for ref in candidate.references
                if not (
                    ref in own if ref.startswith(APP_REFERENCE_PREFIX) else _framework_reference_resolves(registry, ref)
                )
            ]
            if bad:
                broken.append(candidate)
                key = candidate.ref.split(':', 2)[2]
                reasons.extend(f"{candidate.kind} '{key}': the reference '{ref}' names nothing" for ref in bad)
        if not broken:
            return kept
        kept = [candidate for candidate in kept if candidate not in broken]


def prepare_notes(
    notes: AgentNotes,
    *,
    registry: Registry,
    handler_metrics: Mapping[str, HandlerMetric],
) -> tuple[PreparedNotes, list[str]]:
    """Check the notes; return the notes that pass and the reasons of those that do not."""
    reasons: list[str] = []
    kept = _keep_resolving(_candidates(notes, handler_metrics, reasons), registry, reasons)
    rank = {kind: number for number, kind in enumerate(APP_KIND_ORDER)}
    ordered = sorted(kept, key=lambda candidate: rank[candidate.kind])

    meaning: dict[str, str] = {}
    healthy: dict[str, str] = {}
    promql: dict[str, tuple[dict[str, str], ...]] = {}
    for candidate in ordered:
        if candidate.kind != 'metric':
            continue
        metric, metric_note = candidate.note
        meaning[metric.exposed] = metric_note.meaning
        if metric_note.healthy is not None:
            healthy[metric.exposed] = metric_note.healthy
        if metric_note.promql:
            promql[metric.exposed] = tuple(row.model_dump() for row in metric_note.promql)

    prepared = PreparedNotes(
        entries=tuple(candidate.entry for candidate in ordered),
        config_meanings={app_config_path(path): text for path, text in notes.app_config.items()},
        metric_notes=MetricNotes(meaning=meaning, healthy=healthy, promql=promql),
        rules=tuple(candidate.note for candidate in ordered if candidate.kind == 'rule'),
    )
    return prepared, reasons


# --- handler rules -----------------------------------------------------------


def freeze(value: Any) -> Any:
    """A read-only copy: maps become read-only maps and lists become tuples, at any depth."""
    if isinstance(value, Mapping):
        return MappingProxyType({key: freeze(item) for key, item in value.items()})
    if isinstance(value, list | tuple):
        return tuple(freeze(item) for item in value)
    return value


# The payload fields of a failure row; a handler rule never sees them.
PAYLOAD_FIELDS = frozenset({'stderr_tail', 'args', 'args_truncated'})


def _without_payloads(state: Mapping[str, Any]) -> dict[str, Any]:
    """The state with no payload field in the rows of ``recent_failures``."""
    failures = state.get('recent_failures')
    if not isinstance(failures, Mapping) or not isinstance(failures.get('failures'), list):
        return dict(state)
    rows = [
        {name: value for name, value in row.items() if name not in PAYLOAD_FIELDS} if isinstance(row, Mapping) else row
        for row in failures['failures']
    ]
    return {**state, 'recent_failures': {**failures, 'failures': rows}}


def snapshot_of(rule_input: RuleInput) -> AgentSnapshot:
    """The frozen snapshot a handler rule reads, from the input of the framework rules, with no payload text."""
    return AgentSnapshot(
        state=freeze(_without_payloads(rule_input.state)),
        metrics=MappingProxyType(dict(rule_input.metrics)),
    )


def _finite_json(value: Any) -> bool:
    """Whether a value is plain JSON with finite numbers only."""
    if isinstance(value, float):
        return math.isfinite(value)
    if isinstance(value, Mapping):
        return all(isinstance(key, str) and _finite_json(item) for key, item in value.items())
    if isinstance(value, list | tuple):
        return all(_finite_json(item) for item in value)
    return value is None or isinstance(value, str | int | bool)


def evidence_of(outcome: Any) -> dict[str, Any] | None:
    """The evidence of a rule's return value.

    Raises ``TypeError`` for a value that is not a finding or ``None`` and for
    evidence that is not plain JSON with finite numbers, and ``ValueError`` for
    evidence whose compact JSON is larger than :data:`EVIDENCE_MAX_BYTES`.
    """
    if outcome is None:
        return None
    if not isinstance(outcome, AgentFinding):
        raise TypeError(f'the rule returned {type(outcome).__name__}, not an AgentFinding or None')
    evidence = outcome.evidence
    if not _finite_json(evidence):
        raise TypeError('the evidence is not plain JSON with finite numbers')
    if len(dumps(evidence).encode('utf-8')) > EVIDENCE_MAX_BYTES:
        raise ValueError(EVIDENCE_TOO_LARGE_MESSAGE)
    return evidence


class _RuleRun:
    """One run of a handler rule on a daemon thread.

    ``done`` is set when the rule returned or raised. ``deadline`` (monotonic
    seconds) is the end of the run's limit; a run still going then is
    abandoned, once. Several diagnose calls may wait on one run.
    """

    def __init__(self, rule: AgentRule, snapshot: AgentSnapshot, *, timeout: float) -> None:
        self.done = threading.Event()
        self.outcome: Any = None
        self.error: BaseException | None = None
        self.deadline = time.monotonic() + timeout
        self.abandoned = False
        self._rule = rule
        self._snapshot = snapshot
        self._waiters: list[tuple[asyncio.AbstractEventLoop, asyncio.Future[None]]] = []
        self._lock = threading.Lock()
        # A daemon thread: a rule that never returns cannot hold the process at exit.
        self._thread = threading.Thread(
            target=self._run,
            name=f'drakkar-agent-rule-{rule.key}',
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()

    def _run(self) -> None:
        try:
            self.outcome = self._rule.check(self._snapshot)
        except BaseException as exc:
            self.error = exc
        with self._lock:
            self.done.set()
            waiters, self._waiters = self._waiters, []
        for loop, future in waiters:
            try:
                loop.call_soon_threadsafe(_resolve, future)
            except RuntimeError:
                # The loop that waited has closed (the worker stopped): nobody reads this outcome.
                pass

    def mark_abandoned(self) -> bool:
        """Mark the run abandoned; True only for the first mark, so the run is logged once."""
        with self._lock:
            if self.abandoned:
                return False
            self.abandoned = True
            return True

    async def wait(self) -> bool:
        """Wait on the calling loop until the run ends or its deadline passes; True when the rule ended."""
        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()
        waiter = (loop, future)
        with self._lock:
            if self.done.is_set():
                return True
            self._waiters.append(waiter)
        try:
            await asyncio.wait_for(future, max(0.0, self.deadline - time.monotonic()))
        except TimeoutError:
            with self._lock:
                if waiter in self._waiters:
                    self._waiters.remove(waiter)
            return self.done.is_set()
        return True


def _resolve(future: asyncio.Future[None]) -> None:
    if not future.done():
        future.set_result(None)


class HandlerRules:
    """Runs the handler's rules for the diagnose route, each bounded by :data:`RULE_TIMEOUT_SECONDS`.

    The rules of one call run at the same time, so a call waits at most the
    limit once. A call that finds a run of a rule still in progress within its
    limit (started by another call) waits for that run and takes its outcome.
    A run that passes its limit is abandoned: it is listed as failed with
    ``timed out after 1 s`` and logged once (``agent_rule_abandoned``); while
    it still runs, later calls list it as failed at once without starting it
    again, so a rule that never returns holds one thread.
    """

    def __init__(self, rules: Sequence[AgentRule], *, timeout: float = RULE_TIMEOUT_SECONDS) -> None:
        self._rules = tuple(rules)
        self._timeout = timeout
        self._running: dict[str, _RuleRun] = {}
        self._entries = {rule.key: _rule_finding_entry(rule) for rule in self._rules}

    @property
    def rules(self) -> tuple[AgentRule, ...]:
        """The rules, in the order the handler gave them."""
        return self._rules

    def _run_for(self, rule: AgentRule, snapshot: AgentSnapshot) -> _RuleRun:
        """The run this call reads: the one in progress, or a new one."""
        previous = self._running.get(rule.key)
        if previous is not None and not previous.done.is_set():
            return previous
        run = _RuleRun(rule, snapshot, timeout=self._timeout)
        run.start()
        self._running[rule.key] = run
        return run

    async def __call__(self, rule_input: RuleInput) -> DiagnoseResult:
        """Run every rule on one frozen snapshot of ``rule_input``."""
        snapshot = snapshot_of(rule_input)
        runs = {rule.key: self._run_for(rule, snapshot) for rule in self._rules}
        findings: list[dict[str, Any]] = []
        checked: list[str] = []
        failed: list[dict[str, str]] = []
        for rule in self._rules:
            name = f'{APP_REFERENCE_PREFIX}{rule.key}'
            checked.append(name)
            run = runs[rule.key]
            if run.abandoned or not await run.wait():
                failed.append({'rule': name, 'error': RULE_TIMED_OUT_MESSAGE})
                if run.mark_abandoned():
                    await logger.awarning('agent_rule_abandoned', category='agent', rule=name)
                continue
            if self._running.get(rule.key) is run:
                del self._running[rule.key]
            if run.error is not None:
                failed.append({'rule': name, 'error': error_text(run.error)})
                continue
            try:
                evidence = evidence_of(run.outcome)
            except Exception as exc:
                failed.append({'rule': name, 'error': error_text(exc)})
                continue
            if evidence is not None:
                findings.append(finding_of(self._entries[rule.key], evidence, rule=name))
        return DiagnoseResult(findings=findings, checked=checked, failed=failed)


def _rule_finding_entry(rule: AgentRule) -> dict[str, Any]:
    """The text of a handler rule's findings, in the shape of a registry rule entry."""
    return {
        'key': rule.key,
        'severity': rule.severity,
        'title': rule.title,
        'explain': rule.explain if rule.explain is not None else f'{APP_REFERENCE_PREFIX}rule:{rule.key}',
        'suggestion': rule.suggestion,
    }
