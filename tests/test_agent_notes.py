"""The handler's agent notes: the models, the checks, the routes that serve them and the handler rules."""

from __future__ import annotations

import asyncio
import gc
import json
import threading
import time
import warnings
from pathlib import Path
from types import SimpleNamespace
from typing import Any, ClassVar
from unittest.mock import AsyncMock, MagicMock

import pytest
from prometheus_client import Counter, Gauge
from pydantic import ValidationError
from structlog.testing import capture_logs

import drakkar as dk
from drakkar.agent.config_view import SECRET_KEY_WORDS
from drakkar.agent.metrics_view import MetricNotes
from drakkar.agent.notes import (
    AGENT_NOTES_ASYNC_MESSAGE,
    HandlerMetric,
    HandlerRules,
    PreparedNotes,
    app_config_path,
    evidence_of,
    freeze,
    handler_metrics_from,
    handler_metrics_of,
    prepare_notes,
    snapshot_of,
)
from drakkar.agent.params import RULE_TIMED_OUT_MESSAGE, STATE_PARTS
from drakkar.agent.registry import approx_tokens, dumps, load_registry, select_manual, selection_json
from drakkar.agent.rules import (
    ERROR_TEXT_MAX_BYTES,
    FRAMEWORK_RULES,
    Baselines,
    RuleInput,
    RuleSettings,
    cut_error_text,
    error_text,
    run_framework_rules,
)
from drakkar.app import DrakkarApp
from drakkar.recorder.helpers import _SECRET_ENV_PATTERNS, sanitize_env_value
from drakkar.uiserver.routes_agent import AgentSurface
from drakkar.uiserver.server import create_api_app
from drakkar.utils import SECRET_NAME_WORDS
from tests.test_agent_registry import _tiny_registry
from tests.test_agent_routes import _client, _config, _drakkar_app, _surface_client
from tests.test_sources_validation import KafkaOnlyHandler

REGISTRY = load_registry()
CONTRACT = json.loads((Path(__file__).parent / 'fixtures' / 'agent_contract.json').read_text(encoding='utf-8'))

# Handler metrics on their own names, so the default registry holds them once for the module.
ORDERS_DONE = Counter('agent_notes_test_orders', 'Orders this worker wrote.')
QUEUE_DEPTH = Gauge('agent_notes_test_queue_depth', 'Orders that wait for a slot.')


class NotesHandler(KafkaOnlyHandler):
    """A handler with two metrics and notes given by the test."""

    orders_done = ORDERS_DONE
    queue_depth = QUEUE_DEPTH
    notes: ClassVar[Any] = None

    def agent_notes(self) -> dk.AgentNotes | None:
        notes = type(self).notes
        if isinstance(notes, Exception):
            raise notes
        return notes


HANDLER_METRICS = handler_metrics_of([ORDERS_DONE, QUEUE_DEPTH])


def _finding(**evidence: Any) -> dk.AgentFinding:
    return dk.AgentFinding(evidence=evidence)


def _rule(key: str, check: Any, **fields: Any) -> dk.AgentRule:
    return dk.AgentRule(
        key=key,
        severity=fields.pop('severity', 'warning'),
        title=fields.pop('title', f'Rule {key} found something'),
        suggestion=fields.pop('suggestion', 'Look at the orders.'),
        check=check,
        **fields,
    )


def _full_notes() -> dk.AgentNotes:
    return dk.AgentNotes(
        concepts=[
            dk.AgentTopic(
                key='billing',
                summary='Each task bills one customer.',
                body='The customer id is the task label customer.',
                related=['concept:pipeline', 'app:recipe:refund', 'config:executor.max_executors'],
            )
        ],
        recipes=[
            dk.AgentRecipe(
                key='refund',
                summary='Find a refund that was not written.',
                question='Where is my refund?',
                steps=[dk.AgentRecipeStep(do='Trace the message.', why='It shows the sinks.')],
                causes=[dk.AgentRecipeCause(cause='The sink failed.', check='A sink_error exists.', fix='Replay.')],
                related=['app:concept:billing', 'app:metric:agent_notes_test_orders_total'],
            )
        ],
        metrics={
            'agent_notes_test_orders': dk.AgentMetricNote(
                meaning='Orders written to the sinks.',
                healthy='Grows while messages arrive.',
                promql=[dk.AgentPromql(title='Orders per second', query='rate(x{{{worker}}}[5m])')],
            )
        },
        app_config={'billing.api_url': 'The billing service.', 'app.retries': 'How often a call is retried.'},
        rules=[_rule('orders-stuck', lambda snapshot: None, explain='app:concept:billing')],
    )


# --- the models -----------------------------------------------------------


def test_the_note_models_are_exported_from_the_package():
    for name in (
        'AgentNotes',
        'AgentTopic',
        'AgentRecipe',
        'AgentRecipeStep',
        'AgentRecipeCause',
        'AgentPromql',
        'AgentMetricNote',
        'AgentRule',
        'AgentFinding',
        'AgentSnapshot',
    ):
        assert name in dk.__all__
        assert getattr(dk, name).__module__ == 'drakkar.agent.notes'


@pytest.mark.parametrize(
    ('model', 'fields'),
    [
        (dk.AgentTopic, {'key': 'Bad Key', 'summary': 'x'}),
        (dk.AgentTopic, {'key': 'ok', 'summary': 'x' * 201}),
        (dk.AgentTopic, {'key': 'ok', 'summary': 'two\nlines'}),
        (dk.AgentTopic, {'key': 'ok', 'summary': 'x', 'unknown': 1}),
        (dk.AgentRecipe, {'key': 'ok', 'summary': 'x', 'question': 'q', 'steps': []}),
        (dk.AgentRule, {'key': 'ok', 'severity': 'fatal', 'title': 't', 'suggestion': 's', 'check': len}),
    ],
)
def test_a_note_that_breaks_its_model_is_refused_when_built(model, fields):
    with pytest.raises(ValidationError):
        model(**fields)


def test_the_snapshot_is_a_frozen_value():
    snapshot = snapshot_of(_rule_input({'budget': {'tasks': 1}, 'partitions': [{'partition': 0}]}))

    with pytest.raises(TypeError):
        snapshot.state['budget']['tasks'] = 2  # type: ignore[index]
    with pytest.raises(ValidationError):
        snapshot.state = {}  # type: ignore[misc]
    assert snapshot.part('partitions') == ({'partition': 0},)
    assert snapshot.part('missing') is None


def test_freeze_copies_at_any_depth():
    source = {'a': [{'b': [1]}]}
    frozen = freeze(source)
    source['a'][0]['b'].append(2)

    assert frozen['a'][0]['b'] == (1,)


# --- the checks -----------------------------------------------------------


def test_valid_notes_become_the_manual_part_app_in_framework_part_order():
    prepared, reasons = prepare_notes(_full_notes(), registry=REGISTRY, handler_metrics=HANDLER_METRICS)

    assert reasons == []
    assert [entry['key'] for entry in prepared.entries] == [
        'app:concept:billing',
        'app:metric:agent_notes_test_orders_total',
        'app:recipe:refund',
        'app:rule:orders-stuck',
    ]
    metric_entry = prepared.entries[1]
    assert metric_entry == {
        'key': 'app:metric:agent_notes_test_orders_total',
        'summary': 'Orders this worker wrote.',
        'meaning': 'Orders written to the sinks.',
        'healthy': 'Grows while messages arrive.',
        'promql': [{'title': 'Orders per second', 'query': 'rate(x{{{worker}}}[5m])'}],
    }
    assert prepared.entries[3] == {
        'key': 'app:rule:orders-stuck',
        'summary': 'Rule orders-stuck found something',
        'severity': 'warning',
        'title': 'Rule orders-stuck found something',
        'explain': 'app:concept:billing',
        'suggestion': 'Look at the orders.',
    }
    assert prepared.config_meanings == {
        'app.billing.api_url': 'The billing service.',
        'app.retries': 'How often a call is retried.',
    }
    assert prepared.metric_notes.meaning == {'agent_notes_test_orders_total': 'Orders written to the sinks.'}
    assert [rule.key for rule in prepared.rules] == ['orders-stuck']


def test_a_note_with_a_reference_that_names_nothing_is_dropped_and_the_others_stay():
    notes = dk.AgentNotes(
        concepts=[
            dk.AgentTopic(key='good', summary='Good.', related=['concept:pipeline']),
            dk.AgentTopic(key='bad', summary='Bad.', related=['concept:no-such-concept', 'app:concept:good']),
        ],
        rules=[_rule('explains-nothing', lambda snapshot: None, explain='rule:no-such-rule')],
    )

    prepared, reasons = prepare_notes(notes, registry=REGISTRY, handler_metrics=HANDLER_METRICS)

    assert [entry['key'] for entry in prepared.entries] == ['app:concept:good']
    assert prepared.rules == ()
    assert reasons == [
        "concept 'bad': the reference 'concept:no-such-concept' names nothing",
        "rule 'explains-nothing': the reference 'rule:no-such-rule' names nothing",
    ]


def test_a_note_that_names_a_dropped_note_is_dropped_too():
    notes = dk.AgentNotes(
        concepts=[
            dk.AgentTopic(key='first', summary='First.', related=['app:concept:second']),
            dk.AgentTopic(key='second', summary='Second.', related=['metric:no_such_metric']),
        ],
    )

    prepared, reasons = prepare_notes(notes, registry=REGISTRY, handler_metrics=HANDLER_METRICS)

    assert prepared.entries == ()
    assert reasons == [
        "concept 'second': the reference 'metric:no_such_metric' names nothing",
        "concept 'first': the reference 'app:concept:second' names nothing",
    ]


@pytest.mark.parametrize(
    'ref',
    [
        'concept:pipeline',
        'config:executor.max_executors',
        'config:sinks.kafka.*.topic',
        'metric:drakkar_pending_tasks',
        'event:task_failed',
        'recipe:lag-growing',
        'route:GET /api/v1/agent',
        'rule:budget-saturated',
    ],
)
def test_a_reference_to_a_framework_entry_or_config_path_resolves(ref):
    notes = dk.AgentNotes(concepts=[dk.AgentTopic(key='k', summary='S.', related=[ref])])

    prepared, reasons = prepare_notes(notes, registry=REGISTRY, handler_metrics=HANDLER_METRICS)

    assert reasons == []
    assert len(prepared.entries) == 1


@pytest.mark.parametrize('ref', ['app:concept:missing', 'app:billing', 'nonsense', 'config:no.such.path'])
def test_a_reference_that_names_nothing_is_a_reason(ref):
    notes = dk.AgentNotes(concepts=[dk.AgentTopic(key='k', summary='S.', related=[ref])])

    prepared, reasons = prepare_notes(notes, registry=REGISTRY, handler_metrics=HANDLER_METRICS)

    assert prepared.entries == ()
    assert reasons == [f"concept 'k': the reference '{ref}' names nothing"]


def test_a_key_used_twice_and_an_unknown_metric_are_reasons():
    notes = dk.AgentNotes(
        concepts=[dk.AgentTopic(key='same', summary='One.'), dk.AgentTopic(key='same', summary='Two.')],
        metrics={'drakkar_pending_tasks': dk.AgentMetricNote(meaning='Not the handler metric.')},
        rules=[_rule('same', lambda snapshot: None), _rule('same', lambda snapshot: None)],
    )

    prepared, reasons = prepare_notes(notes, registry=REGISTRY, handler_metrics=HANDLER_METRICS)

    assert [entry['summary'] for entry in prepared.entries] == ['One.', 'Rule same found something']
    assert reasons == [
        "concept 'same': the key is used twice",
        "metric 'drakkar_pending_tasks': the handler declares no such metric",
        "rule 'same': the key is used twice",
    ]


def test_handler_metrics_answer_to_the_exposed_name_and_the_family_name():
    assert HANDLER_METRICS['agent_notes_test_orders_total'] == HandlerMetric(
        exposed='agent_notes_test_orders_total', help='Orders this worker wrote.'
    )
    assert HANDLER_METRICS['agent_notes_test_orders'] is HANDLER_METRICS['agent_notes_test_orders_total']
    assert HANDLER_METRICS['agent_notes_test_queue_depth'].exposed == 'agent_notes_test_queue_depth'


def test_a_long_metric_help_text_is_cut_for_the_summary():
    metric = HandlerMetric(exposed='m_total', help='word ' * 100)
    notes = dk.AgentNotes(metrics={'m_total': dk.AgentMetricNote(meaning='Meaning.')})

    prepared, _ = prepare_notes(notes, registry=REGISTRY, handler_metrics={'m_total': metric})

    (entry,) = prepared.entries
    assert len(entry['summary']) == 200
    assert entry['summary'].endswith('...')
    assert 'healthy' not in entry and 'promql' not in entry


def test_an_empty_metric_help_text_gives_the_meaning_as_the_summary():
    notes = dk.AgentNotes(metrics={'m': dk.AgentMetricNote(meaning='Meaning.')})

    prepared, _ = prepare_notes(notes, registry=REGISTRY, handler_metrics={'m': HandlerMetric(exposed='m', help='  ')})

    assert prepared.entries[0]['summary'] == 'Meaning.'


def _notes_of(data: dict[str, Any]) -> dk.AgentNotes:
    """Plain notes of the fixture as models; a fixture rule has no check, so each gets one that finds nothing."""
    data = json.loads(json.dumps(data))
    for rule in data.get('rules', []):
        rule['check'] = lambda snapshot: None
    return dk.AgentNotes.model_validate(data)


@pytest.mark.parametrize('case', CONTRACT['notes_cases']['cases'], ids=lambda case: case['name'])
def test_notes_cases(case):
    section = CONTRACT['notes_cases']
    registry = _tiny_registry()
    handler_metrics = handler_metrics_from(
        (metric['family'], metric['type'], metric['help']) for metric in section['handler_metrics']
    )

    prepared, reasons = prepare_notes(_notes_of(case['notes']), registry=registry, handler_metrics=handler_metrics)

    selection = select_manual(registry, parts=['app'], detail='full', app_entries=prepared.entries)
    assert dumps(selection_json(selection, section['promql_filters'])) == case['json_text']
    assert reasons == case['reasons']
    assert dict(prepared.config_meanings) == case['config_meanings']
    assert [rule.key for rule in prepared.rules] == case['rules']


def test_app_config_paths_take_the_app_prefix_once():
    assert app_config_path('billing.url') == 'app.billing.url'
    assert app_config_path('app.billing.url') == 'app.billing.url'


# --- collecting the notes at startup ---------------------------------------


def _worker(notes: Any, **sections: Any) -> DrakkarApp:
    NotesHandler.notes = notes
    return DrakkarApp(handler=NotesHandler(), config=_config(**sections), worker_id='w-1')


async def test_startup_collects_and_checks_the_notes():
    app = _worker(_full_notes())

    with capture_logs() as logs:
        await app._lifecycle._collect_agent_notes()

    assert isinstance(app._agent_notes, PreparedNotes)
    assert len(app._agent_notes.entries) == 4
    assert app._agent_registry is not None
    assert [entry for entry in logs if entry['event'].startswith('agent_notes')] == []


async def test_startup_logs_invalid_notes_with_their_reasons_and_keeps_the_others():
    notes = dk.AgentNotes(
        concepts=[
            dk.AgentTopic(key='kept', summary='Kept.'),
            dk.AgentTopic(key='dropped', summary='Dropped.', related=['concept:nope']),
        ]
    )
    app = _worker(notes)

    with capture_logs() as logs:
        await app._lifecycle._collect_agent_notes()

    (invalid,) = [entry for entry in logs if entry['event'] == 'agent_notes_invalid']
    assert (invalid['log_level'], invalid['category']) == ('warning', 'agent')
    assert invalid['reasons'] == ["concept 'dropped': the reference 'concept:nope' names nothing"]
    assert [entry['key'] for entry in app._agent_notes.entries] == ['app:concept:kept']


@pytest.mark.parametrize('notes', [RuntimeError('notes are broken'), {'concepts': []}])
async def test_a_hook_that_raises_or_returns_another_type_leaves_the_worker_without_notes(notes):
    app = _worker(notes)

    with capture_logs() as logs:
        await app._lifecycle._collect_agent_notes()

    (failed,) = [entry for entry in logs if entry['event'] == 'agent_notes_failed']
    assert (failed['log_level'], failed['category']) == ('error', 'handler')
    assert failed['error']
    assert app._agent_notes is None


async def test_an_async_hook_fails_with_a_clear_error_and_no_unawaited_warning():
    class AsyncNotesHandler(KafkaOnlyHandler):
        async def agent_notes(self) -> dk.AgentNotes | None:  # type: ignore[override]
            return None

    app = DrakkarApp(handler=AsyncNotesHandler(), config=_config(), worker_id='w-1')

    with warnings.catch_warnings():
        warnings.simplefilter('error')
        with capture_logs() as logs:
            await app._lifecycle._collect_agent_notes()
        gc.collect()

    (failed,) = [entry for entry in logs if entry['event'] == 'agent_notes_failed']
    assert failed['error'] == AGENT_NOTES_ASYNC_MESSAGE
    assert app._agent_notes is None


async def test_no_notes_and_the_surface_off_collect_nothing():
    handler_called: list[bool] = []

    class CountingHandler(KafkaOnlyHandler):
        def agent_notes(self) -> dk.AgentNotes | None:
            handler_called.append(True)
            return None

    off = DrakkarApp(handler=CountingHandler(), config=_config(agent={'enabled': False}), worker_id='w-1')
    await off._lifecycle._collect_agent_notes()
    assert handler_called == []

    on = DrakkarApp(handler=CountingHandler(), config=_config(), worker_id='w-1')
    await on._lifecycle._collect_agent_notes()
    assert handler_called == [True]
    assert on._agent_notes is None


def test_the_base_handler_gives_no_notes():
    assert KafkaOnlyHandler().agent_notes() is None


async def test_the_startup_collects_the_notes_before_the_api_server_starts():
    order: list[str] = []
    app = _worker(None, sinks={'filesystem': {'results-out': {'base_path': '/tmp'}}})

    async def notes() -> None:
        order.append('notes')

    async def api() -> None:
        order.append('api')
        raise asyncio.CancelledError

    app._lifecycle._collect_agent_notes = notes  # type: ignore[method-assign]
    app._lifecycle._start_recorder_and_api = api  # type: ignore[method-assign]
    app._lifecycle._prepare_storage = _noop  # type: ignore[method-assign]
    app._lifecycle._setup_watchdog = _noop  # type: ignore[method-assign]
    app._lifecycle._build_executor_pool = _noop  # type: ignore[method-assign]
    app._lifecycle._start_observability = _noop  # type: ignore[method-assign]
    app._lifecycle._shutdown = _noop  # type: ignore[method-assign]

    with pytest.raises(asyncio.CancelledError):
        await app._lifecycle._async_run()

    assert order == ['notes', 'api']


async def _noop() -> None:
    return None


# --- the routes -------------------------------------------------------------


def _app_with_notes(notes: dk.AgentNotes, **sections: Any) -> Any:
    prepared, reasons = prepare_notes(notes, registry=REGISTRY, handler_metrics=HANDLER_METRICS)
    assert reasons == []
    drakkar_app = _drakkar_app(_config(**sections))
    drakkar_app.handler = NotesHandler()
    drakkar_app._agent_notes = prepared
    drakkar_app._agent_registry = REGISTRY
    return drakkar_app


async def test_the_manual_serves_the_notes_in_part_app_and_the_index_sizes_follow():
    plain = _drakkar_app(_config())
    plain._agent_notes = None
    async with _client(create_api_app(plain)) as client:
        empty_index = (await client.get('/api/v1/agent')).json()

    async with _client(create_api_app(_app_with_notes(_full_notes()))) as client:
        index = (await client.get('/api/v1/agent')).json()
        manual = (await client.get('/api/v1/agent/manual', params={'parts': 'app', 'detail': 'full'})).json()
        brief = await client.get('/api/v1/agent/manual', params={'parts': 'app'})
        topic = (await client.get('/api/v1/agent/manual', params={'topic': 'app:recipe:refund'})).json()

    assert (empty_index['app_notes'], index['app_notes']) == (False, True)
    sizes = index['routes'][0]['approx_tokens']
    assert sizes['app'] == approx_tokens(brief.text)
    assert sizes['app'] > empty_index['routes'][0]['approx_tokens']['app']
    keys = [entry['key'] for entry in manual['parts']['app']]
    assert keys == [
        'app:concept:billing',
        'app:metric:agent_notes_test_orders_total',
        'app:recipe:refund',
        'app:rule:orders-stuck',
    ]
    # PromQL placeholders are filled as in the framework entries.
    assert '{{worker}}' not in manual['parts']['app'][1]['promql'][0]['query']
    assert [entry['key'] for entry in topic['parts']['app']] == [
        'app:concept:billing',
        'app:metric:agent_notes_test_orders_total',
        'app:recipe:refund',
    ]


async def test_the_config_route_gives_the_app_key_meanings():
    drakkar_app = _app_with_notes(
        _full_notes(), app={'billing': {'api_url': 'https://bill.example'}, 'retries': 3, 'other': 1}
    )
    async with _client(create_api_app(drakkar_app)) as client:
        answer = (await client.get('/api/v1/agent/config', params={'parts': 'app'})).json()

    meanings = {entry['path']: entry['meaning'] for entry in answer['entries']}
    assert meanings == {
        'app.billing.api_url': 'The billing service.',
        'app.other': '',
        'app.retries': 'How often a call is retried.',
    }


async def test_the_metrics_route_gives_the_handler_metric_notes():
    async with _client(create_api_app(_app_with_notes(_full_notes()))) as client:
        names = 'agent_notes_test_orders_total,agent_notes_test_queue_depth'
        brief = (await client.get('/api/v1/agent/metrics', params={'names': names})).json()
        full = (await client.get('/api/v1/agent/metrics', params={'names': names, 'detail': 'full'})).json()

    orders, queue = brief['metrics']
    assert (orders['meaning'], orders['healthy']) == ('Orders written to the sinks.', 'Grows while messages arrive.')
    assert 'promql' not in orders
    assert (queue['meaning'], queue['healthy']) == (None, None)
    full_orders, full_queue = full['metrics']
    assert full_orders['promql'][0]['title'] == 'Orders per second'
    assert '{{worker}}' not in full_orders['promql'][0]['query']
    assert 'promql' not in full_queue


# --- the handler rules -------------------------------------------------------


def _rule_input(state: dict[str, Any] | None = None) -> RuleInput:
    return RuleInput(
        state=state if state is not None else dict.fromkeys(STATE_PARTS),
        metrics={'drakkar_sink_dlq_messages_total': 3.0},
        settings=RuleSettings(warn_lag_ms=100, agent_token_set=True),
        now=time.monotonic(),
    )


async def test_handler_rules_follow_the_framework_rules_in_the_diagnose_answer():
    def hot(snapshot: dk.AgentSnapshot) -> dk.AgentFinding | None:
        return _finding(orders=7, metrics=sorted(snapshot.metrics))

    notes = dk.AgentNotes(
        concepts=[dk.AgentTopic(key='billing', summary='Billing.')],
        rules=[
            _rule('orders-hot', hot, severity='critical'),
            _rule('quiet', lambda snapshot: None),
            _rule('explained', lambda snapshot: _finding(), severity='info', explain='app:concept:billing'),
        ],
    )
    async with _client(create_api_app(_app_with_notes(notes))) as client:
        answer = (await client.get('/api/v1/agent/diagnose')).json()

    assert answer['checked'][-3:] == ['app:orders-hot', 'app:quiet', 'app:explained']
    assert answer['checked'][:-3] == [entry['key'] for entry in REGISTRY.parts['rules']]
    assert answer['failed'] == []
    by_rule = {finding['rule']: finding for finding in answer['findings']}
    assert by_rule['app:orders-hot'] == {
        'rule': 'app:orders-hot',
        'severity': 'critical',
        'title': 'Rule orders-hot found something',
        'evidence': {
            'orders': 7,
            'metrics': [
                'drakkar_recorder_dropped_events_total',
                'drakkar_recorder_postgres_dropped_rows_total',
                'drakkar_sink_dlq_messages_total',
            ],
        },
        'explain': 'app:rule:orders-hot',
        'suggestion': 'Look at the orders.',
    }
    assert by_rule['app:explained']['explain'] == 'app:concept:billing'
    assert answer['findings'][0]['severity'] == 'critical'


async def test_a_raising_or_wrong_handler_rule_fails_alone():
    def raising(snapshot: dk.AgentSnapshot) -> None:
        raise ValueError('no orders table')

    def mutating(snapshot: dk.AgentSnapshot) -> None:
        snapshot.state['budget']['tasks'] = 0  # type: ignore[index]

    seen: list[Any] = []

    def reading(snapshot: dk.AgentSnapshot) -> dk.AgentFinding:
        seen.append(snapshot.part('budget')['tasks'])
        return _finding(tasks=snapshot.part('budget')['tasks'])

    rules = HandlerRules(
        [
            _rule('raising', raising),
            _rule('mutating', mutating),
            _rule('wrong-type', lambda snapshot: {'evidence': {}}),
            _rule('not-json', lambda snapshot: _finding(value=object())),
            _rule('not-finite', lambda snapshot: _finding(value=float('nan'))),
            _rule('exits', lambda snapshot: (_ for _ in ()).throw(SystemExit(3))),
            _rule('reading', reading),
        ]
    )

    result = await rules(_rule_input({'budget': {'tasks': 5}}))

    failed = {row['rule']: row['error'] for row in result.failed}
    assert set(failed) == {
        'app:raising',
        'app:mutating',
        'app:wrong-type',
        'app:not-json',
        'app:not-finite',
        'app:exits',
    }
    assert failed['app:raising'] == 'no orders table'
    assert failed['app:wrong-type'] == 'the rule returned dict, not an AgentFinding or None'
    assert failed['app:exits'] == '3'
    assert [finding['evidence'] for finding in result.findings] == [{'tasks': 5}]
    assert seen == [5]


def test_evidence_of_accepts_none_and_plain_findings():
    assert evidence_of(None) is None
    assert evidence_of(_finding(a=[1, 'b', None, True, 0.5], b={'c': 1})) == {
        'a': [1, 'b', None, True, 0.5],
        'b': {'c': 1},
    }


async def test_handler_rules_run_off_the_event_loop():
    threads: list[tuple[str, bool]] = []

    def check(snapshot: dk.AgentSnapshot) -> None:
        try:
            asyncio.get_running_loop()
            on_loop = True
        except RuntimeError:
            on_loop = False
        threads.append((threading.current_thread().name, on_loop))

    await HandlerRules([_rule('where', check)])(_rule_input())

    assert threads == [('drakkar-agent-rule-where', False)]


async def test_a_slow_handler_rule_is_abandoned_logged_once_and_not_started_again():
    release = threading.Event()
    starts: list[float] = []

    def slow(snapshot: dk.AgentSnapshot) -> dk.AgentFinding:
        starts.append(time.monotonic())
        release.wait(10)
        return _finding()

    rules = HandlerRules([_rule('slow', slow), _rule('fast', lambda snapshot: _finding(ok=1))], timeout=0.5)
    try:
        with capture_logs() as logs:
            began = time.monotonic()
            first = await rules(_rule_input())
            first_seconds = time.monotonic() - began
            began = time.monotonic()
            second = await rules(_rule_input())
            second_seconds = time.monotonic() - began
    finally:
        release.set()

    for result in (first, second):
        assert result.checked == ['app:slow', 'app:fast']
        assert result.failed == [{'rule': 'app:slow', 'error': RULE_TIMED_OUT_MESSAGE}]
        assert [finding['rule'] for finding in result.findings] == ['app:fast']
    assert 0.45 < first_seconds < 5
    # The abandoned run still goes on: the second call neither starts the rule nor waits for it.
    assert second_seconds < 0.4
    assert len(starts) == 1
    abandoned = [entry for entry in logs if entry['event'] == 'agent_rule_abandoned']
    assert abandoned == [
        {'event': 'agent_rule_abandoned', 'log_level': 'warning', 'category': 'agent', 'rule': 'app:slow'}
    ]


async def _until(condition: Any, limit: float = 5.0) -> None:
    """Poll ``condition`` on the loop until it is true; fail after ``limit`` seconds."""
    ends = time.monotonic() + limit
    while not condition():
        assert time.monotonic() < ends, 'the condition did not come true in time'
        await asyncio.sleep(0.01)


async def test_a_rule_that_ended_after_its_limit_runs_again_at_the_next_call():
    release = threading.Event()
    calls: list[int] = []

    def late(snapshot: dk.AgentSnapshot) -> dk.AgentFinding:
        calls.append(1)
        if len(calls) == 1:
            release.wait(10)
        return _finding(call=len(calls))

    rules = HandlerRules([_rule('late', late)], timeout=0.1)
    first = await rules(_rule_input())
    (abandoned_run,) = rules._running.values()
    release.set()
    await _until(abandoned_run.done.is_set)
    second = await rules(_rule_input())

    assert first.failed == [{'rule': 'app:late', 'error': RULE_TIMED_OUT_MESSAGE}]
    assert second.failed == []
    assert [finding['evidence'] for finding in second.findings] == [{'call': 2}]


async def test_two_calls_at_the_same_time_share_a_run_within_its_limit():
    starts: list[float] = []
    release = threading.Event()

    def busy(snapshot: dk.AgentSnapshot) -> dk.AgentFinding:
        starts.append(time.monotonic())
        release.wait(0.3)
        return _finding(n=1)

    rules = HandlerRules([_rule('busy', busy)], timeout=5)
    with capture_logs() as logs:
        first, second = await asyncio.gather(rules(_rule_input()), rules(_rule_input()))

    for result in (first, second):
        assert result.failed == []
        assert [finding['evidence'] for finding in result.findings] == [{'n': 1}]
    assert len(starts) == 1
    assert [entry for entry in logs if entry['event'] == 'agent_rule_abandoned'] == []
    assert rules._running == {}


async def test_a_call_that_joins_a_run_gets_the_timeout_of_that_run():
    release = threading.Event()

    def hangs(snapshot: dk.AgentSnapshot) -> None:
        release.wait(10)

    rules = HandlerRules([_rule('hangs', hangs)], timeout=0.3)
    try:
        with capture_logs() as logs:
            first = asyncio.create_task(rules(_rule_input()))
            await asyncio.sleep(0)
            second = await rules(_rule_input())
            first_result = await first
    finally:
        release.set()

    for result in (first_result, second):
        assert result.failed == [{'rule': 'app:hangs', 'error': RULE_TIMED_OUT_MESSAGE}]
    assert len([entry for entry in logs if entry['event'] == 'agent_rule_abandoned']) == 1


def test_a_rule_that_ends_after_its_loop_closed_leaves_no_traceback(monkeypatch):
    errors: list[Any] = []
    monkeypatch.setattr(threading, 'excepthook', errors.append)
    release = threading.Event()

    def late(snapshot: dk.AgentSnapshot) -> None:
        release.wait(10)

    rules = HandlerRules([_rule('late', late)], timeout=0.05)
    loop = asyncio.new_event_loop()
    try:
        result = loop.run_until_complete(rules(_rule_input()))
    finally:
        loop.close()
    (run,) = rules._running.values()
    release.set()
    run._thread.join(5)

    assert result.failed == [{'rule': 'app:late', 'error': RULE_TIMED_OUT_MESSAGE}]
    assert not run._thread.is_alive()
    assert errors == []


async def test_evidence_over_the_size_limit_fails_the_rule():
    limits = CONTRACT['notes_cases']
    # {"t":"..."} is 8 bytes around the text.
    fits = _finding(t='x' * (limits['evidence_max_bytes'] - 8))
    too_large = _finding(t='x' * (limits['evidence_max_bytes'] - 7))
    assert len(dumps(fits.evidence).encode('utf-8')) == limits['evidence_max_bytes']

    result = await HandlerRules([_rule('fits', lambda snapshot: fits), _rule('large', lambda snapshot: too_large)])(
        _rule_input()
    )

    assert [finding['rule'] for finding in result.findings] == ['app:fits']
    assert result.failed == [{'rule': 'app:large', 'error': limits['evidence_too_large_error']}]


async def test_a_slow_and_a_raising_rule_hold_neither_the_api_server_nor_the_other_rules():
    """With the real 1 s limit, through the API app: health answers while a rule hangs."""
    release = threading.Event()
    started = threading.Event()

    def hangs(snapshot: dk.AgentSnapshot) -> None:
        started.set()
        release.wait(10)

    def raises(snapshot: dk.AgentSnapshot) -> None:
        raise RuntimeError('broken rule')

    notes = dk.AgentNotes(
        rules=[_rule('hangs', hangs), _rule('raises', raises), _rule('fine', lambda snapshot: _finding(n=1))]
    )
    loop_ticks = 0
    stop_ticking = asyncio.Event()

    async def tick() -> None:
        nonlocal loop_ticks
        while not stop_ticking.is_set():
            loop_ticks += 1
            await asyncio.sleep(0.01)

    ticker = asyncio.create_task(tick())
    try:
        async with _client(create_api_app(_app_with_notes(notes))) as client:
            began = time.monotonic()
            diagnose = asyncio.create_task(client.get('/api/v1/agent/diagnose'))
            await _until(started.is_set)
            health = await asyncio.wait_for(client.get('/healthz'), timeout=5)
            diagnose_was_waiting = not diagnose.done()
            answer = (await diagnose).json()
            diagnose_seconds = time.monotonic() - began
    finally:
        release.set()
        stop_ticking.set()
        await ticker

    assert health.status_code == 200
    # Health answered while the diagnose call still waited on the hanging rule.
    assert diagnose_was_waiting
    assert 0.9 < diagnose_seconds < 5
    assert loop_ticks > 10
    failed = {row['rule']: row['error'] for row in answer['failed']}
    assert failed == {'app:hangs': 'timed out after 1 s', 'app:raises': 'broken rule'}
    assert [finding['rule'] for finding in answer['findings'] if finding['rule'].startswith('app:')] == ['app:fine']


async def test_a_surface_without_rules_runs_no_handler_rules():
    notes = dk.AgentNotes(concepts=[dk.AgentTopic(key='billing', summary='Billing.')])
    surface = AgentSurface(_app_with_notes(notes))

    assert surface.app_rules is None
    assert [entry['key'] for entry in surface.app_entries] == ['app:concept:billing']


async def test_the_surface_reuses_the_registry_read_at_startup():
    drakkar_app = _app_with_notes(_full_notes())
    surface = AgentSurface(drakkar_app)

    await surface.ensure_loaded()

    assert surface.registry is REGISTRY


async def test_a_surface_ignores_an_app_without_checked_notes():
    drakkar_app = _drakkar_app(_config())
    surface = AgentSurface(drakkar_app)

    assert (surface.app_entries, surface.app_rules, surface.metric_notes) == ((), None, MetricNotes())


async def test_handler_rules_through_a_given_surface():
    drakkar_app = _app_with_notes(dk.AgentNotes(rules=[_rule('one', lambda snapshot: _finding(x=1))]))
    surface = AgentSurface(drakkar_app)

    async with _surface_client(drakkar_app, surface) as client:
        answer = (await client.get('/api/v1/agent/diagnose')).json()

    assert 'app:one' in answer['checked']
    assert dumps(answer).count('app:one') == 2


# --- bounds and payloads -------------------------------------------------------


@pytest.mark.parametrize('case', CONTRACT['rule_error_cases']['cases'], ids=lambda case: case['name'])
def test_rule_error_cases(case):
    assert CONTRACT['rule_error_cases']['max_bytes'] == ERROR_TEXT_MAX_BYTES
    assert cut_error_text(case['text']) == case['cut']
    assert error_text(RuntimeError(case['text'])) == case['cut']


def test_an_error_text_with_a_lone_surrogate_is_cut_without_raising():
    assert cut_error_text('\ud800' + 'x' * 600) == '?' + 'x' * 499


async def test_a_long_handler_rule_error_is_cut():
    def raises(snapshot: dk.AgentSnapshot) -> None:
        raise ValueError('é' * 400)

    result = await HandlerRules([_rule('long', raises)])(_rule_input())

    (row,) = result.failed
    assert row['error'] == 'é' * 250
    assert len(row['error'].encode('utf-8')) <= ERROR_TEXT_MAX_BYTES


def test_a_long_framework_rule_error_is_cut(monkeypatch):
    def raises(rule_input: RuleInput, baselines: Baselines) -> None:
        raise ValueError('z' * 900)

    monkeypatch.setitem(FRAMEWORK_RULES, 'agent-unauthenticated', raises)
    entries = [entry for entry in REGISTRY.parts['rules'] if entry['key'] == 'agent-unauthenticated']

    result = run_framework_rules(_rule_input(), rule_entries=entries, baselines=Baselines())

    assert result.failed == [{'rule': 'agent-unauthenticated', 'error': 'z' * 500}]


async def test_a_handler_rule_snapshot_holds_no_payload_field():
    seen: list[Any] = []

    def reads(snapshot: dk.AgentSnapshot) -> None:
        seen.append(snapshot.part('recent_failures'))

    failure = {'task_id': 't-1', 'exit_code': 1, 'stderr_tail': 'SECRET', 'args': ['x'], 'args_truncated': False}
    state = {**dict.fromkeys(STATE_PARTS), 'recent_failures': {'failures': [failure, 'odd row']}}

    await HandlerRules([_rule('reads', reads)])(_rule_input(state))

    assert seen == [{'failures': ({'task_id': 't-1', 'exit_code': 1}, 'odd row')}]
    assert failure['stderr_tail'] == 'SECRET'


@pytest.mark.parametrize('failures', [None, {'failures': None}, {'note': 'x'}])
async def test_a_snapshot_keeps_a_recent_failures_part_without_rows(failures):
    seen: list[Any] = []
    state = {**dict.fromkeys(STATE_PARTS), 'recent_failures': failures}

    await HandlerRules([_rule('reads', lambda snapshot: seen.append(snapshot.part('recent_failures')))])(
        _rule_input(state)
    )

    assert seen == [freeze(failures)]


async def test_diagnose_reads_no_payload_column_even_with_payloads_on():
    queries: list[str] = []

    async def read(query: str, params: Any) -> tuple[list[str], list[tuple[Any, ...]]]:
        queries.append(query)
        return ['id', 'ts', 'partition', 'task_id', 'exit_code', 'duration', 'error_kind', 'offset'], []

    store = MagicMock()
    store.is_open = True
    store.read = read
    seen: list[Any] = []
    notes = dk.AgentNotes(rules=[_rule('reads', lambda snapshot: seen.append(snapshot.part('recent_failures')))])
    drakkar_app = _app_with_notes(notes, agent={'enabled': True, 'include_payloads': True})
    drakkar_app._recorder = SimpleNamespace(memory_store=store, flush_memory=AsyncMock(), persistence='none')

    async with _client(create_api_app(drakkar_app)) as client:
        await client.get('/api/v1/agent/diagnose')
        await client.get('/api/v1/agent/state', params={'parts': 'recent_failures'})

    diagnose_query, state_query = queries
    assert 'stderr' not in diagnose_query and 'args' not in diagnose_query
    assert 'stderr' in state_query
    assert seen == [{'failures': ()}]


async def test_app_notes_is_true_with_config_meanings_only():
    drakkar_app = _app_with_notes(dk.AgentNotes(app_config={'ledger.url': 'The ledger address.'}))

    async with _client(create_api_app(drakkar_app)) as client:
        index = (await client.get('/api/v1/agent')).json()

    assert index['app_notes'] is True


def test_the_agent_words_hold_every_word_of_the_ui_config_reference():
    assert set(SECRET_NAME_WORDS) <= set(SECRET_KEY_WORDS)
    assert set(SECRET_KEY_WORDS) - set(SECRET_NAME_WORDS) == {'dsn', 'oauthbearer.config'}
    # The UI config reference keeps its patterns: one glob per word, plus two of its own.
    assert set(_SECRET_ENV_PATTERNS) == {
        '*PASSWORD*',
        '*PASSWD*',
        '*SECRET*',
        '*TOKEN*',
        '*KEY*',
        '*API_KEY*',
        '*CREDENTIAL*',
        '*_DSN',
        '*AUTH*',
        '*PRIVATE*',
        '*CERT*',
        '*SALT*',
    }


@pytest.mark.parametrize('name', ['DB_PASSWD', 'GITHUB_AUTH', 'SSH_PRIVATE', 'TLS_CERT', 'APP_SALT'])
async def test_the_config_route_masks_what_the_ui_config_reference_masks(name):
    drakkar_app = _drakkar_app(_config(executor={'env': {name: 'hunter2'}}))

    async with _client(create_api_app(drakkar_app)) as client:
        answer = await client.get('/api/v1/agent/config', params={'parts': 'executor'})

    assert sanitize_env_value(name, 'hunter2') == '***'
    assert 'hunter2' not in answer.text


@pytest.mark.parametrize('include_payloads', [False, True])
def test_the_failures_query_reads_the_metadata_of_the_kept_rows_only(include_payloads):
    import sqlite3

    from drakkar.agent.state import recent_failures_query
    from drakkar.recorder.schema import SCHEMA_EVENTS

    query, params = recent_failures_query(include_payloads=include_payloads)
    db = sqlite3.connect(':memory:')
    db.executescript(SCHEMA_EVENTS)
    plan = ' '.join(row[-1] for row in db.execute(f'EXPLAIN QUERY PLAN {query}', params))

    assert 'WHERE id IN (SELECT id FROM events WHERE event = ? ORDER BY ts DESC, id DESC LIMIT ?)' in query
    assert query.endswith('ORDER BY ts DESC, id DESC')
    # The outer rows are found by their ids, not by a scan of every failed task.
    assert plan.startswith('SEARCH events USING INTEGER PRIMARY KEY')
