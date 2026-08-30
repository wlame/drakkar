"""Emitter tests: validation, drop paths, and the pinned wire envelope."""

from datetime import UTC, datetime

import pytest
from structlog.testing import capture_logs

from drakkar.annotations import Annotator
from drakkar.app import DrakkarApp
from drakkar.config import (
    DrakkarConfig,
    ExecutorConfig,
    KafkaConfig,
    LoggingConfig,
    MetricsConfig,
    SinksConfig,
    TimelineEventType,
    UIConfig,
    UITimelineConfig,
)
from drakkar.handler import BaseDrakkarHandler
from drakkar.hookctx import bind_hook_context, clear_hook_context
from drakkar.timeline_events import (
    NoOpTimelineEventEmitter,
    TimelineEventEmitter,
    TimelineMatch,
    referenced_link_bases,
)


class RecordingSink:
    """Stub recorder capturing what the annotator writes; mock target is the framework seam, not the code under test."""

    def __init__(self):
        self.records = []

    def record_annotation(self, **kwargs):
        self.records.append(kwargs)


def _emitter(sink: RecordingSink, *decls: dict) -> TimelineEventEmitter:
    types = {}
    for d in decls:
        t = TimelineEventType.model_validate(d)
        types[t.name] = t
    return TimelineEventEmitter(Annotator(sink), types)


def _marker(**over) -> dict:
    base = {'name': 'deploy', 'kind': 'marker', 'color': 'purple'}
    base.update(over)
    return base


@pytest.fixture
def window_ctx():
    token = bind_hook_context(hook='on_message_complete', partition=0, window_id=41, offsets=(1200, 1201))
    yield
    clear_hook_context(token)


@pytest.fixture
def no_offsets_ctx():
    token = bind_hook_context(hook='on_message_complete', partition=0, offsets=(), offset=None)
    yield
    clear_hook_context(token)


def test_emitted_envelope_matches_the_pinned_wire_bytes(window_ctx):
    sink = RecordingSink()
    _emitter(sink, _marker()).emit(
        'deploy', text='v2.1', ts=datetime.fromtimestamp(1756350000, tz=UTC), values={'sha': 'ab12f'}
    )
    assert len(sink.records) == 1
    # encode_json (drakkar/recorder/helpers.py) applies OPT_SORT_KEYS, so the
    # wire order is alphabetical regardless of payload construction order.
    assert sink.records[0]['metadata_json'] == (
        '{"data":{"end_ts_ms":null,"match":null,"text":"v2.1","ts_ms":1756350000000,'
        '"type":"deploy","values":{"sha":"ab12f"}},"hook":"on_message_complete",'
        '"kind":"timeline_event","offsets":[1200,1201],"scope":"window","window_id":41}'
    )


def test_unknown_type_drops_without_raising(window_ctx):
    sink = RecordingSink()
    _emitter(sink, _marker()).emit('not_declared')
    assert sink.records == []


def test_disabled_type_is_a_silent_noop(window_ctx):
    sink = RecordingSink()
    _emitter(sink, _marker(enabled=False)).emit('deploy')
    assert sink.records == []


def test_range_requires_end_ts(window_ctx):
    sink = RecordingSink()
    emitter = _emitter(sink, _marker(name='span', kind='range'))
    emitter.emit('span')  # no end_ts -> bad_shape drop
    assert sink.records == []
    emitter.emit('span', ts=datetime.fromtimestamp(10, tz=UTC), end_ts=datetime.fromtimestamp(20, tz=UTC))
    assert len(sink.records) == 1


def test_end_ts_on_a_marker_drops(window_ctx):
    sink = RecordingSink()
    _emitter(sink, _marker()).emit('deploy', end_ts=datetime.now(tz=UTC))
    assert sink.records == []


def test_end_before_start_drops(window_ctx):
    sink = RecordingSink()
    _emitter(sink, _marker(name='span', kind='range')).emit(
        'span', ts=datetime.fromtimestamp(20, tz=UTC), end_ts=datetime.fromtimestamp(10, tz=UTC)
    )
    assert sink.records == []


def test_highlight_action_autofills_offsets_match(window_ctx):
    sink = RecordingSink()
    _emitter(sink, _marker(action='highlight')).emit('deploy')
    import json

    envelope = json.loads(sink.records[0]['metadata_json'])
    assert envelope['data']['match'] == {'offsets': [[0, 1200], [0, 1201]]}


def test_highlight_action_with_no_offsets_drops(no_offsets_ctx):
    sink = RecordingSink()
    _emitter(sink, _marker(action='highlight')).emit('deploy')
    assert sink.records == []


def test_match_with_two_fields_drops(window_ctx):
    sink = RecordingSink()
    _emitter(sink, _marker(action='highlight')).emit('deploy', match=TimelineMatch(window_id=1, label=('k', 'v')))
    assert sink.records == []


def test_offsets_match_wire_shape(window_ctx):
    sink = RecordingSink()
    _emitter(sink, _marker(action='filter')).emit('deploy', match=TimelineMatch(offsets=((0, 1200), (0, 1201))))
    import json

    envelope = json.loads(sink.records[0]['metadata_json'])
    assert envelope['data']['match'] == {'offsets': [[0, 1200], [0, 1201]]}


def test_noop_emitter_swallows_everything():
    NoOpTimelineEventEmitter().emit('anything')  # must not raise


def test_handler_default_timeline_event_is_noop():
    from drakkar.handler import BaseDrakkarHandler

    class H(BaseDrakkarHandler):
        async def arrange(self, messages):  # pragma: no cover - unused
            return []

    H().timeline_event('anything')  # must not raise with nothing installed


def test_handler_delegates_to_installed_emitter(window_ctx):
    from drakkar.handler import BaseDrakkarHandler

    class H(BaseDrakkarHandler):
        async def arrange(self, messages):  # pragma: no cover - unused
            return []

    sink = RecordingSink()
    handler = H()
    handler._timeline_events = _emitter(sink, _marker())
    handler.timeline_event('deploy', text='hi')
    assert len(sink.records) == 1


class _ExplodingDatetime(datetime):
    """A datetime whose .timestamp() always raises, so the guard is exercised
    deterministically instead of relying on platform-specific mktime() overflow
    behavior for extreme values like datetime.min."""

    def timestamp(self) -> float:
        raise OverflowError('timestamp out of range')


def test_ts_timestamp_overflow_drops_without_raising(window_ctx):
    sink = RecordingSink()
    _emitter(sink, _marker()).emit('deploy', ts=_ExplodingDatetime(1, 1, 1))
    assert sink.records == []


def test_end_ts_timestamp_overflow_drops_without_raising(window_ctx):
    sink = RecordingSink()
    emitter = _emitter(sink, _marker(name='span', kind='range'))
    emitter.emit('span', ts=datetime.fromtimestamp(10, tz=UTC), end_ts=_ExplodingDatetime(9999, 1, 1))
    assert sink.records == []


def _minimal_config(ui: UIConfig | None = None) -> DrakkarConfig:
    """Smallest DrakkarConfig that satisfies DrakkarApp.__init__ (mirrors tests/test_uipages.py)."""
    return DrakkarConfig(
        kafka=KafkaConfig(brokers='localhost:9092', source_topic='test-in'),
        executor=ExecutorConfig(binary_path='/bin/echo'),
        sinks=SinksConfig(),
        metrics=MetricsConfig(enabled=False),
        logging=LoggingConfig(level='WARNING', format='console'),
        ui=ui or UIConfig(),
    )


def _app_with_timeline_events(*event_types: TimelineEventType) -> DrakkarApp:
    class _Handler(BaseDrakkarHandler):
        async def arrange(self, messages):  # pragma: no cover - unused
            return []

    ui = UIConfig(timeline=UITimelineConfig(events=list(event_types)))
    return DrakkarApp(handler=_Handler(), config=_minimal_config(ui=ui))


def test_startup_warns_on_timeline_event_link_missing_base():
    """{jira} in a deploy event's link template is unconfigured in ui.link_bases."""
    deploy = TimelineEventType(
        name='deploy', kind='marker', color='purple', action='link', link='https://{jira}/browse/{text}'
    )
    with capture_logs() as cap:
        _app_with_timeline_events(deploy)

    warnings = [r for r in cap if r['log_level'] == 'warning' and r.get('event') == 'timeline_event_link_bases_missing']
    assert len(warnings) == 1
    assert warnings[0]['event_type'] == 'deploy'
    assert warnings[0]['missing_bases'] == ['jira']
    assert 'link_bases' in warnings[0]['message']


def test_startup_silent_when_timeline_event_link_bases_are_configured():
    deploy = TimelineEventType(
        name='deploy', kind='marker', color='purple', action='link', link='https://{jira}/browse/{text}'
    )
    ui = UIConfig(link_bases={'jira': 'https://jira.example.com'}, timeline=UITimelineConfig(events=[deploy]))

    class _Handler(BaseDrakkarHandler):
        async def arrange(self, messages):  # pragma: no cover - unused
            return []

    with capture_logs() as cap:
        DrakkarApp(handler=_Handler(), config=_minimal_config(ui=ui))

    assert not [r for r in cap if r.get('event') == 'timeline_event_link_bases_missing']


def test_referenced_link_bases_excludes_builtins_and_non_lowercase_tokens():
    assert referenced_link_bases('https://{jira}/{ts_ms}/{end_ts_ms}/{text}') == {'jira'}
    assert referenced_link_bases('https://{Jira}/{NOT_LOWER}') == set()
