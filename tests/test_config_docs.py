"""Validation tests for ui.docs (UIDocsConfig / DocsAnchor) and the startup anchor cross-checks."""

import pytest
from pydantic import ValidationError
from structlog.testing import capture_logs

from drakkar.app import DrakkarApp
from drakkar.config import (
    DocsAnchor,
    DrakkarConfig,
    ExecutorConfig,
    FileSinkConfig,
    KafkaConfig,
    LoggingConfig,
    MetricsConfig,
    SinksConfig,
    TimelineEventType,
    UIConfig,
    UIDocsConfig,
    UITimelineConfig,
)
from drakkar.handler import BaseDrakkarHandler
from drakkar.probe import Column
from drakkar.uipages import Page, TasksSource, Widget


def _anchor(**over) -> dict:
    base = {'match': {'label': 'module'}, 'path': 'architecture/scanners/#modules'}
    base.update(over)
    return base


def test_docs_defaults_are_off():
    cfg = UIDocsConfig()
    assert (cfg.site_dir, cfg.title, cfg.anchors) == ('', 'Docs', [])


def test_anchor_match_requires_exactly_one_selector():
    with pytest.raises(ValidationError, match='exactly one'):
        DocsAnchor.model_validate(_anchor(match={'label': 'module', 'sink': 'archive_results_db'}))
    with pytest.raises(ValidationError, match='exactly one'):
        DocsAnchor.model_validate(_anchor(match={}))


def test_anchor_value_only_with_label():
    with pytest.raises(ValidationError, match='value'):
        DocsAnchor.model_validate(_anchor(match={'sink': 'archive_results_db', 'value': 'x'}))
    ok = DocsAnchor.model_validate(_anchor(match={'label': 'module', 'value': 'vendor'}))
    assert ok.match.value == 'vendor'


@pytest.mark.parametrize(
    'bad_path',
    [
        '../escape.md',
        'a/../../b',
        'a/../b',
        '..',
        '/absolute.md',
        'https://example.com/page',
        'javascript:alert(1)',
        'notes:2026.html',
        'a#b#c',
        '',
    ],
)
def test_anchor_path_rejects_traversal_absolute_and_scheme(bad_path):
    with pytest.raises(ValidationError, match='path'):
        DocsAnchor.model_validate(_anchor(path=bad_path))


def test_anchor_path_accepts_relative_with_fragment():
    assert DocsAnchor.model_validate(_anchor()).path == 'architecture/scanners/#modules'


def test_anchor_path_accepts_dots_inside_a_segment():
    """Only a whole '..' segment traverses; dots inside a filename are ordinary characters."""
    assert DocsAnchor.model_validate(_anchor(path='release..notes.html')).path == 'release..notes.html'


def test_duplicate_anchor_matches_are_rejected():
    anchors = [_anchor(), _anchor(path='operations/runbook/')]
    with pytest.raises(ValidationError, match='duplicate'):
        UIDocsConfig.model_validate({'anchors': anchors})


def test_anchors_differing_in_selector_or_value_are_not_duplicates():
    cfg = UIDocsConfig.model_validate(
        {
            'anchors': [
                _anchor(),
                _anchor(match={'label': 'module', 'value': 'vendor'}),
                _anchor(match={'sink': 'archive_results_db'}),
                _anchor(match={'event': 'deploy_marker'}),
                _anchor(match={'page': 'scanners'}),
            ]
        }
    )
    assert len(cfg.anchors) == 5


def test_more_than_200_anchors_fail():
    anchors = [_anchor(match={'label': f'k{i}'}) for i in range(201)]
    with pytest.raises(ValidationError):
        UIDocsConfig.model_validate({'anchors': anchors})


def test_extra_keys_forbidden():
    with pytest.raises(ValidationError):
        UIDocsConfig.model_validate({'site_dir': '', 'unknown': 1})


# --- App-level wiring: docs-anchor cross-checks at startup ------------------
#
# DrakkarApp.__init__ resolves each anchor's sink/event/page selector against
# what the deployment actually declares and warns per anchor that names
# nothing. Mirrors the missing-link-base warnings next to it in app.py: warn,
# never fail — a stale anchor is a dead UI affordance, not a broken worker.


def _minimal_config(ui: UIConfig | None = None, sinks: SinksConfig | None = None) -> DrakkarConfig:
    """Smallest DrakkarConfig that satisfies DrakkarApp.__init__ (mirrors tests/test_uipages.py)."""
    return DrakkarConfig(
        kafka=KafkaConfig(brokers='localhost:9092', source_topic='test-in'),
        executor=ExecutorConfig(binary_path='/bin/echo'),
        sinks=sinks or SinksConfig(),
        metrics=MetricsConfig(enabled=False),
        logging=LoggingConfig(level='WARNING', format='console'),
        ui=ui or UIConfig(),
    )


def _startup_warnings(*, anchors=(), sinks=None, events=(), ui_pages=None, link_bases=None) -> list[dict]:
    """Construct an app with the given docs config and return its warning records."""

    class _Handler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):  # pragma: no cover - never called
            return []

    _Handler.ui_pages = ui_pages
    ui = UIConfig(
        link_bases=link_bases or {},
        timeline=UITimelineConfig(events=list(events)),
        docs=UIDocsConfig.model_validate({'site_dir': '/srv/operator-docs', 'anchors': list(anchors)}),
    )
    with capture_logs() as cap:
        DrakkarApp(handler=_Handler(), config=_minimal_config(ui=ui, sinks=sinks))
    return [record for record in cap if record['log_level'] == 'warning']


def _declared_page(slug: str, *, link_template: str = '') -> Page:
    column = Column(link_template=link_template) if link_template else 'task_id'
    columns = {'task_id': column} if link_template else [column]
    return Page(
        slug=slug,
        title=slug.title(),
        widgets=[Widget(title='Recent tasks', view='table', source=TasksSource(), columns=columns)],
    )


def _file_sinks(*names: str) -> SinksConfig:
    return SinksConfig(filesystem={name: FileSinkConfig(base_path='/var/lib/drakkar/out') for name in names})


def test_startup_warns_on_docs_anchor_naming_an_unknown_sink():
    warnings = _startup_warnings(
        anchors=[_anchor(match={'sink': 'archive_results_db'})],
        sinks=_file_sinks('publish_results_files'),
    )
    assert [w['event'] for w in warnings] == ['docs_anchor_unknown_sink']
    assert warnings[0]['category'] == 'docs'
    assert warnings[0]['selector'] == 'sink'
    assert warnings[0]['target'] == 'archive_results_db'
    assert 'ui.docs.anchors' in warnings[0]['message']


def test_startup_silent_when_docs_anchor_names_a_configured_sink():
    assert (
        _startup_warnings(
            anchors=[_anchor(match={'sink': 'archive_results_db'})],
            sinks=_file_sinks('archive_results_db'),
        )
        == []
    )


def test_startup_silent_when_docs_anchor_names_a_custom_sink_instance():
    """Plugin-registered instances live one level deeper under sinks.custom."""
    sinks = SinksConfig(custom={'vector_index': {'embed_results_index': {'endpoint': 'http://index:8080'}}})
    assert _startup_warnings(anchors=[_anchor(match={'sink': 'embed_results_index'})], sinks=sinks) == []


def test_startup_warns_on_docs_anchor_naming_an_unknown_event():
    deploy = TimelineEventType(name='deploy_marker', kind='marker', color='purple')
    warnings = _startup_warnings(anchors=[_anchor(match={'event': 'rollback_marker'})], events=[deploy])
    assert [w['event'] for w in warnings] == ['docs_anchor_unknown_event']
    assert (warnings[0]['selector'], warnings[0]['target']) == ('event', 'rollback_marker')


def test_startup_silent_when_docs_anchor_names_a_declared_event():
    deploy = TimelineEventType(name='deploy_marker', kind='marker', color='purple')
    assert _startup_warnings(anchors=[_anchor(match={'event': 'deploy_marker'})], events=[deploy]) == []


def test_startup_warns_on_docs_anchor_naming_an_unknown_page():
    warnings = _startup_warnings(anchors=[_anchor(match={'page': 'scanners'})], ui_pages=[_declared_page('orders')])
    assert [w['event'] for w in warnings] == ['docs_anchor_unknown_page']
    assert (warnings[0]['selector'], warnings[0]['target']) == ('page', 'scanners')


def test_startup_silent_when_docs_anchor_names_a_declared_page():
    assert _startup_warnings(anchors=[_anchor(match={'page': 'orders'})], ui_pages=[_declared_page('orders')]) == []


def test_startup_never_warns_on_a_label_anchor():
    """Label keys come from runtime task labels, so there is nothing to check them against."""
    assert _startup_warnings(anchors=[_anchor(match={'label': 'module', 'value': 'vendor'})]) == []


def test_startup_warns_once_per_unresolved_anchor():
    warnings = _startup_warnings(
        anchors=[
            _anchor(match={'sink': 'archive_results_db'}),
            _anchor(match={'event': 'deploy_marker'}, path='operations/deploys/'),
        ]
    )
    assert [w['event'] for w in warnings] == ['docs_anchor_unknown_sink', 'docs_anchor_unknown_event']


def test_startup_does_not_flag_the_builtin_docs_base_in_a_timeline_link():
    """{docs} resolves to the worker's own site, so it never counts as a missing link base."""
    deploy = TimelineEventType(
        name='deploy_marker', kind='marker', color='purple', action='link', link='{docs}/operations/deploys/'
    )
    assert _startup_warnings(events=[deploy]) == []


def test_startup_does_not_flag_the_builtin_docs_base_in_a_page_template():
    page = _declared_page('orders', link_template='{docs}/tasks/#{value}')
    assert _startup_warnings(ui_pages=[page]) == []
