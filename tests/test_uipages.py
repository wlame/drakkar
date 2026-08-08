"""Tests for declared UI pages: models, validation, wire schema."""

import pytest
from pydantic import ValidationError
from structlog.testing import capture_logs

from drakkar.app import DrakkarApp
from drakkar.config import (
    DrakkarConfig,
    ExecutorConfig,
    KafkaConfig,
    LoggingConfig,
    MetricsConfig,
    SinksConfig,
    UIConfig,
)
from drakkar.handler import BaseDrakkarHandler
from drakkar.probe import Column
from drakkar.uipages import (
    AnnotationsSource,
    EventsSource,
    MetricsSource,
    Page,
    TasksSource,
    UIPagesConfigError,
    Widget,
    build_pages,
    pages_referenced_bases,
)


def _page(**overrides):
    base = dict(
        slug='orders',
        title='Orders',
        widgets=[
            Widget(
                title='Recent orders',
                view='table',
                source=AnnotationsSource(kind_prefix='order.', limit=100),
                columns={
                    'order_id': Column(link_template='{shop_admin}/orders/{value}'),
                    'status': Column(badge_colors={'paid': 'green', '*': 'gray'}),
                },
            )
        ],
    )
    base.update(overrides)
    return Page(**base)


def test_build_pages_produces_wire_list_with_derived_columns():
    wire = build_pages([_page()])
    assert wire[0].slug == 'orders'
    widget = wire[0].widgets[0]
    assert widget.source == {'kind': 'annotations', 'kind_prefix': 'order.', 'limit': 100}
    assert [c.key for c in widget.columns] == ['order_id', 'status']
    assert widget.columns[0].label == 'Order id'
    assert widget.columns[0].link_template == '{shop_admin}/orders/{value}'


def test_build_pages_none_and_empty_return_empty_list():
    assert build_pages(None) == []
    assert build_pages([]) == []


def test_duplicate_slugs_rejected():
    with pytest.raises(UIPagesConfigError, match='duplicate'):
        build_pages([_page(), _page(title='Other')])


def test_invalid_slug_rejected():
    with pytest.raises(UIPagesConfigError, match='slug'):
        build_pages([_page(slug='Orders Page')])


def test_events_source_requires_event_types():
    with pytest.raises(ValidationError):
        EventsSource(event_types=[])
    with pytest.raises(ValidationError):
        EventsSource(event_types=[''])


def test_metrics_source_requires_metric():
    with pytest.raises(ValidationError):
        MetricsSource(metric='')


@pytest.mark.parametrize(
    ('model', 'kwargs', 'stray_field'),
    [
        (Page, {'slug': 'orders', 'title': 'Orders', 'widgets': [], 'titel': 'x'}, 'titel'),
        (
            Widget,
            {'title': 'W', 'view': 'keyvalue', 'source': TasksSource(), 'colums': ['a']},
            'colums',
        ),
        (EventsSource, {'event_types': ['task_failed'], 'limt': 5}, 'limt'),
        (AnnotationsSource, {'kind_prefx': 'order.'}, 'kind_prefx'),
        (TasksSource, {'limi': 5}, 'limi'),
        (MetricsSource, {'metric': 'drakkar_tasks_total', 'metrc': 'x'}, 'metrc'),
    ],
)
def test_author_models_reject_typo_kwargs(model, kwargs, stray_field):
    with pytest.raises(ValidationError) as exc_info:
        model(**kwargs)
    assert stray_field in str(exc_info.value)


def test_widget_cap_enforced():
    widget = Widget(title='W', view='keyvalue', source=TasksSource())
    with pytest.raises(UIPagesConfigError, match='widgets'):
        build_pages([_page(widgets=[widget] * 13)])


def test_columns_only_valid_on_table_view():
    widget = Widget(title='W', view='keyvalue', source=TasksSource(), columns=['a'])
    with pytest.raises(UIPagesConfigError, match='columns'):
        build_pages([_page(widgets=[widget])])


def test_table_view_requires_columns():
    """Page rows are dynamic dicts, so an omitted `columns` cannot fall back to
    "every field" the way probe-details' fixed-row-model table view does — an
    omitted columns produces a blank table on the wire, not a full one."""
    widget = Widget(title='W', view='table', source=TasksSource())
    with pytest.raises(UIPagesConfigError, match='requires columns'):
        build_pages([_page(widgets=[widget])])
    empty_list = Widget(title='W', view='table', source=TasksSource(), columns=[])
    with pytest.raises(UIPagesConfigError, match='requires columns'):
        build_pages([_page(widgets=[empty_list])])


def test_badge_view_requires_field_and_colors():
    widget = Widget(
        title='W', view='badge', source=EventsSource(event_types=['task_failed']), badge_colors={'failed': 'red'}
    )
    with pytest.raises(UIPagesConfigError, match='field'):
        build_pages([_page(widgets=[widget])])


def test_stat_and_metrics_are_paired_both_ways():
    stat_without_metrics = Widget(title='W', view='stat', source=TasksSource())
    with pytest.raises(UIPagesConfigError, match='metrics'):
        build_pages([_page(widgets=[stat_without_metrics])])
    metrics_without_stat = Widget(
        title='W', view='string', field='x', source=MetricsSource(metric='drakkar_tasks_total')
    )
    with pytest.raises(UIPagesConfigError, match='stat'):
        build_pages([_page(widgets=[metrics_without_stat])])


def test_column_templates_allow_unknown_row_refs_but_reject_bad_syntax():
    ok = Widget(
        title='W',
        view='table',
        source=TasksSource(),
        columns={'task_id': Column(link_template='{tracing}/task/{row.anything_goes}')},
    )
    build_pages([_page(widgets=[ok])])  # must not raise
    bad = Widget(
        title='W',
        view='table',
        source=TasksSource(),
        columns={'task_id': Column(link_template='{tracing}/task/{value')},
    )
    with pytest.raises(UIPagesConfigError, match='malformed'):
        build_pages([_page(widgets=[bad])])


def test_column_renderer_reaches_wire_on_page_columns():
    widget = Widget(title='W', view='table', source=TasksSource(), columns={'task_id': Column(renderer='taskChip')})
    wire = build_pages([_page(widgets=[widget])])
    assert wire[0].widgets[0].columns[0].renderer == 'taskChip'


def test_column_renderer_exclusive_with_link_template_on_page_columns():
    widget = Widget(
        title='W',
        view='table',
        source=TasksSource(),
        columns={'task_id': Column(renderer='taskChip', link_template='{tracing}/task/{value}')},
    )
    with pytest.raises(UIPagesConfigError, match='renderer'):
        build_pages([_page(widgets=[widget])])


def test_pages_referenced_bases_collects_column_templates():
    wire = build_pages([_page()])
    assert pages_referenced_bases(wire) == {'shop_admin'}


# --- App-level wiring: startup validation + the missing-link-bases warning --
#
# DrakkarApp.__init__ builds the declared pages via build_pages() right
# alongside the probe-details layout (see the app.py call site) and folds
# their referenced {base} tokens into the same single missing-link-bases
# warning. Mirrors the fixture pattern in tests/test_probe_enrichment.py's
# startup-warning tests.


def _minimal_config(ui: UIConfig | None = None) -> DrakkarConfig:
    """Smallest DrakkarConfig that satisfies DrakkarApp.__init__."""
    return DrakkarConfig(
        kafka=KafkaConfig(brokers='localhost:9092', source_topic='test-in'),
        executor=ExecutorConfig(binary_path='/bin/echo'),
        sinks=SinksConfig(),
        metrics=MetricsConfig(enabled=False),
        logging=LoggingConfig(level='WARNING', format='console'),
        ui=ui or UIConfig(),
    )


@pytest.fixture
def app_factory():
    """Build a DrakkarApp from a handler with the given ``ui_pages``.

    Construction is synchronous and never touches Kafka, so no mocking is
    needed to exercise the __init__-time validation/warning.
    """

    def _build(*, ui_pages=None, ui: UIConfig | None = None) -> DrakkarApp:
        class _Handler(BaseDrakkarHandler):
            async def arrange(self, messages, pending):
                return []

        _Handler.ui_pages = ui_pages
        return DrakkarApp(handler=_Handler(), config=_minimal_config(ui=ui))

    return _build


def test_app_validates_ui_pages_at_startup(app_factory):
    bad = Page(
        slug='x',
        title='X',
        widgets=[Widget(title='W', view='table', source=TasksSource(), columns=['a', 'a'])],
    )
    with pytest.raises(UIPagesConfigError, match='duplicate'):
        app_factory(ui_pages=[bad])


def test_startup_warning_covers_page_template_bases(app_factory):
    """{shop_admin} from the declared page's column template is unconfigured."""
    with capture_logs() as cap:
        app_factory(ui_pages=[_page()])

    warnings = [r for r in cap if r['log_level'] == 'warning' and r.get('event') == 'probe_details_link_bases_missing']
    assert len(warnings) == 1
    assert warnings[0]['missing_bases'] == ['shop_admin']
    assert 'link_bases' in warnings[0]['message']


def test_startup_silent_when_page_bases_are_configured(app_factory):
    """No warning once every referenced base is present in ui.link_bases."""
    with capture_logs() as cap:
        app_factory(ui_pages=[_page()], ui=UIConfig(link_bases={'shop_admin': 'https://admin.example.com'}))

    assert not [r for r in cap if r.get('event') == 'probe_details_link_bases_missing']
