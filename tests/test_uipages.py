"""Tests for declared UI pages (Phase 2): models, validation, wire schema."""

import pytest
from pydantic import ValidationError

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


def test_widget_cap_enforced():
    widget = Widget(title='W', view='keyvalue', source=TasksSource())
    with pytest.raises(UIPagesConfigError, match='widgets'):
        build_pages([_page(widgets=[widget] * 13)])


def test_columns_only_valid_on_table_view():
    widget = Widget(title='W', view='keyvalue', source=TasksSource(), columns=['a'])
    with pytest.raises(UIPagesConfigError, match='columns'):
        build_pages([_page(widgets=[widget])])


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


def test_pages_referenced_bases_collects_column_templates():
    wire = build_pages([_page()])
    assert pages_referenced_bases(wire) == {'shop_admin'}
