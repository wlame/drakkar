"""Tests for probe-details enrichment declarations (Phase 1)."""

import pytest

from drakkar.probe import (
    Column,
    Detail,
    Element,
    Link,
    ProbeDetailsConfigError,
    _validate_template,
    probe_field,
)


def test_validate_template_accepts_value_base_and_row_tokens():
    _validate_template('{jenkins}/job/{row.job_name}/{value}', where='M.f', row_fields={'job_name'})  # must not raise


def test_validate_template_rejects_unbalanced_braces():
    with pytest.raises(ProbeDetailsConfigError, match='malformed template'):
        _validate_template('{jira}/browse/{value', where='M.f', row_fields=None)


def test_validate_template_rejects_row_token_outside_rows():
    with pytest.raises(ProbeDetailsConfigError, match='row'):
        _validate_template('{jira}/{row.x}', where='M.f', row_fields=None)


def test_validate_template_rejects_unknown_row_field():
    with pytest.raises(ProbeDetailsConfigError, match="names 'nope'"):
        _validate_template('{x}/{row.nope}', where='M.f', row_fields={'job_name'})


def test_validate_template_rejects_dotted_base_names():
    with pytest.raises(ProbeDetailsConfigError, match='base'):
        _validate_template('{jira.internal}/x', where='M.f', row_fields=None)


def test_column_model_holds_enrichment_options():
    col = Column(link_template='{jira}/browse/{value}', hint='Open the ticket')
    assert col.badge_colors is None and col.format is None


def test_column_rejects_unknown_format():
    with pytest.raises(ValueError):
        Column(format='fortnights')


def test_detail_models_compose():
    detail = Detail(
        title='Order {row.order_id}',
        elements=[
            Element(field='customer', view='keyvalue'),
            Element(view='links', links=[Link(label='Jira', template='{jira}/browse/{row.ticket}')]),
        ],
    )
    assert detail.elements[1].links[0].label == 'Jira'


def test_badge_view_requires_badge_colors():
    with pytest.raises(ProbeDetailsConfigError, match='badge_colors'):
        probe_field(section='S', view='badge', default='')


def test_badge_colors_only_valid_with_badge_view():
    with pytest.raises(ProbeDetailsConfigError, match="view 'badge'"):
        probe_field(section='S', view='string', badge_colors={'*': 'gray'}, default='')


def test_badge_colors_rejects_unknown_color_name():
    with pytest.raises(ProbeDetailsConfigError, match='unknown color'):
        probe_field(section='S', view='badge', badge_colors={'ok': 'chartreuse'}, default='')


def test_link_template_and_detail_are_exclusive_on_scalars():
    with pytest.raises(ProbeDetailsConfigError, match='detail'):
        probe_field(
            section='S',
            view='string',
            link_template='{jira}/{value}',
            detail=Detail(elements=[Element(field='x', view='string')]),
            default='',
        )


def test_columns_only_valid_on_row_bearing_views():
    with pytest.raises(ProbeDetailsConfigError, match='columns'):
        probe_field(section='S', view='string', columns=['a'], default='')


def test_detail_only_valid_on_row_bearing_views():
    with pytest.raises(ProbeDetailsConfigError, match='detail'):
        probe_field(
            section='S',
            view='keyvalue',
            detail=Detail(elements=[Element(field='x', view='string')]),
            default_factory=dict,
        )


def test_format_rejected_on_non_scalar_views():
    with pytest.raises(ProbeDetailsConfigError, match='format'):
        probe_field(section='S', view='dict', format='bytes', default_factory=dict)


def test_columns_list_normalized_to_metadata():
    field_info = probe_field(section='S', view='table', columns=['a', 'b'], default_factory=list)
    meta = field_info.json_schema_extra['drakkar_probe']
    assert list(meta['columns']) == ['a', 'b']
    assert all(isinstance(c, Column) for c in meta['columns'].values())
