"""Tests for probe-details enrichment declarations (Phase 1)."""

import pytest

from drakkar.probe import (
    Column,
    Detail,
    Element,
    Link,
    ProbeDetailsConfigError,
    _validate_template,
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
