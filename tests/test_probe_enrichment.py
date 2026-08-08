"""Tests for probe-details enrichment declarations (Phase 1)."""

import pytest
from pydantic import BaseModel
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
from drakkar.probe import (
    Column,
    Detail,
    Element,
    Link,
    ProbeDetailsConfigError,
    _validate_template,
    build_layout,
    probe_field,
    referenced_bases,
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


class _BuildRow(BaseModel):
    build_id: str
    job_name: str
    duration_ms: int


def _table_model(**field_kwargs):
    class M(BaseModel):
        builds: list[_BuildRow] = probe_field(section='Builds', view='table', default_factory=list, **field_kwargs)

    return M


def test_layout_carries_column_options_in_declared_order():
    M = _table_model(
        columns={
            'build_id': Column(link_template='{jenkins}/job/{row.job_name}/{value}'),
            'duration_ms': Column(format='duration_ms'),
        }
    )
    entry = build_layout(M).sections[0].entries[0]
    assert [c.key for c in entry.columns] == ['build_id', 'duration_ms']
    assert entry.columns[0].link_template == '{jenkins}/job/{row.job_name}/{value}'
    assert entry.columns[1].format == 'duration_ms'


def test_columns_subset_must_be_row_model_fields():
    M = _table_model(columns=['build_id', 'nope'])
    with pytest.raises(ProbeDetailsConfigError, match="'nope'"):
        build_layout(M)


def test_column_templates_validate_row_references():
    M = _table_model(columns={'build_id': Column(link_template='{x}/{row.missing}')})
    with pytest.raises(ProbeDetailsConfigError, match="'missing'"):
        build_layout(M)


def test_column_badge_colors_rejects_unknown_color_name():
    M = _table_model(columns={'build_id': Column(badge_colors={'ok': 'chartreuse'})})
    with pytest.raises(ProbeDetailsConfigError, match='chartreuse'):
        build_layout(M)


def test_detail_element_fields_must_exist_on_row_model():
    M = _table_model(detail=Detail(elements=[Element(field='nope', view='string')]))
    with pytest.raises(ProbeDetailsConfigError, match="'nope'"):
        build_layout(M)


def test_detail_links_element_requires_links_and_no_field():
    with pytest.raises(ProbeDetailsConfigError, match='links'):
        build_layout(_table_model(detail=Detail(elements=[Element(view='links')])))


def test_badge_scalar_entry_reaches_wire():
    class M(BaseModel):
        status: str = probe_field(section='S', view='badge', badge_colors={'ok': 'green', '*': 'gray'}, default='')

    entry = build_layout(M).sections[0].entries[0]
    assert entry.view == 'badge' and entry.badge_colors == {'ok': 'green', '*': 'gray'}


def test_referenced_bases_walks_columns_details_and_links():
    M = _table_model(
        columns={'build_id': Column(link_template='{jenkins}/x/{value}')},
        detail=Detail(
            elements=[Element(view='links', links=[Link(label='Ticket', template='{jira}/browse/{row.build_id}')])]
        ),
    )
    assert referenced_bases(build_layout(M)) == {'jenkins', 'jira'}


# --- Startup warning for unconfigured link_bases -----------------------------
#
# DrakkarApp.__init__ builds the probe-details layout right after validating
# the handler (see the build_layout() call site), then diffs the templates'
# referenced bases against ui.link_bases. A missing base is not a startup
# error — the UI degrades to plain text — but it should be visible in logs
# rather than silently discovered by an operator clicking a dead link.


class _MissingBaseRow(BaseModel):
    ticket: str


class _MissingBaseDetails(BaseModel):
    rows: list[_MissingBaseRow] = probe_field(
        section='S',
        view='table',
        default_factory=list,
        columns={'ticket': Column(link_template='{jira}/browse/{value}')},
    )


class _MissingBaseHandler(BaseDrakkarHandler):
    probe_details_model = _MissingBaseDetails

    async def arrange(self, messages, pending):
        return []


def _minimal_config(ui: UIConfig | None = None) -> DrakkarConfig:
    """Smallest DrakkarConfig that satisfies DrakkarApp.__init__.

    Mirrors ``test_config_no_sinks`` in tests/test_app.py — construction is
    synchronous and never touches Kafka, so no mocking is needed to exercise
    the __init__-time warning.
    """
    return DrakkarConfig(
        kafka=KafkaConfig(brokers='localhost:9092', source_topic='test-in'),
        executor=ExecutorConfig(binary_path='/bin/echo'),
        sinks=SinksConfig(),
        metrics=MetricsConfig(enabled=False),
        logging=LoggingConfig(level='WARNING', format='console'),
        ui=ui or UIConfig(),
    )


def test_startup_warns_about_missing_link_bases():
    """A template base absent from ui.link_bases logs one warning naming it."""
    with capture_logs() as cap:
        DrakkarApp(handler=_MissingBaseHandler(), config=_minimal_config())

    warnings = [r for r in cap if r['log_level'] == 'warning' and r.get('event') == 'probe_details_link_bases_missing']
    assert len(warnings) == 1
    assert warnings[0]['missing_bases'] == ['jira']
    assert 'jira' in warnings[0]['message']


def test_startup_silent_when_all_referenced_bases_are_configured():
    """No warning is logged once the referenced base is present in ui.link_bases."""
    config = _minimal_config(ui=UIConfig(link_bases={'jira': 'https://jira.internal.example.com'}))

    with capture_logs() as cap:
        DrakkarApp(handler=_MissingBaseHandler(), config=config)

    assert not [r for r in cap if r.get('event') == 'probe_details_link_bases_missing']


def test_startup_silent_when_ui_disabled_even_with_missing_link_bases():
    """No warning when ui.enabled=False — no UI means no link is ever rendered.

    Mirrors the guard in warn_if_ui_unauthenticated (drakkar/app_security.py):
    the layout build itself still runs (fail-fast validation stays unconditional),
    but the operational warning about unresolved bases would be a false positive
    with the UI off.
    """
    config = _minimal_config(ui=UIConfig(enabled=False))

    with capture_logs() as cap:
        DrakkarApp(handler=_MissingBaseHandler(), config=config)

    assert not [r for r in cap if r.get('event') == 'probe_details_link_bases_missing']
