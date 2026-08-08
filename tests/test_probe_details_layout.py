"""Layout generation + startup validation for user-defined probe details."""

import json
import pathlib

import pytest
from pydantic import BaseModel

from drakkar.probe import (
    Column,
    Detail,
    Element,
    Link,
    ProbeDetailsConfigError,
    build_layout,
    probe_field,
)


class PickedRow(BaseModel):
    item_id: str
    score: float


class GoodDetails(BaseModel):
    selection_note: str | None = probe_field(section='Selection', view='string', default=None)
    counters: dict[str, int] = probe_field(section='Selection', view='keyvalue', default_factory=dict)
    context_blob: dict = probe_field(section='Selection', view='dict', default_factory=dict)
    picked_items: list[PickedRow] = probe_field(section='Rows', view='table', default_factory=list)


def test_build_layout_groups_sections_in_first_appearance_order():
    layout = build_layout(GoodDetails)
    assert [s.title for s in layout.sections] == ['Selection', 'Rows']
    assert [e.key for e in layout.sections[0].entries] == ['selection_note', 'counters', 'context_blob']


def test_build_layout_prettifies_default_labels():
    layout = build_layout(GoodDetails)
    entry = layout.sections[0].entries[0]
    assert entry.label == 'Selection note'


def test_build_layout_honours_label_override():
    class Overridden(BaseModel):
        n: int = probe_field(section='S', view='string', label='Custom label', default=0)

    layout = build_layout(Overridden)
    assert layout.sections[0].entries[0].label == 'Custom label'


def test_build_layout_table_columns_come_from_row_model():
    layout = build_layout(GoodDetails)
    table = layout.sections[1].entries[0]
    assert table.view == 'table'
    assert [(c.key, c.label) for c in table.columns] == [('item_id', 'Item id'), ('score', 'Score')]


def test_build_layout_non_table_entries_have_null_columns():
    layout = build_layout(GoodDetails)
    assert layout.sections[0].entries[0].columns is None


def test_build_layout_is_cached_per_model():
    assert build_layout(GoodDetails) is build_layout(GoodDetails)


def test_probe_field_rejects_unknown_view_at_definition_time():
    with pytest.raises(ProbeDetailsConfigError, match='view'):
        probe_field(section='S', view='grid')  # type: ignore[arg-type]


def test_build_layout_rejects_unannotated_field():
    class Missing(BaseModel):
        plain: int = 0

    with pytest.raises(ProbeDetailsConfigError, match='plain'):
        build_layout(Missing)


def test_build_layout_rejects_field_without_default():
    class Required(BaseModel):
        must: str = probe_field(section='S', view='string')

    with pytest.raises(ProbeDetailsConfigError, match='must'):
        build_layout(Required)


def test_build_layout_rejects_empty_section():
    with pytest.raises(ProbeDetailsConfigError, match='section'):
        probe_field(section='', view='string', default=None)


def test_build_layout_rejects_table_of_scalars():
    class BadTable(BaseModel):
        rows: list[str] = probe_field(section='S', view='table', default_factory=list)

    with pytest.raises(ProbeDetailsConfigError, match='rows'):
        build_layout(BadTable)


def test_build_layout_tables_columns_come_from_row_model():
    class Grouped(BaseModel):
        per_file_rows: dict[str, list[PickedRow]] = probe_field(section='Files', view='tables', default_factory=dict)

    layout = build_layout(Grouped)
    entry = layout.sections[0].entries[0]
    assert entry.view == 'tables'
    assert [(c.key, c.label) for c in entry.columns] == [('item_id', 'Item id'), ('score', 'Score')]


def test_build_layout_accepts_nullable_tables_field():
    class NullableGrouped(BaseModel):
        groups: dict[str, list[PickedRow]] | None = probe_field(section='Files', view='tables', default=None)

    assert build_layout(NullableGrouped).sections[0].entries[0].view == 'tables'


def test_build_layout_rejects_tables_with_scalar_rows():
    class BadRows(BaseModel):
        groups: dict[str, list[str]] = probe_field(section='S', view='tables', default_factory=dict)

    with pytest.raises(ProbeDetailsConfigError, match='groups'):
        build_layout(BadRows)


def test_build_layout_rejects_tables_on_plain_list():
    class BadShape(BaseModel):
        groups: list[PickedRow] = probe_field(section='S', view='tables', default_factory=list)

    with pytest.raises(ProbeDetailsConfigError, match='groups'):
        build_layout(BadShape)


def test_build_layout_rejects_tables_with_non_string_keys():
    class BadKeys(BaseModel):
        groups: dict[int, list[PickedRow]] = probe_field(section='S', view='tables', default_factory=dict)

    with pytest.raises(ProbeDetailsConfigError, match='groups'):
        build_layout(BadKeys)


class TreeRow(BaseModel):
    file: str
    section: str
    score: float


def test_build_layout_tree_carries_group_by_and_columns():
    class Treed(BaseModel):
        matches: list[TreeRow] = probe_field(
            section='Files', view='tree', group_by=('file', 'section'), default_factory=list
        )

    entry = build_layout(Treed).sections[0].entries[0]
    assert entry.view == 'tree'
    assert entry.group_by == ['file', 'section']
    assert [c.key for c in entry.columns] == ['file', 'section', 'score']


def test_build_layout_non_tree_entries_have_null_group_by():
    layout = build_layout(GoodDetails)
    assert all(e.group_by is None for s in layout.sections for e in s.entries)


def test_probe_field_rejects_tree_without_group_by():
    with pytest.raises(ProbeDetailsConfigError, match='group_by'):
        probe_field(section='S', view='tree')


def test_probe_field_rejects_group_by_on_non_tree_view():
    with pytest.raises(ProbeDetailsConfigError, match='group_by'):
        probe_field(section='S', view='table', group_by=('file',))


def test_probe_field_rejects_group_by_deeper_than_four_levels():
    with pytest.raises(ProbeDetailsConfigError, match='at most 4'):
        probe_field(section='S', view='tree', group_by=('a', 'b', 'c', 'd', 'e'))


def test_probe_field_rejects_duplicate_group_by_entries():
    with pytest.raises(ProbeDetailsConfigError, match='unique'):
        probe_field(section='S', view='tree', group_by=('file', 'file'))


def test_build_layout_rejects_group_by_naming_unknown_row_field():
    class BadKeys(BaseModel):
        matches: list[TreeRow] = probe_field(
            section='S', view='tree', group_by=('no_such_field',), default_factory=list
        )

    with pytest.raises(ProbeDetailsConfigError, match='no_such_field'):
        build_layout(BadKeys)


def test_build_layout_rejects_tree_on_non_list_field():
    class BadShape(BaseModel):
        matches: dict[str, list[TreeRow]] = probe_field(
            section='S', view='tree', group_by=('file',), default_factory=dict
        )

    with pytest.raises(ProbeDetailsConfigError, match='matches'):
        build_layout(BadShape)


def test_build_layout_rejects_keyvalue_with_non_scalar_values():
    class BadKV(BaseModel):
        kv: dict[str, list[int]] = probe_field(section='S', view='keyvalue', default_factory=dict)

    with pytest.raises(ProbeDetailsConfigError, match='kv'):
        build_layout(BadKV)


def test_build_layout_rejects_string_view_on_list():
    class BadString(BaseModel):
        s: list[int] = probe_field(section='S', view='string', default_factory=list)

    with pytest.raises(ProbeDetailsConfigError, match='s'):
        build_layout(BadString)


def test_build_layout_accepts_optional_scalar_string_view():
    class OptScalar(BaseModel):
        v: int | None = probe_field(section='S', view='string', default=None)

    assert build_layout(OptScalar).sections[0].entries[0].view == 'string'


class ParityRow(BaseModel):
    item_id: str
    score: float


class ParityTreeRow(BaseModel):
    file: str
    section: str
    score: float


class GoldenBuildRow(BaseModel):
    build_id: str
    job_name: str
    duration_ms: int
    outcome: str
    labels: dict[str, str]
    steps: list[dict]


class GoldenCardRow(BaseModel):
    build_id: str


class ParityDetails(BaseModel):
    strategy_note: str | None = probe_field(section='Arrange', view='string', default=None)
    counters: dict[str, int] = probe_field(section='Arrange', view='keyvalue', default_factory=dict)
    context_blob: dict = probe_field(section='Arrange', view='dict', default_factory=dict)
    picked_items: list[ParityRow] = probe_field(section='Tasks', view='table', default_factory=list)
    per_file_rows: dict[str, list[ParityRow]] = probe_field(section='Tasks', view='tables', default_factory=dict)
    tree_matches: list[ParityTreeRow] = probe_field(
        section='Tasks', view='tree', group_by=('file', 'section'), default_factory=list
    )
    release_state: str = probe_field(
        section='Enrichment',
        view='badge',
        badge_colors={'shipped': 'green', 'blocked': 'red', '*': 'gray'},
        default='',
    )
    build_rows: list[GoldenBuildRow] = probe_field(
        section='Enrichment',
        view='table',
        default_factory=list,
        columns={
            'build_id': Column(
                link_template='{jenkins}/job/{row.job_name}/{value}',
                hint='Open build {value}',
            ),
            'duration_ms': Column(format='duration_ms'),
            'outcome': Column(badge_colors={'passed': 'green', 'failed': 'red', '*': 'gray'}),
        },
        detail=Detail(
            title='Build {row.build_id}',
            elements=[
                Element(field='job_name', view='string'),
                Element(field='labels', view='keyvalue'),
                Element(field='steps', view='table'),
                Element(
                    view='links',
                    links=[
                        Link(label='Jenkins job', template='{jenkins}/job/{row.job_name}'),
                    ],
                ),
            ],
        ),
    )
    render_payload: dict = probe_field(section='Enrichment', view='custom', renderer='orderCard', default_factory=dict)
    card_rows: list[GoldenCardRow] = probe_field(
        section='Enrichment',
        view='table',
        default_factory=list,
        columns={'build_id': Column(renderer='buildChip')},
    )


def test_layout_matches_cross_backend_golden_fixture():
    golden = json.loads(pathlib.Path('tests/fixtures/probe_user_details_layout.json').read_text())
    assert build_layout(ParityDetails).model_dump() == golden
