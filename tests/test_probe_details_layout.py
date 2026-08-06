"""Layout generation + startup validation for user-defined probe details."""

import pytest
from pydantic import BaseModel

from drakkar.probe import (
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
