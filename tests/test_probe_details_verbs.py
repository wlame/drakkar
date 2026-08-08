"""The probe.set/append/update verbs: bound-probe behavior and production no-op."""

import pytest
from pydantic import BaseModel

from drakkar import probe
from drakkar.probe import _DetailsState, build_layout, probe_field


class PickedRow(BaseModel):
    item_id: str
    score: float


class TreeMatchRow(BaseModel):
    file: str
    section: str
    score: float


class VerbDetails(BaseModel):
    selection_note: str | None = probe_field(section='Selection', view='string', default=None)
    counters: dict[str, int] = probe_field(section='Selection', view='keyvalue', default_factory=dict)
    context_blob: dict = probe_field(section='Selection', view='dict', default_factory=dict)
    picked_items: list[PickedRow] = probe_field(section='Rows', view='table', default_factory=list)
    per_file_rows: dict[str, list[PickedRow]] = probe_field(section='Rows', view='tables', default_factory=dict)
    context_or_none: dict | None = probe_field(section='Optional', view='dict', default=None)
    rows_or_none: list[PickedRow] | None = probe_field(section='Optional', view='table', default=None)
    groups_or_none: dict[str, list[PickedRow]] | None = probe_field(section='Optional', view='tables', default=None)
    tree_matches: list[TreeMatchRow] = probe_field(
        section='Rows', view='tree', group_by=('file', 'section'), default_factory=list
    )


@pytest.fixture
def errors() -> list[tuple[str, str, str, str]]:
    return []


@pytest.fixture
def state(errors) -> _DetailsState:
    return _DetailsState(
        model=VerbDetails,
        layout=build_layout(VerbDetails),
        stage=lambda: 'arrange',
        now_ms=lambda: 12.5,
        on_error=lambda field, op, cls, msg: errors.append((field, op, cls, msg)),
    )


@pytest.fixture
def bound(state):
    token = probe._active_state.set(state)
    yield state
    probe._active_state.reset(token)


def test_verbs_outside_probe_are_silent_noops():
    # No state bound: nothing raises, nothing happens.
    probe.set(selection_note='x')
    probe.append('picked_items', PickedRow(item_id='a', score=1.0))
    probe.update('counters', matched=1)


def test_set_writes_field_and_records_stamped_write(bound):
    probe.set(selection_note='lookback chosen')
    assert bound.instance.selection_note == 'lookback chosen'
    assert len(bound.writes) == 1
    w = bound.writes[0]
    assert (w.field, w.op, w.origin_stage, w.ms_since_start) == ('selection_note', 'set', 'arrange', 12.5)


def test_set_with_two_fields_records_two_writes(bound):
    probe.set(selection_note='a', context_blob={'k': 1})
    assert [w.field for w in bound.writes] == ['selection_note', 'context_blob']


def test_set_unknown_field_records_error_not_write(bound, errors):
    probe.set(no_such_field=1)
    assert bound.writes == []
    field, op, cls, _msg = errors[0]
    assert (field, op, cls) == ('no_such_field', 'set', 'ProbeDetailsError')


def test_set_wrong_type_records_validation_error(bound, errors):
    probe.set(selection_note=[1, 2, 3])
    assert bound.writes == []
    assert errors[0][2] == 'ValidationError'


def test_append_adds_row_and_coerces_dict(bound):
    probe.append('picked_items', PickedRow(item_id='a', score=1.0))
    probe.append('picked_items', {'item_id': 'b', 'score': 2.0})
    assert [r.item_id for r in bound.instance.picked_items] == ['a', 'b']
    assert [w.op for w in bound.writes] == ['append', 'append']


def test_append_to_non_table_records_error(bound, errors):
    probe.append('selection_note', PickedRow(item_id='a', score=1.0))
    assert errors[0][:3] == ('selection_note', 'append', 'ProbeDetailsError')


def test_append_invalid_row_records_validation_error(bound, errors):
    probe.append('picked_items', {'item_id': 'a'})  # score missing
    assert errors[0][2] == 'ValidationError'
    assert bound.instance.picked_items == []


def test_append_with_group_creates_subtable_and_appends(bound):
    probe.append('per_file_rows', PickedRow(item_id='a', score=1.0), group='first_input_file.csv')
    probe.append('per_file_rows', {'item_id': 'b', 'score': 2.0}, group='first_input_file.csv')
    probe.append('per_file_rows', {'item_id': 'c', 'score': 3.0}, group='second_input_file.csv')
    assert list(bound.instance.per_file_rows) == ['first_input_file.csv', 'second_input_file.csv']
    assert [r.item_id for r in bound.instance.per_file_rows['first_input_file.csv']] == ['a', 'b']
    assert [r.item_id for r in bound.instance.per_file_rows['second_input_file.csv']] == ['c']
    assert len(bound.writes) == 3
    assert all(w.op == 'append' for w in bound.writes)


def test_append_to_tables_without_group_records_error(bound, errors):
    probe.append('per_file_rows', PickedRow(item_id='a', score=1.0))
    assert errors[0][:3] == ('per_file_rows', 'append', 'ProbeDetailsError')
    assert 'group' in errors[0][3]
    assert bound.instance.per_file_rows == {}


def test_append_with_group_on_plain_table_records_error(bound, errors):
    probe.append('picked_items', PickedRow(item_id='a', score=1.0), group='some_group')
    assert errors[0][:3] == ('picked_items', 'append', 'ProbeDetailsError')
    assert bound.instance.picked_items == []


def test_append_invalid_row_into_group_records_validation_error(bound, errors):
    probe.append('per_file_rows', {'item_id': 'a'}, group='first_input_file.csv')
    assert errors[0][:3] == ('per_file_rows', 'append', 'ValidationError')
    assert bound.instance.per_file_rows == {}


def test_append_to_nullable_tables_field_succeeds(bound):
    assert bound.instance.groups_or_none is None
    probe.append('groups_or_none', {'item_id': 'a', 'score': 1.0}, group='first_input_file.csv')
    assert [r.item_id for r in bound.instance.groups_or_none['first_input_file.csv']] == ['a']
    assert len(bound.writes) == 1
    assert bound.writes[0].op == 'append'


def test_append_to_tree_field_appends_flat_rows(bound):
    probe.append('tree_matches', TreeMatchRow(file='first_input_file.csv', section='header', score=1.0))
    probe.append('tree_matches', {'file': 'first_input_file.csv', 'section': 'body', 'score': 2.0})
    assert [r.section for r in bound.instance.tree_matches] == ['header', 'body']
    assert len(bound.writes) == 2
    assert all(w.op == 'append' for w in bound.writes)


def test_append_with_group_on_tree_field_records_error(bound, errors):
    probe.append('tree_matches', TreeMatchRow(file='f', section='s', score=1.0), group='some_group')
    assert errors[0][:3] == ('tree_matches', 'append', 'ProbeDetailsError')
    assert bound.instance.tree_matches == []


def test_to_user_details_serializes_tree_rows_flat(bound):
    probe.append('tree_matches', TreeMatchRow(file='f.csv', section='s1', score=1.5))
    details = bound.to_user_details()
    assert details.data['tree_matches'] == [{'file': 'f.csv', 'section': 's1', 'score': 1.5}]


def test_update_on_tables_field_records_error(bound, errors):
    probe.update('per_file_rows', k=1)
    assert errors[0][:3] == ('per_file_rows', 'update', 'ProbeDetailsError')


def test_to_user_details_serializes_grouped_tables_as_ordered_pairs(bound):
    probe.append('per_file_rows', PickedRow(item_id='a', score=1.0), group='first_input_file.csv')
    details = bound.to_user_details()
    assert details.data['per_file_rows'] == [['first_input_file.csv', [{'item_id': 'a', 'score': 1.0}]]]


def test_to_user_details_keeps_first_append_order_for_numeric_groups(bound):
    # Integer-like group names are the reason tables travel as pairs: a JSON
    # object would let JS clients re-enumerate "12"/"3" numerically.
    probe.append('per_file_rows', PickedRow(item_id='a', score=1.0), group='12')
    probe.append('per_file_rows', PickedRow(item_id='b', score=2.0), group='3')
    details = bound.to_user_details()
    assert [pair[0] for pair in details.data['per_file_rows']] == ['12', '3']


def test_to_user_details_serializes_empty_tables_field_as_empty_list(bound):
    assert bound.to_user_details().data['per_file_rows'] == []


def test_append_with_non_string_group_records_error(bound, errors):
    probe.append('per_file_rows', PickedRow(item_id='a', score=1.0), group=123)  # type: ignore[arg-type]
    assert errors[0][:3] == ('per_file_rows', 'append', 'ProbeDetailsError')
    assert 'string group' in errors[0][3]
    assert bound.instance.per_file_rows == {}


def test_update_merges_into_keyvalue(bound):
    probe.update('counters', matched=3)
    probe.update('counters', skipped=1, matched=4)
    assert bound.instance.counters == {'matched': 4, 'skipped': 1}


def test_update_on_scalar_field_records_error(bound, errors):
    probe.update('selection_note', k=1)
    assert errors[0][:3] == ('selection_note', 'update', 'ProbeDetailsError')


def _bound_state_with_caps(errors, **caps) -> _DetailsState:
    """A bound state with explicit caps — the path ui.probe_details.* config takes."""
    return _DetailsState(
        model=VerbDetails,
        layout=build_layout(VerbDetails),
        stage=lambda: 'arrange',
        now_ms=lambda: 12.5,
        on_error=lambda field, op, cls, msg: errors.append((field, op, cls, msg)),
        **caps,
    )


def test_write_cap_records_one_error_then_drops(errors):
    state = _bound_state_with_caps(errors, max_writes=3)
    token = probe._active_state.set(state)
    try:
        for i in range(5):
            probe.update('counters', **{f'k{i}': i})
    finally:
        probe._active_state.reset(token)
    assert len(state.writes) == 3
    assert len(errors) == 1
    assert 'cap' in errors[0][3]


def test_byte_cap_records_one_error_then_drops(errors):
    state = _bound_state_with_caps(errors, max_total_bytes=10)
    token = probe._active_state.set(state)
    try:
        probe.set(selection_note='a value that serializes past ten bytes')
        probe.set(selection_note='dropped')
    finally:
        probe._active_state.reset(token)
    assert len(state.writes) == 1
    assert state.instance.selection_note != 'dropped'
    assert len(errors) == 1
    assert 'cap' in errors[0][3]


def test_to_user_details_serializes_data_layout_and_writes(bound):
    probe.set(selection_note='x')
    probe.append('picked_items', PickedRow(item_id='a', score=1.0))
    details = bound.to_user_details()
    assert details.model == 'VerbDetails'
    assert details.data['selection_note'] == 'x'
    assert details.data['picked_items'] == [{'item_id': 'a', 'score': 1.0}]
    assert len(details.writes) == 2


def test_to_user_details_replaces_unserializable_value_with_placeholder(bound):
    cyclic: dict = {}
    cyclic['self'] = cyclic
    probe.set(context_blob=cyclic)
    details = bound.to_user_details()
    assert isinstance(details.data['context_blob'], str)
    assert details.data['context_blob'].startswith('<unserializable')


def test_update_on_nullable_dict_field_succeeds(bound):
    # Nullable dict field starts as None; update should initialize it
    assert bound.instance.context_or_none is None
    probe.update('context_or_none', k=1)
    assert bound.instance.context_or_none == {'k': 1}
    assert len(bound.writes) == 1
    assert bound.writes[0].op == 'update'


def test_append_to_nullable_table_field_succeeds(bound):
    # Nullable table field starts as None; append should initialize it
    assert bound.instance.rows_or_none is None
    probe.append('rows_or_none', {'item_id': 'a', 'score': 1.0})
    assert len(bound.instance.rows_or_none) == 1
    assert bound.instance.rows_or_none[0].item_id == 'a'
    assert len(bound.writes) == 1
    assert bound.writes[0].op == 'append'
