"""The probe.set/append/update verbs: bound-probe behavior and production no-op."""

import pytest
from pydantic import BaseModel

from drakkar import probe
from drakkar.probe import _DetailsState, build_layout, probe_field


class PickedRow(BaseModel):
    item_id: str
    score: float


class VerbDetails(BaseModel):
    selection_note: str | None = probe_field(section='Selection', view='string', default=None)
    counters: dict[str, int] = probe_field(section='Selection', view='keyvalue', default_factory=dict)
    context_blob: dict = probe_field(section='Selection', view='dict', default_factory=dict)
    picked_items: list[PickedRow] = probe_field(section='Rows', view='table', default_factory=list)


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


def test_update_merges_into_keyvalue(bound):
    probe.update('counters', matched=3)
    probe.update('counters', skipped=1, matched=4)
    assert bound.instance.counters == {'matched': 4, 'skipped': 1}


def test_update_on_scalar_field_records_error(bound, errors):
    probe.update('selection_note', k=1)
    assert errors[0][:3] == ('selection_note', 'update', 'ProbeDetailsError')


def test_write_cap_records_one_error_then_drops(bound, errors, monkeypatch):
    monkeypatch.setattr(probe, 'MAX_WRITES', 3)
    for i in range(5):
        probe.update('counters', **{f'k{i}': i})
    assert len(bound.writes) == 3
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
