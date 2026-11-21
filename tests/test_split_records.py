"""Edge cases of the split models and the record readers."""

import json

import pytest
from pydantic import BaseModel

from drakkar.models import SourceMessage
from drakkar.split.models import PartDLQ, PartResult, SinkCounts, SplitJob, SplitPart
from drakkar.split.records import (
    JobRef,
    log_record,
    new_attempt_id,
    parse_log_record,
    parse_part_headers,
    part_result_from_body,
    part_value,
    result_body,
)

REF = JobRef(group='g', partition=3, offset=9, attempt=1, attempt_id='aaaaaaaaaaaaaaaa')


def _valid():
    wire = log_record(REF, 'open', {'worker': 'w'})
    return list(wire.headers), wire.value


def _replace(headers, name, value):
    return [(n, value if n == name else v) for n, v in headers]


def _without(headers, name):
    return [(n, v) for n, v in headers if n != name]


def test_valid_record_parses():
    headers, value = _valid()
    assert parse_log_record(headers, value) is not None


@pytest.mark.parametrize(
    'mutate',
    [
        lambda h, v: (_replace(h, 'dk-split-version', b'2'), v),
        lambda h, v: (_without(h, 'dk-split-group'), v),
        lambda h, v: (_replace(h, 'dk-split-partition', b'x'), v),
        lambda h, v: (_replace(h, 'dk-split-offset', b'-1'), v),
        lambda h, v: (_replace(h, 'dk-split-record', b'bogus'), v),
        lambda h, v: (h, b'[1]'),
        lambda h, v: (h, b'not json'),
        lambda h, v: (h, b'{"a":NaN}'),
        lambda h, v: (h, None),
        lambda h, v: (None, v),
    ],
    ids=['version', 'missing-group', 'partition', 'offset', 'kind', 'array', 'text', 'nan', 'no-value', 'no-headers'],
)
def test_invalid_record_parses_to_none(mutate):
    headers, value = mutate(*_valid())
    assert parse_log_record(headers, value) is None


def test_part_headers_of_an_ordinary_message_are_none():
    assert parse_part_headers([('traceparent', b'x')]) is None
    assert parse_part_headers(None) is None


def test_part_value_accepts_models_and_bytes_only():
    class Item(BaseModel):
        n: int

    assert part_value(Item(n=1)) == b'{"n":1}'
    assert part_value(b'raw') == b'raw'
    with pytest.raises(TypeError):
        part_value({'n': 1})


def test_new_attempt_id_is_16_hex_and_unique():
    first, second = new_attempt_id(), new_attempt_id()
    assert len(first) == 16
    assert set(first) <= set('0123456789abcdef')
    assert first != second


def test_job_id_shows_partition_offset_attempt():
    part = SplitPart(
        group='g', source_topic='t', source_partition=7, source_offset=500, attempt=1, attempt_id='a', index=0
    )
    assert part.job_id == '7:500#1'


class Summary(BaseModel):
    matches: int


def test_result_round_trips_through_its_body():
    result = PartResult(
        index=4,
        worker='w',
        outcome='failed',
        tasks_succeeded=2,
        deliveries={'kafka/out': SinkCounts(delivered=1)},
        dlq=PartDLQ(stage='execute', error='x', task_id='t'),
        data=Summary(matches=3),
        duration_seconds=0.5,
        finished_at=10.0,
    )
    body = json.loads(json.dumps(result_body(result)))
    assert body['part'] == 4 and 'index' not in body
    restored = part_result_from_body(body)
    assert restored == result.model_copy(update={'data': {'matches': 3}})


def test_result_without_data_has_null_data():
    body = result_body(PartResult(index=0, worker='w', outcome='ok'))
    assert body['data'] is None
    assert part_result_from_body(body).data is None


def test_split_job_reports_missing_parts_from_its_supplier():
    message = SourceMessage(topic='t', partition=1, offset=2, value=b'', timestamp=0)
    job = SplitJob(
        source_message=message,
        job_id='1:2#1',
        attempt=1,
        local=False,
        total_parts=3,
        received_parts=1,
        failed_parts=0,
        dlq_parts=0,
        tasks_succeeded=0,
        tasks_failed=0,
        tasks_replaced=0,
        deliveries={},
        timed_out='',
        opened_at=0.0,
        duration_seconds=0.0,
        missing=lambda: [1, 2],
        results_source=lambda: (_ for _ in ()),  # type: ignore[arg-type,return-value]
    )
    assert job.missing_parts() == [1, 2]


def test_source_message_split_is_not_serialized():
    part = SplitPart(
        group='g', source_topic='t', source_partition=1, source_offset=2, attempt=1, attempt_id='a', index=0
    )
    message = SourceMessage(topic='t', partition=1, offset=2, value=b'', timestamp=0, split=part)
    assert message.split == part
    assert 'split' not in message.model_dump()
    assert SourceMessage(topic='t', partition=1, offset=2, value=b'', timestamp=0).split is None
