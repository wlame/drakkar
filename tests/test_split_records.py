"""Edge cases of the split models and the record readers."""

import json

import pytest
from pydantic import BaseModel

from drakkar.models import SourceMessage
from drakkar.split import records
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
    items_done: int


def test_result_round_trips_through_its_body():
    result = PartResult(
        index=4,
        worker='w',
        outcome='failed',
        tasks_succeeded=2,
        deliveries={'kafka/out': SinkCounts(delivered=1)},
        dlq=PartDLQ(stage='execute', error='x', task_id='t'),
        data=Summary(items_done=3),
        duration_seconds=0.5,
        finished_at=10.0,
    )
    body = json.loads(json.dumps(result_body(result)))
    assert body['part'] == 4 and 'index' not in body
    restored = part_result_from_body(body)
    assert restored == result.model_copy(update={'data': {'items_done': 3}})


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


def _part_headers():
    return [
        ('dk-split-version', b'1'),
        ('dk-split-group', b'g'),
        ('dk-split-topic', b't'),
        ('dk-split-partition', b'1'),
        ('dk-split-offset', b'2'),
        ('dk-split-attempt', b'1'),
        ('dk-split-attempt-id', b'a'),
        ('dk-split-index', b'0'),
        ('dk-split-origin', b'kafka'),
    ]


HUGE = b'9' * 5000
OVER_INT64 = str(2**63).encode()


@pytest.mark.parametrize(
    ('name', 'value'),
    [
        ('dk-split-group', b'\xff\xfe'),
        ('dk-split-attempt-id', b'\xff'),
        ('dk-split-record', None),
        ('dk-split-partition', None),
        ('dk-split-offset', HUGE),
        ('dk-split-offset', OVER_INT64),
        ('dk-split-partition', b'-1'),
        ('dk-split-attempt', b'-1'),
    ],
)
def test_log_header_faults_give_none(name, value):
    headers, body = _valid()
    assert parse_log_record(_replace(headers, name, value), body) is None


@pytest.mark.parametrize(
    ('name', 'value'),
    [
        ('dk-split-group', b'\xff'),
        ('dk-split-topic', None),
        ('dk-split-offset', HUGE),
        ('dk-split-offset', OVER_INT64),
        ('dk-split-partition', b'-1'),
        ('dk-split-attempt', b'-1'),
        ('dk-split-origin', b'ftp'),
        ('dk-split-origin', None),
    ],
)
def test_part_header_faults_give_none(name, value):
    assert parse_part_headers(_replace(_part_headers(), name, value)) is None


def test_the_largest_int64_is_accepted():
    headers, body = _valid()
    entry = parse_log_record(_replace(headers, 'dk-split-offset', str(2**63 - 1).encode()), body)
    assert entry is not None and entry.ref.offset == 2**63 - 1


def test_first_duplicate_header_wins():
    headers, body = _valid()
    entry = parse_log_record([*headers, ('dk-split-offset', b'999')], body)
    assert entry is not None and entry.ref.offset == 9


@pytest.mark.parametrize(
    'value',
    [b'[' * 200000, '{"a":1}'.encode('utf-16'), '{"a":1}'.encode('utf-32'), b'\xff{}'],
    ids=['deep', 'utf16', 'utf32', 'bad-utf8'],
)
def test_hostile_values_give_none(value):
    headers, _ = _valid()
    assert parse_log_record(headers, value) is None


@pytest.mark.parametrize('partition_count', [0, -1])
def test_part_partition_refuses_a_topic_with_no_partition(partition_count):
    with pytest.raises(ValueError, match='the parts topic needs at least 1 partition'):
        records.part_partition(500, 0, partition_count)


def test_part_headers_without_the_origin_header_are_not_a_part():
    record = records.part_record(
        REF,
        source_topic='requests',
        index=0,
        request_id='req-1',
        key=None,
        value=b'x',
        source_headers=[],
        partition_count=8,
    )
    assert parse_part_headers(record.headers) is not None
    without_origin = [header for header in record.headers if header[0] != records.H_ORIGIN]

    # The producer always writes the header, so a record without it is not a part of this format.
    assert parse_part_headers(without_origin) is None


def _http_part_record(*, deliver_sinks):
    return records.part_record(
        REF,
        source_topic='requests',
        index=0,
        request_id='order-123',
        key=b'order-123',
        value=b'x',
        source_headers=[('dk-origin', b'http'), ('dk-http-client', b'tenant-a'), ('dk-deliver-sinks', b'false')],
        partition_count=8,
        origin='http',
        deliver_sinks=deliver_sinks,
    )


@pytest.mark.parametrize(('deliver_sinks', 'text'), [(True, b'on'), (False, b'off')])
def test_part_record_of_http_job_has_sinks_header_after_origin(deliver_sinks, text):
    record = _http_part_record(deliver_sinks=deliver_sinks)
    names = [name for name, _ in record.headers]
    headers = dict(record.headers)

    assert headers['dk-split-sinks'] == text
    assert names.index('dk-split-sinks') == names.index('dk-split-origin') + 1


def test_part_record_of_kafka_job_has_no_sinks_header():
    record = records.part_record(
        REF,
        source_topic='requests',
        index=0,
        request_id='r',
        key=None,
        value=b'x',
        source_headers=[],
        partition_count=8,
        deliver_sinks=False,
    )

    assert 'dk-split-sinks' not in [name for name, _ in record.headers]


def test_part_drops_http_job_headers():
    names = [name for name, _ in _http_part_record(deliver_sinks=True).headers]

    assert not {'dk-origin', 'dk-http-client', 'dk-deliver-sinks'} & set(names)


@pytest.mark.parametrize(
    ('origin', 'sinks', 'expected'),
    [
        (b'http', b'on', True),
        (b'http', b'off', False),
        (b'http', None, False),
        (b'http', b'ON', False),
        (b'kafka', None, True),
        (b'kafka', b'off', True),
    ],
)
def test_parse_part_headers_reads_deliver_sinks(origin, sinks, expected):
    headers = [(n, v) for n, v in _part_headers() if n != 'dk-split-origin']
    headers.append(('dk-split-origin', origin))
    if sinks is not None:
        headers.append(('dk-split-sinks', sinks))

    part = parse_part_headers(headers)

    assert part is not None
    assert part.deliver_sinks is expected


@pytest.mark.parametrize('partition_count', [0, -1])
def test_key_partition_needs_a_partition(partition_count):
    with pytest.raises(ValueError, match='the parts topic needs at least 1 partition'):
        records.key_partition(b'customer-17', partition_count)


def test_part_record_with_a_key_changes_only_the_partition():
    facts = {
        'source_topic': 'requests',
        'index': 0,
        'request_id': 'req-1',
        'key': b'request-key',
        'value': b'{"n": 1}',
        'source_headers': [('traceparent', b'00-1'), ('dk-old', b'x')],
        'partition_count': 12,
    }
    plain = records.part_record(REF, **facts)
    keyed = records.part_record(REF, **facts, affinity_key=b'customer-17')

    assert (keyed.key, keyed.value, keyed.headers) == (plain.key, plain.value, plain.headers)
    assert plain.partition == records.part_partition(REF.offset, 0, 12) == 9
    assert keyed.partition == records.key_partition(b'customer-17', 12) == 11
