"""Status records and the status map of HTTP split jobs."""

from __future__ import annotations

import itertools
import json

import pytest

from drakkar.split.http_status import (
    FAILED_OUTCOMES,
    STATE_RANK,
    PartCounts,
    StatusMap,
    StatusRecord,
    answer_body,
    parse_status_record,
    status_record,
)
from drakkar.split.records import ANY_PARTITION

UPDATED = 1_700_000_000.123456
UPDATED_TEXT = '2023-11-14T22:13:20.123456Z'
VERSION_HEADER = [('dk-status-version', b'1')]


def make(state='accepted', *, rid='order-1', partition=7, offset=500, client='tenant-a', updated=UPDATED, **extra):
    """A status record with defaults for the fields a test does not care about."""
    fields = {'parts': None, 'result_json': None, 'duration_seconds': None, 'outcome': None, 'error': None}
    fields.update(extra)
    return StatusRecord(
        request_id=rid, state=state, partition=partition, offset=offset, client=client, updated_at=updated, **fields
    )


def running(**kw):
    return make('running', parts=PartCounts(total=1000, received=412, failed=0), **kw)


def done(**kw):
    kw.setdefault('result_json', '{"items":3}')
    kw.setdefault('duration_seconds', 1.5)
    return make('done', **kw)


def failed(**kw):
    kw.setdefault('outcome', 'timeout')
    kw.setdefault('error', 'too slow')
    return make('failed', **kw)


@pytest.mark.parametrize(
    'record',
    [
        make(),
        running(),
        make('running', parts=PartCounts(total=None, received=0, failed=0)),
        done(),
        done(result_json='null', parts=PartCounts(total=3, received=3, failed=0)),
        failed(),
        failed(outcome='parse', error=''),
    ],
)
def test_status_record_round_trip(record):
    wire = status_record(record)
    assert wire.key == b'order-1'
    assert wire.headers == VERSION_HEADER
    assert wire.partition == ANY_PARTITION
    assert parse_status_record(wire.key, wire.headers, wire.value) == record


def test_status_value_is_sorted_compact_json():
    wire = status_record(running())
    assert wire.value == (
        b'{"client":"tenant-a","job":"7:500","parts":{"failed":0,"received":412,"total":1000},'
        b'"state":"running","updated_at":"' + UPDATED_TEXT.encode() + b'"}'
    )


def test_status_record_error_is_cut_to_2000_bytes():
    wire = status_record(failed(error='é' * 3000))
    body = json.loads(wire.value)
    assert len(body['error'].encode()) <= 2000
    assert body['error'].startswith('é')


def test_parse_cuts_a_long_error_to_2000_bytes():
    body = json.loads(status_record(failed()).value)
    body['error'] = 'x' * 100_000
    parsed = parse_status_record(b'order-1', VERSION_HEADER, json.dumps(body).encode())
    assert parsed is not None
    assert parsed.error == 'x' * 2000


def _good_value(**override):
    body = json.loads(status_record(running()).value)
    body.update(override)
    return json.dumps(body).encode()


@pytest.mark.parametrize(
    ('key', 'headers', 'value'),
    [
        (b'k', VERSION_HEADER, b'not json'),
        (b'k', VERSION_HEADER, b'[1]'),
        (b'k', [('dk-status-version', b'2')], _good_value()),
        (b'k', [], _good_value()),
        (b'k', VERSION_HEADER, json.dumps({k: v for k, v in json.loads(_good_value()).items() if k != 'job'}).encode()),
        (b'k', VERSION_HEADER, _good_value(job='7-500')),
        (b'k', VERSION_HEADER, _good_value(state='paused')),
        (b'k', VERSION_HEADER, _good_value(state='failed', outcome='mystery', error='')),
        (b'k', VERSION_HEADER, _good_value(updated_at='yesterday')),
        (b'\xff', VERSION_HEADER, _good_value()),
        (None, VERSION_HEADER, _good_value()),
        (b'k', VERSION_HEADER, None),
    ],
)
def test_parse_rejects(key, headers, value):
    assert parse_status_record(key, headers, value) is None


def test_map_rank_rule():
    m = StatusMap(3600)
    assert m.put(done(), now=UPDATED)
    assert not m.put(running(), now=UPDATED)
    assert m.get('order-1').state == 'done'
    assert m.put(failed(), now=UPDATED)
    assert m.get('order-1').state == 'failed'
    m2 = StatusMap(3600)
    m2.put(make(), now=UPDATED)
    assert m2.put(done(), now=UPDATED)
    assert m2.get('order-1').state == 'done'


def test_map_lower_offset_wins():
    m = StatusMap(3600)
    m.put(make(offset=500), now=UPDATED)
    assert not m.put(done(offset=600), now=UPDATED)
    assert m.get('order-1').offset == 500
    m2 = StatusMap(3600)
    m2.put(make(offset=600), now=UPDATED)
    assert m2.put(make(offset=500), now=UPDATED)
    assert m2.get('order-1').offset == 500


def test_map_result_is_independent_of_record_order():
    records = [make(offset=500), running(offset=500), done(offset=500), make(offset=600)]
    for order in itertools.permutations(records):
        m = StatusMap(3600)
        for record in order:
            m.put(record, now=UPDATED)
        got = m.get('order-1')
        assert (got.state, got.offset) == ('done', 500), order


def test_map_keeps_client_when_new_record_has_none():
    m = StatusMap(3600)
    m.put(make(), now=UPDATED)
    m.put(running(client=''), now=UPDATED)
    stored = m.get('order-1')
    assert stored.state == 'running'
    assert stored.client == 'tenant-a'


def test_map_does_not_store_ended_records_older_than_retention():
    m = StatusMap(100)
    assert not m.put(done(), now=UPDATED + 100)
    assert m.get('order-1') is None
    assert len(m) == 0
    assert m.put(make(offset=900), now=UPDATED + 10_000)


def test_map_expires_ended_entries_after_retention():
    m = StatusMap(100)
    m.put(done(), now=UPDATED)
    assert m.expire(UPDATED + 99, limit=10) == 0
    assert m.expire(UPDATED + 100, limit=10) == 1
    assert m.get('order-1') is None
    assert m.counts()['done'] == 0


def test_map_never_expires_open_entries():
    m = StatusMap(100)
    m.put(running(), now=UPDATED)
    assert m.expire(UPDATED + 10**9, limit=10) == 0
    assert m.get('order-1') is not None


def test_map_expire_respects_limit():
    m = StatusMap(100)
    for n in range(5):
        m.put(done(rid=f'id-{n}'), now=UPDATED)
    assert m.expire(UPDATED + 100, limit=2) == 2
    assert len(m) == 3
    assert m.expire(UPDATED + 100, limit=10) == 3


def test_map_done_and_failed_of_one_job_follow_arrival_order():
    first = StatusMap(3600)
    first.put(done(), now=UPDATED)
    first.put(failed(), now=UPDATED)
    second = StatusMap(3600)
    second.put(failed(), now=UPDATED)
    second.put(done(), now=UPDATED)
    assert (first.get('order-1').state, second.get('order-1').state) == ('failed', 'done')


def test_map_expire_spends_its_limit_on_the_stale_heap_items_of_replaced_entries():
    m = StatusMap(100)
    # Three ended records of one id: the first two leave stale heap items with the same deadline.
    for state in (done, failed, done):
        m.put(state(rid='a'), now=UPDATED)
    m.put(done(rid='b'), now=UPDATED)
    assert m.expire(UPDATED + 100, limit=2) == 0
    assert len(m) == 2
    assert m.expire(UPDATED + 100, limit=10) == 2
    assert len(m) == 0


def test_map_replaced_ended_entry_does_not_expire_early():
    m = StatusMap(100)
    m.put(done(), now=UPDATED)
    m.put(failed(updated=UPDATED + 50), now=UPDATED + 50)
    assert m.expire(UPDATED + 100, limit=10) == 0
    assert m.expire(UPDATED + 150, limit=10) == 1


def test_map_counts_by_state_follow_replacements():
    m = StatusMap(3600)
    m.put(make(), now=UPDATED)
    m.put(make(rid='b'), now=UPDATED)
    assert m.counts() == {'accepted': 2, 'running': 0, 'done': 0, 'failed': 0}
    m.put(running(), now=UPDATED)
    m.put(done(rid='b'), now=UPDATED)
    assert m.counts() == {'accepted': 0, 'running': 1, 'done': 1, 'failed': 0}


def test_answer_body_shapes():
    accepted = json.loads(answer_body(make(parts=PartCounts(1, 0, 0))))
    assert accepted == {'request_id': 'order-1', 'status': 'accepted', 'updated_at': UPDATED_TEXT}

    unfinished = json.loads(answer_body(make('running', parts=PartCounts(None, 4, 1))))
    assert unfinished['parts'] == {'total': None, 'received': 4, 'failed': 1}

    finished = json.loads(answer_body(done()))
    assert finished['result'] == {'items': 3}
    assert finished['duration_seconds'] == 1.5
    assert 'parts' not in finished
    with_parts = json.loads(answer_body(done(result_json='null', parts=PartCounts(3, 3, 0))))
    assert with_parts['result'] is None
    assert with_parts['parts'] == {'total': 3, 'received': 3, 'failed': 0}

    bad = json.loads(answer_body(failed()))
    assert (bad['outcome'], bad['error']) == ('timeout', 'too slow')
    assert 'result' not in bad


def test_constants():
    assert STATE_RANK == {'accepted': 0, 'running': 1, 'done': 2, 'failed': 2}
    assert 'response_failed' in FAILED_OUTCOMES


def test_map_expired_end_removes_the_open_entries_of_its_job_in_every_order():
    records = [make(), running(), done()]
    for order in itertools.permutations(records):
        m = StatusMap(100)
        for record in order:
            m.put(record, now=UPDATED + 1000)
        assert m.get('order-1') is None, order
        assert len(m) == 0 and sum(m.counts().values()) == 0, order


def test_map_expired_end_does_not_remove_a_newer_end_of_its_job():
    m = StatusMap(100)
    m.put(done(updated=UPDATED + 1000), now=UPDATED + 1000)
    m.put(failed(), now=UPDATED + 1000)
    assert m.get('order-1').state == 'done'


def test_map_expired_end_does_not_touch_another_job_of_the_id():
    m = StatusMap(100)
    m.put(make(offset=900, updated=UPDATED + 1000), now=UPDATED + 1000)
    m.put(done(offset=500), now=UPDATED + 1000)
    assert m.get('order-1').offset == 900


def test_map_new_job_of_an_id_whose_old_job_expired_is_stored():
    m = StatusMap(100)
    for record in (make(), done(), running()):
        m.put(record, now=UPDATED + 1000)
    later = UPDATED + 1000
    assert m.put(make(offset=900, updated=later), now=later)
    assert m.put(running(offset=900, updated=later), now=later)
    assert (m.get('order-1').state, m.get('order-1').offset) == ('running', 900)


def test_map_forgets_an_expired_job_after_two_mark_rounds():
    m = StatusMap(100)
    m.put(done(), now=UPDATED + 1000)
    m.forget_old_marks()
    assert not m.put(make(), now=UPDATED + 1000)
    m.forget_old_marks()
    assert m.put(make(), now=UPDATED + 1000)


@pytest.mark.parametrize('outcome', [None, 'lost'])
def test_status_record_refuses_a_failed_record_without_a_known_outcome(outcome):
    with pytest.raises(ValueError, match='a failed status needs an outcome'):
        status_record(failed(outcome=outcome))


def test_map_marks_every_expired_job_of_one_id():
    m = StatusMap(100)
    now = UPDATED + 1000
    m.put(done(offset=500), now=now)
    m.put(done(offset=900), now=now)
    assert not m.put(make(offset=500), now=now)
    assert not m.put(make(offset=900), now=now)
    assert len(m) == 0


def test_parse_writes_a_lone_surrogate_of_a_result_as_the_replacement_character():
    body = json.loads(status_record(done()).value)
    value = json.dumps({**body, 'result': {'name': 'a\ud800b', '\udc00': [1]}}).encode()
    parsed = parse_status_record(b'order-1', VERSION_HEADER, value)
    assert parsed is not None
    assert parsed.result_json == '{"name":"a\ufffdb","\ufffd":[1]}'
    assert json.loads(answer_body(parsed))['result'] == {'name': 'a\ufffdb', '\ufffd': [1]}


def test_parse_keeps_a_surrogate_pair_of_a_result():
    body = json.loads(status_record(done()).value)
    value = json.dumps({**body, 'result': '\U0001f600'}).encode()
    parsed = parse_status_record(b'order-1', VERSION_HEADER, value)
    assert parsed is not None
    assert parsed.result_json == '"\U0001f600"'
