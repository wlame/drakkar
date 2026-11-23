"""The wire contract of HTTP split jobs: ``tests/fixtures/split_http_contract.json`` pins the source message, the status records and the status map."""

import base64
import json
from datetime import datetime
from pathlib import Path

import pytest

from drakkar.split.http_message import (
    FALSE_TEXT,
    H_DELIVER_SINKS,
    H_HTTP_CLIENT,
    H_ORIGIN_HTTP,
    ORIGIN_HTTP_VALUE,
    TRUE_TEXT,
    read_http_job,
    source_message_record,
)
from drakkar.split.http_status import (
    FAILED_OUTCOMES,
    H_STATUS_VERSION,
    STATE_RANK,
    STATUS_VERSION,
    PartCounts,
    StatusMap,
    StatusRecord,
    parse_status_record,
    status_record,
)
from drakkar.split.records import H_REQUEST_ID

FIXTURE = json.loads((Path(__file__).parent / 'fixtures' / 'split_http_contract.json').read_text(encoding='utf-8'))


def _bytes(value: str | dict | None) -> bytes | None:
    """Decode the fixture's byte notation."""
    if value is None:
        return None
    if isinstance(value, dict):
        return base64.b64decode(value['base64'])
    return value.encode('utf-8')


def _headers(pairs: list) -> list[tuple[str, bytes | None]]:
    return [(name, _bytes(value)) for name, value in pairs]


def _epoch(text: str) -> float:
    return datetime.fromisoformat(text[:-1] + '+00:00').timestamp()


def _record(data: dict | None) -> StatusRecord | None:
    """The status record of a fixture input."""
    if data is None:
        return None
    fields = dict(data)
    fields['updated_at'] = _epoch(fields['updated_at'])
    parts = fields.pop('parts', None)
    fields['parts'] = None if parts is None else PartCounts(**parts)
    if 'result' in fields:
        fields['result_json'] = fields.pop('result')
    return StatusRecord(**fields)


def test_constants_and_closed_sets_match_the_fixture():
    assert (FIXTURE['status_version'], FIXTURE['status_header']) == (STATUS_VERSION, H_STATUS_VERSION)
    assert FIXTURE['source_message_header_order'] == [H_ORIGIN_HTTP, H_REQUEST_ID, H_HTTP_CLIENT, H_DELIVER_SINKS]
    assert FIXTURE['failed_outcomes'] == list(FAILED_OUTCOMES)
    assert FIXTURE['state_rank'] == STATE_RANK
    assert (ORIGIN_HTTP_VALUE, TRUE_TEXT, FALSE_TEXT) == (b'http', b'true', b'false')


@pytest.mark.parametrize('case', FIXTURE['source_message_cases'], ids=lambda c: c['name'])
def test_source_message_is_written_byte_for_byte_and_read_back(case):
    facts = case['input']
    wire = source_message_record(
        request_id=facts['request_id'],
        client=facts['client'],
        deliver_sinks=facts['deliver_sinks'],
        body=_bytes(facts['body']) or b'',
    )
    assert (wire.key, wire.value, wire.partition) == (_bytes(case['key']), _bytes(case['value']), case['partition'])
    assert wire.headers == _headers(case['headers'])
    assert [name for name, _ in wire.headers] == FIXTURE['source_message_header_order']
    job = read_http_job(_headers(case['headers']))
    assert job is not None
    assert (job.request_id, job.client, job.deliver_sinks) == (
        facts['request_id'],
        facts['client'],
        facts['deliver_sinks'],
    )


@pytest.mark.parametrize('case', FIXTURE['status_cases'], ids=lambda c: c['name'])
def test_status_record_is_written_byte_for_byte(case):
    wire = status_record(_record(case['input']))  # ty: ignore[invalid-argument-type]
    assert wire.key == _bytes(case['key'])
    assert wire.headers == _headers(case['headers'])
    assert wire.value == _bytes(case['value'])


@pytest.mark.parametrize('case', FIXTURE['status_cases'], ids=lambda c: c['name'])
def test_status_record_is_read_back(case):
    parsed = parse_status_record(_bytes(case['key']), _headers(case['headers']), _bytes(case['value']))
    assert parsed == _record(case['input'])


def test_status_cases_cover_every_state_and_outcome():
    inputs = [case['input'] for case in FIXTURE['status_cases']]
    assert {data['state'] for data in inputs} == set(STATE_RANK)
    assert {data['outcome'] for data in inputs if data['state'] == 'failed'} == set(FAILED_OUTCOMES)


@pytest.mark.parametrize('case', FIXTURE['invalid_status_cases'], ids=lambda c: c['name'])
def test_invalid_status_record_is_refused(case):
    assert case['why']
    assert parse_status_record(_bytes(case['key']), _headers(case['headers']), _bytes(case['value'])) is None


@pytest.mark.parametrize('case', FIXTURE['map_cases'], ids=lambda c: c['name'])
def test_status_map_keeps_the_record_the_fixture_says(case):
    status_map = StatusMap(case['retention_seconds'])
    now = _epoch(case['now'])
    for data in case['records']:
        status_map.put(_record(data), now=now)  # ty: ignore[invalid-argument-type]
    assert status_map.get(case['records'][0]['request_id']) == _record(case['expected'])
