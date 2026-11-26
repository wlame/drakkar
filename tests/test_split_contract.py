"""The split wire contract: ``tests/fixtures/split_contract.json`` pins header order, worked records and closed sets."""

import base64
import json
from pathlib import Path
from typing import get_args

import pytest

from drakkar.split import records
from drakkar.split.models import JobAction, PartResult
from drakkar.split.records import (
    ClosedOutcome,
    JobRef,
    LogEntry,
    RecordKind,
    closed_body,
    key_partition,
    log_record,
    open_body,
    parse_log_record,
    parse_part_headers,
    part_partition,
    part_record,
    result_body,
)
from drakkar.split.table import IgnoreReason, JobTable

FIXTURE = json.loads((Path(__file__).parent / 'fixtures' / 'split_contract.json').read_text(encoding='utf-8'))


def _bytes(value: str | dict | None) -> bytes | None:
    """Decode the fixture's byte notation."""
    if value is None:
        return None
    if isinstance(value, dict):
        return base64.b64decode(value['base64'])
    return value.encode('utf-8')


def _headers(pairs: list) -> list[tuple[str, bytes | None]]:
    return [(name, _bytes(value)) for name, value in pairs]


def _ref(raw: dict) -> JobRef:
    return JobRef(**raw)


def _log_body(case: dict) -> dict:
    kind, data = case['kind'], case['input']
    builders = {
        'open': lambda: open_body(**data),
        'closed': lambda: closed_body(**data),
        'result': lambda: result_body(PartResult(**data)),
    }
    return builders.get(kind, lambda: data)()


def test_header_orders_and_constants_match_the_fixture():
    assert list(records.LOG_HEADER_ORDER) == FIXTURE['log_header_order']
    assert list(records.PART_HEADER_ORDER) == FIXTURE['part_header_order']
    assert FIXTURE['version'] == records.SPLIT_VERSION
    assert FIXTURE['parts_partition_base'] == records.PARTS_PARTITION_BASE


def test_closed_sets_match_the_fixture():
    assert list(get_args(RecordKind)) == FIXTURE['record_kinds']
    assert list(get_args(ClosedOutcome)) == FIXTURE['closed_outcomes']
    assert [action.value for action in JobAction] == FIXTURE['job_actions']
    assert [reason.value for reason in IgnoreReason] == FIXTURE['ignore_reasons']


@pytest.mark.parametrize('case', FIXTURE['split_done_parts_cases'], ids=lambda c: f'{c["parts"]}-{c["verdict"]}')
def test_split_done_part_count_follows_the_fixture(case):
    job_ref = JobRef(group='workers', partition=7, offset=500, attempt=1, attempt_id='fedcba9876543210')
    table = JobTable(partition=7, group='workers')
    opened = LogEntry(ref=job_ref, kind='open', body=open_body(worker='w1', instance='i-1', request_id=None))
    table.apply(opened, offset=0, timestamp_ms=1_000)
    done = LogEntry(ref=job_ref, kind='split-done', body={'parts': case['parts']})
    applied = table.apply(done, offset=1, timestamp_ms=2_000)
    if case['verdict'] == 'malformed':
        assert applied.ignored is IgnoreReason.MALFORMED
        assert applied.job is not None and applied.job.parts is None
    else:
        assert applied.ignored is None
        assert applied.job is not None and applied.job.parts == case['parts']


@pytest.mark.parametrize('case', FIXTURE['partition_cases'], ids=lambda c: f'{c["offset"]}-{c["index"]}')
def test_part_partition_follows_the_fixture(case):
    assert part_partition(case['offset'], case['index'], case['partition_count']) == case['partition']


@pytest.mark.parametrize('case', FIXTURE['key_partition_cases'], ids=lambda c: f'{c["key"]}-{c["partition_count"]}')
def test_key_partition_cases_of_the_contract(case):
    assert key_partition(_bytes(case['key']), case['partition_count']) == case['partition']


@pytest.mark.parametrize('case', FIXTURE['log_cases'], ids=lambda c: c['name'])
def test_log_record_is_produced_byte_for_byte(case):
    wire = log_record(_ref(case['ref']), case['kind'], _log_body(case))
    assert wire.key == _bytes(case['key'])
    assert wire.value == _bytes(case['value'])
    assert wire.headers == _headers(case['headers'])
    assert wire.partition == case['partition']


@pytest.mark.parametrize('case', FIXTURE['log_cases'], ids=lambda c: c['name'])
def test_log_record_is_read_back(case):
    entry = parse_log_record(_headers(case['headers']), _bytes(case['value']))
    assert entry is not None
    assert entry.ref == _ref(case['ref'])
    assert entry.kind == case['kind']
    assert entry.body == json.loads(case['value'])


@pytest.mark.parametrize('case', FIXTURE['part_cases'], ids=lambda c: c['name'])
def test_part_record_is_produced_byte_for_byte(case):
    facts = case['input']
    wire = part_record(
        _ref(case['ref']),
        source_topic=facts['source_topic'],
        index=facts['index'],
        request_id=facts['request_id'],
        key=_bytes(facts['key']),
        value=_bytes(facts['value']),
        source_headers=_headers(facts['source_headers']),
        partition_count=facts['partition_count'],
        origin=facts.get('origin', 'kafka'),
        deliver_sinks=facts.get('deliver_sinks', True),
    )
    assert wire.key == _bytes(case['key'])
    assert wire.value == _bytes(facts['value'])
    assert wire.headers == _headers(case['headers'])
    assert wire.partition == case['partition']


@pytest.mark.parametrize('case', FIXTURE['part_cases'], ids=lambda c: c['name'])
def test_part_headers_are_read_back(case):
    part = parse_part_headers(_headers(case['headers']))
    assert part is not None
    # Cases of Kafka jobs omit the switch, which is always true for them.
    assert part.model_dump() == {'deliver_sinks': True, **case['parsed']}


@pytest.mark.parametrize('case', FIXTURE['part_parse_cases'], ids=lambda c: c['name'])
def test_part_headers_written_by_another_release_are_read_as_the_fixture_says(case):
    part = parse_part_headers(_headers(case['headers']))
    assert part is not None
    assert part.model_dump() == case['parsed']
