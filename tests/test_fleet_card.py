"""Worker card schema 1: building, serializing, decoding and validating cards.

``tests/fixtures/fleet_card.json`` pins the card bytes, the rejection
reasons and their order, and the card file names for any implementation of
the card format.
"""

import json
from datetime import datetime
from pathlib import Path

import pytest

from drakkar.fleet.card import (
    CARD_SCHEMA,
    MAX_CARD_BYTES,
    WorkerIdentity,
    build_card,
    decode_card,
    parse_card,
    sanitize_worker_name,
    serialize_card,
    validate_card,
)

FIXTURE = json.loads((Path(__file__).parent / 'fixtures' / 'fleet_card.json').read_text(encoding='utf-8'))

STRUCTURAL_REASONS = {'too_large', 'not_json', 'not_object'}


def _ids(cases: list[dict]) -> list[str]:
    return [case['name'] for case in cases]


def _identity_from_fixture() -> WorkerIdentity:
    inputs = FIXTURE['inputs']
    return WorkerIdentity(
        worker=inputs['worker'],
        instance=inputs['instance'],
        cluster=inputs['cluster'],
        url=inputs['url'],
        backend=inputs['backend'],
        version=inputs['version'],
        started_at=datetime.fromisoformat(inputs['started_at']),
    )


def test_build_card_matches_the_fixture_card():
    card = build_card(
        _identity_from_fixture(),
        state=FIXTURE['inputs']['state'],
        heartbeat_at=datetime.fromisoformat(FIXTURE['inputs']['heartbeat_at']),
    )

    assert card == FIXTURE['card']
    assert card['schema'] == CARD_SCHEMA


def test_build_card_copies_the_features_mapping():
    features = {'cache': {'cluster': {'ms': 1, 'key': 'k'}}}

    card = build_card(_identity_from_fixture(), state='running', heartbeat_at=datetime(2026, 1, 1), features=features)
    features['cache'] = {}

    assert card['features'] == {'cache': {'cluster': {'ms': 1, 'key': 'k'}}}


def test_serialize_card_produces_the_fixture_bytes():
    assert serialize_card(FIXTURE['card']) == FIXTURE['bytes'].encode('utf-8')


@pytest.mark.parametrize('case', FIXTURE['byte_cases'], ids=_ids(FIXTURE['byte_cases']))
def test_serialize_card_matches_the_byte_cases(case):
    assert serialize_card(case['card']) == case['bytes'].encode('utf-8')


def test_serialized_card_has_no_trailing_newline():
    assert not serialize_card(FIXTURE['card']).endswith(b'\n')


@pytest.mark.parametrize('case', FIXTURE['valid'], ids=_ids(FIXTURE['valid']))
def test_parse_card_accepts_a_valid_card(case):
    card, reason = parse_card(case['raw'].encode('utf-8'))

    assert reason == ''
    assert card == json.loads(case['raw'])


@pytest.mark.parametrize('case', FIXTURE['invalid'], ids=_ids(FIXTURE['invalid']))
def test_parse_card_rejects_an_invalid_card_with_the_contract_reason(case):
    card, reason = parse_card(case['raw'].encode('utf-8'))

    assert card is None
    assert reason == case['reason']


@pytest.mark.parametrize('case', FIXTURE['invalid'], ids=_ids(FIXTURE['invalid']))
def test_decode_card_rejects_only_structural_problems(case):
    """The provider-side decode stops at size, JSON and object; field rules are validate_card's."""
    card, reason = decode_card(case['raw'].encode('utf-8'))

    if case['reason'] in STRUCTURAL_REASONS:
        assert (card, reason) == (None, case['reason'])
    else:
        assert reason == ''
        assert validate_card(card) == case['reason']


def test_decode_card_rejects_invalid_utf8_as_not_json():
    assert decode_card(b'{"schema":1,"worker":"\xff"}') == (None, 'not_json')


def test_max_card_bytes_is_the_contract_limit():
    assert MAX_CARD_BYTES == 4096


@pytest.mark.parametrize('case', FIXTURE['sanitize'], ids=_ids(FIXTURE['sanitize']))
def test_sanitize_worker_name_matches_the_fixture(case):
    assert sanitize_worker_name(case['name']) == case['expected']
