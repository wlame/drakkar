"""The detach contract fixture: the closed sets, the card value of ``features.kafka`` and the slot scopes."""

import json
from pathlib import Path

import pytest

from drakkar import detach as detach_module
from drakkar.config import KafkaDetachConfig
from drakkar.detach import (
    ACTIONS,
    ATTACH_CAUSES,
    ATTACH_REFUSALS,
    DETACH_REFUSALS,
    REFUSED_PROBE,
    STATES,
    DetachOrigin,
)
from drakkar.detach_group import kafka_card_feature
from drakkar.detach_slots import slot_scope

FIXTURES = Path(__file__).parent / 'fixtures'
CONTRACT = json.loads((FIXTURES / 'detach_contract.json').read_text(encoding='utf-8'))
CARD_CASES = {case['name']: case for case in json.loads((FIXTURES / 'fleet_card.json').read_text())['byte_cases']}


def test_states_match_the_fixture():
    assert list(STATES) == CONTRACT['states']


def test_origins_match_the_fixture():
    assert [str(origin) for origin in DetachOrigin] == CONTRACT['origins']


def test_actions_match_the_fixture():
    assert list(ACTIONS) == CONTRACT['actions']


def test_detach_answers_match_the_fixture_in_check_order():
    assert list(DETACH_REFUSALS) == CONTRACT['detach_answers']
    assert CONTRACT['handler_only_answers'] == [REFUSED_PROBE]


def test_attach_answers_match_the_fixture():
    assert CONTRACT['attach_answers'] == ['', *ATTACH_REFUSALS]


def test_attach_causes_match_the_fixture():
    assert list(ATTACH_CAUSES) == CONTRACT['attach_causes']


def test_every_refusal_of_the_module_is_in_the_fixture():
    refusals = {value for name, value in vars(detach_module).items() if name.startswith('REFUSED_')}
    listed = set(CONTRACT['detach_answers']) | set(CONTRACT['attach_answers']) | set(CONTRACT['handler_only_answers'])

    assert refusals == listed - {''}


def test_every_cause_of_the_module_is_in_the_fixture():
    causes = {value for name, value in vars(detach_module).items() if name.startswith('CAUSE_')}

    assert causes == set(CONTRACT['attach_causes'])


@pytest.mark.parametrize('case', CONTRACT['slot_scopes'], ids=lambda case: case['name'])
def test_slot_scope_matches_the_fixture(case):
    assert slot_scope(case['cluster'], case['consumer_group'], case['index']) == case['scope']


@pytest.mark.parametrize(
    ('name', 'enabled'), [('kafka_feature_detach_off', False), ('kafka_feature_detach_on', True)], ids=['off', 'on']
)
def test_the_card_feature_matches_the_card_fixture(name, enabled):
    expected = CARD_CASES[name]['card']['features']['kafka']
    detach = KafkaDetachConfig(
        enabled=enabled,
        max_detached=expected.get('max_detached', 1),
        min_attached=expected.get('min_attached', 1),
    )

    feature = kafka_card_feature(consumer_group=expected['group'], detached=expected['detached'], detach=detach)

    assert feature == expected
