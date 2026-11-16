"""The consumer group as the registry shows it, and the card key ``features.kafka``."""

from __future__ import annotations

from typing import Any

import pytest

from drakkar.config import KafkaDetachConfig
from drakkar.detach_group import GroupView, group_view, kafka_card_feature, mismatched_peers
from drakkar.fleet.service import PeerView

CLUSTER = 'search'
GROUP = 'matcher'


def _view(
    worker: str = 'w-2',
    *,
    cluster: Any = CLUSTER,
    online: bool | None = True,
    kafka: Any = None,
    features: Any = None,
) -> PeerView:
    card: dict[str, Any] = {'schema': 1, 'worker': worker, 'cluster': cluster, 'url': f'http://{worker}:8080/'}
    if features is not None:
        card['features'] = features
    elif kafka is not None:
        card['features'] = {'kafka': kafka}
    return PeerView(card=card, age_seconds=1.0, online=online, last_seen_ts=None)


def _kafka(group: Any = GROUP, detached: Any = False, **extra: Any) -> dict[str, Any]:
    return {'group': group, 'detached': detached, **extra}


@pytest.mark.parametrize(
    ('view', 'expected'),
    [
        pytest.param(_view(kafka=_kafka()), GroupView(size=2, attached_peers=1), id='same_group_online_attached'),
        pytest.param(
            _view(kafka=_kafka(detached=True)), GroupView(size=2, attached_peers=0), id='same_group_online_detached'
        ),
        pytest.param(_view(online=False, kafka=_kafka()), GroupView(size=1, attached_peers=0), id='offline'),
        pytest.param(_view(online=None, kafka=_kafka()), GroupView(size=1, attached_peers=0), id='online_unknown'),
        pytest.param(_view(cluster='other', kafka=_kafka()), GroupView(size=1, attached_peers=0), id='other_cluster'),
        pytest.param(_view(kafka=_kafka(group='other')), GroupView(size=1, attached_peers=0), id='other_group'),
        pytest.param(_view(features={'ui': True}), GroupView(size=1, attached_peers=0), id='no_kafka_feature'),
        pytest.param(_view(), GroupView(size=1, attached_peers=0), id='no_features'),
        pytest.param(_view(features=['kafka']), GroupView(size=1, attached_peers=0), id='features_not_a_dict'),
        pytest.param(_view(features={'kafka': 'matcher'}), GroupView(size=1, attached_peers=0), id='kafka_not_a_dict'),
        pytest.param(_view(kafka=_kafka(group=7)), GroupView(size=1, attached_peers=0), id='group_not_a_string'),
        pytest.param(
            _view(kafka=_kafka(detached='false')), GroupView(size=2, attached_peers=0), id='detached_not_a_bool'
        ),
        pytest.param(_view(kafka={'group': GROUP}), GroupView(size=2, attached_peers=0), id='detached_missing'),
        pytest.param(_view(kafka=_kafka(detached=0)), GroupView(size=2, attached_peers=0), id='detached_zero'),
    ],
)
def test_group_view_counts_one_peer(view, expected):
    assert group_view([view], cluster=CLUSTER, consumer_group=GROUP) == expected


def test_group_view_with_no_peers_is_this_worker_alone():
    assert group_view([], cluster=CLUSTER, consumer_group=GROUP) == GroupView(size=1, attached_peers=0)


def test_group_view_adds_up_a_mixed_group():
    views = [
        _view('w-2', kafka=_kafka()),
        _view('w-3', kafka=_kafka()),
        _view('w-4', kafka=_kafka(detached=True)),
        _view('w-5', online=False, kafka=_kafka()),
        _view('w-6', kafka=_kafka(group='other')),
    ]

    assert group_view(views, cluster=CLUSTER, consumer_group=GROUP) == GroupView(size=4, attached_peers=2)


def test_group_view_skips_a_card_that_is_not_a_dict():
    view = PeerView(card='w-2', age_seconds=None, online=True, last_seen_ts=None)  # type: ignore[arg-type]

    assert group_view([view], cluster=CLUSTER, consumer_group=GROUP) == GroupView(size=1, attached_peers=0)


def test_kafka_card_feature_with_detach_on_carries_the_limits():
    detach = KafkaDetachConfig(enabled=True, max_detached=2, min_attached=3)

    feature = kafka_card_feature(consumer_group=GROUP, detached=True, detach=detach)

    assert feature == {'group': GROUP, 'detached': True, 'max_detached': 2, 'min_attached': 3}


def test_kafka_card_feature_with_detach_off_has_only_the_group_and_the_state():
    detach = KafkaDetachConfig(enabled=False, max_detached=2, min_attached=3)

    feature = kafka_card_feature(consumer_group=GROUP, detached=False, detach=detach)

    assert feature == {'group': GROUP, 'detached': False}


def test_mismatched_peers_lists_group_peers_whose_limits_differ():
    views = [
        _view('w-2', kafka=_kafka(max_detached=1, min_attached=1)),
        _view('w-3', kafka=_kafka(max_detached=2, min_attached=1)),
        _view('w-4', kafka=_kafka(max_detached=1, min_attached=5)),
        _view('w-5', kafka=_kafka()),
        _view('w-6', kafka=_kafka(max_detached=1)),
        _view('w-7', kafka=_kafka(max_detached=True, min_attached=1)),
        _view('w-8', kafka=_kafka(group='other', max_detached=9, min_attached=9)),
        _view('w-9', online=False, kafka=_kafka(max_detached=9, min_attached=9)),
    ]

    found = mismatched_peers(views, cluster=CLUSTER, consumer_group=GROUP, max_detached=1, min_attached=1)

    assert found == [('w-3', 2, 1), ('w-4', 1, 5)]


def test_mismatched_peers_is_empty_when_every_peer_agrees():
    views = [_view('w-2', kafka=_kafka(max_detached=1, min_attached=2))]

    assert mismatched_peers(views, cluster=CLUSTER, consumer_group=GROUP, max_detached=1, min_attached=2) == []
