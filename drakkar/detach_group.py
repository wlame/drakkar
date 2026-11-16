"""The consumer group of this worker as the registry shows it, and the card key ``features.kafka``.

Each worker with the Kafka source on publishes ``features.kafka`` in its
registry card: its consumer group, whether it is detached now, and, when the
detach feature is on, its ``max_detached`` and ``min_attached``. The functions
here read the peer cards of the last registry refresh. A card comes from
another process, so every key is checked for its type before it is used, and
a card of any other shape is not counted; nothing here raises on a card.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import Any

from drakkar.config import KafkaDetachConfig
from drakkar.fleet.service import PeerView


@dataclass(frozen=True)
class GroupView:
    """The size of a consumer group as the registry shows it.

    ``size`` counts the online workers of the group, this worker included.
    ``attached_peers`` counts the other online workers of the group that
    publish ``detached`` exactly ``False``; a peer with any other value is in
    ``size`` but not here, so an unclear peer never counts as attached.
    """

    size: int
    attached_peers: int


def _group_features(views: Sequence[PeerView], *, cluster: str, consumer_group: str) -> Iterator[dict[str, Any]]:
    """The ``features.kafka`` mapping of each online peer of the own cluster that publishes ``consumer_group``."""
    for view in views:
        if view.online is not True:
            continue
        card = view.card
        if not isinstance(card, dict) or card.get('cluster') != cluster:
            continue
        features = card.get('features')
        if not isinstance(features, dict):
            continue
        kafka = features.get('kafka')
        if not isinstance(kafka, dict):
            continue
        group = kafka.get('group')
        if isinstance(group, str) and group == consumer_group:
            yield kafka


def group_view(views: Sequence[PeerView], *, cluster: str, consumer_group: str) -> GroupView:
    """Count the online workers of ``consumer_group`` in ``cluster``, this worker included.

    ``views`` are the peer views of the registry, which leave out this
    worker's own card; that is why ``size`` adds one.
    """
    size = 1
    attached_peers = 0
    for kafka in _group_features(views, cluster=cluster, consumer_group=consumer_group):
        size += 1
        # `is False`: a 0, a text or a missing value is not a clear "attached".
        if kafka.get('detached') is False:
            attached_peers += 1
    return GroupView(size=size, attached_peers=attached_peers)


def kafka_card_feature(*, consumer_group: str, detached: bool, detach: KafkaDetachConfig) -> dict[str, Any]:
    """The card value of ``features.kafka``; the two limits only when the detach feature is on."""
    feature: dict[str, Any] = {'group': consumer_group, 'detached': detached}
    if detach.enabled:
        feature['max_detached'] = detach.max_detached
        feature['min_attached'] = detach.min_attached
    return feature


def _whole_number(value: Any) -> int | None:
    """``value`` when it is an integer and not a boolean, else ``None``."""
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    return None


def mismatched_peers(
    views: Sequence[PeerView],
    *,
    cluster: str,
    consumer_group: str,
    max_detached: int,
    min_attached: int,
) -> list[tuple[str, int, int]]:
    """The online peers of the group whose ``max_detached`` or ``min_attached`` differ from this worker's.

    Each item is ``(worker, peer max_detached, peer min_attached)``, sorted by
    worker. A peer that does not publish both values as integers is skipped:
    it either has the feature off or runs a release without it.
    """
    found: list[tuple[str, int, int]] = []
    for view in views:
        for kafka in _group_features([view], cluster=cluster, consumer_group=consumer_group):
            worker = view.card.get('worker')
            peer_max = _whole_number(kafka.get('max_detached'))
            peer_min = _whole_number(kafka.get('min_attached'))
            if not isinstance(worker, str) or peer_max is None or peer_min is None:
                continue
            if (peer_max, peer_min) != (max_detached, min_attached):
                found.append((worker, peer_max, peer_min))
    return sorted(found)
