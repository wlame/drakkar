"""Split mode as the UI sees it: capabilities, the worker row, and the labels stamped on tasks."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from drakkar.config import DrakkarConfig
from drakkar.fleet.service import PeerView
from drakkar.models import ExecutorTask, SourceMessage, SplitPart
from drakkar.partition import MessageTracker
from drakkar.split.records import (
    PARTS_PARTITION_BASE,
    SPLIT_JOB_LABEL,
    SPLIT_PART_LABEL,
    SPLIT_PART_LOCAL,
)
from drakkar.uiserver.routes_pages import peer_worker_row
from drakkar.uiserver.server_helpers import capabilities_of
from tests.test_split_part_mode import PartPipeline, one_task_handler, part_message


def _config() -> DrakkarConfig:
    return DrakkarConfig(
        sources={'kafka': {'enabled': True}},
        connections={'kafka': {'main': {}}},
        ui={'release': {'enabled': False}},
    )


def test_capabilities_split_enabled_names_topics_and_base():
    runtime = SimpleNamespace(parts_topic='t_g_split_parts', results_topic='t_g_split_results')

    caps = capabilities_of(_config(), None, split=runtime)  # ty: ignore[invalid-argument-type]

    assert caps['split'] == {
        'enabled': True,
        'parts_topic': 't_g_split_parts',
        'results_topic': 't_g_split_results',
        'parts_partition_base': 1000000,
    }


def test_capabilities_split_off_has_nulls():
    caps = capabilities_of(_config(), None, split=None)

    assert caps['split'] == {'enabled': False, 'parts_topic': None, 'results_topic': None, 'parts_partition_base': None}


def _peer(card_extra: dict) -> PeerView:
    card = {'schema': 1, 'worker': 'w-2', 'cluster': 'search', 'url': 'http://w-2:8080/', **card_extra}
    return PeerView(card=card, age_seconds=1.0, online=True, last_seen_ts=1_760_000_000.0)


@pytest.mark.parametrize(
    ('card_extra', 'expected'),
    [
        ({'features': {'split': {'parts_topic': 'p', 'results_topic': 'r'}}}, True),
        ({'features': {'ui': True}}, False),
        ({}, None),
    ],
)
def test_peer_worker_row_split_true_false_null(card_extra, expected):
    row = peer_worker_row(_peer(card_extra), own_cluster='search', own_hash='0123456789abcdef')

    assert row['split'] is expected


def _tracker_of(message: SourceMessage) -> MessageTracker:
    return MessageTracker(source_message=message)


def _stamp(proc, message: SourceMessage, task: ExecutorTask) -> None:
    proc._message_trackers[message.offset] = _tracker_of(message)
    task.source_offsets = [message.offset]
    proc._stamp_request_ids([task])


def _task(**labels: str) -> ExecutorTask:
    return ExecutorTask(task_id='t-1', args=[], source_offsets=[0], labels=dict(labels))


def test_part_task_gets_split_labels():
    pipeline = PartPipeline(one_task_handler())
    message = part_message(offset=9, index=3)
    message.split = _part_of(message)
    task = _task()

    _stamp(pipeline.processor, message, task)

    assert task.labels[SPLIT_JOB_LABEL] == '7:500'
    assert task.labels[SPLIT_PART_LABEL] == '3'


def _part_of(message: SourceMessage):
    return SplitPart(
        group='workers',
        source_topic='requests',
        source_partition=7,
        source_offset=500,
        attempt=1,
        attempt_id='0123456789abcdef',
        index=3,
    )


def test_local_task_gets_local_label():
    pipeline = PartPipeline(one_task_handler())
    proc = pipeline.processor
    proc._owner = object()  # ty: ignore[invalid-assignment]
    message = SourceMessage(topic='requests', partition=2, offset=40, value=b'{}', timestamp=0)
    task = _task()

    _stamp(proc, message, task)

    assert task.labels[SPLIT_JOB_LABEL] == '2:40'
    assert task.labels[SPLIT_PART_LABEL] == SPLIT_PART_LOCAL


def test_handler_split_labels_are_kept():
    pipeline = PartPipeline(one_task_handler())
    message = part_message(offset=9)
    message.split = _part_of(message)
    task = _task(**{SPLIT_JOB_LABEL: 'mine'})

    _stamp(pipeline.processor, message, task)

    assert task.labels[SPLIT_JOB_LABEL] == 'mine'
    assert task.labels[SPLIT_PART_LABEL] == '3'


def test_no_split_labels_without_split():
    pipeline = PartPipeline(one_task_handler())
    proc = pipeline.processor
    proc._part_mode = None
    message = SourceMessage(topic='requests', partition=2, offset=40, value=b'{}', timestamp=0)
    task = _task()

    _stamp(proc, message, task)

    assert SPLIT_JOB_LABEL not in task.labels
    assert SPLIT_PART_LABEL not in task.labels


def test_parts_partition_base_is_exposed():
    assert PARTS_PARTITION_BASE == 1000000


def test_card_features_name_the_split_topics_only_with_a_runtime():
    from drakkar.app import DrakkarApp
    from tests.test_agent_routes import KafkaOnlyHandler

    app = DrakkarApp(handler=KafkaOnlyHandler(), config=_config(), worker_id='w-1')
    assert 'split' not in app._lifecycle._card_features()

    app._split = SimpleNamespace(parts_topic='p', results_topic='r')  # ty: ignore[invalid-assignment]

    assert app._lifecycle._card_features()['split'] == {'parts_topic': 'p', 'results_topic': 'r'}
