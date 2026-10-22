"""Slot computation, scopes and definitions, beyond the shared fixture cases."""

from __future__ import annotations

import pytest

from drakkar.cronexpr import CronNeverFiresError
from drakkar.recorder import history
from drakkar.schedule import (
    DEFAULT_CLUSTER,
    SCOPE_INVALID,
    ClusterTaskDefinition,
    cluster_tasks_hash,
    next_cron_slot,
    next_interval_slot,
    parse_task_scope,
    sanitize_cluster,
    task_scope,
)


def test_next_interval_slot_ignores_the_fraction_of_a_second():
    assert next_interval_slot(59.999, 30) == 60


def test_next_interval_slot_on_a_boundary_is_the_next_one():
    assert next_interval_slot(60.0, 30) == 90


def test_next_cron_slot_ignores_the_fraction_of_a_second():
    # 23:59:59.5 truncates to 23:59:59, so midnight is still ahead.
    assert next_cron_slot(86_399.5, '0 0 * * *') == 86_400


def test_next_cron_slot_raises_for_an_expression_that_never_fires():
    with pytest.raises(CronNeverFiresError):
        next_cron_slot(0, '0 0 30 2 *')


def test_parse_task_scope_refuses_a_dotted_path_segment():
    for scope in ('main/..', 'main/../x', '../x', 'main/.x'):
        with pytest.raises(ValueError) as caught:
            parse_task_scope(scope)
        assert str(caught.value) == SCOPE_INVALID.format(scope=scope)


def test_task_scope_parses_back_to_the_sanitized_cluster():
    assert parse_task_scope(task_scope('main cluster', 'cache.sync')) == ('main_cluster', 'cache.sync')


def test_history_keeps_using_the_same_cluster_sanitizer():
    assert history.sanitize_cluster is sanitize_cluster
    assert sanitize_cluster(None) == DEFAULT_CLUSTER


@pytest.mark.parametrize(('interval', 'cron'), [(None, None), (60, '* * * * *')])
def test_cluster_task_definition_needs_exactly_one_schedule(interval, cron):
    with pytest.raises(ValueError, match='exactly one of interval and cron'):
        ClusterTaskDefinition(name='t', interval=interval, cron=cron, lease_seconds=60)


@pytest.mark.parametrize(
    ('interval', 'lease_seconds', 'key'),
    [(600.0, 60, 'interval'), (True, 60, 'interval'), (600, 60.0, 'lease_seconds')],
)
def test_cluster_task_definition_refuses_a_number_that_is_not_an_int(interval, lease_seconds, key):
    with pytest.raises(TypeError, match=f'{key} must be an int'):
        ClusterTaskDefinition(name='t', interval=interval, cron=None, lease_seconds=lease_seconds)


def test_cluster_tasks_hash_ignores_the_order_of_the_definitions():
    a = ClusterTaskDefinition(name='a', interval=30, cron=None, lease_seconds=40)
    b = ClusterTaskDefinition(name='b', interval=None, cron='0 * * * *', lease_seconds=60)

    assert cluster_tasks_hash([a, b]) == cluster_tasks_hash([b, a])
