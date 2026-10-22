"""Unit tests for the integration producer's phase plan and repeat scale.

Pure functions only: no Kafka, no network. Not part of the main ``tests/``
suite; they pin the harness producer's own behaviour so any implementation
of it stays provably identical.
"""

import random

import pytest
from producer import make_repeat, phase_plan, repeat_ranges


def test_phase_plan_5000_keeps_todays_sizes():
    assert [p.size for p in phase_plan(5000)] == [10, 500, 200, 2000, 300, 1990]


def test_phase_plan_keeps_todays_rates_and_pauses():
    plan = phase_plan(5000)
    assert [p.rate for p in plan] == [0.2, 15, None, 20, None, 10]
    assert [p.pause_seconds for p in plan] == [0, 15, 0, 10, 0, 0]


def test_phase_plan_1000_keeps_every_phase():
    plan = phase_plan(1000)
    assert [p.size for p in plan] == [10, 100, 40, 400, 60, 390]
    assert all(p.size > 0 for p in plan)
    assert sum(p.size for p in plan) == 1000


@pytest.mark.parametrize('total', [1, 5, 10, 11, 37, 1000, 5000, 12345])
def test_phase_plan_sends_each_id_once_before_the_flood(total):
    plan = phase_plan(total)
    assert sum(p.size for p in plan) == total
    assert all(p.size > 0 for p in plan)


def test_phase_plan_flood_resends_total_ids():
    # The flood is a step of main() after the plan: it re-sends the whole id
    # range, so its size is the total, not the sum of anything left over.
    total = 1000
    ids = [f'req-{n:06d}' for n in range(1, total + 1)]
    taken = []
    for phase in phase_plan(total):
        taken.extend(ids[len(taken) : len(taken) + phase.size])
    assert taken == ids
    assert len(ids) == total


def test_repeat_ranges_scale_both_ranges_with_a_floor_of_one():
    assert repeat_ranges(1.0) == ((1, 15), (150, 250))
    assert repeat_ranges(0.2) == ((1, 3), (30, 50))
    assert repeat_ranges(0.01) == ((1, 1), (2, 2))


@pytest.mark.parametrize('scale', [0, -1, -0.5])
def test_repeat_scale_must_be_positive(scale):
    with pytest.raises(ValueError):
        repeat_ranges(scale)
    with pytest.raises(ValueError):
        make_repeat(scale)


def test_make_repeat_stays_inside_the_scaled_ranges():
    random.seed(7)
    values = {make_repeat(0.2) for _ in range(2000)}
    assert values <= set(range(1, 4)) | set(range(30, 51))
    assert values & set(range(30, 51))
    assert values & set(range(1, 4))
