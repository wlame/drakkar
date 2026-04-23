"""Tests for drakkar.utils module."""

import pytest

from drakkar.utils import redact_url, wait_for_aligned_startup


def test_redact_url_with_credentials():
    assert redact_url('mongodb://admin:secret@host:27017/db') == 'mongodb://***:***@host:27017/db'


def test_redact_url_without_credentials():
    url = 'redis://localhost:6379/0'
    assert redact_url(url) == url


def test_redact_url_empty_string():
    assert redact_url('') == ''


def test_redact_url_plain_string_without_scheme():
    plain = 'localhost:9092'
    assert redact_url(plain) == plain


class _FakeClock:
    """Monotonically-advancing clock stub; ``clock()`` returns the stored
    time, ``sleep()`` appends to ``sleeps`` and advances the stored time.
    Lets tests verify alignment math without actually waiting."""

    def __init__(self, start: float) -> None:
        self._now = start
        self.sleeps: list[float] = []

    def clock(self) -> float:
        return self._now

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self._now += seconds


@pytest.mark.asyncio
async def test_wait_for_aligned_startup_sleeps_min_then_to_next_boundary():
    """Wakes at :03 → sleeps 4s to :07 → aligns to next :10 → total sleep 7s."""
    fake = _FakeClock(start=1_700_000_003.0)
    slept = await wait_for_aligned_startup(
        min_wait_seconds=4.0,
        align_interval_seconds=10,
        _clock=fake.clock,
        _sleep=fake.sleep,
    )
    assert fake.sleeps == [4.0, 3.0]
    assert slept == pytest.approx(7.0)


@pytest.mark.asyncio
async def test_wait_for_aligned_startup_lands_on_boundary_no_extra_wait():
    """Min wait lands EXACTLY on a boundary → no extra sleep."""
    fake = _FakeClock(start=1_700_000_006.0)
    slept = await wait_for_aligned_startup(
        min_wait_seconds=4.0,
        align_interval_seconds=10,
        _clock=fake.clock,
        _sleep=fake.sleep,
    )
    assert fake.sleeps == [4.0]
    assert slept == pytest.approx(4.0)


@pytest.mark.asyncio
async def test_wait_for_aligned_startup_zero_min_still_aligns():
    """Zero min wait still advances to the next boundary."""
    fake = _FakeClock(start=1_700_000_003.5)
    slept = await wait_for_aligned_startup(
        min_wait_seconds=0.0,
        align_interval_seconds=10,
        _clock=fake.clock,
        _sleep=fake.sleep,
    )
    assert fake.sleeps == [pytest.approx(6.5)]
    assert slept == pytest.approx(6.5)


@pytest.mark.asyncio
async def test_wait_for_aligned_startup_custom_interval():
    """A 30-second interval aligns on multiples of 30 epoch seconds.
    1_699_999_980 = 30 * 56_666_666 is on a 30-boundary; +4s min lands at
    ...984; next multiple of 30 is 1_700_000_010 (a.k.a. 30 * 56_666_667),
    which is 26s away — total sleep 30s."""
    fake = _FakeClock(start=1_699_999_980.0)
    slept = await wait_for_aligned_startup(
        min_wait_seconds=4.0,
        align_interval_seconds=30,
        _clock=fake.clock,
        _sleep=fake.sleep,
    )
    assert fake.sleeps == [4.0, 26.0]
    assert slept == pytest.approx(30.0)


@pytest.mark.asyncio
async def test_wait_for_aligned_startup_uses_real_clock_when_no_injection():
    """Smoke check: no injected hooks → uses real time.time + asyncio.sleep.
    Uses 0s min and 1s interval so the test completes in under 1s."""
    import time

    t0 = time.monotonic()
    slept = await wait_for_aligned_startup(min_wait_seconds=0.0, align_interval_seconds=1)
    elapsed = time.monotonic() - t0
    assert 0 <= elapsed <= 1.1
    assert 0 <= slept <= 1.1
