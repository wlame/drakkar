"""Unit tests for the log of undecodable cards that the connection registries share."""

from unittest.mock import AsyncMock

from drakkar.providers.cardlog import InvalidCardLog


def make_logger() -> AsyncMock:
    """A logger whose async ``awarning`` records its calls, as structlog's async bound logger has one."""
    return AsyncMock()


async def test_report_logs_once_per_name():
    log = InvalidCardLog()
    logger = make_logger()

    await log.report(logger, 'w-1', 'not_json')
    await log.report(logger, 'w-1', 'not_json')
    await log.report(logger, 'w-2', 'not_json')

    assert logger.awarning.await_count == 2


async def test_cut_to_forgets_names_that_left_the_listing():
    log = InvalidCardLog()
    logger = make_logger()

    await log.report(logger, 'w-1', 'not_json')
    log.cut_to(['w-2'])
    await log.report(logger, 'w-1', 'not_json')

    assert logger.awarning.await_count == 2


async def test_cut_to_keeps_names_that_stay_in_the_listing():
    log = InvalidCardLog()
    logger = make_logger()

    await log.report(logger, 'w-1', 'not_json')
    log.cut_to(iter(['w-1', 'w-2']))
    await log.report(logger, 'w-1', 'not_json')

    assert logger.awarning.await_count == 1


async def test_report_carries_the_category_worker_and_reason():
    log = InvalidCardLog()
    logger = make_logger()

    await log.report(logger, 'w-1', 'bad_schema')

    logger.awarning.assert_awaited_once_with('fleet_card_invalid', category='fleet', worker='w-1', reason='bad_schema')
