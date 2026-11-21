"""When a restored split job may be closed as orphaned: only when the source no longer gives its message.

A message that the source gave to the processor can still miss its split:
it fails to parse, the partition loop dies in the middle of its window, or a
stop drops it. Its job must stay open for the next holder of the message.
"""

from __future__ import annotations

import pytest
from confluent_kafka import KafkaError
from structlog.testing import capture_logs

from drakkar.metrics import split_jobs
from drakkar.split.models import PartResult
from drakkar.split.records import result_body
from tests.split_owner_helpers import (
    Item,
    OwnerWorker,
    SplittingHandler,
    World,
    items,
    owner_config,
    source_message,
    summary,
    wait_until,
)

FIRST = '1' * 16
NOT_JSON = b'not json'


class ParsingHandler(SplittingHandler):
    """A handler whose messages have a model, so a value that is no JSON fails to parse."""

    input_model = Item


def complete_job(world: World, offset: int = 500) -> None:
    """A job of one part in the results log, with its result: it ends as soon as an owner holds its message."""
    world.put_open(offset, FIRST)
    world.put(world.ref(offset, FIRST), 'split-done', {'parts': 1})
    world.put(world.ref(offset, FIRST), 'result', result_body(PartResult(index=0, worker='w', outcome='ok')))


def orphans_counted() -> float:
    return split_jobs.labels(outcome='orphaned')._value.get()


def closed_outcomes(world: World, offset: int = 500) -> list[str]:
    return [entry.body['outcome'] for entry in world.entries() if entry.kind == 'closed' and entry.ref.offset == offset]


async def test_a_job_behind_a_message_that_kills_the_loop_is_not_orphaned_and_ends_with_its_hook_after_a_restart():
    world = World()
    complete_job(world)
    counted = orphans_counted()

    # The window holds 499 and 500. The parse of 499 raises, so the loop dies before 500 is begun.
    handler = ParsingHandler({501: items(1)}, answer=None)
    worker = OwnerWorker(world, handler, config=owner_config(on_parse_error='raise'))
    await worker.start(source_message(499, value=NOT_JSON), source_message(500))
    await wait_until(lambda: worker.pending == 2)
    # The loop was restarted once and takes the next message: a later offset is begun.
    worker.give(source_message(501))
    await wait_until(lambda: handler.split_calls == [501])
    await worker.caught_up()

    assert closed_outcomes(world) == []
    assert orphans_counted() == counted
    assert handler.jobs == [] and worker.commits == []
    # Both offsets of the dead window are tracked: no commit can pass them.
    assert worker.processor.offset_tracker.committable() is None
    await worker.crash()

    # The next owner gets the messages again and lets the bad one go to the DLQ: the job ends with its hook.
    second_handler = ParsingHandler(answer=summary())
    second = OwnerWorker(world, second_handler, config=owner_config(on_parse_error='dlq'))
    await second.start(source_message(499, value=NOT_JSON), source_message(500))
    await wait_until(lambda: second.committed == 501)

    assert [job.source_message.offset for job in second_handler.jobs] == [500]
    assert closed_outcomes(world) == ['aggregated']
    assert orphans_counted() == counted
    await second.stop()


@pytest.mark.parametrize('policy', ['dlq', 'skip'])
async def test_a_job_whose_own_message_fails_to_parse_is_not_orphaned(policy):
    world = World()
    complete_job(world)
    counted = orphans_counted()
    handler = ParsingHandler({501: items(1)}, answer=summary())
    worker = OwnerWorker(world, handler, config=owner_config(on_parse_error=policy))

    await worker.start(source_message(500, value=NOT_JSON), source_message(501))
    await wait_until(lambda: 501 in handler.split_calls)
    await worker.caught_up()

    assert 'orphaned' not in closed_outcomes(world)
    assert orphans_counted() == counted
    await worker.stop()


async def test_a_message_that_was_queued_before_the_owner_existed_is_not_orphaned():
    world = World()
    complete_job(world)
    worker = OwnerWorker(world, SplittingHandler(answer=summary()))
    await worker.restore()
    # The pause of the partition failed, so messages arrive while the restore runs.
    worker.give(source_message(500), source_message(501))

    worker.run()

    assert worker.owner is not None and worker.owner._restored_waiting == set()
    assert worker.processor.queue_size == 2
    await wait_until(lambda: worker.committed == 502)
    assert closed_outcomes(world) == ['aggregated']
    await worker.stop()


async def test_a_message_that_a_stop_dropped_from_the_queue_is_not_orphaned_by_a_later_begin():
    world = World()
    complete_job(world)
    worker = OwnerWorker(world, SplittingHandler(answer=summary()))
    await worker.restore()
    worker.run()
    owner = worker.owner
    assert owner is not None

    # The source gave 500, and the message never reached ``begin``.
    owner.note_delivered([500])
    await owner.begin(source_message(502))
    await worker.caught_up()

    assert 'orphaned' not in closed_outcomes(world)
    await worker.stop()


async def test_a_message_the_source_passed_is_still_orphaned_after_a_window_that_died():
    world = World()
    complete_job(world)
    handler = ParsingHandler(answer=None)
    worker = OwnerWorker(world, handler, config=owner_config(on_parse_error='raise'))

    # 499 kills the loop; the source never gives 500; 501 comes after the restart of the loop.
    await worker.start(source_message(499, value=NOT_JSON))
    await wait_until(lambda: worker.pending == 1)
    worker.give(source_message(501))
    await wait_until(lambda: closed_outcomes(world) == ['orphaned'])
    await worker.crash()


async def test_an_orphan_whose_closed_record_cannot_be_written_is_not_counted_and_not_logged():
    world = World()
    complete_job(world)
    counted = orphans_counted()
    worker = OwnerWorker(world, SplittingHandler(answer=summary()))
    worker.client.fail_next_deliveries(KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART))

    with capture_logs() as logs:
        await worker.start(source_message(502))
        await wait_until(lambda: any(line['event'] == 'split_closed_write_failed' for line in logs))
        await wait_until(lambda: worker.committed == 503)

    assert closed_outcomes(world) == []
    assert orphans_counted() == counted
    assert [line for line in logs if line['event'] == 'split_job_orphaned'] == []
    await worker.stop()
