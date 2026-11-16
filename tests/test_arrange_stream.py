"""The partition loop with a stream handler: groups, their completion and the message end."""

import asyncio
import gc
import weakref
from unittest.mock import AsyncMock, MagicMock

import pytest
from structlog.testing import capture_logs

from drakkar.budget import PendingBudget
from drakkar.groups import ArrangeStreamError, check_stream_item
from drakkar.handler import BaseDrakkarHandler
from drakkar.models import (
    CollectResult,
    ErrorAction,
    ExecutorTask,
    KafkaPayload,
    SinkDeliveryFailedError,
)
from drakkar.partition import FANOUT_CHUNK_TASKS, MessageTracker, PartitionProcessor, Window
from drakkar.sinks.manager import DeliveryOutcome
from tests.conftest import wait_for
from tests.stream_helpers import (
    Out,
    RecordingCollect,
    RecordingStreamHandler,
    WithGroupHook,
    make_msg,
    make_pool,
    run_until_settled,
    stream_processor,
    stream_task,
)
from tests.test_sources_kafka import _source_with_fake_consumer


async def test_stream_tasks_start_before_the_generator_ends():
    gate = asyncio.Event()

    class Gated(RecordingStreamHandler):
        async def arrange_stream(self, message):
            yield self.make_group(message, 0, 2)
            await gate.wait()
            self.log.append('pull:1')
            yield self.make_group(message, 1, 1)

    handler = Gated()
    proc = stream_processor(handler)
    proc.enqueue(make_msg(0))
    proc.start()
    await wait_for(lambda: 'task:m0-g0-t0' in handler.log and 'task:m0-g0-t1' in handler.log)
    assert 'pull:1' not in handler.log
    gate.set()
    await wait_for(lambda: not proc.offset_tracker.has_pending())
    await proc.stop()
    assert handler.log.index('task:m0-g0-t1') < handler.log.index('pull:1') < handler.log.index('task:m0-g1-t0')


async def test_each_yielded_list_is_one_group_with_index_count_and_labels():
    class Labelled(WithGroupHook):
        group_labels = ('kind', 'size')

        def make_group(self, message, index, size):
            return [
                stream_task(f'g{index}-t{position}', message.offset, labels={'kind': f'k{index}', 'size': '7'})
                for position in range(size)
            ]

    handler = Labelled([2, 1, 3])
    proc = stream_processor(handler)
    proc.enqueue(make_msg(0))
    await run_until_settled(proc)

    groups = [call[0] for call in handler.group_calls]
    assert sorted(group.index for group in groups) == [0, 1, 2]
    by_index = {group.index: group for group in groups}
    assert [by_index[i].task_count for i in range(3)] == [2, 1, 3]
    assert by_index[0].labels == {'kind': ['k0', 'k0'], 'size': ['7', '7']}
    assert by_index[2].labels == {'kind': ['k2'] * 3, 'size': ['7'] * 3}


async def test_group_hook_gets_results_and_errors_and_the_group_keeps_none():
    refs: list[weakref.ref] = []

    class Keeper(WithGroupHook):
        async def on_group_complete(self, group, results, errors):
            refs.extend(weakref.ref(result) for result in results)
            return await super().on_group_complete(group, results, errors)

        async def on_error(self, task, error):
            return ErrorAction.SKIP

    handler = Keeper([3], exit_code=lambda task_id: 1 if task_id.endswith('t1') else 0)
    proc = stream_processor(handler)
    proc.enqueue(make_msg(0))
    await run_until_settled(proc)

    group, results, errors = handler.group_calls[0]
    assert sorted(result.task.task_id for result in results) == ['m0-g0-t0', 'm0-g0-t2']
    assert [error.task.task_id for error in errors] == ['m0-g0-t1']
    assert (group.succeeded, group.failed, group.replaced) == (2, 1, 0)
    assert 'results' not in group.model_dump() and 'errors' not in group.model_dump()
    handler.group_calls.clear()
    del results, errors
    gc.collect()
    assert refs and all(ref() is None for ref in refs)


async def test_group_hook_keeps_its_own_lists_after_the_hook_returns():
    class KeepsTheLists(WithGroupHook):
        async def on_group_complete(self, group, results, errors):
            # Kept as they are, not copied: the hook owns what it was given.
            self.kept = (results, errors)
            return await super().on_group_complete(group, results, errors)

        async def on_error(self, task, error):
            return ErrorAction.SKIP

    handler = KeepsTheLists([2], exit_code=lambda task_id: 1 if task_id.endswith('t1') else 0)
    proc = stream_processor(handler)
    proc.enqueue(make_msg(0))
    await run_until_settled(proc)

    results, errors = handler.kept
    assert [result.task.task_id for result in results] == ['m0-g0-t0']
    assert [error.task.task_id for error in errors] == ['m0-g0-t1']


async def test_group_results_are_freed_after_the_group_hook_while_the_message_is_open():
    refs: list[weakref.ref] = []
    gate = asyncio.Event()
    hook_ended = asyncio.Event()

    class HoldsTheStream(RecordingStreamHandler):
        async def arrange_stream(self, message):
            yield self.make_group(message, 0, 3)
            await gate.wait()
            yield self.make_group(message, 1, 1)

        async def on_group_complete(self, group, results, errors):
            refs.extend(weakref.ref(result) for result in results)
            if group.index == 0:
                hook_ended.set()
            return None

    handler = HoldsTheStream()
    proc = stream_processor(handler)
    proc.enqueue(make_msg(0))
    proc.start()
    await asyncio.wait_for(hook_ended.wait(), timeout=2)
    tracker = proc._message_trackers[0]
    await wait_for(lambda: tracker.open_groups == 0)
    gc.collect()
    assert tracker.stream_open
    assert len(refs) == 3 and all(ref() is None for ref in refs)
    gate.set()
    await wait_for(lambda: not proc.offset_tracker.has_pending())
    await proc.stop()


async def test_message_hook_fires_once_after_every_group_hook_and_delivery():
    class Noting(WithGroupHook):
        async def on_group_complete(self, group, results, errors):
            group.notes['index'] = group.index
            return await super().on_group_complete(group, results, errors)

        async def on_message_complete(self, group):
            await super().on_message_complete(group)
            return CollectResult(kafka=[KafkaPayload(data=Out(v='message'))])

    handler = Noting([2, 1, 3])
    proc = stream_processor(handler, on_collect=RecordingCollect(handler.log))
    proc.enqueue(make_msg(0))
    await run_until_settled(proc)

    deliveries = [entry for entry in handler.log if entry.startswith('deliver:group-')]
    assert len(deliveries) == 3
    message_at = handler.log.index('message:0')
    assert all(handler.log.index(entry) < message_at for entry in deliveries)
    assert handler.log.count('message:0') == 1
    assert handler.log[-1] == 'deliver:message'
    message_group = handler.message_groups[0]
    assert [group.index for group in message_group.groups] == [0, 1, 2]
    assert message_group.tasks == [] and message_group.results == [] and message_group.errors == []
    assert message_group.total == 6 and message_group.succeeded == 6
    assert [group.notes for group in message_group.groups] == [{'index': 0}, {'index': 1}, {'index': 2}]


async def test_offset_completes_only_after_the_stream_ends():
    gate = asyncio.Event()

    class Gated(RecordingStreamHandler):
        async def arrange_stream(self, message):
            yield self.make_group(message, 0, 2)
            await gate.wait()
            yield self.make_group(message, 1, 1)

    handler = Gated()
    proc = stream_processor(handler)
    proc.enqueue(make_msg(0))
    proc.start()
    await wait_for(lambda: handler.log.count('task:m0-g0-t0') == 1 and handler.log.count('task:m0-g0-t1') == 1)
    tracker = proc._message_trackers[0]
    # Every task settled and the only group finished: the stream alone keeps the message open.
    await wait_for(lambda: tracker.remaining == 0 and tracker.open_groups == 0)
    assert tracker.stream_open
    assert proc.offset_tracker.has_pending()
    assert handler.message_groups == []
    gate.set()
    await wait_for(lambda: not proc.offset_tracker.has_pending())
    await proc.stop()
    assert len(handler.message_groups) == 1


async def test_next_message_streams_while_the_last_group_of_the_previous_one_runs():
    release = asyncio.Event()

    class SlowLastTask(RecordingStreamHandler):
        async def on_task_complete(self, result):
            if result.task.task_id == 'm0-g0-t0':
                await release.wait()
            return await super().on_task_complete(result)

    handler = SlowLastTask([1])
    proc = stream_processor(handler)
    proc.enqueue(make_msg(0))
    proc.enqueue(make_msg(1))
    proc.start()
    await wait_for(lambda: 'task:m1-g0-t0' in handler.log)
    assert 'task:m0-g0-t0' not in handler.log
    assert proc.offset_tracker.has_pending()
    release.set()
    await wait_for(lambda: not proc.offset_tracker.has_pending())
    await proc.stop()
    assert [group.source_message.offset for group in handler.message_groups] == [1, 0]


async def test_empty_stream_and_stream_of_empty_lists_complete_the_message_with_no_groups():
    class Empty(RecordingStreamHandler):
        async def arrange_stream(self, message):
            if message.offset == 1:
                yield []
                yield []

    recorder = MagicMock()
    handler = Empty()
    proc = stream_processor(handler, recorder=recorder)
    proc.enqueue(make_msg(0))
    proc.enqueue(make_msg(1))
    await run_until_settled(proc)

    assert [group.groups for group in handler.message_groups] == [[], []]
    assert [group.total for group in handler.message_groups] == [0, 0]
    recorder.record_arranged.assert_not_called()
    assert [call.kwargs['group_count'] for call in recorder.record_message_complete.call_args_list] == [0, 0]


async def test_replacement_tasks_join_the_group_and_count_as_replaced():
    class Replacing(RecordingStreamHandler):
        async def on_error(self, task, error):
            return [stream_task(f'{task.task_id}-r{n}', task.source_offsets[0]) for n in range(2)]

    handler = Replacing([1], exit_code=lambda task_id: 1 if task_id == 'm0-g0-t0' else 0)
    proc = stream_processor(handler)
    proc.enqueue(make_msg(0))
    await run_until_settled(proc)

    group = handler.message_groups[0].groups[0]
    assert group.task_count == 1
    assert (group.succeeded, group.failed, group.replaced) == (2, 0, 1)
    assert handler.task_groups == [group, group]
    assert handler.message_groups[0].total == 3


async def test_replacement_with_another_offset_still_settles_its_own_message_and_group():
    class ReplacingElsewhere(RecordingStreamHandler):
        async def on_error(self, task, error):
            return [stream_task(f'{task.task_id}-r0', offset=99)]

    recorder = MagicMock()
    handler = ReplacingElsewhere([2, 1], exit_code=lambda task_id: 1 if task_id == 'm0-g0-t0' else 0)
    proc = stream_processor(handler, recorder=recorder)
    proc.enqueue(make_msg(0))
    await run_until_settled(proc)

    first = handler.message_groups[0].groups[0]
    assert (first.succeeded, first.failed, first.replaced) == (2, 0, 1)
    assert handler.task_groups.count(first) == 2
    # The tracker counters agree with the group counters.
    completed = recorder.record_message_complete.call_args.kwargs
    assert (completed['task_count'], completed['succeeded'], completed['replaced']) == (4, 3, 1)


async def test_skipped_sink_is_counted_on_the_group():
    class Delivering(RecordingStreamHandler):
        async def on_task_complete(self, result):
            await super().on_task_complete(result)
            return CollectResult(kafka=[KafkaPayload(data=Out(v=result.task.task_id))])

    handler = Delivering([2])
    collect = RecordingCollect(
        handler.log,
        outcomes={('kafka', 'out'): DeliveryOutcome.SKIPPED, ('postgres', 'db'): DeliveryOutcome.DELIVERED},
    )
    proc = stream_processor(handler, on_collect=collect)
    proc.enqueue(make_msg(0))
    await run_until_settled(proc)

    assert handler.message_groups[0].groups[0].skipped_deliveries == {'kafka/out': 2}


async def test_group_result_skip_is_counted_and_an_answer_that_is_not_a_map_counts_nothing():
    answers = iter([None, {('kafka', 'out'): DeliveryOutcome.SKIPPED}])

    async def on_collect(result, partition_id, message_context=None):
        return next(answers)

    class TaskDelivering(WithGroupHook):
        async def on_task_complete(self, result):
            return CollectResult(kafka=[KafkaPayload(data=Out(v='task'))])

    handler = TaskDelivering([1])
    proc = stream_processor(handler, on_collect=on_collect)
    proc.enqueue(make_msg(0))
    await run_until_settled(proc)

    assert handler.message_groups[0].groups[0].skipped_deliveries == {'kafka/out': 1}


async def test_notes_that_are_not_scalars_are_dropped_and_logged():
    class BadNotes(WithGroupHook):
        async def on_group_complete(self, group, results, errors):
            group.notes.update({'count': 3, 'ratio': 0.5, 'tasks': [1, 2], 'nested': {'a': 1}})
            return None

    handler = BadNotes([1])
    proc = stream_processor(handler)
    proc.enqueue(make_msg(4))
    with capture_logs() as cap:
        await run_until_settled(proc)

    assert handler.message_groups[0].groups[0].notes == {'count': 3, 'ratio': 0.5}
    entry = next(entry for entry in cap if entry['event'] == 'group_notes_invalid')
    assert entry['keys'] == ['nested', 'tasks']
    assert (entry['log_level'], entry['category'], entry['partition'], entry['offset'], entry['group_index']) == (
        'error',
        'handler',
        0,
        4,
        0,
    )


async def test_notes_written_without_a_group_hook_are_checked_when_the_group_ends():
    class TaskNotes(RecordingStreamHandler):
        async def on_task_complete(self, result):
            result.task.group.notes['bad'] = object()
            return await super().on_task_complete(result)

    handler = TaskNotes([1])
    proc = stream_processor(handler)
    proc.enqueue(make_msg(0))
    with capture_logs() as cap:
        await run_until_settled(proc)

    assert handler.message_groups[0].groups[0].notes == {}
    assert any(entry['event'] == 'group_notes_invalid' for entry in cap)


async def test_group_hook_exception_is_logged_and_the_message_still_completes():
    class Raising(WithGroupHook):
        async def on_group_complete(self, group, results, errors):
            raise RuntimeError('boom')

    recorder = MagicMock()
    handler = Raising([2])
    proc = stream_processor(handler, recorder=recorder)
    proc.enqueue(make_msg(3))
    with capture_logs() as cap:
        await run_until_settled(proc)

    entry = next(entry for entry in cap if entry['event'] == 'on_group_complete_failed')
    assert (entry['log_level'], entry['category'], entry['offset'], entry['group_index'], entry['error']) == (
        'error',
        'handler',
        3,
        0,
        'boom',
    )
    assert len(handler.message_groups) == 1
    assert recorder.record_group_complete.call_args.kwargs['output_message_count'] == 0


@pytest.mark.parametrize(
    'delivery_error',
    [SinkDeliveryFailedError('out', 'kafka', 'down'), RuntimeError('unknown')],
    ids=['delivery_failed', 'unexpected_error'],
)
async def test_group_result_delivery_failure_stalls_the_offset_in_stall_mode(delivery_error):
    handler = WithGroupHook([1])
    on_stall = AsyncMock()

    async def on_collect(result, partition_id, message_context=None):
        raise delivery_error

    proc = stream_processor(handler, on_collect=on_collect, on_dlq_failure='stall', on_stall=on_stall)
    proc.enqueue(make_msg(0))
    proc.start()
    await wait_for(lambda: 0 in proc._stalled_offsets)
    await proc.stop()

    assert proc.offset_tracker.has_pending()
    assert proc.offset_tracker.committable() is None
    assert len(handler.message_groups) == 1
    on_stall.assert_awaited_once_with(0)


async def test_unexpected_group_delivery_error_commits_in_drop_mode():
    handler = WithGroupHook([1])

    async def on_collect(result, partition_id, message_context=None):
        raise RuntimeError('unknown')

    proc = stream_processor(handler, on_collect=on_collect)
    proc.enqueue(make_msg(0))
    with capture_logs() as cap:
        await run_until_settled(proc)

    entry = next(entry for entry in cap if entry['event'] == 'on_group_complete_sink_delivery_failed')
    assert entry['offset_action'] == 'commit'
    assert proc._stalled_offsets == set()


async def test_group_result_is_not_delivered_when_deliveries_are_suppressed():
    handler = WithGroupHook([1])
    collect = RecordingCollect(handler.log)
    proc = stream_processor(handler, on_collect=collect)
    proc.suppress_deliveries()
    proc.enqueue(make_msg(0))
    proc.start()
    await wait_for(lambda: len(handler.message_groups) == 1)
    await proc.stop()
    assert not any(entry.startswith('deliver:') for entry in handler.log)


async def test_yielding_a_task_twice_is_a_stream_error():
    shared = stream_task('shared', 0)

    class Twice(RecordingStreamHandler):
        async def arrange_stream(self, message):
            yield [shared]
            yield [shared]

    handler = Twice()
    proc = stream_processor(handler)
    with pytest.raises(
        ArrangeStreamError, match=r'^arrange_stream yielded task shared twice; a task belongs to one group$'
    ):
        await proc._process_stream_message(make_msg(0))
    await wait_for(lambda: proc.inflight_count == 0)
    group = handler.task_groups[0]
    assert (group.task_count, group.succeeded) == (1, 1)


async def test_the_same_task_twice_in_one_list_is_a_stream_error():
    shared = stream_task('shared', 0)
    with pytest.raises(ArrangeStreamError, match='yielded task shared twice'):
        check_stream_item([shared, shared], make_msg(0))
    assert shared.group is None


@pytest.mark.parametrize(
    ('item', 'expected'),
    [
        ('text', 'arrange_stream must yield a list of ExecutorTask, got str'),
        ((stream_task('a', 0),), 'arrange_stream must yield a list of ExecutorTask, got tuple'),
        ([stream_task('a', 0), 5], 'arrange_stream must yield a list of ExecutorTask, got int'),
        (
            [ExecutorTask(task_id='wide', source_offsets=[0, 1])],
            'arrange_stream yielded task wide with source_offsets [0, 1]; '
            'a stream task covers only its message, offset 0',
        ),
    ],
    ids=['text', 'tuple', 'list_with_int', 'wrong_offsets'],
)
async def test_item_that_is_not_a_list_of_tasks_and_wrong_offsets_are_stream_errors(item, expected):
    class Yielding(RecordingStreamHandler):
        async def arrange_stream(self, message):
            yield item

    proc = stream_processor(Yielding())
    with pytest.raises(ArrangeStreamError) as caught:
        await proc._process_stream_message(make_msg(0))
    assert str(caught.value) == expected
    assert proc.inflight_count == 0


async def test_a_result_that_is_not_an_async_iterator_is_a_stream_error():
    class NotAGenerator(BaseDrakkarHandler):
        def arrange_stream(self, message):
            return [[stream_task('a', message.offset)]]

    class Coroutine(BaseDrakkarHandler):
        async def arrange_stream(self, message):  # an async def without a yield
            return []

    with pytest.raises(ArrangeStreamError, match=r'^arrange_stream must be an async generator, got list$'):
        await stream_processor(NotAGenerator())._process_stream_message(make_msg(0))
    with pytest.raises(ArrangeStreamError, match=r'^arrange_stream must be an async generator, got coroutine$'):
        await stream_processor(Coroutine())._process_stream_message(make_msg(0))


async def test_a_generator_exception_propagates_unchanged():
    class Failing(RecordingStreamHandler):
        async def arrange_stream(self, message):
            yield self.make_group(message, 0, 1)
            raise ValueError('side api down')

    handler = Failing()
    proc = stream_processor(handler)
    with pytest.raises(ValueError, match='side api down'):
        await proc._process_stream_message(make_msg(0))
    await wait_for(lambda: proc.inflight_count == 0)
    assert handler.message_groups == []


async def test_a_stream_of_many_single_tasks_gives_the_loop_a_turn():
    handler = RecordingStreamHandler()
    proc = stream_processor(handler, budget=PendingBudget(max_tasks=100_000, max_bytes=1 << 40))
    message = make_msg(0)
    tracker = MessageTracker(source_message=message, stream_open=True)
    proc._message_trackers[0] = tracker
    tasks = [stream_task(f't{n}', 0) for n in range(3 * FANOUT_CHUNK_TASKS)]
    window = Window(window_id=1, source_messages=[message])
    turns: list[int] = []
    asyncio.get_running_loop().call_soon(turns.append, 1)

    await proc._start_group(tracker, tasks, window)

    assert turns == [1]
    # Tasks of the first chunks may already have settled: only the scheduled count is fixed.
    assert tracker.scheduled_count == len(tasks)
    await wait_for(lambda: proc.inflight_count == 0)
    assert window.group is not None and window.group.succeeded == len(tasks)
    assert tracker.open_groups == 0
    assert proc._account is not None and proc._account.budget.tasks == 0


async def test_arranged_event_per_group_and_group_complete_event_only_with_the_hook():
    with_hook = MagicMock()
    proc = stream_processor(WithGroupHook([2, 1]), recorder=with_hook)
    proc.enqueue(make_msg(0))
    await run_until_settled(proc)

    arranged = [call.kwargs for call in with_hook.record_arranged.call_args_list]
    assert [kwargs['group_index'] for kwargs in arranged] == [0, 1]
    assert [len(call.args[2]) for call in with_hook.record_arranged.call_args_list] == [2, 1]
    window_ids = [kwargs['window_id'] for kwargs in arranged]
    assert window_ids == [1, 2]
    completed = sorted(
        (call.kwargs for call in with_hook.record_group_complete.call_args_list), key=lambda k: k['group_index']
    )
    assert [(k['window_id'], k['task_count'], k['succeeded'], k['replaced']) for k in completed] == [
        (1, 2, 2, 0),
        (2, 1, 1, 0),
    ]
    assert [k['output_message_count'] for k in completed] == [1, 1]
    with_hook.record_window_complete.assert_not_called()
    assert with_hook.record_message_complete.call_args.kwargs['group_count'] == 2

    without_hook = MagicMock()
    proc = stream_processor(RecordingStreamHandler([1]), recorder=without_hook)
    proc.enqueue(make_msg(0))
    await run_until_settled(proc)
    assert without_hook.record_arranged.call_count == 1
    without_hook.record_group_complete.assert_not_called()


async def test_list_handler_is_untouched():
    windows: list[int] = []
    groups: list[object] = []

    class ListHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return [stream_task(f'task-{msg.offset}', msg.offset) for msg in messages]

        async def on_task_complete(self, result):
            groups.append(result.task.group)
            return None

        async def on_window_complete(self, results, source_messages):
            windows.append(len(results))
            return None

    recorder = MagicMock()
    proc = PartitionProcessor(
        partition_id=0, handler=ListHandler(), executor_pool=make_pool(), window_size=10, recorder=recorder
    )
    assert proc._stream_mode is False
    proc.enqueue(make_msg(0))
    proc.enqueue(make_msg(1))
    await run_until_settled(proc)

    assert groups == [None, None]
    assert sum(windows) == 2
    assert all('group_index' not in call.kwargs for call in recorder.record_arranged.call_args_list)
    assert all(call.kwargs['group_count'] is None for call in recorder.record_message_complete.call_args_list)


async def test_an_unexpected_task_error_is_kept_as_a_group_error():
    class RaisingTaskHook(WithGroupHook):
        async def on_task_complete(self, result):
            raise RuntimeError('hook bug')

    handler = RaisingTaskHook([1])
    proc = stream_processor(handler)
    proc.enqueue(make_msg(0))
    await run_until_settled(proc)

    group, results, errors = handler.group_calls[0]
    assert results == [] and [error.task.task_id for error in errors] == ['m0-g0-t0']
    assert (group.succeeded, group.failed) == (0, 1)


async def test_kept_group_output_is_charged_and_released():
    budget = PendingBudget(max_tasks=10_000, max_bytes=1 << 40)
    bytes_in_hook: list[int] = []

    class Measuring(WithGroupHook):
        async def on_error(self, task, error):
            return ErrorAction.SKIP

        async def on_group_complete(self, group, results, errors):
            bytes_in_hook.append(budget.bytes)
            return None

    handler = Measuring([2], exit_code=lambda task_id: 1 if task_id.endswith('t1') else 0)
    proc = stream_processor(handler, budget=budget)
    proc.enqueue(make_msg(0))
    await run_until_settled(proc)

    assert bytes_in_hook and bytes_in_hook[0] > 0
    assert (budget.tasks, budget.bytes) == (0, 0)


async def test_kafka_source_logs_that_window_size_has_no_effect_for_a_stream_handler(monkeypatch):
    source, _ = _source_with_fake_consumer(monkeypatch)
    source._context().handler = RecordingStreamHandler()
    with capture_logs() as cap:
        await source.start()
    entry = next(entry for entry in cap if entry['event'] == 'arrange_stream_enabled')
    assert (entry['log_level'], entry['category'], entry['hint']) == (
        'info',
        'lifecycle',
        'executor.window_size has no effect: a stream handler takes one message at a time',
    )

    source, _ = _source_with_fake_consumer(monkeypatch)
    with capture_logs() as cap:
        await source.start()
    assert not any(entry['event'] == 'arrange_stream_enabled' for entry in cap)


async def test_a_message_that_fails_to_parse_follows_the_policy_and_opens_no_stream():
    class Payload(Out):
        pass

    class Parsing(RecordingStreamHandler):
        input_model = Payload

    handler = Parsing()
    dlq_send = AsyncMock(return_value=True)
    proc = stream_processor(handler, on_parse_error='dlq', dlq_send=dlq_send)
    proc.enqueue(make_msg(0))
    await run_until_settled(proc)

    dlq_send.assert_awaited_once()
    assert handler.log == [] and handler.message_groups == []
    assert proc._message_trackers == {}
