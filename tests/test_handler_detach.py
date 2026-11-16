"""The handler's detach, attach, detach_state and request_stop, and the check of a detach inside a partition's work."""

import asyncio
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from structlog.testing import capture_logs

import drakkar
from drakkar import cluster_tasks, detach_slots
from drakkar import detach as detach_module
from drakkar.app import DrakkarApp
from drakkar.config import DrakkarConfig
from drakkar.detach import ATTACHED_STATE, STATE_DETACHED, AttachResult, DetachOrigin, DetachResult
from drakkar.executor import ExecutorPool
from drakkar.handler import BaseDrakkarHandler
from drakkar.hook_scope import inside_pipeline_hook, pipeline_hook
from drakkar.models import CollectResult, DeliveryAction, ErrorAction, ExecutorTask, KafkaPayload, PrecomputedResult
from drakkar.periodic import run_periodic_task
from drakkar.uiserver.runner import DebugRunner, ProbeInput
from tests.clocks import VirtualClock
from tests.conftest import kafka_connections, wait_for
from tests.fleet_fakes import RecordingLogger
from tests.stream_helpers import Out, RecordingStreamHandler, make_msg, stream_processor, stream_task
from tests.test_detach_controller import NOW, Harness, peer
from tests.test_lifecycle_sources import StubSource, _app
from tests.test_sources_kafka import _source_with_fake_consumer
from tests.test_sources_validation import KafkaOnlyHandler

UNTIL = datetime(2027, 1, 1, tzinfo=UTC)


@pytest.fixture
def logs(monkeypatch) -> RecordingLogger:
    recording = RecordingLogger()
    monkeypatch.setattr(detach_module, 'logger', recording)
    monkeypatch.setattr(detach_slots, 'logger', recording)
    monkeypatch.setattr(cluster_tasks, 'logger', recording)
    return recording


@pytest.fixture
async def worker(tmp_path, logs):
    """A worker with the detach switch on, its handler wired to the app."""
    clock = VirtualClock(NOW)
    clock.limit = NOW
    harness = Harness(tmp_path, clock)
    harness.fleet.views = [peer()]
    harness.app._lifecycle._wire_detach_front()
    yield harness
    await harness.controller.release_at_stop(1.0)
    detach_module.detached.set(0)


def handler_of(worker: Harness) -> BaseDrakkarHandler:
    return worker.app._handler


def spy_detach(worker: Harness) -> AsyncMock:
    answer = DetachResult(detached=True, reason='', until=UNTIL)
    spy = AsyncMock(return_value=answer)
    worker.controller.detach = spy
    return spy


def make_app(handler: BaseDrakkarHandler | None = None) -> DrakkarApp:
    config = DrakkarConfig(
        connections=kafka_connections(),
        ui={'enabled': False, 'release': {'enabled': False}},
        recorder={'enabled': False},
        metrics={'enabled': False},
        executor={'binary_path': '/usr/bin/echo'},
        sources={'kafka': {'enabled': True, 'startup_align': {'enabled': False}}},
    )
    return DrakkarApp(handler=handler or KafkaOnlyHandler(), config=config, worker_id='w-1')


# --- the front before and after the wiring ----------------------------------------


async def test_detach_before_the_app_runs_answers_not_running():
    handler = KafkaOnlyHandler()

    assert await handler.detach() == DetachResult(detached=False, reason='not_running', until=None)
    assert await handler.attach() == AttachResult(attached=False, reason='not_running')
    assert handler.detach_state == ATTACHED_STATE
    handler.request_stop(reason='nothing to stop', exit_code=1)


async def test_detach_passes_origin_handler_to_the_controller(worker):
    spy = spy_detach(worker)

    result = await handler_of(worker).detach(duration=60, reason='slow disk')

    assert result is spy.return_value
    spy.assert_awaited_once_with(origin=DetachOrigin.HANDLER, duration=60, reason='slow disk', in_pipeline_hook=False)


async def test_detach_state_reads_the_controller(worker):
    assert handler_of(worker).detach_state is worker.controller.state


@pytest.mark.parametrize('too_far', ['short', 'long'])
async def test_detach_with_a_duration_out_of_range_raises_value_error(worker, too_far):
    duration = 5 if too_far == 'short' else worker.app.config.sources.kafka.detach.max_duration + 1

    with pytest.raises(ValueError, match='duration must be from'):
        await handler_of(worker).detach(duration=duration)


# --- the pipeline hook check ---------------------------------------------------------


async def test_detach_inside_a_pipeline_hook_is_refused(worker):
    with pipeline_hook():
        result = await handler_of(worker).detach()

    assert result == DetachResult(detached=False, reason='inside_pipeline_hook', until=None)
    assert sum(worker.provider.calls.values()) == 0


async def test_detach_outside_a_pipeline_hook_passes_false(worker):
    spy = spy_detach(worker)
    with pipeline_hook():
        pass

    await handler_of(worker).detach()

    assert spy.await_args.kwargs['in_pipeline_hook'] is False


async def test_detach_awaited_through_gather_inside_a_hook_is_refused(worker):
    handler = handler_of(worker)
    with pipeline_hook():
        (gathered,) = await asyncio.gather(handler.detach())
        waited = await asyncio.wait_for(handler.detach(), timeout=5)

    assert gathered.reason == 'inside_pipeline_hook'
    assert waited.reason == 'inside_pipeline_hook'
    assert sum(worker.provider.calls.values()) == 0


async def test_a_task_started_in_a_hook_may_detach_after_the_hook_returned(worker):
    spy = spy_detach(worker)
    handler = handler_of(worker)
    hook_returned = asyncio.Event()

    async def later() -> DetachResult:
        await hook_returned.wait()
        return await handler.detach(reason='later')

    with pipeline_hook():
        task = asyncio.create_task(later())
    hook_returned.set()
    result = await task

    assert result is spy.return_value
    assert spy.await_args.kwargs['in_pipeline_hook'] is False


async def test_a_nested_scope_keeps_the_outer_one_open():
    with pipeline_hook():
        with pipeline_hook():
            assert inside_pipeline_hook()
        assert inside_pipeline_hook()
    assert not inside_pipeline_hook()


async def test_attach_and_request_stop_are_allowed_inside_a_pipeline_hook(worker):
    attach = AsyncMock(return_value=AttachResult(attached=True, reason=''))
    worker.controller.attach = attach
    request_stop = MagicMock()
    worker.app.request_stop = request_stop
    handler = handler_of(worker)

    with pipeline_hook():
        result = await handler.attach(reason='disk ok')
        handler.request_stop(reason='cannot detach', exit_code=3)

    assert result.attached is True
    attach.assert_awaited_once_with(cause='handler', reason='disk ok')
    request_stop.assert_called_once_with(reason='cannot detach', exit_code=3)


# --- every hook of a partition's work runs in a scope ----------------------------------


class SeenHooks:
    """Records, per hook name, whether the hook saw an open pipeline hook scope."""

    def __init__(self) -> None:
        self.seen: dict[str, bool] = {}

    def note(self, hook: str) -> None:
        self.seen[hook] = inside_pipeline_hook()


class ListHandler(BaseDrakkarHandler):
    def __init__(self, seen: SeenHooks) -> None:
        super().__init__()
        self.hooks = seen

    async def arrange(self, messages, pending):
        self.hooks.note('arrange')
        task = ExecutorTask(
            task_id=f'ok-{messages[0].offset}',
            source_offsets=[messages[0].offset],
            precomputed=PrecomputedResult(stdout='out', exit_code=0),
        )
        failing = ExecutorTask(
            task_id=f'bad-{messages[0].offset}',
            source_offsets=[messages[0].offset],
            precomputed=PrecomputedResult(stdout='', exit_code=1),
        )
        return [task, failing]

    async def on_task_complete(self, result):
        self.hooks.note('on_task_complete')

    async def on_error(self, task, error):
        self.hooks.note('on_error')
        return ErrorAction.SKIP

    async def on_message_complete(self, group):
        self.hooks.note('on_message_complete')

    async def on_window_complete(self, results, source_messages):
        self.hooks.note('on_window_complete')

    async def on_assign(self, partitions):
        self.hooks.note('on_assign')

    async def on_revoke(self, partitions):
        self.hooks.note('on_revoke')


class StreamHandler(RecordingStreamHandler):
    def __init__(self, seen: SeenHooks) -> None:
        super().__init__([1, 1])
        self.hooks = seen

    async def arrange_stream(self, message):
        self.hooks.note('arrange_stream:0')
        yield [stream_task('s0', message.offset)]
        self.hooks.note('arrange_stream:1')
        yield [stream_task('s1', message.offset)]

    async def on_group_complete(self, group, results, errors):
        self.hooks.note('on_group_complete')


async def test_every_listed_hook_runs_inside_a_pipeline_hook_scope():
    seen = SeenHooks()
    list_proc = stream_processor(ListHandler(seen))
    list_proc.enqueue(make_msg(0))
    list_proc.start()
    await wait_for(lambda: 'on_window_complete' in seen.seen and 'on_message_complete' in seen.seen)
    await list_proc.stop()

    stream_proc = stream_processor(StreamHandler(seen))
    stream_proc.enqueue(make_msg(0))
    stream_proc.start()
    await wait_for(lambda: not stream_proc.offset_tracker.has_pending() and 'on_group_complete' in seen.seen)
    await stream_proc.stop()

    assert seen.seen == {
        'arrange': True,
        'on_task_complete': True,
        'on_error': True,
        'on_message_complete': True,
        'on_window_complete': True,
        'arrange_stream:0': True,
        'arrange_stream:1': True,
        'on_group_complete': True,
    }
    assert not inside_pipeline_hook()


async def test_on_delivery_error_of_a_partition_delivery_runs_inside_a_scope():
    seen = SeenHooks()

    class DeliveryHandler(KafkaOnlyHandler):
        async def on_delivery_error(self, error):
            seen.note('on_delivery_error')
            return DeliveryAction.SKIP

    app = make_app(DeliveryHandler())

    async def deliver_all(result, *, on_delivery_error, partition_id, message_context):
        await on_delivery_error(MagicMock(sink_type='kafka', sink_name='out'))
        return {}

    app._sink_manager = MagicMock(deliver_all=deliver_all)

    await app._handle_collect(CollectResult(kafka=[KafkaPayload(data=Out(v='x'))]), partition_id=0)

    assert seen.seen == {'on_delivery_error': True}


async def test_on_assign_on_revoke_and_periodic_tasks_run_outside_a_scope(monkeypatch):
    seen = SeenHooks()
    source, _ = _source_with_fake_consumer(monkeypatch)
    source._context().handler = ListHandler(seen)
    await source.start()

    source.on_assign([3])
    await wait_for(lambda: 'on_assign' in seen.seen)
    await source.on_revoke([3])
    await wait_for(lambda: 'on_revoke' in seen.seen)

    clock = VirtualClock(NOW)

    async def tick() -> None:
        seen.note('periodic')

    periodic = asyncio.create_task(run_periodic_task('tick', tick, interval=10, on_error='continue', clock=clock))
    await wait_for(lambda: 'periodic' in seen.seen)
    periodic.cancel()
    await asyncio.gather(periodic, return_exceptions=True)
    await source.stop(asyncio.get_running_loop().time() + 1)

    assert seen.seen == {'on_assign': False, 'on_revoke': False, 'periodic': False}


# --- request_stop ------------------------------------------------------------------


async def test_request_stop_signals_every_source_and_logs_once():
    app = make_app()
    app._loop = asyncio.get_running_loop()
    app._running = True
    sources = [MagicMock(), MagicMock()]
    app.sources = sources

    with capture_logs() as captured:
        app.request_stop(reason='cannot detach', exit_code=1)
        app.request_stop(reason='again', exit_code=2)

    lines = [line for line in captured if line['event'] == 'stop_requested']
    assert lines == [
        {
            'event': 'stop_requested',
            'log_level': 'warning',
            'category': 'lifecycle',
            'reason': 'cannot detach',
            'exit_code': 1,
        }
    ]
    assert app._running is False
    for source in sources:
        source.signal_stop.assert_called_once_with()


async def test_request_stop_from_a_thread_stops_the_sources_on_the_loop():
    app = make_app()
    app._loop = asyncio.get_running_loop()
    source = MagicMock()
    app.sources = [source]

    await asyncio.to_thread(app.request_stop, 'from a thread', 4)

    await wait_for(lambda: source.signal_stop.called)
    assert app._stop_exit_code == 4


async def test_second_request_stop_keeps_the_first_exit_code():
    app = make_app()
    app._loop = asyncio.get_running_loop()
    app.sources = []

    app.request_stop(reason='first', exit_code=3)
    app.request_stop(reason='second', exit_code=0)

    assert app._stop_exit_code == 3


def run_app(app: DrakkarApp, monkeypatch, exit_code: int | None) -> None:
    """Run ``app`` with a lifecycle that only asks for a stop (or not) and then ends cleanly."""

    async def short_run() -> None:
        if exit_code is not None:
            app.request_stop(reason='done', exit_code=exit_code)

    monkeypatch.setattr('drakkar.app.setup_logging', lambda *args, **kwargs: None)
    monkeypatch.setattr(app._lifecycle, '_async_run', short_run)
    app.run()


def test_run_exits_with_the_requested_code_after_a_clean_shutdown(monkeypatch):
    app = make_app()
    app.sources = []

    with pytest.raises(SystemExit) as exit_info:
        run_app(app, monkeypatch, 7)

    assert exit_info.value.code == 7


@pytest.mark.parametrize('exit_code', [None, 0])
def test_run_returns_after_a_stop_with_code_0_or_none(monkeypatch, exit_code):
    app = make_app()
    app.sources = []

    run_app(app, monkeypatch, exit_code)


@pytest.mark.parametrize('exit_code', [-1, 256, True, 1.0])
async def test_request_stop_with_an_exit_code_outside_0_to_255_raises_value_error(exit_code: Any):
    app = make_app()
    app._lifecycle._wire_detach_front()

    with pytest.raises(ValueError, match='exit_code must be a whole number from 0 to 255'):
        app._handler.request_stop(exit_code=exit_code)
    with pytest.raises(ValueError, match='exit_code must be a whole number from 0 to 255'):
        app.request_stop(reason='', exit_code=exit_code)
    assert app._stop_exit_code is None


async def test_the_result_types_are_exported_from_the_package():
    assert drakkar.DetachResult is DetachResult
    assert drakkar.AttachResult is AttachResult
    assert drakkar.DetachOrigin is DetachOrigin
    assert drakkar.DetachState is detach_module.DetachState


# --- a pull left running ------------------------------------------------------------


async def test_a_task_started_by_a_pull_left_running_may_detach_after_the_stream_was_given_up(monkeypatch):
    # The loop notices the stop at its next turn; a short turn keeps the test fast.
    monkeypatch.setattr('drakkar.partition.IDLE_POLL_TIMEOUT', 0.05)
    release = asyncio.Event()
    started: list[asyncio.Task] = []

    async def background() -> bool:
        await release.wait()
        return inside_pipeline_hook()

    class Hanging(RecordingStreamHandler):
        async def arrange_stream(self, message):
            yield [stream_task('first', message.offset)]
            started.append(asyncio.create_task(background()))
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                # The stop's cancel is ignored once, so the pull is still
                # running when the loop gives the stream up; the close cancels it again.
                await asyncio.Event().wait()
            yield []

    proc = stream_processor(Hanging())
    proc.enqueue(make_msg(0))
    proc.start()
    await wait_for(lambda: bool(started))
    await proc.stop()
    release.set()

    assert await started[0] is False


# --- the Message Probe ----------------------------------------------------------------


class ProbeCalls:
    """What each hook got from detach, attach and request_stop, by hook name."""

    def __init__(self) -> None:
        self.answers: dict[str, tuple[Any, ...]] = {}

    async def call_all(self, handler: BaseDrakkarHandler, hook: str) -> None:
        detached = await handler.detach(duration=3600, reason='probe')
        (gathered,) = await asyncio.gather(handler.attach(reason='probe'))
        await handler.offload(handler.request_stop, 'from a thread', 2)
        handler.request_stop(reason='from the hook', exit_code=3)
        self.answers[hook] = (detached, gathered)


def probe_task(task_id: str, exit_code: int) -> ExecutorTask:
    return ExecutorTask(
        task_id=task_id,
        source_offsets=[0],
        precomputed=PrecomputedResult(stdout='out', exit_code=exit_code),
    )


class ProbedListHandler(KafkaOnlyHandler):
    def __init__(self, calls: ProbeCalls) -> None:
        super().__init__()
        self.calls = calls

    async def arrange(self, messages, pending):
        await self.calls.call_all(self, 'arrange')
        return [probe_task('ok', 0), probe_task('bad', 1)]

    async def on_task_complete(self, result):
        await self.calls.call_all(self, 'on_task_complete')

    async def on_error(self, task, error):
        await self.calls.call_all(self, 'on_error')
        return ErrorAction.SKIP

    async def on_message_complete(self, group):
        await self.calls.call_all(self, 'on_message_complete')

    async def on_window_complete(self, results, source_messages):
        await self.calls.call_all(self, 'on_window_complete')


class ProbedStreamHandler(KafkaOnlyHandler):
    def __init__(self, calls: ProbeCalls) -> None:
        super().__init__()
        self.calls = calls

    async def arrange_stream(self, message):
        await self.calls.call_all(self, 'arrange_stream')
        yield [probe_task('s0', 0)]

    async def on_group_complete(self, group, results, errors):
        await self.calls.call_all(self, 'on_group_complete')


PROBE_DETACH = DetachResult(detached=False, reason='probe', until=None)
PROBE_ATTACH = AttachResult(attached=False, reason='probe')


@pytest.mark.parametrize(
    ('handler_class', 'hooks'),
    [
        (
            ProbedListHandler,
            {'arrange', 'on_task_complete', 'on_error', 'on_message_complete', 'on_window_complete'},
        ),
        (ProbedStreamHandler, {'arrange_stream', 'on_group_complete'}),
    ],
)
async def test_detach_attach_and_request_stop_have_no_effect_in_a_message_probe(worker, handler_class, hooks):
    calls = ProbeCalls()
    handler = handler_class(calls)
    worker.app._handler = handler
    worker.app._lifecycle._wire_detach_front()
    runner = DebugRunner(
        handler=handler,
        executor_pool=ExecutorPool(binary_path='/nonexistent', max_executors=2, task_timeout_seconds=5),
        app_config=worker.app.config,
    )
    state_before = worker.controller.state

    with capture_logs() as captured:
        await runner.run(ProbeInput(value='{}', partition=0, offset=0, topic='in'))

    assert set(calls.answers) == hooks
    for hook, answers in calls.answers.items():
        assert answers == (PROBE_DETACH, PROBE_ATTACH), hook
    assert sum(worker.provider.calls.values()) == 0
    assert worker.controller.state == state_before
    assert worker.app._stop_exit_code is None
    assert worker.source._stop_requested is False
    suppressed = [line for line in captured if line['event'] == 'stop_request_suppressed']
    assert len(suppressed) == 2 * len(hooks)
    assert suppressed[0]['category'] == 'debug'
    assert {(line['reason'], line['exit_code']) for line in suppressed} == {('from a thread', 2), ('from the hook', 3)}
    assert not [line for line in captured if line['event'] == 'stop_requested']

    # The flag does not leak out of the probe.
    result = await handler.detach(duration=3600, reason='after the probe')
    assert result.detached is True
    assert worker.controller.state.state == STATE_DETACHED


# --- request_stop from on_startup -----------------------------------------------------


def test_request_stop_in_on_startup_ends_the_run_with_its_exit_code(monkeypatch):
    class StopsAtStartup(KafkaOnlyHandler):
        async def on_startup(self, config):
            self.request_stop(reason='x', exit_code=3)
            return config

    source = StubSource('kafka')
    app = _app(monkeypatch, source, handler=StopsAtStartup())
    monkeypatch.setattr('drakkar.app.setup_logging', lambda *args, **kwargs: None)

    with pytest.raises(SystemExit) as exit_info:
        app.run()

    assert exit_info.value.code == 3
    assert 'stop' in source.calls
