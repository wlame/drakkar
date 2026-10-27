"""The HTTP source delivers a request's sinks in one pass, with each sink's declared policy and dependencies."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock

import pytest
from pydantic import BaseModel
from structlog.testing import capture_logs

from drakkar.config import WebAppConfig, WebClientConfig
from drakkar.handler import BaseDrakkarHandler
from drakkar.metrics import dlq_unconfigured_drops, sink_deliveries_not_started, webapp_dropped_after_timeout
from drakkar.models import CollectResult, CustomPayload, DeliveryError, KafkaPayload, PostgresPayload
from drakkar.sinks.dlq_record import render_headers
from drakkar.sinks.manager import SinkManager
from drakkar.webapp.models import WebRequestContext
from drakkar.webapp.runner import WebappRunner
from tests.test_sink_delivery_policy import Data, FakeSink, RecordingDLQ, manager_with, policy

RECEIVED_AT = datetime(2026, 11, 5, 10, 15, 30, 123000, tzinfo=UTC)
RECEIVED_MS = int(RECEIVED_AT.timestamp() * 1000)
# Not what the parsed model serializes to: the record must carry the bytes the client sent.
RAW_BODY = b'{ "q" : 1 }\n'


class Req(BaseModel):
    q: int = 0


class Handler(BaseDrakkarHandler):
    """Arranges no tasks and returns a fixed CollectResult from ``on_message_complete``."""

    def __init__(self, collect: CollectResult) -> None:
        super().__init__()
        self.collect = collect

    async def arrange(self, messages, pending):
        return []

    async def arrange_http_request(self, req, pending):
        return []

    async def on_message_complete(self, group):
        return self.collect

    async def on_http_request_complete(self, group):
        return None


def make_ctx() -> WebRequestContext:
    ctx = WebRequestContext(
        request_id='req-7',
        client_name='anonymous',
        request=Req(q=1),
        started_at=RECEIVED_AT,
        headers={},
        raw_body=RAW_BODY,
    )
    # Allocated up front so a sink can set it while the runner delivers.
    ctx.cancelled = asyncio.Event()
    return ctx


def make_runner(handler: BaseDrakkarHandler, manager: SinkManager, *, recorder: Any = None) -> WebappRunner:
    app = MagicMock()
    app._handler = handler
    app._executor_pool = MagicMock()
    app._recorder = recorder
    app._sink_manager = manager
    config = WebAppConfig(
        enabled=True,
        host='127.0.0.1',
        port=0,
        path='/process',
        sinks_enabled=True,
        clients=[WebClientConfig(name='anonymous', token='', rpm=4)],
    )
    return WebappRunner(app, config)


class OrderedSink(FakeSink):
    """A sink that writes when its delivery starts and ends into a shared journal."""

    def __init__(self, name: str, sink_type: str, journal: list[str]) -> None:
        super().__init__(name, sink_type)
        self.journal = journal

    async def deliver(self, payloads: list[BaseModel]) -> None:
        self.journal.append(f'{self.sink_type} start')
        # A few loop turns, so a dependent that does not wait would start in between.
        for _ in range(3):
            await asyncio.sleep(0)
        await super().deliver(payloads)
        self.journal.append(f'{self.sink_type} end')


async def test_http_request_dlq_writes_body_record_and_stops_dependents():
    kafka = FakeSink('out', 'kafka')
    kafka.fail = True
    postgres = FakeSink('main', 'postgres')
    dlq = RecordingDLQ()
    manager = manager_with(
        (kafka, policy(max_retries=0)),
        (postgres, policy(depends_on=('kafka/out',))),
        dlq=dlq,
    )
    collect = CollectResult(kafka=[KafkaPayload(data=Data())], postgres=[PostgresPayload(table='t', data=Data())])

    report = await make_runner(Handler(collect), manager).run(make_ctx())

    [(record, partition_id)] = dlq.sent
    assert (record.key, record.value, partition_id) == (None, RAW_BODY, -1)
    headers = dict(render_headers(record))
    assert headers['dk-request-id'] == b'req-7'
    assert headers['dk-source-timestamp'] == str(RECEIVED_MS).encode()
    assert headers['dk-sink'] == b'kafka/out'
    for absent in ('dk-source-topic', 'dk-source-partition', 'dk-source-offset'):
        assert absent not in headers
    assert postgres.delivered == []
    assert report.status == 'ok'
    assert report.sinks is not None
    assert report.sinks.by_type['kafka'].model_dump() == {
        'attempted': 1,
        'delivered': 0,
        'dlq': 1,
        'errors': ['kafka/out: kafka is down'],
    }
    assert report.sinks.by_type['postgres'].model_dump() == {
        'attempted': 1,
        'delivered': 0,
        'dlq': 0,
        'errors': ['postgres/main: not started'],
    }


async def test_http_depends_on_orders_groups_across_types():
    # kafka comes first in the per-type order of the summary, but it depends on postgres.
    journal: list[str] = []
    kafka = OrderedSink('out', 'kafka', journal)
    postgres = OrderedSink('main', 'postgres', journal)
    manager = manager_with((kafka, policy(depends_on=('postgres/main',))), (postgres, policy()))
    collect = CollectResult(kafka=[KafkaPayload(data=Data())], postgres=[PostgresPayload(table='t', data=Data())])

    report = await make_runner(Handler(collect), manager).run(make_ctx())

    assert journal == ['postgres start', 'postgres end', 'kafka start', 'kafka end']
    assert report.sinks is not None
    assert {kind: result.delivered for kind, result in report.sinks.by_type.items()} == {'kafka': 1, 'postgres': 1}


async def test_http_summary_counts_per_type_from_outcomes():
    out = FakeSink('out', 'kafka')
    audit = FakeSink('audit', 'kafka')
    audit.fail = True
    postgres = FakeSink('main', 'postgres')
    plugin = FakeSink('lake', 'datalake')
    manager = manager_with(
        (out, policy()),
        (audit, policy(on_failure='skip', max_retries=0)),
        (postgres, policy()),
        (plugin, policy()),
    )
    calls: list[Any] = []
    deliver_all = manager.deliver_all

    async def spy(*args, **kwargs):
        calls.append(kwargs.get('message_context'))
        return await deliver_all(*args, **kwargs)

    manager.deliver_all = spy  # ty: ignore[invalid-assignment]
    collect = CollectResult(
        kafka=[
            KafkaPayload(sink='out', data=Data(value=1)),
            KafkaPayload(sink='out', data=Data(value=2)),
            KafkaPayload(sink='audit', data=Data(value=3)),
        ],
        postgres=[PostgresPayload(table='t', data=Data()) for _ in range(3)],
        custom=[CustomPayload(sink='lake', data=Data())],
    )

    report = await make_runner(Handler(collect), manager).run(make_ctx())

    [context] = calls
    assert context.request_id == 'req-7'
    assert report.sinks is not None
    assert {kind: result.model_dump() for kind, result in report.sinks.by_type.items()} == {
        'kafka': {'attempted': 3, 'delivered': 2, 'dlq': 0, 'errors': ['kafka/audit: kafka is down']},
        'postgres': {'attempted': 3, 'delivered': 3, 'dlq': 0, 'errors': []},
        'custom': {'attempted': 1, 'delivered': 1, 'dlq': 0, 'errors': []},
    }
    assert [len(batch) for batch in out.delivered] == [2]


async def test_http_cancellation_between_groups_keeps_its_log_event():
    ctx = make_ctx()

    class CancellingSink(FakeSink):
        async def deliver(self, payloads: list[BaseModel]) -> None:
            await super().deliver(payloads)
            # The caller's request timed out while this group was in flight.
            assert ctx.cancelled is not None
            ctx.cancelled.set()

    kafka = CancellingSink('out', 'kafka')
    postgres = FakeSink('main', 'postgres')
    manager = manager_with((kafka, policy()), (postgres, policy(depends_on=('kafka/out',))))
    collect = CollectResult(kafka=[KafkaPayload(data=Data())], postgres=[PostgresPayload(table='t', data=Data())])
    recorder = MagicMock()
    dropped_before = webapp_dropped_after_timeout.labels(client='anonymous')._value.get()
    not_started = sink_deliveries_not_started.labels(sink_type='postgres', sink_name='main', reason='dependency_failed')
    not_started_before = not_started._value.get()

    with capture_logs() as logs, pytest.raises(asyncio.CancelledError):
        await make_runner(Handler(collect), manager, recorder=recorder).run(ctx)

    assert len(kafka.delivered) == 1
    assert postgres.delivered == []
    dropped = [entry for entry in logs if entry['event'] == 'webapp_request_dropped_after_timeout']
    assert len(dropped) == 1
    assert dropped[0]['stage'] == 'during_sinks'
    assert dropped[0]['next_sink_type'] == 'postgres'
    assert dropped[0]['next_sink'] == 'postgres/main'
    assert dropped[0]['request_id'] == 'req-7'
    assert webapp_dropped_after_timeout.labels(client='anonymous')._value.get() == dropped_before + 1
    recorder.record_webapp_request_dropped_after_timeout.assert_called_once_with(ctx)
    # A cancelled request is not a failed dependency.
    assert not_started._value.get() == not_started_before


async def test_http_cancellation_before_parallel_groups_logs_once():
    ctx = make_ctx()
    assert ctx.cancelled is not None
    kafka = FakeSink('out', 'kafka')
    postgres = FakeSink('main', 'postgres')
    manager = manager_with((kafka, policy()), (postgres, policy()))
    collect = CollectResult(kafka=[KafkaPayload(data=Data())], postgres=[PostgresPayload(table='t', data=Data())])
    runner = make_runner(Handler(collect), manager)

    async def cancel_on_message_complete(group):
        # The timeout lands after the post-execute gate, before any group starts.
        ctx.cancelled.set()
        return collect

    runner._app._handler.on_message_complete = cancel_on_message_complete

    with capture_logs() as logs, pytest.raises(asyncio.CancelledError):
        await runner.run(ctx)

    assert (kafka.delivered, postgres.delivered) == ([], [])
    dropped = [entry for entry in logs if entry['event'] == 'webapp_request_dropped_after_timeout']
    assert [entry['stage'] for entry in dropped] == ['during_sinks']


async def test_http_raising_hook_applies_the_declared_policy():
    kafka = FakeSink('out', 'kafka')
    kafka.fail = True
    dlq = RecordingDLQ()
    manager = manager_with((kafka, policy(on_failure='skip', max_retries=0)), dlq=dlq)
    handler = Handler(CollectResult(kafka=[KafkaPayload(data=Data())]))

    async def raising_hook(error: DeliveryError):
        raise RuntimeError('hook bug')

    handler.on_delivery_error = raising_hook  # ty: ignore[invalid-assignment]

    with capture_logs() as logs:
        report = await make_runner(handler, manager).run(make_ctx())

    assert dlq.sent == []
    assert [entry['event'] for entry in logs].count('delivery_error_hook_failed') == 1
    assert report.sinks is not None
    assert report.sinks.by_type['kafka'].model_dump() == {
        'attempted': 1,
        'delivered': 0,
        'dlq': 0,
        'errors': ['kafka/out: kafka is down'],
    }


async def test_http_dlq_off_counts_one_unconfigured_drop():
    kafka = FakeSink('out', 'kafka')
    kafka.fail = True
    manager = manager_with((kafka, policy(max_retries=0)))
    manager.attach_runtime(recorder=None, dlq_sink=None, dlq_on_send_failure='drop', dlq_enabled=False)
    before = dlq_unconfigured_drops._value.get()

    report = await make_runner(Handler(CollectResult(kafka=[KafkaPayload(data=Data())])), manager).run(make_ctx())

    assert dlq_unconfigured_drops._value.get() == before + 1
    assert report.sinks is not None
    assert report.sinks.by_type['kafka'].model_dump() == {
        'attempted': 1,
        'delivered': 0,
        'dlq': 0,
        'errors': ['kafka/out: kafka is down', 'kafka/out: failed, no DLQ record'],
    }


async def test_http_stall_raise_is_reported_in_the_summary():
    kafka = FakeSink('out', 'kafka')
    kafka.fail = True
    postgres = FakeSink('main', 'postgres')
    manager = manager_with((kafka, policy(max_retries=0)), (postgres, policy()))
    manager.attach_runtime(recorder=None, dlq_sink=RecordingDLQ(ok=False), dlq_on_send_failure='stall')
    collect = CollectResult(kafka=[KafkaPayload(data=Data())], postgres=[PostgresPayload(table='t', data=Data())])

    report = await make_runner(Handler(collect), manager).run(make_ctx())

    # The raise carries no outcomes, so every type of the request reports it.
    assert report.status == 'ok'
    assert report.sinks is not None
    assert set(report.sinks.by_type) == {'kafka', 'postgres'}
    for result in report.sinks.by_type.values():
        assert any(error.startswith('deliver_all_raised: ') for error in result.errors)
