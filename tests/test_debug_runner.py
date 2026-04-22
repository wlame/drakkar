"""Tests for the Message Probe debug runner.

Covers the building blocks of the no-footprint probe (Task 1) plus the
``DebugRunner`` happy-path orchestration added in Task 2a:

- ``DebugCacheProxy`` — Cache-shaped wrapper that suppresses writes and
  optionally forwards reads while logging every call.
- ``DebugSinkCollector`` — collects every ``CollectResult`` returned by
  the handler's hooks and flattens them into ``PlannedSinkRecord`` rows
  grouped by sink type.
- ``DebugRunner`` — orchestrates the arrange → execute →
  on_task_complete → on_message_complete → on_window_complete sequence
  inline, keeping partial state for timeout-truncated reports.
- The pydantic report models (``ProbeInput``, ``ProbeCacheCall``,
  ``ProbeTaskEntry``, ``ProbeStageResult``, ``ProbeError``,
  ``PlannedSinkRecord``, ``DebugReport``) — exercised via a round-trip
  serialize/deserialize test.
"""

from __future__ import annotations

import time
from contextlib import ExitStack
from typing import Any
from unittest.mock import patch

import pytest
from pydantic import BaseModel, ValidationError

import drakkar.offsets
import drakkar.partition
import drakkar.recorder
import drakkar.sinks.manager
from drakkar.cache import Cache, CacheScope, NoOpCache
from drakkar.config import DrakkarConfig, ExecutorConfig
from drakkar.debug_runner import (
    DebugCacheProxy,
    DebugReport,
    DebugRunner,
    DebugSinkCollector,
    PlannedSinkRecord,
    ProbeCacheCall,
    ProbeError,
    ProbeInput,
    ProbeStageResult,
    ProbeTaskEntry,
    _make_value_preview,
    _probe_stage,
)
from drakkar.executor import ExecutorPool
from drakkar.handler import BaseDrakkarHandler
from drakkar.models import (
    CollectResult,
    ExecutorResult,
    ExecutorTask,
    FilePayload,
    HttpPayload,
    KafkaPayload,
    MessageGroup,
    MongoPayload,
    PendingContext,
    PostgresPayload,
    PrecomputedResult,
    RedisPayload,
    SourceMessage,
    make_task_id,
)

# --- shared fixtures --------------------------------------------------------


class _TinyOutput(BaseModel):
    """Minimal BaseModel used as the ``data`` field of every sink payload in tests."""

    id: int = 1
    note: str = 'hello'


def _fresh_cache() -> Cache:
    """Build a real Cache with a fixed worker id (mirrors the convention in test_cache_api.py)."""
    return Cache(origin_worker_id='worker-test')


def _make_proxy(
    *,
    real: Cache | NoOpCache | None = None,
    use_cache: bool,
) -> DebugCacheProxy:
    """Construct a DebugCacheProxy around ``real`` (defaults to a fresh Cache)."""
    return DebugCacheProxy(
        real=real if real is not None else _fresh_cache(),
        use_cache=use_cache,
        start_time=time.monotonic(),
    )


@pytest.fixture(autouse=True)
def _reset_probe_stage():
    """Reset the probe-stage contextvar between tests so previous tests don't leak.

    Uses ``ContextVar.set`` + ``reset`` token pattern to guarantee the
    default ('unknown') is restored even if the test raises.
    """
    token = _probe_stage.set('unknown')
    yield
    _probe_stage.reset(token)


# --- _make_value_preview ----------------------------------------------------


def test_make_value_preview_returns_none_for_none():
    """``None`` stays ``None`` so the UI cell renders empty rather than the literal 'None'."""
    assert _make_value_preview(None) is None


def test_make_value_preview_truncates_long_strings():
    """Preview truncates at 120 chars (117 + '...') so it fits on one UI row."""
    preview = _make_value_preview('x' * 300)
    assert preview is not None
    assert len(preview) == 120
    assert preview.endswith('...')


def test_make_value_preview_collapses_newlines():
    """Multiline values are collapsed to one line for the cache-call table."""
    preview = _make_value_preview('line1\nline2\r\nline3')
    assert preview is not None
    assert '\n' not in preview
    assert '\r' not in preview


def test_make_value_preview_handles_dict():
    """dict values preview as their repr (verbatim structure)."""
    preview = _make_value_preview({'a': 1, 'b': 'two'})
    assert preview is not None
    # repr of a small dict fits comfortably under the 120-char cap
    assert "'a': 1" in preview


# --- DebugCacheProxy: get / peek / contains with use_cache=True -------------


async def test_proxy_get_forwards_to_real_cache_when_use_cache_true():
    """With use_cache=True, ``get`` returns the real cache's value on a seeded key."""
    real = _fresh_cache()
    real.set('k', 'hello')
    proxy = _make_proxy(real=real, use_cache=True)

    value = await proxy.get('k')
    assert value == 'hello'
    # One call logged with hit outcome
    assert len(proxy.calls) == 1
    entry = proxy.calls[0]
    assert entry.op == 'get'
    assert entry.outcome == 'hit'
    assert entry.key == 'k'


async def test_proxy_get_returns_miss_when_key_absent_and_use_cache_true():
    """With use_cache=True but no seeded value, ``get`` still logs a miss."""
    proxy = _make_proxy(use_cache=True)
    value = await proxy.get('missing')
    assert value is None
    assert len(proxy.calls) == 1
    assert proxy.calls[0].outcome == 'miss'


async def test_proxy_get_suppresses_read_when_use_cache_false():
    """With use_cache=False, ``get`` never calls the real cache, always reports miss."""
    real = _fresh_cache()
    real.set('k', 'seeded')
    proxy = _make_proxy(real=real, use_cache=False)

    value = await proxy.get('k')
    assert value is None
    assert len(proxy.calls) == 1
    assert proxy.calls[0].outcome == 'miss'


def test_proxy_peek_forwards_when_use_cache_true():
    """Peek forwards to the real cache only when use_cache is True."""
    real = _fresh_cache()
    real.set('k', 'cached')
    proxy = _make_proxy(real=real, use_cache=True)

    assert proxy.peek('k') == 'cached'
    assert len(proxy.calls) == 1
    assert proxy.calls[0].op == 'peek'
    assert proxy.calls[0].outcome == 'hit'


def test_proxy_peek_suppresses_when_use_cache_false():
    """Peek with use_cache=False returns None and never touches the real cache."""
    real = _fresh_cache()
    real.set('k', 'cached')
    proxy = _make_proxy(real=real, use_cache=False)
    assert proxy.peek('k') is None
    assert proxy.calls[0].outcome == 'miss'


def test_proxy_contains_forwards_when_use_cache_true():
    """__contains__ respects the real cache when use_cache=True."""
    real = _fresh_cache()
    real.set('k', 'v')
    proxy = _make_proxy(real=real, use_cache=True)
    assert 'k' in proxy
    assert 'other' not in proxy
    assert len(proxy.calls) == 2
    assert proxy.calls[0].op == 'contains' and proxy.calls[0].outcome == 'hit'
    assert proxy.calls[1].op == 'contains' and proxy.calls[1].outcome == 'miss'


def test_proxy_contains_suppresses_when_use_cache_false():
    """__contains__ with use_cache=False always reports False."""
    real = _fresh_cache()
    real.set('k', 'v')
    proxy = _make_proxy(real=real, use_cache=False)
    assert 'k' not in proxy
    assert proxy.calls[0].outcome == 'miss'


# --- DebugCacheProxy: set / delete always suppressed ------------------------


def test_proxy_set_is_suppressed_regardless_of_use_cache():
    """Writes NEVER touch the real cache. Tested with both use_cache values."""
    for use_cache in (True, False):
        real = _fresh_cache()
        proxy = _make_proxy(real=real, use_cache=use_cache)
        proxy.set('k', 'value-that-must-not-be-stored')
        # Real cache stays empty on both branches.
        assert real.peek('k') is None
        # Call is still logged as 'suppressed'.
        assert proxy.calls[-1].op == 'set'
        assert proxy.calls[-1].outcome == 'suppressed'


def test_proxy_set_records_scope_in_call_log():
    """The scope kwarg is reflected in the ProbeCacheCall.scope field."""
    proxy = _make_proxy(use_cache=False)
    proxy.set('k', 'v', scope=CacheScope.CLUSTER)
    assert proxy.calls[-1].scope == 'CLUSTER'


def test_proxy_set_records_value_preview():
    """The value passed to set shows up in the call log as a short preview."""
    proxy = _make_proxy(use_cache=False)
    proxy.set('k', {'nested': {'value': 42}})
    preview = proxy.calls[-1].value_preview
    assert preview is not None
    assert 'nested' in preview


def test_proxy_delete_is_suppressed():
    """``delete`` never forwards to the real cache; returns False unconditionally."""
    real = _fresh_cache()
    real.set('k', 'v')
    proxy = _make_proxy(real=real, use_cache=True)
    result = proxy.delete('k')
    assert result is False
    # Real cache still has the value — the delete was not propagated.
    assert real.peek('k') == 'v'
    assert proxy.calls[-1].op == 'delete'
    assert proxy.calls[-1].outcome == 'suppressed'


# --- DebugCacheProxy: origin_stage from contextvar --------------------------


async def test_proxy_call_log_captures_origin_stage():
    """Every call picks up the current ``_probe_stage`` contextvar value."""
    proxy = _make_proxy(use_cache=False)

    token = _probe_stage.set('arrange')
    try:
        await proxy.get('k')
    finally:
        _probe_stage.reset(token)

    token = _probe_stage.set('task_complete:t-abc')
    try:
        proxy.set('k2', 'v2')
    finally:
        _probe_stage.reset(token)

    assert proxy.calls[0].origin_stage == 'arrange'
    assert proxy.calls[1].origin_stage == 'task_complete:t-abc'


async def test_proxy_ms_since_start_is_monotonic():
    """Call-log timestamps increase monotonically across calls.

    Hand-waving small tolerances: asyncio.sleep inflates the gap to a
    positive number big enough for float comparisons.
    """
    proxy = _make_proxy(use_cache=False)
    await proxy.get('first')
    # Busy-wait briefly instead of sleep to keep the test deterministic
    # without introducing scheduler-dependent timing.
    target = time.monotonic() + 0.005
    while time.monotonic() < target:
        pass
    await proxy.get('second')
    assert proxy.calls[1].ms_since_start > proxy.calls[0].ms_since_start


# --- DebugCacheProxy: NoOpCache as backend ----------------------------------


async def test_proxy_forwards_to_noopcache_without_error():
    """The proxy accepts NoOpCache as the real backend — all reads miss."""
    proxy = DebugCacheProxy(real=NoOpCache(), use_cache=True, start_time=time.monotonic())
    assert await proxy.get('k') is None
    assert proxy.peek('k') is None
    assert 'k' not in proxy
    # All three should be misses.
    assert [c.outcome for c in proxy.calls] == ['miss', 'miss', 'miss']


# --- DebugSinkCollector -----------------------------------------------------


async def test_sink_collector_captures_across_stages():
    """Each captured entry records the ``_probe_stage`` at the moment of capture."""
    collector = DebugSinkCollector()
    cr1 = CollectResult(kafka=[KafkaPayload(data=_TinyOutput())])
    cr2 = CollectResult(kafka=[KafkaPayload(data=_TinyOutput(id=2))])

    token = _probe_stage.set('task_complete:t-1')
    try:
        await collector(cr1, 0)
    finally:
        _probe_stage.reset(token)

    token = _probe_stage.set('message_complete')
    try:
        await collector(cr2, 0)
    finally:
        _probe_stage.reset(token)

    assert [stage for stage, _ in collector.entries] == ['task_complete:t-1', 'message_complete']


async def test_sink_collector_ignores_partition_id():
    """partition_id is accepted but does not mutate collector state beyond ordering."""
    collector = DebugSinkCollector()
    cr = CollectResult(kafka=[KafkaPayload(data=_TinyOutput())])
    # Pass a non-zero partition_id; collector should still work identically
    await collector(cr, partition_id=7)
    assert len(collector.entries) == 1


async def test_sink_collector_flatten_kafka_payload():
    """Kafka payload flattens with destination=sink name and key in extras (decoded)."""
    collector = DebugSinkCollector()
    cr = CollectResult(
        kafka=[
            KafkaPayload(sink='results', key=b'my-key', data=_TinyOutput(id=10)),
        ]
    )
    token = _probe_stage.set('task_complete:t-1')
    try:
        await collector(cr, 0)
    finally:
        _probe_stage.reset(token)
    flat = collector.flatten()
    assert len(flat) == 1
    rec = flat[0]
    assert rec.sink_type == 'kafka'
    assert rec.destination == 'results'
    assert rec.origin_stage == 'task_complete:t-1'
    assert rec.payload == {'id': 10, 'note': 'hello'}
    assert rec.extras == {'sink_instance': 'results', 'key': 'my-key'}


async def test_sink_collector_flatten_kafka_default_sink():
    """Kafka payload with empty sink name uses '(default)' as the destination label."""
    collector = DebugSinkCollector()
    cr = CollectResult(kafka=[KafkaPayload(data=_TinyOutput())])
    await collector(cr, 0)
    flat = collector.flatten()
    assert flat[0].destination == '(default)'
    assert flat[0].extras == {'sink_instance': '', 'key': None}


async def test_sink_collector_flatten_postgres_payload():
    """Postgres payload flattens with destination=table."""
    collector = DebugSinkCollector()
    cr = CollectResult(postgres=[PostgresPayload(table='results', data=_TinyOutput())])
    await collector(cr, 0)
    flat = collector.flatten()
    assert flat[0].sink_type == 'postgres'
    assert flat[0].destination == 'results'


async def test_sink_collector_flatten_mongo_payload():
    """Mongo payload flattens with destination=collection."""
    collector = DebugSinkCollector()
    cr = CollectResult(mongo=[MongoPayload(collection='events', data=_TinyOutput())])
    await collector(cr, 0)
    flat = collector.flatten()
    assert flat[0].sink_type == 'mongo'
    assert flat[0].destination == 'events'


async def test_sink_collector_flatten_http_payload():
    """HTTP payload flattens with destination=sink instance name."""
    collector = DebugSinkCollector()
    cr = CollectResult(http=[HttpPayload(sink='webhook', data=_TinyOutput())])
    await collector(cr, 0)
    flat = collector.flatten()
    assert flat[0].sink_type == 'http'
    assert flat[0].destination == 'webhook'


async def test_sink_collector_flatten_redis_payload():
    """Redis payload flattens with destination=key; TTL is carried in extras."""
    collector = DebugSinkCollector()
    cr = CollectResult(redis=[RedisPayload(key='k-1', data=_TinyOutput(), ttl=60)])
    await collector(cr, 0)
    flat = collector.flatten()
    assert flat[0].sink_type == 'redis'
    assert flat[0].destination == 'k-1'
    assert flat[0].extras['ttl'] == 60


async def test_sink_collector_flatten_file_payload():
    """File payload flattens with destination=path."""
    collector = DebugSinkCollector()
    cr = CollectResult(files=[FilePayload(path='/var/log/out.jsonl', data=_TinyOutput())])
    await collector(cr, 0)
    flat = collector.flatten()
    assert flat[0].sink_type == 'files'
    assert flat[0].destination == '/var/log/out.jsonl'


async def test_sink_collector_flatten_multiple_types_in_one_collect_result():
    """A single CollectResult with multiple sink fields produces one PlannedSinkRecord per payload."""
    collector = DebugSinkCollector()
    cr = CollectResult(
        kafka=[KafkaPayload(data=_TinyOutput())],
        postgres=[PostgresPayload(table='t', data=_TinyOutput()), PostgresPayload(table='t2', data=_TinyOutput())],
        mongo=[MongoPayload(collection='c', data=_TinyOutput())],
    )
    await collector(cr, 0)
    flat = collector.flatten()
    # 1 kafka + 2 postgres + 1 mongo = 4
    assert len(flat) == 4
    sink_types = [r.sink_type for r in flat]
    # Order follows the canonical CollectResult field order
    assert sink_types == ['kafka', 'postgres', 'postgres', 'mongo']


async def test_sink_collector_flatten_empty():
    """Flatten on no captures returns empty list."""
    collector = DebugSinkCollector()
    assert collector.flatten() == []


async def test_sink_collector_flatten_preserves_entry_order():
    """When two CollectResults are captured across stages, their origin_stage is preserved."""
    collector = DebugSinkCollector()
    token = _probe_stage.set('task_complete:t-a')
    try:
        await collector(CollectResult(kafka=[KafkaPayload(data=_TinyOutput(id=1))]), 0)
    finally:
        _probe_stage.reset(token)
    token = _probe_stage.set('message_complete')
    try:
        await collector(CollectResult(kafka=[KafkaPayload(data=_TinyOutput(id=2))]), 0)
    finally:
        _probe_stage.reset(token)
    flat = collector.flatten()
    assert [r.origin_stage for r in flat] == ['task_complete:t-a', 'message_complete']
    # IDs preserved in the same order
    assert [r.payload['id'] for r in flat] == [1, 2]


# --- DebugReport round-trip -------------------------------------------------


def test_debug_report_serialize_deserialize_round_trip():
    """A full DebugReport can be JSON-serialized and restored byte-for-byte.

    This is the contract the HTTP endpoint depends on: FastAPI will
    ``model_dump()`` the return value into JSON, and the frontend sends
    it back in subsequent requests (e.g. Re-run). Round-trip equality
    guards against accidentally adding a field that doesn't serialize
    cleanly.
    """
    report = DebugReport(
        input=ProbeInput(value='{"x": 1}', key='k', partition=2, offset=100, topic='t'),
        parsed_payload={'x': 1},
        message_label='2:100',
        arrange=ProbeStageResult(duration_seconds=0.01),
        tasks=[
            ProbeTaskEntry(
                task_id='t-1',
                labels={'req': 'abc'},
                source_offsets=[100],
                precomputed=False,
                status='done',
                exit_code=0,
                duration_seconds=0.05,
                stdout='out',
                stderr='',
            ),
        ],
        planned_sink_payloads=[
            PlannedSinkRecord(
                sink_type='kafka',
                destination='results',
                origin_stage='task_complete:t-1',
                payload={'id': 1, 'note': 'hello'},
                extras={'sink_instance': 'results', 'key': None},
            ),
        ],
        cache_calls=[
            ProbeCacheCall(
                op='get',
                key='k',
                scope='-',
                outcome='miss',
                origin_stage='arrange',
                ms_since_start=0.5,
            ),
        ],
        cache_summary={'calls': 1, 'hits': 0, 'misses': 1, 'writes_suppressed': 0},
        timing={'total_wallclock': 0.1, 'arrange': 0.01},
        errors=[
            ProbeError(
                stage='on_task_complete:t-1',
                exception_class='ValueError',
                message='bad',
                traceback='Traceback...',
                occurred_at_ms=20.0,
            ),
        ],
    )

    as_json = report.model_dump_json()
    restored = DebugReport.model_validate_json(as_json)
    assert restored == report


def test_debug_report_defaults_produce_valid_empty_shape():
    """A minimal DebugReport with only mandatory fields is valid."""
    report = DebugReport(
        input=ProbeInput(value=''),
        arrange=ProbeStageResult(),
    )
    # All list/dict fields default to empty, optional fields default to None.
    assert report.tasks == []
    assert report.cache_calls == []
    assert report.errors == []
    assert report.on_message_complete is None
    assert report.on_window_complete is None
    assert report.truncated is False


def test_probe_input_defaults():
    """ProbeInput accepts just ``value``; every other field has a safe default."""
    pi = ProbeInput(value='{}')
    assert pi.key is None
    assert pi.partition == 0
    assert pi.offset == 0
    assert pi.topic == ''
    assert pi.timestamp is None
    assert pi.use_cache is False


def test_probe_task_entry_status_literal_rejects_invalid():
    """Pydantic rejects status values outside the 'done'/'failed'/'replaced' set."""
    with pytest.raises(ValidationError):
        ProbeTaskEntry(
            task_id='t',
            source_offsets=[],
            status='weird',  # type: ignore[arg-type]
        )


# --- DebugRunner: happy path ------------------------------------------------
#
# These tests exercise the orchestration layer added in Task 2a. They use
# precomputed tasks (``ExecutorTask.precomputed``) so the test does not
# depend on a real binary living at a specific path; the executor's fast
# path returns a synthetic ``ExecutorResult`` from those fields directly.
#
# A small concrete handler subclass is defined per test so each test
# controls exactly which hooks fire and what they return.


def _make_executor_pool() -> ExecutorPool:
    """Real ExecutorPool; precomputed tasks never hit the binary path.

    The binary points to a deliberately bogus path to ensure the tests
    fail loudly if the precomputed fast path ever regresses and a real
    subprocess spawn is attempted.
    """
    return ExecutorPool(
        binary_path='/nonexistent/binary/should-never-run',
        max_executors=2,
        task_timeout_seconds=5,
    )


def _make_config() -> DrakkarConfig:
    """Minimal DrakkarConfig — only executor block is read by the runner in task 2a."""
    return DrakkarConfig(
        executor=ExecutorConfig(
            binary_path='/nonexistent/binary/should-never-run',
            task_timeout_seconds=5,
            max_retries=1,
        )
    )


def _make_precomputed_task(
    *,
    task_id: str | None = None,
    offset: int = 0,
    stdout: str = 'ok',
    labels: dict[str, str] | None = None,
    stdin: str | None = None,
) -> ExecutorTask:
    """Build an ExecutorTask whose outcome is supplied inline.

    Using ``precomputed`` means ``ExecutorPool.execute`` synthesises the
    ExecutorResult without running a subprocess. Ideal for unit tests of
    orchestration logic.
    """
    return ExecutorTask(
        task_id=task_id or make_task_id(),
        source_offsets=[offset],
        labels=labels or {},
        stdin=stdin,
        precomputed=PrecomputedResult(
            stdout=stdout,
            stderr='',
            exit_code=0,
            duration_seconds=0.001,
        ),
    )


class _HappyPathHandler(BaseDrakkarHandler):
    """Handler that produces N precomputed tasks and emits one Kafka record per hook.

    - ``arrange`` returns ``task_count`` precomputed tasks tied to the
      incoming message.
    - ``on_task_complete`` returns a CollectResult with one kafka payload
      per task.
    - ``on_message_complete`` returns a CollectResult with one kafka
      payload summarising the message.
    - ``on_window_complete`` returns a CollectResult with one kafka
      payload summarising the window.

    Every hook increments a visible counter on the handler so tests can
    assert each was called exactly once (or ``task_count`` times for
    on_task_complete).
    """

    def __init__(self, task_count: int = 2) -> None:
        self._task_count = task_count
        self.arrange_calls = 0
        self.on_task_complete_calls = 0
        self.on_message_complete_calls = 0
        self.on_window_complete_calls = 0
        # Optional hook for mid-run partial-report inspection. Tests can
        # assign a callable taking (handler, result) to snapshot the
        # runner's state between task completions.
        self.on_task_complete_hook: object | None = None

    async def arrange(
        self,
        messages: list[SourceMessage],
        pending: PendingContext,
    ) -> list[ExecutorTask]:
        self.arrange_calls += 1
        msg = messages[0]
        return [
            _make_precomputed_task(
                task_id=f't-{i}',
                offset=msg.offset,
                stdout=f'stdout-{i}',
                labels={'idx': str(i)},
                stdin=f'stdin-{i}',
            )
            for i in range(self._task_count)
        ]

    async def on_task_complete(self, result: ExecutorResult) -> CollectResult | None:
        self.on_task_complete_calls += 1
        hook = self.on_task_complete_hook
        if hook is not None:
            hook(self, result)  # type: ignore[operator]
        return CollectResult(
            kafka=[
                KafkaPayload(
                    sink='results',
                    key=result.task.task_id.encode(),
                    data=_TinyOutput(id=self.on_task_complete_calls, note='task'),
                ),
            ],
        )

    async def on_message_complete(self, group: MessageGroup) -> CollectResult | None:
        self.on_message_complete_calls += 1
        return CollectResult(
            kafka=[KafkaPayload(sink='rollup', data=_TinyOutput(id=group.succeeded, note='msg'))],
        )

    async def on_window_complete(
        self,
        results: list[ExecutorResult],
        source_messages: list[SourceMessage],
    ) -> CollectResult | None:
        self.on_window_complete_calls += 1
        return CollectResult(
            kafka=[KafkaPayload(sink='window', data=_TinyOutput(id=len(results), note='window'))],
        )


async def test_runner_happy_path_runs_full_sequence():
    """Two tasks, all hooks fire, report is well-formed, truncated=False."""
    handler = _HappyPathHandler(task_count=2)
    runner = DebugRunner(
        handler=handler,
        executor_pool=_make_executor_pool(),
        app_config=_make_config(),
    )

    probe_input = ProbeInput(
        value='{"hello": "world"}',
        key='k-1',
        partition=3,
        offset=42,
        topic='in',
    )

    report = await runner.run(probe_input)

    # Every hook fired the expected number of times.
    assert handler.arrange_calls == 1
    assert handler.on_task_complete_calls == 2
    assert handler.on_message_complete_calls == 1
    assert handler.on_window_complete_calls == 1

    # Report has all top-level structures populated.
    assert report.input == probe_input
    assert report.truncated is False
    assert report.arrange.duration_seconds is not None
    assert report.on_message_complete is not None
    assert report.on_window_complete is not None
    # Tasks: 2, both with status=done and exit_code=0
    assert len(report.tasks) == 2
    assert all(t.status == 'done' for t in report.tasks)
    assert all(t.exit_code == 0 for t in report.tasks)
    assert [t.task_id for t in report.tasks] == ['t-0', 't-1']
    # Labels and stdin survive the round trip.
    assert report.tasks[0].labels == {'idx': '0'}
    assert report.tasks[0].stdin == 'stdin-0'
    # Precomputed flag reflects ExecutorTask.precomputed.
    assert all(t.precomputed is True for t in report.tasks)
    # on_task_complete results attached to each task entry.
    assert all(t.on_task_complete_result is not None for t in report.tasks)

    # Planned sink payloads: 2 from on_task_complete + 1 from
    # on_message_complete + 1 from on_window_complete = 4 total Kafka records.
    assert len(report.planned_sink_payloads) == 4
    sinks_by_stage = {r.origin_stage: r for r in report.planned_sink_payloads}
    assert 'task_complete:t-0' in sinks_by_stage
    assert 'task_complete:t-1' in sinks_by_stage
    assert 'message_complete' in sinks_by_stage
    assert 'window_complete' in sinks_by_stage

    # No errors captured.
    assert report.errors == []

    # Timing dict populated for every stage.
    assert 'arrange' in report.timing
    assert 'on_message_complete' in report.timing
    assert 'on_window_complete' in report.timing
    assert 'total_wallclock' in report.timing


async def test_runner_populates_all_report_sections():
    """Spot-check every stage's output landed in the final DebugReport.

    Keeps the happy-path test above focused on counts; this test confirms
    the *content* routed to each section.
    """
    handler = _HappyPathHandler(task_count=1)
    runner = DebugRunner(
        handler=handler,
        executor_pool=_make_executor_pool(),
        app_config=_make_config(),
    )

    report = await runner.run(
        ProbeInput(value='"x"', partition=0, offset=7, topic='t'),
    )

    # Section A: parsed_payload + message_label echoed.
    # _HappyPathHandler has no input_model so parsed_payload stays None.
    assert report.parsed_payload is None
    assert report.message_label == '0:7'

    # Section B: arrange duration present and non-negative.
    assert report.arrange.duration_seconds is not None
    assert report.arrange.duration_seconds >= 0

    # Section C: one task entry with expected fields.
    assert len(report.tasks) == 1
    task = report.tasks[0]
    assert task.stdout == 'stdout-0'
    assert task.on_task_complete_duration is not None

    # Sections D & E: on_message_complete + on_window_complete captured
    # with their CollectResults attached.
    assert report.on_message_complete is not None
    assert report.on_message_complete.collect_result is not None
    assert len(report.on_message_complete.collect_result.kafka) == 1
    assert report.on_window_complete is not None
    assert report.on_window_complete.collect_result is not None
    assert len(report.on_window_complete.collect_result.kafka) == 1


async def test_runner_restores_handler_cache_after_run():
    """handler.cache is swapped during the probe and restored on exit."""
    handler = _HappyPathHandler(task_count=1)
    original_cache = handler.cache  # class-level NoOpCache

    runner = DebugRunner(
        handler=handler,
        executor_pool=_make_executor_pool(),
        app_config=_make_config(),
    )

    await runner.run(ProbeInput(value='x', offset=1))

    # After the run returns, handler.cache is the exact original object,
    # not a DebugCacheProxy. Identity (``is``) is the strong guarantee.
    assert handler.cache is original_cache


# --- DebugRunner: mid-run partial report ------------------------------------


async def test_runner_latest_partial_report_mid_run_is_truncated():
    """latest_partial_report() called mid-run reflects only completed-so-far tasks.

    The handler's on_task_complete hook inspects the runner's partial
    state the moment task 0 finishes, before task 1 has been submitted.
    At that point the partial should contain exactly one task entry and
    latest_partial_report should return truncated=True.
    """
    captured: dict[str, Any] = {}
    handler = _HappyPathHandler(task_count=2)
    runner = DebugRunner(
        handler=handler,
        executor_pool=_make_executor_pool(),
        app_config=_make_config(),
    )

    # Hook: the FIRST time on_task_complete fires, snapshot the
    # runner's partial state. We want to verify that:
    #   - only the task whose on_task_complete just ran is present
    #   - truncated=True
    #   - arrange already completed (so it has a duration)
    def _snapshot(h: _HappyPathHandler, result: ExecutorResult) -> None:
        if h.on_task_complete_calls == 1:
            captured['report'] = runner.latest_partial_report()

    handler.on_task_complete_hook = _snapshot

    final = await runner.run(ProbeInput(value='x', offset=1))

    partial: DebugReport = captured['report']
    assert partial.truncated is True
    # At snapshot time, the current task has just completed its
    # execution but its on_task_complete_result is being built. The
    # runner appends the ProbeTaskEntry to partial['tasks'] AFTER the
    # hook returns, so inside the hook the partial still reflects zero
    # completed tasks.
    assert len(partial.tasks) == 0
    # arrange completed before any task ran.
    assert partial.arrange.duration_seconds is not None
    # on_message_complete / on_window_complete have not fired yet.
    assert partial.on_message_complete is None
    assert partial.on_window_complete is None
    # input was set at the start of run().
    assert partial.input.value == 'x'

    # The full run still completes correctly.
    assert final.truncated is False
    assert len(final.tasks) == 2


async def test_runner_partial_report_without_run_returns_empty_shape():
    """latest_partial_report() before run() ever ran returns a valid empty report."""
    handler = _HappyPathHandler(task_count=0)
    runner = DebugRunner(
        handler=handler,
        executor_pool=_make_executor_pool(),
        app_config=_make_config(),
    )
    report = runner.latest_partial_report()
    assert report.truncated is True
    assert report.tasks == []
    assert report.input.value == ''  # synthetic empty ProbeInput


# --- Task 2b: Safety guarantee negative-assertion tests ---------------------
#
# The Message Probe promises a *zero-footprint* run: no rows written, no
# offsets committed, no sinks called, no cache mutations, no
# PartitionProcessor state touched. These six tests enforce the
# guarantees listed in the Solution Overview by actively sabotaging any
# module the probe MUST NOT touch — patched methods raise AssertionError
# if they are reached, so any accidental regression fails loudly.
#
# Each test pairs with one bullet in the "Safety guarantees" list of
# docs/plans/20260422-message-probe-debug-tab.md; keeping the mapping
# 1:1 makes it obvious when a promise is unprotected.


class _CacheWritingHandler(_HappyPathHandler):
    """Variant of _HappyPathHandler that also calls ``self.cache.set(...)`` during hooks.

    Used by guarantee #3 to prove that three writes inside a single
    probe do NOT mutate the live Cache's ``_dirty`` / ``_bytes_sum``
    state, and that all three show up in the report with
    ``outcome='suppressed'``.
    """

    async def arrange(
        self,
        messages: list[SourceMessage],
        pending: PendingContext,
    ) -> list[ExecutorTask]:
        # One write during arrange
        self.cache.set('arrange-key', {'stage': 'arrange'})
        return await super().arrange(messages, pending)

    async def on_task_complete(self, result: ExecutorResult) -> CollectResult | None:
        # One write per task_complete invocation
        self.cache.set(f'task-key-{result.task.task_id}', {'stage': 'task'})
        return await super().on_task_complete(result)


class _TaskCompleteRaisingHandler(_HappyPathHandler):
    """Variant that raises inside ``on_task_complete`` so we can prove cache restore."""

    async def on_task_complete(self, result: ExecutorResult) -> CollectResult | None:
        self.on_task_complete_calls += 1
        raise RuntimeError('boom from on_task_complete')


# ---- guarantee #1: EventRecorder never called during a probe --------------


async def test_runner_never_calls_event_recorder():
    """Every EventRecorder.record_* invocation would raise — probe must never hit them.

    The runner calls ``executor_pool.execute(task, recorder=None, ...)``
    so every ``recorder.record_*`` call is guarded by ``if recorder:``
    inside the executor. We also capture the kwargs passed to
    ``ExecutorPool.execute`` and assert ``recorder=None`` each time, so
    a regression in the runner (forgetting to pass ``recorder=None``)
    fails this test even before the recorder itself can raise.
    """
    handler = _HappyPathHandler(task_count=2)
    runner = DebugRunner(
        handler=handler,
        executor_pool=_make_executor_pool(),
        app_config=_make_config(),
    )

    # Record every ExecutorPool.execute call to verify recorder=None.
    captured_kwargs: list[dict[str, Any]] = []
    original_execute = ExecutorPool.execute

    async def _spy_execute(self_pool: ExecutorPool, task: ExecutorTask, **kwargs: Any) -> ExecutorResult:
        captured_kwargs.append(kwargs)
        return await original_execute(self_pool, task, **kwargs)

    # Patch every record_* method on EventRecorder to raise. If any of them
    # fires during the probe, the test fails loudly.
    record_method_names = [
        name
        for name in dir(drakkar.recorder.EventRecorder)
        if name.startswith('record_') and callable(getattr(drakkar.recorder.EventRecorder, name))
    ]
    # Sanity: we really did find at least a handful of record_* methods
    # (guards against a typo / rename that silently neutralises this test).
    assert len(record_method_names) >= 5

    # Patch every record_* method and the executor.execute spy inside a
    # single ExitStack so all patches are unwound cleanly when the block
    # exits (even if runner.run raises).
    with ExitStack() as stack:
        stack.enter_context(patch.object(ExecutorPool, 'execute', _spy_execute))
        for name in record_method_names:
            stack.enter_context(
                patch.object(
                    drakkar.recorder.EventRecorder,
                    name,
                    side_effect=AssertionError(f'EventRecorder.{name} called during probe'),
                )
            )
        report = await runner.run(ProbeInput(value='x', offset=1))

    assert report.errors == []
    # Every execute call received recorder=None explicitly.
    assert len(captured_kwargs) == 2
    for kw in captured_kwargs:
        assert kw.get('recorder') is None, f'expected recorder=None, got {kw!r}'


# ---- guarantee #2: SinkManager never instantiated or called ---------------


async def test_runner_never_instantiates_or_calls_sink_manager():
    """SinkManager.__init__ and deliver_all would raise — probe must never hit them.

    The runner replaces the real SinkManager with a DebugSinkCollector;
    the production class should stay untouched for the whole probe.
    """
    handler = _HappyPathHandler(task_count=1)
    runner = DebugRunner(
        handler=handler,
        executor_pool=_make_executor_pool(),
        app_config=_make_config(),
    )

    # Sabotage both construction and the main delivery entrypoint.
    with (
        patch.object(
            drakkar.sinks.manager.SinkManager,
            '__init__',
            side_effect=AssertionError('SinkManager constructed during probe'),
        ),
        patch.object(
            drakkar.sinks.manager.SinkManager,
            'deliver_all',
            side_effect=AssertionError('SinkManager.deliver_all called during probe'),
        ),
    ):
        report = await runner.run(ProbeInput(value='x', offset=1))

    # Probe finished cleanly — handler's CollectResults went to the
    # DebugSinkCollector, not SinkManager.
    assert report.errors == []
    # The in-memory sink collector still produced the planned records
    # from the CollectResult fields the handler returned.
    assert len(report.planned_sink_payloads) >= 1


# ---- guarantee #3: Cache._dirty and _bytes_sum unchanged -------------------


async def test_runner_does_not_mutate_live_cache_dirty_state():
    """A handler that calls ``cache.set(...)`` three times must leave the live Cache untouched.

    The real Cache accumulates pending writes in ``_dirty`` and tracks a
    running ``_bytes_sum``. After a probe, both must be byte-identical
    to their pre-probe snapshot, and the report must show 3 suppressed
    set calls.
    """
    real_cache = _fresh_cache()
    handler = _CacheWritingHandler(task_count=2)
    # Attach the real cache as the handler's cache so the proxy wraps it.
    handler.cache = real_cache

    runner = DebugRunner(
        handler=handler,
        executor_pool=_make_executor_pool(),
        app_config=_make_config(),
    )

    initial_dirty_len = len(real_cache._dirty)
    initial_bytes_sum = real_cache._bytes_sum

    report = await runner.run(ProbeInput(value='x', offset=1))

    # The live cache state is *exactly* what it was before the probe.
    assert len(real_cache._dirty) == initial_dirty_len
    assert real_cache._bytes_sum == initial_bytes_sum
    # And the handler's cache attribute was restored to the real cache
    # (not the proxy).
    assert handler.cache is real_cache

    # The report captured all three set calls as suppressed writes:
    # 1 in arrange + 1 per task_complete (2 tasks).
    set_calls = [c for c in report.cache_calls if c.op == 'set']
    assert len(set_calls) == 3
    assert all(c.outcome == 'suppressed' for c in set_calls)


# ---- guarantee #4: no offset-commit path reached --------------------------


async def test_runner_never_reaches_offset_commit_path():
    """PartitionProcessor._try_commit and OffsetTracker mutations must never fire.

    The probe bypasses PartitionProcessor entirely, so its offset state
    machine (OffsetTracker.register / complete / acknowledge_commit)
    and the committing coroutine (_try_commit) should remain cold.
    """
    handler = _HappyPathHandler(task_count=1)
    runner = DebugRunner(
        handler=handler,
        executor_pool=_make_executor_pool(),
        app_config=_make_config(),
    )

    # Patch PartitionProcessor's commit helper and every OffsetTracker
    # method that mutates state. Read-only methods (pending_count etc.)
    # are left alone — they'd fire during metrics scrapes but not during
    # the probe itself.
    with (
        patch.object(
            drakkar.partition.PartitionProcessor,
            '_try_commit',
            side_effect=AssertionError('PartitionProcessor._try_commit called during probe'),
        ),
        patch.object(
            drakkar.offsets.OffsetTracker,
            'register',
            side_effect=AssertionError('OffsetTracker.register called during probe'),
        ),
        patch.object(
            drakkar.offsets.OffsetTracker,
            'complete',
            side_effect=AssertionError('OffsetTracker.complete called during probe'),
        ),
        patch.object(
            drakkar.offsets.OffsetTracker,
            'acknowledge_commit',
            side_effect=AssertionError('OffsetTracker.acknowledge_commit called during probe'),
        ),
    ):
        report = await runner.run(ProbeInput(value='x', offset=1))

    assert report.errors == []


# ---- guarantee #5: handler.cache restored after exception -----------------


async def test_runner_restores_handler_cache_when_on_task_complete_raises():
    """If ``on_task_complete`` raises, the ``finally`` in ``_run_locked`` still restores cache.

    Task 3 will add graceful exception capture (probe returns a report
    with the error instead of propagating). For now, the runner
    propagates the exception, but the cache must still be restored.
    This test handles both cases: it catches the exception if raised,
    and either way asserts ``handler.cache is original_cache``.
    """
    handler = _TaskCompleteRaisingHandler(task_count=1)
    original_cache = handler.cache  # class-level NoOpCache
    runner = DebugRunner(
        handler=handler,
        executor_pool=_make_executor_pool(),
        app_config=_make_config(),
    )

    # pytest.raises doesn't fail if no exception is raised when we use
    # a try/except wrapper manually; using the wrapper instead of
    # pytest.raises keeps the test robust to whether Task 3 has
    # landed or not.
    try:
        await runner.run(ProbeInput(value='x', offset=1))
    except Exception:
        # Task 2a/2b: the runner currently lets the hook exception
        # propagate — Task 3 will catch it. Either path is acceptable
        # for guarantee #5; the point is that cache is still restored.
        pass

    # The strong post-condition: the handler's cache attribute is
    # exactly the original object, not a DebugCacheProxy.
    assert handler.cache is original_cache


# ---- guarantee #6: no PartitionProcessor state touched --------------------


async def test_runner_never_constructs_or_invokes_partition_processor():
    """PartitionProcessor.__init__ and its core internals must never fire during a probe.

    The runner must NOT reach into PartitionProcessor for any reason:
    no construction, no ``_process_window``, no ``_execute_and_track``.
    This is what makes the probe safe in a production worker — we can
    run it without perturbing the live partition state.
    """
    handler = _HappyPathHandler(task_count=1)
    runner = DebugRunner(
        handler=handler,
        executor_pool=_make_executor_pool(),
        app_config=_make_config(),
    )

    with (
        patch.object(
            drakkar.partition.PartitionProcessor,
            '__init__',
            side_effect=AssertionError('PartitionProcessor constructed during probe'),
        ),
        patch.object(
            drakkar.partition.PartitionProcessor,
            '_process_window',
            side_effect=AssertionError('PartitionProcessor._process_window called during probe'),
        ),
        patch.object(
            drakkar.partition.PartitionProcessor,
            '_execute_and_track',
            side_effect=AssertionError('PartitionProcessor._execute_and_track called during probe'),
        ),
        patch.object(
            drakkar.partition.PartitionProcessor,
            '_try_commit',
            side_effect=AssertionError('PartitionProcessor._try_commit called during probe'),
        ),
    ):
        report = await runner.run(ProbeInput(value='x', offset=1))

    assert report.errors == []
    assert len(report.tasks) == 1


# --- Task 2c: use_cache toggle + proxy integration tests -------------------
#
# These tests exercise the ``use_cache`` flag end-to-end through the
# runner. They prove that:
#   - with ``use_cache=True`` reads forward to the live Cache and return
#     hits when keys are present;
#   - with ``use_cache=False`` reads bypass the live Cache entirely and
#     always return miss;
#   - set/delete writes are always suppressed (never touch the live
#     cache) regardless of ``use_cache``;
#   - ``ProbeCacheCall.origin_stage`` reflects which hook made the call
#     (arrange / task_complete:<id> / message_complete);
#   - ``DebugReport.cache_summary`` counts match the cache_calls list.
#
# Each test defines a tiny handler subclass so the cache interaction is
# fully explicit at the test site — easier to follow than a
# configuration-driven helper.


class _CacheGetInArrangeHandler(_HappyPathHandler):
    """Calls ``self.cache.get(...)`` in ``arrange`` and stores the returned value.

    Used to verify that ``use_cache=True`` forwards reads to the live
    cache (returning the seeded value) while ``use_cache=False`` always
    reports a miss even though the live cache has the key.
    """

    def __init__(self, lookup_key: str) -> None:
        super().__init__(task_count=1)
        self._lookup_key = lookup_key
        # Captures the value returned by ``cache.get`` so the test can
        # assert the handler actually saw the hit (not just the report).
        self.seen_value: Any = 'sentinel-not-yet-set'

    async def arrange(
        self,
        messages: list[SourceMessage],
        pending: PendingContext,
    ) -> list[ExecutorTask]:
        # Record what cache.get returned so the test can assert the
        # handler observed the real value (with use_cache=True) or None
        # (with use_cache=False).
        self.seen_value = await self.cache.get(self._lookup_key)
        return await super().arrange(messages, pending)


async def test_probe_with_use_cache_true_returns_seeded_value():
    """use_cache=True: a seeded key is visible to the handler and logs as a hit.

    The live Cache.set puts the entry directly in ``_memory`` so the
    proxy's forwarded ``get`` hits the memory fast path synchronously —
    no reader DB plumbing needed for the test.
    """
    real_cache = _fresh_cache()
    real_cache.set('known', {'a': 1})

    handler = _CacheGetInArrangeHandler(lookup_key='known')
    handler.cache = real_cache

    runner = DebugRunner(
        handler=handler,
        executor_pool=_make_executor_pool(),
        app_config=_make_config(),
    )

    report = await runner.run(ProbeInput(value='x', offset=1, use_cache=True))

    # The handler saw the real value (hit forwarded through the proxy).
    assert handler.seen_value == {'a': 1}

    # The first cache call is the arrange-time get, and it logged as a hit.
    assert len(report.cache_calls) >= 1
    first = report.cache_calls[0]
    assert first.op == 'get'
    assert first.key == 'known'
    assert first.outcome == 'hit'


async def test_probe_with_use_cache_false_returns_miss_despite_seeded_key():
    """use_cache=False: handler gets None even though the live cache has the key.

    The proxy short-circuits the read path entirely when ``use_cache`` is
    False, so the seeded value never leaks into the probe. This is the
    "cold cache simulation" mode — useful for testing handler behaviour
    with an empty cache.
    """
    real_cache = _fresh_cache()
    real_cache.set('known', {'a': 1})

    handler = _CacheGetInArrangeHandler(lookup_key='known')
    handler.cache = real_cache

    runner = DebugRunner(
        handler=handler,
        executor_pool=_make_executor_pool(),
        app_config=_make_config(),
    )

    report = await runner.run(ProbeInput(value='x', offset=1, use_cache=False))

    # The handler saw None — the proxy did not forward the read.
    assert handler.seen_value is None

    first = report.cache_calls[0]
    assert first.op == 'get'
    assert first.key == 'known'
    # Miss despite the live cache containing the key — that's the whole
    # point of the use_cache=False mode.
    assert first.outcome == 'miss'


class _MixedCacheCallsHandler(_HappyPathHandler):
    """Calls ``get`` → ``set`` → ``get`` in a single ``arrange`` invocation.

    Used to verify that write suppression does not leak into follow-up
    reads on the same proxy: even though ``set('foo', ...)`` is in the
    call log as suppressed, the next ``get('foo')`` should still miss
    because the live cache was never mutated and the proxy does not
    cache the suppressed value.
    """

    def __init__(self) -> None:
        super().__init__(task_count=1)
        # Captures the values returned by the two gets so tests can
        # assert both returned None (confirming the second get did not
        # see the suppressed set).
        self.first_get: Any = 'sentinel'
        self.second_get: Any = 'sentinel'

    async def arrange(
        self,
        messages: list[SourceMessage],
        pending: PendingContext,
    ) -> list[ExecutorTask]:
        # First get — key is absent from the live cache, so even with
        # use_cache=True this is a miss.
        self.first_get = await self.cache.get('foo')
        # Write is suppressed — the live cache is not mutated.
        self.cache.set('foo', {'v': 1})
        # Second get — if the suppressed write leaked, this would return
        # {'v': 1}; but the proxy forwards to the live cache which is
        # still empty, so this must be a miss too.
        self.second_get = await self.cache.get('foo')
        return await super().arrange(messages, pending)


async def test_probe_captures_mixed_get_set_get_and_leaves_cache_unchanged():
    """Suppressed writes do NOT leak into follow-up reads on the same proxy.

    The handler does ``get('foo')`` (miss), ``set('foo', ...)``
    (suppressed), ``get('foo')`` (still miss). With use_cache=True the
    read path forwards to the live Cache — since set was suppressed,
    the live Cache remains empty and the second get must also miss.
    """
    real_cache = _fresh_cache()
    # Snapshot the internal state we want to prove stayed unchanged.
    initial_dirty_len = len(real_cache._dirty)
    initial_bytes_sum = real_cache._bytes_sum

    handler = _MixedCacheCallsHandler()
    handler.cache = real_cache

    runner = DebugRunner(
        handler=handler,
        executor_pool=_make_executor_pool(),
        app_config=_make_config(),
    )

    report = await runner.run(ProbeInput(value='x', offset=1, use_cache=True))

    # Both gets returned None — the handler never saw the suppressed write.
    assert handler.first_get is None
    assert handler.second_get is None

    # The first three cache calls are the handler's three interactions,
    # in order. Later calls (if any) would come from downstream hooks,
    # but the default _HappyPathHandler hooks don't touch the cache.
    first_three = report.cache_calls[:3]
    assert [(c.op, c.outcome) for c in first_three] == [
        ('get', 'miss'),
        ('set', 'suppressed'),
        ('get', 'miss'),
    ]
    # All three refer to the same key so the sequencing is unambiguous.
    assert all(c.key == 'foo' for c in first_three)

    # Live cache internals must be byte-identical to the pre-probe snapshot.
    assert len(real_cache._dirty) == initial_dirty_len
    assert real_cache._bytes_sum == initial_bytes_sum


class _OriginStageProbingHandler(_HappyPathHandler):
    """Calls cache.get in each of the three major hooks to verify origin_stage tagging.

    The runner sets ``_probe_stage`` before each hook; the proxy reads
    it when logging every call. This handler exercises the arrange,
    on_task_complete, and on_message_complete stages so the test can
    assert each call's origin_stage matches the hook that made it.
    """

    async def arrange(
        self,
        messages: list[SourceMessage],
        pending: PendingContext,
    ) -> list[ExecutorTask]:
        await self.cache.get('during_arrange')
        return await super().arrange(messages, pending)

    async def on_task_complete(self, result: ExecutorResult) -> CollectResult | None:
        await self.cache.get('during_task_complete')
        return await super().on_task_complete(result)

    async def on_message_complete(self, group: MessageGroup) -> CollectResult | None:
        await self.cache.get('during_message_complete')
        return await super().on_message_complete(group)


async def test_probe_origin_stage_on_call_log_reflects_calling_hook():
    """Each cache call is tagged with the stage/hook that made it.

    The runner sets _probe_stage before each hook call; this test
    asserts that the proxy captures the right stage for three distinct
    hooks (arrange, on_task_complete, on_message_complete). The
    task_complete tag is 'task_complete:<task_id>' so we assert on the
    prefix and then confirm the suffix matches the task's id.
    """
    handler = _OriginStageProbingHandler(task_count=1)
    runner = DebugRunner(
        handler=handler,
        executor_pool=_make_executor_pool(),
        app_config=_make_config(),
    )

    report = await runner.run(ProbeInput(value='x', offset=1, use_cache=False))

    # Build a small lookup: key → call entry. The three keys are unique
    # so this is a 1-to-1 mapping and we can assert each independently.
    by_key = {c.key: c for c in report.cache_calls}

    # arrange-time call is tagged with the bare 'arrange' stage name.
    assert by_key['during_arrange'].origin_stage == 'arrange'

    # on_task_complete-time call carries the running task's id suffix.
    # The happy-path handler emits exactly one task entry, so we can
    # look up the id it generated and assert the tag matches.
    assert len(report.tasks) == 1
    expected_task_id = report.tasks[0].task_id
    tc_stage = by_key['during_task_complete'].origin_stage
    assert tc_stage.startswith('task_complete:')
    assert tc_stage == f'task_complete:{expected_task_id}'

    # on_message_complete-time call uses the bare 'message_complete' name.
    assert by_key['during_message_complete'].origin_stage == 'message_complete'


class _CacheSummaryHandler(_HappyPathHandler):
    """Exercises the cache with a known mix: 2 hits, 3 misses, 1 set, 1 delete.

    Used to verify that ``DebugReport.cache_summary`` counts line up
    exactly with what the call log records. Two hit keys are seeded
    ahead of time; the other three read keys are left absent so they
    miss. One set and one delete are added to confirm both paths count
    toward ``writes_suppressed``.
    """

    async def arrange(
        self,
        messages: list[SourceMessage],
        pending: PendingContext,
    ) -> list[ExecutorTask]:
        # Two hits (seeded) + three misses (not seeded).
        await self.cache.get('hit-1')
        await self.cache.get('hit-2')
        await self.cache.get('miss-1')
        await self.cache.get('miss-2')
        await self.cache.get('miss-3')
        # One set + one delete → both recorded as ``suppressed``.
        self.cache.set('write-1', {'payload': True})
        self.cache.delete('delete-1')
        return await super().arrange(messages, pending)


async def test_probe_cache_summary_counts_match_cache_calls():
    """cache_summary aggregates outcomes: 2 hits, 3 misses, 2 writes_suppressed.

    The handler does 2 get-hits, 3 get-misses, 1 set, 1 delete — seven
    calls total. Both set and delete count as writes_suppressed in the
    summary (the proxy logs both with outcome='suppressed').
    """
    real_cache = _fresh_cache()
    # Seed the two hit keys so arrange's get() forwards hit the memory
    # fast path. The 'miss-*' keys are intentionally absent.
    real_cache.set('hit-1', 'value-1')
    real_cache.set('hit-2', 'value-2')

    handler = _CacheSummaryHandler(task_count=0)
    handler.cache = real_cache

    runner = DebugRunner(
        handler=handler,
        executor_pool=_make_executor_pool(),
        app_config=_make_config(),
    )

    report = await runner.run(ProbeInput(value='x', offset=1, use_cache=True))

    # Exact summary shape, as documented in debug_runner._summarize_cache_calls.
    assert report.cache_summary == {
        'calls': 7,
        'hits': 2,
        'misses': 3,
        'writes_suppressed': 2,
    }
    # Sanity: the summary is derived from cache_calls, so the list must
    # match the same totals.
    outcomes = [c.outcome for c in report.cache_calls]
    assert outcomes.count('hit') == 2
    assert outcomes.count('miss') == 3
    assert outcomes.count('suppressed') == 2
