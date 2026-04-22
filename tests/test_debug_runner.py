"""Tests for the Message Probe debug runner (Task 1).

Task 1 introduces the building blocks of the no-footprint probe:

- ``DebugCacheProxy`` — Cache-shaped wrapper that suppresses writes and
  optionally forwards reads while logging every call.
- ``DebugSinkCollector`` — collects every ``CollectResult`` returned by
  the handler's hooks and flattens them into ``PlannedSinkRecord`` rows
  grouped by sink type.
- The pydantic report models (``ProbeInput``, ``ProbeCacheCall``,
  ``ProbeTaskEntry``, ``ProbeStageResult``, ``ProbeError``,
  ``PlannedSinkRecord``, ``DebugReport``) — exercised via a round-trip
  serialize/deserialize test.

No DebugRunner / endpoint logic is touched here — those land in Tasks 2
and 5 respectively. The tests focus on the surface of these three
pieces and the invariants we promise at the report level.
"""

from __future__ import annotations

import time

import pytest
from pydantic import BaseModel, ValidationError

from drakkar.cache import Cache, CacheScope, NoOpCache
from drakkar.debug_runner import (
    DebugCacheProxy,
    DebugReport,
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
from drakkar.models import (
    CollectResult,
    FilePayload,
    HttpPayload,
    KafkaPayload,
    MongoPayload,
    PostgresPayload,
    RedisPayload,
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
