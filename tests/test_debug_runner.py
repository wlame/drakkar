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

import asyncio
import contextlib
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
    ErrorAction,
    ExecutorError,
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
    """Default the probe-stage contextvar to a probe-mode value for the test body.

    The proxy distinguishes probe-task callers (any non-default
    ``_probe_stage``) from concurrent production callers (the default
    ``'unknown'``) — see ``DebugCacheProxy`` class docstring. Most of the
    proxy tests in this file exercise probe-mode semantics (logging +
    suppression) so we set the contextvar to a sentinel ``'test'`` before
    every test. Tests that intentionally exercise the production-pass-
    through path explicitly reset to ``'unknown'`` inside their own body
    (see the ``concurrent-production isolation`` section below).

    Uses ``ContextVar.set`` + ``reset`` token pattern to guarantee the
    previous value is restored even if the test raises.
    """
    token = _probe_stage.set('test')
    yield
    _probe_stage.reset(token)


# --- _make_value_preview ----------------------------------------------------


def test_make_value_preview_returns_none_for_none():
    """``None`` stays ``None`` so the UI cell renders empty rather than the literal 'None'."""
    assert _make_value_preview(None) is None


def test_make_value_preview_truncates_long_strings():
    """Preview truncates at 240 chars (237 + '...') so it fits on one UI row.

    The cap was raised from 120 → 240 to make better use of the wide
    Cache calls table in fullscreen — most realistic cached values fit
    on a single row at the new width without needing a hover tooltip.
    """
    preview = _make_value_preview('x' * 500)
    assert preview is not None
    assert len(preview) == 240
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
    # repr of a small dict fits comfortably under the 240-char cap
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


def test_proxy_delete_is_suppressed_but_reports_presence_with_use_cache_true():
    """``delete`` with use_cache=True reports whether the key was live without mutating.

    Mirrors the real ``Cache.delete`` contract (returns True if key is
    in memory, False otherwise). The live cache value must remain
    untouched — only the return value reflects presence.
    """
    real = _fresh_cache()
    real.set('k', 'v')
    proxy = _make_proxy(real=real, use_cache=True)
    result = proxy.delete('k')
    assert result is True  # key was present in the real memory
    # Real cache still has the value — the delete was not propagated.
    assert real.peek('k') == 'v'
    assert proxy.calls[-1].op == 'delete'
    assert proxy.calls[-1].outcome == 'suppressed'


def test_proxy_delete_returns_false_when_key_absent_with_use_cache_true():
    """With use_cache=True but key not present, delete returns False."""
    real = _fresh_cache()
    proxy = _make_proxy(real=real, use_cache=True)
    result = proxy.delete('missing')
    assert result is False
    assert proxy.calls[-1].op == 'delete'
    assert proxy.calls[-1].outcome == 'suppressed'


def test_proxy_delete_returns_false_when_use_cache_false():
    """With use_cache=False, delete never looks at real cache — returns False.

    Matches ``NoOpCache.delete``: the probe refuses to leak any live
    cache state when the operator opted out via ``use_cache=False``.
    """
    real = _fresh_cache()
    real.set('k', 'v')
    proxy = _make_proxy(real=real, use_cache=False)
    result = proxy.delete('k')
    assert result is False
    assert real.peek('k') == 'v'
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


# --- DebugCacheProxy: concurrent-production isolation -----------------------
#
# The probe swaps ``handler.cache`` on the SHARED handler instance — the
# same one the live worker uses for production messages. Without a guard,
# any production task that calls ``self.cache.X`` while a probe is in
# flight would (a) land in the probe's call log as pollution and (b) have
# its writes silently dropped because the proxy suppresses writes by
# design.
#
# The proxy distinguishes probe callers from production callers via the
# ``_probe_stage`` contextvar (asyncio gives each task its own context).
# These tests pin down the four guarantees:
#
#   1. Read calls outside any ``_stage(...)`` block pass through to the
#      real cache and return the real value (NOT a forced miss).
#   2. Set calls outside any ``_stage(...)`` block actually mutate the
#      real cache (NOT silently dropped).
#   3. Delete calls outside any ``_stage(...)`` block actually mutate the
#      real cache (NOT silently dropped).
#   4. None of those production-origin calls land in ``proxy.calls`` —
#      the probe report stays clean of pollution.


@contextlib.contextmanager
def _production_caller():
    """Override the autouse fixture so the test body runs as a 'production task'.

    The autouse ``_reset_probe_stage`` fixture sets ``_probe_stage='test'``
    so default proxy tests stay in probe-mode. The pass-through tests
    below are about the OPPOSITE behavior — what happens when a non-probe
    asyncio task hits the proxy. Resetting the contextvar to ``'unknown'``
    inside this context manager simulates that. The previous value is
    restored on exit so the autouse cleanup still works correctly.
    """
    token = _probe_stage.set('unknown')
    try:
        yield
    finally:
        _probe_stage.reset(token)


async def test_proxy_get_passes_through_to_real_cache_outside_probe_stage():
    """Production-task get (no ``_probe_stage`` set) returns the real value, no log entry."""
    real = _fresh_cache()
    real.set('present-key', 'real-value')
    proxy = _make_proxy(real=real, use_cache=False)

    with _production_caller():
        # ``_probe_stage`` is 'unknown'. The proxy must pass through to the
        # real cache despite ``use_cache=False`` — that flag is a probe
        # knob, not a production gate.
        value = await proxy.get('present-key')

    assert value == 'real-value'
    # No call recorded — production traffic must not pollute the probe log.
    assert proxy.calls == []


def test_proxy_peek_passes_through_outside_probe_stage():
    """Production-task peek pulls from the real cache and skips the log."""
    real = _fresh_cache()
    real.set('k', 'v')
    proxy = _make_proxy(real=real, use_cache=False)

    with _production_caller():
        assert proxy.peek('k') == 'v'
    assert proxy.calls == []


def test_proxy_contains_passes_through_outside_probe_stage():
    """Production-task ``in`` check pulls from the real cache and skips the log."""
    real = _fresh_cache()
    real.set('k', 'v')
    proxy = _make_proxy(real=real, use_cache=False)

    with _production_caller():
        assert 'k' in proxy
        assert 'missing' not in proxy
    assert proxy.calls == []


def test_proxy_set_writes_to_real_cache_outside_probe_stage():
    """Production-task set ACTUALLY MUTATES the real cache.

    This is the load-bearing fix for the "no cache writes — zero footprint
    on the live system" guarantee. Before the contextvar gate, the proxy
    suppressed every set unconditionally; concurrent production traffic
    during a probe lost its writes silently.
    """
    real = _fresh_cache()
    proxy = _make_proxy(real=real, use_cache=False)

    with _production_caller():
        proxy.set('new-key', 'new-value', ttl=60.0, scope=CacheScope.CLUSTER)

    # The write must have landed in the real cache.
    assert real.peek('new-key') == 'new-value'
    # And the probe log must NOT contain it (production-origin, transparent).
    assert proxy.calls == []


def test_proxy_delete_mutates_real_cache_outside_probe_stage():
    """Production-task delete ACTUALLY REMOVES the entry from the real cache."""
    real = _fresh_cache()
    real.set('k', 'v')
    proxy = _make_proxy(real=real, use_cache=False)

    with _production_caller():
        result = proxy.delete('k')

    assert result is True
    assert real.peek('k') is None  # actually deleted
    assert proxy.calls == []


async def test_concurrent_production_traffic_does_not_pollute_probe_log():
    """End-to-end: a sibling task hammering the proxy while a probe stage is
    running must not show up in the probe's call log, and its writes must
    survive in the real cache.

    Mirrors the integration-runtime scenario where the live worker's poll
    loop processes Kafka messages concurrent with a Message Probe pasted
    by an operator. The probe and the production task each have their own
    asyncio contextvar context — the probe sets ``_probe_stage`` inside
    its task, the production task does not.
    """
    real = _fresh_cache()
    proxy = _make_proxy(real=real, use_cache=False)

    # ``done`` flips after the probe-stage block exits. The production task
    # writes one entry per loop iteration until then.
    done = asyncio.Event()
    written_keys: list[str] = []

    async def production_task() -> None:
        # Production task simulates a sibling asyncio task — its
        # contextvar context defaults to the autouse 'test' value, which
        # the proxy would still treat as probe-mode. Reset to 'unknown'
        # so the proxy correctly classifies this as production-origin.
        _probe_stage.set('unknown')
        n = 0
        while not done.is_set():
            key = f'prod-{n}'
            proxy.set(key, f'value-{n}', ttl=10.0, scope=CacheScope.LOCAL)
            written_keys.append(key)
            n += 1
            # Yield so the probe coroutine can interleave.
            await asyncio.sleep(0)

    prod_handle = asyncio.create_task(production_task())
    # Brief warm-up so production has fired several writes before the probe
    # enters its stage.
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    # Run a couple of probe-stage operations. Production keeps running.
    token = _probe_stage.set('arrange')
    try:
        await proxy.get('probe-key-1')
        proxy.peek('probe-key-2')
        # Continue yielding so production gets more interleaved writes
        # while we're inside the probe stage.
        for _ in range(3):
            await asyncio.sleep(0)
    finally:
        _probe_stage.reset(token)

    done.set()
    await prod_handle

    # 1. Probe log contains EXACTLY two entries — the probe's get + peek.
    #    Production task's many ``set`` calls are absent.
    assert len(proxy.calls) == 2, (
        f'expected 2 probe-origin calls, got {len(proxy.calls)}: {[(c.op, c.key, c.origin_stage) for c in proxy.calls]}'
    )
    assert {c.op for c in proxy.calls} == {'get', 'peek'}
    assert all(c.origin_stage == 'arrange' for c in proxy.calls)
    assert all(c.key.startswith('probe-key-') for c in proxy.calls)

    # 2. Every production write survived in the real cache. None were dropped.
    assert len(written_keys) > 0  # sanity: production did run
    for key in written_keys:
        assert real.peek(key) is not None, f'production write for {key} was silently dropped'


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
    assert rec.extras == {'sink_instance': 'results', 'topic': None, 'key': 'my-key'}


async def test_sink_collector_flatten_kafka_default_sink():
    """Kafka payload with empty sink name uses '(default)' as the destination label."""
    collector = DebugSinkCollector()
    cr = CollectResult(kafka=[KafkaPayload(data=_TinyOutput())])
    await collector(cr, 0)
    flat = collector.flatten()
    assert flat[0].destination == '(default)'
    assert flat[0].extras == {'sink_instance': '', 'topic': None, 'key': None}


async def test_sink_collector_flatten_kafka_uses_real_topic_when_mapping_provided():
    """When ``kafka_sink_topics`` is populated, destination = real topic (not sink name).

    This makes the UI Kafka-UI deep-link resolve to the correct topic
    instead of the sink instance name (which produced broken links).
    """
    collector = DebugSinkCollector(kafka_sink_topics={'results': 'output_topic'})
    cr = CollectResult(
        kafka=[KafkaPayload(sink='results', data=_TinyOutput())],
    )
    await collector(cr, 0)
    flat = collector.flatten()
    assert flat[0].destination == 'output_topic'
    assert flat[0].extras['topic'] == 'output_topic'
    assert flat[0].extras['sink_instance'] == 'results'


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


def _make_config(*, max_retries: int = 1) -> DrakkarConfig:
    """Minimal DrakkarConfig — only executor block is read by the runner.

    ``max_retries`` is exposed so Task 4's tests can control the retry
    budget per scenario (retry path, retries exhausted, etc.).
    """
    return DrakkarConfig(
        executor=ExecutorConfig(
            binary_path='/nonexistent/binary/should-never-run',
            task_timeout_seconds=5,
            max_retries=max_retries,
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


async def test_runner_partial_report_for_mid_run_is_truncated():
    """partial_report_for(task) called mid-run reflects only completed-so-far tasks.

    The handler's on_task_complete hook inspects the runner's partial
    state the moment task 0 finishes, before task 1 has been submitted.
    At that point the partial should carry truncated=True and the task
    list should still be empty (the runner appends the entry AFTER the
    hook returns).

    Uses the production entrypoint (``start_probe`` +
    ``partial_report_for``) — the same path the /api/debug/probe
    endpoint takes on a wall-clock timeout.
    """
    captured: dict[str, Any] = {}
    handler = _HappyPathHandler(task_count=2)
    runner = DebugRunner(
        handler=handler,
        executor_pool=_make_executor_pool(),
        app_config=_make_config(),
    )

    # ``start_probe`` creates the task synchronously (doesn't run yet —
    # the loop has to yield first), so we can install the hook after
    # the task exists but before it executes. The hook captures the
    # partial via ``partial_report_for(task)``, which is what the
    # endpoint uses.
    run_task = runner.start_probe(ProbeInput(value='x', offset=1))

    def _snapshot(h: _HappyPathHandler, result: ExecutorResult) -> None:
        if h.on_task_complete_calls == 1:
            captured['report'] = DebugRunner.partial_report_for(run_task)

    handler.on_task_complete_hook = _snapshot

    final = await run_task

    partial: DebugReport = captured['report']
    assert partial.truncated is True
    # At snapshot time, the current task has just completed its
    # execution but its on_task_complete_result is being built. The
    # runner appends the ProbeTaskEntry to partial.tasks AFTER the
    # hook returns, so inside the hook the partial still reflects zero
    # completed tasks.
    assert len(partial.tasks) == 0
    # arrange completed before any task ran.
    assert partial.arrange.duration_seconds is not None
    # on_message_complete / on_window_complete have not fired yet.
    assert partial.on_message_complete is None
    assert partial.on_window_complete is None
    # input was set at the start of the run.
    assert partial.input.value == 'x'

    # The full run still completes correctly.
    assert final.truncated is False
    assert len(final.tasks) == 2


async def test_runner_partial_report_for_foreign_task_returns_empty_shape():
    """partial_report_for() on a task without probe state returns a valid empty report.

    Mirrors the endpoint-path fallback when a caller's wall-clock
    timeout fires before ``start_probe`` even got scheduled. We feed
    the method a completed asyncio task that carries no
    ``_drakkar_probe_state`` attribute — the method should not raise
    and should return a truncated stub report.
    """

    async def _noop() -> DebugReport:
        return DebugReport(input=ProbeInput(value=''), arrange=ProbeStageResult(), truncated=False)

    foreign_task: asyncio.Task[DebugReport] = asyncio.create_task(_noop())
    await foreign_task
    report = DebugRunner.partial_report_for(foreign_task)
    assert report.truncated is True
    assert report.tasks == []
    assert report.input.value == ''


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
    """If ``on_task_complete`` raises and the error IS propagated, cache restore still runs.

    Task 3 made the runner catch on_task_complete exceptions so they land
    in ``DebugReport.errors`` instead of propagating. To exercise the
    exception-propagation path, we patch ``_run_stages`` to re-raise the
    captured error, forcing the cache restore through a real exception
    flow (not just the happy path). Without the ``finally`` block the
    assertion ``handler.cache is original_cache`` would fail.
    """
    handler = _TaskCompleteRaisingHandler(task_count=1)
    original_cache = handler.cache  # class-level NoOpCache
    runner = DebugRunner(
        handler=handler,
        executor_pool=_make_executor_pool(),
        app_config=_make_config(),
    )

    # Patch _run_stages to raise, bypassing Task 3's in-runner error
    # capture. This forces the cache-restore path to go through a real
    # exception flow — without the finally clause in _run_locked, the
    # assertion below would fail.
    async def _raise_stages(**kwargs):
        raise RuntimeError('injected-stage-failure')

    with (
        patch.object(runner, '_run_stages', side_effect=_raise_stages),
        pytest.raises(RuntimeError, match='injected-stage-failure'),
    ):
        await runner.run(ProbeInput(value='x', offset=1))

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


# --- Task 3: Error capture for every hook -----------------------------------
#
# These tests exercise the graceful error-capture layer added in Task 3.
# The runner must never propagate a hook's exception; every failure
# becomes a ``ProbeError`` on ``DebugReport.errors`` with the full
# traceback, the corresponding stage's ``error`` field gets a one-line
# summary, and the pipeline short-circuits according to the plan rules:
#
#   deserialize error       → skip every downstream stage
#   message_label error     → NON-fatal; still run arrange + tasks + hooks
#   arrange error           → skip tasks + hooks
#   on_task_complete error  → record on the task entry, keep processing tasks
#   on_message_complete err → still run on_window_complete (hooks are independent)
#   on_window_complete err  → captured and returned
#
# Each test defines a small handler subclass that injects a raise at the
# target stage so the probe's behaviour can be asserted end-to-end.


class _DeserializeRaisingHandler(_HappyPathHandler):
    """Raises in ``deserialize_message`` — every downstream stage must be skipped."""

    def deserialize_message(self, msg: SourceMessage) -> SourceMessage:
        raise ValueError('bad payload from deserialize')


async def test_runner_captures_deserialize_error_and_skips_downstream():
    """deserialize raise → one error, arrange/tasks/hooks all skipped, report is valid JSON."""
    handler = _DeserializeRaisingHandler(task_count=2)
    runner = DebugRunner(
        handler=handler,
        executor_pool=_make_executor_pool(),
        app_config=_make_config(),
    )

    report = await runner.run(ProbeInput(value='not json', offset=1))

    # Exactly one error, tagged with the deserialize stage.
    assert len(report.errors) == 1
    err = report.errors[0]
    assert err.stage == 'deserialize'
    assert err.exception_class == 'ValueError'
    assert 'bad payload from deserialize' in err.message
    # Full traceback is captured — a Traceback header appears in every
    # format_exc output when called inside an except block.
    assert 'Traceback' in err.traceback or 'ValueError' in err.traceback

    # Dedicated deserialize_error field carries the same ProbeError so
    # the UI can highlight it in section A.
    assert report.deserialize_error is not None
    assert report.deserialize_error.stage == 'deserialize'

    # Arrange stage stays at the default empty shape — it was skipped.
    assert report.arrange.duration_seconds is None
    assert report.arrange.error is None
    assert report.arrange.collect_result is None

    # No tasks, no hooks were invoked.
    assert report.tasks == []
    assert report.on_message_complete is None
    assert report.on_window_complete is None
    assert handler.arrange_calls == 0
    assert handler.on_task_complete_calls == 0
    assert handler.on_message_complete_calls == 0
    assert handler.on_window_complete_calls == 0

    # Report round-trips through JSON without raising.
    as_json = report.model_dump_json()
    restored = DebugReport.model_validate_json(as_json)
    assert restored == report


class _ArrangeRaisingHandler(_HappyPathHandler):
    """Raises in ``arrange`` — tasks + hooks must be skipped but deserialize ran OK."""

    async def arrange(
        self,
        messages: list[SourceMessage],
        pending: PendingContext,
    ) -> list[ExecutorTask]:
        self.arrange_calls += 1
        raise RuntimeError('arrange blew up')


async def test_runner_captures_arrange_error_and_skips_downstream():
    """arrange raise → one error, no tasks/hooks; parsed_payload survives from deserialize."""
    handler = _ArrangeRaisingHandler(task_count=1)
    runner = DebugRunner(
        handler=handler,
        executor_pool=_make_executor_pool(),
        app_config=_make_config(),
    )

    report = await runner.run(ProbeInput(value='payload', offset=1))

    # Exactly one error tagged with the arrange stage.
    assert len(report.errors) == 1
    assert report.errors[0].stage == 'arrange'
    assert report.errors[0].exception_class == 'RuntimeError'
    assert 'arrange blew up' in report.errors[0].message

    # Deserialize ran (default BaseDrakkarHandler.deserialize_message sets
    # payload=None when input_model is unset). message_label also ran,
    # since the plan puts arrange *after* message_label in sequence.
    assert report.message_label == '0:1'

    # Arrange stage has a short error summary + duration (even for the
    # failing case — we time the attempted call).
    assert report.arrange.error is not None
    assert 'arrange blew up' in report.arrange.error
    assert report.arrange.duration_seconds is not None

    # Downstream stages skipped.
    assert report.tasks == []
    assert report.on_message_complete is None
    assert report.on_window_complete is None
    assert handler.on_task_complete_calls == 0
    assert handler.on_message_complete_calls == 0
    assert handler.on_window_complete_calls == 0


class _OnTaskCompleteSelectiveRaisingHandler(_HappyPathHandler):
    """Raises in ``on_task_complete`` only for the FIRST task; the second succeeds.

    Used to prove that a single task's hook failure does not abort the
    pipeline — remaining tasks are still processed and
    on_message_complete still runs.
    """

    async def on_task_complete(self, result: ExecutorResult) -> CollectResult | None:
        self.on_task_complete_calls += 1
        if result.task.task_id == 't-0':
            raise ValueError('boom for task 0')
        return CollectResult(
            kafka=[
                KafkaPayload(
                    sink='results',
                    key=result.task.task_id.encode(),
                    data=_TinyOutput(id=self.on_task_complete_calls, note='task'),
                ),
            ],
        )


async def test_runner_captures_on_task_complete_error_and_continues_other_tasks():
    """on_task_complete raise for task 0 → task 0 carries the error, task 1 succeeds, hooks still fire."""
    handler = _OnTaskCompleteSelectiveRaisingHandler(task_count=2)
    runner = DebugRunner(
        handler=handler,
        executor_pool=_make_executor_pool(),
        app_config=_make_config(),
    )

    report = await runner.run(ProbeInput(value='x', offset=1))

    # Both tasks got an on_task_complete invocation (the failing task's
    # exception was captured, not propagated).
    assert handler.on_task_complete_calls == 2

    # Exactly one error, tagged with the failing task's id.
    assert len(report.errors) == 1
    assert report.errors[0].stage == 'task_complete:t-0'
    assert report.errors[0].exception_class == 'ValueError'

    # Task 0's entry carries the one-line error summary, task 1's does not.
    assert len(report.tasks) == 2
    assert report.tasks[0].task_id == 't-0'
    assert report.tasks[0].on_task_complete_error is not None
    assert 'boom for task 0' in report.tasks[0].on_task_complete_error
    assert report.tasks[1].task_id == 't-1'
    assert report.tasks[1].on_task_complete_error is None
    # Task 1's on_task_complete_result IS populated (the handler returned
    # a CollectResult for it).
    assert report.tasks[1].on_task_complete_result is not None

    # on_message_complete still ran (a per-task hook failure does not
    # block the per-message hook).
    assert report.on_message_complete is not None
    assert handler.on_message_complete_calls == 1
    assert report.on_window_complete is not None
    assert handler.on_window_complete_calls == 1


class _OnMessageCompleteRaisingHandler(_HappyPathHandler):
    """Raises in ``on_message_complete`` — ``on_window_complete`` must still fire."""

    async def on_message_complete(self, group: MessageGroup) -> CollectResult | None:
        self.on_message_complete_calls += 1
        raise RuntimeError('message complete explosion')


async def test_runner_captures_on_message_complete_error_but_still_runs_on_window_complete():
    """on_message_complete raise → error recorded, on_window_complete still runs and succeeds."""
    handler = _OnMessageCompleteRaisingHandler(task_count=1)
    runner = DebugRunner(
        handler=handler,
        executor_pool=_make_executor_pool(),
        app_config=_make_config(),
    )

    report = await runner.run(ProbeInput(value='x', offset=1))

    # Error captured under the on_message_complete stage.
    assert len(report.errors) == 1
    assert report.errors[0].stage == 'on_message_complete'
    assert report.errors[0].exception_class == 'RuntimeError'

    # Stage result reflects the error + no CollectResult.
    assert report.on_message_complete is not None
    assert report.on_message_complete.error is not None
    assert 'message complete explosion' in report.on_message_complete.error
    assert report.on_message_complete.collect_result is None

    # on_window_complete still fired — the hooks are independent and
    # the plan mandates we keep going so the operator sees both outcomes.
    assert handler.on_window_complete_calls == 1
    assert report.on_window_complete is not None
    assert report.on_window_complete.error is None
    # The window hook produced a kafka record — it should appear in the
    # flattened sink payloads.
    sinks_by_stage = {r.origin_stage: r for r in report.planned_sink_payloads}
    assert 'window_complete' in sinks_by_stage


class _OnWindowCompleteRaisingHandler(_HappyPathHandler):
    """Raises in ``on_window_complete`` — last stage, the probe simply captures and returns."""

    async def on_window_complete(
        self,
        results: list[ExecutorResult],
        source_messages: list[SourceMessage],
    ) -> CollectResult | None:
        self.on_window_complete_calls += 1
        raise ValueError('window complete explosion')


async def test_runner_captures_on_window_complete_error_and_returns():
    """on_window_complete raise → error captured, probe returns normally, no raise."""
    handler = _OnWindowCompleteRaisingHandler(task_count=1)
    runner = DebugRunner(
        handler=handler,
        executor_pool=_make_executor_pool(),
        app_config=_make_config(),
    )

    # The important guarantee: runner.run() does NOT raise.
    report = await runner.run(ProbeInput(value='x', offset=1))

    assert len(report.errors) == 1
    assert report.errors[0].stage == 'on_window_complete'
    assert report.errors[0].exception_class == 'ValueError'

    # on_window_complete stage result carries the error summary.
    assert report.on_window_complete is not None
    assert report.on_window_complete.error is not None
    assert 'window complete explosion' in report.on_window_complete.error
    assert report.on_window_complete.collect_result is None

    # on_message_complete fired normally (it is upstream of the failure).
    assert report.on_message_complete is not None
    assert report.on_message_complete.error is None
    assert handler.on_message_complete_calls == 1


class _ArrangeRaisesAfterCacheWriteHandler(_HappyPathHandler):
    """Writes to cache inside ``arrange``, then raises.

    Used to verify that cache calls captured BEFORE a fatal stage error
    still appear in the final report — the runner must not clear the
    cache_calls list when a stage raises.
    """

    async def arrange(
        self,
        messages: list[SourceMessage],
        pending: PendingContext,
    ) -> list[ExecutorTask]:
        self.arrange_calls += 1
        # One suppressed write BEFORE the raise.
        self.cache.set('arrange-key', {'stage': 'arrange'})
        raise RuntimeError('arrange blew up after cache write')


async def test_runner_arrange_error_preserves_cache_calls_in_report():
    """A cache call made before a fatal arrange error is still captured in the report.

    This is the simpler variant of the "deserialize error preserves
    cache_calls" scenario — deserialize runs before the proxy swap even
    has a chance to log anything the handler might call, so we instead
    exercise the "fatal mid-run failure" path via arrange, which the
    plan explicitly calls out as the fallback case.
    """
    handler = _ArrangeRaisesAfterCacheWriteHandler(task_count=1)
    runner = DebugRunner(
        handler=handler,
        executor_pool=_make_executor_pool(),
        app_config=_make_config(),
    )

    report = await runner.run(ProbeInput(value='x', offset=1))

    # Arrange failed → one error captured, downstream skipped.
    assert len(report.errors) == 1
    assert report.errors[0].stage == 'arrange'

    # The cache_calls list preserves the handler's suppressed write.
    set_calls = [c for c in report.cache_calls if c.op == 'set' and c.key == 'arrange-key']
    assert len(set_calls) == 1
    assert set_calls[0].outcome == 'suppressed'
    # Summary counts mirror the call log (one suppressed write).
    assert report.cache_summary['writes_suppressed'] >= 1


# --- Task 4: on_error path (retries and replacements) ----------------------
#
# These tests exercise the probe's ``on_error`` integration. When the
# executor raises ``ExecutorTaskError`` (e.g. a precomputed task with
# non-zero exit_code), the runner calls ``handler.on_error(task, err)``
# and interprets the return value:
#
#   ErrorAction.RETRY   → re-run the task (subject to max_retries)
#   list[ExecutorTask]  → execute each replacement in sequence
#   ErrorAction.SKIP    → leave the failed entry as-is
#   (anything else)     → treated as SKIP
#
# If ``on_error`` itself raises, we capture a ProbeError tagged
# ``on_error:<task_id>`` and keep the failed entry.
#
# Each test wires a small handler subclass that drives the exact
# scenario under test, using precomputed tasks with non-zero exit_code
# to simulate subprocess failures without spawning real subprocesses.


def _make_failing_precomputed_task(
    *,
    task_id: str,
    offset: int = 0,
    stderr: str = 'boom',
    exit_code: int = 1,
) -> ExecutorTask:
    """Build an ExecutorTask whose precomputed exit_code is non-zero.

    ``ExecutorPool._execute_precomputed`` raises ``ExecutorTaskError``
    for any non-zero exit_code, so this task triggers the exact same
    code path a real subprocess failure would. Ideal for unit-testing
    on_error orchestration without flaky subprocess spawning.
    """
    return ExecutorTask(
        task_id=task_id,
        source_offsets=[offset],
        precomputed=PrecomputedResult(
            stdout='',
            stderr=stderr,
            exit_code=exit_code,
            duration_seconds=0.001,
        ),
    )


class _RetryThenSucceedHandler(_HappyPathHandler):
    """arrange returns a precomputed task that fails the first time and
    succeeds on retry.

    Each time ``arrange`` is called (only once in practice), it returns
    a SINGLE task. That task is a precomputed task whose exit_code
    starts non-zero. The ``on_task_complete`` fast-path uses the task's
    own precomputed data, so we swap the task's precomputed payload
    between attempts by mutating the returned task's ``precomputed``
    field when ``on_error`` fires. Cleaner: we keep the task reference
    and flip its ``precomputed.exit_code`` from 1 → 0 inside on_error.

    on_error always returns RETRY.
    """

    def __init__(self) -> None:
        super().__init__(task_count=1)
        # Track how many times on_error fired so tests can assert the
        # retry cycle ran exactly as expected.
        self.on_error_calls = 0
        # Hold a reference to the task we yielded so on_error can mutate
        # its precomputed payload for the next attempt.
        self._task: ExecutorTask | None = None

    async def arrange(
        self,
        messages: list[SourceMessage],
        pending: PendingContext,
    ) -> list[ExecutorTask]:
        self.arrange_calls += 1
        msg = messages[0]
        self._task = _make_failing_precomputed_task(
            task_id='t-flaky',
            offset=msg.offset,
            stderr='first-attempt-fail',
            exit_code=1,
        )
        return [self._task]

    async def on_error(
        self,
        task: ExecutorTask,
        error: ExecutorError,
    ) -> str | list[ExecutorTask]:
        self.on_error_calls += 1
        # Flip the precomputed outcome so the retry succeeds with
        # exit_code=0. The runner reuses the SAME ExecutorTask instance
        # for the retry — mutating its precomputed field in-place makes
        # the next execution attempt see the new outcome.
        if task.precomputed is not None:
            task.precomputed = PrecomputedResult(
                stdout='retry-worked',
                stderr='',
                exit_code=0,
                duration_seconds=0.001,
            )
        return ErrorAction.RETRY


async def test_runner_retries_failed_task_when_on_error_returns_retry():
    """First attempt fails, on_error returns RETRY, second attempt succeeds.

    Assertions:
      - Two task entries total (original failed + retry done).
      - Retry entry carries retry_of=<original task_id>.
      - on_task_complete fired for the successful retry (its result is
        attached to the retry entry).
      - No ProbeError recorded (on_error completed cleanly).
    """
    handler = _RetryThenSucceedHandler()
    runner = DebugRunner(
        handler=handler,
        executor_pool=_make_executor_pool(),
        app_config=_make_config(max_retries=1),
    )

    report = await runner.run(ProbeInput(value='x', offset=1))

    assert handler.on_error_calls == 1
    # No on_error-raise error captured; report should be clean.
    assert report.errors == []

    # Exactly two task entries: original + one retry.
    assert len(report.tasks) == 2
    original, retry = report.tasks
    assert original.task_id == 't-flaky'
    assert original.status == 'failed'
    assert original.retry_of is None  # original is never marked as a retry
    assert original.subprocess_exception is not None or original.stderr

    assert retry.task_id == 't-flaky'
    assert retry.status == 'done'
    assert retry.retry_of == 't-flaky'
    # Successful retry → on_task_complete fired and attached its result.
    assert retry.on_task_complete_result is not None
    assert retry.exit_code == 0
    assert retry.stdout == 'retry-worked'


class _AlwaysRetryHandler(_HappyPathHandler):
    """Subprocess always fails; on_error always asks to RETRY.

    Used to prove the ``max_retries`` cap: with ``max_retries=2`` and a
    handler that never stops asking for RETRY, the probe should record
    1 original + 2 retry entries (3 total), all ``status='failed'``.
    """

    def __init__(self) -> None:
        super().__init__(task_count=1)
        self.on_error_calls = 0

    async def arrange(
        self,
        messages: list[SourceMessage],
        pending: PendingContext,
    ) -> list[ExecutorTask]:
        self.arrange_calls += 1
        return [
            _make_failing_precomputed_task(
                task_id='t-always-fail',
                offset=messages[0].offset,
                stderr='always-fails',
                exit_code=2,
            ),
        ]

    async def on_error(
        self,
        task: ExecutorTask,
        error: ExecutorError,
    ) -> str | list[ExecutorTask]:
        self.on_error_calls += 1
        return ErrorAction.RETRY


async def test_runner_stops_retrying_when_max_retries_exhausted():
    """With max_retries=2 and a handler that always RETRies, we see 3 failed entries.

    1 original attempt + 2 retries (max_retries cap) = 3 task entries.
    All ``status='failed'``. ``on_task_complete`` fires ZERO times —
    every attempt failed and the executor never produced a successful
    ExecutorResult to feed to the hook.
    """
    handler = _AlwaysRetryHandler()
    runner = DebugRunner(
        handler=handler,
        executor_pool=_make_executor_pool(),
        app_config=_make_config(max_retries=2),
    )

    report = await runner.run(ProbeInput(value='x', offset=1))

    # on_error fires after EACH failed attempt (original + 2 retries = 3).
    assert handler.on_error_calls == 3
    # on_task_complete never ran — no attempt produced a valid result.
    assert handler.on_task_complete_calls == 0

    # Three task entries total, every one failed.
    assert len(report.tasks) == 3
    assert all(t.status == 'failed' for t in report.tasks)
    # The original has no retry_of; the two retries both retry t-always-fail.
    assert report.tasks[0].retry_of is None
    assert report.tasks[1].retry_of == 't-always-fail'
    assert report.tasks[2].retry_of == 't-always-fail'

    # None of the failed attempts produced an on_task_complete result.
    assert all(t.on_task_complete_result is None for t in report.tasks)

    # Hooks downstream of the task loop still fire — the probe does not
    # abort just because every attempt failed.
    assert handler.on_message_complete_calls == 1
    assert handler.on_window_complete_calls == 1


class _ReplacementHandler(_HappyPathHandler):
    """Subprocess fails; on_error returns TWO replacement tasks that succeed.

    Tests the ``list`` branch of the on_error return contract. The
    replacements are precomputed-success tasks so ``on_task_complete``
    fires for each one.
    """

    def __init__(self) -> None:
        super().__init__(task_count=1)
        self.on_error_calls = 0

    async def arrange(
        self,
        messages: list[SourceMessage],
        pending: PendingContext,
    ) -> list[ExecutorTask]:
        self.arrange_calls += 1
        return [
            _make_failing_precomputed_task(
                task_id='t-original',
                offset=messages[0].offset,
                stderr='original-fails',
                exit_code=1,
            ),
        ]

    async def on_error(
        self,
        task: ExecutorTask,
        error: ExecutorError,
    ) -> str | list[ExecutorTask]:
        self.on_error_calls += 1
        # Two successful precomputed replacements. Both inherit the
        # original's source_offsets so the message-tracker bookkeeping
        # (irrelevant for the probe, but kept for production parity)
        # would be happy in a real run too.
        return [
            ExecutorTask(
                task_id='r-A',
                source_offsets=task.source_offsets,
                precomputed=PrecomputedResult(
                    stdout='replacement-A',
                    stderr='',
                    exit_code=0,
                    duration_seconds=0.001,
                ),
            ),
            ExecutorTask(
                task_id='r-B',
                source_offsets=task.source_offsets,
                precomputed=PrecomputedResult(
                    stdout='replacement-B',
                    stderr='',
                    exit_code=0,
                    duration_seconds=0.001,
                ),
            ),
        ]


async def test_runner_executes_replacement_tasks_returned_from_on_error():
    """on_error returns [A, B] → original marked replaced, A and B execute.

    Assertions:
      - Three task entries total: original + A + B.
      - Original entry has ``status='replaced'``.
      - A and B have ``status='done'`` and ``replacement_for='t-original'``.
      - on_task_complete fires once per replacement (2 times total).
    """
    handler = _ReplacementHandler()
    runner = DebugRunner(
        handler=handler,
        executor_pool=_make_executor_pool(),
        app_config=_make_config(max_retries=3),
    )

    report = await runner.run(ProbeInput(value='x', offset=1))

    assert handler.on_error_calls == 1
    # on_task_complete fires for both successful replacements; the
    # original never produced a result so it does not count.
    assert handler.on_task_complete_calls == 2
    assert report.errors == []

    # Exactly three task entries: 1 original + 2 replacements.
    assert len(report.tasks) == 3
    original, rep_a, rep_b = report.tasks
    assert original.task_id == 't-original'
    assert original.status == 'replaced'

    assert rep_a.task_id == 'r-A'
    assert rep_a.status == 'done'
    assert rep_a.replacement_for == 't-original'
    assert rep_a.on_task_complete_result is not None

    assert rep_b.task_id == 'r-B'
    assert rep_b.status == 'done'
    assert rep_b.replacement_for == 't-original'
    assert rep_b.on_task_complete_result is not None


class _OnErrorRaisesHandler(_HappyPathHandler):
    """Subprocess fails; on_error itself raises.

    Proves the ``on_error``-raises path: the probe captures a ProbeError
    tagged ``on_error:<task_id>`` and the failed task entry stays as
    ``status='failed'`` (no retry, no replacement).
    """

    def __init__(self) -> None:
        super().__init__(task_count=1)
        self.on_error_calls = 0

    async def arrange(
        self,
        messages: list[SourceMessage],
        pending: PendingContext,
    ) -> list[ExecutorTask]:
        self.arrange_calls += 1
        return [
            _make_failing_precomputed_task(
                task_id='t-broken',
                offset=messages[0].offset,
                stderr='subprocess-fail',
                exit_code=9,
            ),
        ]

    async def on_error(
        self,
        task: ExecutorTask,
        error: ExecutorError,
    ) -> str | list[ExecutorTask]:
        self.on_error_calls += 1
        raise RuntimeError('on_error itself blew up')


async def test_runner_captures_on_error_exception_and_marks_task_failed():
    """on_error raises → ProbeError recorded, failed entry kept, no retry/replace.

    Assertions:
      - Exactly one task entry, ``status='failed'``.
      - Exactly one ProbeError with stage=='on_error:t-broken'.
      - No additional retry / replacement entries were appended.
    """
    handler = _OnErrorRaisesHandler()
    runner = DebugRunner(
        handler=handler,
        executor_pool=_make_executor_pool(),
        app_config=_make_config(max_retries=3),
    )

    report = await runner.run(ProbeInput(value='x', offset=1))

    assert handler.on_error_calls == 1
    # Only one task entry — no retries, no replacements.
    assert len(report.tasks) == 1
    assert report.tasks[0].task_id == 't-broken'
    assert report.tasks[0].status == 'failed'

    # Exactly one ProbeError, tagged with the on_error:<task_id> stage.
    on_error_errors = [e for e in report.errors if e.stage.startswith('on_error:')]
    assert len(on_error_errors) == 1
    assert on_error_errors[0].stage == 'on_error:t-broken'
    assert on_error_errors[0].exception_class == 'RuntimeError'
    assert 'on_error itself blew up' in on_error_errors[0].message


# ---- Review phase 1: missing-test gap fills --------------------------------


class _MessageLabelRaisingHandler(_HappyPathHandler):
    """Handler where ``message_label`` raises — downstream must continue."""

    def __init__(self) -> None:
        super().__init__(task_count=1)

    def message_label(self, message: SourceMessage) -> str:
        raise RuntimeError('label-boom')


async def test_runner_message_label_error_is_nonfatal():
    """message_label raising → error recorded but arrange + tasks + hooks still fire."""
    handler = _MessageLabelRaisingHandler()
    runner = DebugRunner(
        handler=handler,
        executor_pool=_make_executor_pool(),
        app_config=_make_config(),
    )
    report = await runner.run(ProbeInput(value='x', offset=1))
    # The error was captured...
    labels = [e for e in report.errors if e.stage == 'message_label']
    assert len(labels) == 1
    assert labels[0].exception_class == 'RuntimeError'
    # ...and every downstream stage still ran.
    assert report.arrange.duration_seconds is not None
    assert len(report.tasks) == 1
    assert report.on_message_complete is not None
    assert report.on_window_complete is not None
    assert report.message_label is None  # stayed empty because label raised


class _ReplacementWithoutParentHandler(_ReplacementHandler):
    """Variant of _ReplacementHandler whose replacements have no parent_task_id set.

    The runner is expected to auto-link them back to the original task
    (mirrors production's PartitionProcessor:437 behaviour). Used to
    verify the auto-link contract.
    """

    async def on_error(
        self,
        task: ExecutorTask,
        error: ExecutorError,
    ) -> str | list[ExecutorTask]:
        self.on_error_calls += 1
        return [
            ExecutorTask(
                task_id='r-no-parent',
                source_offsets=task.source_offsets,
                # parent_task_id NOT set — runner must fill it in.
                precomputed=PrecomputedResult(
                    stdout='replacement',
                    stderr='',
                    exit_code=0,
                    duration_seconds=0.001,
                ),
            ),
        ]


async def test_runner_autopopulates_replacement_parent_task_id():
    """Replacement with parent_task_id=None gets auto-linked to the failed task.

    Matches production's ``drakkar/partition.py:437`` auto-link for any
    replacement the user handler did not explicitly link. Without this,
    the probe's lineage graph would diverge from a real worker's.
    """
    handler = _ReplacementWithoutParentHandler()
    runner = DebugRunner(
        handler=handler,
        executor_pool=_make_executor_pool(),
        app_config=_make_config(max_retries=3),
    )
    report = await runner.run(ProbeInput(value='x', offset=1))
    # The replacement entry has parent_task_id pointing at the failed task.
    replacement = next(t for t in report.tasks if t.task_id == 'r-no-parent')
    assert replacement.parent_task_id == 't-original'


class _TerminalFailureHandler(_HappyPathHandler):
    """arrange returns a failing task; on_error SKIPs.

    Used to verify terminal failures flow into on_message_complete +
    on_window_complete (production puts them on window.results /
    MessageGroup.errors; the probe must mirror that).
    """

    def __init__(self) -> None:
        super().__init__(task_count=1)
        # Record what the handler sees inside on_message_complete /
        # on_window_complete so tests can assert shape.
        self.mc_results_seen: list[ExecutorResult] = []
        self.mc_errors_seen: list[ExecutorError] = []
        self.mc_tasks_seen: list[ExecutorTask] = []
        self.wc_results_seen: list[ExecutorResult] = []

    async def arrange(
        self,
        messages: list[SourceMessage],
        pending: PendingContext,
    ) -> list[ExecutorTask]:
        self.arrange_calls += 1
        return [
            _make_failing_precomputed_task(
                task_id='t-fail',
                offset=messages[0].offset,
                stderr='subprocess-boom',
                exit_code=1,
            ),
        ]

    async def on_error(
        self,
        task: ExecutorTask,
        error: ExecutorError,
    ) -> str | list[ExecutorTask]:
        return ErrorAction.SKIP

    async def on_message_complete(self, group: MessageGroup) -> CollectResult | None:
        self.on_message_complete_calls += 1
        self.mc_results_seen = list(group.results)
        self.mc_errors_seen = list(group.errors)
        self.mc_tasks_seen = list(group.tasks)
        return None

    async def on_window_complete(
        self,
        results: list[ExecutorResult],
        source_messages: list[SourceMessage],
    ) -> CollectResult | None:
        self.on_window_complete_calls += 1
        self.wc_results_seen = list(results)
        return None


async def test_runner_feeds_terminal_failures_into_message_and_window_hooks():
    """SKIP'd terminal failures flow into on_window_complete, and tracker.errors.

    Mirrors production's three-list separation (see
    ``partition.py:406, 473, 479, 525-528``):
      - ``MessageGroup.results`` mirrors ``tracker.results`` — SUCCESS-
        only. A SKIP'd terminal failure must NOT appear here.
      - ``MessageGroup.errors`` mirrors ``tracker.errors`` — FAILURE-
        only. The failed task's ExecutorError must appear here.
      - ``on_window_complete(results, ...)`` mirrors ``window.results``
        — BOTH successes and terminal failures. The failed ExecutorResult
        appears here even though it's not on the message group.
    """
    handler = _TerminalFailureHandler()
    runner = DebugRunner(
        handler=handler,
        executor_pool=_make_executor_pool(),
        app_config=_make_config(),
    )
    await runner.run(ProbeInput(value='x', offset=1))

    # on_message_complete sees the failure in errors ONLY, not in
    # results — matches production's success-only ``tracker.results``.
    assert handler.mc_results_seen == []
    assert len(handler.mc_errors_seen) == 1
    assert handler.mc_errors_seen[0].task.task_id == 't-fail'
    # Task list includes the failed original.
    assert [t.task_id for t in handler.mc_tasks_seen] == ['t-fail']

    # on_window_complete's results list carries the failed ExecutorResult
    # — production puts it on ``window.results`` even though it's not on
    # ``tracker.results``. Confirms the probe mirrors that split.
    assert len(handler.wc_results_seen) == 1
    assert handler.wc_results_seen[0].exit_code == 1


class _MixedSuccessFailureHandler(_HappyPathHandler):
    """Two tasks: A succeeds, B fails terminally (SKIP'd on_error).

    Regression fixture for the split between ``MessageGroup.results``
    (success-only, mirrors ``tracker.results``) and
    ``on_window_complete``'s ``results`` (both, mirrors
    ``window.results``). The previous probe implementation accidentally
    put B's ExecutorResult on BOTH ``group.results`` AND ``group.errors``
    — this fixture lets a test prove that behaviour is gone.
    """

    def __init__(self) -> None:
        super().__init__(task_count=2)
        self.mc_results_seen: list[ExecutorResult] = []
        self.mc_errors_seen: list[ExecutorError] = []
        self.wc_results_seen: list[ExecutorResult] = []

    async def arrange(
        self,
        messages: list[SourceMessage],
        pending: PendingContext,
    ) -> list[ExecutorTask]:
        # t-A uses the success fixture; t-B uses the failing fixture —
        # keeps lineage predictable so the assertions can key on IDs.
        self.arrange_calls += 1
        return [
            _make_precomputed_task(task_id='t-A', offset=messages[0].offset, stdout='A-ok'),
            _make_failing_precomputed_task(
                task_id='t-B',
                offset=messages[0].offset,
                stderr='B-boom',
                exit_code=1,
            ),
        ]

    async def on_error(
        self,
        task: ExecutorTask,
        error: ExecutorError,
    ) -> str | list[ExecutorTask]:
        return ErrorAction.SKIP

    async def on_message_complete(self, group: MessageGroup) -> CollectResult | None:
        self.on_message_complete_calls += 1
        self.mc_results_seen = list(group.results)
        self.mc_errors_seen = list(group.errors)
        return None

    async def on_window_complete(
        self,
        results: list[ExecutorResult],
        source_messages: list[SourceMessage],
    ) -> CollectResult | None:
        self.on_window_complete_calls += 1
        self.wc_results_seen = list(results)
        return None


async def test_runner_mixed_success_failure_splits_results_vs_errors_production_parity():
    """Mixed outcomes → MessageGroup.results success-only, errors failure-only, window both.

    Regression for the message-group / window divergence (code review
    phase 1 iteration 3, issue 2). Production's
    ``PartitionProcessor._execute_and_track``:
      - partition.py:525-526 appends to ``tracker.results`` ONLY on
        success (``task_result`` only set at partition.py:407).
      - partition.py:527-528 appends to ``tracker.errors`` ONLY on
        failure.
      - partition.py:406, 473, 479 append to ``window.results`` on BOTH
        paths.

    This test schedules A=succeed + B=fail and asserts:
      - ``group.results`` contains A's result only (no B) —
        success-only ``tracker.results`` parity.
      - ``group.errors`` contains B's error only — failure-only
        ``tracker.errors`` parity.
      - ``on_window_complete`` receives BOTH A and B's ExecutorResults
        — ``window.results`` parity.

    A regression where B's result double-counted (appeared in BOTH
    ``group.results`` and ``group.errors``) would be caught by the first
    assertion.
    """
    handler = _MixedSuccessFailureHandler()
    runner = DebugRunner(
        handler=handler,
        executor_pool=_make_executor_pool(),
        app_config=_make_config(),
    )
    await runner.run(ProbeInput(value='x', offset=1))

    # MessageGroup.results mirrors production's tracker.results:
    # exactly ONE entry (A's success) — B MUST NOT appear here.
    assert len(handler.mc_results_seen) == 1
    assert handler.mc_results_seen[0].task.task_id == 't-A'
    assert handler.mc_results_seen[0].exit_code == 0
    # No failing task leaked into the success list — double-counting
    # regression fingerprint.
    assert all(r.task.task_id != 't-B' for r in handler.mc_results_seen)

    # MessageGroup.errors mirrors production's tracker.errors:
    # exactly ONE entry (B's failure) — A MUST NOT appear here.
    assert len(handler.mc_errors_seen) == 1
    assert handler.mc_errors_seen[0].task.task_id == 't-B'
    assert all(e.task.task_id != 't-A' for e in handler.mc_errors_seen)

    # on_window_complete's ``results`` mirrors production's
    # window.results — BOTH A (success) and B (failure) appear, each
    # with its own ExecutorResult.
    wc_ids = [r.task.task_id for r in handler.wc_results_seen]
    assert sorted(wc_ids) == ['t-A', 't-B']
    wc_by_id = {r.task.task_id: r for r in handler.wc_results_seen}
    assert wc_by_id['t-A'].exit_code == 0
    assert wc_by_id['t-B'].exit_code == 1


async def test_runner_replacement_tasks_appear_in_message_group_tasks():
    """on_error replacement → MessageGroup.tasks sees both original and replacement.

    Production's ``tracker.tasks`` (passed as ``MessageGroup.tasks``)
    grows as on_error adds replacements. The probe mirrors that so a
    handler that walks ``group.tasks`` sees the same shape.
    """
    seen_tasks: list[ExecutorTask] = []

    class _ReplaceAndRecord(_ReplacementHandler):
        async def on_message_complete(self, group: MessageGroup) -> CollectResult | None:
            seen_tasks.extend(group.tasks)
            return None

    handler = _ReplaceAndRecord()
    runner = DebugRunner(
        handler=handler,
        executor_pool=_make_executor_pool(),
        app_config=_make_config(max_retries=3),
    )
    await runner.run(ProbeInput(value='x', offset=1))
    # Both the failed original and the two replacements show up.
    ids = {t.task_id for t in seen_tasks}
    assert ids == {'t-original', 'r-A', 'r-B'}


class _RetryThenReplaceHandler(_HappyPathHandler):
    """Subprocess always fails; on_error returns RETRY, then replacements on retry."""

    def __init__(self) -> None:
        super().__init__(task_count=1)
        self.on_error_calls = 0

    async def arrange(
        self,
        messages: list[SourceMessage],
        pending: PendingContext,
    ) -> list[ExecutorTask]:
        self.arrange_calls += 1
        return [
            _make_failing_precomputed_task(
                task_id='t-original',
                offset=messages[0].offset,
                stderr='first-fail',
                exit_code=1,
            ),
        ]

    async def on_error(
        self,
        task: ExecutorTask,
        error: ExecutorError,
    ) -> str | list[ExecutorTask]:
        self.on_error_calls += 1
        if self.on_error_calls == 1:
            return ErrorAction.RETRY
        # Second call (from the retry loop) → return replacements.
        return [
            ExecutorTask(
                task_id=f'r-{self.on_error_calls}',
                source_offsets=task.source_offsets,
                precomputed=PrecomputedResult(
                    stdout='repl',
                    stderr='',
                    exit_code=0,
                    duration_seconds=0.001,
                ),
            ),
        ]


async def test_runner_retry_loop_handles_replacement_after_retry():
    """Retry fails, on_error then returns a replacement list → retry entry replaced."""
    handler = _RetryThenReplaceHandler()
    runner = DebugRunner(
        handler=handler,
        executor_pool=_make_executor_pool(),
        app_config=_make_config(max_retries=3),
    )
    report = await runner.run(ProbeInput(value='x', offset=1))
    # Three entries: original (failed), retry (replaced), replacement (done).
    # Original + retry share task_id, so we inspect by order.
    assert len(report.tasks) == 3
    assert report.tasks[0].task_id == 't-original'
    assert report.tasks[0].status == 'failed'
    assert report.tasks[1].task_id == 't-original'
    assert report.tasks[1].status == 'replaced'
    assert report.tasks[1].retry_of == 't-original'
    assert report.tasks[2].task_id == 'r-2'
    assert report.tasks[2].status == 'done'
    assert report.tasks[2].replacement_for == 't-original'


class _RetryThenSkipHandler(_HappyPathHandler):
    """Subprocess always fails; on_error returns RETRY, then SKIP on retry."""

    def __init__(self) -> None:
        super().__init__(task_count=1)
        self.on_error_calls = 0

    async def arrange(
        self,
        messages: list[SourceMessage],
        pending: PendingContext,
    ) -> list[ExecutorTask]:
        self.arrange_calls += 1
        return [_make_failing_precomputed_task(task_id='t-skip', offset=messages[0].offset)]

    async def on_error(
        self,
        task: ExecutorTask,
        error: ExecutorError,
    ) -> str | list[ExecutorTask]:
        self.on_error_calls += 1
        if self.on_error_calls == 1:
            return ErrorAction.RETRY
        return ErrorAction.SKIP


async def test_runner_retry_loop_handles_skip_after_retry():
    """Retry loop: on_error returns SKIP after the retry → loop exits, entry stays failed."""
    handler = _RetryThenSkipHandler()
    runner = DebugRunner(
        handler=handler,
        executor_pool=_make_executor_pool(),
        app_config=_make_config(max_retries=3),
    )
    report = await runner.run(ProbeInput(value='x', offset=1))
    assert handler.on_error_calls == 2  # original + retry
    assert len(report.tasks) == 2  # original + one retry
    assert report.tasks[0].status == 'failed'
    assert report.tasks[1].status == 'failed'
    assert report.tasks[1].retry_of == 't-skip'


class _RetryThenOnErrorRaisesHandler(_HappyPathHandler):
    """Subprocess fails; on_error RETRY the first time, raises on the retry failure."""

    def __init__(self) -> None:
        super().__init__(task_count=1)
        self.on_error_calls = 0

    async def arrange(
        self,
        messages: list[SourceMessage],
        pending: PendingContext,
    ) -> list[ExecutorTask]:
        self.arrange_calls += 1
        return [_make_failing_precomputed_task(task_id='t-err', offset=messages[0].offset)]

    async def on_error(
        self,
        task: ExecutorTask,
        error: ExecutorError,
    ) -> str | list[ExecutorTask]:
        self.on_error_calls += 1
        if self.on_error_calls == 1:
            return ErrorAction.RETRY
        raise RuntimeError('on_error-blew-up')


async def test_runner_retry_loop_captures_on_error_raised_on_retry():
    """On_error raising inside the retry loop is captured as a ProbeError and stops the loop."""
    handler = _RetryThenOnErrorRaisesHandler()
    runner = DebugRunner(
        handler=handler,
        executor_pool=_make_executor_pool(),
        app_config=_make_config(max_retries=3),
    )
    report = await runner.run(ProbeInput(value='x', offset=1))
    # Exactly one ProbeError tagged with on_error:<id>, captured from
    # the raising on_error call inside the retry loop.
    raising = [e for e in report.errors if e.stage.startswith('on_error:')]
    assert len(raising) == 1
    assert 'on_error-blew-up' in raising[0].message


# ---- _build_source_message + _serialize_payload ---------------------------


class _InputModel(BaseModel):
    """Simple BaseModel used as a handler's input_model for deserialize coverage."""

    hello: str


class _BaseModelPayloadHandler(_HappyPathHandler):
    """Overrides deserialize to set msg.payload to a BaseModel — exercises _serialize_payload."""

    def deserialize_message(self, message: SourceMessage) -> None:
        import json as _json

        data = _json.loads(message.value.decode('utf-8'))
        message.payload = _InputModel(**data)


async def test_runner_serializes_base_model_payload_to_json_dict():
    """msg.payload as a BaseModel → parsed_payload in the report is a JSON-friendly dict."""
    handler = _BaseModelPayloadHandler(task_count=0)
    runner = DebugRunner(
        handler=handler,
        executor_pool=_make_executor_pool(),
        app_config=_make_config(),
    )
    report = await runner.run(ProbeInput(value='{"hello": "world"}', offset=1))
    assert report.parsed_payload == {'hello': 'world'}


def test_serialize_payload_passes_through_plain_dict():
    """Plain dict (not BaseModel) passes through unchanged via _serialize_payload."""
    from drakkar.debug_runner import _serialize_payload

    payload = {'x': 1, 'y': [1, 2, 3]}
    assert _serialize_payload(payload) is payload


def test_build_source_message_defaults_timestamp_to_current_time_ms():
    """Default timestamp = wall-clock ms (close to time.time() * 1000)."""
    from drakkar.debug_runner import _build_source_message

    before_ms = int(time.time() * 1000)
    msg = _build_source_message(ProbeInput(value='x'))
    after_ms = int(time.time() * 1000) + 1
    assert before_ms <= msg.timestamp <= after_ms


def test_build_source_message_key_none_produces_none_bytes():
    """key=None → message.key is None (not b'')."""
    from drakkar.debug_runner import _build_source_message

    msg = _build_source_message(ProbeInput(value='hello'))
    assert msg.key is None


def test_build_source_message_encodes_value_and_key_as_utf8():
    """value and key are UTF-8 encoded to bytes."""
    from drakkar.debug_runner import _build_source_message

    msg = _build_source_message(ProbeInput(value='héllo', key='kéy'))
    assert msg.value == 'héllo'.encode()
    assert msg.key == 'kéy'.encode()


# ---- DebugCacheProxy: ttl kwarg + bytes-value preview ----------------------


def test_proxy_set_accepts_ttl_kwarg_without_affecting_log():
    """``set(..., ttl=60)`` is a suppressed no-op — ttl is only for signature parity.

    The ttl value is not reflected in the call log (we don't store it),
    but passing it must not raise. Documents the "ttl accepted for
    signature parity" contract in the proxy's set method.
    """
    proxy = _make_proxy(use_cache=False)
    proxy.set('k', 'v', ttl=60.0)
    # Proxy accepted ttl without error; call was logged as a suppressed write.
    assert proxy.calls[-1].op == 'set'
    assert proxy.calls[-1].outcome == 'suppressed'


def test_make_value_preview_handles_bytes():
    """bytes values show up in the preview via repr() — no UnicodeDecodeError."""
    preview = _make_value_preview(b'hello')
    assert preview is not None
    # repr(bytes) adds the b'' wrapper; assertion is loose (just checks content).
    assert 'hello' in preview


# ---- DebugSinkCollector: non-UTF-8 kafka key ------------------------------


async def test_sink_collector_flatten_kafka_key_handles_non_utf8_bytes():
    """Non-UTF-8 kafka keys decode with ``errors='replace'`` so JSON roundtrip works."""
    collector = DebugSinkCollector()
    # 0xff is not a valid UTF-8 lead byte — triggers the errors='replace' path.
    cr = CollectResult(kafka=[KafkaPayload(sink='s', key=b'\xff\xfe', data=_TinyOutput())])
    await collector(cr, 0)
    flat = collector.flatten()
    # U+FFFD (REPLACEMENT CHARACTER) is what errors='replace' produces.
    assert flat[0].extras['key'] == '��'


# ---- DebugRunner: probe lock serialization --------------------------------


class _LockSerializationHandler(_HappyPathHandler):
    """Handler whose on_task_complete awaits a shared event to prove lock serialization.

    The test starts two probes. The first one's on_task_complete blocks
    on ``release`` until the test releases it. Meanwhile probe B is
    launched — it must queue on the probe lock. Completion order then
    tells us whether the lock actually serialized the two runs.
    """

    def __init__(self, release_event: asyncio.Event, entered_event: asyncio.Event) -> None:
        super().__init__(task_count=1)
        self._release = release_event
        self._entered = entered_event

    async def on_task_complete(self, result: ExecutorResult) -> CollectResult | None:
        self.on_task_complete_calls += 1
        if self.on_task_complete_calls == 1:
            # First call only — signal the test and wait for release.
            self._entered.set()
            await self._release.wait()
        return None


async def test_runner_probe_lock_serializes_overlapping_runs():
    """Two concurrent ``runner.run`` calls must serialize — neither races on handler.cache.

    Launches two runs in parallel. A blocking event inside the first
    run's on_task_complete keeps it under the lock; the second run
    must wait. Completion order proves serialization: the second
    cannot finish before the first.
    """
    release = asyncio.Event()
    entered = asyncio.Event()
    handler = _LockSerializationHandler(release_event=release, entered_event=entered)
    runner = DebugRunner(
        handler=handler,
        executor_pool=_make_executor_pool(),
        app_config=_make_config(),
    )

    completion_order: list[str] = []

    async def _run_one(label: str, offset: int) -> None:
        await runner.run(ProbeInput(value=label, offset=offset))
        completion_order.append(label)

    task_a = asyncio.create_task(_run_one('A', 1))
    # Wait until A is blocked inside its hook (so A has the lock).
    await entered.wait()
    # Start B — it must wait on the probe lock.
    task_b = asyncio.create_task(_run_one('B', 2))
    # Give B a moment to queue up on the lock.
    await asyncio.sleep(0.05)
    # B cannot have finished yet because A is still holding the lock.
    assert completion_order == []
    # Release A.
    release.set()
    await asyncio.gather(task_a, task_b)
    # A finished first, proving the lock serialized the two runs.
    assert completion_order == ['A', 'B']


# ---- Cross-probe contamination: partial reports don't leak state ----------


async def test_partial_report_for_independent_task_is_not_contaminated():
    """Two concurrent start_probe calls → each task's partial reflects ONLY its own state.

    Regression test for the "cross-probe contamination" bug: when a
    probe's wait_for times out while it is still waiting on the probe
    lock, the partial report must be the EMPTY stub, not some other
    probe's in-flight state.
    """
    handler = _HappyPathHandler(task_count=1)
    runner = DebugRunner(
        handler=handler,
        executor_pool=_make_executor_pool(),
        app_config=_make_config(),
    )
    # Start probe A — it will run to completion on its own.
    task_a = runner.start_probe(ProbeInput(value='A', offset=1))
    # Start probe B — it waits on the lock (acquired by A).
    task_b = runner.start_probe(ProbeInput(value='B', offset=2))
    # Immediately cancel B BEFORE it acquires the lock to simulate a
    # wait_for timeout on the endpoint side.
    task_b.cancel()
    # Reading B's partial report must reflect B's empty state — NOT any
    # data from A's in-flight or completed state.
    b_report = DebugRunner.partial_report_for(task_b)
    assert b_report.truncated is True
    assert b_report.input.value == 'B'
    assert b_report.tasks == []  # B never ran any task
    # Let A finish cleanly.
    await task_a
    # Also verify B actually got cancelled.
    with contextlib.suppress(asyncio.CancelledError):
        await task_b


# ---- Regression: cache proxy chaining across concurrent probes -------------
#
# When probe B starts WHILE probe A is inside the lock, probe B's run-state
# must NOT capture A's ``DebugCacheProxy`` as its ``real`` backend.
# Otherwise B's reads would chain through A's proxy and — when A had
# ``use_cache=False`` — see false misses for keys that live in the real
# cache. The fix: build the proxy inside ``_run_locked`` (after acquiring
# the lock) so ``real`` is always the true ``handler.cache``.


class _CacheReadingHandler(BaseDrakkarHandler):
    """Handler whose ``on_task_complete`` reads a specific key from the cache.

    The first probe runs a blocking ``on_task_complete`` that holds the
    probe lock — during that window, the test launches a second probe
    (with ``use_cache=True``) and cancels it once A is done. We then
    verify that probe B actually observed the seeded value on its read.
    """

    def __init__(self, *, read_key: str, block_event: asyncio.Event | None = None) -> None:
        self._read_key = read_key
        self._block_event = block_event
        self.read_value: Any = '__unset__'

    async def arrange(
        self,
        messages: list[SourceMessage],
        pending: PendingContext,
    ) -> list[ExecutorTask]:
        msg = messages[0]
        return [_make_precomputed_task(task_id='t-0', offset=msg.offset)]

    async def on_task_complete(self, result: ExecutorResult) -> CollectResult | None:
        # Record the value of the seeded key. If the proxy forwarded
        # correctly to the REAL cache, this reads the seeded value.
        # If it chained through a prior probe's use_cache=False proxy,
        # this would be None (a false miss).
        self.read_value = await self.cache.get(self._read_key)
        if self._block_event is not None:
            await self._block_event.wait()
        return None

    async def on_message_complete(self, group: MessageGroup) -> CollectResult | None:
        return None

    async def on_window_complete(
        self,
        results: list[ExecutorResult],
        source_messages: list[SourceMessage],
    ) -> CollectResult | None:
        return None


async def test_concurrent_probes_with_different_use_cache_dont_chain_proxies():
    """Probe B (use_cache=True) reads the REAL cache even if probe A (use_cache=False) is in flight.

    Regression test for the proxy-chaining bug: when probe B's
    ``RunState`` was built while probe A held the lock, probe B's
    ``DebugCacheProxy`` captured A's proxy as its ``real`` backend. If
    A had ``use_cache=False``, every read from B (with
    ``use_cache=True``) would fall through A's proxy and get a false
    miss for keys that actually live in the real cache.

    The fix constructs the proxy inside ``_run_locked`` (after
    acquiring the lock), so the ``real`` target is the unchanged
    ``handler.cache``, not whatever was swapped in by a concurrent
    probe.
    """
    seeded_cache = _fresh_cache()
    seeded_cache.set('shared-key', 'real-value')

    # Shared handler: attr shared between the two probes. One handler
    # per probe would be more realistic but the runner swaps
    # handler.cache at probe start, so the live cache attribute that
    # matters for this bug is the one on the shared handler.
    block = asyncio.Event()
    handler = _CacheReadingHandler(read_key='shared-key', block_event=block)
    # Attach the seeded real cache to the handler so both probes share it.
    setattr(handler, 'cache', seeded_cache)  # noqa: B010
    runner = DebugRunner(
        handler=handler,
        executor_pool=_make_executor_pool(),
        app_config=_make_config(),
    )

    # Probe A: use_cache=False. While inside on_task_complete, A will
    # block on ``block`` — this ensures A holds the probe lock during
    # the window when B's start_probe runs and creates B's RunState.
    task_a = runner.start_probe(ProbeInput(value='A', offset=1, use_cache=False))

    # Wait a beat for A to actually enter on_task_complete (holding the lock).
    # A small sleep is fine — on_task_complete is reached after arrange
    # + executor.execute on a precomputed task, which is near-instant.
    for _ in range(50):
        if handler.read_value != '__unset__':
            break
        await asyncio.sleep(0.01)
    assert handler.read_value is None, 'probe A should have observed miss (use_cache=False)'

    # Now A is inside on_task_complete, holding the lock. Build probe B:
    # this is when the bug would fire — B's RunState was historically
    # built with real=handler.cache at this exact moment, which was A's
    # proxy (already installed by _run_locked). The fix moves proxy
    # construction to post-lock-acquire, so B's proxy will target the
    # true seeded_cache instead.
    #
    # To keep the test deterministic we don't actually inspect B's
    # observed value here — we release A first, wait for B to acquire
    # the lock and run, then check the captured value.
    handler.read_value = '__unset__'  # reset before B runs
    task_b = runner.start_probe(ProbeInput(value='B', offset=2, use_cache=True))

    # Let A finish so B can acquire the lock.
    block.set()
    await task_a
    report_b = await task_b

    # With the fix, B's on_task_complete saw the seeded value through
    # the REAL cache — NOT None via chained proxies.
    assert handler.read_value == 'real-value', (
        f"probe B with use_cache=True must read seeded value 'real-value' via the real cache; "
        f"got {handler.read_value!r}. If None, the proxy chained through probe A's proxy."
    )
    # B's cache_calls log also proves the read was a hit.
    b_get_calls = [c for c in report_b.cache_calls if c.op == 'get' and c.key == 'shared-key']
    assert len(b_get_calls) == 1
    assert b_get_calls[0].outcome == 'hit'


# ---- Regression: on_task_complete raises → synthesize failure, drop success --


class _TaskSucceedsHookRaisesHandler(_HappyPathHandler):
    """Executor task succeeds cleanly, but ``on_task_complete`` raises.

    Captures what ``on_message_complete`` actually receives so the test
    can assert the probe mirrors production's catch-all behaviour —
    synthesizing a failure ``ExecutorResult`` + ``ExecutorError`` into
    the window/group rather than forwarding the real success result.
    """

    def __init__(self) -> None:
        super().__init__(task_count=1)
        self.observed_group: MessageGroup | None = None

    async def on_task_complete(self, result: ExecutorResult) -> CollectResult | None:
        self.on_task_complete_calls += 1
        raise RuntimeError('hook exploded but task succeeded')

    async def on_message_complete(self, group: MessageGroup) -> CollectResult | None:
        self.on_message_complete_calls += 1
        # Snapshot the group so the test can inspect what the hook saw.
        self.observed_group = group
        return None


async def test_on_message_complete_sees_synthesized_failure_when_on_task_complete_raises():
    """Task succeeds + on_task_complete raises → synthesized failure flows into errors + window.

    Regression test for the terminal-list divergence. Production's
    ``PartitionProcessor._execute_and_track`` (drakkar/partition.py:
    476-495) appends a synthesized ``ExecutorResult(exit_code=-1,
    stderr=str(e), ...)`` to ``window.results`` and a synthesized
    ``ExecutorError`` to ``task_error`` (which later lands in
    ``tracker.errors`` at line 528). It does NOT set ``task_result``
    on this path, so ``tracker.results`` stays EMPTY (line 525-526 only
    runs when ``task_result is not None``).

    The probe mirrors that exact split:
      - ``group.results`` is empty (success-only, no success here).
      - ``group.errors`` has the synthesized ExecutorError.
      - ``on_window_complete``'s ``results`` has the synthesized failure
        ExecutorResult (exit_code=-1).
    """
    handler = _TaskSucceedsHookRaisesHandler()
    runner = DebugRunner(
        handler=handler,
        executor_pool=_make_executor_pool(),
        app_config=_make_config(),
    )

    report = await runner.run(ProbeInput(value='x', offset=1))

    # The on_task_complete error was captured (Task 3's guarantee).
    tc_errors = [e for e in report.errors if e.stage.startswith('task_complete:')]
    assert len(tc_errors) == 1
    assert tc_errors[0].exception_class == 'RuntimeError'

    # on_message_complete ran and observed the hook-raise failure.
    assert handler.observed_group is not None
    group = handler.observed_group

    # Production puts the synthesized result on ``window.results`` NOT
    # ``tracker.results``. So ``MessageGroup.results`` (mirrors
    # tracker.results) is empty on this path.
    assert group.results == []

    # MessageGroup.errors has a synthesized ExecutorError for the
    # failed-via-hook task (production's tracker.errors shape).
    assert len(group.errors) == 1
    assert group.errors[0].task.task_id == 't-0'
    assert 'hook exploded but task succeeded' in (group.errors[0].exception or '')
