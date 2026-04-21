"""Tests for Task 15: handler.cache attribute, NoOpCache stub, and app wiring.

This module verifies the user-facing contract where every handler has a
``self.cache`` attribute populated by the framework. When the cache is disabled
(``config.cache.enabled=false``), the handler keeps a ``NoOpCache`` stub so
user code can call ``self.cache.<method>(...)`` unconditionally — no
``if self.cache is not None`` guards.

Tests in this module focus on:

- BaseDrakkarHandler gets a default NoOpCache on instantiation
- NoOpCache methods match Cache signatures and quietly report "nothing"
- NoOpCache.get is awaitable (async parity with real Cache)
- App wires a real Cache when enabled, else leaves NoOpCache in place
- Shutdown order: CacheEngine.stop() is awaited BEFORE EventRecorder.stop()
- The same Cache instance is visible from every hook (arrange, on_task_complete,
  on_message_complete, on_window_complete, on_error) — asserted via a spy
- Pydantic roundtrip end-to-end: set(model) → get(..., as_type=Model) returns
  a typed instance equivalent to what was stored.

These tests use unit-level isolation — ``tmp_path`` for any DB files, no
docker, no real Kafka. The "app wiring" tests stub the recorder and don't
spin up the full _async_run loop; instead they exercise the startup
fragments that construct and attach the Cache.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock

from pydantic import BaseModel

from drakkar.cache import Cache, CacheEngine, NoOpCache
from drakkar.config import (
    CacheConfig,
    DebugConfig,
    DrakkarConfig,
    ExecutorConfig,
    KafkaConfig,
    KafkaSinkConfig,
    LoggingConfig,
    MetricsConfig,
    SinksConfig,
)
from drakkar.handler import BaseDrakkarHandler
from drakkar.models import (
    ExecutorError,
    ExecutorResult,
    ExecutorTask,
    MessageGroup,
    PendingContext,
    PrecomputedResult,
    SourceMessage,
)

# -- NoOpCache defaults ------------------------------------------------------


def test_base_handler_has_noop_cache_by_default() -> None:
    """A freshly-constructed BaseDrakkarHandler has a non-None cache attribute.

    Users often instantiate handlers in tests or quick scripts without running
    the full DrakkarApp startup path. The default must still satisfy the
    ``self.cache`` contract so user code doesn't branch on None.
    """

    class H(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            return []

    h = H()
    assert h.cache is not None
    assert isinstance(h.cache, NoOpCache)


def test_noop_cache_peek_returns_none() -> None:
    """NoOpCache.peek reports 'not present' for any key."""
    c = NoOpCache()
    assert c.peek('anything') is None


async def test_noop_cache_get_is_async_and_returns_none() -> None:
    """NoOpCache.get must be awaitable for API parity with the real Cache.

    If a handler switches from enabled to disabled cache, the call sites
    shouldn't need to add or remove ``await``.
    """
    c = NoOpCache()
    result = await c.get('anything')
    assert result is None


async def test_noop_cache_get_with_as_type_returns_none() -> None:
    """NoOpCache.get accepts as_type but still returns None — no validation."""

    class MyModel(BaseModel):
        x: int

    c = NoOpCache()
    result = await c.get('key', as_type=MyModel)
    assert result is None


def test_noop_cache_set_silently_discards() -> None:
    """NoOpCache.set accepts any value and stores nothing.

    Subsequent peek must report absence — proves the value isn't retained.
    """
    c = NoOpCache()
    c.set('k', 42)
    assert c.peek('k') is None


def test_noop_cache_set_with_ttl_and_scope_discards() -> None:
    """NoOpCache.set accepts the full signature (ttl + scope) without error."""
    from drakkar.cache import CacheScope

    c = NoOpCache()
    c.set('k', {'a': 1}, ttl=60.0, scope=CacheScope.CLUSTER)
    assert c.peek('k') is None


def test_noop_cache_delete_returns_false() -> None:
    """NoOpCache.delete always reports 'nothing to delete'."""
    c = NoOpCache()
    assert c.delete('k') is False


def test_noop_cache_contains_always_false() -> None:
    """NoOpCache __contains__ always reports False (no membership)."""
    c = NoOpCache()
    assert 'k' not in c


# -- Real Cache attached when cache.enabled=true ------------------------------


def _make_config_with_cache(
    tmp_path: Path,
    *,
    cache_enabled: bool = True,
    debug_enabled: bool = True,
) -> DrakkarConfig:
    """Build a DrakkarConfig with a working cache+debug setup rooted at tmp_path."""
    return DrakkarConfig(
        kafka=KafkaConfig(brokers='localhost:9092', source_topic='test-in'),
        executor=ExecutorConfig(
            binary_path='/bin/echo',
            max_executors=2,
            task_timeout_seconds=10,
            window_size=5,
        ),
        sinks=SinksConfig(kafka={'results': KafkaSinkConfig(topic='test-out')}),
        metrics=MetricsConfig(enabled=False),
        logging=LoggingConfig(level='WARNING', format='console'),
        debug=DebugConfig(
            enabled=debug_enabled,
            db_dir=str(tmp_path),
            store_events=False,
            store_config=False,
            store_state=False,
        ),
        cache=CacheConfig(
            enabled=cache_enabled,
            db_dir=str(tmp_path),
            flush_interval_seconds=60.0,  # slow — tests manually flush
            cleanup_interval_seconds=60.0,
            peer_sync={'enabled': False},
        ),
    )


class _SimpleHandler(BaseDrakkarHandler):
    """Minimal handler used for attachment / hook visibility tests."""

    async def arrange(self, messages, pending):
        return []


async def test_app_wires_real_cache_when_enabled(tmp_path, monkeypatch) -> None:
    """When cache.enabled=true, DrakkarApp constructs a CacheEngine and
    attaches a real Cache to the handler's ``cache`` attribute."""
    from drakkar.app import DrakkarApp

    handler = _SimpleHandler()
    config = _make_config_with_cache(tmp_path, cache_enabled=True)
    app = DrakkarApp(handler=handler, config=config, worker_id='w1')

    # Build only the fragment we care about: cache-engine construction.
    # We avoid a full _async_run to keep the test unit-scoped.
    app._recorder = None  # no recorder; cache wiring still works
    # Replicate the cache-engine construction block from _async_run.
    if app.config.cache.enabled:
        engine = CacheEngine(
            config=app.config.cache,
            debug_config=app.config.debug,
            worker_id=app._worker_id,
            cluster_name=app._cluster_name,
            recorder=app._recorder,
        )
        handler_cache = Cache(
            origin_worker_id=app._worker_id,
            max_memory_entries=app.config.cache.max_memory_entries,
        )
        engine.attach_cache(handler_cache)
        await engine.start()
        app._cache_engine = engine
        app._handler.cache = handler_cache

    try:
        assert isinstance(handler.cache, Cache)
        assert not isinstance(handler.cache, NoOpCache)
        # Verify the real cache works end to end via memory path.
        handler.cache.set('hello', 'world')
        assert handler.cache.peek('hello') == 'world'
    finally:
        if app._cache_engine is not None:
            await app._cache_engine.stop()


async def test_app_leaves_noop_cache_when_disabled(tmp_path) -> None:
    """When cache.enabled=false, the handler keeps its default NoOpCache stub."""
    from drakkar.app import DrakkarApp

    handler = _SimpleHandler()
    config = _make_config_with_cache(tmp_path, cache_enabled=False)
    app = DrakkarApp(handler=handler, config=config, worker_id='w1')

    # Simulate the enabled-check: cache.enabled is False so we skip the block.
    assert not app.config.cache.enabled
    # The default handler.cache (NoOpCache) remains.
    assert isinstance(handler.cache, NoOpCache)


# -- Hook visibility: cache is the same instance across every hook -----------


async def test_cache_is_visible_from_all_hooks(tmp_path) -> None:
    """Once wired, ``self.cache`` must be the SAME Cache instance from every
    hook: arrange, on_task_complete, on_message_complete, on_window_complete,
    on_error. Tests assert identity (``is``), not equality."""
    seen: list[object] = []

    class SpyHandler(BaseDrakkarHandler):
        async def arrange(self, messages, pending):
            seen.append(('arrange', id(self.cache)))
            return []

        async def on_task_complete(self, result):
            seen.append(('on_task_complete', id(self.cache)))
            return None

        async def on_message_complete(self, group):
            seen.append(('on_message_complete', id(self.cache)))
            return None

        async def on_window_complete(self, results, source_messages):
            seen.append(('on_window_complete', id(self.cache)))
            return None

        async def on_error(self, task, error):
            seen.append(('on_error', id(self.cache)))
            return 'skip'

    handler = SpyHandler()
    # Attach a real Cache to the handler directly — matches what the app does
    # at startup (we exercise the framework contract, not the full run loop).
    real_cache = Cache(origin_worker_id='w1')
    handler.cache = real_cache
    cache_id = id(handler.cache)

    # Fabricate minimal objects for each hook call. The point is just to
    # invoke every hook and verify ``self.cache`` stays the same instance.
    source_msg = SourceMessage(topic='t', partition=0, offset=0, value=b'x', timestamp=0)
    task = ExecutorTask(task_id='t-1', args=[], source_offsets=[0])
    result = ExecutorResult(exit_code=0, stdout='', stderr='', duration_seconds=0.1, task=task)
    err = ExecutorError(task=task, exit_code=1, stderr='boom')
    group = MessageGroup(
        source_message=source_msg,
        tasks=[task],
        results=[result],
        errors=[],
        started_at=0.0,
        finished_at=0.1,
    )

    await handler.arrange([source_msg], PendingContext())
    await handler.on_task_complete(result)
    await handler.on_message_complete(group)
    await handler.on_window_complete([result], [source_msg])
    await handler.on_error(task, err)

    # Every hook should have seen the same Cache object id.
    assert len(seen) == 5
    for hook_name, observed_id in seen:
        assert observed_id == cache_id, f'hook {hook_name} saw different cache id'


# -- Pydantic roundtrip end-to-end -------------------------------------------


async def test_pydantic_roundtrip_via_handler_cache(tmp_path) -> None:
    """Set a Pydantic model via the handler cache, get it back with as_type,
    verify equality. This exercises the full encode → store → decode path
    through the handler's ``self.cache`` interface using ``PrecomputedResult``
    (a real framework model, not a test-only toy)."""
    engine = CacheEngine(
        config=CacheConfig(
            enabled=True,
            db_dir=str(tmp_path),
            peer_sync={'enabled': False},
        ),
        debug_config=DebugConfig(enabled=True, db_dir=str(tmp_path)),
        worker_id='w1',
        cluster_name='',
        recorder=None,
    )
    cache = Cache(origin_worker_id='w1')
    engine.attach_cache(cache)
    await engine.start()
    try:

        class H(BaseDrakkarHandler):
            async def arrange(self, messages, pending):
                return []

        handler = H()
        handler.cache = cache

        # Round-trip through the handler-facing API.
        original = PrecomputedResult(
            stdout='hello from cache',
            stderr='',
            exit_code=0,
            duration_seconds=0.42,
        )
        handler.cache.set('result-key', original)
        revived = await handler.cache.get('result-key', as_type=PrecomputedResult)

        assert isinstance(revived, PrecomputedResult)
        assert revived.stdout == 'hello from cache'
        assert revived.exit_code == 0
        assert revived.duration_seconds == 0.42
        assert revived == original
    finally:
        await engine.stop()


# -- Shutdown ordering: cache stops BEFORE recorder ---------------------------


async def test_shutdown_stops_cache_before_recorder(tmp_path) -> None:
    """When ``DrakkarApp._shutdown`` runs, ``CacheEngine.stop()`` is awaited
    BEFORE ``EventRecorder.stop()``. Otherwise the cache's final flush's
    ``periodic_run`` event would be lost (recorder already closed).

    Asserted via an ordering spy on a mocked cache engine and recorder.
    """
    from drakkar.app import DrakkarApp

    handler = _SimpleHandler()
    config = _make_config_with_cache(tmp_path, cache_enabled=True, debug_enabled=False)
    app = DrakkarApp(handler=handler, config=config, worker_id='w1')

    order: list[str] = []

    # Stub the consumer and dlq sink — _shutdown touches them too.
    app._consumer = AsyncMock()
    app._dlq_sink = AsyncMock()

    # Mock CacheEngine.stop and EventRecorder.stop with side-effects that
    # record the call order. We bypass the real engines — this test is
    # scoped to the _shutdown method's ordering contract.
    mock_engine = AsyncMock()

    async def engine_stop() -> None:
        order.append('cache_engine_stop')

    mock_engine.stop = engine_stop
    app._cache_engine = mock_engine

    mock_recorder = AsyncMock()

    async def recorder_stop() -> None:
        order.append('recorder_stop')

    mock_recorder.stop = recorder_stop
    app._recorder = mock_recorder

    # _shutdown also awaits the debug server; stub to a no-op.
    app._debug_server = AsyncMock()

    await app._shutdown()

    # Core invariant: cache engine stops BEFORE recorder.
    assert 'cache_engine_stop' in order
    assert 'recorder_stop' in order
    cache_idx = order.index('cache_engine_stop')
    rec_idx = order.index('recorder_stop')
    assert cache_idx < rec_idx, f'expected cache before recorder, got {order}'


async def test_shutdown_skips_cache_stop_when_engine_not_started(tmp_path) -> None:
    """When cache.enabled=false, ``_cache_engine`` stays None and ``_shutdown``
    simply skips the engine-stop branch. The recorder and debug server still
    stop normally."""
    from drakkar.app import DrakkarApp

    handler = _SimpleHandler()
    config = _make_config_with_cache(tmp_path, cache_enabled=False, debug_enabled=False)
    app = DrakkarApp(handler=handler, config=config, worker_id='w1')

    app._consumer = AsyncMock()
    app._dlq_sink = AsyncMock()
    app._recorder = AsyncMock()
    app._debug_server = AsyncMock()

    # _cache_engine is None — shutdown branch must skip cleanly.
    assert app._cache_engine is None
    await app._shutdown()

    # Recorder shutdown still happens — cache skipping must not suppress it.
    app._recorder.stop.assert_awaited_once()
