"""Main Drakkar application — wires all components together.

Orchestrates Kafka consumption, subprocess execution, sink delivery,
and offset management. Uses the pluggable sink system for output.
"""

import asyncio
import math
import os
import signal
import time
from collections.abc import Coroutine
from datetime import UTC, datetime
from pathlib import Path

import structlog
from structlog.contextvars import bind_contextvars, unbind_contextvars

from drakkar import __version__
from drakkar.cache import Cache, CacheEngine
from drakkar.config import DrakkarConfig, load_config
from drakkar.consumer import KafkaConsumer
from drakkar.executor import ExecutorPool
from drakkar.handler import BaseDrakkarHandler
from drakkar.logging import close_logging, setup_logging
from drakkar.metrics import (
    assigned_partitions,
    backpressure_active,
    consumer_idle,
    discover_handler_metrics,
    executor_idle_waste,
    start_metrics_server,
    total_queued,
    worker_info,
)
from drakkar.models import CollectResult, DeliveryAction, DeliveryError
from drakkar.partition import PartitionProcessor
from drakkar.periodic import discover_periodic_tasks, run_periodic_task
from drakkar.recorder import EventRecorder
from drakkar.sinks.dlq import DLQSink
from drakkar.sinks.filesystem import FileSink
from drakkar.sinks.http import HttpSink
from drakkar.sinks.kafka import KafkaSink
from drakkar.sinks.manager import SinkManager, SinkNotConfiguredError
from drakkar.sinks.mongo import MongoSink
from drakkar.sinks.postgres import PostgresSink
from drakkar.sinks.redis import RedisSink

logger = structlog.get_logger()

POLL_IDLE_SLEEP = 0.05  # seconds to sleep when Kafka poll returns no messages

# Loopback hosts treated as safe for an unauthenticated debug UI. Anything
# outside this set exposes operational data (events, config, probe) to other
# machines, so we require an ``auth_token`` to be set before allowing startup.
# Matched case-insensitively so ``LOCALHOST`` and ``127.0.0.1`` behave alike.
_LOOPBACK_HOSTS: frozenset[str] = frozenset({'127.0.0.1', 'localhost', '::1'})


class InsecureDebugConfigError(RuntimeError):
    """Debug UI is configured in a way that exposes operational data without auth."""


def _validate_debug_security(config: DrakkarConfig) -> None:
    """Fail fast if the debug UI is bound to a non-loopback host without auth.

    The debug UI serves the flight recorder (events, per-task output, worker
    config, Kafka peek endpoints). Binding to a public interface without
    ``auth_token`` set would leak all of that to anything on the network.

    Whitespace-only ``auth_token`` values are treated as empty — a string of
    spaces protects nothing but might fool an operator into thinking it does.
    ``DebugConfig`` already strips the token on load (see the field validator),
    so the check below is a plain emptiness test.

    Raises:
        InsecureDebugConfigError: When debug is enabled, bound to a
            non-loopback host, and ``auth_token`` is empty or whitespace.
    """
    if not config.debug.enabled:
        return

    host_normalized = config.debug.host.strip().lower()
    if host_normalized in _LOOPBACK_HOSTS:
        return

    if config.debug.auth_token != '':
        return

    raise InsecureDebugConfigError(
        f'Debug UI is bound to a non-loopback host ({config.debug.host}) without auth_token. '
        'Either set debug.auth_token, or set debug.host=127.0.0.1, or set debug.enabled=false.'
    )


class DrakkarApp:
    """Main application that orchestrates Kafka consumption, subprocess
    execution, and result delivery to configured sinks.
    """

    def __init__(
        self,
        handler: BaseDrakkarHandler,
        config_path: str | Path | None = None,
        config: DrakkarConfig | None = None,
        worker_id: str = '',
    ) -> None:
        if config is not None:
            self._config = config
        else:
            self._config = load_config(config_path)

        self._handler = handler
        self._worker_id = worker_id or os.environ.get(self._config.worker_name_env, '') or f'drakkar-{id(self):x}'
        self._cluster_name = ''
        if self._config.cluster_name_env:
            self._cluster_name = os.environ.get(self._config.cluster_name_env, '')
        if not self._cluster_name:
            self._cluster_name = self._config.cluster_name
        self._start_time = time.monotonic()

        self._executor_pool: ExecutorPool | None = None
        self._consumer: KafkaConsumer | None = None
        # SinkManager receives the circuit breaker default so the breaker
        # installed on each registered sink honors operator thresholds.
        self._sink_manager: SinkManager = SinkManager(
            circuit_breaker_config=self._config.sinks.circuit_breaker,
        )
        self._dlq_sink: DLQSink | None = None
        self._recorder: EventRecorder | None = None
        self._debug_server = None
        # Framework cache — constructed in _async_run when cache.enabled=true,
        # else the handler keeps its default NoOpCache stub. Held here so
        # _shutdown can stop the engine in the correct order (before recorder,
        # so the final flush's periodic_run event still records).
        self._cache_engine: CacheEngine | None = None

        self._processors: dict[int, PartitionProcessor] = {}
        self._running = False
        self._paused = False
        self._background_tasks: set[asyncio.Task] = set()
        self._periodic_tasks: list[asyncio.Task] = []
        self._config_summary: str = ''
        # Main event loop — captured at the top of _async_run. The debug
        # FastAPI server runs in a separate thread with its own event loop,
        # but the ExecutorPool's asyncio.Semaphore is bound to this loop.
        # The Message Probe endpoint uses this ref to dispatch runner.run()
        # back here via asyncio.run_coroutine_threadsafe so acquires don't
        # fail with "bound to a different event loop" on a contended pool.
        self._loop: asyncio.AbstractEventLoop | None = None

    @property
    def config(self) -> DrakkarConfig:
        return self._config

    @property
    def handler(self) -> BaseDrakkarHandler:
        """Return the user-supplied handler instance.

        Exposed so the debug server can introspect the handler (e.g. to
        detect which completion hooks are implemented) without reaching
        into private state. Read-only.
        """
        return self._handler

    @property
    def processors(self) -> dict[int, PartitionProcessor]:
        return self._processors

    @property
    def recorder(self) -> EventRecorder | None:
        return self._recorder

    @property
    def sink_manager(self) -> SinkManager:
        return self._sink_manager

    @property
    def cache_engine(self) -> CacheEngine | None:
        """Return the framework-managed CacheEngine when cache.enabled=True.

        The debug UI reads this to decide whether to render the Cache nav
        link and the /debug/cache page. None when the cache is disabled —
        debug server routes should gracefully 404 in that state rather
        than attempting to query a non-existent reader connection.
        """
        return self._cache_engine

    @property
    def config_summary(self) -> str:
        return self._config_summary

    @property
    def main_loop(self) -> asyncio.AbstractEventLoop | None:
        """Return the event loop the pipeline runs on, or None before start.

        Exposed so the debug server (which runs on a separate thread + loop)
        can dispatch the Message Probe back to this loop — the ExecutorPool's
        semaphore is bound here and cannot be acquired from another loop
        once it has contention.
        """
        return self._loop

    def run(self) -> None:
        """Start the application. Blocks until shutdown."""
        setup_logging(
            self._config.logging,
            worker_id=self._worker_id,
            consumer_group=self._config.kafka.consumer_group,
            version=__version__,
            cluster_name=self._cluster_name,
        )
        asyncio.run(self._async_run())

    def _build_sinks(self) -> None:
        """Create sink instances from config and register with SinkManager."""
        kafka_brokers = self._config.kafka.brokers

        for name, cfg in self._config.sinks.kafka.items():
            self._sink_manager.register(KafkaSink(name, cfg, brokers_fallback=kafka_brokers))

        for name, cfg in self._config.sinks.postgres.items():
            self._sink_manager.register(PostgresSink(name, cfg))

        for name, cfg in self._config.sinks.mongo.items():
            self._sink_manager.register(MongoSink(name, cfg))

        for name, cfg in self._config.sinks.http.items():
            self._sink_manager.register(HttpSink(name, cfg))

        for name, cfg in self._config.sinks.redis.items():
            self._sink_manager.register(RedisSink(name, cfg))

        for name, cfg in self._config.sinks.filesystem.items():
            self._sink_manager.register(FileSink(name, cfg))

    def _build_dlq(self) -> None:
        """Create the DLQ sink from config."""
        dlq_topic = self._config.dlq.topic or f'{self._config.kafka.source_topic}_dlq'
        dlq_brokers = self._config.dlq.brokers or self._config.kafka.brokers
        self._dlq_sink = DLQSink(topic=dlq_topic, brokers=dlq_brokers)

    async def _async_run(self) -> None:
        # Capture the running loop so the debug server (separate thread)
        # can dispatch probes back here for ExecutorPool access.
        self._loop = asyncio.get_running_loop()

        log = logger.bind(worker_id=self._worker_id)

        bind_contextvars(hook='on_startup')
        self._config = await self._handler.on_startup(self._config)
        unbind_contextvars('hook')

        self._config_summary = self._config.config_summary(
            worker_id=self._worker_id,
            cluster_name=self._cluster_name,
        )
        await log.ainfo('drakkar_starting', category='lifecycle', config=self._config_summary)

        # validate at least one sink is configured
        if self._config.sinks.is_empty:
            raise SinkNotConfiguredError('No sinks configured. Add at least one sink to the sinks: section in config.')

        self._executor_pool = ExecutorPool(
            binary_path=self._config.executor.binary_path,
            max_executors=self._config.executor.max_executors,
            task_timeout_seconds=self._config.executor.task_timeout_seconds,
            env=self._config.executor.env,
            inherit_parent_env=self._config.executor.env_inherit_parent,
            inherit_deny_patterns=self._config.executor.env_inherit_deny,
        )

        start_metrics_server(self._config.metrics)
        worker_info.info(
            {
                'worker_id': self._worker_id,
                'version': __version__,
                'consumer_group': self._config.kafka.consumer_group,
            }
        )

        user_metrics = discover_handler_metrics(self._handler)
        if user_metrics:
            await log.ainfo(
                'user_metrics_discovered',
                category='lifecycle',
                metrics=[f'{m._name} ({m._type})' for m in user_metrics.values()],
            )

        if self._config.debug.enabled:
            # Fail fast before anything observable starts up. Raising here
            # skips the recorder and debug server entirely — no half-started
            # artifacts remain for the operator to clean up.
            _validate_debug_security(self._config)

            self._recorder = EventRecorder(
                self._config.debug,
                worker_name=self._worker_id,
                cluster_name=self._cluster_name,
            )
            self._recorder.set_state_provider(self._get_worker_state)
            await self._recorder.start()
            await self._recorder.write_config(self._config)

            from drakkar.debug_server import DebugServer

            self._debug_server = DebugServer(
                config=self._config.debug,
                recorder=self._recorder,
                app=self,
            )
            await self._debug_server.start()

        # Framework cache. Constructed after the recorder so the cache
        # engine can pass it as the sink for its periodic_run events. If
        # cache.enabled=false, we leave the handler's default NoOpCache stub
        # in place — user code can call self.cache.<method>(...) unconditionally.
        if self._config.cache.enabled:
            self._cache_engine = CacheEngine(
                config=self._config.cache,
                debug_config=self._config.debug,
                worker_id=self._worker_id,
                cluster_name=self._cluster_name,
                recorder=self._recorder,
            )
            # The handler-facing Cache: origin_worker_id is this worker's
            # id so LWW tiebreaks during peer-sync can identify our writes.
            handler_cache = Cache(
                origin_worker_id=self._worker_id,
                max_memory_entries=self._config.cache.max_memory_entries,
            )
            # Wire the Cache to the engine BEFORE start() so the engine's
            # reader connection is attached atomically as part of start().
            self._cache_engine.attach_cache(handler_cache)
            await self._cache_engine.start()
            # Replace the handler's default NoOpCache with the real one.
            # Users access via self.cache regardless of which variant is
            # installed — signatures are identical.
            self._handler.cache = handler_cache

        # build and connect sinks
        self._build_sinks()
        await self._sink_manager.connect_all()

        # build and connect DLQ
        self._build_dlq()
        assert self._dlq_sink is not None
        await self._dlq_sink.connect()

        # log sink topology
        await log.ainfo(
            'sinks_configured',
            category='lifecycle',
            sinks=self._config.sinks.summary(),
            dlq_topic=self._dlq_sink.topic,
        )

        self._consumer = KafkaConsumer(
            config=self._config.kafka,
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
        )

        # expose postgres pool for on_ready if available
        pg_pool = None
        for (sink_type, _), sink in self._sink_manager.sinks.items():
            if sink_type == 'postgres' and hasattr(sink, 'pool'):
                pg_pool = sink.pool
                break

        bind_contextvars(hook='on_ready')
        await self._handler.on_ready(self._config, pg_pool)
        unbind_contextvars('hook')

        # start periodic tasks declared on the handler
        for name, method, meta in discover_periodic_tasks(self._handler):
            task = asyncio.create_task(
                run_periodic_task(
                    name=name,
                    coro_fn=method,
                    seconds=meta.seconds,
                    on_error=meta.on_error,
                    recorder=self._recorder,
                ),
                name=f'periodic:{name}',
            )
            self._periodic_tasks.append(task)

        # Stagger startup: sleep until the next wall-clock alignment
        # boundary so a fleet of workers in a rolling deploy converges
        # on a single Kafka consumer-group rebalance instead of N. See
        # KafkaConfig.startup_align_* for tuning and rationale.
        if self._config.kafka.startup_align_enabled:
            from drakkar.utils import wait_for_aligned_startup

            min_wait = self._config.kafka.startup_min_wait_seconds
            interval = self._config.kafka.startup_align_interval_seconds
            target_wall = math.ceil((time.time() + min_wait) / interval) * interval
            await log.ainfo(
                'startup_align_waiting',
                category='lifecycle',
                min_wait_seconds=min_wait,
                align_interval_seconds=interval,
                target_wall_unix=target_wall,
                target_wall_iso=datetime.fromtimestamp(target_wall, tz=UTC).isoformat(),
            )
            slept = await wait_for_aligned_startup(min_wait, interval)
            await log.ainfo('startup_align_done', category='lifecycle', slept_seconds=round(slept, 3))

        await self._consumer.subscribe()
        self._running = True

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self._handle_signal)

        try:
            await self._poll_loop()
        except asyncio.CancelledError:
            pass
        finally:
            await self._shutdown()

    def _total_queued(self) -> int:
        """Total messages buffered across all partition queues + in-flight tasks."""
        return sum(p.queue_size + p.inflight_count for p in self._processors.values())

    def _total_waiting(self) -> int:
        """Messages waiting in partition queues, not yet dispatched to executors."""
        return sum(p.queue_size for p in self._processors.values())

    def _get_worker_state(self) -> dict:
        """Return current worker state for the recorder's state sync."""
        return {
            'uptime_seconds': time.monotonic() - self._start_time,
            'assigned_partitions': sorted(self._processors.keys()),
            'partition_count': len(self._processors),
            'pool_active': self._executor_pool.active_count if self._executor_pool else 0,
            'pool_max': self._executor_pool.max_executors if self._executor_pool else 0,
            'total_queued': self._total_queued(),
            'paused': self._paused,
        }

    async def _poll_loop(self) -> None:
        """Main polling loop with backpressure via Kafka pause/resume."""
        assert self._consumer is not None
        assert self._executor_pool is not None
        max_executors = self._config.executor.max_executors
        high_watermark = max_executors * self._config.executor.backpressure_high_multiplier
        low_watermark = max(1, max_executors * self._config.executor.backpressure_low_multiplier)
        last_tick = time.monotonic()

        while self._running:
            now = time.monotonic()
            dt = now - last_tick
            last_tick = now

            total = self._total_queued()
            total_queued.set(total)

            # Executor idle waste: slots sitting free while messages wait in queues.
            # Uses queue_size only (not inflight) — inflight tasks ARE using slots.
            waiting = self._total_waiting()
            if waiting > 0:
                idle_slots = max_executors - self._executor_pool.active_count
                if idle_slots > 0:
                    executor_idle_waste.inc(idle_slots * dt)

            if self._paused and total <= low_watermark:
                partition_ids = list(self._processors.keys())
                if partition_ids:
                    await self._consumer.resume(partition_ids)
                    self._paused = False
                    backpressure_active.set(0)

            if not self._paused and total >= high_watermark:
                partition_ids = list(self._processors.keys())
                if partition_ids:
                    await self._consumer.pause(partition_ids)
                    self._paused = True
                    backpressure_active.set(1)

            messages = await self._consumer.poll_batch()
            for msg in messages:
                processor = self._processors.get(msg.partition)
                if processor:
                    processor.enqueue(msg)

            if not messages:
                # Consumer idle: no messages from Kafka, nothing queued, not paused.
                # Measures time with genuinely nothing to do (consumer lag is zero).
                if total == 0 and not self._paused:
                    consumer_idle.inc(dt)
                await asyncio.sleep(POLL_IDLE_SLEEP)

    def _on_assign(self, partition_ids: list[int]) -> None:
        """Handle new partition assignments."""
        assert self._executor_pool is not None
        if self._recorder:
            self._recorder.record_assigned(partition_ids)
        newly_added: list[int] = []
        for pid in partition_ids:
            if pid not in self._processors:
                processor = PartitionProcessor(
                    partition_id=pid,
                    handler=self._handler,
                    executor_pool=self._executor_pool,
                    window_size=self._config.executor.window_size,
                    max_retries=self._config.executor.max_retries,
                    on_collect=self._handle_collect,
                    on_commit=self._handle_commit,
                    recorder=self._recorder,
                )
                self._processors[pid] = processor
                processor.start()
                newly_added.append(pid)

        assigned_partitions.set(len(self._processors))

        # If backpressure is active, the poll loop has already paused the
        # previously-assigned partitions. Newly-assigned partitions were not
        # in that pause set, so Kafka would deliver messages from them until
        # the next poll tick re-evaluated the watermark. Pause them now so
        # the backpressure gate is not bypassed between assignment and the
        # next _poll_loop iteration.
        if self._paused and newly_added and self._consumer is not None:
            consumer = self._consumer

            async def _pause_newly_assigned() -> None:
                await consumer.pause(newly_added)

            pt = asyncio.ensure_future(self._safe_call(_pause_newly_assigned()))
            self._background_tasks.add(pt)
            pt.add_done_callback(self._background_tasks.discard)

        async def _on_assign_with_ctx() -> None:
            bind_contextvars(hook='on_assign', partitions=partition_ids)
            try:
                await self._handler.on_assign(partition_ids)
            finally:
                unbind_contextvars('hook', 'partitions')

        t = asyncio.ensure_future(self._safe_call(_on_assign_with_ctx()))
        self._background_tasks.add(t)
        t.add_done_callback(self._background_tasks.discard)

    def _on_revoke(self, partition_ids: list[int]) -> None:
        """Handle partition revocation."""
        if self._recorder:
            self._recorder.record_revoked(partition_ids)
        for pid in partition_ids:
            processor = self._processors.pop(pid, None)
            if processor:
                t = asyncio.ensure_future(self._stop_processor(processor))
                self._background_tasks.add(t)
                t.add_done_callback(self._background_tasks.discard)

        assigned_partitions.set(len(self._processors))

        async def _on_revoke_with_ctx() -> None:
            bind_contextvars(hook='on_revoke', partitions=partition_ids)
            try:
                await self._handler.on_revoke(partition_ids)
            finally:
                unbind_contextvars('hook', 'partitions')

        t = asyncio.ensure_future(self._safe_call(_on_revoke_with_ctx()))
        self._background_tasks.add(t)
        t.add_done_callback(self._background_tasks.discard)

    async def _safe_call(self, coro: Coroutine) -> None:
        """Run a coroutine and log any exception instead of leaving it unretrieved."""
        try:
            await coro
        except Exception as e:
            logger.warning('async_callback_failed', category='lifecycle', error=str(e))

    async def _stop_processor(self, processor: PartitionProcessor) -> None:
        """Drain in-flight tasks, commit final offsets, then stop.

        Only commits the watermark when drain completed cleanly. If drain
        timed out, tasks may still be in flight — committing their offsets
        now would silently skip them on partition reassign and lose data.
        Preferring at-least-once duplication over silent loss.
        """
        try:
            processor.signal_stop()
            drained_cleanly = False
            try:
                await asyncio.wait_for(processor.drain(), timeout=self._config.executor.drain_timeout_seconds)
                drained_cleanly = True
            except TimeoutError:
                logger.warning(
                    'stop_processor_drain_timeout',
                    category='lifecycle',
                    partition=processor.partition_id,
                    inflight=processor.inflight_count,
                    queue_size=processor.queue_size,
                )
            if drained_cleanly:
                committable = processor.offset_tracker.committable()
                if committable is not None and self._consumer:
                    try:
                        await self._consumer.commit({processor.partition_id: committable})
                        processor.offset_tracker.acknowledge_commit(committable)
                    except Exception as e:
                        logger.warning(
                            'stop_processor_commit_failed',
                            category='kafka',
                            partition=processor.partition_id,
                            error=str(e),
                        )
            await processor.stop()
        except Exception as e:
            logger.warning(
                'stop_processor_failed',
                category='lifecycle',
                partition=processor.partition_id,
                error=str(e),
            )

    async def _handle_collect(self, result: CollectResult, partition_id: int) -> None:
        """Deliver CollectResult payloads to configured sinks.

        Validates that all payload sink types are configured, then delivers
        via SinkManager. On delivery error, calls the handler's
        on_delivery_error hook and handles DLQ/RETRY/SKIP.
        """
        if not result.has_outputs:
            return

        self._sink_manager.validate_collect(result)

        async def _on_delivery_error(error: 'DeliveryError') -> DeliveryAction:
            bind_contextvars(hook='on_delivery_error', sink_type=error.sink_type, sink_name=error.sink_name)
            try:
                action = await self._handler.on_delivery_error(error)
            finally:
                unbind_contextvars('hook', 'sink_type', 'sink_name')
            if action == DeliveryAction.DLQ and self._dlq_sink:
                await self._dlq_sink.send(error, partition_id=partition_id)
            return action

        await self._sink_manager.deliver_all(
            result,
            on_delivery_error=_on_delivery_error,
            partition_id=partition_id,
            recorder=self._recorder,
        )

        if self._recorder:
            for payload in result.kafka:
                self._recorder.record_produced(payload, source_partition=partition_id)

    async def _handle_commit(self, partition_id: int, offset: int) -> None:
        """Commit an offset for a specific partition."""
        if self._consumer:
            await self._consumer.commit({partition_id: offset})
        if self._recorder:
            self._recorder.record_committed(partition_id, offset)

    def _handle_signal(self) -> None:
        """Handle shutdown signals."""
        logger.info('shutdown_signal_received', category='lifecycle')
        self._running = False

    async def _shutdown(self) -> None:
        """Graceful shutdown: cancel periodic tasks, drain executors, commit offsets, close sinks."""
        log = logger.bind(worker_id=self._worker_id)
        await log.ainfo('drakkar_shutting_down', category='lifecycle')

        # cancel periodic tasks
        for task in self._periodic_tasks:
            task.cancel()
        if self._periodic_tasks:
            await asyncio.gather(*self._periodic_tasks, return_exceptions=True)
            self._periodic_tasks.clear()

        for processor in list(self._processors.values()):
            processor.signal_stop()

        drain_timeout = self._config.executor.drain_timeout_seconds
        await log.ainfo('draining_executors', category='lifecycle', timeout=drain_timeout)
        drained_cleanly = False
        try:
            await asyncio.wait_for(self._drain_all_processors(), timeout=drain_timeout)
            drained_cleanly = True
            await log.ainfo('executors_drained', category='lifecycle')
        except TimeoutError:
            await log.awarning(
                'drain_timeout',
                category='lifecycle',
                msg=f'some executors did not finish in {drain_timeout}s; skipping final commit',
            )

        # Only commit final offsets if drain succeeded cleanly. After a
        # timeout we cannot be sure tasks have stopped running, so committing
        # here would silently skip in-flight work on restart — preferring
        # at-least-once duplication over silent loss.
        if drained_cleanly:
            for processor in list(self._processors.values()):
                committable = processor.offset_tracker.committable()
                if committable is not None and self._consumer:
                    try:
                        await self._consumer.commit({processor.partition_id: committable})
                        processor.offset_tracker.acknowledge_commit(committable)
                    except Exception as e:
                        await log.awarning(
                            'final_commit_failed',
                            category='kafka',
                            partition=processor.partition_id,
                            error=str(e),
                        )

        for processor in list(self._processors.values()):
            await processor.stop()
        self._processors.clear()

        # Wait for background tasks scheduled by rebalance callbacks
        # (_stop_processor from revoke, on_assign/revoke handler hooks,
        # backpressure pauses) to complete BEFORE we close the consumer.
        # These tasks hold references to self._consumer; closing it while
        # they run would cause use-after-close errors and skip their final
        # commits.
        if self._background_tasks:
            bg_snapshot = list(self._background_tasks)
            try:
                await asyncio.wait_for(
                    asyncio.gather(*bg_snapshot, return_exceptions=True),
                    timeout=drain_timeout,
                )
            except TimeoutError:
                await log.awarning(
                    'background_task_drain_timeout',
                    category='lifecycle',
                    count=len(bg_snapshot),
                )

        # Stop the cache engine BEFORE the recorder so the engine's final
        # flush (``_flush_once`` called inside ``CacheEngine.stop()``) can
        # still record its ``periodic_run`` event through the recorder. If
        # we stopped the recorder first, that last event would be dropped —
        # users lose observability on the most critical flush of the
        # lifecycle (the one that persists whatever was in memory when
        # shutdown signalled).
        if self._cache_engine is not None:
            await self._cache_engine.stop()
            self._cache_engine = None

        if self._recorder:
            await self._recorder.stop()

        if self._debug_server:
            await self._debug_server.stop()

        # close all sinks and DLQ
        await self._sink_manager.close_all()
        if self._dlq_sink:
            await self._dlq_sink.close()

        if self._consumer:
            await self._consumer.close()

        await log.ainfo('drakkar_stopped', category='lifecycle')
        close_logging()

    async def _drain_all_processors(self) -> None:
        """Wait for all partition processors to finish queued + in-flight work."""
        drain_tasks = [
            processor.drain()
            for processor in self._processors.values()
            if processor.queue_size > 0 or processor.offset_tracker.has_pending() or processor.inflight_count > 0
        ]
        if drain_tasks:
            await asyncio.gather(*drain_tasks)
