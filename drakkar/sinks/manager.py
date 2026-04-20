"""Sink manager — orchestrates routing, delivery, and error handling.

The SinkManager holds all configured sink instances, validates that
CollectResult payloads target existing sinks, and delivers payloads
with error handling via the on_delivery_error handler hook.
"""

from __future__ import annotations

import time
from collections import defaultdict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import structlog
from pydantic import BaseModel

from drakkar.metrics import sink_deliveries_skipped, sink_delivery_retries
from drakkar.models import CollectResult, DeliveryAction, DeliveryError
from drakkar.sinks.base import BaseSink

if TYPE_CHECKING:
    from drakkar.recorder import EventRecorder

logger = structlog.get_logger()

DeliveryErrorCallback = Callable[[DeliveryError], Awaitable[DeliveryAction]]

# Mapping from CollectResult field name to sink_type string
_FIELD_TO_SINK_TYPE: dict[str, str] = {
    'kafka': 'kafka',
    'postgres': 'postgres',
    'mongo': 'mongo',
    'http': 'http',
    'redis': 'redis',
    'files': 'filesystem',
}


@dataclass
class SinkStats:
    """Per-sink delivery statistics tracked in memory."""

    delivered_count: int = 0
    delivered_payloads: int = 0
    error_count: int = 0
    retry_count: int = 0
    last_delivery_ts: float | None = None
    last_delivery_duration: float | None = None
    last_error: str | None = None
    last_error_ts: float | None = None


class SinkNotConfiguredError(Exception):
    """Raised when a CollectResult references a sink type or name that isn't configured."""


class AmbiguousSinkError(Exception):
    """Raised when a payload has empty sink name but multiple sinks of that type exist."""


class SinkManager:
    """Manages all sink instances and routes payloads from CollectResult.

    Responsibilities:
        - Register and hold sink instances keyed by (type, name)
        - Connect/close all sinks during worker lifecycle
        - Validate that CollectResult only targets configured sinks
        - Route payloads to the correct sink instance
        - Handle delivery errors via the on_delivery_error callback
        - Track per-sink delivery stats for the debug UI
    """

    def __init__(self) -> None:
        self._sinks: dict[tuple[str, str], BaseSink[Any]] = {}
        self._by_type: dict[str, list[BaseSink[Any]]] = defaultdict(list)
        self._stats: dict[tuple[str, str], SinkStats] = {}

    @property
    def sinks(self) -> dict[tuple[str, str], BaseSink[Any]]:
        """All registered sinks keyed by (sink_type, name)."""
        return dict(self._sinks)

    @property
    def sink_count(self) -> int:
        """Total number of registered sinks."""
        return len(self._sinks)

    def register(self, sink: BaseSink[Any]) -> None:
        """Register a sink instance.

        Raises ValueError if a sink with the same (type, name) already exists.
        """
        key = (sink.sink_type, sink.name)
        if key in self._sinks:
            raise ValueError(f'Duplicate sink: type={sink.sink_type!r}, name={sink.name!r}')
        self._sinks[key] = sink
        self._by_type[sink.sink_type].append(sink)
        self._stats[key] = SinkStats()

    def get_sink_info(self) -> list[dict]:
        """Return list of all configured sinks with their type, name, and optional UI URL."""
        return [
            {'sink_type': sink_type, 'name': name, 'ui_url': sink.ui_url}
            for (sink_type, name), sink in self._sinks.items()
        ]

    def get_all_stats(self) -> dict[tuple[str, str], SinkStats]:
        """Return stats for all sinks, keyed by (sink_type, name)."""
        return dict(self._stats)

    async def connect_all(self) -> None:
        """Connect all registered sinks. Raises on first failure."""
        for sink in self._sinks.values():
            await sink.connect()
            await logger.ainfo(
                'sink_connected',
                category='sink',
                sink_type=sink.sink_type,
                sink_name=sink.name,
            )

    async def close_all(self) -> None:
        """Close all registered sinks. Logs errors but doesn't raise."""
        for sink in self._sinks.values():
            try:
                await sink.close()
            except Exception as e:
                await logger.awarning(
                    'sink_close_error',
                    category='sink',
                    sink_type=sink.sink_type,
                    sink_name=sink.name,
                    error=str(e),
                )

    def resolve_sink(self, sink_type: str, sink_name: str) -> BaseSink[Any]:
        """Resolve a sink instance by type and name.

        If sink_name is empty and exactly one sink of that type exists,
        returns that sink (convenient default). Otherwise:
        - Empty name + multiple sinks → AmbiguousSinkError
        - Explicit name not found → SinkNotConfiguredError
        """
        if not sink_name:
            candidates = self._by_type.get(sink_type, [])
            if len(candidates) == 1:
                return candidates[0]
            if len(candidates) == 0:
                raise SinkNotConfiguredError(
                    f'No {sink_type!r} sink configured, but the handler returned {sink_type} payloads'
                )
            names = [s.name for s in candidates]
            raise AmbiguousSinkError(
                f'{len(candidates)} {sink_type!r} sinks configured ({names}), '
                f'but payload has empty sink name — specify which one'
            )
        key = (sink_type, sink_name)
        if key not in self._sinks:
            raise SinkNotConfiguredError(
                f'Sink {sink_type!r}/{sink_name!r} not configured, but the handler returned a payload targeting it'
            )
        return self._sinks[key]

    def validate_collect(self, result: CollectResult) -> None:
        """Validate that every payload in the result targets a configured sink.

        Iterates all populated fields, resolves each payload's sink,
        and raises SinkNotConfiguredError or AmbiguousSinkError on first problem.
        Called before delivery so the worker crashes fast on misconfiguration.
        """
        for field_name, sink_type in _FIELD_TO_SINK_TYPE.items():
            payloads = getattr(result, field_name)
            if not payloads:
                continue
            for payload in payloads:
                self.resolve_sink(sink_type, payload.sink)

    async def deliver_all(
        self,
        result: CollectResult,
        on_delivery_error: DeliveryErrorCallback,
        partition_id: int,
        max_retries: int = 3,
        recorder: EventRecorder | None = None,
    ) -> None:
        """Route and deliver all payloads in a CollectResult to their sinks.

        Groups payloads by (sink_type, resolved_sink_name), then delivers
        each group. On delivery failure, calls on_delivery_error and handles
        the returned action (DLQ, RETRY, SKIP).

        Args:
            result: The CollectResult from on_task_complete(),
                on_message_complete(), or on_window_complete().
            on_delivery_error: Handler callback for delivery failures.
            partition_id: Source partition (for DLQ metadata).
            max_retries: Max delivery retry attempts before falling through to DLQ.
            recorder: Optional EventRecorder for sink delivery/error events.
        """
        groups: dict[tuple[str, str], list[BaseModel]] = defaultdict(list)

        for field_name, sink_type in _FIELD_TO_SINK_TYPE.items():
            payloads = getattr(result, field_name)
            if not payloads:
                continue
            for payload in payloads:
                sink = self.resolve_sink(sink_type, payload.sink)
                groups[(sink.sink_type, sink.name)].append(payload)

        for (sink_type, sink_name), payloads in groups.items():
            sink = self._sinks[(sink_type, sink_name)]
            stats = self._stats[(sink_type, sink_name)]
            attempt = 0
            while True:
                try:
                    start = time.monotonic()
                    await sink.deliver(payloads)
                    duration = time.monotonic() - start
                    stats.delivered_count += 1
                    stats.delivered_payloads += len(payloads)
                    stats.last_delivery_ts = time.time()
                    stats.last_delivery_duration = round(duration, 4)
                    if recorder:
                        recorder.record_sink_delivery(
                            sink_type=sink_type,
                            sink_name=sink_name,
                            payload_count=len(payloads),
                            duration=duration,
                        )
                    break
                except Exception as e:
                    attempt += 1
                    stats.error_count += 1
                    stats.last_error = str(e)
                    stats.last_error_ts = time.time()
                    if recorder:
                        recorder.record_sink_error(
                            sink_type=sink_type,
                            sink_name=sink_name,
                            error=str(e),
                            attempt=attempt,
                        )
                    error = DeliveryError(
                        sink_name=sink_name,
                        sink_type=sink_type,
                        error=str(e),
                        payloads=payloads,
                    )
                    action = await on_delivery_error(error)

                    if action == DeliveryAction.RETRY and attempt < max_retries:
                        stats.retry_count += 1
                        sink_delivery_retries.labels(sink_type=sink_type, sink_name=sink_name).inc()
                        await logger.awarning(
                            'sink_delivery_retry',
                            category='sink',
                            sink_type=sink_type,
                            sink_name=sink_name,
                            attempt=attempt,
                        )
                        continue
                    elif action == DeliveryAction.SKIP:
                        sink_deliveries_skipped.labels(sink_type=sink_type, sink_name=sink_name).inc()
                        await logger.awarning(
                            'sink_delivery_skipped',
                            category='sink',
                            sink_type=sink_type,
                            sink_name=sink_name,
                            payload_count=len(payloads),
                        )
                        break
                    else:
                        # DLQ or RETRY exhausted — DLQ handling is done by the caller (app.py)
                        # since it needs access to the DLQ sink which lives outside the manager.
                        await logger.awarning(
                            'sink_delivery_failed_to_dlq',
                            category='sink',
                            sink_type=sink_type,
                            sink_name=sink_name,
                            payload_count=len(payloads),
                            attempts=attempt,
                        )
                        break
