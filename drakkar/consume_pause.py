"""Timed debug pause of the pipeline consumer — the Live page's pause control.

Lets an operator stop message intake for a bounded period (a few presets on
the Live page; any 1..3600 s through the API) to inspect a live worker
without racing it, then auto-resume. Opt-in via ``ui.consume_pause.enabled``
because pausing directly affects the pipeline's work.

Why this never triggers a rebalance
-----------------------------------
The pause uses ``consumer.pause()`` on the assigned partitions — the same
primitive backpressure already uses. The consumer stays subscribed, the
poll loop keeps running (returning nothing), heartbeats continue, and no
offsets move. Kafka's group coordinator sees a perfectly healthy member;
only fetching stops. Leaving/rejoining the group is exactly what this
design avoids.

Coordination with backpressure and stall pauses
-----------------------------------------------
Three actors share ``consumer.pause``/``resume``; the rules that keep them
from stepping on each other:

- While a debug pause is active, the backpressure loop must not resume
  partitions (its low-watermark branch checks ``active`` — see
  ``lifecycle.py``). Backpressure *pausing* during a debug pause is a
  harmless no-op.
- A debug resume leaves partitions paused when backpressure currently
  holds them (``app._paused``) — the backpressure loop resumes them when
  queues drain, exactly as it would have without the debug pause.
- Stall-paused partitions (``dlq.on_send_failure='stall'``) are never
  touched in either direction — they stay paused until restart/revoke,
  same contract as everywhere else.
- Partitions assigned *during* a debug pause are paused immediately by
  ``_on_assign`` (same hole-plugging backpressure does), so a rebalance
  cannot leak messages past an active pause.

Every method runs on the app's main event loop — the UI server dispatches
through ``dispatch_to_loop`` — so the state needs no locking.
"""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Any

import structlog

from drakkar.metrics import consume_pause_active

if TYPE_CHECKING:
    from drakkar.app import DrakkarApp

logger = structlog.get_logger()

# The API-level bound on one pause, independent of the configured presets.
# One hour is far beyond any debugging session and far below "forgot about
# it forever" — the feature is explicitly a bounded pause.
MAX_PAUSE_SECONDS = 3600


class ConsumerNotReadyError(Exception):
    """The pipeline consumer is not running yet (HTTP 503 at the routes layer)."""


class ConsumePauseController:
    """Owns the debug-pause state: the deadline, the auto-resume timer, and
    the pause/resume calls against the app's consumer."""

    def __init__(self, app: DrakkarApp) -> None:
        self._app = app
        self._resume_at_ms: int | None = None
        self._requested_seconds: int | None = None
        self._timer: asyncio.Task | None = None

    @property
    def active(self) -> bool:
        """True while a debug pause holds the consumer."""
        return self._resume_at_ms is not None

    def state(self) -> dict[str, Any]:
        """The wire-shape state served by GET/POST /api/v1/debug/consume-pause."""
        cfg = self._app.config.ui.consume_pause
        return {
            'enabled': cfg.enabled,
            'durations_seconds': list(cfg.durations_seconds),
            'active': self.active,
            'resume_at_ms': self._resume_at_ms,
            'requested_seconds': self._requested_seconds,
        }

    def _pausable_partitions(self) -> list[int]:
        """Assigned partitions minus the stall-paused set (never touched)."""
        app = self._app
        return [p for p in app._processors if p not in app._stalled_partitions]

    async def pause(self, duration_seconds: int) -> dict[str, Any]:
        """Pause consuming for ``duration_seconds``, replacing any active pause.

        A second pause while one is active simply moves the deadline — the
        operator asked for "this much more time", not an error.
        """
        app = self._app
        if app._consumer is None:
            raise ConsumerNotReadyError('Consumer is not running')

        self._cancel_timer()
        self._resume_at_ms = int(time.time() * 1000) + duration_seconds * 1000
        self._requested_seconds = duration_seconds

        partition_ids = self._pausable_partitions()
        if partition_ids:
            await app._consumer.pause(partition_ids)
        consume_pause_active.set(1)
        logger.warning(
            'consume_pause_started',
            category='kafka',
            duration_seconds=duration_seconds,
            resume_at_ms=self._resume_at_ms,
            partitions=partition_ids,
        )

        # Auto-resume timer on the app loop. Tracked in _background_tasks so
        # shutdown cancels it with the rest (same pattern as _on_assign's
        # fire-and-forget pause task in lifecycle.py).
        self._timer = asyncio.create_task(self._auto_resume(duration_seconds))
        app._background_tasks.add(self._timer)
        self._timer.add_done_callback(app._background_tasks.discard)
        return self.state()

    async def resume(self) -> dict[str, Any]:
        """End the debug pause now. Idempotent — resuming while not paused
        just reports the current state."""
        self._cancel_timer()
        if not self.active:
            return self.state()
        self._resume_at_ms = None
        self._requested_seconds = None
        consume_pause_active.set(0)

        app = self._app
        # Backpressure still holds the partitions? Leave them paused — its
        # loop resumes them when queues drain, same as without a debug pause.
        if app._consumer is not None and not app._paused:
            partition_ids = self._pausable_partitions()
            if partition_ids:
                await app._consumer.resume(partition_ids)
        logger.warning('consume_pause_ended', category='kafka')
        return self.state()

    async def _auto_resume(self, duration_seconds: int) -> None:
        """The bounded-pause guarantee: resume when the deadline fires."""
        await asyncio.sleep(duration_seconds)
        logger.info('consume_pause_deadline_reached', category='kafka')
        # `resume` cancels self._timer — which is this very task — before
        # doing anything else. Detach first so the cancel is a no-op.
        self._timer = None
        await self.resume()

    def _cancel_timer(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
