"""OOM / SIGKILL detection via per-worker watchdog file.

A *watchdog file* is a small marker file at
``{data_dir}/{worker_id}.watchdog`` whose presence and contents tell the
next startup whether the previous run terminated cleanly:

- File **absent** → no prior run, or the previous shutdown both wrote the
  ``CLEAN_EXIT`` marker AND deleted the file. Either case is clean.
- File **present** with body == ``CLEAN_EXIT`` → the previous shutdown
  reached the end of its drain phase and wrote the marker, but a process
  death between the write and the unlink left the file behind. Still
  considered clean.
- File **present** with any other body (typically empty, from
  :meth:`WatchdogFile.write` at startup) → the previous run never reached
  :meth:`mark_clean` and was killed externally (OOM killer, ``kill -9``,
  pod-pressure eviction, kernel panic). The next startup logs a structured
  warning and increments :data:`drakkar.metrics.suspected_oom_kills`.

This module deliberately depends on nothing beyond ``pathlib`` and
``structlog`` so it can be exercised in unit tests without spinning up
the full ``DrakkarApp``.
"""

from __future__ import annotations

from pathlib import Path

import structlog

from drakkar.metrics import suspected_oom_kills

logger = structlog.get_logger()

# Sentinel written into the watchdog file by ``mark_clean`` at the end of
# a successful drain. If the file is later observed with this exact body,
# the next startup treats it as a clean exit even though the unlink that
# normally follows did not happen (e.g. process killed between the write
# and the unlink). Kept as a plain ASCII string so it survives any OS-
# level newline / encoding quirks across container runtimes.
CLEAN_EXIT_MARKER = 'CLEAN_EXIT'


class WatchdogFile:
    """Per-worker liveness marker that survives a process kill.

    See module docstring for the lifecycle. Construct one instance per
    worker startup; call :meth:`check_previous` once before
    :meth:`write` to decide whether the previous run died cleanly, then
    :meth:`mark_clean` at the very end of a successful shutdown.

    The path is composed as ``{data_dir}/{worker_id}.watchdog``. The
    constructor creates ``data_dir`` if it does not yet exist (with
    parents) so callers do not have to coordinate directory creation
    with the recorder / cache engine.
    """

    def __init__(self, data_dir: Path, worker_id: str) -> None:
        # Coerce to ``Path`` so callers can pass plain strings from
        # ``config.debug.db_dir`` without having to convert at the call
        # site. ``Path(Path(...))`` is a no-op so this is also safe when
        # the caller already owns a ``Path``.
        self._data_dir = Path(data_dir)
        self._worker_id = worker_id
        self._path = self._data_dir / f'{worker_id}.watchdog'

        # Ensure the durable directory exists. ``parents=True`` lets us
        # accept paths like ``/var/drakkar/data`` even on first boot;
        # ``exist_ok=True`` keeps the call idempotent for repeated
        # constructions in tests.
        self._data_dir.mkdir(parents=True, exist_ok=True)

    @property
    def path(self) -> Path:
        """Absolute path to the watchdog file (read-only accessor for tests)."""
        return self._path

    def check_previous(self) -> bool:
        """Decide whether the previous run terminated cleanly.

        Returns True iff the previous run ended cleanly. Emits a
        structured warning and increments
        :data:`drakkar.metrics.suspected_oom_kills` only when the file
        exists and lacks the :data:`CLEAN_EXIT_MARKER` body — that is
        the SIGKILL / OOM signature.

        The check is best-effort: if reading the file raises ``OSError``
        (e.g. a transient filesystem error mid-startup) we treat it as a
        clean run rather than crash the worker — observability over
        availability would be the wrong tradeoff at this layer.
        """
        if not self._path.exists():
            # No prior run, or the previous shutdown both wrote the
            # marker and unlinked successfully. Clean either way.
            return True

        try:
            body = self._path.read_text(encoding='utf-8').strip()
        except OSError:
            # Treat unreadable watchdog as clean — there is no point
            # tripping a false OOM alarm because a kernel-level read
            # failed mid-startup. The metric already covers the common
            # case (file present with empty body).
            return True

        if body == CLEAN_EXIT_MARKER:
            # Previous shutdown wrote the marker but did not get a
            # chance to unlink. Still a clean exit.
            return True

        # File exists, body is NOT the clean-exit marker — the previous
        # run was killed before reaching ``mark_clean``. Surface the
        # event via the structured log AND the Prometheus counter so
        # operators can alert on either.
        logger.warning(
            'previous_run_ended_unexpectedly',
            category='watchdog',
            worker_id=self._worker_id,
            watchdog_path=str(self._path),
            reason='watchdog file present without CLEAN_EXIT marker — possible OOM kill or SIGKILL',
        )
        suspected_oom_kills.inc()
        return False

    def write(self) -> None:
        """Create a fresh watchdog file claiming the slot for this run.

        Body is empty by default. ``mark_clean`` later overwrites this
        with :data:`CLEAN_EXIT_MARKER` before unlinking. ``write_text``
        with mode ``'w'`` truncates any pre-existing file so a crashed
        prior run's leftover content cannot leak into this one.
        """
        self._path.write_text('', encoding='utf-8')

    def mark_clean(self) -> None:
        """Mark the current run as cleanly terminated and remove the file.

        Order is deliberate: write the :data:`CLEAN_EXIT_MARKER` first,
        then unlink. If the process dies between the write and the
        unlink, the next startup still sees the marker and treats it as
        clean. ``FileNotFoundError`` on either step is tolerated — the
        method is idempotent and safe to call without a prior
        :meth:`write`, which simplifies shutdown paths that may run
        before the lifecycle reaches the watchdog-write step.
        """
        try:
            self._path.write_text(CLEAN_EXIT_MARKER, encoding='utf-8')
        except FileNotFoundError:
            # Parent dir vanished (test cleanup, container teardown).
            # Nothing to mark — fall through to the unlink, which will
            # also no-op for the same reason.
            return
        try:
            self._path.unlink()
        except FileNotFoundError:
            # Another process / cleanup hook removed the file between
            # our write and unlink. Idempotent: nothing to do.
            return
