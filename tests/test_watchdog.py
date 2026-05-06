"""Tests for the OOM / SIGKILL watchdog file.

Covers :class:`drakkar.watchdog.WatchdogFile` lifecycle:

  - Clean startup with no prior watchdog → no warn, no metric.
  - Prior watchdog without the ``CLEAN_EXIT`` marker → warn + metric.
  - Prior watchdog WITH the ``CLEAN_EXIT`` marker → no warn, no metric.
  - ``write`` then ``mark_clean`` round-trip removes the file.
  - ``mark_clean`` is safe to call without a prior ``write``.

Pattern mirrors ``tests/test_shutdown_metrics.py`` for the metric
assertions: snapshot the counter via ``Counter._value.get()`` before
the call, assert the delta after. Structured-log assertions go through
``structlog.testing.capture_logs`` (same pattern as
``tests/test_cache_engine_lifecycle.py``) since the project's
structlog configuration does not pipe events into the stdlib ``logging``
module that ``caplog`` would observe.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import structlog.testing

from drakkar.metrics import suspected_oom_kills
from drakkar.watchdog import CLEAN_EXIT_MARKER, WatchdogFile

WORKER_ID = 'test-worker-7'


@pytest.fixture
def watchdog(tmp_path: Path) -> WatchdogFile:
    """Construct a :class:`WatchdogFile` rooted at a fresh temp dir.

    The constructor performs no disk I/O; directory creation happens
    lazily inside :meth:`WatchdogFile.write`. ``tmp_path`` is already
    created by pytest so this fixture leaves disk state untouched.
    """
    return WatchdogFile(data_dir=tmp_path, worker_id=WORKER_ID)


# --- check_previous: clean cases ---


def test_clean_startup_no_previous_watchdog(tmp_path: Path) -> None:
    """An empty data directory → no prior run → ``check_previous`` returns
    ``True``, no warning is logged, the OOM counter stays put.
    """
    before = suspected_oom_kills._value.get()

    wd = WatchdogFile(data_dir=tmp_path, worker_id=WORKER_ID)

    with structlog.testing.capture_logs() as captured:
        result = wd.check_previous()

    assert result is True
    suspect_events = [ev for ev in captured if ev.get('event') == 'previous_run_ended_unexpectedly']
    assert suspect_events == []
    assert suspected_oom_kills._value.get() == before


def test_previous_watchdog_with_clean_exit(tmp_path: Path) -> None:
    """A leftover watchdog body of ``CLEAN_EXIT`` is treated as a clean
    exit (the prior shutdown wrote the marker but did not get to unlink).

    No warning, no metric increment.
    """
    before = suspected_oom_kills._value.get()
    path = tmp_path / f'{WORKER_ID}.watchdog'
    path.write_text(CLEAN_EXIT_MARKER, encoding='utf-8')

    wd = WatchdogFile(data_dir=tmp_path, worker_id=WORKER_ID)

    with structlog.testing.capture_logs() as captured:
        result = wd.check_previous()

    assert result is True
    suspect_events = [ev for ev in captured if ev.get('event') == 'previous_run_ended_unexpectedly']
    assert suspect_events == []
    assert suspected_oom_kills._value.get() == before


# --- check_previous: suspect cases ---


def test_previous_watchdog_without_clean_exit(tmp_path: Path) -> None:
    """A leftover watchdog with empty body is the SIGKILL signature:
    ``check_previous`` must return ``False``, log the structured warning,
    and bump :data:`drakkar.metrics.suspected_oom_kills`.
    """
    before = suspected_oom_kills._value.get()
    # Empty-body file is exactly what ``WatchdogFile.write`` leaves on
    # disk during normal startup; an OOM kill mid-run preserves it.
    path = tmp_path / f'{WORKER_ID}.watchdog'
    path.write_text('', encoding='utf-8')

    wd = WatchdogFile(data_dir=tmp_path, worker_id=WORKER_ID)

    with structlog.testing.capture_logs() as captured:
        result = wd.check_previous()

    assert result is False
    suspect_events = [ev for ev in captured if ev.get('event') == 'previous_run_ended_unexpectedly']
    assert len(suspect_events) == 1
    event = suspect_events[0]
    assert event['log_level'] == 'warning'
    assert event['category'] == 'watchdog'
    assert event['worker_id'] == WORKER_ID
    assert event['watchdog_path'] == str(path)
    assert suspected_oom_kills._value.get() == before + 1


def test_previous_watchdog_with_garbage_body(tmp_path: Path) -> None:
    """Any body that is not the exact ``CLEAN_EXIT`` marker triggers the
    suspect path. Guards against a partial-write or stray content from
    a buggy fork being misread as clean.
    """
    before = suspected_oom_kills._value.get()
    path = tmp_path / f'{WORKER_ID}.watchdog'
    path.write_text('CLEAN', encoding='utf-8')  # missing ``_EXIT`` suffix

    wd = WatchdogFile(data_dir=tmp_path, worker_id=WORKER_ID)

    with structlog.testing.capture_logs() as captured:
        result = wd.check_previous()

    assert result is False
    suspect_events = [ev for ev in captured if ev.get('event') == 'previous_run_ended_unexpectedly']
    assert len(suspect_events) == 1
    assert suspected_oom_kills._value.get() == before + 1


# --- write / mark_clean lifecycle ---


def test_write_creates_empty_watchdog_file(watchdog: WatchdogFile) -> None:
    """``write`` creates the watchdog file with empty body — the
    ``check_previous`` of a future run reads this as "not yet marked clean".
    """
    assert not watchdog.path.exists()

    watchdog.write()

    assert watchdog.path.exists()
    assert watchdog.path.read_text(encoding='utf-8') == ''


def test_clean_shutdown_writes_marker_and_deletes(watchdog: WatchdogFile) -> None:
    """``write`` then ``mark_clean`` round-trip leaves the directory clean.

    ``mark_clean`` writes ``CLEAN_EXIT`` *then* unlinks. We assert the
    final state (file gone) — the in-between marker state is the
    crash-recovery contract that ``test_previous_watchdog_with_clean_exit``
    covers.
    """
    watchdog.write()
    assert watchdog.path.exists()

    watchdog.mark_clean()

    assert not watchdog.path.exists()


def test_mark_clean_writes_marker_before_unlink(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify the on-disk marker is written before the unlink.

    Patches ``Path.unlink`` (class-level — ``Path`` instances do not
    accept attribute assignment) to capture the file body at the moment
    of deletion. That body is what the *next* startup would observe if
    the process died between the write and the unlink — exactly the
    crash-recovery contract that
    ``test_previous_watchdog_with_clean_exit`` exercises from the
    other side.
    """
    wd = WatchdogFile(data_dir=tmp_path, worker_id=WORKER_ID)
    wd.write()

    captured: dict[str, str] = {}
    real_unlink = Path.unlink

    def spy_unlink(self: Path, *args: object, **kwargs: object) -> None:
        if self == wd.path:
            captured['body'] = self.read_text(encoding='utf-8')
        real_unlink(self, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(Path, 'unlink', spy_unlink)

    wd.mark_clean()

    assert captured.get('body') == CLEAN_EXIT_MARKER
    assert not wd.path.exists()


def test_mark_clean_after_external_unlink_is_idempotent(
    watchdog: WatchdogFile, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If something else removes the file between ``write`` and
    ``mark_clean``, the second call must still not raise.

    Mirrors the production scenario where a sidecar log-rotator or a
    container-cleanup hook races with shutdown. We simulate the race
    by patching ``Path.write_text`` to raise ``FileNotFoundError`` —
    the production path uses ``write_text`` for the marker write, and
    ``mark_clean`` is documented to tolerate that error.
    """
    watchdog.write()
    watchdog.path.unlink()

    # Patch ``write_text`` for this watchdog's specific path to raise
    # FileNotFoundError, simulating the parent-dir-vanished race.
    real_write_text = Path.write_text

    def maybe_missing(self: Path, *args: object, **kwargs: object) -> int:
        if self == watchdog.path:
            raise FileNotFoundError(self)
        return real_write_text(self, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(Path, 'write_text', maybe_missing)

    # Should not raise — both the write and unlink steps in mark_clean
    # tolerate FileNotFoundError on the parent dir / file.
    watchdog.mark_clean()


# --- Lazy directory creation ---


def test_constructor_does_not_touch_disk(tmp_path: Path) -> None:
    """Constructing on a missing directory does NOT create it.

    Disk I/O is deferred to :meth:`write` — mirrors the lazy-init
    pattern used by ``EventRecorder`` and ``CacheEngine`` so callers
    can build a :class:`WatchdogFile` even when the data dir is not
    yet available.
    """
    nested = tmp_path / 'a' / 'b' / 'c'
    assert not nested.exists()

    wd = WatchdogFile(data_dir=nested, worker_id=WORKER_ID)

    # Constructor must not have created the directory.
    assert not nested.exists()
    assert wd.path.parent == nested


def test_write_creates_data_dir_lazily(tmp_path: Path) -> None:
    """``write`` creates ``data_dir`` (with parents) on first call.

    Previously the constructor did this work; moved here so a startup
    that builds a watchdog but never reaches ``write`` (e.g., aborts
    in ``on_startup``) leaves no leftover directory on disk.
    """
    nested = tmp_path / 'a' / 'b' / 'c'
    wd = WatchdogFile(data_dir=nested, worker_id=WORKER_ID)
    assert not nested.exists()

    wd.write()

    assert nested.is_dir()
    assert wd.path.exists()


def test_write_idempotent_on_existing_dir(tmp_path: Path) -> None:
    """A pre-existing data dir is fine — ``mkdir(exist_ok=True)`` no-ops."""
    wd = WatchdogFile(data_dir=tmp_path, worker_id=WORKER_ID)
    # tmp_path already exists; ``write`` should succeed without raising.
    wd.write()
    assert wd.path.exists()
    assert wd.path.parent == tmp_path


def test_path_includes_worker_id(tmp_path: Path) -> None:
    """File path is ``{data_dir}/{worker_id}.watchdog`` — guard against a
    refactor that renames the suffix or swaps in a hash, both of which
    would silently break operator runbooks.
    """
    wd = WatchdogFile(data_dir=tmp_path, worker_id='w42')
    assert wd.path == tmp_path / 'w42.watchdog'
