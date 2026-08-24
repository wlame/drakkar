"""The atomic live-link helper both SQLite stores use.

The flight recorder publishes ``<worker>-live.db`` and the cache engine
publishes ``<worker>-cache.db``; peer discovery, cross-worker traces and
the UI all resolve workers through those links. Both must be replaced
atomically — a peer scan must never see a half-created link — and both must
survive a crash between the two syscalls that do it.
"""

from __future__ import annotations

import os

import pytest
from structlog.testing import capture_logs

from drakkar.dbfiles import atomic_symlink


def test_creates_the_link(tmp_path):
    link = str(tmp_path / 'worker-live.db')

    assert atomic_symlink(link, 'worker-20260101.db') is True

    assert os.path.islink(link)
    assert os.readlink(link) == 'worker-20260101.db'


def test_repoints_an_existing_link(tmp_path):
    link = str(tmp_path / 'worker-live.db')
    atomic_symlink(link, 'worker-20260101.db')

    atomic_symlink(link, 'worker-20260102.db')

    assert os.readlink(link) == 'worker-20260102.db'


def test_a_stale_tmp_from_a_crashed_run_does_not_wedge_the_link(tmp_path):
    """A crash between symlink() and replace() leaves the .tmp behind.

    Without removing it first, every later os.symlink raises FileExistsError
    and the link never refreshes again — so peers and the UI keep resolving
    this worker to a database it rotated away from hours ago.
    """
    link = str(tmp_path / 'worker-live.db')
    os.symlink('worker-from-the-crashed-run.db', link + '.tmp')

    assert atomic_symlink(link, 'worker-20260102.db') is True

    assert os.readlink(link) == 'worker-20260102.db'
    assert not os.path.lexists(link + '.tmp'), 'the tmp must not survive a successful publish'


def test_a_stale_tmp_that_is_a_regular_file_is_also_cleared(tmp_path):
    link = str(tmp_path / 'worker-live.db')
    (tmp_path / 'worker-live.db.tmp').write_text('not a symlink')

    assert atomic_symlink(link, 'worker-20260102.db') is True

    assert os.readlink(link) == 'worker-20260102.db'


def test_a_failure_is_reported_once_not_swallowed(tmp_path, monkeypatch):
    """A filesystem without symlinks is not fatal — peers just cannot find
    this worker — but it must not be invisible either."""
    link = str(tmp_path / 'worker-live.db')

    def refuse(*args, **kwargs):
        raise OSError('symlinks not supported')

    monkeypatch.setattr(os, 'symlink', refuse)

    with capture_logs() as first:
        assert atomic_symlink(link, 'target.db') is False
    with capture_logs() as second:
        assert atomic_symlink(link, 'target.db') is False

    (warning,) = [entry for entry in first if entry['event'] == 'db_live_link_failed']
    assert warning['link'] == link
    assert warning['log_level'] == 'warning'
    assert [entry for entry in second if entry['event'] == 'db_live_link_failed'] == [], (
        'a link on a filesystem that cannot do symlinks must not log on every rotation'
    )


@pytest.fixture(autouse=True)
def _reset_warned_links():
    """The once-per-link warning is process-global state."""
    from drakkar import dbfiles

    dbfiles._WARNED_LINKS.clear()
    yield
    dbfiles._WARNED_LINKS.clear()
