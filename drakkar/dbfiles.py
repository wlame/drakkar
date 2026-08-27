"""Filesystem handling for the SQLite files Drakkar writes.

Two jobs, both shared by the flight recorder and the cache engine so the
two stores behave identically: tightening file permissions, and publishing
the "this is my current database" symlink that peer discovery and the UI
resolve workers through.

A leaf module — it imports nothing from Drakkar.

Both stores hold message-derived data — the recorder persists task args
and subprocess stdout/stderr, the cache persists handler results — so the
files must not be readable by other local users. That matters most in the
default ``db_dir`` of ``/tmp``, which is world-writable and shared by every
process on the host. Same-user peer workers (cache sync, debug merge) are
unaffected: 0600 still grants the owner full access.
"""

from __future__ import annotations

import contextlib
import os

import structlog

logger = structlog.get_logger()

# Owner-only for the DB files themselves. SQLite creates them 0644 & ~umask.
DB_FILE_MODE = 0o600

# Owner-only for a ``db_dir`` Drakkar creates. The files inside are already
# 0600, so group/other bits on the directory grant nothing except the
# ability to enumerate and to create sibling files.
DB_DIR_MODE = 0o700

# SQLite's write-ahead-log sidecars. They hold pages not yet checkpointed
# into the main file — the same data, at the same sensitivity.
_WAL_SUFFIXES = ('-wal', '-shm')


# Durability level for every WAL writer this framework opens.
#
# SQLite defaults to ``synchronous=FULL``, which fsyncs the WAL on every
# single commit. These stores commit often — the recorder on its flush
# interval, on each state sync and on any UI poll that forces a flush; the
# handler cache on its own flush interval — and a ``db_dir`` is routinely a
# network mount, where one fsync is tens of milliseconds during which reads
# on the same connection queue behind it.
#
# NORMAL is the standard choice for WAL and the one SQLite's own
# documentation recommends there: the WAL is synced at checkpoints rather
# than per commit. It remains fully safe across an application crash — the
# process dying, an OOM kill, a failed deploy — and gives up only the last
# transactions in the event of a host power loss or kernel panic. For a
# flight recorder, a derived stats cache and a last-writer-wins cache, that
# is the right trade; none of them is a system of record.
#
# ``synchronous`` is per-connection and, unlike ``journal_mode``, is NOT
# stored in the database header — so every writer connection must set it,
# including the one rotation opens — a pooled connection would otherwise
# start at the SQLite default of FULL.
WAL_SYNCHRONOUS_PRAGMA = 'PRAGMA synchronous=NORMAL'


def secure_db_file(db_path: str) -> None:
    """Create ``db_path`` owner-only, or tighten it and its WAL sidecars.

    MUST be called **before** the SQLite driver opens ``db_path``, and in
    particular before ``PRAGMA journal_mode=WAL``. SQLite copies the main
    database's permission bits onto ``-wal`` and ``-shm`` at the moment it
    creates them, so tightening the main file first is what makes the
    sidecars owner-only too — and keeps them that way, because the sidecars
    are deleted on a clean close and re-created (again inheriting the main
    file's mode) on the next write. Doing this *after* the pragma leaves
    both sidecars at 0644 while the main file is 0600; that ordering was
    the original bug.

    Creating the file ourselves, rather than letting the driver create it
    0644 and chmod-ing afterwards, closes the window in which the DB is
    briefly world-readable. ``O_EXCL`` additionally means we never follow a
    symlink planted at the path: in a world-writable ``db_dir`` the
    filename is predictable, and without it a pre-planted link would have
    us chmod an arbitrary file the worker owns. When the path already
    exists (a restart re-opening the cache DB, or sidecars left behind by
    an unclean shutdown) ``O_EXCL`` fails and we fall through to chmod,
    which is the right move for a file that is genuinely ours.

    An empty file is a valid empty SQLite database, so pre-creating it
    changes nothing the driver sees.

    Best-effort throughout — a permissions failure must never abort store
    startup. On a read-only or foreign-owned directory the chmod is both
    impossible and not ours to make.
    """
    with contextlib.suppress(OSError):
        os.close(os.open(db_path, os.O_CREAT | os.O_EXCL | os.O_RDWR, DB_FILE_MODE))
    for path in (db_path, *(db_path + suffix for suffix in _WAL_SUFFIXES)):
        _chmod_no_follow(path)


def _chmod_no_follow(path: str) -> None:
    """chmod ``path`` to :data:`DB_FILE_MODE`, refusing to act through a symlink.

    Plain ``os.chmod`` resolves symlinks, so on the already-exists branch
    it would happily re-mode whatever a planted link points at — undoing
    the protection ``O_EXCL`` gives the create branch. Linux has no
    ``lchmod``, so the portable answer is to open with ``O_NOFOLLOW``
    (fails with ELOOP on a symlink) and ``fchmod`` the descriptor. Going
    through a descriptor also removes the check-then-act window: the mode
    lands on the file we opened, not on whatever the name resolves to a
    moment later.

    ``O_RDONLY`` is enough — ``fchmod`` is authorised by ownership, not by
    the open mode.
    """
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError:
        return
    try:
        os.fchmod(fd, DB_FILE_MODE)
    except OSError:
        pass
    finally:
        os.close(fd)


# Links already reported as unpublishable, so a filesystem that cannot do
# symlinks costs one warning rather than one per rotation. Keyed by link
# path: two different links (the recorder's and the cache's) each get their
# own line, which is what an operator needs to see.
_WARNED_LINKS: set[str] = set()


def atomic_symlink(link: str, target: str) -> bool:
    """Point ``link`` at ``target``, replacing any existing link atomically.

    Both SQLite stores publish a "this is my current database" symlink —
    the recorder's ``<worker>-live.db`` and the cache's
    ``<worker>-cache.db`` — that peer discovery, cross-worker traces and
    the UI resolve workers through. Writing it in place would leave a
    window where a peer scan sees no link at all, so the link is built
    under a ``.tmp`` name and ``os.replace``d into position, which is
    atomic within a filesystem.

    A leftover ``.tmp`` is removed first. Without that, one crash between
    the ``symlink`` and the ``replace`` wedges the link permanently:
    ``os.symlink`` then raises ``FileExistsError`` on every later call, and
    peers keep resolving this worker to whatever database it had rotated
    away from at the moment it died.

    ``target`` is relative (a bare filename) so the link stays valid if the
    directory is moved or mounted elsewhere.

    Returns True when the link was published. A failure is **not** fatal —
    on a filesystem without symlink support the missing link only means
    peers cannot discover this worker — but it is reported once per link
    rather than swallowed, because "no peers found" and "we never published
    ourselves" look identical from the outside.
    """
    tmp = link + '.tmp'
    try:
        # lexists/remove rather than unlink(missing_ok=True) semantics: the
        # leftover may be a dangling symlink, which os.path.exists() denies.
        with contextlib.suppress(FileNotFoundError):
            os.remove(tmp)
        os.symlink(target, tmp)
        os.replace(tmp, link)
    except OSError as exc:
        if link not in _WARNED_LINKS:
            _WARNED_LINKS.add(link)
            logger.warning(
                'db_live_link_failed',
                category='storage',
                link=link,
                target=target,
                error=str(exc),
                error_type=type(exc).__name__,
                hint='peers and the UI cannot resolve this worker until the link can be written',
            )
        return False
    _WARNED_LINKS.discard(link)
    return True


def remove_symlink(link: str) -> None:
    """Remove ``link`` on graceful shutdown, tolerating its absence.

    Only removes an actual symlink: a regular file sitting at that path was
    not published by this worker and is not this worker's to delete.
    """
    with contextlib.suppress(OSError):
        if os.path.islink(link):
            os.remove(link)
