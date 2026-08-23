"""Filesystem permissions for the SQLite files Drakkar writes.

A leaf module (no Drakkar imports) shared by the flight recorder and the
cache engine, so both stores tighten their files identically and the Go
backend has a single behaviour to mirror.

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
# including the one rotation opens. The Go backend applies the same level as
# a DSN pragma (its pooled connections would otherwise each start at FULL).
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
