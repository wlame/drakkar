"""The built-in registry providers: ``filesystem``, ``static`` and ``none``.

``BUILTIN_PROVIDERS`` maps each name to its factory ``(options) -> provider``.
Plugins add names to that table at lookup time (see ``drakkar.fleet.plugins``).
"""

import asyncio
import concurrent.futures
import errno
import os
import queue
import stat
import tempfile
import threading
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any

import structlog

from drakkar.dbfiles import DB_DIR_MODE, DB_FILE_MODE
from drakkar.fleet.card import CARD_SCHEMA, MAX_CARD_BYTES, decode_card, sanitize_worker_name, serialize_card
from drakkar.fleet.provider import PeerCard, PeerCardList, RegistryContext, RegistryFactory

# Cards live in this subfolder of the registry folder, one file per worker.
WORKERS_FOLDER = 'workers'
CARD_SUFFIX = '.json'
TEMP_SUFFIX = '.tmp'

# No symlinks anywhere in the shared folder: a planted link must not make a
# worker read or overwrite a file elsewhere. O_NONBLOCK keeps an open of a
# planted FIFO from hanging; regular files ignore it.
_READ_FLAGS = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
_WRITE_FLAGS = os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW

# Files that could not be decoded, as (file name, reason). A module-level
# alias: inside a provider class body the name ``list`` is the method.
FileRejects = list[tuple[str, str]]


class FleetRegistryFolderError(RuntimeError):
    """An explicitly configured card folder cannot be created or written."""


def _workers_folder_exists(workers_dir: str) -> bool:
    """True for a real folder, False when it is missing; ``OSError`` for a symlink or a file.

    The registry never follows a link: listing or pruning through one would
    read, and delete, cards in a folder somewhere else.
    """
    try:
        info = os.lstat(workers_dir)
    except FileNotFoundError:
        return False
    if not stat.S_ISDIR(info.st_mode):
        raise NotADirectoryError(
            f'{workers_dir} is not a folder (a symlink or a file); the registry does not follow it'
        )
    return True


def prepare_registry_folder(directory: str) -> None:
    """Create ``<directory>/workers`` (mode 0700) and check this process can write it.

    The check writes and deletes a probe file, because a permission check
    without a write can disagree with the file server (root squashing,
    ACLs). Raises ``OSError`` when the folder cannot be used. Used at startup
    for a folder the operator set explicitly; a derived folder is left to
    the publish loop, whose failures are counted and retried.
    """
    workers_dir = os.path.join(directory, WORKERS_FOLDER)
    if not _workers_folder_exists(workers_dir):
        os.makedirs(workers_dir, mode=DB_DIR_MODE, exist_ok=True)
    fd, probe = tempfile.mkstemp(prefix='.write-probe-', suffix=TEMP_SUFFIX, dir=workers_dir)
    os.close(fd)
    os.unlink(probe)


class NoneRegistry:
    """No peers: ``list`` is empty and the writes do nothing."""

    async def start(self, ctx: RegistryContext) -> None:
        return None

    async def publish(self, card: dict[str, Any]) -> None:
        return None

    async def list(self) -> PeerCardList:
        return []

    async def withdraw(self, worker: str, instance: str) -> None:
        return None

    async def stop(self) -> None:
        return None


class StaticRegistry:
    """A fixed peer list from config. The ages are unknown, so liveness is unknown."""

    def __init__(self, peers: Iterable[Mapping[str, Any]]) -> None:
        self._cards = tuple(
            {'schema': CARD_SCHEMA, 'worker': peer['name'], 'cluster': peer.get('cluster', ''), 'url': peer['url']}
            for peer in peers
        )

    async def start(self, ctx: RegistryContext) -> None:
        return None

    async def publish(self, card: dict[str, Any]) -> None:
        return None

    async def list(self) -> PeerCardList:
        # Copies, so a caller that edits a card cannot change the next listing.
        return [PeerCard(card=dict(card), age_seconds=None) for card in self._cards]

    async def withdraw(self, worker: str, instance: str) -> None:
        return None

    async def stop(self) -> None:
        return None


@dataclass(frozen=True)
class _ThreadJob:
    """One call for the provider thread, its result, and how to free the slot afterwards."""

    call: Callable[[], Any]
    future: concurrent.futures.Future
    free_slot: Callable[[], None]


def _call_soon(loop: asyncio.AbstractEventLoop, callback: Callable[[], Any]) -> None:
    """Schedule ``callback`` on ``loop`` from another thread; a closed loop (process exit) is ignored."""
    try:
        loop.call_soon_threadsafe(callback)
    except RuntimeError:
        pass


class _SkippedFile(Exception):
    """A card file that is not read. ``reason`` is ``''`` when it vanished since the listing."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class _ProviderThread:
    """One daemon thread that runs a provider's blocking calls, in order.

    Not the shared ``asyncio.to_thread`` pool: a call stuck on a dead network
    mount would hold a pool thread for as long as it hangs, and enough of them
    would starve every other ``to_thread`` user in the worker. Here a stuck
    call holds only this thread, so the next registry call times out instead.
    The thread is a daemon because the pool threads are joined at interpreter
    exit, and a hung call would then keep the process from exiting.
    """

    def __init__(self, name: str) -> None:
        self._jobs: queue.SimpleQueue[_ThreadJob | None] = queue.SimpleQueue()
        # One call at a time: a caller takes the slot before it queues its
        # call, and the thread frees the slot when the call ends. While a
        # call hangs, later callers wait for the slot and give up with their
        # timeout without queuing anything, so nothing piles up in memory.
        self._slot = asyncio.Semaphore(1)
        self.thread = threading.Thread(target=self._serve, name=name, daemon=True)
        self.thread.start()

    def _serve(self) -> None:
        while (job := self._jobs.get()) is not None:
            try:
                # False when the caller gave up (timeout) before the call
                # started: skip it rather than run a stale publish or list.
                if job.future.set_running_or_notify_cancel():
                    try:
                        job.future.set_result(job.call())
                    except Exception as exc:
                        job.future.set_exception(exc)
            finally:
                job.free_slot()

    async def run(self, call: Callable[[], Any]) -> Any:
        """Run ``call`` on the thread and wait for its result.

        Waiting for the slot can be cancelled with nothing queued; a call
        whose caller gave up after queuing it is skipped by the thread.
        """
        loop = asyncio.get_running_loop()
        await self._slot.acquire()
        future: concurrent.futures.Future = concurrent.futures.Future()
        self._jobs.put(_ThreadJob(call=call, future=future, free_slot=lambda: _call_soon(loop, self._slot.release)))
        return await asyncio.wrap_future(future)

    def stop(self) -> None:
        """Let the thread exit after the calls already queued."""
        self._jobs.put(None)


def _read_card_file(path: str) -> tuple[bytes, os.stat_result]:
    """Read at most one byte more than a card may hold, with the file's stat.

    Raises ``_SkippedFile``: reason ``''`` for a file gone since the
    listing, ``not_a_file`` for a symlink (the open refuses to follow it), a
    folder or a FIFO, ``unreadable`` for any other open or read error
    (permissions, I/O).
    """
    try:
        fd = os.open(path, _READ_FLAGS)
    except FileNotFoundError as exc:
        raise _SkippedFile('') from exc
    except OSError as exc:
        raise _SkippedFile('not_a_file' if exc.errno == errno.ELOOP else 'unreadable') from exc
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            raise _SkippedFile('not_a_file')
        chunks: list[bytes] = []
        remaining = MAX_CARD_BYTES + 1
        while remaining > 0 and (chunk := os.read(fd, remaining)):
            chunks.append(chunk)
            remaining -= len(chunk)
        return b''.join(chunks), info
    except OSError as exc:
        raise _SkippedFile('unreadable') from exc
    finally:
        os.close(fd)


def _unlink_quietly(path: str) -> None:
    """Delete ``path``; another worker may have deleted it first, which is fine."""
    try:
        os.unlink(path)
    except OSError:
        pass


class FilesystemRegistry:
    """Cards as files in ``<directory>/workers/``, ages from the file server's clock.

    Each card is written to a temp file and renamed over ``<worker>.json``, so
    a reader never sees half a card. Ages compare file mtimes with a sample of
    the server's clock: the mtime of this worker's own card right after it
    was written, moved forward by the local monotonic time since. No host's
    wall clock is ever compared with another's. Before the first successful
    publish there is no sample, and every age is unknown.
    """

    def __init__(
        self,
        directory: str,
        prune_after_seconds: float,
        now_monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if not directory:
            raise ValueError('the filesystem registry needs a folder')
        self._workers_dir = os.path.join(directory, WORKERS_FOLDER)
        self._prune_after = prune_after_seconds
        self._now_monotonic = now_monotonic
        # (server mtime of our last card, local monotonic time of that sample).
        # Read and written only on the provider thread.
        self._sample: tuple[float, float] | None = None
        self._reported_files: set[str] = set()
        self._logger: Any = structlog.get_logger()
        self._runner: _ProviderThread | None = None

    async def start(self, ctx: RegistryContext) -> None:
        self._logger = ctx.logger
        self._runner = _ProviderThread(name=f'drakkar-fleet-{sanitize_worker_name(ctx.worker)}')

    async def publish(self, card: dict[str, Any]) -> None:
        await self._run(lambda: self._publish_sync(card))

    async def list(self) -> PeerCardList:
        cards, rejects = await self._run(self._list_sync)
        for file_name, reason in rejects:
            if file_name not in self._reported_files:
                self._reported_files.add(file_name)
                await self._logger.awarning(
                    'fleet_card_invalid',
                    category='fleet',
                    file=file_name,
                    reason=reason,
                )
        return cards

    async def withdraw(self, worker: str, instance: str) -> None:
        await self._run(lambda: self._withdraw_sync(worker, instance))

    async def stop(self) -> None:
        if self._runner is not None:
            self._runner.stop()

    async def _run(self, call: Callable[[], Any]) -> Any:
        if self._runner is None:
            raise RuntimeError('FilesystemRegistry used before start()')
        return await self._runner.run(call)

    def _card_path(self, worker: str) -> str:
        return os.path.join(self._workers_dir, sanitize_worker_name(worker) + CARD_SUFFIX)

    def _publish_sync(self, card: dict[str, Any]) -> None:
        if not _workers_folder_exists(self._workers_dir):
            os.makedirs(self._workers_dir, mode=DB_DIR_MODE, exist_ok=True)
        final_path = self._card_path(card['worker'])
        stem = sanitize_worker_name(card['worker'])
        instance = sanitize_worker_name(str(card.get('instance', '')))
        temp_path = os.path.join(self._workers_dir, f'.{stem}{CARD_SUFFIX}.{instance}{TEMP_SUFFIX}')
        data = serialize_card(card)
        fd = os.open(temp_path, _WRITE_FLAGS, DB_FILE_MODE)
        try:
            view = memoryview(data)
            while view:
                view = view[os.write(fd, view) :]
        finally:
            os.close(fd)
        try:
            os.replace(temp_path, final_path)
        except OSError:
            _unlink_quietly(temp_path)
            raise
        self._sample = (os.stat(final_path, follow_symlinks=False).st_mtime, self._now_monotonic())

    def _server_now(self) -> float | None:
        """The file server's current time, estimated from the last sample; ``None`` without one."""
        if self._sample is None:
            return None
        sampled_mtime, sampled_at = self._sample
        return sampled_mtime + (self._now_monotonic() - sampled_at)

    def _list_sync(self) -> tuple[PeerCardList, FileRejects]:
        """Read every card file: the cards, and the files that could not be decoded with the reason."""
        if not _workers_folder_exists(self._workers_dir):
            return [], []
        try:
            names = sorted(os.listdir(self._workers_dir))
        except FileNotFoundError:
            return [], []
        server_now = self._server_now()
        cards: PeerCardList = []
        rejects: FileRejects = []
        for name in names:
            path = os.path.join(self._workers_dir, name)
            if name.startswith('.'):
                if name.endswith(TEMP_SUFFIX):
                    self._prune_stale_temp(path, server_now)
                continue
            if not name.endswith(CARD_SUFFIX):
                continue
            try:
                raw, info = _read_card_file(path)
            except _SkippedFile as skipped:
                if skipped.reason:
                    rejects.append((name, skipped.reason))
                continue
            # max(0, ...): a card written after our sample, or a coarse
            # server clock, must not read as an age below zero.
            age = None if server_now is None else max(0.0, server_now - info.st_mtime)
            if age is not None and age > self._prune_after:
                _unlink_quietly(path)
                continue
            card, reason = decode_card(raw)
            if card is None:
                rejects.append((name, reason))
                continue
            cards.append(PeerCard(card=card, age_seconds=age))
        return cards, rejects

    def _prune_stale_temp(self, path: str, server_now: float | None) -> None:
        """Delete a temp file a crashed writer left behind, once it is as old as a prunable card."""
        if server_now is None:
            return
        try:
            mtime = os.stat(path, follow_symlinks=False).st_mtime
        except OSError:
            return
        if server_now - mtime > self._prune_after:
            _unlink_quietly(path)

    def _withdraw_sync(self, worker: str, instance: str) -> None:
        if not _workers_folder_exists(self._workers_dir):
            return
        path = self._card_path(worker)
        try:
            raw, _info = _read_card_file(path)
        except _SkippedFile:
            return
        card, _reason = decode_card(raw)
        # Only our own card: another process with the same worker name may
        # have replaced it, and its card must stay.
        if card is not None and card.get('instance') == instance:
            _unlink_quietly(path)


def _filesystem_factory(options: Mapping[str, Any]) -> FilesystemRegistry:
    return FilesystemRegistry(directory=options.get('dir', ''), prune_after_seconds=options['prune_after_seconds'])


def _static_factory(options: Mapping[str, Any]) -> StaticRegistry:
    return StaticRegistry(options.get('peers', []))


def _none_factory(options: Mapping[str, Any]) -> NoneRegistry:
    return NoneRegistry()


BUILTIN_PROVIDERS: dict[str, RegistryFactory] = {
    'filesystem': _filesystem_factory,
    'static': _static_factory,
    'none': _none_factory,
}
