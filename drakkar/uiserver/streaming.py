"""Streaming of downloads: chunked file streams, gzip streams, the stream with an idle limit, and the open-download bound.

The download routes send a file that is already open (often unlinked) in
chunks, so a large file never sits in memory. The sync generators run in
Starlette's thread pool; the slice stream is async and reads each chunk in
a thread that a cancel waits for.
"""

from __future__ import annotations

import asyncio
import weakref
import zlib
from collections.abc import AsyncIterator, Callable, Iterator
from typing import Any, BinaryIO

from fastapi.responses import StreamingResponse
from starlette.types import Message, Receive, Scope, Send

from drakkar.concurrency import run_in_daemon_thread_to_end

# A download is streamed in chunks of this size.
CHUNK_BYTES = 1024 * 1024
# gzip framing for zlib: a gzip header and trailer around the deflate stream.
GZIP_WBITS = 31
# The fastest level: most of the size gain on SQLite pages for a small part
# of the CPU of the default level, so a download takes little from the tasks.
GZIP_LEVEL = 1


def gzip_chunks(handle: BinaryIO) -> Iterator[bytes]:
    """Stream ``handle`` as one gzip member, chunk by chunk, and close it after the last chunk.

    Synchronous, so Starlette runs it in its thread pool: the compression
    never runs on the API server's loop. Only one chunk is held at a time.
    """
    compressor = zlib.compressobj(level=GZIP_LEVEL, wbits=GZIP_WBITS)
    # The file closes when the stream ends or this generator is closed. The
    # server does not close it when a client leaves; the file then closes
    # when the generator is collected.
    with handle:
        while chunk := handle.read(CHUNK_BYTES):
            if compressed := compressor.compress(chunk):
                yield compressed
        yield compressor.flush()


def _read_part(handle: BinaryIO, compressor: Any) -> bytes | None:
    """The next chunk of ``handle``, compressed with ``compressor`` when given; ``None`` after the end. Blocking."""
    chunk = handle.read(CHUNK_BYTES)
    if compressor is None:
        return chunk or None
    if chunk:
        return compressor.compress(chunk)
    return None


async def stream_file(handle: BinaryIO, *, gzip: bool) -> AsyncIterator[bytes]:
    """Stream ``handle`` chunk by chunk (as one gzip member with ``gzip``); the file closes when the stream ends.

    Each chunk is read (and compressed) on a daemon thread, so a read that
    hangs on a network mount cannot hold the process at exit; a cancel waits
    for that thread, so the file is never closed under a read. The file closes as soon
    as the stream ends, is closed, or is cancelled, not when it is collected.
    """
    compressor = zlib.compressobj(level=GZIP_LEVEL, wbits=GZIP_WBITS) if gzip else None
    try:
        while True:
            part = await run_in_daemon_thread_to_end(_read_part, handle, compressor)
            if part is None:
                break
            if part:
                yield part
        if compressor is not None:
            yield compressor.flush()
    finally:
        handle.close()


# The limit an :class:`OpenDownloads` holder is full at.
OPEN_LIMIT_BYTES = 'bytes'
OPEN_LIMIT_COUNT = 'count'


class OpenDownloads:
    """Downloads being made or still being sent: their bytes and their number, each bounded.

    Each one pins an unlinked temp file or a buffer in RAM until its stream
    ends. The bytes bound keeps them from filling the disk; the count bound
    keeps many small ones from using up the open files of the process.

    A route checks :meth:`full_limit` and, when it is ``None``, calls
    :meth:`reserve` with no ``await`` in between, so two requests can never
    pass the check on the same room. The reservation counts from then on,
    its bytes once :meth:`OpenDownload.set_size` knows them, and must be
    released on every path. Changed only on the API server's loop.
    """

    def __init__(self, *, max_bytes: int, max_count: int) -> None:
        self.max_bytes = max_bytes
        self.max_count = max_count
        self.bytes = 0
        self.count = 0

    def full_limit(self) -> str | None:
        """The limit a new download must wait for (:data:`OPEN_LIMIT_BYTES` first), or ``None`` when there is room."""
        if self.bytes >= self.max_bytes:
            return OPEN_LIMIT_BYTES
        if self.count >= self.max_count:
            return OPEN_LIMIT_COUNT
        return None

    def reserve(self) -> OpenDownload:
        """Count one download, of no bytes yet, until the returned reservation is released."""
        self.count += 1
        return OpenDownload(self)


class OpenDownload:
    """One download counted in an :class:`OpenDownloads` holder; released once, later releases do nothing."""

    def __init__(self, holder: OpenDownloads) -> None:
        self._holder = holder
        self._size = 0
        self._released = False

    def set_size(self, size: int) -> None:
        """Count ``size`` bytes for this download (the size of its file, once it is made)."""
        if self._released:
            return
        self._holder.bytes += size - self._size
        self._size = size

    def release(self) -> None:
        """Stop counting this download."""
        if self._released:
            return
        self._released = True
        self._holder.bytes -= self._size
        self._holder.count -= 1


class IdleLimitedStreamingResponse(StreamingResponse):
    """A stream that ends when the client takes no chunk for ``idle_seconds``, and calls ``on_close`` once.

    A client that keeps the connection open without reading would otherwise
    hold the file (and the bytes it pins) for ever. ``on_close`` runs when
    the stream ends, when the response call ends on any path (a client that
    left before the body started included), and, for a response that is
    never called at all, when the response object is collected. The stream
    is closed in every case where it started, so its file closes at once.
    """

    def __init__(
        self, content: AsyncIterator[bytes], *, idle_seconds: float, on_close: Callable[[], None], **kwargs: Any
    ) -> None:
        super().__init__(content, **kwargs)
        self._idle_seconds = idle_seconds
        # A finalizer runs its function at most once: the call paths below
        # and the collection of a response that was never sent share it.
        self._close = weakref.finalize(self, on_close)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        try:
            await super().__call__(scope, receive, send)
        finally:
            self._close()

    async def stream_response(self, send: Send) -> None:
        idle_seconds = self._idle_seconds

        async def send_within_limit(message: Message) -> None:
            async with asyncio.timeout(idle_seconds):
                await send(message)

        try:
            await super().stream_response(send_within_limit)
        finally:
            body = self.body_iterator
            aclose = getattr(body, 'aclose', None)
            try:
                if aclose is not None:
                    await aclose()
            finally:
                self._close()
