"""Streaming of downloads: chunked file streams, gzip streams, and the slice stream with its idle limit.

The download routes send a file that is already open (often unlinked) in
chunks, so a large file never sits in memory. The sync generators run in
Starlette's thread pool; the slice stream is async and reads each chunk in
a thread that a cancel waits for.
"""

from __future__ import annotations

import asyncio
import zlib
from collections.abc import AsyncIterator, Callable, Iterator
from typing import Any, BinaryIO

from fastapi.responses import StreamingResponse
from starlette.types import Message, Send

from drakkar.concurrency import run_in_thread_to_end

# A download is streamed in chunks of this size.
CHUNK_BYTES = 1024 * 1024
# gzip framing for zlib: a gzip header and trailer around the deflate stream.
GZIP_WBITS = 31
# The fastest level: most of the size gain on SQLite pages for a small part
# of the CPU of the default level, so a download takes little from the tasks.
GZIP_LEVEL = 1


def read_chunks(handle: BinaryIO) -> Iterator[bytes]:
    """Stream ``handle`` to the client and close it after the last chunk."""
    with handle:
        while chunk := handle.read(CHUNK_BYTES):
            yield chunk


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

    Each chunk is read (and compressed) in a thread; a cancel waits for that
    thread, so the file is never closed under a read. The file closes as soon
    as the stream ends, is closed, or is cancelled, not when it is collected.
    """
    compressor = zlib.compressobj(level=GZIP_LEVEL, wbits=GZIP_WBITS) if gzip else None
    try:
        while True:
            part = await run_in_thread_to_end(_read_part, handle, compressor)
            if part is None:
                break
            if part:
                yield part
        if compressor is not None:
            yield compressor.flush()
    finally:
        handle.close()


class IdleLimitedStreamingResponse(StreamingResponse):
    """A stream that ends when the client takes no chunk for ``idle_seconds``, and calls ``on_close`` once at its end.

    A client that keeps the connection open without reading would otherwise
    hold the file (and the bytes it pins) for ever. The stream is closed in
    every case, so its file closes at once.
    """

    def __init__(
        self, content: AsyncIterator[bytes], *, idle_seconds: float, on_close: Callable[[], None], **kwargs: Any
    ) -> None:
        super().__init__(content, **kwargs)
        self._idle_seconds = idle_seconds
        self._on_close = on_close

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
                self._on_close()
