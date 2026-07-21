"""Redis sink — sets key-value pairs in Redis.

Wraps redis.asyncio.Redis. Each RedisPayload's data field is serialized
via model_dump_json() and stored as a string value under the configured
key prefix + payload key, with optional TTL.
"""

import time
from dataclasses import dataclass

import structlog

from drakkar.config import RedisSinkConfig
from drakkar.metrics import sink_deliver_duration, sink_deliver_errors, sink_payloads_delivered
from drakkar.models import RedisPayload
from drakkar.sinks.base import BaseSink
from drakkar.utils import redact_url

logger = structlog.get_logger()


@dataclass(frozen=True)
class _RedisItem:
    """One payload reduced to its prefixed key, serialized value, and TTL."""

    key: str
    value: str
    ttl: int | None


class RedisSink(BaseSink[RedisPayload]):
    """Sets key-value pairs in Redis.

    Each RedisPayload is serialized:
        - key = config.key_prefix + payload.key
        - value = payload.data.model_dump_json()
        - TTL = payload.ttl (optional, in seconds)
    """

    sink_type = 'redis'

    # Redis ``SET`` is write-replace on a fixed key — re-executing the
    # same command produces the same post-state regardless of how many
    # times it ran. That makes RedisSink safe to retry on transient
    # errors (connection reset mid-command, timeout, etc.), so we opt
    # into automatic retry via ``idempotent=True``. TTLs are also set as
    # part of the SET so the retry doesn't "refresh" a key that was
    # already written in an earlier attempt in some surprising way.
    idempotent = True

    def __init__(self, name: str, config: RedisSinkConfig) -> None:
        super().__init__(name, ui_url=config.ui_url)
        self._config = config
        self._client = None

    async def connect(self) -> None:
        """Create the Redis client from the configured URL."""
        import redis.asyncio as aioredis

        self._client = aioredis.from_url(self._config.url)
        await logger.ainfo(
            'redis_sink_connected',
            category='sink',
            sink_name=self._name,
            url=redact_url(self._config.url),
            key_prefix=self._config.key_prefix,
        )

    async def deliver(self, payloads: list[RedisPayload]) -> None:
        """Set every payload as a key-value pair in Redis.

        Keys are prefixed with ``config.key_prefix``; a payload carrying a
        TTL is set with an expiration.

        Multiple payloads are written through ONE pipeline instead of one
        round-trip per key. Failure granularity is preserved: a pipeline
        that fails is retried key-by-key so the error an operator sees
        names the offending key, exactly as the per-payload loop did. That
        retry is safe because ``SET`` is write-replace — re-running it for
        keys the pipeline already applied is a no-op. See the Go backend's
        ``internal/sinks/redis.go`` — the two must stay observably
        identical (divergence #18 in its migration notes).
        """
        if not payloads or not self._client:
            return

        start = time.monotonic()
        labels = {'sink_type': self.sink_type, 'sink_name': self._name}
        try:
            items, bad_index, build_error = self._build_items(payloads)
            if build_error is not None:
                # The per-payload loop SET every key BEFORE the
                # unserializable payload, then raised. Reproduce those side
                # effects (a set failure on the way takes precedence,
                # exactly as the sequential loop would have hit it first).
                for item in items[:bad_index]:
                    await self._set_single(item)
                raise build_error
            if len(items) == 1:
                await self._set_single(items[0])
            else:
                await self._set_batch(items)

            sink_payloads_delivered.labels(**labels).inc(len(payloads))
            sink_deliver_duration.labels(**labels).observe(time.monotonic() - start)
        except Exception:
            sink_deliver_errors.labels(**labels).inc()
            raise

    def _build_items(self, payloads: list[RedisPayload]) -> tuple[list[_RedisItem], int, Exception | None]:
        """Serialize every payload up front.

        On the first bad payload returns the items built so far, the failing
        index, and the error — the caller replays the legacy partial side
        effects before raising it.
        """
        items: list[_RedisItem] = []
        for i, payload in enumerate(payloads):
            try:
                items.append(
                    _RedisItem(
                        key=self._config.key_prefix + payload.key,
                        value=payload.data.model_dump_json(),
                        ttl=payload.ttl,
                    )
                )
            except Exception as e:
                return items, i, e
        return items, len(items), None

    async def _set_batch(self, items: list[_RedisItem]) -> None:
        """Write every item through one pipeline, falling back per key on failure."""
        assert self._client is not None
        try:
            pipe = self._client.pipeline(transaction=False)
            for item in items:
                if item.ttl is not None:
                    pipe.set(item.key, item.value, ex=item.ttl)
                else:
                    pipe.set(item.key, item.value)
            await pipe.execute()
            return
        except Exception:
            # Pipeline failed — fall back to per-key SETs so the error names
            # the offending key. Safe to replay: SET is write-replace.
            pass
        for item in items:
            await self._set_single(item)

    async def _set_single(self, item: _RedisItem) -> None:
        """Set one key — the shape the pre-batching loop produced."""
        assert self._client is not None
        if item.ttl is not None:
            await self._client.set(item.key, item.value, ex=item.ttl)
        else:
            await self._client.set(item.key, item.value)

    async def close(self) -> None:
        """Close the Redis client."""
        if self._client:
            try:
                await self._client.aclose()
            except Exception as e:
                await logger.awarning(
                    'redis_sink_close_error',
                    category='sink',
                    sink_name=self._name,
                    error=str(e),
                )
            self._client = None
