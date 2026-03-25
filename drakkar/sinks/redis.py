"""Redis sink — sets key-value pairs in Redis.

Wraps redis.asyncio.Redis. Each RedisPayload's data field is serialized
via model_dump_json() and stored as a string value under the configured
key prefix + payload key, with optional TTL.
"""

import time

import structlog

from drakkar.config import RedisSinkConfig
from drakkar.metrics import sink_deliver_duration, sink_deliver_errors, sink_payloads_delivered
from drakkar.models import RedisPayload
from drakkar.sinks.base import BaseSink

logger = structlog.get_logger()


class RedisSink(BaseSink[RedisPayload]):
    """Sets key-value pairs in Redis.

    Each RedisPayload is serialized:
        - key = config.key_prefix + payload.key
        - value = payload.data.model_dump_json()
        - TTL = payload.ttl (optional, in seconds)
    """

    sink_type = 'redis'

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
            url=self._config.url,
            key_prefix=self._config.key_prefix,
        )

    async def deliver(self, payloads: list[RedisPayload]) -> None:
        """Set all payloads as key-value pairs in Redis.

        Keys are prefixed with config.key_prefix. If a payload has a TTL,
        the key is set with an expiration.
        """
        if not payloads or not self._client:
            return

        start = time.monotonic()
        labels = {'sink_type': self.sink_type, 'sink_name': self._name}
        try:
            for payload in payloads:
                full_key = self._config.key_prefix + payload.key
                value = payload.data.model_dump_json()
                if payload.ttl is not None:
                    await self._client.set(full_key, value, ex=payload.ttl)
                else:
                    await self._client.set(full_key, value)

            sink_payloads_delivered.labels(**labels).inc(len(payloads))
            sink_deliver_duration.labels(**labels).observe(time.monotonic() - start)
        except Exception:
            sink_deliver_errors.labels(**labels).inc()
            raise

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
