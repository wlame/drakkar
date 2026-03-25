"""MongoDB sink — inserts documents into collections.

Wraps motor's AsyncIOMotorClient. Each MongoPayload's data field is
serialized via model_dump() to get a dict suitable for MongoDB insertion.
"""

import time

import structlog

from drakkar.config import MongoSinkConfig
from drakkar.metrics import sink_deliver_duration, sink_deliver_errors, sink_payloads_delivered
from drakkar.models import MongoPayload
from drakkar.sinks.base import BaseSink

logger = structlog.get_logger()


class MongoSink(BaseSink[MongoPayload]):
    """Inserts documents into MongoDB collections.

    Each MongoPayload is serialized:
        - collection = payload.collection
        - document = payload.data.model_dump()

    Uses motor's AsyncIOMotorClient for native asyncio support.
    """

    sink_type = 'mongo'

    def __init__(self, name: str, config: MongoSinkConfig) -> None:
        super().__init__(name, ui_url=config.ui_url)
        self._config = config
        self._client = None
        self._db = None

    async def connect(self) -> None:
        """Create the motor client and get database reference."""
        from motor.motor_asyncio import AsyncIOMotorClient

        self._client = AsyncIOMotorClient(self._config.uri)
        self._db = self._client[self._config.database]
        await logger.ainfo(
            'mongo_sink_connected',
            category='sink',
            sink_name=self._name,
            uri=self._config.uri,
            database=self._config.database,
        )

    async def deliver(self, payloads: list[MongoPayload]) -> None:
        """Insert all payloads as documents into their respective collections.

        Each payload's data is serialized via model_dump() and inserted
        into the collection specified by payload.collection.
        """
        if not payloads or self._db is None:
            return

        start = time.monotonic()
        labels = {'sink_type': self.sink_type, 'sink_name': self._name}
        try:
            for payload in payloads:
                collection = self._db[payload.collection]
                document = payload.data.model_dump()
                await collection.insert_one(document)

            sink_payloads_delivered.labels(**labels).inc(len(payloads))
            sink_deliver_duration.labels(**labels).observe(time.monotonic() - start)
        except Exception:
            sink_deliver_errors.labels(**labels).inc()
            raise

    async def close(self) -> None:
        """Close the motor client."""
        if self._client is not None:
            try:
                self._client.close()
            except Exception as e:
                await logger.awarning(
                    'mongo_sink_close_error',
                    category='sink',
                    sink_name=self._name,
                    error=str(e),
                )
            self._client = None
            self._db = None
