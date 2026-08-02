"""MongoDB sink — inserts documents into collections.

Wraps PyMongo's AsyncMongoClient. Each MongoPayload's data field is
serialized via model_dump() to get a dict suitable for MongoDB insertion.
"""

import time
from dataclasses import dataclass

import structlog

from drakkar.config import MongoSinkConfig
from drakkar.metrics import sink_deliver_duration, sink_deliver_errors, sink_payloads_delivered
from drakkar.models import MongoOp, MongoPayload
from drakkar.sinks.base import BaseSink
from drakkar.utils import redact_url

logger = structlog.get_logger()


@dataclass(frozen=True)
class _MongoDoc:
    """One payload reduced to its target collection and serialized document."""

    collection: str
    document: dict


def _group_into_runs(docs: list[_MongoDoc]) -> list[list[_MongoDoc]]:
    """Bucket documents into consecutive runs sharing a collection.

    A document merges only with its immediate neighbours, which guarantees
    execution order equals payload order. Global bucketing would be a
    slightly better batcher but reorders: payloads ``A(c1), B(c2), C(c1)``
    would execute as A, C, B, deferring B past C. That is harmless for
    inserts — and is what this sink used to do — but once updates and
    deletes exist, an update to a document and a later delete of it must not
    be reordered relative to each other.

    Handlers overwhelmingly emit uniform payload lists, so runs are long in
    practice and the batching cost is small. The Postgres sink groups the
    same way, for the same reason.
    """
    runs: list[list[_MongoDoc]] = []
    for doc in docs:
        if runs and runs[-1][0].collection == doc.collection:
            runs[-1].append(doc)
        else:
            runs.append([doc])
    return runs


def _build_doc(payload: MongoPayload) -> _MongoDoc:
    """Reduce one payload to the document its operation writes.

    TEMPORARY guard: ``MongoPayload`` already carries every op, so an op
    the sink cannot yet execute must fail loudly rather than fall through
    to an insert of the wrong thing. Removed as each op lands.
    """
    if payload.op is not MongoOp.INSERT:
        raise ValueError(f'mongo op {payload.op.value!r} cannot be built yet')
    # The per-op field contract guarantees it for an insert.
    assert payload.data is not None
    return _MongoDoc(collection=payload.collection, document=payload.data.model_dump())


class MongoSink(BaseSink[MongoPayload]):
    """Inserts documents into MongoDB collections.

    Each MongoPayload is serialized:
        - collection = payload.collection
        - document = payload.data.model_dump()

    Uses PyMongo's AsyncMongoClient for native asyncio support.
    """

    sink_type = 'mongo'

    # ``insert_one`` / ``insert_many`` without a stable ``_id`` and
    # unique-index is NOT idempotent — a retry can duplicate documents
    # in the collection. We keep the safe default of ``False`` so
    # transient Mongo errors route to ``on_delivery_error`` instead of
    # being auto-retried. Users who set a deterministic ``_id`` on the
    # payload (or use an upsert in a custom subclass) can flip this to
    # ``True`` to opt into automatic transient-error retry.
    idempotent = False

    def __init__(self, name: str, config: MongoSinkConfig) -> None:
        super().__init__(name, ui_url=config.ui_url)
        self._config = config
        self._client = None
        self._db = None

    async def connect(self) -> None:
        """Create the PyMongo async client and get database reference."""
        from pymongo import AsyncMongoClient

        self._client = AsyncMongoClient(self._config.uri)
        self._db = self._client[self._config.database]
        await logger.ainfo(
            'mongo_sink_connected',
            category='sink',
            sink_name=self._name,
            uri=redact_url(self._config.uri),
            database=self._config.database,
        )

    async def deliver(self, payloads: list[MongoPayload]) -> None:
        """Insert every payload as a document into its target collection.

        Documents are grouped by collection and each group is sent as ONE
        ``insert_many`` instead of one round-trip per payload. Failure
        granularity is preserved: a batch that fails is retried
        document-by-document so the error an operator sees names the
        offending document, exactly as the per-payload loop did. See the Go
        backend's ``internal/sinks/mongo.go`` — the two must stay observably
        identical (divergence #18 in its migration notes).
        """
        if not payloads or self._db is None:
            return

        start = time.monotonic()
        labels = {'sink_type': self.sink_type, 'sink_name': self._name}
        try:
            docs, bad_index, build_error = self._build_docs(payloads)
            if build_error is not None:
                # The per-payload loop inserted every document BEFORE the
                # invalid payload, then raised. Reproduce those side effects
                # (an insert failure on the way takes precedence, exactly as
                # the sequential loop would have hit it first).
                for doc in docs[:bad_index]:
                    await self._insert_single(doc)
                raise build_error
            for group in _group_into_runs(docs):
                await self._deliver_group(group)

            sink_payloads_delivered.labels(**labels).inc(len(payloads))
            sink_deliver_duration.labels(**labels).observe(time.monotonic() - start)
        except Exception:
            sink_deliver_errors.labels(**labels).inc()
            raise

    @staticmethod
    def _build_docs(payloads: list[MongoPayload]) -> tuple[list[_MongoDoc], int, Exception | None]:
        """Serialize every payload up front.

        On the first bad payload returns the documents built so far, the
        failing index, and the error — the caller replays the legacy partial
        side effects before raising it.
        """
        docs: list[_MongoDoc] = []
        for i, payload in enumerate(payloads):
            try:
                docs.append(_build_doc(payload))
            except Exception as e:
                return docs, i, e
        return docs, len(docs), None

    async def _deliver_group(self, group: list[_MongoDoc]) -> None:
        """Insert one collection's documents, falling back per-document on failure.

        On an ``insert_many`` failure, the fallback resends each document one
        at a time so the error an operator sees names the document that
        actually failed. A side effect: documents earlier in the group that
        the failed batch already wrote get re-inserted as brand-new documents
        (with a fresh, server-assigned ``_id``) — an accepted duplicate under
        this framework's at-least-once delivery guarantee.
        """
        if len(group) == 1:
            await self._insert_single(group[0])
            return
        assert self._db is not None
        try:
            await self._db[group[0].collection].insert_many([d.document for d in group])
            return
        except Exception:
            # Batch failed — fall back to per-document delivery so the error
            # names the offending document (and to ride out a transient).
            pass
        for doc in group:
            # PyMongo's insert_many mutates every document it is handed,
            # writing a generated ``_id`` back into the caller's dict —
            # even on documents whose insert never actually committed
            # (e.g. the whole batch was rejected before the server was
            # reached). If we resend a document that now carries that
            # leftover ``_id``, Mongo treats it as an update-in-place
            # against an existing document with that id and raises
            # DuplicateKeyError on the FIRST document, no matter which one
            # was genuinely bad — misnaming the culprit and aborting the
            # fallback before it reaches the real offender. Stripping the
            # injected id makes each retry insert as a brand-new document
            # (get a fresh, server-assigned id), so the error correctly
            # names the actual failure. The cost is accepted: documents
            # already written by the failed batch get written again under
            # a new id — an allowed duplicate under this framework's
            # at-least-once delivery guarantee.
            doc.document.pop('_id', None)
            await self._insert_single(doc)

    async def _insert_single(self, doc: _MongoDoc) -> None:
        """Insert one document — the shape the pre-batching loop produced."""
        assert self._db is not None
        await self._db[doc.collection].insert_one(doc.document)

    async def close(self) -> None:
        """Close the PyMongo async client.

        ``AsyncMongoClient.close`` is a coroutine — unlike motor's sync
        ``close()``. Calling it without ``await`` creates an un-awaited
        coroutine that never closes the client, leaking connections on
        shutdown with no error.
        """
        if self._client is not None:
            try:
                await self._client.close()
            except Exception as e:
                await logger.awarning(
                    'mongo_sink_close_error',
                    category='sink',
                    sink_name=self._name,
                    error=str(e),
                )
            self._client = None
            self._db = None
