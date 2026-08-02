"""MongoDB sink — inserts documents into collections.

Wraps PyMongo's AsyncMongoClient. Each MongoPayload's data field is
serialized via model_dump() to get a dict suitable for MongoDB insertion.
"""

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

import structlog

from drakkar.config import MongoSinkConfig
from drakkar.metrics import sink_deliver_duration, sink_deliver_errors, sink_payloads_delivered
from drakkar.models import MongoOp, MongoPayload
from drakkar.sinks.base import BaseSink
from drakkar.utils import redact_url

if TYPE_CHECKING:
    # Type-only: the runtime imports stay inside the functions that need
    # them, so importing drakkar does not pull in pymongo for workers that
    # never use this sink.
    from pymongo.operations import DeleteMany, DeleteOne, InsertOne, UpdateMany, UpdateOne

    _WriteModel = InsertOne | UpdateOne | UpdateMany | DeleteOne | DeleteMany

logger = structlog.get_logger()


class MongoWriteError(Exception):
    """One or more operations in a bulk write were rejected by the server.

    Deliberately NOT a subclass of the builtin ``ConnectionError`` or
    ``TimeoutError``: ``SinkManager`` treats those as transient and
    fast-retries them, and a write error such as a duplicate key fails
    identically every time. Retrying it would burn the retry budget and, for
    a batch containing an insert, could duplicate documents.
    """


@dataclass(frozen=True)
class _MongoUnit:
    """One payload reduced to the write it performs.

    Carries both shapes the sink needs: ``model`` for a bulk write, and the
    driver method plus arguments for the direct call a single-payload run
    uses. The two are the same operation through two APIs, so building them
    together keeps them from drifting.
    """

    op: MongoOp
    collection: str
    label: str
    """Human-readable identification for error messages, e.g.
    ``update_one collection=jobs``. Never the document, which carries
    message content, nor an operator's MQL."""
    model: '_WriteModel'
    """The pymongo write model: InsertOne, UpdateOne, DeleteMany, …"""
    method: str
    """The AsyncCollection method name for the direct path."""
    args: tuple[object, ...]
    kwargs: dict[str, object]


def _group_into_runs(units: list[_MongoUnit]) -> list[list[_MongoUnit]]:
    """Bucket units into consecutive runs sharing a collection.

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
    runs: list[list[_MongoUnit]] = []
    for unit in units:
        if runs and runs[-1][0].collection == unit.collection:
            runs[-1].append(unit)
        else:
            runs.append([unit])
    return runs


def _build_unit(payload: MongoPayload) -> _MongoUnit:
    """Reduce one payload to the write its operation performs.

    TEMPORARY guard: ``MongoPayload`` already carries every op, so an op
    the sink cannot yet execute must fail loudly rather than fall through
    to an insert of the wrong thing. Removed as each op lands.
    """
    from pymongo import InsertOne

    if payload.op is not MongoOp.INSERT:
        raise ValueError(f'mongo op {payload.op.value!r} cannot be built yet')
    # The per-op field contract guarantees it for an insert.
    assert payload.data is not None
    document = payload.data.model_dump()
    return _MongoUnit(
        op=payload.op,
        collection=payload.collection,
        label=f'{payload.op.value} collection={payload.collection}',
        model=InsertOne(document),
        method='insert_one',
        args=(document,),
        kwargs={},
    )


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
        """Write every payload, one operation each, in payload order.

        Payloads are grouped into consecutive runs of the same collection
        and each run is sent as ONE ordered bulk write instead of one
        round-trip per payload. A run can carry heterogeneous operations —
        an insert, an update and a delete against one collection travel
        together, which ``insert_many`` could not express at all.
        """
        if not payloads or self._db is None:
            return

        start = time.monotonic()
        labels = {'sink_type': self.sink_type, 'sink_name': self._name}
        try:
            units, bad_index, build_error = self._build_units(payloads)
            if build_error is not None:
                # The per-payload loop wrote everything BEFORE the invalid
                # payload, then raised. Reproduce those side effects (a write
                # failure on the way takes precedence, exactly as the
                # sequential loop would have hit it first).
                for unit in units[:bad_index]:
                    await self._execute_single(unit)
                raise build_error
            for run in _group_into_runs(units):
                await self._deliver_run(run)

            sink_payloads_delivered.labels(**labels).inc(len(payloads))
            sink_deliver_duration.labels(**labels).observe(time.monotonic() - start)
        except Exception:
            sink_deliver_errors.labels(**labels).inc()
            raise

    @staticmethod
    def _build_units(payloads: list[MongoPayload]) -> tuple[list[_MongoUnit], int, Exception | None]:
        """Build the write for every payload up front.

        On the first bad payload returns the units built so far, the failing
        index, and the error — the caller replays the legacy partial side
        effects before raising it.
        """
        units: list[_MongoUnit] = []
        for i, payload in enumerate(payloads):
            try:
                units.append(_build_unit(payload))
            except Exception as e:
                return units, i, e
        return units, len(units), None

    async def _deliver_run(self, run: list[_MongoUnit]) -> None:
        """Send one collection's writes as a single ordered bulk write.

        ``ordered=True`` is load-bearing three times over: execution order
        equals payload order, execution stops at the first failure, and the
        index in ``writeErrors`` is positionally aligned with the models we
        submitted — which is what names the offending payload EXACTLY,
        without re-sending anything.

        Nothing is ever replayed. The previous fallback re-sent the run one
        document at a time, which forced a workaround for PyMongo writing a
        generated ``_id`` back into every document it was handed: a resent
        document carrying that leftover id raised DuplicateKeyError on the
        FIRST document rather than the guilty one, so the fallback stripped
        the id and knowingly wrote duplicates. With no replay that whole
        problem disappears — this is a strict improvement on the 1.3.0 fix,
        not a regression of it.
        """
        from pymongo.errors import BulkWriteError

        if len(run) == 1:
            await self._execute_single(run[0])
            return
        assert self._db is not None
        try:
            await self._db[run[0].collection].bulk_write([unit.model for unit in run], ordered=True)
        except BulkWriteError as e:
            errors = e.details.get('writeErrors') or []
            if not errors:
                raise
            first = errors[0]
            unit = run[first['index']]
            raise MongoWriteError(
                f'{len(errors)} of {len(run)} Mongo operations failed; '
                f'first failure on {unit.label}: {first.get("errmsg", "")}'
            ) from e

    async def _execute_single(self, unit: _MongoUnit) -> None:
        """Run one write directly — the shape the pre-batching loop produced.

        Avoids bulk-write overhead for a one-payload run, and attribution
        comes from the raised error itself.
        """
        assert self._db is not None
        await getattr(self._db[unit.collection], unit.method)(*unit.args, **unit.kwargs)

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
