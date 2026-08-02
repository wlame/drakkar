"""MongoDB sink — inserts documents into collections.

Wraps PyMongo's AsyncMongoClient. Each MongoPayload's data field is
serialized via model_dump() to get a dict suitable for MongoDB insertion.
"""

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

import structlog
from pydantic import BaseModel

from drakkar.config import MongoSinkConfig
from drakkar.metrics import sink_deliver_duration, sink_deliver_errors, sink_payloads_delivered
from drakkar.models import MongoOp, MongoPayload
from drakkar.mql import CompiledTemplate, compile_template, substitute
from drakkar.sinks.base import BaseSink
from drakkar.utils import redact_url

if TYPE_CHECKING:
    # Type-only: the runtime imports stay inside the functions that need
    # them, so importing drakkar does not pull in pymongo for workers that
    # never use this sink.
    from pymongo.operations import DeleteMany, DeleteOne, InsertOne, UpdateMany, UpdateOne

    _WriteModel = InsertOne | UpdateOne | UpdateMany | DeleteOne | DeleteMany
    # What a per-op builder returns: the bulk-write model, plus the
    # driver method and arguments the single-payload path calls.
    _BuiltUnit = tuple[_WriteModel, str, tuple[object, ...], dict[str, object]]
    # An update document is a mapping of operators, or an aggregation
    # pipeline; a delete carries none at all.
    _UpdateDocument = dict[str, Any] | list[dict[str, Any]] | None

logger = structlog.get_logger()


class MongoWriteError(Exception):
    """One or more operations in a bulk write were rejected by the server.

    Deliberately NOT a subclass of the builtin ``ConnectionError`` or
    ``TimeoutError``: ``SinkManager`` treats those as transient and
    fast-retries them, and a write error such as a duplicate key fails
    identically every time. Retrying it would burn the retry budget and, for
    a batch containing an insert, could duplicate documents.
    """


# Ops a retry could apply twice with a different end state. Everything else
# in MongoOp converges, which is what makes a batch of updates and deletes
# safe to fast-retry even though the sink's type-level flag says otherwise.
_NOT_IDEMPOTENT_OPS = frozenset({MongoOp.INSERT, MongoOp.STATEMENT})


def _as_builtin_transient(exc: BaseException) -> BaseException | None:
    """Translate a PyMongo transient error into the builtin equivalent.

    ``SinkManager`` classifies transient errors by matching the BUILTIN
    ``ConnectionError`` / ``TimeoutError`` (``manager._TRANSIENT_ERRORS``),
    but ``pymongo.errors.ConnectionFailure`` and ``NetworkTimeout`` inherit
    only from ``PyMongoError``. A dropped Mongo connection was therefore
    never eligible for the idempotent fast-retry — the same latent defect
    the Redis sink had. Nothing depended on it while this sink was
    unconditionally non-idempotent; ``batch_idempotent`` makes it live for
    update, upsert and delete batches, so it is fixed first.

    ``manager`` explicitly delegates this responsibility to sinks: "raise a
    library-specific exception that the sink implementation can remap before
    re-raising".

    Returns ``None`` when the error is not transient, so the caller can
    re-raise it untouched — remapping a write error such as a duplicate key
    would make the manager retry a request that fails identically every
    time. ``NetworkTimeout`` is checked first because it inherits from
    ``ConnectionFailure``.
    """
    from pymongo.errors import ConnectionFailure, NetworkTimeout

    if isinstance(exc, NetworkTimeout):
        return TimeoutError(str(exc))
    if isinstance(exc, ConnectionFailure):
        return ConnectionError(str(exc))
    return None


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


def _dumped(model: BaseModel | None, field: str, op: MongoOp) -> dict:
    """Serialize a payload body, refusing an empty result.

    This is the second of two independent guards on the unbounded-write
    hazard, and the reason it exists separately from the payload validator:
    the validator checks the payload as CONSTRUCTED, while this checks what
    the model actually dumps, so a model mutated afterwards — or one whose
    fields are all None — cannot slip through. An empty Mongo filter matches
    EVERY document, so ``delete_many`` with one empties a collection; an
    empty ``$set`` is simply malformed.

    The two guards also route differently: the validator surfaces through
    ``on_error`` in the handler's own code, this one through
    ``on_delivery_error``.
    """
    assert model is not None  # the per-op field contract guarantees it
    dumped = model.model_dump()
    if not dumped:
        raise ValueError(
            f'MongoPayload(op={op.value!r}) has an empty {field!r} — '
            f'an empty filter matches every document in the collection'
            if field == 'filter'
            else f'MongoPayload(op={op.value!r}) has an empty {field!r}'
        )
    return dumped


def _build_insert(payload: MongoPayload) -> '_BuiltUnit':
    """InsertOne — the document as given."""
    from pymongo import InsertOne

    document = _dumped(payload.data, 'data', payload.op)
    return InsertOne(document), 'insert_one', (document,), {}


def _write_for(op: MongoOp, predicate: dict[str, Any], update: '_UpdateDocument') -> '_BuiltUnit':
    """Map one mutating op to its driver write model and direct call.

    The single place op → driver write lives, so a declarative payload and
    an operator-authored statement with the same op always issue the same
    thing. ``update`` is a mapping for the declarative ops and may be an
    aggregation pipeline for a statement; the driver accepts either.
    """
    from pymongo import DeleteMany, DeleteOne, UpdateMany, UpdateOne

    if op is MongoOp.DELETE_ONE:
        return DeleteOne(predicate), 'delete_one', (predicate,), {}
    if op is MongoOp.DELETE_MANY:
        return DeleteMany(predicate), 'delete_many', (predicate,), {}

    # Everything else writes a document, guaranteed present by both the
    # per-op payload contract and the statement config validation.
    assert update is not None
    if op is MongoOp.UPDATE_MANY:
        return UpdateMany(predicate, update), 'update_many', (predicate, update), {}
    if op is MongoOp.UPSERT:
        return UpdateOne(predicate, update, upsert=True), 'update_one', (predicate, update), {'upsert': True}
    return UpdateOne(predicate, update), 'update_one', (predicate, update), {}


def _build_update(payload: MongoPayload) -> '_BuiltUnit':
    """The declarative update ops assign fields; anything richer is a statement."""
    predicate = _dumped(payload.filter, 'filter', payload.op)
    return _write_for(payload.op, predicate, {'$set': _dumped(payload.data, 'data', payload.op)})


def _build_delete(payload: MongoPayload) -> '_BuiltUnit':
    """DeleteOne/DeleteMany against a required, non-empty filter."""
    return _write_for(payload.op, _dumped(payload.filter, 'filter', payload.op), None)


# Which driver write each op builds. A table rather than a branch chain: the
# mapping IS the specification, and it is what the Go backend has to
# reproduce operation for operation.
#
# One and many stay separate entries rather than a flag, because the blast
# radius differs by orders of magnitude and the driver makes the distinction
# primary.
_MONGO_UNIT_BUILDERS = {
    MongoOp.INSERT: _build_insert,
    MongoOp.UPDATE_ONE: _build_update,
    MongoOp.UPDATE_MANY: _build_update,
    MongoOp.UPSERT: _build_update,
    MongoOp.DELETE_ONE: _build_delete,
    MongoOp.DELETE_MANY: _build_delete,
}


@dataclass(frozen=True)
class _CompiledStatement:
    """One operator-authored statement, ready to bind parameters into."""

    collection: str
    op: MongoOp
    template: CompiledTemplate


def _build_statement_unit(payload: MongoPayload, statements: dict[str, _CompiledStatement]) -> _MongoUnit:
    """Look up an operator-authored statement and bind its parameters.

    The statement supplies the collection and the operation; the payload
    supplies only values, which are substituted into whole-value positions
    and never into keys. That is what keeps message content out of operator
    positions, where a value of ``{"$gt": ""}`` would match every document.
    """
    statement = statements.get(payload.statement)
    if statement is None:
        known = ', '.join(sorted(statements)) or '<none configured>'
        raise ValueError(f'unknown mongo statement {payload.statement!r}; configured: {known}')
    params = payload.params.model_dump() if payload.params is not None else {}
    try:
        document = substitute(statement.template, params)
    except ValueError as e:
        raise ValueError(f'mongo statement {payload.statement!r}: {e}') from e
    bound = cast(dict[str, Any], document)
    model, method, args, kwargs = _write_for(statement.op, bound['filter'], bound.get('update'))
    return _MongoUnit(
        op=MongoOp.STATEMENT,
        collection=statement.collection,
        # The statement NAME, never its document — an operator may have
        # written the template around row data.
        label=f'statement={payload.statement} collection={statement.collection}',
        model=model,
        method=method,
        args=args,
        kwargs=kwargs,
    )


def _build_unit(payload: MongoPayload, statements: dict[str, _CompiledStatement]) -> _MongoUnit:
    """Reduce one payload to the write its operation performs."""
    if payload.op is MongoOp.STATEMENT:
        return _build_statement_unit(payload, statements)
    model, method, args, kwargs = _MONGO_UNIT_BUILDERS[payload.op](payload)
    return _MongoUnit(
        op=payload.op,
        collection=payload.collection,
        label=f'{payload.op.value} collection={payload.collection}',
        model=model,
        method=method,
        args=args,
        kwargs=kwargs,
    )


class MongoSink(BaseSink[MongoPayload]):
    """Inserts documents into MongoDB collections.

    Each MongoPayload is serialized:
        - collection = payload.collection
        - document = payload.data.model_dump()

    Uses PyMongo's AsyncMongoClient for native asyncio support.
    """

    sink_type = 'mongo'

    # An insert without a stable ``_id`` and a unique index is NOT
    # idempotent — a retry can duplicate documents in the collection. This
    # stays the conservative type-level fallback for any path that still
    # reads the flag; the real decision is per batch, in
    # ``batch_idempotent``, because a batch of updates and deletes IS safe
    # to repeat and only the payloads say which kind a batch is.
    idempotent = False

    def batch_idempotent(self, payloads: list[MongoPayload]) -> bool:
        """Retry-safe unless the batch inserts or runs operator MQL.

        ``$set`` against a fixed filter converges, an upsert converges on
        insert-or-set, and a delete converges on removal. A plain insert
        duplicates documents, and an operator's MQL is opaque to the
        framework — ``$inc`` accumulates and we cannot tell.

        Marking individual statements idempotent in configuration is a
        natural extension, deliberately left out for now, as it is in both
        companion sinks.
        """
        return not any(payload.op in _NOT_IDEMPOTENT_OPS for payload in payloads)

    def __init__(self, name: str, config: MongoSinkConfig) -> None:
        super().__init__(name, ui_url=config.ui_url)
        self._config = config
        self._client = None
        self._db = None
        # Operator-authored MQL, compiled at connect(). Never compiled on
        # the delivery path.
        self._statements: dict[str, _CompiledStatement] = {}

    async def connect(self) -> None:
        """Create the PyMongo async client and get database reference."""
        from pymongo import AsyncMongoClient

        self._client = AsyncMongoClient(self._config.uri)
        self._db = self._client[self._config.database]
        # Compiling here rather than per delivery turns each write into a
        # document copy plus N assignments. Config validation already
        # compiled these once, so this is also the sink's own guard against
        # a config assembled in code rather than loaded from YAML.
        self._statements = {
            name: _CompiledStatement(
                collection=statement.collection,
                op=statement.op,
                template=compile_template(statement.template()),
            )
            for name, statement in self._config.statements.items()
        }
        await logger.ainfo(
            'mongo_sink_connected',
            category='sink',
            sink_name=self._name,
            uri=redact_url(self._config.uri),
            database=self._config.database,
            statements=len(self._statements),
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
        except Exception as exc:
            sink_deliver_errors.labels(**labels).inc()
            transient = _as_builtin_transient(exc)
            if transient is not None:
                raise transient from exc
            raise

    def _build_units(self, payloads: list[MongoPayload]) -> tuple[list[_MongoUnit], int, Exception | None]:
        """Build the write for every payload up front.

        On the first bad payload returns the units built so far, the failing
        index, and the error — the caller replays the legacy partial side
        effects before raising it.
        """
        units: list[_MongoUnit] = []
        for i, payload in enumerate(payloads):
            try:
                units.append(_build_unit(payload, self._statements))
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
