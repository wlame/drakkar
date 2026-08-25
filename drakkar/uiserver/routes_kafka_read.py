"""Ad-hoc Kafka read JSON API: ``/api/v1/debug/kafka/*``.

Routes:
  * ``/api/v1/debug/kafka/topics``            — the readable aliases (never raw topic names).
  * ``/api/v1/debug/kafka/{alias}/message``   — one message by (partition, offset).
  * ``/api/v1/debug/kafka/{alias}/messages``  — NDJSON stream of a time window.

The read logic lives in ``drakkar.kafka_read``; this module only maps it
onto HTTP. Reads join no consumer group and commit no offsets — see that
module's docstring for the contract.

Two gates stack on every route, mirroring the cache router and the
probe/merge endpoints:

  * ``require_auth`` — the ``ui.auth_token`` bearer check (no-op with no
    token configured).
  * ``ui.kafka_read_enabled`` — operator kill switch, 403 when off. The
    routes stay registered so the served surface always equals the
    OpenAPI contract; policy, not absence, refuses.

The startup warning for "Kafka is authenticated but the UI is not" is
emitted from the router factory (server construction = worker startup):
it names the exposed aliases so the operator sees exactly which topics
are reachable without a token, and serving continues — closing the gap
is a one-line config change (``ui.auth_token`` or
``ui.kafka_read_enabled=false``), and the admin owns that call.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse, StreamingResponse

from drakkar.kafka_read import (
    STREAM_LIMIT_MAX,
    AliasTarget,
    KafkaReadNotFound,
    KafkaReadUnavailable,
    build_alias_table,
    fetch_message,
    stream_messages,
)

if TYPE_CHECKING:
    from drakkar.uiserver.server import UIDeps

logger = structlog.get_logger()


def _disabled_response() -> JSONResponse:
    """403 for the kafka-read surface when the operator switched it off.

    Same shape and reasoning as ``routes_debug._disabled_response``: the
    route exists, policy refuses, and naming the config key makes the fix
    a one-liner.
    """
    return JSONResponse(
        {'error': 'The kafka read endpoints are disabled by configuration (set ui.kafka_read_enabled=true to enable)'},
        status_code=403,
    )


def create_kafka_read_router(deps: UIDeps) -> APIRouter:
    """Build the router that owns ``/api/v1/debug/kafka/*`` endpoints."""
    # Kafka reads expose message contents — gate the whole router behind
    # require_auth (no-op without a token), like the cache router.
    router = APIRouter(dependencies=[Depends(deps.require_auth)])
    config = deps.config

    # The alias table is resolved once at startup: DrakkarConfig is
    # immutable at runtime, so topics/credentials cannot change under a
    # running server (matches the Jinja globals capturing config the
    # same way). ``_config`` rather than the public property: it is the
    # attribute the server module already reads for Jinja globals, and
    # the one the test fixtures wire with a real DrakkarConfig.
    aliases: dict[str, AliasTarget] = build_alias_table(deps.drakkar_app._config)

    _warn_if_exposed_without_ui_auth(config.auth_token, config.kafka_read_enabled, aliases)

    def _target_or_404(alias: str) -> AliasTarget:
        target = aliases.get(alias)
        if target is None:
            raise HTTPException(
                status_code=404,
                detail=f"Unknown topic alias '{alias}' — valid aliases: {sorted(aliases)}",
            )
        return target

    @router.get('/api/v1/debug/kafka/topics')
    async def kafka_topics():
        """The readable topic aliases and their kinds.

        Deliberately omits raw topic names and broker addresses — the
        alias is the entire addressing surface of this API.
        """
        if not config.kafka_read_enabled:
            return _disabled_response()
        return {'topics': [{'alias': t.alias, 'kind': t.kind} for t in aliases.values()]}

    @router.get('/api/v1/debug/kafka/{alias}/message')
    async def kafka_message(
        alias: str,
        partition: int = Query(ge=0),
        offset: int = Query(ge=0),
    ):
        """One message by exact coordinates, with full record metadata."""
        if not config.kafka_read_enabled:
            return _disabled_response()
        target = _target_or_404(alias)
        try:
            return await fetch_message(target, partition, offset)
        except KafkaReadNotFound as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except KafkaReadUnavailable as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @router.get('/api/v1/debug/kafka/{alias}/messages')
    async def kafka_messages(
        alias: str,
        from_ts: int = Query(ge=0, description='Start of the window, epoch milliseconds (inclusive)'),
        to_ts: int | None = Query(default=None, ge=0, description='End of the window, epoch milliseconds'),
        limit: int | None = Query(default=None, ge=1, le=STREAM_LIMIT_MAX),
        partition: int | None = Query(default=None, ge=0),
    ):
        """NDJSON stream of messages with ``timestamp >= from_ts``.

        One message object per line. Ends at ``to_ts``, after ``limit``
        messages, or at the end-of-topic snapshot taken when the request
        started — whichever comes first. A mid-stream broker failure is
        reported as a final ``{"error": ...}`` line (the 200 header is
        already on the wire by then).
        """
        if not config.kafka_read_enabled:
            return _disabled_response()
        target = _target_or_404(alias)
        if to_ts is not None and to_ts < from_ts:
            raise HTTPException(
                status_code=422,
                detail=[{'loc': ['query', 'to_ts'], 'msg': 'to_ts must be greater than or equal to from_ts'}],
            )

        agen = stream_messages(target, from_ts_ms=from_ts, to_ts_ms=to_ts, limit=limit, partition=partition)
        # Pull the first message before responding: metadata errors (unknown
        # partition, unreachable brokers) surface on the first iteration,
        # and this is the last moment a real HTTP status can carry them —
        # StreamingResponse commits 200 before the body runs.
        try:
            first = await anext(agen, None)
        except KafkaReadNotFound as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except KafkaReadUnavailable as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

        async def ndjson_body():
            try:
                if first is not None:
                    yield first.model_dump_json() + '\n'
                    async for message in agen:
                        yield message.model_dump_json() + '\n'
            except KafkaReadUnavailable as exc:
                yield json.dumps({'error': str(exc)}) + '\n'
            finally:
                # Covers client disconnects too: Starlette cancels the body
                # generator, and closing agen runs kafka_read's cleanup
                # (the ad-hoc consumer is closed, not leaked).
                await agen.aclose()

        return StreamingResponse(ndjson_body(), media_type='application/x-ndjson')

    return router


def _warn_if_exposed_without_ui_auth(
    auth_token: str,
    kafka_read_enabled: bool,
    aliases: dict[str, AliasTarget],
) -> None:
    """Startup warning: Kafka requires credentials, the UI does not.

    An operator who secured the brokers but left ``ui.auth_token`` empty
    has (perhaps unknowingly) made these topics readable without any
    credential through this API. Serving continues — the admin owns the
    trade-off — but the warning names the exact aliases exposed so the
    decision is a visible one.
    """
    if not kafka_read_enabled or auth_token:
        return
    exposed = sorted(t.alias for t in aliases.values() if t.security.protocol != 'PLAINTEXT')
    if not exposed:
        return
    logger.warning(
        'kafka_read_exposed_without_ui_auth',
        category='kafka',
        aliases=exposed,
        reason='Kafka security is configured for these topics but ui.auth_token is empty: '
        'their messages are readable through /api/v1/debug/kafka/* without any credential. '
        'Set ui.auth_token to gate the API, or ui.kafka_read_enabled=false to close it.',
    )
