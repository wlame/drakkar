"""Redis sink — sets key-value pairs in Redis.

Wraps redis.asyncio.Redis. Each RedisPayload's data field is serialized
via model_dump_json() and stored as a string value under the configured
key prefix + payload key, with optional TTL.
"""

import time
from collections.abc import Callable
from dataclasses import dataclass

import structlog

from drakkar.config import RedisSinkConfig
from drakkar.metrics import sink_deliver_duration, sink_deliver_errors, sink_payloads_delivered
from drakkar.models import RedisOp, RedisPayload
from drakkar.sinks.base import BaseSink
from drakkar.utils import redact_url

logger = structlog.get_logger()


@dataclass(frozen=True)
class _PlainCommand:
    """One payload reduced to a redis-py method call.

    ``queue`` and ``execute`` are the same call through two different
    objects, so the pipeline path and the single-payload path share one
    shape and neither has to know which ops exist.
    """

    op: RedisOp
    label: str
    """Human-readable identification for error messages, e.g.
    ``hset key=drakkar:session:42``. Never the stored value, which can
    carry message content."""
    method: str
    """The redis-py method name: 'set', 'hset', 'incrby', …"""
    args: tuple[object, ...]
    kwargs: dict[str, object]

    async def queue(self, pipe: object) -> None:
        """Add this command to a pipeline.

        Declared ``async`` even though plain pipeline command methods are
        SYNCHRONOUS and return the pipeline. ``AsyncScript.__call__`` is
        ``async def`` and must be awaited even when only queueing, so
        making both queue methods awaitable keeps one uniform call site.
        """
        getattr(pipe, self.method)(*self.args, **self.kwargs)

    async def execute(self, client: object) -> None:
        """Run this command directly, outside a pipeline."""
        await getattr(client, self.method)(*self.args, **self.kwargs)


# One rendered redis-py call: method name, positional args, keyword args.
_Rendered = tuple[str, tuple[object, ...], dict[str, object]]
# A renderer receives the payload and its ALREADY-PREFIXED key.
_CommandRenderer = Callable[[RedisPayload, str], _Rendered]


def _render_set(payload: RedisPayload, key: str) -> _Rendered:
    """SET pk <json> [EX ttl]."""
    assert payload.data is not None  # the per-op contract guarantees it
    kwargs: dict[str, object] = {'ex': payload.ttl} if payload.ttl is not None else {}
    return 'set', (key, payload.data.model_dump_json()), kwargs


def _render_push(payload: RedisPayload, key: str) -> _Rendered:
    """LPUSH/RPUSH pk <json> — one list element is one serialized object."""
    assert payload.data is not None
    method = 'rpush' if payload.side == 'right' else 'lpush'
    return method, (key, payload.data.model_dump_json()), {}


# Which redis-py call each op renders to. A table rather than a branch chain:
# the mapping IS the specification, and it is what the Go backend has to
# reproduce argument-for-argument.
#
# Every renderer receives the ALREADY-PREFIXED key, so no renderer can
# forget the sink instance's namespace.
_COMMAND_RENDERERS: dict[RedisOp, _CommandRenderer] = {
    RedisOp.SET: _render_set,
    RedisOp.DELETE: lambda p, key: ('delete', (key,), {}),
    RedisOp.EXPIRE: lambda p, key: ('expire', (key, p.ttl), {}),
    RedisOp.INCRBY: lambda p, key: ('incrby', (key, p.amount), {}),
    RedisOp.PUSH: _render_push,
    RedisOp.TRIM: lambda p, key: ('ltrim', (key, p.start, p.stop), {}),
}


class RedisCommandError(Exception):
    """One or more commands in a pipeline failed on the server.

    Deliberately NOT a subclass of the builtin ``ConnectionError`` or
    ``TimeoutError``: ``SinkManager`` treats those as transient and
    fast-retries them, and a command error such as ``WRONGTYPE`` fails
    identically every time. Retrying it would burn the retry budget and, for
    a batch containing an accumulating command, could double-apply it.
    """


def _as_builtin_transient(exc: BaseException) -> BaseException | None:
    """Translate a redis-py transient error into the builtin equivalent.

    ``SinkManager`` classifies transient errors by matching the BUILTIN
    ``ConnectionError`` / ``TimeoutError`` (``manager._TRANSIENT_ERRORS``),
    but ``redis.exceptions.ConnectionError`` and its ``TimeoutError``
    inherit only from ``RedisError``. A dropped Redis connection was
    therefore never eligible for the idempotent fast-retry, so this sink's
    ``idempotent = True`` declaration did nothing — while the Go backend,
    whose classifier is structural, has always retried. Remapping here
    closes that divergence.

    ``manager`` explicitly delegates this responsibility to sinks: "raise a
    library-specific exception that the sink implementation can remap
    before re-raising".

    Returns ``None`` when the error is not transient, so the caller can
    re-raise it untouched — remapping a command error such as ``WRONGTYPE``
    would make the manager retry a request that fails identically every
    time. The redis import is local because ``redis`` is only imported when
    this sink is actually used.
    """
    from redis.exceptions import ConnectionError as RedisConnectionError
    from redis.exceptions import TimeoutError as RedisTimeoutError

    if isinstance(exc, RedisTimeoutError):
        return TimeoutError(str(exc))
    if isinstance(exc, RedisConnectionError):
        return ConnectionError(str(exc))
    return None


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
    # into automatic retry via ``idempotent=True``.
    #
    # TTLs ride along on the SET, which means a retried ``SET … EX 3600``
    # DOES restart the expiry window. That is accepted: the drift is
    # bounded by the fast-retry's backoff (hundreds of milliseconds). The
    # alternative — an absolute ``EXAT`` deadline computed here — would
    # converge exactly but would take the timestamp from the worker's
    # clock, so worker/server skew would shift real expiry times. Clock
    # skew is the worse operational hazard.
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
            commands, bad_index, build_error = self._build_items(payloads)
            if build_error is not None:
                # The per-payload loop SET every key BEFORE the
                # unserializable payload, then raised. Reproduce those side
                # effects (a set failure on the way takes precedence,
                # exactly as the sequential loop would have hit it first).
                for command in commands[:bad_index]:
                    await self._execute_single(command)
                raise build_error
            if len(commands) == 1:
                await self._execute_single(commands[0])
            else:
                await self._execute_batch(commands)

            sink_payloads_delivered.labels(**labels).inc(len(payloads))
            sink_deliver_duration.labels(**labels).observe(time.monotonic() - start)
        except Exception as exc:
            sink_deliver_errors.labels(**labels).inc()
            transient = _as_builtin_transient(exc)
            if transient is not None:
                raise transient from exc
            raise

    def _build_items(self, payloads: list[RedisPayload]) -> tuple[list[_PlainCommand], int, Exception | None]:
        """Build a command for every payload up front.

        On the first bad payload returns the commands built so far, the
        failing index, and the error — the caller replays the legacy partial
        side effects before raising it.
        """
        commands: list[_PlainCommand] = []
        for i, payload in enumerate(payloads):
            try:
                commands.append(self._build_command(payload))
            except Exception as e:
                return commands, i, e
        return commands, len(commands), None

    def _build_command(self, payload: RedisPayload) -> _PlainCommand:
        """Reduce one payload to the redis-py call that carries it out.

        The unknown-op guard is TEMPORARY: ``RedisOp`` declares every command
        the design specifies before every renderer exists, and without the
        guard a payload naming an unrendered op would fall through silently
        rather than fail. It goes away once the table is complete.
        """
        renderer = _COMMAND_RENDERERS.get(payload.op)
        if renderer is None:
            raise ValueError(f'RedisSink cannot yet build op {payload.op.value!r}')
        key = self._config.key_prefix + payload.key
        method, args, kwargs = renderer(payload, key)
        return _PlainCommand(
            op=payload.op,
            label=f'{payload.op.value} key={key}',
            method=method,
            args=args,
            kwargs=kwargs,
        )

    async def _execute_batch(self, commands: list[_PlainCommand]) -> None:
        """Send every command through one pipeline, propagating any failure.

        A failure is NOT replayed. The original fallback caught every
        exception, discarded it, and re-sent the whole batch one command at
        a time — which hid real defects (including a broken test mock that
        meant this path was never exercised at all) and would double-apply
        any command that accumulates rather than replaces.

        Transient errors are handled one level up instead: ``deliver``
        remaps them to the builtins ``SinkManager`` recognises, so the
        manager's bounded fast-retry re-sends the batch when that is safe.
        """
        assert self._client is not None
        pipe = self._client.pipeline(transaction=False)
        for command in commands:
            await command.queue(pipe)

        # raise_on_error=False returns a list positionally aligned with the
        # queued commands, with per-command server errors present as
        # exception OBJECTS rather than raised. redis-py guarantees the
        # lengths match and disconnects if they do not. That alignment is
        # what lets the failing payload be named WITHOUT re-sending the ones
        # that succeeded — which is what makes a command that accumulates
        # (INCRBY, LPUSH) safe to batch at all.
        #
        # A connection-level failure still raises out of execute() instead
        # of returning a list. There we cannot know what was applied, so it
        # propagates with the whole batch and nothing is replayed.
        results = await pipe.execute(raise_on_error=False)

        # strict: a short result list would silently drop the failures at
        # the tail. redis-py guarantees one result per queued command, so a
        # mismatch is a broken client, and a loud error beats an unreported
        # write failure.
        failed = [(cmd, res) for cmd, res in zip(commands, results, strict=True) if isinstance(res, Exception)]
        if failed:
            command, error = failed[0]
            raise RedisCommandError(
                f'{len(failed)} of {len(commands)} Redis commands failed; first failure on {command.label}: {error}'
            ) from error

    async def _execute_single(self, command: _PlainCommand) -> None:
        """Run one command directly — the shape the pre-batching loop produced."""
        assert self._client is not None
        await command.execute(self._client)

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
