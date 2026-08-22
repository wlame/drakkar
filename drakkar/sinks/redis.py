"""Redis sink — one write command per payload, or a named Lua script.

Wraps redis.asyncio.Redis. A RedisPayload's ``op`` selects the command:
one write verb per data type (SET, DEL, EXPIRE, INCRBY, HSET, HDEL,
LPUSH/RPUSH, LTRIM, SADD, SREM, ZADD), or ``script`` to run Lua the
operator authored in configuration, invoked by name with KEYS and ARGV
bound rather than interpolated.

Every key is namespaced with ``config.key_prefix``, including every entry
of a script's ``keys``. One delivery is one pipeline, and a per-command
failure is attributed positionally — nothing is ever re-sent, which is
what makes a command that accumulates (INCRBY, LPUSH) safe to batch.
"""

import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

import structlog

from drakkar.config import RedisSinkConfig
from drakkar.metrics import sink_deliver_duration, sink_deliver_errors, sink_payloads_delivered
from drakkar.models import RedisOp, RedisPayload
from drakkar.sinks.base import BaseSink
from drakkar.utils import redact_url

if TYPE_CHECKING:
    # Type-only: the runtime import stays inside connect(), so importing
    # drakkar does not pull in redis for workers that never use this sink.
    import redis.asyncio

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


# redis-py's AsyncScript: awaitable, called with keys/args/client.
_RegisteredScript = Callable[..., Awaitable[object]]


@dataclass(frozen=True)
class _ScriptCommand:
    """One payload reduced to an invocation of a registered Lua script.

    Same two methods as ``_PlainCommand``, so the delivery paths do not know
    which kind they are driving.
    """

    op: RedisOp
    label: str
    """Identification for error messages — the script NAME, never its body,
    which an operator may have written around row data."""
    script: _RegisteredScript
    """The ``AsyncScript`` returned by ``register_script``."""
    keys: list[str]
    args: list[object]

    async def queue(self, pipe: object) -> None:
        """Add this script invocation to a pipeline.

        Genuinely awaited, unlike the plain-command version: this call is
        ``async def`` in redis-py and must be awaited even to queue. It
        works because ``Pipeline`` defines ``__await__``, so the script's
        internal ``await client.evalsha(...)`` resolves to the pipeline
        itself. The script also detects the Pipeline and registers itself,
        so ``Pipeline.execute()`` runs SCRIPT EXISTS / SCRIPT LOAD first —
        NOSCRIPT recovery is redis-py's job, not ours.
        """
        await self.script(keys=self.keys, args=self.args, client=pipe)

    async def execute(self, client: object) -> None:
        """Run this script directly, outside a pipeline."""
        await self.script(keys=self.keys, args=self.args, client=client)


# Either kind of command. Both expose queue() and execute().
_Command = _PlainCommand | _ScriptCommand

# One rendered redis-py call: method name, positional args, keyword args.
_Rendered = tuple[str, tuple[object, ...], dict[str, object]]
# A renderer receives the payload and its ALREADY-PREFIXED key.
_CommandRenderer = Callable[[RedisPayload, str], _Rendered]


def _sorted_mapping(value: object) -> dict[str, object]:
    """Emit a payload MAPPING in sorted key order.

    Argument order does not change what HSET or ZADD leave behind, but it
    does change the emitted command — and the Go backend decodes these into
    a map with no order to preserve, so sorting is the only rule both
    backends can honour unconditionally. Postgres columns are sorted for
    exactly this reason.

    LISTS (hdel fields, sadd/srem members) are NOT sorted: those are the
    caller's own order, like an explicit `update_columns` on the Postgres
    side, and both backends can preserve a sequence.
    """
    # The cast states an invariant the annotation cannot: ``value`` is typed
    # ``object`` because every renderer takes the same payload, but the
    # per-op field contract has already narrowed a mapping op's collection to
    # ``dict[str, ...]``. Without it, ``sorted`` sees keys of type ``object``
    # and cannot know they are comparable.
    mapping = cast(dict[str, object], value) if isinstance(value, dict) else {}
    return {name: mapping[name] for name in sorted(mapping)}


def _render_set(payload: RedisPayload, key: str) -> _Rendered:
    """SET pk <json> [EX ttl]."""
    # Explicit raise rather than assert: ``python -O`` strips asserts, which
    # would turn a broken per-op contract into a confusing AttributeError.
    if payload.data is None:
        raise RuntimeError(
            f'RedisPayload(op={payload.op.value!r}) reached the renderer without data — per-op contract broken'
        )
    kwargs: dict[str, object] = {'ex': payload.ttl} if payload.ttl is not None else {}
    return 'set', (key, payload.data.model_dump_json()), kwargs


def _render_push(payload: RedisPayload, key: str) -> _Rendered:
    """LPUSH/RPUSH pk <json> — one list element is one serialized object."""
    if payload.data is None:
        raise RuntimeError(
            f'RedisPayload(op={payload.op.value!r}) reached the renderer without data — per-op contract broken'
        )
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
    # A mapping so several fields go in one HSET. Values pass through as
    # str/int/float — redis-py's encoder handles them, and stringifying here
    # would silently change what is stored.
    #
    # dict()/unpacking are safe because the per-op contract already narrowed
    # the shape: hset and zadd require a mapping, hdel and sadd/srem a list.
    RedisOp.HSET: lambda p, key: ('hset', (key,), {'mapping': _sorted_mapping(p.fields)}),
    RedisOp.HDEL: lambda p, key: ('hdel', (key, *(p.fields or ())), {}),
    RedisOp.SADD: lambda p, key: ('sadd', (key, *(p.members or ())), {}),
    RedisOp.SREM: lambda p, key: ('srem', (key, *(p.members or ())), {}),
    # ZADD's mapping stays keyed by MEMBER. That is redis-py's own
    # zadd(name, mapping) signature, and it emits `ZADD key score member`
    # itself — the argument flip is the client's job, not ours. A backend
    # whose client takes (score, member) pairs has to flip explicitly.
    RedisOp.ZADD: lambda p, key: ('zadd', (key, _sorted_mapping(p.members)), {}),
}


# Ops a retry could apply twice with a different end state. Everything else
# in RedisOp converges, which is why the class-level flag stays True.
_NOT_IDEMPOTENT_OPS = frozenset({RedisOp.INCRBY, RedisOp.PUSH, RedisOp.SCRIPT})


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
    """Issues one Redis write command per payload.

    ``RedisPayload.op`` selects the command and defaults to ``SET``. Keys
    are always namespaced with ``config.key_prefix``. Operator-authored Lua
    declared under ``sinks.redis.<instance>.scripts`` is registered at
    connect() and invoked by name.
    """

    sink_type = 'redis'

    # Most Redis writes are write-replace on a fixed key or a removal, so
    # re-executing one produces the same post-state however many times it
    # ran. That makes the SET-shaped ops safe to retry on transient errors
    # (connection reset mid-command, timeout), which is what this flag opts
    # into. The real decision is per batch — see ``batch_idempotent`` — and
    # this stays as the value for a batch that contains only convergent ops.
    #
    # TTLs ride along on the SET, which means a retried ``SET … EX 3600``
    # DOES restart the expiry window. That is accepted: the drift is
    # bounded by the fast-retry's backoff (hundreds of milliseconds). The
    # alternative — an absolute ``EXAT`` deadline computed here — would
    # converge exactly but would take the timestamp from the worker's
    # clock, so worker/server skew would shift real expiry times. Clock
    # skew is the worse operational hazard.
    idempotent = True

    def batch_idempotent(self, payloads: list[RedisPayload]) -> bool:
        """Retry-safe unless the batch contains a command that accumulates.

        ``SET``/``HSET`` replace, ``DELETE``/``HDEL``/``SREM`` converge on
        removal, ``SADD`` is a no-op for a member already present, ``ZADD``
        sets the score rather than incrementing it, and ``TRIM`` to a fixed
        range converges. ``INCRBY`` and ``PUSH`` accumulate, and operator
        Lua is opaque to the framework — exactly like a Postgres named
        statement.

        Marking individual scripts idempotent in configuration is a natural
        extension, deliberately left out for now.
        """
        return not any(payload.op in _NOT_IDEMPOTENT_OPS for payload in payloads)

    def __init__(self, name: str, config: RedisSinkConfig) -> None:
        super().__init__(name, ui_url=config.ui_url)
        self._config = config
        self._client: redis.asyncio.Redis | None = None
        # Operator-authored Lua, registered at connect(). Never registered on
        # the delivery path.
        self._scripts: dict[str, _RegisteredScript] = {}

    @property
    def client(self) -> 'redis.asyncio.Redis | None':
        """The ``redis.asyncio`` client, available after connect().

        Mirrors ``PostgresSink.pool``, and closes the asymmetry between the
        two sinks.

        **Reachability, stated plainly:** a handler cannot get here today.
        Handlers never receive sink instances — ``SinkManager.sinks`` lives
        on the manager, and ``on_ready(config, db_pool)`` is handed only the
        Postgres pool. So this is reachable only from a plugin sink
        subclass. Giving handlers general access to sink clients affects
        every sink type and is a separate design, deliberately not settled
        as a side effect of the Redis command work.
        """
        return self._client

    async def connect(self) -> None:
        """Create the Redis client from the configured URL."""
        import redis.asyncio as aioredis

        self._client = aioredis.from_url(self._config.url)
        # register_script computes the SHA1 LOCALLY with no round trip, so
        # this stays cheap and does not fail when Redis is briefly away. The
        # script is not sent until first use, and redis-py handles the
        # SCRIPT LOAD then.
        self._scripts = {name: self._client.register_script(body) for name, body in self._config.scripts.items()}
        await logger.ainfo(
            'redis_sink_connected',
            category='sink',
            sink_name=self._name,
            url=redact_url(self._config.url),
            key_prefix=self._config.key_prefix,
            scripts=len(self._scripts),
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
        if not payloads:
            return
        # ``deliver`` must raise on failure (BaseSink contract) — silently
        # returning here would let the offset commit past lost payloads.
        if self._client is None:
            raise RuntimeError(f'RedisSink {self._name!r} is not connected — call connect() before deliver()')

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

    def _build_items(self, payloads: list[RedisPayload]) -> tuple[list[_Command], int, Exception | None]:
        """Build a command for every payload up front.

        On the first bad payload returns the commands built so far, the
        failing index, and the error — the caller replays the legacy partial
        side effects before raising it.
        """
        commands: list[_Command] = []
        for i, payload in enumerate(payloads):
            try:
                commands.append(self._build_command(payload))
            except Exception as e:
                return commands, i, e
        return commands, len(commands), None

    def _build_command(self, payload: RedisPayload) -> _Command:
        """Reduce one payload to the redis-py call that carries it out.

        A script is looked up by name; everything else goes through the
        renderer table.
        """
        if payload.op is RedisOp.SCRIPT:
            return self._build_script_command(payload)
        renderer = _COMMAND_RENDERERS[payload.op]
        key = self._config.key_prefix + payload.key
        method, args, kwargs = renderer(payload, key)
        return _PlainCommand(
            op=payload.op,
            label=f'{payload.op.value} key={key}',
            method=method,
            args=args,
            kwargs=kwargs,
        )

    def _build_script_command(self, payload: RedisPayload) -> _ScriptCommand:
        """Look up an operator-authored script and bind its keys and args.

        EVERY entry of ``keys`` is prefixed, not just the single-key ops'
        ``key``: the prefix is this sink instance's namespace, and a script
        given raw keys could write outside it.
        """
        script = self._scripts.get(payload.script)
        if script is None:
            known = ', '.join(sorted(self._scripts)) or '<none configured>'
            raise ValueError(f'unknown redis script {payload.script!r} on sink {self._name!r}; configured: {known}')
        keys = [self._config.key_prefix + key for key in payload.keys]
        return _ScriptCommand(
            op=payload.op,
            label=f'script={payload.script} keys={keys}',
            script=script,
            keys=keys,
            args=list(payload.args),
        )

    async def _execute_batch(self, commands: list[_Command]) -> None:
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
        # Explicit raise rather than assert — asserts vanish under ``python -O``
        # (see the matching note in BaseSink.should_skip_delivery).
        if self._client is None:
            raise RuntimeError(f'RedisSink {self._name!r} has no client — internal invariant broken')
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

    async def _execute_single(self, command: _Command) -> None:
        """Run one command directly — the shape the pre-batching loop produced."""
        if self._client is None:
            raise RuntimeError(f'RedisSink {self._name!r} has no client — internal invariant broken')
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
