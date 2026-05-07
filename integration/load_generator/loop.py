"""Standalone HTTP load generator for the Drakkar webapp integration scenario.

Periodically POSTs ``RankRequest``-shaped JSON bodies to the webapp endpoint
exposed by ``worker-1`` (default), demonstrating end-to-end synchronous HTTP
processing through the Drakkar pipeline.

Configurable via environment variables:

- ``WORKER_HOST``       — target host (default ``worker-1``)
- ``WORKER_PORT``       — target port (default ``8091`` to match the integration convention of debug-UI port + 10; the framework default outside the integration cluster is ``8090``)
- ``WORKER_PATH``       — webapp path (default ``/process``)
- ``CLIENT_TOKEN``      — bearer token; empty matches the anonymous client
- ``INTERVAL_SECONDS``  — sleep between requests (default ``10``)
- ``RANK_SCORE_MIN``    — lower bound for the random ``score`` field (default ``1``)
- ``RANK_SCORE_MAX``    — upper bound for the random ``score`` field (default ``100``)

No drakkar dependency — this script lives in its own container and only needs
``httpx`` to talk to the webapp. It logs every request/response to stdout and
swallows connection errors so a brief worker restart does not kill the loop.
"""

from __future__ import annotations

import os
import random
import signal
import sys
import time
import uuid
from types import FrameType

import httpx

# Module-level shutdown flag toggled by the SIGTERM handler. The main loop
# checks it after every sleep so the container exits within ``INTERVAL_SECONDS``
# of receiving the signal — fast enough for ``docker compose down`` to feel
# responsive without breaking an in-flight request.
_SHUTDOWN = False


def _on_signal(signum: int, frame: FrameType | None) -> None:
    """Flip the shutdown flag on SIGTERM/SIGINT and log the trigger."""

    global _SHUTDOWN
    _SHUTDOWN = True
    print(f'[load_generator] received signal {signum}, shutting down', flush=True)


def _env_str(name: str, default: str) -> str:
    value = os.environ.get(name, default)
    return value if value else default


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == '':
        return default
    try:
        return int(raw)
    except ValueError:
        print(f'[load_generator] invalid int for {name}={raw!r}, using default {default}', flush=True)
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw == '':
        return default
    try:
        return float(raw)
    except ValueError:
        print(f'[load_generator] invalid float for {name}={raw!r}, using default {default}', flush=True)
        return default


def _build_payload(score_min: int, score_max: int) -> dict[str, object]:
    """Build a single ``RankRequest``-shaped payload.

    The shape mirrors ``integration/worker/handler.py`` ``RankRequest``:
    a ``request_id`` for traceability and a ``score`` integer the handler
    feeds into the executor binary. Kept small on purpose — the goal is to
    show the pipeline running, not to stress-test serialisation.
    """

    return {
        'request_id': f'lg-{uuid.uuid4().hex[:12]}',
        'score': random.randint(score_min, score_max),
    }


def main() -> int:
    # Wire signal handlers BEFORE the loop so the very first iteration honours
    # SIGTERM. SIGINT is wired too so Ctrl+C in ``docker compose up`` (without
    # ``-d``) terminates cleanly.
    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)

    host = _env_str('WORKER_HOST', 'worker-1')
    port = _env_int('WORKER_PORT', 8091)
    path = _env_str('WORKER_PATH', '/process')
    if not path.startswith('/'):
        path = '/' + path
    token = os.environ.get('CLIENT_TOKEN', '')
    interval = _env_float('INTERVAL_SECONDS', 10.0)
    score_min = _env_int('RANK_SCORE_MIN', 1)
    score_max = _env_int('RANK_SCORE_MAX', 100)
    if score_max < score_min:
        score_max = score_min

    url = f'http://{host}:{port}{path}'
    headers: dict[str, str] = {'content-type': 'application/json'}
    if token:
        headers['authorization'] = f'Bearer {token}'

    client_label = 'tenant' if token else 'anonymous'
    print(
        f'[load_generator] starting target={url} client={client_label} '
        f'interval={interval}s score_range={score_min}..{score_max}',
        flush=True,
    )

    # ``httpx.Client`` reuses a connection across iterations, which makes the
    # webapp's per-request keep-alive behaviour visible in the recorder. A
    # short timeout matches the demo intent: we'd rather log a 504-like local
    # failure and try again than hang the loop while the worker is down.
    with httpx.Client(timeout=httpx.Timeout(connect=5.0, read=15.0, write=5.0, pool=5.0)) as client:
        request_seq = 0
        while not _SHUTDOWN:
            request_seq += 1
            payload = _build_payload(score_min=score_min, score_max=score_max)
            started = time.monotonic()
            status: int | None = None
            err: str | None = None
            response_body_preview: str = ''
            try:
                response = client.post(url, json=payload, headers=headers)
                status = response.status_code
                # Truncate the response body to keep stdout compact even when
                # the webapp returns the full ``WebReport`` envelope.
                body_text = response.text or ''
                response_body_preview = body_text[:200].replace('\n', ' ')
            except httpx.RequestError as exc:
                err = f'{type(exc).__name__}: {exc}'
            elapsed = time.monotonic() - started

            if err is not None:
                print(
                    f'[load_generator] #{request_seq} request_id={payload["request_id"]} '
                    f'score={payload["score"]} status=ERR err={err} elapsed={elapsed:.2f}s',
                    flush=True,
                )
            else:
                print(
                    f'[load_generator] #{request_seq} request_id={payload["request_id"]} '
                    f'score={payload["score"]} status={status} elapsed={elapsed:.2f}s '
                    f'body={response_body_preview!r}',
                    flush=True,
                )

            # Sleep in small slices so SIGTERM is observed within ~250ms even
            # when ``INTERVAL_SECONDS`` is large. ``time.sleep`` is interrupted
            # by signals on Linux, but slicing keeps behaviour predictable
            # across platforms and avoids a lingering iteration.
            slept = 0.0
            slice_size = 0.25
            while not _SHUTDOWN and slept < interval:
                time.sleep(min(slice_size, interval - slept))
                slept += slice_size

    print('[load_generator] exiting cleanly', flush=True)
    return 0


if __name__ == '__main__':
    sys.exit(main())
