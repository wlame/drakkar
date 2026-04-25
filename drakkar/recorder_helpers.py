"""Standalone helpers for the flight recorder.

Holds the JSON encoder fast path (orjson with stdlib fallback), the
secret-redaction logic for env-var values, the wall-clock formatters, the
DB-file path helpers, the read-only connection opener, and the worker
IP detection routine.

Everything in here is functional and free of recorder runtime state, so
tests can exercise the encoders / sanitizers / path helpers without
constructing an :class:`EventRecorder`.
"""

from __future__ import annotations

import contextlib
import fnmatch
import glob
import json
import os
import socket
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import aiosqlite

from drakkar.utils import redact_url

# Fast JSON encoder for the recorder hot path. orjson is an optional
# dependency (``pip install drakkar[perf]``) — when available, SQLite
# payload encoding (args / metadata / labels) uses it for a ~2-4x speedup
# over ``json.dumps``. When orjson is not installed we transparently
# fall back to stdlib ``json`` so the recorder keeps working with the
# same on-wire semantics.
#
# Contract:
# - ``_encode_json(obj)`` returns BYTES (UTF-8). The low-level primitive
#   — callers that want str use ``_encode_json_str(obj)`` which decodes
#   once on the way out.
# - The recorder stores TEXT columns in SQLite, which requires ``str``
#   on insert; those sites use ``_encode_json_str``.
# - Keys are SORTED so repeated encodes of the same dict produce
#   identical output (deterministic hashes / cache dedup downstream).
# - Non-JSON-native types fall back to ``default=str`` / orjson's
#   built-in ``datetime`` + ``UUID`` handlers.
# - Datetimes tagged with ``tzinfo=UTC`` end with "Z" in both paths
#   (matches the existing ``isoformat().replace("+00:00", "Z")``
#   convention used elsewhere in the code base).
try:
    import orjson

    _HAS_ORJSON = True

    def _encode_json(obj: Any) -> bytes:
        """Encode ``obj`` as UTF-8 JSON bytes via orjson.

        Options:
        - ``OPT_SORT_KEYS``: deterministic output regardless of insertion order.
        - ``OPT_UTC_Z``: UTC datetimes serialize as ``...Z`` (not ``+00:00``).
        - ``OPT_NON_STR_KEYS``: coerce non-string dict keys (rare but safe).
        The ``default=str`` hook catches anything orjson can't serialize
        natively (custom classes etc.), matching the stdlib fallback.
        """
        return orjson.dumps(
            obj,
            option=orjson.OPT_SORT_KEYS | orjson.OPT_UTC_Z | orjson.OPT_NON_STR_KEYS,
            default=str,
        )
except ImportError:  # pragma: no cover - exercised via monkeypatch in tests
    _HAS_ORJSON = False

    def _encode_json(obj: Any) -> bytes:
        """Stdlib fallback encoder (used when orjson is not installed).

        Matches orjson byte-for-byte on common payloads:
        - ``separators=(',', ':')`` → compact layout (no spaces).
        - ``ensure_ascii=False`` → emit UTF-8 directly instead of
          escaping non-ASCII (e.g. ``"naïve"`` stays as-is, not
          ``"na\\u00efve"``). orjson always writes raw UTF-8.
        - ``sort_keys=True`` → deterministic key order.
        - ``default=str`` → fallback coercion for non-native types;
          catches anything orjson's ``default=str`` would also catch.

        These choices keep the on-disk recorder DB stable regardless of
        which path (orjson vs. stdlib) produced the bytes, so swapping
        the ``perf`` extra on/off does not change stored content.
        """
        return json.dumps(obj, sort_keys=True, default=str, separators=(',', ':'), ensure_ascii=False).encode('utf-8')


def _encode_json_str(obj: Any) -> str:
    """Encode ``obj`` as a JSON string (UTF-8 text).

    Thin wrapper over :func:`_encode_json` that decodes the bytes once so
    the string-typed SQLite insert sites can use it transparently.
    """
    return _encode_json(obj).decode('utf-8')


# Env var name patterns whose values get redacted before being written to the
# recorder SQLite file. Applied case-insensitively. The recorder DB can be
# downloaded via the debug UI, so writing raw secrets would effectively
# publish them — this filter is the last line of defence.
_SECRET_ENV_PATTERNS = (
    '*PASSWORD*',
    '*SECRET*',
    '*TOKEN*',
    '*_KEY',
    '*API_KEY*',
    '*CREDENTIAL*',
    '*_DSN',
)


def _sanitize_env_value(name: str, value: str) -> str:
    """Return a safe-to-store version of an env var value.

    Redacts fully when the var name matches a common-secret pattern. For
    other values, strips embedded credentials from URL-shaped strings
    (handles DSNs, HTTP-with-basic-auth, Kafka SASL_SSL, etc.).
    """
    name_upper = name.upper()
    if any(fnmatch.fnmatchcase(name_upper, p.upper()) for p in _SECRET_ENV_PATTERNS):
        return '***' if value else ''
    return redact_url(value)


def _format_dt(ts: float) -> str:
    """Format a Unix timestamp as 'YYYY-MM-DD HH:MM:SS.mmm'."""
    dt = datetime.fromtimestamp(ts, tz=UTC)
    return dt.strftime('%Y-%m-%d %H:%M:%S.') + f'{dt.microsecond // 1000:03d}'


def _make_db_path(db_dir: str, worker_name: str) -> str:
    """Generate a timestamped DB filename inside db_dir.

    ('/shared', 'worker-1') -> '/shared/worker-1-2026-03-16__14_55_00.db'
    """
    ts = datetime.now(tz=UTC).strftime('%Y-%m-%d__%H_%M_%S')
    return str(Path(db_dir) / f'{worker_name}-{ts}.db')


def _live_link_path(db_dir: str, worker_name: str) -> str:
    """Path for the live symlink: {db_dir}/{worker_name}-live.db."""
    return str(Path(db_dir) / f'{worker_name}-live.db')


def _list_db_files(db_dir: str, worker_name: str) -> list[str]:
    """List all timestamped DB files for a worker, oldest first.

    Excludes the -live.db symlink.
    """
    pattern = str(Path(db_dir) / f'{worker_name}-*.db')
    live = _live_link_path(db_dir, worker_name)
    files = [f for f in glob.glob(pattern) if f != live and not os.path.islink(f)]
    files.sort()
    return files


async def _open_reader(db_path: str) -> aiosqlite.Connection:
    """Open a read-only aiosqlite connection to ``db_path``.

    Uses the ``file:...?mode=ro`` SQLite URI form so any write attempt
    through this handle fails fast with an SQLite error. Each aiosqlite
    connection spawns its own worker thread, which is the property that
    lets debug-UI SELECTs run in parallel with writer flushes/commits.
    """
    return await aiosqlite.connect(f'file:{db_path}?mode=ro', uri=True)


def detect_worker_ip() -> str:
    """Detect the worker's outbound IP address.

    Uses ``contextlib.closing`` so the UDP socket is always closed, even
    if ``getsockname()`` raises. Without the wrapper an exception after
    ``connect()`` would leak the file descriptor; this function is called
    on every DB rotation so the leak would accumulate over days.
    """
    try:
        with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_DGRAM)) as s:
            s.connect(('10.255.255.255', 1))
            return s.getsockname()[0]
    except Exception:
        return '127.0.0.1'
