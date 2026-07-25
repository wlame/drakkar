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

from drakkar.timefmt import format_rfc3339_micro
from drakkar.utils import redact_url

# Fast JSON encoder for the recorder hot path. orjson is an optional
# dependency (``pip install drakkar[perf]``) — when available, SQLite
# payload encoding (args / metadata / labels) uses it for a ~2-4x speedup
# over ``json.dumps``. When orjson is not installed we transparently
# fall back to stdlib ``json`` so the recorder keeps working with the
# same on-wire semantics.
#
# Contract:
# - ``encode_json(obj)`` returns BYTES (UTF-8). The low-level primitive
#   — callers that want str use ``encode_json_str(obj)`` which decodes
#   once on the way out.
# - The recorder stores TEXT columns in SQLite, which requires ``str``
#   on insert; those sites use ``encode_json_str``.
# - Keys are SORTED so repeated encodes of the same dict produce
#   identical output (deterministic hashes / cache dedup downstream).
# - Datetimes render via :func:`drakkar.timefmt.format_rfc3339_micro`
#   in BOTH paths (fixed six-digit microseconds, ``Z`` suffix) — the
#   canonical cross-backend format, byte-identical to the Go backend.
# - Other non-JSON-native types fall back to ``str()``.


def _json_default(obj: Any) -> Any:
    """Fallback serializer shared by both encoder paths.

    Datetimes render in the canonical cross-backend timestamp format;
    anything else JSON can't represent degrades to ``str()``.
    """
    if isinstance(obj, datetime):
        return format_rfc3339_micro(obj)
    return str(obj)


try:
    import orjson  # ty: ignore[unresolved-import]

    _HAS_ORJSON = True

    def encode_json(obj: Any) -> bytes:
        """Encode ``obj`` as UTF-8 JSON bytes via orjson.

        Options:
        - ``OPT_SORT_KEYS``: deterministic output regardless of insertion order.
        - ``OPT_PASSTHROUGH_DATETIME``: orjson hands datetimes to
          ``default`` instead of rendering them natively, so both encoder
          paths emit the one canonical timestamp format. orjson's native
          rendering omits the fraction for whole seconds — neither
          deterministic-width nor what the Go backend writes.
        - ``OPT_NON_STR_KEYS``: coerce non-string dict keys (rare but safe).
        """
        return orjson.dumps(
            obj,
            option=orjson.OPT_SORT_KEYS | orjson.OPT_PASSTHROUGH_DATETIME | orjson.OPT_NON_STR_KEYS,
            default=_json_default,
        )
except ImportError:  # pragma: no cover - exercised via monkeypatch in tests
    _HAS_ORJSON = False

    def encode_json(obj: Any) -> bytes:
        """Stdlib fallback encoder (used when orjson is not installed).

        Matches orjson byte-for-byte on common payloads:
        - ``separators=(',', ':')`` → compact layout (no spaces).
        - ``ensure_ascii=False`` → emit UTF-8 directly instead of
          escaping non-ASCII (e.g. ``"naïve"`` stays as-is, not
          ``"na\\u00efve"``). orjson always writes raw UTF-8.
        - ``sort_keys=True`` → deterministic key order.
        - ``default=_json_default`` → the same fallback hook the orjson
          path uses (canonical datetimes, ``str()`` for the rest).

        These choices keep the on-disk recorder DB stable regardless of
        which path (orjson vs. stdlib) produced the bytes, so swapping
        the ``perf`` extra on/off does not change stored content.
        """
        return json.dumps(obj, sort_keys=True, default=_json_default, separators=(',', ':'), ensure_ascii=False).encode(
            'utf-8'
        )


def encode_json_str(obj: Any) -> str:
    """Encode ``obj`` as a JSON string (UTF-8 text).

    Thin wrapper over :func:`encode_json` that decodes the bytes once so
    the string-typed SQLite insert sites can use it transparently.
    """
    return encode_json(obj).decode('utf-8')


# Env var name patterns whose values get redacted before being written to the
# recorder SQLite file. Applied case-insensitively. The recorder DB can be
# downloaded via the debug UI, so writing raw secrets would effectively
# publish them — this filter is the last line of defence.
#
# Deliberately BROADER than ExecutorConfig.env_inherit_deny: over-matching
# here costs an operator one ``***`` and nothing else, whereas over-matching
# in the inheritance list withholds a variable the user's binary may need.
# ``*KEY*`` therefore also matches innocent names like MONKEY_PATCH_ENABLED —
# accepted by design.
_SECRET_ENV_PATTERNS = (
    '*PASSWORD*',
    '*PASSWD*',
    '*SECRET*',
    '*TOKEN*',
    '*KEY*',
    '*API_KEY*',
    '*CREDENTIAL*',
    '*_DSN',
    '*AUTH*',
    '*PRIVATE*',
    '*CERT*',
    '*SALT*',
)


def sanitize_env_value(name: str, value: str) -> str:
    """Return a safe-to-store version of an env var value.

    Redacts fully when the var name matches a common-secret pattern. For
    other values, strips embedded credentials from URL-shaped strings
    (handles DSNs, HTTP-with-basic-auth, Kafka SASL_SSL, etc.).
    """
    name_upper = name.upper()
    if any(fnmatch.fnmatchcase(name_upper, p.upper()) for p in _SECRET_ENV_PATTERNS):
        return '***' if value else ''
    return redact_url(value)


def format_dt(ts: float) -> str:
    """Format a Unix timestamp as 'YYYY-MM-DD HH:MM:SS.mmm'."""
    dt = datetime.fromtimestamp(ts, tz=UTC)
    return dt.strftime('%Y-%m-%d %H:%M:%S.') + f'{dt.microsecond // 1000:03d}'


def make_db_path(db_dir: str, worker_name: str) -> str:
    """Generate a timestamped DB filename inside db_dir.

    ('/shared', 'worker-1') -> '/shared/worker-1-2026-03-16__14_55_00.db'
    """
    ts = datetime.now(tz=UTC).strftime('%Y-%m-%d__%H_%M_%S')
    return str(Path(db_dir) / f'{worker_name}-{ts}.db')


def live_link_path(db_dir: str, worker_name: str) -> str:
    """Path for the live symlink: {db_dir}/{worker_name}-live.db."""
    return str(Path(db_dir) / f'{worker_name}-live.db')


def list_db_files(db_dir: str, worker_name: str) -> list[str]:
    """List all timestamped DB files for a worker, oldest first.

    Excludes the -live.db symlink.
    """
    pattern = str(Path(db_dir) / f'{worker_name}-*.db')
    live = live_link_path(db_dir, worker_name)
    files = [f for f in glob.glob(pattern) if f != live and not os.path.islink(f)]
    files.sort()
    return files


# Canonical recorder busy_timeout, in milliseconds — set explicitly on
# every recorder connection (writer, own reader, peer reads) on BOTH
# backends so cross-process WAL contention in a shared db_dir behaves
# identically no matter which backend opened the file. The cache uses
# its own values (drakkar/cache/sql.py). See docs/local-databases.md.
BUSY_TIMEOUT_MS = 5000


async def open_reader(db_path: str) -> aiosqlite.Connection:
    """Open a read-only aiosqlite connection to ``db_path``.

    Uses the ``file:...?mode=ro`` SQLite URI form so any write attempt
    through this handle fails fast with an SQLite error. Each aiosqlite
    connection spawns its own worker thread, which is the property that
    lets debug-UI SELECTs run in parallel with writer flushes/commits.
    """
    db = await aiosqlite.connect(f'file:{db_path}?mode=ro', uri=True)
    await db.execute(f'PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}')
    return db


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
