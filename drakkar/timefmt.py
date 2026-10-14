"""The one canonical timestamp string format.

Every framework-controlled datetime that is rendered to text — webapp
report fields, recorder JSON metadata, ad-hoc datetimes inside recorder
payload columns — uses ONE format, so the same instant always renders to
the same bytes whoever wrote it (see docs/local-databases.md):

    2026-07-05T12:34:56.123456Z

UTC, RFC 3339, fixed six-digit microseconds, ``Z`` suffix. Microseconds
are Python's maximum real precision, and the fixed width keeps output
deterministic — ``isoformat()`` omits the fraction for whole seconds, and
the common RFC 3339 renderers trim trailing zeros, so neither is
byte-stable.

The recorder's ``dt``/``created_at_dt`` SQLite columns keep their own
display format (``YYYY-MM-DD HH:MM:SS.mmm``) — this module governs
datetimes embedded in JSON.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta


def format_rfc3339_micro(dt: datetime) -> str:
    """Render ``dt`` in the canonical timestamp format.

    Naive datetimes are interpreted as UTC (the framework only produces
    aware UTC datetimes; the fallback keeps user-supplied values safe);
    aware ones are converted.
    """
    aware = dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)
    return aware.strftime('%Y-%m-%dT%H:%M:%S.%f') + 'Z'


_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def format_epoch_micro(ts: float) -> str:
    """Render epoch seconds in the canonical timestamp format.

    The microsecond is ``round(ts * 1_000_000)`` — round half to even on the
    IEEE double product — so the same float renders to the same text in any
    implementation that does the same arithmetic. ``datetime.fromtimestamp``
    is not used: its own rounding is an implementation detail.
    """
    return format_rfc3339_micro(_EPOCH + timedelta(microseconds=round(ts * 1_000_000)))


def format_epoch_ns(ns: int) -> str:
    """Render an integer nanosecond epoch time (a file mtime) in the canonical timestamp format.

    Truncated to the microsecond with integer arithmetic, so it is exact:
    no float rounding, the same text in any implementation.
    """
    return format_rfc3339_micro(_EPOCH + timedelta(microseconds=ns // 1000))
