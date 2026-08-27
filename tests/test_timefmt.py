"""Tests for the canonical timestamp format.

``format_rfc3339_micro`` is the ONE formatter for framework-controlled
datetimes rendered to text (webapp report fields, recorder JSON
metadata). The same instant must always render to the same bytes, so
these tests pin exact strings, not shapes.
"""

from datetime import UTC, datetime, timedelta, timezone

from drakkar.timefmt import format_rfc3339_micro


def test_utc_datetime_renders_fixed_width_z_suffix():
    dt = datetime(2026, 7, 5, 12, 34, 56, 123456, tzinfo=UTC)
    assert format_rfc3339_micro(dt) == '2026-07-05T12:34:56.123456Z'


def test_whole_seconds_keep_six_digit_fraction():
    # isoformat() would drop the fraction here; the canonical format
    # never does — deterministic width is the contract.
    dt = datetime(2026, 7, 5, 12, 34, 56, tzinfo=UTC)
    assert format_rfc3339_micro(dt) == '2026-07-05T12:34:56.000000Z'


def test_non_utc_zone_converts_to_utc():
    plus_two = timezone(timedelta(hours=2))
    dt = datetime(2026, 7, 5, 14, 34, 56, 1, tzinfo=plus_two)
    assert format_rfc3339_micro(dt) == '2026-07-05T12:34:56.000001Z'


def test_naive_datetime_interpreted_as_utc():
    dt = datetime(2026, 7, 5, 12, 34, 56, 999999)
    assert format_rfc3339_micro(dt) == '2026-07-05T12:34:56.999999Z'


def test_reexported_from_package_root():
    import drakkar

    assert drakkar.format_rfc3339_micro is format_rfc3339_micro


def test_webapp_report_json_dump_uses_canonical_format():
    """WebReport timestamps hit the wire in the canonical format.

    ``model_dump(mode='json')`` is exactly what the webapp server sends;
    plain ``model_dump()`` must keep real datetimes for in-process use.
    """
    from drakkar.webapp import WebReport

    report = WebReport(
        request_id='req_1',
        client='anonymous',
        started_at=datetime(2026, 7, 5, 12, 0, 0, 250000, tzinfo=UTC),
        finished_at=datetime(2026, 7, 5, 12, 0, 1, tzinfo=UTC),
        duration_ms=750.0,
        status='ok',
    )
    dumped = report.model_dump(mode='json')
    assert dumped['started_at'] == '2026-07-05T12:00:00.250000Z'
    assert dumped['finished_at'] == '2026-07-05T12:00:01.000000Z'
    assert isinstance(report.model_dump()['started_at'], datetime)
