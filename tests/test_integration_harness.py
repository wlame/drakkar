"""The docker harness must keep speaking the API the backend actually serves.

The harness is not exercised by the unit suite and only runs on a schedule,
so an endpoint removal can leave it calling a dead path for weeks. It fails
quietly when that happens: every curl in ``chaos-test.sh`` ends in
``|| echo "0"`` or ``|| continue``, so a 404 reads as "this worker completed
zero tasks" rather than as an error, and the whole chaos scenario reports
plausible-looking nonsense.

These cases pin the harness against the vendored OpenAPI artifact — the same
one ``test_openapi_parity.py`` pins against the live route table — so a route
change fails here in the unit suite instead of silently in a nightly run.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
INTEGRATION = REPO_ROOT / 'integration'
OPENAPI = yaml.safe_load((REPO_ROOT / 'drakkar' / 'uiserver' / 'openapi.yaml').read_text())
SERVED_PATHS = frozenset(OPENAPI['paths'])

# Files that address the workers over HTTP.
HARNESS_FILES = ('chaos-test.sh',)

# ``/api/v1/debug/download/{filename}`` in the spec vs a concrete filename in
# a script: compare on the template by replacing each concrete segment.
PATH_PARAM_RE = re.compile(r'\{[^}]+\}')


def _spec_matches(path: str) -> bool:
    """True when ``path`` matches a served route, templated segments included."""
    if path in SERVED_PATHS:
        return True
    for served in SERVED_PATHS:
        if '{' not in served:
            continue
        pattern = '^' + PATH_PARAM_RE.sub('[^/]+', re.escape(served).replace('\\{', '{').replace('\\}', '}')) + '$'
        pattern = pattern.replace('\\{', '{').replace('\\}', '}')
        if re.match(pattern.replace('{', '').replace('}', ''), path):
            return True
    return False


def _referenced_api_paths(text: str) -> set[str]:
    """Every ``/api/...`` path a script addresses, shell variables stripped."""
    found = set()
    for match in re.finditer(r'/api/[A-Za-z0-9/_.\-{}$]*', text):
        path = match.group(0)
        # Drop a trailing shell expansion or quote artefact.
        path = re.sub(r'\$\{[^}]*\}', 'X', path).rstrip('/"\'')
        found.add(path)
    return found


@pytest.mark.parametrize('filename', HARNESS_FILES)
def test_harness_only_calls_paths_the_backend_serves(filename: str) -> None:
    path = INTEGRATION / filename
    assert path.is_file(), f'{filename} missing from integration/'
    referenced = _referenced_api_paths(path.read_text())
    assert referenced, f'{filename} addresses no API path — did the check stop working?'
    dead = sorted(p for p in referenced if not _spec_matches(p))
    assert not dead, (
        f'{filename} calls paths the backend does not serve: {dead}. '
        'Legacy unprefixed /api/* routes were removed in v1.19 — use /api/v1/...'
    )


@pytest.mark.parametrize('filename', HARNESS_FILES)
def test_harness_uses_no_unprefixed_api_paths(filename: str) -> None:
    """The narrower, more legible version of the rule above."""
    text = (INTEGRATION / filename).read_text()
    unprefixed = sorted({m.group(0) for m in re.finditer(r'/api/(?!v1/)[a-z0-9_\-]+', text)})
    assert not unprefixed, f'{filename} uses removed unprefixed paths: {unprefixed}'


def test_worker_images_do_not_install_removed_runtime_dependencies() -> None:
    """v1.19 dropped the Jinja templates; the images should not pull jinja2."""
    removed = ('jinja2',)
    offenders = []
    for dockerfile in sorted(INTEGRATION.glob('*/Dockerfile')):
        text = dockerfile.read_text().lower()
        for package in removed:
            if re.search(rf'\b{re.escape(package)}\b', text):
                offenders.append(f'{dockerfile.relative_to(REPO_ROOT)}: {package}')
    assert not offenders, 'images install dependencies the framework no longer uses: ' + ', '.join(offenders)


# Route modules whose docstring lists the endpoints they own. The listing is
# the first thing a reader trusts, and nothing else checks it.
ROUTE_MODULES = sorted((REPO_ROOT / 'drakkar' / 'uiserver').glob('routes_*.py'))


def _documented_paths(source: str) -> set[str]:
    """Every ``/...`` path named in the module docstring's bullet list."""
    docstring = re.match(r'\s*"""(.*?)"""', source, re.S)
    if not docstring:
        return set()
    return {m.group(1) for m in re.finditer(r'^\s*\*\s+``(/[^`]*)``', docstring.group(1), re.M)}


@pytest.mark.parametrize('module', ROUTE_MODULES, ids=lambda p: p.name)
def test_route_module_docstring_names_only_real_endpoints(module: Path) -> None:
    """A docstring must not advertise a route the backend stopped serving.

    v1.19 removed the server-rendered pages and every unprefixed ``/api/*``
    alias, but the module docstrings kept listing both — the exact drift
    that sent the chaos test to a dead endpoint.
    """
    documented = _documented_paths(module.read_text())
    if not documented:
        pytest.skip(f'{module.name} has no route listing')
    # ``/ws``, ``/healthz`` and ``/readyz`` are contractual unprefixed routes.
    unprefixed_ok = {'/healthz', '/readyz', '/ws'}
    dead = sorted(path for path in documented - unprefixed_ok if not _spec_matches(path.replace('/*', '/x')))
    assert not dead, f'{module.name} documents endpoints the backend does not serve: {dead}'


def test_side_effecting_endpoint_routes_are_served_v1_paths() -> None:
    """The operator warning must name endpoints where they actually are.

    ``SideEffectingEndpoint.route`` is display-only — nothing routes on it —
    which is exactly why both backends kept advertising
    ``POST /api/debug/probe`` long after the unprefixed aliases were removed
    in v1.19. An operator who acts on the warning has to find the endpoint.
    """
    from drakkar.app_security import SIDE_EFFECTING_ENDPOINTS

    for endpoint in SIDE_EFFECTING_ENDPOINTS:
        method, _, path = endpoint.route.partition(' ')
        assert method in {'GET', 'POST', 'PUT', 'DELETE'}, f'{endpoint.route!r} is not "<METHOD> <path>"'
        assert path.startswith('/api/v1/'), f'{endpoint.route!r} does not name a /api/v1 path'
        assert path in SERVED_PATHS, f'{endpoint.route!r} names a path the backend does not serve'


# --- delivery verification ------------------------------------------------
#
# integration/verify_delivery.py is what turns the harness from "the workers
# booted" into "every message reached its sinks, in order". It only runs in a
# scheduled job against live containers, so the parts that can be pinned
# without one are pinned here: the constants it shares with the producer and
# the handler, and the pass/fail logic itself.

sys.path.insert(0, str(INTEGRATION))
import verify_delivery as vd  # noqa: E402

PRODUCER = INTEGRATION / 'infra' / 'producer.py'
WORKER_HANDLER = INTEGRATION / 'worker' / 'handler.py'


def test_verifier_and_producer_agree_on_the_request_id_format() -> None:
    """Both sides build the same ids, or the verifier checks a set nobody sent.

    They cannot import each other — the producer runs inside a container
    built from its own context — so the format literal is duplicated and
    pinned here instead.
    """
    declared = f"REQUEST_ID_FORMAT = '{vd.REQUEST_ID_FORMAT}'"
    assert declared in PRODUCER.read_text(), (
        f'integration/infra/producer.py does not declare {declared} — the verifier would rebuild '
        f'a different id set than the producer sent, and every request would look lost'
    )


def test_notify_threshold_matches_the_handler() -> None:
    """The reordering check is only meaningful at the handler's own threshold.

    ``update_not_reordered`` looks for high-match rows still reading
    notified=false. Set the bar above what the handler ever notifies on and
    the check silently inspects an empty set forever.
    """
    assert f'aggregate.total_matches > {vd.NOTIFY_THRESHOLD}' in WORKER_HANDLER.read_text(), (
        f'integration/worker/handler.py no longer notifies above {vd.NOTIFY_THRESHOLD} matches; '
        f'update NOTIFY_THRESHOLD in verify_delivery.py to match, or the check tests nothing'
    )


def _summary(request_id: str, *, matches: int = 0, succeeded: int = 1, notified: bool = False) -> vd.SummaryRow:
    return vd.SummaryRow(
        request_id=request_id,
        total_matches=matches,
        succeeded_tasks=succeeded,
        notified=notified,
    )


def test_expected_request_ids_covers_the_full_produced_range() -> None:
    assert vd.expected_request_ids(3) == {'req-000001', 'req-000002', 'req-000003'}


def test_expected_request_ids_is_empty_for_no_messages() -> None:
    assert vd.expected_request_ids(0) == set()


def test_run_checks_passes_on_a_complete_in_order_delivery() -> None:
    summaries = [_summary('req-000001', matches=50, notified=True), _summary('req-000002', matches=3)]
    report = vd.run_checks(
        total_messages=2,
        summaries=summaries,
        result_ids={'req-000001', 'req-000002'},
        result_rows=4,
        max_duplication=10.0,
    )
    assert report.ok
    assert report.violations == []


def test_run_checks_reports_a_request_that_never_reached_the_sink() -> None:
    report = vd.run_checks(
        total_messages=2,
        summaries=[_summary('req-000001')],
        result_ids={'req-000001'},
        result_rows=1,
        max_duplication=10.0,
    )
    assert [v.check for v in report.violations] == ['no_loss']
    assert report.violations[0].sample == ('req-000002',)


def test_run_checks_reports_a_summary_nobody_produced() -> None:
    report = vd.run_checks(
        total_messages=1,
        summaries=[_summary('req-000001'), _summary('req-999999')],
        result_ids={'req-000001', 'req-999999'},
        result_rows=2,
        max_duplication=10.0,
    )
    assert [v.check for v in report.violations] == ['no_phantom']
    assert report.violations[0].sample == ('req-999999',)


def test_run_checks_reports_an_update_that_ran_before_its_upsert() -> None:
    """A high-match row still reading notified=false is the reordering signature."""
    reordered = _summary('req-000001', matches=vd.NOTIFY_THRESHOLD + 1, notified=False)
    report = vd.run_checks(
        total_messages=1,
        summaries=[reordered],
        result_ids={'req-000001'},
        result_rows=1,
        max_duplication=10.0,
    )
    assert [v.check for v in report.violations] == ['update_not_reordered']


def test_run_checks_accepts_a_low_match_request_that_was_never_notified() -> None:
    """Below the threshold the handler appends no UPDATE, so false is correct."""
    report = vd.run_checks(
        total_messages=1,
        summaries=[_summary('req-000001', matches=vd.NOTIFY_THRESHOLD, notified=False)],
        result_ids={'req-000001'},
        result_rows=1,
        max_duplication=10.0,
    )
    assert report.ok


def test_run_checks_reports_a_rollup_whose_task_rows_are_missing() -> None:
    report = vd.run_checks(
        total_messages=1,
        summaries=[_summary('req-000001', succeeded=2)],
        result_ids=set(),
        result_rows=0,
        max_duplication=10.0,
    )
    assert 'task_rows_present' in [v.check for v in report.violations]


def test_run_checks_accepts_a_request_whose_every_task_failed() -> None:
    """No successful task means no per-task row is owed."""
    report = vd.run_checks(
        total_messages=1,
        summaries=[_summary('req-000001', succeeded=0)],
        result_ids=set(),
        result_rows=0,
        max_duplication=10.0,
    )
    assert report.ok


def test_run_checks_tolerates_duplicates_within_the_cap() -> None:
    """At-least-once means redelivery; the flood phase guarantees it."""
    report = vd.run_checks(
        total_messages=1,
        summaries=[_summary('req-000001')],
        result_ids={'req-000001'},
        result_rows=8,
        max_duplication=10.0,
    )
    assert report.ok
    assert report.duplication == 8.0


def test_run_checks_reports_duplication_past_the_cap() -> None:
    report = vd.run_checks(
        total_messages=1,
        summaries=[_summary('req-000001')],
        result_ids={'req-000001'},
        result_rows=40,
        max_duplication=10.0,
    )
    assert [v.check for v in report.violations] == ['duplication_bounded']


def test_duplication_factor_is_zero_when_nothing_was_written() -> None:
    """No divide-by-zero on a run where the sink stayed empty."""
    assert vd.duplication_factor(0, 0) == 0.0


def test_violation_render_names_the_check_and_its_sample() -> None:
    rendered = vd.Violation(check='no_loss', detail='2 missing', sample=('req-000007',)).render()
    assert rendered.startswith('FAIL no_loss: 2 missing')
    assert 'req-000007' in rendered
