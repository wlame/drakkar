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
