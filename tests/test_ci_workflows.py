"""Hardening invariants for the GitHub Actions workflows.

Workflow files are configuration that nothing else checks: a missing
``permissions`` block hands every job a write-capable token, a missing
``timeout-minutes`` lets a hung test hold a runner for six hours, and a
floating tool version makes a build fail for reasons unrelated to the
change. None of that shows up in a green run — it shows up on the day it
bites. These cases pin the invariants instead.

Parsed as data rather than grepped so a reformatting of the YAML cannot
quietly turn an assertion into a no-op.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest
import yaml

WORKFLOW_DIR = Path(__file__).resolve().parent.parent / '.github' / 'workflows'
WORKFLOWS = sorted(path.name for path in WORKFLOW_DIR.glob('*.yml'))

# The uv release CI resolves. Pinned so CI and a developer machine run the
# same resolver; Dependabot's github-actions ecosystem bumps it.
EXPECTED_UV_VERSION = '0.11.32'

# Jobs allowed to hold a runner longer than the default cap, with the reason.
LONG_RUNNING_JOBS: dict[tuple[str, str], int] = {
    ('integration.yml', 'harness'): 30,  # docker compose harness + load generator
}
DEFAULT_TIMEOUT_MINUTES = 20


def load(name: str) -> dict[str, Any]:
    """Parse one workflow file."""
    return yaml.safe_load((WORKFLOW_DIR / name).read_text())


def jobs(workflow: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return workflow.get('jobs', {})


def test_every_workflow_is_covered_by_these_checks() -> None:
    """A new workflow must not slip past the hardening cases."""
    assert WORKFLOWS, 'no workflows found'
    assert set(WORKFLOWS) == {'ci.yml', 'docs.yml', 'integration.yml', 'release.yml'}


@pytest.mark.parametrize('name', WORKFLOWS)
def test_workflow_declares_least_privilege_permissions(name: str) -> None:
    """Without a top-level block the job token inherits the repo default."""
    workflow = load(name)
    assert 'permissions' in workflow, f'{name} has no top-level permissions block'
    assert workflow['permissions'].get('contents') == 'read', (
        f'{name} must default to contents: read; widen per job where genuinely needed'
    )


@pytest.mark.parametrize('name', WORKFLOWS)
def test_workflow_declares_concurrency(name: str) -> None:
    """Two runs of the same ref must not do the same work twice."""
    workflow = load(name)
    assert 'concurrency' in workflow, f'{name} has no concurrency block'
    assert workflow['concurrency'].get('group'), f'{name} concurrency has no group'


def test_publishing_runs_are_never_cancelled() -> None:
    """Cancelling a half-finished publish is worse than doing the work twice."""
    for name in ('release.yml', 'integration.yml'):
        workflow = load(name)
        assert workflow['concurrency'].get('cancel-in-progress') is False, f'{name} must not cancel an in-progress run'


def test_superseded_ci_runs_are_cancelled() -> None:
    """A new push to a PR makes the previous run's result irrelevant."""
    assert load('ci.yml')['concurrency'].get('cancel-in-progress') is True


@pytest.mark.parametrize('name', WORKFLOWS)
def test_every_job_bounds_its_runtime(name: str) -> None:
    """A hung step must fail the job, not occupy a runner for six hours."""
    for job_name, job in jobs(load(name)).items():
        timeout = job.get('timeout-minutes')
        assert timeout is not None, f'{name}:{job_name} has no timeout-minutes'
        allowed = LONG_RUNNING_JOBS.get((name, job_name), DEFAULT_TIMEOUT_MINUTES)
        assert timeout <= allowed, f'{name}:{job_name} timeout {timeout} exceeds the allowed {allowed}'


@pytest.mark.parametrize('name', WORKFLOWS)
def test_job_level_permissions_keep_what_checkout_needs(name: str) -> None:
    """A job-level ``permissions`` block REPLACES the workflow-level one.

    Unlisted scopes become ``none``, so a job that narrows permissions to
    add one scope silently drops ``contents: read`` — and ``actions/checkout``
    then fails on a private repository. Every job that both checks out and
    declares its own block must repeat it.
    """
    for job_name, job in jobs(load(name)).items():
        if 'permissions' not in job:
            continue
        checks_out = any(step.get('uses', '').startswith('actions/checkout') for step in job.get('steps', []))
        if not checks_out:
            continue
        assert job['permissions'].get('contents') == 'read', (
            f'{name}:{job_name} overrides permissions without repeating contents: read, which actions/checkout needs'
        )


@pytest.mark.parametrize('name', WORKFLOWS)
def test_uv_is_pinned_to_an_explicit_version(name: str) -> None:
    """``version: latest`` makes CI's resolver drift from the developer's."""
    for job_name, job in jobs(load(name)).items():
        for index, step in enumerate(job.get('steps', [])):
            uses = step.get('uses', '')
            if not uses.startswith('astral-sh/setup-uv'):
                continue
            version = step.get('with', {}).get('version')
            where = f'{name}:{job_name} step {index}'
            assert version and version != 'latest', f'{where} does not pin uv'
            if version == EXPECTED_UV_VERSION:
                continue
            # The experimental-CPython lane selects its own uv through a matrix
            # expression. That still counts as pinned when the fallback is the
            # expected version and every matrix override is an explicit version.
            fallback = re.fullmatch(r"\$\{\{ matrix\.uv-version \|\| '([0-9.]+)' \}\}", version)
            assert fallback and fallback.group(1) == EXPECTED_UV_VERSION, (
                f'{where} pins uv {version}, expected {EXPECTED_UV_VERSION}'
            )
            overrides = [
                include.get('uv-version')
                for include in job.get('strategy', {}).get('matrix', {}).get('include', [])
                if 'uv-version' in include
            ]
            assert overrides and all(re.fullmatch(r'[0-9]+(\.[0-9]+)*', str(v)) for v in overrides), (
                f'{where} uses a matrix uv-version whose overrides are not explicit versions: {overrides}'
            )


JUSTFILE = WORKFLOW_DIR.parent.parent / 'justfile'

# Workflows that install dependencies and therefore must prove the lock is
# current before they build or test anything against it.
INSTALLING_WORKFLOWS = ('ci.yml', 'release.yml')


@pytest.mark.parametrize('name', INSTALLING_WORKFLOWS)
def test_a_job_checks_the_lockfile_is_current(name: str) -> None:
    """Without this, a dependency change that skipped ``uv lock`` lets the
    runner resolve whatever is current: CI then tests a set of packages that
    never appeared in the diff, and ``[tool.uv] exclude-newer`` is not applied
    the way CONTRIBUTING describes. It runs in milliseconds, so it belongs in
    the first job rather than behind the test matrix.
    """
    commands = [
        step.get('run', '') for job in jobs(load(name)).values() for step in job.get('steps', []) if 'run' in step
    ]
    assert any('lock-check' in run or 'uv lock --check' in run for run in commands), (
        f'{name} never verifies uv.lock against pyproject.toml'
    )


def test_install_recipes_refuse_a_stale_lockfile() -> None:
    """``uv sync`` without ``--locked`` re-resolves silently when the lock is
    stale. Every install path has to fail instead, so the failure is a red run
    with an actionable message rather than a build against unreviewed
    packages.
    """
    text = JUSTFILE.read_text()
    offenders = [
        line.strip()
        for line in text.splitlines()
        if re.match(r'\s+uv sync\b', line) and '--locked' not in line and '--frozen' not in line
    ]
    assert offenders == [], f'justfile installs without --locked: {offenders}'


# Recipes ``just ci`` must depend on, with what a local run that skips one
# lets through. The aggregate is the contributor-facing promise that a green
# `just ci` means a green CI, so every gate CI enforces belongs in this table.
CI_AGGREGATE_GATES = {
    'lock-check': 'a dependency change that skipped `uv lock` resolves packages nobody reviewed',
    'docs-build': 'a broken link or a page missing from the nav reaches main and reddens the Pages deploy',
}


@pytest.mark.parametrize('recipe', sorted(CI_AGGREGATE_GATES))
def test_ci_aggregate_runs_every_gate_ci_enforces(recipe: str) -> None:
    """``just ci`` must be exactly the gates GitHub CI enforces — a local run
    that skips one is how the two drift apart.
    """
    text = JUSTFILE.read_text()
    ci_line = next(line for line in text.splitlines() if line.startswith('ci:'))
    assert recipe in ci_line, f'`just ci` does not depend on {recipe}, so {CI_AGGREGATE_GATES[recipe]}: {ci_line!r}'


def test_a_pull_request_builds_the_docs() -> None:
    """The strict build is what validates cross-links, the nav and snippet
    anchors. Gated only by the post-merge deploy, a broken link passes review,
    lands on ``main`` and then needs a second pull request to fix.
    """
    commands = [
        step.get('run', '') for job in jobs(load('ci.yml')).values() for step in job.get('steps', []) if 'run' in step
    ]
    assert any('docs-build' in run for run in commands), (
        'ci.yml never builds the docs, so docs breakage is found only after merge'
    )
