# Drakkar — Kafka subprocess orchestration framework.
# Single dev entrypoint. The GitHub workflows (.github/workflows/) call these
# same recipes, so local `just ci` and CI cannot disagree.

set shell := ["bash", "-uc"]

# Coverage floor — mirrored by [tool.coverage.report] fail_under in pyproject.toml.
coverage_min := "75"

# List all recipes
default:
    @just --list --unsorted

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

# Install/sync all dependencies (dev + perf extras, dev group)
install:
    uv sync --extra=dev --extra=perf

# ---------------------------------------------------------------------------
# Quality gates
# ---------------------------------------------------------------------------

# Format code with ruff
fmt:
    uv run --extra=dev ruff format drakkar/ tests/

# Verify formatting without modifying files (CI gate)
fmt-check:
    uv run --extra=dev ruff format --check drakkar/ tests/

# Lint with ruff
lint:
    uv run --extra=dev ruff check drakkar/ tests/

# Lint and auto-fix what ruff can fix safely
lint-fix:
    uv run --extra=dev ruff check --fix drakkar/ tests/

# Type-check with ty (tests/ and integration/ excluded via pyproject)
typecheck:
    uv run ty check drakkar/

# Dependency CVE scan (pip-audit against the resolved lock).
# Deliberately NOT part of `just ci`: a newly published CVE must fail the
# security job without blocking every unrelated PR behind an unrelated gate.
# Mirrors the Go backend's `just vuln`.
audit:
    uv run --with=pip-audit pip-audit

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

# Run the unit test suite; extra pytest args pass through (just test -k cache)
test *args:
    uv run --extra=dev pytest {{ args }}

# Run tests with the coverage gate (fail_under from pyproject) + CI artifacts
cover:
    uv run --extra=dev pytest --cov=drakkar --cov-report=term-missing --cov-report=xml --junitxml=junit.xml

# Regenerate the Python-written cross-backend DB fixtures consumed by
# drakkar-go's interop tests (commit the result in that repo)
gen-db-fixtures:
    uv run python scripts/gen_db_fixtures.py --out=../drakkar-go/internal/crossbackend/testdata/python-db

# Embed a drakkar-ui release as the offline fallback bundle (then commit the result)
embed-ui version:
    #!/usr/bin/env bash
    set -euo pipefail
    tag="{{ version }}"
    url="https://github.com/wlame/drakkar-ui/releases/download/${tag}/drakkar-ui-${tag}.tar.gz"
    dest="drakkar/uihost/bundle"
    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"' EXIT
    echo "downloading ${url}"
    curl -fsSL "$url" -o "$tmp/bundle.tar.gz"
    mkdir "$tmp/x" && tar -xzf "$tmp/bundle.tar.gz" -C "$tmp/x"
    test -f "$tmp/x/index.html" || { echo "no index.html at archive root" >&2; exit 1; }
    rm -rf "$dest" && mkdir -p "$dest"
    cp -R "$tmp/x/." "$dest/"
    printf '%s\n' "$tag" > "$dest/VERSION"
    echo "embedded ${tag} into ${dest} — review and commit"

# ---------------------------------------------------------------------------
# CI / pre-push
# ---------------------------------------------------------------------------

# Exactly what GitHub CI enforces, same order: format → lint → types → tests+coverage
ci: fmt-check lint typecheck cover

# Full pre-push battery: ci + strict docs build
check: ci docs-build

# ---------------------------------------------------------------------------
# Docs
# ---------------------------------------------------------------------------

# Live preview with auto-reload at http://127.0.0.1:8000.
docs-serve:
    uv run mkdocs serve

# Build the docs site strictly into ./site (needs network for font/diagram self-hosting).
docs-build:
    uv run mkdocs build --strict

# Publish the docs to the gh-pages branch (requires a configured git remote).
docs-deploy:
    uv run mkdocs gh-deploy --strict

# ---------------------------------------------------------------------------
# Build & release
# ---------------------------------------------------------------------------

# Print the version a build would stamp (single source: drakkar/__init__.py)
version:
    @sed -n "s/^__version__ = '\([^']*\)'/\1/p" drakkar/__init__.py

# Build sdist + wheel into dist/
build:
    uv build

# Remove build/test/docs artifacts
clean:
    rm -rf dist/ build/ site/ .pytest_cache/ .coverage coverage.xml junit.xml
    find . -type d -name __pycache__ -not -path './.venv/*' -exec rm -rf {} +

# Requires main + clean tree, runs the full ci gate, then bumps the version
# (major|minor|patch), commits, and tags via scripts/bump.sh. It prints the
# push commands and NEVER pushes automatically.
release part='patch':
    #!/usr/bin/env bash
    set -euo pipefail
    branch=$(git rev-parse --abbrev-ref HEAD)
    if [[ "$branch" != "main" ]]; then
        echo "Error: release must run from main (currently on '$branch')"
        exit 1
    fi
    if ! git diff --quiet HEAD; then
        echo "Error: working tree has uncommitted changes"
        exit 1
    fi
    just ci
    ./scripts/bump.sh {{ part }}

# ---------------------------------------------------------------------------
# Product routines
# ---------------------------------------------------------------------------

# Manage the decoupled drakkar-ui bundle (e.g. just drakkar-ui where, just drakkar-ui update)
drakkar-ui *args:
    uv run drakkar-ui {{ args }}

# Start the integration environment (Kafka, sinks, worker clusters, load generator)
integration-up:
    docker compose -f integration/docker-compose.yml up -d --build

# Tear the integration environment down, including volumes
integration-down:
    docker compose -f integration/docker-compose.yml down -v

# Tail logs from the integration environment (just integration-logs worker-1)
integration-logs *args:
    docker compose -f integration/docker-compose.yml logs -f {{ args }}

# Run the rolling-outage chaos test against the integration environment
chaos:
    cd integration && ./chaos-test.sh

# Replay dead-lettered records (docs/sinks.md#dlq-replay); script flags pass through
replay-dlq *args:
    uv run python scripts/replay_dlq.py {{ args }}
