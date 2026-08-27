# Drakkar — Kafka subprocess orchestration framework.
# Single dev entrypoint. The GitHub workflows (.github/workflows/) call these
# same recipes, so local `just ci` and CI cannot disagree.

set shell := ["bash", "-uc"]

# Coverage floor — mirrored by [tool.coverage.report] fail_under in pyproject.toml.
coverage_min := "95"

# List all recipes
default:
    @just --list --unsorted

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

# Install/sync all dependencies (the dev group is installed by default)
install:
    uv sync --extra=perf

# ---------------------------------------------------------------------------
# Quality gates
# ---------------------------------------------------------------------------

# Format code with ruff
fmt:
    uv run ruff format drakkar/ tests/

# Verify formatting without modifying files (CI gate)
fmt-check:
    uv run ruff format --check drakkar/ tests/

# Lint with ruff
lint:
    uv run ruff check drakkar/ tests/

# Lint and auto-fix what ruff can fix safely
lint-fix:
    uv run ruff check --fix drakkar/ tests/

# Type-check with ty (tests/ and integration/ excluded via pyproject)
typecheck:
    uv run ty check drakkar/

# The Swagger UI assets are served offline from drakkar/uiserver/swagger/
# (no CDN) and ship in the wheel, so they are ordinary third-party
# dependencies that nothing else updates: the lockfile does not cover them,
# pip-audit does not see them, and Dependabot has no manifest to read. Run
# this when swagger-ui-dist publishes a security release, then mirror the
# result into the Go backend so both serve the same bytes.
#
# Re-vendor the Swagger UI assets (e.g. just vendor-swagger 5.32.8)
vendor-swagger version:
    #!/usr/bin/env bash
    set -euo pipefail
    dir=drakkar/uiserver/swagger
    base="https://unpkg.com/swagger-ui-dist@{{ version }}"
    for asset in swagger-ui-bundle.js swagger-ui.css LICENSE; do
        echo "fetching $asset"
        curl -fsSL "$base/$asset" -o "$dir/$asset.new"
        mv "$dir/$asset.new" "$dir/$asset"
    done
    echo "{{ version }}" > "$dir/VERSION"
    echo "vendored swagger-ui-dist {{ version }}; run 'just test' and mirror into ../drakkar-go"

# Dependency CVE scan (pip-audit against the installed environment).
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
    uv run pytest {{ args }}

# Run tests with the coverage gate (fail_under from pyproject) + CI artifacts
cover:
    uv run pytest --cov=drakkar --cov-report=term-missing --cov-report=xml --junitxml=junit.xml

# Regenerate the Python-written cross-backend DB fixtures consumed by
# drakkar-go's interop tests (commit the result in that repo)
gen-db-fixtures:
    uv run python scripts/gen_db_fixtures.py --out=../drakkar-go/internal/crossbackend/testdata/python-db

# Regenerate the shared recorder event-type vocabulary fixture from
# drakkar.recorder.schema.EventType, here and in the Go repo's testdata
gen-event-vocabulary:
    uv run python scripts/gen_event_vocabulary.py
    uv run python scripts/gen_event_vocabulary.py --out=../drakkar-go/internal/recorder/testdata

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

# Cut a release (major|minor|patch): ci gate, changelog, version, commit, tag — never pushes
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

# Show what a release would do without changing anything
release-dry part='patch':
    @./scripts/bump.sh {{ part }} --dry-run

# Print one version's changelog section (e.g. just release-notes 1.3.0)
release-notes version:
    @awk '/^## \[{{ version }}\]/{f=1;next} /^## \[/{f=0} f' CHANGELOG.md

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

# Needs the harness up and the producer finished; pass the TOTAL_MESSAGES it
# ran with (just verify-delivery 200). Fails when a request was lost,
# duplicated past the cap, or delivered out of payload order.
#
# Check the harness delivered every request, in order (just verify-delivery 200)
verify-delivery total='5000' *args:
    uv run python integration/verify_delivery.py --total-messages={{ total }} {{ args }}

# Run the rolling-outage chaos test against the integration environment
chaos:
    cd integration && ./chaos-test.sh

# Replay dead-lettered records (docs/sinks.md#dlq-replay); script flags pass through
replay-dlq *args:
    uv run python scripts/replay_dlq.py {{ args }}
