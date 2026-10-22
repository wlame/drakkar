# Drakkar — Kafka subprocess orchestration framework.
# Single dev entrypoint. The GitHub workflows (.github/workflows/) call these
# same recipes, so local `just ci` and CI cannot disagree.

set shell := ["bash", "-uc"]

# Coverage floor on the combined line+branch measure — mirrored by
# [tool.coverage.report] fail_under in pyproject.toml.
coverage_min := "95"

# List all recipes
default:
    @just --list --unsorted

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

# Install/sync all dependencies (the dev group is installed by default).
# --locked fails when uv.lock does not match pyproject.toml instead of
# quietly re-resolving: without it a dependency change that skipped `uv lock`
# installs whatever resolves today, so CI tests a set nobody reviewed and the
# [tool.uv] exclude-newer date pin is not applied the way CONTRIBUTING
# describes. The fix when it fails is `uv lock`, and the lock diff belongs in
# the same change.
install:
    uv sync --locked --extra=perf

# Install dependencies without optional extras. Reproduces what a user who
# installed py-drakkar without `[perf]` runs, so the recorder's stdlib JSON
# fallback is exercised rather than orjson's fast path. Also the escape hatch
# for a CI lane on a CPython that a native extra has no wheel for yet.
install-minimal:
    uv sync --locked

# Fail when uv.lock is stale against pyproject.toml. Runs in milliseconds and
# gives a contributor the specific message before any test does.
lock-check:
    uv lock --check

# Needed only where confluent-kafka has no wheel and builds from its sdist:
# the experimental-CPython lanes. Distribution packages of librdkafka are far
# behind what that build demands, so the headers are compiled, not installed.
# With no argument the version comes from the confluent-kafka pin in uv.lock.
#
# Build and install librdkafka from source (e.g. just install-librdkafka 2.15.0)
install-librdkafka *args:
    ./scripts/install-librdkafka.sh {{ args }}

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
# this when swagger-ui-dist publishes a security release.
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
    echo "vendored swagger-ui-dist {{ version }}; run 'just test'"

# Dependency CVE scan (pip-audit against the installed environment).
# Deliberately NOT part of `just ci`: a newly published CVE must fail the
# security job without blocking every unrelated PR behind an unrelated gate.
audit:
    uv run --with=pip-audit pip-audit

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

# Distribution for both test recipes. `loadfile` keeps a file's tests in one
# worker: the suite shares process-global state within a file (the Prometheus
# default registry above all), so scattering a file across workers would make
# the result depend on the split. Pass `-n0` to run serially.
test_parallel := "-n auto --dist loadfile"

# Run the unit test suite; extra pytest args pass through (just test -k cache)
test *args:
    uv run pytest {{ test_parallel }} {{ args }}

# Run tests with the coverage gate (fail_under from pyproject) + CI artifacts
cover:
    uv run pytest {{ test_parallel }} --cov=drakkar --cov-report=term-missing --cov-report=xml --junitxml=junit.xml

# Unit tests of the integration-harness handlers. Separate from `test`
# because pyproject's testpaths is tests/ only, so the harness handlers
# would otherwise never run in any gate.
test-harness:
    uv run pytest integration/worker/test_handler.py integration/infra/test_producer.py integration/test_plant_history.py -q

# Regenerate the recorder event-type vocabulary fixture from
# drakkar.recorder.schema.EventType (test_event_vocabulary.py pins it)
gen-event-vocabulary:
    uv run python scripts/gen_event_vocabulary.py

# ---------------------------------------------------------------------------
# CI / pre-push
# ---------------------------------------------------------------------------

# Exactly what GitHub CI enforces, same order: lock → format → lint → types → tests+coverage → harness → docs
ci: lock-check fmt-check lint typecheck cover test-harness docs-build

# Full pre-push battery: ci + the CVE scan CI keeps in a job of its own
check: ci audit

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

# Start the integration environment; provider: filesystem, postgres, redis or consul; profile: full or quick (just integration-up provider=consul profile=quick)
integration-up provider='filesystem' profile='full':
    #!/usr/bin/env bash
    set -euo pipefail
    # The main cluster's registry and archive-election provider; read by
    # docker-compose.yml. just passes `provider=postgres` through as it is,
    # so the prefix is stripped and the value is checked.
    provider="{{ provider }}"
    provider="${provider#provider=}"
    case "$provider" in
        filesystem|postgres|redis|consul) ;;
        *) echo "unknown provider '$provider': expected filesystem, postgres, redis or consul" >&2; exit 2 ;;
    esac
    export HARNESS_PROVIDER="$provider"
    # The profile sets the size of the run; read by docker-compose.yml. A
    # value already in the caller's environment wins (CI sets a small total).
    profile="{{ profile }}"
    profile="${profile#profile=}"
    case "$profile" in
        full)  profile_total=5000; profile_scale=1.0 ;;
        quick) profile_total=1000; profile_scale=0.2 ;;
        *) echo "unknown profile '$profile': expected full or quick" >&2; exit 2 ;;
    esac
    export TOTAL_MESSAGES="${TOTAL_MESSAGES:-$profile_total}"
    export REPEAT_SCALE="${REPEAT_SCALE:-$profile_scale}"
    # A non-default interpreter image means a release candidate, which means no
    # confluent-kafka wheel: the service images compile it from source and need
    # librdkafka headers newer than any distro package. Build that base once
    # here rather than in four image builds at the same time.
    if [ -n "${HARNESS_PYTHON_IMAGE:-}" ]; then
        docker build \
            -f integration/infra/Dockerfile.harness-base \
            --build-arg PYTHON_IMAGE="$HARNESS_PYTHON_IMAGE" \
            --build-arg LIBRDKAFKA_VERSION="${HARNESS_LIBRDKAFKA_VERSION:-}" \
            -t drakkar-harness-base:local .
        export HARNESS_PYTHON_IMAGE=drakkar-harness-base:local
    fi
    docker compose -f integration/docker-compose.yml up -d --build

# Tear the integration environment down, including volumes
integration-down:
    docker compose -f integration/docker-compose.yml down -v

# Tail logs from the integration environment (just integration-logs worker-1)
integration-logs *args:
    docker compose -f integration/docker-compose.yml logs -f {{ args }}

# Needs the harness up and the producer finished. Without a total it reads the
# one the running producer was started with; pass one to override it
# (just verify-delivery 200). Fails when a request was lost, duplicated past the
# cap, or delivered out of payload order.
#
# Check the harness delivered every request, in order (just verify-delivery [total])
verify-delivery total='' *args:
    #!/usr/bin/env bash
    set -euo pipefail
    total="{{ total }}"
    if [ -z "$total" ]; then
        id=$(docker compose -f integration/docker-compose.yml ps -aq producer)
        if [ -z "$id" ] || [ "$(docker inspect -f '{{{{.State.Running}}' "$id" 2>/dev/null)" != true ]; then
            echo "no total given and the producer container is not running; pass the total (just verify-delivery 200)" >&2
            exit 1
        fi
        total=$(docker inspect -f '{{{{range .Config.Env}}{{{{println .}}{{{{end}}' "$id" | sed -n 's/^TOTAL_MESSAGES=//p')
        if [ -z "$total" ]; then
            echo "the producer container has no TOTAL_MESSAGES in its environment; pass the total" >&2
            exit 1
        fi
    fi
    uv run python integration/verify_delivery.py --total-messages="$total" {{ args }}

# Run the rolling-outage chaos test against the integration environment
chaos:
    cd integration && ./chaos-test.sh

# Needs the harness up with the same provider. Stops at the first failing
# scenario; each script prints one verdict line.
#
# Wait for the workers, then run the four provider outage scenarios (just integration-scenarios provider=postgres)
integration-scenarios provider='filesystem':
    #!/usr/bin/env bash
    set -euo pipefail
    provider="{{ provider }}"
    provider="${provider#provider=}"
    case "$provider" in
        filesystem|postgres|redis|consul) ;;
        *) echo "unknown provider '$provider': expected filesystem, postgres, redis or consul" >&2; exit 2 ;;
    esac
    export HARNESS_PROVIDER="$provider"
    integration/wait-ready.sh
    for scenario in service-outage worker-cut-off unclean-exit secrets; do
        integration/scenario-${scenario}.sh
    done

# Run the harness for every provider (filesystem, postgres, redis, consul) and print a PASS/FAIL/N/A table; profile: full or quick (just integration-matrix profile=quick)
integration-matrix profile='quick':
    #!/usr/bin/env bash
    set -euo pipefail
    profile="{{ profile }}"
    profile="${profile#profile=}"
    case "$profile" in
        full|quick) ;;
        *) echo "unknown profile '$profile': expected full or quick" >&2; exit 2 ;;
    esac
    providers=(filesystem postgres redis consul)
    steps=(service-outage worker-cut-off unclean-exit secrets drain-delivery)
    scratch=$(mktemp -d)
    trap 'rm -rf "$scratch"' EXIT
    any_failed=false

    # Run one harness script, keep its output on screen and in a file, and
    # record PASS, FAIL or N/A for the provider and step. A scenario prints
    # one VERDICT line; "not applicable" in it means N/A, not FAIL.
    #   run_step <provider> <step> <script>
    run_step() {
        local provider=$1 step=$2 script=$3 log_file status verdict
        log_file="$scratch/${provider}-${step}.log"
        set +e
        HARNESS_PROVIDER="$provider" "$script" 2>&1 | tee "$log_file"
        status=${PIPESTATUS[0]}
        set -e
        verdict=PASS
        if [ "$status" -ne 0 ]; then
            verdict=FAIL
            any_failed=true
        elif grep -q 'VERDICT .*: not applicable' "$log_file"; then
            verdict=N/A
        fi
        echo "$verdict" >"$scratch/${provider}-${step}.verdict"
    }

    for provider in "${providers[@]}"; do
        echo "=== matrix: ${provider} (profile ${profile}) ==="
        # The shared folder is a bind mount that `integration-down` does not
        # clear: one provider's archive, lock or work file must not reach the next.
        find integration/shared -mindepth 1 -maxdepth 1 ! -name README.md -exec rm -rf {} +
        if ! just integration-up "provider=${provider}" "profile=${profile}"; then
            echo "integration-up failed for ${provider}" >&2
            any_failed=true
            for step in "${steps[@]}"; do echo FAIL >"$scratch/${provider}-${step}.verdict"; done
            just integration-down || true
            continue
        fi
        if integration/wait-ready.sh; then
            for step in service-outage worker-cut-off unclean-exit secrets; do
                run_step "$provider" "$step" "integration/scenario-${step}.sh"
            done
            run_step "$provider" drain-delivery integration/verify-after-drain.sh
        else
            echo "the workers of ${provider} were not ready" >&2
            any_failed=true
            for step in "${steps[@]}"; do echo FAIL >"$scratch/${provider}-${step}.verdict"; done
        fi
        just integration-down || true
    done

    echo
    echo "=== matrix summary (profile ${profile}) ==="
    printf '%-12s' provider
    for step in "${steps[@]}"; do printf '%-16s' "$step"; done
    echo
    for provider in "${providers[@]}"; do
        printf '%-12s' "$provider"
        for step in "${steps[@]}"; do printf '%-16s' "$(cat "$scratch/${provider}-${step}.verdict")"; done
        echo
    done
    if [ "$any_failed" = true ]; then
        echo "matrix: at least one step failed" >&2
        exit 1
    fi
    echo "matrix: every step passed or does not apply"

# Replay dead-lettered records (docs/sinks.md#dlq-replay); script flags pass through
replay-dlq *args:
    uv run python scripts/replay_dlq.py {{ args }}
