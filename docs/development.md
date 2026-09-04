# Development

Everything in this repo runs through [`just`](https://github.com/casey/just) — the `justfile` at the repository root is the single dev entrypoint. The GitHub workflows call the **same recipes**, so `just ci` locally and CI cannot disagree.

## Prerequisites

- [uv](https://docs.astral.sh/uv/) — the only Python tool you install yourself; it manages Python versions and all dependencies (pip is not used anywhere).
- [just](https://github.com/casey/just#installation) — `brew install just`, `cargo install just`, or your distro's package. No `just`? `uvx --from=rust-just just <recipe>` works too.
- Docker (only for the [integration environment](integration.md)).

## Setup

```bash
git clone https://github.com/wlame/drakkar && cd drakkar
just install        # uv sync --extra=perf; the dev dependency-group installs by default
```

## Everyday recipes

`just` (no arguments) lists every recipe. The ones you'll use constantly:

| Recipe | What it does |
|--------|--------------|
| `just test` | Run the unit test suite (`pytest`) across all cores. Extra args pass through: `just test -k cache`, `just test tests/test_partition.py -x`; add `-n0` to run serially |
| `just cover` | Tests with the coverage gate — fails under the 95% floor from `pyproject.toml`, writes `coverage.xml` + `junit.xml` |
| `just fmt` | Format with **ruff** (`ruff format`) |
| `just fmt-check` | Formatting check only — the CI gate |
| `just lint` | Lint with **ruff** (`ruff check`); `just lint-fix` applies safe autofixes |
| `just typecheck` | Type-check with **ty** (`tests/` and `integration/` are excluded via `[tool.ty.src]`) |
| `just lock-check` | Fail when `uv.lock` is stale against `pyproject.toml` — the CI gate |
| `just ci` | Exactly what GitHub CI enforces, in the same order: `lock-check → fmt-check → lint → typecheck → cover → docs-build` |
| `just check` | Full pre-push battery: `ci` + the dependency CVE scan (`audit`), which CI keeps in a job of its own |

ruff and ty both run from the repo's own pinned dependencies (`uv run`), never from ad-hoc latest versions — what passes locally passes in CI and vice versa.

Tests run under `pytest-xdist` with `--dist loadfile`, so each test file stays
in one worker process. Files share process state — the Prometheus default
registry is the main one — and scattering a file across workers would make the
outcome depend on the split. A test that hangs is failed after 60 s by
`pytest-timeout` instead of holding the runner until the job cap.

## Docs

```bash
just docs-serve     # live-reload at http://127.0.0.1:8000
just docs-build     # strict build into site/ (broken links / warnings fail)
```

The `Deploy docs` workflow publishes to GitHub Pages on pushes to `main` that touch `docs/` or `mkdocs.yml`, using the same `just docs-build`.

## Versioning & releases

The version lives in **one place**: `__version__` in `drakkar/__init__.py` (hatch reads it at build time, the release workflow verifies the git tag matches it).

```bash
just version            # print the current version
just release            # patch bump: requires main + clean tree, runs `just ci`,
                        # then commits "release: bump version to X.Y.Z" and tags vX.Y.Z
just release minor      # minor bump (X.Y.0)
just release major      # major bump (X.0.0)
```

`just release` **never pushes** — it prints the exact `git push` commands. After pushing the commit and tag, create a GitHub Release from the tag; the `Release` workflow then re-runs the quality gates, verifies the tag matches `__version__`, builds with `uv build`, and publishes to PyPI via trusted publishing.

## Build artifacts

```bash
just build          # sdist + wheel into dist/
just clean          # remove dist/, site/, coverage and pytest artifacts, __pycache__
```

## Integration environment

The full Docker environment (Kafka, Postgres, MongoDB, Redis, webhook receiver, multi-worker clusters, load generator) is described in [Integration Tests](integration.md). Shortcuts:

```bash
just integration-up             # build + start everything
just integration-logs worker-1  # follow a worker's logs
just chaos                      # rolling-outage chaos test (workers stop/start mid-load)
just integration-down           # tear down, including volumes
```

## Operator tools

```bash
just replay-dlq -- --help       # DLQ replay tool (docs: sinks.md#dlq-replay)
```

(Flags after the recipe name pass straight through to `scripts/replay_dlq.py`.)

## CI layout

Four workflows in `.github/workflows/`, all driving the justfile:

| Workflow | Trigger | Recipes |
|----------|---------|---------|
| `ci.yml` | push / PR to `main` | `fmt-check`, `lint`, then per Python (3.13, 3.14): `install`, `typecheck` (3.14 only), `cover` — plus a non-blocking lane on the next CPython's release candidate running `install-minimal` + `test` |
| `integration.yml` | nightly cron / manual | the Docker harness: `integration-up`, readiness + delivery verification, `integration-down` — on the stable images, plus a non-blocking lane rebuilding every image on the next CPython's `-rc` Docker image |
| `release.yml` | GitHub Release published | same gates + tag-vs-`just version` check + `build` + PyPI publish |
| `docs.yml` | docs changes on `main` | `docs-build` + GitHub Pages deploy |

The release-candidate lanes are experimental by design: they surface breakage
on the next CPython months early (missing wheels force C extensions to build
from source there), and `continue-on-error` keeps them from ever gating a
merge or a release.

Building `confluent-kafka` from source needs librdkafka headers no older than
the `confluent-kafka` being built, and every distribution package is older
than that, so both lanes compile librdkafka first — `just install-librdkafka`
on the unit lane, and a shared base image
(`integration/infra/Dockerfile.harness-base`) for the harness. The version
comes from the `confluent-kafka` pin in `uv.lock`, so bumping the dependency
does not leave a second number behind.

The release-candidate lanes float their *interpreter* request but not their
tooling. The unit lane asks for `3.15` and gets the newest build of it,
because [python-build-standalone](https://github.com/astral-sh/python-build-standalone)
publishes a candidate days after python.org does and an exact pin fails with
`no download found` in between. The harness lane floats the same way through
its `python:3.15-rc-slim` image tag. Each run reports the interpreter it
actually tested.

`uv` itself stays pinned to an explicit version on every lane, experimental
included — a floating tool version breaks a build for reasons unrelated to
the change, and `tests/test_ci_workflows.py` enforces it. Because the set of
downloadable interpreters is compiled into each `uv` release, the lane reaches
a newer candidate only once that pin is bumped to a release carrying it. That
is a deliberate, reviewable step rather than an automatic one.
