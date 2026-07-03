# Development

Everything in this repo runs through [`just`](https://github.com/casey/just) — the `justfile` at the repository root is the single dev entrypoint. The GitHub workflows call the **same recipes**, so `just ci` locally and CI cannot disagree.

## Prerequisites

- [uv](https://docs.astral.sh/uv/) — the only Python tool you install yourself; it manages Python versions and all dependencies (pip is not used anywhere).
- [just](https://github.com/casey/just#installation) — `brew install just`, `cargo install just`, or your distro's package. No `just`? `uvx --from=rust-just just <recipe>` works too.
- Docker (only for the [integration environment](integration.md)).

## Setup

```bash
git clone https://github.com/wlame/drakkar && cd drakkar
just install        # uv sync with the dev + perf extras
```

## Everyday recipes

`just` (no arguments) lists every recipe. The ones you'll use constantly:

| Recipe | What it does |
|--------|--------------|
| `just test` | Run the unit test suite (`pytest`). Extra args pass through: `just test -k cache`, `just test tests/test_partition.py -x` |
| `just cover` | Tests with the coverage gate — fails under the 75% floor from `pyproject.toml`, writes `coverage.xml` + `junit.xml` |
| `just fmt` | Format with **ruff** (`ruff format`) |
| `just fmt-check` | Formatting check only — the CI gate |
| `just lint` | Lint with **ruff** (`ruff check`); `just lint-fix` applies safe autofixes |
| `just typecheck` | Type-check with **ty** (`tests/` and `integration/` are excluded via `[tool.ty.src]`) |
| `just ci` | Exactly what GitHub CI enforces, in the same order: `fmt-check → lint → typecheck → cover` |
| `just check` | Full pre-push battery: `ci` + strict docs build |

ruff and ty both run from the repo's own pinned dependencies (`uv run`), never from ad-hoc latest versions — what passes locally passes in CI and vice versa.

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

Three workflows in `.github/workflows/`, all driving the justfile:

| Workflow | Trigger | Recipes |
|----------|---------|---------|
| `ci.yml` | push / PR to `main` | `fmt-check`, `lint`, then per Python (3.13, 3.14): `install`, `typecheck` (3.14 only), `cover` |
| `release.yml` | GitHub Release published | same gates + tag-vs-`just version` check + `build` + PyPI publish |
| `docs.yml` | docs changes on `main` | `docs-build` + GitHub Pages deploy |
