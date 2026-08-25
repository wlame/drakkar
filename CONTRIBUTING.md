# Contributing

## Setup

```bash
just install     # uv sync --extra=perf (the dev dependency-group installs by default)
just ci           # what CI enforces: fmt-check, lint, typecheck, cover
```

`uv` only — never `pip`. Every gate runs through `just`, and the GitHub
workflows call the same recipes, so a local `just ci` and CI cannot
disagree.

## Before every commit

Run `just ci`. It must pass. `just check` additionally builds the docs
strictly and is the full pre-push battery.

## Commit messages

One imperative sentence, capital letter, full stop. No conventional-commit
prefixes, no body.

    CORRECT:   Bound the process reap after SIGKILL.
    INCORRECT: fix: bound proc reap
               fixed the reap timeout issue

## Coverage

The floor is 95% (`just cover`), and the suite currently sits only
fractionally above it (~95.3%). That is no longer slack: new code
without tests will trip the gate almost immediately, so add tests
alongside the code that needs them rather than planning to backfill
later.

## Dependencies

`[tool.uv] exclude-newer` in `pyproject.toml` refuses to resolve anything
published after the date it names, so a package compromised today cannot
reach a build here before the yank catches it. Upgrading a dependency
therefore means bumping that date — keep it roughly a week behind the day
you bump — and re-running `uv lock`, so the change shows up in the lockfile
diff. Two tests fail if the pin goes missing or lands in the future.

## Cross-backend parity

Drakkar has two implementations: this one and `drakkar-go`. The following
surfaces are **contractual** and must stay identical across both:

- config format (YAML keys and `DK_` env overrides)
- DLQ JSON bytes
- **metric names and help text**
- the config-summary one-liner
- `/api/v1` request and response shapes

A change to any of them lands on both backends in the same change, or not
at all. **Adding a metric obliges an identical metric in the Go backend**
— budget for that before introducing one.

## Domain neutrality

Drakkar is a general-purpose framework. Shipped surfaces — docs, code
comments, config descriptions, metric help, example YAML — must not name
any specific downstream application or its domain vocabulary. Describe
motivations generically ("processes that emit very large output", not a
named system).

## Orientation

`AGENTS.md` is the condensed map: the mental model, the numbered
invariants, and the gotchas. Read it before deriving structure from
source.
