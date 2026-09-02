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
diff. Write it as an explicit UTC instant (`2026-08-18T00:00:00Z`): uv reads
a bare date as *local* midnight, which makes `uv.lock` differ between
machines in different timezones. Tests fail if the pin goes missing, loses
its timezone, or lands in the future.

Any change to the dependency list must include the `uv.lock` diff. CI
enforces it: `just lock-check` (`uv lock --check`) runs before anything else
and `just install` uses `uv sync --locked`, so a stale lock is a red run with
a specific message rather than a silent re-resolve that tests packages nobody
reviewed.

## Wire contracts

Some surfaces are contracts rather than implementation details — something
outside this repository already depends on their exact bytes:

- config format (YAML keys and `DK_` env overrides)
- DLQ JSON bytes
- **metric names and help text**
- the config-summary one-liner
- `/api/v1` request and response shapes
- the recorder and cache SQLite schemas, which workers sharing a `db_dir`
  read from each other

Changing any of them is a breaking change, not a refactor. Metric names and
help text are the easiest to change by accident and the most disruptive to
change silently: a rename breaks every dashboard and alert built on it.

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
