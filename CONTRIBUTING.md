# Contributing

## Setup

```bash
just install     # uv sync --extra=perf (the dev dependency-group installs by default)
just ci           # what CI enforces: fmt-check, lint, typecheck, cover, docs-build
```

`uv` only — never `pip`. Every gate runs through `just`, and the GitHub
workflows call the same recipes, so a local `just ci` and CI cannot
disagree.

## Before every commit

Run `just ci`. It must pass — it includes the strict docs build, so a
broken link fails here rather than after merge. `just check` adds the
dependency CVE scan and is the full pre-push battery.

## Commit messages

One imperative sentence, capital letter, full stop. No conventional-commit
prefixes, no body.

    CORRECT:   Bound the process reap after SIGKILL.
    INCORRECT: fix: bound proc reap
               fixed the reap timeout issue

## Coverage

The floor is 95% (`just cover`) and it measures **branches as well as
lines**: a line inside a `try` does not count as covered until its
`except` has run too. The suite sits fractionally above the floor
(~95.0%), so that is not slack — new code without tests trips the gate
almost immediately. Add the tests alongside the code, and give the error
path one of its own; that is where the framework's real bugs live.

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

## Comment style

A comment describes the code as it stands — what it does and why — not the
history that produced it. State the current behavior and its rationale:
the invariant it protects, the failure it avoids, the trade-off it makes.
Don't narrate what an earlier version did, name a bug a change fixed, or
say something "used to" work differently — a reader six months from now
has no earlier version to compare against, only the code in front of them.

    CORRECT:   Deferring the commit is always safe for at-least-once — it
               can only make the worker redo work after a crash, never
               skip it.
    INCORRECT: This used to commit once per message, which made the commit
               rate the bottleneck, so we changed it to coalesce.

    CORRECT:   Nothing is ever replayed, deliberately: PyMongo writes a
               generated `_id` back into every document it is handed, so
               re-sending one raises duplicate-key on an innocent document.
    INCORRECT: This retired the old `_id`-stripping workaround from the
               1.3.0 fix; do not bring either half back.

A rejected alternative is worth keeping if it explains the current design —
frame it as what that alternative would cost or break, not as what the code
used to do before this one replaced it. A genuinely open question — a
trade-off worth revisiting under different constraints, or a natural
extension not yet built — belongs too, stated as a forward-looking note
("deliberately left out for now") rather than a status update on a past
decision. `git log` and `CHANGELOG.md` are where the "what changed and
when" story belongs; a comment only has to be true today.

## Orientation

`AGENTS.md` is the condensed map: the mental model, the numbered
invariants, and the gotchas. Read it before deriving structure from
source.
