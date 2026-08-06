#!/usr/bin/env bash
set -euo pipefail

# Cut a release: promote the changelog, bump __version__, commit, and tag.
#
# Usage: ./scripts/bump.sh [major|minor|patch] [--dry-run]
# Default: patch
#
# Never pushes and never creates a GitHub Release. It prints those commands
# for you to run — publishing to PyPI is triggered by the GitHub Release
# (see .github/workflows/release.yml), not by the tag, so pushing the tag
# alone releases nothing.

PART="patch"
DRY_RUN=0
for arg in "$@"; do
    case "$arg" in
        major|minor|patch) PART="$arg" ;;
        --dry-run) DRY_RUN=1 ;;
        *)
            echo "Usage: $0 [major|minor|patch] [--dry-run]"
            exit 1
            ;;
    esac
done

VERSION_FILE="drakkar/__init__.py"
CHANGELOG="CHANGELOG.md"

# Resolve repo root (script may be called from any directory)
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

# ── read + compute the version ───────────────────────────────────────────

CURRENT=$(sed -n "s/^__version__ = '\([^']*\)'/\1/p" "$VERSION_FILE")
if [[ -z "$CURRENT" ]]; then
    echo "Error: could not read __version__ from $VERSION_FILE"
    exit 1
fi

IFS='.' read -r MAJOR MINOR PATCH <<< "$CURRENT"
case "$PART" in
    major) MAJOR=$((MAJOR + 1)); MINOR=0; PATCH=0 ;;
    minor) MINOR=$((MINOR + 1)); PATCH=0 ;;
    patch) PATCH=$((PATCH + 1)) ;;
esac

NEW_VERSION="${MAJOR}.${MINOR}.${PATCH}"
TAG="v${NEW_VERSION}"
TODAY=$(date +%F)

# ── preflight ────────────────────────────────────────────────────────────

# A dirty tree blocks a real run but not a preview — you often want to see
# the plan before committing the last change.
if ! git diff --quiet HEAD 2>/dev/null; then
    if [[ "$DRY_RUN" -eq 1 ]]; then
        echo "Warning: working tree has uncommitted changes — a real run would refuse."
        echo
    else
        echo "Error: working tree has uncommitted changes. Commit or stash first."
        exit 1
    fi
fi

if git rev-parse "$TAG" >/dev/null 2>&1; then
    echo "Error: tag $TAG already exists"
    exit 1
fi

if ! grep -q '^## \[Unreleased\]' "$CHANGELOG"; then
    echo "Error: $CHANGELOG has no '## [Unreleased]' section to promote"
    exit 1
fi

if grep -q "^## \[${NEW_VERSION}\]" "$CHANGELOG"; then
    echo "Error: $CHANGELOG already has a '## [${NEW_VERSION}]' section"
    exit 1
fi

# Refuse to release nothing. Everything between [Unreleased] and the next
# version heading must contain at least one non-blank line.
UNRELEASED_LINES=$(awk '
    /^## \[Unreleased\]/ { inside = 1; next }
    /^## \[/            { inside = 0 }
    inside && NF        { count++ }
    END                 { print count + 0 }
' "$CHANGELOG")
if [[ "$UNRELEASED_LINES" -eq 0 ]]; then
    echo "Error: [Unreleased] is empty — nothing to release"
    exit 1
fi

echo "Release plan"
echo "  version:   $CURRENT -> $NEW_VERSION  ($PART)"
echo "  tag:       $TAG (lightweight)"
echo "  changelog: [Unreleased] -> [$NEW_VERSION] - $TODAY  ($UNRELEASED_LINES lines)"
echo

if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "--dry-run: nothing changed."
    exit 0
fi

# ── apply ────────────────────────────────────────────────────────────────

# Promote the changelog: keep a fresh empty [Unreleased] at the top and put
# the released heading directly beneath it, so everything that was pending
# now sits under the new version.
awk -v ver="$NEW_VERSION" -v dt="$TODAY" '
    !promoted && /^## \[Unreleased\]/ {
        print "## [Unreleased]"
        print ""
        print "## [" ver "] - " dt
        promoted = 1
        next
    }
    { print }
' "$CHANGELOG" > "${CHANGELOG}.tmp"
mv "${CHANGELOG}.tmp" "$CHANGELOG"

# Update __version__ (sed -i.bak is the portable macOS/Linux form)
sed -i.bak "s/__version__ = '${CURRENT}'/__version__ = '${NEW_VERSION}'/" "$VERSION_FILE"
rm -f "${VERSION_FILE}.bak"

# One commit covering both. The message is a single imperative sentence with
# no conventional-commit prefix, matching this repo's history.
git add "$VERSION_FILE" "$CHANGELOG"
git commit -m "Bump version to ${NEW_VERSION}."

# Lightweight tag, matching v1.1.0 / v1.2.0 / v1.2.1.
git tag "$TAG"

echo "Created commit and tag $TAG"
echo
echo "Next steps — nothing has been pushed:"
echo
echo "  git push origin main"
echo "  git push origin $TAG"
echo
echo "  # The GitHub Release is what publishes to PyPI. Pushing the tag alone"
echo "  # does not trigger release.yml."
echo "  just release-notes ${NEW_VERSION} | gh release create $TAG --title=\"$TAG\" --notes-file=-"
echo
echo "  gh run watch   # follow lint -> test -> verify-tag -> publish"
