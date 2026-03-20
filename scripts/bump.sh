#!/usr/bin/env bash
set -euo pipefail

# Bump version in drakkar/__init__.py, commit, and create a git tag.
# Usage: ./scripts/bump.sh [major|minor|patch]
# Default: patch

PART="${1:-patch}"
VERSION_FILE="drakkar/__init__.py"

# Resolve repo root (script may be called from any directory)
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

# Read current version
CURRENT=$(sed -n "s/^__version__ = '\([^']*\)'/\1/p" "$VERSION_FILE")
if [[ -z "$CURRENT" ]]; then
    echo "Error: could not read __version__ from $VERSION_FILE"
    exit 1
fi

IFS='.' read -r MAJOR MINOR PATCH <<< "$CURRENT"

case "$PART" in
    major)
        MAJOR=$((MAJOR + 1))
        MINOR=0
        PATCH=0
        ;;
    minor)
        MINOR=$((MINOR + 1))
        PATCH=0
        ;;
    patch)
        PATCH=$((PATCH + 1))
        ;;
    *)
        echo "Usage: $0 [major|minor|patch]"
        exit 1
        ;;
esac

NEW_VERSION="${MAJOR}.${MINOR}.${PATCH}"
TAG="v${NEW_VERSION}"

# Check for uncommitted changes
if ! git diff --quiet HEAD 2>/dev/null; then
    echo "Error: working tree has uncommitted changes. Commit or stash first."
    exit 1
fi

# Check tag doesn't already exist
if git rev-parse "$TAG" >/dev/null 2>&1; then
    echo "Error: tag $TAG already exists"
    exit 1
fi

echo "Bumping: $CURRENT -> $NEW_VERSION"

# Update __init__.py (portable: works on both macOS and Linux)
sed -i.bak "s/__version__ = '${CURRENT}'/__version__ = '${NEW_VERSION}'/" "$VERSION_FILE"
rm -f "${VERSION_FILE}.bak"

# Commit and tag
git add "$VERSION_FILE"
git commit -m "release: bump version to ${NEW_VERSION}"
git tag -a "$TAG" -m "Release ${NEW_VERSION}"

echo ""
echo "Done! Created commit and tag $TAG"
echo ""
echo "Next steps:"
echo "  git push origin main      # push the commit"
echo "  git push origin $TAG      # push the tag"
echo "  Then create a Release on GitHub from tag $TAG"
