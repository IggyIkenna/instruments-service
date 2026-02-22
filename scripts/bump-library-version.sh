#!/bin/bash
# Auto-bump library version if code changes
# Used as pre-commit hook for library repos (unified-cloud-services, unified-*-interface)

set -e

# Only run if pyproject.toml exists (library repo)
if [ ! -f "pyproject.toml" ]; then
    exit 0
fi

# Extract current version from pyproject.toml
CURRENT_VERSION=$(grep '^version = ' pyproject.toml | sed 's/version = "\(.*\)"/\1/')

if [ -z "$CURRENT_VERSION" ]; then
    echo "⚠️  Could not extract version from pyproject.toml"
    exit 0
fi

# Check if there are code changes (not just docs/tests)
CODE_CHANGED=$(git diff --cached --name-only | grep -E "^(unified_|.*\.py$)" | grep -v "^tests/" | wc -l | tr -d ' ')

if [ "$CODE_CHANGED" -eq 0 ]; then
    echo "✓ No code changes, skipping version bump"
    exit 0
fi

# Parse version components
IFS='.' read -r MAJOR MINOR PATCH <<< "$CURRENT_VERSION"

# Bump patch version
NEW_PATCH=$((PATCH + 1))
NEW_VERSION="${MAJOR}.${MINOR}.${NEW_PATCH}"

echo "📦 Code changes detected - bumping version: $CURRENT_VERSION → $NEW_VERSION"

# Update pyproject.toml
if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS
    sed -i '' "s/^version = \"$CURRENT_VERSION\"/version = \"$NEW_VERSION\"/" pyproject.toml
else
    # Linux
    sed -i "s/^version = \"$CURRENT_VERSION\"/version = \"$NEW_VERSION\"/" pyproject.toml
fi

# Stage the version bump
git add pyproject.toml

echo "✓ Version bumped to $NEW_VERSION and staged for commit"
