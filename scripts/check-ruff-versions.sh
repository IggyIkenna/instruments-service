#!/bin/bash
# Check that ruff versions are consistent across all config files
#
# This prevents quality gate failures caused by version mismatches between:
#   - .pre-commit-config.yaml (local pre-commit hooks)
#   - pyproject.toml (dependency specification)
#   - .github/workflows/quality-gates.yml or cloudbuild.yaml (CI)
#
# Usage:
#   ./scripts/check-ruff-versions.sh
#
# Returns:
#   Exit 0: All versions match
#   Exit 1: Version mismatch detected

set -e

echo "🔍 Checking ruff version consistency..."

# Extract versions from each file
PRECOMMIT_VERSION=""
PYPROJECT_VERSION=""
CI_VERSION=""

if [ -f ".pre-commit-config.yaml" ]; then
    PRECOMMIT_VERSION=$(grep -A 2 "ruff-pre-commit" .pre-commit-config.yaml | grep "rev:" | sed 's/.*rev: v//' | sed 's/[^0-9.].*$//' | head -1)
fi

if [ -f "pyproject.toml" ]; then
    PYPROJECT_VERSION=$(grep "ruff==" pyproject.toml | sed 's/.*ruff==\([0-9.]*\).*/\1/' | head -1)
fi

if [ -f ".github/workflows/quality-gates.yml" ]; then
    CI_VERSION=$(grep "ruff==" .github/workflows/quality-gates.yml | sed 's/.*ruff==\([0-9.]*\).*/\1/' | head -1)
elif [ -f "cloudbuild.yaml" ]; then
    CI_VERSION=$(grep "ruff==" cloudbuild.yaml | sed 's/.*ruff==\([0-9.]*\).*/\1/' | head -1)
fi

echo "  .pre-commit-config.yaml: ${PRECOMMIT_VERSION:-NOT FOUND}"
echo "  pyproject.toml:          ${PYPROJECT_VERSION:-NOT FOUND}"
echo "  CI config:               ${CI_VERSION:-NOT FOUND}"
echo ""

# Check if all versions match
if [ -n "$PRECOMMIT_VERSION" ] && [ -n "$PYPROJECT_VERSION" ] && [ -n "$CI_VERSION" ]; then
    if [ "$PRECOMMIT_VERSION" = "$PYPROJECT_VERSION" ] && [ "$PYPROJECT_VERSION" = "$CI_VERSION" ]; then
        echo "✅ All ruff versions match: $PRECOMMIT_VERSION"
        exit 0
    else
        echo "❌ Ruff version MISMATCH detected!"
        echo "  Expected: $PYPROJECT_VERSION (from pyproject.toml)"
        echo "  .pre-commit-config.yaml: $PRECOMMIT_VERSION"
        echo "  CI config: $CI_VERSION"
        echo ""
        echo "This will cause quality gate failures. Please update all files to use the same ruff version."
        echo "Recommended: Use the latest version from pyproject.toml across all config files."
        exit 1
    fi
else
    echo "⚠️  Could not find ruff version in one or more config files"
    exit 1
fi
