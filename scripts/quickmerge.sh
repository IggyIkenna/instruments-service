#!/bin/bash
# quickmerge: Push changes through a PR with auto-merge
#
# Usage:
#   ./scripts/quickmerge.sh "commit message"
#   ./scripts/quickmerge.sh "commit message" --files "path1 path2 path3"
#
# When --files is provided: only stage and commit those paths (repo-relative).
# When --files is omitted: stage all changes (git add -A).
# Agents MUST use --files with the list of files they changed.
#
# What it does:
#   1. Runs quality gates FIRST (scripts/quality-gates.sh --no-fix)
#      - If quality gates fail, script exits immediately (fail fast)
#      - Does NOT proceed with merge if quality gates fail
#   2. Stashes changes, checkouts main, pulls latest
#   3. Creates timestamped branch FROM main (avoids merge conflicts)
#   4. Reapplies stashed changes, commits (pre-commit hooks run)
#   5. Pushes branch, creates PR with auto-merge (squash)
#   6. Stays on PR branch
#
# The PR auto-merges once CI quality gates pass.
# The branch is auto-deleted after merge.
#
# IMPORTANT: Quality gates MUST pass before any branch/PR is created.
# If quality gates fail, fix issues and re-run quickmerge.
#
# Prerequisites:
#   - gh CLI installed and authenticated (gh auth login)
#   - Auto-merge enabled on the repo (Settings > General > Allow auto-merge)

set -e

# Parse arguments: COMMIT_MSG and optional --files "path1 path2"
COMMIT_MSG="chore: automated update"
FILES_ARG=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --files)
            FILES_ARG="$2"
            shift 2
            ;;
        *)
            COMMIT_MSG="$1"
            shift
            ;;
    esac
done

REPO_DIR="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
REPO_NAME=$(basename "$REPO_DIR")

cd "$REPO_DIR"

# Install project dependencies before quality gates.
# Use UV only (never pip except bootstrap). See .cursor/rules/uv-package-manager.mdc
if [ -f "pyproject.toml" ]; then
    echo "[$REPO_NAME] Installing project dependencies..."
    command -v uv >/dev/null 2>&1 || pip install uv --quiet
    if [ "$REPO_NAME" = "unified-cloud-services" ]; then
        uv pip install -e ".[databento]" || uv pip install -e ".[dev]" || uv pip install -e .
    elif [ "$REPO_NAME" = "instruments-service" ]; then
        uv pip install -e . --no-deps || uv pip install -e .
    elif [ "$REPO_NAME" = "execution-services" ]; then
        [ -d "../unified-cloud-services" ] && uv pip install -e ../unified-cloud-services -q 2>/dev/null || true
        uv pip install -e ".[dev]" --no-deps || uv pip install -e ".[dev]" || uv pip install -e .
    elif [ "$REPO_NAME" = "unified-trading-deployment-v2" ]; then
        uv pip install -e ".[dev]" || uv pip install -e .
        uv pip install fastapi
    else
        uv pip install -e ".[dev]" || uv pip install -e .
    fi
fi

# Check for changes
if [ -z "$(git status --porcelain)" ]; then
    echo "No changes to commit in $REPO_NAME"
    exit 0
fi

# Run quality gates in two phases: (1) auto-fix ruff format/lint, (2) verify
# Uses same ruff version as Cloud Build and GitHub Actions - prevents CI format failures
if [ -f "scripts/quality-gates.sh" ]; then
    echo "[$REPO_NAME] Phase 1: Running quality gates (auto-fix ruff format + check)..."
    bash scripts/quality-gates.sh
    echo "[$REPO_NAME] Phase 2: Verifying quality gates (--no-fix)..."
    if ! bash scripts/quality-gates.sh --no-fix; then
        echo "[$REPO_NAME] ❌ Quality gates FAILED - Fix remaining issues before merging"
        exit 1
    fi
    echo "[$REPO_NAME] ✅ Quality gates PASSED - Proceeding with merge"
else
    echo "[$REPO_NAME] ⚠️  No quality-gates.sh found (skipping quality gate check)"
fi

# Stash changes, sync with main, create branch from main, reapply (cursor rules compliance)
# This ensures we never branch from a stale PR branch and avoids merge conflicts.
echo "[$REPO_NAME] Stashing changes and syncing with main..."
git stash push -u -m "quickmerge-$$" --quiet

git checkout main --quiet
git fetch origin main --quiet
git reset --hard origin/main --quiet

BRANCH="auto/$(date +%Y%m%d-%H%M%S)-$$"
echo "[$REPO_NAME] Creating branch $BRANCH from main"

git checkout -b "$BRANCH" --quiet

# Restore stashed changes
if git stash list | grep -q "quickmerge-$$"; then
    git stash pop --quiet
fi

# Stage: --files for selective add, else add all
sync 2>/dev/null || true
sleep 0.5
if [ -n "$FILES_ARG" ]; then
    ADDED_ANY=0
    for f in $FILES_ARG; do
        if [ -e "$f" ]; then
            git add "$f"
            ADDED_ANY=1
        else
            echo "[$REPO_NAME] ⚠️  Path not found: $f"
        fi
    done
    if [ "$ADDED_ANY" = 0 ]; then
        echo "[$REPO_NAME] ❌ No valid paths from --files. Nothing to commit."
        exit 1
    fi
else
    git add -A
fi
git commit -m "$COMMIT_MSG" --quiet

git push -u origin "$BRANCH" --quiet 2>/dev/null

# Create PR with auto-merge
# Extract issue references from commit message for PR body
ISSUE_REFS=$(echo "$COMMIT_MSG" | grep -o -E "(Fixes|Closes|Resolves) [^#]*#[0-9]+" || echo "")
PR_BODY="Automated PR. Will auto-merge once quality gates pass.

$ISSUE_REFS"

PR_URL=$(gh pr create \
    --title "$COMMIT_MSG" \
    --body "$PR_BODY" \
    --base main \
    --head "$BRANCH" 2>/dev/null)

PR_NUM=$(echo "$PR_URL" | grep -o "[0-9]*$")
gh pr merge "$PR_NUM" --auto --squash --delete-branch 2>/dev/null || true

echo "[$REPO_NAME] PR created: $PR_URL (auto-merge enabled)"

# STAY ON PR BRANCH (enhanced workflow - don't return to main)
# This allows you to continue working while CI runs
echo "[$REPO_NAME] Staying on branch $BRANCH (PR will auto-merge when CI passes)"
echo "[$REPO_NAME] Current branch: $BRANCH"
echo "[$REPO_NAME] To sync with main later: git checkout main && git pull"
