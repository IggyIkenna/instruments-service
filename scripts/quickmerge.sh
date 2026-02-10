#!/bin/bash
# quickmerge: Push changes through a PR with auto-merge
#
# Usage:
#   ./scripts/quickmerge.sh "commit message"
#
# What it does:
#   1. Stashes changes, checkouts main, pulls latest
#   2. Creates timestamped branch FROM main (avoids merge conflicts)
#   3. Reapplies stashed changes, commits (pre-commit hooks run)
#   4. Pushes branch, creates PR with auto-merge (squash)
#   5. Stays on PR branch
#
# The PR auto-merges once quality gates pass.
# The branch is auto-deleted after merge.
#
# Prerequisites:
#   - gh CLI installed and authenticated (gh auth login)
#   - Auto-merge enabled on the repo (Settings > General > Allow auto-merge)

set -e

COMMIT_MSG="${1:-chore: automated update}"
REPO_DIR="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
REPO_NAME=$(basename "$REPO_DIR")

cd "$REPO_DIR"

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
git pull origin main --quiet

BRANCH="auto/$(date +%Y%m%d-%H%M%S)-$$"
echo "[$REPO_NAME] Creating branch $BRANCH from main"

git checkout -b "$BRANCH" --quiet

# Restore stashed changes
if git stash list | grep -q "quickmerge-$$"; then
    git stash pop --quiet
fi

# Commit and push
# NOTE: No --no-verify. Pre-commit hooks (ruff, linting) run on commit.
# Flush filesystem buffers to ensure all editor saves are on disk
# This prevents stale file versions from being committed when an IDE
# (e.g., Cursor, VSCode) has pending writes in its buffer
sync 2>/dev/null || true
sleep 0.5
git add -A
git commit -m "$COMMIT_MSG" --quiet

git push -u origin "$BRANCH" --quiet 2>/dev/null

# Create PR with auto-merge
PR_URL=$(gh pr create \
    --title "$COMMIT_MSG" \
    --body "Automated PR. Will auto-merge once quality gates pass." \
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
