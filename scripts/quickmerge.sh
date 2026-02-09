#!/bin/bash
# quickmerge: Push changes through a PR with auto-merge
#
# Usage:
#   ./scripts/quickmerge.sh "commit message"
#
# What it does:
#   1. Creates a timestamped branch from main
#   2. Commits all changes (pre-commit hooks run — ruff format, linting, etc.)
#   3. Pushes the branch
#   4. Creates a PR with auto-merge enabled (squash)
#   5. Returns to main branch
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

# Create branch
BRANCH="auto/$(date +%Y%m%d-%H%M%S)-$$"
echo "[$REPO_NAME] Creating branch $BRANCH"

# Ensure on main and up to date
echo "[$REPO_NAME] Staying on branch $BRANCH (PR will auto-merge when CI passes)"
echo "[$REPO_NAME] Current branch: $BRANCH"
echo "[$REPO_NAME] To sync with main later: git checkout main && git pull"
git pull --quiet 2>/dev/null || true

# Create branch, commit, push
# NOTE: No --no-verify. Pre-commit hooks (ruff, linting) run on commit.
git checkout -b "$BRANCH" --quiet
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
