#!/bin/bash
# quickmerge: Push changes through a PR with auto-merge
#
# Usage:
#   ./scripts/quickmerge.sh "commit message"
#
# What it does:
#   1. Creates a timestamped branch from main
#   2. Commits all staged/unstaged changes
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
git checkout main --quiet 2>/dev/null || true
git pull --quiet 2>/dev/null || true

# Create branch, commit, push
git checkout -b "$BRANCH" --quiet
git add -A
git commit --no-verify -m "$COMMIT_MSG" --quiet

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

# Return to main
git checkout main --quiet 2>/dev/null || true
