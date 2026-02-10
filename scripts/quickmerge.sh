#!/bin/bash
# quickmerge: Push changes through a PR with auto-merge
#
# Usage:
#   ./scripts/quickmerge.sh "commit message"
#   ./scripts/quickmerge.sh "commit message" --files "path1 path2 path3"
#
# When --files is provided: only stage and commit those paths (repo-relative).
# When --files is omitted: stage all changes (git add -A).
#
# Agents MUST use --files with the list of files they changed to avoid
# committing other agents' partial work in multi-agent sessions.
#
# What it does:
#   1. Runs quality gates FIRST (scripts/quality-gates.sh)
#      - If quality gates fail, script exits immediately (fail fast)
#   2. Stashes changes, checkouts main, pulls latest
#   3. Creates timestamped branch FROM main (avoids merge conflicts)
#   4. Reapplies stashed changes, stages (--files or -A), commits
#   5. Pushes branch, creates PR with auto-merge (squash)
#   6. Returns to main and pull
#
# Prerequisites:
#   - gh CLI installed and authenticated (gh auth login)
#   - Auto-merge enabled on the repo (Settings > General > Allow auto-merge)
#
# Notes:
#   - If quickmerge fails and you fix it: run quickmerge again directly. Do NOT
#     run quality gates first—quickmerge already runs quality gates and pre-commit fixes.

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

# Check for changes
if [ -z "$(git status --porcelain)" ]; then
    echo "No changes to commit in $REPO_NAME"
    exit 0
fi

# Early exit: if identical to main, nothing to merge (main already past quality gates)
git fetch origin main --quiet 2>/dev/null || true
if git rev-parse origin/main &>/dev/null && [ -z "$(git diff origin/main 2>/dev/null)" ]; then
    echo "[$REPO_NAME] No differences from main - nothing to merge"
    exit 0
fi

# Run quality gates in two phases: (1) auto-fix ruff format/lint, (2) verify
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

# Stash when dirty, sync with main, create branch from main, reapply
RESTORE_STASH=0
if [ -n "$(git status --porcelain)" ]; then
    echo "[$REPO_NAME] Stashing changes and syncing with main..."
    git stash push -u -m "quickmerge-$$" --quiet
    RESTORE_STASH=1
fi

git checkout main --quiet
git pull origin main --quiet

BRANCH="auto/$(date +%Y%m%d-%H%M%S)-$$"
echo "[$REPO_NAME] Creating branch $BRANCH from main"

git checkout -b "$BRANCH" --quiet

# Restore stashed changes
if [ "$RESTORE_STASH" = 1 ] && git stash list | grep -q "quickmerge-$$"; then
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
    if [ -z "$(git diff --cached --name-only)" ]; then
        echo "[$REPO_NAME] ❌ No changes in --files paths. Nothing to commit."
        exit 1
    fi
else
    git add -A
fi

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

git checkout main --quiet 2>/dev/null || true
git pull origin main --quiet 2>/dev/null || true
