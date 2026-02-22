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
#   2. Stashes changes, creates new branch from origin/main (without checking out local main)
#   3. Restores stashed changes onto new branch
#   4. Stages changes (--files or -A), commits
#   5. Pushes branch, creates PR with auto-merge (squash)
#   6. Stays on PR branch (does NOT checkout main)
#
# Prerequisites:
#   - gh CLI installed and authenticated (gh auth login)
#   - Auto-merge enabled on the repo (Settings > General > Allow auto-merge)
#
# Notes:
#   - If quickmerge fails and you fix it: run quickmerge again directly. Do NOT
#     run quality gates first—quickmerge already runs quality gates and pre-commit fixes.

set -e

# Source Cursor Team Kit enforcement prompts (optional but recommended)
WORKSPACE_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
if [ -f "$WORKSPACE_ROOT/.cursor/scripts/cursor-team-kit-enforcement.sh" ]; then
    source "$WORKSPACE_ROOT/.cursor/scripts/cursor-team-kit-enforcement.sh"
    CURSOR_TEAM_KIT_ENABLED=1
else
    CURSOR_TEAM_KIT_ENABLED=0
fi

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

# Activate venv before quality gates (local parity with CI python + deps).
# macOS/Linux: .venv/bin/activate  |  Windows: .venv/Scripts/activate
VENV_ACTIVATED=0
if [ -f ".venv/bin/activate" ]; then
    source .venv/bin/activate
    VENV_ACTIVATED=1
    echo "[$REPO_NAME] Using .venv (Python $(python --version 2>&1))"
elif [ -f ".venv/Scripts/activate" ]; then
    source .venv/Scripts/activate
    VENV_ACTIVATED=1
    echo "[$REPO_NAME] Using .venv (Python $(python --version 2>&1))"
else
    echo "[$REPO_NAME] ⚠️  No .venv found - using system Python. Consider: python -m venv .venv && source .venv/bin/activate"
fi

# Install project dependencies before quality gates.
# Use UV only (never pip except bootstrap). See .cursor/rules/uv-package-manager.mdc
if [ -f "pyproject.toml" ]; then
    echo "[$REPO_NAME] Installing project dependencies..."
    command -v uv >/dev/null 2>&1 || pip install uv --quiet
    if [ "$REPO_NAME" = "unified-cloud-services" ]; then
        uv pip install -e ".[databento]" || uv pip install -e ".[dev]" || uv pip install -e . || true
    elif [ "$REPO_NAME" = "instruments-service" ]; then
        uv pip install -e . --no-deps || uv pip install -e . || true
    elif [ "$REPO_NAME" = "execution-services" ]; then
        uv pip install -e ".[dev]" --no-deps || uv pip install -e ".[dev]" || uv pip install -e . || true
    elif [ "$REPO_NAME" = "unified-trading-deployment-v2" ]; then
        uv pip install -e ".[dev]" || uv pip install -e . || true
        uv pip install fastapi || true
    else
        uv pip install -e ".[dev]" || uv pip install -e . || true
    fi
fi

# Check for changes
if [ -z "$(git status --porcelain)" ]; then
    echo "No changes to commit in $REPO_NAME"
    exit 0
fi

# Cursor Team Kit: Prompt for deslop (code cleanup)
if [ "$CURSOR_TEAM_KIT_ENABLED" = 1 ]; then
    prompt_deslop "$REPO_NAME"
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

# Cursor Team Kit: Prompt for smoke tests (UI repos only)
if [ "$CURSOR_TEAM_KIT_ENABLED" = 1 ]; then
    prompt_smoke_tests "$REPO_NAME"
fi

# Get current branch and check if we have changes
CURRENT_BRANCH=$(git branch --show-current)
RESTORE_STASH=0

# Stash ALL changes (including untracked) before switching branches
if [ -n "$(git status --porcelain)" ]; then
    echo "[$REPO_NAME] Stashing changes..."
    git stash push -u -m "quickmerge-$$" --quiet
    RESTORE_STASH=1
fi

# Sync with origin/main without checking out (don't lose current position)
git fetch origin main --quiet

# Create timestamped branch FROM origin/main (not local main)
BRANCH="auto/$(date +%Y%m%d-%H%M%S)-$$"
echo "[$REPO_NAME] Creating branch $BRANCH from origin/main"

git checkout -b "$BRANCH" origin/main --quiet

# Restore stashed changes on new branch
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
echo "[$REPO_NAME] Staying on branch $BRANCH (PR will auto-merge when CI passes)"
echo "[$REPO_NAME] Current branch: $BRANCH"
echo "[$REPO_NAME] To sync with main later: git checkout main && git pull"

# Cursor Team Kit: Show ci-watcher instructions
if [ "$CURSOR_TEAM_KIT_ENABLED" = 1 ]; then
    show_ci_watcher_prompt "$REPO_NAME" "$PR_NUM" "$PR_URL"
fi

# DON'T checkout main yet - PR hasn't merged!
# Stay on PR branch so you can keep working without losing changes
# When PR merges, you can: git checkout main && git pull
