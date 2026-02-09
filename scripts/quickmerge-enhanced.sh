#!/bin/bash
#
# Enhanced Quickmerge Script
#
# Creates a PR with auto-merge enabled while safely handling:
# - Working on PR branches (syncs with main)
# - Uncommitted changes (stashes and reapplies)
# - Merge conflicts (stops and alerts)
# - Stays on PR branch (never returns to main)
#
# Usage:
#   bash scripts/quickmerge-enhanced.sh "commit message"
#
# Before running:
#   1. Make code changes
#   2. Write unit tests
#   3. Run tests locally and verify they pass
#

set -euo pipefail

REPO_NAME=$(basename "$(git rev-parse --show-toplevel)")
TIMESTAMP=$(date -u +"%Y%m%d-%H%M%S")
RANDOM_SUFFIX=$(openssl rand -hex 3)
BRANCH_NAME="auto/${TIMESTAMP}-${RANDOM_SUFFIX}"

# Color output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}[$REPO_NAME]${NC} Enhanced Quickmerge starting..."

# Check for commit message
if [ $# -eq 0 ]; then
    echo -e "${RED}Error: Commit message required${NC}"
    echo "Usage: bash scripts/quickmerge-enhanced.sh \"commit message\""
    exit 1
fi

COMMIT_MSG="$1"

# Get current branch
CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
echo -e "${GREEN}[$REPO_NAME]${NC} Current branch: ${CURRENT_BRANCH}"

# Check if there are uncommitted changes
if ! git diff-index --quiet HEAD --; then
    echo -e "${YELLOW}[$REPO_NAME]${NC} Uncommitted changes detected, stashing..."
    git stash push -m "quickmerge-temp-stash-${TIMESTAMP}"
    STASHED=true
else
    STASHED=false
fi

# Switch to main and pull latest
echo -e "${GREEN}[$REPO_NAME]${NC} Syncing with main..."
git checkout main
git pull origin main

# Check if we can apply stashed changes without conflicts
if [ "$STASHED" = true ]; then
    echo -e "${YELLOW}[$REPO_NAME]${NC} Reapplying stashed changes..."
    if ! git stash pop; then
        echo -e "${RED}[$REPO_NAME]${NC} MERGE CONFLICT detected!"
        echo -e "${RED}Your stashed changes conflict with updated main.${NC}"
        echo -e "${YELLOW}Resolve conflicts, then run:${NC}"
        echo "  git add -A"
        echo "  git stash drop"
        echo "  bash scripts/quickmerge-enhanced.sh \"$COMMIT_MSG\""
        exit 1
    fi
fi

# Create new branch from updated main
echo -e "${GREEN}[$REPO_NAME]${NC} Creating branch ${BRANCH_NAME}"
git checkout -b "${BRANCH_NAME}"

# Stage all changes
echo -e "${GREEN}[$REPO_NAME]${NC} Staging changes..."
git add -A

# Check if there are changes to commit
if git diff-index --quiet HEAD --; then
    echo -e "${YELLOW}[$REPO_NAME]${NC} No changes to commit"
    git checkout main
    exit 0
fi

# Commit (pre-commit hooks run here)
echo -e "${GREEN}[$REPO_NAME]${NC} Committing..."
git commit -m "$COMMIT_MSG"

# Push branch
echo -e "${GREEN}[$REPO_NAME]${NC} Pushing branch..."
git push -u origin "${BRANCH_NAME}"

# Create PR with auto-merge
echo -e "${GREEN}[$REPO_NAME]${NC} Creating PR..."
if gh pr create --fill --head "${BRANCH_NAME}" --base main; then
    echo -e "${GREEN}[$REPO_NAME]${NC} Enabling auto-merge (squash)..."
    gh pr merge --auto --squash "${BRANCH_NAME}"

    # Get PR URL
    PR_URL=$(gh pr view "${BRANCH_NAME}" --json url -q .url 2>/dev/null || echo "")
    if [ -n "$PR_URL" ]; then
        echo -e "${GREEN}[$REPO_NAME]${NC} PR created: ${PR_URL}"
    fi
else
    echo -e "${RED}[$REPO_NAME]${NC} Failed to create PR"
    exit 1
fi

# STAY ON PR BRANCH (don't return to main)
echo -e "${GREEN}[$REPO_NAME]${NC} Staying on branch ${BRANCH_NAME}"
echo -e "${YELLOW}You are now on: ${BRANCH_NAME}${NC}"
echo -e "${YELLOW}PR will auto-merge when CI passes${NC}"
echo -e "${YELLOW}Continue working on this branch or wait for merge${NC}"
echo ""
echo -e "${GREEN}To sync with main later:${NC}"
echo "  bash scripts/quickmerge-enhanced.sh \"next commit message\""
echo ""
echo -e "${GREEN}To manually check PR status:${NC}"
echo "  gh pr view ${BRANCH_NAME}"
