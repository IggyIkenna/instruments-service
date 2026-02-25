#!/bin/bash
# quickmerge: Push changes through a PR with auto-merge
#
# Canonical SSOT template for service-with-deps repos.
# Base: unified-cloud-services (most advanced deployed version).
#
# Usage:
#   ./scripts/quickmerge.sh "commit message"
#   ./scripts/quickmerge.sh "commit message" --files "path1 path2 path3"
#   ./scripts/quickmerge.sh "commit message" --dep-branch "my-feature"
#   ./scripts/quickmerge.sh "commit message" --auto-branch
#   ./scripts/quickmerge.sh "Revert: bad change" --rollback
#   ./scripts/quickmerge.sh "commit message" --quick
#   ./scripts/quickmerge.sh "commit message" --dev
#
# Flags:
#   --files "path1 path2"     Stage only these paths (repo-relative)
#   --dep-branch "name"       Use branch isolation (deps differ from main)
#   --auto-branch             Auto-generate branch: prefix-slug-timestamp
#   --rollback                Git revert mode (revert last commit, create PR)
#   --rollback-cascade        Revert + cascade to dependent repos
#   --rollback-deploy-only    Revert + deploy only (skip quality gates, push)
#   --quick                   Skip pre-flight audit and Act simulation
#   --dev / --prod            Set ENVIRONMENT (overrides .env)
#   --skip-tests              Pass --skip-tests to quality gates
#
# Mutual exclusions:
#   --dep-branch + --auto-branch  BLOCKED
#   --rollback + --dep-branch     BLOCKED
#   --rollback + --auto-branch   BLOCKED
#   --rollback + --quick         BLOCKED (rollback requires full gates)
#
# Pipeline stages:
#   1. Dependency validation (blocking)
#   2. Pre-flight audit (pre-flight-audit.sh) — SKIPPED with --quick
#   3. Local quality gates + Cloud Build validator (if cloudbuild.yaml modified)
#   4. Act simulation (act -j quality-gates) — SKIPPED with --quick
#   5. Create PR branch, commit, push, create PR
#
# Branch end state:
#   main path (no --dep-branch): checkout main after push
#   dep-branch path: stay on branch (PR will auto-merge when CI passes)
#
# Prerequisites:
#   - gh CLI installed and authenticated (gh auth login)
#   - Auto-merge enabled on the repo (Settings > General > Allow auto-merge)
#   - pre-flight-audit.sh at $WORKSPACE_ROOT/.cursor/scripts/pre-flight-audit.sh

set -e

# -----------------------------------------------------------------------------
# Setup: workspace root, repo dir
# -----------------------------------------------------------------------------
REPO_DIR="$(git rev-parse --show-toplevel 2>/dev/null || (cd "$(dirname "$0")/.." && pwd))"
REPO_NAME=$(basename "$REPO_DIR")
WORKSPACE_ROOT="$(cd "$REPO_DIR/.." && pwd)"

# Cursor Team Kit (disabled for automation)
CURSOR_TEAM_KIT_ENABLED=0

# -----------------------------------------------------------------------------
# Parse arguments
# -----------------------------------------------------------------------------
COMMIT_MSG="chore: automated update"
FILES_ARG=""
DEP_BRANCH=""
AUTO_BRANCH=false
ROLLBACK=""
ROLLBACK_CASCADE=false
ROLLBACK_DEPLOY_ONLY=false
QUICK=false
SKIP_TESTS=""
ENV_FLAG=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --files)
            FILES_ARG="$2"
            shift 2
            ;;
        --dep-branch)
            DEP_BRANCH="$2"
            shift 2
            ;;
        --auto-branch)
            AUTO_BRANCH=true
            shift
            ;;
        --rollback)
            ROLLBACK="revert"
            shift
            ;;
        --rollback-cascade)
            ROLLBACK="revert"
            ROLLBACK_CASCADE=true
            shift
            ;;
        --rollback-deploy-only)
            ROLLBACK="revert"
            ROLLBACK_DEPLOY_ONLY=true
            shift
            ;;
        --quick)
            QUICK=true
            shift
            ;;
        --skip-tests)
            SKIP_TESTS="--skip-tests"
            shift
            ;;
        --dev)
            ENV_FLAG="development"
            shift
            ;;
        --prod)
            ENV_FLAG="production"
            shift
            ;;
        *)
            COMMIT_MSG="$1"
            shift
            ;;
    esac
done

# -----------------------------------------------------------------------------
# Flag validation (mutual exclusions)
# -----------------------------------------------------------------------------
if [ -n "$DEP_BRANCH" ] && [ "$AUTO_BRANCH" = true ]; then
    echo "ERROR: --dep-branch and --auto-branch are mutually exclusive"
    exit 1
fi
if [ -n "$ROLLBACK" ] && { [ -n "$DEP_BRANCH" ] || [ "$AUTO_BRANCH" = true ]; }; then
    echo "ERROR: --rollback is mutually exclusive with --dep-branch and --auto-branch"
    exit 1
fi
if [ "$QUICK" = true ] && [ -n "$ROLLBACK" ]; then
    echo "ERROR: --rollback requires full quality gates (no --quick)"
    exit 1
fi

cd "$REPO_DIR"

# -----------------------------------------------------------------------------
# Early exit: no changes vs origin/main (skip for rollback)
# -----------------------------------------------------------------------------
if [ -z "$ROLLBACK" ]; then
    git fetch origin main --quiet 2>/dev/null || true
    if [ -z "$(git status --porcelain)" ] && git diff origin/main --quiet 2>/dev/null; then
        echo "[$REPO_NAME] Nothing to commit — exiting fast"
        exit 0
    fi
fi

# -----------------------------------------------------------------------------
# Environment: .env or --dev/--prod
# -----------------------------------------------------------------------------
if [ -n "$ENV_FLAG" ]; then
    export ENVIRONMENT="$ENV_FLAG"
    echo "[$REPO_NAME] ENVIRONMENT=$ENVIRONMENT (from flag)"
elif [ -f ".env" ]; then
    export $(grep -v '^#' .env | xargs) 2>/dev/null || true
    export ENVIRONMENT="${ENVIRONMENT:-production}"
    echo "[$REPO_NAME] ENVIRONMENT=$ENVIRONMENT (from .env)"
else
    # Don't set ENVIRONMENT - let branch detection block set it
    :
fi

# -----------------------------------------------------------------------------
# Auto-detect environment: branch builds always use dev project
# -----------------------------------------------------------------------------
CURRENT_BRANCH=$(git branch --show-current 2>/dev/null || echo "main")
if [ -z "${ENVIRONMENT:-}" ]; then
    if [ "$CURRENT_BRANCH" = "main" ] || [ "${ENV_FLAG:-}" = "production" ]; then
        export ENVIRONMENT="production"
    else
        export ENVIRONMENT="development"
        export GCP_PROJECT_ID="${GCP_PROJECT_ID_DEV:-${GCP_PROJECT_ID:-}}"
        echo "[$REPO_NAME] 🟡 BRANCH MODE: using dev project (branch: $CURRENT_BRANCH)"
    fi
fi
echo "[$REPO_NAME] ENVIRONMENT=${ENVIRONMENT:-production}"

# -----------------------------------------------------------------------------
# Venv and dependencies (skip for rollback-deploy-only)
# -----------------------------------------------------------------------------
if [ "$ROLLBACK_DEPLOY_ONLY" != true ]; then
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
        echo "[$REPO_NAME] ⚠️  No .venv found - using system Python"
    fi

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
fi

# -----------------------------------------------------------------------------
# ROLLBACK MODE: revert last commit on main, create PR
# -----------------------------------------------------------------------------
if [ -n "$ROLLBACK" ]; then
    echo "=========================================="
    echo "ROLLBACK MODE: $REPO_NAME"
    echo "=========================================="

    git fetch origin main --quiet 2>/dev/null || true
    BRANCH="revert/$(date +%Y%m%d-%H%M)"

    if [ "$ROLLBACK_DEPLOY_ONLY" = true ]; then
        echo "[$REPO_NAME] --rollback-deploy-only: skipping quality gates"
        git checkout -b "$BRANCH" origin/main --quiet
        if ! git revert HEAD --no-edit 2>/dev/null; then
            echo "[$REPO_NAME] ❌ Nothing to revert on main"
            git checkout main --quiet 2>/dev/null || true
            exit 1
        fi
        git push -u origin "$BRANCH" --quiet
        gh pr create --title "${COMMIT_MSG:-Revert: last commit}" --body "Rollback (deploy-only)" --base main --head "$BRANCH" 2>/dev/null || true
        PR_NUM=$(gh pr list --head "$BRANCH" -q .number 2>/dev/null)
        [ -n "$PR_NUM" ] && gh pr merge "$PR_NUM" --auto --squash --delete-branch 2>/dev/null || true
        echo "[$REPO_NAME] Rollback PR created (deploy-only)"
    else
        # Full rollback: revert first, then run quality gates on reverted state
        git checkout -b "$BRANCH" origin/main --quiet
        if ! git revert HEAD --no-edit 2>/dev/null; then
            echo "[$REPO_NAME] ❌ Nothing to revert on main"
            git checkout main --quiet 2>/dev/null || true
            exit 1
        fi
        if [ -f "scripts/quality-gates.sh" ]; then
            bash scripts/quality-gates.sh $SKIP_TESTS
            bash scripts/quality-gates.sh --no-fix $SKIP_TESTS || { echo "Quality gates failed"; exit 1; }
        fi
        git push -u origin "$BRANCH" --quiet
        gh pr create --title "${COMMIT_MSG:-Revert: last commit}" --body "Rollback" --base main --head "$BRANCH" 2>/dev/null || true
        PR_NUM=$(gh pr list --head "$BRANCH" -q .number 2>/dev/null)
        [ -n "$PR_NUM" ] && gh pr merge "$PR_NUM" --auto --squash --delete-branch 2>/dev/null || true
        echo "[$REPO_NAME] Rollback PR created"
    fi

    if [ "$ROLLBACK_CASCADE" = true ] && [ -f ".dependency-matrix.json" ]; then
        echo "[$REPO_NAME] --rollback-cascade: run quickmerge --rollback in dependent repos:"
        jq -r '.dependencies[].name' .dependency-matrix.json 2>/dev/null | while read dep; do
            [ -d "$WORKSPACE_ROOT/$dep" ] && echo "  cd $WORKSPACE_ROOT/$dep && bash scripts/quickmerge.sh 'Revert: cascade from $REPO_NAME' --rollback"
        done
    fi

    git checkout main --quiet 2>/dev/null || true
    git pull --quiet 2>/dev/null || true
    exit 0
fi

# -----------------------------------------------------------------------------
# STAGE 1: Dependency Validation
# -----------------------------------------------------------------------------
echo "=========================================="
echo "STAGE 1: Dependency Validation"
echo "=========================================="
echo ""

if [ -f ".dependency-matrix.json" ]; then
    DEPS=$(jq -r '.dependencies[].name' .dependency-matrix.json 2>/dev/null || echo "")

    if [ -n "$DEPS" ]; then
        echo "Checking dependencies vs origin/main..."
        HAS_DIFF=false

        for dep in $DEPS; do
            dep_path="$WORKSPACE_ROOT/$dep"
            if [ -d "$dep_path" ]; then
                cd "$dep_path"
                git fetch origin main --quiet 2>/dev/null || true
                if ! git diff origin/main --quiet 2>/dev/null; then
                    HAS_DIFF=true
                    echo "❌ $dep: DIFFERS from main"
                else
                    echo "✅ $dep: Matches main"
                fi
                cd "$REPO_DIR"
            fi
        done
        echo ""

        if [ "$HAS_DIFF" = "true" ] && [ -z "$DEP_BRANCH" ]; then
            echo "═══════════════════════════════════════════════════════════════"
            echo "❌ DEPENDENCY CONFLICT DETECTED"
            echo "═══════════════════════════════════════════════════════════════"
            echo ""
            echo "Dependencies differ from main, but no --dep-branch specified."
            echo ""
            echo "Option 1: DISCARD local dependency changes"
            echo "  cd <dep_path> && git reset --hard origin/main"
            echo ""
            echo "Option 2: USE BRANCH ISOLATION (recommended)"
            echo "  bash scripts/quickmerge.sh \"$COMMIT_MSG\" --dep-branch \"my-feature\""
            echo ""
            echo "Option 3: AUTO-GENERATE BRANCH NAME"
            echo "  bash scripts/quickmerge.sh \"$COMMIT_MSG\" --auto-branch"
            echo ""
            echo "═══════════════════════════════════════════════════════════════"
            exit 1
        fi

        if [ -n "$DEP_BRANCH" ]; then
            echo "✅ --dep-branch specified: $DEP_BRANCH"
        fi
    else
        echo "✅ No dependencies found"
    fi
else
    echo "✅ No .dependency-matrix.json (no dependencies)"
fi

echo ""

# -----------------------------------------------------------------------------
# STAGE 2: Pre-flight Audit — SKIPPED with --quick
# -----------------------------------------------------------------------------
if [ "$QUICK" != true ] && [ -f "$WORKSPACE_ROOT/.cursor/scripts/pre-flight-audit.sh" ]; then
    echo "=========================================="
    echo "STAGE 2: Pre-flight Audit"
    echo "=========================================="
    echo ""
    if bash "$WORKSPACE_ROOT/.cursor/scripts/pre-flight-audit.sh" "$REPO_NAME"; then
        echo "[$REPO_NAME] ✅ Pre-flight audit PASSED"
    else
        echo "[$REPO_NAME] ❌ Pre-flight audit FAILED"
        exit 1
    fi
    echo ""
elif [ "$QUICK" = true ]; then
    echo "[$REPO_NAME] --quick: Skipping pre-flight audit"
    echo ""
fi

# -----------------------------------------------------------------------------
# Auto-branch name generation
# -----------------------------------------------------------------------------
if [ "$AUTO_BRANCH" = true ] && [ -z "$DEP_BRANCH" ]; then
    PREFIX=$(echo "$COMMIT_MSG" | grep -oE '^(feat|fix|refactor|chore|docs|test|style|perf|ci|build)' || echo "auto")
    SLUG=$(echo "$COMMIT_MSG" | sed 's/^[^:]*: //' | tr ' ' '-' | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9-]//g' | cut -c1-20)
    TIMESTAMP=$(date +%Y%m%d-%H%M)
    DEP_BRANCH="${PREFIX}-${SLUG}-${TIMESTAMP}"
    echo "[$REPO_NAME] Auto-branch: $DEP_BRANCH"
fi

# -----------------------------------------------------------------------------
# Check for changes (non-rollback path)
# -----------------------------------------------------------------------------
if [ -z "$(git status --porcelain)" ]; then
    echo "[$REPO_NAME] No changes to commit"
    exit 0
fi

# -----------------------------------------------------------------------------
# STAGE 3: Local Quality Gates + Cloud Build validator
# -----------------------------------------------------------------------------
echo "=========================================="
echo "STAGE 3: Local Quality Gates"
echo "=========================================="
echo ""

if [ -f "scripts/quality-gates.sh" ]; then
    echo "[$REPO_NAME] Phase 1: Running quality gates (auto-fix)..."
    bash scripts/quality-gates.sh $SKIP_TESTS
    echo "[$REPO_NAME] Phase 2: Verifying quality gates (--no-fix)..."
    if ! bash scripts/quality-gates.sh --no-fix $SKIP_TESTS; then
        echo "[$REPO_NAME] ❌ Quality gates FAILED"
        exit 1
    fi

    # Cloud Build validator (if cloudbuild.yaml modified)
    if [ -f "cloudbuild.yaml" ] && command -v gcloud &>/dev/null; then
        if git diff --name-only HEAD 2>/dev/null | grep -q "cloudbuild.yaml"; then
            echo "[$REPO_NAME] Validating cloudbuild.yaml..."
            if gcloud meta validate-yaml cloudbuild.yaml 2>/dev/null; then
                echo "[$REPO_NAME] ✅ Cloud Build YAML valid"
            else
                echo "[$REPO_NAME] ⚠️  Cloud Build validation failed (non-blocking)"
            fi
        fi
    fi

    echo "[$REPO_NAME] ✅ Quality gates PASSED"
else
    echo "[$REPO_NAME] ⚠️  No quality-gates.sh found (skipping)"
fi

echo ""

# -----------------------------------------------------------------------------
# STAGE 4: Act simulation — SKIPPED with --quick
# -----------------------------------------------------------------------------
if [ "$QUICK" != true ] && command -v act &>/dev/null; then
    echo "=========================================="
    echo "STAGE 4: Act Simulation (GitHub Actions)"
    echo "=========================================="
    echo ""
    ACT_SECRETS=""
    [ -f ~/.secrets ] && ACT_SECRETS="--secret-file ~/.secrets"
    if act -j quality-gates $ACT_SECRETS 2>/dev/null; then
        echo "[$REPO_NAME] ✅ Act simulation PASSED"
    else
        echo "[$REPO_NAME] ⚠️  Act simulation failed or act not configured (continuing)"
    fi
    echo ""
elif [ "$QUICK" = true ]; then
    echo "[$REPO_NAME] --quick: Skipping Act simulation"
    echo ""
fi

# -----------------------------------------------------------------------------
# STAGE 5: Create PR Branch, Commit, Push
# -----------------------------------------------------------------------------
echo "=========================================="
echo "STAGE 5: Create PR Branch"
echo "=========================================="
echo ""

RESTORE_STASH=0
if [ -n "$(git status --porcelain)" ]; then
    echo "[$REPO_NAME] Stashing changes..."
    git stash push -u -m "quickmerge-$$" --quiet
    RESTORE_STASH=1
fi

git fetch origin main --quiet 2>/dev/null || true

# Branch name
if [ -n "$DEP_BRANCH" ]; then
    BRANCH="$DEP_BRANCH"
    echo "[$REPO_NAME] Using branch: $BRANCH"
else
    BRANCH="auto/$(date +%Y%m%d-%H%M%S)-$$"
    echo "[$REPO_NAME] Creating auto-generated branch: $BRANCH"
fi

git checkout -b "$BRANCH" origin/main --quiet

if [ "$RESTORE_STASH" = 1 ] && git stash list | grep -q "quickmerge-$$"; then
    git stash pop --quiet
fi

# Stage
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
        echo "[$REPO_NAME] ❌ No valid paths from --files"
        exit 1
    fi
    if [ -z "$(git diff --cached --name-only)" ]; then
        echo "[$REPO_NAME] ❌ No changes in --files paths"
        exit 1
    fi
else
    git add -A
fi

git commit -m "$COMMIT_MSG" --quiet

git push -u origin "$BRANCH" --quiet 2>/dev/null

# Create PR
ISSUE_REFS=$(echo "$COMMIT_MSG" | grep -o -E "(Fixes|Closes|Resolves) [^#]*#[0-9]+" || echo "")
PR_BODY="Automated PR. Will auto-merge once quality gates pass.

$ISSUE_REFS"

PR_URL=$(gh pr create \
    --title "$COMMIT_MSG" \
    --body "$PR_BODY" \
    --base main \
    --head "$BRANCH" 2>/dev/null)

PR_NUM=$(echo "$PR_URL" | grep -o "[0-9]*$" || echo "")
[ -n "$PR_NUM" ] && gh pr merge "$PR_NUM" --auto --squash --delete-branch 2>/dev/null || true

echo "[$REPO_NAME] PR created: $PR_URL (auto-merge enabled)"

# -----------------------------------------------------------------------------
# Branch end state: main path → checkout main; dep-branch path → stay on branch
# -----------------------------------------------------------------------------
if [ -n "$DEP_BRANCH" ]; then
    echo "[$REPO_NAME] Staying on branch $BRANCH (dep-branch path)"
    echo "[$REPO_NAME] To sync with main later: git checkout main && git pull"
else
    echo "[$REPO_NAME] Checking out main..."
    git checkout main --quiet 2>/dev/null || true
    git pull --quiet 2>/dev/null || true
    echo "[$REPO_NAME] On main, up to date"
fi
