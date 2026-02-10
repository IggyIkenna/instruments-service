#!/usr/bin/env bash
#
# Quality Gates for instruments-service
#
# This script runs the EXACT same checks as GitHub Actions and Cloud Build.
# Run this locally before pushing to catch issues early.
#
# Usage:
#   ./scripts/quality-gates.sh           # Run all checks (with auto-fix)
#   ./scripts/quality-gates.sh --lint    # Linting only (with auto-fix)
#   ./scripts/quality-gates.sh --test    # Tests only
#   ./scripts/quality-gates.sh --quick   # Unit tests only (fast)
#   ./scripts/quality-gates.sh --no-fix  # Skip auto-fix (CI mode)
#
# Requirements:
#   - Python 3.13+
#   - ruff, pytest, pytest-asyncio, pytest-mock installed
#   - unified-cloud-services available (local or via GH_PAT)
#
set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Get script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
REPO_ROOT="$(dirname "$PROJECT_ROOT")"

# Change to project root
cd "$PROJECT_ROOT"

echo -e "${BLUE}======================================================================${NC}"
echo -e "${BLUE}INSTRUMENTS-SERVICE QUALITY GATES${NC}"
echo -e "${BLUE}======================================================================${NC}"
echo -e "Project: ${PROJECT_ROOT}"
echo -e "Python:  $(python3 --version)"
echo ""

# Parse arguments
RUN_LINT=true
RUN_TESTS=true
QUICK_MODE=false
AUTO_FIX=true  # Default to auto-fix for local runs

for arg in "$@"; do
    case $arg in
        --lint)
            RUN_LINT=true
            RUN_TESTS=false
            ;;
        --test)
            RUN_LINT=false
            RUN_TESTS=true
            ;;
        --quick)
            QUICK_MODE=true
            ;;
        --no-fix)
            AUTO_FIX=false
            ;;
        --fix)
            AUTO_FIX=true
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --lint     Run linting only (with auto-fix)"
            echo "  --test     Run tests only"
            echo "  --quick    Run unit tests only (faster)"
            echo "  --fix      Auto-fix linting issues (default)"
            echo "  --no-fix   Skip auto-fix (CI mode)"
            echo "  --help     Show this help message"
            exit 0
            ;;
    esac
done

# Track overall status
LINT_STATUS=0
TEST_STATUS=0
CONFIG_STATUS=0

# Source directories
SOURCE_DIRS="instruments_service/ tests/"

# ============================================================================
# STEP 0: CLOUD BUILD CONFIG VALIDATION
# ============================================================================
echo -e "\n${BLUE}[0/3] CLOUD BUILD CONFIG VALIDATION${NC}"
echo "----------------------------------------------------------------------"

# Check for unescaped shell variables in cloudbuild.yaml
# In Cloud Build YAML, shell variables must be escaped with $$ not $
if [ -f "cloudbuild.yaml" ]; then
    # Check for $PYTEST_EXIT or $? that should be $$PYTEST_EXIT or $$?
    # Exclude lines that already have $$ (properly escaped)
    UNESCAPED=$(grep -E '\$PYTEST_EXIT|\$\?' cloudbuild.yaml | grep -v '\$\$' || true)
    if [ -n "$UNESCAPED" ]; then
        echo -e "${RED}❌ cloudbuild.yaml has unescaped shell variables:${NC}"
        echo "$UNESCAPED"
        echo -e "${YELLOW}Fix: Change \$PYTEST_EXIT to \$\$PYTEST_EXIT and \$? to \$\$?${NC}"
        CONFIG_STATUS=1
    else
        echo -e "${GREEN}✅ cloudbuild.yaml shell variables properly escaped${NC}"
    fi
else
    echo -e "${YELLOW}No cloudbuild.yaml found (skipping)${NC}"
fi

# Check Python version in pyproject.toml matches expected
if [ -f "pyproject.toml" ]; then
    PYTHON_VERSION=$(grep 'requires-python' pyproject.toml | head -1)
    # Expected: >=3.12,<3.14 for most services (or >=3.11,<3.12 for execution-services)
    if echo "$PYTHON_VERSION" | grep -q '>=3.12,<3.14'; then
        echo -e "${GREEN}✅ pyproject.toml Python version correct: $PYTHON_VERSION${NC}"
    elif echo "$PYTHON_VERSION" | grep -q '>=3.11,<3.12'; then
        echo -e "${GREEN}✅ pyproject.toml Python version correct (execution-services): $PYTHON_VERSION${NC}"
    else
        echo -e "${RED}❌ pyproject.toml Python version may be incorrect: $PYTHON_VERSION${NC}"
        echo -e "${YELLOW}Expected: requires-python = \">=3.12,<3.14\" (or >=3.11,<3.12 for execution-services)${NC}"
        CONFIG_STATUS=1
    fi
else
    echo -e "${YELLOW}No pyproject.toml found (skipping)${NC}"
fi

# ============================================================================
# STEP 1: AUTO-FIX (ruff format + ruff check --fix)
# ============================================================================
if [ "$RUN_LINT" = true ] && [ "$AUTO_FIX" = true ]; then
    echo -e "\n${BLUE}[1/3] AUTO-FIX (ruff format + ruff check --fix)${NC}"
    echo "----------------------------------------------------------------------"

    # Check if ruff is installed
    if ! command -v ruff &> /dev/null; then
        echo -e "${YELLOW}Installing ruff...${NC}"
        pip install ruff==0.15.0 --quiet
    fi

    # Auto-format with ruff format
    echo "Running: ruff format $SOURCE_DIRS"
    ruff format $SOURCE_DIRS

    # Auto-fix with ruff check --fix
    echo "Running: ruff check --fix $SOURCE_DIRS"
    ruff check --fix $SOURCE_DIRS

    echo -e "${GREEN}✅ Auto-fix complete${NC}"
fi

# ============================================================================
# STEP 2: LINTING (ruff)
# ============================================================================
if [ "$RUN_LINT" = true ]; then
    echo -e "\n${BLUE}[2/3] LINTING (ruff)${NC}"
    echo "----------------------------------------------------------------------"

    # Check if ruff is installed
    if ! command -v ruff &> /dev/null; then
        echo -e "${YELLOW}Installing ruff...${NC}"
        pip install ruff==0.15.0 --quiet
    fi

    # Run ruff check (same as Cloud Build and GitHub Actions)
    echo "Running: ruff check $SOURCE_DIRS"
    if ruff check $SOURCE_DIRS; then
        echo -e "${GREEN}✅ Linting PASSED${NC}"
    else
        echo -e "${RED}❌ Linting FAILED${NC}"
        LINT_STATUS=1
    fi
fi

# ============================================================================
# STEP 3: TESTS (pytest)
# ============================================================================
if [ "$RUN_TESTS" = true ]; then
    echo -e "\n${BLUE}[3/3] TESTS (pytest)${NC}"
    echo "----------------------------------------------------------------------"

    # Check if pytest is installed
    if ! python3 -c "import pytest" &> /dev/null; then
        echo -e "${YELLOW}Installing pytest...${NC}"
        pip install pytest pytest-asyncio pytest-mock --quiet
    fi

    # Set environment variables for smoke tests
    export DEPLOYMENT_CONFIG_DIR="${REPO_ROOT}/unified-trading-deployment-v2/configs"
    export CLOUD_MOCK_MODE="true"
    export GOOGLE_CLOUD_PROJECT="test-project"

    # Use parallel execution if pytest-xdist available
    if python3 -c "import xdist" 2>/dev/null || python3 -c "import pytest_xdist" 2>/dev/null; then
        PARALLEL_ARGS="-n auto"
    else
        PARALLEL_ARGS=""
    fi

    if [ "$QUICK_MODE" = true ]; then
        # Quick mode: unit tests only
        echo "Running: pytest tests/unit/ -v --tb=short $PARALLEL_ARGS (quick mode)"
        if python3 -m pytest tests/unit/ -v --tb=short $PARALLEL_ARGS; then
            echo -e "${GREEN}✅ Unit tests PASSED${NC}"
        else
            echo -e "${RED}❌ Unit tests FAILED${NC}"
            TEST_STATUS=1
        fi
    else
        # Full mode: all tests (matching GitHub Actions - most exhaustive)

        # Unit tests (parallel with pytest-xdist when available)
        echo -e "\n${YELLOW}Running unit tests...${NC}"
        if [ -d "tests/unit" ]; then
            if python3 -m pytest tests/unit/ -v --tb=short --timeout=60 $PARALLEL_ARGS; then
                echo -e "${GREEN}✅ Unit tests PASSED${NC}"
            else
                echo -e "${RED}❌ Unit tests FAILED${NC}"
                TEST_STATUS=1
            fi
        else
            echo "No unit tests directory found"
        fi

        # Integration tests (excluding performance tests)
        echo -e "\n${YELLOW}Running integration tests...${NC}"
        if [ -d "tests/integration" ]; then
            if python3 -m pytest tests/integration/ -v --tb=short --timeout=120 \
                --ignore=tests/integration/test_performance.py \
                -k "not api and not live and not download"; then
                echo -e "${GREEN}✅ Integration tests PASSED${NC}"
            else
                echo -e "${RED}❌ Integration tests FAILED${NC}"
                TEST_STATUS=1
            fi
        else
            echo "No integration tests directory found"
        fi

        # E2E tests
        echo -e "\n${YELLOW}Running e2e tests...${NC}"
        if [ -d "tests/e2e" ]; then
            if python3 -m pytest tests/e2e/ -v --tb=short --timeout=180; then
                echo -e "${GREEN}✅ E2E tests PASSED${NC}"
            else
                echo -e "${RED}❌ E2E tests FAILED${NC}"
                TEST_STATUS=1
            fi
        else
            echo "No e2e tests directory found"
        fi

        # Smoke tests (shard combinatorics)
        echo -e "\n${YELLOW}Running smoke tests...${NC}"
        if [ -d "tests/smoke" ]; then
            if python3 -m pytest tests/smoke/ -v --tb=short --timeout=180; then
                echo -e "${GREEN}✅ Smoke tests PASSED${NC}"
            else
                echo -e "${RED}❌ Smoke tests FAILED${NC}"
                TEST_STATUS=1
            fi
        else
            echo "No smoke tests directory found"
        fi
    fi
fi

# ============================================================================
# FINAL SUMMARY
# ============================================================================
echo ""
echo -e "${BLUE}======================================================================${NC}"
echo -e "${BLUE}QUALITY GATES SUMMARY${NC}"
echo -e "${BLUE}======================================================================${NC}"

OVERALL_STATUS=0

if [ $CONFIG_STATUS -eq 0 ]; then
    echo -e "Config:   ${GREEN}✅ PASSED${NC}"
else
    echo -e "Config:   ${RED}❌ FAILED${NC}"
    OVERALL_STATUS=1
fi

if [ "$RUN_LINT" = true ]; then
    if [ $LINT_STATUS -eq 0 ]; then
        echo -e "Linting:  ${GREEN}✅ PASSED${NC}"
    else
        echo -e "Linting:  ${RED}❌ FAILED${NC}"
        OVERALL_STATUS=1
    fi
fi

if [ "$RUN_TESTS" = true ]; then
    if [ $TEST_STATUS -eq 0 ]; then
        echo -e "Tests:    ${GREEN}✅ PASSED${NC}"
    else
        echo -e "Tests:    ${RED}❌ FAILED${NC}"
        OVERALL_STATUS=1
    fi
fi

echo -e "${BLUE}======================================================================${NC}"

if [ $OVERALL_STATUS -eq 0 ]; then
    echo -e "\n${GREEN}✅ ALL QUALITY GATES PASSED - Safe to push!${NC}\n"
else
    echo -e "\n${RED}❌ QUALITY GATES FAILED - Fix issues before pushing${NC}\n"
fi

exit $OVERALL_STATUS
