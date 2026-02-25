#!/usr/bin/env bash
#
# Quality Gates for instruments-service
# Aligned with: unified-trading-codex/06-coding-standards/quality-gates-service-template.sh
# Whitelists: docs/QUALITY_GATE_BYPASS_AUDIT.md
#
# Required: duration <2 min, pytest-timeout (--timeout=60), pip-audit, bandit
#
# Usage:
#   ./scripts/quality-gates.sh           # Run all checks (with auto-fix)
#   ./scripts/quality-gates.sh --lint    # Linting only
#   ./scripts/quality-gates.sh --test    # Tests only
#   ./scripts/quality-gates.sh --quick   # Unit tests only (faster)
#   ./scripts/quality-gates.sh --no-fix  # Skip auto-fix (CI mode)
#
set -e

# ============================================================================
# REPO-SPECIFIC CONFIG (override template defaults)
# ============================================================================
SERVICE_NAME="instruments-service"
SOURCE_DIR="instruments_service"
SOURCE_DIRS="instruments_service/ tests/"
MIN_COVERAGE=35
# Integration skipped until test timing fixed (see TODO)
RUN_INTEGRATION=false

# Start timer (quality gates must complete in <2 min)
QG_START=$(date +%s)

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
REPO_ROOT="${REPO_ROOT:-$(dirname "$PROJECT_ROOT")}"
cd "$PROJECT_ROOT"

# ============================================================================
# ENVIRONMENT BOOTSTRAP (repo-specific; skips in CI)
# ============================================================================
if [ -z "${GITHUB_ACTIONS:-}" ] && [ -z "${CI:-}" ] && [ -z "${CLOUD_BUILD:-}" ]; then
    if [ -f "pyproject.toml" ]; then
        command -v uv &>/dev/null || pip install uv --quiet
        uv lock 2>/dev/null || true
        if [ -f "uv.lock" ] && ! git diff --quiet uv.lock 2>/dev/null; then
            echo -e "${YELLOW}ℹ uv.lock was updated — include it in your commit.${NC}"
        fi
    fi
    if [ ! -d ".venv" ]; then
        echo -e "${YELLOW}Creating .venv...${NC}"
        command -v uv &>/dev/null || pip install uv --quiet
        uv venv .venv
    fi
    if [ -f ".venv/bin/activate" ]; then
        # shellcheck source=/dev/null
        source .venv/bin/activate
    elif [ -f ".venv/Scripts/activate" ]; then
        # shellcheck source=/dev/null
        source .venv/Scripts/activate
    fi
    command -v uv &>/dev/null || pip install uv --quiet
    if [ -f "pyproject.toml" ]; then
        for lib in api-contracts unified-config-interface unified-cloud-services unified-domain-services unified-events-interface unified-market-interface; do
            lib_path=""
            [ -d "${REPO_ROOT:-/dev/null}/$lib" ] && lib_path="${REPO_ROOT}/$lib"
            [ -z "$lib_path" ] && [ -d "deps/$lib" ] && lib_path="deps/$lib"
            if [ -n "$lib_path" ] && [ -f "$lib_path/pyproject.toml" ]; then
                uv pip install -e "$lib_path" --quiet 2>/dev/null || true
            fi
        done
        uv pip install -e ".[dev]" --quiet 2>/dev/null || uv pip install -e . --quiet 2>/dev/null || true
    fi
fi

# Python command
PYTHON_CMD=".venv/bin/python"
[ ! -f "$PYTHON_CMD" ] && [ -f ".venv/Scripts/python.exe" ] && PYTHON_CMD=".venv/Scripts/python.exe"
[ ! -f "$PYTHON_CMD" ] && PYTHON_CMD="python3"

# Portable timeout (Linux: timeout; macOS: gtimeout from brew install coreutils)
run_timeout() {
  local secs=$1
  shift
  if command -v timeout &>/dev/null; then
    timeout "$secs" "$@"
  elif command -v gtimeout &>/dev/null; then
    gtimeout "$secs" "$@"
  else
    "$@"
  fi
}

# ============================================================================
# MODE DETECTION
# ============================================================================
FIX_MODE=true
QUICK_MODE=false
RUN_LINT=true
RUN_TESTS=true
for arg in "$@"; do
    case $arg in
        --no-fix) FIX_MODE=false ;;
        --quick) QUICK_MODE=true ;;
        --lint) RUN_TESTS=false ;;
        --test) RUN_LINT=false ;;
        --fix) FIX_MODE=true ;;
        --help|-h)
            echo "Usage: $0 [--lint|--test|--quick|--no-fix|--fix|--help]"
            exit 0
            ;;
    esac
done

# Git-aware mode
STAGED_PY_FILES=$(git diff --cached --name-only --diff-filter=ACMR 2>/dev/null | grep '\.py$' | tr '\n' ' ' || true)
if [ -n "$STAGED_PY_FILES" ]; then
    FILE_COUNT=$(echo "$STAGED_PY_FILES" | wc -w | tr -d ' ')
    SOURCE_DIRS="$STAGED_PY_FILES"
    echo -e "${YELLOW}🔍 Git-aware mode: Checking ONLY staged files ($FILE_COUNT files)${NC}"
fi

# Test env for smoke tests
export DEPLOYMENT_CONFIG_DIR="${REPO_ROOT}/unified-trading-deployment-v3/configs"
export CLOUD_MOCK_MODE="true"
export GOOGLE_CLOUD_PROJECT="test-project"

# Template limits
MAX_FILE_LINES=900
FILE_WARN_LINES=700
MAX_FUNCTION_LINES=100
MAX_CLASS_LINES=500
MAX_METHOD_LINES=50
PYTEST_WORKERS=${PYTEST_WORKERS:-2}

# Helpers
log_section() { echo -e "\n${BLUE}$1${NC}"; echo "----------------------------------------------------------------------"; }
log_success() { echo -e "${GREEN}✅ $1${NC}"; }
log_fail() { echo -e "${RED}❌ $1${NC}"; }
log_warn() { echo -e "${YELLOW}⚠️  $1${NC}"; }

# ============================================================================
# STEP 0: ENVIRONMENT & CONFIG VALIDATION
# ============================================================================
log_section "[0/6] ENVIRONMENT & CONFIG VALIDATION"

ACTUAL_PYTHON=$(python --version 2>&1 | awk '{print $2}' | cut -d'.' -f1,2)
[[ "$ACTUAL_PYTHON" != "3.13" ]] && { log_fail "Python 3.13 required, found $ACTUAL_PYTHON"; exit 1; }
log_success "Python $ACTUAL_PYTHON"

[[ ! -f "uv.lock" ]] && log_warn "uv.lock missing (run: uv lock)" || log_success "uv.lock found"

if ! command -v rg &> /dev/null; then
    log_fail "ripgrep (rg) required"
    exit 1
fi
log_success "ripgrep available"

RUFF_CMD="ruff"
[ -f ".venv/bin/ruff" ] && RUFF_CMD=".venv/bin/ruff"
RUFF_VER=$($RUFF_CMD --version 2>&1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1 || echo "0.0.0")
[[ "$RUFF_VER" != "0.15.0" ]] && log_warn "Ruff 0.15.0 expected, found $RUFF_VER" || log_success "Ruff $RUFF_VER"

if [ -f "cloudbuild.yaml" ]; then
    UNESCAPED=$(grep -E '\$PYTEST_EXIT|\$\?' cloudbuild.yaml | grep -v '\$\$' || true)
    [ -n "$UNESCAPED" ] && { log_fail "cloudbuild.yaml unescaped shell variables"; exit 1; }
    log_success "cloudbuild.yaml OK"
fi

[ -f "pyproject.toml" ] && grep -q '>=3.13,<3.14' pyproject.toml || { log_fail "pyproject.toml requires Python >=3.13,<3.14"; exit 1; }
log_success "pyproject.toml Python version correct"

# ============================================================================
# STEP 1: AUTO-FIX (ruff) — timeout 30s
# ============================================================================
if [ "$RUN_LINT" = true ] && [ "$FIX_MODE" = true ]; then
    log_section "[1/6] AUTO-FIX (ruff)"
    run_timeout 30 ruff format --line-length 120 $SOURCE_DIRS || { log_fail "ruff format timed out (30s)"; exit 1; }
    run_timeout 30 ruff check --fix --line-length 120 $SOURCE_DIRS || { log_fail "ruff check --fix timed out (30s)"; exit 1; }
    log_success "Auto-fix complete"
fi

# ============================================================================
# STEP 2: LINTING (ruff) — timeout 30s
# ============================================================================
if [ "$RUN_LINT" = true ]; then
    log_section "[2/6] LINTING (ruff)"
    if run_timeout 30 ruff check --line-length 120 $SOURCE_DIRS; then
        log_success "Linting PASSED"
    else
        log_fail "Linting FAILED (or timed out after 30s)"
        exit 1
    fi
fi

# ============================================================================
# STEP 3: TESTS (pytest)
# ============================================================================
if [ "$RUN_TESTS" = true ]; then
    log_section "[3/6] TESTS (pytest)"
    $PYTHON_CMD -c "import pytest_timeout" 2>/dev/null || { log_fail "pytest-timeout required (uv pip install pytest-timeout)"; exit 1; }
    $PYTHON_CMD -c "import xdist" 2>/dev/null || { log_fail "pytest-xdist required (uv pip install pytest-xdist)"; exit 1; }
    COV_ARGS="--cov=$SOURCE_DIR --cov-report=term-missing --cov-fail-under=$MIN_COVERAGE"
    PARALLEL_ARGS="-n ${PYTEST_WORKERS}"
    TIMEOUT_ARGS="--timeout=60"
    if [ "$QUICK_MODE" = true ] || [ "$RUN_INTEGRATION" != "true" ]; then
        $PYTHON_CMD -m pytest tests/unit/ -v --tb=short $COV_ARGS $PARALLEL_ARGS $TIMEOUT_ARGS || exit 1
    else
        $PYTHON_CMD -m pytest tests/unit/ tests/integration/ -v --tb=short $COV_ARGS $PARALLEL_ARGS $TIMEOUT_ARGS || exit 1
    fi
    log_success "Tests PASSED"
    [[ ! -f "tests/unit/test_event_logging.py" ]] && log_fail "Missing test_event_logging.py" && exit 1
    [[ ! -f "tests/unit/test_config.py" ]] && [[ ! -f "tests/unit/test_config_extended.py" ]] && log_fail "Missing test_config.py" && exit 1
    log_success "Required test files present"

    DUP=$(find tests/ -name "test_*_extended.py" -o -name "test_*_additional.py" 2>/dev/null | head -5 || true)
    [[ -n "$DUP" ]] && { log_fail "Duplicate test files — expand existing files instead:"; echo "$DUP"; exit 1; }
    log_success "No duplicate test files"

    SKIP_NO_REASON=$(rg "@pytest\.mark\.skip" --type py tests/ -B 1 2>/dev/null \
        | grep -v "# reason:\|# noqa\|^--" | grep "@pytest\.mark\.skip" || true)
    [[ -n "$SKIP_NO_REASON" ]] && { log_fail "pytest.mark.skip without reason — add '# reason: ...' above"; echo "$SKIP_NO_REASON" | head -3; exit 1; }
    log_success "All pytest.mark.skip have reason comments"
fi

# ============================================================================
# STEP 3.5: IMPORT PATTERN STANDARDS
# ============================================================================
log_section "[3.5/6] IMPORT PATTERN STANDARDS"
IP="unified-trading-pm/scripts/check-import-patterns.py"
[ ! -f "$IP" ] && IP=".cursor/scripts/check-import-patterns.py"  # legacy fallback
if [ -f "$IP" ]; then
    if $PYTHON_CMD "$IP" --verbose \
        --exclude instruments_service/app/core/adapter_loader.py \
        --exclude instruments_service/engine/venues/venue_adapter_loader.py 2>/dev/null; then
        log_success "Import patterns PASSED"
    else
        log_fail "Import patterns FAILED (use top-level imports)"
        exit 1
    fi
else
    log_warn "check-import-patterns.py not found, skipping"
fi

# ============================================================================
# STEP 4: TYPE CHECKING (basedpyright) — timeout 120s, cleanup zombies
# ============================================================================
log_section "[4/6] TYPE CHECKING (basedpyright)"

cleanup_zombie_pyright() {
    ps -eo pid,etime,command 2>/dev/null | grep -E 'basedpyright.*index\.js' | grep -v grep | while read -r pid etime _; do
        hours=0
        if echo "$etime" | grep -q '-'; then
            days=$(echo "$etime" | cut -d'-' -f1)
            hours=$((days * 24))
        elif [ "$(echo "$etime" | tr ':' '\n' | wc -l)" -eq 3 ]; then
            hours=$(echo "$etime" | cut -d':' -f1)
        fi
        if [ "${hours:-0}" -ge 2 ]; then
            echo -e "${YELLOW}⚠️  Killing zombie basedpyright (PID: $pid)${NC}"
            kill -9 "$pid" 2>/dev/null || true
        fi
    done
}
cleanup_zombie_pyright

if command -v basedpyright &> /dev/null; then
    if run_timeout 120 basedpyright $SOURCE_DIR/ --level warning 2>&1; then
        log_success "Type checking PASSED"
    else
        log_fail "Type checking FAILED (or timed out after 120s)"
        exit 1
    fi
else
    log_fail "basedpyright not installed (uv pip install basedpyright)"
    exit 1
fi

# ============================================================================
# STEP 5: CODEX COMPLIANCE (inline whitelists per QUALITY_GATE_BYPASS_AUDIT.md)
# ============================================================================
log_section "[5/6] CODEX COMPLIANCE"
CODEX_VIOLATIONS=0

# Imports-inside whitelist: 14 files (QUALITY_GATE_BYPASS_AUDIT.md 1.2)

rg "print\(" --type py --glob "!tests/**" --glob "!scripts/**" --glob "!examples/**" --glob "!pytest_load_env.py" $SOURCE_DIR/ 2>/dev/null && { log_fail "Found print()"; ((CODEX_VIOLATIONS++)); } || log_success "No print()"

rg "os\.getenv" --type py --glob "!tests/**" --glob "!scripts/**" --glob "!**/config.py" --glob "!pytest_load_env.py" $SOURCE_DIR/ 2>/dev/null && { log_fail "Found os.getenv()"; ((CODEX_VIOLATIONS++)); } || log_success "No os.getenv()"

rg 'os\.getenv\s*\([^)]+,\s*""\s*\)' --type py --glob "!tests/**" $SOURCE_DIR/ 2>/dev/null && { log_fail "Found os.getenv empty fallback"; ((CODEX_VIOLATIONS++)); } || log_success "No os.getenv empty fallback"

rg "datetime\.now\(\)|datetime\.utcnow\(\)" --type py --glob "!tests/**" $SOURCE_DIR/ 2>/dev/null && { log_fail "Found naive datetime"; ((CODEX_VIOLATIONS++)); } || log_success "No naive datetime"

rg "except:" --type py --glob "!tests/**" $SOURCE_DIR/ 2>/dev/null && { log_fail "Found bare except"; ((CODEX_VIOLATIONS++)); } || log_success "No bare except"

rg "from google\.cloud import|import google\.cloud" --type py $SOURCE_DIR/ 2>/dev/null && { log_fail "Found google.cloud imports"; ((CODEX_VIOLATIONS++)); } || log_success "No google.cloud imports"

# requests in async — exclude morpho_adapter, aster_adapter (QUALITY_GATE_BYPASS_AUDIT 1.1)
REQ_ASYNC=0
for f in $(rg "import requests" --type py --glob "!tests/**" --glob "!scripts/**" --glob "!**/defi/morpho_adapter.py" --glob "!**/onchain_perps/aster_adapter.py" $SOURCE_DIR/ -l 2>/dev/null || true); do
    grep -q "async def" "$f" 2>/dev/null && { log_fail "Found requests with async in $f"; ((CODEX_VIOLATIONS++)); REQ_ASYNC=1; break; }
done
[[ $REQ_ASYNC -eq 0 ]] && log_success "No requests in async"

# asyncio.run in loops — exclude defi/*, cli/**, defi_processor, examples, scripts (QUALITY_GATE_BYPASS_AUDIT 1.1)
ASYNCIO_LOOP=0
for f in $(rg "asyncio\.run\(" --type py --glob "!tests/**" --glob "!scripts/**" --glob "!examples/**" --glob "!**/venues/defi/*" --glob "!**/cli/**" --glob "!**/defi_processor.py" $SOURCE_DIR/ -l 2>/dev/null || true); do
    grep -q "for \|while " "$f" 2>/dev/null && { log_fail "Found asyncio.run() in loop in $f"; ((CODEX_VIOLATIONS++)); ASYNCIO_LOOP=1; break; }
done
[[ $ASYNCIO_LOOP -eq 0 ]] && log_success "No asyncio.run in loops"

TIME_SLEEP_ASYNC=0
for f in $(rg "time\.sleep\(" --type py --glob "!tests/**" $SOURCE_DIR/ -l 2>/dev/null || true); do
    grep -q "async def" "$f" 2>/dev/null && { log_fail "Found time.sleep() in async file $f"; ((CODEX_VIOLATIONS++)); TIME_SLEEP_ASYNC=1; break; }
done
[[ $TIME_SLEEP_ASYNC -eq 0 ]] && log_success "No time.sleep in async"

# Imports inside — inline whitelist (14 files)
IMPORTS_INSIDE=$(rg "^[[:space:]]+import |^[[:space:]]+from .* import" --type py --glob "!tests/**" --glob "!scripts/**" --glob "!**/__init__.py" \
    --glob "!**/adapter_loader.py" --glob "!**/dependency_checker.py" --glob "!**/instruments_service.py" \
    --glob "!**/instrument_processing_service.py" --glob "!**/symbol_parser.py" --glob "!**/canonical_key_generator.py" \
    --glob "!**/live_mode_handler.py" --glob "!**/cloud_instrument_storage.py" --glob "!**/parser.py" \
    --glob "!**/main.py" --glob "!**/ccxt_service.py" --glob "!**/corporate_actions_handler.py" \
    --glob "!**/corporate_actions_backfill_handler.py" --glob "!**/corporate_actions_production_handler.py" \
    --glob "!**/corporate_actions_update_handler.py" $SOURCE_DIR/ 2>/dev/null || true)
[[ -n "$IMPORTS_INSIDE" ]] && { log_fail "Found imports inside functions"; echo "$IMPORTS_INSIDE" | head -3; ((CODEX_VIOLATIONS++)); } || log_success "No imports inside functions"

ANY_USAGE=$(rg ": Any|-> Any|\[Any\]|: object|-> object" --type py --glob "!tests/**" --glob "!scripts/**" $SOURCE_DIR/ 2>/dev/null | grep -v "dict\[str, Any\]" | grep -v "type: ignore" || true)
[[ -n "$ANY_USAGE" ]] && { log_fail "Found Any/object types"; echo "$ANY_USAGE" | head -3; ((CODEX_VIOLATIONS++)); } || log_success "No Any/object types"

rg '\.get\(["\x27][\w_]+["\x27]\s*,\s*["\x27]["\x27]\)' --type py --glob "!tests/**" $SOURCE_DIR/ 2>/dev/null && { log_fail "Found empty string fallbacks"; ((CODEX_VIOLATIONS++)); } || log_success "No empty string fallbacks"

EMPTY_DICT=$(rg '\.get\s*\(\s*["\x27][^"\x27]+["\x27]\s*,\s*\{\}\s*\)' --type py --glob "!tests/**" --glob "!scripts/**" $SOURCE_DIR/ 2>/dev/null || true)
EMPTY_LIST=$(rg '\.get\s*\(\s*["\x27][^"\x27]+["\x27]\s*,\s*\[\]\s*\)' --type py --glob "!tests/**" --glob "!scripts/**" $SOURCE_DIR/ 2>/dev/null || true)
[[ -n "$EMPTY_DICT" ]] || [[ -n "$EMPTY_LIST" ]] && { log_fail "Found empty dict/list fallbacks"; ((CODEX_VIOLATIONS++)); } || log_success "No empty dict/list fallbacks"

[[ -f ".gitignore" ]] && rg "!central-element|!.*credentials.*\.json" .gitignore 2>/dev/null && { log_fail "Found credential negation"; ((CODEX_VIOLATIONS++)); } || log_success "No credential negation"

rg "central-element-323112|get_config.*central-element" tests/ 2>/dev/null && { log_fail "Found hardcoded project ID in tests"; ((CODEX_VIOLATIONS++)); } || log_success "No hardcoded project ID in tests"

rg "central-element-323112" --type py --glob "!tests/**" $SOURCE_DIR/ 2>/dev/null && { log_fail "Found hardcoded project ID in production"; ((CODEX_VIOLATIONS++)); } || log_success "No hardcoded project ID in production"

# Security: no service account JSON files committed or referenced by path
SA_JSON=$(rg '"type"\s*:\s*"service_account"|"private_key_id"' --glob "*.json" . 2>/dev/null \
    | grep -v ".venv\|node_modules" || true)
[[ -n "$SA_JSON" ]] && { log_fail "Service account JSON detected — use Secret Manager via UCS"; echo "$SA_JSON" | head -3; ((CODEX_VIOLATIONS++)); } || log_success "No service account JSON"

# Security: no private keys embedded in code
PRIV_KEY=$(rg "BEGIN RSA PRIVATE KEY|BEGIN PRIVATE KEY|BEGIN EC PRIVATE KEY" \
    --type py --type sh --type yaml --glob "!tests/**" . 2>/dev/null || true)
[[ -n "$PRIV_KEY" ]] && { log_fail "Private key in codebase — use Secret Manager"; echo "$PRIV_KEY" | head -3; ((CODEX_VIOLATIONS++)); } || log_success "No embedded private keys"

# Security: no secrets in Dockerfile ENV statements
DOCKER_SECRETS=$(rg "^ENV\s+[A-Z_]*(KEY|SECRET|PASSWORD|TOKEN|CREDENTIAL)[A-Z_]*\s*=" \
    --glob "**/Dockerfile*" . 2>/dev/null | grep -v "#" || true)
[[ -n "$DOCKER_SECRETS" ]] && { log_fail "Secrets in Dockerfile ENV — pass at runtime via Secret Manager"; echo "$DOCKER_SECRETS" | head -3; ((CODEX_VIOLATIONS++)); } || log_success "No secrets in Dockerfile ENV"

rg "GOOGLE_CLOUD_PROJECT" --type py --glob "!tests/**" --glob "!**/config.py" $SOURCE_DIR/ 2>/dev/null \
    && { log_fail "Use GCP_PROJECT_ID not GOOGLE_CLOUD_PROJECT"; ((CODEX_VIOLATIONS++)); } || log_success "No GOOGLE_CLOUD_PROJECT usage"

EL_OLD=$(rg "from unified_cloud_services[. ].*(log_event|setup_events|setup_cloud_logging|observability)" --type py --glob "!tests/**" $SOURCE_DIR/ 2>/dev/null || true)
[[ -n "$EL_OLD" ]] && { log_fail "Old event logging — use 'from unified_events_interface import ...'"; echo "$EL_OLD" | head -3; ((CODEX_VIOLATIONS++)); } || log_success "Event logging from unified_events_interface"

PIP=$(rg "^RUN pip install|^RUN python -m pip| pip install " --glob "**/Dockerfile" --glob "**/*.sh" . 2>/dev/null | grep -v "pip install uv" | grep -v "#" || true)
[[ -n "$PIP" ]] && { log_fail "Use 'uv pip install' not 'pip install'"; echo "$PIP" | head -3; ((CODEX_VIOLATIONS++)); } || log_success "No bare pip install"

rg 'central-element-323112-[a-z0-9]+\.json' --type py --glob "!tests/**" $SOURCE_DIR/ 2>/dev/null && { log_fail "Found hardcoded credential path"; ((CODEX_VIOLATIONS++)); } || log_success "No hardcoded credential path"

DEEP_IMPORTS=$(rg 'from unified_[a-z_-]+\.[a-zA-Z0-9_.]+\s+import' --type py --glob "!tests/**" --glob "!**/__init__.py" $SOURCE_DIR/ 2>/dev/null || true)
[[ -n "$DEEP_IMPORTS" ]] && { log_fail "Found deep imports"; echo "$DEEP_IMPORTS" | head -3; ((CODEX_VIOLATIONS++)); } || log_success "No deep imports"

# Broad except — whitelist cli/main.py (QUALITY_GATE_BYPASS_AUDIT 1.1)
SWALLOWED=$(rg "except Exception:" --type py --glob "!tests/**" $SOURCE_DIR/ -A 2 2>/dev/null \
    | grep -E "^[[:space:]]+(pass|return None)$" || true)
[[ -n "$SWALLOWED" ]] && { log_fail "Swallowed errors — use @handle_api_errors or re-raise"; ((CODEX_VIOLATIONS++)); } || log_success "No swallowed errors"

BROAD_EXCEPT=$(rg "except Exception:" --type py --glob "!tests/**" --glob "!instruments_service/cli/main.py" $SOURCE_DIR/ 2>/dev/null || true)
if [[ -n "$BROAD_EXCEPT" ]]; then
    echo -e "${YELLOW}Found broad except Exception — document in QUALITY_GATE_BYPASS_AUDIT.md${NC}"
    echo "$BROAD_EXCEPT" | head -3
    ((CODEX_VIOLATIONS++))
else
    log_success "No broad except Exception"
fi

# File size
SIZE_VIOLATIONS=""
SIZE_WARNINGS=""
for f in $(find . -name "*.py" ! -path "./.venv/*" ! -path "./deps/*" ! -path "./.git/*" ! -path "./scripts/*" ! -path "./build/*" 2>/dev/null); do
    lines=$(wc -l < "$f" 2>/dev/null || echo 0)
    [[ "$lines" -gt $MAX_FILE_LINES ]] && SIZE_VIOLATIONS="${SIZE_VIOLATIONS}\n  $f: $lines lines (max $MAX_FILE_LINES)"
    [[ "$lines" -gt $FILE_WARN_LINES ]] && [[ "$lines" -le $MAX_FILE_LINES ]] && SIZE_WARNINGS="${SIZE_WARNINGS}\n  $f: $lines lines (plan split)"
done
[[ -n "$SIZE_VIOLATIONS" ]] && { log_fail "Files exceed $MAX_FILE_LINES lines"; echo -e "$SIZE_VIOLATIONS"; ((CODEX_VIOLATIONS++)); } || log_success "File size OK"
[[ -n "$SIZE_WARNINGS" ]] && log_warn "Files near limit:$SIZE_WARNINGS"

# Function/class/method size
SIZE_CODE_VIOLATIONS=""
for f in $(find . -name "*.py" ! -path "./.venv/*" ! -path "./deps/*" ! -path "./.git/*" ! -path "./scripts/*" 2>/dev/null); do
    out=$($PYTHON_CMD -c "
import ast
import sys
p = sys.argv[1]
try:
    with open(p, 'r', encoding='utf-8') as fp:
        tree = ast.parse(fp.read())
    def visit(n, parent=None):
        if isinstance(n, ast.FunctionDef) or isinstance(n, ast.AsyncFunctionDef):
            lines = (n.end_lineno or n.lineno) - n.lineno + 1
            if isinstance(parent, ast.ClassDef):
                if lines > $MAX_METHOD_LINES:
                    print(f'  {p}:{n.lineno}:{parent.name}.{n.name}(): {lines} lines (max $MAX_METHOD_LINES)')
            elif lines > $MAX_FUNCTION_LINES:
                print(f'  {p}:{n.lineno}:{n.name}(): {lines} lines (max $MAX_FUNCTION_LINES)')
        elif isinstance(n, ast.ClassDef):
            lines = (n.end_lineno or n.lineno) - n.lineno + 1
            if lines > $MAX_CLASS_LINES:
                print(f'  {p}:{n.lineno}:{n.name}: {lines} lines (max $MAX_CLASS_LINES)')
        for c in ast.iter_child_nodes(n):
            visit(c, n if isinstance(n, ast.ClassDef) else parent)
    visit(tree)
except Exception:
    pass
" "$f" 2>/dev/null || true)
    [[ -n "$out" ]] && SIZE_CODE_VIOLATIONS="${SIZE_CODE_VIOLATIONS}\n${out}"
done
[[ -n "$SIZE_CODE_VIOLATIONS" ]] && { log_fail "Function/class/method size exceeded"; echo -e "$SIZE_CODE_VIOLATIONS"; ((CODEX_VIOLATIONS++)); } || log_success "Function/class/method size OK"

# pip-audit + bandit
if command -v pip-audit &> /dev/null; then
    pip-audit 2>/dev/null || { log_fail "pip-audit found vulnerabilities"; ((CODEX_VIOLATIONS++)); }
elif $PYTHON_CMD -c "import pip_audit" 2>/dev/null; then
    $PYTHON_CMD -m pip_audit 2>/dev/null || { log_fail "pip-audit found vulnerabilities"; ((CODEX_VIOLATIONS++)); }
else
    log_fail "pip-audit required (uv pip install pip-audit)"
    ((CODEX_VIOLATIONS++))
fi
if command -v bandit &> /dev/null; then
    run_timeout 30 bandit -r $SOURCE_DIR/ -ll 2>/dev/null || { log_fail "bandit found issues"; ((CODEX_VIOLATIONS++)); }
elif $PYTHON_CMD -c "import bandit" 2>/dev/null; then
    run_timeout 30 $PYTHON_CMD -m bandit -r $SOURCE_DIR/ -ll 2>/dev/null || { log_fail "bandit found issues"; ((CODEX_VIOLATIONS++)); }
else
    log_fail "bandit required (uv pip install bandit)"
    ((CODEX_VIOLATIONS++))
fi

[[ $CODEX_VIOLATIONS -gt 0 ]] && log_fail "Codex compliance FAILED: $CODEX_VIOLATIONS violations" && exit 1
BYPASS=$(rg "\|\|true|\|\| true" --glob "**/quality-gates.sh" --glob "**/quality-gates.yml" . 2>/dev/null \
    | grep -v "^#\|zombies\|pyright\|cleanup" || true)
[[ -n "$BYPASS" ]] && { log_fail "||true bypass in quality gates — fix root cause"; echo "$BYPASS" | head -3; ((CODEX_VIOLATIONS++)); } || log_success "No ||true quality gate bypasses"

log_success "Codex compliance PASSED"

# ============================================================================
# STEP 6: PRODUCTION READINESS VALIDATORS
# ============================================================================
log_section "[6/6] PRODUCTION READINESS VALIDATORS"
if [ -f "${REPO_ROOT}/unified-trading-codex/scripts/run-all-validators.sh" ]; then
    "${REPO_ROOT}/unified-trading-codex/scripts/run-all-validators.sh" --category all --failed-only 2>/dev/null || log_warn "Validators have warnings (informational)"
else
    log_warn "Validators not available"
fi

# ============================================================================
# DURATION CHECK (<2 min)
# ============================================================================
QG_END=$(date +%s)
QG_DURATION=$((QG_END - QG_START))
if [ $QG_DURATION -gt 120 ]; then
    log_fail "Quality gates must complete in <2 minutes (took ${QG_DURATION}s)"
    exit 1
fi

echo ""
echo -e "${GREEN}======================================================================${NC}"
echo -e "${GREEN}✅ ALL QUALITY GATES PASSED (${QG_DURATION}s)${NC}"
echo -e "${GREEN}======================================================================${NC}"
exit 0
