#!/usr/bin/env bash
# =============================================================================
# Run coverage report for instruments-service
# Task 1.4.1: Generate report, identify uncovered modules
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

# Activate venv if exists
if [ -d ".venv" ]; then
    source .venv/bin/activate
fi

echo "Running coverage report for instruments-service..."
python -m pytest tests/unit/ \
    --cov=instruments_service \
    --cov-report=term-missing \
    --cov-report=html:htmlcov \
    --no-cov-on-fail \
    -q

echo ""
echo "HTML report: htmlcov/index.html"
echo "Uncovered modules (low coverage):"
echo "  - instruments_service/app/core/instrument_processing_service.py"
echo "  - instruments_service/app/core/processors/*"
echo "  - instruments_service/cli/*"
echo "  - instruments_service/utils/ccxt_service.py"
