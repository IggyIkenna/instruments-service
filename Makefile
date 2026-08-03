# Makefile for instruments-service
# Thin wrapper around scripts/quality-gates.sh — the ONLY sanctioned entrypoint
# for lint/type-check/tests (workspace hard rule: never run pytest/ruff/
# basedpyright directly). MIN_COVERAGE lives in scripts/quality-gates.sh, not here.
#
# Usage:
#   make ci-local    # Run all CI checks locally (matches GitHub Actions)
#   make lint        # Run linting only
#   make test        # Run tests only
#   make type-check  # Run type checking only (lint+typecheck, no tests)

.PHONY: ci-local lint test type-check help install

# Default target
help:
	@echo "Local CI Testing for instruments-service (mirrors GitHub Actions)"
	@echo ""
	@echo "Available targets:"
	@echo "  ci-local    - Run all CI checks locally (recommended before push)"
	@echo "  install     - Install dependencies"
	@echo "  lint        - Run ruff linting and formatting via quality-gates.sh"
	@echo "  type-check  - Run basedpyright via quality-gates.sh"
	@echo "  test        - Run tests with coverage via quality-gates.sh"
	@echo "  help        - Show this help message"
	@echo ""
	@echo "Example: make ci-local"

# Install dependencies
install:
	@echo "Installing dependencies..."
	pip install uv
	uv pip install --system -e "."

# Main target that mirrors CI exactly
ci-local:
	@echo "Running CI checks locally via scripts/quality-gates.sh..."
	bash scripts/quality-gates.sh --no-fix

# Ruff linting (matches CI exactly)
lint:
	@echo "Running lint via scripts/quality-gates.sh..."
	bash scripts/quality-gates.sh --no-fix --lint

# Type checking (matches CI)
type-check:
	@echo "Running type-check via scripts/quality-gates.sh..."
	QG_SLICE=typecheck bash scripts/quality-gates.sh --no-fix

# Tests with coverage (matches CI coverage threshold: MIN_COVERAGE in scripts/quality-gates.sh)
test:
	@echo "Running tests via scripts/quality-gates.sh..."
	bash scripts/quality-gates.sh --no-fix --test

# Individual targets for debugging
lint-fix:
	@echo "Auto-fixing linting issues via scripts/quality-gates.sh..."
	bash scripts/quality-gates.sh --fix --lint

# Security scan
security:
	@echo "Running pip-audit security scan..."
	pip-audit
# Fix import patterns
.PHONY: fix-imports
fix-imports:
	@echo "🔧 Fixing import patterns..."
	@python3 .cursor/scripts/check-import-patterns.py --fix

# Check import patterns
.PHONY: check-imports
check-imports:
	@echo "🔍 Checking import patterns..."
	@python3 .cursor/scripts/check-import-patterns.py --verbose
