# Makefile for instruments-service
# This mirrors the exact CI checks for local testing consistency
#
# CRITICAL: This must match .github/workflows/quality-gates.yml exactly
#
# Usage:
#   make ci-local    # Run all CI checks locally (matches GitHub Actions)
#   make lint        # Run linting only
#   make test        # Run tests only
#   make type-check  # Run type checking only

.PHONY: ci-local lint test type-check help install

# Default target
help:
	@echo "Local CI Testing for instruments-service (mirrors GitHub Actions)"
	@echo ""
	@echo "Available targets:"
	@echo "  ci-local    - Run all CI checks locally (recommended before push)"
	@echo "  install     - Install dependencies"
	@echo "  lint        - Run ruff linting and formatting"
	@echo "  type-check  - Run basedpyright with timeout"
	@echo "  test        - Run tests with coverage"
	@echo "  help        - Show this help message"
	@echo ""
	@echo "Example: make ci-local"

# Install dependencies
install:
	@echo "Installing dependencies..."
	pip install uv
	uv pip install --system -e ".[dev]"

# Main target that mirrors CI exactly
ci-local:
	@echo "Running CI checks locally..."
	@echo "============================="
	@echo ""
	@echo "Step 1: Ruff linting"
	@$(MAKE) lint
	@echo ""
	@echo "Step 2: Type checking (with timeout)"
	@$(MAKE) type-check
	@echo ""
	@echo "Step 3: Tests with coverage"
	@$(MAKE) test
	@echo ""
	@echo "✅ All CI checks passed!"

# Ruff linting (matches CI exactly)
lint:
	@echo "Running ruff check and format..."
	ruff check . --output-format=text
	ruff format . --check

# Type checking with timeout (CRITICAL: matches CI timeout)
type-check:
	@echo "Running basedpyright with timeout..."
	# CRITICAL: Use timeout to prevent hanging (must match CI)
	timeout 60 basedpyright instruments_service/ || (echo "Type check failed or timed out" && exit 1)

# Tests with coverage (matches CI coverage threshold)
test:
	@echo "Running tests with coverage..."
	pytest --cov=instruments_service --cov-fail-under=35

# Individual targets for debugging
lint-fix:
	@echo "Auto-fixing linting issues..."
	ruff format .
	ruff check . --fix

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
