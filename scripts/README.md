# Scripts Directory

Utility scripts for instruments-service.

## Available Scripts

### `run_quality_gates.py`

**Purpose**: Run quality gates (tests, coverage, performance tests)

**Usage**:
```bash
# Run all quality gates (coverage threshold: 65%)
python scripts/run_quality_gates.py

# Skip performance tests (faster for development)
python scripts/run_quality_gates.py --skip-performance

# Custom coverage threshold
python scripts/run_quality_gates.py --coverage-threshold 75

# Force GitHub installation (mimics CI/CD workflow)
# Skips local monorepo and PyPI, only uses GitHub Packages/repository
python scripts/run_quality_gates.py --use-github

# Combine options
python scripts/run_quality_gates.py --coverage-threshold 65 --use-github --skip-performance
```

**Arguments**:
- `--coverage-threshold <percentage>`: Minimum coverage percentage required (default: 65%)
- `--skip-performance`: Skip performance tests for faster execution during development
- `--use-github`: Force GitHub installation mode
  - Skips local monorepo (`../unified-cloud-services`) and PyPI
  - Only attempts GitHub Packages and GitHub repository installation
  - Requires `GH_PAT` in `.env` file or environment variable
  - Useful for testing the same installation path as GitHub Actions CI/CD

**Coverage Threshold**: 65% (default)

**Installation Sources** (when `--use-github` is NOT used):
1. Local monorepo (`../unified-cloud-services`) - editable install
2. PyPI (if package is published)
3. GitHub Packages (requires `GH_PAT`)
4. GitHub repository (requires `GH_PAT`)

**Installation Sources** (when `--use-github` IS used):
1. GitHub Packages (requires `GH_PAT` from `.env` or environment)
2. GitHub repository (requires `GH_PAT` from `.env` or environment)

**Environment Variables**:
- `GH_PAT`: GitHub Personal Access Token for accessing private repositories and GitHub Packages
  - Can be set in `instruments-service/.env` file (recommended for local dev)
  - Or exported as environment variable: `export GH_PAT="your_token"`
  - Required scopes: `repo` (Full control), `read:packages` (Download packages)
  - Create token at: https://github.com/settings/tokens/new

---

## Adding New Scripts

When adding new scripts:

1. **Add to this README**: Document purpose, usage, and requirements
2. **Make executable**: `chmod +x scripts/your_script.py`
3. **Add shebang**: `#!/usr/bin/env python3` at top
4. **Add docstring**: Explain purpose and usage
5. **Handle errors**: Use proper error handling and exit codes
