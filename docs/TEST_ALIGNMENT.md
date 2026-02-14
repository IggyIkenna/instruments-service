# Test Alignment: Local, GitHub Actions, Cloud Build

This document ensures all three test environments run **exactly the same tests** with **exactly the same commands**.

## ✅ Standardized Commands

All three environments now use:

- **Pytest**: `python -m pytest` (or `python3 -m pytest` for local)
- **Same test filters**: `--ignore=tests/integration/test_performance.py -k "not api and not live and not download"`
- **Same timeouts**: `--timeout=60` (unit), `--timeout=120` (integration), `--timeout=180` (e2e/smoke)
- **Same verbosity**: `-v --tb=short`

## Test Execution Order

All environments run tests in this order:

1. **Unit tests**: `python -m pytest tests/unit/ -v --tb=short --timeout=60`
2. **Integration tests**: `python -m pytest tests/integration/ -v --tb=short --timeout=120 --ignore=tests/integration/test_performance.py -k "not api and not live and not download"`
3. **E2E tests**: `python -m pytest tests/e2e/ -v --tb=short --timeout=180`
4. **Smoke tests**: `python -m pytest tests/smoke/ -v --tb=short --timeout=180`

## Environment Variables

All environments set:

- `CLOUD_MOCK_MODE="true"`
- `GOOGLE_CLOUD_PROJECT="test-project"`
- `DEPLOYMENT_CONFIG_DIR` (path to deployment configs)

## Dependency Installation Order

All environments install dependencies in this order:

1. `python -m pip install --upgrade pip setuptools wheel`
2. `pip install -e deps/unified-cloud-services` (or `/workspace/unified-cloud-services` in Cloud Build)
3. `pip install -e deps/unified-trading-deployment-v2` (optional)
4. `pip install -e . --no-deps` (instruments-service)
5. Core dependencies (pydantic, pandas, etc.)
6. Test dependencies (pytest, ruff, etc.)

## Files to Keep in Sync

When changing test commands, update **all three** files:

1. `.github/workflows/quality-gates.yml` (GitHub Actions)
2. `cloudbuild.yaml` (Cloud Build)
3. `scripts/quality-gates.sh` (Local script)

## Verification

To verify alignment, run:

```bash
# Local
./scripts/quality-gates.sh

# Should match GitHub Actions and Cloud Build output
```

## Differences (Intentional)

- **Local script**: Uses `python3` instead of `python` (for macOS compatibility)
- **Local script**: Has `--quick` mode for faster iteration
- **Local script**: Auto-fixes linting issues by default (CI doesn't auto-fix)

All other differences should be eliminated.
