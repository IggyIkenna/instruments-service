# Branch Protection Rules

## Main Branch Protection

Configure the following branch protection rules for the `main` branch:

### Required Settings

- **Require pull request reviews**: 1 approval minimum
- **Dismiss stale reviews on new commits**: Enabled
- **Require status checks to pass**: Enabled
  - Required checks: `quality-gates`
- **Require branches to be up to date**: Enabled
- **Include administrators**: Enabled (applies to all users)

### Status Check Requirements

The following status checks must pass before merging:

- `quality-gates` workflow completion
- Ruff linting (no E, F, W, I rule violations)
- Basedpyright type checking (with 60s timeout)
- Tests with 35% minimum coverage
- pip-audit security scan

### Quality Gate Requirements

#### Python Service: instruments-service

- **Ruff linting**: PASS (no E, F, W, I rule violations)
- **Basedpyright**: PASS (with 60s timeout to prevent hangs)
- **Tests**: PASS with 35% minimum coverage
- **Security scan**: PASS (pip-audit for dependencies)

## Timeout Configurations

All long-running operations have strict timeouts to prevent hanging builds:

| Operation         | Timeout     | Rationale                             |
| ----------------- | ----------- | ------------------------------------- |
| Global job        | 15 minutes  | Prevents runaway builds               |
| Basedpyright      | 60 seconds  | **CRITICAL**: prevents infinite hangs |
| Unit tests        | 60 seconds  | Fast feedback                         |
| Integration tests | 120 seconds | Reasonable test time                  |
| E2E tests         | 180 seconds | Complete end-to-end validation        |

## Enforcement Rules

### No Merge Conditions

Pull requests will be blocked from merging if:

- Any required status check fails
- Code coverage falls below 35%
- Ruff linting errors exist
- Basedpyright type checking fails or times out
- pip-audit finds security vulnerabilities
- Tests fail or timeout

### Emergency Procedures

In case of critical hotfixes:

1. Create emergency branch from `main`
2. Apply minimal fix with tests
3. Run local quality gates: `make ci-local`
4. Request expedited review
5. Merge only after all checks pass

## Local Testing

Before creating pull requests, run local quality gates:

```bash
# Full CI simulation
make ci-local

# Individual checks
make lint        # Ruff linting
make type-check  # Basedpyright with timeout
make test        # Tests with coverage
```

This mirrors the exact CI checks and catches issues before pushing.

## Troubleshooting

### Common Issues

- **Basedpyright timeout**: Usually indicates complex types or circular imports - simplify type definitions
- **Coverage drops**: Add tests for new code paths, ensure 35% minimum maintained
- **Ruff failures**: Run `ruff format .` and `ruff check . --fix` locally before committing

### Getting Help

- Check workflow logs in GitHub Actions tab
- Review specific error messages in PR comments
- Run `bash scripts/quality-gates.sh` locally to reproduce issues
- Check instrument definitions and CCXT integration patterns
