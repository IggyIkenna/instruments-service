# Contributing

## Branch Protection

Direct pushes to `main` are blocked. All changes must go through a PR with passing quality gates.

## Quick Merge Workflow

Use the quickmerge script to automate branch creation, PR, and auto-merge:

```bash
# From this repo directory - commits all changes and creates auto-merging PR
/Users/ikennaigboaka/Documents/repos/unified-trading-system-repos/git-quickmerge.sh "your commit message"

# Or use the shell alias (after sourcing .zshrc)
gqm "your commit message"

# For all repos with changes at once
gqm-all
```

The PR will auto-merge once quality gates (ruff lint + format + tests) pass.

## Quality Gates

All three layers run the same checks:
1. **Pre-commit hooks** (local): `ruff check --fix` + `ruff format` (auto-fix on commit)
2. **GitHub Actions** (on PR): `ruff check --fix` + `ruff format` + `git diff --exit-code` (fail if unformatted)
3. **Cloud Build** (on merge): Same check before Docker build

### Running locally
```bash
ruff check --fix .
ruff format .
```
