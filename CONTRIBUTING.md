# Contributing

## Branch Protection

Direct pushes to `main` are blocked (even for admins). All changes must go through a PR with passing quality gates.

## Quick Merge Workflow

Use the quickmerge script to automate branch creation, PR, and auto-merge:

```bash
# From this repo directory
./scripts/quickmerge.sh "your commit message"
```

The PR will auto-merge once quality gates (ruff lint + format + tests) pass.
The branch is auto-deleted after merge.

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

### Prerequisites

- `gh` CLI installed and authenticated (`gh auth login`)
- `ruff==0.15.0` installed (`pip install ruff==0.15.0`)
