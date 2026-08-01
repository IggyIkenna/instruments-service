# Branch Protection Rules

> Branch protection is **centrally managed from PM** (ruleset + classic protection, rolled out via
> workflow templates) — it is not configured per-repo by hand. This file documents the effective
> rules; the SSOT is `codex/08-workflows/ci-cd-flow.md`.

## Protected Branches

Both `live-defi-rollout` (LDR, the integration branch every clone tracks) and `main` reject direct
code pushes. Code reaches LDR only through `quickmerge --agent`; `main` is a reconciled projection
back-merged to LDR. There is no `main` working branch.

### Required Settings

- **Require pull request**: enabled (code lands via the quickmerge → LDR → promote-PR flow)
- **Require status checks to pass**: enabled
  - Required check: **`quality-gates-v2`** (the single required check across all repos)
- **Require branches to be up to date**: enabled
- **No force push** / **no direct push**: enabled
- **Include administrators**: enabled

### Status Check: `quality-gates-v2`

The promote PR must pass `quality-gates-v2`, which runs the same gate set as local
`bash scripts/quality-gates.sh`:

- Ruff version check + `ruff check` + `ruff format --check`
- `basedpyright` type checking
- Unit tests with the coverage floor (a **ratcheted, moving target** — read `MIN_COVERAGE` at the
  top of `scripts/quality-gates.sh`, mirrored by `[tool.coverage.report] fail_under` in
  `pyproject.toml`; do not hardcode a number here)
- pip-audit security scan
- The workspace code-rule bans (no `os.getenv`, no `Any`, no inline `gs://`, UTC datetimes only, …)

## Timeout Configurations

Long-running operations carry strict timeouts to prevent hanging builds:

| Operation         | Timeout     | Rationale                             |
| ----------------- | ----------- | ------------------------------------- |
| Global job        | 15 minutes  | Prevents runaway builds               |
| Basedpyright      | 60 seconds  | **CRITICAL**: prevents infinite hangs |
| Unit tests        | 60 seconds  | Fast feedback                         |
| Integration tests | 120 seconds | Reasonable test time                  |
| E2E tests         | 180 seconds | Complete end-to-end validation        |

## Enforcement

A promote PR is blocked from merging if `quality-gates-v2` fails for any reason (lint, type-check,
tests, coverage below the ratcheted floor, pip-audit findings, or a timeout).

### Hotfixes

Ship a hotfix via `quickmerge --agent` with `--hotfix` (requires `[hotfix]` in the message). Never
force-push a shared branch; never `[skip ci]` a v2-gated promote-PR head (write `skip-ci` if you
must reference the marker in prose).

## Local Verification

Before shipping, run the canonical gate entrypoint (never run `pytest`/`ruff`/`basedpyright`
standalone):

```bash
bash scripts/quality-gates.sh
```

This mirrors the exact `quality-gates-v2` checks and catches issues before pushing.

## Troubleshooting

- **Basedpyright timeout**: usually complex types or circular imports — simplify type definitions
- **Coverage drop**: add tests for new code paths; keep the ratcheted floor from regressing
- **Ruff failures**: run `ruff format .` and `ruff check . --fix` locally before committing
- **CI logs**: `gh run view --log-failed` for the failing `quality-gates-v2` run
