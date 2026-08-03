# Contributing to instruments-service

> The workspace-wide rules (git discipline, quality gates, the 8 code rules, CI verification) live in
> the auto-loaded workspace `CLAUDE.md` (symlinked at `.claude/CLAUDE.md`) and its codex SSOTs. This
> file is the service-local quick reference; **the codex is authoritative** where they differ:
>
> - Shipping pipeline / quickmerge / LDR→main promote / branch protection →
>   `codex/08-workflows/ci-cd-flow.md`
> - Quality gates → `codex/06-coding-standards/quality-gates.md`
> - Per-slot worktrees → `codex/05-infrastructure/per-tab-worktrees.md`

## Development Workflow

### Session Start

Each clone tracks the integration branch `live-defi-rollout` (LDR) directly — there is no `main`
working branch. Sync before starting work:

```bash
git pull --ff-only origin live-defi-rollout
```

Check for conflicts with local uncommitted work. If conflicts exist, resolve them or ask for
guidance — never force-push a shared branch.

### Making Changes

**ALWAYS follow this sequence**:

1. **Make code changes**
2. **Write unit tests** for your changes
3. **Commit** with a conventional commit message (include the `Quickmerge: agent` trailer)
4. **Run quality gates** (Pass 1): `bash scripts/quality-gates.sh` — must exit 0. Never run
   `pytest`/`ruff`/`basedpyright` standalone; the gate is the entrypoint.
5. **ONLY THEN ship** via quickmerge (Pass 2, below)

### Shipping: quickmerge (agent mode)

Code reaches the integration branch **only** through quickmerge — a raw `git push` of code is
banned (it dodges the dependency gates). After a green `quality-gates.sh`:

```bash
bash scripts/quickmerge.sh "feat(...): descriptive commit message" --agent --files '<your files>'
```

**What quickmerge does**:

1. Verifies the `.qg_last_passed_sha` sentinel == HEAD (refuses if QG did not pass on this exact
   SHA — never pass `--skip-*` to dodge it)
2. Commits + pushes to `live-defi-rollout`
3. The standing LDR→`main` promote job (`ldr-to-main-promote-fleet.yml`, every 15 min) opens the
   promote PR; the `quality-gates-v2` required check gates it

`per-repo quickmerge.sh` is a symlink to the PM SSOT — do not edit it locally.

### End of Session

Verify nothing is left unpushed:

```bash
git rev-list --count HEAD ^origin/live-defi-rollout   # must be 0
```

### Critical Rules

**NEVER**:

- ❌ Run quickmerge without a green `quality-gates.sh`
- ❌ Push code directly to `live-defi-rollout` or `main` (both rejected / banned — ship via quickmerge)
- ❌ Use `--no-verify` or any `--skip-*` flag to dodge the QG sentinel
- ❌ Force-push a shared branch

**ALWAYS**:

- ✅ Write tests before shipping
- ✅ Run `bash scripts/quality-gates.sh` (green) before quickmerge
- ✅ Let pre-commit hooks run (ruff format/check)
- ✅ Verify CI after every push (`gh run list --branch live-defi-rollout`; required check = `quality-gates-v2`)

### Test Requirements

Every code change MUST include tests:

- **New features**: unit tests covering happy path + edge cases (holidays, UTC spanning, etc.);
  integration tests if the feature touches multiple components
- **Bug fixes**: a unit test reproducing the bug + a unit test verifying the fix
- **Refactors**: existing tests still pass; add tests for any new behaviour

### Quality Gates (CI)

`quality-gates-v2` runs on every promote PR (the single required check across all repos). Local
`bash scripts/quality-gates.sh` runs the same gate set: ruff version check, `ruff check`,
`ruff format --check`, `basedpyright`, unit tests, and the workspace code-rule bans (no `os.getenv`,
no `Any`, no inline `gs://`, UTC datetimes only, etc.). See
`codex/06-coding-standards/quality-gates.md`.

### Branch Protection

Branch protection is centrally managed via ruleset + classic protection (rolled out from PM,
not per-repo). `live-defi-rollout` and `main` reject direct code pushes; `main` is a reconciled
projection back-merged to LDR. See `.github/BRANCH_PROTECTION.md` and
`codex/08-workflows/ci-cd-flow.md`.

### Working with Multiple Agents/Sessions

Each slot is its own `git clone --reference` on `live-defi-rollout` (see
`codex/05-infrastructure/per-tab-worktrees.md`). Never edit files another agent has dirty; if a
push to LDR is rejected as behind, `git pull --rebase --autostash` keeping both sides' work, then
re-ship — never force-resolve a conflict for a green push.

### File Locations

- Source code: `instruments_service/`
- Tests: `tests/unit/`, `tests/integration/`, `tests/e2e/`
- Config: `instruments_service/config/`
- Scripts: `scripts/`
- Documentation: `docs/`

### For More Details

- Workspace rules: the auto-loaded `CLAUDE.md` (symlinked `.claude/CLAUDE.md`)
- Setup: `docs/SETUP_GUIDE.md`
- Adapter model: `docs/ADAPTER_ARCHITECTURE.md`
