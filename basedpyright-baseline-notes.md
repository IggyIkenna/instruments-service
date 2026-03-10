# basedpyright Baseline Suppression Notes — instruments-service

## File: `.basedpyright-baseline.json`

### When Created

Commit `bc16d7c` — 2026-03-10 — "fix(typecheck): add basedpyright baseline and fix pre-commit hook path"

### Suppression Count

- **Total suppressions: 2,837** across **75 files**
- Baseline file size: ~22,850 lines

### Breakdown by Error Code

| Code                         | Count | Root Cause                                                                             |
| ---------------------------- | ----- | -------------------------------------------------------------------------------------- |
| `reportUnknownMemberType`    | 1,170 | Third-party libraries (nautilus_trader, pandas, numpy) lack complete type stubs        |
| `reportUnknownVariableType`  | 839   | Same — chained access through untyped library return values                            |
| `reportUnknownArgumentType`  | 449   | Untyped library parameters propagated into service code                                |
| `reportMissingImports`       | 97    | Optional deps not installed in type-check environment (e.g. vendor adapters)           |
| `reportAttributeAccessIssue` | 94    | Dynamic attribute access on library objects with incomplete stubs                      |
| `reportUnknownParameterType` | 60    | Callback signatures from untyped third-party event systems                             |
| `reportUnnecessaryCast`      | 60    | Defensive casts inserted before baseline existed                                       |
| Other                        | 68    | `reportUnnecessaryIsInstance`, `reportUnnecessaryComparison`, `reportUntypedBaseClass` |

### Why Suppressed (Rationale)

The dominant cause (2,458 / 2,837 = 87%) is **nautilus_trader's incomplete type stubs**.
`nautilus_trader` uses Cython-compiled extensions and as of 2026-03-10 its `py.typed` marker
and stub coverage are incomplete for the member types used in `instruments-service`.

This is a third-party tooling constraint, not a service code quality issue. Adding `# type: ignore`
to 2,800+ call sites would violate the workspace no-type-ignore rule, so the baseline was the
correct mechanism.

The `reportMissingImports` (97) originate from optional vendor adapters that are only installed
at runtime, not in the typecheck venv.

### Plan to Reduce

1. **Short-term (next sprint):** Pin nautilus_trader to a release with improved stub coverage
   when one becomes available. Track: `https://github.com/nautechsystems/nautilus_trader`
2. **Medium-term:** Replace direct nautilus_trader attribute access in hot paths with typed
   adapter wrappers — wrapping the untyped surface area concentrates suppressions to 1–2 files.
3. **Long-term:** Contribute stub improvements upstream or vendor a partial `nautilus_trader.pyi`
   stub package. Target: reduce from 2,837 to under 200 suppressions.
4. **Immediate wins:** The 120 `reportUnnecessaryCast` + `reportUnnecessaryIsInstance` +
   `reportUnnecessaryComparison` entries are safe to eliminate without any upstream changes —
   they are defensive code patterns that basedpyright can now verify statically.

### SSOT Reference

`unified-trading-codex/06-coding-standards/quality-gates.md` — baseline suppression policy
