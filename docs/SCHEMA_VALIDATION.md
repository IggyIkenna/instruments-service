# Schema Validation — instruments-service

> **Canonical SSOT:** [`/codex/02-data/schema-governance.md`](../../unified-trading-pm/codex/02-data/schema-governance.md)
> (validation integration point, DRY/SoC enforcement, STEP 5.12 quality gate) and
> [`/codex/02-data/canonical-schema-groups.md`](../../unified-trading-pm/codex/02-data/canonical-schema-groups.md)
> (UAC `canonical`/`internal` schema groups). Per S5.9, this file references the canonical schemas used — it does not
> redefine them.
>
> **Reorganized 2026-06-18** (`codex_vs_repo_docs_ssot_audit_2026_06_01.md` Appendix B): the in-repo schema-validation
> mechanics live in [`ADAPTER_ARCHITECTURE.md`](ADAPTER_ARCHITECTURE.md) — the "Real module map" section documents
> `process_write.py` (stage 5-6: schema validation + per-venue parquet/manifest writes) and `schemas.py` (the canonical
> `CanonicalOptionsChain` / `CanonicalExpiryCalendar` / `OHLCVRef` / `FundingRateRef` schemas); the "Command flow"
> section documents `_validate_records()` (stage 5, schema validation with per-record failure).

## instruments-service-specific schema validation

`DomainValidationService("instruments").validate(df)` flags anomalies via the event log, then
`ParquetSchemaEnforcer(INSTRUMENTS_SCHEMA).validate_dataframe(df)` blocks bad writes before the per-venue parquet
write — see [`ADAPTER_ARCHITECTURE.md`](ADAPTER_ARCHITECTURE.md) and `README.md` § "What it does" for the full
validate → write pipeline.

This stub exists so the S5.1 required-docs audit
(`/codex/06-coding-standards/documentation-standards.md` § S5.1) finds the canonical filename — do not add content
here; edit `ADAPTER_ARCHITECTURE.md` or the codex SSOTs above instead (operator ruling 2026-08-08,
`plans/active/issues/s5_7_required_docs_gaps_2026_07_29.md`).
