# GCS Paths — instruments-service

> **Reorganized 2026-06-18** (`codex_vs_repo_docs_ssot_audit_2026_06_01.md` Appendix B): the GCS bucket/path content
> lives in [`SETUP_GUIDE.md`](SETUP_GUIDE.md) § 7.1 "GCS bucket resolution" (buckets are resolved via UTL's
> `resolve_bucket_name(cloud="gcp", kind="instruments", asset_group=...)`, never hardcoded or built with an inline
> `gs://` string) and in [`ADAPTER_ARCHITECTURE.md`](ADAPTER_ARCHITECTURE.md)'s "Storage / bucket resolution" section
> (every write goes through `engine.orchestrator.catalogue._get_instruments_bucket()` →
> `resolve_bucket_name(...)`). Bucket-naming conventions + hive layout are canonical codex content, not repeated here —
> see `/codex/02-data/per-asset-group-bucket-layouts.md` and `/codex/02-data/partitioning.md`.

## instruments-service-specific paths

Category-specific buckets per asset group (`_CEFI`/`_TRADFI`/`_DEFI`, each with a `_TEST` variant) — see
[`SETUP_GUIDE.md`](SETUP_GUIDE.md) § 4 "Environment Configuration" for the exact env-var names
(`INSTRUMENTS_GCS_BUCKET_*`) and § 7.1 for the resolution call.

This stub exists so the S5.1 required-docs audit
(`/codex/06-coding-standards/documentation-standards.md` § S5.1) finds the canonical filename — do not add content
here; edit `SETUP_GUIDE.md` or `ADAPTER_ARCHITECTURE.md` instead (operator ruling 2026-08-08,
`plans/active/issues/s5_7_required_docs_gaps_2026_07_29.md`).
