# Configuration — instruments-service

> **Reorganized 2026-06-18** (`codex_vs_repo_docs_ssot_audit_2026_06_01.md` Appendix B): the configuration content lives
> in [`SETUP_GUIDE.md`](SETUP_GUIDE.md) — see § 4 "Environment Configuration" (the `.env` fields, per-asset-group GCS
> bucket vars, BigQuery dataset), § 5 "Credentials & Authentication" (`InstrumentsServiceConfig` /
> `UnifiedCloudConfig`, ADC vs explicit `GOOGLE_APPLICATION_CREDENTIALS`), and § 6 "Secrets & API Keys" (the secret-name
> config fields, resolving a secret via `unified_trading_library.get_secret_client`).

## instruments-service-specific configuration

See [`SETUP_GUIDE.md`](SETUP_GUIDE.md) §§ 4-6 for `InstrumentsServiceConfig`'s full field list, defaults, and secret
names (Tardis, Databento, The Graph, Alchemy, Aavescan, IBKR).

This stub exists so the S5.1 required-docs audit
(`/codex/06-coding-standards/documentation-standards.md` § S5.1) finds the canonical filename — do not add content
here; edit `SETUP_GUIDE.md` instead (operator ruling 2026-08-08,
`plans/active/issues/s5_7_required_docs_gaps_2026_07_29.md`).
