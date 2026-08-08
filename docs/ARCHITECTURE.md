# Architecture — instruments-service

> **Reorganized 2026-06-18** (`codex_vs_repo_docs_ssot_audit_2026_06_01.md` Appendix B): the architecture content lives
> in [`ADAPTER_ARCHITECTURE.md`](ADAPTER_ARCHITECTURE.md), which consolidates the former `ARCHITECTURE.md` +
> `specs/COMMAND_FLOW_ANALYSIS.md` + `specs/COMMAND_FLOW_DIAGRAM.md` + `specs/VENUE_ADAPTERS.md` + the
> general/cross-cutting parts of `specs/INSTRUMENT_SPECIFICATION.md` (the canonical-id grammar +
> `canonical_id_builder.py` explanation). Per-asset-group instrument-type catalogs and worked examples live in the 5
> asset-group docs (`CEFI_INSTRUMENTS.md`, `DEFI_INSTRUMENTS.md`, `PREDICTION_INSTRUMENTS.md`, `SPORTS_INSTRUMENTS.md`,
> `TRADFI_INSTRUMENTS.md`), not here.

## instruments-service-specific architecture

See [`ADAPTER_ARCHITECTURE.md`](ADAPTER_ARCHITECTURE.md) for the module map, command-flow orchestration
(`process_instruments()`'s 8 stages), venue-adapter pattern, and the canonical instrument-id specification.

This stub exists so the S5.1 required-docs audit
(`/codex/06-coding-standards/documentation-standards.md` § S5.1) finds the canonical filename — do not add content
here; edit `ADAPTER_ARCHITECTURE.md` instead (operator ruling 2026-08-08,
`plans/active/issues/s5_7_required_docs_gaps_2026_07_29.md`).
