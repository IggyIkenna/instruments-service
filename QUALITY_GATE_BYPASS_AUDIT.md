# Quality Gate Bypass Audit — instruments-service

All active QG exceptions are documented here. Each entry must have a reason and a tracked remediation path.

---

## 1. Quality Gate Script Exclusions

### 1.1 Path/Glob Exclusions

| Check                           | Excluded Path               | Reason                                                                                                                                                                                                                                                                |
| ------------------------------- | --------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **gcs_bucket (STEP 5.11/5.12)** | `**/engine/orchestrator.py` | `_write_catalogue_record` calls `ManifestWriter.add(gcs_bucket=...)` — this is the UCI schema field name for a catalogue record, not a raw GCS SDK import. Remediation: rename `gcs_bucket` → `storage_bucket` in UCI `catalogue.py` to eliminate the false positive. |

### 1.2 Inline Suppressions

No `# type: ignore`, `# noqa`, or basedpyright baseline errors.

**Broad-except (`# broad-except-ok`)** — one-off migration script only:

| Location                                                       | Lines                                          | Reason                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| -------------------------------------------------------------- | ---------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `scripts/complete_cefi_manifest_canonical_dedup_2026_07_17.py` | pass-1 per-VM shard load + `_verify_gate` load | **Per-object isolation for a transient per-VM shard.** A live backfill VM's `_index/per_vm/*.parquet` shard can be consolidated + DELETED between the `list_blobs` snapshot and the `download_bytes` (observed 404). The main index MUST load (re-raised); a vanished per-VM shard is skipped with a warning. One-off migration (`# Lifecycle: oneoff`), deleted after the cutover drain. Same pattern as the sibling `canonicalize_cefi_defi_instrument_type_2026_07_17.py`. |

### 1.3 Pragma No-Cover

| Location                          | Lines                                                    | Reason                                                     |
| --------------------------------- | -------------------------------------------------------- | ---------------------------------------------------------- |
| `instruments_service/__main__.py` | `if __name__ == "__main__"`                              | Bootstrap entry point — not unit-testable                  |
| `instruments_service/cli/main.py` | `_SERVICE_NAME`, `_add_service_args`, `main_service_cli` | CLI entry point — requires subprocess to test meaningfully |

---

## 2. Type System

No basedpyright baseline errors. `0 errors, 0 warnings` as of 2026-03-24.

---

## 3. Notes

- **STARTED/STOPPED/FAILED lifecycle events**: Emitted by UTL `ServiceBootstrap.run()`. The per-service source check is removed from this repo's QG stub.
- **INSTRUMENTS_SCHEMA**: The UIC schema definition is out of sync with `InstrumentRecord` field names. The `ParquetSchemaEnforcer` has been removed from `orchestrator.py` until UIC is updated.
