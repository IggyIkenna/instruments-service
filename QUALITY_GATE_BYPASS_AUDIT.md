# Quality Gate Bypass Audit — instruments-service

All active QG exceptions are documented here. Each entry must have a reason and a tracked remediation path.

---

## 1. Quality Gate Script Exclusions

### 1.1 Path/Glob Exclusions

| Check                                 | Excluded Path                                                                                                                                                       | Reason                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| ------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **gcs_bucket (STEP 5.11/5.12)**       | `**/engine/orchestrator.py`                                                                                                                                         | `_write_catalogue_record` calls `ManifestWriter.add(gcs_bucket=...)` — this is the UCI schema field name for a catalogue record, not a raw GCS SDK import. Remediation: rename `gcs_bucket` → `storage_bucket` in UCI `catalogue.py` to eliminate the false positive.                                                                                                                                                                                                                                                                                                                                                      |
| **broad-except (`BE_EXCLUDE_GLOBS`)** | `**/reference_data/adapters/defi/_solana_utils.py`, `**/reference_data/adapters/defi/_solana_pool_discovery.py`, `**/reference_data/utils/evm_creation_resolver.py` | Audited 2026-07-25 (`instruments_service_codex_compliance_ceiling_drift_2026_07_20.md` P3 #3) — see § 1.2 below for the per-site breakdown. `_solana_pool_discovery.py` split from `_solana_utils.py` 2026-08-08; inherits same GCS-boundary broad-except justification. (This repo's `base-service.sh` reads `BE_EXCLUDE_GLOBS` directly — the array was already correctly wired; a mid-audit hypothesis that it was a dead `BROAD_EXCEPT_EXTRA_EXCLUDES`/`BE_EXCLUDE_GLOBS` name mismatch, based on cross-referencing the sibling `base-library.sh` used by library-type repos, was WRONG and reverted before shipping.) |

### 1.2 Inline Suppressions

No `# type: ignore` or basedpyright baseline errors.

**`# noqa: qg-empty-fallback`**:

| Location                                           | Line                          | Reason                                                                                                                                                                                                                                                                                                                                                  |
| -------------------------------------------------- | ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `instruments_service/oracle/defi_removal_probe.py` | `payload.get("removals", [])` | Audited 2026-07-25 (`instruments_service_codex_compliance_ceiling_drift_2026_07_20.md` P3 #2). Absent `"removals"` key = malformed/legacy artifact, not an error — `load_removals()` is documented to "never raise" and degrade to Option A (all live), same as the blob-absent/JSON-decode-failure branches immediately above it in the same function. |

**Broad-except (`# broad-except-ok`)** — one-off migration script:

| Location                                                       | Lines                                          | Reason                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| -------------------------------------------------------------- | ---------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `scripts/complete_cefi_manifest_canonical_dedup_2026_07_17.py` | pass-1 per-VM shard load + `_verify_gate` load | **Per-object isolation for a transient per-VM shard.** A live backfill VM's `_index/per_vm/*.parquet` shard can be consolidated + DELETED between the `list_blobs` snapshot and the `download_bytes` (observed 404). The main index MUST load (re-raised); a vanished per-VM shard is skipped with a warning. One-off migration (`# Lifecycle: oneoff`), deleted after the cutover drain. Same pattern as the sibling `canonicalize_cefi_defi_instrument_type_2026_07_17.py`. |

**Broad-except (audited, left broad, inline-documented)** — 2026-07-25 audit (`instruments_service_codex_compliance_ceiling_drift_2026_07_20.md` P3 #3). 14 bare `except Exception:` sites were reviewed across `block_resolver.py` / `evm_creation_resolver.py` / `_solana_utils.py` / the 4 sports-orchestrator files (`sports_fixtures.py`, `sfi.py`, `transfermarkt.py`, `weather.py`); 6 were narrowed to a specific exception type (`KeyError`, `ValueError`, `(ValueError, TypeError)`, `BucketNamingError`) where the failure mode was concretely known. The 8 below stay broad — genuinely wide, unenumerable exception surfaces at a network/storage/auth boundary, each a best-effort probe or degrade-gracefully pattern where the fallback path is correct regardless of the specific exception. (`_save_discovered_pools` moved from `_solana_utils.py` to `_solana_pool_discovery.py` 2026-08-08; the broad-except justification is unchanged.)

| Location                                                                         | Pattern                                                  | Reason                                                                                                                                             |
| -------------------------------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| `reference_data/utils/block_resolver.py::_resolve_alchemy_key`                   | Secret Manager client + `.get_secret().strip()`          | ADC/credential exception surface (`google.auth.exceptions.*`) isn't a small closed set; `get_secret()` already swallows GCP-API errors internally. |
| `reference_data/utils/evm_creation_resolver.py::_resolve_rpc_url`                | Secret Manager client (same pattern as above)            | Same as above.                                                                                                                                     |
| `reference_data/utils/evm_creation_resolver.py::_save_cache`                     | GCS read-merge (nested try, download_bytes + JSON parse) | `download_bytes` doesn't pre-wrap the GCS SDK exception surface; read-merge is best-effort by design (write still proceeds with un-merged cache).  |
| `reference_data/adapters/defi/_solana_utils.py::_load_cache` (GCS branch)        | GCS read (download_bytes + JSON parse)                   | Same GCS-read-boundary rationale as above.                                                                                                         |
| `reference_data/adapters/defi/_solana_pool_discovery.py::_save_discovered_pools` | GCS read-merge (nested try, download_bytes + JSON parse) | Same GCS-read-boundary rationale as above. Moved from `_solana_utils.py` 2026-08-08.                                                               |
| `engine/orchestrator/sports_fixtures.py::_resolve_sports_ref_blob`               | `storage_client.bucket(...).blob(...).exists()` probe    | Best-effort canonical-vs-legacy probe; any failure correctly falls back to the legacy path, same as a confirmed-absent result.                     |
| `engine/orchestrator/weather.py` (existing-venues probe)                         | GCS list/download + `pd.read_parquet` parse              | Best-effort probe; any failure correctly degrades to "no existing weather data, fetch everything."                                                 |
| `engine/orchestrator/weather.py` (merge-before-write)                            | GCS list/download + `pd.read_parquet` parse              | Best-effort merge; any failure correctly falls back to writing the newly-fetched data only (the safe, non-data-losing default).                    |

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
