#!/usr/bin/env python3
# Epic: defi_master
# Lifecycle: oneoff
# Delete-when: after the EXTENDED-STARKNET/LIGHTER-ZKSYNC defi-catalogue purge is verified in
#   prod AND enumerate_expected_universe.py --asset-group defi stops re-seeding
#   expected_unattempted rows for bare venue=EXTENDED/LIGHTER in the MTDS defi manifest.
"""Purge the 2 cefi-reclassified on-chain perp venues' STALE rows from the DEFI instrument
catalogue (`instruments-store-defi-prd-.../prod/catalog.parquet`).

Root cause (found 2026-08-04, defi_hyperliquid_residual_manifest_rows_2026_08_04.md): the
2026-06-25 EXTENDED/PACIFICA/LIGHTER defi->cefi reclassification
(instruments_foundation_completeness_2026_06_24.md) fixed the CAPTURE path
(`instruments_service/engine/orchestrator/defi.py::_build_defi_venues()` no longer enumerates
them) and purged the defi `_index` manifest rows
(`purge_cefi_perp_defi_contamination_2026_06_25.py`), but explicitly LEFT the CATALOGUE alone
("Catalogue (prod/catalog.parquet) is asset_group-AGNOSTIC venue-keyed instrument defs -> left
intact" -- that reasoning assumed every catalogue consumer re-resolves asset_group via UAC
VENUE_TO_ASSET_GROUP before treating a row as belonging to a given asset_group's expected
universe).

`enumerate_expected_universe.py`'s v2 per-instrument enumerator does NOT do that re-resolution:
for a `--asset-group defi` run it downloads `instruments-store-defi-prd-.../prod/catalog.parquet`
wholesale and treats EVERY row in it as defi-expected (asset_group is implicit in "which bucket
you read from", not re-checked per-row). Because `build_instrument_catalogue.py` guards against
catalogue SHRINKAGE (`--allow-catalogue-shrink`, default off) and normal `--mode incremental` runs
never touch the frozen tail, the 15 EXTENDED-STARKNET + 18 LIGHTER-ZKSYNC PERPETUAL rows already
present in the defi catalogue before 2026-06-25 survived every subsequent rebuild (available_to
still None -- the G3b anti-false-delist guard correctly refuses to infer delisting from mere
capture-silence, which is right for genuine trading halts but wrong here: these were EXPLICITLY
removed from the defi universe, not merely quiet).

Confirmed 2026-08-04, direct bounded reads (pyarrow.fs.GcsFileSystem, footer+filter, no corpus
walk): defi catalogue carries exactly 15 EXTENDED-STARKNET + 18 LIGHTER-ZKSYNC rows (all
`instrument_type=PERPETUAL`, `available_to=None`); the cefi catalogue already carries the REAL,
actively-maintained twins (286 EXTENDED-STARKNET + 221 LIGHTER-ZKSYNC rows,
`instruments-store-cefi-prd-.../prod/catalog.parquet`) -- confirming these are NOT unique data,
just a duplicate stuck in the wrong asset_group's bucket. Confirmed live-fire: MTDS's
`market-data-tick-defi-prd-...` manifest carries 4,980 (EXTENDED) + 5,976 (LIGHTER) bare-venue
`expected_unattempted` rows (data_type=oracle_prices/perp_funding), written_at as recently as
2026-08-04 (today) -- these are RE-MATERIALISED on every `enumerate_expected_universe.py
--asset-group defi` run, not historical residue; deleting them without this catalogue fix would
just have them reappear tomorrow (the exact "would just get re-seeded" trap this repo's own GMX
precedent already taught).

This script fixes the ROOT (the catalogue), mirroring
`purge_cefi_perp_defi_contamination_2026_06_25.py`'s exact structure (same bucket, sibling blob).
It does NOT touch the MTDS manifest's already-materialised expected_unattempted rows (harmless
placeholders, no real captured data lost either way -- capture_status=expected_unattempted, never
`captured`; a separate, low-priority hygiene follow-up, not this script's job) and does NOT touch
the cefi catalogue (left fully intact -- it is the correct, authoritative home for these 507
combined rows).

Usage:
  GCP_PROJECT_ID=central-element-323112 PROJECT_ID=central-element-323112 \\
    DEPLOYMENT_ENV_SHORT=prd CLOUD_PROVIDER=gcp \\
    .venv/bin/python scripts/purge_defi_catalogue_cefi_reclassified_venues_2026_08_04.py  # dry-run
  ... --apply  # snapshot + backup + write + verify
"""

from __future__ import annotations

import io
import sys

import pandas as pd
from unified_trading_library import gcs_bucket_soft_delete_retention_seconds, get_storage_client

PID = "central-element-323112"
DEFI_IS = f"instruments-store-defi-prd-{PID}"
BLOB = "prod/catalog.parquet"
VENUES = ("EXTENDED-STARKNET", "LIGHTER-ZKSYNC")
APPLY = "--apply" in sys.argv
_MIN_SOFT_DELETE_RETENTION_SEC = 604800  # 7 days — /codex/02-data/gcs-and-manifest-delete-safety-protocol.md §3a


def main() -> int:
    st = get_storage_client(project_id=PID)
    raw = st.download_bytes(DEFI_IS, BLOB)
    df = pd.read_parquet(io.BytesIO(raw))
    mask = df["venue"].astype(str).isin(VENUES)
    print(f"defi catalogue rows={len(df):,} | stale cefi-reclassified rows to purge={int(mask.sum())}")
    if mask.sum():
        print(df.loc[mask, "venue"].value_counts().to_string())
    out = df[~mask].copy()
    print(f"after purge: {len(out):,} rows (removed {len(df) - len(out)})")
    # safety asserts
    assert len(df) - len(out) == int(mask.sum())
    assert int(out["venue"].astype(str).isin(VENUES).sum()) == 0, "stale rows remain!"
    if not APPLY:
        print("DRY-RUN (pass --apply to write). Snapshot + backup + write skipped.")
        return 0
    retention = gcs_bucket_soft_delete_retention_seconds(DEFI_IS)
    print(f"fresh GCS Soft Delete retention for {DEFI_IS}: {retention}s")
    if retention < _MIN_SOFT_DELETE_RETENTION_SEC:
        print(
            f"REFUSING to write: bucket {DEFI_IS} soft-delete retention is {retention}s, below the "
            f"required {_MIN_SOFT_DELETE_RETENTION_SEC}s (7 days) per "
            "/codex/02-data/gcs-and-manifest-delete-safety-protocol.md §3a. No object touched."
        )
        return 3
    # 1. snapshot 2. backup 3. write
    st.upload_bytes(DEFI_IS, "prod/snapshots/pre_cefi_reclassified_venue_purge_2026_08_04.parquet", raw)
    st.upload_bytes(DEFI_IS, BLOB + ".pre_cefi_reclassified_venue_purge_2026_08_04.bak", raw)
    buf = io.BytesIO()
    out.to_parquet(buf, index=False)
    st.upload_bytes(DEFI_IS, BLOB, buf.getvalue())
    print("APPLIED: snapshot + .bak written; catalogue rewritten.")
    # verify round-trip
    chk = pd.read_parquet(io.BytesIO(st.download_bytes(DEFI_IS, BLOB)))
    remaining = int(chk["venue"].astype(str).isin(VENUES).sum())
    print(f"VERIFY live: rows={len(chk):,} | stale cefi-reclassified rows remaining={remaining}")
    assert remaining == 0, "verify failed — stale rows still present post-write!"
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
