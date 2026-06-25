#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after the EXTENDED/PACIFICA/LIGHTER defi _index contamination purge is verified in prod
"""Phase 2 — purge the defi _index contamination for the 3 cefi on-chain perps.

EXTENDED/PACIFICA/LIGHTER were reclassified defi->cefi (Phase 1, IS@2f7d454).
Their 1,802 defi `_index` rows are contamination (wrong asset_group). This:
  1. SNAPSHOTS the current _index -> _index/snapshots/pre_phase2_purge_2026_06_25.parquet
  2. backs up the live blob -> _index/availability_index.parquet.phase2.bak
  3. removes rows where asset_group==defi AND venue~(EXTENDED|PACIFICA|LIGHTER)
     (KEEPS the 119 cefi rows — EXTENDED's correct home)
  4. writes back + verifies (0 defi rows for the 3; cefi/other unchanged).
Catalogue (prod/catalog.parquet) is asset_group-AGNOSTIC venue-keyed instrument
defs -> left intact (valid instrument definitions; asset_group is in UAC).
"""

from __future__ import annotations

import io
import sys

import pandas as pd
from unified_trading_library import get_storage_client

PID = "central-element-323112"
IS = f"instruments-store-defi-prd-{PID}"
BLOB = "_index/availability_index.parquet"
VENUES = ("EXTENDED", "PACIFICA", "LIGHTER")
APPLY = "--apply" in sys.argv


def main() -> int:
    st = get_storage_client(project_id=PID)
    raw = st.download_bytes(IS, BLOB)
    df = pd.read_parquet(io.BytesIO(raw))
    vmask = df["venue"].astype(str).str.contains("|".join(VENUES), na=False)
    defi_mask = vmask & (df["asset_group"].astype(str) == "defi")
    cefi_keep = int((vmask & (df["asset_group"].astype(str) == "cefi")).sum())
    print(f"_index rows={len(df):,} | purge (defi+3venues)={int(defi_mask.sum())} | KEEP cefi-3venue={cefi_keep}")
    out = df[~defi_mask].copy()
    print(f"after purge: {len(out):,} rows (removed {len(df) - len(out)})")
    # safety asserts
    assert len(df) - len(out) == int(defi_mask.sum())
    rem = out["venue"].astype(str).str.contains("|".join(VENUES), na=False)
    assert int((rem & (out["asset_group"].astype(str) == "defi")).sum()) == 0, "defi 3-venue rows remain!"
    assert int((rem & (out["asset_group"].astype(str) == "cefi")).sum()) == cefi_keep, "cefi rows changed!"
    if not APPLY:
        print("DRY-RUN (pass --apply to write). Snapshot + backup + write skipped.")
        return 0
    # 1. snapshot 2. backup 3. write
    st.upload_bytes(IS, "_index/snapshots/pre_phase2_purge_2026_06_25.parquet", raw)
    st.upload_bytes(IS, BLOB + ".phase2.bak", raw)
    buf = io.BytesIO()
    out.to_parquet(buf, index=False)
    st.upload_bytes(IS, BLOB, buf.getvalue())
    print("APPLIED: snapshot + .phase2.bak written; _index rewritten.")
    # verify round-trip
    chk = pd.read_parquet(io.BytesIO(st.download_bytes(IS, BLOB)))
    cm = chk["venue"].astype(str).str.contains("|".join(VENUES), na=False)
    print(
        f"VERIFY live: rows={len(chk):,} | defi-3venue={int((cm & (chk['asset_group'].astype(str) == 'defi')).sum())} | cefi-3venue={int((cm & (chk['asset_group'].astype(str) == 'cefi')).sum())}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
