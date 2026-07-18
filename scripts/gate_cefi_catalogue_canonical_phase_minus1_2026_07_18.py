#!/usr/bin/env python
# Epic: cefi_master (canonical-completeness program)
# Lifecycle: one-off cutover gate — Phase-1 catalogue verification before the D4 GCS cutover
# Delete-when: cefi canonical migration DONE (both cefi_residual_followups + cefi_consolidated_closeout archived)
"""Phase-1 catalogue gate — assert the REBUILT ``prod/catalog.parquet`` is fully canonical.

The migration ``--apply`` scripts (rename / manifest / content) all build their wire→canonical map
from this catalogue, so it MUST be canonical-clean before any of them run against prod. This gate
reads the live prod catalogue and asserts, for every cefi row:

  1. ``instrument_id`` uses ``PERPETUAL`` — **never** ``:PERP:`` (0 offenders).
  2. ``instrument_id == canonical_instrument_id`` (the raw-glued trap: they must agree).
  3. **Quote MANDATORY** — the symbol segment (``VENUE:TYPE:<SYMBOL>[@MARKER…]``) is ``BASE-QUOTE``,
     i.e. it contains a ``-`` (0 offenders). This is the DERIBIT-fix guard: a marker-resolved id
     whose quote silently dropped to base-only (``DERIBIT:FUTURE:AVAX@LIN-20260718``) is a violation.

Plus a NON-gating coverage sample (blueprint open-q #14): OPTION + dated-FUTURE ``raw_symbol`` rows,
so we can eyeball that options/dated-futures actually rolled up with resolvable ids.

Read-only. Exits non-zero if any of the three assertions has >0 offenders — this is a real gate.

    GCP_PROJECT_ID=central-element-323112 CLOUD_PROVIDER=gcp DEPLOYMENT_ENV=prod CLOUD_MOCK_MODE=false \
      .venv/bin/python scripts/gate_cefi_catalogue_canonical_phase_minus1_2026_07_18.py
"""

from __future__ import annotations

import io
import sys

import pandas as pd
from unified_trading_library import (
    get_config,
    get_storage_client,
    resolve_bucket_name,
)

CATALOG_FILENAME = "catalog.parquet"


def _symbol_segment(instrument_id: str) -> str:
    """Return the ``BASE-QUOTE`` symbol segment of a cefi instrument_id.

    ``VENUE:TYPE:BASE-QUOTE@MARKER[-YYYYMMDD][-STRIKE-C|P]`` → ``BASE-QUOTE``.
    ``VENUE:SPOT_PAIR:BASE-QUOTE`` (no marker) → ``BASE-QUOTE``.
    """
    parts = instrument_id.split(":", 2)
    if len(parts) < 3:
        return ""
    rest = parts[2]
    # everything left of the margin marker is the BASE-QUOTE symbol
    return rest.split("@", 1)[0]


def main() -> int:
    env = get_config("DEPLOYMENT_ENV", "prod")
    bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="cefi")
    blob = f"{env}/{CATALOG_FILENAME}"
    storage = get_storage_client()
    print(f"[gate] reading gs://{bucket}/{blob}")
    payload = storage.download_bytes(bucket, blob)
    df = pd.read_parquet(io.BytesIO(payload))
    total = len(df)
    print(f"[gate] catalogue rows: {total:,}  columns: {list(df.columns)}")

    if "instrument_id" not in df.columns:
        print("[gate] FATAL: no instrument_id column")
        return 2

    ids = df["instrument_id"].astype(str)

    # ---- assertion 1: no :PERP: (must be PERPETUAL) ----
    perp = df[ids.str.contains(":PERP:", regex=False)]
    n_perp = len(perp)

    # ---- assertion 2: instrument_id == canonical_instrument_id ----
    if "canonical_instrument_id" in df.columns:
        canon = df["canonical_instrument_id"].astype(str)
        mismatch = df[(ids != canon) & canon.ne("") & canon.ne("None") & canon.notna()]
        n_mismatch = len(mismatch)
    else:
        print("[gate] NOTE: no canonical_instrument_id column — skipping assertion 2")
        n_mismatch = 0

    # ---- assertion 3: quote MANDATORY (symbol segment contains '-') ----
    seg = ids.map(_symbol_segment)
    # only derivative/spot rows carry a BASE-QUOTE; guard against blank segments
    missing_quote = df[seg.ne("") & ~seg.str.contains("-", regex=False)]
    n_missing_quote = len(missing_quote)

    print("\n===== ASSERTIONS =====")
    print(f"  1. :PERP: offenders              = {n_perp:,}   (expect 0)")
    print(f"  2. id != canonical_id offenders  = {n_mismatch:,}   (expect 0)")
    print(f"  3. missing-quote offenders       = {n_missing_quote:,}   (expect 0)")

    for name, bad in (("PERP", perp), ("MISMATCH", mismatch if n_mismatch else df.iloc[:0]), ("MISSING-QUOTE", missing_quote)):
        if len(bad):
            print(f"\n  --- sample {name} offenders (up to 15) ---")
            for v in bad["instrument_id"].astype(str).head(15).tolist():
                print(f"      {v}")

    # ---- non-gating coverage sample (open-q #14) ----
    print("\n===== COVERAGE SAMPLE (non-gating) =====")
    if "instrument_type" in df.columns:
        itype = df["instrument_type"].astype(str).str.upper()
        n_opt = int((itype == "OPTION").sum())
        n_fut = int((itype == "FUTURE").sum())
        n_perp_t = int((itype == "PERPETUAL").sum())
        n_spot = int((itype == "SPOT_PAIR").sum())
        print(f"  OPTION={n_opt:,}  FUTURE(dated)={n_fut:,}  PERPETUAL={n_perp_t:,}  SPOT_PAIR={n_spot:,}")
        cols = [c for c in ("venue", "raw_symbol", "instrument_id") if c in df.columns]
        for label, mask in (("OPTION", itype == "OPTION"), ("FUTURE", itype == "FUTURE")):
            sub = df[mask]
            if len(sub):
                print(f"  --- {label} raw_symbol → instrument_id (5 samples of {len(sub):,}) ---")
                for _, row in sub[cols].head(5).iterrows():
                    print("      " + "  ".join(f"{c}={row[c]}" for c in cols))
    else:
        print("  NOTE: no instrument_type column")

    green = (n_perp == 0) and (n_mismatch == 0) and (n_missing_quote == 0)
    print(f"\n[gate] GREEN={green}")
    return 0 if green else 1


if __name__ == "__main__":
    sys.exit(main())
