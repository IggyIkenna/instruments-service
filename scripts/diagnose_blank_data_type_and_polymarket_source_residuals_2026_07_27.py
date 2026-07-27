#!/usr/bin/env python3
# Epic: mtds_mdps_master
# Lifecycle: oneoff
# Delete-when: after the blank-data_type phantom marker + batch_polymarket_clob blank-source
# residual diagnosis (prediction_satellite_ao_dispatch_batch2 todo, prediction_phantom_reconciler_wipes_bundle_atom_2026_07_10.md) lands + is verified
"""Read-only diagnosis for two prediction `_index` residual row-sets, per
plans/active/prediction_satellite_ao_dispatch_batch2_2026_07_25.md's combined
residual-row-diagnosis todo:

(a) 17 blank-`data_type` phantom aggregate-marker rows — re-verify each row's
    supersession/genuine-phantom status.
(b) `batch_polymarket_clob` blank-`source` rows — the source issue measured
    27,292 such rows on 2026-07-10; the later 2026-07-19
    canonicalize_prediction_manifest_2026_07_18.py --dry-run measured only 2.

NO writes. Reads the consolidated _index once (single-walk discipline).
"""

from __future__ import annotations

import io
import sys

import pandas as pd
from unified_trading_library import get_storage_client, resolve_bucket_name

INDEX_BLOB = "_index/availability_index.parquet"


def main() -> int:
    bucket = resolve_bucket_name(cloud="gcp", kind="market-data-tick-prediction")
    storage = get_storage_client()
    print(f"Reading gs://{bucket}/{INDEX_BLOB}")
    raw = storage.download_bytes(bucket, INDEX_BLOB)
    df = pd.read_parquet(io.BytesIO(raw))
    print(f"Total rows: {len(df)}")

    cs = df["capture_status"].fillna("").astype(str)
    dt = df["data_type"].fillna("").astype(str)
    pm = df["pipeline_mode"].fillna("").astype(str) if "pipeline_mode" in df.columns else pd.Series([""] * len(df))
    src = df["source"].fillna("").astype(str) if "source" in df.columns else pd.Series([""] * len(df))
    venue = df["venue"].fillna("").astype(str) if "venue" in df.columns else pd.Series([""] * len(df))

    # --- (a) blank data_type rows ---
    print("\n=== (a) blank data_type rows ===")
    blank_dt_mask = dt.str.strip() == ""
    df_a = df[blank_dt_mask]
    print(f"blank data_type rows: {len(df_a)}")
    if len(df_a):
        print("capture_status dist:", cs[blank_dt_mask].value_counts().to_dict())
        print("venue dist:", venue[blank_dt_mask].value_counts().to_dict())
        print("pipeline_mode dist:", pm[blank_dt_mask].value_counts().to_dict())
        print("source dist:", src[blank_dt_mask].value_counts().to_dict())
        if "error_reason" in df.columns:
            print(
                "error_reason dist:",
                df.loc[blank_dt_mask, "error_reason"].fillna("").astype(str).value_counts().to_dict(),
            )
        if "date" in df.columns:
            print(
                "date range:",
                df_a["date"].astype(str).min(),
                "->",
                df_a["date"].astype(str).max(),
            )
        if "instrument_id" in df.columns:
            print(
                "sample instrument_id values:",
                df_a["instrument_id"].dropna().astype(str).unique()[:20].tolist(),
            )
        if "written_at" in df.columns:
            print(
                "written_at range:",
                df_a["written_at"].astype(str).min(),
                "->",
                df_a["written_at"].astype(str).max(),
            )
        print("\nfull row dump (all columns, up to 17 rows):")
        with pd.option_context("display.max_columns", None, "display.width", 220):
            print(df_a.to_string())
    else:
        print("0 blank-data_type rows — already clean.")

    # --- (b) batch_polymarket_clob blank-source rows ---
    print("\n=== (b) batch_polymarket_clob blank-source rows ===")
    is_bpc = pm == "batch_polymarket_clob"
    is_blank_src = src.str.strip() == ""
    b_mask = is_bpc & is_blank_src
    df_b = df[b_mask]
    print(f"batch_polymarket_clob blank-source rows: {len(df_b)}")
    print(f"(all batch_polymarket_clob rows, any source: {int(is_bpc.sum())})")
    if len(df_b):
        print("capture_status dist:", cs[b_mask].value_counts().to_dict())
        print("venue dist:", venue[b_mask].value_counts().to_dict())
        if "date" in df.columns:
            print(
                "date range:",
                df_b["date"].astype(str).min(),
                "->",
                df_b["date"].astype(str).max(),
            )
        if "written_at" in df.columns:
            print(
                "written_at range:",
                df_b["written_at"].astype(str).min(),
                "->",
                df_b["written_at"].astype(str).max(),
            )
    else:
        print("0 rows — already resolved.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
