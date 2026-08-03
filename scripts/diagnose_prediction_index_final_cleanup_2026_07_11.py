#!/usr/bin/env python3
# Epic: mtds_mdps_master
# Lifecycle: oneoff
# Delete-when: after prediction _index final residual cleanup 2026-07-11 lands + is verified
"""Read-only diagnosis for the prediction pred-prd _index final residual cleanup.

Classes:
  A. v4/v5 POLYMARKET trades/prediction_trades captured rows, blank source — check
     whether a captured v9 prediction_canonical_question_group bundle exists for the
     same (date, venue) [superseded-vs-kept verdict].
  B. phantom rows (error_reason=phantom_captured_no_parquet_at_canonical_path) — report
     distribution (venue/data_type) so the unphantom pass impact can be sized.
  C. lowercase venue="kalshi" duplicate rows vs canonical "KALSHI".

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
    print(f"columns: {sorted(df.columns)}")

    cs = df["capture_status"].fillna("").astype(str)
    venue = df["venue"].fillna("").astype(str)
    src = df["source"].fillna("").astype(str) if "source" in df.columns else pd.Series([""] * len(df))
    dt = df["data_type"].fillna("").astype(str)
    sv = df["schema_version"] if "schema_version" in df.columns else pd.Series([None] * len(df))

    print("\n=== capture_status distribution ===")
    print(cs.value_counts(dropna=False).to_string())

    print("\n=== schema_version distribution ===")
    print(sv.value_counts(dropna=False).to_string())

    # --- Class A ---
    print("\n=== CLASS A: v4/v5 POLYMARKET trades/prediction_trades, blank source, captured ===")
    sv_str = sv.astype(str)
    is_v4_v5 = sv_str.isin(["4", "5", "4.0", "5.0"])
    is_trades_dt = dt.isin(["trades", "prediction_trades"])
    is_polymarket = venue.str.upper() == "POLYMARKET"
    is_blank_src = src.str.strip() == ""
    is_captured = cs == "captured"
    a_mask = is_v4_v5 & is_trades_dt & is_polymarket & is_blank_src & is_captured
    df_a = df[a_mask]
    print(f"Class A candidate rows: {len(df_a)}")
    if len(df_a):
        print("date range:", df_a["date"].astype(str).min(), "->", df_a["date"].astype(str).max())
        print("data_type dist:", dt[a_mask].value_counts().to_dict())
        print("schema_version dist:", sv_str[a_mask].value_counts().to_dict())
        # instrument_id sample - to derive cqg via object if possible
        if "instrument_id" in df_a.columns:
            print("sample instrument_id values:", df_a["instrument_id"].dropna().astype(str).unique()[:10].tolist())
        if "underlying" in df_a.columns:
            print(
                "underlying (cqg) dist top 15:",
                df_a["underlying"].fillna("").astype(str).value_counts().head(15).to_dict(),
            )

        # Bundle existence check: for each (date, venue=POLYMARKET) in class A, does a
        # captured v9 prediction_canonical_question_group bundle row exist for the same date?
        bundle_mask = (
            (dt == "prediction_canonical_question_group") & (cs == "captured") & (venue.str.upper() == "POLYMARKET")
        )
        bundle_dates = set(df.loc[bundle_mask, "date"].astype(str).unique())
        print(f"\nCaptured v9 POLYMARKET bundle dates available: {len(bundle_dates)}")
        a_dates = df_a["date"].astype(str)
        superseded = a_dates.isin(bundle_dates)
        n_superseded = int(superseded.sum())
        n_not_superseded = len(df_a) - n_superseded
        print(f"Class A rows WITH a same-date captured v9 POLYMARKET bundle (superseded): {n_superseded}")
        print(f"Class A rows WITHOUT a same-date captured v9 POLYMARKET bundle (NOT superseded): {n_not_superseded}")
        if n_not_superseded:
            not_superseded_dates = sorted(a_dates[~superseded].unique())
            print(f"Sample not-superseded dates (up to 20): {not_superseded_dates[:20]}")
    else:
        print("No Class A rows found under this predicate — check filter assumptions.")

    # Broader schema_version 4/5 sweep across ALL venues/data_types (to catch scope drift)
    print("\n=== Class A (broad): ALL v4/v5 rows regardless of venue/data_type/status ===")
    broad_v4v5 = df[is_v4_v5]
    print(f"Total v4/v5 rows: {len(broad_v4v5)}")
    if len(broad_v4v5):
        print("venue dist:", venue[is_v4_v5].value_counts().to_dict())
        print("data_type dist:", dt[is_v4_v5].value_counts().to_dict())
        print("capture_status dist:", cs[is_v4_v5].value_counts().to_dict())
        print("blank-source count:", int((src[is_v4_v5].str.strip() == "").sum()))

    # --- Class B ---
    print("\n=== CLASS B: phantom rows ===")
    err_col = "error_reason" if "error_reason" in df.columns else None
    if err_col:
        err = df[err_col].fillna("").astype(str)
        phantom_mask = err.str.startswith("phantom_captured_no_parquet_at_canonical")
        df_b = df[phantom_mask]
        print(f"Phantom rows total: {len(df_b)}")
        if len(df_b):
            print("venue dist:", venue[phantom_mask].value_counts().to_dict())
            print("data_type dist:", dt[phantom_mask].value_counts().to_dict())
            print("capture_status dist (should be attempted_failed):", cs[phantom_mask].value_counts().to_dict())
            print("date range:", df_b["date"].astype(str).min(), "->", df_b["date"].astype(str).max())
    else:
        print("No error_reason column present.")

    # --- Class C ---
    print("\n=== CLASS C: lowercase venue='kalshi' duplicate rows ===")
    lower_kalshi_mask = venue == "kalshi"
    df_c = df[lower_kalshi_mask]
    print(f"lowercase 'kalshi' rows: {len(df_c)}")
    upper_kalshi_mask = venue == "KALSHI"
    print(f"uppercase 'KALSHI' rows: {int(upper_kalshi_mask.sum())}")
    if len(df_c):
        print("capture_status dist:", cs[lower_kalshi_mask].value_counts().to_dict())
        print("data_type dist:", dt[lower_kalshi_mask].value_counts().to_dict())
        # Check byte-identical duplicate against uppercase counterpart on shared key columns
        key_cols = [
            c
            for c in ("date", "data_type", "capture_status", "pipeline_mode", "source", "error_reason")
            if c in df.columns
        ]
        print(f"key cols used for dup-compare: {key_cols}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
