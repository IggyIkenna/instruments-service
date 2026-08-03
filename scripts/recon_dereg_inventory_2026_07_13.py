#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: one-off
# Delete-when: after the 24-league de-registration run (2026-07-13) is confirmed applied in prod
"""Recon step 2: exact inventory of the 24 to-be-deregistered league_ids (LOCAL, read-only).

Reads the locally downloaded availability-index snapshot, builds:
  - per (league_id, data_type, source, capture_status) raw + atom-deduped counts,
  - every captured atom (league, date, data_type, source) with its expected GCS data path(s),
  - total captured per league_id,
  - universe math check: distinct league_ids - 24 == 94 == get_expected_leagues_for_source('api_football').

Usage:
  python scripts/recon_dereg_inventory_2026_07_13.py <local_index.parquet> <out_inventory.parquet>
"""

from __future__ import annotations

import sys

import pandas as pd
from unified_api_contracts.sports import (
    LEAGUE_REGISTRY,
    candidate_parquet_paths,
    canonicalize_league_id,
    get_expected_leagues_for_source,
)

DEREG_IDS = [
    "110",
    "119",
    "122",
    "15066",
    "235",
    "236",
    "239",
    "244",
    "253",
    "254",
    "283",
    "315",
    "32",
    "357",
    "358",
    "362",
    "365",
    "408",
    "493",
    "71",
    "850",
    "LA_LIGA_2",
    "RFPL",
    "SCOTTISH_LEAGUE_CUP_185",
]

ATOM_COLS = ["league_id", "date", "data_type", "source"]


def main() -> int:
    index_path, out_path = sys.argv[1], sys.argv[2]
    df = pd.read_parquet(index_path)
    df["league_id"] = df["league_id"].fillna("").astype(str)
    total_rows = len(df)

    distinct_ids = sorted(x for x in df["league_id"].unique() if x)
    n_distinct = len(distinct_ids)

    expected_af = {ld.league_id for ld in get_expected_leagues_for_source("api_football")}
    dereg = set(DEREG_IDS)
    post_removal = set(distinct_ids) - dereg
    missing_dereg = sorted(dereg - set(distinct_ids))
    identical = post_removal == expected_af

    print(f"TOTAL_INDEX_ROWS={total_rows}")
    print(f"DISTINCT_LEAGUE_IDS={n_distinct}")
    print(f"DEREG_IDS_PRESENT={len(dereg & set(distinct_ids))}/24 (missing: {missing_dereg})")
    print(f"POST_REMOVAL_COUNT={len(post_removal)} EXPECTED_AF_COUNT={len(expected_af)}")
    print(f"POST_REMOVAL_SET_IDENTICAL_TO_EXPECTED_AF={identical}")
    if not identical:
        print(f"  post_removal - expected: {sorted(post_removal - expected_af)}")
        print(f"  expected - post_removal: {sorted(expected_af - post_removal)}")
    print(f"LEAGUE_REGISTRY_SIZE={len(LEAGUE_REGISTRY)}")
    non_football = sorted(set(LEAGUE_REGISTRY) - expected_af - set(distinct_ids))
    print(f"REGISTRY_NOT_IN_INDEX_NOT_AF={non_football}")

    sub = df[df["league_id"].isin(dereg)].copy()
    print(f"DEREG_TOTAL_ROWS={len(sub)}")

    # Raw counts per league x data_type x source x capture_status
    grp = (
        sub.groupby(["league_id", "data_type", "source", "capture_status"], dropna=False)
        .agg(raw_rows=("date", "size"), atoms=("date", lambda s: s.nunique()))
        .reset_index()
    )
    # Deduped atom counts (distinct league,date,data_type,source per status)
    dedup = sub.drop_duplicates(subset=[*ATOM_COLS, "capture_status"])
    grp_dedup = (
        dedup.groupby(["league_id", "data_type", "source", "capture_status"], dropna=False)
        .size()
        .rename("dedup_rows")
        .reset_index()
    )
    grp = grp.merge(grp_dedup, on=["league_id", "data_type", "source", "capture_status"], how="left")

    cap = sub[sub["capture_status"] == "captured"].copy()
    cap_atoms = cap.drop_duplicates(subset=ATOM_COLS).copy()

    def _paths(row: pd.Series) -> str:
        pm = row.get("pipeline_mode")
        pm = str(pm) if pd.notna(pm) and str(pm) else None
        cands = candidate_parquet_paths(
            str(row["data_type"]), str(row["date"]), str(row["league_id"]), pipeline_mode=pm
        )
        return "|".join(cands)

    cap_atoms["date"] = cap_atoms["date"].astype(str)
    cap_atoms["expected_gcs_paths"] = cap_atoms.apply(_paths, axis=1)
    cap_atoms["canonical_target"] = cap_atoms["league_id"].map(
        lambda x: canonicalize_league_id(x) if canonicalize_league_id(x) != x else ""
    )

    # Save machine-readable inventory: one row per captured atom + summary tables appended as separate parquets
    keep = [
        *ATOM_COLS,
        "pipeline_mode",
        "instrument_count",
        "row_count",
        "written_at",
        "expected_gcs_paths",
        "canonical_target",
    ]
    inv = cap_atoms[[c for c in keep if c in cap_atoms.columns]].copy()
    inv["written_at"] = inv["written_at"].astype(str)
    inv.to_parquet(out_path, index=False)
    grp.to_parquet(out_path.replace(".parquet", "_counts.parquet"), index=False)

    print("\n=== Per-league captured totals (raw rows / deduped atoms) ===")
    cap_tot = cap.groupby("league_id").size().rename("captured_raw")
    atom_tot = cap_atoms.groupby("league_id").size().rename("captured_atoms")
    tot = pd.concat([cap_tot, atom_tot], axis=1).fillna(0).astype(int)
    print(tot.to_string())
    print(f"\nTOTAL_CAPTURED_RAW={len(cap)} TOTAL_CAPTURED_ATOMS={len(cap_atoms)}")

    print("\n=== Counts by league x data_type x source x capture_status ===")
    print(grp.to_string(index=False))

    # Per-status totals across the 24
    print("\n=== Status totals across the 24 ===")
    print(sub.groupby("capture_status").size().to_string())
    return 0


if __name__ == "__main__":
    sys.exit(main())
