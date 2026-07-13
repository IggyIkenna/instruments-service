#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: one-off
# Delete-when: after the 24-league de-registration run (2026-07-13) is confirmed applied in prod
"""Recon step 3b/4a: targeted probes (READ-ONLY).

  A. For the 16 SCOTTISH_LEAGUE_CUP_185 captured atoms (2018 dates): list what
     actually exists under day=<D> for entity=fixture_stats (any league).
  B. Spot-check one LA_LIGA_2 vs SEGUNDA_DIVISION ODDS DIFF pair: row counts +
     column-level comparison to characterize the content difference.
  C. List _index/per_vm/ shards and count rows under the 24 dereg ids in each.
  D. Download prod/catalog.parquet and report which of the 24 ids it contains.

Usage:
  GCP_PROJECT_ID=... PROJECT_ID=... DEPLOYMENT_ENV_SHORT=prd CLOUD_PROVIDER=gcp \
    python scripts/recon_dereg_probes_2026_07_13.py
"""

from __future__ import annotations

import io
import sys

import pandas as pd
from unified_trading_library import get_storage_client, resolve_bucket_name

DEREG_IDS = {
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
}

SCOTTISH_DATES = [
    "2018-07-13",
    "2018-07-14",
    "2018-07-17",
    "2018-07-18",
    "2018-07-21",
    "2018-07-22",
    "2018-07-24",
    "2018-07-25",
    "2018-07-28",
    "2018-07-29",
    "2018-08-18",
    "2018-08-19",
    "2018-09-25",
    "2018-09-26",
    "2018-10-28",
    "2018-12-02",
]

SPOT_PAIR = (
    "sports_reference/by_date/day=2024-11-09/pipeline_mode=batch_footystats/entity=footystats_odds/fetched_at_hour=2026-06-29T05/league=LA_LIGA_2/footystats_odds.parquet",
    "sports_reference/by_date/day=2024-11-09/pipeline_mode=batch_footystats/entity=footystats_odds/fetched_at_hour=2026-06-29T05/league=SEGUNDA_DIVISION/footystats_odds.parquet",
)


def main() -> int:
    bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")
    storage = get_storage_client()

    print("=== A. SCOTTISH 2018 dates: fixture_stats listings (any league) ===")
    for d in SCOTTISH_DATES:
        names: list[str] = []
        for pfx in (
            f"sports_reference/by_date/day={d}/entity=fixture_stats/",
            f"sports_reference/by_date/day={d}/pipeline_mode=batch_api_football/entity=fixture_stats/",
        ):
            names.extend(b.name for b in storage.list_blobs(bucket, prefix=pfx))
        scot = [n for n in names if "SCOTTISH" in n]
        print(f"{d}: total_objs={len(names)} scottish_objs={scot if scot else 'NONE'}")

    print("\n=== B. Spot-check LA_LIGA_2 vs SEGUNDA_DIVISION ODDS pair (2024-11-09) ===")
    src_df = pd.read_parquet(io.BytesIO(storage.download_bytes(bucket, SPOT_PAIR[0])))
    tgt_df = pd.read_parquet(io.BytesIO(storage.download_bytes(bucket, SPOT_PAIR[1])))
    print(f"src rows={len(src_df)} cols={len(src_df.columns)}; tgt rows={len(tgt_df)} cols={len(tgt_df.columns)}")
    print(f"same columns: {list(src_df.columns) == list(tgt_df.columns)}")
    for c in ("canonical_league", "league_id", "league"):
        if c in src_df.columns:
            print(f"src[{c}] uniques: {src_df[c].unique()[:5]}; tgt[{c}] uniques: {tgt_df[c].unique()[:5]}")
    key_cols = [c for c in ("match_id", "fixture_id", "home_team", "away_team", "date") if c in src_df.columns]
    if key_cols:
        s = set(map(tuple, src_df[key_cols].astype(str).values.tolist()))
        t = set(map(tuple, tgt_df[key_cols].astype(str).values.tolist()))
        print(
            f"key cols {key_cols}: src_keys={len(s)} tgt_keys={len(t)} shared={len(s & t)} src_only={len(s - t)} tgt_only={len(t - s)}"
        )
    # full-content comparison ignoring league-label columns
    label_cols = [c for c in ("canonical_league", "league_id", "league") if c in src_df.columns]
    try:
        a = (
            src_df.drop(columns=label_cols)
            .sort_values(by=[c for c in src_df.columns if c not in label_cols][:3])
            .reset_index(drop=True)
        )
        b = (
            tgt_df.drop(columns=label_cols)
            .sort_values(by=[c for c in tgt_df.columns if c not in label_cols][:3])
            .reset_index(drop=True)
        )
        print(f"identical modulo league-label cols: {a.equals(b)}")
    except (KeyError, TypeError, ValueError) as e:
        print(f"modulo-label comparison failed: {e}")

    print("\n=== C. per-VM shards: rows under the 24 dereg ids ===")
    shard_names = [
        b.name
        for b in storage.list_blobs(bucket, prefix="_index/per_vm/")
        if b.name.endswith(".parquet") and ".bak" not in b.name
    ]
    print(f"per_vm shards: {len(shard_names)}")
    total = 0
    for n in shard_names:
        df = pd.read_parquet(io.BytesIO(storage.download_bytes(bucket, n)))
        if df.empty or "league_id" not in df.columns:
            continue
        hits = df[df["league_id"].fillna("").astype(str).isin(DEREG_IDS)]
        if len(hits):
            total += len(hits)
            print(f"  {n}: {len(hits)} rows -> {hits.groupby(['league_id', 'capture_status']).size().to_dict()}")
    print(f"PER_VM_TOTAL_DEREG_ROWS={total}")

    print("\n=== D. prod/catalog.parquet ===")
    cat = pd.read_parquet(io.BytesIO(storage.download_bytes(bucket, "prod/catalog.parquet")))
    print(f"catalog rows={len(cat)} cols={list(cat.columns)[:12]}")
    if "league_id" in cat.columns:
        lid = cat["league_id"].fillna("").astype(str)
        present = sorted(set(lid) & DEREG_IDS)
        print(f"dereg ids IN CATALOG: {present}")
        print(f"catalog distinct league_ids: {lid[lid != ''].nunique()}")
        sub = cat[lid.isin(DEREG_IDS)]
        if len(sub):
            keep = [
                c
                for c in ("instrument_id", "league_id", "sport", "source", "first_seen", "last_seen")
                if c in cat.columns
            ]
            print(sub[keep].to_string(index=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
