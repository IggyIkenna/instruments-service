#!/usr/bin/env python3
# Epic: instruments_master
# Lifecycle: one-off
# Delete-when: after the 24-league de-registration run (2026-07-13) is confirmed applied in prod
"""Recon step 3: collision check for the two re-keys (READ-ONLY).

For every captured atom under LA_LIGA_2 (-> SEGUNDA_DIVISION) and
SCOTTISH_LEAGUE_CUP_185 (-> SCOTTISH_LEAGUE_CUP):
  1. Does the canonical-target atom (same date+data_type+source) already exist in the index?
  2. Do the source GCS data objects actually exist (list day/entity prefixes, match league=)?
  3. Does a GCS object already exist at the target path (league= swapped)? Same or different content?

Usage:
  GCP_PROJECT_ID=central-element-323112 PROJECT_ID=central-element-323112 \
    DEPLOYMENT_ENV_SHORT=prd CLOUD_PROVIDER=gcp \
    python scripts/recon_dereg_collisions_2026_07_13.py <local_index.parquet> <out_collisions.parquet>
"""

from __future__ import annotations

import sys
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from unified_api_contracts.sports import SPORTS_DATA_TYPE_TO_FOLDER
from unified_trading_library import get_storage_client, resolve_bucket_name

REKEYS = {"LA_LIGA_2": "SEGUNDA_DIVISION", "SCOTTISH_LEAGUE_CUP_185": "SCOTTISH_LEAGUE_CUP"}
ATOM_COLS = ["league_id", "date", "data_type", "source"]
BY_DATE = "sports_reference/by_date/"


def main() -> int:
    index_path, out_path = sys.argv[1], sys.argv[2]
    bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")
    storage = get_storage_client()

    df = pd.read_parquet(index_path)
    df["league_id"] = df["league_id"].fillna("").astype(str)
    df["date"] = df["date"].astype(str)

    src_cap = df[(df["league_id"].isin(REKEYS)) & (df["capture_status"] == "captured")].copy()
    atoms = src_cap.drop_duplicates(subset=ATOM_COLS).copy()
    print(f"REKEY_CAPTURED_ATOMS={len(atoms)} (raw rows {len(src_cap)})")

    # 1. Index-level: target atom existence (any status) keyed on (target_league, date, data_type, source)
    tgt_rows = df[df["league_id"].isin(set(REKEYS.values()))]
    tgt_status: dict[tuple[str, str, str, str], str] = {}
    for r in tgt_rows.itertuples():
        key = (r.league_id, r.date, r.data_type, str(r.source))
        st = str(r.capture_status)
        prev = tgt_status.get(key)
        # keep the "strongest" status for reporting (captured > others)
        if prev is None or (st == "captured" and prev != "captured"):
            tgt_status[key] = st

    # 2+3. GCS listings per (day, entity_folder [, pipeline_mode]) — cached
    needed: set[tuple[str, str, str]] = set()  # (day, folder, pm_or_empty)
    for r in atoms.itertuples():
        folder = SPORTS_DATA_TYPE_TO_FOLDER.get(str(r.data_type), "")
        pm = str(r.pipeline_mode) if pd.notna(r.pipeline_mode) and str(r.pipeline_mode) else ""
        needed.add((r.date, folder, ""))
        if pm:
            needed.add((r.date, folder, pm))

    def _list(key: tuple[str, str, str]) -> tuple[tuple[str, str, str], dict[str, tuple[int, str]]]:
        day, folder, pm = key
        prefix = (
            f"{BY_DATE}day={day}/pipeline_mode={pm}/entity={folder}/" if pm else f"{BY_DATE}day={day}/entity={folder}/"
        )
        out: dict[str, tuple[int, str]] = {}
        for b in storage.list_blobs(bucket, prefix=prefix):
            out[b.name] = (int(b.size or 0), str(b.crc32c or ""))
        return key, out

    listings: dict[tuple[str, str, str], dict[str, tuple[int, str]]] = {}
    with ThreadPoolExecutor(max_workers=16) as ex:
        for key, blobs in ex.map(_list, sorted(needed)):
            listings[key] = blobs

    records: list[dict[str, object]] = []
    for r in atoms.itertuples():
        src_league = r.league_id
        tgt_league = REKEYS[src_league]
        folder = SPORTS_DATA_TYPE_TO_FOLDER.get(str(r.data_type), "")
        pm = str(r.pipeline_mode) if pd.notna(r.pipeline_mode) and str(r.pipeline_mode) else ""
        blobs: dict[str, tuple[int, str]] = {}
        blobs.update(listings.get((r.date, folder, ""), {}))
        if pm:
            blobs.update(listings.get((r.date, folder, pm), {}))

        src_marker = f"/league={src_league}/"
        src_objs = {n: m for n, m in blobs.items() if src_marker in n}
        tgt_key = (tgt_league, r.date, str(r.data_type), str(r.source))
        idx_target_status = tgt_status.get(tgt_key, "")

        same = diff = tgt_exist = 0
        pairs = []
        for n, (sz, crc) in sorted(src_objs.items()):
            tn = n.replace(src_marker, f"/league={tgt_league}/")
            tm = blobs.get(tn)
            if tm is not None:
                tgt_exist += 1
                if tm == (sz, crc):
                    same += 1
                else:
                    diff += 1
            pairs.append(f"{n} -> {tn} [{'ABSENT' if tm is None else ('SAME' if tm == (sz, crc) else 'DIFF')}]")

        records.append(
            {
                "league_id": src_league,
                "target_league": tgt_league,
                "date": r.date,
                "data_type": str(r.data_type),
                "source": str(r.source),
                "pipeline_mode": pm,
                "src_gcs_objects": len(src_objs),
                "src_object_names": "|".join(sorted(src_objs)),
                "index_target_atom_status": idx_target_status,
                "tgt_gcs_objects_existing": tgt_exist,
                "tgt_same_content": same,
                "tgt_diff_content": diff,
                "pairs": "|".join(pairs),
            }
        )

    res = pd.DataFrame(records)
    res.to_parquet(out_path, index=False)

    print("\n=== Summary by league x data_type ===")
    summ = res.groupby(["league_id", "data_type"]).agg(
        atoms=("date", "size"),
        atoms_with_src_obj=("src_gcs_objects", lambda s: int((s > 0).sum())),
        total_src_objs=("src_gcs_objects", "sum"),
        atoms_index_target_exists=("index_target_atom_status", lambda s: int((s != "").sum())),
        atoms_index_target_captured=("index_target_atom_status", lambda s: int((s == "captured").sum())),
        tgt_objs_existing=("tgt_gcs_objects_existing", "sum"),
        tgt_same=("tgt_same_content", "sum"),
        tgt_diff=("tgt_diff_content", "sum"),
    )
    print(summ.to_string())

    no_src = res[res["src_gcs_objects"] == 0]
    print(f"\nATOMS_WITH_NO_SOURCE_GCS_OBJECT={len(no_src)}")
    if len(no_src):
        print(no_src[["league_id", "date", "data_type", "source", "pipeline_mode"]].to_string(index=False))

    overl = res[res["tgt_gcs_objects_existing"] > 0]
    print(
        f"\nATOMS_WITH_TARGET_GCS_OBJECT_PRESENT={len(overl)} (same={int(res['tgt_same_content'].sum())}, diff={int(res['tgt_diff_content'].sum())})"
    )
    if len(overl):
        for _, row in overl.iterrows():
            print(f"  {row['league_id']} {row['date']} {row['data_type']} -> {row['pairs']}")

    idx_col = res[res["index_target_atom_status"] != ""]
    print(f"\nATOMS_WITH_INDEX_TARGET_ATOM={len(idx_col)}")
    print(
        idx_col.groupby(["league_id", "data_type", "index_target_atom_status"]).size().to_string()
        if len(idx_col)
        else "  none"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
