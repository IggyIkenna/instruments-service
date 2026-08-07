# Epic: infrastructure_master
# Lifecycle: permanent
# Delete-when: NA
# TEAMS coverage census against the CONSOLIDATED sports availability index.
#
# Why this exists: the Track S2 TEAMS full-history backfill todo's done-when requires "a fresh
# coverage census cited" (plans/active/sports_consolidated_native_ao_extract_2026_07_25.md todo).
# This produces that census by reading the consolidated _index/availability_index.parquet and
# emitting the TEAMS 4-state capture_status distribution, the surviving-dedup-twin count, and the
# blank league_id representation.
#
# Re-run this; the numbers carry a date. Run AFTER the backfill VM reaches terminal STOPPED and the
# consolidator has rebuilt the index (verify with: gsutil stat <bucket>/_index/availability_index.parquet
# -> consolidator_run_at freshness), then cite the output in the plan flip.
#
# TRAPS HIT (2026-08-05, slot-4):
#   - The reader backfills missing columns to "" (read_availability_index/_read_index.py), so it CANNOT
#     distinguish raw NULL vs "" league_id — the option-B-collapse twin check MUST read the raw parquet
#     via pyarrow (as here), not the reader.
#   - TEAMS rows are data_type=="TEAMS" with instrument_type=""; the dedup dim is league_id (row_key
#     {date, data_type, league_id}), NOT instrument_id (None for TEAMS).
#   - Bounded: this reads the full 9.25M-row index (~1-2GB RSS projected); ALWAYS run under
#     scripts/dev/run-bounded-analysis.sh --mem-cap 6G (workspace memory-bounding HARD RULE).
#   - BATCH_API_FOOTBALL pipeline_mode, source=api_football — do not filter these out in the census.

"""Post-backfill TEAMS coverage census against the consolidated sports availability index.

Evidence cited by the Track S2 TEAMS backfill flip. Bounded column-projected pyarrow read of the
single consolidated index blob; run under run-bounded-analysis.sh --mem-cap 6G.
"""

from __future__ import annotations

import sys

import pyarrow.fs as pafs
import pyarrow.parquet as pq

BUCKET = "instruments-store-sports-prd-central-element-323112"
BLOB = "_index/availability_index.parquet"
COLS = [
    "date",
    "venue",
    "data_type",
    "service_name",
    "instrument_type",
    "league_id",
    "instrument_id",
    "capture_status",
    "source",
    "pipeline_mode",
]


def main() -> int:
    fs = pafs.GcsFileSystem()
    tbl = pq.read_table(f"{BUCKET}/{BLOB}", filesystem=fs, columns=COLS)
    df = tbl.to_pandas()
    print(f"loaded rows={len(df)}")

    teams = df[df["data_type"] == "TEAMS"].copy()
    del df
    print(f"TEAMS rows={len(teams)}")

    # 1) Fresh 4-state coverage distribution.
    print("capture_status distribution (TEAMS):")
    print(teams["capture_status"].value_counts(dropna=False).to_string())

    # 2) Option B still operating: 0 surviving normalized-key duplicate groups.
    teams["league_id_norm"] = teams["league_id"].where(
        teams["league_id"].notna() & (teams["league_id"] != ""), "__EMPTY__"
    )
    teams["instrument_type_norm"] = teams["instrument_type"].fillna("")
    teams["instrument_id_norm"] = teams["instrument_id"].where(
        teams["instrument_id"].notna() & (teams["instrument_id"] != ""), "__EMPTY__"
    )
    key_cols = [
        "date",
        "venue",
        "data_type",
        "service_name",
        "instrument_type_norm",
        "league_id_norm",
        "instrument_id_norm",
        "pipeline_mode",
    ]
    grp = teams.groupby(key_cols, dropna=False).size().reset_index(name="n")
    twins = grp[grp["n"] > 1]
    print(f"surviving normalized-key duplicate groups (must be 0): {len(twins)}")

    # 3) Blank-league_id representation (NULL vs "" vs populated).
    print(f"TEAMS rows NULL league_id: {int(teams['league_id'].isna().sum())}")
    print(f"TEAMS rows '' league_id:  {int((teams['league_id'] == '').sum())}")

    # 4) Date span actually captured (sanity: should span 2020-06-06 → 2026-08-05).
    captured_dates = teams.loc[teams["capture_status"] == "captured", "date"]
    if len(captured_dates):
        print(f"captured TEAMS date span: {captured_dates.min()} → {captured_dates.max()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
