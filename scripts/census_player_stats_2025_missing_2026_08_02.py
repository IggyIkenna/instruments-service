# Epic: sports_master
# Lifecycle: one-off, resolves the 2025 follow-up in
#   plans/active/issues/canonical_player_stats_fixture_events_quality_2026_07_16.md
# Delete-when: the 2025-cell root-cause investigation closes.
"""Re-derive the 88 (of 1,298) `PLAYER_STATS` manifest `capture_status=captured`
cells dated 2025 that have NO backing GCS object at any candidate path, and
characterize their manifest provenance columns (`attempted_at`, `written_at`,
`enumerator_run_id`, `job_id`, `error_reason`, `instrument_count`) to root-cause
whether this is a live write-completion race, a later deletion, or another
current-pipeline gap.

Mirrors `census_fixture_events_phantom_missing_2026_07_26.py`'s methodology
(same single-manifest-read, real-path-existence-check pattern) applied to
PLAYER_STATS and pre-filtered to `day` startswith "2025" — the population this
finding's own follow-up scoped investigation to (the 2018-2020 population is
already root-caused separately as the Defect-3 writer-generation quirk).

Single-walk discipline: one manifest index read; per-cell existence checks are
bounded to the ~1,298 candidate rows the prior 2026-07-26 census already sized,
not a corpus walk.
"""

from __future__ import annotations

import argparse
import io
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
from unified_trading_library import get_storage_client

BUCKET = "instruments-store-sports-prd-central-element-323112"
INDEX_PATH = "_index/availability_index.parquet"


def _object_exists(bucket: str, path: str) -> bool:
    client = get_storage_client()
    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            return client.blob_exists(bucket, path)
        except Exception as exc:
            last_exc = exc
            time.sleep(0.2 * (attempt + 1))
    assert last_exc is not None
    raise last_exc


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=40)
    ap.add_argument("--out-missing-rows", default="scripts/_player_stats_2025_missing_rows_2026_08_02.parquet")
    ap.add_argument("--out-report", default="scripts/_player_stats_2025_missing_root_cause_2026_08_02.json")
    args = ap.parse_args()

    from unified_api_contracts.canonical.domain.sports.gcs_paths import candidate_parquet_paths

    client = get_storage_client()
    raw = client.download_bytes(BUCKET, INDEX_PATH)
    df = pd.read_parquet(io.BytesIO(raw))

    ps = df[(df["data_type"] == "PLAYER_STATS") & (df["capture_status"] == "captured")].copy()
    ps["date"] = ps["date"].astype(str)
    ps["league_id"] = ps["league_id"].astype(str)
    ps = ps[ps["date"].str.startswith("2025")]
    ps = ps.drop_duplicates(subset=["date", "league_id"])
    print(f"censusing {len(ps)} candidate PLAYER_STATS captured rows dated 2025", file=sys.stderr)

    rows = ps[["date", "league_id"]].to_dict("records")

    def _work(row: dict[str, str]) -> tuple[str, str, bool]:
        day, league = row["date"], row["league_id"]
        paths = candidate_parquet_paths("PLAYER_STATS", day, league, pipeline_mode="batch_api_football")
        for p in paths:
            try:
                if _object_exists(BUCKET, p):
                    return day, league, True
            except Exception:
                return day, league, True  # do not mis-flag a read_error as missing
        return day, league, False

    missing_keys: set[tuple[str, str]] = set()
    done = 0
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(_work, r): r for r in rows}
        for fut in as_completed(futures):
            done += 1
            if done % 200 == 0:
                print(f"  {done}/{len(rows)} checked, missing so far={len(missing_keys)}", file=sys.stderr)
            day, league, exists = fut.result()
            if not exists:
                missing_keys.add((day, league))

    print(f"final missing count: {len(missing_keys)}", file=sys.stderr)

    ps["_key"] = list(zip(ps["date"].astype(str), ps["league_id"].astype(str), strict=False))
    missing_df = ps[ps["_key"].isin(missing_keys)].drop(columns=["_key"])

    keep_cols = [
        c
        for c in [
            "date",
            "league_id",
            "pipeline_mode",
            "attempted_at",
            "written_at",
            "enumerator_run_id",
            "job_id",
            "error_reason",
            "instrument_count",
            "source",
        ]
        if c in missing_df.columns
    ]
    missing_df[keep_cols].to_parquet(args.out_missing_rows, index=False)
    print(f"wrote {len(missing_df)} missing rows -> {args.out_missing_rows}", file=sys.stderr)

    # Root-cause characterization, same shape as the fixture_events sibling script:
    written_at_col = (
        missing_df["written_at"].astype(str) if "written_at" in missing_df.columns else pd.Series(dtype=str)
    )
    distinct_written_at = int(written_at_col.nunique()) if len(written_at_col) else 0
    written_at_sorted = sorted(written_at_col.unique().tolist())

    attempted_at_col = (
        missing_df["attempted_at"].astype(str) if "attempted_at" in missing_df.columns else pd.Series(dtype=str)
    )
    distinct_attempted_at = int(attempted_at_col.nunique()) if len(attempted_at_col) else 0

    job_id_col = missing_df["job_id"].astype(str) if "job_id" in missing_df.columns else pd.Series(dtype=str)
    distinct_job_ids = int(job_id_col.nunique()) if len(job_id_col) else 0
    job_id_sample = sorted(job_id_col.unique().tolist())[:20]

    enum_run_id_col = (
        missing_df["enumerator_run_id"].astype(str)
        if "enumerator_run_id" in missing_df.columns
        else pd.Series(dtype=str)
    )
    distinct_enum_run_ids = int(enum_run_id_col.nunique()) if len(enum_run_id_col) else 0

    error_reason_col = (
        missing_df["error_reason"].astype(str) if "error_reason" in missing_df.columns else pd.Series(dtype=str)
    )
    error_reason_counts = error_reason_col.value_counts().to_dict()

    instrument_count_col = (
        missing_df["instrument_count"] if "instrument_count" in missing_df.columns else pd.Series(dtype=float)
    )
    instrument_count_counts = instrument_count_col.value_counts().to_dict()

    # Compare against the PRESENT (non-missing) 2025 PLAYER_STATS population's
    # written_at distribution — same-timestamp overlap suggests a bulk event
    # touched both; a distinct cluster suggests a separate write-without-persist
    # (or a separate delete) event unique to the missing rows.
    present_df = ps[~ps["_key"].isin(missing_keys)] if "_key" in ps.columns else ps
    present_written_at = (
        present_df["written_at"].astype(str) if "written_at" in present_df.columns else pd.Series(dtype=str)
    )
    overlap_written_at = (
        len(set(written_at_col.unique()) & set(present_written_at.unique()))
        if len(written_at_col) and len(present_written_at)
        else 0
    )

    # Check for a matching attempted_failed row on the SAME (date, league_id,
    # pipeline_mode) key anywhere in the full index (not just the 2025-captured
    # subset) — a same-key row can't hold both states simultaneously today, so
    # a match here would only appear if a LATER attempt re-flipped the state,
    # which the current snapshot can't distinguish without a write history.
    af = df[(df["data_type"] == "PLAYER_STATS") & (df["capture_status"] == "attempted_failed")].copy()
    af["date"] = af["date"].astype(str)
    af["league_id"] = af["league_id"].astype(str)
    af_keys = set(zip(af["date"], af["league_id"], strict=False))
    matching_attempted_failed = sorted(k for k in missing_keys if k in af_keys)

    league_counts = missing_df["league_id"].value_counts().to_dict()
    date_counts = missing_df["date"].value_counts().sort_index().to_dict()

    report = {
        "total_captured_player_stats_2025_rows_censused": len(rows),
        "missing_count": len(missing_keys),
        "missing_by_date": date_counts,
        "missing_by_league_id": league_counts,
        "missing_distinct_written_at_count": distinct_written_at,
        "missing_written_at_all": written_at_sorted,
        "missing_distinct_attempted_at_count": distinct_attempted_at,
        "written_at_overlap_between_missing_and_present_2025": overlap_written_at,
        "missing_distinct_job_ids": distinct_job_ids,
        "missing_job_id_sample": job_id_sample,
        "missing_distinct_enumerator_run_ids": distinct_enum_run_ids,
        "missing_error_reason_counts": error_reason_counts,
        "missing_instrument_count_distribution": {str(k): v for k, v in instrument_count_counts.items()},
        "matching_attempted_failed_same_key_count": len(matching_attempted_failed),
        "matching_attempted_failed_same_key_sample": matching_attempted_failed[:20],
    }
    Path(args.out_report).write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2), file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
