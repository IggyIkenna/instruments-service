# Epic: sports_master
# Lifecycle: one-off, resolves issues/sports_fixture_events_phantom_manifest_rows_2026_07_25.md
# Delete-when: the phantom-row campaign closes (re-census shows 0 captured rows with no backing object).
"""Re-derive the exact 4,991-row phantom `capture_status=captured` FIXTURE_EVENTS
manifest set (rows claiming captured with NO backing object at any candidate
path), and characterize their manifest provenance columns (`attempted_at`,
`written_at`, `enumerator_run_id`, `job_id`, `error_reason`) to root-cause
whether the writer ever persisted these objects.

The 2026-07-25 schema-variant census
(`census_fixture_events_schema_variants_2026_07_25.py`) already counted 4,991
"missing" objects but did not retain the (date, league_id) identity of that
subset (it only kept fixture_ids for objects that DID exist under a
non-canonical schema, to seed the recovery-ids parquet). This script re-walks
ONLY the FIXTURE_EVENTS captured population (same single manifest read; no new
whole-corpus GCS walk beyond re-checking object existence for this one
data_type) and writes the exact missing-row identity list + a provenance
census for root-cause.

Single-walk discipline: this re-derives the missing SUBSET's identity once;
the resulting parquet is the durable artifact used by the fix script
(`reflip_fixture_events_phantom_rows_2026_07_26.py`) — do not re-walk to
regenerate it.
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
    ap.add_argument("--out-missing-rows", default="scripts/_fixture_events_phantom_missing_rows_2026_07_26.parquet")
    ap.add_argument("--out-report", default="scripts/_fixture_events_phantom_root_cause_2026_07_26.json")
    args = ap.parse_args()

    from unified_api_contracts.canonical.domain.sports.gcs_paths import candidate_parquet_paths

    client = get_storage_client()
    raw = client.download_bytes(BUCKET, INDEX_PATH)
    df = pd.read_parquet(io.BytesIO(raw))

    fe = df[(df["data_type"] == "FIXTURE_EVENTS") & (df["capture_status"] == "captured")].copy()
    fe = fe.drop_duplicates(subset=["date", "league_id"])
    print(f"censusing {len(fe)} candidate FIXTURE_EVENTS captured rows", file=sys.stderr)

    # Cast to str up front — league_id has 435 genuine NaN values among captured
    # FIXTURE_EVENTS rows, and NaN != NaN breaks set-based key matching between
    # this loop's raw dict values and the later str-cast `fe["_key"]` filter.
    fe["date"] = fe["date"].astype(str)
    fe["league_id"] = fe["league_id"].astype(str)
    rows = fe[["date", "league_id"]].to_dict("records")

    def _work(row: dict[str, str]) -> tuple[str, str, bool]:
        day, league = row["date"], row["league_id"]
        paths = candidate_parquet_paths("FIXTURE_EVENTS", day, league, pipeline_mode="batch_api_football")
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
            if done % 5000 == 0:
                print(f"  {done}/{len(rows)} checked, missing so far={len(missing_keys)}", file=sys.stderr)
            day, league, exists = fut.result()
            if not exists:
                missing_keys.add((day, league))

    print(f"final missing count: {len(missing_keys)}", file=sys.stderr)

    fe["_key"] = list(zip(fe["date"].astype(str), fe["league_id"].astype(str), strict=False))
    missing_df = fe[fe["_key"].isin(missing_keys)].drop(columns=["_key"])

    keep_cols = [
        c
        for c in [
            "date",
            "league_id",
            "attempted_at",
            "written_at",
            "enumerator_run_id",
            "job_id",
            "error_reason",
            "instrument_count",
        ]
        if c in missing_df.columns
    ]
    missing_df[keep_cols].to_parquet(args.out_missing_rows, index=False)
    print(f"wrote {len(missing_df)} missing rows -> {args.out_missing_rows}", file=sys.stderr)

    # Root-cause characterization: bucket by year + written_at day (post-migration
    # re-stamps collapse to a handful of distinct timestamps if this was a bulk
    # manifest migration, vs. spread-out timestamps if the original per-fixture
    # writer actually ran and simply never persisted the object).
    missing_df["year"] = missing_df["date"].astype(str).str[:4]
    by_year = missing_df["year"].value_counts().sort_index().to_dict()

    written_at_col = (
        missing_df["written_at"].astype(str) if "written_at" in missing_df.columns else pd.Series(dtype=str)
    )
    distinct_written_at = int(written_at_col.nunique()) if len(written_at_col) else 0
    written_at_sample = sorted(written_at_col.unique().tolist())[:10]

    job_id_col = missing_df["job_id"].astype(str) if "job_id" in missing_df.columns else pd.Series(dtype=str)
    distinct_job_ids = int(job_id_col.nunique()) if len(job_id_col) else 0

    error_reason_col = (
        missing_df["error_reason"].astype(str) if "error_reason" in missing_df.columns else pd.Series(dtype=str)
    )
    error_reason_counts = error_reason_col.value_counts().to_dict()

    # Compare against the present (non-missing) FIXTURE_EVENTS population's
    # written_at distribution for the same years, to see whether missing rows
    # share the SAME re-stamp timestamps (bulk migration touched both present
    # and missing rows identically) or a DISTINCT one (missing rows are a
    # separate write-without-persist event).
    present_df = fe[~fe["_key"].isin(missing_keys)] if "_key" in fe.columns else fe
    present_written_at = (
        present_df["written_at"].astype(str) if "written_at" in present_df.columns else pd.Series(dtype=str)
    )
    distinct_present_written_at = int(present_written_at.nunique()) if len(present_written_at) else 0
    overlap_written_at = (
        len(set(written_at_col.unique()) & set(present_written_at.unique()))
        if len(written_at_col) and len(present_written_at)
        else 0
    )

    report = {
        "total_captured_fixture_events_rows_censused": len(rows),
        "missing_count": len(missing_keys),
        "missing_by_year": by_year,
        "missing_distinct_written_at_count": distinct_written_at,
        "missing_written_at_sample": written_at_sample,
        "present_distinct_written_at_count": distinct_present_written_at,
        "written_at_overlap_between_missing_and_present": overlap_written_at,
        "missing_distinct_job_ids": distinct_job_ids,
        "missing_error_reason_counts": error_reason_counts,
    }
    Path(args.out_report).write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2), file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
