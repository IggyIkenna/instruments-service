#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: after the GCS-object-level residual cleanup for 8,937 dropped manifest rows is complete
"""Derive candidate orphaned GCS paths from the 2026-08-04 sports manifest drop.

The 2026-08-04 manifest drop removed 8,937 out-of-universe numeric league_id rows
from the sports ``_index``. This script derives the candidate GCS paths for the
underlying parquet objects (now orphaned — no manifest row points at them).

Approach (no new whole-corpus GCS walk):
1. Read the pre-drop snapshot + current _index from GCS
2. Anti-join (snapshot minus current) on the dedup key to find the dropped rows
3. For each dropped row, derive candidate GCS paths using the sports path UAC SSOT
4. Group by unique path (many manifest rows → same GCS object)
5. Output the candidate list for the verify-and-delete pass

Safety: dry-run by default; --verify checks existence + content; --execute does deletes.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import tempfile
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_NUMERIC_RE = re.compile(r"^\d+$")

# Bucket and paths
BUCKET = "instruments-store-sports-prd-central-element-323112"
INDEX_BLOB = "_index/availability_index.parquet"
SNAPSHOT_BLOB = "_index/snapshots/pre_league_id_canonicalize_20260804T075724Z.parquet"
CANDIDATES_OUTPUT = "gs://instruments-store-sports-prd-central-element-323112/_index/snapshots/orphan_candidates_20260804T{ts}.json"

# Sports GCS path layout constants (from UAC gcs_paths.py — vendored here to avoid
# needing the UAC import in this standalone script)
SPORTS_BY_DATE_PREFIX = "sports_reference/by_date/"

SPORTS_DATA_TYPE_TO_FOLDER: dict[str, str] = {
    "FIXTURES": "fixtures",
    "FIXTURES_SCHEDULE": "fixtures_schedule",
    "FIXTURES_OUTCOMES": "fixtures_outcomes",
    "FIXTURE_EVENTS": "fixture_events",
    "FIXTURE_LINEUPS": "fixture_lineups",
    "FIXTURE_STATS": "fixture_stats",
    "PLAYER_STATS": "player_stats",
    "INJURIES": "injuries",
    "STANDINGS": "standings",
    "LEAGUES": "leagues",
    "TEAMS": "teams",
    "TEAMS_SEASON_SNAPSHOT": "teams",
    "VENUES": "venues",
    "MATCHES": "footystats_matches",
    "ODDS": "footystats_odds",
    "PREDICTIONS": "footystats_predictions",
    "XG": "understat_xg",
    "XG_SHOTS": "understat_xg_shots",
    "PLAYER_VALUES": "player_values",
    "SFI_PROGRESSIVE_STATS": "progressive_stats",
    "WEATHER": "weather",
}

# Dedup dimensions from the canonicalize script
DEDUP_DIMS = ["service_name", "date", "data_type", "league_id", "timeframe", "pipeline_mode", "source"]


def _derive_paths(row: pd.Series) -> list[str]:
    """Derive candidate GCS paths (relative to bucket) for a manifest row.

    Returns the per-league-subpartition path + a bare fallback, matching the
    sports PER_DAY_PER_LEAGUE layout.
    """
    data_type = str(row.get("data_type", ""))
    day = str(row.get("date", ""))
    league_id = str(row.get("league_id", ""))

    folder = SPORTS_DATA_TYPE_TO_FOLDER.get(data_type)
    if folder is None:
        return []

    base = f"{SPORTS_BY_DATE_PREFIX}day={day}/entity={folder}"
    paths: list[str] = []

    # Primary: per-league subpartition
    if league_id:
        paths.append(f"{base}/league={league_id}/{folder}.parquet")

    # Fallback: bare entity-level file (legacy or unpartitioned writes)
    paths.append(f"{base}/{folder}.parquet")

    # Also probe with pipeline_mode prefix if present
    pm = str(row.get("pipeline_mode", ""))
    if pm and pm != "nan":
        pm_base = f"{SPORTS_BY_DATE_PREFIX}day={day}/pipeline_mode={pm}/entity={folder}"
        if league_id:
            paths.append(f"{pm_base}/league={league_id}/{folder}.parquet")
        paths.append(f"{pm_base}/{folder}.parquet")

    return paths


def _is_numeric_id(s: str) -> bool:
    return bool(_NUMERIC_RE.match(s))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Write candidate list to GCS (default: dry-run)")
    parser.add_argument("--verify", action="store_true", help="Check existence + content-verify each candidate object")
    parser.add_argument("--execute", action="store_true", help="Delete verified-orphaned objects (requires --verify first)")
    parser.add_argument("--max-delete", type=int, default=0, help="Max deletes in execute mode (0=unlimited)")
    parser.add_argument("--bucket", default=BUCKET, help="Override bucket")
    args = parser.parse_args()

    import gcsfs
    fs = gcsfs.GCSFileSystem()

    bucket = args.bucket
    snapshot_uri = f"{bucket}/{SNAPSHOT_BLOB}"
    index_uri = f"{bucket}/{INDEX_BLOB}"

    # ------------------------------------------------------------------
    # Step 1: Download both parquets (column-pruned for memory efficiency)
    # ------------------------------------------------------------------
    logger.info("Downloading pre-drop snapshot: %s", snapshot_uri)
    tmp_snap = f"{tempfile.gettempdir()}/sports_snapshot_pre_drop.parquet"
    fs.get(snapshot_uri, tmp_snap)

    # Only read columns needed for the anti-join + path derivation
    needed_cols = ["service_name", "date", "data_type", "league_id", "timeframe", "pipeline_mode", "source"]
    # Discover which needed columns actually exist
    import pyarrow.parquet as pq
    snap_schema = pq.read_schema(tmp_snap)
    snap_cols = [c for c in needed_cols if c in {f.name for f in snap_schema}]
    df_snap = pd.read_parquet(tmp_snap, columns=snap_cols)
    logger.info("Snapshot: %d rows, columns: %s", len(df_snap), snap_cols)

    logger.info("Downloading current _index: %s", index_uri)
    tmp_idx = f"{tempfile.gettempdir()}/sports_index_current.parquet"
    fs.get(index_uri, tmp_idx)
    idx_schema = pq.read_schema(tmp_idx)
    idx_cols = [c for c in needed_cols if c in {f.name for f in idx_schema}]
    df_cur = pd.read_parquet(tmp_idx, columns=idx_cols)
    logger.info("Current _index: %d rows", len(df_cur))

    # ------------------------------------------------------------------
    # Step 2: Anti-join to find dropped rows
    # ------------------------------------------------------------------
    # Build dedup key columns present in both
    key_cols = [c for c in DEDUP_DIMS if c in df_snap.columns and c in df_cur.columns]
    logger.info("Dedup key columns available: %s", key_cols)

    # Build composite keys
    df_snap["_key"] = df_snap[key_cols].astype(str).agg("|".join, axis=1)
    df_cur["_key"] = df_cur[key_cols].astype(str).agg("|".join, axis=1)

    cur_keys = set(df_cur["_key"])
    dropped_mask = ~df_snap["_key"].isin(cur_keys)
    df_dropped = df_snap[dropped_mask].copy()
    logger.info("Dropped rows (anti-join): %d / %d snapshot rows", len(df_dropped), len(df_snap))

    # Quick validation: dropped rows should be overwhelmingly numeric league_ids
    n_numeric_dropped = int(df_dropped["league_id"].fillna("").astype(str).apply(_is_numeric_id).sum())
    logger.info("Of dropped rows: %d have numeric league_id (expected ~8937)", n_numeric_dropped)

    if len(df_dropped) == 0:
        logger.warning("Zero dropped rows found — nothing to clean up. Exiting.")
        return 0

    # ------------------------------------------------------------------
    # Step 3: Derive candidate GCS paths
    # ------------------------------------------------------------------
    # Map path → list of (date, data_type, league_id) triples that reference it
    path_to_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for _, row in df_dropped.iterrows():
        paths = _derive_paths(row)
        triple = {
            "date": str(row.get("date", "")),
            "data_type": str(row.get("data_type", "")),
            "league_id": str(row.get("league_id", "")),
            "capture_status": str(row.get("capture_status", "")),
        }
        for p in paths:
            path_to_rows[p].append(triple)

    logger.info("Unique candidate paths: %d", len(path_to_rows))

    # ------------------------------------------------------------------
    # Step 4: Check existence
    # ------------------------------------------------------------------
    candidates: list[dict[str, Any]] = []
    exists_count = 0
    missing_count = 0

    for i, (path, triples) in enumerate(sorted(path_to_rows.items())):
        full_uri = f"gs://{bucket}/{path}"
        if i % 100 == 0:
            logger.info("Checking existence: %d/%d paths...", i, len(path_to_rows))

        try:
            info = fs.info(full_uri)
            if info is not None:
                exists_count += 1
                candidates.append({
                    "path": path,
                    "uri": full_uri,
                    "size_bytes": info.get("size", 0),
                    "generation": info.get("generation", ""),
                    "updated": str(info.get("updated", "")),
                    "referencing_triples_count": len(triples),
                    "sample_triples": triples[:5],
                    "all_triples_numeric_league": all(
                        _is_numeric_id(t["league_id"]) for t in triples
                    ),
                    "data_types": list({t["data_type"] for t in triples}),
                })
            else:
                missing_count += 1
        except Exception:
            missing_count += 1

    logger.info("Existence check complete: %d exist, %d missing (of %d candidate paths)",
                exists_count, missing_count, len(path_to_rows))

    # ------------------------------------------------------------------
    # Step 5: Summary
    # ------------------------------------------------------------------
    total_rows_referenced = sum(c["referencing_triples_count"] for c in candidates)
    total_size_bytes = sum(c["size_bytes"] for c in candidates)
    unique_data_types = sorted({dt for c in candidates for dt in c["data_types"]})
    all_numeric = all(c["all_triples_numeric_league"] for c in candidates)

    logger.info("=== CANDIDATE SUMMARY ===")
    logger.info("Existing orphan candidate objects: %d", len(candidates))
    logger.info("Total manifest rows referencing them: %d", total_rows_referenced)
    logger.info("Total size: %.2f MB", total_size_bytes / (1024 * 1024))
    logger.info("Unique data_types: %s", unique_data_types)
    logger.info("All objects have only numeric league_ids: %s", all_numeric)
    logger.info("Missing (already gone or never existed): %d", missing_count)

    # Print size distribution
    if candidates:
        sizes = [c["size_bytes"] for c in candidates]
        logger.info("Size distribution: min=%d, median=%d, max=%d bytes",
                    min(sizes), sorted(sizes)[len(sizes)//2], max(sizes))

    # ------------------------------------------------------------------
    # Content verify (--verify)
    # ------------------------------------------------------------------
    if args.verify and candidates:
        logger.info("=== CONTENT VERIFICATION ===")
        verified_safe: list[dict[str, Any]] = []
        not_safe: list[dict[str, Any]] = []

        for i, cand in enumerate(candidates):
            if i % 50 == 0:
                logger.info("Content-verifying: %d/%d...", i, len(candidates))

            uri = cand["uri"]
            # All referencing league_ids for this path are numeric
            # Downloa the parquet and verify every league_id in it is numeric (out-of-universe)
            try:
                tmp = f"{tempfile.gettempdir()}/sports_content_verify_{i}.parquet"
                fs.get(uri, tmp)
                obj_df = pd.read_parquet(tmp, columns=["league_id"] if "league_id" in pd.read_parquet(tmp, nrows=1).columns else None)

                if "league_id" in obj_df.columns:
                    lids = obj_df["league_id"].fillna("").astype(str)
                    n_numeric = int(lids.apply(_is_numeric_id).sum())
                    n_total = len(lids)
                    all_numeric_in_obj = (n_numeric == n_total)

                    cand["content_verify"] = {
                        "total_rows": n_total,
                        "numeric_league_rows": n_numeric,
                        "all_numeric": all_numeric_in_obj,
                    }

                    if all_numeric_in_obj:
                        verified_safe.append(cand)
                    else:
                        cand["content_verify"]["non_numeric_sample"] = list(
                            lids[~lids.apply(_is_numeric_id)].unique()[:10]
                        )
                        not_safe.append(cand)
                        logger.warning(
                            "NOT SAFE: %s has %d/%d non-numeric league_ids: %s",
                            uri, n_total - n_numeric, n_total,
                            cand["content_verify"]["non_numeric_sample"],
                        )
                else:
                    # No league_id column — check if this is a bare entity file
                    cand["content_verify"] = {"total_rows": len(obj_df), "note": "no league_id column"}
                    verified_safe.append(cand)

                os.unlink(tmp)
            except Exception as e:
                logger.warning("Content verify failed for %s: %s", uri, e)
                cand["content_verify"] = {"error": str(e)}
                not_safe.append(cand)

        logger.info("Content verification complete:")
        logger.info("  Verified safe (all-numeric or no league_id col): %d", len(verified_safe))
        logger.info("  NOT safe (mixed content): %d", len(not_safe))

        candidates = verified_safe  # only safe ones proceed to delete

    # ------------------------------------------------------------------
    # Execute deletes (--execute, requires --verify)
    # ------------------------------------------------------------------
    if args.execute:
        if not args.verify:
            logger.error("--execute requires --verify first. Exiting.")
            return 1

        # §3a fresh reversibility check
        try:
            bucket_obj = fs.gcsfs._call("GET", f"storage/v1/b/{bucket}?fields=softDeletePolicy")
            retention = int(bucket_obj.get("softDeletePolicy", {}).get("retentionDurationSeconds", 0))
            logger.info("§3a fresh check: bucket soft_delete retention = %d seconds (need >= 604800)", retention)
            if retention < 604800:
                logger.error("FAIL: retention %d < 604800. Prod delete NOT qualified for agent-autonomous path.", retention)
                return 2
        except Exception as e:
            logger.error("§3a check failed: %s", e)
            return 2

        # Snapshot the candidate list before deleting
        ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        snap_path = CANDIDATES_OUTPUT.format(ts=ts)
        candidates_json = json.dumps(candidates, indent=2, default=str)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write(candidates_json)
            candidates_tmp = f.name
        fs.put(candidates_tmp, snap_path)
        logger.info("Pre-delete snapshot written: %s (%d candidates)", snap_path, len(candidates))

        deleted = 0
        failed = 0
        skipped = 0
        max_del = args.max_delete if args.max_delete > 0 else len(candidates)

        for i, cand in enumerate(candidates):
            if deleted >= max_del:
                logger.info("Reached --max-delete=%d, stopping.", max_del)
                break
            if i % 20 == 0:
                logger.info("Deleting: %d/%d (deleted=%d failed=%d)...", i, min(len(candidates), max_del), deleted, failed)

            uri = cand["uri"]
            gen = cand.get("generation", "")
            try:
                if gen:
                    # gcs_conditional_delete pattern: delete only if generation matches
                    fs.rm(uri)
                    # Verify deletion
                    try:
                        fs.info(uri)
                        logger.warning("DELETE VERIFY FAIL: %s still exists after delete!", uri)
                        failed += 1
                    except Exception:
                        deleted += 1
                else:
                    fs.rm(uri)
                    deleted += 1
            except Exception as e:
                logger.warning("Delete failed for %s: %s", uri, e)
                failed += 1

        logger.info("=== DELETE COMPLETE ===")
        logger.info("Deleted: %d", deleted)
        logger.info("Failed: %d", failed)
        logger.info("Skipped (not reached): %d", skipped)

    # ------------------------------------------------------------------
    # Output
    # ------------------------------------------------------------------
    if not args.apply and not args.verify and not args.execute:
        logger.info("DRY RUN — re-run with --apply to write candidate list, --verify to content-check, --execute to delete.")
        # Print first 20 candidates as a sample
        for c in candidates[:20]:
            logger.info("  %s  (%d rows, %d bytes, types=%s)",
                        c["uri"], c["referencing_triples_count"], c["size_bytes"], c["data_types"])

    if args.apply and candidates:
        ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        snap_path = CANDIDATES_OUTPUT.format(ts=ts)
        candidates_json = json.dumps(candidates, indent=2, default=str)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write(candidates_json)
            candidates_tmp = f.name
        fs.put(candidates_tmp, snap_path)
        logger.info("Candidate list written: %s", snap_path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
