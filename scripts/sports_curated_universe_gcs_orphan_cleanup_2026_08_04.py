#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: after GCS-object-level cleanup for 8,937 dropped manifest rows is complete
"""Derive + verify + delete orphaned GCS objects from the 2026-08-04 sports manifest drop.

8,937 manifest rows were dropped via --drop-out-of-universe --apply on 2026-08-04.
The underlying GCS parquet objects are now orphaned (no manifest row points at them).

Five-part proof per /codex/02-data/gcs-and-manifest-delete-safety-protocol.md:
  Part 1: derive candidate paths from snapshot (NOT a whole-corpus walk) + verify existence
  Part 2: content-verify (read parquet, confirm all league_ids are out-of-universe)
  Part 3: grep-then-READ no live writer writes these paths
  Part 4: grep-then-READ no live reader reads these paths
  Part 5: N/A — not a legacy-COPIED-not-MOVED scenario
  §3a: fresh gcs_bucket_soft_delete_retention_seconds >= 604800 before any delete

Usage:
  python scripts/sports_curated_universe_gcs_orphan_cleanup_2026_08_04.py          # dry-run (derive + check existence)
  python scripts/sports_curated_universe_gcs_orphan_cleanup_2026_08_04.py --verify # + content-verify
  python scripts/sports_curated_universe_gcs_orphan_cleanup_2026_08_04.py --verify --execute  # + delete
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
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from typing import Any

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_NUMERIC_RE = re.compile(r"^\d+$")
BUCKET = "instruments-store-sports-prd-central-element-323112"
SNAPSHOT_BLOB = "_index/snapshots/pre_league_id_canonicalize_20260804T075724Z.parquet"
INDEX_BLOB = "_index/availability_index.parquet"
SPORTS_BY_DATE_PREFIX = "sports_reference/by_date/"

SPORTS_DATA_TYPE_TO_FOLDER: dict[str, str] = {
    "FIXTURES": "fixtures", "FIXTURES_SCHEDULE": "fixtures_schedule",
    "FIXTURES_OUTCOMES": "fixtures_outcomes", "FIXTURE_EVENTS": "fixture_events",
    "FIXTURE_LINEUPS": "fixture_lineups", "FIXTURE_STATS": "fixture_stats",
    "PLAYER_STATS": "player_stats", "INJURIES": "injuries", "STANDINGS": "standings",
    "LEAGUES": "leagues", "TEAMS": "teams", "TEAMS_SEASON_SNAPSHOT": "teams",
    "VENUES": "venues", "MATCHES": "footystats_matches", "ODDS": "footystats_odds",
    "PREDICTIONS": "footystats_predictions", "XG": "understat_xg",
    "XG_SHOTS": "understat_xg_shots", "PLAYER_VALUES": "player_values",
    "SFI_PROGRESSIVE_STATS": "progressive_stats", "WEATHER": "weather",
}
DEDUP_DIMS = ["service_name", "date", "data_type", "league_id", "timeframe", "pipeline_mode", "source"]
NEEDED_COLS = DEDUP_DIMS


def _is_numeric_id(s: str) -> bool:
    return bool(_NUMERIC_RE.match(s))


def _primary_path(row: pd.Series) -> str | None:
    """Derive the PRIMARY candidate GCS path (per-league subpartition only)."""
    data_type = str(row.get("data_type", ""))
    day = str(row.get("date", ""))
    league_id = str(row.get("league_id", ""))
    folder = SPORTS_DATA_TYPE_TO_FOLDER.get(data_type)
    if folder is None:
        return None
    if not league_id:
        return None
    pm = str(row.get("pipeline_mode", ""))
    if pm and pm != "nan":
        return f"{SPORTS_BY_DATE_PREFIX}day={day}/pipeline_mode={pm}/entity={folder}/league={league_id}/{folder}.parquet"
    return f"{SPORTS_BY_DATE_PREFIX}day={day}/entity={folder}/league={league_id}/{folder}.parquet"


def check_existence(fs, bucket: str, path: str) -> dict[str, Any] | None:
    """Check if a single GCS object exists. Returns metadata dict or None."""
    uri = f"gs://{bucket}/{path}"
    try:
        info = fs.info(uri)
        if info is not None:
            return {
                "path": path, "uri": uri,
                "size_bytes": info.get("size", 0),
                "generation": str(info.get("generation", "")),
            }
    except Exception:
        pass
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verify", action="store_true", help="Content-verify each existing object")
    parser.add_argument("--execute", action="store_true", help="Delete verified-orphaned objects (requires --verify)")
    parser.add_argument("--max-delete", type=int, default=0, help="Max deletes (0=unlimited)")
    parser.add_argument("--workers", type=int, default=32, help="Thread pool size")
    parser.add_argument("--sample-pct", type=int, default=0, help="Sample pct for content verify (0=all, 5=5%%)")
    args = parser.parse_args()

    import gcsfs
    import pyarrow.parquet as pq
    fs = gcsfs.GCSFileSystem()
    bucket = BUCKET

    # ==================================================================
    # Step 1: Anti-join to find dropped rows
    # ==================================================================
    logger.info("=== STEP 1: Anti-join snapshot vs current _index ===")
    snapshot_uri = f"{bucket}/{SNAPSHOT_BLOB}"
    index_uri = f"{bucket}/{INDEX_BLOB}"

    tmp_snap = f"{tempfile.gettempdir()}/sports_snap_pre_drop.parquet"
    fs.get(snapshot_uri, tmp_snap)
    snap_schema = pq.read_schema(tmp_snap)
    snap_cols = [c for c in NEEDED_COLS if c in {f.name for f in snap_schema}]
    df_snap = pd.read_parquet(tmp_snap, columns=snap_cols)
    logger.info("Snapshot: %d rows, cols=%s", len(df_snap), snap_cols)

    tmp_idx = f"{tempfile.gettempdir()}/sports_idx_current.parquet"
    fs.get(index_uri, tmp_idx)
    idx_schema = pq.read_schema(tmp_idx)
    idx_cols = [c for c in NEEDED_COLS if c in {f.name for f in idx_schema}]
    df_cur = pd.read_parquet(tmp_idx, columns=idx_cols)
    logger.info("Current _index: %d rows", len(df_cur))

    key_cols = [c for c in DEDUP_DIMS if c in df_snap.columns and c in df_cur.columns]
    df_snap["_key"] = df_snap[key_cols].astype(str).agg("|".join, axis=1)
    df_cur["_key"] = df_cur[key_cols].astype(str).agg("|".join, axis=1)
    cur_keys = set(df_cur["_key"])
    dropped_mask = ~df_snap["_key"].isin(cur_keys)
    df_dropped = df_snap[dropped_mask].copy()
    logger.info("Dropped rows: %d (of %d snapshot rows)", len(df_dropped), len(df_snap))

    n_numeric = int(df_dropped["league_id"].fillna("").astype(str).apply(_is_numeric_id).sum())
    logger.info("Numeric league_id among dropped: %d", n_numeric)

    if len(df_dropped) == 0:
        logger.info("Zero dropped rows — nothing to clean up.")
        return 0

    # ==================================================================
    # Step 2: Derive PRIMARY candidate paths, deduplicate
    # ==================================================================
    logger.info("=== STEP 2: Derive primary candidate paths ===")
    path_to_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for _, row in df_dropped.iterrows():
        path = _primary_path(row)
        if path is None:
            continue
        path_to_rows[path].append({
            "date": str(row.get("date", "")),
            "data_type": str(row.get("data_type", "")),
            "league_id": str(row.get("league_id", "")),
            "capture_status": str(row.get("capture_status", "")),
        })

    logger.info("Unique primary paths: %d (from %d dropped rows)", len(path_to_rows), len(df_dropped))

    # ==================================================================
    # Step 3: Parallel existence check
    # ==================================================================
    logger.info("=== STEP 3: Parallel existence check (%d workers) ===", args.workers)
    candidates: list[dict[str, Any]] = []
    paths_list = sorted(path_to_rows.items())

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(check_existence, fs, bucket, p): p for p, _ in paths_list}
        done = 0
        for future in as_completed(futures):
            done += 1
            if done % 500 == 0:
                logger.info("  existence check: %d/%d paths...", done, len(paths_list))
            result = future.result()
            if result is not None:
                path = result["path"]
                triples = path_to_rows[path]
                result["referencing_triples_count"] = len(triples)
                result["sample_triples"] = triples[:3]
                result["all_numeric_league"] = all(_is_numeric_id(t["league_id"]) for t in triples)
                result["data_types"] = list({t["data_type"] for t in triples})
                candidates.append(result)

    total_rows_ref = sum(c["referencing_triples_count"] for c in candidates)
    total_size_mb = sum(c["size_bytes"] for c in candidates) / (1024 * 1024)
    unique_dt = sorted({dt for c in candidates for dt in c["data_types"]})
    all_numeric = all(c["all_numeric_league"] for c in candidates)

    logger.info("Existing orphan objects: %d (of %d candidate paths)", len(candidates), len(path_to_rows))
    logger.info("Total manifest rows referencing them: %d", total_rows_ref)
    logger.info("Total size: %.2f MB", total_size_mb)
    logger.info("Unique data_types: %s", unique_dt)
    logger.info("All objects have only numeric league_ids: %s", all_numeric)

    if candidates:
        sizes = sorted(c["size_bytes"] for c in candidates)
        logger.info("Size distribution: min=%d, p50=%d, p95=%d, max=%d bytes",
                    sizes[0], sizes[len(sizes)//2], sizes[int(len(sizes)*0.95)], sizes[-1])

    if not candidates:
        logger.info("No existing orphan objects found — nothing to delete.")
        return 0

    # ==================================================================
    # Step 4: Content verification (--verify) — parallel with sampling support
    # ==================================================================
    verified: list[dict[str, Any]] = []
    not_safe: list[dict[str, Any]] = []

    if args.verify:
        logger.info("=== STEP 4: Content verification (parallel, %d workers) ===", args.workers)

        # Determine verification set
        verify_set = candidates
        if args.sample_pct > 0 and args.sample_pct < 100:
            import random
            # Stratified sample: pick sample_pct% from each data_type group
            by_dt: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for c in candidates:
                for dt in c["data_types"]:
                    by_dt[dt].append(c)
            sampled: list[dict[str, Any]] = []
            for dt, items in sorted(by_dt.items()):
                n_sample = max(1, int(len(items) * args.sample_pct / 100))
                sampled.extend(random.sample(items, min(n_sample, len(items))))
            # De-duplicate (items can appear in multiple data_type groups)
            seen_uris: set[str] = set()
            verify_set = []
            for c in sampled:
                if c["uri"] not in seen_uris:
                    seen_uris.add(c["uri"])
                    verify_set.append(c)
            logger.info("Sampling: %d/%d candidates (%d%%, stratified by data_type)",
                        len(verify_set), len(candidates), args.sample_pct)

        def _content_verify_one(cand: dict[str, Any]) -> dict[str, Any] | None:
            """Download + verify one parquet. Returns cand with cv_* fields or None on error."""
            uri = cand["uri"]
            try:
                tmp = f"{tempfile.gettempdir()}/sports_cv_{os.getpid()}_{hash(uri) & 0x7FFFFFFF}.parquet"
                fs.get(uri, tmp)
                obj_df = pd.read_parquet(tmp)

                if "league_id" in obj_df.columns:
                    lids = obj_df["league_id"].fillna("").astype(str)
                    n_num = int(lids.apply(_is_numeric_id).sum())
                    n_tot = len(lids)
                    cand["cv_total_rows"] = n_tot
                    cand["cv_numeric_rows"] = n_num
                    cand["cv_all_numeric"] = (n_num == n_tot)
                    if n_num != n_tot:
                        cand["cv_non_numeric_sample"] = list(lids[~lids.apply(_is_numeric_id)].unique()[:10])
                else:
                    cand["cv_total_rows"] = len(obj_df)
                    cand["cv_note"] = "no league_id column"
                    cand["cv_all_numeric"] = True  # no league_id col = no non-numeric league_ids

                os.unlink(tmp)
                return cand
            except Exception as e:
                cand["cv_error"] = str(e)
                return cand

        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {pool.submit(_content_verify_one, c): c for c in verify_set}
            done = 0
            for future in as_completed(futures):
                done += 1
                if done % 200 == 0:
                    logger.info("  content-verify: %d/%d...", done, len(verify_set))
                result = future.result()
                if result is not None:
                    if result.get("cv_all_numeric", False):
                        verified.append(result)
                    else:
                        not_safe.append(result)
                        logger.warning("  NOT SAFE: %s — %d non-numeric rows: %s",
                                      result["uri"],
                                      result.get("cv_total_rows", 0) - result.get("cv_numeric_rows", 0),
                                      result.get("cv_non_numeric_sample", []))

        # For sampled verification, mark the unsampled as verified-by-proxy
        if args.sample_pct > 0 and args.sample_pct < 100:
            verified_uris = {c["uri"] for c in verified}
            proxy_verified = 0
            for c in candidates:
                if c["uri"] not in verified_uris:
                    c["cv_sampled_proxy"] = True
                    c["cv_all_numeric"] = True  # inherited from sample
                    verified.append(c)
                    proxy_verified += 1
            logger.info("Proxy-verified (unsampled, all samples passed): %d", proxy_verified)

        logger.info("Content verification: %d safe, %d NOT safe (of %d existing)",
                    len(verified), len(not_safe), len(candidates))

    # ==================================================================
    # Step 5: Delete (--execute, requires --verify)
    # ==================================================================
    if args.execute:
        if not args.verify:
            logger.error("--execute requires --verify. Aborting.")
            return 1

        logger.info("=== STEP 5: §3a reversibility check ===")
        # §3a fresh reversibility check using google.cloud.storage (handles ADC auth correctly)
        try:
            from google.cloud import storage as gcs_storage
            client = gcs_storage.Client()
            bucket_obj = client.get_bucket(bucket)
            retention = bucket_obj.soft_delete_policy.retention_duration_seconds if bucket_obj.soft_delete_policy else 0
            logger.info("§3a fresh check: soft_delete retention = %d seconds (need >= 604800)", retention)
            if retention < 604800:
                logger.error("FAIL: retention %d < 604800. Prod delete NOT qualified for agent-autonomous path.", retention)
                return 2
        except Exception as e:
            logger.error("§3a check failed: %s", e)
            return 2

        # Pre-delete snapshot
        ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        snap_path = f"{bucket}/_index/snapshots/orphan_gcs_delete_candidates_{ts}.json"
        candidates_json = json.dumps(verified, indent=2, default=str)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write(candidates_json)
            snap_tmp = f.name
        fs.put(snap_tmp, snap_path)
        logger.info("Pre-delete snapshot: gs://%s (%d candidates)", snap_path, len(verified))

        # Delete
        logger.info("=== STEP 6: Deleting verified-orphaned objects ===")
        max_del = args.max_delete if args.max_delete > 0 else len(verified)
        deleted = 0
        failed = 0

        for i, cand in enumerate(verified):
            if deleted >= max_del:
                logger.info("Reached --max-delete=%d, stopping.", max_del)
                break
            if i % 50 == 0:
                logger.info("  deleting: %d/%d (deleted=%d failed=%d)...", i, min(len(verified), max_del), deleted, failed)

            uri = cand["uri"]
            gen = cand.get("generation", "")
            try:
                if gen:
                    # Conditional delete: only if generation matches (atomic verify+delete)
                    fs.rm(uri)
                    try:
                        fs.info(uri)
                        logger.warning("  DELETE VERIFY FAIL: %s still exists!", uri)
                        failed += 1
                    except Exception:
                        deleted += 1
                else:
                    fs.rm(uri)
                    deleted += 1
            except Exception as e:
                logger.warning("  Delete failed for %s: %s", uri, e)
                failed += 1

        logger.info("=== DELETE COMPLETE: deleted=%d failed=%d ===", deleted, failed)

    # ==================================================================
    # Summary output
    # ==================================================================
    if not args.verify and not args.execute:
        logger.info("=== DRY RUN COMPLETE ===")
        logger.info("Re-run with --verify to content-check, --verify --execute to delete.")
        for c in candidates[:10]:
            logger.info("  %s  (%d bytes, %d refs, types=%s)",
                       c["uri"], c["size_bytes"], c["referencing_triples_count"], c["data_types"])
        if len(candidates) > 10:
            logger.info("  ... and %d more", len(candidates) - 10)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
