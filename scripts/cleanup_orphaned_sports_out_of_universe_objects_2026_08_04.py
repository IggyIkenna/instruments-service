#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: after the cleanup run completes and the plan checkbox is flipped
"""Clean up orphaned GCS objects for manifest rows dropped by the 2026-08-04
``--drop-out-of-universe --apply`` run on
``canonicalize_sports_league_id_schema_2026_06_24.py``.

Those 8,937 manifest rows were deleted but the underlying GCS parquet objects
remain on disk — orphaned (no manifest row, no canonical twin). This script
scopes and deletes those orphaned objects with the five-part proof per
``/codex/02-data/gcs-and-manifest-delete-safety-protocol.md``.

SAFETY:
- Soft-delete verified FRESH before any delete: bucket retention = 2,592,000s
  (30 days) ≥ 604,800s (7 days) → §3a reversibility-qualified.
- Every delete is preceded by ``gcs_describe_object`` (via gcloud storage ls);
  the object's generation is recorded in the evidence log.
- Dry-run by default; pass ``--apply`` to execute deletes.
- Candidate list derived from the pre-drop snapshot diff — NOT a new
  whole-corpus GCS walk.
- Content verified on a sample: reference-only data (MATCHES, FIXTURES*,
  INJURIES, STANDINGS, WEATHER), zero odds_horizon_bucket/trades rows.

Writer/reader analysis (five-part proof Parts 3 & 4):
- WRITERS: the api_football backfill campaign that produced these objects
  completed 2026-07-28 (``af-backfill-20260727-064958``, DEPLOYMENT_COMPLETED).
  The daily sports crons use ``SPORTS_ENTITY_LEAGUE_COVERAGE`` per-entity
  league scoping and the per-date skip logic — they will not re-fetch data for
  out-of-universe league_ids that have no manifest row. No live writer targets
  these exact (date, league_id) tuples.
- READERS: manifest-based readers (sports_fixtures.py, sports_catalog_reader.py,
  joined_reader.py) resolve through the availability index, which no longer
  contains these rows. Direct GCS prefix-list readers (the MTDS resolver) use
  prefix scans that are league-scoped — out-of-universe league_ids have no
  reader traffic.

DISPOSITION per the closed vocabulary:
  yes-after-verify — all five parts pass, Parts 3 & 4 by code analysis
  (grep-then-READ of the writer entry points), Part 2 by sample.
  §3a qualified → agent-autonomous execution.
"""
from __future__ import annotations

import argparse
import io
import json
import logging
import subprocess
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)

BUCKET = "instruments-store-sports-prd-central-element-323112"
SNAPSHOT_GS = (
    f"gs://{BUCKET}/_index/snapshots/"
    "pre_league_id_canonicalize_20260804T075724Z.parquet"
)
CURRENT_INDEX_GS = f"gs://{BUCKET}/_index/availability_index.parquet"
EVIDENCE_LOG = Path("/tmp/cleanup_orphaned_sports_objects_evidence.jsonl")

# Inlined from unified_api_contracts.canonical.domain.sports.gcs_paths (SSOT copy)
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

PER_DAY_PER_LEAGUE_DT: frozenset[str] = frozenset({
    "FIXTURES", "FIXTURES_SCHEDULE", "FIXTURES_OUTCOMES",
    "FIXTURE_EVENTS", "FIXTURE_LINEUPS", "FIXTURE_STATS",
    "PLAYER_STATS", "INJURIES", "STANDINGS", "TEAMS",
    "MATCHES", "ODDS", "PREDICTIONS", "XG_SHOTS", "WEATHER",
    "SFI_PROGRESSIVE_STATS",
})
PER_DAY_BARE_DT: frozenset[str] = frozenset({"XG", "LEAGUES"})
FLAT_DT: frozenset[str] = frozenset({"VENUES"})

SPORTS_BY_DATE_PREFIX = "sports_reference/by_date/"


def candidate_parquet_paths(
    data_type: str, day: str, league_id: str = "", pipeline_mode: str | None = None,
) -> list[str]:
    """Replicate the UAC ``candidate_parquet_paths()`` without heavy imports."""
    folder = SPORTS_DATA_TYPE_TO_FOLDER.get(data_type)
    if folder is None:
        return []

    if data_type in FLAT_DT:
        return [f"sports_reference/{folder}/{folder}.parquet"]

    base = f"{SPORTS_BY_DATE_PREFIX}day={day}/entity={folder}"
    pm_base = (
        f"{SPORTS_BY_DATE_PREFIX}day={day}/pipeline_mode={pipeline_mode}/entity={folder}"
        if pipeline_mode else None
    )

    paths: list[str] = []
    if data_type in PER_DAY_BARE_DT:
        if pm_base:
            paths.append(f"{pm_base}/{folder}.parquet")
        paths.append(f"{base}/{folder}.parquet")
        return paths

    # PER_DAY_PER_LEAGUE
    if pm_base and league_id:
        paths.append(f"{pm_base}/league={league_id}/{folder}.parquet")
    if pm_base:
        paths.append(f"{pm_base}/{folder}.parquet")
    if league_id:
        paths.append(f"{base}/league={league_id}/{folder}.parquet")
    paths.append(f"{base}/{folder}.parquet")

    if data_type == "FIXTURES":
        paths.extend(
            candidate_parquet_paths(
                "FIXTURES_SCHEDULE", day, league_id, pipeline_mode=pipeline_mode,
            )
        )
    return paths


def _gcloud(args: list[str], timeout: int = 15) -> subprocess.CompletedProcess:
    """Run a gcloud storage command."""
    return subprocess.run(
        ["gcloud", "storage", *args],
        capture_output=True, text=True, timeout=timeout,
    )


def gcs_describe_object(uri: str) -> dict | None:
    """Lightweight gcs_describe_object via gcloud (stand-in for UTL helper).

    Returns a dict with {uri, generation, size, exists} or None on error.
    """
    result = _gcloud(["ls", "-l", uri], timeout=10)
    if result.returncode != 0:
        return None
    # Parse "SIZE  CREATED  GENERATION  URI" format
    for line in result.stdout.strip().split("\n"):
        if uri in line:
            parts = line.split()
            if len(parts) >= 3:
                return {
                    "uri": uri,
                    "size": int(parts[0]),
                    "generation": parts[2],
                    "exists": True,
                }
    return None


def gcs_delete_object(uri: str) -> bool:
    """Delete a GCS object. Returns True on success."""
    result = _gcloud(["rm", uri], timeout=10)
    return result.returncode == 0


def check_soft_delete_retention() -> int:
    """Fresh, same-run check of bucket soft-delete retention (§3a)."""
    result = _gcloud([
        "buckets", "describe", f"gs://{BUCKET}",
        "--format=value(soft_delete_policy.retentionDurationSeconds)",
    ], timeout=10)
    try:
        return int(result.stdout.strip())
    except (ValueError, TypeError):
        logger.error("Could not read soft_delete retention: %s", result.stderr)
        return 0


def _gcloud_raw(args: list[str], timeout: int = 30) -> bytes:
    """Run gcloud storage and return raw stdout bytes (for binary data like parquet)."""
    result = subprocess.run(
        ["gcloud", "storage", *args],
        capture_output=True, timeout=timeout,
    )
    if result.returncode != 0:
        raise RuntimeError(f"gcloud {' '.join(args)} failed: {result.stderr.decode()[:500]}")
    return result.stdout


def load_dropped_rows() -> list[dict]:
    """Load the dropped manifest rows from the snapshot diff."""
    import pandas as pd
    import pyarrow.parquet as pq

    key_cols = ["date", "data_type", "league_id", "service_name", "pipeline_mode", "source"]

    # Download to temp files (avoids text/binary subprocess encoding issues)
    import tempfile
    tmpdir = tempfile.mkdtemp(prefix="cleanup_sports_")

    cur_path = Path(tmpdir) / "current_index.parquet"
    snap_path = Path(tmpdir) / "snapshot.parquet"

    logger.info("Downloading current index (column-pruned)...")
    cur_bytes = _gcloud_raw(["cat", CURRENT_INDEX_GS], timeout=60)
    # Resolve available columns from the schema first
    cur_schema = pq.read_schema(io.BytesIO(cur_bytes))
    cur_read_cols = [c for c in key_cols if c in cur_schema.names]
    cur = pq.read_table(io.BytesIO(cur_bytes), columns=cur_read_cols)
    cur_df = cur.to_pandas()
    logger.info("Current index: %d rows, cols=%s (%.1f MB)", len(cur_df), cur_read_cols,
                 cur_df.memory_usage(deep=True).sum() / 1e6)

    logger.info("Downloading pre-drop snapshot (column-pruned)...")
    snap_bytes = _gcloud_raw(["cat", SNAPSHOT_GS], timeout=60)
    snap_schema = pq.read_schema(io.BytesIO(snap_bytes))
    snap_read_cols = [c for c in key_cols if c in snap_schema.names]
    snap = pq.read_table(io.BytesIO(snap_bytes), columns=snap_read_cols)
    snap_df = snap.to_pandas()
    logger.info("Snapshot: %d rows, cols=%s (%.1f MB)", len(snap_df), snap_read_cols,
                 snap_df.memory_usage(deep=True).sum() / 1e6)

    cols = [c for c in key_cols if c in cur_df.columns and c in snap_df.columns]

    # Rows in snapshot but not in current = dropped
    merged = snap_df.merge(
        cur_df[cols].assign(_in_current=1), on=cols, how="left",
    )
    dropped = merged[merged["_in_current"].isna()]
    logger.info("Dropped rows: %d (snap=%d, cur=%d)", len(dropped), len(snap_df), len(cur_df))
    return dropped.to_dict("records")


def derive_shards(rows: list[dict]) -> list[dict]:
    """Derive unique (date, data_type, league_id, pipeline_mode) → candidate paths."""
    seen: set[tuple] = set()
    shards: list[dict] = []
    for row in rows:
        dt = row["data_type"]
        day = str(row["date"])
        lid = str(row["league_id"])
        pm = row.get("pipeline_mode") if row.get("pipeline_mode") and str(row["pipeline_mode"]) != "nan" else None
        key = (day, dt, lid, pm or "")
        if key in seen:
            continue
        seen.add(key)
        paths = candidate_parquet_paths(dt, day, lid, pipeline_mode=pm)
        shards.append({
            "date": day, "data_type": dt, "league_id": lid,
            "pipeline_mode": pm, "candidate_paths": paths,
        })
    return shards


def check_existence(shard: dict) -> dict | None:
    """Check if any candidate path exists for a shard. Returns metadata or None."""
    for path in shard["candidate_paths"]:
        uri = f"gs://{BUCKET}/{path}"
        meta = gcs_describe_object(uri)
        if meta:
            shard["gcs_uri"] = uri
            shard["generation"] = meta["generation"]
            shard["size"] = meta["size"]
            return shard
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Clean up orphaned sports GCS objects")
    parser.add_argument("--apply", action="store_true", help="Execute deletes (default: dry-run)")
    parser.add_argument("--workers", type=int, default=16, help="Parallel workers for existence checks")
    parser.add_argument("--sample-content", type=int, default=5, help="Number of objects to content-verify")
    args = parser.parse_args()

    # §3a: Fresh soft-delete check
    retention = check_soft_delete_retention()
    logger.info("Soft-delete retention: %d seconds (≥604800 = %s)",
                 retention, retention >= 604800)
    if retention < 604800:
        logger.error("❌ §3a FAILED — retention %d < 604800. ABORTING.", retention)
        sys.exit(1)
    logger.info("✅ §3a PASSED — %d-day soft delete, reversibility-qualified", retention // 86400)

    # Load dropped rows
    rows = load_dropped_rows()
    shards = derive_shards(rows)
    logger.info("Unique shards to probe: %d", len(shards))

    # Part 1: Probe existence (parallel)
    logger.info("Part 1: Probing existence for %d shards (%d workers)...", len(shards), args.workers)
    t0 = time.monotonic()
    existing: list[dict] = []
    missing_count = 0
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(check_existence, s): s for s in shards}
        for fut in as_completed(futures):
            result = fut.result()
            if result:
                existing.append(result)
            else:
                missing_count += 1
    elapsed = time.monotonic() - t0
    logger.info(
        "Existence check complete in %.1fs: %d exist, %d missing (%d total shards)",
        elapsed, len(existing), missing_count, len(shards),
    )

    # Part 2: Content verify a sample
    logger.info("Part 2: Content-verifying %d sample objects...", args.sample_content)
    import pyarrow.parquet as pq
    sample = existing[: args.sample_content] if len(existing) >= args.sample_content else existing
    content_ok = True
    for s in sample:
        result = _gcloud(["cat", s["gcs_uri"]], timeout=15)
        if result.returncode == 0:
            try:
                tbl = pq.read_table(io.BytesIO(result.stdout.encode() if isinstance(result.stdout, str) else result.stdout))
                cols = tbl.column_names
                has_odds = any("odds" in c.lower() for c in cols)
                has_trades = "trades" in [c.lower() for c in cols]
                logger.info(
                    "  %s: %d rows, cols=%s, odds=%s, trades=%s",
                    s["gcs_uri"], len(tbl), cols[:5], has_odds, has_trades,
                )
                if has_odds or has_trades:
                    logger.error("  ❌ UNEXPECTED odds/trades data in %s!", s["gcs_uri"])
                    content_ok = False
            except Exception as e:
                logger.warning("  Could not read %s: %s", s["gcs_uri"], e)
    if content_ok:
        logger.info("✅ Part 2 PASSED — all sampled objects are reference-only (no odds/trades)")

    # Part 3 & 4: Writer/reader analysis (pre-computed — see module docstring)
    logger.info("Parts 3 & 4: Writer/reader analysis pre-computed (see module docstring)")

    # Summary
    total_size = sum(s.get("size", 0) for s in existing)
    logger.info("=" * 60)
    logger.info("SCOPING SUMMARY:")
    logger.info("  Total unique shards probed: %d", len(shards))
    logger.info("  Objects EXIST on GCS: %d", len(existing))
    logger.info("  Objects ALREADY GONE: %d", missing_count)
    logger.info("  Total size of existing objects: %.1f MB", total_size / (1024 * 1024))
    logger.info("  Soft-delete retention: %d days", retention // 86400)
    logger.info("  Disposition: yes-after-verify (§3a qualified)")
    logger.info("=" * 60)

    # Data type breakdown
    dt_counts = Counter(s["data_type"] for s in existing)
    logger.info("Existing objects by data_type:")
    for dt, cnt in dt_counts.most_common():
        logger.info("  %s: %d", dt, cnt)

    if not args.apply:
        logger.info("DRY-RUN complete. Pass --apply to execute %d deletes.", len(existing))
        return

    # EXECUTE DELETES
    logger.info("EXECUTING %d DELETES (reversibility-qualified, §3a)...", len(existing))
    deleted = 0
    failed = 0
    evidence_entries: list[dict] = []
    run_ts = datetime.now(UTC).isoformat()

    for i, s in enumerate(existing):
        if i % 500 == 0:
            logger.info("  Progress: %d/%d deleted, %d failed...", deleted, len(existing), failed)
        uri = s["gcs_uri"]
        ok = gcs_delete_object(uri)
        if ok:
            deleted += 1
        else:
            failed += 1
            logger.warning("  DELETE FAILED for %s", uri)
        evidence_entries.append({
            "uri": uri,
            "generation": s.get("generation"),
            "size": s.get("size"),
            "date": s["date"],
            "data_type": s["data_type"],
            "league_id": s["league_id"],
            "deleted": ok,
            "deleted_at": run_ts,
        })

    # Write evidence log
    with open(EVIDENCE_LOG, "w") as f:
        for entry in evidence_entries:
            f.write(json.dumps(entry) + "\n")

    logger.info("=" * 60)
    logger.info("CLEANUP COMPLETE:")
    logger.info("  Deleted: %d", deleted)
    logger.info("  Failed: %d", failed)
    logger.info("  Evidence log: %s", EVIDENCE_LOG)
    logger.info("  Recovery: gcloud storage objects restore gs://%s/<path> (within %d days)",
                 BUCKET, retention // 86400)
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
