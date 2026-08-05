#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: after successful prod execution and plan checkbox flip
"""Prune cartesian-junk player_values snapshot trigger objects under season=2026/.

Per the root-cause investigation in
plans/active/issues/sports_decision16_anomalies_investigation_2026_08_04.md, the
transfermarkt snapshot writer generates trigger-date partitions for ALL historical
trigger dates under every season. For season=2026 this produced 405 parquet files
(~17-20 KB each, ~7 MB total) spanning trigger dates 2014-2018 that have no
meaningful relationship to the 2026 season.

This script deletes ONLY trigger dates BEFORE 2026-01-01 under
``sports_reference/snapshots/entity=player_values/season=2026/``. The 31 trigger
dates >= 2026-01-01 are KEPT.

§3a reversibility: instruments-store-sports-prd-central-element-323112 soft-delete
retention = 2,592,000s (30 days, well above the 604,800s / 7-day threshold).
Fresh gcloud check confirmed 2026-08-05.

DRY-RUN by default. ``--apply`` executes the deletes.
"""

from __future__ import annotations

import argparse
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

import gcsfs

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("prune_player_values_trigger_junk")

BUCKET = "instruments-store-sports-prd-central-element-323112"
BASE_URI = f"gs://{BUCKET}/sports_reference/snapshots/entity=player_values/season=2026/"
CUTOFF_YEAR = 2026


def _trigger_year(trigger_dir: str) -> int | None:
    """Extract the trigger year from a prefix like 'trigger=2014-01-01'."""
    try:
        return int(trigger_dir.split("=")[1].split("-")[0])
    except (IndexError, ValueError):
        return None


def _list_parquets_under_prefix(fs: gcsfs.GCSFileSystem, bucket: str, prefix: str) -> list[str]:
    """List all .parquet files recursively under a prefix. Returns full gs:// URIs."""
    results: list[str] = []
    for path in fs.ls(f"{bucket}/{prefix}", detail=False):
        path_str = str(path).rstrip("/")
        if path_str.endswith(".parquet"):
            results.append(f"gs://{path_str}")
        elif "/trigger=" in path_str:
            # Recurse into trigger= directories
            for inner in fs.ls(path_str, detail=False):
                inner_str = str(inner)
                if inner_str.endswith(".parquet"):
                    results.append(f"gs://{inner_str}")
    return results


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true", help="Execute deletes (default: dry-run)")
    ap.add_argument("--workers", type=int, default=16, help="Concurrent delete workers")
    args = ap.parse_args()

    logger.info("Bucket: %s  apply=%s  cutoff_year=%d", BUCKET, args.apply, CUTOFF_YEAR)
    logger.info("§3a: soft-delete retention confirmed 2,592,000s (30d) via gcloud 2026-08-05 — reversibility-qualified")

    fs = gcsfs.GCSFileSystem(token="google_default")

    # List trigger= directories under season=2026
    listing = fs.ls(BASE_URI, detail=False)

    to_delete: list[str] = []
    to_keep: list[str] = []

    for entry in listing:
        entry_str = str(entry).rstrip("/")
        trigger_dir = entry_str.split("/")[-1]
        if not trigger_dir.startswith("trigger="):
            continue
        year = _trigger_year(trigger_dir)
        if year is None:
            logger.warning("Could not parse year from: %s", trigger_dir)
            continue

        # List parquet files under this trigger directory
        trigger_uri = f"gs://{entry_str}"
        parquets = [str(p) for p in fs.ls(trigger_uri, detail=False) if str(p).endswith(".parquet")]
        for p in parquets:
            uri = f"gs://{p}"
            if year < CUTOFF_YEAR:
                to_delete.append(uri)
            else:
                to_keep.append(uri)

    logger.info("Pre-2026 (DELETE): %d parquet files", len(to_delete))
    logger.info("2026+ (KEEP):    %d parquet files", len(to_keep))

    if not to_delete:
        logger.info("Nothing to delete — exiting")
        return 0

    # Show sample
    for uri in sorted(to_delete)[:5]:
        logger.info("  DELETE: %s", uri)
    if len(to_delete) > 5:
        logger.info("  ... and %d more", len(to_delete) - 5)
    for uri in sorted(to_keep)[:3]:
        logger.info("  KEEP:   %s", uri)

    if not args.apply:
        logger.info("DRY-RUN complete — %d files would be deleted. Pass --apply to execute.", len(to_delete))
        return 0

    # Execute deletes
    logger.info("EXECUTING deletes with %d workers...", args.workers)
    deleted = 0
    failed = 0

    def _delete_one(uri: str) -> tuple[str, bool]:
        try:
            path = uri.replace("gs://", "")
            if not fs.exists(path):
                return (uri, True)  # already gone = success
            fs.rm(path)
            return (uri, True)
        except Exception:
            logger.exception("Delete failed: %s", uri)
            return (uri, False)

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(_delete_one, uri): uri for uri in to_delete}
        for future in as_completed(futures):
            uri, ok = future.result()
            if ok:
                deleted += 1
            else:
                failed += 1
            if (deleted + failed) % 50 == 0:
                logger.info("Progress: %d deleted, %d failed", deleted, failed)

    logger.info("DONE: %d deleted, %d failed, %d kept", deleted, failed, len(to_keep))

    if failed > 0:
        logger.error("%d deletes FAILED — re-run to retry", failed)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
