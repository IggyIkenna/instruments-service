#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: after prd-run confirmed (A_LEAGUE/2025-09-01/FIXTURES attempted_failed == 0)
"""fix_a_league_fixtures_2025_09_01_20260627.py — correct 1 stale FIXTURES_FETCH_FAILED row.

ROOT CAUSE (2026-06-27): the prd manifest has 1 residual attempted_failed row:
  date=2025-09-01 / league_id=A_LEAGUE / data_type=FIXTURES / error=FIXTURES_FETCH_FAILED
written 2026-06-25 during the systemic batch failure. The af-backfill-20260627-182057
VM ran (rc=0) but did not overwrite this row because emit_empty_gaps_for_entity skips
leagues whose fixture calendar returns empty for the target date — A_LEAGUE runs
Oct-May; September 2025 is off-season so the calendar check (get_league_fixture_calendar)
returns [] → the gap-emit is skipped → the stale attempted_failed persists.

FIX (this script): flip the 1 row to empty_confirmed / EXPECTED_NO_FIXTURE.
Evidence:
  - Non-prd index (rebuilt by rescan from actual GCS parquets): 0 FIXTURES failures
  - A_LEAGUE season schedule: 2024/25 ends ~May 2025; 2025/26 starts ~Oct 2025
  - 2025-09-01 is a confirmed off-season date with no A_LEAGUE fixtures in GCS

Safe direct-write: same pattern as reclassify_oos_sports_expected_unattempted_2026_06_24.py.
No backfill VMs running (last VM af-backfill-20260627-182057 stopped 18:32 UTC).

Usage::

    MANIFEST_PER_VM_SHARDS=true VM_NAME=fix-a-league-20260627 \\
    DEPLOYMENT_ENV_SHORT=prd GCP_PROJECT_ID=central-element-323112 \\
    PROJECT_ID=central-element-323112 \\
    .venv/bin/python scripts/fix_a_league_fixtures_2025_09_01_20260627.py [--apply]
"""

from __future__ import annotations

import argparse
import io
import logging
import os
import sys
from datetime import UTC, datetime

import gcsfs
import pandas as pd
from unified_trading_library import resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("fix_a_league_fixtures")

_MANIFEST_BLOB = "_index/availability_index.parquet"
_TARGET = {"date": "2025-09-01", "league_id": "A_LEAGUE", "data_type": "FIXTURES"}
_OLD_STATUS = "attempted_failed"
_OLD_REASON = "FIXTURES_FETCH_FAILED"
_NEW_STATUS = "empty_confirmed"
_NEW_REASON = "EXPECTED_NO_FIXTURE"


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--apply", action="store_true", help="Write corrected manifest. Default: dry-run.")
    args = p.parse_args()

    if not os.environ.get("DEPLOYMENT_ENV_SHORT"):
        logger.error("DEPLOYMENT_ENV_SHORT must be set (e.g. prd). Refusing — would resolve wrong bucket.")
        return 1

    if args.apply and (os.environ.get("MANIFEST_PER_VM_SHARDS") != "true" or not os.environ.get("VM_NAME")):
        logger.error(
            "--apply requires MANIFEST_PER_VM_SHARDS=true AND VM_NAME=<unique> "
            "per the manifest concurrency principle. Refusing to mutate without shard isolation."
        )
        return 1

    bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")
    fs = gcsfs.GCSFileSystem()

    logger.info("Loading sports manifest from gs://%s/%s", bucket, _MANIFEST_BLOB)
    with fs.open(f"{bucket}/{_MANIFEST_BLOB}", "rb") as fh:
        df = pd.read_parquet(fh)
    logger.info("Manifest rows: %d", len(df))

    mask = (
        (df["date"].astype(str) == _TARGET["date"])
        & (df["league_id"].astype(str) == _TARGET["league_id"])
        & (df["data_type"].astype(str) == _TARGET["data_type"])
        & (df["capture_status"].astype(str) == _OLD_STATUS)
        & (df["error_reason"].astype(str) == _OLD_REASON)
    )
    n_match = int(mask.sum())
    logger.info("Target rows matching %s: %d", _TARGET, n_match)

    if n_match == 0:
        logger.info("Nothing to fix — row already corrected or absent. Gate already met.")
        return 0

    if n_match > 1:
        logger.warning("Found %d matching rows (expected 1) — will flip all.", n_match)

    logger.info("Will flip: %s -> %s / %s", _OLD_STATUS, _NEW_STATUS, _NEW_REASON)
    logger.info("Matching rows:\n%s", df[mask][["date", "league_id", "data_type", "capture_status", "error_reason", "written_at"]].to_string())

    if not args.apply:
        logger.info("DRY RUN — manifest not modified. Re-run with --apply to flip.")
        return 0

    # Snapshot before mutating.
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    snap = f"{bucket}/_index/snapshots/pre_fix_a_league_fixtures_{stamp}.parquet"
    sbuf = io.BytesIO()
    df.to_parquet(sbuf, index=False)
    sbuf.seek(0)
    with fs.open(snap, "wb") as fh:
        fh.write(sbuf.getvalue())
    logger.info("Snapshot -> gs://%s", snap)

    now_iso = datetime.now(UTC).isoformat()
    df.loc[mask, "capture_status"] = _NEW_STATUS
    df.loc[mask, "error_reason"] = _NEW_REASON
    df.loc[mask, "attempted_at"] = now_iso
    df.loc[mask, "written_at"] = now_iso

    out = io.BytesIO()
    df.to_parquet(out, index=False)
    out.seek(0)
    logger.info("Writing corrected manifest (%d rows total, %d flipped)", len(df), n_match)
    with fs.open(f"{bucket}/{_MANIFEST_BLOB}", "wb") as fh:
        fh.write(out.read())
    logger.info("Done. A_LEAGUE/2025-09-01/FIXTURES -> %s / %s", _NEW_STATUS, _NEW_REASON)
    return 0


if __name__ == "__main__":
    sys.exit(main())
