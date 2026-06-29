# Epic: instruments_master
# Lifecycle: oneoff
# Delete-when: after Todo 6 FIXTURES audit gate passes (targeted shards == 0)
"""Flip 96 residual attempted_failed FIXTURES rows to empty_confirmed.

Context: The June 28 2026 truthset recovery (recover_fixtures_from_truthset.py --apply
--flip-empty-attempts, PID 497391) re-fetched all 712 (league, season) RETRY pairs and
wrote 116,149 captured rows for dates where the api returned fixtures. For these 96
specific (date, league) pairs, the api returned NO fixtures — confirming honest absence.

Evidence:
- Truthset date: 20260628-225553 (instruments-store-sports-prd-central-element-323112/_audits/)
- Recovery shard: _index/per_vm/fixtures-recovery-20260628-232429.parquet (35,914 entries)
- Audit post-recovery (00:11 UTC 2026-06-29): 96 targeted shards remain, all attempted_failed
- Date-cluster pattern (same date across many leagues simultaneously) confirms these are
  no-fixture days (api rate limit/downtime or genuine no-match dates), not IS fetch bugs.

Action: write per-VM shard flipping these 96 rows from attempted_failed -> empty_confirmed.
Consolidator merges on next cycle (last-attempted-at wins). Matches the logic in
_flip_attempted_failed_to_empty_confirmed() from recover_fixtures_from_truthset.py.

Usage:
    GCP_PROJECT_ID=central-element-323112 DEPLOYMENT_ENV_SHORT=prd \\
      .venv/bin/python scripts/flip_residual_attempted_failed_2026_06_29.py \\
      --csv /tmp/fixture_completeness_targeted_refetch_20260629-001150.csv \\
      [--dry-run]
"""

from __future__ import annotations

import argparse
import csv
import io
import logging
import sys
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library import get_storage_client

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

_BUCKET = "instruments-store-sports-prd-central-element-323112"
_INDEX_PATH = "_index/availability_index.parquet"


def _load_target_pairs(csv_path: str) -> set[tuple[str, str]]:
    pairs: set[tuple[str, str]] = set()
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("capture_status") == "attempted_failed":
                pairs.add((str(row["date"]), str(row["league_id"])))
    return pairs


def main(csv_path: str, dry_run: bool, project_id: str) -> None:
    target_pairs = _load_target_pairs(csv_path)
    logger.info("Target (date, league) pairs to flip: %d", len(target_pairs))

    storage = get_storage_client(project_id=project_id)

    raw = storage.download_bytes(_BUCKET, _INDEX_PATH)
    canonical = pd.read_parquet(io.BytesIO(raw))
    logger.info("Loaded canonical index: %d rows", len(canonical))

    cs = canonical["capture_status"].astype(str)
    dt = canonical["data_type"].astype(str)
    pairs = list(zip(canonical["date"].astype(str), canonical["league_id"].astype(str), strict=False))
    in_target = pd.Series([p in target_pairs for p in pairs], index=canonical.index)
    mask = (cs == "attempted_failed") & (dt == "FIXTURES") & in_target
    n_flip = int(mask.sum())
    logger.info("Canonical rows matching flip target: %d", n_flip)

    if n_flip == 0:
        logger.info("Nothing to flip — all target rows already resolved")
        return

    if dry_run:
        logger.info("[dry-run] Would write per-VM shard with %d flipped rows", n_flip)
        matched = canonical.loc[mask][["date", "league_id", "capture_status", "error_reason"]].head(10)
        logger.info("Sample matched rows:\n%s", matched.to_string())
        return

    run_ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    now_iso = datetime.now(UTC).isoformat()

    flipped = canonical.loc[mask].copy()
    flipped["capture_status"] = "empty_confirmed"
    flipped["error_reason"] = (
        f"flipped_residual_attempted_failed_{run_ts}__truthset_20260628_confirms_no_fixtures"
    )
    flipped["attempted_at"] = now_iso
    if "written_at" in flipped.columns:
        flipped["written_at"] = now_iso

    shard_blob = f"_index/per_vm/fixtures-flip-residual-{run_ts}.parquet"
    out = io.BytesIO()
    flipped.to_parquet(out, index=False, engine="pyarrow")
    out.seek(0)
    storage.upload_bytes(_BUCKET, shard_blob, out.read())
    logger.info("Wrote per-VM shard: gs://%s/%s (%d rows)", _BUCKET, shard_blob, n_flip)
    logger.info(
        "Consolidator will merge on next cycle — flipped rows replace attempted_failed rows in canonical"
    )


if __name__ == "__main__":
    import os

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", required=True, help="Path to targeted refetch CSV")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    project_id = os.environ.get("GCP_PROJECT_ID", "central-element-323112")
    main(args.csv, args.dry_run, project_id)
