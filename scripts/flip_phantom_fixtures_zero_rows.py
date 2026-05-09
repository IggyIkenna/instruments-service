"""Phase 1 of sports_phantom_fixtures_recovery_2026_05_06 — flip phantom FIXTURES rows.

The sports availability manifest carries ~100,000 ``captured`` rows where
``data_type='FIXTURES'`` AND ``instrument_count==0``. These violate
CLAUDE.md "4 pillars" rule #1 (``row_count > 0`` for ``captured`` shards;
zero-row shards must be ``empty_confirmed``, never ``captured``).

Root cause (already fixed, instruments-service ``f36651c``): the FIXTURES
adapter was emitting ``manifest.add(row_count=0, ...)`` for every
Prediction-tier league x date, creating phantom ``captured`` rows that
the orchestrator's ``_should_skip_shard`` then trusts — preventing
re-attempt under the writer fix.

What this script does:
  1. Read the canonical sports manifest at
     ``gs://instruments-store-sports-{pid}/_index/availability_index.parquet``.
  2. Backup the existing parquet to a timestamped sibling
     ``_index/availability_index.{run_ts}.bak.parquet`` (idempotent + reversible).
  3. Identify rows where ``capture_status == 'captured'`` AND
     ``data_type == 'FIXTURES'`` AND ``instrument_count == 0``.
  4. Flip those rows to ``capture_status='empty_confirmed'`` with
     ``error_reason='phantom_zero_row_count_fixed_by_f36651c'``. Preserve
     the original ``attempted_at`` (the audit trail of when the writer
     first wrote the phantom) so the historical record stays honest.
  5. Upload the patched parquet in place.
  6. Print summary (rows touched, before/after capture_status distribution,
     per-league breakdown).

Why ``empty_confirmed`` and not ``attempted_failed``: per the plan's analysis,
``manifest.add(row_count=0)`` was a manifest-only API — no parquet was ever
written for these dates, so they're equivalent to "we tried, source returned
zero data, recorded honestly". That's exactly the ``empty_confirmed``
semantic. Flipping to ``attempted_failed`` would force the orchestrator to
re-attempt every one (~100k api_football calls) when the orchestrator's
writer fix can already write ``empty_confirmed`` correctly on retry —
shorter path is to flip directly.

Why no path probing: the bug signature
``capture_status='captured' AND data_type='FIXTURES' AND instrument_count==0``
is unambiguous. The legacy ``manifest.add()`` API didn't write parquets, so
all such rows are guaranteed to have no on-disk artifact. Skipping the per-row
GCS probe takes the run from ~2h to ~30s.

Usage:
  python scripts/flip_phantom_fixtures_zero_rows.py --dry-run   # report only
  python scripts/flip_phantom_fixtures_zero_rows.py --apply     # write changes

Idempotent — re-running after an --apply is a no-op since flipped rows
no longer have ``capture_status='captured'``.
"""

from __future__ import annotations

import argparse
import io
import logging
import sys
from datetime import UTC, datetime

import pandas as pd
from unified_trading_library import get_storage_client

logger = logging.getLogger(__name__)

_INDEX_PATH = "_index/availability_index.parquet"
_TARGET_CAPTURE_STATUS = "empty_confirmed"
_TARGET_ERROR_REASON = "phantom_zero_row_count_fixed_by_f36651c"


def _bucket_for_project(project_id: str) -> str:
    return f"instruments-store-sports-{project_id}"


def _backup_path(run_ts: str) -> str:
    return f"_index/availability_index.{run_ts}.bak.parquet"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--project-id",
        default="central-element-323112",
        help="GCP project id (default: central-element-323112).",
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would change without touching GCS.",
    )
    mode.add_argument(
        "--apply",
        action="store_true",
        help="Write the patched manifest back to GCS (after backup).",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    bucket = _bucket_for_project(args.project_id)
    run_ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    backup_path = _backup_path(run_ts)

    storage = get_storage_client(project_id=args.project_id)
    logger.info("Reading manifest gs://%s/%s", bucket, _INDEX_PATH)
    raw_bytes = storage.download_bytes(bucket, _INDEX_PATH)
    df = pd.read_parquet(io.BytesIO(raw_bytes))
    logger.info("Manifest rows: %d", len(df))

    required_cols = ("capture_status", "data_type", "instrument_count")
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        logger.error(
            "Manifest is missing required columns %s — is this an older schema? Aborting.",
            missing,
        )
        return 2

    # Phantom signature: captured + data_type=FIXTURES + instrument_count=0.
    # Numeric coercion handles the case where instrument_count was written as
    # an object dtype (older schema-v2/v3 rows occasionally had this drift).
    instrument_count_int = pd.to_numeric(df["instrument_count"], errors="coerce").fillna(-1).astype(int)
    captured_mask = df["capture_status"].astype(str) == "captured"
    fixtures_mask = df["data_type"].astype(str) == "FIXTURES"
    zero_count_mask = instrument_count_int == 0

    phantom_mask = captured_mask & fixtures_mask & zero_count_mask
    phantom_count = int(phantom_mask.sum())

    real_fixtures_count = int((captured_mask & fixtures_mask & ~zero_count_mask).sum())
    logger.info("FIXTURES captured + instrument_count > 0 (real): %d", real_fixtures_count)
    logger.info("FIXTURES captured + instrument_count == 0 (phantom — to flip): %d", phantom_count)

    if phantom_count == 0:
        logger.info("No phantom rows to flip — manifest already honest. Exiting.")
        return 0

    # Per-league breakdown for the audit log.
    if "league_id" in df.columns:
        by_league = df.loc[phantom_mask].groupby("league_id").size().sort_values(ascending=False)
        logger.info("Phantom row distribution by league (top 15):")
        for league_id, count in by_league.head(15).items():
            logger.info("  %-40s %d", league_id, count)
        logger.info("Total leagues with phantom rows: %d", len(by_league))

    if args.dry_run:
        logger.info(
            "[dry-run] Would flip %d rows: capture_status -> '%s', error_reason -> '%s'",
            phantom_count,
            _TARGET_CAPTURE_STATUS,
            _TARGET_ERROR_REASON,
        )
        logger.info("[dry-run] Would write backup to gs://%s/%s", bucket, backup_path)
        logger.info("[dry-run] attempted_at preserved on flipped rows (historical record).")
        return 0

    logger.info("Writing backup to gs://%s/%s", bucket, backup_path)
    storage.upload_bytes(bucket, backup_path, raw_bytes)

    # Flip capture_status + error_reason; preserve original attempted_at.
    df.loc[phantom_mask, "capture_status"] = _TARGET_CAPTURE_STATUS
    if "error_reason" in df.columns:
        df.loc[phantom_mask, "error_reason"] = _TARGET_ERROR_REASON

    out_buf = io.BytesIO()
    df.to_parquet(out_buf, index=False, engine="pyarrow")
    out_buf.seek(0)
    storage.upload_bytes(bucket, _INDEX_PATH, out_buf.read())
    logger.info(
        "Flipped %d phantom rows to '%s'; manifest re-uploaded. Backup retained at gs://%s/%s",
        phantom_count,
        _TARGET_CAPTURE_STATUS,
        bucket,
        backup_path,
    )
    logger.info(
        "Verify with:  gcloud storage ls -l gs://%s/_index/availability_index.parquet gs://%s/%s",
        bucket,
        bucket,
        backup_path,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
