"""Phase 1b — re-flip phantom rows to attempted_failed for orchestrator re-attempt.

Correction to ``flip_phantom_fixtures_zero_rows.py`` (commit ``962982e``):
that script flipped 100,431 phantom FIXTURES rows from ``captured`` →
``empty_confirmed``, but the orchestrator's ``_should_skip_shard`` skips both
states. The plan's intent was to force re-attempt under the writer fix
(``f36651c``), which requires ``capture_status='attempted_failed'``.

This script does TWO things in one backup-then-write pass:

1. **Re-flip** the 100,431 rows tagged
   ``error_reason='phantom_zero_row_count_fixed_by_f36651c'`` from
   ``empty_confirmed`` → ``attempted_failed`` (with new error_reason
   ``phantom_re_attempt_after_writer_fix_f36651c``).

2. **Extend** to api_football per-fixture data types that share the same
   bug class. These entities depend on FIXTURES enumeration, so when
   FIXTURES was phantom-empty they also recorded ``manifest.add(row_count=0)``
   for the same (date, league) pairs. Targeting:
     * INJURIES, FIXTURE_STATS, FIXTURE_EVENTS, FIXTURE_LINEUPS, PLAYER_STATS
   Filter: ``capture_status='captured' AND instrument_count==0`` AND
   the (date, league) pair matches one of the FIXTURES phantom pairs.
   ~75,590 rows expected.

What we DO NOT touch in this pass:
  - ``empty_confirmed`` rows on the per-fixture entities (~223k). Most are
    legitimately empty (no fixtures that day → nothing to fetch). The few
    wrongs (fixtures existed but skipped due to FIXTURES phantom) will get
    re-attempted naturally when the orchestrator re-runs FIXTURES first
    and the per-fixture entity then sees real fixtures to enumerate. A
    follow-up audit after FIXTURES backfill completes can compare
    fresh-FIXTURES-rows vs per-fixture empty rows on the same pairs and
    flip any genuine drift.

Backup-then-write pattern. Idempotent — re-running after --apply finds 0 rows.

Usage:
  python scripts/flip_phantom_to_attempted_failed.py --dry-run
  python scripts/flip_phantom_to_attempted_failed.py --apply
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
_PRIOR_FIXTURES_MARKER = "phantom_zero_row_count_fixed_by_f36651c"
_TARGET_CAPTURE_STATUS = "attempted_failed"
_TARGET_ERROR_REASON = "phantom_re_attempt_after_writer_fix_f36651c"

# api_football per-fixture data types — same manifest.add(row_count=0) bug class
# when FIXTURES is phantom-empty. The orchestrator iterates these per-(date,league)
# and emits the same zero-row pattern when the fixture enumeration finds none.
_AF_PER_FIXTURE_DATA_TYPES: tuple[str, ...] = (
    "INJURIES",
    "FIXTURE_STATS",
    "FIXTURE_EVENTS",
    "FIXTURE_LINEUPS",
    "PLAYER_STATS",
)


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

    required_cols = ("capture_status", "data_type", "instrument_count", "error_reason")
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        logger.error(
            "Manifest is missing required columns %s — aborting.",
            missing,
        )
        return 2

    # --- (1) FIXTURES re-flip: empty_confirmed → attempted_failed ---
    fixtures_re_flip_mask = (
        (df["capture_status"].astype(str) == "empty_confirmed")
        & (df["error_reason"].astype(str) == _PRIOR_FIXTURES_MARKER)
    )
    fixtures_re_flip_count = int(fixtures_re_flip_mask.sum())
    logger.info("(1) FIXTURES rows tagged %r in empty_confirmed: %d", _PRIOR_FIXTURES_MARKER, fixtures_re_flip_count)

    # Build the (date, league_id) phantom pair set — for cross-referencing
    # per-fixture entities below.
    fixtures_phantom_pairs = df.loc[fixtures_re_flip_mask, ["date", "league_id"]]
    phantom_pair_set: set[tuple[str, str]] = set(
        zip(
            fixtures_phantom_pairs["date"].astype(str),
            fixtures_phantom_pairs["league_id"].astype(str),
            strict=False,
        )
    )
    logger.info("Unique (date, league) phantom pairs: %d", len(phantom_pair_set))

    # --- (2) api_football per-fixture cap-zero on phantom pairs ---
    instrument_count_int = pd.to_numeric(df["instrument_count"], errors="coerce").fillna(-1).astype(int)
    af_cap_zero_mask = (
        (df["capture_status"].astype(str) == "captured")
        & (instrument_count_int == 0)
        & (df["data_type"].astype(str).isin(_AF_PER_FIXTURE_DATA_TYPES))
    )
    # Restrict to the phantom (date, league) pair set.
    af_pairs = list(zip(df["date"].astype(str), df["league_id"].astype(str), strict=False))
    af_on_phantom_mask = pd.Series([p in phantom_pair_set for p in af_pairs])
    af_target_mask = af_cap_zero_mask & af_on_phantom_mask
    af_target_count = int(af_target_mask.sum())
    logger.info(
        "(2) api_football per-fixture cap-zero on phantom pairs: %d (across %s)",
        af_target_count,
        ", ".join(_AF_PER_FIXTURE_DATA_TYPES),
    )
    # Per-data-type breakdown
    if af_target_count > 0:
        by_dt = df.loc[af_target_mask].groupby("data_type").size().sort_values(ascending=False)
        for dt, n in by_dt.items():
            logger.info("    %-20s %d", dt, n)

    total_to_flip = fixtures_re_flip_count + af_target_count
    logger.info("Total rows to flip to attempted_failed: %d", total_to_flip)

    if total_to_flip == 0:
        logger.info("Nothing to flip — manifest already canonical. Exiting.")
        return 0

    if args.dry_run:
        logger.info(
            "[dry-run] Would flip %d rows: capture_status -> '%s', error_reason -> '%s'",
            total_to_flip,
            _TARGET_CAPTURE_STATUS,
            _TARGET_ERROR_REASON,
        )
        logger.info("[dry-run] Would write backup to gs://%s/%s", bucket, backup_path)
        return 0

    logger.info("Writing backup to gs://%s/%s", bucket, backup_path)
    storage.upload_bytes(bucket, backup_path, raw_bytes)

    # Apply both flips
    combined_mask = fixtures_re_flip_mask | af_target_mask
    df.loc[combined_mask, "capture_status"] = _TARGET_CAPTURE_STATUS
    df.loc[combined_mask, "error_reason"] = _TARGET_ERROR_REASON
    # Stamp attempted_at with NOW so the orchestrator's TTL freshness checks
    # treat this as a fresh failure rather than ancient history.
    if "attempted_at" in df.columns:
        df.loc[combined_mask, "attempted_at"] = datetime.now(UTC).isoformat()

    out_buf = io.BytesIO()
    df.to_parquet(out_buf, index=False, engine="pyarrow")
    out_buf.seek(0)
    storage.upload_bytes(bucket, _INDEX_PATH, out_buf.read())
    logger.info(
        "Flipped %d rows to '%s'; manifest re-uploaded. Backup retained at gs://%s/%s",
        total_to_flip,
        _TARGET_CAPTURE_STATUS,
        bucket,
        backup_path,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
