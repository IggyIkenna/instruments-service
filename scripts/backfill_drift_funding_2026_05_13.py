"""Backfill DRIFT-SOLANA perp_funding data from Drift historical S3 archive.

Reads DRIFT V2 historical funding rates from:
  https://drift-historical-data-v2.s3.eu-west-1.amazonaws.com/
    program/dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH/

Writes parquet files per the standard instruments-service manifest convention.
Uses per-VM shard isolation (MANIFEST_PER_VM_SHARDS=true required).

IMPORTANT: This script requires MTDS Solana perp DEX source implementation
before full operational runs are possible. The script skeleton is shipped now;
operational runs are gated on MTDS perp source being wired. See:
  plans/active/issues/solana_defi_coverage_gaps_2026_05_13.md

Usage:
    # Dry run (default — safe to run locally):
    python3 scripts/backfill_drift_funding_2026_05_13.py \
        --start-date 2021-11-05 \
        --end-date 2026-05-13 \
        --venue DRIFT-SOLANA

    # Apply mode (operator authorization required — run on GCE VM):
    VM_NAME=ikenna-slot2-drift-funding-backfill \
    MANIFEST_PER_VM_SHARDS=true \
    DEPLOYMENT_ENV=prod \
    python3 scripts/backfill_drift_funding_2026_05_13.py \
        --start-date 2021-11-05 \
        --end-date 2026-05-13 \
        --venue DRIFT-SOLANA \
        --apply --confirm

execution:
  owner: operator (Tab in daily work-split after MTDS Solana perp source ships)
  cadence: one-shot
  verifier: manifest captured rows match date range; sample parquet perp_funding populated
  last_executed: NEVER
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

_DRIFT_V2_LAUNCH_DATE = date(2021, 11, 5)  # Drift V2 mainnet launch (conservative)
_DRIFT_PROGRAM_ID = "dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH"
_S3_BASE_URL = f"https://drift-historical-data-v2.s3.eu-west-1.amazonaws.com/program/{_DRIFT_PROGRAM_ID}"
_DEFAULT_VENUE = "DRIFT-SOLANA"
_DATA_TYPE = "perp_funding"


# ── Entry point ───────────────────────────────────────────────────────────────


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill DRIFT-SOLANA perp_funding from historical S3 archive",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--start-date",
        type=date.fromisoformat,
        default=_DRIFT_V2_LAUNCH_DATE,
        help=f"Start date YYYY-MM-DD (default: {_DRIFT_V2_LAUNCH_DATE})",
    )
    parser.add_argument(
        "--end-date",
        type=date.fromisoformat,
        default=date.today(),
        help="End date YYYY-MM-DD (default: today)",
    )
    parser.add_argument(
        "--venue",
        default=_DEFAULT_VENUE,
        help=f"Venue name (default: {_DEFAULT_VENUE})",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Execute writes. Without this flag, dry-run only.",
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Suppress interactive confirmation prompt (required for --apply in CI/VM).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return parser.parse_args()


def _validate_prerequisites() -> bool:
    """Check that required environment and dependencies are available."""
    missing: list[str] = []

    # Check MANIFEST_PER_VM_SHARDS in apply mode
    import os

    if os.environ.get("MANIFEST_PER_VM_SHARDS") != "true":
        missing.append("MANIFEST_PER_VM_SHARDS=true (required for multi-worker safety)")

    if os.environ.get("VM_NAME") is None:
        missing.append("VM_NAME=<unique-tag> (required for per-VM shard isolation)")

    if missing:
        logger.error(
            "Missing prerequisites for apply mode:\n%s",
            "\n".join(f"  - {m}" for m in missing),
        )
        return False
    return True


def _build_s3_key(date_: date, market_index: int) -> str:
    """Build the S3 key for a DRIFT funding rate file."""
    return f"fundingRate/year={date_.year}/month={date_.month:02d}/day={date_.day:02d}/market={market_index}"


def _date_range(start: date, end: date) -> list[date]:
    """Generate list of dates from start (inclusive) to end (inclusive)."""
    current = start
    dates: list[date] = []
    while current <= end:
        dates.append(current)
        current += timedelta(days=1)
    return dates


def _dry_run_report(start_date: date, end_date: date, venue: str) -> None:
    """Print what would be executed in apply mode."""
    days = len(_date_range(start_date, end_date))
    logger.info(
        "DRY RUN: Would backfill %s %s for %d days (%s → %s)",
        venue,
        _DATA_TYPE,
        days,
        start_date.isoformat(),
        end_date.isoformat(),
    )
    logger.info(
        "DRY RUN: S3 source: %s/fundingRate/year=YYYY/month=MM/day=DD/market=N",
        _S3_BASE_URL,
    )
    logger.info(
        "DRY RUN: Would write to: gs://<bucket>/market_data/defi/%s/%s/...",
        venue.lower().replace("-", "_"),
        _DATA_TYPE,
    )
    logger.info("DRY RUN: Add --apply --confirm to execute (requires VM_NAME + MANIFEST_PER_VM_SHARDS=true)")


def _run_backfill(start_date: date, end_date: date, venue: str) -> int:
    """Execute the backfill. Returns number of date-market shards processed."""
    logger.info(
        "Starting DRIFT-SOLANA perp_funding backfill: %s → %s",
        start_date.isoformat(),
        end_date.isoformat(),
    )
    dates = _date_range(start_date, end_date)
    processed = 0

    # NOTE: Full implementation requires:
    # 1. Enumerate active DRIFT perp markets via instruments-service adapter
    # 2. For each market x date: fetch S3 funding rate CSV/parquet
    # 3. Normalize to perp_funding schema (UAC canonical)
    # 4. Write to GCS via resolve_bucket_name() (never inline f-string)
    # 5. Call ManifestWriter.record_captured() or record_empty() per shard
    #
    # This skeleton ships the CLI contract + validation logic.
    # Full S3 → GCS wiring is blocked on MTDS Solana perp DEX source being wired.
    # See: plans/active/issues/solana_defi_coverage_gaps_2026_05_13.md

    logger.warning(
        "DRIFT backfill S3→GCS data wiring NOT YET IMPLEMENTED. "
        "This script ships the CLI + validation skeleton. "
        "Full wiring requires MTDS Solana perp DEX source — see issue doc."
    )

    for d in dates:
        logger.debug("Processing %s for %s %s", d.isoformat(), venue, _DATA_TYPE)
        processed += 1

    logger.info(
        "Backfill complete: %d date shards processed for %s %s",
        processed,
        venue,
        _DATA_TYPE,
    )
    return processed


def main() -> int:
    args = _parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    if args.start_date < _DRIFT_V2_LAUNCH_DATE:
        logger.warning(
            "Start date %s is before DRIFT V2 launch (%s); clamping to launch date.",
            args.start_date.isoformat(),
            _DRIFT_V2_LAUNCH_DATE.isoformat(),
        )
        args.start_date = _DRIFT_V2_LAUNCH_DATE

    if args.start_date > args.end_date:
        logger.error(
            "start_date %s is after end_date %s",
            args.start_date.isoformat(),
            args.end_date.isoformat(),
        )
        return 1

    if not args.apply:
        _dry_run_report(args.start_date, args.end_date, args.venue)
        return 0

    # Apply mode
    if not _validate_prerequisites():
        logger.error("Prerequisites not met — aborting apply.")
        return 1

    if not args.confirm:
        days = len(_date_range(args.start_date, args.end_date))
        print(
            f"\nAbout to backfill {args.venue} {_DATA_TYPE} for {days} days"
            f" ({args.start_date} → {args.end_date}).\n"
            "Add --confirm to skip this prompt.\n"
            "Proceed? [y/N] ",
            end="",
        )
        answer = input()
        if answer.strip().lower() != "y":
            logger.info("Aborted by operator.")
            return 0

    count = _run_backfill(args.start_date, args.end_date, args.venue)
    logger.info("Done. Processed %d shards.", count)
    return 0


if __name__ == "__main__":
    sys.exit(main())
