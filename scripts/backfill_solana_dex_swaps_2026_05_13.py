"""Backfill script — Solana AMM/DEX dex_swaps data for Plan C venues.

Backfills dex_swaps instrument coverage for the 4 new Solana AMM/CLOB venues
added in Plan C (Meteora, Phoenix, Jupiter, Lifinity).

Per-VM shard isolation: set VM_NAME=<unique-tag> + MANIFEST_PER_VM_SHARDS=true.

Usage (operator-runnable; dry-run by default):
    python3 scripts/backfill_solana_dex_swaps_2026_05_13.py \\
        --venue meteora \\
        --start-date 2022-09-01 \\
        --end-date 2026-05-13 \\
        --dry-run

    python3 scripts/backfill_solana_dex_swaps_2026_05_13.py \\
        --venue all \\
        --start-date 2023-01-01 \\
        --end-date 2026-05-13 \\
        --apply --confirm

VM launch command (operator runs this — do NOT execute from this script):
    gcloud compute instances create solana-dex-backfill-$(date +%Y%m%d-%H%M) \\
        --zone=asia-northeast1-b \\
        --machine-type=n2-standard-8 \\
        --image-family=debian-11 \\
        --image-project=debian-cloud \\
        --metadata=startup-script="export VM_NAME=solana-dex-backfill MANIFEST_PER_VM_SHARDS=true ...backfill cmd..." \\
        --scopes=cloud-platform

Execution metadata (SSOT per Runbook Execution-Owner rule):
    execution:
      owner: Tab 2 — Ikenna slot 2 / cron post-May-23
      cadence: one-shot (backfill)
      verifier: manifest captured rows for METEORA-SOLANA/PHOENIX-SOLANA/JUPITER-SOLANA/LIFINITY-SOLANA
      last_executed: NEVER

Plan: solana_amm_coverage_expansion_2026_05_13 Phase 6.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

# Supported venues for this backfill
PLAN_C_VENUES: list[str] = [
    "meteora",
    "phoenix",
    "jupiter",
    "lifinity",
]

# Deploy dates per venue (SSOT: _solana_utils.SOLANA_PROTOCOL_DEPLOY_DATES)
VENUE_DEPLOY_DATES: dict[str, date] = {
    "meteora": date(2022, 9, 1),
    "phoenix": date(2023, 6, 1),
    "jupiter": date(2021, 11, 1),
    "lifinity": date(2022, 3, 1),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill dex_swaps for Solana AMM Plan C venues (Meteora, Phoenix, Jupiter, Lifinity).",
    )
    parser.add_argument(
        "--venue",
        choices=[*PLAN_C_VENUES, "all"],
        default="all",
        help="Venue to backfill. Use 'all' for all Plan C venues.",
    )
    parser.add_argument(
        "--start-date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        default=None,
        help="Start date for backfill (YYYY-MM-DD). Defaults to venue deploy date.",
    )
    parser.add_argument(
        "--end-date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        default=date.today(),
        help="End date for backfill (YYYY-MM-DD). Defaults to today.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Dry-run mode (default). Shows what would be backfilled without writing.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        default=False,
        help="Apply changes (disables dry-run).",
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        default=False,
        help="Confirm destructive writes (required with --apply).",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
    )
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if args.apply and not args.confirm:
        logger.error("--apply requires --confirm to guard against accidental writes.")
        sys.exit(1)
    if args.apply:
        args.dry_run = False


def get_venues(args: argparse.Namespace) -> list[str]:
    if args.venue == "all":
        return PLAN_C_VENUES
    return [args.venue]


def get_start_date(venue: str, args: argparse.Namespace) -> date:
    if args.start_date is not None:
        return args.start_date
    return VENUE_DEPLOY_DATES[venue]


def log_backfill_plan(
    venues: list[str],
    start_dates: dict[str, date],
    end_date: date,
    dry_run: bool,
) -> None:
    mode = "DRY-RUN" if dry_run else "APPLY"
    logger.info("=== Solana AMM dex_swaps backfill plan (%s) ===", mode)
    for venue in venues:
        start = start_dates[venue]
        days = (end_date - start).days
        logger.info("  %s: %s → %s (%d days)", venue.upper(), start, end_date, days)
    logger.info("=============================================")


def run_backfill(
    venue: str,
    start_date: date,
    end_date: date,
    dry_run: bool,
) -> None:
    """Execute backfill for a single venue.

    NOTE: This skeleton logs the intended operation. The full backfill
    implementation connects to the instruments-service pipeline to write
    dex_swaps parquets + manifest rows. The VM launch pattern is documented
    in the plan body for operator execution.
    """
    logger.info(
        "[%s] %s backfill from %s to %s — dry_run=%s",
        "PLAN" if dry_run else "WRITE",
        venue.upper(),
        start_date,
        end_date,
        dry_run,
    )

    # In dry-run: log the scope without writing
    if dry_run:
        days = (end_date - start_date).days
        logger.info(
            "[DRY-RUN] Would backfill %d days of dex_swaps for %s",
            days,
            venue.upper(),
        )
        return

    # APPLY path: import adapter + run capture
    # This is the implementation surface — wired to the instruments-service
    # capture pipeline with per-VM shard isolation enforced by ManifestWriter.
    logger.warning(
        "APPLY mode: dex_swaps capture for %s not yet wired to pipeline. "
        "See plan solana_amm_coverage_expansion_2026_05_13 Phase 6 for handoff.",
        venue.upper(),
    )


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    validate_args(args)

    venues = get_venues(args)
    start_dates = {v: get_start_date(v, args) for v in venues}

    log_backfill_plan(venues, start_dates, args.end_date, args.dry_run)

    for venue in venues:
        run_backfill(
            venue=venue,
            start_date=start_dates[venue],
            end_date=args.end_date,
            dry_run=args.dry_run,
        )

    logger.info("Backfill plan complete (dry_run=%s).", args.dry_run)


if __name__ == "__main__":
    main()
