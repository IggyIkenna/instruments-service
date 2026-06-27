#!/usr/bin/env python3
# Epic: prediction_venue_perps_and_live_clob_depth
# Lifecycle: permanent — invoked daily by Cloud Scheduler `is-daily-enum-<ag>` (13:30 UTC)
# Delete-when: NA
"""daily_is_enumeration.py — rolling-window IS instrument batch enumeration for Cloud Scheduler.

Runs ``python -m instruments_service --operation instruments --mode batch`` for the
requested asset_group(s) over a trailing date window (default 3 days ending today).
Runs at 13:30 UTC so Polymarket BTC daily markets (~13:00 UTC listing) are captured
in the IS parquets before MTDS reads the universe at its next restart.

Each AG is isolated: a failure in one does not prevent the others from completing.
Exits 0 only when ALL requested AGs succeed; exits 1 on any failure (triggers Cloud
Scheduler retry).

Invoked by `deployment-service/terraform/gcp/daily_is_enumeration_scheduler.tf` with
``--asset-group <ag>`` per Cloud Run Job (one job per AG, parallel execution).
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from datetime import UTC, datetime, timedelta

# Epic: prediction_venue_perps_and_live_clob_depth
# Lifecycle: permanent

logger = logging.getLogger(__name__)

_ALL_ASSET_GROUPS: list[str] = ["cefi", "defi", "tradfi", "sports", "prediction"]


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--asset-group",
        default=None,
        help="Single asset group to enumerate; omit to run all sequentially.",
    )
    parser.add_argument(
        "--days-back",
        type=int,
        default=3,
        help="Rolling window size in days ending today (default: 3). "
        "Provides catch-up resilience if a prior day's run was missed.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Python logging level (default: INFO).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        default=False,
        help="Pass --force to IS batch enumeration (re-fetches even if parquets exist). "
        "Recommended for the 13:30 UTC run so morning partial parquets are overwritten.",
    )
    return parser.parse_args(argv)


def _run_enumeration_for_ag(
    ag: str,
    start_date: str,
    end_date: str,
    *,
    force: bool,
    log_level: str,
) -> bool:
    """Run IS batch enumeration for a single asset group. Returns True on success."""
    cmd: list[str] = [
        sys.executable,
        "-m",
        "instruments_service",
        "--operation",
        "instruments",
        "--mode",
        "batch",
        "--asset-group",
        ag,
        "--start-date",
        start_date,
        "--end-date",
        end_date,
        "--log-level",
        log_level,
    ]
    if force:
        cmd.append("--force")
    logger.info(
        "IS enum START asset_group=%s %s → %s force=%s",
        ag,
        start_date,
        end_date,
        force,
    )
    result = subprocess.run(cmd, check=False)  # nosec B603 — fixed args, no user input
    if result.returncode == 0:
        logger.info("IS enum OK asset_group=%s", ag)
        return True
    logger.error(
        "IS enum FAILED asset_group=%s exit_code=%d (other AGs continue)",
        ag,
        result.returncode,
    )
    return False


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        stream=sys.stdout,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    now = datetime.now(UTC)
    end_date = now.date().isoformat()
    start_date = (now.date() - timedelta(days=max(0, args.days_back - 1))).isoformat()

    asset_groups = [args.asset_group] if args.asset_group else _ALL_ASSET_GROUPS
    logger.info(
        "IS daily enumeration START window=%s → %s days_back=%d force=%s AGs=%s",
        start_date,
        end_date,
        args.days_back,
        args.force,
        asset_groups,
    )

    failed: list[str] = []
    for ag in asset_groups:
        ok = _run_enumeration_for_ag(
            ag,
            start_date,
            end_date,
            force=args.force,
            log_level=args.log_level,
        )
        if not ok:
            failed.append(ag)

    if failed:
        logger.error(
            "IS daily enumeration DONE with failures — failed_ags=%s succeeded=%d",
            failed,
            len(asset_groups) - len(failed),
        )
        sys.exit(1)
    logger.info(
        "IS daily enumeration DONE OK — all %d AG(s) succeeded", len(asset_groups)
    )


if __name__ == "__main__":
    main()
