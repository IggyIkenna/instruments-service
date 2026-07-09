#!/usr/bin/env python3
# Epic: sports_data_capture_gap_2026_06_29
# Lifecycle: oneoff
# Delete-when: after IS bucket shows XG/UNDERSTAT/EPL captured > 0 rows + consolidator merge confirmed
"""Backfill Understat XG for EPL 2025 season (dates 2025-08-01 to 2025-12-01).

Root cause D (sports_data_capture_gap_2026_06_29 issue): the IS bucket holds 365
``empty_confirmed`` rows for XG/UNDERSTAT/EPL 2025 — the batch_understat pipeline
never fetched from understat.com for these dates. This script re-triggers the
pipeline with ``force=True`` to overwrite the stale empty_confirmed rows with
live-fetched xG data (captured) or honest per-league empty rows where fixtures
were genuinely absent.

CONSOLIDATOR-SAFE: set ``MANIFEST_PER_VM_SHARDS=true`` + a unique ``VM_NAME`` so
each run writes per-VM shards instead of racing on the canonical index blob. The
manifest consolidator's last-write-wins merge promotes the captures.

Usage::

    cd instruments-service

    # Dry-run (lists dates, no fetch/write):
    GCP_PROJECT_ID=central-element-323112 DEPLOYMENT_ENV_SHORT=prd \\
      CLOUD_PROVIDER=gcp CLOUD_MOCK_MODE=false \\
      MANIFEST_PER_VM_SHARDS=true VM_NAME=xg-epl2025-backfill-$(date +%s) \\
      .venv/bin/python scripts/backfill_understat_xg_epl_2025_2026_06_29.py --dry-run

    # Live run:
    GCP_PROJECT_ID=central-element-323112 DEPLOYMENT_ENV_SHORT=prd \\
      CLOUD_PROVIDER=gcp CLOUD_MOCK_MODE=false \\
      MANIFEST_PER_VM_SHARDS=true VM_NAME=xg-epl2025-backfill-$(date +%s) \\
      .venv/bin/python scripts/backfill_understat_xg_epl_2025_2026_06_29.py

After the run: verify with::

    python scripts/query_sports_is_gaps.py --data-type XG --league EPL --start 2025-08-01
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import date, timedelta

import unified_trading_library.manifest_writer as _mw
from unified_trading_library import resolve_bucket_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_DEFAULT_START = date(2025, 8, 1)
_DEFAULT_END = date(2025, 12, 1)


def _date_range(start: date, end: date) -> list[str]:
    result: list[str] = []
    current = start
    while current <= end:
        result.append(current.isoformat())
        current += timedelta(days=1)
    return result


async def _run_backfill(dates: list[str], bucket: str) -> dict[str, int]:
    from instruments_service.engine.orchestrator.understat import _fetch_understat_xg  # noqa: imports-inside-functions

    total: dict[str, int] = {}
    n = len(dates)
    for i, d in enumerate(dates):
        logger.info("Processing date=%s  (%d/%d)", d, i + 1, n)
        counts = await _fetch_understat_xg(d, bucket, force=True)
        for k, v in counts.items():
            total[k] = total.get(k, 0) + v

    # Guaranteed drain BEFORE the coroutine returns, not via atexit: atexit's
    # process_final=True drain races the asyncio event loop's own executor
    # teardown ("cannot schedule new futures after interpreter shutdown"),
    # silently dropping buffered writes. Calling the drain explicitly here,
    # while the loop is still alive, avoids the race for this one-off script.
    # See plans/active/issues/manifest_atexit_drain_races_asyncio_shutdown_2026_07_09.md.
    flushed = _mw.flush_all_pending_buckets()
    logger.info("EXPLICIT PRE-EXIT DRAIN: %s", flushed)

    return total


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List dates that would be processed without calling the understat adapter or writing to GCS",
    )
    parser.add_argument(
        "--start-date",
        default=_DEFAULT_START.isoformat(),
        help=f"Inclusive start date ISO-8601 (default: {_DEFAULT_START})",
    )
    parser.add_argument(
        "--end-date",
        default=_DEFAULT_END.isoformat(),
        help=f"Inclusive end date ISO-8601 (default: {_DEFAULT_END})",
    )
    args = parser.parse_args()

    bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")
    dates = _date_range(date.fromisoformat(args.start_date), date.fromisoformat(args.end_date))

    logger.info(
        "understat XG backfill: %d dates [%s → %s] bucket=%s dry_run=%s",
        len(dates),
        dates[0] if dates else "N/A",
        dates[-1] if dates else "N/A",
        bucket,
        args.dry_run,
    )

    if args.dry_run:
        for d in dates:
            print(f"  would process: {d}")
        return 0

    total = asyncio.run(_run_backfill(dates, bucket))
    logger.info("Backfill complete — total captured rows by entity: %s", total)
    return 0


if __name__ == "__main__":
    sys.exit(main())
