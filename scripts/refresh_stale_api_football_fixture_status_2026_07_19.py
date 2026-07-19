# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: after the two known residual clusters (2026-02-21..03-22, 2026-06-24..07-14)
#   are confirmed re-fetched and parent issue doc archived
"""Bounded backfill-correction sweep for the api_football stale-NS fixture-status finding.

Plan: ``unified-trading-pm/plans/active/issues/
api_football_enrichment_stale_ns_fixture_status_and_gate_reader_inconsistency_2026_07_19.md``.

Root cause (already FIXED, instruments-service@4ef4cfeb): every FIXTURES row
captured through the live/daily write path was permanently stamped
``status_short=NS`` regardless of the real outcome, because
``CanonicalFixture`` has no ``status`` attribute — the flatten step read a
field that was always ``None``. The write-path fix only corrects NEW writes
going forward; rows captured BEFORE the fix still carry the wrong status on
disk and need a one-time backfill-correction re-fetch.

This script targets ONLY the two known residual clusters (do NOT blanket-
``--force`` a wider window — most other dates are already correctly
settled, and a blanket re-fetch wastes real api-football budget):

  * 2026-02-21..2026-03-22 (mixed ~30-45% NS)
  * 2026-06-24..2026-07-14 (~100% NS)

Uses the targeted scan (``_find_stale_fixture_leagues_for_date``) + trigger
(``run_sports_fixture_status_refresh``) shipped in this same issue doc's
periodic-status-refresh todo — so only genuinely-still-non-terminal
(date, league) cells are re-fetched, never a blanket per-date re-fetch.

Usage
-----
::

  # Dry-run (default) — read-only GCS scan, reports (date, league) cells that
  # would be re-fetched, NO api-football calls, NO writes:
  python scripts/refresh_stale_api_football_fixture_status_2026_07_19.py --dry-run

  # Apply — real api-football re-fetch + GCS/manifest writes for both clusters:
  python scripts/refresh_stale_api_football_fixture_status_2026_07_19.py --apply
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import date, timedelta

from unified_trading_library import MockEventSink, get_secret, resolve_bucket_name, setup_events

from instruments_service.engine.orchestrator.sports_fixtures import (
    _find_stale_fixture_leagues_for_date as find_stale_fixture_leagues_for_date,  # pyright: ignore[reportPrivateUsage]
)
from instruments_service.triggers.sports_fixture_status_refresh import (
    run_sports_fixture_status_refresh,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# The two known residual clusters (issue doc's Recommended decision item 2).
_CLUSTERS: list[tuple[date, date]] = [
    (date(2026, 2, 21), date(2026, 3, 22)),
    (date(2026, 6, 24), date(2026, 7, 14)),
]


def _dates_in(start: date, end: date) -> list[date]:
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def _dry_run_report(bucket: str) -> dict[str, int]:
    """Read-only pre-flight — counts stale (date, league) cells per cluster, no API calls."""
    totals: dict[str, int] = {}
    for start, end in _CLUSTERS:
        cluster_key = f"{start.isoformat()}..{end.isoformat()}"
        stale_cells = 0
        for day in _dates_in(start, end):
            stale_leagues = find_stale_fixture_leagues_for_date(bucket, day.isoformat())
            stale_cells += len(stale_leagues)
        totals[cluster_key] = stale_cells
        logger.info("DRY-RUN cluster=%s: %d stale (date, league) cell(s) would be re-fetched", cluster_key, stale_cells)
    return totals


async def _apply(bucket: str, api_key: str) -> dict[str, dict[str, int]]:
    """Real re-fetch — one ``run_sports_fixture_status_refresh`` call per cluster."""
    results: dict[str, dict[str, int]] = {}
    for start, end in _CLUSTERS:
        cluster_key = f"{start.isoformat()}..{end.isoformat()}"
        span_days = (end - start).days + 1
        logger.info("APPLY cluster=%s (span=%d days) — starting real re-fetch", cluster_key, span_days)
        result = await run_sports_fixture_status_refresh(
            today=end,
            api_key=api_key,
            bucket=bucket,
            min_age_days=0,
            lookback_days=span_days,
            correlation_id=f"fixture-status-backfill-{cluster_key}",
        )
        results[cluster_key] = result
        logger.info("APPLY cluster=%s DONE — %d (date, league) cell(s) re-written", cluster_key, len(result))
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Run the real re-fetch (default: dry-run report only).")
    args = parser.parse_args()

    bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")
    logger.info("bucket=%s", bucket)

    if not args.apply:
        totals = _dry_run_report(bucket)
        logger.info("DRY-RUN totals: %s (total=%d)", totals, sum(totals.values()))
        return

    api_key = get_secret("api-football-api-key")
    if not api_key or not api_key.strip():
        raise RuntimeError("api-football-api-key did not resolve from Secret Manager — check ADC/permissions")
    setup_events(service_name="api-football-fixture-status-backfill", mode="batch", sink=MockEventSink())
    results = asyncio.run(_apply(bucket, api_key.strip()))
    total_cells = sum(len(v) for v in results.values())
    logger.info("APPLY totals: %d cluster(s), %d total (date, league) cell(s) re-written", len(results), total_cells)


if __name__ == "__main__":
    main()
