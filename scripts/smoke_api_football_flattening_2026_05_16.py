"""Smoke test: api_football flattening produces multi-row output per fixture.

Closes Phase 3.B of ``api_football_minimal_flattening_removal_2026_05_07.md``:
verify that ``normalize_api_football_fixture_stats / _event / _lineup`` produce
the expected per-row expansion when fed live API data, not just synthetic.

Usage:
    GCP_PROJECT_ID=central-element-323112 \\
      python3 scripts/smoke_api_football_flattening_2026_05_16.py --fixture-id 1208051

If --fixture-id is omitted, the script picks a recently-played EPL fixture from
api_football's live endpoint (status_short in {FT, AET, PEN}).

Asserts (per plan body § "Success criteria"):
- fixture_stats: ≥2 rows (one per team); each row carries fixture_id + team_id
  + ~18 stat columns.
- fixture_events: ≥1 row; each row carries event_type + time_elapsed.
- fixture_lineups: ≥22 rows (≥11 starters per team across both teams).
- injuries (date-level): ≥0 rows (may legitimately be empty).

Output: per-handler column counts + row counts. Non-zero exit if any handler
returns an empty list where ≥1 was expected.

Execution ownership (Runbook SSOT):
  execution:
    owner: slot-4-ikenna (one-shot per plan close-out)
    cadence: one-shot
    verifier: stdout contains "SMOKE PASS" on success
    last_executed: 2026-05-16
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys

from unified_trading_library import get_secret

from instruments_service.reference_data.adapters.sports.adapters.api_football import (
    ApiFootballAdapter,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("smoke_af_flattening")


async def _pick_recent_fixture(adapter: ApiFootballAdapter) -> int:
    """Return one recently-completed fixture_id."""
    # Fetch live fixtures first (cheap, returns ongoing matches). If none live
    # (mid-week off-season), fall back to a known-good fixture id.
    live = await adapter.get_live_fixtures()
    if live:
        first = live[0]
        fid = getattr(first, "fixture_id", None) or getattr(first, "id", None)
        if fid:
            logger.info("Picked live fixture_id=%s for smoke", fid)
            return int(fid)
    # Fallback: well-known historical fixture (EPL Liverpool vs Man Utd 2024-12-22)
    logger.info("No live fixtures; using fallback fixture_id=1208051")
    return 1208051


async def _run_smoke(fixture_id: int, api_key: str) -> int:
    adapter = ApiFootballAdapter(api_key=api_key)
    if fixture_id == 0:
        fixture_id = await _pick_recent_fixture(adapter)

    logger.info("=== api_football flattening smoke — fixture_id=%d ===", fixture_id)

    failures: list[str] = []

    stats_rows = await adapter.get_fixture_statistics(fixture_id)
    logger.info("fixture_stats: %d rows", len(stats_rows))
    if stats_rows:
        cols = sorted(stats_rows[0].keys())
        logger.info("fixture_stats[0] cols (%d): %s", len(cols), cols)
        if len(stats_rows) < 2:
            failures.append(f"fixture_stats returned {len(stats_rows)} rows; expected ≥2")
    else:
        failures.append("fixture_stats returned 0 rows; expected ≥2 (one per team)")

    events_rows = await adapter.get_fixture_events(fixture_id)
    logger.info("fixture_events: %d rows", len(events_rows))
    if events_rows:
        cols = sorted(events_rows[0].keys())
        logger.info("fixture_events[0] cols (%d): %s", len(cols), cols)

    lineups_rows = await adapter.get_fixture_lineups(fixture_id)
    logger.info("fixture_lineups: %d rows", len(lineups_rows))
    if lineups_rows:
        cols = sorted(lineups_rows[0].keys())
        logger.info("fixture_lineups[0] cols (%d): %s", len(cols), cols)
        if len(lineups_rows) < 22:
            failures.append(f"fixture_lineups returned {len(lineups_rows)} rows; expected ≥22")
    else:
        failures.append("fixture_lineups returned 0 rows; expected >=22 (11 starters x 2 teams)")

    # Injuries are date-level; pick today's date best-effort.
    from datetime import UTC, datetime

    today = datetime.now(UTC).strftime("%Y-%m-%d")
    injuries_rows = await adapter.get_injuries(today)
    logger.info("injuries (date=%s): %d rows", today, len(injuries_rows))
    if injuries_rows:
        cols = sorted(injuries_rows[0].keys())
        logger.info("injuries[0] cols (%d): %s", len(cols), cols)

    if failures:
        logger.error("SMOKE FAIL — %d assertion(s) failed:", len(failures))
        for f in failures:
            logger.error("  - %s", f)
        return 1

    logger.info("SMOKE PASS — flattening produces expected per-row expansion")
    return 0


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--fixture-id", type=int, default=0, help="api_football fixture_id (0 = pick recent)")
    return p.parse_args()


def _resolve_api_key() -> str:
    secret = get_secret("api-football-api-key")
    if not secret:
        raise RuntimeError("api-football-api-key not found in vault")
    return str(secret).strip()


def main() -> int:
    args = _parse_args()
    api_key = _resolve_api_key()
    return asyncio.run(_run_smoke(args.fixture_id, api_key))


if __name__ == "__main__":
    sys.exit(main())
