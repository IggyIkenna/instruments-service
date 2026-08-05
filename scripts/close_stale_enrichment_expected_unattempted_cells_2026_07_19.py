# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: the api_football_enrichment_stale_ns_fixture_status_and_gate_reader_inconsistency
#   issue doc is archived and this sweep has been run at least once against production
"""Close residual expected_unattempted cells for the 4 per-fixture ENRICHMENT entities.

Plan: ``unified-trading-pm/plans/active/issues/
api_football_enrichment_stale_ns_fixture_status_and_gate_reader_inconsistency_2026_07_19.md``
(final todo — "Add an off-season/no-fixture-that-date gap-closer for the 4 per-fixture
ENRICHMENT entities").

Root cause + why this is SAFE (not a blind mirror of process_write.py's FIXTURES closer):
see ``instruments_service.engine.orchestrator.sports_reference_core.
_close_stale_enrichment_expected_unattempted_cells``'s docstring. Short version: a live
spot-check on 3 sampled stuck cells (FIXTURE_STATS, 2020-06-14, AUSTRIAN_2_LIGA/
AUSTRIAN_BUNDESLIGA/GREEK_SUPER_LEAGUE) disproved the naive "off-season" hypothesis — a real
fixture existed for all 3 (FIXTURES captured), and for GREEK_SUPER_LEAGUE the SIBLING
FIXTURE_EVENTS/FIXTURE_LINEUPS entities were also captured for the identical cell. A blind
closer would have stamped ``EXPECTED_NO_FIXTURE`` over a genuine pending-fetch gap — false-
empty corruption. This script only closes a cell when the classification is independently
provable: (1) the provider genuinely has no coverage for that (league, entity), or (2) the
FIXTURES entity's own manifest row for the identical (date, league) already proves no fixture
existed. Every other stuck cell is left untouched (a real pending-fetch gap for a future
targeted re-fetch, not this script's concern).

Single manifest read (single-walk discipline): one ``read_availability_index`` call over
source=api_football, restricted to FIXTURES + the 4 enrichment data_types — no whole-corpus
GCS walk.

Usage
-----
::

  # Dry-run (default) — read-only manifest scan, reports how many (date, entity, league)
  # cells WOULD be closed and by which reason, NO manifest writes:
  python scripts/close_stale_enrichment_expected_unattempted_cells_2026_07_19.py --dry-run

  # Apply — real manifest record_empty writes for the provably-closeable subset:
  python scripts/close_stale_enrichment_expected_unattempted_cells_2026_07_19.py --apply
"""

from __future__ import annotations

import argparse
import logging
import os

import pandas as pd
from unified_trading_library import ManifestWriter, resolve_bucket_name

from instruments_service.engine.orchestrator.sports_reference_core import (
    _close_stale_enrichment_expected_unattempted_cells as close_stale_enrichment_cells,  # pyright: ignore[reportPrivateUsage]
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_ENRICHMENT_ENTITIES = ["FIXTURE_EVENTS", "FIXTURE_LINEUPS", "FIXTURE_STATS", "PLAYER_STATS"]


def _read_manifest_slices(
    bucket: str,
) -> tuple[pd.DataFrame, dict[tuple[str, str], str], dict[tuple[str, str], None]]:
    """One manifest read → (stuck enrichment cells, FIXTURES empty-reason lookup,
    FIXTURES expected_unattempted set).

    The third return value (new as of 2026-08-05) feeds the season-calendar
    gate 3: when FIXTURES itself was never fetched for a (date, league),
    ``get_league_fixture_calendar`` can independently prove off-season /
    no-fixture-that-date — closing the Christmas/holiday/today false-positive
    class (``sports_enrichment_closer_holiday_and_today_false_gaps_2026_08_03.md``).
    """
    from unified_trading_library import read_availability_index

    logger.info("Reading manifest (slim columns) from gs://%s/ ...", bucket)
    df: pd.DataFrame = read_availability_index(
        bucket,
        columns=["date", "data_type", "source", "capture_status", "error_reason", "league_id"],
    )
    logger.info("  %d total rows read", len(df))
    af = df[df["source"] == "api_football"].copy()

    enrichment = af[af["data_type"].isin(_ENRICHMENT_ENTITIES)]
    is_eu = enrichment["capture_status"] == "expected_unattempted"
    reason = enrichment["error_reason"].fillna("")
    stuck_cells = enrichment[is_eu & ~reason.str.startswith("EXPECTED_")].copy()
    logger.info("  %d stuck (expected_unattempted, blank-reason) enrichment cells", len(stuck_cells))

    fixtures = af[(af["data_type"] == "FIXTURES") & (af["capture_status"] == "empty_confirmed")]
    fixtures_empty_reason_by_date_league = {
        (str(row["date"]), str(row["league_id"])): str(row["error_reason"])
        for _, row in fixtures[["date", "league_id", "error_reason"]].dropna().iterrows()
    }
    logger.info(
        "  %d FIXTURES empty_confirmed (date, league) cells available as the safety cross-check",
        len(fixtures_empty_reason_by_date_league),
    )

    # Gate-3 feed: FIXTURES expected_unattempted (blank reason) → season-calendar check.
    fixtures_eu = af[
        (af["data_type"] == "FIXTURES")
        & (af["capture_status"] == "expected_unattempted")
        & (af["error_reason"].fillna("") == "")
    ]
    fixtures_expected_unattempted_by_date_league: dict[tuple[str, str], None] = {
        (str(row["date"]), str(row["league_id"])): None for _, row in fixtures_eu[["date", "league_id"]].iterrows()
    }
    logger.info(
        "  %d FIXTURES expected_unattempted (date, league) cells for season-calendar gate 3",
        len(fixtures_expected_unattempted_by_date_league),
    )
    return stuck_cells, fixtures_empty_reason_by_date_league, fixtures_expected_unattempted_by_date_league


def _dry_run_report(
    stuck_cells: pd.DataFrame,
    fixtures_empty_reason_by_date_league: dict[tuple[str, str], str],
    fixtures_expected_unattempted_by_date_league: dict[tuple[str, str], None],
) -> None:
    """Read-only preview — reports what WOULD close without writing anything."""
    from unified_api_contracts.canonical.domain.sports.league_data import get_league_fixture_calendar
    from unified_api_contracts.registry import is_league_entity_covered

    from instruments_service.engine.orchestrator.sports import _canonical_league_id

    no_coverage = 0
    no_fixture_mirror = 0
    season_calendar = 0
    left_untouched = 0
    for (_date, data_type), group in stuck_cells.groupby(["date", "data_type"]):
        for lid in {str(lid) for lid in group["league_id"].dropna().unique() if str(lid)}:
            if not is_league_entity_covered(lid, str(data_type)):
                no_coverage += 1
            elif (str(_date), lid) in fixtures_empty_reason_by_date_league:
                no_fixture_mirror += 1
            elif (str(_date), lid) in fixtures_expected_unattempted_by_date_league:
                _canon_lid = _canonical_league_id(lid)
                if not _canon_lid.isdigit() and not get_league_fixture_calendar(_canon_lid, str(_date), str(_date)):
                    season_calendar += 1
                else:
                    left_untouched += 1
            else:
                left_untouched += 1
    logger.info(
        "DRY-RUN: %d cell(s) would close as EXPECTED_NO_PROVIDER_COVERAGE, "
        "%d would close mirroring FIXTURES' own empty reason, "
        "%d would close via season-calendar gate 3 (FIXTURES expected_unattempted + off-season), "
        "%d left untouched (genuine pending-fetch gaps, not this script's concern)",
        no_coverage,
        no_fixture_mirror,
        season_calendar,
        left_untouched,
    )


def _apply(
    bucket: str,
    stuck_cells: pd.DataFrame,
    fixtures_empty_reason_by_date_league: dict[tuple[str, str], str],
    fixtures_expected_unattempted_by_date_league: dict[tuple[str, str], None],
) -> dict[str, int]:
    manifest = ManifestWriter(service_name="instruments-service", catalogue_bucket=bucket, per_vm_shards=True)
    counts = close_stale_enrichment_cells(
        manifest=manifest,
        stuck_cells=stuck_cells,
        fixtures_empty_reason_by_date_league=fixtures_empty_reason_by_date_league,
        fixtures_expected_unattempted_by_date_league=fixtures_expected_unattempted_by_date_league,
    )
    manifest.flush()
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply", action="store_true", help="Write the closeable cells (default: dry-run report only)."
    )
    parser.add_argument("--project", default="central-element-323112", help="GCP project ID")
    args = parser.parse_args()

    os.environ.setdefault("CLOUD_PROVIDER", "gcp")
    os.environ.setdefault("GCP_PROJECT_ID", args.project)

    bucket = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports")
    logger.info("bucket=%s", bucket)

    stuck_cells, fixtures_empty_reason_by_date_league, fixtures_expected_unattempted_by_date_league = (
        _read_manifest_slices(bucket)
    )

    if not args.apply:
        _dry_run_report(stuck_cells, fixtures_empty_reason_by_date_league, fixtures_expected_unattempted_by_date_league)
        return

    counts = _apply(
        bucket, stuck_cells, fixtures_empty_reason_by_date_league, fixtures_expected_unattempted_by_date_league
    )
    logger.info(
        "APPLY DONE — %d (date, entity) group(s) had cells closed; %d total league-cells closed",
        len(counts),
        sum(counts.values()),
    )


if __name__ == "__main__":
    main()
