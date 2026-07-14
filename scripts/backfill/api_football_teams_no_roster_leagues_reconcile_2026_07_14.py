# Epic: sports_master
# Lifecycle: ONE-OFF — relabels api_football TEAMS ``expected_unattempted``
#   rows to ``empty_confirmed(EXPECTED_NO_PROVIDER_COVERAGE)`` for the 8
#   cup/one-off-competition leagues whose api_football ``/teams`` endpoint
#   returns 0 teams (no persistent roster). Root-caused in
#   plans/active/sports_data_sources_canonical_completion_2026_07_13.md todo -017.
# Delete-when: manifest shows 0 expected_unattempted api_football TEAMS rows for
#   the 8 NO_ROSTER leagues below.
# SSOT: plans/active/sports_data_sources_canonical_completion_2026_07_13.md
"""api_football_teams_no_roster_leagues_reconcile_2026_07_14.py — closes the
api_football TEAMS ``expected_unattempted`` residual for 8 cup/one-off
competitions.

ROOT CAUSE (diagnosed 2026-07-13/14, see the plan above): api_football's
``/teams`` endpoint returns **0 teams — no roster to backfill with** for these 8
cup / one-off competitions (confirmed live via the 61-league backfill's own fetch
phase, NOT a script/API-key issue). Cups have no persistent registered team
roster; teams qualify per round. These leagues are ALREADY correctly marked
``is_league_entity_covered(league, "TEAMS") == False`` in the UAC coverage map,
and the live TEAMS writer already emits ``EXPECTED_NO_PROVIDER_COVERAGE`` for the
dates it processes (776 such rows exist). The residual is purely HISTORICAL:
dates 2018-01-01→2026-07-10 were enumerator-seeded as ``expected_unattempted``,
and the season-cached TEAMS fetch (``_fetch_teams_and_standings``) only runs
``emit_empty_gaps_for_entity`` on the current/recent dates it actually processes
— so the historical cells never got the honest-absence emission and sit
``expected_unattempted`` forever.

FIX: data-only. No code change is needed (coverage map + writer are already
correct for going-forward dates). This script writes one terminal
``record_empty(reason=EXPECTED_NO_PROVIDER_COVERAGE)`` per orphaned
``(date, data_type=TEAMS, league_id)`` cell — the EXACT reason the live writer
already uses for these (league, entity) pairs — so the historical cells match the
776 already-correct rows. The manifest reader's last-write-wins dedup supersedes
the seeded ``expected_unattempted`` cell at read time.

Usage:
  api_football_teams_no_roster_leagues_reconcile_2026_07_14.py [--dry-run]
Writes to REAL prod GCS (instruments-store-sports-prd-<project>) unless --dry-run.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout, force=True)
log = logging.getLogger("api_football_teams_no_roster_reconcile")

# The 8 cup/one-off competitions whose api_football /teams returns 0 teams.
_NO_ROSTER_LEAGUES: frozenset[str] = frozenset(
    {
        "COPA_LIGA_PROFESIONAL",
        "COPA_MX",
        "EMPEROR_CUP",
        "GREEK_SUPER_LEAGUE_2",
        "J2_LEAGUE",
        "SCOTTISH_LEAGUE_CUP",
        "SUPERCOPA_ESPANA",
        "SUPERCOPPA_ITALIANA",
    }
)


def _args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--project", default="central-element-323112")
    p.add_argument("--vm-name", default="af-teams-no-roster-reconcile-2026-07-14")
    p.add_argument("--dry-run", action="store_true", help="Scan + report the orphaned rows found, do not write.")
    return p.parse_args()


ARGS = _args()
os.environ["GCP_PROJECT_ID"] = ARGS.project
os.environ["GOOGLE_CLOUD_PROJECT"] = ARGS.project
os.environ["DEPLOYMENT_ENV"] = "prod"
os.environ["MANIFEST_PER_VM_SHARDS"] = "true"
os.environ["VM_NAME"] = ARGS.vm_name
os.environ.pop("CLOUD_MOCK_MODE", None)

from unified_trading_library import setup_events

setup_events("instruments-service", "local")

import pandas as pd
import unified_trading_library.manifest_writer as _mw
from unified_api_contracts import EmptyConfirmedReason, PipelineMode
from unified_trading_library import ManifestWriter, read_availability_index, resolve_bucket_name

from instruments_service.engine.orchestrator import _sports_ref_source

BUCKET = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports", deployment_env="prod")


def find_orphans() -> pd.DataFrame:
    """Live: api_football TEAMS expected_unattempted rows for the 8 NO_ROSTER leagues."""
    _mw._INDEX_CACHE.clear()  # bust the read cache — always read live
    df = read_availability_index(BUCKET)
    src = df.get("source", pd.Series("", index=df.index)).astype("string").fillna("")
    dt = df["data_type"].astype("string").fillna("").str.upper()
    lid = df["league_id"].astype("string").fillna("")
    mask = (
        (src == "api_football")
        & (dt == "TEAMS")
        & (df["capture_status"] == "expected_unattempted")
        & (lid.isin(_NO_ROSTER_LEAGUES))
    )
    return df[mask].copy()


def main() -> int:
    orphans = find_orphans()
    log.info("Found %d api_football TEAMS expected_unattempted rows for the 8 NO_ROSTER leagues", len(orphans))
    if orphans.empty:
        log.info("Nothing to reconcile.")
        return 0

    for lid_val, grp in orphans.groupby(orphans["league_id"].astype("string")):
        dates = sorted(grp["date"].astype(str).unique())
        log.info("  league=%s: %d dates (%s .. %s)", lid_val, len(dates), dates[0], dates[-1])

    if ARGS.dry_run:
        log.info("DRY RUN — no writes performed. Re-run without --dry-run to apply.")
        return 0

    manifest = ManifestWriter(service_name="instruments-service", catalogue_bucket=BUCKET)
    source_key = _sports_ref_source("teams")
    n_written = 0
    for _, row in orphans.iterrows():
        manifest.record_empty(
            row_key={"date": str(row["date"]), "data_type": "TEAMS", "league_id": str(row["league_id"])},
            reason=EmptyConfirmedReason.EXPECTED_NO_PROVIDER_COVERAGE,
            pipeline_mode=PipelineMode.BATCH_API_FOOTBALL,
            source=source_key,
        )
        n_written += 1
    manifest.write()
    log.info("Reconciled %d rows to empty_confirmed(EXPECTED_NO_PROVIDER_COVERAGE); manifest flushed.", n_written)

    remaining = find_orphans()
    log.info(
        "=== POST-RECONCILE VERIFY: %d expected_unattempted TEAMS rows remain for the 8 leagues ===", len(remaining)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
