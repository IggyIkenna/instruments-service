# Epic: sports_master
# Lifecycle: ONE-OFF — closes the api_football INJURIES/PLAYER_STATS/
#   FIXTURE_STATS/FIXTURE_EVENTS/FIXTURE_LINEUPS blank-``league_id``
#   "attempted_failed" residual root-caused in
#   plans/active/sports_data_sources_canonical_completion_2026_07_13.md
#   (2026-07-15 addendum) — the broader same-root-cause sweep flagged (but
#   deliberately out of scope) by the footystats/open_meteo/SFI fix
#   (instruments-service@ed3e75b8, 2026-07-14).
# Delete-when: manifest shows 0 blank-``league_id`` attempted_failed rows for
#   source=api_football across INJURIES/PLAYER_STATS/FIXTURE_STATS/
#   FIXTURE_EVENTS/FIXTURE_LINEUPS.
# SSOT: plans/active/sports_data_sources_canonical_completion_2026_07_13.md
"""api_football_blank_league_orphan_reconcile_2026_07_15.py — closes the
``attempted_failed`` rows that carry a BLANK/None ``league_id`` for
api_football's INJURIES (date-wide entity) and PLAYER_STATS / FIXTURE_STATS /
FIXTURE_EVENTS / FIXTURE_LINEUPS (per-fixture entities).

ROOT CAUSE (diagnosed 2026-07-15, see the plan's Progress Log): the same
structural bug the footystats/open_meteo/SFI fix (instruments-service@
ed3e75b8, 2026-07-14) closed for those three sources also existed for
api_football:

  * INJURIES — ``_fetch_injuries``'s top-level ``except Exception`` handler
    in ``instruments_service/engine/orchestrator/sports_reference_core.py``
    called ``hooks.note_failed("INJURIES", exc)`` with NO ``league_id`` —
    an ACTIVE bug (``get_injuries(date)`` is a single date-wide call; any
    top-level exception, e.g. a genuine ``ApiFootballResponseError`` quota/
    plan error, wrote ONE blank-``league_id`` date-aggregate row). Fixed
    2026-07-15 to loop the expected-leagues denominator and call
    ``hooks.note_failed(..., league_id=...)`` per league (mirrors the
    footystats fix).
  * PLAYER_STATS / FIXTURE_STATS / FIXTURE_EVENTS / FIXTURE_LINEUPS —
    ``_write_per_fixture_entities`` in
    ``instruments_service/engine/orchestrator/sports_reference_fixtures.py``
    had TWO blank-``league_id`` write paths: (a) the "entity produced zero
    rows AND at least one fixture call raised" branch (CF-11), and (b) the
    "rows fetched but couldn't be mapped to any league" (``LEAGUE_MAP_
    INCOMPLETE``) bare-path fallback. Branch (a) fixed 2026-07-15 to write
    per-affected-league (derived from ``af_fid_to_league``, the exact
    per-date fixture→league denominator this entity's fetch was attempted
    against). Branch (b) is left as-is (genuinely unmappable rows — there
    is no league to attribute them to) but its historical rows are IN
    SCOPE for this script's reconciliation (same "can never be superseded"
    fate regardless of origin).

Because a blank-``league_id`` row_key can never be produced by any CURRENT
success path (``record_captured`` / ``emit_empty_gaps_for_entity`` always key
on a real canonical ``league_id``), every row already sitting at one of these
blank keys is permanently orphaned — the residual-closer's own
``_live_failed()`` re-drives the (date, entity) forever, burning API quota,
even after every real per-league cell for that date is genuinely (re-)
captured.

FIX: this script does not re-fetch anything (that is the residual-closer's
job — run it BEFORE this script so real per-league data is captured first
where possible). For each blank-``league_id`` ``attempted_failed`` row in the
target set, it checks whether REAL per-league data (any ``captured`` or
``empty_confirmed`` row with a non-blank ``league_id``) already exists for
the same ``(source, data_type, date)`` cell — i.e. at least one genuine
per-league outcome is on record for that date, meaning the date-wide/entity
fetch DID run cleanly at least once since the blank row was written. Only
THEN does it retire the orphan via the normal
``ManifestWriter.record_expected_empty`` API at the EXACT same blank row_key,
using the closed-set reason ``EmptyConfirmedReason.
EXPECTED_REFDATA_CADENCE_CHANGE`` ("shard cadence/granularity changed;
pre-migration shards are honest absence under the new cadence") — the same
reason the footystats/open_meteo/SFI reconciliation used. Dates with NO
per-league coverage yet are left untouched (still genuinely unresolved, not
force-closed).

This is NOT hiding a failure: it never claims data was captured at the
blank-key row_key (the real per-league captures already carry that claim
correctly under their own real ``league_id`` keys) — it only retires the
obsolete blank-key shard shape once real coverage is confirmed present.

Usage:
  api_football_blank_league_orphan_reconcile_2026_07_15.py [--dry-run] [--vm-name <tag>]
Env it sets: DEPLOYMENT_ENV=prod, MANIFEST_PER_VM_SHARDS=true, VM_NAME=<tag>.
Writes to REAL prod GCS (instruments-store-sports-prd-<project>) unless --dry-run.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout, force=True)
log = logging.getLogger("api_football_blank_league_orphan_reconcile")


def _args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--project", default="central-element-323112")
    p.add_argument("--vm-name", default="api-football-blank-league-orphan-reconcile-2026-07-15")
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Scan + report the orphaned rows found, but do not write anything.",
    )
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

BUCKET = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports", deployment_env="prod")
SOURCE = "api_football"

# Closed set — the api_football data_types root-caused 2026-07-15 (INJURIES +
# the 4 per-fixture entities). A wider live sweep also found small residuals
# for understat/XG, odds_api/ODDS, mdps_odds_horizon_bucket — deliberately OUT
# of this script's blast radius (different source, separate follow-up).
_TARGET_DATA_TYPES: frozenset[str] = frozenset(
    {"INJURIES", "PLAYER_STATS", "FIXTURE_STATS", "FIXTURE_EVENTS", "FIXTURE_LINEUPS"}
)


def _live_index() -> pd.DataFrame:
    """Live (uncached) read of the full sports availability index."""
    _mw._INDEX_CACHE.clear()
    return read_availability_index(BUCKET)


def find_orphans(df: pd.DataFrame) -> pd.DataFrame:
    """Blank-``league_id`` ``attempted_failed`` rows in the target set."""
    src = df.get("source", pd.Series("", index=df.index)).astype("string").fillna("")
    dt = df["data_type"].astype("string").fillna("").str.upper()
    is_failed = df["capture_status"] == "attempted_failed"
    is_blank_league = df["league_id"].isna()
    target_mask = (src == SOURCE) & dt.isin(_TARGET_DATA_TYPES)
    return df[is_failed & is_blank_league & target_mask].copy()


def find_real_coverage_dates(df: pd.DataFrame) -> dict[str, set[str]]:
    """Per data_type, the set of dates that already have REAL per-league
    coverage (a ``captured`` or ``empty_confirmed`` row with a real,
    non-blank ``league_id``) — the "safe to retire the blank orphan" signal.
    """
    src = df.get("source", pd.Series("", index=df.index)).astype("string").fillna("")
    dt = df["data_type"].astype("string").fillna("").str.upper()
    is_real_status = df["capture_status"].isin(["captured", "empty_confirmed"])
    has_league = df["league_id"].notna()
    target_mask = (src == SOURCE) & dt.isin(_TARGET_DATA_TYPES)
    covered = df[is_real_status & has_league & target_mask]
    out: dict[str, set[str]] = {dtv: set() for dtv in _TARGET_DATA_TYPES}
    for dtv, grp in covered.groupby(covered["data_type"].astype(str).str.upper()):
        if dtv in out:
            out[dtv] = set(grp["date"].astype(str).unique())
    return out


def main() -> int:
    df = _live_index()
    orphans = find_orphans(df)
    real_coverage = find_real_coverage_dates(df)

    log.info("Found %d blank-league_id attempted_failed rows in target set", len(orphans))
    if orphans.empty:
        log.info("Nothing to reconcile.")
        return 0

    dt_upper = orphans["data_type"].astype("string").str.upper()
    for dt_val, grp in orphans.groupby(dt_upper):
        dates = sorted(grp["date"].astype(str).unique())
        _covered = real_coverage.get(dt_val, set())
        _retirable = sum(1 for d in dates if d in _covered)
        log.info(
            "  data_type=%s: %d dates (%s .. %s), %d/%d have real per-league coverage now",
            dt_val,
            len(dates),
            dates[0],
            dates[-1],
            _retirable,
            len(dates),
        )

    if ARGS.dry_run:
        log.info("DRY RUN — no writes performed. Re-run without --dry-run to apply.")
        return 0

    manifest = ManifestWriter(service_name="instruments-service", catalogue_bucket=BUCKET)
    n_written = 0
    n_skipped_no_coverage = 0
    for _, row in orphans.iterrows():
        data_type = str(row["data_type"]).upper()
        date = str(row["date"])
        if date not in real_coverage.get(data_type, set()):
            # No real per-league outcome recorded for this date yet — the
            # gap is still genuinely unresolved; leave the orphan in place
            # rather than force-closing a date that was never actually
            # re-captured (residual-closer re-attempts are still the right
            # remediation for these).
            n_skipped_no_coverage += 1
            continue
        manifest.record_expected_empty(
            row_key={"date": date, "data_type": data_type},
            reason=EmptyConfirmedReason.EXPECTED_REFDATA_CADENCE_CHANGE,
            pipeline_mode=PipelineMode.BATCH_API_FOOTBALL,
            source=SOURCE,
        )
        n_written += 1
    manifest.write()
    log.info(
        "Reconciled %d orphaned rows (real per-league coverage confirmed); %d left untouched "
        "(no real per-league coverage yet — genuinely still open); manifest flushed.",
        n_written,
        n_skipped_no_coverage,
    )

    remaining = find_orphans(_live_index())
    log.info("=== POST-RECONCILE VERIFY: %d blank-league_id attempted_failed rows remain ===", len(remaining))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
