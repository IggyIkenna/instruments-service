# Epic: sports_master
# Lifecycle: ONE-OFF — closes the "blank-dt-461" residual root-caused in
#   plans/active/sports_data_sources_canonical_completion_2026_07_13.md
#   (2026-07-15 addendum) and tracked as finding A's blank-data_type component in
#   plans/active/issues/api_football_reverify_attempted_failed_and_asset_group_2026_07_14.md.
# Delete-when: manifest shows 0 blank-``data_type`` attempted_failed rows with
#   source=api_football and venue in {FOOTYSTATS, OPEN_METEO,
#   SOCCER_FOOTBALL_INFO, TRANSFERMARKT, UNDERSTAT}.
# SSOT: plans/active/sports_data_sources_canonical_completion_2026_07_13.md
"""api_football_blank_dt_venue_orphan_reconcile_2026_07_15.py — closes the
461 ``attempted_failed`` rows that carry a BLANK ``data_type``/``league_id``
but a populated ``venue`` naming a T1 sports source, all stamped
``source=api_football``.

ROOT CAUSE (diagnosed 2026-07-15): these rows come from
``instruments_service/engine/orchestrator/process_completeness.py``'s
whole-date shard-completeness check. When a venue is missing after retries,
it writes ``record_failed(row_key={"date": date, "venue": venue}, ...)`` — a
row_key with NO ``data_type``/``league_id``. The 2026-07-13 fix
(``api_football_write_path_blank_data_type_2026_07_13``) remapped this to
``{"date": date, "data_type": "FIXTURES"}`` for the ``API_FOOTBALL`` venue
specifically (since API_FOOTBALL's manifest cell is keyed by data_type, not
venue), but never generalised the fix to the other 5 T1 sports venues
(FOOTYSTATS / OPEN_METEO / SOCCER_FOOTBALL_INFO / TRANSFERMARKT / UNDERSTAT),
which have the identical per-league/per-entity keying and the identical
permanent-orphan problem: no current write path can ever again target a
blank-``data_type`` row_key, since every real per-source success path
(``record_captured``/``record_empty``) keys on a real ``data_type``.

All 461 orphans' ``attempted_at`` timestamps cluster in a single ~36-hour
window (2026-06-25/26) — a one-time historical completeness sweep, not a
live/recurring bug — and the underlying trigger (footystats/transfermarkt/
soccer_football_info had NO scheduled driver at all until this session's
Terraform fix) has since been addressed. Live-verified 2026-07-15: of the 92
orphaned dates per venue, real per-source coverage now exists for 74/92
(footystats/open_meteo/soccer_football_info/transfermarkt) or 90/92
(understat) — the remaining 18 (2 for understat) are genuinely still-open
gaps and are deliberately left untouched by this script.

FIX: mirrors ``sports_blank_league_orphan_reconcile_2026_07_14.py`` and
``api_football_blank_league_orphan_reconcile_2026_07_15.py`` exactly — no
re-fetch, writes ONE terminal reconciliation row at the EXACT same orphaned
row_key (``{"date": date, "venue": venue}``, ``source=api_football``, blank
``data_type``/``league_id``) via ``ManifestWriter.record_expected_empty``
with ``EmptyConfirmedReason.EXPECTED_REFDATA_CADENCE_CHANGE``, ONLY when a
real per-source (captured|empty_confirmed) row already exists for that exact
(source, date). This is NOT hiding a failure: it does not claim the T1
source's own data was captured under THIS row_key (that claim already exists
honestly under the source's own real per-league/per-entity keys) — it marks
the obsolete blank-key shard shape as no-longer-applicable, matching the
already-established convention for this class of migration debt.

Usage:
  api_football_blank_dt_venue_orphan_reconcile_2026_07_15.py [--dry-run] [--vm-name <tag>]
Env it sets: DEPLOYMENT_ENV=prod, MANIFEST_PER_VM_SHARDS=true, VM_NAME=<tag>.
Writes to REAL prod GCS (instruments-store-sports-prd-<project>) unless --dry-run.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout, force=True)
log = logging.getLogger("api_football_blank_dt_venue_orphan_reconcile")


def _args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--project", default="central-element-323112")
    p.add_argument("--vm-name", default="api-football-blank-dt-venue-orphan-reconcile-2026-07-15")
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
from unified_trading_library import ManifestWriter, resolve_bucket_name

BUCKET = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports", deployment_env="prod")

# venue (as stamped in the orphan row) -> the real source key to check for
# genuine coverage before retiring the orphan.
_VENUE_TO_SOURCE: dict[str, str] = {
    "FOOTYSTATS": "footystats",
    "OPEN_METEO": "open_meteo",
    "SOCCER_FOOTBALL_INFO": "soccer_football_info",
    "TRANSFERMARKT": "transfermarkt",
    "UNDERSTAT": "understat",
}


def find_orphans(df: pd.DataFrame) -> pd.DataFrame:
    """Blank-data_type attempted_failed rows, source=api_football, venue in the target set."""
    src = df.get("source", pd.Series("", index=df.index)).astype("string").fillna("")
    dt = df["data_type"].astype("string").fillna("")
    venue = df.get("venue", pd.Series("", index=df.index)).astype("string").fillna("")
    is_failed = df["capture_status"] == "attempted_failed"
    is_blank_dt = dt == ""
    is_target_venue = venue.isin(_VENUE_TO_SOURCE.keys())
    return df[is_failed & is_blank_dt & (src == "api_football") & is_target_venue].copy()


def real_coverage_dates(df: pd.DataFrame, source: str) -> set[str]:
    """Dates where the real source has a captured/empty_confirmed row (any row)."""
    sub = df[df["source"] == source]
    covered = sub[sub["capture_status"].isin(["captured", "empty_confirmed"])]
    return set(covered["date"].astype(str))


def _read_canonical_direct() -> pd.DataFrame:
    """Read the canonical consolidated blob directly via gcsfs, bypassing
    ``read_availability_index()``'s consolidator-freshness guard.

    This reconciliation targets rows from a historical (2026-06-25/26) sweep
    that has long since been folded into the canonical index by the
    consolidator — it does not need the "reflects the last few seconds of
    writes" guarantee the staleness guard protects, and the per-VM-shard
    fallback merge that guard's escape hatch performs only sees RECENT
    unconsolidated shards, missing this historical data entirely (confirmed
    live: it returned 0 orphan rows). A direct read of the current canonical
    blob is accurate for this task regardless of "staleness" from the
    consolidator's perspective.
    """
    import io

    import gcsfs

    fs = gcsfs.GCSFileSystem()
    with fs.open(f"{BUCKET}/_index/availability_index.parquet", "rb") as f:
        data = f.read()
    return pd.read_parquet(io.BytesIO(data))


def main() -> int:
    _mw._INDEX_CACHE.clear()  # bust the 60s read cache — always read live
    df = _read_canonical_direct()

    orphans = find_orphans(df)
    log.info("Found %d blank-data_type/venue attempted_failed orphan rows", len(orphans))
    if orphans.empty:
        log.info("Nothing to reconcile.")
        return 0

    for venue, grp in orphans.groupby("venue"):
        dates = sorted(grp["date"].astype(str).unique())
        log.info("  venue=%s: %d dates (%s .. %s)", venue, len(dates), dates[0], dates[-1])

    reconcilable: list[tuple[str, str]] = []  # (date, venue)
    still_open: list[tuple[str, str]] = []
    for venue, grp in orphans.groupby("venue"):
        source = _VENUE_TO_SOURCE[str(venue)]
        covered_dates = real_coverage_dates(df, source)
        for date in sorted(grp["date"].astype(str).unique()):
            if date in covered_dates:
                reconcilable.append((date, str(venue)))
            else:
                still_open.append((date, str(venue)))

    log.info(
        "Reconcilable (real per-source coverage exists): %d. Still genuinely open (left untouched): %d",
        len(reconcilable),
        len(still_open),
    )
    if still_open:
        by_venue: dict[str, list[str]] = {}
        for date, venue in still_open:
            by_venue.setdefault(venue, []).append(date)
        for venue, dates in sorted(by_venue.items()):
            log.info("  still open: venue=%s dates=%s", venue, sorted(dates))

    if ARGS.dry_run:
        log.info("DRY RUN — no writes performed. Re-run without --dry-run to apply.")
        return 0

    if not reconcilable:
        log.info("Nothing reconcilable — no writes needed.")
        return 0

    manifest = ManifestWriter(service_name="instruments-service", catalogue_bucket=BUCKET)
    n_written = 0
    for date, venue in reconcilable:
        # source='api_football' (matching the orphan's own identity, so this write
        # supersedes it at read time) requires pipeline_mode=BATCH_API_FOOTBALL, not
        # BATCH_INSTRUMENTS_SERVICE (the writer's own original choice) — the coherence
        # check _assert_source_matches_pipeline_mode requires source ==
        # source_string_for(pipeline_mode). The original orphan rows predate this guard.
        manifest.record_expected_empty(
            row_key={"date": date, "venue": venue},
            reason=EmptyConfirmedReason.EXPECTED_REFDATA_CADENCE_CHANGE,
            pipeline_mode=PipelineMode.BATCH_API_FOOTBALL,
            source="api_football",
        )
        n_written += 1
    manifest.write()
    log.info(
        "Reconciled %d orphaned rows; manifest flushed to the per-VM shard. "
        "The canonical index reflects this only after the consolidator's next merge cycle — "
        "verify separately once it catches up.",
        n_written,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
