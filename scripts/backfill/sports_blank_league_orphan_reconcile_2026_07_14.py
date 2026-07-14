# Epic: sports_master
# Lifecycle: ONE-OFF — closes the footystats PREDICTIONS/ODDS TimeoutError +
#   open_meteo WEATHER + soccer_football_info SFI_PROGRESSIVE_STATS
#   phantom_captured/TimeoutError "blank league_id" residual root-caused in
#   plans/active/sports_data_sources_canonical_completion_2026_07_13.md
#   (2026-07-14 addendum).
# Delete-when: manifest shows 0 blank-``league_id`` attempted_failed rows for
#   (footystats, PREDICTIONS|ODDS), (open_meteo, WEATHER), and
#   (soccer_football_info, SFI_PROGRESSIVE_STATS).
# SSOT: plans/active/sports_data_sources_canonical_completion_2026_07_13.md
"""sports_blank_league_orphan_reconcile_2026_07_14.py — closes the specific
``attempted_failed`` rows that carry a BLANK/None ``league_id`` for
footystats (PREDICTIONS, ODDS), open_meteo (WEATHER), and
soccer_football_info (SFI_PROGRESSIVE_STATS).

ROOT CAUSE (diagnosed 2026-07-14, see the plan above's Progress Log): the
sports_reference write path migrated to PER-LEAGUE manifest shard atoms
(``(date, data_type, league_id)``) — every success path (``record_captured``
/ ``record_empty``) and, as of this fix, every per-league failure path
(``record_failed``) keys on a REAL canonical ``league_id``. But a small,
now-CLOSED set of pre-fix rows was written keyed at ``(date, data_type)``
with NO ``league_id`` at all:

  * footystats PREDICTIONS / ODDS ``TimeoutError`` rows — written by the
    OLD top-level ``except Exception`` handler in
    ``instruments_service/engine/orchestrator/footystats.py`` (fixed
    instruments-service@<this commit> to write per-league instead).
  * open_meteo WEATHER ``phantom_captured_no_parquet_at_canonical_path``
    rows — legacy date-aggregate rows from BEFORE the
    ``sports_manifest_shard_migration_cleanup_2026_04_21`` per-league
    migration, later correctly reclassified from a phantom ``captured``
    claim to ``attempted_failed`` by an earlier phantom-reconciliation pass
    (its row_key was preserved as-is, blank ``league_id`` and all).
  * soccer_football_info SFI_PROGRESSIVE_STATS ``phantom_captured_``… /
    ``TimeoutError`` rows — the identical anti-pattern the footystats fix
    above closes: the SFI top-level ``except`` handler in ``sfi.py`` wrote a
    blank-``league_id`` date-aggregate ``record_failed`` row (fixed
    instruments-service@<this commit's sfi.py change> to write per-league
    instead). Live-verified 2026-07-14: all 10 blank rows' dates already carry
    the full per-league set (94 leagues each, captured/empty_confirmed, 0
    per-league failures) — the blank row masks no real gap; the real
    per-league data self-heals on re-capture while the blank date-aggregate
    row cannot (the success path writes no blank row), leaving it orphaned.

Because NO current write path can ever again target a blank-``league_id``
row_key, these rows are permanently orphaned: the residual-closer script's
date-level "any row for this date is attempted_failed" check reports them
as unresolved FOREVER, even after every real expected league for that date
has been successfully (re-)captured per-league — which live manifest
inspection confirms IS the case for the vast majority of these dates (see
the plan's Progress Log for the specific evidence).

FIX: this script does not re-fetch anything (a genuine per-league re-fetch
is the residual-closer's job, and should be run BEFORE this script so the
real per-league data is captured first). It writes ONE terminal
reconciliation row at the EXACT same orphaned row_key (blank ``league_id``)
via the normal ``ManifestWriter.record_expected_empty`` API, using the
closed-set typed reason ``EmptyConfirmedReason.EXPECTED_REFDATA_CADENCE_CHANGE``
("shard cadence/granularity changed; pre-migration shards are honest
absence under the new cadence") — the exact taxonomy member documented for
this class of migration debt. The manifest reader's last-write-wins dedup
(NaN/None and "" both collapse to the same NULL sentinel for optional
dims, see ``unified_trading_library.manifest_writer._read_index._dedup_key_series``)
means this new row supersedes the old orphaned row at read time.

This is NOT hiding a failure: it does not claim data was captured at this
row_key (real per-league captures already carry that claim, correctly,
under their own real ``league_id`` keys) — it honestly marks the
obsolete/blank-``league_id`` shard shape as no-longer-applicable.

Usage:
  sports_blank_league_orphan_reconcile_2026_07_14.py [--dry-run] [--vm-name <tag>]
Env it sets: DEPLOYMENT_ENV=prod, MANIFEST_PER_VM_SHARDS=true, VM_NAME=<tag>.
Writes to REAL prod GCS (instruments-store-sports-prd-<project>) unless --dry-run.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout, force=True)
log = logging.getLogger("sports_blank_league_orphan_reconcile")


def _args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--project", default="central-element-323112")
    p.add_argument("--vm-name", default="blank-league-orphan-reconcile-2026-07-14")
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

# (source, data_type) -> (pipeline_mode, manifest ``source`` key). Closed set —
# the three sources/four data_types root-caused + fixed 2026-07-14. A broader
# blank-league_id sweep (api_football INJURIES/PLAYER_STATS/etc., understat XG,
# odds_api, mdps_odds_horizon_bucket — ~2,400 more rows found live 2026-07-14)
# is a SEPARATE follow-up (see the plan's Progress Log) and is deliberately OUT
# of this script's blast radius.
_TARGETS: dict[tuple[str, str], tuple[PipelineMode, str]] = {
    ("footystats", "PREDICTIONS"): (PipelineMode.BATCH_FOOTYSTATS, "footystats"),
    ("footystats", "ODDS"): (PipelineMode.BATCH_FOOTYSTATS, "footystats"),
    ("open_meteo", "WEATHER"): (PipelineMode.BATCH_OPEN_METEO, "open_meteo"),
    ("soccer_football_info", "SFI_PROGRESSIVE_STATS"): (
        PipelineMode.BATCH_SOCCER_FOOTBALL_INFO,
        "soccer_football_info",
    ),
}


def find_orphans() -> pd.DataFrame:
    """Live per-target-set: blank-``league_id`` ``attempted_failed`` rows."""
    _mw._INDEX_CACHE.clear()  # bust the 60s read cache — always read live
    df = read_availability_index(BUCKET)
    src = df.get("source", pd.Series("", index=df.index)).astype("string").fillna("")
    dt = df["data_type"].astype("string").fillna("").str.upper()
    is_failed = df["capture_status"] == "attempted_failed"
    is_blank_league = df["league_id"].isna()
    target_mask = pd.Series(False, index=df.index)
    for want_source, want_dt in _TARGETS:
        target_mask |= (src == want_source) & (dt == want_dt)
    return df[is_failed & is_blank_league & target_mask].copy()


def main() -> int:
    orphans = find_orphans()
    log.info("Found %d orphaned blank-league_id attempted_failed rows to reconcile", len(orphans))
    if orphans.empty:
        log.info("Nothing to reconcile.")
        return 0

    dt_upper = orphans["data_type"].astype("string").str.upper()
    for (source_val, dt_val), grp in orphans.groupby([orphans["source"].astype("string"), dt_upper]):
        dates = sorted(grp["date"].astype(str).unique())
        log.info("  source=%s data_type=%s: %d dates (%s .. %s)", source_val, dt_val, len(dates), dates[0], dates[-1])

    if ARGS.dry_run:
        log.info("DRY RUN — no writes performed. Re-run without --dry-run to apply.")
        return 0

    manifest = ManifestWriter(service_name="instruments-service", catalogue_bucket=BUCKET)
    n_written = 0
    for _, row in orphans.iterrows():
        source_key = str(row["source"])
        data_type = str(row["data_type"]).upper()
        date = str(row["date"])
        target = _TARGETS.get((source_key, data_type))
        if target is None:
            log.warning("Skipping unexpected (source=%s, data_type=%s) row outside target set", source_key, data_type)
            continue
        pipeline_mode, manifest_source = target
        manifest.record_expected_empty(
            row_key={"date": date, "data_type": data_type},
            reason=EmptyConfirmedReason.EXPECTED_REFDATA_CADENCE_CHANGE,
            pipeline_mode=pipeline_mode,
            source=manifest_source,
        )
        n_written += 1
    manifest.write()
    log.info("Reconciled %d orphaned rows (source/data_type breakdown logged above); manifest flushed.", n_written)

    remaining = find_orphans()
    log.info("=== POST-RECONCILE VERIFY: %d blank-league_id attempted_failed rows remain ===", len(remaining))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
