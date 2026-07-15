# Epic: sports_master
# Lifecycle: ONE-OFF — reconciles the stale CF11_MATCH_DAY_EMPTY_GUARANTEED_TYPE
#   api_football attempted_failed manifest rows to `captured`, where the
#   canonical per-league DATA parquet is ALREADY PRESENT on disk (a
#   manifest-vs-data drift: the v9-rebuild CF-11 gate marked the cell
#   attempted_failed from manifest-emptiness even though the fixture
#   events/lineups parquet exists + api_football has the data). Root-caused
#   in plans/active/sports_data_sources_canonical_completion_2026_07_13.md
#   (task -023) + issue doc
#   api_football_cf11_record_captured_noop_manifest_vs_data_drift_2026_07_15.md.
# Delete-when: 0 attempted_failed rows with
#   error_reason=CF11_MATCH_DAY_EMPTY_GUARANTEED_TYPE for source=api_football.
# SSOT: plans/active/sports_data_sources_canonical_completion_2026_07_13.md
"""Reconcile CF11 api_football attempted_failed cells to captured from the
PRESENT per-league parquet.

The prior closers left these attempted_failed because they never called
``ManifestWriter.write()`` after ``record_captured`` staged the row on the
writer instance (``self._records``); ``flush_all_pending_buckets()`` drains the
bucket-level pending, NOT a live writer's un-written ``_records``. This script
does the missing step: for each stuck CF11 cell whose per-league parquet EXISTS
(data-backed, no provider re-fetch needed), it reads that parquet, calls
``record_captured`` with it, and calls ``write()`` — flipping the cell to
``captured`` truthfully. Cells with NO present parquet are SKIPPED + reported
(never fake-stamped).

Reads the sports availability index with a generous
``MANIFEST_CONSOLIDATED_STALENESS_SEC`` so the ~11-min-cadence sports
consolidator's healthy consolidated blob is served (see sibling finding
sports_manifest_read_staleness_budget_missing_2026_07_15.md).

Usage: api_football_cf11_manifest_reconcile_2026_07_15.py [--vm-name <tag>] [--dry-run]
Writes to REAL prod GCS (instruments-store-sports-prd-<project>).
"""

from __future__ import annotations

import argparse
import io
import logging
import os
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout, force=True)
log = logging.getLogger("api_football_cf11_manifest_reconcile")

CF11 = "CF11_MATCH_DAY_EMPTY_GUARANTEED_TYPE"
CF11_DATA_TYPES = ("FIXTURE_STATS", "FIXTURE_EVENTS", "FIXTURE_LINEUPS")
_DT_TO_ENTITY = {
    "FIXTURE_STATS": "fixture_stats",
    "FIXTURE_EVENTS": "fixture_events",
    "FIXTURE_LINEUPS": "fixture_lineups",
}


def _args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--vm-name", default="api-football-cf11-manifest-reconcile")
    p.add_argument("--project", default="central-element-323112")
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args()


ARGS = _args()
os.environ["GCP_PROJECT_ID"] = ARGS.project
os.environ["GOOGLE_CLOUD_PROJECT"] = ARGS.project
os.environ["DEPLOYMENT_ENV"] = "prod"
os.environ["MANIFEST_PER_VM_SHARDS"] = "true"
os.environ["VM_NAME"] = ARGS.vm_name
os.environ["MANIFEST_CONSOLIDATED_STALENESS_SEC"] = "3600"
os.environ.pop("CLOUD_MOCK_MODE", None)

from unified_trading_library import setup_events

setup_events("instruments-service", "local")

import pandas as pd
import unified_trading_library.manifest_writer as _mw
from unified_trading_library import (
    ManifestWriter,
    get_storage_client,
    read_availability_index,
    resolve_bucket_name,
)

from instruments_service.engine import orchestrator as o

BUCKET = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports", deployment_env="prod")
_CLIENT = get_storage_client()


def _live_cf11() -> pd.DataFrame:
    _mw._INDEX_CACHE.clear()
    _mw._CANONICAL_CACHE.clear()
    df = read_availability_index(BUCKET)
    af = df[(df.get("source", "") == "api_football") & (df["capture_status"] == "attempted_failed")].copy()
    af["error_reason"] = af["error_reason"].fillna("").astype(str)
    af["data_type"] = af["data_type"].fillna("").astype(str)
    af["league_id"] = af.get("league_id", "").fillna("").astype(str)
    return af[af["error_reason"].str.contains(CF11) & af["data_type"].isin(CF11_DATA_TYPES)].copy()


def _read_present_parquet(date: str, entity: str, league: str) -> pd.DataFrame | None:
    """Read the canonical (then legacy) per-league parquet, or None if absent."""
    pm = o._sports_ref_pm(entity)
    for prefix in (
        f"sports_reference/by_date/day={date}/pipeline_mode={pm}/entity={entity}/league={league}/",
        f"sports_reference/by_date/day={date}/entity={entity}/league={league}/",
    ):
        names: list[str] = []
        for b in _CLIENT.list_blobs(BUCKET, prefix=prefix):
            name = b if isinstance(b, str) else getattr(b, "name", None)
            if isinstance(name, str) and name.endswith(".parquet"):
                names.append(name)
        if names:
            return pd.read_parquet(io.BytesIO(_CLIENT.download_bytes(BUCKET, names[0])))
    return None


def main() -> None:
    cf11 = _live_cf11()
    log.info("LIVE CF11 api_football attempted_failed cells: %d", len(cf11))
    if cf11.empty:
        log.info("=== nothing to reconcile — 0 CF11 cells ===")
        return

    mw = ManifestWriter(service_name="instruments-service", catalogue_bucket=BUCKET)
    reconciled = 0
    skipped_no_parquet: list[str] = []
    for _, row in cf11.sort_values(["date", "league_id", "data_type"]).iterrows():
        date, league, dt = str(row["date"]), str(row["league_id"]), str(row["data_type"])
        entity = _DT_TO_ENTITY[dt]
        pdf = _read_present_parquet(date, entity, league)
        if pdf is None or pdf.empty:
            skipped_no_parquet.append(f"{date}/{league}/{dt}")
            log.warning("SKIP (no present parquet — NOT stamping): %s / %s / %s", date, league, dt)
            continue
        log.info("reconcile %s / %s / %s -> captured (%d parquet rows)", date, league, dt, len(pdf))
        if ARGS.dry_run:
            reconciled += 1
            continue
        # Data-backed reconciliation: the per-league parquet is present on disk, so this is a
        # truthful captured record (not a phantom), just re-stamping a manifest row the prior
        # closers staged but never persisted (they omitted ManifestWriter.write()).
        mw.record_captured(
            row_key={"date": date, "data_type": dt, "league_id": league},
            df=pdf,
            asset_group="sports",
            instrument_type="",
            data_type=dt,
            league_id=league,
            pipeline_mode=o.PipelineMode.BATCH_API_FOOTBALL,
            source="api_football",
            service_emission_state=None,
        )
        reconciled += 1

    if ARGS.dry_run:
        log.info("DRY-RUN: would reconcile %d cells; %d skipped (no parquet)", reconciled, len(skipped_no_parquet))
        return

    mw.write()  # THE step the prior closers omitted — persist the staged captured rows.
    _mw.flush_all_pending_buckets()
    log.info(
        "WROTE %d captured rows; skipped(no-parquet)=%d %s", reconciled, len(skipped_no_parquet), skipped_no_parquet
    )

    remaining = _live_cf11()
    log.info("=== FINAL CF11 api_football attempted_failed remaining: %d ===", len(remaining))
    if not remaining.empty:
        log.info("remaining:\n%s", remaining[["date", "league_id", "data_type"]].to_string(index=False))


if __name__ == "__main__":
    main()
