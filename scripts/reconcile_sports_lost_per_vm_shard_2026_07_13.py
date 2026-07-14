# Epic: sports_master
# Lifecycle: ONE-OFF — recovers real GCS-captured data whose manifest
#   bookkeeping was lost when a per-VM shard
#   (_index/per_vm/sports-attempted-failed-residual-closer-round3.parquet,
#   10,059 rows, all capture_status=captured, written_at 2026-07-13T21:07:05Z
#   .. 21:18:33Z) was orphaned by a manifest-consolidator SIGKILL race
#   (root-caused + fixed in unified-trading-library@d352fb9e) and pruned
#   before ever being consolidated into the canonical manifest.
# Delete-when: manifest shows 0 residual "real file on disk, no captured
#   row" cells for footystats (PREDICTIONS/ODDS), soccer_football_info
#   (SFI_PROGRESSIVE_STATS), and open_meteo (WEATHER) — i.e. a re-run of
#   this script's dry-run reports zero reconcilable cells.
# SSOT: plans/active/sports_data_sources_canonical_completion_2026_07_13.md
"""reconcile_sports_lost_per_vm_shard_2026_07_13.py — recovers manifest rows
for real GCS parquets that the manifest-consolidator SIGKILL-orphan bug
(2026-07-13) never got to record.

ROOT CAUSE (see the plan above's Progress Log): the round-3 invocation of
``sports_attempted_failed_residual_closer_2026_07_13.py`` wrote real
per-league parquet files to GCS AND called ``manifest.record_captured*``
for each one (write-then-record — confirmed by reading
``instruments_service/engine/orchestrator/{footystats,sfi,weather}.py``:
every ``_gated_sink_write`` call precedes its corresponding
``record_captured``/``record_captured_from_counts`` call). Its per-VM
shard was pruned by the consolidator bug before ever merging into
canonical, so the REAL parquet files survive on GCS but canonical still
shows their (date, data_type, league_id) cell as ``attempted_failed`` or
entirely absent.

This script does NOT re-fetch anything from an upstream API. For each of
the 4 residual data_types it:
  1. Reads the LIVE canonical manifest's currently ``attempted_failed``
     dates.
  2. Lists real blobs under the entity's canonical
     (``pipeline_mode=``-qualified) prefix for each such date, falling back
     to the legacy (no ``pipeline_mode=``) prefix.
  3. Parses each blob's league_id (+ optional ``fetched_at_hour=`` snapshot
     partition for footystats PREDICTIONS/ODDS — the latest snapshot per
     league is used).
  4. Cross-references each discovered (date, league_id) cell against the
     CURRENT canonical manifest: any cell that does not already show
     ``capture_status == "captured"`` is reconciled — the real parquet is
     read and its TRUE row count backfills a ``record_captured_from_counts``
     row (fail-honest: an unreadable file is skipped + logged loudly, never
     a phantom 0 — mirrors ``reconcile_manifest_from_per_league_parquets.py``).
  5. Writes reconciled rows to a per-VM shard (never a direct canonical
     rewrite) — the manifest-consolidator daemon merges per-VM shards into
     canonical on its normal ~60s cadence (mirrors the residual closer +
     ``reconcile_manifest_from_per_league_parquets.py`` pattern).

Usage:
  reconcile_sports_lost_per_vm_shard_2026_07_13.py [--vm-name <tag>] [--dry-run]
Writes to REAL prod GCS (instruments-store-sports-prd-<project>) unless
``--dry-run`` is passed.
"""

from __future__ import annotations

import argparse
import io
import logging
import os
import re
import sys
from datetime import UTC, datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout, force=True)
log = logging.getLogger("reconcile_sports_lost_per_vm_shard")


def _args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--vm-name", default="sports-manifest-reconcile-lost-shard-2026-07-13")
    p.add_argument("--project", default="central-element-323112")
    p.add_argument("--dry-run", action="store_true", help="Report reconcilable cells without writing anything.")
    return p.parse_args()


ARGS = _args()
os.environ["GCP_PROJECT_ID"] = ARGS.project
os.environ["GOOGLE_CLOUD_PROJECT"] = ARGS.project
os.environ["DEPLOYMENT_ENV"] = "prod"
os.environ["MANIFEST_PER_VM_SHARDS"] = "true"
os.environ["VM_NAME"] = ARGS.vm_name
os.environ.pop("CLOUD_MOCK_MODE", None)

import pandas as pd
import unified_trading_library.manifest_writer as _mw
from unified_api_contracts import PipelineMode
from unified_trading_library import (
    ManifestWriter,
    StorageClient,
    get_storage_client,
    read_availability_index,
    resolve_bucket_name,
)

BUCKET = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports", deployment_env="prod")

_LEAGUE_RE = re.compile(r"/league=(?P<lid>[^/]+)/[^/]+\.parquet$")
_HOUR_RE = re.compile(r"/fetched_at_hour=(?P<hour>[^/]+)/")


class _Target:
    """One (source, data_type, entity, pipeline_mode) reconciliation target."""

    def __init__(self, source: str, data_type: str, entity: str, pipeline_mode: PipelineMode) -> None:
        self.source = source
        self.data_type = data_type
        self.entity = entity
        self.pipeline_mode = pipeline_mode


TARGETS = [
    _Target("footystats", "PREDICTIONS", "footystats_predictions", PipelineMode.BATCH_FOOTYSTATS),
    _Target("footystats", "ODDS", "footystats_odds", PipelineMode.BATCH_FOOTYSTATS),
    _Target(
        "soccer_football_info", "SFI_PROGRESSIVE_STATS", "progressive_stats", PipelineMode.BATCH_SOCCER_FOOTBALL_INFO
    ),
    _Target("open_meteo", "WEATHER", "weather", PipelineMode.BATCH_OPEN_METEO),
]


def _attempted_failed_dates(df: pd.DataFrame, source: str, data_type: str) -> list[str]:
    dt = df["data_type"].astype("string").fillna("").str.upper()
    src = df.get("source", pd.Series("", index=df.index)).astype("string").fillna("")
    sub = df[dt.eq(data_type) & (src == source) & (df["capture_status"] == "attempted_failed")]
    return sorted(sub["date"].astype(str).unique())


def _current_status_lookup(df: pd.DataFrame, source: str, data_type: str) -> dict[tuple[str, str], str]:
    dt = df["data_type"].astype("string").fillna("").str.upper()
    src = df.get("source", pd.Series("", index=df.index)).astype("string").fillna("")
    mask = dt.eq(data_type) & (src == source)
    sub = df[mask]
    dates = sub["date"].astype(str)
    leagues = sub.get("league_id", pd.Series("", index=sub.index)).astype("string").fillna("")
    statuses = sub["capture_status"].astype(str)
    return dict(zip(zip(dates, leagues, strict=True), statuses, strict=True))


def _discover_real_cells(client: StorageClient, entity: str, pipeline_mode_value: str, date: str) -> dict[str, str]:
    """Return {league_id: best_blob_path} for real parquets found for one date.

    Probes the canonical (``pipeline_mode=``-qualified) prefix first, then
    the legacy (no ``pipeline_mode=``) prefix. When multiple
    ``fetched_at_hour=`` snapshots exist for the same league, the
    lexicographically-latest hour wins (most recent snapshot).
    """
    canon_prefix = f"sports_reference/by_date/day={date}/pipeline_mode={pipeline_mode_value}/entity={entity}/"
    legacy_prefix = f"sports_reference/by_date/day={date}/entity={entity}/"
    blobs: list[str] = []
    for prefix in (canon_prefix, legacy_prefix):
        try:
            blobs.extend(b.name for b in client.list_blobs(BUCKET, prefix=prefix) if b.name.endswith(".parquet"))
        except Exception as exc:
            log.warning("list_blobs failed for prefix=%s: %s", prefix, exc)

    by_league: dict[str, list[str]] = {}
    for blob_name in blobs:
        m = _LEAGUE_RE.search(blob_name)
        if not m:
            continue  # bare (no-league) file — not the observed shape for these 4 entities
        by_league.setdefault(m.group("lid"), []).append(blob_name)

    def _hour_key(blob_name: str) -> str:
        m = _HOUR_RE.search(blob_name)
        return m.group("hour") if m else ""

    best: dict[str, str] = {}
    for league, candidates in by_league.items():
        candidates.sort(key=_hour_key)
        best[league] = candidates[-1]
    return best


def main() -> int:
    log.info("bucket=%s vm=%s dry_run=%s", BUCKET, ARGS.vm_name, ARGS.dry_run)
    client = get_storage_client()

    manifest = ManifestWriter(service_name="instruments-service", catalogue_bucket=BUCKET)

    grand_reconciled = 0
    grand_already_captured = 0
    grand_read_errors = 0
    grand_no_real_file_dates = 0

    for target in TARGETS:
        _mw._INDEX_CACHE.clear()  # bust the 60s read cache — always read live
        df = read_availability_index(BUCKET)
        dates = _attempted_failed_dates(df, target.source, target.data_type)
        status_lookup = _current_status_lookup(df, target.source, target.data_type)
        log.info(
            "[%s:%s] %d attempted_failed date(s) to probe for real files",
            target.source,
            target.data_type,
            len(dates),
        )

        n_reconciled = 0
        n_already_captured = 0
        n_read_errors = 0
        n_no_real_file = 0

        for date in dates:
            cells = _discover_real_cells(client, target.entity, target.pipeline_mode.value, date)
            if not cells:
                n_no_real_file += 1
                continue
            for league_id, blob_name in sorted(cells.items()):
                cur_status = status_lookup.get((date, league_id))
                if cur_status == "captured":
                    n_already_captured += 1
                    continue
                try:
                    data = client.download_bytes(BUCKET, blob_name)
                    true_count = len(pd.read_parquet(io.BytesIO(data)))
                except Exception as exc:
                    n_read_errors += 1
                    log.warning(
                        "could not read %s (%s/%s/%s) to count real rows: %s — skipping (no phantom 0 written)",
                        blob_name,
                        target.data_type,
                        date,
                        league_id,
                        exc,
                    )
                    continue
                if ARGS.dry_run:
                    log.info(
                        "[DRY-RUN] would reconcile %s/%s/%s league=%s rows=%d (prior_status=%s)",
                        target.source,
                        target.data_type,
                        date,
                        league_id,
                        true_count,
                        cur_status,
                    )
                    n_reconciled += 1
                    continue
                manifest.record_captured_from_counts(
                    row_key={"date": date, "data_type": target.data_type, "league_id": league_id},
                    total_rows=true_count,
                    expected_root_clusters={},
                    observed_clusters={"": true_count},
                    available_at_envelope=pd.Timestamp(datetime.now(UTC)),
                    pipeline_mode=target.pipeline_mode,
                    source=target.source,
                    service_emission_state=None,
                )
                manifest.write()
                n_reconciled += 1
                log.info(
                    "RECONCILED %s/%s/%s league=%s rows=%d (prior_status=%s) blob=%s",
                    target.source,
                    target.data_type,
                    date,
                    league_id,
                    true_count,
                    cur_status,
                    blob_name,
                )

        log.info(
            "[%s:%s] reconciled=%d already_captured=%d read_errors=%d dates_with_no_real_file=%d",
            target.source,
            target.data_type,
            n_reconciled,
            n_already_captured,
            n_read_errors,
            n_no_real_file,
        )
        grand_reconciled += n_reconciled
        grand_already_captured += n_already_captured
        grand_read_errors += n_read_errors
        grand_no_real_file_dates += n_no_real_file

    if not ARGS.dry_run:
        flushed = _mw.flush_all_pending_buckets()
        log.info("EXPLICIT PRE-EXIT DRAIN: %s", flushed)

    log.info(
        "FINAL TALLY: reconciled=%d already_captured=%d read_errors=%d dates_with_no_real_file=%d",
        grand_reconciled,
        grand_already_captured,
        grand_read_errors,
        grand_no_real_file_dates,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
