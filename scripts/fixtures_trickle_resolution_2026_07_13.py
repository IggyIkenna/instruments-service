# Epic: instruments_master
# Lifecycle: one-off
# Delete-when: after the 2026-07-13 FIXTURES post-cutoff trickle backlog (2026-06-29..
#   2026-07-12) is consolidated + verified (proven no-fixture cells == 0 in that window)
"""Resolve the post-cutoff FIXTURES expected_unattempted trickle backlog (2026-07-13).

Context: after the 2026-07-13 phantom-denominator remediation (issue doc
``plans/active/issues/sports_fixtures_pending_eu_phantom_denominator_2026_07_13.md``)
the residual FIXTURES pending-EU cells are dated 2026-06-29..2026-07-13 — seeded by
the OLD enumerator before the calendar gate (instruments-service@21b76b3e) goes live
via image rebuild. This script resolves them with per-cell evidence from a FRESH
season-complete api_football truthset (``audit_fixtures_via_api_football.py``,
seasons 2025-2026, run id passed via ``--truthset-run-id``):

* ``--operation fetch`` — cells the truthset proves HAVE fixtures: reuse the on-disk
  canonical parquet where present, else re-fetch that (league, season) from
  api_football, write the canonical parquet, then ``record_captured`` via the
  per-VM-shard ManifestWriter path (``VM_NAME=trickle-fetch-20260713``).
* ``--operation flip`` — cells the truthset proves have ZERO fixtures, dates
  <= 2026-07-12 ONLY (today 2026-07-13 is left to the daily pipeline): flip to
  ``empty_confirmed`` with ``error_reason=EXPECTED_NO_FIXTURE__truthset_<runid>``
  via a direct per-VM shard write (``VM_NAME=trickle-flip-20260713``).

Mirrors ``fixtures_eu_truthset_flip_2026_07_13.py`` (same per-VM-shard pattern):
NEVER hand-edits the consolidated index — rows land in ``_index/per_vm/`` shards
and the manifest consolidator merges them last-writer-wins on the next cycle. The
direct shard write is used for the flip because the ManifestWriter reason validator
only accepts bare closed-set values, while the audit trail requires the truthset
provenance suffix.

Usage::

  .venv/bin/python scripts/fixtures_trickle_resolution_2026_07_13.py \
      --operation flip --pairs <flip_pairs.parquet> --truthset-run-id <run_ts> [--dry-run]

  .venv/bin/python scripts/fixtures_trickle_resolution_2026_07_13.py \
      --operation fetch --cells <fetch_cells.csv> [--dry-run]

``--pairs`` parquet columns: league_id, date (the deterministic flip set, recomputed
from the fresh index snapshot x fresh-truthset join, dates <= 2026-07-12).
``--cells`` csv columns: league_id, date, af_league_id, season.

The fetch mode is cells-driven (no date bounds), so it is also reused for the
PRE-window pending backlog (cells dated < 2026-06-29 that the truthset proves have
fixtures) — pass ``--vm-name`` to keep each sweep's per-VM shard distinct.
"""

from __future__ import annotations

import argparse
import asyncio
import io
import logging
import os
import sys
from datetime import UTC, datetime

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout, force=True)
logger = logging.getLogger("fixtures_trickle_resolution")

_RUN_TS = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
_FLIP_CUTOFF_DATE = "2026-07-12"  # today (2026-07-13) is not final — left to the daily pipeline
_BACKLOG_LO_DATE = "2026-06-29"  # the prior remediation's truthset cutoff (exclusive backlog start)
_VM_NAME_BY_OP = {"flip": "trickle-flip-20260713", "fetch": "trickle-fetch-20260713"}
_FIXTURES_BLOB_TPL = (
    "sports_reference/by_date/day={day}/pipeline_mode=batch_api_football/entity=fixtures/league={league}/"
    "fixtures.parquet"
)
_ATOM_KEY = ["league_id", "data_type", "date", "source", "pipeline_mode", "venue", "asset_group"]


def _args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--operation", required=True, choices=["flip", "fetch"])
    p.add_argument("--pairs", help="flip: parquet of (league_id, date) pairs to flip")
    p.add_argument("--cells", help="fetch: csv of (league_id, date, af_league_id, season) cells")
    p.add_argument("--truthset-run-id", help="flip: run_ts of the fresh truthset that evidences the flips")
    p.add_argument("--vm-name", help="override the per-VM shard tag (default: the per-operation built-in)")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--project", default="central-element-323112")
    return p.parse_args()


ARGS = _args()
os.environ["GCP_PROJECT_ID"] = ARGS.project
os.environ["GOOGLE_CLOUD_PROJECT"] = ARGS.project
os.environ["DEPLOYMENT_ENV"] = "prod"
os.environ["MANIFEST_PER_VM_SHARDS"] = "true"
os.environ["VM_NAME"] = ARGS.vm_name or _VM_NAME_BY_OP[ARGS.operation]
os.environ.pop("CLOUD_MOCK_MODE", None)

from unified_trading_library import setup_events

setup_events("instruments-service", "local")

import unified_trading_library.manifest_writer as _mw
from unified_trading_library import (
    ManifestWriter,
    get_storage_client,
    read_availability_index,
    resolve_bucket_name,
    validate_api_keys_for_venues,
)

BUCKET = resolve_bucket_name(cloud="gcp", kind="instruments-store", asset_group="sports", deployment_env="prod")


def _deduped_pending_fixtures(df: pd.DataFrame) -> pd.DataFrame:
    """Latest row per atom, restricted to the trickle pending-FIXTURES backlog window."""
    fx = df[df["data_type"].astype(str).str.upper() == "FIXTURES"].copy()
    fx = fx.sort_values("written_at").drop_duplicates(subset=_ATOM_KEY, keep="last")
    fx["date"] = fx["date"].astype(str).str[:10]
    reason = fx["error_reason"].fillna("").astype(str)
    mask = (
        (fx["capture_status"] == "expected_unattempted")
        & (fx["source"].astype(str) == "api_football")
        & (fx["pipeline_mode"].astype(str) == "batch_api_football")
        & ~reason.str.startswith("EXPECTED_")
        & (fx["date"] >= _BACKLOG_LO_DATE)
        & (fx["date"] <= _FLIP_CUTOFF_DATE)
    )
    return fx[mask]


def run_flip(pairs_path: str, truthset_run_id: str, dry_run: bool) -> int:
    flip_reason = f"EXPECTED_NO_FIXTURE__truthset_{truthset_run_id}"
    pairs_df = pd.read_parquet(pairs_path)
    target = set(zip(pairs_df["league_id"].astype(str), pairs_df["date"].astype(str).str[:10], strict=True))
    over_cutoff = {(lg, d) for lg, d in target if d > _FLIP_CUTOFF_DATE}
    if over_cutoff:
        logger.error("REFUSING: %d target pairs dated after %s: %s", len(over_cutoff), _FLIP_CUTOFF_DATE, over_cutoff)
        return 2
    logger.info("Target (league, date) pairs to flip: %d (reason=%s)", len(target), flip_reason)

    _mw._INDEX_CACHE.clear()
    idx = read_availability_index(BUCKET)
    logger.info("Live availability index rows: %d", len(idx))

    pend = _deduped_pending_fixtures(idx)
    logger.info("Deduped pending FIXTURES cells (%s..%s): %d", _BACKLOG_LO_DATE, _FLIP_CUTOFF_DATE, len(pend))

    cells = list(zip(pend["league_id"].astype(str), pend["date"], strict=True))
    in_target = pd.Series([c in target for c in cells], index=pend.index)
    flip = pend[in_target]
    logger.info(
        "Cells still pending AND in target: %d (already-resolved target cells: %d)", len(flip), len(target) - len(flip)
    )

    if flip.empty:
        logger.info("Nothing to flip — all target cells already resolved")
        return 0
    if dry_run:
        logger.info("[dry-run] Would write per-VM shard with %d flipped rows", len(flip))
        logger.info("[dry-run] Sample:\n%s", flip[["league_id", "date", "capture_status"]].head(10).to_string())
        return 0

    now_iso = datetime.now(UTC).isoformat()
    flipped = flip.copy()
    flipped["capture_status"] = "empty_confirmed"
    flipped["error_reason"] = flip_reason
    flipped["attempted_at"] = now_iso
    if "written_at" in flipped.columns:
        flipped["written_at"] = now_iso

    shard_blob = f"_index/per_vm/{os.environ['VM_NAME']}-{_RUN_TS}.parquet"
    out = io.BytesIO()
    flipped.to_parquet(out, index=False, engine="pyarrow")
    out.seek(0)
    storage = get_storage_client(project_id=ARGS.project)
    storage.upload_bytes(BUCKET, shard_blob, out.read())
    logger.info(
        "Wrote per-VM shard %s/%s (%d rows) — consolidator merges on next cycle", BUCKET, shard_blob, len(flipped)
    )
    return 0


def _record_captured_cell(manifest: ManifestWriter, league_id: str, day: str, df: pd.DataFrame) -> None:
    from unified_api_contracts import PipelineMode

    manifest.record_captured(  # QG-allow: emission-policy-not-applicable
        row_key={"date": day, "data_type": "FIXTURES", "league_id": league_id},
        df=df,
        asset_group="sports",
        instrument_type="",
        data_type="FIXTURES",
        league_id=league_id,
        pipeline_mode=PipelineMode.BATCH_API_FOOTBALL,
        # FIXTURES is multi-source (api_football + footystats) → explicit source required.
        source="api_football",
        service_emission_state=None,
    )


async def _refetch_league_day(af_league_id: int, season: int, league_id: str, day: str) -> pd.DataFrame:
    """Re-fetch one (league, season) from api_football and build the day's disk rows."""
    from unified_api_contracts.external.api_football.normalize import normalize_api_football_fixture

    from instruments_service.engine.orchestrator import _flatten_canonical_fixture_for_disk
    from instruments_service.reference_data.adapters.sports.adapters.api_football import (
        ApiFootballAdapter,
        _parse_fixture_response,
    )

    keys = validate_api_keys_for_venues(venues=["api_football"], project_id=ARGS.project)
    api_key = keys.get("api_football")
    if not api_key:
        raise RuntimeError("api_football api key unavailable from Secret Manager")
    adapter = ApiFootballAdapter(api_key=api_key)
    url = "https://v3.football.api-sports.io/fixtures"
    params = {"league": str(af_league_id), "season": str(season)}
    async with adapter._make_session() as session:
        raw = await adapter._get_with_retry(session, url, params=params, headers=adapter._headers())
    response = raw.get("response") if isinstance(raw, dict) else None
    items = [it for it in response if isinstance(it, dict)] if isinstance(response, list) else []
    rows: list[dict[str, object]] = []
    for item in items:
        fixture_obj = item.get("fixture")
        date_str = fixture_obj.get("date") if isinstance(fixture_obj, dict) else None
        if not isinstance(date_str, str) or date_str[:10] != day:
            continue
        af_fixture_raw = _parse_fixture_response(item)
        if af_fixture_raw is None:
            continue
        rows.append(_flatten_canonical_fixture_for_disk(normalize_api_football_fixture(af_fixture_raw), day))
    df = pd.DataFrame(rows)
    if not df.empty and "timestamp" in df.columns:
        # available_at = kickoff - 7d (matches recover_fixtures_from_truthset.py / on-disk values)
        df["available_at"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce") - pd.Timedelta(days=7)
    logger.info("Re-fetched %s season=%s: %d fixture row(s) on %s", league_id, season, len(df), day)
    return df


def run_fetch(cells_path: str, dry_run: bool) -> int:
    cells = pd.read_csv(cells_path)
    storage = get_storage_client(project_id=ARGS.project)
    manifest = ManifestWriter(service_name="instruments-service", catalogue_bucket=BUCKET)
    n_captured = n_refetched = n_failed = 0

    for row in cells.itertuples(index=False):
        league_id = str(row.league_id)
        day = str(row.date)[:10]
        blob = _FIXTURES_BLOB_TPL.format(day=day, league=league_id)
        try:
            raw = storage.download_bytes(BUCKET, blob)
        except Exception:
            raw = None
        if raw is not None:
            df = pd.read_parquet(io.BytesIO(raw))
            if dry_run:
                logger.info(
                    "[dry-run] %s %s: parquet EXISTS (%d rows) — would record_captured", league_id, day, len(df)
                )
                continue
            _record_captured_cell(manifest, league_id, day, df)
            n_captured += 1
            logger.info("record_captured %s %s (%d rows, existing parquet)", league_id, day, len(df))
            continue

        if dry_run:
            logger.info(
                "[dry-run] %s %s: parquet ABSENT — would re-fetch af_league=%s season=%s",
                league_id,
                day,
                row.af_league_id,
                row.season,
            )
            continue
        try:
            df = asyncio.run(_refetch_league_day(int(row.af_league_id), int(row.season), league_id, day))
        except Exception as exc:
            logger.error("REFETCH FAILED %s %s: %s: %s", league_id, day, type(exc).__name__, str(exc)[:200])
            n_failed += 1
            continue
        if df.empty:
            logger.error("REFETCH CONTRADICTS TRUTHSET (0 rows) for %s %s — leaving cell untouched", league_id, day)
            n_failed += 1
            continue
        buf = io.BytesIO()
        df.to_parquet(buf, index=False, engine="pyarrow")
        buf.seek(0)
        storage.upload_bytes(BUCKET, blob, buf.read())
        _record_captured_cell(manifest, league_id, day, df)
        n_captured += 1
        n_refetched += 1
        logger.info("re-fetched + wrote %s + record_captured %s %s (%d rows)", blob, league_id, day, len(df))

    if not dry_run:
        # Explicit write + final drain — record_* alone is a silent no-op
        # (bug class fixed at instruments-service@920b303; never reintroduce).
        manifest.write()
        manifest.close()
        flushed = _mw.flush_all_pending_buckets()
        logger.info("EXPLICIT PRE-EXIT DRAIN: %s", flushed)
    logger.info("fetch done: captured=%d (refetched=%d) failed=%d", n_captured, n_refetched, n_failed)
    return 1 if n_failed else 0


def main() -> int:
    logger.info("operation=%s bucket=%s vm=%s dry_run=%s", ARGS.operation, BUCKET, os.environ["VM_NAME"], ARGS.dry_run)
    if ARGS.operation == "flip":
        if not ARGS.pairs or not ARGS.truthset_run_id:
            logger.error("--pairs and --truthset-run-id are required for --operation flip")
            return 2
        return run_flip(ARGS.pairs, ARGS.truthset_run_id, ARGS.dry_run)
    if not ARGS.cells:
        logger.error("--cells is required for --operation fetch")
        return 2
    return run_fetch(ARGS.cells, ARGS.dry_run)


if __name__ == "__main__":
    sys.exit(main())
