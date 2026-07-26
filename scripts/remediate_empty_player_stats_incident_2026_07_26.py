#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: the 240-object incident list
#   (plans/active/issues/sports_player_stats_normalize_empty_write_incident_2026_07_26.md)
#   is fully remediated or explicitly dispositioned (0 remaining 0-column
#   PLAYER_STATS objects, or every residual documented as a genuine
#   zero-player-stats fixture).
"""remediate_empty_player_stats_incident_2026_07_26.py — live re-fetch
remediation for the 240 canonical PLAYER_STATS objects that
``normalize_nested_player_stats_2026_07_26.py``'s 2026-07-26 04:30Z
``--apply`` run overwrote with a fully empty (0-column, 0-row) parquet file.

INCIDENT: that script's flatten step legitimately produced 0 player records
for every team-block in 240 objects (root cause + fix in the sibling script,
see its ``empty_result_flagged`` guard added post-incident) and, at the time,
wrote that empty result without a check -- silently discarding the source
rows. This bucket has NO object versioning and
``soft_delete_policy.retentionDurationSeconds=0`` (confirmed live via
``gcloud storage buckets describe`` + ``gcloud storage ls -a`` showing a
single generation) -- the original bytes are NOT recoverable from GCS. A
BigQuery-mirror pre-check (``sports_analytics``/``sports_betting`` datasets)
found no player_stats table -- no alternative recovery source exists either.

REMEDIATION (operator-authorized, BLK-bf61980b, 2026-07-26): re-fetch the
affected fixtures LIVE from the api_football API -- external data is always
available (HARD RULE) for a modest, well-bounded 240-cell pull. For each
affected (date, league_id, pipeline_mode) cell:

1. Read the SIBLING ``entity=fixtures`` object for the same (date, league,
   pipeline_mode) to recover the ``af_fixture_id``s that existed for that day
   (this sibling data was NOT touched by the incident -- still intact).
2. Call the REAL production adapter method,
   ``ApiFootballAdapter.get_fixture_player_stats(fixture_id)``, per fixture --
   this already returns fully normalized flat per-player records (it calls
   ``normalize_api_football_player_stats`` internally, the exact same
   production mapping function), so there is no hand-rolled parsing here.
3. Concatenate all fixtures' records for the cell, de-dupe via the same
   production writer-side gate (``_dedupe_player_stats_df``), and write.

**Fixtures with a genuinely empty live result stay FLAGGED, never written
empty** -- mirrors the sibling script's post-incident guard exactly. A
post-write READ-BACK verifies ``num_columns > 0 AND num_rows > 0`` via
pyarrow before counting a cell as remediated.

SCOPE: reads its cell list from a fixed JSON file produced by the incident
enumeration (``--cells-file``, defaults to the incident list checked into
this script's sibling ``_incident_2026_07_26_affected_cells.json``) -- never
a fresh broad scan. This is intentionally narrower than the sibling script's
manifest-driven scope: exactly the 240 (or however many remain unremediated)
incident objects, nothing wider.

DRY-RUN by default (fetches + computes, does not write). ``--apply`` performs
the rewrite.

Usage::

    GCP_PROJECT_ID=central-element-323112 PROJECT_ID=central-element-323112 \\
      DEPLOYMENT_ENV_SHORT=prd CLOUD_PROVIDER=gcp \\
      .venv/bin/python scripts/remediate_empty_player_stats_incident_2026_07_26.py \\
      --cells-file <path> [--apply] [--limit N]
"""

from __future__ import annotations

import argparse
import asyncio
import io
import json
import logging
from dataclasses import dataclass, field

import pandas as pd
from google.cloud import secretmanager
from unified_api_contracts.canonical.domain.sports.gcs_paths import candidate_parquet_paths
from unified_trading_library import get_storage_client, setup_events

from instruments_service.engine.orchestrator.sports_reference_fixture_entity_gates import (
    _dedupe_player_stats_df,
)
from instruments_service.reference_data.adapters.sports.adapters.api_football import (
    ApiFootballAdapter,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("remediate_empty_player_stats")

_BUCKET = "instruments-store-sports-prd-central-element-323112"
_PLAYER_STATS_DATA_TYPE = "PLAYER_STATS"
_FIXTURES_DATA_TYPE = "FIXTURES"
_SECRET_NAME = "projects/central-element-323112/secrets/api-football-api-key/versions/latest"


@dataclass
class IncidentCell:
    date: str
    league_id: str
    pipeline_mode: str
    obj_path: str


@dataclass
class RemediationResult:
    cell: IncidentCell
    status: (
        str  # "remediated" | "no_fixture_ids" | "no_player_data_live" | "would_remediate" | "error" | "verify_failed"
    )
    fixture_ids: list[str] = field(default_factory=list)
    rows_written: int = 0
    detail: str = ""


def _fetch_api_key() -> str:
    client = secretmanager.SecretManagerServiceClient()
    resp = client.access_secret_version(request={"name": _SECRET_NAME})
    return resp.payload.data.decode("utf-8").strip()


def _resolve_object(client, data_type: str, cell: IncidentCell) -> str | None:
    for cand in candidate_parquet_paths(data_type, cell.date, cell.league_id, pipeline_mode=cell.pipeline_mode):
        if client.blob_exists(_BUCKET, cand):
            return cand
    return None


def _load_fixture_ids(client, cell: IncidentCell) -> list[str]:
    obj_path = _resolve_object(client, _FIXTURES_DATA_TYPE, cell)
    if obj_path is None:
        return []
    data, _ = client.download_bytes_with_generation(_BUCKET, obj_path)
    pdf = pd.read_parquet(io.BytesIO(data))
    if "af_fixture_id" not in pdf.columns:
        return []
    return [str(v) for v in pdf["af_fixture_id"].dropna().tolist()]


async def _remediate_cell(client, adapter: ApiFootballAdapter, cell: IncidentCell, apply: bool) -> RemediationResult:
    fixture_ids = _load_fixture_ids(client, cell)
    if not fixture_ids:
        return RemediationResult(cell=cell, status="no_fixture_ids")

    all_records: list[dict[str, object]] = []
    for fid in fixture_ids:
        try:
            recs = await adapter.get_fixture_player_stats(int(fid))
        except Exception as e:
            return RemediationResult(cell=cell, status="error", fixture_ids=fixture_ids, detail=f"fixture={fid}: {e!r}")
        for rec in recs:
            all_records.append(dict(rec))

    if not all_records:
        # Genuinely no player stats for any fixture on this live re-fetch --
        # stays flagged, never written empty (same guard as the sibling script).
        return RemediationResult(cell=cell, status="no_player_data_live", fixture_ids=fixture_ids)

    flat_df = pd.DataFrame(all_records)
    flat_df = _dedupe_player_stats_df(flat_df)
    if len(flat_df) == 0:
        return RemediationResult(cell=cell, status="no_player_data_live", fixture_ids=fixture_ids)

    if not apply:
        return RemediationResult(
            cell=cell, status="would_remediate", fixture_ids=fixture_ids, rows_written=len(flat_df)
        )

    # Fresh generation read right before write (the incident-list generation
    # may be stale by now) -- CAS-safe against a concurrent writer.
    obj_path = _resolve_object(client, _PLAYER_STATS_DATA_TYPE, cell)
    if obj_path is None:
        return RemediationResult(
            cell=cell, status="error", fixture_ids=fixture_ids, detail="player_stats object vanished"
        )
    _, generation = client.download_bytes_with_generation(_BUCKET, obj_path)

    out_buf = io.BytesIO()
    flat_df.to_parquet(out_buf, index=False)
    out_bytes = out_buf.getvalue()
    try:
        new_gen = client.conditional_upload_bytes(_BUCKET, obj_path, out_bytes, if_generation_match=generation)
    except Exception as e:
        return RemediationResult(cell=cell, status="error", fixture_ids=fixture_ids, detail=repr(e))
    if new_gen is None:
        return RemediationResult(
            cell=cell, status="error", fixture_ids=fixture_ids, detail="CAS_LOST_RACE (generation changed under us)"
        )

    # MANDATORY read-back verify (per BLK-bf61980b guardrail #2).
    verify_data, _ = client.download_bytes_with_generation(_BUCKET, obj_path)
    verify_pdf = pd.read_parquet(io.BytesIO(verify_data))
    if len(verify_pdf.columns) == 0 or len(verify_pdf) == 0:
        return RemediationResult(
            cell=cell,
            status="verify_failed",
            fixture_ids=fixture_ids,
            rows_written=len(flat_df),
            detail=f"post-write read-back shows cols={len(verify_pdf.columns)} rows={len(verify_pdf)}",
        )
    return RemediationResult(cell=cell, status="remediated", fixture_ids=fixture_ids, rows_written=len(flat_df))


async def _main_async(cells: list[IncidentCell], apply: bool, concurrency: int) -> int:
    setup_events("instruments-service", "local")
    client = get_storage_client()
    api_key = _fetch_api_key()
    adapter = ApiFootballAdapter(api_key=api_key)

    sem = asyncio.Semaphore(concurrency)

    async def bound(cell: IncidentCell) -> RemediationResult:
        async with sem:
            return await _remediate_cell(client, adapter, cell, apply)

    results = await asyncio.gather(*(bound(c) for c in cells))

    counts: dict[str, int] = {}
    for r in results:
        counts[r.status] = counts.get(r.status, 0) + 1

    logger.info("=" * 60)
    logger.info("DONE. apply=%s", apply)
    logger.info("Status counts: %s", counts)
    for r in results:
        if r.status in ("error", "verify_failed", "no_player_data_live", "no_fixture_ids"):
            logger.warning(
                "%s: %s %s %s -- %s", r.status, r.cell.date, r.cell.league_id, r.cell.pipeline_mode, r.detail
            )

    remaining_bad = counts.get("error", 0) + counts.get("verify_failed", 0)
    return 0 if remaining_bad == 0 else 1


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--cells-file", required=True, help="JSON file listing the incident cells to remediate")
    ap.add_argument("--apply", action="store_true", help="perform the rewrite (default: dry-run)")
    ap.add_argument("--limit", type=int, default=None, help="cap the number of cells processed (first N)")
    ap.add_argument("--concurrency", type=int, default=8, help="max concurrent adapter calls (default 8)")
    args = ap.parse_args()

    with open(args.cells_file) as f:
        raw_cells = json.load(f)
    cells = [
        IncidentCell(date=c["date"], league_id=c["league_id"], pipeline_mode=c["pipeline_mode"], obj_path=c["obj_path"])
        for c in raw_cells
    ]
    if args.limit:
        cells = cells[: args.limit]

    logger.info("Remediating %d incident cells (apply=%s, concurrency=%d)", len(cells), args.apply, args.concurrency)
    return asyncio.run(_main_async(cells, args.apply, args.concurrency))


if __name__ == "__main__":
    raise SystemExit(main())
