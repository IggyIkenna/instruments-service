#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: 0 canonical entity=player_stats cells carry within-object
#   (fixture_id, player_id) duplicate rows (verified post-apply via a fresh
#   manifest-driven census matching this script's own methodology).
"""dedup_canonical_player_stats_2026_07_25.py — idempotent within-object
de-dup of canonical sports player_stats cells on (fixture_id, player_id).

CONTEXT: `plans/active/issues/canonical_player_stats_fixture_events_quality_2026_07_16.md`
Finding 1 measured 740,725 within-object exact-duplicate rows (~26% of
2,882,420 total rows) across all 27,296 canonical
`sports_reference/by_date/day={D}/pipeline_mode={PM}/entity=player_stats/league={L}/player_stats.parquet`
objects in `instruments-store-sports-prd`. The writer appends without
de-duping on (fixture_id, player_id). A prior remediation (the 2026-07-16
"T2.4" legacy-bucket-cutover union) de-duped the 4,015 cells it happened to
touch as a side effect; its own tooling (`~/tmp-cutover/t2_4_build_canon_keys.py`)
was session-local and did not survive to this session. This script covers
ALL captured cells uniformly rather than trying to reconstruct which 4,015
were already touched — de-duping an already-clean object is a safe no-op
(0 duplicate rows found -> skip, no write), so re-running over the full
population converges to the same end state regardless of prior partial
coverage.

SCOPE + OBJECT RESOLUTION: driven entirely by a single bounded manifest read
(`read_availability_index`, data_type=PLAYER_STATS, capture_status=captured)
-- no fresh GCS walk (single-walk discipline). Per manifest cell, the real
object path is resolved via UAC's `candidate_parquet_paths(..., pipeline_mode=...)`
SSOT (NOT hand-built) -- a 300-cell probe (2026-07-25) confirmed the
`pipeline_mode=` segment is REQUIRED for cells to resolve (a `league_id`-only
candidate without `pipeline_mode` misses the object entirely on every
sampled cell that has a `pipeline_mode`).

SCHEMA HETEROGENEITY (found during the same probe, not previously
documented for player_stats): ~8% of captured cells (24/289 in the probe)
carry a NESTED schema -- columns `[team, players, fixture_id, available_at]`
where `players` is a list-of-dicts per team, not one flat row per player.
This script SKIPS any object that does not carry BOTH `fixture_id` AND
`player_id` as flat top-level columns -- it never guesses how to dedupe a
schema it wasn't built for. Skipped cells are counted and logged separately;
they are NOT part of this script's Finding-1 scope (that finding's own
2,882,420-row / 27,296-object count implies the flat schema is the
dominant/measured shape) and are left for a future schema-normalisation
pass, mirroring the fixture_events heterogeneity (Finding 2) this same
issue doc already tracks as separate work.

WRITE SAFETY: generation-matched read-then-write (download_bytes_with_generation
+ conditional_upload_bytes) so a concurrent writer landing between this
script's read and write fails the CAS check instead of being silently
clobbered -- the same class of TOCTOU race this session already found (and
fixed, at the manifest-consolidator level) for the league_id migration.
De-dup itself is `drop_duplicates(subset=["fixture_id","player_id"], keep="first")`
-- exact-duplicate ROWS only (every column must match for the row to count
as a duplicate is NOT required; the key is (fixture_id, player_id) alone,
matching Finding 1's own methodology and the issue doc's own dup-census
definition). No other column is touched; row ORDER of the surviving rows is
preserved.

DRY-RUN by default -- reads + computes + logs without writing. ``--apply``
performs the rewrite. ``--limit N`` caps the number of manifest cells
processed (for sampling/testing).

Usage::

    GCP_PROJECT_ID=central-element-323112 PROJECT_ID=central-element-323112 \\
      DEPLOYMENT_ENV_SHORT=prd CLOUD_PROVIDER=gcp \\
      .venv/bin/python scripts/dedup_canonical_player_stats_2026_07_25.py [--apply] [--limit N] [--workers N]
"""

from __future__ import annotations

import argparse
import io
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

import pandas as pd
from unified_api_contracts.canonical.domain.sports.gcs_paths import candidate_parquet_paths
from unified_trading_library import get_storage_client, read_availability_index

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("dedup_canonical_player_stats")

_BUCKET = "instruments-store-sports-prd-central-element-323112"
_DATA_TYPE = "PLAYER_STATS"
_KEY_COLS = ["fixture_id", "player_id"]


@dataclass(frozen=True)
class Cell:
    date: str
    league_id: str
    pipeline_mode: str


@dataclass
class CellResult:
    cell: Cell
    status: str  # "not_found" | "schema_skip" | "clean" | "deduped" | "error" | "would_dedupe"
    obj_path: str = ""
    rows_before: int = 0
    dup_rows: int = 0
    detail: str = ""


def _resolve_object(client, cell: Cell) -> str | None:
    for cand in candidate_parquet_paths(_DATA_TYPE, cell.date, cell.league_id, pipeline_mode=cell.pipeline_mode):
        if client.blob_exists(_BUCKET, cand):
            return cand
    return None


def _process_cell(client, cell: Cell, apply: bool) -> CellResult:
    obj_path = _resolve_object(client, cell)
    if obj_path is None:
        return CellResult(cell=cell, status="not_found")

    try:
        data, generation = client.download_bytes_with_generation(_BUCKET, obj_path)
        pdf = pd.read_parquet(io.BytesIO(data))
    except Exception as e:
        return CellResult(cell=cell, status="error", obj_path=obj_path, detail=repr(e))

    if not all(c in pdf.columns for c in _KEY_COLS):
        return CellResult(
            cell=cell, status="schema_skip", obj_path=obj_path, rows_before=len(pdf), detail=str(list(pdf.columns))
        )

    dup_mask = pdf.duplicated(subset=_KEY_COLS, keep="first")
    dup_count = int(dup_mask.sum())
    if dup_count == 0:
        return CellResult(cell=cell, status="clean", obj_path=obj_path, rows_before=len(pdf))

    if not apply:
        return CellResult(cell=cell, status="would_dedupe", obj_path=obj_path, rows_before=len(pdf), dup_rows=dup_count)

    deduped = pdf[~dup_mask].reset_index(drop=True)
    out_buf = io.BytesIO()
    deduped.to_parquet(out_buf, index=False)
    out_bytes = out_buf.getvalue()
    try:
        new_gen = client.conditional_upload_bytes(_BUCKET, obj_path, out_bytes, if_generation_match=generation)
    except Exception as e:
        return CellResult(
            cell=cell, status="error", obj_path=obj_path, rows_before=len(pdf), dup_rows=dup_count, detail=repr(e)
        )
    if new_gen is None:
        # CAS lost the race to a concurrent writer -- do NOT treat as success;
        # a future re-run will pick this cell up fresh (idempotent).
        return CellResult(
            cell=cell,
            status="error",
            obj_path=obj_path,
            rows_before=len(pdf),
            dup_rows=dup_count,
            detail="CAS_LOST_RACE (generation changed under us)",
        )
    return CellResult(cell=cell, status="deduped", obj_path=obj_path, rows_before=len(pdf), dup_rows=dup_count)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true", help="perform the rewrite (default: dry-run, read-only)")
    ap.add_argument("--limit", type=int, default=None, help="cap the number of manifest cells processed (first N)")
    ap.add_argument(
        "--sample", type=int, default=None, help="process a random N-cell sample instead of the first N (testing)"
    )
    ap.add_argument("--workers", type=int, default=16, help="parallel worker threads (default 16)")
    args = ap.parse_args()

    client = get_storage_client()
    df = read_availability_index(_BUCKET, columns=["date", "league_id", "data_type", "capture_status", "pipeline_mode"])
    sub = df[(df["data_type"] == _DATA_TYPE) & (df["capture_status"] == "captured")]
    if args.sample:
        sub = sub.sample(n=min(args.sample, len(sub)), random_state=42)
    cells = [
        Cell(date=str(r["date"]), league_id=str(r["league_id"]), pipeline_mode=str(r["pipeline_mode"]))
        for _, r in sub.iterrows()
    ]
    if args.limit:
        cells = cells[: args.limit]

    logger.info(
        "Processing %d PLAYER_STATS captured cells (apply=%s, workers=%d)", len(cells), args.apply, args.workers
    )

    counts: dict[str, int] = {}
    total_dup_rows = 0
    errors: list[CellResult] = []
    schema_skips: list[CellResult] = []
    not_found: list[CellResult] = []

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(_process_cell, client, c, args.apply): c for c in cells}
        for done, fut in enumerate(as_completed(futures), start=1):
            res = fut.result()
            counts[res.status] = counts.get(res.status, 0) + 1
            total_dup_rows += res.dup_rows
            if res.status == "error":
                errors.append(res)
            elif res.status == "schema_skip":
                schema_skips.append(res)
            elif res.status == "not_found":
                not_found.append(res)
            if done % 1000 == 0:
                logger.info("Progress: %d/%d — %s", done, len(cells), counts)

    logger.info("=" * 60)
    logger.info("DONE. apply=%s", args.apply)
    logger.info("Status counts: %s", counts)
    logger.info("Total duplicate rows found/removed: %d", total_dup_rows)
    if not_found:
        logger.info("Sample not_found (up to 10): %s", [(r.cell.date, r.cell.league_id) for r in not_found[:10]])
    if schema_skips:
        logger.info(
            "Sample schema_skip (up to 5): %s", [(r.cell.date, r.cell.league_id, r.detail) for r in schema_skips[:5]]
        )
    if errors:
        logger.warning("Sample errors (up to 10): %s", [(r.obj_path, r.detail) for r in errors[:10]])

    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
