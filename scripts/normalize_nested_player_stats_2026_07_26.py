#!/usr/bin/env python3
# Epic: sports_master
# Lifecycle: oneoff
# Delete-when: 0 canonical entity=player_stats cells carry the nested
#   [team, players, fixture_id, available_at] schema (verified post-apply
#   via a fresh manifest-driven census matching this script's own
#   methodology).
"""normalize_nested_player_stats_2026_07_26.py — flatten canonical sports
player_stats cells still carrying the raw, pre-normalization nested schema
into the canonical one-row-per-player shape.

CONTEXT: `plans/active/issues/canonical_player_stats_fixture_events_quality_2026_07_16.md`
Finding 1's 2026-07-25 dedup pass (`dedup_canonical_player_stats_2026_07_25.py`)
SKIPPED ~3,274/26,687 (~12%) captured PLAYER_STATS cells that carry a NESTED
schema (columns ``[team, players, fixture_id, available_at]``, one row per
TEAM rather than one row per PLAYER) rather than guess at how to dedupe a
shape it wasn't built for. This script is that follow-up: it flattens those
cells into the canonical shape.

ORIGIN (git-archaeology, see `sports_satellite_ao_dispatch_batch5_2026_07_26.md`
Progress Log): the nested shape is the RAW, un-normalized API-Football
``/fixtures/players`` response, written directly by the very first version of
the orchestrator (``instruments-service@f16ce40f``, 2026-04-11) before
``normalize_api_football_player_stats`` was wired in (superseded the very next
day by ``instruments-service@afa34ebf``, 2026-04-12). Each row is one
per-team block: ``team`` = the raw API-Football team dict
(``{"id":..., "name":..., "logo":..., "update":...}``), ``players`` = a list
of per-player dicts (``{"player": {...}, "statistics": [...]}``).

**Live-verified 2026-07-26** (read-only probe over 4 real nested cells): both
``team`` and ``players`` are stored as Python ``repr()`` STRINGS (the
2026-04-11 writer stringified every value before building the DataFrame —
confirmed via ``git show f16ce40f:instruments_service/engine/orchestrator.py``),
not native Parquet struct/list columns. They parse cleanly with
``ast.literal_eval`` (single-quoted Python literal syntax, not JSON) — the
data is fully recoverable, not lossy.

NORMALIZATION: reuses the EXACT SAME production mapping function,
``unified_api_contracts.external.api_football.normalize.normalize_api_football_player_stats``,
that every already-flat canonical cell was produced by — no hand-rolled
mapping logic, so the flattened rows are semantically identical to what the
same raw API response would have produced had the writer been fixed a day
earlier. Also reuses the production writer-side de-dup gate,
``instruments_service.engine.orchestrator._dedupe_player_stats_df``, on the
flattened frame for consistency with every other canonical write path.

PER-OBJECT ALL-OR-NOTHING: if ANY row in an object fails to
``ast.literal_eval`` (corrupt/truncated string), the WHOLE object is left
untouched and counted ``parse_error`` — never a partial rewrite that silently
drops some players. These become the "documented unrecoverable subset" the
plan's done-criterion allows for.

SCOPE + OBJECT RESOLUTION: single bounded manifest read
(`read_availability_index`, data_type=PLAYER_STATS, capture_status=captured)
— no fresh GCS walk (single-walk discipline), same as the sibling dedup
script. Object paths resolved via UAC's `candidate_parquet_paths(...)` SSOT.
A cell is skipped (not this script's scope) if it already carries the flat
schema (``fixture_id`` + ``player_id`` as top-level columns) — that's the
already-deduped population.

WRITE SAFETY: generation-matched read-then-write (download_bytes_with_generation
+ conditional_upload_bytes), identical to `dedup_canonical_player_stats_2026_07_25.py`
— a concurrent writer landing between read and write fails the CAS check
instead of being silently clobbered. Idempotent: re-running after a
successful flatten sees the flat schema and skips (not this script's scope
per the check above), so a partial run is always safely resumable.

DRY-RUN by default -- reads + computes + logs without writing. ``--apply``
performs the rewrite. ``--limit N`` caps the number of manifest cells
processed (for sampling/testing).

Usage::

    GCP_PROJECT_ID=central-element-323112 PROJECT_ID=central-element-323112 \\
      DEPLOYMENT_ENV_SHORT=prd CLOUD_PROVIDER=gcp \\
      .venv/bin/python scripts/normalize_nested_player_stats_2026_07_26.py [--apply] [--limit N] [--workers N]
"""

from __future__ import annotations

import argparse
import ast
import io
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field

import numpy as np
import pandas as pd
from unified_api_contracts.canonical.domain.sports.gcs_paths import candidate_parquet_paths
from unified_api_contracts.external.api_football.normalize import normalize_api_football_player_stats
from unified_trading_library import get_storage_client, read_availability_index

from instruments_service.engine.orchestrator.sports_reference_fixture_entity_gates import (
    _dedupe_player_stats_df,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("normalize_nested_player_stats")

_BUCKET = "instruments-store-sports-prd-central-element-323112"
_DATA_TYPE = "PLAYER_STATS"
_FLAT_KEY_COLS = ("fixture_id", "player_id")
_NESTED_COLS = ("team", "players", "fixture_id", "available_at")


@dataclass(frozen=True)
class Cell:
    date: str
    league_id: str
    pipeline_mode: str


@dataclass
class CellResult:
    cell: Cell
    status: str  # "not_found" | "already_flat" | "unrecognized_schema" | "parse_error" | "normalized" | "would_normalize" | "error"
    obj_path: str = ""
    rows_before: int = 0
    rows_after: int = 0
    detail: str = ""
    parse_errors: list[str] = field(default_factory=list)


def _resolve_object(client, cell: Cell) -> str | None:
    for cand in candidate_parquet_paths(_DATA_TYPE, cell.date, cell.league_id, pipeline_mode=cell.pipeline_mode):
        if client.blob_exists(_BUCKET, cand):
            return cand
    return None


def _coerce_parsed(val: object) -> object:
    """Normalize a cell value into a plain Python dict/list/scalar.

    Two on-disk representations were found live (2026-07-26 probe): (1) a
    Python ``repr()`` string (single-quoted, from the 2026-04-11 writer's
    blanket ``str(v)`` sanitisation) -- parsed via ``ast.literal_eval``; (2) a
    NATIVE parquet list/struct column, which pandas/pyarrow decode as a
    ``numpy.ndarray`` (of dicts) rather than a Python list -- converted via
    ``.tolist()``. Both are real, not a guess: the ``numpy.ndarray`` case
    surfaced from a live dry-run sample, not speculatively handled.
    """
    if isinstance(val, str):
        return ast.literal_eval(val)
    if isinstance(val, np.ndarray):
        return val.tolist()
    return val


def _flatten_nested_df(pdf: pd.DataFrame) -> tuple[list[dict[str, object]], list[str]]:
    """Flatten a nested-schema player_stats DataFrame into flat per-player
    records. Returns (records, parse_error_messages). Empty parse_error_messages
    means every row parsed cleanly."""
    records: list[dict[str, object]] = []
    parse_errors: list[str] = []
    for idx, row in pdf.iterrows():
        try:
            team_dict = _coerce_parsed(row["team"])
            players_list = _coerce_parsed(row["players"])
            if not isinstance(team_dict, dict) or not isinstance(players_list, list):
                raise ValueError(f"unexpected parsed types: team={type(team_dict)} players={type(players_list)}")
        except Exception as e:
            parse_errors.append(f"row {idx}: {e!r}")
            continue
        fixture_id = str(row["fixture_id"])
        raw_block = {"team": team_dict, "players": players_list}
        recs = normalize_api_football_player_stats(raw_block, fixture_id=fixture_id)
        for rec in recs:
            rec["available_at"] = row["available_at"]
        records.extend(recs)
    return records, parse_errors


def _process_cell(client, cell: Cell, apply: bool) -> CellResult:
    obj_path = _resolve_object(client, cell)
    if obj_path is None:
        return CellResult(cell=cell, status="not_found")

    try:
        data, generation = client.download_bytes_with_generation(_BUCKET, obj_path)
        pdf = pd.read_parquet(io.BytesIO(data))
    except Exception as e:
        return CellResult(cell=cell, status="error", obj_path=obj_path, detail=repr(e))

    if all(c in pdf.columns for c in _FLAT_KEY_COLS):
        return CellResult(cell=cell, status="already_flat", obj_path=obj_path, rows_before=len(pdf))

    if not all(c in pdf.columns for c in _NESTED_COLS):
        return CellResult(
            cell=cell,
            status="unrecognized_schema",
            obj_path=obj_path,
            rows_before=len(pdf),
            detail=str(list(pdf.columns)),
        )

    records, parse_errors = _flatten_nested_df(pdf)
    if parse_errors:
        # All-or-nothing: any unparseable row leaves the WHOLE object untouched
        # rather than risk a partial rewrite silently dropping players.
        return CellResult(
            cell=cell,
            status="parse_error",
            obj_path=obj_path,
            rows_before=len(pdf),
            parse_errors=parse_errors,
        )

    if not records:
        # INCIDENT GUARD (added 2026-07-26 after a real prod incident: the
        # 2026-07-26 04:30Z --apply run wrote 240 objects as a fully empty
        # (0-column, 0-row) parquet -- every row's team/players parsed fine
        # but normalize_api_football_player_stats legitimately returned zero
        # player records for every team-block in the object. Writing that
        # silently discards whatever the source rows described, and this
        # bucket has NO object versioning / soft-delete (retentionDurationSeconds=0
        # confirmed), so an empty write is NOT recoverable from GCS. Refuse
        # the write and flag for manual/live-refetch remediation instead --
        # see plans/active/issues/sports_player_stats_normalize_empty_write_incident_2026_07_26.md.
        return CellResult(
            cell=cell,
            status="empty_result_flagged",
            obj_path=obj_path,
            rows_before=len(pdf),
            detail="flatten produced 0 player records from a non-empty nested object -- left untouched",
        )

    flat_df = pd.DataFrame(records)
    flat_df = _dedupe_player_stats_df(flat_df)

    if len(flat_df) == 0:
        # Same guard, post-dedupe (defensive -- dedupe never empties a
        # non-empty frame today, but never trust that silently).
        return CellResult(
            cell=cell,
            status="empty_result_flagged",
            obj_path=obj_path,
            rows_before=len(pdf),
            detail="flat_df empty after dedupe -- left untouched",
        )

    if not apply:
        return CellResult(
            cell=cell, status="would_normalize", obj_path=obj_path, rows_before=len(pdf), rows_after=len(flat_df)
        )

    out_buf = io.BytesIO()
    flat_df.to_parquet(out_buf, index=False)
    out_bytes = out_buf.getvalue()
    try:
        new_gen = client.conditional_upload_bytes(_BUCKET, obj_path, out_bytes, if_generation_match=generation)
    except Exception as e:
        return CellResult(
            cell=cell, status="error", obj_path=obj_path, rows_before=len(pdf), rows_after=len(flat_df), detail=repr(e)
        )
    if new_gen is None:
        # CAS lost the race to a concurrent writer -- do NOT treat as success;
        # a future re-run will pick this cell up fresh (idempotent).
        return CellResult(
            cell=cell,
            status="error",
            obj_path=obj_path,
            rows_before=len(pdf),
            rows_after=len(flat_df),
            detail="CAS_LOST_RACE (generation changed under us)",
        )
    return CellResult(cell=cell, status="normalized", obj_path=obj_path, rows_before=len(pdf), rows_after=len(flat_df))


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
    total_rows_before = 0
    total_rows_after = 0
    errors: list[CellResult] = []
    parse_error_cells: list[CellResult] = []
    unrecognized: list[CellResult] = []
    empty_flagged: list[CellResult] = []

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(_process_cell, client, c, args.apply): c for c in cells}
        for done, fut in enumerate(as_completed(futures), start=1):
            res = fut.result()
            counts[res.status] = counts.get(res.status, 0) + 1
            total_rows_before += res.rows_before
            total_rows_after += res.rows_after
            if res.status == "error":
                errors.append(res)
            elif res.status == "parse_error":
                parse_error_cells.append(res)
            elif res.status == "unrecognized_schema":
                unrecognized.append(res)
            elif res.status == "empty_result_flagged":
                empty_flagged.append(res)
            if done % 500 == 0:
                logger.info("Progress: %d/%d — %s", done, len(cells), counts)

    logger.info("=" * 60)
    logger.info("DONE. apply=%s", args.apply)
    logger.info("Status counts: %s", counts)
    logger.info("Total rows before (team-blocks): %d, after (flat players): %d", total_rows_before, total_rows_after)
    if parse_error_cells:
        logger.warning(
            "Cells with unparseable rows (left untouched, %d): %s",
            len(parse_error_cells),
            [(r.cell.date, r.cell.league_id, r.parse_errors[:2]) for r in parse_error_cells[:10]],
        )
    if unrecognized:
        logger.warning(
            "Cells with an unrecognized schema (neither flat nor nested, %d): %s",
            len(unrecognized),
            [(r.cell.date, r.cell.league_id, r.detail) for r in unrecognized[:10]],
        )
    if empty_flagged:
        logger.warning(
            "Cells flagged empty-result (left untouched, needs live-refetch remediation, %d): %s",
            len(empty_flagged),
            [(r.cell.date, r.cell.league_id) for r in empty_flagged[:10]],
        )
    if errors:
        logger.warning("Sample errors (up to 10): %s", [(r.obj_path, r.detail) for r in errors[:10]])

    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
