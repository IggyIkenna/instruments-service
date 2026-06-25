"""GET /api/data-status — honest-coverage summary for an instruments-service bucket.

Mirrors the ``--operation=status`` CLI path but served over HTTP so the
deployment-api / deployment-UI can consume it without shelling out.

Query params:
    asset_group: str (default "defi") — instruments bucket selector
    bucket:      str (optional) — explicit bucket override; if omitted,
                 resolved via ``get_write_bucket_name("instruments", asset_group)``
    data_type:   str (optional) — restrict to a single data_type row

Response (200):
    {"bucket": str, "rows": [{"asset_group": str|null, "data_type": str,
     "counts": {captured, empty_confirmed, attempted_failed,
                expected_unattempted_known_empty, expected_unattempted_pending_fetch},
     "coverage": float,
     "depth_coverage": float|null,          -- sports FIXTURES only
     "depth_breakdown": [                   -- sports FIXTURES only
       {"league_id": str, "season_year": int,
        "captured_fixtures": int, "expected_fixtures": int|null,
        "depth_coverage": float|null}
     ]}]}

Response (500):
    {"error": str, "bucket": str}
"""

from __future__ import annotations

import logging
from datetime import date as _date

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from unified_api_contracts import CaptureStatusCounts
from unified_api_contracts.sports import get_expected_fixture_count
from unified_trading_library import (
    compute_coverage_for_bucket,
    get_write_bucket_name,
    read_availability_index,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["data-status"])

_SPORTS_FIXTURES_DT = "FIXTURES"


def _season_year_from_date(d: _date) -> int:
    """Return the football season start-year for *d*.

    Football seasons run Aug → Jul, so a match on 2025-09-01 belongs to the
    2025/26 season (season_year=2025), while 2026-03-15 belongs to 2025/26
    (season_year=2025).
    """
    return d.year if d.month >= 8 else d.year - 1


def _compute_fixtures_depth_coverage(
    index: pd.DataFrame,
) -> tuple[float | None, list[dict[str, object]]]:
    """Compute Tier-B depth coverage for sports FIXTURES shards.

    Uses the manifest ``row_count`` column (number of fixture records per
    captured shard) as the numerator and ``get_expected_fixture_count`` from
    UAC as the denominator.  No parquet reads required.

    Returns
    -------
    overall_depth : float | None
        Ratio of total captured fixtures to total expected fixtures across all
        (league_id, season_year) groups.  None when no expected counts are
        available.
    breakdown : list[dict]
        Per-(league_id, season_year) detail rows.
    """
    fixtures_mask = index["data_type"] == _SPORTS_FIXTURES_DT
    base = index[fixtures_mask].copy()

    if base.empty:
        return None, []

    # Derive season_year from the date column.
    parsed_dates: pd.Series = pd.to_datetime(base["date"], errors="coerce")
    base = base.copy()
    base["season_year"] = parsed_dates.apply(lambda ts: _season_year_from_date(ts.date()) if pd.notna(ts) else None)
    base = base.dropna(subset=["season_year", "league_id"])
    base["season_year"] = base["season_year"].astype(int)

    # Ensure row_count is numeric; unknown counts default to 0.
    if "row_count" in base.columns:
        base["row_count"] = pd.to_numeric(base["row_count"], errors="coerce").fillna(0).astype(int)
    else:
        base["row_count"] = 0

    breakdown: list[dict[str, object]] = []
    total_captured = 0
    total_expected = 0
    has_any_expected = False

    groups = base.groupby(["league_id", "season_year"], sort=True)
    for (league_id, season_year), grp in groups:
        captured_fixtures = int(grp.loc[grp["capture_status"] == "captured", "row_count"].sum())
        expected_fixtures: int | None = get_expected_fixture_count(str(league_id), int(season_year))
        if expected_fixtures is not None and expected_fixtures > 0:
            depth: float | None = round(captured_fixtures / expected_fixtures, 6)
            total_captured += captured_fixtures
            total_expected += expected_fixtures
            has_any_expected = True
        else:
            depth = None

        breakdown.append(
            {
                "league_id": str(league_id),
                "season_year": int(season_year),
                "captured_fixtures": captured_fixtures,
                "expected_fixtures": expected_fixtures,
                "depth_coverage": depth,
            }
        )

    overall_depth: float | None = round(total_captured / total_expected, 6) if has_any_expected else None
    return overall_depth, breakdown


@router.get("/data-status")
async def get_data_status(
    asset_group: str = Query(default="defi", description="Instruments bucket selector"),
    bucket: str | None = Query(default=None, description="Explicit bucket override"),
    data_type: str | None = Query(default=None, description="Filter to single data_type"),
) -> dict[str, object]:
    resolved_bucket: str = bucket if bucket else get_write_bucket_name("instruments", asset_group)

    try:
        index: pd.DataFrame = read_availability_index(resolved_bucket)
    except Exception as exc:
        logger.warning("data-status: failed to read index for bucket %s: %s", resolved_bucket, exc)
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    if index.empty or "data_type" not in index.columns:
        return {"bucket": resolved_bucket, "rows": []}

    unique_values: object = index["data_type"].dropna().unique()
    all_data_types: list[str] = sorted(str(v) for v in unique_values if v is not None)  # pyright: ignore[reportArgumentType,reportUnknownVariableType]
    if data_type is not None:
        all_data_types = [dt for dt in all_data_types if dt == data_type]

    # Pre-compute fixtures depth coverage once (reuses the already-loaded index).
    fixtures_depth: float | None = None
    fixtures_breakdown: list[dict[str, object]] = []
    if asset_group == "sports" and (data_type is None or data_type == _SPORTS_FIXTURES_DT):
        fixtures_depth, fixtures_breakdown = _compute_fixtures_depth_coverage(index)

    rows: list[dict[str, object]] = []
    for dt in all_data_types:
        counts: CaptureStatusCounts
        ratio: float
        counts, ratio = compute_coverage_for_bucket(resolved_bucket, asset_group=asset_group, data_type=dt)
        row: dict[str, object] = {
            "asset_group": asset_group,
            "data_type": dt,
            "counts": counts._asdict(),
            "coverage": round(ratio, 6),
        }
        if asset_group == "sports" and dt == _SPORTS_FIXTURES_DT:
            row["depth_coverage"] = fixtures_depth
            row["depth_breakdown"] = fixtures_breakdown
        rows.append(row)

    return {"bucket": resolved_bucket, "rows": rows}
