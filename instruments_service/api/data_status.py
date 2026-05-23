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
     "coverage": float}]}

Response (500):
    {"error": str, "bucket": str}
"""

from __future__ import annotations

import logging

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from unified_api_contracts import CaptureStatusCounts
from unified_trading_library import (
    compute_coverage_for_bucket,
    get_write_bucket_name,
    read_availability_index,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["data-status"])


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

    rows: list[dict[str, object]] = []
    for dt in all_data_types:
        counts: CaptureStatusCounts
        ratio: float
        counts, ratio = compute_coverage_for_bucket(resolved_bucket, asset_group=asset_group, data_type=dt)
        rows.append(
            {
                "asset_group": asset_group,
                "data_type": dt,
                "counts": counts._asdict(),
                "coverage": round(ratio, 6),
            }
        )

    return {"bucket": resolved_bucket, "rows": rows}
