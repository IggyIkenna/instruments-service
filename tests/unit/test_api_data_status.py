"""Unit tests for api/data_status.py — covers all branches for coverage."""

from __future__ import annotations

from collections import namedtuple
from datetime import date
from unittest.mock import patch

import pandas as pd
import pytest

_CaptureStatusCounts = namedtuple(
    "CaptureStatusCounts",
    [
        "captured",
        "empty_confirmed",
        "attempted_failed",
        "expected_unattempted_known_empty",
        "expected_unattempted_pending_fetch",
    ],
)


def _make_counts(**kwargs: int) -> object:
    defaults = {
        "captured": 10,
        "empty_confirmed": 0,
        "attempted_failed": 0,
        "expected_unattempted_known_empty": 0,
        "expected_unattempted_pending_fetch": 0,
    }
    defaults.update(kwargs)
    return _CaptureStatusCounts(**defaults)


@pytest.mark.asyncio
async def test_get_data_status_happy_path() -> None:
    """Happy path: returns rows with coverage data."""
    index_df = pd.DataFrame(
        {
            "data_type": ["trades", "funding_rates"],
            "capture_status": ["captured", "captured"],
        }
    )
    counts = _make_counts()

    with (
        patch("instruments_service.api.data_status.read_availability_index", return_value=index_df),
        patch("instruments_service.api.data_status.get_write_bucket_name", return_value="test-bucket"),
        patch("instruments_service.api.data_status.compute_coverage_for_bucket", return_value=(counts, 0.95)),
    ):
        from instruments_service.api.data_status import get_data_status

        result = await get_data_status(asset_group="defi", bucket=None, data_type=None)

    assert result["bucket"] == "test-bucket"
    rows = result["rows"]
    assert isinstance(rows, list)
    assert len(rows) == 2
    data_types = {r["data_type"] for r in rows}
    assert data_types == {"trades", "funding_rates"}


@pytest.mark.asyncio
async def test_get_data_status_explicit_bucket() -> None:
    """Explicit bucket bypasses get_write_bucket_name."""
    index_df = pd.DataFrame({"data_type": ["trades"], "capture_status": ["captured"]})
    counts = _make_counts()

    with (
        patch("instruments_service.api.data_status.read_availability_index", return_value=index_df),
        patch("instruments_service.api.data_status.get_write_bucket_name") as mock_bucket,
        patch("instruments_service.api.data_status.compute_coverage_for_bucket", return_value=(counts, 1.0)),
    ):
        from instruments_service.api.data_status import get_data_status

        result = await get_data_status(asset_group="defi", bucket="my-explicit-bucket", data_type=None)

    mock_bucket.assert_not_called()
    assert result["bucket"] == "my-explicit-bucket"


@pytest.mark.asyncio
async def test_get_data_status_data_type_filter() -> None:
    """data_type filter restricts results to matching rows."""
    index_df = pd.DataFrame(
        {
            "data_type": ["trades", "funding_rates"],
            "capture_status": ["captured", "captured"],
        }
    )
    counts = _make_counts()

    with (
        patch("instruments_service.api.data_status.read_availability_index", return_value=index_df),
        patch("instruments_service.api.data_status.get_write_bucket_name", return_value="test-bucket"),
        patch("instruments_service.api.data_status.compute_coverage_for_bucket", return_value=(counts, 0.8)),
    ):
        from instruments_service.api.data_status import get_data_status

        result = await get_data_status(asset_group="defi", bucket=None, data_type="trades")

    rows = result["rows"]
    assert len(rows) == 1
    assert rows[0]["data_type"] == "trades"


@pytest.mark.asyncio
async def test_get_data_status_empty_index() -> None:
    """Empty index returns empty rows list."""
    with (
        patch("instruments_service.api.data_status.read_availability_index", return_value=pd.DataFrame()),
        patch("instruments_service.api.data_status.get_write_bucket_name", return_value="test-bucket"),
    ):
        from instruments_service.api.data_status import get_data_status

        result = await get_data_status(asset_group="defi", bucket=None, data_type=None)

    assert result["rows"] == []


@pytest.mark.asyncio
async def test_get_data_status_index_no_data_type_column() -> None:
    """Index without data_type column returns empty rows."""
    index_df = pd.DataFrame({"venue": ["BINANCE"]})

    with (
        patch("instruments_service.api.data_status.read_availability_index", return_value=index_df),
        patch("instruments_service.api.data_status.get_write_bucket_name", return_value="test-bucket"),
    ):
        from instruments_service.api.data_status import get_data_status

        result = await get_data_status(asset_group="defi", bucket=None, data_type=None)

    assert result["rows"] == []


@pytest.mark.asyncio
async def test_get_data_status_read_exception_raises_http_500() -> None:
    """Exception from read_availability_index raises HTTPException 500."""
    from fastapi import HTTPException

    with (
        patch("instruments_service.api.data_status.read_availability_index", side_effect=RuntimeError("GCS error")),
        patch("instruments_service.api.data_status.get_write_bucket_name", return_value="test-bucket"),
    ):
        from instruments_service.api.data_status import get_data_status

        with pytest.raises(HTTPException) as exc_info:
            await get_data_status(asset_group="defi", bucket=None, data_type=None)

    assert exc_info.value.status_code == 500


@pytest.mark.asyncio
async def test_get_data_status_all_null_data_types_returns_empty_rows() -> None:
    """Index with only null data_type values returns empty rows after dropna."""
    index_df = pd.DataFrame({"data_type": [None, None], "capture_status": ["captured", "captured"]})

    with (
        patch("instruments_service.api.data_status.read_availability_index", return_value=index_df),
        patch("instruments_service.api.data_status.get_write_bucket_name", return_value="test-bucket"),
    ):
        from instruments_service.api.data_status import get_data_status

        result = await get_data_status(asset_group="defi", bucket=None, data_type=None)

    assert result["rows"] == []


@pytest.mark.asyncio
async def test_get_data_status_data_type_filter_no_match_returns_empty() -> None:
    """data_type filter that matches no rows returns empty rows list."""
    index_df = pd.DataFrame({"data_type": ["trades"], "capture_status": ["captured"]})
    counts = _make_counts()

    with (
        patch("instruments_service.api.data_status.read_availability_index", return_value=index_df),
        patch("instruments_service.api.data_status.get_write_bucket_name", return_value="test-bucket"),
        patch("instruments_service.api.data_status.compute_coverage_for_bucket", return_value=(counts, 1.0)),
    ):
        from instruments_service.api.data_status import get_data_status

        result = await get_data_status(asset_group="defi", bucket=None, data_type="nonexistent_type")

    assert result["rows"] == []


@pytest.mark.asyncio
async def test_get_data_status_coverage_rounded_to_six_decimals() -> None:
    """Coverage ratio is rounded to 6 decimal places."""
    index_df = pd.DataFrame({"data_type": ["trades"], "capture_status": ["captured"]})
    counts = _make_counts()
    raw_ratio = 1 / 3  # 0.333333...

    with (
        patch("instruments_service.api.data_status.read_availability_index", return_value=index_df),
        patch("instruments_service.api.data_status.get_write_bucket_name", return_value="test-bucket"),
        patch("instruments_service.api.data_status.compute_coverage_for_bucket", return_value=(counts, raw_ratio)),
    ):
        from instruments_service.api.data_status import get_data_status

        result = await get_data_status(asset_group="defi", bucket=None, data_type=None)

    rows = result["rows"]
    assert len(rows) == 1
    assert rows[0]["coverage"] == round(raw_ratio, 6)


# ---------------------------------------------------------------------------
# _season_year_from_date
# ---------------------------------------------------------------------------


def test_season_year_august_is_start_of_new_season() -> None:
    from instruments_service.api.data_status import _season_year_from_date

    assert _season_year_from_date(date(2025, 8, 1)) == 2025


def test_season_year_july_still_prior_season() -> None:
    from instruments_service.api.data_status import _season_year_from_date

    assert _season_year_from_date(date(2026, 7, 31)) == 2025


def test_season_year_january_prior_season() -> None:
    from instruments_service.api.data_status import _season_year_from_date

    assert _season_year_from_date(date(2026, 1, 15)) == 2025


# ---------------------------------------------------------------------------
# _compute_fixtures_depth_coverage
# ---------------------------------------------------------------------------


def test_compute_fixtures_depth_coverage_empty_df_returns_none() -> None:
    from instruments_service.api.data_status import _compute_fixtures_depth_coverage

    empty = pd.DataFrame(columns=["data_type", "capture_status", "date", "league_id", "row_count"])
    overall, breakdown = _compute_fixtures_depth_coverage(empty)
    assert overall is None
    assert breakdown == []


def test_compute_fixtures_depth_coverage_no_fixtures_data_type() -> None:
    from instruments_service.api.data_status import _compute_fixtures_depth_coverage

    df = pd.DataFrame(
        {
            "data_type": ["FIXTURE_STATS", "FIXTURE_EVENTS"],
            "capture_status": ["captured", "captured"],
            "date": ["2025-09-01", "2025-09-01"],
            "league_id": ["EPL", "EPL"],
            "row_count": [10, 5],
        }
    )
    overall, breakdown = _compute_fixtures_depth_coverage(df)
    assert overall is None
    assert breakdown == []


def test_compute_fixtures_depth_coverage_with_known_league() -> None:
    """EPL 2025 has a known expected count — depth_coverage should be a float."""
    from instruments_service.api.data_status import _compute_fixtures_depth_coverage

    df = pd.DataFrame(
        {
            "data_type": ["FIXTURES", "FIXTURES"],
            "capture_status": ["captured", "captured"],
            "date": ["2025-09-01", "2025-10-15"],
            "league_id": ["EPL", "EPL"],
            "row_count": [10, 8],
        }
    )

    with patch(
        "instruments_service.api.data_status.get_expected_fixture_count",
        return_value=380,
    ) as mock_fn:
        overall, breakdown = _compute_fixtures_depth_coverage(df)

    mock_fn.assert_called_once_with("EPL", 2025)
    assert len(breakdown) == 1
    entry = breakdown[0]
    assert entry["league_id"] == "EPL"
    assert entry["season_year"] == 2025
    assert entry["captured_fixtures"] == 18  # 10 + 8
    assert entry["expected_fixtures"] == 380
    assert entry["depth_coverage"] == round(18 / 380, 6)
    assert overall == round(18 / 380, 6)


def test_compute_fixtures_depth_coverage_unknown_league_returns_null_depth() -> None:
    """When get_expected_fixture_count returns None, depth_coverage is None."""
    from instruments_service.api.data_status import _compute_fixtures_depth_coverage

    df = pd.DataFrame(
        {
            "data_type": ["FIXTURES"],
            "capture_status": ["captured"],
            "date": ["2025-09-01"],
            "league_id": ["UNKNOWN_LEAGUE"],
            "row_count": [5],
        }
    )

    with patch(
        "instruments_service.api.data_status.get_expected_fixture_count",
        return_value=None,
    ):
        overall, breakdown = _compute_fixtures_depth_coverage(df)

    assert overall is None
    assert len(breakdown) == 1
    assert breakdown[0]["depth_coverage"] is None
    assert breakdown[0]["expected_fixtures"] is None


def test_compute_fixtures_depth_coverage_only_counts_captured_shards() -> None:
    """attempted_failed shards should not contribute row_count to numerator."""
    from instruments_service.api.data_status import _compute_fixtures_depth_coverage

    df = pd.DataFrame(
        {
            "data_type": ["FIXTURES", "FIXTURES"],
            "capture_status": ["captured", "attempted_failed"],
            "date": ["2025-09-01", "2025-09-08"],
            "league_id": ["EPL", "EPL"],
            "row_count": [10, 99],  # 99 must NOT be counted
        }
    )

    with patch(
        "instruments_service.api.data_status.get_expected_fixture_count",
        return_value=380,
    ):
        _overall, breakdown = _compute_fixtures_depth_coverage(df)

    assert breakdown[0]["captured_fixtures"] == 10


def test_compute_fixtures_depth_coverage_multi_league_multi_season() -> None:
    """Multiple leagues and seasons each get their own breakdown entry."""
    from instruments_service.api.data_status import _compute_fixtures_depth_coverage

    df = pd.DataFrame(
        {
            "data_type": ["FIXTURES", "FIXTURES", "FIXTURES"],
            "capture_status": ["captured", "captured", "captured"],
            "date": ["2025-09-01", "2025-03-01", "2025-10-01"],
            "league_id": ["EPL", "EPL", "LALIGA"],
            "row_count": [10, 5, 8],
        }
    )
    # EPL 2025 (Aug-Jul): 2025-09-01 → season_year=2025; EPL 2024 (2025-03): season_year=2024
    # LALIGA 2025: 2025-10-01 → season_year=2025

    expected_map = {("EPL", 2025): 380, ("EPL", 2024): 380, ("LALIGA", 2025): 380}

    def _side_effect(league_id: str, season_year: int) -> int:
        return expected_map.get((league_id, season_year), 0)

    with patch(
        "instruments_service.api.data_status.get_expected_fixture_count",
        side_effect=_side_effect,
    ):
        overall, breakdown = _compute_fixtures_depth_coverage(df)

    assert len(breakdown) == 3
    league_seasons = {(b["league_id"], b["season_year"]) for b in breakdown}
    assert ("EPL", 2025) in league_seasons
    assert ("EPL", 2024) in league_seasons
    assert ("LALIGA", 2025) in league_seasons
    assert overall is not None


# ---------------------------------------------------------------------------
# get_data_status — sports FIXTURES depth_coverage integration
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_data_status_sports_fixtures_includes_depth_coverage() -> None:
    """For sports FIXTURES, the row includes depth_coverage and depth_breakdown."""
    index_df = pd.DataFrame(
        {
            "data_type": ["FIXTURES"],
            "capture_status": ["captured"],
            "date": ["2025-09-01"],
            "league_id": ["EPL"],
            "row_count": [10],
        }
    )
    counts = _make_counts()

    with (
        patch("instruments_service.api.data_status.read_availability_index", return_value=index_df),
        patch("instruments_service.api.data_status.get_write_bucket_name", return_value="sports-bucket"),
        patch("instruments_service.api.data_status.compute_coverage_for_bucket", return_value=(counts, 0.5)),
        patch("instruments_service.api.data_status.get_expected_fixture_count", return_value=380),
    ):
        from instruments_service.api.data_status import get_data_status

        result = await get_data_status(asset_group="sports", bucket=None, data_type=None)

    rows = result["rows"]
    assert len(rows) == 1
    row = rows[0]
    assert row["data_type"] == "FIXTURES"
    assert "depth_coverage" in row
    assert "depth_breakdown" in row
    assert row["depth_coverage"] == round(10 / 380, 6)
    assert len(row["depth_breakdown"]) == 1


@pytest.mark.asyncio
async def test_get_data_status_non_sports_no_depth_fields() -> None:
    """Non-sports asset_group rows do not have depth_coverage."""
    index_df = pd.DataFrame({"data_type": ["trades"], "capture_status": ["captured"]})
    counts = _make_counts()

    with (
        patch("instruments_service.api.data_status.read_availability_index", return_value=index_df),
        patch("instruments_service.api.data_status.get_write_bucket_name", return_value="defi-bucket"),
        patch("instruments_service.api.data_status.compute_coverage_for_bucket", return_value=(counts, 0.9)),
    ):
        from instruments_service.api.data_status import get_data_status

        result = await get_data_status(asset_group="defi", bucket=None, data_type=None)

    rows = result["rows"]
    assert len(rows) == 1
    assert "depth_coverage" not in rows[0]
    assert "depth_breakdown" not in rows[0]
