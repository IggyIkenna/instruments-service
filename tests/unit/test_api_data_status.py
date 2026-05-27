"""Unit tests for api/data_status.py — covers all branches for coverage."""

from __future__ import annotations

from collections import namedtuple
from unittest.mock import MagicMock, patch

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
