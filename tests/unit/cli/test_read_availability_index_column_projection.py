"""Regression guard for the 3 NEW bare ``read_availability_index(bucket)`` call
sites in ``instruments_service/cli/main.py`` found by
``read_availability_index_bare_defi_callers_2026_07_27.md`` (CLI subcommands
added 2026-08-03, after the doc's 2026-07-31 zero-new-occurrences re-verify).

Pins the exact ``columns=`` list per call site so a future edit can't
silently drop back to a bare (unprojected) call on a defi-asset-group
bucket — one cache-miss from decoding the whole ~1.58 GB+ consolidated
availability index into memory.
"""

from __future__ import annotations

import json
from unittest.mock import patch

import pandas as pd
import pytest

from instruments_service.cli.main import (
    _run_coverage_status,
    _run_refresh_league_entity_coverage,
)


class _FakeCaptureStatusCounts:
    """Minimal stand-in for UTL's ``CaptureStatusCounts`` NamedTuple."""

    def _asdict(self) -> dict[str, int]:
        return {"captured": 1, "expected_unattempted": 0, "attempted_failed": 0, "empty_confirmed": 0}


def test_coverage_status_projects_columns(capsys: pytest.CaptureFixture[str]) -> None:
    """``_run_coverage_status`` only ever reads the ``data_type`` column."""
    index_df = pd.DataFrame({"data_type": ["DEX_POOLS"]})
    with (
        patch("unified_trading_library.read_availability_index", return_value=index_df) as mock_read,
        patch("unified_trading_library.compute_coverage_for_bucket", return_value=(_FakeCaptureStatusCounts(), 0.5)),
    ):
        _run_coverage_status(["--bucket", "test-bucket", "--asset-group", "defi"])
    mock_read.assert_called_once_with("test-bucket", columns=["data_type"])
    out = json.loads(capsys.readouterr().out)
    assert out["bucket"] == "test-bucket"
    assert out["rows"][0]["data_type"] == "DEX_POOLS"


def test_coverage_status_empty_index_short_circuits(capsys: pytest.CaptureFixture[str]) -> None:
    with patch("unified_trading_library.read_availability_index", return_value=pd.DataFrame()) as mock_read:
        _run_coverage_status(["--bucket", "test-bucket"])
    mock_read.assert_called_once_with("test-bucket", columns=["data_type"])
    out = json.loads(capsys.readouterr().out)
    assert out == {"bucket": "test-bucket", "rows": []}


def test_refresh_league_entity_coverage_projects_columns(tmp_path: object) -> None:
    """``_run_refresh_league_entity_coverage`` only ever reads
    data_type/capture_status/league_id (the entity/league coverage scan)."""
    from pathlib import Path

    uac_json = Path(str(tmp_path)) / "sports_league_entity_coverage.json"
    uac_json.write_text("{}")
    index_df = pd.DataFrame(
        {
            "data_type": ["INJURIES"],
            "capture_status": ["captured"],
            "league_id": ["39"],
        }
    )
    with (
        patch("unified_trading_library.read_availability_index", return_value=index_df) as mock_read,
        patch("instruments_service.cli.main.get_write_bucket_name", return_value="test-sports-bucket"),
    ):
        _run_refresh_league_entity_coverage(["--uac-json", str(uac_json)])
    mock_read.assert_called_once_with("test-sports-bucket", columns=["data_type", "capture_status", "league_id"])
