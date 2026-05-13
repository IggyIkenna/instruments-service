"""Unit tests — backfill_drift_funding_2026_05_13 script.

Tests the CLI argument parsing, date-range logic, prerequisite validation,
and dry-run behaviour. All tests are credential-free and do not hit real S3/GCS.

Plan: solana_perp_dex_adapters_2026_05_13 Phase 5.
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pytest

# Ensure scripts/ is importable
_SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent.parent / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

from backfill_drift_funding_2026_05_13 import (  # noqa: E402
    _DRIFT_V2_LAUNCH_DATE,
    _build_s3_key,
    _date_range,
    _dry_run_report,
    _validate_prerequisites,
)


def test_drift_v2_launch_date() -> None:
    """DRIFT V2 launch date must be 2021-11-05 or earlier."""
    assert date(2021, 11, 5) >= _DRIFT_V2_LAUNCH_DATE


def test_date_range_single_day() -> None:
    d = date(2024, 1, 15)
    assert _date_range(d, d) == [d]


def test_date_range_multi_day() -> None:
    start = date(2024, 1, 1)
    end = date(2024, 1, 5)
    result = _date_range(start, end)
    assert len(result) == 5
    assert result[0] == start
    assert result[-1] == end


def test_date_range_empty_when_inverted() -> None:
    start = date(2024, 2, 1)
    end = date(2024, 1, 1)
    # Inverted range returns empty (while condition fails immediately)
    assert _date_range(start, end) == []


def test_build_s3_key_format() -> None:
    key = _build_s3_key(date(2023, 4, 7), market_index=0)
    assert "fundingRate" in key
    assert "year=2023" in key
    assert "month=04" in key
    assert "day=07" in key
    assert "market=0" in key


def test_dry_run_report_does_not_raise(caplog: pytest.LogCaptureFixture) -> None:
    import logging

    with caplog.at_level(logging.INFO):
        _dry_run_report(date(2021, 11, 5), date(2021, 12, 1), "DRIFT-SOLANA")
    assert "DRY RUN" in caplog.text


def test_validate_prerequisites_fails_without_env_vars() -> None:
    with patch.dict("os.environ", {}, clear=False):
        import os

        os.environ.pop("MANIFEST_PER_VM_SHARDS", None)
        os.environ.pop("VM_NAME", None)
        assert _validate_prerequisites() is False


def test_validate_prerequisites_passes_with_required_vars() -> None:
    env = {"MANIFEST_PER_VM_SHARDS": "true", "VM_NAME": "test-vm"}
    with patch.dict("os.environ", env):
        assert _validate_prerequisites() is True
