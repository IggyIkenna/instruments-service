"""Unit tests for G1.2 thin-day partial-capture detection in process_completeness."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from instruments_service.engine.orchestrator.process_completeness import (
    _THIN_DAY_ABS_FLOOR,
    _THIN_DAY_FRACTION,
    _THIN_DAY_MIN_HISTORY,
    _THIN_DAY_WINDOW,
    _detect_thin_day_venues,
)


def _make_index(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


@pytest.fixture()
def cefi_history_normal() -> pd.DataFrame:
    """14 days of BINANCE-FUTURES at 678 instruments (healthy baseline)."""
    rows = [
        {
            "asset_group": "cefi",
            "capture_status": "captured",
            "venue": "BINANCE-FUTURES",
            "date": f"2026-06-{d:02d}",
            "instrument_count": 678,
        }
        for d in range(1, 15)
    ]
    return _make_index(rows)


class TestDetectThinDayVenues:
    def test_empty_written_venues_returns_empty(self) -> None:
        result = _detect_thin_day_venues(
            counts={},
            written_venues=set(),
            date="2026-06-26",
            bucket="test-bucket",
        )
        assert result == set()

    def test_index_load_failure_returns_empty(self) -> None:
        with patch("instruments_service.engine.orchestrator.process_completeness._orch") as mock_orch:
            mock_orch.read_availability_index.side_effect = Exception("GCS unavailable")
            mock_orch.logger = MagicMock()
            result = _detect_thin_day_venues(
                counts={"BINANCE-FUTURES": 47},
                written_venues={"BINANCE-FUTURES"},
                date="2026-06-26",
                bucket="test-bucket",
            )
        assert result == set()

    def test_thin_day_detected_below_fraction(self, cefi_history_normal: pd.DataFrame) -> None:
        # 47 / 678 ≈ 6.9% — well below the 50% threshold
        with patch("instruments_service.engine.orchestrator.process_completeness._orch") as mock_orch:
            mock_orch.read_availability_index.return_value = cefi_history_normal
            mock_orch.logger = MagicMock()
            result = _detect_thin_day_venues(
                counts={"BINANCE-FUTURES": 47},
                written_venues={"BINANCE-FUTURES"},
                date="2026-06-26",
                bucket="test-bucket",
            )
        assert result == {"BINANCE-FUTURES"}

    def test_healthy_day_not_flagged(self, cefi_history_normal: pd.DataFrame) -> None:
        # 678 / 678 = 100% — not thin
        with patch("instruments_service.engine.orchestrator.process_completeness._orch") as mock_orch:
            mock_orch.read_availability_index.return_value = cefi_history_normal
            mock_orch.logger = MagicMock()
            result = _detect_thin_day_venues(
                counts={"BINANCE-FUTURES": 678},
                written_venues={"BINANCE-FUTURES"},
                date="2026-06-26",
                bucket="test-bucket",
            )
        assert result == set()

    def test_borderline_exactly_at_threshold_not_flagged(self, cefi_history_normal: pd.DataFrame) -> None:
        # median=678; threshold=339; count=339 → NOT flagged (strict <, not <=)
        with patch("instruments_service.engine.orchestrator.process_completeness._orch") as mock_orch:
            mock_orch.read_availability_index.return_value = cefi_history_normal
            mock_orch.logger = MagicMock()
            result = _detect_thin_day_venues(
                counts={"BINANCE-FUTURES": 339},
                written_venues={"BINANCE-FUTURES"},
                date="2026-06-26",
                bucket="test-bucket",
            )
        assert result == set()

    def test_count_below_abs_floor_skipped(self, cefi_history_normal: pd.DataFrame) -> None:
        # count=5 < _THIN_DAY_ABS_FLOOR=20 → skip even if ratio is terrible
        with patch("instruments_service.engine.orchestrator.process_completeness._orch") as mock_orch:
            mock_orch.read_availability_index.return_value = cefi_history_normal
            mock_orch.logger = MagicMock()
            result = _detect_thin_day_venues(
                counts={"BINANCE-FUTURES": _THIN_DAY_ABS_FLOOR - 1},
                written_venues={"BINANCE-FUTURES"},
                date="2026-06-26",
                bucket="test-bucket",
            )
        assert result == set()

    def test_no_cefi_history_skips_venue(self) -> None:
        # Venue present in written_venues but no CeFi history in index → skip
        idx = _make_index(
            [
                {
                    "asset_group": "tradfi",
                    "capture_status": "captured",
                    "venue": "BINANCE-FUTURES",
                    "date": "2026-06-01",
                    "instrument_count": 678,
                }
            ]
        )
        with patch("instruments_service.engine.orchestrator.process_completeness._orch") as mock_orch:
            mock_orch.read_availability_index.return_value = idx
            mock_orch.logger = MagicMock()
            result = _detect_thin_day_venues(
                counts={"BINANCE-FUTURES": 47},
                written_venues={"BINANCE-FUTURES"},
                date="2026-06-26",
                bucket="test-bucket",
            )
        assert result == set()

    def test_insufficient_history_skips_venue(self) -> None:
        # Only 1 prior day — below _THIN_DAY_MIN_HISTORY=2
        idx = _make_index(
            [
                {
                    "asset_group": "cefi",
                    "capture_status": "captured",
                    "venue": "BINANCE-FUTURES",
                    "date": "2026-06-25",
                    "instrument_count": 678,
                }
            ]
        )
        with patch("instruments_service.engine.orchestrator.process_completeness._orch") as mock_orch:
            mock_orch.read_availability_index.return_value = idx
            mock_orch.logger = MagicMock()
            result = _detect_thin_day_venues(
                counts={"BINANCE-FUTURES": 47},
                written_venues={"BINANCE-FUTURES"},
                date="2026-06-26",
                bucket="test-bucket",
            )
        assert result == set()

    def test_today_date_excluded_from_history(self) -> None:
        # Index contains a row for today's date — must not be included in median
        rows = [
            {
                "asset_group": "cefi",
                "capture_status": "captured",
                "venue": "BINANCE-FUTURES",
                "date": f"2026-06-{d:02d}",
                "instrument_count": 678 if d < 26 else 47,  # today=47
            }
            for d in range(12, 27)  # 12..26 inclusive
        ]
        idx = _make_index(rows)
        with patch("instruments_service.engine.orchestrator.process_completeness._orch") as mock_orch:
            mock_orch.read_availability_index.return_value = idx
            mock_orch.logger = MagicMock()
            result = _detect_thin_day_venues(
                counts={"BINANCE-FUTURES": 47},
                written_venues={"BINANCE-FUTURES"},
                date="2026-06-26",
                bucket="test-bucket",
            )
        # 47/678 ≈ 6.9% < 50% → thin
        assert result == {"BINANCE-FUTURES"}

    def test_multiple_venues_only_thin_flagged(self, cefi_history_normal: pd.DataFrame) -> None:
        # Add BINANCE-SPOT at normal counts
        extra_rows = list(cefi_history_normal.to_dict("records")) + [
            {
                "asset_group": "cefi",
                "capture_status": "captured",
                "venue": "BINANCE-SPOT",
                "date": f"2026-06-{d:02d}",
                "instrument_count": 400,
            }
            for d in range(1, 15)
        ]
        idx = _make_index(extra_rows)
        with patch("instruments_service.engine.orchestrator.process_completeness._orch") as mock_orch:
            mock_orch.read_availability_index.return_value = idx
            mock_orch.logger = MagicMock()
            result = _detect_thin_day_venues(
                counts={"BINANCE-FUTURES": 47, "BINANCE-SPOT": 400},
                written_venues={"BINANCE-FUTURES", "BINANCE-SPOT"},
                date="2026-06-26",
                bucket="test-bucket",
            )
        assert result == {"BINANCE-FUTURES"}  # SPOT is healthy (400/400=100%)
