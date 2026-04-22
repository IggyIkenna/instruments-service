"""Tests for Transfermarkt + SFI mapping-cache write/read helpers + drift events.

Plan: ``transfermarkt_sfi_team_mapping_cache_and_drift_detection_2026_04_22``.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from io import BytesIO
from unittest.mock import MagicMock, patch

import pandas as pd

from instruments_service.engine.orchestrator import (
    _cache_is_fresh,
    _maybe_emit_drift_anomaly,
    _read_sfi_league_mapping,
    _read_transfermarkt_team_mapping,
    _sfi_mapping_blob_path,
    _transfermarkt_mapping_blob_path,
    _write_sfi_league_mapping,
    _write_transfermarkt_team_mapping,
)

# ---------------------------------------------------------------------------
# Path builders
# ---------------------------------------------------------------------------


class TestCacheBlobPaths:
    def test_transfermarkt_path_partitioned_by_season(self) -> None:
        assert _transfermarkt_mapping_blob_path(2024) == (
            "sports_reference/mappings/transfermarkt_league_teams/"
            "season=2024/teams.parquet"
        )

    def test_sfi_path_flat(self) -> None:
        assert (
            _sfi_mapping_blob_path()
            == "sports_reference/mappings/sfi_league_mapping.parquet"
        )


# ---------------------------------------------------------------------------
# Transfermarkt write
# ---------------------------------------------------------------------------


class TestWriteTransfermarktTeamMapping:
    def test_writes_parquet_with_last_fetched_at(self) -> None:
        mock_sink = MagicMock()
        with patch(
            "instruments_service.engine.orchestrator.get_data_sink",
            return_value=mock_sink,
        ):
            _write_transfermarkt_team_mapping(
                "test-bucket",
                [
                    {
                        "league_id": "EPL",
                        "canonical_league": "EPL",
                        "team_id": "ARSENAL",
                        "name": "Arsenal",
                        "squad_size": "25",
                        "player_count": "25",
                    },
                ],
                season=2024,
            )
        mock_sink.write.assert_called_once()
        kwargs = mock_sink.write.call_args.kwargs
        df = kwargs["data"]
        assert "last_fetched_at" in df.columns
        assert len(df) == 1
        assert kwargs["filename"] == "teams.parquet"
        assert kwargs["partition"]["season"] == "2024"

    def test_empty_teams_returns_without_write(self) -> None:
        mock_sink = MagicMock()
        with patch(
            "instruments_service.engine.orchestrator.get_data_sink",
            return_value=mock_sink,
        ):
            _write_transfermarkt_team_mapping("bucket", [], season=2024)
        mock_sink.write.assert_not_called()

    def test_write_failure_is_non_blocking(self) -> None:
        """Shard-isolation: cache write errors MUST NOT raise — they go through
        ``classify_and_emit_error`` like every other GCS write in the orchestrator.
        """
        mock_sink = MagicMock()
        mock_sink.write.side_effect = RuntimeError("GCS down")
        with (
            patch(
                "instruments_service.engine.orchestrator.get_data_sink",
                return_value=mock_sink,
            ),
            patch(
                "instruments_service.engine.orchestrator.classify_and_emit_error"
            ) as mock_classify,
        ):
            _write_transfermarkt_team_mapping(
                "bucket",
                [{"league_id": "EPL", "team_id": "ARSENAL"}],
                season=2024,
            )
        mock_classify.assert_called_once()


# ---------------------------------------------------------------------------
# Transfermarkt read
# ---------------------------------------------------------------------------


class TestReadTransfermarktTeamMapping:
    def test_returns_none_when_blob_absent(self) -> None:
        mock_storage = MagicMock()
        mock_storage.download_bytes.return_value = None
        with patch(
            "instruments_service.engine.orchestrator.get_storage_client",
            return_value=mock_storage,
        ):
            result = _read_transfermarkt_team_mapping("bucket", 2024)
        assert result is None

    def test_returns_dataframe_on_hit(self) -> None:
        df = pd.DataFrame(
            [{"league_id": "EPL", "team_id": "ARSENAL", "name": "Arsenal"}]
        )
        buf = BytesIO()
        df.to_parquet(buf)
        mock_storage = MagicMock()
        mock_storage.download_bytes.return_value = buf.getvalue()
        with patch(
            "instruments_service.engine.orchestrator.get_storage_client",
            return_value=mock_storage,
        ):
            result = _read_transfermarkt_team_mapping("bucket", 2024)
        assert result is not None
        assert len(result) == 1
        mock_storage.download_bytes.assert_called_once_with(
            "bucket",
            "sports_reference/mappings/transfermarkt_league_teams/"
            "season=2024/teams.parquet",
        )

    def test_returns_none_on_exception(self) -> None:
        mock_storage = MagicMock()
        mock_storage.download_bytes.side_effect = RuntimeError("boom")
        with patch(
            "instruments_service.engine.orchestrator.get_storage_client",
            return_value=mock_storage,
        ):
            result = _read_transfermarkt_team_mapping("bucket", 2024)
        assert result is None


# ---------------------------------------------------------------------------
# SFI write + read
# ---------------------------------------------------------------------------


class TestWriteSfiLeagueMapping:
    def test_writes_flat_parquet(self) -> None:
        mock_sink = MagicMock()
        with patch(
            "instruments_service.engine.orchestrator.get_data_sink",
            return_value=mock_sink,
        ):
            _write_sfi_league_mapping(
                "bucket",
                [
                    {
                        "canonical_league_id": "EPL",
                        "sfi_league_hex": "0xABC",
                        "name": "EPL",
                    }
                ],
            )
        mock_sink.write.assert_called_once()
        kwargs = mock_sink.write.call_args.kwargs
        assert kwargs["filename"] == "sfi_league_mapping.parquet"
        assert kwargs["partition"] == {}
        assert "last_fetched_at" in kwargs["data"].columns

    def test_empty_leagues_returns_without_write(self) -> None:
        mock_sink = MagicMock()
        with patch(
            "instruments_service.engine.orchestrator.get_data_sink",
            return_value=mock_sink,
        ):
            _write_sfi_league_mapping("bucket", [])
        mock_sink.write.assert_not_called()


class TestReadSfiLeagueMapping:
    def test_returns_none_when_blob_absent(self) -> None:
        mock_storage = MagicMock()
        mock_storage.download_bytes.return_value = None
        with patch(
            "instruments_service.engine.orchestrator.get_storage_client",
            return_value=mock_storage,
        ):
            result = _read_sfi_league_mapping("bucket")
        assert result is None


# ---------------------------------------------------------------------------
# Cache staleness
# ---------------------------------------------------------------------------


class TestCacheIsFresh:
    def test_fresh_cache_within_ttl(self) -> None:
        now = datetime.now(UTC)
        df = pd.DataFrame(
            [
                {"league_id": "EPL", "last_fetched_at": now.isoformat()},
                {"league_id": "LA_LIGA", "last_fetched_at": now.isoformat()},
            ]
        )
        assert _cache_is_fresh(df, timedelta(days=7)) is True

    def test_stale_cache_outside_ttl(self) -> None:
        old = datetime.now(UTC) - timedelta(days=30)
        df = pd.DataFrame(
            [{"league_id": "EPL", "last_fetched_at": old.isoformat()}]
        )
        assert _cache_is_fresh(df, timedelta(days=7)) is False

    def test_missing_column_returns_false(self) -> None:
        df = pd.DataFrame([{"league_id": "EPL"}])
        assert _cache_is_fresh(df, timedelta(days=7)) is False

    def test_empty_dataframe_returns_false(self) -> None:
        assert _cache_is_fresh(pd.DataFrame(), timedelta(days=7)) is False

    def test_mixed_freshness_uses_oldest(self) -> None:
        now = datetime.now(UTC)
        old = now - timedelta(days=30)
        df = pd.DataFrame(
            [
                {"league_id": "EPL", "last_fetched_at": now.isoformat()},
                {"league_id": "LA_LIGA", "last_fetched_at": old.isoformat()},
            ]
        )
        assert _cache_is_fresh(df, timedelta(days=7)) is False


# ---------------------------------------------------------------------------
# Drift-detection: _maybe_emit_drift_anomaly
# ---------------------------------------------------------------------------


class TestDriftAnomaly:
    def test_exact_match_emits_nothing(self) -> None:
        with patch(
            "instruments_service.engine.orchestrator.log_event"
        ) as mock_log:
            result = _maybe_emit_drift_anomaly(
                venue="transfermarkt",
                endpoint="get_teams",
                league_id="EPL",
                date="2024-09-01",
                season=2024,
                got_count=20,
                expected_count=20,
            )
        assert result is None
        mock_log.assert_not_called()

    def test_unknown_league_emits_nothing(self) -> None:
        """Absent expected-count is a silent skip, never an anomaly."""
        with patch(
            "instruments_service.engine.orchestrator.log_event"
        ) as mock_log:
            result = _maybe_emit_drift_anomaly(
                venue="transfermarkt",
                endpoint="get_teams",
                league_id="UNKNOWN_LEAGUE",
                date="2024-09-01",
                season=2024,
                got_count=15,
                expected_count=None,
            )
        assert result is None
        mock_log.assert_not_called()

    def test_zero_expected_silent_skip(self) -> None:
        with patch(
            "instruments_service.engine.orchestrator.log_event"
        ) as mock_log:
            result = _maybe_emit_drift_anomaly(
                venue="transfermarkt",
                endpoint="get_teams",
                league_id="EDGE",
                date="2024-09-01",
                season=2024,
                got_count=5,
                expected_count=0,
            )
        assert result is None
        mock_log.assert_not_called()

    def test_within_threshold_silent(self) -> None:
        with patch(
            "instruments_service.engine.orchestrator.log_event"
        ) as mock_log:
            result = _maybe_emit_drift_anomaly(
                venue="transfermarkt",
                endpoint="get_teams",
                league_id="EPL",
                date="2024-09-01",
                season=2024,
                got_count=19,  # 5% low — within 10% TM threshold
                expected_count=20,
            )
        assert result is None
        mock_log.assert_not_called()

    def test_epl_17_teams_medium_severity(self) -> None:
        """Plan test case: EPL returns 17 vs expected 20 → 15.0% MEDIUM."""
        with patch(
            "instruments_service.engine.orchestrator.log_event"
        ) as mock_log:
            result = _maybe_emit_drift_anomaly(
                venue="transfermarkt",
                endpoint="get_teams",
                league_id="EPL",
                date="2024-09-01",
                season=2024,
                got_count=17,
                expected_count=20,
            )
        assert result is not None
        assert round(result, 1) == 15.0
        mock_log.assert_called_once()
        args, kwargs = mock_log.call_args
        assert args[0] == "ADAPTER_FETCH_ANOMALY"
        details = kwargs["details"]
        assert details["league_id"] == "EPL"
        assert details["expected_count"] == 20
        assert details["got_count"] == 17
        assert details["deviation_pct"] == 15.0
        assert details["severity"] == "MEDIUM"
        assert details["season"] == 2024

    def test_epl_12_teams_high_severity(self) -> None:
        """Plan test case: EPL returns 12 teams → severity=HIGH (40% off)."""
        with patch(
            "instruments_service.engine.orchestrator.log_event"
        ) as mock_log:
            result = _maybe_emit_drift_anomaly(
                venue="transfermarkt",
                endpoint="get_teams",
                league_id="EPL",
                date="2024-09-01",
                season=2024,
                got_count=12,
                expected_count=20,
            )
        assert result is not None
        assert round(result, 1) == 40.0
        details = mock_log.call_args.kwargs["details"]
        assert details["severity"] == "HIGH"

    def test_sfi_wider_threshold(self) -> None:
        """SFI uses 15/30% threshold — 12% deviation should be silent."""
        with patch(
            "instruments_service.engine.orchestrator.log_event"
        ) as mock_log:
            result = _maybe_emit_drift_anomaly(
                venue="soccer_football_info",
                endpoint="get_leagues",
                league_id="__denominator__",
                date="2024-09-01",
                season=None,
                got_count=29,
                expected_count=33,
                threshold_pct=15.0,
                high_severity_pct=30.0,
            )
        assert result is None
        mock_log.assert_not_called()

    def test_sfi_wider_threshold_triggers_beyond_15(self) -> None:
        with patch(
            "instruments_service.engine.orchestrator.log_event"
        ) as mock_log:
            result = _maybe_emit_drift_anomaly(
                venue="soccer_football_info",
                endpoint="get_leagues",
                league_id="__denominator__",
                date="2024-09-01",
                season=None,
                got_count=27,  # ~18% off 33
                expected_count=33,
                threshold_pct=15.0,
                high_severity_pct=30.0,
            )
        assert result is not None
        assert result > 15.0
        details = mock_log.call_args.kwargs["details"]
        assert details["severity"] == "MEDIUM"
        assert "season" not in details  # SFI does not pass season
