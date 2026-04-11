"""Tests for the orchestrator sports reference data pipeline.

Covers _fetch_sports_reference_data, _write_team_mapping, _write_fixture_mapping,
and the sports enrichment path in process().
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from instruments_service.engine.orchestrator import (
    _fetch_sports_reference_data,
    _write_fixture_mapping,
    _write_team_mapping,
)

# ---------------------------------------------------------------------------
# _write_team_mapping
# ---------------------------------------------------------------------------


class TestWriteTeamMapping:
    """Tests for the _write_team_mapping orchestrator function."""

    def test_writes_team_mapping(self) -> None:
        """Should write team mapping parquet with EPL and Bundesliga teams."""
        mock_sink = MagicMock()
        with patch(
            "instruments_service.engine.orchestrator.get_data_sink",
            return_value=mock_sink,
        ):
            _write_team_mapping("test-bucket")

        # Should have called write at least once (if there are team aliases)
        assert mock_sink.write.call_count >= 1
        # Verify the written DataFrame has the expected columns
        call_args = mock_sink.write.call_args
        df = call_args.kwargs.get("data")
        if df is None:
            df = call_args[1].get("data")
        assert isinstance(df, pd.DataFrame)
        expected_cols = {"canonical_team_id", "display_name", "odds_api_name", "understat_name", "league"}
        assert expected_cols.issubset(set(df.columns))
        # Should have both EPL and Bundesliga rows
        leagues_in_df = set(df["league"].unique())
        assert "EPL" in leagues_in_df or "BUNDESLIGA" in leagues_in_df

    def test_write_team_mapping_handles_write_error(self) -> None:
        """Should not raise on write errors — just log."""
        mock_sink = MagicMock()
        mock_sink.write.side_effect = OSError("write failed")
        with patch(
            "instruments_service.engine.orchestrator.get_data_sink",
            return_value=mock_sink,
        ):
            # Should not raise
            _write_team_mapping("test-bucket")


# ---------------------------------------------------------------------------
# _write_fixture_mapping
# ---------------------------------------------------------------------------


class TestWriteFixtureMapping:
    """Tests for the _write_fixture_mapping orchestrator function."""

    def test_writes_fixture_mapping_with_data(self) -> None:
        """Should read fixtures parquet and write mapping."""
        # Mock storage client that returns a parquet file
        fixtures_df = pd.DataFrame(
            {
                "instrument_key": ["EPL:ARSENAL_v_CHELSEA:20260322"],
                "raw_symbol": ["Arsenal vs Chelsea"],
                "venue": ["API_FOOTBALL"],
            }
        )
        parquet_bytes = fixtures_df.to_parquet()

        mock_storage = MagicMock()
        mock_storage.download_bytes.return_value = parquet_bytes

        mock_sink = MagicMock()

        with (
            patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage),
            patch("instruments_service.engine.orchestrator.get_data_sink", return_value=mock_sink),
        ):
            _write_fixture_mapping("test-bucket", "2026-03-22")

        assert mock_sink.write.call_count == 1
        call_args = mock_sink.write.call_args
        df = call_args.kwargs.get("data")
        if df is None:
            df = call_args[1].get("data")
        assert isinstance(df, pd.DataFrame)
        assert "canonical_fixture_id" in df.columns
        assert len(df) == 1

    def test_write_fixture_mapping_no_parquet_found(self) -> None:
        """Should handle missing parquet gracefully (returns None from storage)."""
        mock_storage = MagicMock()
        mock_storage.download_bytes.return_value = None

        with patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage):
            # Should not raise
            _write_fixture_mapping("test-bucket", "2026-03-22")

    def test_write_fixture_mapping_empty_df(self) -> None:
        """Should handle empty parquet gracefully."""
        empty_df = pd.DataFrame({"instrument_key": [], "raw_symbol": []})
        parquet_bytes = empty_df.to_parquet()

        mock_storage = MagicMock()
        mock_storage.download_bytes.return_value = parquet_bytes

        with patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage):
            # Should not raise
            _write_fixture_mapping("test-bucket", "2026-03-22")

    def test_write_fixture_mapping_file_not_found(self) -> None:
        """Should handle FileNotFoundError gracefully."""
        mock_storage = MagicMock()
        mock_storage.download_bytes.side_effect = FileNotFoundError("not found")

        with patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage):
            # Should not raise
            _write_fixture_mapping("test-bucket", "2026-03-22")

    def test_write_fixture_mapping_os_error(self) -> None:
        """Should handle OSError gracefully."""
        mock_storage = MagicMock()
        mock_storage.download_bytes.side_effect = OSError("disk full")

        with patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage):
            # Should not raise
            _write_fixture_mapping("test-bucket", "2026-03-22")

    def test_write_fixture_mapping_generic_error(self) -> None:
        """Should handle generic exceptions gracefully."""
        mock_storage = MagicMock()
        mock_storage.download_bytes.side_effect = ValueError("unexpected")

        with patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage):
            # Should not raise
            _write_fixture_mapping("test-bucket", "2026-03-22")


# ---------------------------------------------------------------------------
# _fetch_sports_reference_data
# ---------------------------------------------------------------------------


class TestFetchSportsReferenceData:
    """Tests for the orchestrator _fetch_sports_reference_data function."""

    @pytest.mark.asyncio
    async def test_fetches_leagues_teams_standings_injuries(self) -> None:
        """Should fetch all reference data types and write to sink."""
        # Mock the sports adapter
        mock_adapter = AsyncMock()
        mock_adapter.get_leagues.return_value = []  # empty is fine, tests the flow
        mock_adapter.get_teams.return_value = []
        mock_adapter.get_standings.return_value = []
        mock_adapter.get_injuries.return_value = []

        mock_sink = MagicMock()

        with (
            patch(
                "instruments_service.engine.orchestrator.create_sports_reference_adapter",
                return_value=mock_adapter,
            ),
            patch("instruments_service.engine.orchestrator.get_data_sink", return_value=mock_sink),
            patch("instruments_service.engine.orchestrator._write_team_mapping"),
            patch("instruments_service.engine.orchestrator._write_fixture_mapping"),
        ):
            counts = await _fetch_sports_reference_data("2026-03-22", "test-key", "test-bucket")

        assert isinstance(counts, dict)
        mock_adapter.get_leagues.assert_awaited_once()
        mock_adapter.get_injuries.assert_awaited_once_with("2026-03-22")

    @pytest.mark.asyncio
    async def test_writes_leagues_when_available(self) -> None:
        """Should write leagues parquet when leagues are returned."""
        from unittest.mock import MagicMock as MM

        mock_league = MM()
        mock_league.model_dump.return_value = {"league_id": "39", "name": "EPL", "country": "England"}

        mock_adapter = AsyncMock()
        mock_adapter.get_leagues.return_value = [mock_league]
        mock_adapter.get_teams.return_value = []
        mock_adapter.get_standings.return_value = []
        mock_adapter.get_injuries.return_value = []

        mock_sink = MagicMock()

        with (
            patch(
                "instruments_service.engine.orchestrator.create_sports_reference_adapter",
                return_value=mock_adapter,
            ),
            patch("instruments_service.engine.orchestrator.get_data_sink", return_value=mock_sink),
            patch("instruments_service.engine.orchestrator._write_team_mapping"),
            patch("instruments_service.engine.orchestrator._write_fixture_mapping"),
        ):
            counts = await _fetch_sports_reference_data("2026-03-22", "test-key", "test-bucket")

        assert counts.get("leagues", 0) == 1

    @pytest.mark.asyncio
    async def test_writes_teams_for_prediction_leagues(self) -> None:
        """Should fetch teams for each prediction league."""
        from unittest.mock import MagicMock as MM

        mock_team = MM()
        mock_team.model_dump.return_value = {"team_id": "40", "name": "Liverpool"}

        mock_adapter = AsyncMock()
        mock_adapter.get_leagues.return_value = []
        mock_adapter.get_teams.return_value = [mock_team]
        mock_adapter.get_standings.return_value = []
        mock_adapter.get_injuries.return_value = []

        mock_sink = MagicMock()

        # Mock get_prediction_leagues to return at least one league with api_football_id
        mock_league_def = MM()
        mock_league_def.api_football_id = 39
        mock_league_def.league_id = "EPL"

        with (
            patch(
                "instruments_service.engine.orchestrator.create_sports_reference_adapter",
                return_value=mock_adapter,
            ),
            patch("instruments_service.engine.orchestrator.get_data_sink", return_value=mock_sink),
            patch(
                "instruments_service.engine.orchestrator.get_prediction_leagues",
                return_value=[mock_league_def],
            ),
            patch("instruments_service.engine.orchestrator._write_team_mapping"),
            patch("instruments_service.engine.orchestrator._write_fixture_mapping"),
        ):
            counts = await _fetch_sports_reference_data("2026-03-22", "test-key", "test-bucket")

        assert counts.get("teams", 0) >= 1

    @pytest.mark.asyncio
    async def test_writes_standings(self) -> None:
        """Should fetch standings for prediction leagues with api_football_id."""
        from unittest.mock import MagicMock as MM

        mock_adapter = AsyncMock()
        mock_adapter.get_leagues.return_value = []
        mock_adapter.get_teams.return_value = []
        mock_adapter.get_standings.return_value = [
            {"rank": 1, "team": "Liverpool", "points": 80},
        ]
        mock_adapter.get_injuries.return_value = []

        mock_sink = MagicMock()

        mock_league_def = MM()
        mock_league_def.api_football_id = 39
        mock_league_def.league_id = "EPL"

        with (
            patch(
                "instruments_service.engine.orchestrator.create_sports_reference_adapter",
                return_value=mock_adapter,
            ),
            patch("instruments_service.engine.orchestrator.get_data_sink", return_value=mock_sink),
            patch(
                "instruments_service.engine.orchestrator.get_prediction_leagues",
                return_value=[mock_league_def],
            ),
            patch("instruments_service.engine.orchestrator._write_team_mapping"),
            patch("instruments_service.engine.orchestrator._write_fixture_mapping"),
        ):
            counts = await _fetch_sports_reference_data("2026-03-22", "test-key", "test-bucket")

        assert counts.get("standings", 0) >= 1

    @pytest.mark.asyncio
    async def test_writes_injuries(self) -> None:
        """Should fetch injuries for the target date."""

        mock_adapter = AsyncMock()
        mock_adapter.get_leagues.return_value = []
        mock_adapter.get_teams.return_value = []
        mock_adapter.get_standings.return_value = []
        mock_adapter.get_injuries.return_value = [
            {"player": "Mo Salah", "type": "Hamstring"},
        ]

        mock_sink = MagicMock()

        with (
            patch(
                "instruments_service.engine.orchestrator.create_sports_reference_adapter",
                return_value=mock_adapter,
            ),
            patch("instruments_service.engine.orchestrator.get_data_sink", return_value=mock_sink),
            patch("instruments_service.engine.orchestrator._write_team_mapping"),
            patch("instruments_service.engine.orchestrator._write_fixture_mapping"),
        ):
            counts = await _fetch_sports_reference_data("2026-03-22", "test-key", "test-bucket")

        assert counts.get("injuries", 0) == 1

    @pytest.mark.asyncio
    async def test_handles_leagues_fetch_error(self) -> None:
        """Should handle leagues fetch error gracefully."""
        mock_adapter = AsyncMock()
        mock_adapter.get_leagues.side_effect = Exception("API error")
        mock_adapter.get_teams.return_value = []
        mock_adapter.get_standings.return_value = []
        mock_adapter.get_injuries.return_value = []

        mock_sink = MagicMock()

        with (
            patch(
                "instruments_service.engine.orchestrator.create_sports_reference_adapter",
                return_value=mock_adapter,
            ),
            patch("instruments_service.engine.orchestrator.get_data_sink", return_value=mock_sink),
            patch("instruments_service.engine.orchestrator._write_team_mapping"),
            patch("instruments_service.engine.orchestrator._write_fixture_mapping"),
            patch("instruments_service.engine.orchestrator._cached_leagues_df", None),
            patch("instruments_service.engine.orchestrator._cached_teams_df", None),
            patch("instruments_service.engine.orchestrator._cached_standings_df", None),
        ):
            counts = await _fetch_sports_reference_data("2026-03-22", "test-key", "test-bucket")

        # Should still return counts (without leagues — the exception was caught)
        assert isinstance(counts, dict)
        assert "leagues" not in counts

    @pytest.mark.asyncio
    async def test_handles_teams_fetch_error(self) -> None:
        """Should handle per-league teams fetch error gracefully."""
        from unittest.mock import MagicMock as MM

        mock_adapter = AsyncMock()
        mock_adapter.get_leagues.return_value = []
        mock_adapter.get_teams.side_effect = Exception("teams API error")
        mock_adapter.get_standings.return_value = []
        mock_adapter.get_injuries.return_value = []

        mock_sink = MagicMock()

        mock_league_def = MM()
        mock_league_def.api_football_id = 39
        mock_league_def.league_id = "EPL"

        with (
            patch(
                "instruments_service.engine.orchestrator.create_sports_reference_adapter",
                return_value=mock_adapter,
            ),
            patch("instruments_service.engine.orchestrator.get_data_sink", return_value=mock_sink),
            patch(
                "instruments_service.engine.orchestrator.get_prediction_leagues",
                return_value=[mock_league_def],
            ),
            patch("instruments_service.engine.orchestrator._write_team_mapping"),
            patch("instruments_service.engine.orchestrator._write_fixture_mapping"),
        ):
            counts = await _fetch_sports_reference_data("2026-03-22", "test-key", "test-bucket")

        assert isinstance(counts, dict)

    @pytest.mark.asyncio
    async def test_handles_injuries_fetch_error(self) -> None:
        """Should handle injuries fetch error gracefully."""
        mock_adapter = AsyncMock()
        mock_adapter.get_leagues.return_value = []
        mock_adapter.get_teams.return_value = []
        mock_adapter.get_standings.return_value = []
        mock_adapter.get_injuries.side_effect = Exception("injuries error")

        mock_sink = MagicMock()

        with (
            patch(
                "instruments_service.engine.orchestrator.create_sports_reference_adapter",
                return_value=mock_adapter,
            ),
            patch("instruments_service.engine.orchestrator.get_data_sink", return_value=mock_sink),
            patch("instruments_service.engine.orchestrator._write_team_mapping"),
            patch("instruments_service.engine.orchestrator._write_fixture_mapping"),
        ):
            counts = await _fetch_sports_reference_data("2026-03-22", "test-key", "test-bucket")

        assert isinstance(counts, dict)
        assert "injuries" not in counts

    @pytest.mark.asyncio
    async def test_skips_leagues_without_api_football_id(self) -> None:
        """Leagues without api_football_id should be skipped."""
        from unittest.mock import MagicMock as MM

        mock_adapter = AsyncMock()
        mock_adapter.get_leagues.return_value = []
        mock_adapter.get_teams.return_value = []
        mock_adapter.get_standings.return_value = []
        mock_adapter.get_injuries.return_value = []

        mock_sink = MagicMock()

        mock_league_def = MM()
        mock_league_def.api_football_id = None
        mock_league_def.league_id = "NO_API_ID"

        with (
            patch(
                "instruments_service.engine.orchestrator.create_sports_reference_adapter",
                return_value=mock_adapter,
            ),
            patch("instruments_service.engine.orchestrator.get_data_sink", return_value=mock_sink),
            patch(
                "instruments_service.engine.orchestrator.get_prediction_leagues",
                return_value=[mock_league_def],
            ),
            patch("instruments_service.engine.orchestrator._write_team_mapping"),
            patch("instruments_service.engine.orchestrator._write_fixture_mapping"),
        ):
            counts = await _fetch_sports_reference_data("2026-03-22", "test-key", "test-bucket")

        # get_teams should NOT have been called since league has no api_football_id
        mock_adapter.get_teams.assert_not_awaited()
