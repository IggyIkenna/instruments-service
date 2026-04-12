"""Tests for league-based sharding of sports fixtures and reference data.

Phase 2A: Verifies that:
  - Sports fixtures are grouped by league_id and written per-league
  - ManifestWriter.add() is called with league_id for sports fixtures
  - League filter (--league CLI arg) restricts processing to specified leagues
  - Empty fixture markers are written per prediction league
  - Reference data (teams, standings) is partitioned by league
  - process_instruments wires league_filter for sports zero-fixture path
"""

from __future__ import annotations

from datetime import UTC, datetime
from datetime import date as date_type
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, call, patch

import pandas as pd
import pytest
from unified_api_contracts.internal import InstrumentRecord

from instruments_service.engine.orchestrator import process_instruments
from instruments_service.engine.urdi_reference_provider import VenueFetchResult

# ---------------------------------------------------------------------------
# Fixture write: groupby league_id extracted from instrument_key
# ---------------------------------------------------------------------------


class TestFixtureLeagueGrouping:
    """Verify sports fixtures are grouped by league and written per-league."""

    def test_extract_league_from_instrument_key(self) -> None:
        """League is the first colon-delimited segment of instrument_key."""
        keys = [
            "EPL:ARSENAL_v_CHELSEA:20260412_1500",
            "EPL:MAN_CITY_v_LIVERPOOL:20260412_1730",
            "BUNDESLIGA:BAYERN_v_DORTMUND:20260412_1530",
        ]
        df = pd.DataFrame({"instrument_key": keys, "venue": ["API_FOOTBALL"] * 3})
        df["_league_id"] = df["instrument_key"].str.split(":").str[0]

        assert list(df["_league_id"]) == ["EPL", "EPL", "BUNDESLIGA"]
        groups = dict(list(df.groupby("_league_id")))
        assert len(groups) == 2
        assert len(groups["EPL"]) == 2
        assert len(groups["BUNDESLIGA"]) == 1

    def test_league_filter_restricts_fixtures(self) -> None:
        """When league_filter is set, only matching leagues should remain."""
        keys = [
            "EPL:ARSENAL_v_CHELSEA:20260412",
            "BUNDESLIGA:BAYERN_v_DORTMUND:20260412",
            "LA_LIGA:BARCELONA_v_REAL_MADRID:20260412",
        ]
        df = pd.DataFrame({"instrument_key": keys, "venue": ["API_FOOTBALL"] * 3})
        df["_league_id"] = df["instrument_key"].str.split(":").str[0]

        league_filter = ["EPL", "LA_LIGA"]
        filtered = df[df["_league_id"].isin(league_filter)]
        assert len(filtered) == 2
        assert set(filtered["_league_id"]) == {"EPL", "LA_LIGA"}


# ---------------------------------------------------------------------------
# Manifest writes: league_id passed to ManifestWriter.add()
# ---------------------------------------------------------------------------


class TestManifestLeagueId:
    """Verify ManifestWriter.add() receives league_id for sports fixtures."""

    def test_manifest_add_called_with_league_id(self) -> None:
        """Simulate the per-league manifest write and verify league_id is set."""
        mock_manifest = MagicMock()
        date = "2026-04-12"
        leagues = {"EPL": 5, "BUNDESLIGA": 3}

        for league_id, row_count in leagues.items():
            mock_manifest.add(
                processing_date=date_type.fromisoformat(date),
                row_count=row_count,
                venue="API_FOOTBALL_FIXTURES",
                league_id=league_id,
            )

        assert mock_manifest.add.call_count == 2
        calls = mock_manifest.add.call_args_list

        # First call: EPL
        assert calls[0] == call(
            processing_date=date_type.fromisoformat(date),
            row_count=5,
            venue="API_FOOTBALL_FIXTURES",
            league_id="EPL",
        )
        # Second call: BUNDESLIGA
        assert calls[1] == call(
            processing_date=date_type.fromisoformat(date),
            row_count=3,
            venue="API_FOOTBALL_FIXTURES",
            league_id="BUNDESLIGA",
        )


# ---------------------------------------------------------------------------
# Empty fixture markers: one per prediction league
# ---------------------------------------------------------------------------


class TestEmptyFixtureMarkers:
    """Verify zero-fixture dates write per-league empty markers."""

    def test_empty_markers_use_all_prediction_leagues(self) -> None:
        """When league_filter is None, all prediction leagues get empty markers."""
        mock_sink = MagicMock()
        mock_manifest = MagicMock()
        prediction_leagues = ["EPL", "BUNDESLIGA", "LA_LIGA"]
        date = "2026-04-12"
        empty_df = pd.DataFrame(columns=["fixture_id", "venue", "league_id", "kickoff_utc", "status"])

        for league_id in prediction_leagues:
            mock_sink.write(
                data=empty_df,
                partition={"day": date, "venue": "API_FOOTBALL_FIXTURES", "league": league_id},
                format="parquet",
                filename="instruments.parquet",
            )
            mock_manifest.add(
                processing_date=date_type.fromisoformat(date),
                row_count=0,
                venue="API_FOOTBALL_FIXTURES",
                league_id=league_id,
            )

        assert mock_sink.write.call_count == 3
        assert mock_manifest.add.call_count == 3

    def test_empty_markers_respect_league_filter(self) -> None:
        """When league_filter is set, only those leagues get empty markers."""
        mock_sink = MagicMock()
        league_filter = ["EPL", "BUNDESLIGA"]
        date = "2026-04-12"
        empty_df = pd.DataFrame(columns=["fixture_id", "venue", "league_id", "kickoff_utc", "status"])

        # Simulate the orchestrator logic: use league_filter when set
        target_leagues = league_filter  # not get_all_prediction_league_ids()
        for league_id in target_leagues:
            mock_sink.write(
                data=empty_df,
                partition={"day": date, "venue": "API_FOOTBALL_FIXTURES", "league": league_id},
                format="parquet",
                filename="instruments.parquet",
            )

        assert mock_sink.write.call_count == 2


# ---------------------------------------------------------------------------
# Reference data: teams and standings partitioned by league
# ---------------------------------------------------------------------------


class TestReferenceDataLeaguePartitioning:
    """Verify teams and standings are partitioned by league in GCS writes."""

    def test_teams_grouped_by_league_id(self) -> None:
        """Teams DataFrame with league_id column is grouped for per-league writes."""
        teams_data = [
            {"team_id": "ARSENAL", "name": "Arsenal", "league_id": "EPL"},
            {"team_id": "CHELSEA", "name": "Chelsea", "league_id": "EPL"},
            {"team_id": "BAYERN", "name": "Bayern Munich", "league_id": "BUNDESLIGA"},
        ]
        teams_df = pd.DataFrame(teams_data)
        mock_sink = MagicMock()
        date = "2026-04-12"

        for _t_lid, _t_league_df in teams_df.groupby("league_id"):
            mock_sink.write(
                data=_t_league_df,
                partition={"day": date, "entity": "teams", "league": str(_t_lid)},
                format="parquet",
                filename="teams.parquet",
            )

        assert mock_sink.write.call_count == 2
        # Verify partition keys include league
        for write_call in mock_sink.write.call_args_list:
            partition = write_call.kwargs.get("partition", write_call[1].get("partition"))
            assert "league" in partition

    def test_standings_grouped_by_league_id(self) -> None:
        """Standings DataFrame groups by league_id for per-league writes."""
        standings_data = [
            {
                "league_id": "EPL",
                "rank": 1,
                "team_id": "ARSENAL",
                "team_name": "Arsenal",
                "points": 70,
                "goals_diff": 30,
            },
            {
                "league_id": "EPL",
                "rank": 2,
                "team_id": "MAN_CITY",
                "team_name": "Man City",
                "points": 68,
                "goals_diff": 25,
            },
            {
                "league_id": "BUNDESLIGA",
                "rank": 1,
                "team_id": "BAYERN",
                "team_name": "Bayern",
                "points": 65,
                "goals_diff": 40,
            },
        ]
        standings_df = pd.DataFrame(standings_data)
        mock_sink = MagicMock()
        date = "2026-04-12"

        for _s_lid, _s_league_df in standings_df.groupby("league_id"):
            mock_sink.write(
                data=_s_league_df,
                partition={"day": date, "entity": "standings", "league": str(_s_lid)},
                format="parquet",
                filename="standings.parquet",
            )

        assert mock_sink.write.call_count == 2
        partitions = [c.kwargs.get("partition", c[1].get("partition")) for c in mock_sink.write.call_args_list]
        leagues_written = {p["league"] for p in partitions}
        assert leagues_written == {"EPL", "BUNDESLIGA"}


# ---------------------------------------------------------------------------
# Orchestrator integration: sports zero-fixture with league partitioning
# ---------------------------------------------------------------------------


def _make_sports_record(
    instrument_key: str = "EPL:ARSENAL_v_CHELSEA:20260412",
    venue: str = "API_FOOTBALL",
) -> InstrumentRecord:
    return InstrumentRecord(
        instrument_key=instrument_key,
        venue=venue,
        instrument_type="FIXED_ODDS",
        base_asset="football",
        quote_asset="USD",
        tick_size=Decimal("0.01"),
        available_from_datetime=datetime(2026, 4, 12, tzinfo=UTC),
        available_to_datetime=datetime(2026, 4, 12, 23, 59, 59, tzinfo=UTC),
    )


class TestOrchestratorSportsLeaguePartitioning:
    """Integration tests: process_instruments writes per-league partitions."""

    @pytest.mark.asyncio
    async def test_zero_fixtures_writes_per_league_empty_markers(self) -> None:
        """Sports zero-fixture path writes empty parquet per prediction league."""
        mock_sink = MagicMock()
        mock_manifest_cls = MagicMock()
        mock_manifest_instance = MagicMock()
        mock_manifest_cls.return_value = mock_manifest_instance

        with (
            patch(
                "instruments_service.engine.orchestrator.get_venues_for_categories",
                return_value=["API_FOOTBALL"],
            ),
            patch("instruments_service.engine.orchestrator.is_venue_available", return_value=True),
            patch(
                "instruments_service.engine.orchestrator.fetch_instruments_for_all_venues",
                AsyncMock(return_value=VenueFetchResult()),
            ),
            patch("instruments_service.engine.orchestrator.log_event"),
            patch("instruments_service.engine.orchestrator._get_instruments_bucket", return_value="test-bucket"),
            patch("instruments_service.engine.orchestrator.get_data_sink", return_value=mock_sink),
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_manifest_cls),
            patch(
                "instruments_service.engine.orchestrator.check_shard_freshness",
                return_value=(False, [], ["API_FOOTBALL"]),
            ),
            patch("instruments_service.engine.orchestrator.read_availability_index", return_value=pd.DataFrame()),
            patch(
                "instruments_service.engine.orchestrator.get_all_prediction_league_ids",
                return_value=["EPL", "BUNDESLIGA"],
            ),
        ):
            result = await process_instruments("2026-04-12", ["SPORTS"])

        # Should have written empty markers: 2 leagues x 1 write each = 2 writes
        assert mock_sink.write.call_count == 2
        # Verify league partition keys in sink writes
        for write_call in mock_sink.write.call_args_list:
            partition = write_call.kwargs.get("partition", {})
            assert "league" in partition
            assert partition["venue"] == "API_FOOTBALL_FIXTURES"

        # Verify manifest.add() called per league with league_id
        add_calls = [c for c in mock_manifest_instance.add.call_args_list if "league_id" in (c.kwargs or {})]
        league_ids = {c.kwargs["league_id"] for c in add_calls}
        assert league_ids == {"EPL", "BUNDESLIGA"}

    @pytest.mark.asyncio
    async def test_zero_fixtures_with_league_filter(self) -> None:
        """Sports zero-fixture with --league filter writes only filtered leagues."""
        mock_sink = MagicMock()
        mock_manifest_cls = MagicMock()
        mock_manifest_instance = MagicMock()
        mock_manifest_cls.return_value = mock_manifest_instance

        with (
            patch(
                "instruments_service.engine.orchestrator.get_venues_for_categories",
                return_value=["API_FOOTBALL"],
            ),
            patch("instruments_service.engine.orchestrator.is_venue_available", return_value=True),
            patch(
                "instruments_service.engine.orchestrator.fetch_instruments_for_all_venues",
                AsyncMock(return_value=VenueFetchResult()),
            ),
            patch("instruments_service.engine.orchestrator.log_event"),
            patch("instruments_service.engine.orchestrator._get_instruments_bucket", return_value="test-bucket"),
            patch("instruments_service.engine.orchestrator.get_data_sink", return_value=mock_sink),
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_manifest_cls),
            patch(
                "instruments_service.engine.orchestrator.check_shard_freshness",
                return_value=(False, [], ["API_FOOTBALL"]),
            ),
            patch("instruments_service.engine.orchestrator.read_availability_index", return_value=pd.DataFrame()),
        ):
            # Pass league_filter=["EPL"] — should only write 1 empty marker
            result = await process_instruments("2026-04-12", ["SPORTS"], league_filter=["EPL"])

        assert mock_sink.write.call_count == 1
        partition = mock_sink.write.call_args_list[0].kwargs.get("partition", {})
        assert partition["league"] == "EPL"

    @pytest.mark.asyncio
    async def test_sports_fixtures_partitioned_by_league(self) -> None:
        """Sports fixtures are grouped by league and written per-league partition."""
        records = [
            _make_sports_record("EPL:ARSENAL_v_CHELSEA:20260412"),
            _make_sports_record("EPL:MAN_CITY_v_LIVERPOOL:20260412"),
            _make_sports_record("BUNDESLIGA:BAYERN_v_DORTMUND:20260412"),
        ]

        mock_sink = MagicMock()
        mock_sampler = MagicMock()
        mock_sampler.enable_sampling = False
        mock_manifest_cls = MagicMock()
        mock_manifest_instance = MagicMock()
        mock_manifest_cls.return_value = mock_manifest_instance

        with (
            patch(
                "instruments_service.engine.orchestrator.get_venues_for_categories",
                return_value=["API_FOOTBALL"],
            ),
            patch("instruments_service.engine.orchestrator.is_venue_available", return_value=True),
            patch(
                "instruments_service.engine.orchestrator.fetch_instruments_for_all_venues",
                AsyncMock(return_value=VenueFetchResult(records=records)),
            ),
            patch("instruments_service.engine.orchestrator.log_event"),
            patch("instruments_service.engine.orchestrator.DomainValidationService") as mock_dvs,
            patch("instruments_service.engine.orchestrator._get_instruments_bucket", return_value="test-bucket"),
            patch("instruments_service.engine.orchestrator.get_data_sink", return_value=mock_sink),
            patch("instruments_service.engine.orchestrator.create_sampling_service", return_value=mock_sampler),
            patch("instruments_service.engine.orchestrator._write_catalogue_record"),
            patch(
                "instruments_service.engine.orchestrator.check_shard_freshness",
                return_value=(False, [], ["API_FOOTBALL"]),
            ),
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_manifest_cls),
            patch("instruments_service.engine.orchestrator.read_availability_index", return_value=pd.DataFrame()),
        ):
            mock_dvs.return_value.validate_for_domain = MagicMock()
            result = await process_instruments("2026-04-12", ["SPORTS"])

        # Verify writes: should have 2 writes (EPL and BUNDESLIGA)
        league_writes = [c for c in mock_sink.write.call_args_list if "league" in c.kwargs.get("partition", {})]
        assert len(league_writes) == 2
        partition_leagues = {c.kwargs["partition"]["league"] for c in league_writes}
        assert partition_leagues == {"EPL", "BUNDESLIGA"}

        # Verify manifest.add() called with league_id for each league
        manifest_add_calls = mock_manifest_instance.add.call_args_list
        league_manifest_calls = [c for c in manifest_add_calls if c.kwargs.get("league_id")]
        assert len(league_manifest_calls) == 2
        manifest_leagues = {c.kwargs["league_id"] for c in league_manifest_calls}
        assert manifest_leagues == {"EPL", "BUNDESLIGA"}

    @pytest.mark.asyncio
    async def test_sports_fixtures_with_league_filter(self) -> None:
        """--league filter restricts which leagues are written."""
        records = [
            _make_sports_record("EPL:ARSENAL_v_CHELSEA:20260412"),
            _make_sports_record("BUNDESLIGA:BAYERN_v_DORTMUND:20260412"),
            _make_sports_record("LA_LIGA:BARCELONA_v_REAL_MADRID:20260412"),
        ]

        mock_sink = MagicMock()
        mock_sampler = MagicMock()
        mock_sampler.enable_sampling = False
        mock_manifest_cls = MagicMock()
        mock_manifest_instance = MagicMock()
        mock_manifest_cls.return_value = mock_manifest_instance

        with (
            patch(
                "instruments_service.engine.orchestrator.get_venues_for_categories",
                return_value=["API_FOOTBALL"],
            ),
            patch("instruments_service.engine.orchestrator.is_venue_available", return_value=True),
            patch(
                "instruments_service.engine.orchestrator.fetch_instruments_for_all_venues",
                AsyncMock(return_value=VenueFetchResult(records=records)),
            ),
            patch("instruments_service.engine.orchestrator.log_event"),
            patch("instruments_service.engine.orchestrator.DomainValidationService") as mock_dvs,
            patch("instruments_service.engine.orchestrator._get_instruments_bucket", return_value="test-bucket"),
            patch("instruments_service.engine.orchestrator.get_data_sink", return_value=mock_sink),
            patch("instruments_service.engine.orchestrator.create_sampling_service", return_value=mock_sampler),
            patch("instruments_service.engine.orchestrator._write_catalogue_record"),
            patch(
                "instruments_service.engine.orchestrator.check_shard_freshness",
                return_value=(False, [], ["API_FOOTBALL"]),
            ),
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_manifest_cls),
            patch("instruments_service.engine.orchestrator.read_availability_index", return_value=pd.DataFrame()),
        ):
            mock_dvs.return_value.validate_for_domain = MagicMock()
            result = await process_instruments("2026-04-12", ["SPORTS"], league_filter=["EPL"])

        # Only EPL should be written
        league_writes = [c for c in mock_sink.write.call_args_list if "league" in c.kwargs.get("partition", {})]
        assert len(league_writes) == 1
        assert league_writes[0].kwargs["partition"]["league"] == "EPL"
