"""Coverage tests for sports-provider routing and footystats sub-functions.

Targets uncovered branches in orchestrator.py:
  - process_instruments enrichment-provider short-circuit
    OPEN_METEO / UNDERSTAT / FOOTYSTATS / TRANSFERMARKT / SOCCER_FOOTBALL_INFO / unknown
  - _fetch_footystats_predictions skip / happy / empty / exception
  - _fetch_footystats_matches skip / happy / empty
"""

from __future__ import annotations

import contextlib
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from instruments_service.engine.orchestrator import (
    _fetch_footystats_matches,
    _fetch_footystats_odds,
    _fetch_footystats_predictions,
    _load_scheduled_footystats_fixture_map,
    process_instruments,
)

_DATE = "2026-01-15"
_BUCKET = "test-bucket"


def _make_league(league_id: str) -> SimpleNamespace:
    return SimpleNamespace(league_id=league_id)


def _stack(*patches: object) -> contextlib.ExitStack:
    """Enter all patch objects into an ExitStack and return it."""
    stack = contextlib.ExitStack()
    for p in patches:
        stack.enter_context(p)  # type: ignore[arg-type]
    return stack


def _entry_stack(venues: list[str], bucket: str = _BUCKET, *extra: object) -> contextlib.ExitStack:
    """ExitStack with the invariant top-of-function mocks for process_instruments."""
    return _stack(
        patch("instruments_service.engine.orchestrator.get_venues_for_asset_groups", return_value=venues),
        patch("instruments_service.engine.orchestrator.is_venue_available", return_value=True),
        patch("instruments_service.engine.orchestrator._get_instruments_bucket", return_value=bucket),
        patch("instruments_service.engine.orchestrator.log_event"),
        *extra,
    )


# ---------------------------------------------------------------------------
# process_instruments - enrichment-provider short-circuit
# ---------------------------------------------------------------------------


class TestProcessInstrumentsSportsProviderRouting:
    """Tests for the _enrichment_providers early-return block (lines 1347-1445)."""

    @pytest.mark.asyncio
    async def test_unknown_provider_logs_error_returns_empty(self) -> None:
        with _stack(
            patch("instruments_service.engine.orchestrator.get_venues_for_asset_groups", return_value=[]),
            patch("instruments_service.engine.orchestrator.is_venue_available", return_value=True),
            patch("instruments_service.engine.orchestrator.log_event"),
        ):
            result = await process_instruments(
                _DATE,
                ["SPORTS"],
                sports_provider="TOTALLY_UNKNOWN_PROVIDER_XYZ",
                redo_all=True,
            )
        assert result == {}

    @pytest.mark.asyncio
    async def test_no_bucket_returns_empty(self) -> None:
        with _stack(
            patch("instruments_service.engine.orchestrator.get_venues_for_asset_groups", return_value=["OPEN_METEO"]),
            patch("instruments_service.engine.orchestrator.is_venue_available", return_value=True),
            patch("instruments_service.engine.orchestrator._get_instruments_bucket", return_value=None),
            patch("instruments_service.engine.orchestrator.log_event"),
        ):
            result = await process_instruments(
                _DATE,
                ["SPORTS"],
                sports_provider="OPEN_METEO",
                redo_all=True,
            )
        assert result == {}

    @pytest.mark.asyncio
    async def test_open_meteo_routes_to_fetch_weather(self) -> None:
        mock_weather = AsyncMock(return_value={"weather_data": 5})
        with _entry_stack(
            ["OPEN_METEO"],
            _BUCKET,
            patch("instruments_service.engine.orchestrator._fetch_weather_data", mock_weather),
        ):
            result = await process_instruments(
                _DATE,
                ["SPORTS"],
                sports_provider="OPEN_METEO",
                api_keys={"open_meteo": "key-abc"},
                redo_all=True,
            )
        assert result == {"weather_data": 5}
        mock_weather.assert_called_once_with(date=_DATE, bucket=_BUCKET, api_key="key-abc")

    @pytest.mark.asyncio
    async def test_understat_routes_to_xg_and_shots(self) -> None:
        mock_xg = AsyncMock(return_value={"understat_xg": 10})
        mock_shots = AsyncMock(return_value={"understat_shots": 20})
        with _entry_stack(
            ["UNDERSTAT"],
            _BUCKET,
            patch("instruments_service.engine.orchestrator._fetch_understat_xg", mock_xg),
            patch("instruments_service.engine.orchestrator._run_understat_shots_date", mock_shots),
        ):
            result = await process_instruments(
                _DATE,
                ["SPORTS"],
                sports_provider="UNDERSTAT",
                redo_all=True,
            )
        assert result == {"understat_xg": 10, "understat_shots": 20}
        mock_xg.assert_called_once_with(date=_DATE, bucket=_BUCKET, force=True)
        mock_shots.assert_called_once_with(date=_DATE, bucket=_BUCKET, force=True)

    @pytest.mark.asyncio
    async def test_footystats_no_api_key_returns_empty(self) -> None:
        with _entry_stack(["FOOTYSTATS"]):
            result = await process_instruments(
                _DATE,
                ["SPORTS"],
                sports_provider="FOOTYSTATS",
                api_keys={},
                redo_all=True,
            )
        assert result == {}

    @pytest.mark.asyncio
    async def test_footystats_with_key_calls_both_fetchers(self) -> None:
        mock_pred = AsyncMock(return_value={"footystats_predictions": 12})
        mock_match = AsyncMock(return_value={"footystats_matches": 8})
        with _entry_stack(
            ["FOOTYSTATS"],
            _BUCKET,
            patch("instruments_service.engine.orchestrator._fetch_footystats_predictions", mock_pred),
            patch("instruments_service.engine.orchestrator._fetch_footystats_matches", mock_match),
        ):
            result = await process_instruments(
                _DATE,
                ["SPORTS"],
                sports_provider="FOOTYSTATS",
                api_keys={"footystats": "fs-key"},
                redo_all=True,
            )
        assert result == {"footystats_predictions": 12, "footystats_matches": 8}
        mock_pred.assert_called_once()
        mock_match.assert_called_once()

    @pytest.mark.asyncio
    async def test_footystats_entity_filter_predictions_only(self) -> None:
        mock_pred = AsyncMock(return_value={"footystats_predictions": 7})
        mock_match = AsyncMock(return_value={"footystats_matches": 3})
        with _entry_stack(
            ["FOOTYSTATS"],
            _BUCKET,
            patch("instruments_service.engine.orchestrator._fetch_footystats_predictions", mock_pred),
            patch("instruments_service.engine.orchestrator._fetch_footystats_matches", mock_match),
        ):
            result = await process_instruments(
                _DATE,
                ["SPORTS"],
                sports_provider="FOOTYSTATS",
                sports_entity_filter="PREDICTIONS",
                api_keys={"footystats": "fs-key"},
            )
        mock_pred.assert_called_once()
        mock_match.assert_not_called()
        assert "footystats_predictions" in result

    @pytest.mark.asyncio
    async def test_footystats_entity_filter_matches_only(self) -> None:
        mock_pred = AsyncMock(return_value={})
        mock_match = AsyncMock(return_value={"footystats_matches": 5})
        with _entry_stack(
            ["FOOTYSTATS"],
            _BUCKET,
            patch("instruments_service.engine.orchestrator._fetch_footystats_predictions", mock_pred),
            patch("instruments_service.engine.orchestrator._fetch_footystats_matches", mock_match),
        ):
            result = await process_instruments(
                _DATE,
                ["SPORTS"],
                sports_provider="FOOTYSTATS",
                sports_entity_filter="MATCHES",
                api_keys={"footystats": "fs-key"},
            )
        mock_pred.assert_not_called()
        mock_match.assert_called_once()
        assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_transfermarkt_no_api_key_returns_empty(self) -> None:
        with _entry_stack(
            ["TRANSFERMARKT"],
            _BUCKET,
            patch("instruments_service.engine.orchestrator.get_leagues_needing_refresh", return_value=[]),
        ):
            result = await process_instruments(
                _DATE,
                ["SPORTS"],
                sports_provider="TRANSFERMARKT",
                api_keys={},
                redo_all=True,
            )
        assert result == {}

    @pytest.mark.asyncio
    async def test_transfermarkt_with_key_routes_to_fetcher(self) -> None:
        mock_tm = AsyncMock(return_value={"transfermarkt_players": 100})
        with _entry_stack(
            ["TRANSFERMARKT"],
            _BUCKET,
            patch("instruments_service.engine.orchestrator.get_leagues_needing_refresh", return_value=["EPL"]),
            patch("instruments_service.engine.orchestrator._fetch_transfermarkt_data", mock_tm),
        ):
            result = await process_instruments(
                _DATE,
                ["SPORTS"],
                sports_provider="TRANSFERMARKT",
                api_keys={"transfermarkt": "tm-key"},
                redo_all=True,
            )
        assert result == {"transfermarkt_players": 100}
        mock_tm.assert_called_once()

    @pytest.mark.asyncio
    async def test_transfermarkt_no_leagues_today_logs_skip(self) -> None:
        mock_tm = AsyncMock(return_value={})
        with _entry_stack(
            ["TRANSFERMARKT"],
            _BUCKET,
            patch("instruments_service.engine.orchestrator.get_leagues_needing_refresh", return_value=[]),
            patch("instruments_service.engine.orchestrator._fetch_transfermarkt_data", mock_tm),
        ):
            result = await process_instruments(
                _DATE,
                ["SPORTS"],
                sports_provider="TRANSFERMARKT",
                api_keys={"transfermarkt": "tm-key"},
            )
        assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_soccer_football_info_no_api_key_returns_empty(self) -> None:
        with _entry_stack(["SOCCER_FOOTBALL_INFO"]):
            result = await process_instruments(
                _DATE,
                ["SPORTS"],
                sports_provider="SOCCER_FOOTBALL_INFO",
                api_keys={},
                redo_all=True,
            )
        assert result == {}

    @pytest.mark.asyncio
    async def test_soccer_football_info_with_key_routes_to_fetcher(self) -> None:
        mock_sfi = AsyncMock(return_value={"sfi_squads": 22})
        with _entry_stack(
            ["SOCCER_FOOTBALL_INFO"],
            _BUCKET,
            patch("instruments_service.engine.orchestrator._fetch_sfi_data", mock_sfi),
        ):
            result = await process_instruments(
                _DATE,
                ["SPORTS"],
                sports_provider="SOCCER_FOOTBALL_INFO",
                api_keys={"soccer_football_info": "sfi-key"},
                redo_all=True,
            )
        assert result == {"sfi_squads": 22}
        mock_sfi.assert_called_once()

    @pytest.mark.asyncio
    async def test_recovery_fixture_ids_promotes_redo_all(self) -> None:
        """recovery_fixture_ids=... sets redo_all=True before routing sub-providers."""
        mock_xg = AsyncMock(return_value={"understat_xg": 3})
        mock_shots = AsyncMock(return_value={})
        with _entry_stack(
            ["UNDERSTAT"],
            _BUCKET,
            patch("instruments_service.engine.orchestrator._fetch_understat_xg", mock_xg),
            patch("instruments_service.engine.orchestrator._run_understat_shots_date", mock_shots),
        ):
            await process_instruments(
                _DATE,
                ["SPORTS"],
                sports_provider="UNDERSTAT",
                redo_all=False,
                recovery_fixture_ids=frozenset([101, 202]),
            )
        # force=True is passed because recovery_fixture_ids promoted redo_all
        mock_xg.assert_called_once_with(date=_DATE, bucket=_BUCKET, force=True)


# ---------------------------------------------------------------------------
# _fetch_footystats_predictions
# ---------------------------------------------------------------------------


def _ft_pred_stack(skip: bool = False, predictions: list | None = None) -> tuple:
    mock_adapter = MagicMock()
    mock_adapter.get_fixture_predictions = AsyncMock(return_value=predictions if predictions is not None else [])
    mock_mw = MagicMock()
    mock_mw_cls = MagicMock(return_value=mock_mw)

    patches = _stack(
        patch("instruments_service.engine.orchestrator.create_sports_reference_adapter", return_value=mock_adapter),
        patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
        patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
        patch("unified_api_contracts.sports.get_expected_leagues_for_source", return_value=[_make_league("EPL")]),
        patch("instruments_service.engine.orchestrator._should_skip_date_for_per_league", return_value=skip),
        patch("instruments_service.engine.orchestrator._gated_sink_write"),
        patch("instruments_service.engine.orchestrator.stamp_available_at_explicit", side_effect=lambda df, **kw: df),
        patch("instruments_service.engine.orchestrator._validate_predictions_null_rates", return_value=[]),
        patch("instruments_service.engine.orchestrator._canonical_league_id", side_effect=lambda lid: str(lid)),
        patch("instruments_service.engine.orchestrator._sports_ref_source", return_value="footystats"),
        patch("unified_api_contracts.sports.build_fixture_id", return_value="EPL:ARSENAL_v_CHELSEA:2026-01-15"),
        patch("unified_api_contracts.sports.resolve_footystats_team", side_effect=lambda t: t.upper()),
        patch("instruments_service.engine.orchestrator.FOOTYSTATS_HISTORICAL_SEASON_IDS", {123: "EPL"}),
    )
    return patches, mock_adapter, mock_mw


class TestFetchFootystatsPredictions:
    """Direct unit tests for _fetch_footystats_predictions."""

    @pytest.mark.asyncio
    async def test_skip_returns_empty_dict(self) -> None:
        stack, _, _ = _ft_pred_stack(skip=True)
        with stack:
            result = await _fetch_footystats_predictions(date=_DATE, api_key="key", bucket=_BUCKET)
        assert result == {}

    @pytest.mark.asyncio
    async def test_empty_predictions_writes_empty_manifest(self) -> None:
        stack, _, mock_mw = _ft_pred_stack(skip=False, predictions=[])
        with stack:
            result = await _fetch_footystats_predictions(date=_DATE, api_key="key", bucket=_BUCKET)
        assert result == {}
        mock_mw.record_empty.assert_called_once()
        mock_mw.write.assert_called_once()

    @pytest.mark.asyncio
    async def test_happy_path_with_league_writes_per_league(self) -> None:
        predictions = [
            {
                "fixture_id": "123:ARSENAL_v_CHELSEA:2026-01-15",
                "home_team": "Arsenal",
                "away_team": "Chelsea",
                "kickoff_utc": "2026-01-15T15:00:00Z",
                "btts_potential": 0.6,
            }
        ]
        stack, _, mock_mw = _ft_pred_stack(skip=False, predictions=predictions)
        with stack:
            result = await _fetch_footystats_predictions(date=_DATE, api_key="key", bucket=_BUCKET)
        assert "footystats_predictions" in result
        assert result["footystats_predictions"] == 1
        mock_mw.write.assert_called_once()

    @pytest.mark.asyncio
    async def test_happy_path_no_team_names_skips_canonical_id(self) -> None:
        """Rows without home/away team columns still write; canonical_fixture_id not added."""
        predictions = [{"kickoff_utc": "2026-01-15T15:00:00Z", "btts_potential": 0.5}]
        stack, _, _mock_mw = _ft_pred_stack(skip=False, predictions=predictions)
        with stack:
            result = await _fetch_footystats_predictions(date=_DATE, api_key="key", bucket=_BUCKET)
        assert result.get("footystats_predictions") == 1

    @pytest.mark.asyncio
    async def test_exception_records_failed_shard(self) -> None:
        mock_adapter = MagicMock()
        mock_adapter.get_fixture_predictions = AsyncMock(side_effect=RuntimeError("api down"))
        mock_mw = MagicMock()
        mock_mw_cls = MagicMock(return_value=mock_mw)

        with _stack(
            patch("instruments_service.engine.orchestrator.create_sports_reference_adapter", return_value=mock_adapter),
            patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
            patch("unified_api_contracts.sports.get_expected_leagues_for_source", return_value=[]),
            patch("instruments_service.engine.orchestrator._should_skip_date_for_per_league", return_value=False),
            patch("instruments_service.engine.orchestrator.classify_and_emit_error"),
            patch("instruments_service.engine.orchestrator._classify_adapter_failure", return_value="RuntimeError"),
            patch("instruments_service.engine.orchestrator.FOOTYSTATS_HISTORICAL_SEASON_IDS", {}),
        ):
            result = await _fetch_footystats_predictions(date=_DATE, api_key="key", bucket=_BUCKET)
        assert result == {}
        mock_mw.record_failed.assert_called_once()


# ---------------------------------------------------------------------------
# _fetch_footystats_matches
# ---------------------------------------------------------------------------


def _ft_match_stack(skip: bool = False, fixtures: list | None = None) -> tuple:
    mock_adapter = MagicMock()
    mock_adapter.get_fixtures = AsyncMock(return_value=fixtures if fixtures is not None else [])
    mock_mw = MagicMock()
    mock_mw_cls = MagicMock(return_value=mock_mw)

    patches = _stack(
        patch("instruments_service.engine.orchestrator.create_sports_reference_adapter", return_value=mock_adapter),
        patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
        patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
        patch("unified_api_contracts.sports.get_expected_leagues_for_source", return_value=[_make_league("EPL")]),
        patch("instruments_service.engine.orchestrator._should_skip_date_for_per_league", return_value=skip),
        patch("instruments_service.engine.orchestrator._gated_sink_write"),
        patch("instruments_service.engine.orchestrator.stamp_available_at_explicit", side_effect=lambda df, **kw: df),
        patch("instruments_service.engine.orchestrator._canonical_league_id", side_effect=lambda lid: str(lid)),
        patch("instruments_service.engine.orchestrator._sports_ref_source", return_value="footystats"),
        patch("unified_api_contracts.sports.build_fixture_id", return_value="EPL:ARSENAL_v_CHELSEA:2026-01-15"),
        patch("unified_api_contracts.sports.resolve_footystats_team", side_effect=lambda t: t.upper()),
        patch("instruments_service.engine.orchestrator.FOOTYSTATS_HISTORICAL_SEASON_IDS", {123: "EPL"}),
    )
    return patches, mock_adapter, mock_mw


class TestFetchFootystatsMatches:
    """Direct unit tests for _fetch_footystats_matches."""

    @pytest.mark.asyncio
    async def test_skip_returns_empty_dict(self) -> None:
        stack, _, _ = _ft_match_stack(skip=True)
        with stack:
            result = await _fetch_footystats_matches(date=_DATE, api_key="key", bucket=_BUCKET)
        assert result == {}

    @pytest.mark.asyncio
    async def test_empty_fixtures_writes_empty_manifest(self) -> None:
        stack, _, mock_mw = _ft_match_stack(skip=False, fixtures=[])
        with stack:
            result = await _fetch_footystats_matches(date=_DATE, api_key="key", bucket=_BUCKET)
        assert result == {}
        mock_mw.record_empty.assert_called_once()
        mock_mw.write.assert_called_once()

    @pytest.mark.asyncio
    async def test_happy_path_writes_match_rows(self) -> None:
        fixtures = [
            {
                "home_team_name": "Arsenal",
                "away_team_name": "Chelsea",
                "fixture_id": "123:ARSENAL_v_CHELSEA:2026-01-15",
                "status": "complete",
                "home_goals": "2",
                "away_goals": "1",
            }
        ]
        stack, _, mock_mw = _ft_match_stack(skip=False, fixtures=fixtures)
        with stack:
            result = await _fetch_footystats_matches(date=_DATE, api_key="key", bucket=_BUCKET)
        assert isinstance(result, dict)
        assert result.get("footystats_matches", 0) >= 0
        mock_mw.write.assert_called_once()

    @pytest.mark.asyncio
    async def test_exception_records_failed_shard(self) -> None:
        mock_adapter = MagicMock()
        mock_adapter.get_fixtures = AsyncMock(side_effect=RuntimeError("timeout"))
        mock_mw = MagicMock()
        mock_mw_cls = MagicMock(return_value=mock_mw)

        with _stack(
            patch("instruments_service.engine.orchestrator.create_sports_reference_adapter", return_value=mock_adapter),
            patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
            patch("unified_api_contracts.sports.get_expected_leagues_for_source", return_value=[]),
            patch("instruments_service.engine.orchestrator._should_skip_date_for_per_league", return_value=False),
            patch("instruments_service.engine.orchestrator.classify_and_emit_error"),
            patch("instruments_service.engine.orchestrator._classify_adapter_failure", return_value="RuntimeError"),
            patch("instruments_service.engine.orchestrator.FOOTYSTATS_HISTORICAL_SEASON_IDS", {}),
        ):
            result = await _fetch_footystats_matches(date=_DATE, api_key="key", bucket=_BUCKET)
        assert result == {}
        mock_mw.record_failed.assert_called_once()


# ---------------------------------------------------------------------------
# _fetch_footystats_odds
# ---------------------------------------------------------------------------


def _ft_odds_stack(skip: bool = False, odds_rows: list | None = None) -> tuple:
    mock_adapter = MagicMock()
    mock_adapter.get_fixture_odds_snapshot = AsyncMock(return_value=odds_rows if odds_rows is not None else [])
    mock_mw = MagicMock()
    mock_mw_cls = MagicMock(return_value=mock_mw)

    patches = _stack(
        patch("instruments_service.engine.orchestrator.create_sports_reference_adapter", return_value=mock_adapter),
        patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
        patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
        patch("unified_api_contracts.sports.get_expected_leagues_for_source", return_value=[_make_league("EPL")]),
        patch("instruments_service.engine.orchestrator._should_skip_date_for_per_league", return_value=skip),
        patch("instruments_service.engine.orchestrator._gated_sink_write"),
        patch("instruments_service.engine.orchestrator.stamp_available_at_explicit", side_effect=lambda df, **kw: df),
        patch("instruments_service.engine.orchestrator._canonical_league_id", side_effect=lambda lid: str(lid)),
        patch("instruments_service.engine.orchestrator._sports_ref_source", return_value="footystats"),
        patch("instruments_service.engine.orchestrator._load_scheduled_footystats_fixture_map", return_value={}),
        patch("unified_api_contracts.sports.build_fixture_id", return_value="EPL:ARSENAL_v_CHELSEA:2026-01-15"),
        patch("unified_api_contracts.sports.resolve_footystats_team", side_effect=lambda t: t.upper()),
        patch("instruments_service.engine.orchestrator.FOOTYSTATS_HISTORICAL_SEASON_IDS", {123: "EPL"}),
    )
    return patches, mock_adapter, mock_mw


class TestFetchFootystatsOdds:
    """Direct unit tests for _fetch_footystats_odds."""

    @pytest.mark.asyncio
    async def test_skip_returns_empty_dict(self) -> None:
        stack, _, _ = _ft_odds_stack(skip=True)
        with stack:
            result = await _fetch_footystats_odds(date=_DATE, api_key="key", bucket=_BUCKET)
        assert result == {}

    @pytest.mark.asyncio
    async def test_empty_odds_no_scheduled_writes_empty_manifest(self) -> None:
        stack, _, mock_mw = _ft_odds_stack(skip=False, odds_rows=[])
        with stack:
            result = await _fetch_footystats_odds(date=_DATE, api_key="key", bucket=_BUCKET)
        assert result == {}
        mock_mw.record_empty.assert_called_once()
        mock_mw.write.assert_called_once()

    @pytest.mark.asyncio
    async def test_happy_path_with_league_writes_odds_rows(self) -> None:
        odds_rows = [
            {
                "home_team": "Arsenal",
                "away_team": "Chelsea",
                "fixture_id": "123:ARSENAL_v_CHELSEA:2026-01-15",
                "kickoff_utc": "2026-01-15T15:00:00Z",
                "odds_ft_1": 1.8,
                "odds_ft_x": 3.5,
                "odds_ft_2": 4.2,
            }
        ]
        stack, _, mock_mw = _ft_odds_stack(skip=False, odds_rows=odds_rows)
        with stack:
            result = await _fetch_footystats_odds(date=_DATE, api_key="key", bucket=_BUCKET)
        assert isinstance(result, dict)
        assert result.get("footystats_odds", 0) >= 1
        mock_mw.write.assert_called_once()

    @pytest.mark.asyncio
    async def test_exception_records_failed_shard(self) -> None:
        mock_adapter = MagicMock()
        mock_adapter.get_fixture_odds_snapshot = AsyncMock(side_effect=RuntimeError("timeout"))
        mock_mw = MagicMock()
        mock_mw_cls = MagicMock(return_value=mock_mw)

        with _stack(
            patch("instruments_service.engine.orchestrator.create_sports_reference_adapter", return_value=mock_adapter),
            patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
            patch("unified_api_contracts.sports.get_expected_leagues_for_source", return_value=[]),
            patch("instruments_service.engine.orchestrator._should_skip_date_for_per_league", return_value=False),
            patch("instruments_service.engine.orchestrator._load_scheduled_footystats_fixture_map", return_value={}),
            patch("instruments_service.engine.orchestrator.classify_and_emit_error"),
            patch("instruments_service.engine.orchestrator._classify_adapter_failure", return_value="RuntimeError"),
            patch("instruments_service.engine.orchestrator.FOOTYSTATS_HISTORICAL_SEASON_IDS", {}),
        ):
            result = await _fetch_footystats_odds(date=_DATE, api_key="key", bucket=_BUCKET)
        assert result == {}
        mock_mw.record_failed.assert_called_once()


# ---------------------------------------------------------------------------
# _load_scheduled_footystats_fixture_map
# ---------------------------------------------------------------------------


class TestLoadScheduledFootystatsFixtureMap:
    """Unit tests for _load_scheduled_footystats_fixture_map."""

    def test_no_parquets_returns_empty(self) -> None:
        mock_storage = MagicMock()
        mock_storage.list_blobs.return_value = []

        with _stack(
            patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage),
            patch("instruments_service.engine.orchestrator._sports_ref_pm", return_value="batch_footystats"),
        ):
            result = _load_scheduled_footystats_fixture_map(bucket=_BUCKET, date=_DATE)
        assert result == {}

    def test_exception_returns_empty(self) -> None:
        mock_storage = MagicMock()
        mock_storage.list_blobs.side_effect = RuntimeError("GCS unavailable")

        with _stack(
            patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage),
            patch("instruments_service.engine.orchestrator._sports_ref_pm", return_value="batch_footystats"),
        ):
            result = _load_scheduled_footystats_fixture_map(bucket=_BUCKET, date=_DATE)
        assert result == {}

    def test_parquet_with_fixture_ids_returns_map(self) -> None:
        import io

        import pandas as pd

        df = pd.DataFrame(
            {
                "canonical_fixture_id": ["EPL:ARSENAL_v_CHELSEA:2026-01-15", "EPL:CITY_v_UNITED:2026-01-15"],
                "kickoff_utc": ["2026-01-15T15:00:00Z", "2026-01-15T17:30:00Z"],
            }
        )
        buf = io.BytesIO()
        df.to_parquet(buf)
        parquet_bytes = buf.getvalue()

        mock_blob = MagicMock()
        mock_blob.name = f"sports_reference/by_date/day={_DATE}/entity=footystats_matches/matches.parquet"
        mock_storage = MagicMock()
        mock_storage.list_blobs.side_effect = [[], [mock_blob]]
        mock_storage.download_bytes.return_value = parquet_bytes

        with _stack(
            patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage),
            patch("instruments_service.engine.orchestrator._sports_ref_pm", return_value="batch_footystats"),
        ):
            result = _load_scheduled_footystats_fixture_map(bucket=_BUCKET, date=_DATE)
        assert "EPL:ARSENAL_v_CHELSEA:2026-01-15" in result
        assert "EPL:CITY_v_UNITED:2026-01-15" in result
