"""Coverage tests for sports data-fetcher sub-functions in orchestrator.py.

Targets:
  - _fetch_understat_xg (lines 6249-6405): skip / empty / happy / exception
  - _run_understat_shots_date (lines 6474-6622): skip / empty / exception
  - _fetch_weather_data (lines 7482-7938): no-fixtures / fixtures-read-fail
  - _fetch_sfi_data (lines 6950-7399): skip / entity-filter / adapter-empty
  - _fetch_transfermarkt_data (lines 6625-6947): skip / no-leagues / empty
"""

from __future__ import annotations

import contextlib
import io
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from instruments_service.engine.orchestrator import (
    _fetch_footystats_matches,
    _fetch_footystats_predictions,
    _fetch_sfi_data,
    _fetch_transfermarkt_data,
    _fetch_understat_xg,
    _fetch_weather_data,
    _run_understat_shots_date,
)

_DATE = "2026-01-15"
_BUCKET = "test-bucket"


def _mk_league(lid: str) -> SimpleNamespace:
    return SimpleNamespace(league_id=lid)


def _stack(*patches: object) -> contextlib.ExitStack:
    s = contextlib.ExitStack()
    for p in patches:
        s.enter_context(p)  # type: ignore[arg-type]
    return s


# ---------------------------------------------------------------------------
# _fetch_understat_xg
# ---------------------------------------------------------------------------


class TestFetchUnderstatXg:
    """Tests for _fetch_understat_xg (lines 6249-6405)."""

    @staticmethod
    def _common_patches(skip_all: bool = False, fixtures=None):
        mock_adapter = MagicMock()
        mock_adapter.get_fixtures = AsyncMock(return_value=fixtures or [])
        mock_adapter._fetch_error_count = 0  # prevent MagicMock > int TypeError
        mock_mw = MagicMock()
        mock_mw_cls = MagicMock(return_value=mock_mw)

        return (
            _stack(
                patch(
                    "instruments_service.engine.orchestrator.create_sports_reference_adapter", return_value=mock_adapter
                ),
                patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
                patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
                patch(
                    "unified_api_contracts.sports.get_expected_leagues_for_source",
                    return_value=[_mk_league("EPL"), _mk_league("BUNDESLIGA")],
                ),
                patch("instruments_service.engine.orchestrator._should_skip_shard", return_value=skip_all),
                patch("instruments_service.engine.orchestrator._gated_sink_write"),
                patch(
                    "instruments_service.engine.orchestrator.stamp_available_at_explicit",
                    side_effect=lambda df, **kw: df,
                ),
                patch(
                    "instruments_service.engine.orchestrator._canonical_league_id",
                    side_effect=lambda lid: str(lid),
                ),
                patch("instruments_service.engine.orchestrator._sports_ref_source", return_value="understat"),
                patch("unified_api_contracts.sports.build_fixture_id", return_value="EPL:ARSENAL_v_CHELSEA:2026-01-15"),
                patch("unified_api_contracts.sports.resolve_understat_team", side_effect=lambda t: t.upper()),
            ),
            mock_adapter,
            mock_mw,
        )

    @pytest.mark.asyncio
    async def test_all_leagues_captured_skip_returns_empty(self) -> None:
        stack, _, _ = self._common_patches(skip_all=True)
        with stack:
            result = await _fetch_understat_xg(date=_DATE, bucket=_BUCKET, force=False)
        assert result == {}

    @pytest.mark.asyncio
    async def test_empty_fixtures_writes_empty_per_league(self) -> None:
        stack, _, mock_mw = self._common_patches(skip_all=False, fixtures=[])
        with stack:
            result = await _fetch_understat_xg(date=_DATE, bucket=_BUCKET)
        assert result == {}
        # record_empty should be called per expected league (EPL + BUNDESLIGA)
        assert mock_mw.record_empty.call_count == 2
        mock_mw.write.assert_called_once()

    @pytest.mark.asyncio
    async def test_happy_path_with_home_away_teams(self) -> None:
        fixtures = [
            {
                "h_title": "Arsenal",
                "a_title": "Chelsea",
                "league": "EPL",
                "date": _DATE,
                "kickoff_utc": f"{_DATE} 15:00:00",  # needed for available_at column
                "h": {"goals": 2},
                "a": {"goals": 1},
            }
        ]
        stack, _, mock_mw = self._common_patches(skip_all=False, fixtures=fixtures)
        with stack:
            result = await _fetch_understat_xg(date=_DATE, bucket=_BUCKET)
        assert isinstance(result, dict)
        mock_mw.write.assert_called_once()

    @pytest.mark.asyncio
    async def test_nested_league_dict_and_noncanonical_name_captures_not_empty(self) -> None:
        """Regression (understat XG capture): the REAL adapter returns fixture
        'league' as a NESTED CanonicalLeague dict, and _canonical_league_id
        UPPER-cases (Bundesliga -> BUNDESLIGA). Two coupled bugs made XG record
        empty despite fixtures existing:
          (1) the flatten exploded the nested 'league' dict into league_* columns,
              leaving NO flat 'league' key -> the whole capture block was skipped;
          (2) the captured-set tracked the RAW league name while the honest-absence
              loop subtracts the CANONICAL, so a non-already-uppercase league got
              empty written OVER its capture (only EPL, raw==canonical, survived).
        Asserts record_captured fires for the canonical league and record_empty
        does NOT. The prior happy-path used a STRING league + identity canonical,
        so it could not catch either bug.
        Ref: plans/active/issues/understat_bulk_download_backfill_2026_06_29."""
        fixtures = [
            {
                "h_title": "Bayern",
                "a_title": "Dortmund",
                # NESTED CanonicalLeague dict — the real _coerce_adapter_output shape.
                "league": {"league_id": "Bundesliga", "name": "Bundesliga"},
                "date": _DATE,
                "kickoff_utc": f"{_DATE} 15:00:00",
                "h": {"goals": 2},
                "a": {"goals": 1},
            }
        ]
        mock_adapter = MagicMock()
        mock_adapter.get_fixtures = AsyncMock(return_value=fixtures)
        mock_adapter._fetch_error_count = 0
        mock_mw = MagicMock()
        with _stack(
            patch("instruments_service.engine.orchestrator.create_sports_reference_adapter", return_value=mock_adapter),
            patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
            patch("instruments_service.engine.orchestrator.ManifestWriter", MagicMock(return_value=mock_mw)),
            patch(
                "unified_api_contracts.sports.get_expected_leagues_for_source",
                return_value=[_mk_league("BUNDESLIGA")],
            ),
            patch("instruments_service.engine.orchestrator._should_skip_shard", return_value=False),
            patch("instruments_service.engine.orchestrator._gated_sink_write"),
            patch(
                "instruments_service.engine.orchestrator.stamp_available_at_explicit",
                side_effect=lambda df, **kw: df,
            ),
            # REAL canonicalisation: UPPER-case so raw 'Bundesliga' != canonical 'BUNDESLIGA'.
            patch(
                "instruments_service.engine.orchestrator._canonical_league_id",
                side_effect=lambda lid: str(lid).upper(),
            ),
            patch("instruments_service.engine.orchestrator._is_in_canonical_write_universe", return_value=True),
            patch("instruments_service.engine.orchestrator._sports_ref_source", return_value="understat"),
            patch(
                "unified_api_contracts.sports.build_fixture_id",
                return_value="BUNDESLIGA:BAYERN_v_DORTMUND:2026-01-15",
            ),
            patch("unified_api_contracts.sports.resolve_understat_team", side_effect=lambda t: t.upper()),
        ):
            await _fetch_understat_xg(date=_DATE, bucket=_BUCKET, force=True)
        captured_leagues = {c.kwargs.get("league_id") for c in mock_mw.record_captured.call_args_list}
        assert "BUNDESLIGA" in captured_leagues, f"XG must CAPTURE the league; got captured={captured_leagues}"
        empty_leagues = {c.kwargs["row_key"].get("league_id") for c in mock_mw.record_empty.call_args_list}
        assert "BUNDESLIGA" not in empty_leagues, (
            f"captured league must not ALSO be recorded empty; empty={empty_leagues}"
        )

    @pytest.mark.asyncio
    async def test_exception_records_failed_per_league(self) -> None:
        mock_adapter = MagicMock()
        mock_adapter.get_fixtures = AsyncMock(side_effect=RuntimeError("network error"))
        mock_mw = MagicMock()
        mock_mw_cls = MagicMock(return_value=mock_mw)

        with _stack(
            patch("instruments_service.engine.orchestrator.create_sports_reference_adapter", return_value=mock_adapter),
            patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
            patch(
                "unified_api_contracts.sports.get_expected_leagues_for_source",
                return_value=[_mk_league("EPL")],
            ),
            patch("instruments_service.engine.orchestrator._should_skip_shard", return_value=False),
            patch("instruments_service.engine.orchestrator.classify_and_emit_error"),
            patch("instruments_service.engine.orchestrator._classify_adapter_failure", return_value="RuntimeError"),
            patch("instruments_service.engine.orchestrator.log_event"),
        ):
            result = await _fetch_understat_xg(date=_DATE, bucket=_BUCKET)
        assert result == {}
        mock_mw.record_failed.assert_called()
        mock_mw.write.assert_called()

    @pytest.mark.asyncio
    async def test_fetch_errors_on_adapter_writes_record_failed(self) -> None:
        """When the EPL league fetch errored (in _failed_league_names) and there are
        no fixtures, record_failed is written for EPL only (the errored league)."""
        mock_adapter = MagicMock()
        mock_adapter.get_fixtures = AsyncMock(return_value=[])
        mock_adapter._fetch_error_count = 3  # simulate partial league fetch errors
        # The orchestrator now scopes record_failed to the leagues that GENUINELY
        # errored (mapped name->canonical). EPL errored → record_failed(EPL); a
        # non-errored league would get record_empty instead. _canonical_league_id
        # is the real one here (not patched), and "EPL" canonicalises to "EPL".
        mock_adapter._failed_league_names = {"EPL"}
        mock_mw = MagicMock()
        mock_mw_cls = MagicMock(return_value=mock_mw)

        with _stack(
            patch("instruments_service.engine.orchestrator.create_sports_reference_adapter", return_value=mock_adapter),
            patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
            patch(
                "unified_api_contracts.sports.get_expected_leagues_for_source",
                return_value=[_mk_league("EPL")],
            ),
            patch("instruments_service.engine.orchestrator._should_skip_shard", return_value=False),
        ):
            result = await _fetch_understat_xg(date=_DATE, bucket=_BUCKET)
        assert result == {}
        # With _fetch_error_count > 0, record_failed is called for each expected league
        mock_mw.record_failed.assert_called_once()
        mock_mw.write.assert_called_once()

    @pytest.mark.asyncio
    async def test_one_of_five_leagues_errored_yields_one_failed_four_empty(self) -> None:
        """A single per-league 404 on a 5-league day must NOT flip all 5 leagues to
        attempted_failed. Only the errored league (EPL) gets record_failed; the other
        four (LA_LIGA, BUNDESLIGA, SERIE_A, LIGUE_1) get record_empty(EXPECTED_NO_FIXTURE).
        Gate for plan sports_p0_sourcing_and_honest_coverage_correctness_2026_06_27 todo #1.
        """
        mock_adapter = MagicMock()
        mock_adapter.get_fixtures = AsyncMock(return_value=[])
        mock_adapter._fetch_error_count = 1  # one league errored
        mock_adapter._failed_league_names = {"EPL"}
        mock_mw = MagicMock()
        mock_mw_cls = MagicMock(return_value=mock_mw)

        five_leagues = [
            _mk_league("EPL"),
            _mk_league("LA_LIGA"),
            _mk_league("BUNDESLIGA"),
            _mk_league("SERIE_A"),
            _mk_league("LIGUE_1"),
        ]

        with _stack(
            patch("instruments_service.engine.orchestrator.create_sports_reference_adapter", return_value=mock_adapter),
            patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
            patch(
                "unified_api_contracts.sports.get_expected_leagues_for_source",
                return_value=five_leagues,
            ),
            patch("instruments_service.engine.orchestrator._should_skip_shard", return_value=False),
        ):
            result = await _fetch_understat_xg(date=_DATE, bucket=_BUCKET)

        assert result == {}
        # Exactly 1 league errored → exactly 1 record_failed
        assert mock_mw.record_failed.call_count == 1
        failed_call_kwargs = mock_mw.record_failed.call_args_list[0][1]
        assert failed_call_kwargs["row_key"]["league_id"] == "EPL"
        assert failed_call_kwargs["error"] == "HTTP_NOT_FOUND"
        # The other 4 leagues get honest-absence empty_confirmed, NOT attempted_failed
        assert mock_mw.record_empty.call_count == 4
        mock_mw.write.assert_called_once()

    @pytest.mark.asyncio
    async def test_pre_cutoff_date_records_expected_empty(self) -> None:
        """Understat xG skipped when date is before source coverage start."""
        from datetime import date as date_type

        mock_adapter = MagicMock()
        mock_adapter.get_fixtures = AsyncMock(return_value=[])
        mock_adapter._fetch_error_count = 0
        mock_mw = MagicMock()
        mock_mw_cls = MagicMock(return_value=mock_mw)
        future_floor = date_type(2027, 1, 1)

        with _stack(
            patch("instruments_service.engine.orchestrator.create_sports_reference_adapter", return_value=mock_adapter),
            patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
            patch(
                "unified_api_contracts.sports.get_expected_leagues_for_source",
                return_value=[_mk_league("EPL")],
            ),
            patch("instruments_service.engine.orchestrator._should_skip_shard", return_value=False),
            patch("instruments_service.engine.orchestrator.get_source_coverage_start", return_value=future_floor),
            patch("instruments_service.engine.orchestrator.is_in_known_gap", return_value=False),
            patch("instruments_service.engine.orchestrator.log_event"),
        ):
            result = await _fetch_understat_xg(date=_DATE, bucket=_BUCKET)
        mock_mw.record_expected_empty.assert_called()
        assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# _run_understat_shots_date
# ---------------------------------------------------------------------------


class TestRunUnderstatShotsDate:
    """Tests for _run_understat_shots_date (lines 6474-6622)."""

    @pytest.mark.asyncio
    async def test_no_match_ids_returns_empty(self) -> None:
        """get_match_ids_for_date returns empty list → empty result dict, no writes."""
        mock_adapter = MagicMock()
        mock_adapter.get_match_ids_for_date = AsyncMock(return_value=[])
        mock_mw = MagicMock()
        mock_mw_cls = MagicMock(return_value=mock_mw)

        with _stack(
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.understat.UnderstatAdapter",
                return_value=mock_adapter,
            ),
            patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
            patch(
                "unified_api_contracts.sports.get_expected_leagues_for_source",
                return_value=[_mk_league("EPL")],
            ),
            patch("instruments_service.engine.orchestrator._should_skip_shard", return_value=False),
        ):
            result = await _run_understat_shots_date(date=_DATE, bucket=_BUCKET)
        assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_skip_all_leagues_captured_returns_empty(self) -> None:
        mock_adapter = MagicMock()
        mock_mw = MagicMock()
        mock_mw_cls = MagicMock(return_value=mock_mw)

        with _stack(
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.understat.UnderstatAdapter",
                return_value=mock_adapter,
            ),
            patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
            patch(
                "unified_api_contracts.sports.get_expected_leagues_for_source",
                return_value=[_mk_league("EPL")],
            ),
            patch("instruments_service.engine.orchestrator._should_skip_shard", return_value=True),
        ):
            result = await _run_understat_shots_date(date=_DATE, bucket=_BUCKET)
        assert result == {}

    @pytest.mark.asyncio
    async def test_shots_returned_writes_captured(self) -> None:
        """match_ids returned + shots for each → gated_sink_write + record_captured per league."""
        mock_adapter = MagicMock()
        mock_adapter.get_match_ids_for_date = AsyncMock(return_value=[("match1", "EPL")])
        # Return one shot dict; _coerce_adapter_output handles dicts
        mock_adapter.get_match_shots = AsyncMock(return_value=[{"x": 0.5, "y": 0.3, "xG": 0.12}])
        # int (not a MagicMock) so the per-match error-attribution comparison
        # (_fetch_error_count > snapshot) works; no shots-call error here.
        mock_adapter._fetch_error_count = 0
        mock_adapter._failed_league_names = set()
        mock_mw = MagicMock()
        mock_mw_cls = MagicMock(return_value=mock_mw)

        with _stack(
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.understat.UnderstatAdapter",
                return_value=mock_adapter,
            ),
            patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
            patch(
                "unified_api_contracts.sports.get_expected_leagues_for_source",
                return_value=[_mk_league("EPL")],
            ),
            patch("instruments_service.engine.orchestrator._should_skip_shard", return_value=False),
            patch("instruments_service.engine.orchestrator._gated_sink_write"),
            patch(
                "instruments_service.engine.orchestrator._canonical_league_id",
                side_effect=lambda lid: str(lid),
            ),
            patch("instruments_service.engine.orchestrator._sports_ref_source", return_value="understat"),
            patch(
                "unified_api_contracts.external.understat.normalize.normalize_understat_shot",
                side_effect=lambda s: s,
            ),
        ):
            result = await _run_understat_shots_date(date=_DATE, bucket=_BUCKET)
        assert isinstance(result, dict)
        mock_mw.record_captured.assert_called_once()

    @pytest.mark.asyncio
    async def test_exception_shard_isolation_no_raise(self) -> None:
        mock_adapter = MagicMock()
        mock_adapter.get_match_ids_for_date = AsyncMock(side_effect=ConnectionError("timeout"))
        mock_mw = MagicMock()
        mock_mw_cls = MagicMock(return_value=mock_mw)

        with _stack(
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.understat.UnderstatAdapter",
                return_value=mock_adapter,
            ),
            patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
            patch(
                "unified_api_contracts.sports.get_expected_leagues_for_source",
                return_value=[_mk_league("EPL")],
            ),
            patch("instruments_service.engine.orchestrator._should_skip_shard", return_value=False),
            patch("instruments_service.engine.orchestrator.classify_and_emit_error"),
            patch("instruments_service.engine.orchestrator._classify_adapter_failure", return_value="ConnectionError"),
            patch("instruments_service.engine.orchestrator.log_event"),
        ):
            result = await _run_understat_shots_date(date=_DATE, bucket=_BUCKET)
        # Shard isolation: no raise
        assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_pre_cutoff_date_records_expected_empty(self) -> None:
        """Understat XG_SHOTS skipped when date is before source coverage start."""
        from datetime import date as date_type

        mock_adapter = MagicMock()
        mock_mw = MagicMock()
        mock_mw_cls = MagicMock(return_value=mock_mw)
        future_floor = date_type(2027, 1, 1)

        with _stack(
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.understat.UnderstatAdapter",
                return_value=mock_adapter,
            ),
            patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
            patch(
                "unified_api_contracts.sports.get_expected_leagues_for_source",
                return_value=[_mk_league("EPL")],
            ),
            patch("instruments_service.engine.orchestrator._should_skip_shard", return_value=False),
            patch("instruments_service.engine.orchestrator.get_source_coverage_start", return_value=future_floor),
            patch("instruments_service.engine.orchestrator.is_in_known_gap", return_value=False),
            patch("instruments_service.engine.orchestrator.log_event"),
        ):
            result = await _run_understat_shots_date(date=_DATE, bucket=_BUCKET)
        mock_mw.record_expected_empty.assert_called()
        assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_one_of_five_leagues_errored_yields_one_failed_four_empty(self) -> None:
        """XG_SHOTS: a single per-league getLeagueData 404 on a 5-league day must NOT flip
        all 5 leagues to attempted_failed. Only the errored league (EPL) gets record_failed;
        the other four (LA_LIGA, BUNDESLIGA, SERIE_A, LIGUE_1) get record_empty(EXPECTED_NO_FIXTURE).
        Gate for plan sports_p0_sourcing_and_honest_coverage_correctness_2026_06_27 todo #1.
        """
        mock_adapter = MagicMock()
        # get_match_ids_for_date returns empty (EPL errored → no match IDs)
        mock_adapter.get_match_ids_for_date = AsyncMock(return_value=[])
        mock_adapter._fetch_error_count = 1  # EPL's getLeagueData errored
        mock_adapter._failed_league_names = {"EPL"}
        mock_mw = MagicMock()
        mock_mw_cls = MagicMock(return_value=mock_mw)

        five_leagues = [
            _mk_league("EPL"),
            _mk_league("LA_LIGA"),
            _mk_league("BUNDESLIGA"),
            _mk_league("SERIE_A"),
            _mk_league("LIGUE_1"),
        ]

        with _stack(
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.understat.UnderstatAdapter",
                return_value=mock_adapter,
            ),
            patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
            patch(
                "unified_api_contracts.sports.get_expected_leagues_for_source",
                return_value=five_leagues,
            ),
            patch("instruments_service.engine.orchestrator._should_skip_shard", return_value=False),
            patch(
                "instruments_service.engine.orchestrator._canonical_league_id",
                side_effect=lambda lid: str(lid),
            ),
        ):
            result = await _run_understat_shots_date(date=_DATE, bucket=_BUCKET)

        assert isinstance(result, dict)
        # Exactly 1 league errored → exactly 1 record_failed
        assert mock_mw.record_failed.call_count == 1
        failed_call_kwargs = mock_mw.record_failed.call_args_list[0][1]
        assert failed_call_kwargs["row_key"]["league_id"] == "EPL"
        assert failed_call_kwargs["error"] == "HTTP_NOT_FOUND"
        # The other 4 leagues get honest-absence empty_confirmed, NOT attempted_failed
        assert mock_mw.record_empty.call_count == 4
        mock_mw.write.assert_called_once()


# ---------------------------------------------------------------------------
# _fetch_weather_data
# ---------------------------------------------------------------------------


class TestFetchWeatherData:
    """Tests for _fetch_weather_data (lines 7482-7938)."""

    @pytest.mark.asyncio
    async def test_no_fixture_blobs_records_empty_manifest(self) -> None:
        """When no fixture parquets exist, record_empty per expected league and return."""
        mock_mw = MagicMock()
        mock_mw_cls = MagicMock(return_value=mock_mw)
        mock_storage = MagicMock()
        mock_storage.list_blobs.return_value = []  # no blobs

        with _stack(
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
            patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage),
            patch(
                "unified_api_contracts.sports.get_expected_leagues_for_source",
                return_value=[_mk_league("EPL"), _mk_league("BUNDESLIGA")],
            ),
            patch("instruments_service.engine.orchestrator.log_event"),
            patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
        ):
            result = await _fetch_weather_data(date=_DATE, bucket=_BUCKET)
        assert result == {}
        # record_empty called once per expected league
        assert mock_mw.record_empty.call_count == 2
        mock_mw.write.assert_called_once()

    @pytest.mark.asyncio
    async def test_fixture_read_exception_records_failed(self) -> None:
        """Exception reading fixture blobs → record_failed per league and return."""
        mock_mw = MagicMock()
        mock_mw_cls = MagicMock(return_value=mock_mw)
        mock_storage = MagicMock()
        mock_storage.list_blobs.side_effect = RuntimeError("GCS unreachable")

        with _stack(
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
            patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage),
            patch(
                "unified_api_contracts.sports.get_expected_leagues_for_source",
                return_value=[_mk_league("EPL")],
            ),
            patch("instruments_service.engine.orchestrator.classify_and_emit_error"),
            patch("instruments_service.engine.orchestrator._classify_adapter_failure", return_value="RuntimeError"),
            patch("instruments_service.engine.orchestrator.log_event"),
            patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
        ):
            result = await _fetch_weather_data(date=_DATE, bucket=_BUCKET)
        assert result == {}
        mock_mw.record_failed.assert_called()
        mock_mw.write.assert_called_once()

    @pytest.mark.asyncio
    async def test_happy_path_with_venue_coordinates(self) -> None:
        """Fixture with venue_name+league_id + VENUE_COORDINATES match → captured manifest row."""
        mock_mw = MagicMock()
        mock_mw_cls = MagicMock(return_value=mock_mw)
        mock_adapter = MagicMock()
        mock_adapter.get_weather_match_window = AsyncMock(return_value={"temperature": 15.0})
        mock_adapter_cls = MagicMock(return_value=mock_adapter)

        fixture_df = pd.DataFrame({"venue_name": ["Anfield"], "league_id": ["EPL"]})
        buf = io.BytesIO()
        fixture_df.to_parquet(buf)
        parquet_bytes = buf.getvalue()

        mock_blob = MagicMock()
        mock_blob.name = f"sports_reference/by_date/day={_DATE}/entity=fixtures/fixtures.parquet"
        mock_storage = MagicMock()
        mock_storage.list_blobs.side_effect = [
            [mock_blob],  # fixture read
            [],  # canon weather prefix check (max_results=1)
            [],  # legacy weather blobs (max_results=10)
        ]
        mock_storage.download_bytes.return_value = parquet_bytes

        fake_coords = {"ANFIELD": SimpleNamespace(latitude=53.43, longitude=-2.96)}

        with _stack(
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
            patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage),
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.open_meteo.OpenMeteoAdapter",
                mock_adapter_cls,
            ),
            patch("unified_api_contracts.registry.sports_venue_coordinates.VENUE_COORDINATES", new=fake_coords),
            patch(
                "unified_api_contracts.sports.get_expected_leagues_for_source",
                return_value=[_mk_league("EPL")],
            ),
            patch("instruments_service.engine.orchestrator._gated_sink_write"),
            patch(
                "instruments_service.engine.orchestrator.stamp_available_at_explicit",
                side_effect=lambda df, **kw: df,
            ),
            patch(
                "instruments_service.engine.orchestrator._canonical_league_id",
                side_effect=lambda lid: str(lid),
            ),
            patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
            patch("instruments_service.engine.orchestrator.log_event"),
        ):
            result = await _fetch_weather_data(date=_DATE, bucket=_BUCKET, api_key="test-key")
        mock_mw.record_captured_from_counts.assert_called()
        assert result.get("weather") == 1

    @pytest.mark.asyncio
    async def test_fixtures_read_finds_data_only_present_at_canonical_prefix(self) -> None:
        """Regression (2026-07-08 stale-path fix).

        Before the fix, ``_fetch_weather_data`` listed ONLY the legacy bare
        ``entity=fixtures/`` prefix (no ``pipeline_mode=``) — real fixtures
        data is written per-league under the canonical ``pipeline_mode=``
        hive segment and was never found there, so real fixture-having dates
        were silently treated as ``empty_confirmed``. This mock storage
        returns fixture data ONLY when the requested prefix carries
        ``pipeline_mode=batch_api_football/entity=fixtures`` — everything
        else (the legacy bare prefix, the weather-existence checks) returns
        empty — so a pass here proves the read genuinely consults the
        canonical per-league location rather than relying on side_effect
        call-order coincidence.
        """
        mock_mw = MagicMock()
        mock_mw_cls = MagicMock(return_value=mock_mw)
        mock_adapter = MagicMock()
        mock_adapter.get_weather_match_window = AsyncMock(return_value={"temperature": 15.0})
        mock_adapter_cls = MagicMock(return_value=mock_adapter)

        fixture_df = pd.DataFrame({"venue_name": ["Anfield"], "league_id": ["EPL"]})
        buf = io.BytesIO()
        fixture_df.to_parquet(buf)
        parquet_bytes = buf.getvalue()

        canonical_blob = MagicMock()
        canonical_blob.name = (
            f"sports_reference/by_date/day={_DATE}/pipeline_mode=batch_api_football/"
            "entity=fixtures/league=EPL/fixtures.parquet"
        )

        def list_blobs_side_effect(**kwargs: object) -> list[MagicMock]:
            prefix = str(kwargs.get("prefix", ""))
            if "pipeline_mode=batch_api_football/entity=fixtures" in prefix:
                return [canonical_blob]
            # Legacy bare fixtures prefix (the pre-fix stale path) and the
            # weather existing-data checks all find nothing.
            return []

        mock_storage = MagicMock()
        mock_storage.list_blobs.side_effect = list_blobs_side_effect
        mock_storage.download_bytes.return_value = parquet_bytes

        fake_coords = {"ANFIELD": SimpleNamespace(latitude=53.43, longitude=-2.96)}

        with _stack(
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
            patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage),
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.open_meteo.OpenMeteoAdapter",
                mock_adapter_cls,
            ),
            patch("unified_api_contracts.registry.sports_venue_coordinates.VENUE_COORDINATES", new=fake_coords),
            patch(
                "unified_api_contracts.sports.get_expected_leagues_for_source",
                return_value=[_mk_league("EPL")],
            ),
            patch("instruments_service.engine.orchestrator._gated_sink_write"),
            patch(
                "instruments_service.engine.orchestrator.stamp_available_at_explicit",
                side_effect=lambda df, **kw: df,
            ),
            patch(
                "instruments_service.engine.orchestrator._canonical_league_id",
                side_effect=lambda lid: str(lid),
            ),
            patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
            patch("instruments_service.engine.orchestrator.log_event"),
        ):
            result = await _fetch_weather_data(date=_DATE, bucket=_BUCKET, api_key="test-key")

        # Pre-fix, this would be {} with a spurious EXPECTED_NO_FIXTURE
        # record_empty — the legacy-only probe never sees the canonical data.
        mock_mw.record_captured_from_counts.assert_called()
        mock_mw.record_empty.assert_not_called()
        assert result.get("weather") == 1

    @pytest.mark.asyncio
    async def test_incremental_rerun_preserves_previously_captured_venue(self) -> None:
        """Regression (2026-07-08 merge-bug fix).

        Simulates an incremental re-run: ANFIELD's weather was already
        captured (found via the canonical ``pipeline_mode=`` weather prefix),
        and this run adds a new venue (OLD_TRAFFORD). Before the fix, the
        "merge with existing" step re-derived a hardcoded LEGACY-ONLY weather
        prefix (no ``pipeline_mode=``) instead of reusing the already-resolved
        canonical prefix — real captured data (like ANFIELD's row here) was
        never found there, so the per-league write silently dropped it,
        writing ONLY the newly-fetched venue. This mock storage returns
        weather data ONLY for the canonical prefix; a pass here proves the
        merge step reuses that same canonical location and both venues
        survive the write.
        """
        mock_mw = MagicMock()
        mock_mw_cls = MagicMock(return_value=mock_mw)
        mock_adapter = MagicMock()
        mock_adapter.get_weather_match_window = AsyncMock(return_value={"temperature": 12.0})
        mock_adapter_cls = MagicMock(return_value=mock_adapter)

        fixture_df = pd.DataFrame({"venue_name": ["Anfield", "Old Trafford"], "league_id": ["EPL", "EPL"]})
        fixtures_buf = io.BytesIO()
        fixture_df.to_parquet(fixtures_buf)
        fixtures_bytes = fixtures_buf.getvalue()

        existing_weather_df = pd.DataFrame(
            {"venue_id": ["ANFIELD"], "date": [_DATE], "latitude": [53.43], "longitude": [-2.96]}
        )
        weather_buf = io.BytesIO()
        existing_weather_df.to_parquet(weather_buf)
        weather_bytes = weather_buf.getvalue()

        fixtures_blob = MagicMock()
        fixtures_blob.name = (
            f"sports_reference/by_date/day={_DATE}/pipeline_mode=batch_api_football/"
            "entity=fixtures/league=EPL/fixtures.parquet"
        )
        weather_blob = MagicMock()
        weather_blob.name = (
            f"sports_reference/by_date/day={_DATE}/pipeline_mode=batch_open_meteo/entity=weather/weather.parquet"
        )

        def list_blobs_side_effect(**kwargs: object) -> list[MagicMock]:
            prefix = str(kwargs.get("prefix", ""))
            if "pipeline_mode=batch_api_football/entity=fixtures" in prefix:
                return [fixtures_blob]
            if "pipeline_mode=batch_open_meteo/entity=weather" in prefix:
                # Real data lives ONLY at the canonical (pipeline_mode=) weather
                # prefix — the legacy bare prefix (the pre-fix merge step's
                # hardcoded, wrong location) has nothing.
                return [weather_blob]
            return []

        def download_bytes_side_effect(**kwargs: object) -> bytes:
            blob_path = str(kwargs.get("blob_path", ""))
            if "entity=fixtures" in blob_path:
                return fixtures_bytes
            return weather_bytes

        mock_storage = MagicMock()
        mock_storage.list_blobs.side_effect = list_blobs_side_effect
        mock_storage.download_bytes.side_effect = download_bytes_side_effect

        fake_coords = {
            "ANFIELD": SimpleNamespace(latitude=53.43, longitude=-2.96),
            "OLD_TRAFFORD": SimpleNamespace(latitude=53.46, longitude=-2.29),
        }

        mock_sink = MagicMock()
        mock_gated_write = MagicMock()
        with _stack(
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
            patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage),
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.open_meteo.OpenMeteoAdapter",
                mock_adapter_cls,
            ),
            patch("unified_api_contracts.registry.sports_venue_coordinates.VENUE_COORDINATES", new=fake_coords),
            patch(
                "unified_api_contracts.sports.get_expected_leagues_for_source",
                return_value=[_mk_league("EPL")],
            ),
            patch("instruments_service.engine.orchestrator._gated_sink_write", mock_gated_write),
            patch(
                "instruments_service.engine.orchestrator.stamp_available_at_explicit",
                side_effect=lambda df, **kw: df,
            ),
            patch(
                "instruments_service.engine.orchestrator._canonical_league_id",
                side_effect=lambda lid: str(lid),
            ),
            patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=mock_sink),
            patch("instruments_service.engine.orchestrator.log_event"),
        ):
            result = await _fetch_weather_data(date=_DATE, bucket=_BUCKET, api_key="test-key")

        # Only the NEW venue (OLD_TRAFFORD) triggers a fresh API call...
        mock_adapter.get_weather_match_window.assert_called_once()
        # ...but the WRITTEN data must carry BOTH venues — pre-fix, the merge
        # step found zero existing blobs (wrong prefix) and this would be 1.
        assert mock_gated_write.call_count == 1
        written_df = mock_gated_write.call_args.kwargs["data"]
        assert len(written_df) == 2, (
            f"Expected merged write of 2 venues (ANFIELD preserved + OLD_TRAFFORD new), got {len(written_df)}"
        )
        assert set(written_df["venue_id"]) == {"ANFIELD", "OLD_TRAFFORD"}
        assert result.get("weather") == 2

    @pytest.mark.asyncio
    async def test_fixture_parquet_no_venue_name_column(self) -> None:
        """Fixture parquet without 'venue_name' column → record_empty and return."""
        mock_mw = MagicMock()
        mock_mw_cls = MagicMock(return_value=mock_mw)

        # Simulate a parquet blob without venue_name column
        df_no_venue = pd.DataFrame({"fixture_id": ["123"], "date": [_DATE]})
        buf = io.BytesIO()
        df_no_venue.to_parquet(buf)
        parquet_bytes = buf.getvalue()

        mock_blob = MagicMock()
        mock_blob.name = f"sports_reference/by_date/day={_DATE}/entity=fixtures/fixtures.parquet"

        mock_storage = MagicMock()
        mock_storage.list_blobs.return_value = [mock_blob]
        mock_storage.download_bytes.return_value = parquet_bytes

        with _stack(
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
            patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage),
            patch(
                "unified_api_contracts.sports.get_expected_leagues_for_source",
                return_value=[_mk_league("EPL")],
            ),
            patch("instruments_service.engine.orchestrator.log_event"),
            patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
        ):
            result = await _fetch_weather_data(date=_DATE, bucket=_BUCKET)
        assert result == {}
        mock_mw.record_empty.assert_called()
        mock_mw.write.assert_called_once()

    @pytest.mark.asyncio
    async def test_pre_cutoff_date_records_expected_empty(self) -> None:
        """Weather skipped when date is before open_meteo coverage start."""
        from datetime import date as date_type

        mock_mw = MagicMock()
        mock_mw_cls = MagicMock(return_value=mock_mw)
        mock_storage = MagicMock()
        mock_storage.list_blobs.return_value = []
        future_floor = date_type(2027, 1, 1)

        with _stack(
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
            patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage),
            patch(
                "unified_api_contracts.sports.get_expected_leagues_for_source",
                return_value=[_mk_league("EPL")],
            ),
            patch("instruments_service.engine.orchestrator.get_source_coverage_start", return_value=future_floor),
            patch("instruments_service.engine.orchestrator.is_in_known_gap", return_value=False),
            patch("instruments_service.engine.orchestrator.log_event"),
            patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
        ):
            result = await _fetch_weather_data(date=_DATE, bucket=_BUCKET)
        mock_mw.record_expected_empty.assert_called()
        assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# _fetch_sfi_data
# ---------------------------------------------------------------------------


class TestFetchSfiData:
    """Tests for _fetch_sfi_data (lines 6950-7399)."""

    @pytest.mark.asyncio
    async def test_skip_all_leagues_captured_returns_empty(self) -> None:
        mock_mw = MagicMock()
        mock_mw_cls = MagicMock(return_value=mock_mw)
        mock_adapter = MagicMock()

        with _stack(
            patch("instruments_service.engine.orchestrator.create_sports_reference_adapter", return_value=mock_adapter),
            patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
            patch(
                "unified_api_contracts.sports.get_expected_leagues_for_source",
                return_value=[_mk_league("EPL")],
            ),
            patch("instruments_service.engine.orchestrator._should_skip_date_for_per_league", return_value=True),
            patch("instruments_service.engine.orchestrator.get_leagues_needing_refresh", return_value=[]),
        ):
            result = await _fetch_sfi_data(date=_DATE, api_key="key", bucket=_BUCKET)
        assert result == {}

    @pytest.mark.asyncio
    async def test_non_progressive_entity_filter_skips_progressive_block(self) -> None:
        """entity_filter other than SFI_PROGRESSIVE_STATS skips the block."""
        mock_mw = MagicMock()
        mock_mw_cls = MagicMock(return_value=mock_mw)
        mock_adapter = MagicMock()
        # When _want_sfi_progressive=False, skip check is not reached
        with _stack(
            patch("instruments_service.engine.orchestrator.create_sports_reference_adapter", return_value=mock_adapter),
            patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
            patch(
                "unified_api_contracts.sports.get_expected_leagues_for_source",
                return_value=[_mk_league("EPL")],
            ),
            patch("instruments_service.engine.orchestrator._should_skip_date_for_per_league", return_value=False),
            patch("instruments_service.engine.orchestrator.get_leagues_needing_refresh", return_value=[]),
        ):
            # entity_filter="OTHER_ENTITY" → _want_sfi_progressive=False → skip check skipped
            result = await _fetch_sfi_data(date=_DATE, api_key="key", bucket=_BUCKET, entity_filter="OTHER_ENTITY")
        assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_progressive_skip_false_adapter_returns_empty_leagues(self) -> None:
        """Adapter returns no leagues → per-league empty write."""
        mock_mw = MagicMock()
        mock_mw_cls = MagicMock(return_value=mock_mw)
        mock_adapter = MagicMock()
        mock_adapter.get_leagues = AsyncMock(return_value=[])

        with _stack(
            patch("instruments_service.engine.orchestrator.create_sports_reference_adapter", return_value=mock_adapter),
            patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
            patch(
                "unified_api_contracts.sports.get_expected_leagues_for_source",
                return_value=[_mk_league("EPL"), _mk_league("LA_LIGA")],
            ),
            patch("instruments_service.engine.orchestrator._should_skip_date_for_per_league", return_value=False),
            patch("instruments_service.engine.orchestrator._should_skip_shard", return_value=False),
            patch("instruments_service.engine.orchestrator.get_leagues_needing_refresh", return_value=["EPL"]),
            patch("instruments_service.engine.orchestrator._gated_sink_write"),
            patch(
                "instruments_service.engine.orchestrator.stamp_available_at_explicit",
                side_effect=lambda df, **kw: df,
            ),
        ):
            result = await _fetch_sfi_data(date=_DATE, api_key="key", bucket=_BUCKET)
        assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_no_match_ids_writes_record_empty(self) -> None:
        """No match descriptors returned → per-league record_empty (no completed matches)."""
        mock_mw = MagicMock()
        mock_mw_cls = MagicMock(return_value=mock_mw)
        mock_adapter = MagicMock()
        mock_adapter.get_leagues = AsyncMock(return_value=[])
        mock_adapter.get_match_descriptors_for_date = AsyncMock(return_value=[])

        with _stack(
            patch("instruments_service.engine.orchestrator.create_sports_reference_adapter", return_value=mock_adapter),
            patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
            patch("unified_api_contracts.sports.get_expected_leagues_for_source", return_value=[_mk_league("EPL")]),
            patch("instruments_service.engine.orchestrator._should_skip_date_for_per_league", return_value=False),
            patch("instruments_service.engine.orchestrator._read_sfi_league_mapping", return_value=None),
            patch("instruments_service.engine.orchestrator.get_source_coverage_start", return_value=None),
            patch("instruments_service.engine.orchestrator.is_in_known_gap", return_value=False),
            patch("instruments_service.engine.orchestrator.get_leagues_needing_refresh", return_value=["EPL"]),
            patch("instruments_service.engine.orchestrator.log_event"),
        ):
            result = await _fetch_sfi_data(date=_DATE, api_key="key", bucket=_BUCKET)
        mock_mw.record_empty.assert_called()
        assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_match_descriptors_exception_writes_record_failed(self) -> None:
        """Exception in get_match_descriptors_for_date → record_failed per league."""
        mock_mw = MagicMock()
        mock_mw_cls = MagicMock(return_value=mock_mw)
        mock_adapter = MagicMock()
        mock_adapter.get_leagues = AsyncMock(return_value=[])
        mock_adapter.get_match_descriptors_for_date = AsyncMock(side_effect=RuntimeError("timeout"))

        with _stack(
            patch("instruments_service.engine.orchestrator.create_sports_reference_adapter", return_value=mock_adapter),
            patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
            patch("unified_api_contracts.sports.get_expected_leagues_for_source", return_value=[_mk_league("EPL")]),
            patch("instruments_service.engine.orchestrator._should_skip_date_for_per_league", return_value=False),
            patch("instruments_service.engine.orchestrator._read_sfi_league_mapping", return_value=None),
            patch("instruments_service.engine.orchestrator.get_source_coverage_start", return_value=None),
            patch("instruments_service.engine.orchestrator.is_in_known_gap", return_value=False),
            patch("instruments_service.engine.orchestrator.get_leagues_needing_refresh", return_value=["EPL"]),
            patch("instruments_service.engine.orchestrator.classify_and_emit_error"),
            patch("instruments_service.engine.orchestrator._classify_adapter_failure", return_value="RuntimeError"),
            patch("instruments_service.engine.orchestrator.log_event"),
        ):
            result = await _fetch_sfi_data(date=_DATE, api_key="key", bucket=_BUCKET)
        mock_mw.record_failed.assert_called()
        assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_pre_cutoff_date_records_expected_empty(self) -> None:
        """SFI progressive stats skipped when date is before source coverage start."""
        from datetime import date as date_type

        mock_mw = MagicMock()
        mock_mw_cls = MagicMock(return_value=mock_mw)
        mock_adapter = MagicMock()
        mock_adapter.get_leagues = AsyncMock(return_value=[])

        # Return a coverage start AFTER _DATE so _sfi_pp_pre_cutoff=True
        future_floor = date_type(2027, 1, 1)

        with _stack(
            patch("instruments_service.engine.orchestrator.create_sports_reference_adapter", return_value=mock_adapter),
            patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
            patch("unified_api_contracts.sports.get_expected_leagues_for_source", return_value=[_mk_league("EPL")]),
            patch("instruments_service.engine.orchestrator._should_skip_date_for_per_league", return_value=False),
            patch("instruments_service.engine.orchestrator._read_sfi_league_mapping", return_value=None),
            patch("instruments_service.engine.orchestrator.get_source_coverage_start", return_value=future_floor),
            patch("instruments_service.engine.orchestrator.is_in_known_gap", return_value=False),
            patch("instruments_service.engine.orchestrator.get_leagues_needing_refresh", return_value=["EPL"]),
            patch("instruments_service.engine.orchestrator.log_event"),
        ):
            result = await _fetch_sfi_data(date=_DATE, api_key="key", bucket=_BUCKET)
        # record_expected_empty called for the date-level + per-league rows
        mock_mw.record_expected_empty.assert_called()
        assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_progressive_stats_match_loop_writes_captured(self) -> None:
        """Progressive stats match loop: matches found + stats returned → record_captured per league."""
        mock_mw = MagicMock()
        mock_mw_cls = MagicMock(return_value=mock_mw)
        mock_adapter = MagicMock()
        # get_leagues returns empty → sfi_league_ids stays []
        mock_adapter.get_leagues = AsyncMock(return_value=[])
        # match descriptors: one match with championship_id matching EPL's hex
        mock_adapter.get_match_descriptors_for_date = AsyncMock(
            return_value=[{"championship_id": "abc123", "match_id": "m1"}]
        )
        # progressive stats: one row
        mock_adapter.get_progressive_stats = AsyncMock(
            return_value=[{"timer_seconds": 1800, "match_id": "m1", "home_score": 1}]
        )

        sfi_ids = {"EPL": "abc123"}

        with _stack(
            patch("instruments_service.engine.orchestrator.create_sports_reference_adapter", return_value=mock_adapter),
            patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
            patch("unified_api_contracts.sports.get_expected_leagues_for_source", return_value=[_mk_league("EPL")]),
            patch("instruments_service.engine.orchestrator._should_skip_date_for_per_league", return_value=False),
            patch("instruments_service.engine.orchestrator._read_sfi_league_mapping", return_value=None),
            patch("instruments_service.engine.orchestrator.get_source_coverage_start", return_value=None),
            patch("instruments_service.engine.orchestrator.is_in_known_gap", return_value=False),
            patch("instruments_service.engine.orchestrator.get_leagues_needing_refresh", return_value=["EPL"]),
            patch("instruments_service.engine.orchestrator.SOCCER_FOOTBALL_INFO_IDS", sfi_ids),
            patch("instruments_service.engine.orchestrator.get_provider_league_id", return_value="abc123"),
            patch("instruments_service.engine.orchestrator._sfi_detect_match_end_time", return_value=None),
            patch("instruments_service.engine.orchestrator._gated_sink_write"),
            patch(
                "instruments_service.engine.orchestrator._canonical_league_id",
                side_effect=lambda lid: str(lid),
            ),
            patch("instruments_service.engine.orchestrator._sports_ref_source", return_value="soccer_football_info"),
            patch("instruments_service.engine.orchestrator._write_sfi_league_mapping"),
            patch("instruments_service.engine.orchestrator.log_event"),
            patch("instruments_service.engine.orchestrator._maybe_emit_drift_anomaly"),
        ):
            result = await _fetch_sfi_data(date=_DATE, api_key="key", bucket=_BUCKET)

        # record_captured called for EPL league
        mock_mw.record_captured.assert_called()
        assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_progressive_stats_all_fetches_empty_writes_record_empty(self) -> None:
        """Match IDs found but all per-match get_progressive_stats returns [] → record_empty SOURCE_RETURNED_ZERO."""
        mock_mw = MagicMock()
        mock_mw_cls = MagicMock(return_value=mock_mw)
        mock_adapter = MagicMock()
        mock_adapter.get_leagues = AsyncMock(return_value=[])
        mock_adapter.get_match_descriptors_for_date = AsyncMock(
            return_value=[{"championship_id": "abc123", "match_id": "m1"}]
        )
        # all progressive fetches return empty list
        mock_adapter.get_progressive_stats = AsyncMock(return_value=[])

        sfi_ids = {"EPL": "abc123"}

        with _stack(
            patch("instruments_service.engine.orchestrator.create_sports_reference_adapter", return_value=mock_adapter),
            patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
            patch("unified_api_contracts.sports.get_expected_leagues_for_source", return_value=[_mk_league("EPL")]),
            patch("instruments_service.engine.orchestrator._should_skip_date_for_per_league", return_value=False),
            patch("instruments_service.engine.orchestrator._read_sfi_league_mapping", return_value=None),
            patch("instruments_service.engine.orchestrator.get_source_coverage_start", return_value=None),
            patch("instruments_service.engine.orchestrator.is_in_known_gap", return_value=False),
            patch("instruments_service.engine.orchestrator.get_leagues_needing_refresh", return_value=["EPL"]),
            patch("instruments_service.engine.orchestrator.SOCCER_FOOTBALL_INFO_IDS", sfi_ids),
            patch("instruments_service.engine.orchestrator.get_provider_league_id", return_value="abc123"),
            patch("instruments_service.engine.orchestrator._sfi_detect_match_end_time", return_value=None),
            patch("instruments_service.engine.orchestrator._write_sfi_league_mapping"),
            patch("instruments_service.engine.orchestrator.log_event"),
            patch("instruments_service.engine.orchestrator._maybe_emit_drift_anomaly"),
        ):
            result = await _fetch_sfi_data(date=_DATE, api_key="key", bucket=_BUCKET)

        # SOURCE_RETURNED_ZERO path: record_empty called for date-level + per-league
        mock_mw.record_empty.assert_called()
        assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# _fetch_transfermarkt_data
# ---------------------------------------------------------------------------


class TestFetchTransfermarktData:
    """Tests for _fetch_transfermarkt_data (lines 6625-6947)."""

    @pytest.mark.asyncio
    async def test_no_leagues_from_adapter_returns_empty_manifest(self) -> None:
        """Adapter returns no leagues → empty manifest writes per expected league."""
        mock_mw = MagicMock()
        mock_mw_cls = MagicMock(return_value=mock_mw)
        mock_adapter = MagicMock()
        mock_adapter.get_leagues = AsyncMock(return_value=[])

        with _stack(
            patch("instruments_service.engine.orchestrator.create_sports_reference_adapter", return_value=mock_adapter),
            patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
            patch(
                "unified_api_contracts.sports.get_expected_leagues_for_source",
                return_value=[_mk_league("EPL"), _mk_league("BUNDESLIGA")],
            ),
            patch("instruments_service.engine.orchestrator._should_skip_date_for_per_league", return_value=False),
            patch("instruments_service.engine.orchestrator._should_skip_shard", return_value=False),
        ):
            result = await _fetch_transfermarkt_data(
                date=_DATE,
                api_key="key",
                bucket=_BUCKET,
                season=2025,
            )
        assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_skip_all_leagues_captured_returns_empty(self) -> None:
        mock_mw = MagicMock()
        mock_mw_cls = MagicMock(return_value=mock_mw)
        mock_adapter = MagicMock()

        with _stack(
            patch("instruments_service.engine.orchestrator.create_sports_reference_adapter", return_value=mock_adapter),
            patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
            patch(
                "unified_api_contracts.sports.get_expected_leagues_for_source",
                return_value=[_mk_league("EPL")],
            ),
            patch("instruments_service.engine.orchestrator._should_skip_date_for_per_league", return_value=True),
        ):
            result = await _fetch_transfermarkt_data(
                date=_DATE,
                api_key="key",
                bucket=_BUCKET,
                season=2025,
            )
        assert result == {}

    @pytest.mark.asyncio
    async def test_entity_filter_player_values_only(self) -> None:
        """entity_filter='PLAYER_VALUES' sets _want_teams=True."""
        mock_mw = MagicMock()
        mock_mw_cls = MagicMock(return_value=mock_mw)
        mock_adapter = MagicMock()
        mock_adapter.get_leagues = AsyncMock(return_value=[])

        with _stack(
            patch("instruments_service.engine.orchestrator.create_sports_reference_adapter", return_value=mock_adapter),
            patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
            patch(
                "unified_api_contracts.sports.get_expected_leagues_for_source",
                return_value=[_mk_league("EPL")],
            ),
            patch("instruments_service.engine.orchestrator._should_skip_date_for_per_league", return_value=False),
            patch("instruments_service.engine.orchestrator._should_skip_shard", return_value=False),
        ):
            result = await _fetch_transfermarkt_data(
                date=_DATE,
                api_key="key",
                bucket=_BUCKET,
                entity_filter="PLAYER_VALUES",
                season=2025,
            )
        assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_teams_returned_writes_record_captured(self) -> None:
        """Adapter returns teams → record_captured_from_counts called for the mapped league."""
        mock_mw = MagicMock()
        mock_mw_cls = MagicMock(return_value=mock_mw)
        mock_adapter = MagicMock()
        mock_adapter.get_teams = AsyncMock(return_value=[{"name": "Arsenal", "team_id": "1", "squad_size": "20"}])

        with _stack(
            patch("instruments_service.engine.orchestrator.create_sports_reference_adapter", return_value=mock_adapter),
            patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
            patch("unified_api_contracts.sports.get_expected_leagues_for_source", return_value=[_mk_league("EPL")]),
            patch("instruments_service.engine.orchestrator.get_prediction_leagues", return_value=[]),
            patch("instruments_service.engine.orchestrator._should_skip_date_for_per_league", return_value=False),
            patch("instruments_service.engine.orchestrator._read_transfermarkt_team_mapping", return_value=None),
            patch("instruments_service.engine.orchestrator.get_provider_league_id", return_value="531"),
            patch("instruments_service.engine.orchestrator._maybe_emit_drift_anomaly"),
            patch("instruments_service.engine.orchestrator.get_expected_team_count_for_league", return_value=20),
            patch("instruments_service.engine.orchestrator._gated_sink_write"),
            patch("instruments_service.engine.orchestrator._write_transfermarkt_team_mapping"),
            patch("instruments_service.engine.orchestrator._write_master_append"),
            patch("instruments_service.engine.orchestrator._write_snapshot_player_values"),
            patch(
                "instruments_service.engine.orchestrator.stamp_available_at_explicit",
                side_effect=lambda df, **kw: df,
            ),
            patch(
                "instruments_service.engine.orchestrator._canonical_league_id",
                side_effect=lambda lid: str(lid),
            ),
            patch("instruments_service.engine.orchestrator.log_event"),
        ):
            result = await _fetch_transfermarkt_data(date=_DATE, api_key="key", bucket=_BUCKET, season=2025)
        mock_mw.record_captured_from_counts.assert_called_once()
        assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_teams_exception_writes_record_failed(self) -> None:
        """Per-league exception in get_teams → record_failed + shard isolation (no raise)."""
        mock_mw = MagicMock()
        mock_mw_cls = MagicMock(return_value=mock_mw)
        mock_adapter = MagicMock()
        mock_adapter.get_teams = AsyncMock(side_effect=RuntimeError("API timeout"))

        with _stack(
            patch("instruments_service.engine.orchestrator.create_sports_reference_adapter", return_value=mock_adapter),
            patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
            patch("unified_api_contracts.sports.get_expected_leagues_for_source", return_value=[_mk_league("EPL")]),
            patch("instruments_service.engine.orchestrator.get_prediction_leagues", return_value=[]),
            patch("instruments_service.engine.orchestrator._should_skip_date_for_per_league", return_value=False),
            patch("instruments_service.engine.orchestrator._read_transfermarkt_team_mapping", return_value=None),
            patch("instruments_service.engine.orchestrator.get_provider_league_id", return_value="531"),
            patch("instruments_service.engine.orchestrator.classify_and_emit_error"),
            patch("instruments_service.engine.orchestrator._classify_adapter_failure", return_value="API_TIMEOUT"),
            patch(
                "instruments_service.engine.orchestrator._canonical_league_id",
                side_effect=lambda lid: str(lid),
            ),
            patch("instruments_service.engine.orchestrator.log_event"),
        ):
            result = await _fetch_transfermarkt_data(date=_DATE, api_key="key", bucket=_BUCKET, season=2025)
        mock_mw.record_failed.assert_called_once()
        assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_cache_hit_no_triggers_emits_captured_from_cache(self) -> None:
        """Cache hit on non-trigger date: populate _captured_league_counts from cached df."""
        mock_mw = MagicMock()
        mock_mw_cls = MagicMock(return_value=mock_mw)
        mock_adapter = MagicMock()

        # Build a fresh cached DataFrame with canonical_league and available_at columns
        from datetime import UTC, datetime

        import pandas as pd

        _now = datetime.now(UTC)
        cached_df = pd.DataFrame(
            {
                "canonical_league": ["EPL", "EPL", "LA_LIGA"],
                "team_id": ["1", "2", "3"],
                "available_at": [_now, _now, _now],
            }
        )

        with _stack(
            patch("instruments_service.engine.orchestrator.create_sports_reference_adapter", return_value=mock_adapter),
            patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
            patch(
                "unified_api_contracts.sports.get_expected_leagues_for_source",
                return_value=[_mk_league("EPL"), _mk_league("LA_LIGA")],
            ),
            patch("instruments_service.engine.orchestrator.get_prediction_leagues", return_value=[]),
            patch("instruments_service.engine.orchestrator._should_skip_date_for_per_league", return_value=False),
            patch("instruments_service.engine.orchestrator._read_transfermarkt_team_mapping", return_value=cached_df),
            patch("instruments_service.engine.orchestrator._cache_is_fresh", return_value=True),
            patch("instruments_service.engine.orchestrator.get_leagues_needing_refresh", return_value=[]),
            patch(
                "instruments_service.engine.orchestrator._canonical_league_id",
                side_effect=lambda lid: str(lid),
            ),
            patch("instruments_service.engine.orchestrator.log_event"),
        ):
            result = await _fetch_transfermarkt_data(date=_DATE, api_key="key", bucket=_BUCKET, season=2025)
        # Cache hit path: record_captured_from_counts for each canonical league in cache
        mock_mw.record_captured_from_counts.assert_called()
        assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# _fetch_footystats_predictions
# ---------------------------------------------------------------------------


class TestFetchFootystatsPredictions:
    """Tests for _fetch_footystats_predictions season-window clip guard."""

    @pytest.mark.asyncio
    async def test_pre_cutoff_date_records_expected_empty(self) -> None:
        """FootyStats predictions skipped when date is before source coverage start."""
        from datetime import date as date_type

        mock_adapter = MagicMock()
        mock_mw = MagicMock()
        mock_mw_cls = MagicMock(return_value=mock_mw)
        future_floor = date_type(2027, 1, 1)

        with _stack(
            patch("instruments_service.engine.orchestrator.create_sports_reference_adapter", return_value=mock_adapter),
            patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
            patch(
                "unified_api_contracts.sports.get_expected_leagues_for_source",
                return_value=[_mk_league("EPL")],
            ),
            patch("instruments_service.engine.orchestrator._should_skip_date_for_per_league", return_value=False),
            patch("instruments_service.engine.orchestrator.get_source_coverage_start", return_value=future_floor),
            patch("instruments_service.engine.orchestrator.is_in_known_gap", return_value=False),
            patch("instruments_service.engine.orchestrator.log_event"),
        ):
            result = await _fetch_footystats_predictions(date=_DATE, api_key="key", bucket=_BUCKET)
        mock_mw.record_expected_empty.assert_called()
        assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_all_leagues_captured_skip_returns_empty(self) -> None:
        """When every expected league is captured, returns early without touching the API."""
        mock_adapter = MagicMock()
        mock_mw = MagicMock()
        mock_mw_cls = MagicMock(return_value=mock_mw)

        with _stack(
            patch("instruments_service.engine.orchestrator.create_sports_reference_adapter", return_value=mock_adapter),
            patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
            patch(
                "unified_api_contracts.sports.get_expected_leagues_for_source",
                return_value=[_mk_league("EPL")],
            ),
            patch("instruments_service.engine.orchestrator._should_skip_date_for_per_league", return_value=True),
            patch("instruments_service.engine.orchestrator.log_event"),
        ):
            result = await _fetch_footystats_predictions(date=_DATE, api_key="key", bucket=_BUCKET)
        assert result == {}


# ---------------------------------------------------------------------------
# _fetch_footystats_matches
# ---------------------------------------------------------------------------


class TestFetchFootystatsMatches:
    """Tests for _fetch_footystats_matches season-window clip guard."""

    @pytest.mark.asyncio
    async def test_pre_cutoff_date_records_expected_empty(self) -> None:
        """FootyStats matches skipped when date is before source coverage start."""
        from datetime import date as date_type

        mock_adapter = MagicMock()
        mock_mw = MagicMock()
        mock_mw_cls = MagicMock(return_value=mock_mw)
        future_floor = date_type(2027, 1, 1)

        with _stack(
            patch("instruments_service.engine.orchestrator.create_sports_reference_adapter", return_value=mock_adapter),
            patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
            patch(
                "unified_api_contracts.sports.get_expected_leagues_for_source",
                return_value=[_mk_league("EPL")],
            ),
            patch("instruments_service.engine.orchestrator._should_skip_date_for_per_league", return_value=False),
            patch("instruments_service.engine.orchestrator.get_source_coverage_start", return_value=future_floor),
            patch("instruments_service.engine.orchestrator.is_in_known_gap", return_value=False),
            patch("instruments_service.engine.orchestrator.log_event"),
        ):
            result = await _fetch_footystats_matches(date=_DATE, api_key="key", bucket=_BUCKET)
        mock_mw.record_expected_empty.assert_called()
        assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_all_leagues_captured_skip_returns_empty(self) -> None:
        """When every expected league is captured, returns early without touching the API."""
        mock_adapter = MagicMock()
        mock_mw = MagicMock()
        mock_mw_cls = MagicMock(return_value=mock_mw)

        with _stack(
            patch("instruments_service.engine.orchestrator.create_sports_reference_adapter", return_value=mock_adapter),
            patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
            patch(
                "unified_api_contracts.sports.get_expected_leagues_for_source",
                return_value=[_mk_league("EPL")],
            ),
            patch("instruments_service.engine.orchestrator._should_skip_date_for_per_league", return_value=True),
            patch("instruments_service.engine.orchestrator.log_event"),
        ):
            result = await _fetch_footystats_matches(date=_DATE, api_key="key", bucket=_BUCKET)
        assert result == {}


# ---------------------------------------------------------------------------
# Season-window guard — off-season skip (per-date sources)
#
# Each guard runs AFTER the genesis-floor guard: when EVERY expected league is
# in its off-season gap on the date, the whole date is skipped call-free and
# per-league expected-empty rows carry the typed EXPECTED_PRE_SEASON /
# EXPECTED_POST_SEASON reason. We patch ``footystats_season_status_for_day`` to
# return a fixed status (calendar-independent) and assert (a) NO adapter network
# call is made and (b) the off-season reason reaches record_expected_empty.
# ---------------------------------------------------------------------------


class TestOffSeasonSeasonWindowGuard:
    """Off-season season-window guard across the per-date fetchers."""

    @pytest.mark.asyncio
    async def test_understat_xg_all_off_season_skips_call(self) -> None:
        mock_adapter = MagicMock()
        mock_adapter.get_fixtures = AsyncMock(return_value=[])
        mock_adapter._fetch_error_count = 0
        mock_mw = MagicMock()
        mock_mw_cls = MagicMock(return_value=mock_mw)

        with _stack(
            patch("instruments_service.engine.orchestrator.create_sports_reference_adapter", return_value=mock_adapter),
            patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
            patch("unified_api_contracts.sports.get_expected_leagues_for_source", return_value=[_mk_league("EPL")]),
            patch("instruments_service.engine.orchestrator._should_skip_shard", return_value=False),
            patch("instruments_service.engine.orchestrator.get_source_coverage_start", return_value=None),
            patch("instruments_service.engine.orchestrator.is_in_known_gap", return_value=False),
            patch(
                "instruments_service.engine.orchestrator.footystats_season_status_for_day",
                return_value="EXPECTED_POST_SEASON",
            ),
            patch("instruments_service.engine.orchestrator.log_event"),
        ):
            # mid-June date — European leagues off-season
            result = await _fetch_understat_xg(date="2026-06-15", bucket=_BUCKET)
        mock_adapter.get_fixtures.assert_not_called()
        mock_mw.record_expected_empty.assert_called()
        _reasons = {c.kwargs.get("reason") for c in mock_mw.record_expected_empty.call_args_list}
        assert "EXPECTED_POST_SEASON" in _reasons
        assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_understat_shots_all_off_season_skips_call(self) -> None:
        mock_adapter = MagicMock()
        mock_adapter.get_match_ids_for_date = AsyncMock(return_value=[])
        mock_mw = MagicMock()
        mock_mw_cls = MagicMock(return_value=mock_mw)

        with _stack(
            patch("instruments_service.engine.orchestrator.create_sports_reference_adapter", return_value=mock_adapter),
            patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
            patch("unified_api_contracts.sports.get_expected_leagues_for_source", return_value=[_mk_league("EPL")]),
            patch("instruments_service.engine.orchestrator._should_skip_shard", return_value=False),
            patch("instruments_service.engine.orchestrator.get_source_coverage_start", return_value=None),
            patch("instruments_service.engine.orchestrator.is_in_known_gap", return_value=False),
            patch(
                "instruments_service.engine.orchestrator.footystats_season_status_for_day",
                return_value="EXPECTED_PRE_SEASON",
            ),
            patch("instruments_service.engine.orchestrator.log_event"),
        ):
            result = await _run_understat_shots_date(date="2026-06-15", bucket=_BUCKET)
        mock_adapter.get_match_ids_for_date.assert_not_called()
        mock_mw.record_expected_empty.assert_called()
        _reasons = {c.kwargs.get("reason") for c in mock_mw.record_expected_empty.call_args_list}
        assert "EXPECTED_PRE_SEASON" in _reasons
        assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_weather_all_off_season_skips_call(self) -> None:
        mock_mw = MagicMock()
        mock_mw_cls = MagicMock(return_value=mock_mw)

        with _stack(
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
            patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
            patch(
                "unified_api_contracts.sports.get_expected_leagues_for_source",
                return_value=[_mk_league("EPL"), _mk_league("BUNDESLIGA")],
            ),
            patch("instruments_service.engine.orchestrator._should_skip_shard", return_value=False),
            patch("instruments_service.engine.orchestrator.get_source_coverage_start", return_value=None),
            patch("instruments_service.engine.orchestrator.is_in_known_gap", return_value=False),
            patch(
                "instruments_service.engine.orchestrator.footystats_season_status_for_day",
                return_value="EXPECTED_POST_SEASON",
            ),
            patch("instruments_service.engine.orchestrator.get_storage_client", return_value=MagicMock()),
            patch("instruments_service.engine.orchestrator.log_event"),
        ):
            result = await _fetch_weather_data(date="2026-06-15", bucket=_BUCKET)
        # Skipped before the GCS fixtures read → no captured/empty rows, only expected-empty.
        mock_mw.record_captured.assert_not_called()
        mock_mw.record_empty.assert_not_called()
        mock_mw.record_expected_empty.assert_called()
        _reasons = {c.kwargs.get("reason") for c in mock_mw.record_expected_empty.call_args_list}
        assert "EXPECTED_POST_SEASON" in _reasons
        assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_sfi_all_off_season_skips_call(self) -> None:
        mock_adapter = MagicMock()
        mock_adapter.get_leagues = AsyncMock(return_value=[])
        mock_adapter.get_match_descriptors_for_date = AsyncMock(return_value=[])
        mock_mw = MagicMock()
        mock_mw_cls = MagicMock(return_value=mock_mw)

        with _stack(
            patch("instruments_service.engine.orchestrator.create_sports_reference_adapter", return_value=mock_adapter),
            patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
            patch("unified_api_contracts.sports.get_expected_leagues_for_source", return_value=[_mk_league("EPL")]),
            patch("instruments_service.engine.orchestrator._should_skip_date_for_per_league", return_value=False),
            patch("instruments_service.engine.orchestrator._read_sfi_league_mapping", return_value=None),
            patch("instruments_service.engine.orchestrator.get_source_coverage_start", return_value=None),
            patch("instruments_service.engine.orchestrator.is_in_known_gap", return_value=False),
            patch(
                "instruments_service.engine.orchestrator.footystats_season_status_for_day",
                return_value="EXPECTED_POST_SEASON",
            ),
            patch("instruments_service.engine.orchestrator.log_event"),
        ):
            result = await _fetch_sfi_data(date="2026-06-15", api_key="key", bucket=_BUCKET)
        mock_adapter.get_match_descriptors_for_date.assert_not_called()
        mock_mw.record_expected_empty.assert_called()
        _reasons = {c.kwargs.get("reason") for c in mock_mw.record_expected_empty.call_args_list}
        assert "EXPECTED_POST_SEASON" in _reasons
        assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_footystats_predictions_all_off_season_skips_call(self) -> None:
        mock_adapter = MagicMock()
        mock_adapter.get_fixture_predictions = AsyncMock(return_value=[])
        mock_mw = MagicMock()
        mock_mw_cls = MagicMock(return_value=mock_mw)

        with _stack(
            patch("instruments_service.engine.orchestrator.create_sports_reference_adapter", return_value=mock_adapter),
            patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
            patch("unified_api_contracts.sports.get_expected_leagues_for_source", return_value=[_mk_league("EPL")]),
            patch("instruments_service.engine.orchestrator._should_skip_date_for_per_league", return_value=False),
            patch("instruments_service.engine.orchestrator.get_source_coverage_start", return_value=None),
            patch("instruments_service.engine.orchestrator.is_in_known_gap", return_value=False),
            patch(
                "instruments_service.engine.orchestrator.footystats_season_status_for_day",
                return_value="EXPECTED_PRE_SEASON",
            ),
            patch("instruments_service.engine.orchestrator.log_event"),
        ):
            result = await _fetch_footystats_predictions(date="2026-06-15", api_key="key", bucket=_BUCKET)
        mock_adapter.get_fixture_predictions.assert_not_called()
        mock_mw.record_expected_empty.assert_called()
        _reasons = {c.kwargs.get("reason") for c in mock_mw.record_expected_empty.call_args_list}
        assert "EXPECTED_PRE_SEASON" in _reasons
        assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_footystats_matches_all_off_season_skips_call(self) -> None:
        mock_adapter = MagicMock()
        mock_adapter.get_fixtures = AsyncMock(return_value=[])
        mock_mw = MagicMock()
        mock_mw_cls = MagicMock(return_value=mock_mw)

        with _stack(
            patch("instruments_service.engine.orchestrator.create_sports_reference_adapter", return_value=mock_adapter),
            patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
            patch("unified_api_contracts.sports.get_expected_leagues_for_source", return_value=[_mk_league("EPL")]),
            patch("instruments_service.engine.orchestrator._should_skip_date_for_per_league", return_value=False),
            patch("instruments_service.engine.orchestrator.get_source_coverage_start", return_value=None),
            patch("instruments_service.engine.orchestrator.is_in_known_gap", return_value=False),
            patch(
                "instruments_service.engine.orchestrator.footystats_season_status_for_day",
                return_value="EXPECTED_POST_SEASON",
            ),
            patch("instruments_service.engine.orchestrator.log_event"),
        ):
            result = await _fetch_footystats_matches(date="2026-06-15", api_key="key", bucket=_BUCKET)
        mock_adapter.get_fixtures.assert_not_called()
        mock_mw.record_expected_empty.assert_called()
        _reasons = {c.kwargs.get("reason") for c in mock_mw.record_expected_empty.call_args_list}
        assert "EXPECTED_POST_SEASON" in _reasons
        assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# Transfer-window guard — Transfermarkt PLAYER_VALUES
#
# Outside every expected league's transfer window AND no league needs a refresh
# today → skip the API call, record per-league EXPECTED_OUTSIDE_TRANSFER_WINDOW.
# A within-window date (or a refresh trigger) still fetches.
# ---------------------------------------------------------------------------


class TestTransfermarktTransferWindowGuard:
    """Transfer-window guard on _fetch_transfermarkt_data (PLAYER_VALUES)."""

    @pytest.mark.asyncio
    async def test_outside_window_non_trigger_skips_call(self) -> None:
        from unified_api_contracts.canonical.crosscutting.honest_coverage import EmptyConfirmedReason

        mock_adapter = MagicMock()
        mock_adapter.get_teams = AsyncMock(return_value=[{"name": "Arsenal", "team_id": "1", "squad_size": "20"}])
        mock_mw = MagicMock()
        mock_mw_cls = MagicMock(return_value=mock_mw)

        with _stack(
            patch("instruments_service.engine.orchestrator.create_sports_reference_adapter", return_value=mock_adapter),
            patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
            patch("unified_api_contracts.sports.get_expected_leagues_for_source", return_value=[_mk_league("EPL")]),
            patch("instruments_service.engine.orchestrator.get_prediction_leagues", return_value=[]),
            patch("instruments_service.engine.orchestrator._should_skip_date_for_per_league", return_value=False),
            patch("instruments_service.engine.orchestrator.is_transfer_window_open", return_value=False),
            patch("instruments_service.engine.orchestrator.get_leagues_needing_refresh", return_value=[]),
            patch("instruments_service.engine.orchestrator.log_event"),
        ):
            result = await _fetch_transfermarkt_data(
                date=_DATE,
                api_key="key",
                bucket=_BUCKET,
                entity_filter="PLAYER_VALUES",
                season=2025,
            )
        mock_adapter.get_teams.assert_not_called()
        mock_mw.record_empty.assert_called()
        _reasons = {c.kwargs.get("reason") for c in mock_mw.record_empty.call_args_list}
        assert EmptyConfirmedReason.EXPECTED_OUTSIDE_TRANSFER_WINDOW in _reasons
        assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_within_window_still_fetches(self) -> None:
        mock_adapter = MagicMock()
        mock_adapter.get_teams = AsyncMock(return_value=[{"name": "Arsenal", "team_id": "1", "squad_size": "20"}])
        mock_mw = MagicMock()
        mock_mw_cls = MagicMock(return_value=mock_mw)

        with _stack(
            patch("instruments_service.engine.orchestrator.create_sports_reference_adapter", return_value=mock_adapter),
            patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
            patch("unified_api_contracts.sports.get_expected_leagues_for_source", return_value=[_mk_league("EPL")]),
            patch("instruments_service.engine.orchestrator.get_prediction_leagues", return_value=[]),
            patch("instruments_service.engine.orchestrator._should_skip_date_for_per_league", return_value=False),
            patch("instruments_service.engine.orchestrator.is_transfer_window_open", return_value=True),
            patch("instruments_service.engine.orchestrator.get_leagues_needing_refresh", return_value=[]),
            patch("instruments_service.engine.orchestrator._read_transfermarkt_team_mapping", return_value=None),
            patch("instruments_service.engine.orchestrator.get_provider_league_id", return_value="531"),
            patch("instruments_service.engine.orchestrator._maybe_emit_drift_anomaly"),
            patch("instruments_service.engine.orchestrator.get_expected_team_count_for_league", return_value=20),
            patch("instruments_service.engine.orchestrator._gated_sink_write"),
            patch("instruments_service.engine.orchestrator._write_transfermarkt_team_mapping"),
            patch("instruments_service.engine.orchestrator._write_master_append"),
            patch("instruments_service.engine.orchestrator._write_snapshot_player_values"),
            patch(
                "instruments_service.engine.orchestrator.stamp_available_at_explicit",
                side_effect=lambda df, **kw: df,
            ),
            patch("instruments_service.engine.orchestrator.log_event"),
        ):
            result = await _fetch_transfermarkt_data(
                date=_DATE,
                api_key="key",
                bucket=_BUCKET,
                entity_filter="PLAYER_VALUES",
                season=2025,
            )
        # Within the transfer window → the guard does NOT short-circuit; the API is hit.
        mock_adapter.get_teams.assert_awaited()
        assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_force_bypasses_window_guard(self) -> None:
        mock_adapter = MagicMock()
        mock_adapter.get_teams = AsyncMock(return_value=[{"name": "Arsenal", "team_id": "1", "squad_size": "20"}])
        mock_mw = MagicMock()
        mock_mw_cls = MagicMock(return_value=mock_mw)

        with _stack(
            patch("instruments_service.engine.orchestrator.create_sports_reference_adapter", return_value=mock_adapter),
            patch("instruments_service.engine.orchestrator._sports_ref_sink_for", return_value=MagicMock()),
            patch("instruments_service.engine.orchestrator.ManifestWriter", mock_mw_cls),
            patch("unified_api_contracts.sports.get_expected_leagues_for_source", return_value=[_mk_league("EPL")]),
            patch("instruments_service.engine.orchestrator.get_prediction_leagues", return_value=[]),
            patch("instruments_service.engine.orchestrator._should_skip_date_for_per_league", return_value=False),
            patch("instruments_service.engine.orchestrator.is_transfer_window_open", return_value=False),
            patch("instruments_service.engine.orchestrator.get_leagues_needing_refresh", return_value=[]),
            patch("instruments_service.engine.orchestrator._read_transfermarkt_team_mapping", return_value=None),
            patch("instruments_service.engine.orchestrator.get_provider_league_id", return_value="531"),
            patch("instruments_service.engine.orchestrator._maybe_emit_drift_anomaly"),
            patch("instruments_service.engine.orchestrator.get_expected_team_count_for_league", return_value=20),
            patch("instruments_service.engine.orchestrator._gated_sink_write"),
            patch("instruments_service.engine.orchestrator._write_transfermarkt_team_mapping"),
            patch("instruments_service.engine.orchestrator._write_master_append"),
            patch("instruments_service.engine.orchestrator._write_snapshot_player_values"),
            patch(
                "instruments_service.engine.orchestrator.stamp_available_at_explicit",
                side_effect=lambda df, **kw: df,
            ),
            patch("instruments_service.engine.orchestrator.log_event"),
        ):
            result = await _fetch_transfermarkt_data(
                date=_DATE,
                api_key="key",
                bucket=_BUCKET,
                entity_filter="PLAYER_VALUES",
                season=2025,
                force=True,
            )
        # force=True → window guard is skipped even outside the window.
        mock_adapter.get_teams.assert_awaited()
        assert isinstance(result, dict)
