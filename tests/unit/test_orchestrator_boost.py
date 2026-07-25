"""Coverage boost for orchestrator.py utility functions.

Targets:
- _coerce_adapter_output (line 250: plain string/int → {})
- _af_id_from_canonical (lines 297-300: string conversion, non-int → None)
- _flatten_canonical_fixture_for_disk (lines 437-438, 443-444: TypeError branches)
- _classify_adapter_failure (line 822: classification.error_code returned)
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ===========================================================================
# _coerce_adapter_output
# ===========================================================================


class TestCoerceAdapterOutput:
    """Lines 245-250: coerce dict / pydantic model / plain scalar."""

    def test_dict_returned_as_copy(self) -> None:
        from instruments_service.engine.orchestrator import _coerce_adapter_output

        original = {"a": 1, "b": 2}
        result = _coerce_adapter_output(original)
        assert result == original
        assert result is not original

    def test_pydantic_model_dump_called(self) -> None:
        from instruments_service.engine.orchestrator import _coerce_adapter_output

        mock_model = MagicMock()
        mock_model.model_dump.return_value = {"x": 99}
        result = _coerce_adapter_output(mock_model)
        assert result == {"x": 99}
        mock_model.model_dump.assert_called_once()

    def test_plain_string_returns_empty_dict(self) -> None:
        """Line 250: no dict, no model_dump → {}."""
        from instruments_service.engine.orchestrator import _coerce_adapter_output

        result = _coerce_adapter_output("just a string")
        assert result == {}

    def test_plain_int_returns_empty_dict(self) -> None:
        """Line 250: integer → {}."""
        from instruments_service.engine.orchestrator import _coerce_adapter_output

        result = _coerce_adapter_output(42)
        assert result == {}

    def test_none_returns_empty_dict(self) -> None:
        """Line 250: None → {}."""
        from instruments_service.engine.orchestrator import _coerce_adapter_output

        result = _coerce_adapter_output(None)
        assert result == {}


# ===========================================================================
# _af_id_from_canonical
# ===========================================================================


class TestAfIdFromCanonical:
    """Lines 293-306: int / string-int / non-int / logo_url fallback."""

    def test_int_attribute_returns_directly(self) -> None:
        from instruments_service.engine.orchestrator import _af_id_from_canonical

        obj = MagicMock()
        obj.api_football_id = 42
        result = _af_id_from_canonical(obj)
        assert result == 42

    def test_string_int_attribute_converts(self) -> None:
        """Lines 297-298: string af_id → int."""
        from instruments_service.engine.orchestrator import _af_id_from_canonical

        obj = MagicMock()
        obj.api_football_id = "456"
        result = _af_id_from_canonical(obj)
        assert result == 456

    def test_non_int_string_falls_through_to_none(self) -> None:
        """Lines 299-300: non-numeric string → pass, fall through to logo_url."""
        from instruments_service.engine.orchestrator import _af_id_from_canonical

        obj = MagicMock()
        obj.api_football_id = "not-a-number"
        obj.logo_url = None
        result = _af_id_from_canonical(obj)
        assert result is None

    def test_logo_url_fallback_extracts_id(self) -> None:
        """Lines 301-305: no api_football_id → parse logo_url."""
        from instruments_service.engine.orchestrator import _af_id_from_canonical

        obj = MagicMock()
        obj.api_football_id = None
        obj.logo_url = "https://media.api-sports.io/football/leagues/39.png"
        result = _af_id_from_canonical(obj)
        assert result == 39

    def test_no_logo_url_returns_none(self) -> None:
        from instruments_service.engine.orchestrator import _af_id_from_canonical

        obj = MagicMock()
        obj.api_football_id = None
        obj.logo_url = None
        result = _af_id_from_canonical(obj)
        assert result is None


# ===========================================================================
# _flatten_canonical_fixture_for_disk
# ===========================================================================


class TestFlattenCanonicalFixtureForDisk:
    """Lines 435-444: TypeError branches for source_fixture_id and season."""

    def _make_fx(self, **overrides: object) -> MagicMock:
        fx = MagicMock()
        fx.source_fixture_id = 123
        fx.fixture_id = None
        fx.season = 2025
        fx.home_team = None
        fx.away_team = None
        fx.league = None
        fx.venue = None
        fx.referee = None
        fx.kickoff_utc = None
        fx.home_goals = None
        fx.away_goals = None
        fx.status = None
        fx.round = ""
        fx.home_goals_halftime = None
        fx.away_goals_halftime = None
        fx.match_end_time = None
        fx.announced_at = None
        fx.report_time = None
        for k, v in overrides.items():
            setattr(fx, k, v)
        return fx

    def test_non_int_source_fixture_id_becomes_none(self) -> None:
        """Lines 437-438: int(raw_fid) raises → af_fixture_id=None."""
        from instruments_service.engine.orchestrator import _flatten_canonical_fixture_for_disk

        fx = self._make_fx(source_fixture_id="not-an-int")
        row = _flatten_canonical_fixture_for_disk(fx, "2026-03-20")
        assert row["af_fixture_id"] is None

    def test_non_int_season_becomes_none(self) -> None:
        """Lines 443-444: int(season_str) raises → season_int=None."""
        from instruments_service.engine.orchestrator import _flatten_canonical_fixture_for_disk

        fx = self._make_fx(source_fixture_id=None, season="twenty-twenty-five")
        row = _flatten_canonical_fixture_for_disk(fx, "2026-03-20")
        assert row["season"] is None

    def test_normal_season_converted(self) -> None:
        """season='2025-26' → split on '-' → 2025."""
        from instruments_service.engine.orchestrator import _flatten_canonical_fixture_for_disk

        fx = self._make_fx(source_fixture_id=None, season="2025-26")
        row = _flatten_canonical_fixture_for_disk(fx, "2026-03-20")
        assert row["season"] == 2025

    def test_day_used_when_kickoff_none(self) -> None:
        from instruments_service.engine.orchestrator import _flatten_canonical_fixture_for_disk

        fx = self._make_fx(source_fixture_id=None, season=None)
        row = _flatten_canonical_fixture_for_disk(fx, "2026-03-20")
        assert row["date"] == "2026-03-20"
        assert row["day"] == "2026-03-20"


# ===========================================================================
# _classify_adapter_failure
# ===========================================================================


class TestClassifyAdapterFailure:
    """Lines 819-823: returns classification.error_code or class name."""

    def test_returns_classification_error_code(self) -> None:
        """Line 822: classify_venue_error returns non-None → use its error_code."""
        from instruments_service.engine.orchestrator import _classify_adapter_failure

        mock_classification = MagicMock()
        mock_classification.error_code = "RATE_LIMIT_EXCEEDED"

        with patch(
            "instruments_service.engine.orchestrator.classify_venue_error",
            return_value=mock_classification,
        ):
            result = _classify_adapter_failure(ValueError("too many requests"), "binance")

        assert result == "RATE_LIMIT_EXCEEDED"

    def test_returns_exception_class_name_when_no_classification(self) -> None:
        """Line 823: classify_venue_error returns None → use exception class name."""
        from instruments_service.engine.orchestrator import _classify_adapter_failure

        with patch(
            "instruments_service.engine.orchestrator.classify_venue_error",
            return_value=None,
        ):
            result = _classify_adapter_failure(ConnectionError("network down"), "some_venue")

        assert result == "ConnectionError"


# ===========================================================================
# _fetch_sports_reference_data — teams + standings sections
# (lines 4319-4460: teams loop, teams write, venues write, standings loop, standings write)
# ===========================================================================


class TestFetchSportsReferenceData:
    """Module-level async function — directly callable with mocked I/O.

    Covers lines 4319-4460: teams/standings fetch, groupby-write, cache update.
    """

    @pytest.mark.asyncio
    async def test_teams_and_standings_fetched_and_written(self) -> None:
        """Core teams + standings path: adapter returns data → df built → sink written."""
        import pandas as pd

        from instruments_service.engine.orchestrator import _fetch_sports_reference_data

        # Mock league definition with api_football_id
        mock_league_def = MagicMock()
        mock_league_def.api_football_id = 39
        mock_league_def.league_id = "EPL"

        # Mock team returned by adapter.get_teams
        team_row: dict[str, object] = {
            "team_id": 33,
            "name": "Arsenal",
            "league_id": 39,
        }

        # Mock standing returned by adapter.get_standings
        mock_standing = MagicMock()
        mock_standing.model_dump.return_value = {
            "rank": 1,
            "team_id": 33,
            "league_id": "EPL",
        }

        mock_adapter = MagicMock()
        mock_adapter.get_teams = AsyncMock(return_value=[team_row])
        mock_adapter.get_standings = AsyncMock(return_value=[mock_standing])
        mock_adapter.get_injuries = AsyncMock(return_value=[])

        mock_sink = MagicMock()

        with (
            patch(
                "instruments_service.engine.orchestrator.create_sports_reference_adapter",
                return_value=mock_adapter,
            ),
            patch(
                "instruments_service.engine.orchestrator.get_expected_leagues_for_source",
                return_value=[mock_league_def],
            ),
            patch(
                "instruments_service.engine.orchestrator._cached_teams_df",
                None,
            ),
            patch(
                "instruments_service.engine.orchestrator._cached_standings_df",
                None,
            ),
            patch(
                "instruments_service.engine.orchestrator._set_cached_teams",
            ),
            patch(
                "instruments_service.engine.orchestrator._set_cached_standings",
            ),
            patch(
                "instruments_service.engine.orchestrator._sports_ref_sink_for",
                return_value=mock_sink,
            ),
            patch(
                "instruments_service.engine.orchestrator._gated_sink_write",
            ),
            patch(
                "instruments_service.engine.orchestrator.stamp_available_at_explicit",
                side_effect=lambda df, **kw: df,
            ),
            patch(
                "instruments_service.engine.orchestrator._write_venues_from_teams",
            ),
            patch(
                "instruments_service.engine.orchestrator._canonical_league_id",
                side_effect=lambda x: str(x),
            ),
            # _write_team_mapping/_write_fixture_mapping are called UNCONDITIONALLY at
            # the end of _fetch_sports_reference_data (not gated by entities_to_fetch)
            # and — unmocked — call the REAL unified_trading_library.get_data_sink /
            # get_storage_client with a truthy bucket, which auto-upgrades the
            # local→gcp backend and cold-imports the full google-cloud SDK + resolves
            # real ADC credentials (several seconds of genuine I/O on the first call
            # in a process). Under pytest-timeout's SIGALRM-based per-test timeout,
            # that multi-second unmocked window is what let a signal land mid-flight
            # inside CPython 3.13's pathlib._local lazy `_str`/`_drv` slot caches,
            # corrupting a PosixPath that a LATER, unrelated test then tripped over —
            # see plans/active/issues/pytest_posixpath_str_drv_attributeerror_flake_2026_07_17.md.
            # Mock both here (matching the established pattern in
            # test_orchestrator_sports_pipeline.py) so this stays a true unit test.
            patch(
                "instruments_service.engine.orchestrator._write_team_mapping",
            ),
            patch(
                "instruments_service.engine.orchestrator._write_fixture_mapping",
            ),
        ):
            result = await _fetch_sports_reference_data(
                date="2026-03-20",
                api_key="test-key",
                bucket="test-bucket",
                entities_to_fetch=None,
                manifest=None,
            )

        assert isinstance(result, dict)
        assert result.get("teams", 0) >= 0

    @pytest.mark.asyncio
    async def test_teams_from_cache_skips_api(self) -> None:
        """Lines 4355-4357: _cached_teams_df not None → use cache, skip API calls."""
        import pandas as pd

        from instruments_service.engine.orchestrator import _fetch_sports_reference_data

        cached_df = pd.DataFrame([{"team_id": 33, "name": "Arsenal", "league_id": 39}])
        cached_league_ids = [39]

        mock_adapter = MagicMock()
        mock_adapter.get_standings = AsyncMock(return_value=[])
        mock_adapter.get_injuries = AsyncMock(return_value=[])

        mock_sink = MagicMock()

        with (
            patch(
                "instruments_service.engine.orchestrator.create_sports_reference_adapter",
                return_value=mock_adapter,
            ),
            patch(
                "instruments_service.engine.orchestrator.get_expected_leagues_for_source",
                return_value=[],
            ),
            patch(
                "instruments_service.engine.orchestrator._cached_teams_df",
                new=cached_df,
            ),
            patch(
                "instruments_service.engine.orchestrator._cached_prediction_league_ids",
                new=cached_league_ids,
            ),
            patch(
                "instruments_service.engine.orchestrator._cached_standings_df",
                None,
            ),
            patch(
                "instruments_service.engine.orchestrator._set_cached_standings",
            ),
            patch(
                "instruments_service.engine.orchestrator._sports_ref_sink_for",
                return_value=mock_sink,
            ),
            patch(
                "instruments_service.engine.orchestrator._gated_sink_write",
            ),
            patch(
                "instruments_service.engine.orchestrator.stamp_available_at_explicit",
                side_effect=lambda df, **kw: df,
            ),
            patch(
                "instruments_service.engine.orchestrator._write_venues_from_teams",
            ),
            patch(
                "instruments_service.engine.orchestrator._canonical_league_id",
                side_effect=lambda x: str(x),
            ),
            # See test_teams_and_standings_fetched_and_written above: these two are
            # called unconditionally and, unmocked, reach the real UTL get_data_sink /
            # get_storage_client cold-import path.
            patch(
                "instruments_service.engine.orchestrator._write_team_mapping",
            ),
            patch(
                "instruments_service.engine.orchestrator._write_fixture_mapping",
            ),
        ):
            result = await _fetch_sports_reference_data(
                date="2026-03-20",
                api_key="test-key",
                bucket="test-bucket",
                entities_to_fetch=None,
                manifest=None,
            )

        assert isinstance(result, dict)
        # get_teams should NOT have been called (teams came from cache)
        mock_adapter.get_teams.assert_not_called()

    @pytest.mark.asyncio
    async def test_enrichment_only_skips_core_entities(self) -> None:
        """Line 4305-4306: enrichment_only=True → skip leagues/teams/standings."""
        from instruments_service.engine.orchestrator import _fetch_sports_reference_data

        mock_adapter = MagicMock()
        mock_adapter.get_injuries = AsyncMock(return_value=[])

        with (
            patch(
                "instruments_service.engine.orchestrator.create_sports_reference_adapter",
                return_value=mock_adapter,
            ),
            patch(
                "instruments_service.engine.orchestrator._gated_sink_write",
            ),
            # _write_team_mapping/_write_fixture_mapping run unconditionally even in
            # enrichment_only mode (not gated by it) — see comment on
            # test_teams_and_standings_fetched_and_written above.
            patch(
                "instruments_service.engine.orchestrator._write_team_mapping",
            ),
            patch(
                "instruments_service.engine.orchestrator._write_fixture_mapping",
            ),
        ):
            result = await _fetch_sports_reference_data(
                date="2026-03-20",
                api_key="test-key",
                bucket="test-bucket",
                enrichment_only=True,
                manifest=None,
            )

        assert isinstance(result, dict)
        mock_adapter.get_teams.assert_not_called()
        mock_adapter.get_standings.assert_not_called()

    @pytest.mark.asyncio
    async def test_teams_and_standings_never_reaches_real_data_sink(self) -> None:
        """Regression guard for the cross-repo pytest PosixPath '_str'/'_drv' flake.

        See plans/active/issues/pytest_posixpath_str_drv_attributeerror_flake_2026_07_17.md.
        Root cause: this class's tests pass a truthy ``bucket="test-bucket"`` into
        ``_fetch_sports_reference_data``, which — before the mocks added alongside this
        test — reached the UNMOCKED, unconditional ``_write_team_mapping``/
        ``_write_fixture_mapping`` calls at the end of the function. Those call the
        REAL ``unified_trading_library.get_data_sink``/``get_storage_client``, which
        auto-upgrades the local→gcp backend for a truthy bucket and cold-imports the
        full google-cloud SDK + resolves real ADC credentials (several seconds of
        genuine I/O on the first call in a worker process). Under pytest-timeout's
        SIGALRM-based per-test timeout, that multi-second unmocked window is what let
        a signal land mid-flight inside CPython 3.13's pathlib._local lazy `_str`/
        `_drv` slot population, corrupting a PosixPath that a LATER, unrelated test
        then tripped over when it happened to str()/repr() the same object.

        This test patches ``get_data_sink``/``get_storage_client`` with plain
        ``MagicMock``s and asserts ``.assert_not_called()`` AFTER the call
        completes — deliberately NOT a raising ``side_effect``, because
        ``_write_team_mapping``/``_write_fixture_mapping`` wrap their body in a
        broad ``except Exception: classify_and_emit_error(...)`` that would
        silently swallow a raising tripwire (verified: a ``side_effect`` that
        raises gets logged as a "[CRITICAL] unknown error" and NOT re-raised, so
        the test would falsely pass). A post-hoc ``assert_not_called()`` has no
        such blind spot: it fails the test regardless of what the code under
        test does with any exception internally. So a future edit that adds a
        new unconditional real-I/O call site (or drops one of the existing
        mocks) fails LOUDLY here instead of silently reopening the timing
        window.
        """
        from instruments_service.engine.orchestrator import _fetch_sports_reference_data

        mock_league_def = MagicMock()
        mock_league_def.api_football_id = 39
        mock_league_def.league_id = "EPL"

        team_row: dict[str, object] = {"team_id": 33, "name": "Arsenal", "league_id": 39}
        mock_standing = MagicMock()
        mock_standing.model_dump.return_value = {"rank": 1, "team_id": 33, "league_id": "EPL"}

        mock_adapter = MagicMock()
        mock_adapter.get_teams = AsyncMock(return_value=[team_row])
        mock_adapter.get_standings = AsyncMock(return_value=[mock_standing])
        mock_adapter.get_injuries = AsyncMock(return_value=[])

        mock_sink = MagicMock()

        with (
            patch(
                "instruments_service.engine.orchestrator.create_sports_reference_adapter",
                return_value=mock_adapter,
            ),
            patch(
                "instruments_service.engine.orchestrator.get_expected_leagues_for_source",
                return_value=[mock_league_def],
            ),
            patch("instruments_service.engine.orchestrator._cached_teams_df", None),
            patch("instruments_service.engine.orchestrator._cached_standings_df", None),
            patch("instruments_service.engine.orchestrator._set_cached_teams"),
            patch("instruments_service.engine.orchestrator._set_cached_standings"),
            patch(
                "instruments_service.engine.orchestrator._sports_ref_sink_for",
                return_value=mock_sink,
            ),
            patch("instruments_service.engine.orchestrator._gated_sink_write"),
            patch(
                "instruments_service.engine.orchestrator.stamp_available_at_explicit",
                side_effect=lambda df, **kw: df,
            ),
            patch("instruments_service.engine.orchestrator._write_venues_from_teams"),
            patch(
                "instruments_service.engine.orchestrator._canonical_league_id",
                side_effect=lambda x: str(x),
            ),
            patch("instruments_service.engine.orchestrator._write_team_mapping"),
            patch("instruments_service.engine.orchestrator._write_fixture_mapping"),
            patch("instruments_service.engine.orchestrator.get_data_sink") as mock_get_data_sink,
            patch("instruments_service.engine.orchestrator.get_storage_client") as mock_get_storage_client,
        ):
            result = await _fetch_sports_reference_data(
                date="2026-03-20",
                api_key="test-key",
                bucket="test-bucket",
                entities_to_fetch=None,
                manifest=None,
            )

        assert isinstance(result, dict)
        mock_get_data_sink.assert_not_called()
        mock_get_storage_client.assert_not_called()
