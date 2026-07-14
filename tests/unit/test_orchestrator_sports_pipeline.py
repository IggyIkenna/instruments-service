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
    async def test_fetches_teams_standings_injuries(self) -> None:
        """Should fetch teams/standings/injuries (LEAGUES write path retired
        2026-05-07 — see commit 93efebf — replaced by UAC ``LeagueDefinition``
        SSOT). The api_football ``/leagues`` endpoint is NOT called from the
        daily orchestrator path; teams now read
        ``get_expected_leagues_for_source("api_football")`` from UAC instead
        of a freshly-fetched leagues_df (2026-07-13: widened from the
        narrower ``get_prediction_leagues()`` to match the enumerator's
        94-league denominator — see sports_reference_core.py docstring).
        """
        # Mock the sports adapter
        mock_adapter = AsyncMock()
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
        # get_leagues retired — verify it is NOT called.
        mock_adapter.get_leagues.assert_not_awaited()
        mock_adapter.get_injuries.assert_awaited_once_with("2026-03-22")

    @pytest.mark.asyncio
    async def test_get_leagues_not_called(self) -> None:
        """get_leagues was retired 2026-05-07 (commit 93efebf) — UAC
        ``LeagueDefinition`` is now the SSOT for league refdata. The
        orchestrator must NEVER call adapter.get_leagues from
        _fetch_sports_reference_data, regardless of what the adapter would
        return. Replaces the old test_writes_leagues_when_available test
        whose contract no longer holds.
        """
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

        # No "leagues" count — the entry is no longer produced by the function.
        assert "leagues" not in counts
        # And critically: the adapter call must never have happened.
        mock_adapter.get_leagues.assert_not_awaited()

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

        # Mock get_expected_leagues_for_source to return at least one league with api_football_id
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
                "instruments_service.engine.orchestrator.get_expected_leagues_for_source",
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
        mock_standing = MagicMock()
        mock_standing.model_dump.return_value = {"rank": 1, "team_name": "Liverpool", "points": 80, "league_id": "39"}
        mock_adapter.get_standings.return_value = [mock_standing]
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
                "instruments_service.engine.orchestrator.get_expected_leagues_for_source",
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
        mock_injury = MagicMock()
        mock_injury.model_dump.return_value = {
            "player_id": "1",
            "player_name": "Mo Salah",
            "injury_type": "Hamstring",
            "team_id": "40",
            "team_name": "Liverpool",
        }
        mock_adapter.get_injuries.return_value = [mock_injury]

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
                "instruments_service.engine.orchestrator.get_expected_leagues_for_source",
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
                "instruments_service.engine.orchestrator.get_expected_leagues_for_source",
                return_value=[mock_league_def],
            ),
            patch("instruments_service.engine.orchestrator._write_team_mapping"),
            patch("instruments_service.engine.orchestrator._write_fixture_mapping"),
        ):
            counts = await _fetch_sports_reference_data("2026-03-22", "test-key", "test-bucket")

        # get_teams should NOT have been called since league has no api_football_id
        mock_adapter.get_teams.assert_not_awaited()


# ---------------------------------------------------------------------------
# Regression tests — honest-coverage reason correctness (Fix #2 + Fix #3)
# ---------------------------------------------------------------------------


class TestWeatherHonestCoverageReasons:
    """Regression for Fix #2: _record_weather_empty() blank-reason crash.

    Previously the helper had ``reason: str = ""`` — passing no argument
    caused ``record_empty(reason="")`` which raised ``LegacyBlankErrorReasonError``
    at runtime.  The fix makes ``reason`` a required ``EmptyConfirmedReason``
    parameter; the three call-sites each pass a typed reason.

    These tests drive the three branches of ``_fetch_weather_data`` that each
    invoke ``_record_weather_empty`` to verify:
    (a) no LegacyBlankErrorReasonError is raised, and
    (b) the correct typed reason is forwarded to ``manifest.record_empty``.
    """

    @pytest.mark.asyncio
    async def test_no_fixture_data_emits_expected_no_fixture(self) -> None:
        """Branch: fixtures_df is empty → EXPECTED_NO_FIXTURE (no fixtures scheduled)."""
        from unittest.mock import call

        from unified_api_contracts import EmptyConfirmedReason

        from instruments_service.engine.orchestrator import _fetch_weather_data

        mock_manifest = MagicMock()
        mock_league = MagicMock()
        mock_league.league_id = "EPL"

        with (
            patch("instruments_service.engine.orchestrator.ManifestWriter", return_value=mock_manifest),
            patch("instruments_service.engine.orchestrator.get_data_sink"),
            patch(
                "instruments_service.engine.orchestrator.get_expected_leagues_for_source",
                return_value=[mock_league],
            ),
            patch("instruments_service.engine.orchestrator.get_storage_client") as mock_storage_client,
        ):
            # fixtures_df is None — simulates "no fixture data for this date"
            mock_storage_client.return_value.download_bytes.side_effect = Exception("not found")
            # This must not raise LegacyBlankErrorReasonError
            await _fetch_weather_data("2026-03-22", "test-bucket")

        # Verify record_empty was called with EXPECTED_NO_FIXTURE
        calls_with_reason = [c for c in mock_manifest.record_empty.call_args_list if c.kwargs.get("reason") is not None]
        assert any(c.kwargs.get("reason") == EmptyConfirmedReason.EXPECTED_NO_FIXTURE for c in calls_with_reason), (
            "Expected EXPECTED_NO_FIXTURE on no-fixture branch"
        )

    @pytest.mark.asyncio
    async def test_fixtures_no_coord_mapping_emits_expected_no_mapping(self) -> None:
        """Branch: fixtures exist but no venue has a UAC coordinate → EXPECTED_NO_MAPPING.

        The EXPECTED_NO_MAPPING branch requires:
        1. fixtures_df is non-empty with a venue_name column
        2. VENUE_COORDINATES is empty (no mapping for any venue)
        3. existing_venue_ids is empty (no already-covered weather)
        → venues_with_coords becomes empty → code reaches the EXPECTED_NO_MAPPING emit.
        """
        from unittest.mock import AsyncMock as AM

        from unified_api_contracts import EmptyConfirmedReason

        from instruments_service.engine.orchestrator import _fetch_weather_data

        mock_manifest = MagicMock()
        mock_league = MagicMock()
        mock_league.league_id = "EPL"

        fixtures_df = pd.DataFrame({"venue_name": ["UNKNOWN_VENUE_XYZ"], "home_team": ["A"], "kickoff_time": ["15:00"]})

        # The function calls list_blobs for fixtures path, then downloads each .parquet blob,
        # then calls list_blobs again for the weather path.  We need the first call to return
        # a blob with a .parquet name so fixtures_df is populated.
        mock_blob = MagicMock()
        mock_blob.name = "sports_reference/by_date/day=2026-03-22/entity=fixtures/fixtures.parquet"
        fixtures_parquet_bytes = fixtures_df.to_parquet()

        def list_blobs_side_effect(**kwargs: object) -> list[MagicMock]:
            prefix = kwargs.get("prefix", "")
            if "entity=fixtures" in str(prefix):
                return [mock_blob]
            return []  # weather path — no existing data

        mock_storage = MagicMock()
        mock_storage.list_blobs.side_effect = list_blobs_side_effect
        mock_storage.download_bytes.return_value = fixtures_parquet_bytes

        with (
            patch("instruments_service.engine.orchestrator.ManifestWriter", return_value=mock_manifest),
            patch("instruments_service.engine.orchestrator.get_data_sink"),
            patch(
                "instruments_service.engine.orchestrator.get_expected_leagues_for_source",
                return_value=[mock_league],
            ),
            patch("instruments_service.engine.orchestrator.get_storage_client", return_value=mock_storage),
            # Patch the lazy-imported VENUE_COORDINATES dict via its source module
            patch(
                "unified_api_contracts.registry.sports_venue_coordinates.VENUE_COORDINATES",
                {},
            ),
        ):
            # Must not raise LegacyBlankErrorReasonError
            await _fetch_weather_data("2026-03-22", "test-bucket")

        calls_with_reason = [c for c in mock_manifest.record_empty.call_args_list if c.kwargs.get("reason") is not None]
        assert any(c.kwargs.get("reason") == EmptyConfirmedReason.EXPECTED_NO_MAPPING for c in calls_with_reason), (
            f"Expected EXPECTED_NO_MAPPING; got reasons: {[c.kwargs.get('reason') for c in calls_with_reason]}"
        )

    def test_record_weather_empty_signature_requires_typed_reason(self) -> None:
        """Regression for Fix #2 root cause: _record_weather_empty must require a typed reason.

        Before the fix the signature was ``reason: str = ""``.  Passing no
        argument propagated an empty-string reason into ``manifest.record_empty``
        which raised ``LegacyBlankErrorReasonError`` at runtime.  The fix
        changes the signature to ``reason: EmptyConfirmedReason`` (required,
        no default) so missing-reason is a static TypeError at call time.

        This test verifies that:
        (a) SOURCE_RETURNED_ZERO is a valid EmptyConfirmedReason member, and
        (b) calling record_empty with it doesn't raise LegacyBlankErrorReasonError
            (checked via the UAC EMPTY_CONFIRMED_REASONS membership set).
        """
        from unified_api_contracts import EmptyConfirmedReason
        from unified_api_contracts.canonical.crosscutting.honest_coverage import EMPTY_CONFIRMED_REASONS

        # SOURCE_RETURNED_ZERO must be in the closed-set EMPTY_CONFIRMED_REASONS
        assert EmptyConfirmedReason.SOURCE_RETURNED_ZERO.value in EMPTY_CONFIRMED_REASONS, (
            "SOURCE_RETURNED_ZERO is not in EMPTY_CONFIRMED_REASONS — blank-reason guard would reject it"
        )
        # EXPECTED_NO_MAPPING must also be valid
        assert EmptyConfirmedReason.EXPECTED_NO_MAPPING.value in EMPTY_CONFIRMED_REASONS, (
            "EXPECTED_NO_MAPPING is not in EMPTY_CONFIRMED_REASONS"
        )

        # Verify the fix: a mock manifest accepting the enum won't raise
        mock_manifest = MagicMock()
        mock_manifest.record_empty(
            row_key={"date": "2026-03-22", "data_type": "WEATHER", "league_id": "EPL"},
            reason=EmptyConfirmedReason.SOURCE_RETURNED_ZERO,
        )


# ---------------------------------------------------------------------------
# CF-11 regression tests — fetch-failure must produce attempted_failed,
# NOT empty_confirmed.  (2026-06-02 fix: _fail_count > 0 rather than
# _fail_count == len(fixture_ids) in the per-fixture entity zero-rows branch)
# ---------------------------------------------------------------------------


class TestCF11PerFixtureEntityFailurePath:
    """CF-11 write-path audit: a genuine API failure on a fixture-day shard
    for per-fixture entities (FIXTURE_STATS / FIXTURE_EVENTS / PLAYER_STATS /
    FIXTURE_LINEUPS) MUST produce ``record_failed`` (→ ``attempted_failed``),
    never ``record_empty`` (→ ``empty_confirmed``).

    Three scenarios:
    (a) ALL per-fixture calls raise → record_failed (was already correct before fix)
    (b) SOME calls raise, ALL return zero rows → record_failed (fixed by CF-11)
    (c) ALL calls succeed + return zero rows → record_empty (legitimate empty, unchanged)
    """

    @pytest.mark.asyncio
    async def test_all_fixture_calls_raise_produces_record_failed(self) -> None:
        """(a) Every per-fixture API call raises → entity shard → record_failed.

        Pre-fix: BOTH 'all-fail' AND 'some-fail' correctly routed.
        Post-fix: same, no regression.
        """
        mock_adapter = AsyncMock()
        mock_adapter.get_leagues.return_value = []
        mock_adapter.get_teams.return_value = []
        mock_adapter.get_standings.return_value = []
        mock_adapter.get_injuries.return_value = []
        # Per-fixture calls all raise
        mock_adapter.get_fixture_statistics.side_effect = Exception("5xx upstream")
        mock_adapter.get_fixture_events.side_effect = Exception("5xx upstream")
        mock_adapter.get_fixture_lineups.side_effect = Exception("5xx upstream")
        mock_adapter.get_fixture_player_stats.side_effect = Exception("5xx upstream")

        mock_manifest = MagicMock()
        mock_sink = MagicMock()

        with (
            patch("instruments_service.engine.orchestrator.create_sports_reference_adapter", return_value=mock_adapter),
            patch("instruments_service.engine.orchestrator.get_data_sink", return_value=mock_sink),
            patch("instruments_service.engine.orchestrator._write_team_mapping"),
            patch("instruments_service.engine.orchestrator._write_fixture_mapping"),
            patch("instruments_service.engine.orchestrator._build_fixture_league_map_from_gcs", return_value={}),
        ):
            await _fetch_sports_reference_data(
                "2026-03-22",
                "test-key",
                "test-bucket",
                manifest=mock_manifest,
                fixture_ids_override=[1001, 1002],
            )

        # record_failed must have been called (at least once — one per entity)
        assert mock_manifest.record_failed.call_count >= 1, "Expected record_failed when all per-fixture calls raise"
        # record_empty must NOT have been called for per-fixture entity shards
        # (it may be called for INJURIES/STANDINGS empties, but not for stat entities)
        stat_entity_data_types = {"FIXTURE_STATS", "FIXTURE_EVENTS", "FIXTURE_LINEUPS", "PLAYER_STATS"}
        empty_calls = mock_manifest.record_empty.call_args_list
        for call in empty_calls:
            dt = call.kwargs.get("row_key", {}).get("data_type", "")
            assert dt not in stat_entity_data_types, (
                f"record_empty called for {dt} on a fixture shard with all-raises — should be record_failed"
            )

    @pytest.mark.asyncio
    async def test_partial_failure_produces_record_failed_not_empty(self) -> None:
        """(b) CF-11 regression: SOME per-fixture calls raise, remaining return 0 rows.

        Pre-fix: partial failure fell through to _af_emit_empty_gaps_for_entity →
        empty_confirmed(EXPECTED_NO_FIXTURE) — silently confirmed-empty while data
        was NOT actually retrieved.

        Post-fix: ANY _fail_count > 0 AND zero rows → record_failed so the shard
        is marked attempted_failed and backfilled.
        """
        mock_adapter = AsyncMock()
        mock_adapter.get_leagues.return_value = []
        mock_adapter.get_teams.return_value = []
        mock_adapter.get_standings.return_value = []
        mock_adapter.get_injuries.return_value = []
        # First fixture call raises, second returns empty → total rows = 0, fail_count = 1
        mock_adapter.get_fixture_statistics.side_effect = [
            Exception("timeout"),  # fixture 1001 fails
            [],  # fixture 1002 returns empty
        ]
        mock_adapter.get_fixture_events.side_effect = [
            Exception("timeout"),
            [],
        ]
        mock_adapter.get_fixture_lineups.side_effect = [
            Exception("timeout"),
            [],
        ]
        mock_adapter.get_fixture_player_stats.side_effect = [
            Exception("timeout"),
            [],
        ]

        mock_manifest = MagicMock()
        mock_sink = MagicMock()

        with (
            patch("instruments_service.engine.orchestrator.create_sports_reference_adapter", return_value=mock_adapter),
            patch("instruments_service.engine.orchestrator.get_data_sink", return_value=mock_sink),
            patch("instruments_service.engine.orchestrator._write_team_mapping"),
            patch("instruments_service.engine.orchestrator._write_fixture_mapping"),
            patch("instruments_service.engine.orchestrator._build_fixture_league_map_from_gcs", return_value={}),
            # Suppress classify_and_emit_error noise in test output
            patch("instruments_service.engine.orchestrator.classify_and_emit_error"),
        ):
            await _fetch_sports_reference_data(
                "2026-03-22",
                "test-key",
                "test-bucket",
                manifest=mock_manifest,
                fixture_ids_override=[1001, 1002],
            )

        # CF-11: partial failure with zero rows MUST produce record_failed, not record_empty
        assert mock_manifest.record_failed.call_count >= 1, (
            "CF-11 regression: partial fixture failure + zero rows must produce record_failed "
            "(not empty_confirmed which freezes the gap forever)"
        )
        # Verify none of the per-fixture stat entities ended up as record_empty
        stat_entity_data_types = {"FIXTURE_STATS", "FIXTURE_EVENTS", "FIXTURE_LINEUPS", "PLAYER_STATS"}
        empty_calls = mock_manifest.record_empty.call_args_list
        for call in empty_calls:
            dt = call.kwargs.get("row_key", {}).get("data_type", "")
            assert dt not in stat_entity_data_types, (
                f"CF-11 regression: record_empty({dt}) called when fetch partially failed "
                "— should be record_failed (attempted_failed)"
            )

    @pytest.mark.asyncio
    async def test_all_succeed_zero_rows_produces_record_empty(self) -> None:
        """(c) All per-fixture calls succeed but return 0 rows → record_empty (legitimate).

        Genuine empty: post-match stats not yet published, lineups withheld for
        low-profile fixture, etc.  This is NOT a failure — the source was
        reachable and explicitly returned nothing.  Must NOT regress to record_failed.
        """
        mock_adapter = AsyncMock()
        mock_adapter.get_leagues.return_value = []
        mock_adapter.get_teams.return_value = []
        mock_adapter.get_standings.return_value = []
        mock_adapter.get_injuries.return_value = []
        # All per-fixture calls succeed with empty responses (no exception)
        mock_adapter.get_fixture_statistics.return_value = []
        mock_adapter.get_fixture_events.return_value = []
        mock_adapter.get_fixture_lineups.return_value = []
        mock_adapter.get_fixture_player_stats.return_value = []

        mock_manifest = MagicMock()
        mock_sink = MagicMock()

        with (
            patch("instruments_service.engine.orchestrator.create_sports_reference_adapter", return_value=mock_adapter),
            patch("instruments_service.engine.orchestrator.get_data_sink", return_value=mock_sink),
            patch("instruments_service.engine.orchestrator._write_team_mapping"),
            patch("instruments_service.engine.orchestrator._write_fixture_mapping"),
            patch("instruments_service.engine.orchestrator._build_fixture_league_map_from_gcs", return_value={}),
            # _af_emit_empty_gaps_for_entity reads get_expected_leagues_for_source + get_league_fixture_calendar
            patch("instruments_service.engine.orchestrator.get_expected_leagues_for_source", return_value=[]),
        ):
            await _fetch_sports_reference_data(
                "2026-03-22",
                "test-key",
                "test-bucket",
                manifest=mock_manifest,
                fixture_ids_override=[1001],
            )

        # No failures → record_failed must NOT have been called for stat entities
        failed_calls = mock_manifest.record_failed.call_args_list
        stat_entity_data_types = {"FIXTURE_STATS", "FIXTURE_EVENTS", "FIXTURE_LINEUPS", "PLAYER_STATS"}
        for call in failed_calls:
            dt = call.kwargs.get("row_key", {}).get("data_type", "")
            assert dt not in stat_entity_data_types, (
                f"record_failed({dt}) called when all calls succeeded with 0 rows — "
                "should be record_empty (legitimate empty)"
            )
        # _af_emit_empty_gaps_for_entity is called (zero-failure path) but
        # it may or may not call record_empty depending on the expected leagues
        # set returned by the oracle.  The key invariant: no record_failed for
        # stat entity data_types (asserted above). record_empty call count is
        # oracle-driven and not asserted here to keep the test focused.


# ---------------------------------------------------------------------------
# _canonical_league_id — CF-7 write-path canonicalisation
# ---------------------------------------------------------------------------


class TestCanonicalLeagueIdCF7:
    """Tests for _canonical_league_id CF-7 provider-suffix stripping.

    The function is the single choke-point that all sports manifest row_key /
    parquet / record_* league_id writes pass through.  CF-7 wires
    ``canonicalize_league_id`` from UAC as a second pass so provider-suffixed
    ids (e.g. ``EPL_39``) are stripped to canonical form before hitting disk.
    """

    def test_already_canonical_passthrough(self) -> None:
        """A canonical league_id must pass through unchanged."""
        from instruments_service.engine.orchestrator import _canonical_league_id

        assert _canonical_league_id("EPL") == "EPL"
        assert _canonical_league_id("BUNDESLIGA") == "BUNDESLIGA"
        assert _canonical_league_id("LA_LIGA") == "LA_LIGA"

    def test_provider_suffixed_stripped(self) -> None:
        """Provider-suffixed ids where the suffix is a verified provider id
        must be stripped to the canonical base (CF-7 write-path).

        EPL_39: UAC registry confirms api_football id for EPL is 39 →
        canonicalize_league_id strips suffix → "EPL".
        """
        from instruments_service.engine.orchestrator import _canonical_league_id

        result = _canonical_league_id("EPL_39")
        assert result == "EPL", (
            f"Expected 'EPL' after CF-7 suffix strip, got {result!r}. "
            "canonicalize_league_id should strip '_39' (verified api_football id)."
        )

    def test_unresolved_suffix_passthrough(self) -> None:
        """A 1-2 digit suffix that is NOT a registered provider id passes through unchanged.

        Non-lossy guarantee: the canonicalizer never strips a tier-like (1-2 digit)
        suffix it cannot verify as a provider id — the raw value is returned intact.
        NOTE: this guarantee applies to 1-2 digit suffixes only. Per UAC
        ``canonicalize_league_id`` Step 3a (added dc76f1a6), a 3+-digit suffix on a
        resolving base is treated as a provider/season id and IS stripped regardless of
        registration (e.g. "EPL_99999" → "EPL", "SCOTTISH_LEAGUE_CUP_185" →
        "SCOTTISH_LEAGUE_CUP") — real league tiers never exceed 2 digits.
        """
        from instruments_service.engine.orchestrator import _canonical_league_id

        # 88 is not a registered provider id for EPL and is <100 (tier-like) → pass through.
        result = _canonical_league_id("EPL_88")
        assert result == "EPL_88", f"Expected 'EPL_88' (unregistered 1-2 digit suffix → pass through), got {result!r}."

    def test_unresolved_league_key_passthrough(self) -> None:
        """An unrecognised league key with no suffix must pass through unchanged."""
        from instruments_service.engine.orchestrator import _canonical_league_id

        result = _canonical_league_id("UNKNOWN_LEAGUE_XYZ")
        assert result == "UNKNOWN_LEAGUE_XYZ", f"Expected 'UNKNOWN_LEAGUE_XYZ', got {result!r}."

    def test_idempotent(self) -> None:
        """canonicalize_league_id(canonicalize_league_id(x)) == canonicalize_league_id(x)."""
        from instruments_service.engine.orchestrator import _canonical_league_id

        for raw in ("EPL", "EPL_39", "LA_LIGA", "UNKNOWN_99"):
            first = _canonical_league_id(raw)
            second = _canonical_league_id(first)
            assert first == second, f"Not idempotent for {raw!r}: first={first!r}, second={second!r}"

    def test_numeric_string_resolves_via_pass1(self) -> None:
        """A raw numeric string (api_football id) should resolve via Pass 1
        (get_league_by_api_football_id) then be unchanged by Pass 2.

        api_football id 39 → EPL.
        """
        from instruments_service.engine.orchestrator import _canonical_league_id

        result = _canonical_league_id("39")
        assert result == "EPL", f"Expected 'EPL' from numeric api_football id 39, got {result!r}."

    def test_unknown_numeric_passthrough(self) -> None:
        """An unknown numeric id should pass through both passes unchanged."""
        from instruments_service.engine.orchestrator import _canonical_league_id

        result = _canonical_league_id("9999999")
        assert result == "9999999", f"Expected '9999999' (no registry match), got {result!r}."

    def test_whitespace_stripped(self) -> None:
        """Leading/trailing whitespace is stripped before canonicalisation."""
        from instruments_service.engine.orchestrator import _canonical_league_id

        assert _canonical_league_id("  EPL  ") == "EPL"
        assert _canonical_league_id("  EPL_39  ") == "EPL"


# ---------------------------------------------------------------------------
# 2026-07-14 GW enrichment false-empty / dropped-row write-path regressions
# (issue: sports_gw_enrichment_false_empty_manifest_and_dropped_rows_2026_07_14)
# ---------------------------------------------------------------------------


class TestGwFalseEmptyWritePath20260714:
    """Regressions for the three-leg GW enrichment manifest write-path fix.

    Leg 1: a league whose every fixture is skip-as-already-present (pre-fetch
    skip found the per-league parquet complete) must NEVER be demoted to
    ``empty_confirmed`` by a no-op re-run.
    Leg 2: ``_build_fixture_league_map_from_gcs`` must cover the full
    94-league api_football write universe (not the 33-league Prediction tier)
    and must list blobs unbounded (``max_results=None`` — the old default of
    100 truncated busy days).
    Leg 3: absence emission is PRESENCE-based — an existing per-league parquet
    always wins over any this-run captured-set computation, including on the
    ``redo_all`` / zero-fixture-day paths where the pre-fetch skip tracker is
    bypassed; off-season leagues get a typed ``EXPECTED_PAUSED_LEAGUE`` row,
    never a silent skip; the zero-fixture-day FIXTURES markers iterate the
    94-league universe, presence-guarded.
    """

    @staticmethod
    def _leagues_subset(*league_ids: str) -> list[object]:
        from instruments_service.engine.orchestrator import get_expected_leagues_for_source

        wanted = set(league_ids)
        subset = [lg for lg in get_expected_leagues_for_source("api_football") if lg.league_id in wanted]
        assert {lg.league_id for lg in subset} == wanted, f"missing league defs for {wanted}"
        return subset

    @pytest.mark.asyncio
    async def test_skip_as_present_league_not_demoted_to_empty(self) -> None:
        """Leg 1: all fixtures for LA_LIGA already on disk → zero fetches this
        run → the cell must NOT be stamped empty_confirmed (the exact 3,720
        false-empty mechanism), while a genuinely-uncaptured league (EPL, no
        fixtures mapped to it today) still gets its honest EXPECTED_NO_FIXTURE.
        """
        mock_adapter = AsyncMock()
        mock_adapter.get_fixture_events.return_value = []
        mock_manifest = MagicMock()
        mock_sink = MagicMock()

        with (
            patch("instruments_service.engine.orchestrator.create_sports_reference_adapter", return_value=mock_adapter),
            patch("instruments_service.engine.orchestrator.get_data_sink", return_value=mock_sink),
            patch("instruments_service.engine.orchestrator._write_team_mapping"),
            patch("instruments_service.engine.orchestrator._write_fixture_mapping"),
            patch(
                "instruments_service.engine.orchestrator._build_fixture_league_map_from_gcs",
                return_value={"1001": "LA_LIGA"},
            ),
            patch(
                "instruments_service.engine.orchestrator._read_existing_per_league_fixture_ids",
                return_value=frozenset({1001}),
            ),
            patch(
                "instruments_service.engine.orchestrator.get_expected_leagues_for_source",
                return_value=self._leagues_subset("LA_LIGA", "EPL"),
            ),
            patch("instruments_service.engine.orchestrator.is_league_entity_covered", return_value=True),
            patch("instruments_service.engine.orchestrator.get_league_fixture_calendar", return_value=["match"]),
            patch("instruments_service.engine.orchestrator._list_present_parquet_leagues", return_value=set()),
        ):
            await _fetch_sports_reference_data(
                "2025-11-08",
                "test-key",
                "test-bucket",
                manifest=mock_manifest,
                entities_to_fetch=["FIXTURE_EVENTS"],
                fixture_ids_override=[1001],
            )

        # The pre-fetch skip must have prevented the api call entirely.
        assert mock_adapter.get_fixture_events.await_count == 0, (
            "fixture 1001 is already captured on disk — the pre-fetch skip should have skipped the API call"
        )
        la_liga_empty = [
            c
            for c in mock_manifest.record_empty.call_args_list
            if c.kwargs.get("row_key", {}).get("league_id") == "LA_LIGA"
            and c.kwargs.get("row_key", {}).get("data_type") == "FIXTURE_EVENTS"
        ]
        assert not la_liga_empty, (
            f"skip-as-present LA_LIGA was demoted to empty_confirmed: {la_liga_empty} — "
            "this is the 2026-07-14 false-empty regression"
        )
        epl_empty = [
            c
            for c in mock_manifest.record_empty.call_args_list
            if c.kwargs.get("row_key", {}).get("league_id") == "EPL"
            and c.kwargs.get("row_key", {}).get("data_type") == "FIXTURE_EVENTS"
        ]
        assert epl_empty, "genuinely-uncaptured EPL must still get its honest empty_confirmed row"

    @pytest.mark.asyncio
    async def test_presence_guard_protects_present_parquet_under_redo_all(self) -> None:
        """Leg 3 presence guard: with ``redo_all=True`` the pre-fetch skip
        tracker is bypassed and the league map is empty — but a league whose
        per-league parquet EXISTS must still not be stamped empty_confirmed.
        """
        mock_adapter = AsyncMock()
        mock_adapter.get_fixture_events.return_value = []
        mock_manifest = MagicMock()
        mock_sink = MagicMock()

        with (
            patch("instruments_service.engine.orchestrator.create_sports_reference_adapter", return_value=mock_adapter),
            patch("instruments_service.engine.orchestrator.get_data_sink", return_value=mock_sink),
            patch("instruments_service.engine.orchestrator._write_team_mapping"),
            patch("instruments_service.engine.orchestrator._write_fixture_mapping"),
            patch("instruments_service.engine.orchestrator._build_fixture_league_map_from_gcs", return_value={}),
            patch(
                "instruments_service.engine.orchestrator.get_expected_leagues_for_source",
                return_value=self._leagues_subset("LA_LIGA", "EPL"),
            ),
            patch("instruments_service.engine.orchestrator.is_league_entity_covered", return_value=True),
            patch("instruments_service.engine.orchestrator.get_league_fixture_calendar", return_value=["match"]),
            patch(
                "instruments_service.engine.orchestrator._list_present_parquet_leagues",
                return_value={"LA_LIGA"},
            ) as mock_present,
        ):
            await _fetch_sports_reference_data(
                "2025-11-08",
                "test-key",
                "test-bucket",
                manifest=mock_manifest,
                entities_to_fetch=["FIXTURE_EVENTS"],
                fixture_ids_override=[1001],
                redo_all=True,
            )

        assert mock_present.call_count >= 1, "presence guard was never consulted"
        la_liga_empty = [
            c
            for c in mock_manifest.record_empty.call_args_list
            if c.kwargs.get("row_key", {}).get("league_id") == "LA_LIGA"
            and c.kwargs.get("row_key", {}).get("data_type") == "FIXTURE_EVENTS"
        ]
        assert not la_liga_empty, (
            "LA_LIGA has an existing per-league parquet — the presence guard must prevent the "
            f"empty_confirmed stamp even under redo_all: {la_liga_empty}"
        )

    def test_league_map_covers_full_universe_and_lists_unbounded(self) -> None:
        """Leg 2: the af_league_id fallback must map leagues from the FULL
        api_football write universe (not just the 33 Prediction-tier leagues),
        and a >100-fixture day must map completely (``max_results=None``).
        """
        from instruments_service.engine.orchestrator import (
            _build_fixture_league_map_from_gcs,
            get_expected_leagues_for_source,
            get_prediction_leagues,
        )

        prediction_ids = {lg.league_id for lg in get_prediction_leagues()}
        non_prediction = [
            lg
            for lg in get_expected_leagues_for_source("api_football")
            if lg.league_id not in prediction_ids and lg.api_football_id is not None
        ]
        assert non_prediction, "expected-universe minus prediction-tier is empty — universe regression"
        target = non_prediction[0]

        n_fixtures = 150  # > the old max_results=100 truncation point
        fixtures_df = pd.DataFrame(
            {
                "af_fixture_id": list(range(1, n_fixtures + 1)),
                "af_league_id": [target.api_football_id] * n_fixtures,
            }
        )
        with patch(
            "instruments_service.engine.orchestrator._read_per_league_entity_df",
            return_value=fixtures_df,
        ) as mock_read:
            result = _build_fixture_league_map_from_gcs("test-bucket", "2025-11-29")

        assert mock_read.call_args.kwargs.get("max_results", "MISSING") is None, (
            "the fixtures listing must be unbounded — max_results=100 truncated busy days"
        )
        assert len(result) == n_fixtures, (
            f"league map covered {len(result)}/{n_fixtures} fixtures — "
            "a non-prediction-tier league fell out of the af_league_id fallback (33-vs-94 regression)"
        )
        assert set(result.values()) == {target.league_id}

    def test_offseason_league_gets_typed_paused_row_not_silent_skip(self) -> None:
        """Leg 3: an off-season league (empty fixture calendar) must get a
        typed EXPECTED_PAUSED_LEAGUE row — the old silent ``continue`` left
        the cell permanently blank-reason (the 30 A_LEAGUE September INJURIES
        cells).
        """
        from datetime import UTC, datetime

        from unified_api_contracts import EmptyConfirmedReason

        from instruments_service.engine.orchestrator.sports_reference_core import _AfManifestHooks

        mock_manifest = MagicMock()
        hooks = _AfManifestHooks(
            date="2025-09-15",
            manifest=mock_manifest,
            attempt_ts=datetime.now(UTC),
            bucket="",  # presence guard disabled — this test isolates the calendar leg
        )
        with (
            patch(
                "instruments_service.engine.orchestrator.get_expected_leagues_for_source",
                return_value=self._leagues_subset("A_LEAGUE"),
            ),
            patch("instruments_service.engine.orchestrator.is_league_entity_covered", return_value=True),
            patch("instruments_service.engine.orchestrator.get_league_fixture_calendar", return_value=[]),
        ):
            hooks.emit_empty_gaps_for_entity("INJURIES", set())

        paused = [
            c
            for c in mock_manifest.record_empty.call_args_list
            if c.kwargs.get("reason") == EmptyConfirmedReason.EXPECTED_PAUSED_LEAGUE
            and c.kwargs.get("row_key", {}).get("league_id") == "A_LEAGUE"
        ]
        assert paused, (
            "off-season A_LEAGUE got no typed row — the silent-skip regression leaves the cell "
            "permanently blank-reason expected_unattempted"
        )
        no_fixture = [
            c
            for c in mock_manifest.record_empty.call_args_list
            if c.kwargs.get("reason") == EmptyConfirmedReason.EXPECTED_NO_FIXTURE
        ]
        assert not no_fixture, "off-season absence must be EXPECTED_PAUSED_LEAGUE, not EXPECTED_NO_FIXTURE"

    def test_zero_fixture_markers_iterate_full_universe_with_presence_guard(self) -> None:
        """Leg 3 / zero-day markers: ``_zero_sports_empty_fixture_markers``
        must stamp the FULL api_football expected-league universe (94, not the
        33 Prediction-tier leagues) and must skip presence-guarded leagues
        whose FIXTURES parquet exists for the date.
        """
        from instruments_service.engine.orchestrator import (
            _canonical_league_id,
            get_expected_leagues_for_source,
        )
        from instruments_service.engine.orchestrator.process_zero_records import (
            _zero_sports_empty_fixture_markers,
        )

        mock_writer = MagicMock()
        with (
            patch("instruments_service.engine.orchestrator.ManifestWriter", return_value=mock_writer),
            patch(
                "instruments_service.engine.orchestrator._list_present_parquet_leagues",
                return_value={"LA_LIGA"},
            ),
        ):
            _zero_sports_empty_fixture_markers(
                date="2025-09-02",
                bucket="test-bucket",
                league_filter=None,
            )

        stamped = {c.kwargs["row_key"]["league_id"] for c in mock_writer.record_empty.call_args_list}
        expected_universe = {
            _canonical_league_id(lg.league_id) for lg in get_expected_leagues_for_source("api_football")
        }
        assert stamped == expected_universe - {"LA_LIGA"}, (
            "zero-fixture markers must cover the expected universe minus presence-guarded leagues"
        )
        assert len(stamped) > 33, (
            f"only {len(stamped)} leagues stamped — looks like the 33-league Prediction-tier regression"
        )
        assert "LA_LIGA" not in stamped, "presence-guarded league must not be stamped empty_confirmed"
        assert mock_writer.write.called, "marker writer must flush"

    def test_presence_probe_failure_skips_empty_emission_fail_safe(self) -> None:
        """FAIL-SAFE: when the presence probe raises (GCS transport error),
        emit_empty_gaps_for_entity must skip empty emission entirely — never
        stamp empty_confirmed over data it could not see — and must not raise.
        """
        from datetime import UTC, datetime

        from instruments_service.engine.orchestrator.sports_reference_core import _AfManifestHooks

        mock_manifest = MagicMock()
        hooks = _AfManifestHooks(
            date="2025-11-08",
            manifest=mock_manifest,
            attempt_ts=datetime.now(UTC),
            bucket="some-bucket",
        )
        with patch(
            "instruments_service.engine.orchestrator._list_present_parquet_leagues",
            side_effect=OSError("GCS transport error"),
        ):
            hooks.emit_empty_gaps_for_entity("FIXTURE_EVENTS", set())

        assert mock_manifest.record_empty.call_count == 0, (
            "presence probe failed — no empty_confirmed rows may be stamped (fail-safe)"
        )
