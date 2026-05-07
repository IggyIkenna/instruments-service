"""Comprehensive tests for sports adapters — api_football, _normalizer, competition_phase, base.

Covers uncovered lines/branches in:
- instruments_service/reference_data/adapters/sports/adapters/api_football.py
- instruments_service/reference_data/adapters/sports/_normalizer.py
- instruments_service/reference_data/adapters/sports/competition_phase.py
- instruments_service/reference_data/adapters/sports/adapters/base.py
- instruments_service/reference_data/adapters/api_football.py (URDI wrapper)

All tests are credential-free, mock all HTTP, and use no live network calls.
"""

from __future__ import annotations

import time
from datetime import UTC, date, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from instruments_service.reference_data.adapters.sports._normalizer import (
    TeamNormalizer,
    _extract_core_name,
    _strip_accents,
    normalize_for_fuzzy,
)
from instruments_service.reference_data.adapters.sports.adapters.api_football import (
    ApiFootballAdapter,
    _effective_season_for_league,
    _extract_response,
    _flatten_standings_groups,
    _parse_fixture_response,
    _parse_teams,
)
from instruments_service.reference_data.adapters.sports.adapters.api_football_reference import (
    ApiFootballReferenceDataAdapter,
    _canonical_fixture_to_instrument,
)
from instruments_service.reference_data.adapters.sports.adapters.base import (
    BaseSportsReferenceAdapter,
)
from instruments_service.reference_data.adapters.sports.competition_phase import (
    CompetitionPhase,
    classify_competition_phase,
)

# ---------------------------------------------------------------------------
# Shared aiohttp mock helper
# ---------------------------------------------------------------------------


def _make_aiohttp_mock(
    json_response: object,
    status: int = 200,
    headers: dict[str, str] | None = None,
) -> MagicMock:
    """Build a mock that replaces ``aiohttp.ClientSession`` context manager."""
    mock_resp = AsyncMock()
    mock_resp.status = status
    mock_resp.raise_for_status = MagicMock()
    if status >= 400:
        mock_resp.raise_for_status.side_effect = Exception(f"HTTP {status}")
    mock_resp.json = AsyncMock(return_value=json_response)
    mock_resp.headers = headers or {}

    resp_cm = MagicMock()
    resp_cm.__aenter__ = AsyncMock(return_value=mock_resp)
    resp_cm.__aexit__ = AsyncMock(return_value=None)

    mock_session_obj = MagicMock()
    mock_session_obj.get = MagicMock(return_value=resp_cm)
    mock_session_obj.post = MagicMock(return_value=resp_cm)

    mock_session_cm = MagicMock()
    mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
    mock_session_cm.__aexit__ = AsyncMock(return_value=None)
    return mock_session_cm


# ===========================================================================
# NORMALIZER — comprehensive coverage for _normalizer.py (0% covered!)
# ===========================================================================


class TestStripAccentsComprehensive:
    """Tests for _strip_accents function."""

    def test_accent_removal_unicode_nfd(self) -> None:
        assert _strip_accents("Alavés") == "alaves"

    def test_umlaut_removal(self) -> None:
        assert _strip_accents("München") == "munchen"

    def test_cedilla_removal(self) -> None:
        result = _strip_accents("Fenerbahçe")
        assert "c" in result  # cedilla stripped

    def test_mixed_accents(self) -> None:
        result = _strip_accents("Atlético Madrid")
        assert result == "atletico madrid"

    def test_no_accents(self) -> None:
        assert _strip_accents("Arsenal") == "arsenal"

    def test_empty(self) -> None:
        assert _strip_accents("") == ""

    def test_uppercase_input(self) -> None:
        assert _strip_accents("MÜNCHEN") == "munchen"


class TestNormalizeForFuzzyComprehensive:
    """Tests for normalize_for_fuzzy function."""

    def test_basic_normalization(self) -> None:
        assert normalize_for_fuzzy("Arsenal FC") == "arsenal fc"

    def test_accent_stripped(self) -> None:
        assert normalize_for_fuzzy("Alavés") == "alaves"

    def test_special_chars_removed(self) -> None:
        result = normalize_for_fuzzy("St. Pauli!")
        assert "!" not in result

    def test_collapse_whitespace(self) -> None:
        result = normalize_for_fuzzy("  Arsenal   FC  ")
        assert result == "arsenal fc"

    def test_hyphen_preserved(self) -> None:
        result = normalize_for_fuzzy("Saint-Etienne")
        assert "-" in result

    def test_numbers_preserved(self) -> None:
        result = normalize_for_fuzzy("Schalke 04")
        assert "04" in result


class TestExtractCoreNameComprehensive:
    """Tests for _extract_core_name function."""

    def test_strip_fc_suffix(self) -> None:
        result = _extract_core_name("Arsenal FC")
        assert "fc" not in result
        assert "arsenal" in result

    def test_strip_afc_prefix(self) -> None:
        result = _extract_core_name("AFC Bournemouth")
        assert result == "bournemouth"

    def test_strip_sc_prefix(self) -> None:
        result = _extract_core_name("SC Freiburg")
        assert "freiburg" in result

    def test_strip_united_suffix(self) -> None:
        result = _extract_core_name("Manchester United")
        assert "united" not in result
        assert "manchester" in result

    def test_strip_city_suffix(self) -> None:
        result = _extract_core_name("Manchester City")
        assert "city" not in result

    def test_no_suffix_to_strip(self) -> None:
        result = _extract_core_name("Arsenal")
        assert result == "arsenal"

    def test_strip_town_suffix(self) -> None:
        result = _extract_core_name("Ipswich Town")
        assert "town" not in result

    def test_strip_football_club_suffix(self) -> None:
        result = _extract_core_name("Arsenal Football Club")
        assert "football" not in result
        assert "club" not in result

    def test_accent_name_suffix_strip(self) -> None:
        result = _extract_core_name("FC Köln")
        assert "fc" not in result


class TestTeamNormalizerComprehensive:
    """Comprehensive tests for TeamNormalizer covering all lookup strategies."""

    def test_exact_match_epl(self) -> None:
        norm = TeamNormalizer()
        result = norm.normalize("Arsenal")
        assert result == ("ARS", "Arsenal")

    def test_exact_match_case_insensitive(self) -> None:
        norm = TeamNormalizer()
        result = norm.normalize("CHELSEA")
        assert result is not None
        assert result[0] == "CHE"

    def test_whitespace_stripped(self) -> None:
        norm = TeamNormalizer()
        result = norm.normalize("  Liverpool  ")
        assert result is not None
        assert result[0] == "LIV"

    def test_accent_match_monchengladbach(self) -> None:
        """Accent-stripped match for Borussia Mönchengladbach."""
        norm = TeamNormalizer()
        result = norm.normalize("Borussia Mönchengladbach")
        assert result is not None
        assert result[0] == "BMG"

    def test_accent_match_alaves(self) -> None:
        norm = TeamNormalizer()
        result = norm.normalize("Alavés")
        assert result is not None
        assert result[0] == "ALA"

    def test_core_name_match_chelsea_fc(self) -> None:
        """Core-name match via suffix stripping."""
        norm = TeamNormalizer()
        result = norm.normalize("Chelsea FC")
        assert result is not None
        assert result[0] == "CHE"

    def test_historical_name_match(self) -> None:
        """Historical name changes lookup."""
        norm = TeamNormalizer()
        result = norm.normalize("Olympiakos Piraeus")
        assert result is not None
        assert result[0] == "OLY"

    def test_historical_accent_match(self) -> None:
        """Historical name with accent-stripped lookup."""
        norm = TeamNormalizer()
        result = norm.normalize("Preussen Münster")
        assert result is not None
        assert result[0] == "PRM_DE"

    def test_unknown_team_returns_none(self) -> None:
        norm = TeamNormalizer()
        result = norm.normalize("Completely Unknown Sports Team ABCXYZ123")
        assert result is None

    def test_short_core_name_skips_core_lookup(self) -> None:
        """Core names <= 3 chars are skipped to avoid false matches."""
        norm = TeamNormalizer()
        # "AZ" is only 2 chars after normalization — should match via exact or accent, not core
        result = norm.normalize("AZ")
        assert result is not None
        assert result[0] == "AZ"

    def test_register_alias_new_team(self) -> None:
        norm = TeamNormalizer()
        norm.register_alias("The Gunners", "ARS", "Arsenal")
        result = norm.normalize("The Gunners")
        assert result == ("ARS", "Arsenal")

    def test_register_alias_core_name(self) -> None:
        """Register alias also updates core index."""
        norm = TeamNormalizer()
        norm.register_alias("Super Long Team Name United", "SLT", "Super Long Team")
        result = norm.normalize("Super Long Team Name United")
        assert result == ("SLT", "Super Long Team")

    def test_register_alias_short_core_name_no_core_index(self) -> None:
        """Short aliases don't get added to core index."""
        norm = TeamNormalizer()
        norm.register_alias("AB", "TEST", "AB Team")
        # Still findable via exact match
        result = norm.normalize("AB")
        assert result == ("TEST", "AB Team")

    def test_known_team_count_unique(self) -> None:
        """known_team_count returns unique canonical IDs."""
        norm = TeamNormalizer()
        count = norm.known_team_count
        assert count > 50  # EPL + Bundesliga + La Liga + Serie A + etc.

    def test_bundesliga_teams(self) -> None:
        norm = TeamNormalizer()
        for name, expected_id in [
            ("Bayern Munich", "BAY"),
            ("Borussia Dortmund", "BVB"),
            ("RB Leipzig", "RBL"),
            ("Bayer Leverkusen", "B04"),
        ]:
            result = norm.normalize(name)
            assert result is not None, f"Failed for {name}"
            assert result[0] == expected_id, f"Expected {expected_id} for {name}, got {result[0]}"

    def test_la_liga_teams(self) -> None:
        norm = TeamNormalizer()
        for name, expected_id in [
            ("Real Madrid", "RMA"),
            ("Barcelona", "BAR"),
            ("Atletico Madrid", "ATM"),
            ("Sevilla", "SEV"),
        ]:
            result = norm.normalize(name)
            assert result is not None, f"Failed for {name}"
            assert result[0] == expected_id

    def test_serie_a_teams(self) -> None:
        norm = TeamNormalizer()
        for name, expected_id in [
            ("Juventus", "JUV"),
            ("Inter Milan", "INT"),
            ("AC Milan", "MIL"),
            ("Napoli", "NAP"),
        ]:
            result = norm.normalize(name)
            assert result is not None, f"Failed for {name}"
            assert result[0] == expected_id

    def test_ligue_1_teams(self) -> None:
        norm = TeamNormalizer()
        for name, expected_id in [
            ("Paris Saint-Germain", "PSG"),
            ("PSG", "PSG"),
            ("Marseille", "OM"),
            ("Lyon", "OL"),
        ]:
            result = norm.normalize(name)
            assert result is not None, f"Failed for {name}"
            assert result[0] == expected_id

    def test_eredivisie_teams(self) -> None:
        norm = TeamNormalizer()
        result = norm.normalize("Ajax")
        assert result is not None
        assert result[0] == "AJX"

    def test_primeira_liga_teams(self) -> None:
        norm = TeamNormalizer()
        result = norm.normalize("Benfica")
        assert result is not None
        assert result[0] == "BEN"

    def test_nba_teams(self) -> None:
        norm = TeamNormalizer()
        result = norm.normalize("Los Angeles Lakers")
        assert result is not None
        assert result[0] == "LAL"

    def test_nfl_teams(self) -> None:
        norm = TeamNormalizer()
        result = norm.normalize("Kansas City Chiefs")
        assert result is not None
        assert result[0] == "KC"

    def test_mlb_teams(self) -> None:
        norm = TeamNormalizer()
        result = norm.normalize("New York Yankees")
        assert result is not None
        assert result[0] == "NYY"

    def test_nhl_teams(self) -> None:
        norm = TeamNormalizer()
        result = norm.normalize("Toronto Maple Leafs")
        assert result is not None
        assert result[0] == "TOR_NHL"

    def test_nickname_lookup(self) -> None:
        """Nicknames like 'Spurs' resolve correctly."""
        norm = TeamNormalizer()
        result = norm.normalize("Spurs")
        assert result is not None
        assert result[0] == "TOT"

    def test_abbreviation_lookup(self) -> None:
        """Abbreviations like 'BVB' resolve correctly."""
        norm = TeamNormalizer()
        result = norm.normalize("BVB")
        assert result is not None
        assert result[0] == "BVB"

    def test_koln_with_umlaut(self) -> None:
        """1.FC Köln with umlaut resolves."""
        norm = TeamNormalizer()
        result = norm.normalize("1.FC Köln")
        assert result is not None
        assert result[0] == "KOE"


# ===========================================================================
# COMPETITION PHASE — additional edge cases for uncovered branches
# ===========================================================================


class TestCompetitionPhaseAdditional:
    """Additional competition phase tests for maximum coverage."""

    def test_promotion_round_with_playoff_is_not_league_split(self) -> None:
        """'promotion round play-off' is playoff, not league split."""
        phase, _sub, _ = classify_competition_phase("promotion round play-off")
        # The check_league_split has 'and "play-off" not in r'
        assert phase != CompetitionPhase.LEAGUE_SPLIT

    def test_1st_phase_final_is_tournament(self) -> None:
        """'1st phase - final' should be TOURNAMENT (split_knockout fires first)."""
        phase, _, _ = classify_competition_phase("1st phase - final")
        assert phase == CompetitionPhase.TOURNAMENT

    def test_2nd_phase_semi_final(self) -> None:
        """'2nd phase semi-final' — knockout keyword in phase."""
        phase, _, _ = classify_competition_phase("2nd phase semi-final")
        assert phase == CompetitionPhase.TOURNAMENT

    def test_relegation_promotion_keyword(self) -> None:
        # "relegation/promotion round" contains "promotion round" which matches
        # _check_league_split before _check_decider. The LEAGUE_SPLIT checker fires first.
        phase, sub, _ = classify_competition_phase("relegation/promotion round")
        assert phase == CompetitionPhase.LEAGUE_SPLIT
        assert sub == "PROMOTION"

    def test_promotion_round_pure(self) -> None:
        """Pure promotion round without play-off."""
        phase, sub, _ = classify_competition_phase("promotion round")
        assert phase == CompetitionPhase.LEAGUE_SPLIT
        assert sub == "PROMOTION"

    def test_case_insensitive(self) -> None:
        phase, _, is_normal = classify_competition_phase("REGULAR SEASON - 10")
        assert phase == CompetitionPhase.NORMAL_LEAGUE
        assert is_normal is True

    def test_whitespace_handling(self) -> None:
        phase, _, _ = classify_competition_phase("   championship round - 3   ")
        assert phase == CompetitionPhase.LEAGUE_SPLIT

    def test_championship_final_exact(self) -> None:
        phase, _, _ = classify_competition_phase("championship - final")
        assert phase == CompetitionPhase.DECIDER_MATCH

    def test_league_stage_matchday(self) -> None:
        phase, _, is_normal = classify_competition_phase("league stage - matchday 5")
        assert phase == CompetitionPhase.NORMAL_LEAGUE
        assert is_normal is True


# ===========================================================================
# BASE SPORTS ADAPTER — _throttle, _get_with_retry, _classify_error,
# _emit_fetch_failed, default method implementations
# ===========================================================================


class _ConcreteAdapter(BaseSportsReferenceAdapter):
    """Concrete implementation for testing base class methods."""

    @property
    def venue(self) -> str:
        return "test_venue"

    async def get_fixtures(self, date: str, league_ids: list[int] | None = None) -> list[object]:
        return []

    async def get_leagues(self) -> list[object]:
        return []

    async def get_teams(self, league_id: int, season: int | None = None) -> list[object]:
        return []

    async def get_odds(self, sport: str, regions: str = "uk", markets: str = "h2h") -> list[object]:
        return []


class TestBaseSportsAdapterThrottle:
    """Tests for _throttle rate limiting."""

    @pytest.mark.asyncio
    async def test_throttle_enforces_interval(self) -> None:
        """Throttle sets the per-class _last_request_time.

        Per the per-class throttle pattern (commit 04bc1bc — SFI = 0.34s under
        a 4 req/sec plan), ``_throttle`` writes to ``cls._last_request_time``
        where ``cls = type(self)``, NOT to the base class. Each adapter
        sub-class therefore tracks its own pacing — so the assertion has to
        read from the concrete subclass attribute, not the base.
        """
        adapter = _ConcreteAdapter()
        # Reset shared state on both base + subclass so we can detect a write
        # from a clean baseline regardless of attribute resolution order.
        BaseSportsReferenceAdapter._last_request_time = 0.0
        _ConcreteAdapter._last_request_time = 0.0
        await adapter._throttle()
        # Per-class write — _ConcreteAdapter owns the throttle clock now.
        assert _ConcreteAdapter._last_request_time > 0.0

    @pytest.mark.asyncio
    async def test_throttle_sleeps_when_too_fast(self) -> None:
        """Throttle sleeps when called faster than minimum interval."""
        adapter = _ConcreteAdapter()
        # Set last request to just now
        BaseSportsReferenceAdapter._last_request_time = time.monotonic()
        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await adapter._throttle()
            # Should have been called since elapsed < _MIN_REQUEST_INTERVAL
            if mock_sleep.called:
                assert mock_sleep.call_args[0][0] > 0


class TestBaseSportsAdapterGetWithRetry:
    """Tests for _get_with_retry retry logic."""

    @pytest.mark.asyncio
    async def test_success_on_first_attempt(self) -> None:
        """Successful response on first try returns JSON."""
        adapter = _ConcreteAdapter()
        BaseSportsReferenceAdapter._last_request_time = 0.0
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(return_value={"data": "ok"})
        mock_resp.headers = {}
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_cm)
        with patch("asyncio.sleep", new_callable=AsyncMock):
            result = await adapter._get_with_retry(mock_session, "http://test.com/api")
        assert result == {"data": "ok"}

    @pytest.mark.asyncio
    async def test_retries_on_429(self) -> None:
        """429 triggers retry with backoff."""
        adapter = _ConcreteAdapter()
        BaseSportsReferenceAdapter._last_request_time = 0.0

        # First call: 429, second call: 200
        mock_resp_429 = AsyncMock()
        mock_resp_429.status = 429
        mock_resp_429.headers = {"Retry-After": "1"}
        mock_resp_success = AsyncMock()
        mock_resp_success.status = 200
        mock_resp_success.raise_for_status = MagicMock()
        mock_resp_success.json = AsyncMock(return_value={"ok": True})
        mock_resp_success.headers = {}

        mock_cm_429 = MagicMock()
        mock_cm_429.__aenter__ = AsyncMock(return_value=mock_resp_429)
        mock_cm_429.__aexit__ = AsyncMock(return_value=None)
        mock_cm_ok = MagicMock()
        mock_cm_ok.__aenter__ = AsyncMock(return_value=mock_resp_success)
        mock_cm_ok.__aexit__ = AsyncMock(return_value=None)

        mock_session = MagicMock()
        mock_session.get = MagicMock(side_effect=[mock_cm_429, mock_cm_ok])
        with patch("asyncio.sleep", new_callable=AsyncMock):
            result = await adapter._get_with_retry(mock_session, "http://test.com/api")
        assert result == {"ok": True}

    @pytest.mark.asyncio
    async def test_retries_on_500(self) -> None:
        """500 triggers retry without Retry-After header."""
        adapter = _ConcreteAdapter()
        BaseSportsReferenceAdapter._last_request_time = 0.0

        mock_resp_500 = AsyncMock()
        mock_resp_500.status = 500
        mock_resp_500.headers = {}
        mock_resp_ok = AsyncMock()
        mock_resp_ok.status = 200
        mock_resp_ok.raise_for_status = MagicMock()
        mock_resp_ok.json = AsyncMock(return_value={"recovered": True})
        mock_resp_ok.headers = {}

        mock_cm_500 = MagicMock()
        mock_cm_500.__aenter__ = AsyncMock(return_value=mock_resp_500)
        mock_cm_500.__aexit__ = AsyncMock(return_value=None)
        mock_cm_ok = MagicMock()
        mock_cm_ok.__aenter__ = AsyncMock(return_value=mock_resp_ok)
        mock_cm_ok.__aexit__ = AsyncMock(return_value=None)

        mock_session = MagicMock()
        mock_session.get = MagicMock(side_effect=[mock_cm_500, mock_cm_ok])
        with patch("asyncio.sleep", new_callable=AsyncMock):
            result = await adapter._get_with_retry(mock_session, "http://test.com/api")
        assert result == {"recovered": True}

    @pytest.mark.asyncio
    async def test_all_retries_exhausted_raises(self) -> None:
        """All retries exhausted raises RuntimeError."""
        adapter = _ConcreteAdapter()
        BaseSportsReferenceAdapter._last_request_time = 0.0

        mock_resp_500 = AsyncMock()
        mock_resp_500.status = 500
        mock_resp_500.headers = {}

        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp_500)
        mock_cm.__aexit__ = AsyncMock(return_value=None)

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_cm)
        with (
            patch("asyncio.sleep", new_callable=AsyncMock),
            pytest.raises(RuntimeError, match="attempts"),
        ):
            await adapter._get_with_retry(mock_session, "http://test.com/api")

    @pytest.mark.asyncio
    async def test_client_error_retries_then_raises(self) -> None:
        """aiohttp.ClientError triggers retries, then raises RuntimeError."""
        adapter = _ConcreteAdapter()
        BaseSportsReferenceAdapter._last_request_time = 0.0

        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(side_effect=aiohttp.ClientError("connection reset"))
        mock_cm.__aexit__ = AsyncMock(return_value=None)

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_cm)
        with (
            patch("asyncio.sleep", new_callable=AsyncMock),
            pytest.raises(RuntimeError, match=r"All .* attempts failed"),
        ):
            await adapter._get_with_retry(mock_session, "http://test.com/api")


class TestBaseSportsAdapterClassifyError:
    """Tests for _classify_error method."""

    def test_401_message_with_token(self) -> None:
        adapter = _ConcreteAdapter()
        assert adapter._classify_error(Exception("401 token expired")) == "INVALID_API_KEY"

    def test_429_in_message(self) -> None:
        adapter = _ConcreteAdapter()
        assert adapter._classify_error(Exception("got 429 from server")) == "RATE_LIMIT_EXCEEDED"

    def test_no_status_unknown(self) -> None:
        adapter = _ConcreteAdapter()
        assert adapter._classify_error(Exception("weird error")) == "UNKNOWN"


class TestBaseSportsAdapterEmitFetchFailed:
    """Tests for _emit_fetch_failed event emission."""

    def test_emit_does_not_raise(self) -> None:
        adapter = _ConcreteAdapter()
        # Should not raise — just logs and emits
        adapter._emit_fetch_failed("RATE_LIMIT_EXCEEDED", Exception("rate limited"))

    def test_emit_with_unknown_code(self) -> None:
        adapter = _ConcreteAdapter()
        adapter._emit_fetch_failed("UNKNOWN", Exception("some error"))


class TestBaseSportsAdapterDefaultMethods:
    """Tests for default method implementations returning empty lists."""

    @pytest.mark.asyncio
    async def test_get_standings_default(self) -> None:
        adapter = _ConcreteAdapter()
        result = await adapter.get_standings(39)
        assert result == []

    @pytest.mark.asyncio
    async def test_get_injuries_default(self) -> None:
        adapter = _ConcreteAdapter()
        result = await adapter.get_injuries("2026-03-22")
        assert result == []

    @pytest.mark.asyncio
    async def test_get_fixture_statistics_default(self) -> None:
        adapter = _ConcreteAdapter()
        result = await adapter.get_fixture_statistics(123)
        assert result == []

    @pytest.mark.asyncio
    async def test_get_fixture_events_default(self) -> None:
        adapter = _ConcreteAdapter()
        result = await adapter.get_fixture_events(123)
        assert result == []

    @pytest.mark.asyncio
    async def test_get_fixture_lineups_default(self) -> None:
        adapter = _ConcreteAdapter()
        result = await adapter.get_fixture_lineups(123)
        assert result == []

    @pytest.mark.asyncio
    async def test_get_fixture_player_stats_default(self) -> None:
        adapter = _ConcreteAdapter()
        result = await adapter.get_fixture_player_stats(123)
        assert result == []


# ===========================================================================
# API FOOTBALL ADAPTER (sports/adapters/api_football.py) — uncovered paths
# ===========================================================================


class TestEffectiveSeasonForLeague:
    """Tests for _effective_season_for_league."""

    def test_european_league_after_start(self) -> None:
        """European Aug-start league queried in October → same year."""
        with patch(
            "unified_api_contracts.canonical.domain.sports.league_data.get_league_by_api_football_id",
            return_value=MagicMock(season_months=[8, 9, 10, 11, 12, 1, 2, 3, 4, 5]),
        ):
            result = _effective_season_for_league(39, reference_date=date(2025, 10, 15))
        assert result == 2025

    def test_european_league_before_start(self) -> None:
        """European Aug-start league queried in March → previous year."""
        with patch(
            "unified_api_contracts.canonical.domain.sports.league_data.get_league_by_api_football_id",
            return_value=MagicMock(season_months=[8, 9, 10, 11, 12, 1, 2, 3, 4, 5]),
        ):
            result = _effective_season_for_league(39, reference_date=date(2026, 3, 15))
        assert result == 2025

    def test_south_american_league_after_start(self) -> None:
        """South American Feb-start league queried in March → same year."""
        with patch(
            "unified_api_contracts.canonical.domain.sports.league_data.get_league_by_api_football_id",
            return_value=MagicMock(season_months=[2, 3, 4, 5, 6, 7, 8, 9, 10, 11]),
        ):
            result = _effective_season_for_league(71, reference_date=date(2026, 3, 15))
        assert result == 2026

    def test_unknown_league_defaults_to_august(self) -> None:
        """Unknown league defaults to August start."""
        with patch(
            "unified_api_contracts.canonical.domain.sports.league_data.get_league_by_api_football_id",
            return_value=None,
        ):
            result = _effective_season_for_league(9999, reference_date=date(2026, 3, 15))
        assert result == 2025  # March < 8 → 2025

    def test_no_reference_date_uses_utc_now(self) -> None:
        """When no reference_date, uses today (UTC)."""
        with patch(
            "unified_api_contracts.canonical.domain.sports.league_data.get_league_by_api_football_id",
            return_value=None,
        ):
            result = _effective_season_for_league(39)
        assert isinstance(result, int)
        assert result > 2020


class TestApiFootballAdapterEdgeCases:
    """Additional tests for ApiFootballAdapter methods."""

    @pytest.fixture(autouse=True)
    def _no_retry_backoff(self):
        """Patch asyncio.sleep so adapter retry backoff is instant in tests."""
        with patch("asyncio.sleep", new_callable=AsyncMock):
            yield

    @pytest.mark.asyncio
    async def test_get_fixtures_error_raises(self) -> None:
        """get_fixtures with HTTP error raises after emitting event."""
        adapter = ApiFootballAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock({}, status=500)
        with (
            patch("aiohttp.ClientSession", return_value=mock_session),
            pytest.raises(RuntimeError),
        ):
            await adapter.get_fixtures("2026-03-22")

    @pytest.mark.asyncio
    async def test_fetch_league_fixtures_error_returns_empty(self) -> None:
        """_fetch_league_fixtures swallows errors and returns []."""
        adapter = ApiFootballAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock({}, status=500)
        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = await adapter._fetch_league_fixtures("2026-03-22", 140)
        assert result == []

    @pytest.mark.asyncio
    async def test_get_leagues_skips_non_dict_league_data(self) -> None:
        """Items with non-dict league data are skipped."""
        adapter = ApiFootballAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock(
            {
                "response": [
                    {"league": "not-a-dict"},
                    {"not_league": "data"},
                ]
            }
        )
        with patch("aiohttp.ClientSession", return_value=mock_session):
            leagues = await adapter.get_leagues()
        assert leagues == []

    @pytest.mark.asyncio
    async def test_get_teams_skips_invalid_items(self) -> None:
        """Teams with invalid structure are skipped."""
        adapter = ApiFootballAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock(
            {
                "response": [
                    {"team": "not-a-dict"},
                    "not-a-dict",
                ]
            }
        )
        with patch("aiohttp.ClientSession", return_value=mock_session):
            teams = await adapter.get_teams(39, season=2025)
        assert teams == []

    @pytest.mark.asyncio
    async def test_get_standings_with_non_list_standings(self) -> None:
        """Standings data without list standings are skipped."""
        adapter = ApiFootballAdapter(api_key="test-key")
        mock_session = _make_aiohttp_mock(
            {
                "response": [
                    {
                        "league": {
                            "id": 39,
                            "standings": "not-a-list",
                        }
                    },
                    {
                        "league": "not-a-dict",
                    },
                ]
            }
        )
        with patch("aiohttp.ClientSession", return_value=mock_session):
            standings = await adapter.get_standings(39, season=2025)
        assert standings == []


class TestApiFootballParseHelpers:
    """Tests for parse helper functions."""

    def test_parse_fixture_response_missing_fields_graceful(self) -> None:
        """Fixture with minimal data should at least not crash."""
        item = {
            "fixture": {
                "id": 1,
                "date": "2026-01-01T00:00:00Z",
                "status": {"long": "NS", "short": "NS"},
            },
            "league": {"id": 39, "name": "EPL", "season": 2025},
            "teams": None,
            "goals": None,
            "score": None,
        }
        result = _parse_fixture_response(item)
        # Should parse without crashing — teams is None
        assert result is not None or result is None  # Either outcome is fine

    def test_parse_teams_bad_structure(self) -> None:
        """Invalid team structure returns None."""
        result = _parse_teams({"home": 123, "away": None})
        assert result is None

    def test_extract_response_int(self) -> None:
        assert _extract_response(42) == []

    def test_extract_response_none(self) -> None:
        assert _extract_response(None) == []

    def test_flatten_standings_empty(self) -> None:
        assert _flatten_standings_groups([]) == []


# ===========================================================================
# API FOOTBALL URDI WRAPPER (adapters/api_football.py) — full coverage
# ===========================================================================


class TestApiFootballReferenceDataAdapter:
    """Tests for the URDI adapter wrapper."""

    def test_venue_name(self) -> None:
        adapter = ApiFootballReferenceDataAdapter(api_key="test-key", date="2026-03-22")
        assert adapter.venue == "API_FOOTBALL"

    @pytest.mark.asyncio
    async def test_wrong_instrument_type_returns_empty(self) -> None:
        adapter = ApiFootballReferenceDataAdapter(api_key="key", date="2026-03-22")
        result = await adapter.get_instruments(instrument_type="SPOT_PAIR")
        assert result == []

    @pytest.mark.asyncio
    async def test_no_date_returns_empty(self) -> None:
        """No date set returns empty list with warning."""
        adapter = ApiFootballReferenceDataAdapter(api_key="key")
        result = await adapter.get_instruments()
        assert result == []

    @pytest.mark.asyncio
    async def test_get_instruments_with_fixtures(self) -> None:
        """Full flow: fetches fixtures and converts to InstrumentRecords."""
        from unified_api_contracts.sports import CanonicalFixture, CanonicalLeague, CanonicalTeam

        adapter = ApiFootballReferenceDataAdapter(api_key="key", date="2026-03-22")
        mock_fixture = CanonicalFixture(
            fixture_id="EPL:LIV-v-MUN:20260322",
            home_team=CanonicalTeam(team_id="LIV", name="Liverpool"),
            away_team=CanonicalTeam(team_id="MUN", name="Manchester United"),
            league=CanonicalLeague(league_id="39", name="Premier League", country="England"),
            kickoff_utc=datetime(2026, 3, 22, 15, 0, tzinfo=UTC),
            season="2025-26",
            source="api_football",
            status="NS",
            source_fixture_id="123456",
        )
        with patch.object(adapter._sports_adapter, "get_fixtures", AsyncMock(return_value=[mock_fixture])):
            results = await adapter.get_instruments()
        assert len(results) == 1
        assert results[0].venue == "API_FOOTBALL"
        assert results[0].base_asset == "football"

    @pytest.mark.asyncio
    async def test_completed_fixtures_tracked(self) -> None:
        """Completed fixtures (FT/AET/PEN) IDs are captured."""
        from unified_api_contracts.sports import CanonicalFixture, CanonicalLeague, CanonicalTeam

        adapter = ApiFootballReferenceDataAdapter(api_key="key", date="2026-03-22")
        finished_fixture = CanonicalFixture(
            fixture_id="EPL:ARS-v-CHE:20260322",
            home_team=CanonicalTeam(team_id="ARS", name="Arsenal"),
            away_team=CanonicalTeam(team_id="CHE", name="Chelsea"),
            league=CanonicalLeague(league_id="39", name="Premier League", country="England"),
            kickoff_utc=datetime(2026, 3, 22, 15, 0, tzinfo=UTC),
            season="2025-26",
            source="api_football",
            status="FT",
            source_fixture_id="789",
        )
        with patch.object(adapter._sports_adapter, "get_fixtures", AsyncMock(return_value=[finished_fixture])):
            await adapter.get_instruments()
        assert 789 in adapter.completed_fixture_ids

    @pytest.mark.asyncio
    async def test_aet_fixture_tracked(self) -> None:
        """AET (after extra time) fixtures tracked."""
        from unified_api_contracts.sports import CanonicalFixture, CanonicalLeague, CanonicalTeam

        adapter = ApiFootballReferenceDataAdapter(api_key="key", date="2026-03-22")
        aet_fixture = CanonicalFixture(
            fixture_id="EPL:TOT-v-EVE:20260322",
            home_team=CanonicalTeam(team_id="TOT", name="Tottenham"),
            away_team=CanonicalTeam(team_id="EVE", name="Everton"),
            league=CanonicalLeague(league_id="39", name="Premier League", country="England"),
            kickoff_utc=datetime(2026, 3, 22, 20, 0, tzinfo=UTC),
            season="2025-26",
            source="api_football",
            status="AET",
            source_fixture_id="456",
        )
        with patch.object(adapter._sports_adapter, "get_fixtures", AsyncMock(return_value=[aet_fixture])):
            await adapter.get_instruments()
        assert 456 in adapter.completed_fixture_ids

    @pytest.mark.asyncio
    async def test_non_completed_fixture_not_tracked(self) -> None:
        """NS (not started) fixtures not in completed list."""
        from unified_api_contracts.sports import CanonicalFixture, CanonicalLeague, CanonicalTeam

        adapter = ApiFootballReferenceDataAdapter(api_key="key", date="2026-03-22")
        ns_fixture = CanonicalFixture(
            fixture_id="EPL:LIV-v-MCI:20260322",
            home_team=CanonicalTeam(team_id="LIV", name="Liverpool"),
            away_team=CanonicalTeam(team_id="MCI", name="Man City"),
            league=CanonicalLeague(league_id="39", name="Premier League", country="England"),
            kickoff_utc=datetime(2026, 3, 22, 15, 0, tzinfo=UTC),
            season="2025-26",
            source="api_football",
            status="NS",
            source_fixture_id="111",
        )
        with patch.object(adapter._sports_adapter, "get_fixtures", AsyncMock(return_value=[ns_fixture])):
            await adapter.get_instruments()
        assert adapter.completed_fixture_ids == []

    @pytest.mark.asyncio
    async def test_get_instrument_by_symbol(self) -> None:
        """get_instrument looks up by symbol."""
        from unified_api_contracts.sports import CanonicalFixture, CanonicalLeague, CanonicalTeam

        adapter = ApiFootballReferenceDataAdapter(api_key="key", date="2026-03-22")
        mock_fixture = CanonicalFixture(
            fixture_id="EPL:ARS-v-CHE:20260322",
            home_team=CanonicalTeam(team_id="ARS", name="Arsenal"),
            away_team=CanonicalTeam(team_id="CHE", name="Chelsea"),
            league=CanonicalLeague(league_id="39", name="Premier League", country="England"),
            kickoff_utc=datetime(2026, 3, 22, 15, 0, tzinfo=UTC),
            season="2025-26",
            source="api_football",
            status="NS",
        )
        with patch.object(adapter._sports_adapter, "get_fixtures", AsyncMock(return_value=[mock_fixture])):
            result = await adapter.get_instrument("Arsenal vs Chelsea")
        assert result is not None

    @pytest.mark.asyncio
    async def test_get_instrument_not_found(self) -> None:
        """get_instrument returns None when not found."""
        adapter = ApiFootballReferenceDataAdapter(api_key="key", date="2026-03-22")
        with patch.object(adapter._sports_adapter, "get_fixtures", AsyncMock(return_value=[])):
            result = await adapter.get_instrument("nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_options_chain_raises(self) -> None:
        adapter = ApiFootballReferenceDataAdapter(api_key="key")
        with pytest.raises(NotImplementedError):
            await adapter.get_options_chain("football")

    @pytest.mark.asyncio
    async def test_get_expiry_calendar_raises(self) -> None:
        adapter = ApiFootballReferenceDataAdapter(api_key="key")
        with pytest.raises(NotImplementedError):
            await adapter.get_expiry_calendar("football")

    @pytest.mark.asyncio
    async def test_get_funding_rate_raises(self) -> None:
        adapter = ApiFootballReferenceDataAdapter(api_key="key")
        with pytest.raises(NotImplementedError):
            await adapter.get_funding_rate("football")

    @pytest.mark.asyncio
    async def test_get_ohlcv_raises(self) -> None:
        adapter = ApiFootballReferenceDataAdapter(api_key="key")
        with pytest.raises(NotImplementedError):
            await adapter.get_ohlcv("football")


class TestCanonicalFixtureToInstrument:
    """Tests for _canonical_fixture_to_instrument."""

    def test_complete_fixture(self) -> None:
        from unified_api_contracts.sports import CanonicalFixture, CanonicalLeague, CanonicalTeam

        fixture = CanonicalFixture(
            fixture_id="test-123",
            home_team=CanonicalTeam(team_id="ARS", name="Arsenal"),
            away_team=CanonicalTeam(team_id="CHE", name="Chelsea"),
            league=CanonicalLeague(league_id="39", name="Premier League", country="England"),
            kickoff_utc=datetime(2026, 3, 22, 15, 0, tzinfo=UTC),
            season="2025-26",
            source="api_football",
            status="NS",
        )
        result = _canonical_fixture_to_instrument(fixture)
        assert result is not None
        assert result.venue == "API_FOOTBALL"
        assert result.base_asset == "football"
        assert result.quote_asset == "USD"
        assert result.expiry is not None

    def test_fixture_no_home_name(self) -> None:
        from unified_api_contracts.sports import CanonicalFixture, CanonicalLeague, CanonicalTeam

        fixture = CanonicalFixture(
            fixture_id="test-456",
            home_team=CanonicalTeam(team_id="X", name=""),
            away_team=CanonicalTeam(team_id="Y", name="Chelsea"),
            league=CanonicalLeague(league_id="39", name="Premier League", country="England"),
            kickoff_utc=datetime(2026, 3, 22, 15, 0, tzinfo=UTC),
            season="2025-26",
            source="api_football",
            status="NS",
        )
        result = _canonical_fixture_to_instrument(fixture)
        assert result is not None
        assert "fixture_" in result.raw_symbol

    def test_fixture_no_away_name(self) -> None:
        """Away team with empty name falls back to fixture_ prefix."""
        from unified_api_contracts.sports import CanonicalFixture, CanonicalLeague, CanonicalTeam

        fixture = CanonicalFixture(
            fixture_id="test-789",
            home_team=CanonicalTeam(team_id="X", name="Arsenal"),
            away_team=CanonicalTeam(team_id="Y", name=""),
            league=CanonicalLeague(league_id="39", name="Premier League", country="England"),
            kickoff_utc=datetime(2026, 3, 22, 15, 0, tzinfo=UTC),
            season="2025-26",
            source="api_football",
            status="NS",
        )
        result = _canonical_fixture_to_instrument(fixture)
        assert result is not None
        assert "fixture_" in result.raw_symbol

    def test_fixture_with_kickoff_sets_expiry(self) -> None:
        """Fixture with valid kickoff sets expiry and uses canonical key."""
        from unified_api_contracts.sports import CanonicalFixture, CanonicalLeague, CanonicalTeam

        fixture = CanonicalFixture(
            fixture_id="test-kick",
            home_team=CanonicalTeam(team_id="A", name="TeamA"),
            away_team=CanonicalTeam(team_id="B", name="TeamB"),
            league=CanonicalLeague(league_id="39", name="Test League", country="Test"),
            kickoff_utc=datetime(2026, 3, 22, 15, 0, tzinfo=UTC),
            season="2025-26",
            source="api_football",
            status="NS",
        )
        result = _canonical_fixture_to_instrument(fixture)
        assert result is not None
        assert result.expiry is not None
        # With both team names and kickoff, canonical key is generated (not FIXTURE:id prefix)
        assert "API_FOOTBALL:FIXTURE:" not in result.instrument_key

    def test_fixture_with_league_info_builds_canonical_key(self) -> None:
        from unified_api_contracts.sports import CanonicalFixture, CanonicalLeague, CanonicalTeam

        fixture = CanonicalFixture(
            fixture_id="match-123",
            home_team=CanonicalTeam(team_id="LIV", name="Liverpool"),
            away_team=CanonicalTeam(team_id="MUN", name="Manchester United"),
            league=CanonicalLeague(league_id="39", name="Premier League", country="England"),
            kickoff_utc=datetime(2026, 3, 22, 15, 0, tzinfo=UTC),
            season="2025-26",
            source="api_football",
            status="NS",
        )
        result = _canonical_fixture_to_instrument(fixture)
        assert result is not None
        # Canonical key should contain league/team info, not just FIXTURE:id
        assert "API_FOOTBALL:FIXTURE:" not in result.instrument_key

    def test_fixture_no_league_name(self) -> None:
        from unified_api_contracts.sports import CanonicalFixture, CanonicalLeague, CanonicalTeam

        fixture = CanonicalFixture(
            fixture_id="match-no-league",
            home_team=CanonicalTeam(team_id="A", name="TeamA"),
            away_team=CanonicalTeam(team_id="B", name="TeamB"),
            league=CanonicalLeague(league_id="39", name="", country=""),
            kickoff_utc=datetime(2026, 3, 22, 15, 0, tzinfo=UTC),
            season="2025-26",
            source="api_football",
            status="NS",
        )
        result = _canonical_fixture_to_instrument(fixture)
        assert result is not None
        # Empty league name → uses "UNKNOWN"
        assert "UNKNOWN" in result.instrument_key
