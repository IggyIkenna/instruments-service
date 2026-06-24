"""Coverage boost for sports reference data adapters.

Targets uncovered lines in:
- api_football.py: get_fixtures_with_raw, _fetch_league_fixtures_with_raw,
  get_live_fixtures, _fetch_league_fixtures (normalize exception),
  get_leagues (exception), _parse_fixture_list_with_raw (normalize exception)
- footystats.py: get_fixtures (exception + None match + league filter + normalize exc),
  get_leagues (exception), get_teams (exception)

All tests are credential-free and network-free — the HTTP layer is mocked
via patch on ``aiohttp.ClientSession`` or ``_get_with_retry``.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Helpers: minimal fixture raw item and mock session builders
# ---------------------------------------------------------------------------


def _minimal_fixture_item(fixture_id: int = 100) -> dict:
    return {
        "fixture": {
            "id": fixture_id,
            "date": "2026-03-22T15:00:00+00:00",
            "status": {"long": "Not Started", "short": "NS"},
        },
        "league": {"id": 39, "name": "Premier League", "season": 2025},
        "teams": {
            "home": {"id": 40, "name": "Liverpool"},
            "away": {"id": 33, "name": "Manchester United"},
        },
        "goals": None,
        "score": None,
    }


def _make_session_cm(json_return: object) -> MagicMock:
    """Build a mock aiohttp.ClientSession context manager returning json_return."""
    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json = AsyncMock(return_value=json_return)
    mock_resp.headers = {}

    resp_cm = MagicMock()
    resp_cm.__aenter__ = AsyncMock(return_value=mock_resp)
    resp_cm.__aexit__ = AsyncMock(return_value=None)

    session_obj = MagicMock()
    session_obj.get = MagicMock(return_value=resp_cm)

    session_cm = MagicMock()
    session_cm.__aenter__ = AsyncMock(return_value=session_obj)
    session_cm.__aexit__ = AsyncMock(return_value=None)
    return session_cm


# ---------------------------------------------------------------------------
# ApiFootballAdapter — get_fixtures_with_raw
# ---------------------------------------------------------------------------


class TestApiFootballGetFixturesWithRaw:
    """get_fixtures_with_raw — season-cache path (league_ids supplied) and no-filter fallback."""

    @pytest.fixture(autouse=True)
    def _no_sleep(self):
        with patch("asyncio.sleep", new_callable=AsyncMock):
            yield

    @pytest.fixture(autouse=True)
    def _clear_season_cache(self):
        """Isolate tests: clear the class-level season fixture cache before each run."""
        from instruments_service.reference_data.adapters.sports.adapters.api_football import (
            ApiFootballAdapter,
        )

        ApiFootballAdapter._season_fixture_cache.clear()
        yield
        ApiFootballAdapter._season_fixture_cache.clear()

    @pytest.mark.asyncio
    async def test_single_league_success(self) -> None:
        """Single league with league_ids: uses season cache, filters by date, returns pairs."""
        from instruments_service.reference_data.adapters.sports.adapters.api_football import (
            ApiFootballAdapter,
        )

        adapter = ApiFootballAdapter(api_key="test-key")
        raw_response = {"response": [_minimal_fixture_item()]}
        session_cm = _make_session_cm(raw_response)

        with patch("aiohttp.ClientSession", return_value=session_cm):
            result = await adapter.get_fixtures_with_raw("2026-03-22", league_ids=[39])

        assert isinstance(result, list)
        assert len(result) == 1
        _canonical_fx, raw_item = result[0]
        assert raw_item == _minimal_fixture_item()

    @pytest.mark.asyncio
    async def test_multi_league_season_cache(self) -> None:
        """Multi-league: each league fetches its own season, results merged and filtered."""
        from instruments_service.reference_data.adapters.sports.adapters.api_football import (
            ApiFootballAdapter,
        )

        adapter = ApiFootballAdapter(api_key="test-key")
        raw_response = {"response": [_minimal_fixture_item()]}
        session_cm = _make_session_cm(raw_response)

        with patch("aiohttp.ClientSession", return_value=session_cm):
            result = await adapter.get_fixtures_with_raw("2026-03-22", league_ids=[39, 140])

        # One fixture per league call (both on 2026-03-22) — combined
        assert isinstance(result, list)
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_no_league_ids_uses_date_path(self) -> None:
        """No league_ids: falls back to original per-date GET /fixtures?date= call."""
        from instruments_service.reference_data.adapters.sports.adapters.api_football import (
            ApiFootballAdapter,
        )

        adapter = ApiFootballAdapter(api_key="test-key")
        raw_response = {"response": [_minimal_fixture_item()]}
        session_cm = _make_session_cm(raw_response)

        with patch("aiohttp.ClientSession", return_value=session_cm):
            result = await adapter.get_fixtures_with_raw("2026-03-22")

        assert isinstance(result, list)
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_exception_reraises(self) -> None:
        """HTTP exception from season fetch re-raises after emit_fetch_failed."""
        from instruments_service.reference_data.adapters.sports.adapters.api_football import (
            ApiFootballAdapter,
        )

        adapter = ApiFootballAdapter(api_key="test-key")

        with (
            patch.object(
                adapter,
                "_get_with_retry",
                side_effect=RuntimeError("network error"),
            ),
            patch.object(adapter, "_emit_fetch_failed"),
            pytest.raises(RuntimeError, match="network error"),
        ):
            await adapter.get_fixtures_with_raw("2026-03-22", league_ids=[39])

    @pytest.mark.asyncio
    async def test_date_filter_excludes_other_days(self) -> None:
        """Season fixtures for different dates are excluded by date filter."""
        from instruments_service.reference_data.adapters.sports.adapters.api_football import (
            ApiFootballAdapter,
        )

        # Fixture on 2026-03-22; query for 2026-03-23 → empty
        adapter = ApiFootballAdapter(api_key="test-key")
        raw_response = {"response": [_minimal_fixture_item()]}
        session_cm = _make_session_cm(raw_response)

        with patch("aiohttp.ClientSession", return_value=session_cm):
            result = await adapter.get_fixtures_with_raw("2026-03-23", league_ids=[39])

        assert result == []


# ---------------------------------------------------------------------------
# ApiFootballAdapter — _fetch_season_fixtures_with_raw (season cache)
# ---------------------------------------------------------------------------


class TestApiFootballFetchSeasonFixturesWithRaw:
    """_fetch_season_fixtures_with_raw: cache hit, success, exception re-raise."""

    @pytest.fixture(autouse=True)
    def _no_sleep(self):
        with patch("asyncio.sleep", new_callable=AsyncMock):
            yield

    @pytest.fixture(autouse=True)
    def _clear_season_cache(self):
        from instruments_service.reference_data.adapters.sports.adapters.api_football import (
            ApiFootballAdapter,
        )

        ApiFootballAdapter._season_fixture_cache.clear()
        yield
        ApiFootballAdapter._season_fixture_cache.clear()

    @pytest.mark.asyncio
    async def test_cache_miss_fetches_and_stores(self) -> None:
        """First call for (league, season) fetches from API and stores in cache."""
        from instruments_service.reference_data.adapters.sports.adapters.api_football import (
            ApiFootballAdapter,
        )

        adapter = ApiFootballAdapter(api_key="test-key")
        raw_response = {"response": [_minimal_fixture_item()]}
        session_cm = _make_session_cm(raw_response)

        with patch("aiohttp.ClientSession", return_value=session_cm):
            result = await adapter._fetch_season_fixtures_with_raw(39, 2025)

        assert isinstance(result, list)
        assert len(result) == 1
        assert ApiFootballAdapter._season_fixture_cache[(39, 2025)] is result

    @pytest.mark.asyncio
    async def test_cache_hit_skips_api(self) -> None:
        """Second call for same (league, season) returns cached result without HTTP."""
        from instruments_service.reference_data.adapters.sports.adapters.api_football import (
            ApiFootballAdapter,
            CanonicalFixture,
        )

        adapter = ApiFootballAdapter(api_key="test-key")
        sentinel: list[tuple[CanonicalFixture, dict[str, object]]] = []
        ApiFootballAdapter._season_fixture_cache[(39, 2025)] = sentinel

        # Even if the HTTP layer would raise, we should get the cached sentinel.
        with patch.object(adapter, "_fetch_and_extract", side_effect=RuntimeError("should not call")):
            result = await adapter._fetch_season_fixtures_with_raw(39, 2025)

        assert result is sentinel

    @pytest.mark.asyncio
    async def test_exception_emits_and_reraises(self) -> None:
        """HTTP failure calls emit_fetch_failed and re-raises the original exception."""
        from instruments_service.reference_data.adapters.sports.adapters.api_football import (
            ApiFootballAdapter,
        )

        adapter = ApiFootballAdapter(api_key="test-key")

        with (
            patch.object(adapter, "_fetch_and_extract", side_effect=RuntimeError("api down")),
            patch.object(adapter, "_emit_fetch_failed") as mock_emit,
            pytest.raises(RuntimeError, match="api down"),
        ):
            await adapter._fetch_season_fixtures_with_raw(39, 2025)

        mock_emit.assert_called_once()


# ---------------------------------------------------------------------------
# ApiFootballAdapter — _fetch_league_fixtures_with_raw
# ---------------------------------------------------------------------------


class TestApiFootballFetchLeagueFixturesWithRaw:
    """_fetch_league_fixtures_with_raw (single-league, legacy per-date fan-out helper)."""

    @pytest.fixture(autouse=True)
    def _no_sleep(self):
        with patch("asyncio.sleep", new_callable=AsyncMock):
            yield

    @pytest.mark.asyncio
    async def test_success_returns_pairs(self) -> None:
        """Lines 267-282: success path returns paired list."""
        from instruments_service.reference_data.adapters.sports.adapters.api_football import (
            ApiFootballAdapter,
        )

        adapter = ApiFootballAdapter(api_key="test-key")
        raw_response = {"response": [_minimal_fixture_item(200)]}
        session_cm = _make_session_cm(raw_response)

        with patch("aiohttp.ClientSession", return_value=session_cm):
            result = await adapter._fetch_league_fixtures_with_raw("2026-03-22", 39)

        assert isinstance(result, list)

    @pytest.mark.asyncio
    async def test_exception_returns_empty_list(self) -> None:
        """Lines 278-281: exception → returns [] instead of raising."""
        from instruments_service.reference_data.adapters.sports.adapters.api_football import (
            ApiFootballAdapter,
        )

        adapter = ApiFootballAdapter(api_key="test-key")

        with (
            patch.object(
                adapter,
                "_get_with_retry",
                side_effect=RuntimeError("fail"),
            ),
            patch.object(adapter, "_emit_fetch_failed"),
        ):
            result = await adapter._fetch_league_fixtures_with_raw("2026-03-22", 39)

        assert result == []


# ---------------------------------------------------------------------------
# ApiFootballAdapter — get_live_fixtures
# ---------------------------------------------------------------------------


class TestApiFootballGetLiveFixtures:
    """Lines 290-302: get_live_fixtures method."""

    @pytest.fixture(autouse=True)
    def _no_sleep(self):
        with patch("asyncio.sleep", new_callable=AsyncMock):
            yield

    @pytest.mark.asyncio
    async def test_success_returns_fixture_list(self) -> None:
        """Lines 290-302: GET /fixtures?live=all returns canonical fixture list."""
        from instruments_service.reference_data.adapters.sports.adapters.api_football import (
            ApiFootballAdapter,
        )

        adapter = ApiFootballAdapter(api_key="test-key")
        raw_response = {"response": [_minimal_fixture_item(301)]}
        session_cm = _make_session_cm(raw_response)

        with patch("aiohttp.ClientSession", return_value=session_cm):
            result = await adapter.get_live_fixtures()

        assert isinstance(result, list)

    @pytest.mark.asyncio
    async def test_exception_returns_empty_list(self) -> None:
        """Lines 295-298: exception → returns [] instead of raising."""
        from instruments_service.reference_data.adapters.sports.adapters.api_football import (
            ApiFootballAdapter,
        )

        adapter = ApiFootballAdapter(api_key="test-key")

        with (
            patch.object(
                adapter,
                "_get_with_retry",
                side_effect=RuntimeError("live fail"),
            ),
            patch.object(adapter, "_emit_fetch_failed"),
        ):
            result = await adapter.get_live_fixtures()

        assert result == []


# ---------------------------------------------------------------------------
# ApiFootballAdapter — _fetch_league_fixtures (normalize exception path)
# ---------------------------------------------------------------------------


class TestApiFootballFetchLeagueFixturesNormalize:
    """Lines 331-333: normalize exception inside per-item loop logs warning + continues."""

    @pytest.fixture(autouse=True)
    def _no_sleep(self):
        with patch("asyncio.sleep", new_callable=AsyncMock):
            yield

    @pytest.mark.asyncio
    async def test_normalize_exception_skips_and_continues(self) -> None:
        """Lines 328-333: normalize_api_football_fixture raises → warning + continue."""
        from instruments_service.reference_data.adapters.sports.adapters.api_football import (
            ApiFootballAdapter,
        )

        adapter = ApiFootballAdapter(api_key="test-key")
        raw_response = {"response": [_minimal_fixture_item(999)]}
        session_cm = _make_session_cm(raw_response)

        with (
            patch("aiohttp.ClientSession", return_value=session_cm),
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.api_football.normalize_api_football_fixture",
                side_effect=ValueError("bad data"),
            ),
        ):
            result = await adapter._fetch_league_fixtures("2026-03-22", 39)

        # Exception swallowed, returns empty
        assert result == []


# ---------------------------------------------------------------------------
# ApiFootballAdapter — get_leagues (exception)
# ---------------------------------------------------------------------------


class TestApiFootballGetLeaguesException:
    """Lines 346-349: get_leagues() re-raises after emit_fetch_failed."""

    @pytest.fixture(autouse=True)
    def _no_sleep(self):
        with patch("asyncio.sleep", new_callable=AsyncMock):
            yield

    @pytest.mark.asyncio
    async def test_exception_reraises(self) -> None:
        """Lines 346-349: exception → emit_fetch_failed + re-raise."""
        from instruments_service.reference_data.adapters.sports.adapters.api_football import (
            ApiFootballAdapter,
        )

        adapter = ApiFootballAdapter(api_key="test-key")

        with (
            patch.object(
                adapter,
                "_get_with_retry",
                side_effect=RuntimeError("leagues fail"),
            ),
            patch.object(adapter, "_emit_fetch_failed"),
            pytest.raises(RuntimeError, match="leagues fail"),
        ):
            await adapter.get_leagues()


# ---------------------------------------------------------------------------
# _parse_fixture_list_with_raw — normalize exception path
# ---------------------------------------------------------------------------


def test_parse_fixture_list_with_raw_normalize_exception_skips() -> None:
    """Lines 678-680: normalize_api_football_fixture raises → fixture skipped with warning."""
    from instruments_service.reference_data.adapters.sports.adapters.api_football import (
        _parse_fixture_list_with_raw,
    )

    item = _minimal_fixture_item(777)

    with patch(
        "instruments_service.reference_data.adapters.sports.adapters.api_football.normalize_api_football_fixture",
        side_effect=ValueError("bad normalize"),
    ):
        result = _parse_fixture_list_with_raw([item])

    assert result == []


# ===========================================================================
# FootyStats adapter
# ===========================================================================


class TestFootystatsGetFixtures:
    """Lines 85-107: get_fixtures error paths."""

    @pytest.fixture(autouse=True)
    def _no_sleep(self):
        with patch("asyncio.sleep", new_callable=AsyncMock):
            yield

    @pytest.mark.asyncio
    async def test_exception_reraises(self) -> None:
        """Lines 85-88: HTTP exception re-raises after emit_fetch_failed."""
        from instruments_service.reference_data.adapters.sports.adapters.footystats import (
            FootystatsAdapter,
        )

        adapter = FootystatsAdapter(api_key="test-key")

        with (
            patch.object(
                adapter,
                "_get_with_retry",
                side_effect=RuntimeError("footystats fail"),
            ),
            patch.object(adapter, "_emit_fetch_failed"),
            pytest.raises(RuntimeError, match="footystats fail"),
        ):
            await adapter.get_fixtures("2026-03-22")

    @pytest.mark.asyncio
    async def test_match_none_skipped(self) -> None:
        """Line 96: _parse_match returning None skips the item (continue branch)."""
        from instruments_service.reference_data.adapters.sports.adapters.footystats import (
            FootystatsAdapter,
        )

        adapter = FootystatsAdapter(api_key="test-key")
        raw_response = {"data": [{"match_id": 1}]}
        session_cm = _make_session_cm(raw_response)

        with (
            patch("aiohttp.ClientSession", return_value=session_cm),
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.footystats._parse_match",
                return_value=None,
            ),
        ):
            result = await adapter.get_fixtures("2026-03-22")

        assert result == []

    @pytest.mark.asyncio
    async def test_league_filter_skips_non_matching(self) -> None:
        """Line 99: league_ids filter skips matches with different competition_id."""
        from instruments_service.reference_data.adapters.sports.adapters.footystats import (
            FootystatsAdapter,
        )

        adapter = FootystatsAdapter(api_key="test-key")
        raw_response = {"data": [{"match_id": 1}]}
        session_cm = _make_session_cm(raw_response)

        mock_match = MagicMock()
        mock_match.competition_id = 999  # won't match league_ids=[1]

        with (
            patch("aiohttp.ClientSession", return_value=session_cm),
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.footystats._parse_match",
                return_value=mock_match,
            ),
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.footystats.normalize_footystats_match",
                return_value=MagicMock(),
            ),
        ):
            result = await adapter.get_fixtures("2026-03-22", league_ids=[1])

        assert result == []

    @pytest.mark.asyncio
    async def test_normalize_exception_continues(self) -> None:
        """Lines 101-107: normalize exception logs warning and continues loop."""
        from instruments_service.reference_data.adapters.sports.adapters.footystats import (
            FootystatsAdapter,
        )

        adapter = FootystatsAdapter(api_key="test-key")
        raw_response = {"data": [{"match_id": 1}]}
        session_cm = _make_session_cm(raw_response)

        mock_match = MagicMock()
        mock_match.competition_id = 1

        with (
            patch("aiohttp.ClientSession", return_value=session_cm),
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.footystats._parse_match",
                return_value=mock_match,
            ),
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.footystats.normalize_footystats_match",
                side_effect=ValueError("bad data"),
            ),
        ):
            result = await adapter.get_fixtures("2026-03-22")

        assert result == []


class TestFootystatsGetLeagues:
    """Lines 124-127: get_leagues exception re-raises."""

    @pytest.fixture(autouse=True)
    def _no_sleep(self):
        with patch("asyncio.sleep", new_callable=AsyncMock):
            yield

    @pytest.mark.asyncio
    async def test_exception_reraises(self) -> None:
        """Lines 124-127: HTTP exception re-raises after emit_fetch_failed."""
        from instruments_service.reference_data.adapters.sports.adapters.footystats import (
            FootystatsAdapter,
        )

        adapter = FootystatsAdapter(api_key="test-key")

        with (
            patch.object(
                adapter,
                "_get_with_retry",
                side_effect=RuntimeError("leagues fail"),
            ),
            patch.object(adapter, "_emit_fetch_failed"),
            pytest.raises(RuntimeError, match="leagues fail"),
        ):
            await adapter.get_leagues()


class TestFootystatsGetTeams:
    """Lines 172-175: get_teams exception re-raises."""

    @pytest.fixture(autouse=True)
    def _no_sleep(self):
        with patch("asyncio.sleep", new_callable=AsyncMock):
            yield

    @pytest.mark.asyncio
    async def test_exception_reraises(self) -> None:
        """Lines 172-175: HTTP exception re-raises after emit_fetch_failed."""
        from instruments_service.reference_data.adapters.sports.adapters.footystats import (
            FootystatsAdapter,
        )

        adapter = FootystatsAdapter(api_key="test-key")

        with (
            patch.object(
                adapter,
                "_get_with_retry",
                side_effect=RuntimeError("teams fail"),
            ),
            patch.object(adapter, "_emit_fetch_failed"),
            pytest.raises(RuntimeError, match="teams fail"),
        ):
            await adapter.get_teams(league_id=1)


# ===========================================================================
# Footystats — non-dict items + exception branches (lines 133, 144-146,
#              181, 194-196, 230-233, 241, 260-266, 294-297, 305, 316-322,
#              483-485)
# ===========================================================================


def _make_footystats_session_cm() -> MagicMock:
    """Async context manager that yields any mock session."""
    mock_sess = MagicMock()
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=mock_sess)
    cm.__aexit__ = AsyncMock(return_value=None)
    return cm


class TestFootystatsGetLeaguesEdgeCases:
    """Lines 133, 144-146: non-dict item skipped, CanonicalLeague exception continues."""

    @pytest.mark.asyncio
    async def test_non_dict_item_skipped(self) -> None:
        """Line 133: _extract_data returns a non-dict item → silently skipped."""
        from instruments_service.reference_data.adapters.sports.adapters.footystats import (
            FootystatsAdapter,
        )

        adapter = FootystatsAdapter(api_key="test-key")
        with (
            patch.object(adapter, "_make_session", return_value=_make_footystats_session_cm()),
            patch.object(adapter, "_get_with_retry", new_callable=AsyncMock, return_value={}),
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.footystats._extract_data",
                return_value=["not_a_dict", 42],
            ),
        ):
            result = await adapter.get_leagues()
        assert result == []

    @pytest.mark.asyncio
    async def test_canonical_league_exception_continues(self) -> None:
        """Lines 144-146: CanonicalLeague constructor raises → logs warning and continues."""
        from instruments_service.reference_data.adapters.sports.adapters.footystats import (
            FootystatsAdapter,
        )

        adapter = FootystatsAdapter(api_key="test-key")
        with (
            patch.object(adapter, "_make_session", return_value=_make_footystats_session_cm()),
            patch.object(adapter, "_get_with_retry", new_callable=AsyncMock, return_value={}),
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.footystats._extract_data",
                return_value=[{"id": "1", "name": "EPL"}],
            ),
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.footystats.CanonicalLeague",
                side_effect=ValueError("bad league"),
            ),
        ):
            result = await adapter.get_leagues()
        assert result == []


class TestFootystatsGetTeamsEdgeCases:
    """Lines 181, 194-196: non-dict item + CanonicalTeam exception."""

    @pytest.mark.asyncio
    async def test_non_dict_item_skipped(self) -> None:
        """Line 181: non-dict item skipped."""
        from instruments_service.reference_data.adapters.sports.adapters.footystats import (
            FootystatsAdapter,
        )

        adapter = FootystatsAdapter(api_key="test-key")
        with (
            patch.object(adapter, "_make_session", return_value=_make_footystats_session_cm()),
            patch.object(adapter, "_get_with_retry", new_callable=AsyncMock, return_value={}),
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.footystats._extract_data",
                return_value=["not_a_dict"],
            ),
        ):
            result = await adapter.get_teams(league_id=1)
        assert result == []

    @pytest.mark.asyncio
    async def test_canonical_team_exception_continues(self) -> None:
        """Lines 194-196: CanonicalTeam raises → logs and continues."""
        from instruments_service.reference_data.adapters.sports.adapters.footystats import (
            FootystatsAdapter,
        )

        adapter = FootystatsAdapter(api_key="test-key")
        with (
            patch.object(adapter, "_make_session", return_value=_make_footystats_session_cm()),
            patch.object(adapter, "_get_with_retry", new_callable=AsyncMock, return_value={}),
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.footystats._extract_data",
                return_value=[{"id": "1", "clean_name": "Arsenal"}],
            ),
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.footystats.CanonicalTeam",
                side_effect=ValueError("bad team"),
            ),
        ):
            result = await adapter.get_teams(league_id=1)
        assert result == []


class TestFootystatsGetFixturePredictionsEdgeCases:
    """Lines 230-233, 241, 260-266: exception paths in get_fixture_predictions."""

    @pytest.mark.asyncio
    async def test_http_exception_reraises(self) -> None:
        """Lines 230-233: HTTP exception re-raises."""
        from instruments_service.reference_data.adapters.sports.adapters.footystats import (
            FootystatsAdapter,
        )

        adapter = FootystatsAdapter(api_key="test-key")
        with (
            patch.object(adapter, "_make_session", return_value=_make_footystats_session_cm()),
            patch.object(adapter, "_get_with_retry", new_callable=AsyncMock, side_effect=RuntimeError("fail")),
            patch.object(adapter, "_emit_fetch_failed"),
            pytest.raises(RuntimeError, match="fail"),
        ):
            await adapter.get_fixture_predictions("2026-03-22")

    @pytest.mark.asyncio
    async def test_parse_match_none_skips(self) -> None:
        """Line 241: _parse_match returns None → item skipped."""
        from instruments_service.reference_data.adapters.sports.adapters.footystats import (
            FootystatsAdapter,
        )

        adapter = FootystatsAdapter(api_key="test-key")
        with (
            patch.object(adapter, "_make_session", return_value=_make_footystats_session_cm()),
            patch.object(adapter, "_get_with_retry", new_callable=AsyncMock, return_value={}),
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.footystats._extract_data",
                return_value=[{"id": "1"}],
            ),
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.footystats._parse_match",
                return_value=None,
            ),
        ):
            result = await adapter.get_fixture_predictions("2026-03-22")
        assert result == []

    @pytest.mark.asyncio
    async def test_normalize_exception_continues(self) -> None:
        """Lines 260-266: normalize_footystats_predictions raises → logged, continues."""
        from instruments_service.reference_data.adapters.sports.adapters.footystats import (
            FootystatsAdapter,
        )

        adapter = FootystatsAdapter(api_key="test-key")
        mock_match = MagicMock()
        mock_match.competition_id = 39
        with (
            patch.object(adapter, "_make_session", return_value=_make_footystats_session_cm()),
            patch.object(adapter, "_get_with_retry", new_callable=AsyncMock, return_value={}),
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.footystats._extract_data",
                return_value=[{"id": "1", "match_id": "1"}],
            ),
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.footystats._parse_match",
                return_value=mock_match,
            ),
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.footystats.normalize_footystats_predictions",
                side_effect=ValueError("bad norm"),
            ),
        ):
            result = await adapter.get_fixture_predictions("2026-03-22")
        assert result == []


class TestFootystatsGetFixtureOddsSnapshotEdgeCases:
    """Lines 294-297, 305, 316-322: exception paths in get_fixture_odds_snapshot."""

    @pytest.mark.asyncio
    async def test_http_exception_reraises(self) -> None:
        """Lines 294-297: HTTP exception re-raises."""
        from instruments_service.reference_data.adapters.sports.adapters.footystats import (
            FootystatsAdapter,
        )

        adapter = FootystatsAdapter(api_key="test-key")
        with (
            patch.object(adapter, "_make_session", return_value=_make_footystats_session_cm()),
            patch.object(adapter, "_get_with_retry", new_callable=AsyncMock, side_effect=RuntimeError("odds fail")),
            patch.object(adapter, "_emit_fetch_failed"),
            pytest.raises(RuntimeError, match="odds fail"),
        ):
            await adapter.get_fixture_odds_snapshot("2026-03-22")

    @pytest.mark.asyncio
    async def test_parse_match_none_skips(self) -> None:
        """Line 305: _parse_match returns None → item skipped."""
        from instruments_service.reference_data.adapters.sports.adapters.footystats import (
            FootystatsAdapter,
        )

        adapter = FootystatsAdapter(api_key="test-key")
        with (
            patch.object(adapter, "_make_session", return_value=_make_footystats_session_cm()),
            patch.object(adapter, "_get_with_retry", new_callable=AsyncMock, return_value={}),
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.footystats._extract_data",
                return_value=[{"id": "1"}],
            ),
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.footystats._parse_match",
                return_value=None,
            ),
        ):
            result = await adapter.get_fixture_odds_snapshot("2026-03-22")
        assert result == []

    @pytest.mark.asyncio
    async def test_normalize_exception_continues(self) -> None:
        """Lines 316-322: normalize_footystats_odds_snapshot raises → continues."""
        from instruments_service.reference_data.adapters.sports.adapters.footystats import (
            FootystatsAdapter,
        )

        adapter = FootystatsAdapter(api_key="test-key")
        mock_match = MagicMock()
        mock_match.competition_id = 39
        with (
            patch.object(adapter, "_make_session", return_value=_make_footystats_session_cm()),
            patch.object(adapter, "_get_with_retry", new_callable=AsyncMock, return_value={}),
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.footystats._extract_data",
                return_value=[{"id": "1", "match_id": "1"}],
            ),
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.footystats._parse_match",
                return_value=mock_match,
            ),
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.footystats.normalize_footystats_odds_snapshot",
                side_effect=ValueError("bad odds"),
            ),
        ):
            result = await adapter.get_fixture_odds_snapshot("2026-03-22")
        assert result == []


class TestFootystatsParseMatchException:
    """Lines 483-485: _parse_match with non-int id → returns None."""

    def test_non_int_match_id_returns_none(self) -> None:
        """Lines 483-485: FootyStatsMatch construction fails on 'not-an-int' → None."""
        from instruments_service.reference_data.adapters.sports.adapters.footystats import _parse_match

        result = _parse_match({"id": "not_an_int"})
        assert result is None

    def test_none_match_id_returns_none(self) -> None:
        """Lines 356-357: match_id is None → returns None."""
        from instruments_service.reference_data.adapters.sports.adapters.footystats import _parse_match

        result = _parse_match({"other_key": "value"})
        assert result is None


class TestApiFootballLiveQuota:
    """Coverage for the live ``/status`` quota read (query, don't hardcode 2026-06-23).

    Covers ``LiveQuota`` / ``_parse_status_body`` / ``_parse_per_minute_limit`` /
    ``ApiFootballAdapter.get_live_quota`` — all credential-free + network-free
    (the HTTP layer is mocked).
    """

    def _adapter(self, api_key: str | None = "test-key"):
        from instruments_service.reference_data.adapters.sports.adapters.api_football import (
            ApiFootballAdapter,
        )

        adapter = ApiFootballAdapter(api_key=api_key)
        # Reset the class-level live-quota cache so tests don't leak into each other
        # (the adapter is a per-VM singleton; the cache is a class attribute).
        type(adapter)._live_quota_cache = None
        type(adapter)._live_quota_ts = 0.0
        return adapter

    def test_parse_status_body_reads_limit_day_and_current(self) -> None:
        from instruments_service.reference_data.adapters.sports.adapters.api_football import (
            _parse_status_body,
        )

        body = {"response": {"requests": {"current": 12345, "limit_day": 300000}}}
        q = _parse_status_body(body, per_minute_limit=1200, fallback_daily=999)
        assert q.live is True
        assert q.per_minute_limit == 1200
        assert q.daily_limit == 300000
        assert q.daily_remaining == 300000 - 12345

    def test_parse_status_body_malformed_falls_back_to_daily_but_stays_live(self) -> None:
        from instruments_service.reference_data.adapters.sports.adapters.api_football import (
            _parse_status_body,
        )

        # No ``requests`` block → daily figures fall back, but read is still LIVE
        # (the per-minute header succeeded).
        q = _parse_status_body({"response": {}}, per_minute_limit=900, fallback_daily=300000)
        assert q.live is True
        assert q.daily_limit == 300000
        assert q.daily_remaining == 300000

    def test_parse_per_minute_limit_reads_header(self) -> None:
        from instruments_service.reference_data.adapters.sports.adapters.api_football import (
            ApiFootballAdapter,
        )

        assert ApiFootballAdapter._parse_per_minute_limit({"X-RateLimit-Limit": "1200"}, 50) == 1200
        # Absent / unparseable header → fallback.
        assert ApiFootballAdapter._parse_per_minute_limit({}, 50) == 50
        assert ApiFootballAdapter._parse_per_minute_limit({"X-RateLimit-Limit": "x"}, 50) == 50
        # Non-positive → fallback.
        assert ApiFootballAdapter._parse_per_minute_limit({"X-RateLimit-Limit": "0"}, 50) == 50

    @pytest.mark.asyncio
    async def test_get_live_quota_no_key_returns_registry_fallback(self) -> None:
        adapter = self._adapter(api_key=None)
        q = await adapter.get_live_quota(fallback_per_minute=1200, fallback_daily=300000)
        assert q.live is False
        assert q.per_minute_limit == 1200
        assert q.daily_limit == 300000
        assert q.daily_remaining == 300000

    @pytest.mark.asyncio
    async def test_get_live_quota_reads_status_live(self) -> None:
        adapter = self._adapter()
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        resp.headers = {"X-RateLimit-Limit": "1200"}
        resp.json = AsyncMock(
            return_value={"response": {"requests": {"current": 100, "limit_day": 300000}}}
        )
        ctx = MagicMock()
        ctx.__aenter__ = AsyncMock(return_value=resp)
        ctx.__aexit__ = AsyncMock(return_value=False)
        session = MagicMock()
        session.get = MagicMock(return_value=ctx)
        session_ctx = MagicMock()
        session_ctx.__aenter__ = AsyncMock(return_value=session)
        session_ctx.__aexit__ = AsyncMock(return_value=False)
        with patch.object(type(adapter), "_make_session", return_value=session_ctx):
            q = await adapter.get_live_quota(fallback_per_minute=900, fallback_daily=450000)
        assert q.live is True
        assert q.per_minute_limit == 1200
        assert q.daily_limit == 300000
        assert q.daily_remaining == 300000 - 100

    @pytest.mark.asyncio
    async def test_get_live_quota_failure_returns_fallback(self) -> None:
        adapter = self._adapter()
        session_ctx = MagicMock()
        session_ctx.__aenter__ = AsyncMock(side_effect=RuntimeError("boom"))
        session_ctx.__aexit__ = AsyncMock(return_value=False)
        with patch.object(type(adapter), "_make_session", return_value=session_ctx):
            q = await adapter.get_live_quota(fallback_per_minute=1200, fallback_daily=300000)
        assert q.live is False
        assert q.daily_remaining == 300000

    @pytest.mark.asyncio
    async def test_get_live_quota_uses_cache_within_ttl(self) -> None:
        from instruments_service.reference_data.adapters.sports.adapters.api_football import (
            LiveQuota,
        )

        adapter = self._adapter()
        import time

        type(adapter)._live_quota_cache = LiveQuota(
            per_minute_limit=1200, daily_limit=300000, daily_remaining=42, live=True
        )
        type(adapter)._live_quota_ts = time.monotonic()
        # No HTTP mock — a cache hit must NOT touch the network.
        q = await adapter.get_live_quota(fallback_per_minute=1, fallback_daily=1)
        assert q.daily_remaining == 42
        assert q.live is True
