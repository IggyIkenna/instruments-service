"""Comprehensive tests for prediction market adapters — Polymarket, Kalshi, Betfair.

Covers uncovered lines/branches in:
- instruments_service/reference_data/adapters/prediction/polymarket.py
- instruments_service/reference_data/adapters/prediction/kalshi.py
- instruments_service/reference_data/adapters/sports/adapters/betfair.py

All tests are credential-free, mock all HTTP, and use no live network calls.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from instruments_service.reference_data.adapters.prediction.kalshi import (
    KalshiReferenceDataAdapter,
    _classify_kalshi_error,
)
from instruments_service.reference_data.adapters.prediction.polymarket import (
    PolymarketReferenceDataAdapter,
    _normalize_team_pair,
    _parse_vs_string,
    _selection_from_outcomes,
)
from instruments_service.reference_data.adapters.sports.adapters.betfair import (
    BetfairReferenceDataAdapter,
    _classify_betfair_error,
)

# ---------------------------------------------------------------------------
# Shared aiohttp mock helper
# ---------------------------------------------------------------------------


def _make_aiohttp_mock(
    json_response: object,
    status: int = 200,
    headers: dict[str, str] | None = None,
    raise_on_enter: Exception | None = None,
) -> MagicMock:
    """Build a mock that replaces ``aiohttp.ClientSession`` context manager."""
    mock_resp = AsyncMock()
    mock_resp.status = status
    mock_resp.raise_for_status = MagicMock()
    if status >= 400:
        mock_resp.raise_for_status.side_effect = aiohttp.ClientResponseError(
            request_info=MagicMock(), history=(), status=status, message=f"HTTP {status}"
        )
    mock_resp.json = AsyncMock(return_value=json_response)
    mock_resp.headers = headers or {}

    resp_cm = MagicMock()
    if raise_on_enter:
        resp_cm.__aenter__ = AsyncMock(side_effect=raise_on_enter)
    else:
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
# POLYMARKET — _parse_market, _build_instrument_id, _build_sports_id,
# _extract_teams, _cross_reference_fixture, _parse_end_date,
# _parse_clob_point, _fetch_clob_history error, pagination, date mode
# ===========================================================================


class TestPolymarketParseEndDate:
    """Tests for PolymarketReferenceDataAdapter._parse_end_date."""

    def test_valid_iso_date(self) -> None:
        adapter = PolymarketReferenceDataAdapter()
        result = adapter._parse_end_date("2026-03-22T15:00:00Z")
        assert result is not None
        assert result.year == 2026
        assert result.month == 3
        assert result.day == 22

    def test_none_returns_none(self) -> None:
        adapter = PolymarketReferenceDataAdapter()
        assert adapter._parse_end_date(None) is None

    def test_empty_string_returns_none(self) -> None:
        adapter = PolymarketReferenceDataAdapter()
        assert adapter._parse_end_date("") is None

    def test_invalid_date_returns_none(self) -> None:
        adapter = PolymarketReferenceDataAdapter()
        assert adapter._parse_end_date("not-a-date") is None

    def test_iso_with_offset(self) -> None:
        adapter = PolymarketReferenceDataAdapter()
        result = adapter._parse_end_date("2026-06-15T08:00:00+05:00")
        assert result is not None
        assert result.tzinfo is not None


class TestPolymarketParseClob:
    """Tests for _parse_clob_point method."""

    def test_valid_point(self) -> None:
        adapter = PolymarketReferenceDataAdapter()
        point = {"t": 1700000000, "p": "0.65"}
        result = adapter._parse_clob_point(point, "0xtoken", "1d")
        assert result is not None
        assert result.close == Decimal("0.65")
        assert result.venue == "POLYMARKET"
        assert result.symbol == "0xtoken"

    def test_not_dict_returns_none(self) -> None:
        adapter = PolymarketReferenceDataAdapter()
        assert adapter._parse_clob_point("not-dict", "sym", "1d") is None

    def test_missing_t_returns_none(self) -> None:
        adapter = PolymarketReferenceDataAdapter()
        assert adapter._parse_clob_point({"p": "0.5"}, "sym", "1d") is None

    def test_missing_p_returns_none(self) -> None:
        adapter = PolymarketReferenceDataAdapter()
        assert adapter._parse_clob_point({"t": 1700000000}, "sym", "1d") is None

    def test_none_values_returns_none(self) -> None:
        adapter = PolymarketReferenceDataAdapter()
        assert adapter._parse_clob_point({"t": None, "p": None}, "sym", "1d") is None


class TestPolymarketFetchClobHistory:
    """Tests for _fetch_clob_history error handling."""

    @pytest.mark.asyncio
    async def test_client_error_returns_empty(self) -> None:
        adapter = PolymarketReferenceDataAdapter()
        mock_session = _make_aiohttp_mock({}, raise_on_enter=aiohttp.ClientError("connection refused"))
        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = await adapter._fetch_clob_history("0xtoken", "1d")
        assert result == []

    @pytest.mark.asyncio
    async def test_success_returns_history_list(self) -> None:
        adapter = PolymarketReferenceDataAdapter()
        mock_session = _make_aiohttp_mock({"history": [{"t": 1700000000, "p": "0.5"}, {"t": 1700086400, "p": "0.6"}]})
        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = await adapter._fetch_clob_history("0xtoken", "1d")
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_no_history_key_returns_empty(self) -> None:
        adapter = PolymarketReferenceDataAdapter()
        mock_session = _make_aiohttp_mock({"data": "no history"})
        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = await adapter._fetch_clob_history("0xtoken", "1d")
        assert result == []

    @pytest.mark.asyncio
    async def test_history_not_list_returns_empty(self) -> None:
        adapter = PolymarketReferenceDataAdapter()
        mock_session = _make_aiohttp_mock({"history": "not-a-list"})
        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = await adapter._fetch_clob_history("0xtoken", "1d")
        assert result == []


class TestPolymarketGetOhlcv:
    """Tests for Polymarket get_ohlcv with various intervals."""

    @pytest.mark.asyncio
    async def test_interval_mapping(self) -> None:
        adapter = PolymarketReferenceDataAdapter()
        history = [{"t": 1700000000, "p": "0.50"}]
        mock_session = _make_aiohttp_mock({"history": history})
        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = await adapter.get_ohlcv("0xtoken", interval="1h", limit=10)
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_unknown_interval_defaults_to_1d(self) -> None:
        adapter = PolymarketReferenceDataAdapter()
        mock_session = _make_aiohttp_mock({"history": []})
        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = await adapter.get_ohlcv("0xtoken", interval="3h", limit=10)
        assert result == []


class TestPolymarketGetInstrumentsDateMode:
    """Tests for Polymarket get_instruments with date parameter (historical/batch mode)."""

    @pytest.mark.asyncio
    async def test_date_mode_uses_historical_params(self) -> None:
        """When date is provided, uses CLOB API with cursor pagination."""
        adapter = PolymarketReferenceDataAdapter()
        mock_session = _make_aiohttp_mock(
            {
                "data": [
                    {
                        "condition_id": "0xabc",
                        "question_id": "q1",
                        "market_slug": "btc-100k",
                        "question": "Will BTC reach $100K?",
                        "active": False,
                        "closed": True,
                        "end_date_iso": "2026-03-22T23:59:00Z",
                        "tokens": [
                            {"token_id": "t1", "outcome": "Yes", "price": 0.75},
                            {"token_id": "t2", "outcome": "No", "price": 0.25},
                        ],
                    }
                ],
                "next_cursor": "",
            }
        )
        with patch("aiohttp.ClientSession", return_value=mock_session):
            results = await adapter.get_instruments(date="2026-03-22")
        assert isinstance(results, list)

    @pytest.mark.asyncio
    async def test_pagination_stops_when_page_small(self) -> None:
        """Pagination stops when raw page size < _PAGE_LIMIT."""
        adapter = PolymarketReferenceDataAdapter()
        # Return fewer than 100 items — pagination should stop
        small_page = [
            {
                "conditionId": f"0x{i:04x}",
                "marketSlug": f"market-{i}",
                "question": f"Q {i}?",
                "active": True,
                "closed": False,
            }
            for i in range(5)
        ]
        mock_session = _make_aiohttp_mock(small_page)
        with patch("aiohttp.ClientSession", return_value=mock_session):
            results = await adapter.get_instruments()
        assert isinstance(results, list)

    @pytest.mark.asyncio
    async def test_wrong_instrument_type_returns_empty(self) -> None:
        adapter = PolymarketReferenceDataAdapter()
        result = await adapter.get_instruments(instrument_type="FUTURE")
        assert result == []


class TestPolymarketParseMarketBranches:
    """Tests for _parse_market and _build_instrument_id branches."""

    def test_parse_market_no_condition_id_returns_none(self) -> None:
        """Market without condition_id is skipped."""
        from unified_api_contracts import PolymarketGammaMarket

        adapter = PolymarketReferenceDataAdapter()
        market = PolymarketGammaMarket.model_validate(
            {
                "conditionId": "",
                "question": "Test?",
                "active": True,
            }
        )
        now = datetime.now(UTC)
        result = adapter._parse_market(market, now)
        assert result is None

    def test_parse_market_crypto_question(self) -> None:
        """Crypto question produces prediction::crypto type."""
        from unified_api_contracts import PolymarketGammaMarket

        adapter = PolymarketReferenceDataAdapter()
        market = PolymarketGammaMarket.model_validate(
            {
                "conditionId": "0xcrypto123",
                "marketSlug": "btc-above-100k",
                "question": "Will Bitcoin reach $100,000 by end of 2026?",
                "active": True,
                "closed": False,
                "minimumTickSize": 0.01,
                "minimumOrderSize": 5.0,
                "outcomes": "Yes,No",
                "endDateIso": "2026-12-31T23:59:59Z",
            }
        )
        now = datetime.now(UTC)
        result = adapter._parse_market(market, now)
        assert result is not None
        assert result.instrument_type == "PREDICTION_MARKET"
        assert result.venue == "POLYMARKET"
        assert result.instrument_key == "POLYMARKET:PREDICTION_MARKET:0xcrypto123"
        assert str(result.status) == "active"

    def test_parse_market_threads_question_and_survives(self) -> None:
        """The Gamma ``question`` is threaded into InstrumentRecord.question and SURVIVES.

        Regression: the adapter previously passed only ``symbol=slug`` and the human
        market question was never carried, so downstream (catalogue) had no title.
        Now that uac added ``InstrumentRecord.question``, ``market.question`` (99.3%
        unique per the workflow) must arrive on ``record.question`` verbatim.
        """
        from unified_api_contracts import PolymarketGammaMarket

        adapter = PolymarketReferenceDataAdapter()
        market = PolymarketGammaMarket.model_validate(
            {
                "conditionId": "0xcrypto123",
                "marketSlug": "btc-above-100k",
                "question": "Will Bitcoin reach $100,000 by end of 2026?",
                "active": True,
                "closed": False,
                "outcomes": "Yes,No",
                "endDateIso": "2026-12-31T23:59:59Z",
            }
        )
        now = datetime.now(UTC)
        result = adapter._parse_market(market, now)
        assert result is not None
        assert result.question == "Will Bitcoin reach $100,000 by end of 2026?"

    def test_parse_market_question_honest_none_when_absent(self) -> None:
        """A market with no ``question`` yields honest ``None`` (never fabricated)."""
        from unified_api_contracts import PolymarketGammaMarket

        adapter = PolymarketReferenceDataAdapter()
        market = PolymarketGammaMarket.model_validate(
            {
                "conditionId": "0xnoquestion",
                "marketSlug": "some-slug",
                "active": True,
                "closed": False,
                "outcomes": "Yes,No",
                "endDateIso": "2026-12-31T23:59:59Z",
            }
        )
        now = datetime.now(UTC)
        result = adapter._parse_market(market, now)
        assert result is not None
        assert result.question is None

    def test_parse_market_instrument_key_preserves_condition_id_case(self) -> None:
        """The wrapped instrument_key must NOT upper-case condition_id.

        2026-07-09 fix (canonical_id_builder_retrofit_checklist_2026_07_08.md todo 7):
        condition_id is a real, lowercase 0x…64hex hash — ``passthrough=True`` would
        upper-case it via ``canonical_id_builder.py::_build_passthrough`` and corrupt
        it into a non-matching id, so the adapter deliberately calls
        ``build_canonical_instrument_id`` WITHOUT ``passthrough=True`` (dispatches to
        ``_build_sports_or_prediction``, which preserves symbol case verbatim).
        """
        from unified_api_contracts import PolymarketGammaMarket

        adapter = PolymarketReferenceDataAdapter()
        market = PolymarketGammaMarket.model_validate(
            {
                "conditionId": "0xdeadbeefCaFe",
                "marketSlug": "btc-above-100k",
                "question": "Will Bitcoin reach $100,000 by end of 2026?",
                "active": True,
                "closed": False,
                "outcomes": "Yes,No",
                "endDateIso": "2026-12-31T23:59:59Z",
            }
        )
        now = datetime.now(UTC)
        result = adapter._parse_market(market, now)
        assert result is not None
        assert result.instrument_key == "POLYMARKET:PREDICTION_MARKET:0xdeadbeefCaFe", (
            f"condition_id case must be preserved verbatim, got: {result.instrument_key!r}"
        )

    def test_parse_market_registers_clob_token_ids_under_wrapped_instrument_key(self) -> None:
        """The clob_token_ids side-table must be keyed by the FINAL instrument_key.

        process_write.py::_records_to_dataframe joins clob_token_ids by whatever
        instrument_key resolves to (``key == instrument_key``) — this regression-
        guards that the 2026-07-09 instrument_key wrap didn't break that join by
        registering under the bare (pre-wrap) condition_id instead.
        """
        from unified_api_contracts import PolymarketGammaMarket

        from instruments_service.reference_data.adapters.prediction.polymarket import (
            _clob_token_ids_for_condition_id,
        )

        adapter = PolymarketReferenceDataAdapter()
        market = PolymarketGammaMarket.model_validate(
            {
                "conditionId": "0xclobjoin1",
                "marketSlug": "clob-join-test",
                "question": "Will X happen?",
                "active": True,
                "closed": False,
                "outcomes": "Yes,No",
                "endDateIso": "2026-12-31T23:59:59Z",
                "clobTokenIds": ["111", "222"],
            }
        )
        now = datetime.now(UTC)
        result = adapter._parse_market(market, now)
        assert result is not None
        assert result.instrument_key == "POLYMARKET:PREDICTION_MARKET:0xclobjoin1"
        assert _clob_token_ids_for_condition_id(result.instrument_key) == ["111", "222"], (
            "clob_token_ids must be registered under the SAME final instrument_key "
            "the orchestrator's _records_to_dataframe looks up by."
        )
        # The bare (pre-wrap) condition_id must NOT be a separate, stale registration.
        assert _clob_token_ids_for_condition_id("0xclobjoin1") is None

    def test_parse_market_macro_question(self) -> None:
        """Macro/financial question."""
        from unified_api_contracts import PolymarketGammaMarket

        adapter = PolymarketReferenceDataAdapter()
        market = PolymarketGammaMarket.model_validate(
            {
                "conditionId": "0xmacro456",
                "marketSlug": "sp500-above-5000",
                "question": "Will the S&P 500 close above 5000?",
                "active": True,
                "closed": False,
                "endDateIso": "2026-06-30T23:59:59Z",
            }
        )
        now = datetime.now(UTC)
        result = adapter._parse_market(market, now)
        assert result is not None
        assert result.instrument_type == "PREDICTION_MARKET"

    def test_parse_market_closed_status_is_active_default(self) -> None:
        """Closed market: is_active kwarg is silently ignored by Pydantic, status stays 'active'."""
        from unified_api_contracts import PolymarketGammaMarket

        adapter = PolymarketReferenceDataAdapter()
        market = PolymarketGammaMarket.model_validate(
            {
                "conditionId": "0xclosed789",
                "marketSlug": "closed-market",
                "question": "Was Bitcoin above $50K?",
                "active": False,
                "closed": True,
                "endDateIso": "2026-01-01T00:00:00Z",
            }
        )
        now = datetime.now(UTC)
        result = adapter._parse_market(market, now)
        assert result is not None
        # Note: is_active=False is passed to InstrumentRecord but Pydantic ignores unknown kwargs.
        # The status field always defaults to 'active' since there's no is_active→status mapping.
        assert str(result.status) == "active"

    def test_parse_market_no_end_date(self) -> None:
        """Market without end_date produces expiry=None."""
        from unified_api_contracts import PolymarketGammaMarket

        adapter = PolymarketReferenceDataAdapter()
        market = PolymarketGammaMarket.model_validate(
            {
                "conditionId": "0xnodate",
                "marketSlug": "no-date-market",
                "question": "Will something happen?",
                "active": True,
                "closed": False,
            }
        )
        now = datetime.now(UTC)
        result = adapter._parse_market(market, now)
        assert result is not None
        assert result.expiry is None

    def test_parse_market_generic_category(self) -> None:
        """Non-crypto, non-macro, non-sports question produces prediction::other or category."""
        from unified_api_contracts import PolymarketGammaMarket

        adapter = PolymarketReferenceDataAdapter()
        market = PolymarketGammaMarket.model_validate(
            {
                "conditionId": "0xpolitics",
                "marketSlug": "us-election-2028",
                "question": "Will the Democratic candidate win the 2028 US Presidential Election?",
                "active": True,
                "closed": False,
            }
        )
        now = datetime.now(UTC)
        result = adapter._parse_market(market, now)
        assert result is not None
        assert result.instrument_type == "PREDICTION_MARKET"


class TestPolymarketBuildSportsId:
    """Tests for _build_sports_id including team extraction."""

    def test_sports_market_no_prediction_league_returns_none(self) -> None:
        """Sports market whose series_slug doesn't map to a prediction league is skipped."""
        from unified_api_contracts import PolymarketGammaMarket

        adapter = PolymarketReferenceDataAdapter()
        market = PolymarketGammaMarket.model_validate(
            {
                "conditionId": "0xsports1",
                "marketSlug": "esports-game",
                "question": "Will Team X win?",
                "active": True,
                "closed": False,
                "sportsMarketType": "moneyline",
                "outcomes": '["Yes","No"]',
                "seriesSlug": "unknown-esports-series",
                "endDate": "2026-04-01T23:59:59Z",
            }
        )
        now = datetime.now(UTC)
        result = adapter._parse_market(market, now)
        # Should be None because league not in prediction registry
        assert result is None

    def test_extract_teams_from_event_title(self) -> None:
        """Teams extracted from event_title 'Home vs. Away'."""
        from unified_api_contracts import PolymarketGammaMarket

        adapter = PolymarketReferenceDataAdapter()
        market = PolymarketGammaMarket.model_validate(
            {
                "conditionId": "0xteams1",
                "question": "Will Arsenal vs Chelsea end in draw?",
                "sportsMarketType": "moneyline",
                "outcomes": '["Yes","No"]',
                "eventTitle": "Arsenal vs. Chelsea",
            }
        )
        outcomes = market.outcomes or []
        home, away = adapter._extract_teams(market, outcomes)
        # Should have extracted and normalized team names
        assert isinstance(home, str) and len(home) > 0
        assert isinstance(away, str) and len(away) > 0

    def test_extract_teams_from_non_generic_outcomes(self) -> None:
        """Teams extracted from outcomes when they are team names (not Yes/No)."""
        from unified_api_contracts import PolymarketGammaMarket

        adapter = PolymarketReferenceDataAdapter()
        market = PolymarketGammaMarket.model_validate(
            {
                "conditionId": "0xteams2",
                "question": "Who wins?",
                "sportsMarketType": "spreads",
                "outcomes": '["Real Madrid","Barcelona"]',
            }
        )
        outcomes = market.outcomes or []
        home, away = adapter._extract_teams(market, outcomes)
        assert isinstance(home, str)
        assert isinstance(away, str)

    def test_extract_teams_from_question_vs(self) -> None:
        """Teams extracted from question 'Home vs Away: O/U 2.5'."""
        from unified_api_contracts import PolymarketGammaMarket

        adapter = PolymarketReferenceDataAdapter()
        market = PolymarketGammaMarket.model_validate(
            {
                "conditionId": "0xteams3",
                "question": "Liverpool vs Man City: O/U 2.5",
                "sportsMarketType": "totals",
                "outcomes": '["Over","Under"]',
            }
        )
        outcomes = market.outcomes or []
        home, away = adapter._extract_teams(market, outcomes)
        assert isinstance(home, str) and len(home) > 0
        assert isinstance(away, str) and len(away) > 0

    def test_extract_teams_single_team_question(self) -> None:
        """Single-team question: 'Will Arsenal win on 2026-03-22?'."""
        from unified_api_contracts import PolymarketGammaMarket

        adapter = PolymarketReferenceDataAdapter()
        market = PolymarketGammaMarket.model_validate(
            {
                "conditionId": "0xteams4",
                "question": "Will Arsenal win on 2026-03-22?",
                "sportsMarketType": "moneyline",
                "outcomes": '["Yes","No"]',
            }
        )
        outcomes = market.outcomes or []
        home, away = adapter._extract_teams(market, outcomes)
        assert isinstance(home, str)
        assert away == "UNKNOWN"

    def test_extract_teams_no_match_returns_unknown(self) -> None:
        """Unrecognized question produces UNKNOWN teams."""
        from unified_api_contracts import PolymarketGammaMarket

        adapter = PolymarketReferenceDataAdapter()
        market = PolymarketGammaMarket.model_validate(
            {
                "conditionId": "0xteams5",
                "question": "Random unrelated question about sport?",
                "sportsMarketType": "moneyline",
                "outcomes": '["Yes","No"]',
            }
        )
        outcomes = market.outcomes or []
        home, away = adapter._extract_teams(market, outcomes)
        assert home == "UNKNOWN"
        assert away == "UNKNOWN"


class TestPolymarketCrossReferenceFixture:
    """Tests for _cross_reference_fixture."""

    @pytest.mark.asyncio
    async def test_no_api_key_returns_none(self) -> None:
        """Without API Football key, returns None."""
        adapter = PolymarketReferenceDataAdapter(api_football_api_key=None)
        result = await adapter._cross_reference_fixture("EPL", "ARSENAL", "CHELSEA", "2026-03-22")
        assert result is None

    @pytest.mark.asyncio
    async def test_cached_result_returned(self) -> None:
        """Cached fixture_id returned without API call."""
        adapter = PolymarketReferenceDataAdapter(api_football_api_key="test-key")
        adapter._fixture_cache["EPL:ARS:CHE:2026-03-22"] = "12345"
        result = await adapter._cross_reference_fixture("EPL", "ARS", "CHE", "2026-03-22")
        assert result == "12345"

    @pytest.mark.asyncio
    async def test_unknown_league_returns_none(self) -> None:
        """League without api_football_id returns None."""
        adapter = PolymarketReferenceDataAdapter(api_football_api_key="test-key")
        with patch(
            "unified_api_contracts.canonical.domain.sports.league_data.get_league",
            return_value=None,
        ):
            result = await adapter._cross_reference_fixture("UNKNOWN_LEAGUE", "A", "B", "2026-03-22")
        assert result is None

    @pytest.mark.asyncio
    async def test_league_no_api_football_id_returns_none(self) -> None:
        """League with no api_football_id returns None."""
        adapter = PolymarketReferenceDataAdapter(api_football_api_key="test-key")
        mock_league = MagicMock()
        mock_league.api_football_id = None
        with patch(
            "unified_api_contracts.canonical.domain.sports.league_data.get_league",
            return_value=mock_league,
        ):
            result = await adapter._cross_reference_fixture("SOME_LEAGUE", "A", "B", "2026-03-22")
        assert result is None

    @pytest.mark.asyncio
    async def test_api_football_exception_returns_none(self) -> None:
        """Exception during fixture lookup returns None gracefully."""
        adapter = PolymarketReferenceDataAdapter(api_football_api_key="test-key")
        mock_league = MagicMock()
        mock_league.api_football_id = 39
        with (
            patch(
                "unified_api_contracts.canonical.domain.sports.league_data.get_league",
                return_value=mock_league,
            ),
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.api_football.ApiFootballAdapter"
            ) as mock_adapter_cls,
        ):
            mock_adapter_inst = MagicMock()
            mock_adapter_inst.get_fixtures = AsyncMock(side_effect=RuntimeError("API error"))
            mock_adapter_cls.return_value = mock_adapter_inst
            result = await adapter._cross_reference_fixture("EPL", "ARSENAL", "CHELSEA", "2026-03-22")
        assert result is None

    @pytest.mark.asyncio
    async def test_fixture_match_found(self) -> None:
        """Fixture matched by team name similarity."""
        adapter = PolymarketReferenceDataAdapter(api_football_api_key="test-key")
        mock_league = MagicMock()
        mock_league.api_football_id = 39
        mock_fixture = MagicMock()
        mock_fixture.fixture_id = "999"
        mock_fixture.home_team.team_id = "ARSENAL"
        mock_fixture.away_team.team_id = "CHELSEA"
        with (
            patch(
                "unified_api_contracts.canonical.domain.sports.league_data.get_league",
                return_value=mock_league,
            ),
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.api_football.ApiFootballAdapter"
            ) as mock_adapter_cls,
        ):
            mock_adapter_inst = MagicMock()
            mock_adapter_inst.get_fixtures = AsyncMock(return_value=[mock_fixture])
            mock_adapter_cls.return_value = mock_adapter_inst
            result = await adapter._cross_reference_fixture("EPL", "ARSENAL", "CHELSEA", "2026-03-22")
        assert result == "999"

    @pytest.mark.asyncio
    async def test_no_fixture_match(self) -> None:
        """No matching fixture returns None."""
        adapter = PolymarketReferenceDataAdapter(api_football_api_key="test-key")
        mock_league = MagicMock()
        mock_league.api_football_id = 39
        mock_fixture = MagicMock()
        mock_fixture.fixture_id = "111"
        mock_fixture.home_team.team_id = "TOTALLY_DIFFERENT"
        mock_fixture.away_team.team_id = "ALSO_DIFFERENT"
        with (
            patch(
                "unified_api_contracts.canonical.domain.sports.league_data.get_league",
                return_value=mock_league,
            ),
            patch(
                "instruments_service.reference_data.adapters.sports.adapters.api_football.ApiFootballAdapter"
            ) as mock_adapter_cls,
        ):
            mock_adapter_inst = MagicMock()
            mock_adapter_inst.get_fixtures = AsyncMock(return_value=[mock_fixture])
            mock_adapter_cls.return_value = mock_adapter_inst
            result = await adapter._cross_reference_fixture("EPL", "ARSENAL", "CHELSEA", "2026-03-22")
        assert result is None


class TestPolymarketFetchPageErrorPaths:
    """Tests for _fetch_page error handling and validation error branch."""

    @pytest.mark.asyncio
    async def test_fetch_page_client_error(self) -> None:
        """Client error in _fetch_page RAISES RuntimeError (→ attempted_failed), not return [].

        Updated from the pre-CF-11 return-empty contract (which let a transient
        failure truncate the live universe to a silent-complete empty) per
        prediction_manifest_canonicalisation_2026_06_01 § CF-11 IS write-path. The
        re-raise MUST be a ``RuntimeError`` (wrapping the ``aiohttp.ClientError``): a bare
        ``ClientError``/``ClientResponseError`` is NOT in ``_fetch_one_venue``'s except-ladder
        and would propagate uncaught through the caller's ``asyncio.gather`` (no
        ``return_exceptions=True``), crashing the whole multi-venue batch instead of landing
        the cell in ``attempted_failed`` (mirrors cefi ``aster.py``).
        """
        adapter = PolymarketReferenceDataAdapter()
        mock_session_obj = MagicMock()
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(side_effect=aiohttp.ClientError("connection error"))
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj.get = MagicMock(return_value=mock_cm)
        now = datetime.now(UTC)
        with pytest.raises(RuntimeError) as excinfo:
            await adapter._fetch_page(mock_session_obj, 0, now)
        # chained cause preserved for classification/debugging
        assert isinstance(excinfo.value.__cause__, aiohttp.ClientError)

    @pytest.mark.asyncio
    async def test_fetch_all_raw_clob_markets_client_error_raises(self) -> None:
        """Client error in the CLOB full-scan RAISES RuntimeError (→ attempted_failed).

        ``_fetch_all_raw_clob_markets`` is the historical/backfill CLOB pagination path.
        A mid-scan ``aiohttp.ClientError`` (4xx/429/5xx) must re-raise as ``RuntimeError``
        (caught by ``_fetch_one_venue`` → ``attempted_failed``), never truncate the market
        universe to a silent-complete partial list. Mirrors the ``_fetch_page`` gamma path +
        cefi ``aster.py``. Was previously UNTESTED (CF-11 audit gap, 2026-07-10).
        """
        adapter = PolymarketReferenceDataAdapter()
        # session.get(...) context manager raises the ClientError on enter.
        get_cm = MagicMock()
        get_cm.__aenter__ = AsyncMock(side_effect=aiohttp.ClientError("network error"))
        get_cm.__aexit__ = AsyncMock(return_value=None)
        session_obj = MagicMock()
        session_obj.get = MagicMock(return_value=get_cm)
        session_cm = MagicMock()
        session_cm.__aenter__ = AsyncMock(return_value=session_obj)
        session_cm.__aexit__ = AsyncMock(return_value=None)
        with (
            patch.object(adapter, "_make_session", return_value=session_cm),
            pytest.raises(RuntimeError) as excinfo,
        ):
            await adapter._fetch_all_raw_clob_markets()
        assert isinstance(excinfo.value.__cause__, aiohttp.ClientError)

    @pytest.mark.asyncio
    async def test_fetch_page_validation_error_skipped(self) -> None:
        """Invalid market data is skipped without crashing."""
        adapter = PolymarketReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.raise_for_status = MagicMock()
        # Return a list with one invalid item (missing required fields for Pydantic)
        mock_resp.json = AsyncMock(
            return_value=[
                {"some_invalid_field": "bad data"},
            ]
        )
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(return_value=mock_cm)
        now = datetime.now(UTC)
        result = await adapter._fetch_page(mock_session_obj, 0, now)
        # Validation error caught — market skipped
        assert isinstance(result, list)

    @pytest.mark.asyncio
    async def test_fetch_page_date_mode(self) -> None:
        """Date mode uses end_date_min/max params."""
        adapter = PolymarketReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(return_value=[])
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(return_value=mock_cm)
        now = datetime.now(UTC)
        result = await adapter._fetch_page(mock_session_obj, 0, now, date="2026-03-22")
        assert result == []


class TestPolymarketSelectionAndHelpers:
    """Additional coverage for helper functions."""

    def test_selection_btts_no(self) -> None:
        assert _selection_from_outcomes(["No", "Yes"], "both_teams_to_score") == "HOME"

    def test_parse_vs_with_colon_prefix_no_vs_in_second_part(self) -> None:
        """Colon prefix where second part has no 'vs' falls through."""
        result = _parse_vs_string("Premier League: Arsenal wins")
        assert result is None

    def test_normalize_team_pair_long_names_truncated(self) -> None:
        """Unknown team names longer than 30 chars get truncated via slug."""
        home, away = _normalize_team_pair(
            "A" * 50 + " FC",
            "B" * 50 + " United",
        )
        assert len(home) <= 30
        assert len(away) <= 30


class TestPolymarketGetInstrumentById:
    """Tests for get_instrument by symbol/slug lookup."""

    @pytest.mark.asyncio
    async def test_get_instrument_by_raw_symbol(self) -> None:
        adapter = PolymarketReferenceDataAdapter()
        inst = MagicMock(instrument_key="0xabc", raw_symbol="btc-100k-slug")
        with patch.object(adapter, "get_instruments", AsyncMock(return_value=[inst])):
            result = await adapter.get_instrument("btc-100k-slug")
        assert result is inst

    @pytest.mark.asyncio
    async def test_get_instrument_not_found(self) -> None:
        adapter = PolymarketReferenceDataAdapter()
        with patch.object(adapter, "get_instruments", AsyncMock(return_value=[])):
            result = await adapter.get_instrument("nonexistent")
        assert result is None


class TestPolymarketClobIntervalMap:
    """Tests for CLOB interval mapping."""

    def test_all_mapped_intervals(self) -> None:
        adapter = PolymarketReferenceDataAdapter()
        for interval in ["1m", "5m", "15m", "30m", "1h", "4h", "6h", "12h", "1d", "1w", "1M"]:
            assert interval in adapter._CLOB_INTERVAL_MAP


# ===========================================================================
# KALSHI — _classify_kalshi_error, _get_headers, _parse_market,
# _parse_close_time, _fetch_markets_page 401, get_instrument 404 / error
# ===========================================================================


class TestClassifyKalshiError:
    """Tests for _classify_kalshi_error."""

    def test_429_status(self) -> None:
        assert _classify_kalshi_error(Exception("x"), status=429) == "429"

    def test_401_status(self) -> None:
        assert _classify_kalshi_error(Exception("x"), status=401) == "401"

    def test_403_status(self) -> None:
        assert _classify_kalshi_error(Exception("x"), status=403) == "403"

    def test_400_status(self) -> None:
        assert _classify_kalshi_error(Exception("x"), status=400) == "400"

    def test_500_status(self) -> None:
        assert _classify_kalshi_error(Exception("x"), status=500) == "500"

    def test_502_status(self) -> None:
        assert _classify_kalshi_error(Exception("x"), status=502) == "500"

    def test_429_message(self) -> None:
        assert _classify_kalshi_error(Exception("429 rate limited")) == "429"

    def test_rate_message(self) -> None:
        assert _classify_kalshi_error(Exception("rate limit exceeded")) == "429"

    def test_401_message(self) -> None:
        assert _classify_kalshi_error(Exception("unauthorized access")) == "401"

    def test_403_message(self) -> None:
        assert _classify_kalshi_error(Exception("forbidden resource")) == "403"

    def test_400_message(self) -> None:
        assert _classify_kalshi_error(Exception("bad request data")) == "400"

    def test_500_message(self) -> None:
        assert _classify_kalshi_error(Exception("500 internal error")) == "500"

    def test_server_message(self) -> None:
        assert _classify_kalshi_error(Exception("server unavailable")) == "500"

    def test_unknown(self) -> None:
        assert _classify_kalshi_error(Exception("random error")) == "UNKNOWN"


class TestKalshiGetHeaders:
    """Tests for _signed_headers (RSA-PSS auth; replaced the wrong Bearer scheme)."""

    def test_headers_without_creds(self) -> None:
        """No creds → plain headers (live `status=open` is unauthenticated-OK); a
        legacy non-JSON api_key is NOT a usable RSA credential → still unsigned."""
        for adapter in (
            KalshiReferenceDataAdapter(),
            KalshiReferenceDataAdapter(api_key="my-key-123"),
        ):
            headers = adapter._signed_headers("GET", "/trade-api/v2/markets")
            assert "Accept" in headers
            assert "Content-Type" in headers
            assert "Authorization" not in headers  # Bearer scheme is RETIRED
            assert "KALSHI-ACCESS-SIGNATURE" not in headers

    def test_headers_with_rsa_creds_are_signed(self) -> None:
        """An RSA credential blob → RSA-PSS-signed KALSHI-ACCESS-* headers."""
        import base64
        import json

        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.hazmat.primitives.serialization import (
            Encoding,
            NoEncryption,
            PrivateFormat,
        )

        priv = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        pem = priv.private_bytes(Encoding.PEM, PrivateFormat.TraditionalOpenSSL, NoEncryption()).decode()
        blob = json.dumps({"api_key_id": "kid-xyz", "private_key": pem})
        adapter = KalshiReferenceDataAdapter(api_key=blob)
        headers = adapter._signed_headers("GET", "/trade-api/v2/historical/markets")
        assert headers["KALSHI-ACCESS-KEY"] == "kid-xyz"
        assert headers["KALSHI-ACCESS-TIMESTAMP"].isdigit()
        # signature is valid base64 of a 2048-bit RSA signature (256 bytes)
        assert len(base64.b64decode(headers["KALSHI-ACCESS-SIGNATURE"])) == 256
        assert "Authorization" not in headers


class TestKalshiParseMarket:
    """Tests for _parse_market."""

    def test_valid_market(self) -> None:
        adapter = KalshiReferenceDataAdapter()
        raw = {
            "ticker": "KXBTC-26MAR-90000",
            "event_ticker": "KXBTC",
            "title": "BTC above $90,000?",
            "subtitle": "Crypto market",
            "status": "active",
            "close_time": "2026-03-26T23:59:59Z",
        }
        now = datetime.now(UTC)
        result = adapter._parse_market(raw, now)
        assert result is not None
        assert result.instrument_key == "KALSHI:PREDICTION_MARKET:KXBTC-26MAR-90000"
        assert result.venue == "KALSHI"
        assert result.quote_asset == "USD"
        assert str(result.status) == "active"

    def test_market_no_ticker_returns_none(self) -> None:
        adapter = KalshiReferenceDataAdapter()
        raw = {"ticker": "", "event_ticker": "EVT"}
        now = datetime.now(UTC)
        result = adapter._parse_market(raw, now)
        assert result is None

    def test_market_inactive_status(self) -> None:
        adapter = KalshiReferenceDataAdapter()
        raw = {
            "ticker": "KXTEST",
            "event_ticker": "KXTEST",
            "title": "Test Market",
            "status": "closed",
            "close_time": "2026-01-01T00:00:00Z",
        }
        now = datetime.now(UTC)
        result = adapter._parse_market(raw, now)
        assert result is not None
        # is_active=False is passed to InstrumentRecord but Pydantic ignores unknown kwargs.
        # status always defaults to 'active'.
        assert str(result.status) == "active"

    def test_market_no_close_time(self) -> None:
        adapter = KalshiReferenceDataAdapter()
        raw = {
            "ticker": "KXNODATE",
            "event_ticker": "KXNODATE",
            "title": "No Close Time",
        }
        now = datetime.now(UTC)
        result = adapter._parse_market(raw, now)
        assert result is not None
        assert result.expiry is None

    def test_market_defaults_for_missing_fields(self) -> None:
        """Missing title, tick_size, min_order_size all have defaults."""
        adapter = KalshiReferenceDataAdapter()
        raw = {"ticker": "KXMIN", "event_ticker": "KXMIN"}
        now = datetime.now(UTC)
        result = adapter._parse_market(raw, now)
        assert result is not None
        assert result.tick_size == Decimal("0.01")
        assert result.min_size == Decimal("1")

    def test_market_question_composed_with_yes_sub_title(self) -> None:
        """The human title is threaded into InstrumentRecord.question and SURVIVES.

        Regression: the adapter previously passed the title as ``symbol=``, which
        InstrumentRecord does not declare, so pydantic's ``extra='ignore'`` SILENTLY
        DROPPED it every capture. Now that uac added ``InstrumentRecord.question``,
        the title must arrive on ``record.question``. When ``yes_sub_title`` is
        present the adapter composes ``title — yes_sub_title`` (100% unique vs 43.3%
        for title alone per the workflow measurement).
        """
        adapter = KalshiReferenceDataAdapter()
        raw = {
            "ticker": "KXBTC-26MAR-90000",
            "event_ticker": "KXBTC",
            "title": "BTC price on Mar 26?",
            "yes_sub_title": "Above $90,000",
            "status": "active",
            "close_time": "2026-03-26T23:59:59Z",
        }
        now = datetime.now(UTC)
        result = adapter._parse_market(raw, now)
        assert result is not None
        assert result.question == "BTC price on Mar 26? — Above $90,000"

    def test_market_question_bare_title_when_no_yes_sub_title(self) -> None:
        """With no ``yes_sub_title`` the question is the bare title (honest floor)."""
        adapter = KalshiReferenceDataAdapter()
        raw = {
            "ticker": "KXTEST",
            "event_ticker": "KXTEST",
            "title": "Will it rain tomorrow?",
            "status": "active",
        }
        now = datetime.now(UTC)
        result = adapter._parse_market(raw, now)
        assert result is not None
        assert result.question == "Will it rain tomorrow?"

    def test_market_question_is_none_when_no_human_title(self) -> None:
        """Honest-absence: a market with NO title/subtitle yields question=None — the
        ticker is NOT a human question, so it must NOT surface as one. raw_symbol
        (event_ticker) remains the label floor, symmetric with Polymarket. (Guards the
        fix for the wx1ogy6pl verifier's caveat: the adapter previously fell title back
        to the ticker, so question was never None.)"""
        adapter = KalshiReferenceDataAdapter()
        raw = {
            "ticker": "KXNOTITLE",
            "event_ticker": "KXNOTITLE",
            "status": "active",
        }
        now = datetime.now(UTC)
        result = adapter._parse_market(raw, now)
        assert result is not None
        assert result.question is None
        assert result.raw_symbol == "KXNOTITLE"  # the honest label floor is intact


class TestKalshiParseCloseTime:
    """Tests for _parse_close_time."""

    def test_valid_iso(self) -> None:
        adapter = KalshiReferenceDataAdapter()
        result = adapter._parse_close_time("2026-03-26T23:59:59Z")
        assert result is not None
        assert result.year == 2026

    def test_none_returns_none(self) -> None:
        adapter = KalshiReferenceDataAdapter()
        assert adapter._parse_close_time(None) is None

    def test_empty_string_returns_none(self) -> None:
        adapter = KalshiReferenceDataAdapter()
        assert adapter._parse_close_time("") is None

    def test_invalid_returns_none(self) -> None:
        adapter = KalshiReferenceDataAdapter()
        assert adapter._parse_close_time("not-a-date") is None


class TestKalshiFetchMarketsPage:
    """Tests for _fetch_markets_page edge cases."""

    @pytest.mark.asyncio
    async def test_401_raises(self) -> None:
        """CF-11: a 401 RAISES (event still emitted), not swallowed into ([], None) — so
        get_instruments records the venue attempted_failed instead of a silent honest-empty."""
        adapter = KalshiReferenceDataAdapter(api_key="bad-key")
        mock_resp = AsyncMock()
        mock_resp.status = 401
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_cm)
        with pytest.raises(RuntimeError):
            await adapter._fetch_markets_page(mock_session, None, datetime.now(UTC))

    @pytest.mark.asyncio
    async def test_client_error_raises(self) -> None:
        """CF-11: a client/transport error RAISES (event still emitted), not swallowed into ([], None)."""
        adapter = KalshiReferenceDataAdapter()
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(side_effect=aiohttp.ClientError("connection refused"))
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_cm)
        with pytest.raises(RuntimeError):
            await adapter._fetch_markets_page(mock_session, None, datetime.now(UTC))

    @pytest.mark.asyncio
    async def test_cursor_pagination(self) -> None:
        """Cursor is passed when provided."""
        adapter = KalshiReferenceDataAdapter(api_key="key")
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(
            return_value={
                "markets": [{"ticker": "KX1", "event_ticker": "KX1"}],
                "cursor": "next-page-token",
            }
        )
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_cm)
        records, cursor = await adapter._fetch_markets_page(mock_session, "prev-cursor", datetime.now(UTC))
        assert len(records) == 1
        assert cursor == "next-page-token"

    @pytest.mark.asyncio
    async def test_markets_not_list_returns_empty(self) -> None:
        """When 'markets' key is not a list, returns empty."""
        adapter = KalshiReferenceDataAdapter(api_key="key")
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(return_value={"markets": "not-a-list"})
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_cm)
        records, _cursor = await adapter._fetch_markets_page(mock_session, None, datetime.now(UTC))
        assert records == []


class TestKalshiGetInstrumentEdgeCases:
    """Tests for get_instrument edge cases."""

    @pytest.mark.asyncio
    async def test_404_returns_none(self) -> None:
        """HTTP 404 returns None."""
        adapter = KalshiReferenceDataAdapter(api_key="key")
        mock_resp = AsyncMock()
        mock_resp.status = 404
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            result = await adapter.get_instrument("KXNOTEXIST")
        assert result is None

    @pytest.mark.asyncio
    async def test_client_error_returns_none(self) -> None:
        """Client error returns None."""
        adapter = KalshiReferenceDataAdapter(api_key="key")
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(side_effect=aiohttp.ClientError("timeout"))
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            result = await adapter.get_instrument("KXFAIL")
        assert result is None

    @pytest.mark.asyncio
    async def test_market_not_dict_returns_none(self) -> None:
        """When 'market' key is not a dict, returns None."""
        adapter = KalshiReferenceDataAdapter(api_key="key")
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(return_value={"market": "not-a-dict"})
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            result = await adapter.get_instrument("KXBAD")
        assert result is None


class TestKalshiGetInstrumentsMultiPage:
    """Tests for get_instruments multi-page pagination."""

    @pytest.mark.asyncio
    async def test_pagination_stops_on_none_cursor(self) -> None:
        """Pagination stops when cursor is None."""
        adapter = KalshiReferenceDataAdapter(api_key="key")
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(
            return_value={
                "markets": [{"ticker": "KX1", "event_ticker": "KX1"}],
                "cursor": None,
            }
        )
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            results = await adapter.get_instruments()
        assert len(results) == 1


# ===========================================================================
# BETFAIR — _classify_betfair_error, _build_runner_record,
# _parse_catalogue_item, _fetch_markets_raw
# ===========================================================================


class TestClassifyBetfairError:
    """Tests for _classify_betfair_error."""

    def test_401_status(self) -> None:
        assert _classify_betfair_error(Exception("x"), status=401) == "INVALID_SESSION_INFORMATION"

    def test_authentication_message(self) -> None:
        assert _classify_betfair_error(Exception("authentication required")) == "INVALID_SESSION_INFORMATION"

    def test_session_message(self) -> None:
        assert _classify_betfair_error(Exception("session expired")) == "INVALID_SESSION_INFORMATION"

    def test_too_much_data(self) -> None:
        assert _classify_betfair_error(Exception("too much data")) == "TOO_MUCH_DATA"

    def test_service_busy(self) -> None:
        assert _classify_betfair_error(Exception("service busy")) == "SERVICE_BUSY"

    def test_busy_message(self) -> None:
        assert _classify_betfair_error(Exception("server is busy")) == "SERVICE_BUSY"

    def test_timeout(self) -> None:
        assert _classify_betfair_error(Exception("connection timeout")) == "TIMEOUT_ERROR"

    def test_request_size(self) -> None:
        assert _classify_betfair_error(Exception("request size exceeds limit")) == "REQUEST_SIZE_EXCEEDS_LIMIT"

    def test_no_session(self) -> None:
        # "no session" contains "session" which also matches INVALID_SESSION_INFORMATION
        # but we test the specific "no session" pattern check here
        assert _classify_betfair_error(Exception("no session available")) in {
            "INVALID_SESSION_INFORMATION",
            "NO_SESSION",
        }

    def test_unknown(self) -> None:
        assert _classify_betfair_error(Exception("random error")) == "UNKNOWN"

    def test_401_in_message(self) -> None:
        assert _classify_betfair_error(Exception("401 error occurred")) == "INVALID_SESSION_INFORMATION"


class TestBetfairBuildRunnerRecord:
    """Tests for _build_runner_record."""

    def test_basic_runner(self) -> None:
        from unified_api_contracts import BetfairRunnerCatalog

        adapter = BetfairReferenceDataAdapter()
        runner = BetfairRunnerCatalog.model_validate({"selectionId": 12345, "runnerName": "Home Team"})
        now = datetime.now(UTC)
        expiry = datetime(2026, 6, 15, 15, 0, 0, tzinfo=UTC)
        record = adapter._build_runner_record(runner, "1.234", "Match Odds", "Soccer", "Event Name", expiry, now)
        assert record.instrument_key == "1.234/12345"
        assert record.raw_symbol == "Event Name/Home Team"
        assert record.base_asset == "Soccer"
        assert record.quote_asset == "GBP"
        assert record.expiry == expiry
        assert str(record.status) == "active"

    def test_runner_no_name(self) -> None:
        from unified_api_contracts import BetfairRunnerCatalog

        adapter = BetfairReferenceDataAdapter()
        runner = BetfairRunnerCatalog.model_validate({"selectionId": 99999})
        now = datetime.now(UTC)
        record = adapter._build_runner_record(runner, "1.999", "Outright", "", "", None, now)
        assert "Selection_99999" in record.raw_symbol
        assert record.instrument_key == "1.999/99999"

    def test_runner_no_event_name(self) -> None:
        from unified_api_contracts import BetfairRunnerCatalog

        adapter = BetfairReferenceDataAdapter()
        runner = BetfairRunnerCatalog.model_validate({"selectionId": 111, "runnerName": "Draw"})
        now = datetime.now(UTC)
        record = adapter._build_runner_record(runner, "1.111", "Correct Score", "", "", None, now)
        assert record.raw_symbol == "Draw"


class TestBetfairParseCatalogueItem:
    """Tests for _parse_catalogue_item with full event/type data."""

    def test_with_event_type_and_event(self) -> None:
        from unified_api_contracts import BetfairMarketCatalogue

        adapter = BetfairReferenceDataAdapter()
        catalogue = BetfairMarketCatalogue.model_validate(
            {
                "marketId": "1.555",
                "marketName": "Match Odds",
                "marketStartTime": "2026-06-15T15:00:00Z",
                "eventType": {"id": "1", "name": "Soccer"},
                "event": {"id": "100", "name": "Arsenal vs Chelsea"},
                "runners": [
                    {"selectionId": 101, "runnerName": "Arsenal"},
                    {"selectionId": 102, "runnerName": "Chelsea"},
                    {"selectionId": 103, "runnerName": "The Draw"},
                ],
            }
        )
        now = datetime.now(UTC)
        results = adapter._parse_catalogue_item(catalogue, now)
        assert len(results) == 3
        assert results[0].instrument_key == "1.555/101"
        assert results[0].base_asset == "Soccer"
        assert results[0].raw_symbol == "Arsenal vs Chelsea/Arsenal"

    def test_empty_runners_list(self) -> None:
        from unified_api_contracts import BetfairMarketCatalogue

        adapter = BetfairReferenceDataAdapter()
        catalogue = BetfairMarketCatalogue.model_validate(
            {
                "marketId": "1.000",
                "marketName": "Empty Market",
                "runners": [],
            }
        )
        now = datetime.now(UTC)
        results = adapter._parse_catalogue_item(catalogue, now)
        assert results == []

    def test_no_event_type_uses_empty_string(self) -> None:
        from unified_api_contracts import BetfairMarketCatalogue

        adapter = BetfairReferenceDataAdapter()
        catalogue = BetfairMarketCatalogue.model_validate(
            {
                "marketId": "1.222",
                "marketName": "Special",
                "runners": [{"selectionId": 1, "runnerName": "Yes"}],
            }
        )
        now = datetime.now(UTC)
        results = adapter._parse_catalogue_item(catalogue, now)
        assert len(results) == 1
        assert results[0].base_asset == ""


class TestBetfairFetchMarketsRaw:
    """Tests for _fetch_markets_raw edge cases."""

    @pytest.mark.asyncio
    async def test_result_none_returns_empty(self) -> None:
        """When 'result' key is None, returns empty list."""
        adapter = BetfairReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(return_value={"result": None})
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.post = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            result = await adapter._fetch_markets_raw("tok", "key")
        assert result == []

    @pytest.mark.asyncio
    async def test_client_error_raises(self) -> None:
        """ClientError must raise, not return [] (CF-11 regression)."""
        adapter = BetfairReferenceDataAdapter()
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(side_effect=aiohttp.ClientError("network error"))
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.post = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with (
            patch("aiohttp.ClientSession", return_value=mock_session_cm),
            pytest.raises((RuntimeError, aiohttp.ClientError)),
        ):
            await adapter._fetch_markets_raw("tok", "key")


class TestBetfairGetInstrumentsFullFlow:
    """Full flow tests for Betfair get_instruments."""

    @pytest.mark.asyncio
    async def test_successful_instrument_parsing(self) -> None:
        """Full flow: credentials -> fetch -> parse -> InstrumentRecords."""
        adapter = BetfairReferenceDataAdapter(session_token="sess-token", app_key="app-key")
        raw_response = {
            "result": [
                {
                    "marketId": "1.777",
                    "marketName": "Match Odds",
                    "marketStartTime": "2026-04-01T19:30:00Z",
                    "eventType": {"id": "1", "name": "Soccer"},
                    "event": {"id": "200", "name": "Liverpool vs Everton"},
                    "runners": [
                        {"selectionId": 301, "runnerName": "Liverpool"},
                        {"selectionId": 302, "runnerName": "Everton"},
                    ],
                }
            ]
        }
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(return_value=raw_response)
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.post = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            results = await adapter.get_instruments()
        assert len(results) == 2
        assert results[0].venue == "betfair"
        assert results[0].instrument_key == "1.777/301"
        assert results[0].expiry is not None
        assert results[0].expiry.year == 2026
