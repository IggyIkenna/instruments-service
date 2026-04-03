"""Unit tests for Polymarket adapter pure-function helpers.

Covers: _match_crypto_asset, _match_macro_index, _parse_vs_string,
_parse_single_team_question, _normalize_team_pair, _selection_from_outcomes,
_get_first_event, _enrich_raw_event_fields, _extract_series_slug,
_classify_polymarket_error.
"""

from __future__ import annotations

from instruments_service.reference_data.adapters.polymarket import (
    _classify_polymarket_error,
    _enrich_raw_event_fields,
    _extract_series_slug,
    _get_first_event,
    _match_crypto_asset,
    _match_macro_index,
    _normalize_team_pair,
    _parse_single_team_question,
    _parse_vs_string,
    _selection_from_outcomes,
)

# ---------------------------------------------------------------------------
# _match_crypto_asset
# ---------------------------------------------------------------------------


class TestMatchCryptoAsset:
    def test_bitcoin(self) -> None:
        assert _match_crypto_asset("will bitcoin reach $100k?") == "BTC"

    def test_btc(self) -> None:
        assert _match_crypto_asset("btc price above $50k") == "BTC"

    def test_ethereum(self) -> None:
        assert _match_crypto_asset("ethereum staking yield") == "ETH"

    def test_eth(self) -> None:
        assert _match_crypto_asset("will eth reach $5000?") == "ETH"

    def test_solana(self) -> None:
        assert _match_crypto_asset("solana tps prediction") == "SOL"

    def test_sol(self) -> None:
        assert _match_crypto_asset("sol price by end of year") == "SOL"

    def test_xrp(self) -> None:
        assert _match_crypto_asset("xrp lawsuit outcome") == "XRP"

    def test_dogecoin(self) -> None:
        assert _match_crypto_asset("dogecoin to the moon?") == "DOGE"

    def test_doge(self) -> None:
        assert _match_crypto_asset("will doge reach $1?") == "DOGE"

    def test_bnb(self) -> None:
        assert _match_crypto_asset("bnb burn schedule") == "BNB"

    def test_hyperliquid(self) -> None:
        assert _match_crypto_asset("hyperliquid volume") == "HYPE"

    def test_hype(self) -> None:
        assert _match_crypto_asset("hype token price") == "HYPE"

    def test_no_match(self) -> None:
        assert _match_crypto_asset("us election 2028") is None

    def test_empty_string(self) -> None:
        assert _match_crypto_asset("") is None


# ---------------------------------------------------------------------------
# _match_macro_index
# ---------------------------------------------------------------------------


class TestMatchMacroIndex:
    def test_sp500(self) -> None:
        assert _match_macro_index("s&p 500 above 5000?") == "SPX"

    def test_spx(self) -> None:
        assert _match_macro_index("closing price (spx) 2026-03-22") == "SPX"

    def test_nasdaq(self) -> None:
        assert _match_macro_index("nasdaq composite prediction") == "NDX"

    def test_dow_jones(self) -> None:
        assert _match_macro_index("dow jones above 40000") == "DJIA"

    def test_crude_oil(self) -> None:
        assert _match_macro_index("crude oil price $80?") == "CRUDE_OIL"

    def test_gold(self) -> None:
        assert _match_macro_index("gold (gc) above $2000?") == "GOLD"

    def test_silver(self) -> None:
        assert _match_macro_index("silver (si) above $30?") == "SILVER"

    def test_no_match(self) -> None:
        assert _match_macro_index("random question") is None


# ---------------------------------------------------------------------------
# _parse_vs_string
# ---------------------------------------------------------------------------


class TestParseVsString:
    def test_simple_vs(self) -> None:
        result = _parse_vs_string("Arsenal vs Chelsea")
        assert result == ("Arsenal", "Chelsea")

    def test_vs_with_period(self) -> None:
        result = _parse_vs_string("Arsenal vs. Chelsea")
        assert result == ("Arsenal", "Chelsea")

    def test_more_markets_suffix(self) -> None:
        result = _parse_vs_string("Arsenal vs. Chelsea - More Markets")
        assert result == ("Arsenal", "Chelsea")

    def test_will_prefix(self) -> None:
        result = _parse_vs_string("Will Arsenal vs. Chelsea end in a draw?")
        assert result is not None
        assert result[0] == "Arsenal"
        assert result[1] == "Chelsea"

    def test_league_prefix(self) -> None:
        result = _parse_vs_string("Super Rugby Pacific: Blues vs Moana Pasifika")
        assert result is not None
        assert result[0] == "Blues"
        assert result[1] == "Moana Pasifika"

    def test_no_vs(self) -> None:
        result = _parse_vs_string("Will Arsenal win?")
        assert result is None

    def test_empty(self) -> None:
        result = _parse_vs_string("")
        assert result is None


# ---------------------------------------------------------------------------
# _parse_single_team_question
# ---------------------------------------------------------------------------


class TestParseSingleTeamQuestion:
    def test_will_win(self) -> None:
        result = _parse_single_team_question("Will Arsenal win on 2026-03-22?")
        assert result == "Arsenal"

    def test_will_lose(self) -> None:
        result = _parse_single_team_question("Will Chelsea lose on 2026-03-22?")
        assert result == "Chelsea"

    def test_will_draw(self) -> None:
        result = _parse_single_team_question("Will Liverpool draw against Man City?")
        assert result == "Liverpool"

    def test_no_match(self) -> None:
        result = _parse_single_team_question("Bitcoin above $100k?")
        assert result is None

    def test_empty(self) -> None:
        result = _parse_single_team_question("")
        assert result is None


# ---------------------------------------------------------------------------
# _normalize_team_pair
# ---------------------------------------------------------------------------


class TestNormalizeTeamPair:
    def test_normalizes_teams(self) -> None:
        # Should return (home, away) as canonical or slugged versions
        home, away = _normalize_team_pair("Arsenal", "Chelsea")
        assert isinstance(home, str) and len(home) > 0
        assert isinstance(away, str) and len(away) > 0

    def test_unknown_teams_slugged(self) -> None:
        home, away = _normalize_team_pair("Totally Unknown FC", "Nonexistent FC")
        assert isinstance(home, str) and len(home) > 0
        assert isinstance(away, str) and len(away) > 0


# ---------------------------------------------------------------------------
# _selection_from_outcomes
# ---------------------------------------------------------------------------


class TestSelectionFromOutcomes:
    def test_empty_outcomes(self) -> None:
        assert _selection_from_outcomes([], "moneyline") == "HOME"

    def test_over_outcome(self) -> None:
        assert _selection_from_outcomes(["Over", "Under"], "totals") == "OVER"

    def test_under_outcome(self) -> None:
        assert _selection_from_outcomes(["Under", "Over"], "totals") == "UNDER"

    def test_totals_yes(self) -> None:
        assert _selection_from_outcomes(["Yes", "No"], "totals") == "OVER"

    def test_btts_yes(self) -> None:
        assert _selection_from_outcomes(["Yes", "No"], "both_teams_to_score") == "OVER"

    def test_totals_no(self) -> None:
        assert _selection_from_outcomes(["No", "Yes"], "totals") == "HOME"

    def test_moneyline_yes(self) -> None:
        assert _selection_from_outcomes(["Yes", "No"], "moneyline") == "HOME"

    def test_spreads_team_name(self) -> None:
        assert _selection_from_outcomes(["Arsenal", "Chelsea"], "spreads") == "HOME"


# ---------------------------------------------------------------------------
# _get_first_event
# ---------------------------------------------------------------------------


class TestGetFirstEvent:
    def test_valid(self) -> None:
        raw = {"events": [{"title": "Arsenal vs Chelsea"}]}
        result = _get_first_event(raw)
        assert result is not None
        assert result["title"] == "Arsenal vs Chelsea"

    def test_no_events_key(self) -> None:
        raw = {"data": "something"}
        result = _get_first_event(raw)
        assert result is None

    def test_empty_events(self) -> None:
        raw = {"events": []}
        result = _get_first_event(raw)
        assert result is None

    def test_events_not_list(self) -> None:
        raw = {"events": "not_a_list"}
        result = _get_first_event(raw)
        assert result is None

    def test_not_dict(self) -> None:
        result = _get_first_event("not_a_dict")
        assert result is None

    def test_first_event_not_dict(self) -> None:
        raw = {"events": ["not_a_dict"]}
        result = _get_first_event(raw)
        assert result is None


# ---------------------------------------------------------------------------
# _extract_series_slug
# ---------------------------------------------------------------------------


class TestExtractSeriesSlug:
    def test_flat_series_slug(self) -> None:
        raw = {"events": [{"seriesSlug": "epl-2025-26", "title": "Test"}]}
        result = _extract_series_slug(raw)
        assert result == "epl-2025-26"

    def test_nested_series_slug(self) -> None:
        raw = {"events": [{"series": [{"slug": "la-liga-2025-26"}]}]}
        result = _extract_series_slug(raw)
        assert result == "la-liga-2025-26"

    def test_no_events(self) -> None:
        raw = {"data": "no events"}
        result = _extract_series_slug(raw)
        assert result is None

    def test_empty_events(self) -> None:
        raw = {"events": []}
        result = _extract_series_slug(raw)
        assert result is None

    def test_event_not_dict(self) -> None:
        raw = {"events": ["not a dict"]}
        result = _extract_series_slug(raw)
        assert result is None

    def test_no_series_slug(self) -> None:
        raw = {"events": [{"title": "Event without series"}]}
        result = _extract_series_slug(raw)
        assert result is None

    def test_empty_series_slug(self) -> None:
        raw = {"events": [{"seriesSlug": ""}]}
        result = _extract_series_slug(raw)
        assert result is None

    def test_nested_series_empty_list(self) -> None:
        raw = {"events": [{"series": []}]}
        result = _extract_series_slug(raw)
        assert result is None

    def test_nested_series_first_not_dict(self) -> None:
        raw = {"events": [{"series": ["not_a_dict"]}]}
        result = _extract_series_slug(raw)
        assert result is None


# ---------------------------------------------------------------------------
# _enrich_raw_event_fields
# ---------------------------------------------------------------------------


class TestEnrichRawEventFields:
    def test_enriches_fields(self) -> None:
        raw = {
            "events": [
                {
                    "title": "Arsenal vs Chelsea",
                    "slug": "arsenal-vs-chelsea",
                    "seriesSlug": "epl-2025-26",
                }
            ]
        }
        _enrich_raw_event_fields(raw)
        assert raw.get("event_title") == "Arsenal vs Chelsea"
        assert raw.get("event_slug") == "arsenal-vs-chelsea"
        assert raw.get("series_slug") == "epl-2025-26"

    def test_does_not_overwrite_existing(self) -> None:
        raw = {
            "series_slug": "existing-value",
            "event_title": "existing-title",
            "events": [
                {
                    "title": "New Title",
                    "slug": "new-slug",
                    "seriesSlug": "new-series",
                }
            ],
        }
        _enrich_raw_event_fields(raw)
        assert raw["series_slug"] == "existing-value"
        assert raw["event_title"] == "existing-title"

    def test_no_events(self) -> None:
        raw = {"data": "no events"}
        _enrich_raw_event_fields(raw)
        # Should not crash
        assert "event_title" not in raw

    def test_not_dict(self) -> None:
        _enrich_raw_event_fields("not a dict")
        # Should not crash


# ---------------------------------------------------------------------------
# _classify_polymarket_error
# ---------------------------------------------------------------------------


class TestClassifyPolymarketError:
    def test_429_status(self) -> None:
        assert _classify_polymarket_error(Exception("rate limited"), status=429) == "429"

    def test_429_message(self) -> None:
        assert _classify_polymarket_error(Exception("429 too many requests")) == "429"

    def test_rate_message(self) -> None:
        assert _classify_polymarket_error(Exception("rate limit")) == "429"

    def test_401_status(self) -> None:
        assert _classify_polymarket_error(Exception("unauthorized"), status=401) == "401"

    def test_401_message(self) -> None:
        assert _classify_polymarket_error(Exception("401 unauthorized")) == "401"

    def test_400_status(self) -> None:
        assert _classify_polymarket_error(Exception("bad request"), status=400) == "400"

    def test_500_status(self) -> None:
        assert _classify_polymarket_error(Exception("server error"), status=500) == "500"

    def test_500_message(self) -> None:
        assert _classify_polymarket_error(Exception("internal server error")) == "500"

    def test_unknown(self) -> None:
        assert _classify_polymarket_error(Exception("something else")) == "UNKNOWN"
