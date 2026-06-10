"""Unit tests for venue adapters (no live network — uses mocked responses)."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from instruments_service.reference_data.adapters.prediction.polymarket import PolymarketReferenceDataAdapter
from instruments_service.reference_data.adapters.sports.adapters.betfair import BetfairReferenceDataAdapter


class TestBetfairAdapter:
    def test_venue_name(self) -> None:
        adapter = BetfairReferenceDataAdapter()
        assert adapter.venue == "betfair"

    def test_get_credentials_raises_without_project_id(self) -> None:
        adapter = BetfairReferenceDataAdapter()
        with pytest.raises(ValueError, match="api_key required"):
            adapter._get_credentials()

    @pytest.mark.asyncio
    async def test_get_instruments_raises_without_credentials(self) -> None:
        adapter = BetfairReferenceDataAdapter()
        with pytest.raises(ValueError, match="api_key required"):
            await adapter.get_instruments()

    @pytest.mark.asyncio
    async def test_get_instruments_non_sports_event_returns_empty(self) -> None:
        adapter = BetfairReferenceDataAdapter()
        results = await adapter.get_instruments(instrument_type="SPOT_PAIR")
        assert results == []

    @pytest.mark.asyncio
    async def test_get_instruments_mocked(self) -> None:
        adapter = BetfairReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(
            return_value={
                "result": [
                    {
                        "marketId": "1.234567",
                        "marketName": "Match Odds",
                        "marketStartTime": "2026-01-01T15:00:00Z",
                        "eventType": {"id": "1", "name": "Soccer"},
                        "event": {"id": "28375802", "name": "Team A vs Team B"},
                        "runners": [
                            {"selectionId": 111, "runnerName": "Team A"},
                            {"selectionId": 222, "runnerName": "Team B"},
                        ],
                    }
                ]
            }
        )
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.post = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with (
            patch("aiohttp.ClientSession", return_value=mock_session_cm),
            patch.object(adapter, "_get_credentials", return_value=("tok", "key")),
        ):
            results = await adapter.get_instruments()
        assert len(results) == 2
        assert results[0].venue == "betfair"
        assert results[0].instrument_type == "EXCHANGE_ODDS"
        assert results[0].instrument_key == "1.234567/111"

    @pytest.mark.asyncio
    async def test_get_instrument_found_mocked(self) -> None:
        adapter = BetfairReferenceDataAdapter()
        with patch.object(
            adapter,
            "get_instruments",
            return_value=[],
        ):
            result = await adapter.get_instrument("1.234567/111")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_options_chain_raises(self) -> None:
        adapter = BetfairReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_options_chain("soccer")

    @pytest.mark.asyncio
    async def test_get_expiry_calendar_raises(self) -> None:
        adapter = BetfairReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_expiry_calendar("soccer")

    @pytest.mark.asyncio
    async def test_get_funding_rate_raises(self) -> None:
        adapter = BetfairReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_funding_rate("1.234567/111")

    @pytest.mark.asyncio
    async def test_get_ohlcv_raises(self) -> None:
        """Betfair OHLCV is not available via reference data API."""
        adapter = BetfairReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_ohlcv("1.234567/111")

    def test_parse_catalogue_item_no_runners(self) -> None:
        from unified_api_contracts.external.betfair.schemas import (
            BetfairMarketCatalogue,
        )

        adapter = BetfairReferenceDataAdapter()
        catalogue = BetfairMarketCatalogue.model_validate(
            {
                "marketId": "1.111",
                "marketName": "No Runners",
            }
        )
        now = datetime.now(UTC)
        results = adapter._parse_catalogue_item(catalogue, now)
        assert results == []

    def test_parse_catalogue_item_runner_no_selection_id(self) -> None:
        from unified_api_contracts.external.betfair.schemas import (
            BetfairMarketCatalogue,
        )

        adapter = BetfairReferenceDataAdapter()
        catalogue = BetfairMarketCatalogue.model_validate(
            {
                "marketId": "1.111",
                "marketName": "Market",
                "runners": [{"runnerName": "NoId"}],
            }
        )
        now = datetime.now(UTC)
        results = adapter._parse_catalogue_item(catalogue, now)
        assert results == []

    @pytest.mark.asyncio
    async def test_get_instruments_raises_on_network_error(self) -> None:
        """Network error must raise, not return [] (CF-11 regression)."""
        import aiohttp as _aiohttp

        adapter = BetfairReferenceDataAdapter()
        mock_session_cm = MagicMock()
        mock_session_obj = MagicMock()
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(side_effect=_aiohttp.ClientError("network"))
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj.post = MagicMock(return_value=mock_cm)
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with (
            patch("aiohttp.ClientSession", return_value=mock_session_cm),
            patch.object(adapter, "_get_credentials", return_value=("tok", "key")),
            pytest.raises((RuntimeError, _aiohttp.ClientError)),
        ):
            await adapter.get_instruments()


# ---------------------------------------------------------------------------
# Polymarket adapter tests
# ---------------------------------------------------------------------------


class TestPolymarketAdapterExtended:
    def test_venue_name(self) -> None:
        adapter = PolymarketReferenceDataAdapter()
        assert adapter.venue == "POLYMARKET"

    @pytest.mark.asyncio
    async def test_get_instruments_non_prediction_market_returns_empty(self) -> None:
        adapter = PolymarketReferenceDataAdapter()
        results = await adapter.get_instruments(instrument_type="SPOT_PAIR")
        assert results == []

    @pytest.mark.asyncio
    async def test_get_instruments_mocked_single_page(self) -> None:
        adapter = PolymarketReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(
            return_value=[
                {
                    "conditionId": "0xabc123",
                    "marketSlug": "will-btc-reach-100k",
                    "question": "Will BTC reach $100K?",
                    "active": True,
                    "closed": False,
                    "minimumTickSize": 0.01,
                    "minimumOrderSize": 1.0,
                }
            ]
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
        assert results[0].venue == "POLYMARKET"
        assert results[0].instrument_key == "0xabc123"
        assert results[0].instrument_type == "PREDICTION_MARKET"

    @pytest.mark.asyncio
    async def test_get_options_chain_raises(self) -> None:
        adapter = PolymarketReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_options_chain("BTC")

    @pytest.mark.asyncio
    async def test_get_expiry_calendar_raises(self) -> None:
        adapter = PolymarketReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_expiry_calendar("BTC")

    @pytest.mark.asyncio
    async def test_get_funding_rate_raises(self) -> None:
        adapter = PolymarketReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_funding_rate("0xabc123")

    @pytest.mark.asyncio
    async def test_get_ohlcv_mocked(self) -> None:
        """Polymarket get_ohlcv uses CLOB API — mock the HTTP call."""
        adapter = PolymarketReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.json = AsyncMock(return_value={"history": []})
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            result = await adapter.get_ohlcv("0xabc123")
        assert isinstance(result, list)

    @pytest.mark.asyncio
    async def test_network_error_raises_not_truncates(self) -> None:
        """A live-mode Gamma page network error must RAISE (→ attempted_failed),
        NOT return an empty/truncated universe. Updated from the pre-CF-11
        return-empty contract (which encoded the silent-truncation bug) per
        prediction_manifest_canonicalisation_2026_06_01 § CF-11 IS write-path
        (sibling of the CLOB-scan fix); see test_live_page_failure_raises_not_truncates.
        """
        import aiohttp as _aiohttp

        adapter = PolymarketReferenceDataAdapter()
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(side_effect=_aiohttp.ClientError("fail"))
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session_cm), pytest.raises(_aiohttp.ClientError):
            await adapter.get_instruments()

    @pytest.mark.asyncio
    async def test_clob_scan_midscan_failure_raises_not_truncates(self) -> None:
        """A mid-scan CLOB page failure must RAISE (→ attempted_failed) + emit
        ADAPTER_FETCH_FAILED, NOT silently return the partial universe accumulated
        so far. Returning partial would be cached 24 h and read as a complete (but
        smaller) universe → false-complete coverage. Regression for
        prediction_manifest_canonicalisation_2026_06_01 § CF-11 IS-side write-path.
        """
        import aiohttp as _aiohttp

        adapter = PolymarketReferenceDataAdapter()

        # Page 0 succeeds with a non-terminal cursor → the scan continues to page 1.
        ok_resp = AsyncMock()
        ok_resp.raise_for_status = MagicMock()
        ok_resp.json = AsyncMock(return_value={"data": [{"conditionId": "0xabc"}], "next_cursor": "MTAwMA=="})
        ok_cm = MagicMock()
        ok_cm.__aenter__ = AsyncMock(return_value=ok_resp)
        ok_cm.__aexit__ = AsyncMock(return_value=None)
        # Page 1 fails mid-scan.
        fail_cm = MagicMock()
        fail_cm.__aenter__ = AsyncMock(side_effect=_aiohttp.ClientError("page 1 down"))
        fail_cm.__aexit__ = AsyncMock(return_value=None)

        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(side_effect=[ok_cm, fail_cm])
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)

        events: list[str] = []
        with (
            patch("aiohttp.ClientSession", return_value=mock_session_cm),
            patch(
                "instruments_service.reference_data.adapters.prediction.polymarket.log_event",
                side_effect=lambda name, **_kw: events.append(name),
            ),
            pytest.raises(_aiohttp.ClientError),
        ):
            await adapter.get_instruments(date="2025-03-14")
        assert "ADAPTER_FETCH_FAILED" in events

    @pytest.mark.asyncio
    async def test_live_page_failure_raises_not_truncates(self) -> None:
        """A live-mode (Gamma) page failure must RAISE (→ attempted_failed) + emit
        ADAPTER_FETCH_FAILED, NOT return ``[]``. Returning ``[]`` makes the
        ``get_instruments`` live-mode pagination loop break (page < _PAGE_LIMIT) →
        a transient failure masquerades as a complete (empty/smaller) universe with
        ZERO failure signal. Regression for
        prediction_manifest_canonicalisation_2026_06_01 § CF-11 IS-side write-path
        (sibling of the CLOB-scan truncation fix).
        """
        import aiohttp as _aiohttp

        adapter = PolymarketReferenceDataAdapter()

        # Live mode (date=None) → first Gamma page fails.
        fail_cm = MagicMock()
        fail_cm.__aenter__ = AsyncMock(side_effect=_aiohttp.ClientError("gamma page 0 down"))
        fail_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(return_value=fail_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)

        events: list[str] = []
        with (
            patch("aiohttp.ClientSession", return_value=mock_session_cm),
            patch(
                "instruments_service.reference_data.adapters.prediction.polymarket.log_event",
                side_effect=lambda name, **_kw: events.append(name),
            ),
            pytest.raises(_aiohttp.ClientError),
        ):
            await adapter.get_instruments(date=None)
        assert "ADAPTER_FETCH_FAILED" in events

    @pytest.mark.asyncio
    async def test_get_instrument_found(self) -> None:
        """get_instrument returns matching record by condition_id."""
        adapter = PolymarketReferenceDataAdapter()
        with patch.object(
            adapter,
            "get_instruments",
            AsyncMock(return_value=[MagicMock(instrument_key="0xabc123", raw_symbol="will-btc-reach-100k")]),
        ):
            result = await adapter.get_instrument("0xabc123")
        assert result is not None

    @pytest.mark.asyncio
    async def test_get_instrument_not_found(self) -> None:
        """get_instrument returns None when no match found."""
        adapter = PolymarketReferenceDataAdapter()
        with patch.object(adapter, "get_instruments", AsyncMock(return_value=[])):
            result = await adapter.get_instrument("0xnotexist")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_ohlcv_with_history_data(self) -> None:
        """get_ohlcv parses price history points into OHLCVRef list."""
        adapter = PolymarketReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(
            return_value={"history": [{"t": 1700000000, "p": "0.65"}, {"t": 1700086400, "p": "0.70"}]}
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
            result = await adapter.get_ohlcv("0xabc123")
        assert len(result) == 2
        assert result[0].venue == "POLYMARKET"


# ---------------------------------------------------------------------------
# Databento adapter mocked tests
# ---------------------------------------------------------------------------
