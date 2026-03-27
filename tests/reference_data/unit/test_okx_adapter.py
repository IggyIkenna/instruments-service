"""Unit tests for venue adapters (no live network — uses mocked responses)."""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from unified_api_contracts.external.okx.schemas import (
    OKXInstrumentInfo,
)

from instruments_service.reference_data.adapters.okx import OKXReferenceDataAdapter


class TestOKXAdapter:
    def test_venue_name(self) -> None:
        adapter = OKXReferenceDataAdapter()
        assert adapter.venue == "okx"


class TestOKXAdapterExtended:
    def _make_okx_session(self, items: list[dict[str, object]]) -> MagicMock:
        mock_resp = AsyncMock()
        mock_resp.json = AsyncMock(return_value={"data": items})
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        return mock_session_cm

    @pytest.mark.asyncio
    async def test_fetch_instruments_spot(self) -> None:
        adapter = OKXReferenceDataAdapter()
        mock_session_cm = self._make_okx_session(
            [
                {
                    "instId": "BTC-USDT",
                    "instType": "SPOT",
                    "state": "live",
                    "baseCcy": "BTC",
                    "quoteCcy": "USDT",
                    "tickSz": "0.01",
                    "lotSz": "0.00001",
                    "minSz": "0.001",
                },
                {
                    "instId": "OLD-USDT",
                    "instType": "SPOT",
                    "state": "suspend",
                    "baseCcy": "OLD",
                    "quoteCcy": "USDT",
                    "tickSz": "0.01",
                    "lotSz": "1",
                    "minSz": "1",
                },
            ]
        )
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            results = await adapter.get_instruments(instrument_type="spot")
        assert len(results) == 1
        assert results[0].raw_symbol == "BTC-USDT"
        assert results[0].instrument_type == "spot"

    @pytest.mark.asyncio
    async def test_fetch_instruments_swap_mapped_to_perp(self) -> None:
        adapter = OKXReferenceDataAdapter()
        mock_session_cm = self._make_okx_session(
            [
                {
                    "instId": "BTC-USDT-SWAP",
                    "instType": "SWAP",
                    "state": "live",
                    "baseCcy": "BTC",
                    "quoteCcy": "USDT",
                    "tickSz": "0.1",
                    "lotSz": "1",
                    "minSz": "1",
                    "ctVal": "0.01",
                    "settleCcy": "USDT",
                },
            ]
        )
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            results = await adapter.get_instruments(instrument_type="perp")
        assert len(results) == 1
        assert results[0].instrument_type == "perp"

    @pytest.mark.asyncio
    async def test_fetch_instruments_futures_mapped(self) -> None:
        adapter = OKXReferenceDataAdapter()
        mock_session_cm = self._make_okx_session(
            [
                {
                    "instId": "BTC-USDT-240329",
                    "instType": "FUTURES",
                    "state": "live",
                    "baseCcy": "BTC",
                    "quoteCcy": "USDT",
                    "tickSz": "0.1",
                    "lotSz": "1",
                    "minSz": "1",
                    "expTime": "1711670400000",
                },
            ]
        )
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            results = await adapter.get_instruments(instrument_type="future")
        assert len(results) == 1
        assert results[0].instrument_type == "future"
        assert results[0].expiry is not None

    @pytest.mark.asyncio
    async def test_get_funding_rate_mocked(self) -> None:
        """get_funding_rate is implemented — mock the HTTP call."""
        adapter = OKXReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(
            return_value={
                "data": [{"fundingRate": "0.0001", "fundingTime": "0"}],
                "code": "0",
            }
        )
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_cm)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = await adapter.get_funding_rate("BTC-USDT-SWAP")
        assert result is not None

    @pytest.mark.asyncio
    async def test_get_ohlcv_mocked(self) -> None:
        """get_ohlcv is implemented — mock the HTTP call."""
        adapter = OKXReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json = AsyncMock(return_value={"data": [], "code": "0"})
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_cm)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session):
            result = await adapter.get_ohlcv("BTC-USDT-SWAP")
        assert isinstance(result, list)

    @pytest.mark.asyncio
    async def test_get_instruments_option_type_fetches_underlyings(self) -> None:
        adapter = OKXReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.json = AsyncMock(return_value={"data": []})
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(return_value=mock_cm)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session_obj)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            results = await adapter.get_instruments(instrument_type="option")
        assert results == []
        # called 3 times (BTC-USD, ETH-USD, SOL-USD)
        assert mock_session_obj.get.call_count == 3

    @pytest.mark.asyncio
    async def test_get_instrument_found(self) -> None:
        adapter = OKXReferenceDataAdapter()
        from datetime import UTC, datetime
        from decimal import Decimal

        from unified_api_contracts.internal import InstrumentRecord

        inst = InstrumentRecord(
            instrument_key="BTC-USDT",
            venue="okx",
            raw_symbol="BTC-USDT",
            instrument_type="spot",
            base_asset="BTC",
            quote_asset="USDT",
            tick_size=Decimal("0.01"),
            lot_size=Decimal("0.00001"),
            min_order_size=Decimal("0.001"),
            is_active=True,
            updated_at=datetime.now(UTC),
        )
        with patch.object(adapter, "get_instruments", return_value=[inst]):
            result = await adapter.get_instrument("BTC-USDT")
        assert result is not None
        assert result.raw_symbol == "BTC-USDT"

    @pytest.mark.asyncio
    async def test_get_instrument_not_found(self) -> None:
        adapter = OKXReferenceDataAdapter()
        with patch.object(adapter, "get_instruments", return_value=[]):
            result = await adapter.get_instrument("NOTEXIST")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_options_chain_mocked(self) -> None:
        adapter = OKXReferenceDataAdapter()
        from datetime import UTC, datetime
        from decimal import Decimal

        from unified_api_contracts.internal import InstrumentRecord

        now = datetime.now(UTC)
        call_inst = InstrumentRecord(
            instrument_key="BTC-USD-241231-50000-C",
            venue="okx",
            raw_symbol="BTC-USD-50000-C",
            instrument_type="option",
            base_asset="BTC",
            quote_asset="USD",
            tick_size=Decimal("0.01"),
            lot_size=Decimal("0.001"),
            min_order_size=Decimal("0.001"),
            strike=Decimal("50000"),
            option_type="C",
            is_active=True,
            updated_at=now,
        )
        put_inst = InstrumentRecord(
            instrument_key="BTC-USD-241231-50000-P",
            venue="okx",
            raw_symbol="BTC-USD-50000-P",
            instrument_type="option",
            base_asset="BTC",
            quote_asset="USD",
            tick_size=Decimal("0.01"),
            lot_size=Decimal("0.001"),
            min_order_size=Decimal("0.001"),
            strike=Decimal("50000"),
            option_type="P",
            is_active=True,
            updated_at=now,
        )
        with patch.object(adapter, "get_instruments", return_value=[call_inst, put_inst]):
            chain = await adapter.get_options_chain("BTC")
        assert chain.venue == "okx"
        assert chain.underlying == "BTC"
        assert len(chain.calls) == 1
        assert len(chain.puts) == 1

    @pytest.mark.asyncio
    async def test_get_expiry_calendar_mocked(self) -> None:
        adapter = OKXReferenceDataAdapter()
        from datetime import UTC, datetime
        from decimal import Decimal

        from unified_api_contracts.internal import InstrumentRecord

        now = datetime.now(UTC)
        inst = InstrumentRecord(
            instrument_key="BTC-USDT-240329",
            venue="okx",
            raw_symbol="BTC-USDT-240329",
            instrument_type="future",
            base_asset="BTC",
            quote_asset="USDT",
            tick_size=Decimal("0.1"),
            lot_size=Decimal("1"),
            min_order_size=Decimal("1"),
            expiry=now,
            is_active=True,
            updated_at=now,
        )
        with patch.object(adapter, "get_instruments", return_value=[inst]):
            calendar = await adapter.get_expiry_calendar("BTC", instrument_type="future")
        assert calendar.venue == "okx"
        assert len(calendar.expiries) == 1

    def test_parse_okx_instrument_none_inst_id(self) -> None:
        adapter = OKXReferenceDataAdapter()
        now = datetime.now(UTC)
        result = adapter._parse_okx_instrument(
            OKXInstrumentInfo.model_validate({"instType": "SPOT", "instId": ""}),
            now,
        )
        assert result is None

    def test_parse_okx_instrument_option_type(self) -> None:
        adapter = OKXReferenceDataAdapter()
        now = datetime.now(UTC)
        result = adapter._parse_okx_instrument(
            OKXInstrumentInfo.model_validate(
                {
                    "instId": "BTC-USD-240329-50000-C",
                    "instType": "OPTION",
                    "state": "live",
                    "baseCcy": "BTC",
                    "quoteCcy": "USD",
                    "tickSz": "0.01",
                    "lotSz": "0.001",
                    "minSz": "0.001",
                    "stk": "50000",
                    "optType": "C",
                    "expTime": "1711670400000",
                    "settleCcy": "USD",
                }
            ),
            now,
        )
        assert result is not None
        assert result.instrument_type == "option"
        assert result.strike == Decimal("50000")
        assert result.option_type == "c"

    @pytest.mark.asyncio
    async def test_fetch_instruments_skips_none_records(self) -> None:
        adapter = OKXReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.json = AsyncMock(
            return_value={
                "data": [
                    # empty instId — _parse_okx_instrument returns None
                    {"instId": "", "instType": "SPOT", "state": "live"},
                    {
                        "instId": "BTC-USDT",
                        "instType": "SPOT",
                        "state": "live",
                        "baseCcy": "BTC",
                        "quoteCcy": "USDT",
                        "tickSz": "0.01",
                        "lotSz": "0.00001",
                        "minSz": "0.001",
                    },
                ]
            }
        )
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(return_value=mock_cm)

        results = await adapter._fetch_instruments(mock_session_obj, "SPOT")
        assert len(results) == 1
        assert results[0].raw_symbol == "BTC-USDT"

    @pytest.mark.asyncio
    async def test_fetch_instruments_with_uly(self) -> None:
        adapter = OKXReferenceDataAdapter()
        mock_resp = AsyncMock()
        mock_resp.json = AsyncMock(return_value={"data": []})
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_session_obj = MagicMock()
        mock_session_obj.get = MagicMock(return_value=mock_cm)
        results = await adapter._fetch_instruments(mock_session_obj, "OPTION", uly="BTC-USD")
        assert results == []
        call_args = mock_session_obj.get.call_args
        assert call_args[1]["params"]["uly"] == "BTC-USD"


# ---------------------------------------------------------------------------
# Extended Bybit adapter tests
# ---------------------------------------------------------------------------
