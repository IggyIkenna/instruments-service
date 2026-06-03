"""Coverage tests for Lighter, Extended, Pacifica and HTTP-path coverage for
existing adapter tests that mock at _fetch_perp_markets level (leaving the
internal HTTP code uncovered).

All tests are credential-free and use unittest.mock.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType


def _make_session_cm() -> MagicMock:
    """Return a mock async context manager that yields a mock session."""
    mock_session = MagicMock()
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=mock_session)
    cm.__aexit__ = AsyncMock(return_value=None)
    return cm


# ─────────────────────────────────────────────────────────────────────────────
# Lighter adapter
# ─────────────────────────────────────────────────────────────────────────────


class TestLighterAdapter:
    def test_venue_name(self) -> None:
        from instruments_service.reference_data.adapters.defi.lighter import LighterReferenceDataAdapter

        assert LighterReferenceDataAdapter().venue == "LIGHTER-ZKSYNC"

    def test_classify_rate_limit_status(self) -> None:
        from instruments_service.reference_data.adapters.defi.lighter import _classify_lighter_error

        assert _classify_lighter_error(Exception("err"), status=429) == "RATE_LIMIT"

    def test_classify_rate_limit_message(self) -> None:
        from instruments_service.reference_data.adapters.defi.lighter import _classify_lighter_error

        assert _classify_lighter_error(Exception("rate limit exceeded")) == "RATE_LIMIT"

    def test_classify_500(self) -> None:
        from instruments_service.reference_data.adapters.defi.lighter import _classify_lighter_error

        assert _classify_lighter_error(Exception("err"), status=503) == "500"

    def test_classify_unknown(self) -> None:
        from instruments_service.reference_data.adapters.defi.lighter import _classify_lighter_error

        assert _classify_lighter_error(Exception("network error")) == "UNKNOWN"

    @pytest.mark.asyncio
    async def test_get_instruments_wrong_type_returns_empty(self) -> None:
        from instruments_service.reference_data.adapters.defi.lighter import LighterReferenceDataAdapter

        adapter = LighterReferenceDataAdapter()
        results = await adapter.get_instruments(instrument_type="SPOT_PAIR")
        assert results == []

    @pytest.mark.asyncio
    async def test_get_instruments_success(self) -> None:
        from instruments_service.reference_data.adapters.defi.lighter import LighterReferenceDataAdapter

        adapter = LighterReferenceDataAdapter()
        raw = {
            "order_book_details": [
                {"symbol": "BTC-USDC", "market_type": "perp"},
                {"symbol": "ETH-USDC", "market_type": "perp"},
                {"symbol": "SOL-USDC", "market_type": "spot"},  # filtered out
                {"symbol": "", "market_type": "perp"},  # empty symbol — filtered
            ]
        }
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(return_value=raw)),
        ):
            results = await adapter.get_instruments()

        assert len(results) == 2
        syms = {r.raw_symbol for r in results}
        assert syms == {"BTC-USDC", "ETH-USDC"}
        assert all(r.instrument_type == InstrumentType.PERPETUAL for r in results)
        assert all(r.settle_asset == "USDC" for r in results)

    @pytest.mark.asyncio
    async def test_get_instruments_non_dict_response(self) -> None:
        from instruments_service.reference_data.adapters.defi.lighter import LighterReferenceDataAdapter

        adapter = LighterReferenceDataAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(return_value=[])),
        ):
            results = await adapter.get_instruments()
        assert results == []

    @pytest.mark.asyncio
    async def test_get_instruments_empty_details(self) -> None:
        from instruments_service.reference_data.adapters.defi.lighter import LighterReferenceDataAdapter

        adapter = LighterReferenceDataAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(return_value={})),
        ):
            results = await adapter.get_instruments()
        assert results == []

    @pytest.mark.asyncio
    async def test_get_instruments_client_error_raises(self) -> None:
        """ClientError from _get_with_retry must raise, not return [] (CF-11 regression)."""
        from instruments_service.reference_data.adapters.defi.lighter import LighterReferenceDataAdapter

        adapter = LighterReferenceDataAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(side_effect=aiohttp.ClientError("failed"))),
            patch("instruments_service.reference_data.adapters.defi.lighter.log_event"),
            pytest.raises((RuntimeError, aiohttp.ClientError)),
        ):
            await adapter.get_instruments()

    @pytest.mark.asyncio
    async def test_get_instruments_runtime_error_raises(self) -> None:
        """RuntimeError from _get_with_retry must raise, not return [] (CF-11 regression)."""
        from instruments_service.reference_data.adapters.defi.lighter import LighterReferenceDataAdapter

        adapter = LighterReferenceDataAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(side_effect=RuntimeError("timeout"))),
            pytest.raises((RuntimeError, aiohttp.ClientError)),
        ):
            await adapter.get_instruments()

    @pytest.mark.asyncio
    async def test_get_instrument_found(self) -> None:
        from instruments_service.reference_data.adapters.defi.lighter import LighterReferenceDataAdapter

        adapter = LighterReferenceDataAdapter()
        raw = {"order_book_details": [{"symbol": "ETH-USDC", "market_type": "perp"}]}
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(return_value=raw)),
        ):
            found = await adapter.get_instrument("ETH-USDC")
        assert found is not None
        assert found.raw_symbol == "ETH-USDC"

    @pytest.mark.asyncio
    async def test_get_instrument_not_found(self) -> None:
        from instruments_service.reference_data.adapters.defi.lighter import LighterReferenceDataAdapter

        adapter = LighterReferenceDataAdapter()
        raw = {"order_book_details": [{"symbol": "ETH-USDC", "market_type": "perp"}]}
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(return_value=raw)),
        ):
            result = await adapter.get_instrument("XYZ-USDC")
        assert result is None

    @pytest.mark.asyncio
    async def test_symbol_with_hyphen_splits_base(self) -> None:
        from instruments_service.reference_data.adapters.defi.lighter import LighterReferenceDataAdapter

        adapter = LighterReferenceDataAdapter()
        raw = {"order_book_details": [{"symbol": "BTC-USDC", "market_type": "perp"}]}
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(return_value=raw)),
        ):
            results = await adapter.get_instruments()
        assert results[0].base_asset == "BTC"

    @pytest.mark.asyncio
    async def test_symbol_no_hyphen_uses_full_sym(self) -> None:
        from instruments_service.reference_data.adapters.defi.lighter import LighterReferenceDataAdapter

        adapter = LighterReferenceDataAdapter()
        raw = {"order_book_details": [{"symbol": "BTCUSD", "market_type": "perp"}]}
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(return_value=raw)),
        ):
            results = await adapter.get_instruments()
        assert results[0].base_asset == "BTCUSD"


# ─────────────────────────────────────────────────────────────────────────────
# Extended adapter
# ─────────────────────────────────────────────────────────────────────────────


class TestExtendedAdapter:
    def test_venue_name(self) -> None:
        from instruments_service.reference_data.adapters.defi.extended import ExtendedReferenceDataAdapter

        assert ExtendedReferenceDataAdapter().venue == "EXTENDED-STARKNET"

    def test_classify_rate_limit_status(self) -> None:
        from instruments_service.reference_data.adapters.defi.extended import _classify_extended_error

        assert _classify_extended_error(Exception("err"), status=429) == "RATE_LIMIT"

    def test_classify_rate_limit_message(self) -> None:
        from instruments_service.reference_data.adapters.defi.extended import _classify_extended_error

        assert _classify_extended_error(Exception("rate exceeded")) == "RATE_LIMIT"

    def test_classify_500(self) -> None:
        from instruments_service.reference_data.adapters.defi.extended import _classify_extended_error

        assert _classify_extended_error(Exception("err"), status=500) == "500"

    def test_classify_unknown(self) -> None:
        from instruments_service.reference_data.adapters.defi.extended import _classify_extended_error

        assert _classify_extended_error(Exception("conn reset")) == "UNKNOWN"

    @pytest.mark.asyncio
    async def test_get_instruments_wrong_type_returns_empty(self) -> None:
        from instruments_service.reference_data.adapters.defi.extended import ExtendedReferenceDataAdapter

        adapter = ExtendedReferenceDataAdapter()
        results = await adapter.get_instruments(instrument_type="SPOT_PAIR")
        assert results == []

    @pytest.mark.asyncio
    async def test_get_instruments_success(self) -> None:
        from instruments_service.reference_data.adapters.defi.extended import ExtendedReferenceDataAdapter

        adapter = ExtendedReferenceDataAdapter()
        raw = {
            "data": [
                {"name": "BTC-USD", "active": True, "status": "ACTIVE"},
                {"name": "ETH-USD", "active": True, "status": "ACTIVE"},
                {"name": "SOL-USD", "active": False, "status": "INACTIVE"},  # filtered
            ]
        }
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(return_value=raw)),
        ):
            results = await adapter.get_instruments()

        assert len(results) == 2
        assert all(r.instrument_type == InstrumentType.PERPETUAL for r in results)

    @pytest.mark.asyncio
    async def test_get_instruments_empty_active_falls_back(self) -> None:
        from instruments_service.reference_data.adapters.defi.extended import (
            _EXTENDED_FALLBACK_MARKETS,
            ExtendedReferenceDataAdapter,
        )

        adapter = ExtendedReferenceDataAdapter()
        raw = {"data": []}  # No ACTIVE markets → fallback
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(return_value=raw)),
        ):
            results = await adapter.get_instruments()

        assert len(results) == len(_EXTENDED_FALLBACK_MARKETS)

    @pytest.mark.asyncio
    async def test_get_instruments_non_dict_falls_back(self) -> None:
        from instruments_service.reference_data.adapters.defi.extended import (
            _EXTENDED_FALLBACK_MARKETS,
            ExtendedReferenceDataAdapter,
        )

        adapter = ExtendedReferenceDataAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(return_value=[])),
        ):
            results = await adapter.get_instruments()

        assert len(results) == len(_EXTENDED_FALLBACK_MARKETS)

    @pytest.mark.asyncio
    async def test_get_instruments_client_error_uses_fallback(self) -> None:
        from instruments_service.reference_data.adapters.defi.extended import (
            _EXTENDED_FALLBACK_MARKETS,
            ExtendedReferenceDataAdapter,
        )

        adapter = ExtendedReferenceDataAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(side_effect=aiohttp.ClientError("failed"))),
            patch("instruments_service.reference_data.adapters.defi.extended.log_event"),
        ):
            results = await adapter.get_instruments()

        assert len(results) == len(_EXTENDED_FALLBACK_MARKETS)

    @pytest.mark.asyncio
    async def test_get_instruments_runtime_error_uses_fallback(self) -> None:
        from instruments_service.reference_data.adapters.defi.extended import (
            _EXTENDED_FALLBACK_MARKETS,
            ExtendedReferenceDataAdapter,
        )

        adapter = ExtendedReferenceDataAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(side_effect=RuntimeError("timeout"))),
        ):
            results = await adapter.get_instruments()

        assert len(results) == len(_EXTENDED_FALLBACK_MARKETS)

    @pytest.mark.asyncio
    async def test_get_instrument_found(self) -> None:
        from instruments_service.reference_data.adapters.defi.extended import ExtendedReferenceDataAdapter

        adapter = ExtendedReferenceDataAdapter()
        raw = {"data": [{"name": "ETH-USD", "active": True, "status": "ACTIVE"}]}
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(return_value=raw)),
        ):
            found = await adapter.get_instrument("ETH-USD")
        assert found is not None

    @pytest.mark.asyncio
    async def test_get_instrument_not_found(self) -> None:
        from instruments_service.reference_data.adapters.defi.extended import ExtendedReferenceDataAdapter

        adapter = ExtendedReferenceDataAdapter()
        raw = {"data": [{"name": "ETH-USD", "active": True, "status": "ACTIVE"}]}
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(return_value=raw)),
        ):
            result = await adapter.get_instrument("XYZ-USD")
        assert result is None

    @pytest.mark.asyncio
    async def test_base_asset_split_from_hyphen(self) -> None:
        from instruments_service.reference_data.adapters.defi.extended import ExtendedReferenceDataAdapter

        adapter = ExtendedReferenceDataAdapter()
        raw = {"data": [{"name": "BTC-USD", "active": True, "status": "ACTIVE"}]}
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(return_value=raw)),
        ):
            results = await adapter.get_instruments()
        assert results[0].base_asset == "BTC"

    @pytest.mark.asyncio
    async def test_base_asset_no_hyphen(self) -> None:
        from instruments_service.reference_data.adapters.defi.extended import ExtendedReferenceDataAdapter

        adapter = ExtendedReferenceDataAdapter()
        raw = {"data": [{"name": "BTCUSD", "active": True, "status": "ACTIVE"}]}
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(return_value=raw)),
        ):
            results = await adapter.get_instruments()
        assert results[0].base_asset == "BTCUSD"


# ─────────────────────────────────────────────────────────────────────────────
# Pacifica adapter
# ─────────────────────────────────────────────────────────────────────────────


class TestPacificaAdapter:
    def test_venue_name(self) -> None:
        from instruments_service.reference_data.adapters.defi.pacifica import PacificaReferenceDataAdapter

        assert PacificaReferenceDataAdapter().venue == "PACIFICA-SOLANA"

    @pytest.mark.asyncio
    async def test_get_instruments_wrong_type_returns_empty(self) -> None:
        from instruments_service.reference_data.adapters.defi.pacifica import PacificaReferenceDataAdapter

        adapter = PacificaReferenceDataAdapter()
        results = await adapter.get_instruments(instrument_type="SPOT_PAIR")
        assert results == []

    @pytest.mark.asyncio
    async def test_get_instruments_returns_all_perps(self) -> None:
        from instruments_service.reference_data.adapters.defi.pacifica import (
            _PACIFICA_TOP_COINS,
            PacificaReferenceDataAdapter,
        )

        adapter = PacificaReferenceDataAdapter()
        results = await adapter.get_instruments()
        assert len(results) == len(_PACIFICA_TOP_COINS)
        assert all(r.instrument_type == InstrumentType.PERPETUAL for r in results)
        assert all(r.venue == "PACIFICA-SOLANA" for r in results)

    @pytest.mark.asyncio
    async def test_get_instruments_perpetual_type_filter(self) -> None:
        from instruments_service.reference_data.adapters.defi.pacifica import (
            _PACIFICA_TOP_COINS,
            PacificaReferenceDataAdapter,
        )

        adapter = PacificaReferenceDataAdapter()
        results = await adapter.get_instruments(instrument_type=InstrumentType.PERPETUAL)
        assert len(results) == len(_PACIFICA_TOP_COINS)

    @pytest.mark.asyncio
    async def test_get_instrument_found(self) -> None:
        from instruments_service.reference_data.adapters.defi.pacifica import PacificaReferenceDataAdapter

        adapter = PacificaReferenceDataAdapter()
        found = await adapter.get_instrument("BTC-PERP")
        assert found is not None
        assert found.base_asset == "BTC"

    @pytest.mark.asyncio
    async def test_get_instrument_not_found(self) -> None:
        from instruments_service.reference_data.adapters.defi.pacifica import PacificaReferenceDataAdapter

        adapter = PacificaReferenceDataAdapter()
        result = await adapter.get_instrument("AAPL-PERP")
        assert result is None

    @pytest.mark.asyncio
    async def test_instrument_fields(self) -> None:
        from instruments_service.reference_data.adapters.defi.pacifica import PacificaReferenceDataAdapter

        adapter = PacificaReferenceDataAdapter()
        results = await adapter.get_instruments()
        btc = next(r for r in results if r.base_asset == "BTC")
        assert btc.settle_asset == "USDC"
        assert btc.quote_asset == "USDC"
        assert btc.raw_symbol == "BTC-PERP"
        assert btc.tick_size == Decimal("0.0001")
        assert btc.status == InstrumentStatus.ACTIVE

    @pytest.mark.asyncio
    async def test_unsupported_methods_raise(self) -> None:
        from instruments_service.reference_data.adapters.defi.pacifica import PacificaReferenceDataAdapter

        adapter = PacificaReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_options_chain("BTC")
        with pytest.raises(NotImplementedError):
            await adapter.get_expiry_calendar("BTC")
        with pytest.raises(NotImplementedError):
            await adapter.get_funding_rate("BTC-PERP")
        with pytest.raises(NotImplementedError):
            await adapter.get_ohlcv("BTC-PERP")


# ─────────────────────────────────────────────────────────────────────────────
# HTTP-path coverage for adapters that already have _fetch_perp_markets tests
# but miss the internal HTTP code
# ─────────────────────────────────────────────────────────────────────────────


class TestFlashTradeHttpPaths:
    @pytest.mark.asyncio
    async def test_fetch_perp_markets_success_list(self) -> None:
        from instruments_service.reference_data.adapters.defi.flash_trade import FlashTradeReferenceDataAdapter

        adapter = FlashTradeReferenceDataAdapter()
        data = [{"name": "BTC-USDC", "isActive": True}]
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(return_value=data)),
        ):
            markets = await adapter._fetch_perp_markets()
        assert markets == data

    @pytest.mark.asyncio
    async def test_fetch_perp_markets_success_dict_markets_key(self) -> None:
        from instruments_service.reference_data.adapters.defi.flash_trade import FlashTradeReferenceDataAdapter

        adapter = FlashTradeReferenceDataAdapter()
        inner = [{"name": "ETH-USDC", "isActive": True}]
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(return_value={"markets": inner})),
        ):
            markets = await adapter._fetch_perp_markets()
        assert markets == inner

    @pytest.mark.asyncio
    async def test_fetch_perp_markets_empty_dict_raises(self) -> None:
        from instruments_service.reference_data.adapters.defi.flash_trade import FlashTradeReferenceDataAdapter

        # A 200 response that is an empty/keyless dict ({}) carries NONE of the expected list keys —
        # it is a malformed/error envelope, NOT an empty universe. It must RAISE so discovery records
        # attempted_failed rather than silently returning [] (DeFi-plan A8b). A genuinely-empty
        # response arrives as a bare [] or {"markets": []}, both of which still return [].
        adapter = FlashTradeReferenceDataAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(return_value={})),
            pytest.raises(ConnectionError),
        ):
            await adapter._fetch_perp_markets()

    @pytest.mark.asyncio
    async def test_fetch_perp_markets_client_error_raises(self) -> None:
        from instruments_service.reference_data.adapters.defi.flash_trade import FlashTradeReferenceDataAdapter

        adapter = FlashTradeReferenceDataAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(side_effect=aiohttp.ClientError("failed"))),
            patch("instruments_service.reference_data.adapters.defi.flash_trade.log_event"),
            pytest.raises(ConnectionError),
        ):
            await adapter._fetch_perp_markets()

    @pytest.mark.asyncio
    async def test_fetch_perp_markets_runtime_error_reraises(self) -> None:
        from instruments_service.reference_data.adapters.defi.flash_trade import FlashTradeReferenceDataAdapter

        adapter = FlashTradeReferenceDataAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(side_effect=RuntimeError("timeout"))),
            pytest.raises(RuntimeError),
        ):
            await adapter._fetch_perp_markets()

    def test_log_fetch_error_executes(self) -> None:
        from instruments_service.reference_data.adapters.defi.flash_trade import FlashTradeReferenceDataAdapter

        adapter = FlashTradeReferenceDataAdapter()
        with patch("instruments_service.reference_data.adapters.defi.flash_trade.log_event") as mock_log:
            adapter._log_fetch_error(aiohttp.ClientError("test error"), "markets")
        mock_log.assert_called_once()


class TestMangoHttpPaths:
    @pytest.mark.asyncio
    async def test_fetch_perp_markets_success(self) -> None:
        from instruments_service.reference_data.adapters.defi.mango import MangoReferenceDataAdapter

        adapter = MangoReferenceDataAdapter()
        data = [{"name": "BTC-PERP", "marketType": "perpV2"}]
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(return_value=data)),
        ):
            markets = await adapter._fetch_perp_markets()
        assert markets is not None

    @pytest.mark.asyncio
    async def test_fetch_perp_markets_client_error_raises(self) -> None:
        from instruments_service.reference_data.adapters.defi.mango import MangoReferenceDataAdapter

        adapter = MangoReferenceDataAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(side_effect=aiohttp.ClientError("fail"))),
            patch("instruments_service.reference_data.adapters.defi.mango.log_event"),
            pytest.raises(ConnectionError),
        ):
            await adapter._fetch_perp_markets()

    @pytest.mark.asyncio
    async def test_fetch_perp_markets_runtime_error_reraises(self) -> None:
        from instruments_service.reference_data.adapters.defi.mango import MangoReferenceDataAdapter

        adapter = MangoReferenceDataAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(side_effect=RuntimeError("timeout"))),
            pytest.raises(RuntimeError),
        ):
            await adapter._fetch_perp_markets()

    def test_log_fetch_error_executes(self) -> None:
        from instruments_service.reference_data.adapters.defi.mango import MangoReferenceDataAdapter

        adapter = MangoReferenceDataAdapter()
        with patch("instruments_service.reference_data.adapters.defi.mango.log_event") as mock_log:
            adapter._log_fetch_error(aiohttp.ClientError("test error"), "markets")
        mock_log.assert_called_once()


class TestMeteoraHttpPaths:
    @pytest.mark.asyncio
    async def test_fetch_pools_success(self) -> None:
        from instruments_service.reference_data.adapters.defi.meteora import MeteoraReferenceDataAdapter

        adapter = MeteoraReferenceDataAdapter()
        data = {"groups": []}
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(return_value=data)),
        ):
            result = await adapter.get_instruments()
        assert isinstance(result, list)

    @pytest.mark.asyncio
    async def test_fetch_pools_client_error_raises(self) -> None:
        from instruments_service.reference_data.adapters.defi.meteora import MeteoraReferenceDataAdapter

        adapter = MeteoraReferenceDataAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(side_effect=aiohttp.ClientError("fail"))),
            patch("instruments_service.reference_data.adapters.defi.meteora.log_event"),
            pytest.raises(ConnectionError),
        ):
            await adapter._fetch_pools()

    @pytest.mark.asyncio
    async def test_fetch_pools_runtime_error_reraises(self) -> None:
        from instruments_service.reference_data.adapters.defi.meteora import MeteoraReferenceDataAdapter

        adapter = MeteoraReferenceDataAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(side_effect=RuntimeError("timeout"))),
            pytest.raises(RuntimeError),
        ):
            await adapter._fetch_pools()


class TestZetaHttpPaths:
    @pytest.mark.asyncio
    async def test_fetch_perp_markets_success(self) -> None:
        from instruments_service.reference_data.adapters.defi.zeta import ZetaReferenceDataAdapter

        adapter = ZetaReferenceDataAdapter()
        data = [{"asset": "BTC", "isActive": True}]
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(return_value=data)),
        ):
            markets = await adapter._fetch_perp_markets()
        assert markets is not None

    @pytest.mark.asyncio
    async def test_fetch_perp_markets_client_error_raises(self) -> None:
        from instruments_service.reference_data.adapters.defi.zeta import ZetaReferenceDataAdapter

        adapter = ZetaReferenceDataAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(side_effect=aiohttp.ClientError("fail"))),
            patch("instruments_service.reference_data.adapters.defi.zeta.log_event"),
            pytest.raises(ConnectionError),
        ):
            await adapter._fetch_perp_markets()

    @pytest.mark.asyncio
    async def test_fetch_perp_markets_runtime_error_reraises(self) -> None:
        from instruments_service.reference_data.adapters.defi.zeta import ZetaReferenceDataAdapter

        adapter = ZetaReferenceDataAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(side_effect=RuntimeError("timeout"))),
            pytest.raises(RuntimeError),
        ):
            await adapter._fetch_perp_markets()

    def test_log_fetch_error_executes(self) -> None:
        from instruments_service.reference_data.adapters.defi.zeta import ZetaReferenceDataAdapter

        adapter = ZetaReferenceDataAdapter()
        with patch("instruments_service.reference_data.adapters.defi.zeta.log_event") as mock_log:
            adapter._log_fetch_error(aiohttp.ClientError("test error"), "markets")
        mock_log.assert_called_once()


class TestPhoenixHttpPaths:
    @pytest.mark.asyncio
    async def test_fetch_perp_markets_success(self) -> None:
        from instruments_service.reference_data.adapters.defi.phoenix import PhoenixReferenceDataAdapter

        adapter = PhoenixReferenceDataAdapter()
        data: object = []
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(return_value=data)),
        ):
            result = await adapter.get_instruments()
        assert isinstance(result, list)

    @pytest.mark.asyncio
    async def test_fetch_markets_client_error_raises(self) -> None:
        from instruments_service.reference_data.adapters.defi.phoenix import PhoenixReferenceDataAdapter

        adapter = PhoenixReferenceDataAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(side_effect=aiohttp.ClientError("fail"))),
            patch("instruments_service.reference_data.adapters.defi.phoenix.log_event"),
            pytest.raises(ConnectionError),
        ):
            await adapter._fetch_markets()


class TestLifinityHttpPaths:
    @pytest.mark.asyncio
    async def test_fetch_perp_markets_success(self) -> None:
        from instruments_service.reference_data.adapters.defi.lifinity import LifinityReferenceDataAdapter

        adapter = LifinityReferenceDataAdapter()
        data: object = []
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(return_value=data)),
        ):
            result = await adapter.get_instruments()
        assert isinstance(result, list)

    @pytest.mark.asyncio
    async def test_fetch_pools_client_error_raises(self) -> None:
        from instruments_service.reference_data.adapters.defi.lifinity import LifinityReferenceDataAdapter

        adapter = LifinityReferenceDataAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(side_effect=aiohttp.ClientError("fail"))),
            patch("instruments_service.reference_data.adapters.defi.lifinity.log_event"),
            pytest.raises(ConnectionError),
        ):
            await adapter._fetch_pools()
