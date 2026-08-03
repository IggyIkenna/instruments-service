"""Coverage tests for Lighter, Extended and HTTP-path coverage for
existing adapter tests that mock at _fetch_perp_markets level (leaving the
internal HTTP code uncovered).

(Pacifica coverage removed 2026-07-16 -- operator ruling: all Solana perp
DEXes dropped except Jupiter, not integrated; the pacifica.py adapter was
deleted in the same landing. SSOT: unified-trading-pm/codex/04-architecture/
solana-defi-coverage.md.)

All tests are credential-free and use unittest.mock.
"""

from __future__ import annotations

from decimal import Decimal
from typing import ClassVar
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
        from instruments_service.reference_data.adapters.cefi.lighter import LighterReferenceDataAdapter

        assert LighterReferenceDataAdapter().venue == "LIGHTER-ZKSYNC"

    def test_classify_rate_limit_status(self) -> None:
        from instruments_service.reference_data.adapters.cefi.lighter import _classify_lighter_error

        assert _classify_lighter_error(Exception("err"), status=429) == "RATE_LIMIT"

    def test_classify_rate_limit_message(self) -> None:
        from instruments_service.reference_data.adapters.cefi.lighter import _classify_lighter_error

        assert _classify_lighter_error(Exception("rate limit exceeded")) == "RATE_LIMIT"

    def test_classify_500(self) -> None:
        from instruments_service.reference_data.adapters.cefi.lighter import _classify_lighter_error

        assert _classify_lighter_error(Exception("err"), status=503) == "500"

    def test_classify_unknown(self) -> None:
        from instruments_service.reference_data.adapters.cefi.lighter import _classify_lighter_error

        assert _classify_lighter_error(Exception("network error")) == "UNKNOWN"

    @pytest.mark.asyncio
    async def test_get_instruments_wrong_type_returns_empty(self) -> None:
        from instruments_service.reference_data.adapters.cefi.lighter import LighterReferenceDataAdapter

        adapter = LighterReferenceDataAdapter()
        results = await adapter.get_instruments(instrument_type="SPOT_PAIR")
        assert results == []

    @pytest.mark.asyncio
    async def test_get_instruments_success(self) -> None:
        from instruments_service.reference_data.adapters.cefi.lighter import LighterReferenceDataAdapter

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
        # Canonical instrument_id VENUE:PERPETUAL:BASE-QUOTE@LIN, routed through the
        # shared UAC builder (2026-07-09 retrofit, canonical_id_builder_retrofit_
        # checklist_2026_07_08.md todo 4). 2026-07-09 (same day, later) —
        # PERPETUAL scope-expansion added the real @LIN margin marker (LIGHTER
        # confirmed linear-margined: docs.lighter.xyz/trading/multi-asset-margin
        # states "Portfolio Balance is the USDC value of the account" venue-wide).
        keys = {r.instrument_key for r in results}
        assert keys == {"LIGHTER-ZKSYNC:PERPETUAL:BTC-USDC@LIN", "LIGHTER-ZKSYNC:PERPETUAL:ETH-USDC@LIN"}

    @pytest.mark.asyncio
    async def test_get_instruments_non_dict_response(self) -> None:
        from instruments_service.reference_data.adapters.cefi.lighter import LighterReferenceDataAdapter

        adapter = LighterReferenceDataAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(return_value=[])),
        ):
            results = await adapter.get_instruments()
        assert results == []

    @pytest.mark.asyncio
    async def test_get_instruments_empty_details(self) -> None:
        from instruments_service.reference_data.adapters.cefi.lighter import LighterReferenceDataAdapter

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
        from instruments_service.reference_data.adapters.cefi.lighter import LighterReferenceDataAdapter

        adapter = LighterReferenceDataAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(side_effect=aiohttp.ClientError("failed"))),
            patch("instruments_service.reference_data.adapters.cefi.lighter.log_event"),
            pytest.raises((RuntimeError, aiohttp.ClientError)),
        ):
            await adapter.get_instruments()

    @pytest.mark.asyncio
    async def test_get_instruments_runtime_error_raises(self) -> None:
        """RuntimeError from _get_with_retry must raise, not return [] (CF-11 regression)."""
        from instruments_service.reference_data.adapters.cefi.lighter import LighterReferenceDataAdapter

        adapter = LighterReferenceDataAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(side_effect=RuntimeError("timeout"))),
            pytest.raises((RuntimeError, aiohttp.ClientError)),
        ):
            await adapter.get_instruments()

    @pytest.mark.asyncio
    async def test_get_instrument_found(self) -> None:
        from instruments_service.reference_data.adapters.cefi.lighter import LighterReferenceDataAdapter

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
        from instruments_service.reference_data.adapters.cefi.lighter import LighterReferenceDataAdapter

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
        from instruments_service.reference_data.adapters.cefi.lighter import LighterReferenceDataAdapter

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
        from instruments_service.reference_data.adapters.cefi.lighter import LighterReferenceDataAdapter

        adapter = LighterReferenceDataAdapter()
        raw = {"order_book_details": [{"symbol": "BTCUSD", "market_type": "perp"}]}
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(return_value=raw)),
        ):
            results = await adapter.get_instruments()
        assert results[0].base_asset == "BTCUSD"


class TestLighterMarketIndexResolution:
    """market_id -> symbol resolution (2026-07-23 investigation).

    Fixture market_id/symbol pairs below are the REAL values live-curled from
    Lighter's public /orderBookDetails on 2026-07-23 (0->ETH, 1->BTC,
    133->BIRB inactive-but-listed) — not fabricated placeholders.
    """

    _REAL_ORDER_BOOK_DETAILS: ClassVar[dict[str, list[dict[str, object]]]] = {
        "order_book_details": [
            {"market_id": 0, "symbol": "ETH", "market_type": "perp"},
            {"market_id": 1, "symbol": "BTC", "market_type": "perp"},
            # Inactive markets stay listed (never disappear) per the live
            # investigation — must still resolve honestly for historical objects
            # captured while BIRB was active.
            {"market_id": 133, "symbol": "BIRB", "market_type": "perp", "status": "inactive"},
        ]
    }

    @pytest.mark.asyncio
    async def test_resolve_market_index_known_real_pairs(self) -> None:
        from instruments_service.reference_data.adapters.cefi.lighter import LighterReferenceDataAdapter

        adapter = LighterReferenceDataAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(return_value=self._REAL_ORDER_BOOK_DETAILS)),
        ):
            assert await adapter.resolve_market_index("0") == "ETH"
            assert await adapter.resolve_market_index("1") == "BTC"
            # Inactive-but-listed market still resolves — no id reuse/disappearance.
            assert await adapter.resolve_market_index("133") == "BIRB"

    @pytest.mark.asyncio
    async def test_resolve_market_index_unknown_falls_through_honestly(self) -> None:
        """A market_id absent from the live universe returns None — never a fabricated guess."""
        from instruments_service.reference_data.adapters.cefi.lighter import LighterReferenceDataAdapter

        adapter = LighterReferenceDataAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(return_value=self._REAL_ORDER_BOOK_DETAILS)),
        ):
            result = await adapter.resolve_market_index("9999")
        assert result is None

    @pytest.mark.asyncio
    async def test_resolve_market_index_non_numeric_returns_none_without_fetch(self) -> None:
        """A non-digit string isn't a market_id — short-circuits before any network call."""
        from instruments_service.reference_data.adapters.cefi.lighter import LighterReferenceDataAdapter

        adapter = LighterReferenceDataAdapter()
        with patch.object(adapter, "_make_session", side_effect=AssertionError("must not fetch")):
            result = await adapter.resolve_market_index("BTC-USDC")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_instrument_by_market_index_resolves_to_correct_symbol(self) -> None:
        """A known real market_index resolves through get_instrument() to its InstrumentRecord."""
        from instruments_service.reference_data.adapters.cefi.lighter import LighterReferenceDataAdapter

        adapter = LighterReferenceDataAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(return_value=self._REAL_ORDER_BOOK_DETAILS)),
        ):
            found = await adapter.get_instrument("0")
        assert found is not None
        assert found.raw_symbol == "ETH"

    @pytest.mark.asyncio
    async def test_get_instrument_unknown_market_index_falls_through_honestly(self) -> None:
        """An unresolvable numeric market_id returns None rather than a fabricated match."""
        from instruments_service.reference_data.adapters.cefi.lighter import LighterReferenceDataAdapter

        adapter = LighterReferenceDataAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(return_value=self._REAL_ORDER_BOOK_DETAILS)),
        ):
            result = await adapter.get_instrument("9999")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_instrument_non_numeric_symbol_unaffected(self) -> None:
        """Existing non-numeric-stem symbol lookups are unaffected by the market_index wiring."""
        from instruments_service.reference_data.adapters.cefi.lighter import LighterReferenceDataAdapter

        adapter = LighterReferenceDataAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(return_value=self._REAL_ORDER_BOOK_DETAILS)),
        ):
            found = await adapter.get_instrument("ETH")
            not_found = await adapter.get_instrument("XYZ")
        assert found is not None
        assert found.raw_symbol == "ETH"
        assert not_found is None


# ─────────────────────────────────────────────────────────────────────────────
# Extended adapter
# ─────────────────────────────────────────────────────────────────────────────


class TestExtendedAdapter:
    def test_venue_name(self) -> None:
        from instruments_service.reference_data.adapters.cefi.extended import ExtendedReferenceDataAdapter

        assert ExtendedReferenceDataAdapter().venue == "EXTENDED-STARKNET"

    def test_classify_rate_limit_status(self) -> None:
        from instruments_service.reference_data.adapters.cefi.extended import _classify_extended_error

        assert _classify_extended_error(Exception("err"), status=429) == "RATE_LIMIT"

    def test_classify_rate_limit_message(self) -> None:
        from instruments_service.reference_data.adapters.cefi.extended import _classify_extended_error

        assert _classify_extended_error(Exception("rate exceeded")) == "RATE_LIMIT"

    def test_classify_500(self) -> None:
        from instruments_service.reference_data.adapters.cefi.extended import _classify_extended_error

        assert _classify_extended_error(Exception("err"), status=500) == "500"

    def test_classify_unknown(self) -> None:
        from instruments_service.reference_data.adapters.cefi.extended import _classify_extended_error

        assert _classify_extended_error(Exception("conn reset")) == "UNKNOWN"

    @pytest.mark.asyncio
    async def test_get_instruments_wrong_type_returns_empty(self) -> None:
        from instruments_service.reference_data.adapters.cefi.extended import ExtendedReferenceDataAdapter

        adapter = ExtendedReferenceDataAdapter()
        results = await adapter.get_instruments(instrument_type="SPOT_PAIR")
        assert results == []

    @pytest.mark.asyncio
    async def test_get_instruments_success(self) -> None:
        from instruments_service.reference_data.adapters.cefi.extended import ExtendedReferenceDataAdapter

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
        # Canonical instrument_id VENUE:PERPETUAL:SYMBOL@LIN, routed through the
        # shared UAC builder (2026-07-09 retrofit, canonical_id_builder_retrofit_
        # checklist_2026_07_08.md todo 4). Extended's symbol is already
        # dash-normalized ("BTC-USD"), passed straight through. 2026-07-09 (same
        # day, later) — PERPETUAL scope-expansion added the real @LIN margin
        # marker (EXTENDED confirmed linear-margined: docs.extended.exchange
        # states "USDC as the base collateral", uniformly USDC-settled markets).
        keys = {r.instrument_key for r in results}
        assert keys == {"EXTENDED-STARKNET:PERPETUAL:BTC-USD@LIN", "EXTENDED-STARKNET:PERPETUAL:ETH-USD@LIN"}

    @pytest.mark.asyncio
    async def test_get_instruments_empty_active_falls_back(self) -> None:
        from instruments_service.reference_data.adapters.cefi.extended import (
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
        from instruments_service.reference_data.adapters.cefi.extended import (
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
        from instruments_service.reference_data.adapters.cefi.extended import (
            _EXTENDED_FALLBACK_MARKETS,
            ExtendedReferenceDataAdapter,
        )

        adapter = ExtendedReferenceDataAdapter()
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(side_effect=aiohttp.ClientError("failed"))),
            patch("instruments_service.reference_data.adapters.cefi.extended.log_event") as mock_log_event,
        ):
            results = await adapter.get_instruments()

        assert len(results) == len(_EXTENDED_FALLBACK_MARKETS)
        # Contract (adapter_contract_baseline.yaml count=3): a ClientError MUST
        # emit ADAPTER_FETCH_FAILED — the honest failure signal — even though
        # Extended degrades to the fallback list rather than raising. Asserting
        # the emit (not just patching it) locks it against the lint-sweep
        # regression class (deleted contract calls) at the TEST level, not only
        # the count baseline.
        assert any(call.args and call.args[0] == "ADAPTER_FETCH_FAILED" for call in mock_log_event.call_args_list), (
            "ClientError on /info/markets must emit ADAPTER_FETCH_FAILED"
        )

    @pytest.mark.asyncio
    async def test_get_instruments_runtime_error_uses_fallback(self) -> None:
        from instruments_service.reference_data.adapters.cefi.extended import (
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
        from instruments_service.reference_data.adapters.cefi.extended import ExtendedReferenceDataAdapter

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
        from instruments_service.reference_data.adapters.cefi.extended import ExtendedReferenceDataAdapter

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
        from instruments_service.reference_data.adapters.cefi.extended import ExtendedReferenceDataAdapter

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
        from instruments_service.reference_data.adapters.cefi.extended import ExtendedReferenceDataAdapter

        adapter = ExtendedReferenceDataAdapter()
        raw = {"data": [{"name": "BTCUSD", "active": True, "status": "ACTIVE"}]}
        with (
            patch.object(adapter, "_make_session", return_value=_make_session_cm()),
            patch.object(adapter, "_get_with_retry", AsyncMock(return_value=raw)),
        ):
            results = await adapter.get_instruments()
        assert results[0].base_asset == "BTCUSD"


# ─────────────────────────────────────────────────────────────────────────────
# HTTP-path coverage for adapters that already have _fetch_perp_markets tests
# but miss the internal HTTP code
# ─────────────────────────────────────────────────────────────────────────────


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
