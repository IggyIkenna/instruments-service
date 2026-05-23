"""Comprehensive unit tests for DeFi reference data adapters — covering get_instruments,
error handling, parsing logic, and edge cases to maximize code coverage.

All tests are credential-free and use unittest.mock. No live network calls.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest
from unified_api_contracts.internal import InstrumentRecord, InstrumentStatus, InstrumentType

# ── shared helpers ─────────────────────────────────────────────────────────────


def _make_instrument(
    raw_symbol: str = "BTCUSDT",
    instrument_type: str = "SPOT_PAIR",
    base_asset: str = "BTC",
    quote_asset: str = "USDT",
    venue: str = "test",
) -> InstrumentRecord:
    return InstrumentRecord(
        instrument_key=raw_symbol,
        venue=venue,
        raw_symbol=raw_symbol,
        instrument_type=instrument_type,
        base_asset=base_asset,
        quote_asset=quote_asset,
        tick_size=Decimal("0.01"),
        lot_size=Decimal("0.001"),
        contract_size=Decimal("1"),
        expiry=None,
        strike=None,
        option_type=None,
    )


def _mock_aiohttp_session_post(json_data: dict[str, object]) -> MagicMock:
    """Create a mock aiohttp session that returns json_data on POST."""
    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json = AsyncMock(return_value=json_data)

    mock_cm = MagicMock()
    mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_cm.__aexit__ = AsyncMock(return_value=None)

    mock_session = MagicMock()
    mock_session.post = MagicMock(return_value=mock_cm)
    mock_session.get = MagicMock(return_value=mock_cm)

    mock_session_cm = MagicMock()
    mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_cm.__aexit__ = AsyncMock(return_value=None)

    return mock_session_cm


def _mock_aiohttp_session_error(exc: Exception) -> MagicMock:
    """Create a mock aiohttp session that raises exc on POST."""
    mock_cm = MagicMock()
    mock_cm.__aenter__ = AsyncMock(side_effect=exc)
    mock_cm.__aexit__ = AsyncMock(return_value=None)

    mock_session = MagicMock()
    mock_session.post = MagicMock(return_value=mock_cm)
    mock_session.get = MagicMock(return_value=mock_cm)

    mock_session_cm = MagicMock()
    mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_cm.__aexit__ = AsyncMock(return_value=None)

    return mock_session_cm


# ── defi_utils additional coverage ────────────────────────────────────────────


class TestDefiUtilsAdditional:
    """Additional coverage for defi_utils not covered by test_defi_utils.py."""

    def test_parse_created_timestamp_zero_returns_none(self) -> None:
        from instruments_service.reference_data.utils.defi_utils import parse_created_timestamp

        assert parse_created_timestamp(0) is None

    def test_parse_created_timestamp_negative_returns_none(self) -> None:
        from instruments_service.reference_data.utils.defi_utils import parse_created_timestamp

        assert parse_created_timestamp(-1) is None

    def test_parse_created_timestamp_string_zero_returns_none(self) -> None:
        from instruments_service.reference_data.utils.defi_utils import parse_created_timestamp

        assert parse_created_timestamp("0") is None

    def test_classify_graph_error_unavailable_in_message(self) -> None:
        from instruments_service.reference_data.utils.defi_utils import classify_graph_error

        assert classify_graph_error(Exception("service unavailable")) == "503"

    def test_order_base_quote_wbtc_is_quote(self) -> None:
        from instruments_service.reference_data.utils.defi_utils import order_base_quote

        base, quote = order_base_quote("AAVE", "WBTC")
        assert base == "AAVE"
        assert quote == "WBTC"

    def test_order_base_quote_eth_is_quote(self) -> None:
        from instruments_service.reference_data.utils.defi_utils import order_base_quote

        base, quote = order_base_quote("ETH", "UNI")
        assert base == "UNI"
        assert quote == "ETH"

    def test_order_base_quote_usde_is_quote(self) -> None:
        from instruments_service.reference_data.utils.defi_utils import order_base_quote

        base, quote = order_base_quote("USDE", "CRV")
        assert base == "CRV"
        assert quote == "USDE"


# ── UniswapV3ReferenceDataAdapter ─────────────────────────────────────────────


class TestUniswapV3Adapter:
    def test_venue_property(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v3 import UniswapV3ReferenceDataAdapter

        adapter = UniswapV3ReferenceDataAdapter()
        assert adapter.venue == "uniswap_v3"

    def test_venue_with_protocol_slug(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v3 import UniswapV3ReferenceDataAdapter

        adapter = UniswapV3ReferenceDataAdapter(protocol_slug="pancakeswap_v3")
        assert adapter.venue == "pancakeswap_v3"

    @pytest.mark.asyncio
    async def test_get_instruments_wrong_type_returns_empty(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v3 import UniswapV3ReferenceDataAdapter

        adapter = UniswapV3ReferenceDataAdapter()
        result = await adapter.get_instruments(instrument_type="FUTURE")
        assert result == []

    @pytest.mark.asyncio
    async def test_get_instruments_no_api_key_returns_empty(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v3 import UniswapV3ReferenceDataAdapter

        adapter = UniswapV3ReferenceDataAdapter()  # no api_key
        result = await adapter.get_instruments()
        assert result == []

    @pytest.mark.asyncio
    async def test_get_instruments_success_with_pools(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v3 import UniswapV3ReferenceDataAdapter

        adapter = UniswapV3ReferenceDataAdapter(api_key="test-key", chain="ETHEREUM")
        pool_data = {
            "data": {
                "pools": [
                    {
                        "id": "0xpool1",
                        "feeTier": "3000",
                        "token0": {"id": "0xt0", "symbol": "WETH", "name": "Wrapped Ether", "decimals": "18"},
                        "token1": {"id": "0xt1", "symbol": "USDC", "name": "USD Coin", "decimals": "6"},
                        "totalValueLockedUSD": "5000000",
                        "createdAtTimestamp": "1677000000",
                    },
                ]
            }
        }
        with (
            patch(
                "instruments_service.reference_data.adapters.defi.uniswap_v3.SUBGRAPH_IDS",
                {"uniswap_v3": {"ETHEREUM": "test-subgraph-id"}},
            ),
            patch("aiohttp.ClientSession", return_value=_mock_aiohttp_session_post(pool_data)),
        ):
            results = await adapter.get_instruments()
        assert len(results) == 1
        assert results[0].instrument_type == InstrumentType.POOL
        assert results[0].base_asset == "WETH"
        assert results[0].quote_asset == "USDC"

    @pytest.mark.asyncio
    async def test_get_instruments_empty_data_response(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v3 import UniswapV3ReferenceDataAdapter

        adapter = UniswapV3ReferenceDataAdapter(api_key="test-key", chain="ETHEREUM")
        with (
            patch(
                "instruments_service.reference_data.adapters.defi.uniswap_v3.SUBGRAPH_IDS",
                {"uniswap_v3": {"ETHEREUM": "test-subgraph-id"}},
            ),
            patch("aiohttp.ClientSession", return_value=_mock_aiohttp_session_post({"data": {"pools": []}})),
        ):
            results = await adapter.get_instruments()
        # Empty pools → falls through to SushiSwap → Messari → returns []
        assert results == []

    @pytest.mark.asyncio
    async def test_get_instruments_http_error_raises(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v3 import UniswapV3ReferenceDataAdapter

        adapter = UniswapV3ReferenceDataAdapter(api_key="test-key", chain="ETHEREUM")
        with (
            patch(
                "instruments_service.reference_data.adapters.defi.uniswap_v3.SUBGRAPH_IDS",
                {"uniswap_v3": {"ETHEREUM": "test-subgraph-id"}},
            ),
            patch(
                "aiohttp.ClientSession",
                return_value=_mock_aiohttp_session_error(aiohttp.ClientError("Connection refused")),
            ),
            pytest.raises(ConnectionError),
        ):
            await adapter.get_instruments()

    def test_build_pool_record_missing_id_returns_none(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v3 import UniswapV3ReferenceDataAdapter

        adapter = UniswapV3ReferenceDataAdapter()
        result = adapter._build_pool_record({})
        assert result is None

    def test_build_pool_record_missing_token_dicts_returns_none(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v3 import UniswapV3ReferenceDataAdapter

        adapter = UniswapV3ReferenceDataAdapter()
        result = adapter._build_pool_record({"id": "0x123", "token0": "not-dict", "token1": "not-dict"})
        assert result is None

    def test_build_pool_record_empty_symbols_returns_none(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v3 import UniswapV3ReferenceDataAdapter

        adapter = UniswapV3ReferenceDataAdapter()
        result = adapter._build_pool_record(
            {
                "id": "0x123",
                "token0": {"symbol": ""},
                "token1": {"symbol": "USDC"},
            }
        )
        assert result is None

    def test_build_pool_record_valid_returns_instrument(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v3 import UniswapV3ReferenceDataAdapter

        adapter = UniswapV3ReferenceDataAdapter()
        result = adapter._build_pool_record(
            {
                "id": "0xabc",
                "feeTier": "3000",
                "token0": {"id": "0xt0", "symbol": "LINK", "name": "Chainlink", "decimals": "18"},
                "token1": {"id": "0xt1", "symbol": "USDC", "name": "USD Coin", "decimals": "6"},
                "totalValueLockedUSD": "1000000",
                "createdAtTimestamp": "1677000000",
            }
        )
        assert result is not None
        assert result.instrument_type == InstrumentType.POOL
        assert result.base_asset == "LINK"
        assert result.quote_asset == "USDC"
        assert result.available_from_datetime is not None

    def test_build_pool_record_no_fee_tier(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v3 import UniswapV3ReferenceDataAdapter

        adapter = UniswapV3ReferenceDataAdapter()
        result = adapter._build_pool_record(
            {
                "id": "0xabc",
                "feeTier": None,
                "token0": {"symbol": "AAVE", "decimals": "18"},
                "token1": {"symbol": "ETH", "decimals": "18"},
            }
        )
        assert result is not None
        assert ":0" in result.instrument_key

    def test_log_fetch_error_classifies(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v3 import UniswapV3ReferenceDataAdapter

        adapter = UniswapV3ReferenceDataAdapter()
        with patch("instruments_service.reference_data.adapters.defi.uniswap_v3.log_event"):
            adapter._log_fetch_error(aiohttp.ClientError("timeout"))

    @pytest.mark.asyncio
    async def test_resolve_block_num_no_date(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v3 import UniswapV3ReferenceDataAdapter

        adapter = UniswapV3ReferenceDataAdapter()
        result = await adapter._resolve_block_num()
        assert result is None

    @pytest.mark.asyncio
    async def test_resolve_block_num_with_date(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v3 import UniswapV3ReferenceDataAdapter

        adapter = UniswapV3ReferenceDataAdapter(date="2024-01-01")
        with patch(
            "instruments_service.reference_data.adapters.defi.uniswap_v3.date_to_block",
            return_value=18000000,
        ):
            result = await adapter._resolve_block_num()
        assert result == 18000000

    @pytest.mark.asyncio
    async def test_get_instrument_found(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v3 import UniswapV3ReferenceDataAdapter

        adapter = UniswapV3ReferenceDataAdapter()
        inst = _make_instrument(raw_symbol="0xabc", venue="uniswap_v3")
        with patch.object(adapter, "get_instruments", return_value=[inst]):
            result = await adapter.get_instrument("0xabc")
        assert result is inst

    @pytest.mark.asyncio
    async def test_get_instrument_not_found(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v3 import UniswapV3ReferenceDataAdapter

        adapter = UniswapV3ReferenceDataAdapter()
        with patch.object(adapter, "get_instruments", return_value=[]):
            result = await adapter.get_instrument("0xabc")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_options_chain_raises(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v3 import UniswapV3ReferenceDataAdapter

        adapter = UniswapV3ReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_options_chain("ETH")

    @pytest.mark.asyncio
    async def test_get_expiry_calendar_raises(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v3 import UniswapV3ReferenceDataAdapter

        adapter = UniswapV3ReferenceDataAdapter()
        with pytest.raises(NotImplementedError):
            await adapter.get_expiry_calendar("ETH")

    @pytest.mark.asyncio
    async def test_fetch_messari_pools_success(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v3 import UniswapV3ReferenceDataAdapter

        adapter = UniswapV3ReferenceDataAdapter(api_key="test-key")
        messari_data = {
            "data": {
                "liquidityPools": [
                    {
                        "id": "0xpool1",
                        "name": "Test Pool",
                        "inputTokens": [
                            {"id": "0xt0", "symbol": "WETH", "name": "WETH", "decimals": "18"},
                            {"id": "0xt1", "symbol": "USDC", "name": "USDC", "decimals": "6"},
                        ],
                        "fees": [{"feePercentage": "0.3", "feeType": "FIXED_TRADING_FEE"}],
                        "totalValueLockedUSD": "1000000",
                        "createdTimestamp": "1677000000",
                    }
                ]
            }
        }
        with patch("aiohttp.ClientSession", return_value=_mock_aiohttp_session_post(messari_data)):
            pools = await adapter._fetch_messari_pools("https://fake-url.com")
        assert len(pools) == 1
        assert pools[0]["feeTier"] == "3000"

    @pytest.mark.asyncio
    async def test_fetch_messari_pools_http_error(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v3 import UniswapV3ReferenceDataAdapter

        adapter = UniswapV3ReferenceDataAdapter(api_key="test-key")
        with patch(
            "aiohttp.ClientSession",
            return_value=_mock_aiohttp_session_error(aiohttp.ClientError("fail")),
        ):
            pools = await adapter._fetch_messari_pools("https://fake-url.com")
        assert pools == []

    @pytest.mark.asyncio
    async def test_fetch_messari_pools_no_data(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v3 import UniswapV3ReferenceDataAdapter

        adapter = UniswapV3ReferenceDataAdapter(api_key="test-key")
        with patch("aiohttp.ClientSession", return_value=_mock_aiohttp_session_post({"errors": []})):
            pools = await adapter._fetch_messari_pools("https://fake-url.com")
        assert pools == []

    @pytest.mark.asyncio
    async def test_fetch_messari_pools_less_than_2_tokens_skipped(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v3 import UniswapV3ReferenceDataAdapter

        adapter = UniswapV3ReferenceDataAdapter(api_key="test-key")
        data = {
            "data": {
                "liquidityPools": [
                    {
                        "id": "0xpool1",
                        "inputTokens": [{"id": "0xt0", "symbol": "WETH"}],
                        "fees": [],
                        "totalValueLockedUSD": "1000000",
                    }
                ]
            }
        }
        with patch("aiohttp.ClientSession", return_value=_mock_aiohttp_session_post(data)):
            pools = await adapter._fetch_messari_pools("https://fake-url.com")
        assert pools == []

    @pytest.mark.asyncio
    async def test_fetch_algebra_pools_success(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v3 import UniswapV3ReferenceDataAdapter

        adapter = UniswapV3ReferenceDataAdapter(api_key="test-key")
        algebra_data = {
            "data": {
                "pools": [
                    {
                        "id": "0xpool_alg",
                        "feeZtO": "100",
                        "feeOtZ": "200",
                        "token0": {"id": "0xt0", "symbol": "WETH", "name": "WETH", "decimals": "18"},
                        "token1": {"id": "0xt1", "symbol": "USDC", "name": "USDC", "decimals": "6"},
                        "totalValueLockedUSD": "500000",
                        "createdAtTimestamp": "1677000000",
                    }
                ]
            }
        }
        with patch("aiohttp.ClientSession", return_value=_mock_aiohttp_session_post(algebra_data)):
            pools = await adapter._fetch_algebra_pools("https://fake-url.com", None)
        assert len(pools) == 1
        assert pools[0]["feeTier"] == "150"  # (100 + 200) // 2

    @pytest.mark.asyncio
    async def test_fetch_algebra_pools_with_block_num(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v3 import UniswapV3ReferenceDataAdapter

        adapter = UniswapV3ReferenceDataAdapter(api_key="test-key")
        # Empty data → returns []
        with patch("aiohttp.ClientSession", return_value=_mock_aiohttp_session_post({"data": {"pools": []}})):
            pools = await adapter._fetch_algebra_pools("https://fake-url.com", 18000000)
        assert pools == []

    @pytest.mark.asyncio
    async def test_fetch_algebra_pools_http_error(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v3 import UniswapV3ReferenceDataAdapter

        adapter = UniswapV3ReferenceDataAdapter(api_key="test-key")
        with patch(
            "aiohttp.ClientSession",
            return_value=_mock_aiohttp_session_error(aiohttp.ClientError("fail")),
        ):
            pools = await adapter._fetch_algebra_pools("https://fake-url.com", None)
        assert pools == []

    @pytest.mark.asyncio
    async def test_fetch_sushiswap_pairs_success(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v3 import UniswapV3ReferenceDataAdapter

        adapter = UniswapV3ReferenceDataAdapter(api_key="test-key")
        sushi_data = {
            "data": {
                "pairs": [
                    {
                        "id": "0xpair1",
                        "token0": {"id": "0xt0", "symbol": "WETH"},
                        "token1": {"id": "0xt1", "symbol": "DAI"},
                        "liquidityUSD": "2000000",
                        "createdAtTimestamp": "1677000000",
                    }
                ]
            }
        }
        with patch("aiohttp.ClientSession", return_value=_mock_aiohttp_session_post(sushi_data)):
            pairs = await adapter._fetch_sushiswap_pairs("https://fake-url.com")
        assert len(pairs) == 1
        assert pairs[0]["feeTier"] == "3000"

    @pytest.mark.asyncio
    async def test_fetch_sushiswap_pairs_http_error(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v3 import UniswapV3ReferenceDataAdapter

        adapter = UniswapV3ReferenceDataAdapter(api_key="test-key")
        with patch(
            "aiohttp.ClientSession",
            return_value=_mock_aiohttp_session_error(aiohttp.ClientError("fail")),
        ):
            pairs = await adapter._fetch_sushiswap_pairs("https://fake-url.com")
        assert pairs == []

    @pytest.mark.asyncio
    async def test_fetch_sushiswap_pairs_no_data(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v3 import UniswapV3ReferenceDataAdapter

        adapter = UniswapV3ReferenceDataAdapter(api_key="test-key")
        with patch("aiohttp.ClientSession", return_value=_mock_aiohttp_session_post({"no_data": True})):
            pairs = await adapter._fetch_sushiswap_pairs("https://fake-url.com")
        assert pairs == []

    @pytest.mark.asyncio
    async def test_get_instruments_schema_error_triggers_algebra_fallback(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v3 import UniswapV3ReferenceDataAdapter

        adapter = UniswapV3ReferenceDataAdapter(api_key="test-key", chain="ETHEREUM")
        # First call returns schema error, then algebra fallback returns data
        schema_error_data = {
            "errors": [{"message": "Type `Pool` has no field `feeTier`"}],
            "data": None,
        }
        algebra_pool = {
            "id": "0xalg",
            "feeZtO": "100",
            "feeOtZ": "200",
            "token0": {"symbol": "UNI", "decimals": "18"},
            "token1": {"symbol": "USDC", "decimals": "6"},
            "totalValueLockedUSD": "500000",
            "createdAtTimestamp": "1677000000",
        }
        algebra_data = {"data": {"pools": [algebra_pool]}}

        call_count = 0

        def mock_post_side_effect(*_args: object, **_kwargs: object) -> MagicMock:
            nonlocal call_count
            call_count += 1
            mock_r = AsyncMock()
            mock_r.status = 200
            mock_r.raise_for_status = MagicMock()
            if call_count <= 1:
                mock_r.json = AsyncMock(return_value=schema_error_data)
            else:
                mock_r.json = AsyncMock(return_value=algebra_data)
            cm = MagicMock()
            cm.__aenter__ = AsyncMock(return_value=mock_r)
            cm.__aexit__ = AsyncMock(return_value=None)
            return cm

        mock_session = MagicMock()
        mock_session.post = MagicMock(side_effect=mock_post_side_effect)
        mock_session_cm = MagicMock()
        mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session_cm.__aexit__ = AsyncMock(return_value=None)

        with (
            patch(
                "instruments_service.reference_data.adapters.defi.uniswap_v3.SUBGRAPH_IDS",
                {"uniswap_v3": {"ETHEREUM": "test-subgraph-id"}},
            ),
            patch("aiohttp.ClientSession", return_value=mock_session_cm),
        ):
            results = await adapter.get_instruments()
        assert len(results) == 1
        assert results[0].base_asset == "UNI"

    @pytest.mark.asyncio
    async def test_get_instruments_indexer_unavailable_returns_empty(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v3 import UniswapV3ReferenceDataAdapter

        adapter = UniswapV3ReferenceDataAdapter(api_key="test-key", chain="ETHEREUM")
        unavailable_data = {
            "errors": [{"message": "bad indexers for subgraph"}],
            "data": None,
        }
        with (
            patch(
                "instruments_service.reference_data.adapters.defi.uniswap_v3.SUBGRAPH_IDS",
                {"uniswap_v3": {"ETHEREUM": "test-subgraph-id"}},
            ),
            patch("aiohttp.ClientSession", return_value=_mock_aiohttp_session_post(unavailable_data)),
        ):
            results = await adapter.get_instruments()
        assert results == []


# ── RaydiumReferenceDataAdapter ───────────────────────────────────────────────


class TestRaydiumAdapter:
    def test_venue_property(self) -> None:
        from instruments_service.reference_data.adapters.defi.raydium import RaydiumReferenceDataAdapter

        adapter = RaydiumReferenceDataAdapter()
        assert adapter.venue == "RAYDIUM-SOLANA"

    @pytest.mark.asyncio
    async def test_get_instruments_wrong_type(self) -> None:
        from instruments_service.reference_data.adapters.defi.raydium import RaydiumReferenceDataAdapter

        adapter = RaydiumReferenceDataAdapter()
        result = await adapter.get_instruments(instrument_type="FUTURE")
        assert result == []

    @pytest.mark.asyncio
    async def test_get_instruments_success(self) -> None:
        from instruments_service.reference_data.adapters.defi.raydium import RaydiumReferenceDataAdapter

        adapter = RaydiumReferenceDataAdapter()
        pool_data = {
            "data": {
                "data": [
                    {
                        "id": "pool_addr_1",
                        "mintA": {"symbol": "SOL", "decimals": 9},
                        "mintB": {"symbol": "USDC", "decimals": 6},
                        "tvl": 50000,
                        "openTime": "1677000000",
                        "type": "Concentrated",
                    }
                ],
                "count": 1,
            }
        }
        with (
            patch.object(adapter, "_get_with_retry", return_value=pool_data),
            patch(
                "instruments_service.reference_data.adapters.defi.raydium.batch_resolve_creation_timestamps",
                return_value={},
            ),
        ):
            results = await adapter.get_instruments()
        assert len(results) == 1
        assert results[0].base_asset == "SOL"
        assert results[0].quote_asset == "USDC"

    @pytest.mark.asyncio
    async def test_get_instruments_low_tvl_filtered(self) -> None:
        from instruments_service.reference_data.adapters.defi.raydium import RaydiumReferenceDataAdapter

        adapter = RaydiumReferenceDataAdapter()
        pool_data = {
            "data": {
                "data": [
                    {
                        "id": "pool_addr_low",
                        "mintA": {"symbol": "MEME"},
                        "mintB": {"symbol": "USDC"},
                        "tvl": 100,  # Below $10k threshold
                        "openTime": "1677000000",
                    }
                ]
            }
        }
        with patch.object(adapter, "_get_with_retry", return_value=pool_data):
            results = await adapter.get_instruments()
        assert results == []

    @pytest.mark.asyncio
    async def test_get_instruments_http_error(self) -> None:
        from instruments_service.reference_data.adapters.defi.raydium import RaydiumReferenceDataAdapter

        adapter = RaydiumReferenceDataAdapter()
        with (
            patch.object(adapter, "_get_with_retry", side_effect=aiohttp.ClientError("fail")),
            patch("instruments_service.reference_data.adapters.defi.raydium.log_event"),
            pytest.raises(ConnectionError),
        ):
            await adapter.get_instruments()

    def test_build_pool_record_missing_id(self) -> None:
        from instruments_service.reference_data.adapters.defi.raydium import RaydiumReferenceDataAdapter

        adapter = RaydiumReferenceDataAdapter()
        assert adapter._build_pool_record({}) is None

    def test_build_pool_record_missing_symbols(self) -> None:
        from instruments_service.reference_data.adapters.defi.raydium import RaydiumReferenceDataAdapter

        adapter = RaydiumReferenceDataAdapter()
        assert adapter._build_pool_record({"id": "x", "mintA": {}, "mintB": {}}) is None

    def test_extract_token_symbol_dict(self) -> None:
        from instruments_service.reference_data.adapters.defi.raydium import RaydiumReferenceDataAdapter

        adapter = RaydiumReferenceDataAdapter()
        result = adapter._extract_token_symbol({"mintA": {"symbol": "sol"}}, "mintA")
        assert result == "SOL"

    def test_extract_token_symbol_flat_field(self) -> None:
        from instruments_service.reference_data.adapters.defi.raydium import RaydiumReferenceDataAdapter

        adapter = RaydiumReferenceDataAdapter()
        result = adapter._extract_token_symbol({"mintSymbolA": "USDC"}, "mintA")
        assert result == "USDC"

    def test_extract_token_symbol_missing(self) -> None:
        from instruments_service.reference_data.adapters.defi.raydium import RaydiumReferenceDataAdapter

        adapter = RaydiumReferenceDataAdapter()
        result = adapter._extract_token_symbol({}, "mintA")
        assert result == ""

    def test_classify_raydium_error(self) -> None:
        from instruments_service.reference_data.adapters.defi.raydium import _classify_raydium_error

        assert _classify_raydium_error(Exception("msg"), status=429) == "RATE_LIMIT"
        assert _classify_raydium_error(Exception("msg"), status=503) == "503"
        assert _classify_raydium_error(Exception("msg"), status=500) == "500"
        assert _classify_raydium_error(Exception("rate limit")) == "RATE_LIMIT"
        assert _classify_raydium_error(Exception("internal server")) == "500"
        assert _classify_raydium_error(Exception("unknown")) == "UNKNOWN"

    def test_build_historical_pool_record(self) -> None:
        from instruments_service.reference_data.adapters.defi.raydium import RaydiumReferenceDataAdapter

        adapter = RaydiumReferenceDataAdapter()
        record = adapter._build_historical_pool_record("pool_123abc")
        assert record is not None
        assert record.status == InstrumentStatus.DELISTED
        assert record.base_asset == "UNKNOWN"
        assert "Historical" in record.instrument_key

    @pytest.mark.asyncio
    async def test_get_instruments_with_historical(self) -> None:
        from instruments_service.reference_data.adapters.defi.raydium import RaydiumReferenceDataAdapter

        adapter = RaydiumReferenceDataAdapter()
        pool_data = {"data": {"data": [], "count": 0}}
        with (
            patch.object(adapter, "_get_with_retry", return_value=pool_data),
            patch(
                "instruments_service.reference_data.adapters.defi.raydium.discover_program_pool_accounts",
                return_value=["addr1", "addr2"],
            ),
            patch(
                "instruments_service.reference_data.adapters.defi.raydium.batch_resolve_creation_timestamps",
                return_value={},
            ),
        ):
            results = await adapter.get_instruments(include_historical=True)
        assert len(results) == 2
        assert all(r.status == InstrumentStatus.DELISTED for r in results)

    @pytest.mark.asyncio
    async def test_discover_historical_pools_no_accounts(self) -> None:
        from instruments_service.reference_data.adapters.defi.raydium import RaydiumReferenceDataAdapter

        adapter = RaydiumReferenceDataAdapter()
        with patch(
            "instruments_service.reference_data.adapters.defi.raydium.discover_program_pool_accounts",
            return_value=[],
        ):
            results = await adapter._discover_historical_pools()
        assert results == []

    @pytest.mark.asyncio
    async def test_discover_historical_pools_all_excluded(self) -> None:
        from instruments_service.reference_data.adapters.defi.raydium import RaydiumReferenceDataAdapter

        adapter = RaydiumReferenceDataAdapter()
        with patch(
            "instruments_service.reference_data.adapters.defi.raydium.discover_program_pool_accounts",
            return_value=["addr1"],
        ):
            results = await adapter._discover_historical_pools(exclude_addresses={"addr1"})
        assert results == []

    @pytest.mark.asyncio
    async def test_get_instrument_not_found(self) -> None:
        from instruments_service.reference_data.adapters.defi.raydium import RaydiumReferenceDataAdapter

        adapter = RaydiumReferenceDataAdapter()
        with patch.object(adapter, "get_instruments", return_value=[]):
            result = await adapter.get_instrument("nonexistent")
        assert result is None


# ── AaveV3ReferenceDataAdapter ────────────────────────────────────────────────


class TestAaveV3Adapter:
    def test_venue_property(self) -> None:
        from instruments_service.reference_data.adapters.defi.aave_v3 import AaveV3ReferenceDataAdapter

        adapter = AaveV3ReferenceDataAdapter()
        assert adapter.venue == "aave_v3"

    def test_venue_with_protocol_slug(self) -> None:
        from instruments_service.reference_data.adapters.defi.aave_v3 import AaveV3ReferenceDataAdapter

        adapter = AaveV3ReferenceDataAdapter(protocol_slug="spark_v3")
        assert adapter.venue == "spark_v3"

    @pytest.mark.asyncio
    async def test_get_instruments_wrong_type(self) -> None:
        from instruments_service.reference_data.adapters.defi.aave_v3 import AaveV3ReferenceDataAdapter

        adapter = AaveV3ReferenceDataAdapter()
        result = await adapter.get_instruments(instrument_type="POOL")
        assert result == []

    @pytest.mark.asyncio
    async def test_get_instruments_no_api_key(self) -> None:
        from instruments_service.reference_data.adapters.defi.aave_v3 import AaveV3ReferenceDataAdapter

        adapter = AaveV3ReferenceDataAdapter()
        result = await adapter.get_instruments()
        assert result == []

    @pytest.mark.asyncio
    async def test_get_instruments_no_subgraph_id(self) -> None:
        from instruments_service.reference_data.adapters.defi.aave_v3 import AaveV3ReferenceDataAdapter

        adapter = AaveV3ReferenceDataAdapter(api_key="test-key")
        with patch(
            "instruments_service.reference_data.adapters.defi.aave_v3.get_subgraph_id",
            return_value=None,
        ):
            result = await adapter.get_instruments()
        assert result == []

    @pytest.mark.asyncio
    async def test_get_instruments_success(self) -> None:
        from instruments_service.reference_data.adapters.defi.aave_v3 import AaveV3ReferenceDataAdapter

        adapter = AaveV3ReferenceDataAdapter(api_key="test-key")
        reserves_data = {
            "data": {
                "reserves": [
                    {
                        "id": "0xreserve1",
                        "underlyingAsset": "0xweth",
                        "symbol": "WETH",
                        "name": "Wrapped Ether",
                        "decimals": 18,
                        "baseLTVasCollateral": "8000",
                        "reserveLiquidationThreshold": "8250",
                        "reserveLiquidationBonus": "10500",
                        "reserveFactor": "1000",
                        "usageAsCollateralEnabled": True,
                        "borrowingEnabled": True,
                        "isActive": True,
                        "isFrozen": False,
                        "isPaused": False,
                        "aToken": {"id": "0xatoken1"},
                    }
                ]
            }
        }
        with (
            patch(
                "instruments_service.reference_data.adapters.defi.aave_v3.get_subgraph_id",
                return_value="test-subgraph",
            ),
            patch("aiohttp.ClientSession", return_value=_mock_aiohttp_session_post(reserves_data)),
            patch(
                "instruments_service.reference_data.adapters.defi.aave_v3.batch_resolve_evm_creation_timestamps",
                return_value={},
            ),
        ):
            results = await adapter.get_instruments()
        assert len(results) == 2  # A_TOKEN + DEBT_TOKEN (borrowingEnabled=True)
        types = {r.instrument_key.split(":")[1] for r in results}
        assert "A_TOKEN" in types
        assert "DEBT_TOKEN" in types

    @pytest.mark.asyncio
    async def test_get_instruments_borrowing_disabled(self) -> None:
        from instruments_service.reference_data.adapters.defi.aave_v3 import AaveV3ReferenceDataAdapter

        adapter = AaveV3ReferenceDataAdapter(api_key="test-key")
        reserves_data = {
            "data": {
                "reserves": [
                    {
                        "symbol": "GHO",
                        "underlyingAsset": "0xgho",
                        "decimals": 18,
                        "borrowingEnabled": False,
                        "aToken": {"id": "0xatoken_gho"},
                    }
                ]
            }
        }
        with (
            patch(
                "instruments_service.reference_data.adapters.defi.aave_v3.get_subgraph_id",
                return_value="test-subgraph",
            ),
            patch("aiohttp.ClientSession", return_value=_mock_aiohttp_session_post(reserves_data)),
            patch(
                "instruments_service.reference_data.adapters.defi.aave_v3.batch_resolve_evm_creation_timestamps",
                return_value={},
            ),
        ):
            results = await adapter.get_instruments()
        assert len(results) == 1  # Only A_TOKEN, no DEBT_TOKEN

    @pytest.mark.asyncio
    async def test_get_instruments_http_error(self) -> None:
        from instruments_service.reference_data.adapters.defi.aave_v3 import AaveV3ReferenceDataAdapter

        adapter = AaveV3ReferenceDataAdapter(api_key="test-key")
        with (
            patch(
                "instruments_service.reference_data.adapters.defi.aave_v3.get_subgraph_id",
                return_value="test-subgraph",
            ),
            patch(
                "aiohttp.ClientSession",
                return_value=_mock_aiohttp_session_error(aiohttp.ClientError("fail")),
            ),
            patch("instruments_service.reference_data.adapters.defi.aave_v3.log_event"),
            pytest.raises(ConnectionError),
        ):
            await adapter.get_instruments()

    def test_build_reserve_records_empty_symbol(self) -> None:
        from instruments_service.reference_data.adapters.defi.aave_v3 import AaveV3ReferenceDataAdapter

        adapter = AaveV3ReferenceDataAdapter()
        result = adapter._build_reserve_records({"symbol": "", "underlyingAsset": "0x"}, "AAVE_V3-ETHEREUM")
        assert result == []

    def test_build_reserve_records_empty_underlying(self) -> None:
        from instruments_service.reference_data.adapters.defi.aave_v3 import AaveV3ReferenceDataAdapter

        adapter = AaveV3ReferenceDataAdapter()
        result = adapter._build_reserve_records({"symbol": "WETH", "underlyingAsset": ""}, "AAVE_V3-ETHEREUM")
        assert result == []

    @pytest.mark.asyncio
    async def test_resolve_block_num_no_date(self) -> None:
        from instruments_service.reference_data.adapters.defi.aave_v3 import AaveV3ReferenceDataAdapter

        adapter = AaveV3ReferenceDataAdapter()
        assert await adapter._resolve_block_num() is None

    @pytest.mark.asyncio
    async def test_resolve_block_num_with_date(self) -> None:
        from instruments_service.reference_data.adapters.defi.aave_v3 import AaveV3ReferenceDataAdapter

        adapter = AaveV3ReferenceDataAdapter(date="2024-01-01")
        with patch(
            "instruments_service.reference_data.adapters.defi.aave_v3.date_to_block",
            return_value=18000000,
        ):
            result = await adapter._resolve_block_num()
        assert result == 18000000

    def test_log_fetch_error(self) -> None:
        from instruments_service.reference_data.adapters.defi.aave_v3 import AaveV3ReferenceDataAdapter

        adapter = AaveV3ReferenceDataAdapter()
        with patch("instruments_service.reference_data.adapters.defi.aave_v3.log_event"):
            adapter._log_fetch_error(aiohttp.ClientError("timeout"))


# ── UniswapV4ReferenceDataAdapter ─────────────────────────────────────────────


class TestUniswapV4Adapter:
    def test_venue_property(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v4 import UniswapV4ReferenceDataAdapter

        adapter = UniswapV4ReferenceDataAdapter()
        assert adapter.venue == "uniswap_v4"

    @pytest.mark.asyncio
    async def test_get_instruments_wrong_type(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v4 import UniswapV4ReferenceDataAdapter

        adapter = UniswapV4ReferenceDataAdapter()
        result = await adapter.get_instruments(instrument_type="FUTURE")
        assert result == []

    @pytest.mark.asyncio
    async def test_get_instruments_no_api_key(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v4 import UniswapV4ReferenceDataAdapter

        adapter = UniswapV4ReferenceDataAdapter()
        result = await adapter.get_instruments()
        assert result == []

    @pytest.mark.asyncio
    async def test_get_instruments_success(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v4 import UniswapV4ReferenceDataAdapter

        adapter = UniswapV4ReferenceDataAdapter(api_key="test-key", chain="ETHEREUM")
        pool_data = {
            "data": {
                "pools": [
                    {
                        "id": "0xpool_v4",
                        "feeTier": "500",
                        "token0": {"id": "0xt0", "symbol": "WETH", "name": "WETH", "decimals": "18"},
                        "token1": {"id": "0xt1", "symbol": "USDT", "name": "USDT", "decimals": "6"},
                        "totalValueLockedUSD": "1000000",
                        "createdAtTimestamp": "1677000000",
                    }
                ]
            }
        }
        with (
            patch(
                "instruments_service.reference_data.adapters.defi.uniswap_v4._SUBGRAPH_IDS",
                {"ETHEREUM": "test-subgraph-id"},
            ),
            patch("aiohttp.ClientSession", return_value=_mock_aiohttp_session_post(pool_data)),
        ):
            results = await adapter.get_instruments()
        assert len(results) == 1
        assert results[0].base_asset == "WETH"

    @pytest.mark.asyncio
    async def test_get_instruments_http_error(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v4 import UniswapV4ReferenceDataAdapter

        adapter = UniswapV4ReferenceDataAdapter(api_key="test-key", chain="ETHEREUM")
        with (
            patch(
                "instruments_service.reference_data.adapters.defi.uniswap_v4._SUBGRAPH_IDS",
                {"ETHEREUM": "test-subgraph-id"},
            ),
            patch(
                "aiohttp.ClientSession",
                return_value=_mock_aiohttp_session_error(aiohttp.ClientError("fail")),
            ),
            patch("instruments_service.reference_data.adapters.defi.uniswap_v4.log_event"),
            pytest.raises(ConnectionError),
        ):
            await adapter.get_instruments()

    def test_build_pool_record_valid(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v4 import UniswapV4ReferenceDataAdapter

        adapter = UniswapV4ReferenceDataAdapter()
        result = adapter._build_pool_record(
            {
                "id": "0xabc",
                "feeTier": "500",
                "token0": {"symbol": "UNI", "decimals": "18"},
                "token1": {"symbol": "USDC", "decimals": "6"},
                "createdAtTimestamp": "1677000000",
            }
        )
        assert result is not None
        assert "UNISWAP_V4" in result.instrument_key

    def test_build_pool_record_missing_token(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v4 import UniswapV4ReferenceDataAdapter

        adapter = UniswapV4ReferenceDataAdapter()
        result = adapter._build_pool_record({"id": "0x1", "token0": "bad", "token1": {}})
        assert result is None

    def test_build_pool_record_empty_symbols(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v4 import UniswapV4ReferenceDataAdapter

        adapter = UniswapV4ReferenceDataAdapter()
        result = adapter._build_pool_record(
            {
                "id": "0x1",
                "token0": {"symbol": ""},
                "token1": {"symbol": "USDC"},
            }
        )
        assert result is None

    def test_log_fetch_error(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v4 import UniswapV4ReferenceDataAdapter

        adapter = UniswapV4ReferenceDataAdapter()
        with patch("instruments_service.reference_data.adapters.defi.uniswap_v4.log_event"):
            adapter._log_fetch_error(aiohttp.ClientError("error"))

    @pytest.mark.asyncio
    async def test_resolve_block_num_with_date(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v4 import UniswapV4ReferenceDataAdapter

        adapter = UniswapV4ReferenceDataAdapter(date="2024-01-01")
        with patch(
            "instruments_service.reference_data.adapters.defi.uniswap_v4.date_to_block",
            return_value=18000000,
        ):
            assert await adapter._resolve_block_num() == 18000000


# ── CompoundV3ReferenceDataAdapter ────────────────────────────────────────────


class TestCompoundV3Adapter:
    def test_venue_property(self) -> None:
        from instruments_service.reference_data.adapters.defi.compound_v3 import CompoundV3ReferenceDataAdapter

        adapter = CompoundV3ReferenceDataAdapter()
        assert adapter.venue == "compound_v3"

    @pytest.mark.asyncio
    async def test_get_instruments_wrong_type(self) -> None:
        from instruments_service.reference_data.adapters.defi.compound_v3 import CompoundV3ReferenceDataAdapter

        adapter = CompoundV3ReferenceDataAdapter()
        result = await adapter.get_instruments(instrument_type="POOL")
        assert result == []

    @pytest.mark.asyncio
    async def test_get_instruments_no_api_key(self) -> None:
        from instruments_service.reference_data.adapters.defi.compound_v3 import CompoundV3ReferenceDataAdapter

        adapter = CompoundV3ReferenceDataAdapter()
        result = await adapter.get_instruments()
        assert result == []

    @pytest.mark.asyncio
    async def test_get_instruments_no_subgraph_id(self) -> None:
        from instruments_service.reference_data.adapters.defi.compound_v3 import CompoundV3ReferenceDataAdapter

        adapter = CompoundV3ReferenceDataAdapter(api_key="test-key")
        with patch(
            "instruments_service.reference_data.adapters.defi.compound_v3.get_subgraph_id",
            return_value=None,
        ):
            result = await adapter.get_instruments()
        assert result == []

    @pytest.mark.asyncio
    async def test_get_instruments_success(self) -> None:
        from instruments_service.reference_data.adapters.defi.compound_v3 import CompoundV3ReferenceDataAdapter

        adapter = CompoundV3ReferenceDataAdapter(api_key="test-key")
        markets_data = {
            "data": {
                "markets": [
                    {
                        "id": "0xmarket1",
                        "cometProxy": "0xcomet1",
                        "creationBlockNumber": 17000000,
                        "configuration": {
                            "name": "Compound USDC",
                            "symbol": "cUSDCv3",
                            "baseToken": {
                                "token": {"id": "0xusdc", "symbol": "USDC", "name": "USD Coin", "decimals": 6},
                                "lastPriceUsd": "1.0",
                            },
                        },
                        "accounting": {
                            "totalBaseSupplyUsd": "1000000",
                            "totalBaseBorrowUsd": "500000",
                            "supplyApr": "0.03",
                            "borrowApr": "0.05",
                        },
                    }
                ]
            }
        }
        with (
            patch(
                "instruments_service.reference_data.adapters.defi.compound_v3.get_subgraph_id",
                return_value="test-subgraph",
            ),
            patch("aiohttp.ClientSession", return_value=_mock_aiohttp_session_post(markets_data)),
            patch(
                "instruments_service.reference_data.adapters.defi.compound_v3.block_to_timestamp",
                return_value=datetime(2023, 6, 1, tzinfo=UTC),
            ),
        ):
            results = await adapter.get_instruments()
        assert len(results) == 2  # SUPPLY + BORROW
        keys = [r.instrument_key for r in results]
        assert any("SUPPLY" in k for k in keys)
        assert any("BORROW" in k for k in keys)

    @pytest.mark.asyncio
    async def test_get_instruments_http_error(self) -> None:
        from instruments_service.reference_data.adapters.defi.compound_v3 import CompoundV3ReferenceDataAdapter

        adapter = CompoundV3ReferenceDataAdapter(api_key="test-key")
        with (
            patch(
                "instruments_service.reference_data.adapters.defi.compound_v3.get_subgraph_id",
                return_value="test-subgraph",
            ),
            patch(
                "aiohttp.ClientSession",
                return_value=_mock_aiohttp_session_error(aiohttp.ClientError("fail")),
            ),
            patch("instruments_service.reference_data.adapters.defi.compound_v3.log_event"),
            pytest.raises(ConnectionError),
        ):
            await adapter.get_instruments()

    def test_build_market_records_no_config(self) -> None:
        from instruments_service.reference_data.adapters.defi.compound_v3 import CompoundV3ReferenceDataAdapter

        adapter = CompoundV3ReferenceDataAdapter()
        result = adapter._build_market_records({"id": "0x1", "configuration": None}, "COMPOUND_V3-ETHEREUM")
        assert result == []

    def test_build_market_records_no_base_token(self) -> None:
        from instruments_service.reference_data.adapters.defi.compound_v3 import CompoundV3ReferenceDataAdapter

        adapter = CompoundV3ReferenceDataAdapter()
        result = adapter._build_market_records(
            {"id": "0x1", "configuration": {"baseToken": None}}, "COMPOUND_V3-ETHEREUM"
        )
        assert result == []

    def test_build_market_records_empty_symbol(self) -> None:
        from instruments_service.reference_data.adapters.defi.compound_v3 import CompoundV3ReferenceDataAdapter

        adapter = CompoundV3ReferenceDataAdapter()
        result = adapter._build_market_records(
            {
                "id": "0x1",
                "configuration": {"baseToken": {"token": {"symbol": ""}}},
            },
            "COMPOUND_V3-ETHEREUM",
        )
        assert result == []

    def test_build_market_records_no_id(self) -> None:
        from instruments_service.reference_data.adapters.defi.compound_v3 import CompoundV3ReferenceDataAdapter

        adapter = CompoundV3ReferenceDataAdapter()
        result = adapter._build_market_records({"configuration": {"baseToken": {}}}, "COMPOUND_V3-ETHEREUM")
        assert result == []

    def test_log_fetch_error(self) -> None:
        from instruments_service.reference_data.adapters.defi.compound_v3 import CompoundV3ReferenceDataAdapter

        adapter = CompoundV3ReferenceDataAdapter()
        with patch("instruments_service.reference_data.adapters.defi.compound_v3.log_event"):
            adapter._log_fetch_error(aiohttp.ClientError("error"))


# ── OrcaReferenceDataAdapter ─────────────────────────────────────────────────


class TestOrcaAdapter:
    def test_venue_property(self) -> None:
        from instruments_service.reference_data.adapters.defi.orca import OrcaReferenceDataAdapter

        adapter = OrcaReferenceDataAdapter()
        assert adapter.venue == "ORCA-SOLANA"

    @pytest.mark.asyncio
    async def test_get_instruments_wrong_type(self) -> None:
        from instruments_service.reference_data.adapters.defi.orca import OrcaReferenceDataAdapter

        adapter = OrcaReferenceDataAdapter()
        result = await adapter.get_instruments(instrument_type="PERPETUAL")
        assert result == []

    @pytest.mark.asyncio
    async def test_get_instruments_success(self) -> None:
        from instruments_service.reference_data.adapters.defi.orca import OrcaReferenceDataAdapter

        adapter = OrcaReferenceDataAdapter()
        whirlpool_data = {
            "whirlpools": [
                {
                    "address": "wp_addr_1",
                    "tokenA": {"symbol": "SOL", "decimals": 9},
                    "tokenB": {"symbol": "USDC", "decimals": 6},
                    "tvl": 50000,
                    "tickSpacing": 64,
                }
            ]
        }
        with (
            patch.object(adapter, "_get_with_retry", return_value=whirlpool_data),
            patch(
                "instruments_service.reference_data.adapters.defi._solana_utils.batch_resolve_creation_timestamps",
                return_value={},
            ),
        ):
            results = await adapter.get_instruments()
        assert len(results) == 1
        assert results[0].base_asset == "SOL"

    @pytest.mark.asyncio
    async def test_get_instruments_low_tvl(self) -> None:
        from instruments_service.reference_data.adapters.defi.orca import OrcaReferenceDataAdapter

        adapter = OrcaReferenceDataAdapter()
        whirlpool_data = {
            "whirlpools": [
                {
                    "address": "wp_addr_low",
                    "tokenA": {"symbol": "MEME"},
                    "tokenB": {"symbol": "USDC"},
                    "tvl": 100,  # Below threshold
                }
            ]
        }
        with patch.object(adapter, "_get_with_retry", return_value=whirlpool_data):
            results = await adapter.get_instruments()
        assert results == []

    @pytest.mark.asyncio
    async def test_get_instruments_http_error(self) -> None:
        from instruments_service.reference_data.adapters.defi.orca import OrcaReferenceDataAdapter

        adapter = OrcaReferenceDataAdapter()
        with (
            patch.object(adapter, "_get_with_retry", side_effect=aiohttp.ClientError("fail")),
            patch("instruments_service.reference_data.adapters.defi.orca.log_event"),
            pytest.raises(ConnectionError),
        ):
            await adapter.get_instruments()

    def test_build_pool_record_no_address(self) -> None:
        from instruments_service.reference_data.adapters.defi.orca import OrcaReferenceDataAdapter

        adapter = OrcaReferenceDataAdapter()
        assert adapter._build_pool_record({}) is None

    def test_build_pool_record_non_dict_tokens(self) -> None:
        from instruments_service.reference_data.adapters.defi.orca import OrcaReferenceDataAdapter

        adapter = OrcaReferenceDataAdapter()
        result = adapter._build_pool_record({"address": "addr", "tokenA": "bad", "tokenB": "bad"})
        assert result is None

    def test_build_pool_record_empty_symbols(self) -> None:
        from instruments_service.reference_data.adapters.defi.orca import OrcaReferenceDataAdapter

        adapter = OrcaReferenceDataAdapter()
        result = adapter._build_pool_record(
            {
                "address": "addr",
                "tokenA": {"symbol": ""},
                "tokenB": {"symbol": "USDC"},
            }
        )
        assert result is None

    def test_classify_orca_error(self) -> None:
        from instruments_service.reference_data.adapters.defi.orca import _classify_orca_error

        assert _classify_orca_error(Exception("msg"), status=429) == "RATE_LIMIT"
        assert _classify_orca_error(Exception("msg"), status=503) == "503"
        assert _classify_orca_error(Exception("msg"), status=500) == "500"
        assert _classify_orca_error(Exception("rate exceeded")) == "RATE_LIMIT"
        assert _classify_orca_error(Exception("unknown")) == "UNKNOWN"

    @pytest.mark.asyncio
    async def test_get_instruments_not_list_whirlpools(self) -> None:
        from instruments_service.reference_data.adapters.defi.orca import OrcaReferenceDataAdapter

        adapter = OrcaReferenceDataAdapter()
        with patch.object(adapter, "_get_with_retry", return_value={"whirlpools": "not_a_list"}):
            results = await adapter.get_instruments()
        assert results == []

    @pytest.mark.asyncio
    async def test_get_instruments_non_dict_pool_skipped(self) -> None:
        from instruments_service.reference_data.adapters.defi.orca import OrcaReferenceDataAdapter

        adapter = OrcaReferenceDataAdapter()
        with patch.object(adapter, "_get_with_retry", return_value={"whirlpools": ["not_a_dict"]}):
            results = await adapter.get_instruments()
        assert results == []


# ── DriftReferenceDataAdapter ─────────────────────────────────────────────────


class TestDriftAdapter:
    def test_venue_property(self) -> None:
        from instruments_service.reference_data.adapters.defi.drift import DriftReferenceDataAdapter

        adapter = DriftReferenceDataAdapter()
        assert adapter.venue == "DRIFT-SOLANA"

    @pytest.mark.asyncio
    async def test_get_instruments_success(self) -> None:
        from instruments_service.reference_data.adapters.defi.drift import DriftReferenceDataAdapter

        adapter = DriftReferenceDataAdapter()
        markets_data = {
            "markets": [
                {"symbol": "SOL-PERP", "marketType": "perp", "status": "active"},
                {"symbol": "SOL", "marketType": "spot", "status": "active", "baseAsset": "SOL"},
                {"symbol": "BTC-PERP", "marketType": "perp", "status": "inactive"},
            ]
        }
        with patch.object(adapter, "_get_with_retry", return_value=markets_data):
            results = await adapter.get_instruments()
        assert len(results) == 2  # Only active markets

    @pytest.mark.asyncio
    async def test_get_instruments_filter_perp_only(self) -> None:
        from instruments_service.reference_data.adapters.defi.drift import DriftReferenceDataAdapter

        adapter = DriftReferenceDataAdapter()
        markets_data = {
            "markets": [
                {"symbol": "SOL-PERP", "marketType": "perp", "status": "active"},
                {"symbol": "SOL", "marketType": "spot", "status": "active", "baseAsset": "SOL"},
            ]
        }
        with patch.object(adapter, "_get_with_retry", return_value=markets_data):
            results = await adapter.get_instruments(instrument_type=InstrumentType.PERPETUAL)
        assert len(results) == 1
        assert results[0].instrument_type == InstrumentType.PERPETUAL

    @pytest.mark.asyncio
    async def test_get_instruments_http_error(self) -> None:
        from instruments_service.reference_data.adapters.defi.drift import DriftReferenceDataAdapter

        adapter = DriftReferenceDataAdapter()
        with (
            patch.object(adapter, "_get_with_retry", side_effect=aiohttp.ClientError("fail")),
            patch("instruments_service.reference_data.adapters.defi.drift.log_event"),
            pytest.raises(ConnectionError),
        ):
            await adapter.get_instruments()

    @pytest.mark.asyncio
    async def test_fetch_all_markets_not_dict(self) -> None:
        from instruments_service.reference_data.adapters.defi.drift import DriftReferenceDataAdapter

        adapter = DriftReferenceDataAdapter()
        with patch.object(adapter, "_get_with_retry", return_value=[]):
            markets = await adapter._fetch_all_markets()
        assert markets == []

    @pytest.mark.asyncio
    async def test_fetch_all_markets_no_markets_key(self) -> None:
        from instruments_service.reference_data.adapters.defi.drift import DriftReferenceDataAdapter

        adapter = DriftReferenceDataAdapter()
        with patch.object(adapter, "_get_with_retry", return_value={"other": "data"}):
            markets = await adapter._fetch_all_markets()
        assert markets == []

    def test_build_perp_record_empty_symbol(self) -> None:
        from instruments_service.reference_data.adapters.defi.drift import DriftReferenceDataAdapter

        adapter = DriftReferenceDataAdapter()
        assert adapter._build_perp_record({"symbol": ""}) is None

    def test_build_perp_record_valid(self) -> None:
        from instruments_service.reference_data.adapters.defi.drift import DriftReferenceDataAdapter

        adapter = DriftReferenceDataAdapter()
        record = adapter._build_perp_record({"symbol": "SOL-PERP"})
        assert record is not None
        assert record.base_asset == "SOL"
        assert record.quote_asset == "USDC"
        assert record.instrument_type == InstrumentType.PERPETUAL

    def test_build_perp_record_no_dash(self) -> None:
        from instruments_service.reference_data.adapters.defi.drift import DriftReferenceDataAdapter

        adapter = DriftReferenceDataAdapter()
        record = adapter._build_perp_record({"symbol": "BTC"})
        assert record is not None
        assert record.base_asset == "BTC"

    def test_build_spot_record_valid(self) -> None:
        from instruments_service.reference_data.adapters.defi.drift import DriftReferenceDataAdapter

        adapter = DriftReferenceDataAdapter()
        record = adapter._build_spot_record({"symbol": "SOL", "baseAsset": "SOL"})
        assert record is not None
        assert record.instrument_type == InstrumentType.SPOT_PAIR

    def test_build_spot_record_empty_base(self) -> None:
        from instruments_service.reference_data.adapters.defi.drift import DriftReferenceDataAdapter

        adapter = DriftReferenceDataAdapter()
        record = adapter._build_spot_record({"symbol": "", "baseAsset": ""})
        assert record is None

    def test_classify_drift_error(self) -> None:
        from instruments_service.reference_data.adapters.defi.drift import _classify_drift_error

        assert _classify_drift_error(Exception("msg"), status=429) == "RATE_LIMIT"
        assert _classify_drift_error(Exception("msg"), status=503) == "503"
        assert _classify_drift_error(Exception("msg"), status=500) == "500"
        assert _classify_drift_error(Exception("unknown")) == "UNKNOWN"

    def test_log_fetch_error(self) -> None:
        from instruments_service.reference_data.adapters.defi.drift import DriftReferenceDataAdapter

        adapter = DriftReferenceDataAdapter()
        with patch("instruments_service.reference_data.adapters.defi.drift.log_event"):
            adapter._log_fetch_error(aiohttp.ClientError("error"), "test_endpoint")


# ── KaminoReferenceDataAdapter ────────────────────────────────────────────────


class TestKaminoAdapter:
    def test_venue_property(self) -> None:
        from instruments_service.reference_data.adapters.defi.kamino import KaminoReferenceDataAdapter

        adapter = KaminoReferenceDataAdapter()
        assert adapter.venue == "KAMINO-SOLANA"

    @pytest.mark.asyncio
    async def test_get_instruments_wrong_type(self) -> None:
        from instruments_service.reference_data.adapters.defi.kamino import KaminoReferenceDataAdapter

        adapter = KaminoReferenceDataAdapter()
        result = await adapter.get_instruments(instrument_type="LENDING")
        assert result == []

    @pytest.mark.asyncio
    async def test_get_instruments_success(self) -> None:
        from instruments_service.reference_data.adapters.defi.kamino import KaminoReferenceDataAdapter

        adapter = KaminoReferenceDataAdapter()
        strategies = [
            {
                "address": "vault_addr_1",
                "status": "LIVE",
                "tokenAMint": "SOL_MINT",
                "tokenBMint": "USDC_MINT",
            }
        ]
        with (
            patch.object(adapter, "_get_with_retry", return_value=strategies),
            patch.object(adapter, "_resolve_symbol", side_effect=lambda m: "SOL" if "SOL" in m else "USDC"),
            patch(
                "instruments_service.reference_data.adapters.defi._solana_utils.batch_resolve_creation_timestamps",
                return_value={},
            ),
        ):
            results = await adapter.get_instruments()
        assert len(results) == 1
        assert results[0].base_asset == "SOL"

    @pytest.mark.asyncio
    async def test_get_instruments_http_error(self) -> None:
        from instruments_service.reference_data.adapters.defi.kamino import KaminoReferenceDataAdapter

        adapter = KaminoReferenceDataAdapter()
        with (
            patch.object(adapter, "_get_with_retry", side_effect=aiohttp.ClientError("fail")),
            patch("instruments_service.reference_data.adapters.defi.kamino.log_event"),
            pytest.raises(ConnectionError),
        ):
            await adapter.get_instruments()

    def test_build_vault_record_not_live(self) -> None:
        from instruments_service.reference_data.adapters.defi.kamino import KaminoReferenceDataAdapter

        adapter = KaminoReferenceDataAdapter()
        result = adapter._build_vault_record({"address": "x", "status": "CLOSED"}, "KAMINO-SOLANA")
        assert result is None

    def test_build_vault_record_unresolvable_symbols(self) -> None:
        from instruments_service.reference_data.adapters.defi.kamino import KaminoReferenceDataAdapter

        adapter = KaminoReferenceDataAdapter()
        with patch.object(adapter, "_resolve_symbol", return_value=""):
            result = adapter._build_vault_record(
                {"address": "x", "status": "LIVE", "tokenAMint": "x", "tokenBMint": "y"},
                "KAMINO-SOLANA",
            )
        assert result is None

    def test_build_vault_record_missing_address(self) -> None:
        from instruments_service.reference_data.adapters.defi.kamino import KaminoReferenceDataAdapter

        adapter = KaminoReferenceDataAdapter()
        result = adapter._build_vault_record({"status": "LIVE"}, "KAMINO-SOLANA")
        assert result is None

    def test_classify_kamino_error(self) -> None:
        from instruments_service.reference_data.adapters.defi.kamino import _classify_kamino_error

        assert _classify_kamino_error(Exception("msg"), status=429) == "RATE_LIMIT"
        assert _classify_kamino_error(Exception("msg"), status=503) == "503"
        assert _classify_kamino_error(Exception("msg"), status=500) == "500"
        assert _classify_kamino_error(Exception("unknown")) == "UNKNOWN"

    def test_log_fetch_error(self) -> None:
        from instruments_service.reference_data.adapters.defi.kamino import KaminoReferenceDataAdapter

        adapter = KaminoReferenceDataAdapter()
        with patch("instruments_service.reference_data.adapters.defi.kamino.log_event"):
            adapter._log_fetch_error(aiohttp.ClientError("error"))


# ── UniswapV2ReferenceDataAdapter ─────────────────────────────────────────────


class TestUniswapV2Adapter:
    def test_venue_property(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v2 import UniswapV2ReferenceDataAdapter

        adapter = UniswapV2ReferenceDataAdapter()
        assert adapter.venue == "uniswap_v2"

    @pytest.mark.asyncio
    async def test_get_instruments_wrong_type(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v2 import UniswapV2ReferenceDataAdapter

        adapter = UniswapV2ReferenceDataAdapter()
        result = await adapter.get_instruments(instrument_type="FUTURE")
        assert result == []

    @pytest.mark.asyncio
    async def test_get_instruments_no_api_key(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v2 import UniswapV2ReferenceDataAdapter

        adapter = UniswapV2ReferenceDataAdapter()
        result = await adapter.get_instruments()
        assert result == []

    @pytest.mark.asyncio
    async def test_get_instruments_success(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v2 import UniswapV2ReferenceDataAdapter

        adapter = UniswapV2ReferenceDataAdapter(api_key="test-key", chain="ETHEREUM")
        pairs_data = {
            "data": {
                "pairs": [
                    {
                        "id": "0xpair1",
                        "token0": {"id": "0xt0", "symbol": "WETH", "name": "WETH", "decimals": "18"},
                        "token1": {"id": "0xt1", "symbol": "USDC", "name": "USDC", "decimals": "6"},
                        "reserveUSD": "5000000",
                        "createdAtTimestamp": "1620000000",
                    }
                ]
            }
        }
        with (
            patch(
                "instruments_service.reference_data.adapters.defi.uniswap_v2._SUBGRAPH_IDS",
                {"ETHEREUM": "test-subgraph-id"},
            ),
            patch("aiohttp.ClientSession", return_value=_mock_aiohttp_session_post(pairs_data)),
        ):
            results = await adapter.get_instruments()
        assert len(results) == 1
        assert results[0].base_asset == "WETH"

    @pytest.mark.asyncio
    async def test_get_instruments_http_error(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v2 import UniswapV2ReferenceDataAdapter

        adapter = UniswapV2ReferenceDataAdapter(api_key="test-key", chain="ETHEREUM")
        with (
            patch(
                "instruments_service.reference_data.adapters.defi.uniswap_v2._SUBGRAPH_IDS",
                {"ETHEREUM": "test-subgraph-id"},
            ),
            patch(
                "aiohttp.ClientSession",
                return_value=_mock_aiohttp_session_error(aiohttp.ClientError("fail")),
            ),
            patch("instruments_service.reference_data.adapters.defi.uniswap_v2.log_event"),
            pytest.raises(ConnectionError),
        ):
            await adapter.get_instruments()

    def test_build_pair_record_valid(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v2 import UniswapV2ReferenceDataAdapter

        adapter = UniswapV2ReferenceDataAdapter()
        result = adapter._build_pair_record(
            {
                "id": "0xpair",
                "token0": {"symbol": "AAVE", "decimals": "18"},
                "token1": {"symbol": "WETH", "decimals": "18"},
                "createdAtTimestamp": "1620000000",
            }
        )
        assert result is not None
        assert result.instrument_type == InstrumentType.POOL

    def test_build_pair_record_missing_id(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v2 import UniswapV2ReferenceDataAdapter

        adapter = UniswapV2ReferenceDataAdapter()
        assert adapter._build_pair_record({}) is None

    def test_build_pair_record_non_dict_tokens(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v2 import UniswapV2ReferenceDataAdapter

        adapter = UniswapV2ReferenceDataAdapter()
        assert adapter._build_pair_record({"id": "x", "token0": "bad", "token1": "bad"}) is None

    def test_build_pair_record_empty_symbols(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v2 import UniswapV2ReferenceDataAdapter

        adapter = UniswapV2ReferenceDataAdapter()
        result = adapter._build_pair_record(
            {
                "id": "0x1",
                "token0": {"symbol": ""},
                "token1": {"symbol": "DAI"},
            }
        )
        assert result is None

    def test_log_fetch_error(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v2 import UniswapV2ReferenceDataAdapter

        adapter = UniswapV2ReferenceDataAdapter()
        with patch("instruments_service.reference_data.adapters.defi.uniswap_v2.log_event"):
            adapter._log_fetch_error(aiohttp.ClientError("error"))

    @pytest.mark.asyncio
    async def test_resolve_block_num(self) -> None:
        from instruments_service.reference_data.adapters.defi.uniswap_v2 import UniswapV2ReferenceDataAdapter

        adapter = UniswapV2ReferenceDataAdapter(date="2024-01-01")
        with patch(
            "instruments_service.reference_data.adapters.defi.uniswap_v2.date_to_block",
            return_value=18000000,
        ):
            assert await adapter._resolve_block_num() == 18000000


# ── MorphoReferenceDataAdapter ────────────────────────────────────────────────


class TestMorphoAdapter:
    def test_venue_property(self) -> None:
        from instruments_service.reference_data.adapters.defi.morpho import MorphoReferenceDataAdapter

        adapter = MorphoReferenceDataAdapter()
        assert adapter.venue == "morpho"

    @pytest.mark.asyncio
    async def test_get_instruments_wrong_type(self) -> None:
        from instruments_service.reference_data.adapters.defi.morpho import MorphoReferenceDataAdapter

        adapter = MorphoReferenceDataAdapter()
        result = await adapter.get_instruments(instrument_type="POOL")
        assert result == []

    @pytest.mark.asyncio
    async def test_get_instruments_unsupported_chain(self) -> None:
        from instruments_service.reference_data.adapters.defi.morpho import MorphoReferenceDataAdapter

        adapter = MorphoReferenceDataAdapter(chain="SOLANA")
        result = await adapter.get_instruments()
        assert result == []

    @pytest.mark.asyncio
    async def test_get_instruments_success(self) -> None:
        from instruments_service.reference_data.adapters.defi.morpho import MorphoReferenceDataAdapter

        adapter = MorphoReferenceDataAdapter()
        market_data = {
            "data": {
                "markets": {
                    "items": [
                        {
                            "uniqueKey": "0xmarketkey123456789",
                            "loanAsset": {"address": "0xusdc", "symbol": "USDC", "name": "USDC", "decimals": 6},
                            "collateralAsset": {"address": "0xweth", "symbol": "WETH", "name": "WETH", "decimals": 18},
                            "lltv": "860000000000000000",
                            "state": {
                                "supplyAssets": "1000000",
                                "borrowAssets": "500000",
                                "supplyApy": "0.03",
                                "borrowApy": "0.05",
                            },
                        }
                    ]
                }
            }
        }
        with patch("aiohttp.ClientSession", return_value=_mock_aiohttp_session_post(market_data)):
            results = await adapter.get_instruments()
        assert len(results) == 1
        assert results[0].base_asset == "WETH"
        assert results[0].quote_asset == "USDC"

    @pytest.mark.asyncio
    async def test_get_instruments_http_error(self) -> None:
        from instruments_service.reference_data.adapters.defi.morpho import MorphoReferenceDataAdapter

        adapter = MorphoReferenceDataAdapter()
        with (
            patch(
                "aiohttp.ClientSession",
                return_value=_mock_aiohttp_session_error(aiohttp.ClientError("fail")),
            ),
            patch("instruments_service.reference_data.adapters.defi.morpho.log_event"),
            pytest.raises(ConnectionError),
        ):
            await adapter.get_instruments()

    @pytest.mark.asyncio
    async def test_get_instruments_no_markets_data(self) -> None:
        from instruments_service.reference_data.adapters.defi.morpho import MorphoReferenceDataAdapter

        adapter = MorphoReferenceDataAdapter()
        with patch("aiohttp.ClientSession", return_value=_mock_aiohttp_session_post({"data": {}})):
            results = await adapter.get_instruments()
        assert results == []

    @pytest.mark.asyncio
    async def test_get_instruments_graphql_errors(self) -> None:
        from instruments_service.reference_data.adapters.defi.morpho import MorphoReferenceDataAdapter

        adapter = MorphoReferenceDataAdapter()
        data_with_errors = {
            "errors": [{"message": "some warning"}],
            "data": {"markets": {"items": []}},
        }
        with patch("aiohttp.ClientSession", return_value=_mock_aiohttp_session_post(data_with_errors)):
            results = await adapter.get_instruments()
        assert results == []

    def test_market_to_record_valid(self) -> None:
        from instruments_service.reference_data.adapters.defi.morpho import MorphoReferenceDataAdapter

        record = MorphoReferenceDataAdapter._market_to_record(
            {
                "uniqueKey": "0xkey123456",
                "loanAsset": {"symbol": "USDC", "decimals": 6},
                "collateralAsset": {"symbol": "WETH", "decimals": 18},
            },
            "MORPHO-ETHEREUM",
        )
        assert record is not None
        assert record.base_asset == "WETH"

    def test_market_to_record_missing_loan_asset(self) -> None:
        from instruments_service.reference_data.adapters.defi.morpho import MorphoReferenceDataAdapter

        record = MorphoReferenceDataAdapter._market_to_record(
            {"uniqueKey": "0xkey", "loanAsset": "not_dict", "collateralAsset": {"symbol": "WETH"}},
            "MORPHO-ETHEREUM",
        )
        assert record is None

    def test_market_to_record_missing_collateral_symbol(self) -> None:
        from instruments_service.reference_data.adapters.defi.morpho import MorphoReferenceDataAdapter

        record = MorphoReferenceDataAdapter._market_to_record(
            {"uniqueKey": "0xkey", "loanAsset": {"symbol": "USDC"}, "collateralAsset": {"symbol": ""}},
            "MORPHO-ETHEREUM",
        )
        assert record is None

    def test_market_to_record_missing_key(self) -> None:
        from instruments_service.reference_data.adapters.defi.morpho import MorphoReferenceDataAdapter

        record = MorphoReferenceDataAdapter._market_to_record(
            {"uniqueKey": "", "loanAsset": {"symbol": "USDC"}, "collateralAsset": {"symbol": "WETH"}},
            "MORPHO-ETHEREUM",
        )
        assert record is None


# ── BalancerReferenceDataAdapter ──────────────────────────────────────────────


class TestBalancerAdapter:
    def test_venue_property(self) -> None:
        from instruments_service.reference_data.adapters.defi.balancer import BalancerReferenceDataAdapter

        adapter = BalancerReferenceDataAdapter()
        assert adapter.venue == "balancer"

    @pytest.mark.asyncio
    async def test_get_instruments_wrong_type(self) -> None:
        from instruments_service.reference_data.adapters.defi.balancer import BalancerReferenceDataAdapter

        adapter = BalancerReferenceDataAdapter()
        result = await adapter.get_instruments(instrument_type="FUTURE")
        assert result == []

    @pytest.mark.asyncio
    async def test_get_instruments_success(self) -> None:
        from instruments_service.reference_data.adapters.defi.balancer import BalancerReferenceDataAdapter

        adapter = BalancerReferenceDataAdapter()
        pool_data = {
            "data": {
                "poolGetPools": [
                    {
                        "id": "pool1",
                        "name": "WETH-USDC Pool",
                        "type": "Weighted",
                        "address": "0xpool1",
                        "chain": "MAINNET",
                        "protocolVersion": 3,
                        "createTime": 1677000000,
                        "poolTokens": [
                            {"address": "0xweth", "symbol": "WETH", "decimals": "18"},
                            {"address": "0xusdc", "symbol": "USDC", "decimals": "6"},
                        ],
                        "dynamicData": {"totalLiquidity": "5000000"},
                    }
                ]
            }
        }
        with patch("aiohttp.ClientSession", return_value=_mock_aiohttp_session_post(pool_data)):
            results = await adapter.get_instruments()
        assert len(results) == 1
        assert results[0].base_asset == "WETH"

    @pytest.mark.asyncio
    async def test_get_instruments_http_error(self) -> None:
        from instruments_service.reference_data.adapters.defi.balancer import BalancerReferenceDataAdapter

        adapter = BalancerReferenceDataAdapter()
        with (
            patch(
                "aiohttp.ClientSession",
                return_value=_mock_aiohttp_session_error(aiohttp.ClientError("fail")),
            ),
            patch("instruments_service.reference_data.adapters.defi.balancer.log_event"),
            pytest.raises(ConnectionError),
        ):
            await adapter.get_instruments()

    @pytest.mark.asyncio
    async def test_get_instruments_empty_response(self) -> None:
        from instruments_service.reference_data.adapters.defi.balancer import BalancerReferenceDataAdapter

        adapter = BalancerReferenceDataAdapter()
        with patch("aiohttp.ClientSession", return_value=_mock_aiohttp_session_post({"data": {"poolGetPools": []}})):
            results = await adapter.get_instruments()
        assert results == []

    def test_pool_to_record_valid(self) -> None:
        from instruments_service.reference_data.adapters.defi.balancer import BalancerReferenceDataAdapter

        adapter = BalancerReferenceDataAdapter()
        record = adapter._pool_to_record(
            {
                "address": "0xaddr",
                "name": "Test Pool",
                "poolTokens": [
                    {"address": "0xt0", "symbol": "WETH", "decimals": "18"},
                    {"address": "0xt1", "symbol": "USDC", "decimals": "6"},
                ],
                "createTime": 1677000000,
            }
        )
        assert record is not None
        assert record.underlying == "Test Pool"

    def test_pool_to_record_missing_address(self) -> None:
        from instruments_service.reference_data.adapters.defi.balancer import BalancerReferenceDataAdapter

        adapter = BalancerReferenceDataAdapter()
        assert adapter._pool_to_record({"poolTokens": []}) is None

    def test_pool_to_record_too_few_tokens(self) -> None:
        from instruments_service.reference_data.adapters.defi.balancer import BalancerReferenceDataAdapter

        adapter = BalancerReferenceDataAdapter()
        assert adapter._pool_to_record({"address": "0x1", "poolTokens": [{"symbol": "WETH"}]}) is None

    def test_pool_to_record_not_list_tokens(self) -> None:
        from instruments_service.reference_data.adapters.defi.balancer import BalancerReferenceDataAdapter

        adapter = BalancerReferenceDataAdapter()
        assert adapter._pool_to_record({"address": "0x1", "poolTokens": "bad"}) is None

    def test_pool_to_record_no_name(self) -> None:
        from instruments_service.reference_data.adapters.defi.balancer import BalancerReferenceDataAdapter

        adapter = BalancerReferenceDataAdapter()
        record = adapter._pool_to_record(
            {
                "address": "0xaddr",
                "name": "",
                "poolTokens": [
                    {"symbol": "WETH", "decimals": "18"},
                    {"symbol": "DAI", "decimals": "18"},
                ],
            }
        )
        assert record is not None
        assert record.underlying is None


# ── MarinadeReferenceDataAdapter ──────────────────────────────────────────────


class TestMarinadeAdapter:
    def test_venue_property(self) -> None:
        from instruments_service.reference_data.adapters.defi.marinade import MarinadeReferenceDataAdapter

        adapter = MarinadeReferenceDataAdapter()
        assert adapter.venue == "MARINADE-SOLANA"

    @pytest.mark.asyncio
    async def test_get_instruments_wrong_type(self) -> None:
        from instruments_service.reference_data.adapters.defi.marinade import MarinadeReferenceDataAdapter

        adapter = MarinadeReferenceDataAdapter()
        result = await adapter.get_instruments(instrument_type="POOL")
        assert result == []

    @pytest.mark.asyncio
    async def test_get_instruments_success(self) -> None:
        from instruments_service.reference_data.adapters.defi.marinade import MarinadeReferenceDataAdapter

        adapter = MarinadeReferenceDataAdapter()
        apy_data = {"value": 6.5, "averageStakingAPY": 6.5}
        with patch.object(adapter, "_get_with_retry", return_value=apy_data):
            results = await adapter.get_instruments()
        assert len(results) == 2  # mSOL + native
        types = {r.instrument_key.split(":")[-1] for r in results}
        assert "MSOL" in types
        assert "NATIVE-SOL" in types

    @pytest.mark.asyncio
    async def test_get_instruments_http_error(self) -> None:
        from instruments_service.reference_data.adapters.defi.marinade import MarinadeReferenceDataAdapter

        adapter = MarinadeReferenceDataAdapter()
        with (
            patch.object(adapter, "_get_with_retry", side_effect=aiohttp.ClientError("fail")),
            patch("instruments_service.reference_data.adapters.defi.marinade.log_event"),
            pytest.raises(ConnectionError),
        ):
            await adapter.get_instruments()

    def test_build_msol_record(self) -> None:
        from instruments_service.reference_data.adapters.defi.marinade import MarinadeReferenceDataAdapter

        adapter = MarinadeReferenceDataAdapter()
        record = adapter._build_msol_record({}, "MARINADE-SOLANA")
        assert record is not None
        assert record.instrument_type == InstrumentType.STAKING
        assert record.base_asset == "SOL"
        assert record.quote_asset == "MSOL"

    def test_build_native_stake_record(self) -> None:
        from instruments_service.reference_data.adapters.defi.marinade import MarinadeReferenceDataAdapter

        adapter = MarinadeReferenceDataAdapter()
        record = adapter._build_native_stake_record({}, "MARINADE-SOLANA")
        assert record is not None
        assert record.base_asset == "SOL"
        assert record.quote_asset == "SOL"

    def test_classify_marinade_error(self) -> None:
        from instruments_service.reference_data.adapters.defi.marinade import _classify_marinade_error

        assert _classify_marinade_error(Exception("msg"), status=429) == "RATE_LIMIT"
        assert _classify_marinade_error(Exception("msg"), status=503) == "503"
        assert _classify_marinade_error(Exception("msg"), status=500) == "500"
        assert _classify_marinade_error(Exception("unknown")) == "UNKNOWN"

    def test_log_fetch_error(self) -> None:
        from instruments_service.reference_data.adapters.defi.marinade import MarinadeReferenceDataAdapter

        adapter = MarinadeReferenceDataAdapter()
        with patch("instruments_service.reference_data.adapters.defi.marinade.log_event"):
            adapter._log_fetch_error(aiohttp.ClientError("error"))


# ── CurveReferenceDataAdapter ─────────────────────────────────────────────────


class TestCurveAdapter:
    def test_venue_property(self) -> None:
        from instruments_service.reference_data.adapters.defi.curve import CurveReferenceDataAdapter

        adapter = CurveReferenceDataAdapter()
        assert adapter.venue == "curve"

    @pytest.mark.asyncio
    async def test_get_instruments_wrong_type(self) -> None:
        from instruments_service.reference_data.adapters.defi.curve import CurveReferenceDataAdapter

        adapter = CurveReferenceDataAdapter()
        result = await adapter.get_instruments(instrument_type="LENDING")
        assert result == []

    @pytest.mark.asyncio
    async def test_get_instruments_unsupported_chain(self) -> None:
        from instruments_service.reference_data.adapters.defi.curve import CurveReferenceDataAdapter

        adapter = CurveReferenceDataAdapter(chain="SOLANA")
        result = await adapter.get_instruments()
        assert result == []

    @pytest.mark.asyncio
    async def test_get_instruments_success(self) -> None:
        from instruments_service.reference_data.adapters.defi.curve import CurveReferenceDataAdapter

        adapter = CurveReferenceDataAdapter()
        pool_data = {
            "data": {
                "poolData": [
                    {
                        "address": "0xcurvepool1",
                        "name": "3pool",
                        "coins": [
                            {"symbol": "DAI", "address": "0xdai", "decimals": "18"},
                            {"symbol": "USDC", "address": "0xusdc", "decimals": "6"},
                        ],
                    }
                ]
            }
        }
        with patch("aiohttp.ClientSession", return_value=_mock_aiohttp_session_post(pool_data)):
            results = await adapter.get_instruments()
        assert len(results) == 1
        assert results[0].underlying == "3pool"

    @pytest.mark.asyncio
    async def test_get_instruments_http_error(self) -> None:
        from instruments_service.reference_data.adapters.defi.curve import CurveReferenceDataAdapter

        adapter = CurveReferenceDataAdapter()
        with (
            patch(
                "aiohttp.ClientSession",
                return_value=_mock_aiohttp_session_error(aiohttp.ClientError("fail")),
            ),
            patch("instruments_service.reference_data.adapters.defi.curve.log_event"),
            pytest.raises(ConnectionError),
        ):
            await adapter.get_instruments()

    @pytest.mark.asyncio
    async def test_get_instruments_pool_no_address_skipped(self) -> None:
        from instruments_service.reference_data.adapters.defi.curve import CurveReferenceDataAdapter

        adapter = CurveReferenceDataAdapter()
        pool_data = {
            "data": {
                "poolData": [
                    {"name": "bad", "coins": [{"symbol": "DAI"}, {"symbol": "USDC"}]},
                ]
            }
        }
        with patch("aiohttp.ClientSession", return_value=_mock_aiohttp_session_post(pool_data)):
            results = await adapter.get_instruments()
        assert results == []

    @pytest.mark.asyncio
    async def test_get_instruments_pool_too_few_coins_skipped(self) -> None:
        from instruments_service.reference_data.adapters.defi.curve import CurveReferenceDataAdapter

        adapter = CurveReferenceDataAdapter()
        pool_data = {
            "data": {
                "poolData": [
                    {"address": "0x1", "name": "bad", "coins": [{"symbol": "DAI"}]},
                ]
            }
        }
        with patch("aiohttp.ClientSession", return_value=_mock_aiohttp_session_post(pool_data)):
            results = await adapter.get_instruments()
        assert results == []

    @pytest.mark.asyncio
    async def test_get_instruments_non_dict_coin(self) -> None:
        from instruments_service.reference_data.adapters.defi.curve import CurveReferenceDataAdapter

        adapter = CurveReferenceDataAdapter()
        pool_data = {
            "data": {
                "poolData": [
                    {"address": "0x1", "name": "test", "coins": ["not_dict", "not_dict"]},
                ]
            }
        }
        with patch("aiohttp.ClientSession", return_value=_mock_aiohttp_session_post(pool_data)):
            results = await adapter.get_instruments()
        assert len(results) == 0  # Non-dict coins → missing decimals → validator rejects record


# ── JitoReferenceDataAdapter ─────────────────────────────────────────────────


class TestJitoAdapter:
    def test_venue_property(self) -> None:
        from instruments_service.reference_data.adapters.defi.jito import JitoReferenceDataAdapter

        adapter = JitoReferenceDataAdapter()
        assert adapter.venue == "JITO-SOLANA"

    @pytest.mark.asyncio
    async def test_get_instruments_wrong_type(self) -> None:
        from instruments_service.reference_data.adapters.defi.jito import JitoReferenceDataAdapter

        adapter = JitoReferenceDataAdapter()
        result = await adapter.get_instruments(instrument_type="POOL")
        assert result == []

    @pytest.mark.asyncio
    async def test_get_instruments_success(self) -> None:
        from instruments_service.reference_data.adapters.defi.jito import JitoReferenceDataAdapter

        adapter = JitoReferenceDataAdapter()
        stats_data = {"stakePoolStats": {"apy": 7.5}}
        with patch.object(adapter, "_get_with_retry", return_value=stats_data):
            results = await adapter.get_instruments()
        assert len(results) == 2
        assert results[0].instrument_type == InstrumentType.STAKING
        assert results[0].base_asset == "SOL"
        assert results[0].quote_asset == "JITOSOL"
        assert results[1].instrument_key.endswith(":JITO-MEV-AGGREGATE")
        assert results[1].source_archive_url_template is not None

    @pytest.mark.asyncio
    async def test_get_instruments_http_error(self) -> None:
        from instruments_service.reference_data.adapters.defi.jito import JitoReferenceDataAdapter

        adapter = JitoReferenceDataAdapter()
        with (
            patch.object(adapter, "_get_with_retry", side_effect=aiohttp.ClientError("fail")),
            patch("instruments_service.reference_data.adapters.defi.jito.log_event"),
            pytest.raises(ConnectionError),
        ):
            await adapter.get_instruments()

    def test_classify_jito_error(self) -> None:
        from instruments_service.reference_data.adapters.defi.jito import _classify_jito_error

        assert _classify_jito_error(Exception("msg"), status=429) == "RATE_LIMIT"
        assert _classify_jito_error(Exception("msg"), status=503) == "503"
        assert _classify_jito_error(Exception("msg"), status=500) == "500"
        assert _classify_jito_error(Exception("rate limit")) == "RATE_LIMIT"
        assert _classify_jito_error(Exception("unknown")) == "UNKNOWN"

    def test_log_fetch_error(self) -> None:
        from instruments_service.reference_data.adapters.defi.jito import JitoReferenceDataAdapter

        adapter = JitoReferenceDataAdapter()
        with patch("instruments_service.reference_data.adapters.defi.jito.log_event"):
            adapter._log_fetch_error(aiohttp.ClientError("error"))


# ── FluidReferenceDataAdapter ─────────────────────────────────────────────────


class TestFluidAdapter:
    def test_venue_property(self) -> None:
        from instruments_service.reference_data.adapters.defi.fluid import FluidReferenceDataAdapter

        adapter = FluidReferenceDataAdapter()
        assert adapter.venue == "fluid"

    @pytest.mark.asyncio
    async def test_get_instruments_wrong_type(self) -> None:
        from instruments_service.reference_data.adapters.defi.fluid import FluidReferenceDataAdapter

        adapter = FluidReferenceDataAdapter()
        result = await adapter.get_instruments(instrument_type="POOL")
        assert result == []

    @pytest.mark.asyncio
    async def test_get_instruments_success(self) -> None:
        from instruments_service.reference_data.adapters.defi.fluid import FluidReferenceDataAdapter

        adapter = FluidReferenceDataAdapter()
        with patch(
            "instruments_service.reference_data.adapters.defi.fluid.batch_resolve_evm_creation_timestamps",
            return_value={},
        ):
            results = await adapter.get_instruments()
        assert len(results) == 6  # 6 curated markets
        assert all(r.instrument_type == InstrumentType.LENDING for r in results)

    @pytest.mark.asyncio
    async def test_get_instruments_with_creation_timestamps(self) -> None:
        from instruments_service.reference_data.adapters.defi.fluid import FluidReferenceDataAdapter

        adapter = FluidReferenceDataAdapter()
        ts_map = {"0xeAbBfca72F8a8bf14C4ac59e69ECB2eB69F0811C": datetime(2024, 10, 1, tzinfo=UTC)}
        with patch(
            "instruments_service.reference_data.adapters.defi.fluid.batch_resolve_evm_creation_timestamps",
            return_value=ts_map,
        ):
            results = await adapter.get_instruments()
        assert len(results) == 6
        # First market has vault address matching our mock
        eth_usdc = [r for r in results if r.base_asset == "ETH" and r.quote_asset == "USDC"]
        assert len(eth_usdc) == 1
        assert eth_usdc[0].available_from_datetime == datetime(2024, 10, 1, tzinfo=UTC)


# ── EigenLayerReferenceDataAdapter ────────────────────────────────────────────


class TestEigenLayerAdapter:
    def test_venue_property(self) -> None:
        from instruments_service.reference_data.adapters.defi.eigenlayer import EigenLayerReferenceDataAdapter

        adapter = EigenLayerReferenceDataAdapter()
        assert adapter.venue == "eigenlayer"

    @pytest.mark.asyncio
    async def test_get_instruments_wrong_type(self) -> None:
        from instruments_service.reference_data.adapters.defi.eigenlayer import EigenLayerReferenceDataAdapter

        adapter = EigenLayerReferenceDataAdapter()
        result = await adapter.get_instruments(instrument_type="POOL")
        assert result == []

    @pytest.mark.asyncio
    async def test_get_instruments_success(self) -> None:
        from instruments_service.reference_data.adapters.defi.eigenlayer import EigenLayerReferenceDataAdapter

        adapter = EigenLayerReferenceDataAdapter()
        results = await adapter.get_instruments()
        assert len(results) == 1
        assert results[0].base_asset == "EIGEN"
        assert results[0].instrument_type == InstrumentType.SPOT_PAIR

    @pytest.mark.asyncio
    async def test_get_instruments_governance_token_type(self) -> None:
        from instruments_service.reference_data.adapters.defi.eigenlayer import EigenLayerReferenceDataAdapter

        adapter = EigenLayerReferenceDataAdapter()
        results = await adapter.get_instruments(instrument_type="GOVERNANCE_TOKEN")
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_get_instruments_governance_token_lowercase(self) -> None:
        from instruments_service.reference_data.adapters.defi.eigenlayer import EigenLayerReferenceDataAdapter

        adapter = EigenLayerReferenceDataAdapter()
        results = await adapter.get_instruments(instrument_type="governance_token")
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_get_instrument_found(self) -> None:
        from instruments_service.reference_data.adapters.defi.eigenlayer import EigenLayerReferenceDataAdapter

        adapter = EigenLayerReferenceDataAdapter()
        result = await adapter.get_instrument("EIGEN")
        assert result is not None
        assert result.base_asset == "EIGEN"

    @pytest.mark.asyncio
    async def test_get_instrument_not_found(self) -> None:
        from instruments_service.reference_data.adapters.defi.eigenlayer import EigenLayerReferenceDataAdapter

        adapter = EigenLayerReferenceDataAdapter()
        result = await adapter.get_instrument("NONEXISTENT")
        assert result is None


# ── LidoReferenceDataAdapter ─────────────────────────────────────────────────


class TestLidoAdapter:
    def test_venue_property(self) -> None:
        from instruments_service.reference_data.adapters.defi.lido import LidoReferenceDataAdapter

        adapter = LidoReferenceDataAdapter()
        assert adapter.venue == "lido"

    @pytest.mark.asyncio
    async def test_get_instruments_wrong_type(self) -> None:
        from instruments_service.reference_data.adapters.defi.lido import LidoReferenceDataAdapter

        adapter = LidoReferenceDataAdapter()
        result = await adapter.get_instruments(instrument_type="POOL")
        assert result == []

    @pytest.mark.asyncio
    async def test_get_instruments_success(self) -> None:
        from instruments_service.reference_data.adapters.defi.lido import LidoReferenceDataAdapter

        adapter = LidoReferenceDataAdapter()
        results = await adapter.get_instruments()
        assert len(results) == 2  # stETH + wstETH
        symbols = {r.instrument_key.split(":")[-1] for r in results}
        assert "STETH" in symbols
        assert "WSTETH" in symbols
        assert all(r.instrument_type == InstrumentType.YIELD_BEARING for r in results)

    @pytest.mark.asyncio
    async def test_get_instruments_yield_bearing_type(self) -> None:
        from instruments_service.reference_data.adapters.defi.lido import LidoReferenceDataAdapter

        adapter = LidoReferenceDataAdapter()
        results = await adapter.get_instruments(instrument_type="yield_bearing")
        assert len(results) == 2

    @pytest.mark.asyncio
    async def test_get_instrument_found(self) -> None:
        from instruments_service.reference_data.adapters.defi.lido import LidoReferenceDataAdapter

        adapter = LidoReferenceDataAdapter()
        result = await adapter.get_instrument("0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84")
        assert result is not None

    @pytest.mark.asyncio
    async def test_get_instrument_not_found_raises_attribute_error(self) -> None:
        """Lido get_instrument references inst.symbol which does not exist on InstrumentRecord."""
        from instruments_service.reference_data.adapters.defi.lido import LidoReferenceDataAdapter

        adapter = LidoReferenceDataAdapter()
        with pytest.raises(AttributeError):
            await adapter.get_instrument("nonexistent")


# ── EtherFiReferenceDataAdapter ───────────────────────────────────────────────


class TestEtherFiAdapter:
    def test_venue_property(self) -> None:
        from instruments_service.reference_data.adapters.defi.etherfi import EtherFiReferenceDataAdapter

        adapter = EtherFiReferenceDataAdapter()
        assert adapter.venue == "etherfi"

    @pytest.mark.asyncio
    async def test_get_instruments_wrong_type(self) -> None:
        from instruments_service.reference_data.adapters.defi.etherfi import EtherFiReferenceDataAdapter

        adapter = EtherFiReferenceDataAdapter()
        result = await adapter.get_instruments(instrument_type="POOL")
        assert result == []

    @pytest.mark.asyncio
    async def test_get_instruments_success(self) -> None:
        from instruments_service.reference_data.adapters.defi.etherfi import EtherFiReferenceDataAdapter

        adapter = EtherFiReferenceDataAdapter()
        results = await adapter.get_instruments()
        assert len(results) == 1
        assert results[0].instrument_type == InstrumentType.YIELD_BEARING
        assert results[0].base_asset == "ETH"
        assert "WEETH" in results[0].instrument_key

    @pytest.mark.asyncio
    async def test_get_instruments_yield_bearing_type(self) -> None:
        from instruments_service.reference_data.adapters.defi.etherfi import EtherFiReferenceDataAdapter

        adapter = EtherFiReferenceDataAdapter()
        results = await adapter.get_instruments(instrument_type="yield_bearing")
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_get_instrument_found(self) -> None:
        from instruments_service.reference_data.adapters.defi.etherfi import EtherFiReferenceDataAdapter

        adapter = EtherFiReferenceDataAdapter()
        result = await adapter.get_instrument("0xcd5fe23c85820f7b72d0926fc9b05b43e359b7ee")
        assert result is not None

    @pytest.mark.asyncio
    async def test_get_instrument_not_found_raises_attribute_error(self) -> None:
        """EtherFi get_instrument references inst.symbol which does not exist on InstrumentRecord."""
        from instruments_service.reference_data.adapters.defi.etherfi import EtherFiReferenceDataAdapter

        adapter = EtherFiReferenceDataAdapter()
        with pytest.raises(AttributeError):
            await adapter.get_instrument("nonexistent")


# ── EthFiGovernanceReferenceDataAdapter ───────────────────────────────────────


class TestEthFiGovernanceAdapter:
    def test_venue_property(self) -> None:
        from instruments_service.reference_data.adapters.defi.ethfi import EthFiGovernanceReferenceDataAdapter

        adapter = EthFiGovernanceReferenceDataAdapter()
        assert adapter.venue == "etherfi-governance"

    @pytest.mark.asyncio
    async def test_get_instruments_wrong_type(self) -> None:
        from instruments_service.reference_data.adapters.defi.ethfi import EthFiGovernanceReferenceDataAdapter

        adapter = EthFiGovernanceReferenceDataAdapter()
        result = await adapter.get_instruments(instrument_type="POOL")
        assert result == []

    @pytest.mark.asyncio
    async def test_get_instruments_success(self) -> None:
        from instruments_service.reference_data.adapters.defi.ethfi import EthFiGovernanceReferenceDataAdapter

        adapter = EthFiGovernanceReferenceDataAdapter()
        results = await adapter.get_instruments()
        assert len(results) == 1
        assert results[0].base_asset == "ETHFI"
        assert results[0].instrument_type == InstrumentType.SPOT_PAIR

    @pytest.mark.asyncio
    async def test_get_instruments_governance_token_type(self) -> None:
        from instruments_service.reference_data.adapters.defi.ethfi import EthFiGovernanceReferenceDataAdapter

        adapter = EthFiGovernanceReferenceDataAdapter()
        results = await adapter.get_instruments(instrument_type="GOVERNANCE_TOKEN")
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_get_instruments_governance_token_lowercase(self) -> None:
        from instruments_service.reference_data.adapters.defi.ethfi import EthFiGovernanceReferenceDataAdapter

        adapter = EthFiGovernanceReferenceDataAdapter()
        results = await adapter.get_instruments(instrument_type="governance_token")
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_get_instrument_found(self) -> None:
        from instruments_service.reference_data.adapters.defi.ethfi import EthFiGovernanceReferenceDataAdapter

        adapter = EthFiGovernanceReferenceDataAdapter()
        result = await adapter.get_instrument("ETHFI")
        assert result is not None

    @pytest.mark.asyncio
    async def test_get_instrument_not_found(self) -> None:
        from instruments_service.reference_data.adapters.defi.ethfi import EthFiGovernanceReferenceDataAdapter

        adapter = EthFiGovernanceReferenceDataAdapter()
        result = await adapter.get_instrument("NONEXISTENT")
        assert result is None


# ── EthenaReferenceDataAdapter ────────────────────────────────────────────────


class TestEthenaAdapter:
    def test_venue_property(self) -> None:
        from instruments_service.reference_data.adapters.defi.ethena import EthenaReferenceDataAdapter

        adapter = EthenaReferenceDataAdapter()
        assert adapter.venue == "ethena"

    @pytest.mark.asyncio
    async def test_get_instruments_wrong_type(self) -> None:
        from instruments_service.reference_data.adapters.defi.ethena import EthenaReferenceDataAdapter

        adapter = EthenaReferenceDataAdapter()
        result = await adapter.get_instruments(instrument_type="POOL")
        assert result == []

    @pytest.mark.asyncio
    async def test_get_instruments_success(self) -> None:
        from instruments_service.reference_data.adapters.defi.ethena import EthenaReferenceDataAdapter

        adapter = EthenaReferenceDataAdapter()
        results = await adapter.get_instruments()
        assert len(results) == 1
        assert results[0].base_asset == "sUSDe"
        assert results[0].instrument_type == InstrumentType.YIELD_BEARING
        assert results[0].underlying == "USDe"

    @pytest.mark.asyncio
    async def test_get_instruments_yield_bearing_type(self) -> None:
        from instruments_service.reference_data.adapters.defi.ethena import EthenaReferenceDataAdapter

        adapter = EthenaReferenceDataAdapter()
        results = await adapter.get_instruments(instrument_type="yield_bearing")
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_get_instrument_found(self) -> None:
        from instruments_service.reference_data.adapters.defi.ethena import EthenaReferenceDataAdapter

        adapter = EthenaReferenceDataAdapter()
        result = await adapter.get_instrument("0x9D39A5DE30e57443BfF2A8307A4256c8797A3497")
        assert result is not None

    @pytest.mark.asyncio
    async def test_get_instrument_not_found_raises_attribute_error(self) -> None:
        """Ethena get_instrument references inst.symbol which does not exist on InstrumentRecord."""
        from instruments_service.reference_data.adapters.defi.ethena import EthenaReferenceDataAdapter

        adapter = EthenaReferenceDataAdapter()
        with pytest.raises(AttributeError):
            await adapter.get_instrument("nonexistent")


# ── Error classifier functions ────────────────────────────────────────────────


class TestErrorClassifiers:
    """Test all per-adapter error classifier functions for coverage."""

    def test_raydium_rate_limit_in_message(self) -> None:
        from instruments_service.reference_data.adapters.defi.raydium import _classify_raydium_error

        assert _classify_raydium_error(Exception("429 rate limited")) == "RATE_LIMIT"

    def test_raydium_unavailable_in_message(self) -> None:
        from instruments_service.reference_data.adapters.defi.raydium import _classify_raydium_error

        assert _classify_raydium_error(Exception("service unavailable 503")) == "503"

    def test_orca_internal_server(self) -> None:
        from instruments_service.reference_data.adapters.defi.orca import _classify_orca_error

        assert _classify_orca_error(Exception("internal server error")) == "500"

    def test_drift_server_500(self) -> None:
        from instruments_service.reference_data.adapters.defi.drift import _classify_drift_error

        assert _classify_drift_error(Exception("500 error"), status=500) == "500"

    def test_kamino_rate_in_message(self) -> None:
        from instruments_service.reference_data.adapters.defi.kamino import _classify_kamino_error

        assert _classify_kamino_error(Exception("rate limit exceeded")) == "RATE_LIMIT"

    def test_marinade_server_error(self) -> None:
        from instruments_service.reference_data.adapters.defi.marinade import _classify_marinade_error

        assert _classify_marinade_error(Exception("server error"), status=502) == "500"

    def test_jito_unavailable(self) -> None:
        from instruments_service.reference_data.adapters.defi.jito import _classify_jito_error

        assert _classify_jito_error(Exception("503 unavailable")) == "503"


# ── RuntimeError path for _get_with_retry failures ───────────────────────────


class TestRuntimeErrorPaths:
    """Test RuntimeError handling in adapters that catch both ClientError and RuntimeError."""

    @pytest.mark.asyncio
    async def test_raydium_runtime_error(self) -> None:
        from instruments_service.reference_data.adapters.defi.raydium import RaydiumReferenceDataAdapter

        adapter = RaydiumReferenceDataAdapter()
        with (
            patch.object(adapter, "_get_with_retry", side_effect=RuntimeError("all retries failed")),
            pytest.raises(RuntimeError, match="all retries failed"),
        ):
            await adapter.get_instruments()

    @pytest.mark.asyncio
    async def test_orca_runtime_error(self) -> None:
        from instruments_service.reference_data.adapters.defi.orca import OrcaReferenceDataAdapter

        adapter = OrcaReferenceDataAdapter()
        with (
            patch.object(adapter, "_get_with_retry", side_effect=RuntimeError("all retries failed")),
            pytest.raises(RuntimeError, match="all retries failed"),
        ):
            await adapter.get_instruments()

    @pytest.mark.asyncio
    async def test_drift_runtime_error(self) -> None:
        from instruments_service.reference_data.adapters.defi.drift import DriftReferenceDataAdapter

        adapter = DriftReferenceDataAdapter()
        with (
            patch.object(adapter, "_get_with_retry", side_effect=RuntimeError("all retries failed")),
            pytest.raises(RuntimeError, match="all retries failed"),
        ):
            await adapter.get_instruments()

    @pytest.mark.asyncio
    async def test_kamino_runtime_error(self) -> None:
        from instruments_service.reference_data.adapters.defi.kamino import KaminoReferenceDataAdapter

        adapter = KaminoReferenceDataAdapter()
        with (
            patch.object(adapter, "_get_with_retry", side_effect=RuntimeError("all retries failed")),
            pytest.raises(RuntimeError, match="all retries failed"),
        ):
            await adapter.get_instruments()

    @pytest.mark.asyncio
    async def test_marinade_runtime_error(self) -> None:
        from instruments_service.reference_data.adapters.defi.marinade import MarinadeReferenceDataAdapter

        adapter = MarinadeReferenceDataAdapter()
        with (
            patch.object(adapter, "_get_with_retry", side_effect=RuntimeError("all retries failed")),
            pytest.raises(RuntimeError, match="all retries failed"),
        ):
            await adapter.get_instruments()

    @pytest.mark.asyncio
    async def test_jito_runtime_error(self) -> None:
        from instruments_service.reference_data.adapters.defi.jito import JitoReferenceDataAdapter

        adapter = JitoReferenceDataAdapter()
        with (
            patch.object(adapter, "_get_with_retry", side_effect=RuntimeError("all retries failed")),
            pytest.raises(RuntimeError, match="all retries failed"),
        ):
            await adapter.get_instruments()


# ── Timestamp resolution in Solana adapters ──────────────────────────────────


class TestTimestampResolution:
    """Test batch_resolve_creation_timestamps integration in adapters."""

    @pytest.mark.asyncio
    async def test_orca_timestamp_resolution(self) -> None:
        from instruments_service.reference_data.adapters.defi.orca import OrcaReferenceDataAdapter

        adapter = OrcaReferenceDataAdapter()
        whirlpool_data = {
            "whirlpools": [
                {
                    "address": "wp_addr_1",
                    "tokenA": {"symbol": "SOL", "decimals": 9},
                    "tokenB": {"symbol": "USDC", "decimals": 6},
                    "tvl": 50000,
                    "tickSpacing": 64,
                }
            ]
        }
        # Use a timestamp EARLIER than the Orca deploy floor date so the RPC result replaces it
        resolved_ts = datetime(2021, 1, 1, tzinfo=UTC)
        with (
            patch.object(adapter, "_get_with_retry", return_value=whirlpool_data),
            patch(
                "instruments_service.reference_data.adapters.defi._solana_utils.batch_resolve_creation_timestamps",
                return_value={"wp_addr_1": resolved_ts},
            ),
        ):
            results = await adapter.get_instruments()
        assert len(results) == 1
        assert results[0].available_from_datetime == resolved_ts

    @pytest.mark.asyncio
    async def test_raydium_timestamp_resolution_min_existing(self) -> None:
        from instruments_service.reference_data.adapters.defi.raydium import RaydiumReferenceDataAdapter

        adapter = RaydiumReferenceDataAdapter()
        pool_data = {
            "data": {
                "data": [
                    {
                        "id": "pool_1",
                        "mintA": {"symbol": "SOL", "decimals": 9},
                        "mintB": {"symbol": "USDC", "decimals": 6},
                        "tvl": 50000,
                        "openTime": "1677000000",
                        "type": "Standard",
                    }
                ]
            }
        }
        # RPC returns a timestamp NEWER than the REST timestamp → should keep REST timestamp
        rpc_ts = datetime(2025, 1, 1, tzinfo=UTC)
        with (
            patch.object(adapter, "_get_with_retry", return_value=pool_data),
            patch(
                "instruments_service.reference_data.adapters.defi.raydium.batch_resolve_creation_timestamps",
                return_value={"pool_1": rpc_ts},
            ),
        ):
            results = await adapter.get_instruments()
        # The REST timestamp is earlier, so it should be kept
        assert len(results) == 1
        assert results[0].available_from_datetime is not None

    @pytest.mark.asyncio
    async def test_kamino_timestamp_resolution(self) -> None:
        from instruments_service.reference_data.adapters.defi.kamino import KaminoReferenceDataAdapter

        adapter = KaminoReferenceDataAdapter()
        strategies = [{"address": "vault_1", "status": "LIVE", "tokenAMint": "SOL", "tokenBMint": "USDC"}]
        # Use a timestamp EARLIER than the Kamino deploy floor date so the RPC result replaces it
        resolved_ts = datetime(2023, 1, 1, tzinfo=UTC)
        with (
            patch.object(adapter, "_get_with_retry", return_value=strategies),
            patch.object(adapter, "_resolve_symbol", side_effect=lambda m: m),
            patch(
                "instruments_service.reference_data.adapters.defi._solana_utils.batch_resolve_creation_timestamps",
                return_value={"vault_1": resolved_ts},
            ),
        ):
            results = await adapter.get_instruments()
        assert len(results) == 1
        assert results[0].available_from_datetime == resolved_ts
