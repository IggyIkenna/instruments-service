"""DeFi metadata round-trip tests for lending-protocol adapters.

Phase 2a of instruments_service_metadata_refactor_2026_04_29: verifies that
Aave V3, Compound V3, and Morpho Blue adapters populate the new DeFi metadata
fields on `InstrumentRecord` (pool_address, base_asset_contract_address,
base_asset_decimals, base_asset_symbol_onchain, atoken_address,
debt_token_address, quote_asset_*).

All tests are credential-free and use unittest.mock — no live network.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


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


class TestAaveV3MetadataRoundTrip:
    """Aave V3 reserves → InstrumentRecord populates DeFi metadata fields."""

    @pytest.mark.asyncio
    async def test_aave_reserve_populates_metadata_fields(self) -> None:
        from instruments_service.reference_data.adapters.defi.aave_v3 import AaveV3ReferenceDataAdapter

        adapter = AaveV3ReferenceDataAdapter(api_key="test-key")
        reserves_data = {
            "data": {
                "reserves": [
                    {
                        "id": "0xreserve_weth_eth",
                        "underlyingAsset": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                        "symbol": "WETH",
                        "name": "Wrapped Ether",
                        "decimals": 18,
                        "borrowingEnabled": True,
                        "isActive": True,
                        "aToken": {"id": "0xaweth_token_addr"},
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

        assert len(results) == 2  # A_TOKEN + DEBT_TOKEN
        for record in results:
            assert record.pool_address == "0xreserve_weth_eth"
            assert record.base_asset_contract_address == "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
            assert record.base_asset_decimals == 18
            assert record.base_asset_symbol_onchain == "WETH"
            assert record.atoken_address == "0xaweth_token_addr"
            # debt token address not surfaced by current subgraph query — expected None
            assert record.debt_token_address is None

    @pytest.mark.asyncio
    async def test_aave_reserve_decimals_as_string(self) -> None:
        """Subgraph sometimes returns numeric fields as strings — coerce."""
        from instruments_service.reference_data.adapters.defi.aave_v3 import AaveV3ReferenceDataAdapter

        adapter = AaveV3ReferenceDataAdapter(api_key="test-key")
        reserves_data = {
            "data": {
                "reserves": [
                    {
                        "id": "0xreserve_usdc",
                        "underlyingAsset": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                        "symbol": "USDC",
                        "decimals": "6",
                        "borrowingEnabled": False,
                        "aToken": {"id": "0xausdc"},
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

        assert len(results) == 1
        record = results[0]
        assert record.base_asset_decimals == 6
        assert record.atoken_address == "0xausdc"


class TestCompoundV3MetadataRoundTrip:
    """Compound V3 markets → InstrumentRecord populates DeFi metadata fields."""

    @pytest.mark.asyncio
    async def test_compound_market_populates_metadata_fields(self) -> None:
        from instruments_service.reference_data.adapters.defi.compound_v3 import CompoundV3ReferenceDataAdapter

        adapter = CompoundV3ReferenceDataAdapter(api_key="test-key")
        markets_data = {
            "data": {
                "markets": [
                    {
                        "id": "0xmarket_id",
                        "cometProxy": "0xc3d688b66703497daa19211eedff47f25384cdc3",
                        "creationBlockNumber": "15331586",
                        "configuration": {
                            "name": "Compound USDC",
                            "symbol": "cUSDCv3",
                            "baseToken": {
                                "token": {
                                    "id": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                                    "symbol": "USDC",
                                    "name": "USD Coin",
                                    "decimals": "6",
                                },
                                "lastPriceUsd": "1.0",
                            },
                        },
                        "accounting": {},
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
                return_value=None,
            ),
        ):
            results = await adapter.get_instruments()

        assert len(results) == 2  # SUPPLY + BORROW
        for record in results:
            assert record.pool_address == "0xc3d688b66703497daa19211eedff47f25384cdc3"
            assert record.base_asset_contract_address == "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
            assert record.base_asset_decimals == 6
            assert record.base_asset_symbol_onchain == "USDC"


class TestMorphoMetadataRoundTrip:
    """Morpho Blue markets → InstrumentRecord populates DeFi metadata fields."""

    @pytest.mark.asyncio
    async def test_morpho_market_populates_metadata_fields(self) -> None:
        from instruments_service.reference_data.adapters.defi.morpho import MorphoReferenceDataAdapter

        adapter = MorphoReferenceDataAdapter()
        markets_data = {
            "data": {
                "markets": {
                    "items": [
                        {
                            "uniqueKey": "0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc",
                            "loanAsset": {
                                "address": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                                "symbol": "USDC",
                                "name": "USD Coin",
                                "decimals": 6,
                            },
                            "collateralAsset": {
                                "address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                                "symbol": "WETH",
                                "name": "Wrapped Ether",
                                "decimals": 18,
                            },
                            "lltv": "860000000000000000",
                            "state": {},
                        }
                    ]
                }
            }
        }
        with patch("aiohttp.ClientSession", return_value=_mock_aiohttp_session_post(markets_data)):
            results = await adapter.get_instruments()

        assert len(results) == 1
        record = results[0]
        # Morpho uses the bytes32 uniqueKey as the canonical pool identifier
        assert record.pool_address == "0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc"
        # Collateral = base
        assert record.base_asset_contract_address == "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
        assert record.base_asset_decimals == 18
        assert record.base_asset_symbol_onchain == "WETH"
        # Loan = quote
        assert record.quote_asset_contract_address == "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
        assert record.quote_asset_decimals == 6
        assert record.quote_asset_symbol_onchain == "USDC"
