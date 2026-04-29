"""Unit tests — Phase 2c: EigenLayer adapter populates DeFi metadata fields.

Verifies that the EigenLayer adapter (hardcoded EIGEN governance token) emits
an InstrumentRecord with the new DeFi metadata columns populated:
`pool_address`, `base_asset_contract_address`, `base_asset_decimals`,
`base_asset_symbol_onchain`.

Aave-shaped fields (`atoken_address`, `debt_token_address`) and DEX-shaped
quote-side fields are left None — EIGEN is a vanilla single-asset ERC-20
governance token, not a lending market or paired pool.

Plan: instruments_service_metadata_refactor_2026_04_29 Phase 2c.
"""

from __future__ import annotations

import pytest

from instruments_service.reference_data.adapters.defi.eigenlayer import (
    EigenLayerReferenceDataAdapter,
)


class TestEigenLayerMetadataRoundTrip:
    """EigenLayer EIGEN governance token → InstrumentRecord populates DeFi metadata."""

    @pytest.mark.asyncio
    async def test_eigen_record_populates_metadata_fields(self) -> None:
        adapter = EigenLayerReferenceDataAdapter()

        results = await adapter.get_instruments()

        assert len(results) == 1
        record = results[0]

        # EIGEN ERC-20 contract on Ethereum mainnet — both pool_address and
        # base_asset_contract_address resolve to it. Adapter MUST not lower-case
        # or otherwise mutate the canonical EIP-55 mixed-case address.
        assert record.pool_address == "0xec53bF9167f50cDEB3Ae105f56099aaaB9061F83"
        assert record.base_asset_contract_address == "0xec53bF9167f50cDEB3Ae105f56099aaaB9061F83"
        # Standard ERC-20 default — EIGEN deployed contract declares 18.
        assert record.base_asset_decimals == 18
        assert record.base_asset_symbol_onchain == "EIGEN"

    @pytest.mark.asyncio
    async def test_eigen_record_leaves_aave_shaped_fields_none(self) -> None:
        """EIGEN is not a lending market — atoken / debt_token / fee_tier MUST be None."""
        adapter = EigenLayerReferenceDataAdapter()

        results = await adapter.get_instruments()

        assert len(results) == 1
        record = results[0]

        # Lending-shape — None.
        assert record.atoken_address is None
        assert record.debt_token_address is None
        assert record.rate_method_selector is None
        # DEX-shape — single-asset governance token has no paired ERC-20.
        assert record.quote_asset_contract_address is None
        assert record.quote_asset_decimals is None
        assert record.quote_asset_symbol_onchain is None
        # No DEX pool fee — EIGEN is the token itself, not a swap pool.
        assert record.pool_fee_tier is None

    @pytest.mark.asyncio
    async def test_eigen_record_canonical_identity_unchanged(self) -> None:
        """Phase 2c is metadata-only — pre-existing identity fields MUST be untouched."""
        adapter = EigenLayerReferenceDataAdapter()

        results = await adapter.get_instruments()

        assert len(results) == 1
        record = results[0]

        assert record.instrument_key == "EIGENLAYER-ETHEREUM:GOVERNANCE_TOKEN:EIGEN"
        assert record.venue == "EIGENLAYER-ETHEREUM"
        assert record.raw_symbol == "0xec53bF9167f50cDEB3Ae105f56099aaaB9061F83"
        assert record.base_asset == "EIGEN"
        assert record.quote_asset == "ETH"
        assert record.underlying == "ETH"

    @pytest.mark.asyncio
    async def test_eigen_record_filters_on_instrument_type(self) -> None:
        """Filtering by instrument_type='SPOT_PAIR' or unrelated value should still work."""
        adapter = EigenLayerReferenceDataAdapter()

        # Whitelisted filter values: None, 'GOVERNANCE_TOKEN', 'governance_token'.
        results_default = await adapter.get_instruments()
        results_upper = await adapter.get_instruments(instrument_type="GOVERNANCE_TOKEN")
        results_lower = await adapter.get_instruments(instrument_type="governance_token")

        assert len(results_default) == 1
        assert len(results_upper) == 1
        assert len(results_lower) == 1
        # Metadata is consistent across all filter modes.
        for record in (results_default[0], results_upper[0], results_lower[0]):
            assert record.base_asset_decimals == 18
            assert record.base_asset_symbol_onchain == "EIGEN"

        # Unrelated instrument type → empty list, no records emitted.
        results_other = await adapter.get_instruments(instrument_type="FUTURE")
        assert results_other == []
