"""AAVE on-chain oracle price adapter — AaveOracle.getAssetPrice reference data.

Discovers the AAVE-ETHEREUM oracle_prices leg: the Phase-0-verified LST reserves
whose USD price is readable via ``AaveOracle.getAssetPrice`` on Ethereum mainnet
(see ``lst_rate_honest_coverage`` plan Phase 0 — real ``eth_call`` returns,
2026-07-21). This extends the already-existing AAVE-ETHEREUM venue
(governance_events remains pipeline/NOT IS-producible via this adapter);
oracle_prices is the new IS-producible leg.

Enumeration is a STATIC curated registry (the verified reserve set below) — no
network call is made at reference-data discovery time, mirroring the Chainlink
oracle adapter (``chainlink.py``). The actual oracle prices are fetched by MTDS
at runtime via the same ``getAssetPrice`` eth_call
(``market-tick-data-service`` ``aave_positions.py::_fetch_rpc_oracle_prices``,
lifted not re-implemented).

Each reserve is emitted as a SPOT_ASSET instrument (the on-chain receipt token
itself), reusing ``build_spot_asset_record`` — the same shape as the other
single-token LST adapters (``cbeth.py`` / ``wbeth.py``).

Reference: plan ``lst_rate_honest_coverage_2026_07_21.md`` Phase 0/1.
"""

from __future__ import annotations

import logging
from datetime import datetime

from unified_api_contracts.internal import InstrumentRecord, InstrumentType

from ...base_adapter import BaseReferenceDataAdapter
from ...schemas import (
    CanonicalExpiryCalendar,
    CanonicalOptionsChain,
    FundingRateRef,
    OHLCVRef,
)
from ...utils.defi_utils import build_spot_asset_record
from ...utils.evm_creation_resolver import get_protocol_floor_date

logger = logging.getLogger(__name__)

_DEFAULT_CHAIN = "ETHEREUM"
_VENUE = "AAVE-ETHEREUM"

# Phase-0-verified AAVE reserves (real `getAssetPrice` eth_call returns,
# 2026-07-21 — wf_f629fbb4-7da journal). osETH and ezETH@0x2416092f... are
# deliberately EXCLUDED (getAssetPrice REVERTS on both — would seed a
# permanent-false-RED honest-coverage cell). All 18-decimal ERC-20 tokens.
_AAVE_ORACLE_RESERVES: dict[str, str] = {
    "WSTETH": "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0",  # DERIVED 2026-07-21 from ethereum eth_call getAssetPrice (wf_f629fbb4-7da)
    "WEETH": "0xCd5fE23C85820F7B72D0926FC9b05b43E359b7ee",  # DERIVED 2026-07-21 from ethereum eth_call getAssetPrice (wf_f629fbb4-7da)
    "RETH": "0xae78736Cd615f374D3085123A210448E74Fc6393",  # DERIVED 2026-07-21 from ethereum eth_call getAssetPrice (wf_f629fbb4-7da)
    "CBETH": "0xBe9895146f7AF43049ca1c1AE358B0541Ea49704",  # DERIVED 2026-07-21 from ethereum eth_call getAssetPrice (wf_f629fbb4-7da)
    "RSETH": "0xA1290d69c65A6Fe4DF752f95823fAe25cB99e5A7",  # DERIVED 2026-07-21 from ethereum eth_call getAssetPrice (wf_f629fbb4-7da), AAVE-path-only
    "EZETH": "0xbf5495Efe5DB9ce00f80364C8B423567e58d2110",  # DERIVED 2026-07-21 from ethereum eth_call getAssetPrice (wf_f629fbb4-7da), this address ONLY
}

_AAVE_ORACLE_DECIMALS = 18


class AaveOracleReferenceDataAdapter(BaseReferenceDataAdapter):
    """AAVE on-chain oracle reference data: curated verified-reserve enumeration.

    Ethereum-only (the Phase-0 verification pass covered mainnet reserves
    only). Each reserve becomes a SPOT_ASSET instrument keyed off the
    receipt-token contract address, mirroring the Chainlink/cbETH pattern.
    """

    def __init__(
        self,
        project_id: str | None = None,
        api_key: str | None = None,
        chain: str = _DEFAULT_CHAIN,
    ) -> None:
        super().__init__(project_id=project_id, api_key=api_key)
        self._chain = chain.upper()

    @property
    def venue(self) -> str:
        """Return the venue identifier (AAVE-ETHEREUM)."""
        return _VENUE

    async def get_instruments(
        self,
        instrument_type: str | None = None,
    ) -> list[InstrumentRecord]:
        """Return AAVE oracle SPOT_ASSET instruments for the verified reserves."""
        if self._chain != _DEFAULT_CHAIN:
            logger.info("AAVE oracle: no verified reserves for chain %s (Ethereum-only)", self._chain)
            return []
        if instrument_type not in (None, InstrumentType.SPOT_ASSET, "spot"):
            logger.info("AAVE oracle only supports SPOT_ASSET instruments; requested %s", instrument_type)
            return []

        available_from = get_protocol_floor_date("aave_v3", self._chain)
        results: list[InstrumentRecord] = []
        for symbol, address in _AAVE_ORACLE_RESERVES.items():
            record = build_spot_asset_record(
                venue=_VENUE,
                symbol=symbol,
                contract_address=address,
                decimals=_AAVE_ORACLE_DECIMALS,
                available_from_datetime=available_from,
            )
            if record:
                results.append(record)

        logger.info("AAVE oracle: built %d SPOT_ASSET instruments on %s", len(results), self._chain)
        return results

    async def get_instrument(self, symbol: str) -> InstrumentRecord | None:
        """Fetch a single instrument by symbol or contract address."""
        instruments = await self.get_instruments()
        sym_upper = symbol.upper()
        for inst in instruments:
            if inst.raw_symbol == symbol or inst.base_asset == sym_upper:
                return inst
        return None

    async def get_options_chain(
        self,
        underlying: str,
        expiry: datetime | None = None,
    ) -> CanonicalOptionsChain:
        """Return options chain; not supported for this venue."""
        raise NotImplementedError("AAVE oracle does not support options")

    async def get_expiry_calendar(
        self,
        underlying: str,
        instrument_type: str = "FUTURE",
    ) -> CanonicalExpiryCalendar:
        """Return expiry calendar; not supported for this venue."""
        raise NotImplementedError("AAVE oracle feeds have no expiry calendar")

    async def get_funding_rate(self, symbol: str) -> FundingRateRef:
        """Return funding rate; not supported for this venue."""
        raise NotImplementedError("AAVE oracle funding rate not supported via reference data")

    async def get_ohlcv(
        self,
        symbol: str,
        interval: str = "1d",
        limit: int = 100,
    ) -> list[OHLCVRef]:
        """Return ohlcv; not supported for this venue."""
        raise NotImplementedError("AAVE oracle OHLCV not supported via reference data")
